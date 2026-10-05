//! Durable shape catalog: the append-only `meta/catalog` event stream, its writer
//! task, and boot-time restore/resume of shapes.

use super::*;

/// The engine's durable **shape catalog**: an append-only event stream replayed at boot so a
/// restart re-registers every shape itself instead of requiring a client re-registration storm.
/// Plain/routed shapes resume with passthrough gates (the change log replays everything after the
/// persisted offset; re-emission across the crash window is idempotent absolute upserts);
/// aggregates re-seed their fold from a fresh Postgres snapshot (their fresh gate then skips the
/// replayed history). Subquery shapes are NOT restorable without persisted inner-node state (a
/// fresh-seeded node cannot detect downtime flips, which would leave stale move-outs forever) —
/// they are dropped loudly at restore for clients to recreate. A restorable shape whose stream
/// storage is definitively missing or closed is likewise dropped and retired at boot; a transport
/// error while checking the stream still aborts the restore, and the boot backs off and retries it
/// (`pg::boot_disposition`) rather than exiting.
pub(crate) const CATALOG_STREAM: &str = "meta/catalog";

/// One catalog event. `Offset` checkpoints the sequencer's processed change-log position (the
/// replay start after a restart), appended at most every ~2s.
///
/// Every event carries an **`eid`** on the wire — assigned at enqueue, not here (see
/// [`CatalogWriter::enqueue`]), because it identifies the *append attempt* the writer may repeat,
/// not the value. The fold ignores an `eid` it has already applied, which is what makes the writer's
/// retry-in-place (no event is ever dropped, ADR-0007) safe for a record whose effect is not
/// naturally idempotent.
// `Created` carries a whole `ShapeRecord` and is much larger than the other variants. Boxing it
// would move the allocation onto every create rather than removing it: these events are built one at
// a time, sent through the writer's channel and serialized immediately, so the enum's size is never
// multiplied by anything.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "t", rename_all = "camelCase")]
pub(crate) enum CatalogEvent {
    /// The immutable storage and namespace identity. It is event zero for every coupled-pilot
    /// catalog and never changes within that physical namespace.
    StoreBound(crate::store_identity::StoreBound),
    /// A shape was created, with the **subscription** that created it (ADR-0008) and the wall-clock
    /// second it was taken at — the lease's start, restored by the fold so a restart does not hand
    /// every subscription a fresh window.
    Created {
        rec: ShapeRecord,
        sig: Option<String>,
        subscription: String,
        at: u64,
    },
    /// A subscription joined a shared feed, or RENEWED its lease (ADR-0008): the same subscription
    /// id twice is one claim, and the second one only moves `at`. The fold keeps a SET, so a
    /// duplicate is a no-op rather than a second count.
    Joined {
        id: String,
        subscription: String,
        at: u64,
    },
    /// A subscription left a shared feed. `Left` for an id the shape does not hold is a no-op (the
    /// release is idempotent — that is the point). With retention, an empty live set keeps the
    /// shape (it goes dormant later), so `Left` never implies teardown.
    ///
    /// `lapsed` marks the retention sweeper's own release: the lease was not renewed within the
    /// idle window (ADR-0008). It changes nothing about the fold — a lapse and an explicit release
    /// are the same act — and exists so the durable record explains why a subscription no one
    /// deleted disappeared.
    Left {
        id: String,
        subscription: String,
        #[serde(default, skip_serializing_if = "is_false")]
        lapsed: bool,
    },
    /// The shape went dormant: routing state dropped, stream + record retained. `resume` is the
    /// change-log position its stream is complete up to — a `(segment, offset)` pair since
    /// ADR-0006, because the offset alone is meaningless once the log has rotated; `gate` is its
    /// original backfill-snapshot fence. Restores as dormant (an improvement over the
    /// in-memory-only lifecycle: a restart no longer forgets dormant shapes).
    Dormant {
        id: String,
        resume: LogPosition,
        gate: crate::pg::SnapshotGate,
    },
    /// A dormant shape was reactivated (replayed + re-registered).
    Reactivated {
        id: String,
    },
    /// Plain-shape backfill sizing, emitted after the streamed snapshot completes. Older records
    /// simply omit this event and therefore have unknown sizing at reactivation time.
    BackfillSized {
        id: String,
        rows: u64,
        bytes: u64,
    },
    Dropped {
        id: String,
    },
    /// The shape's stream retirement COMPLETED: closed, then deleted (ADR-0007). Written only after
    /// storage accepted the delete — a 404/410 counts, deletion is idempotent — so a `Dropped` with
    /// no `Retired` after it is exactly "the engine promised to remove this stream and did not".
    ///
    /// The pair is what makes retirement survive a crash: `Dropped` is the durable INTENT (always
    /// written before the retirement is attempted), `Retired` the durable COMPLETION, and the boot
    /// hands every unmatched intent to the retirement queue. Without it a stream whose delete was
    /// refused by storage would stay open forever, serving rows Postgres no longer has, with no
    /// record anywhere that it should be gone (the engine has already forgotten the shape).
    Retired {
        id: String,
    },
    /// The sequencer's checkpoint: the change-log position a restart replays from, plus the
    /// `(lsn, seq)` de-duplication highwater it had applied up to at that position (ADR-0003).
    ///
    /// The highwater has to travel WITH the position. A commit too large for one request body is
    /// appended in several chunks, so a crash can leave a prefix of a transaction applied and
    /// checkpointed while the rest is re-delivered; without the restored highwater that prefix would
    /// be applied twice, and aggregate/subquery contributor weights are not idempotent under
    /// duplicates. `None` is a checkpoint taken before anything was applied (a fresh boot).
    Offset {
        pos: LogPosition,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        highwater: Option<(u64, u64)>,
    },
    /// The change log rotated (ADR-0006): `segment` became the CURRENT segment at `at` (unix
    /// seconds). Written on the first creation of segment 0 too, so every segment's start time is
    /// in the log — segment `n` was closed when `n+1` began, which is what the retain window that
    /// governs deletion measures against. The fold's last one is the current segment.
    ChangesRotated {
        segment: u32,
        at: u64,
    },
    /// The retention sweeper deleted a rotated-out change-log segment (ADR-0006). Without it the
    /// fold's segment set would keep every segment the log ever had, so every restart would re-plan
    /// retiring streams that are long gone (harmless — a delete of an absent stream is a no-op — but
    /// it would also make the `changes_segments_retained` gauge wrong until the first sweep).
    ChangesSegmentDeleted {
        segment: u32,
    },
    /// An external reader's durable resume point. Unlike a shape read, this is an explicit
    /// retention claim and is therefore replayed before the retention sweeper is allowed to run.
    ConsumerPinned {
        id: String,
        position: LogPosition,
    },
    /// Release a consumer pin (explicitly, or because the retain window won). Keeping this as a
    /// separate event makes eviction crash-safe: deletion can only observe the pin gone after the
    /// catalog has recorded why it is gone.
    ConsumerEvicted {
        id: String,
    },
    /// **Audit only**: a table's schema drifted and was re-introspected (ADR-0005). The restore
    /// ignores it — every dependent shape of the table was retired by the same handler, so it is
    /// already `Dropped` in the log. It is written so the durable record explains *why* a swathe of
    /// shapes disappeared at a given point.
    SchemaChanged {
        table: TableRef,
        fingerprint: crate::schema::SchemaFingerprint,
    },
    /// Durable deployment-handoff receipt. It is appended only after the sequencer processed the
    /// complete source transaction and every direct and deferred shape append caused by it landed.
    SourceDrained(SourceDrainReceipt),
    /// The engine created (or first adopted) its replication slot: the **epoch** every shape after
    /// this point in the log belongs to (ADR-0004). The LAST one wins — a reset appends a new one
    /// after the `Dropped` records of the epoch it ended, so a fold reads "these shapes, in this
    /// epoch" straight off the log.
    SlotBound(crate::engine::epoch::SlotBinding),
}

/// `serde`'s `skip_serializing_if` wants a path, so the "omit the default" predicate is a function.
fn is_false(b: &bool) -> bool {
    !*b
}

/// Exit code when the durable catalog **refused** an event (`EX_IOERR`): storage answered, the
/// answer will not change with time, and the event cannot be written.
///
/// This is the pathological state "memory and storage disagree" — the engine is serving shapes the
/// durable record does not describe, and every second it keeps running widens the gap. There is no
/// in-process repair: the catalog is append-only and the fold IS the restart contract, so the only
/// way memory becomes consistent again is to re-fold from storage, i.e. to restart. So the process
/// exits, loudly and named, rather than continuing over a record it knows is wrong.
pub(crate) const EXIT_CATALOG_REFUSED: i32 = 74;

/// What the writer does about an append it could not complete. Pure and separately decided, so the
/// "exit the process" branch is unit-testable without exiting anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AppendVerdict {
    /// Transport, timeout or 5xx: storage is having a moment. Retry THIS event, in place, forever —
    /// the queue is ordered and single-consumer, so everything behind it waits, which is exactly
    /// right (a later event must never reach the log before an earlier one).
    Retry,
    /// A definite refusal: a 4xx the catalog cannot recover from, or an event that would not
    /// serialize. Nothing about waiting changes the answer.
    Refused,
}

/// Classify a failed catalog append. `is_unavailable` is deliberately the ONLY forgiving side (see
/// [`crate::ds::is_unavailable`]): it forgives the transport and 5xx, and nothing else.
pub(crate) fn classify_append(e: &anyhow::Error) -> AppendVerdict {
    if crate::ds::is_unavailable(e) { AppendVerdict::Retry } else { AppendVerdict::Refused }
}

/// The retry schedule for a catalog append: 100 ms before the first retry, doubling, capped at 5 s.
/// `attempt` counts from 1. Pure, so the schedule is a unit test rather than a comment.
pub(crate) fn catalog_backoff(attempt: u32) -> std::time::Duration {
    let step = attempt.saturating_sub(1).min(6);
    std::time::Duration::from_millis(100u64.saturating_mul(1u64 << step)).min(CATALOG_BACKOFF_MAX)
}

const CATALOG_BACKOFF_MAX: std::time::Duration = std::time::Duration::from_secs(5);

/// One queued catalog event, plus the (optional) acknowledgement its sender is waiting on.
struct CatalogSend {
    ev: CatalogEvent,
    /// Monotonic queue position. A client retry that finds its in-memory mutation already applied
    /// waits until this position has landed before acknowledging the apparent no-op.
    seq: u64,
    /// The event id the fold de-duplicates on (ADR-0008). Assigned at ENQUEUE, so every append
    /// attempt of this event — the first and every retry after a lost response — carries the same
    /// one.
    eid: String,
    /// Resolved once the append has SUCCEEDED — the durable-before-ack half of a client-facing
    /// mutation (see [`CatalogWriter::send_durable`]). `None` for engine-initiated events, which
    /// nobody is waiting on.
    done: Option<tokio::sync::oneshot::Sender<()>>,
}

/// The catalog writer's ordered channel, plus the count of events sent but not yet appended.
///
/// The counter exists for one caller: the circuit-tier drift path exits the process, and it must
/// not do so with its own `Dropped`/`SchemaChanged` events still in the queue — a restart would
/// then restore shapes whose streams it had just deleted (see [`CatalogWriter::drain`]).
#[derive(Clone)]
pub(crate) struct CatalogWriter {
    tx: mpsc::UnboundedSender<CatalogSend>,
    shutdown: crate::shutdown::ShutdownToken,
    in_flight: Arc<std::sync::atomic::AtomicI64>,
    /// This Engine instance's half of every `eid` it mints: a boot nonce, so two engines writing to the
    /// same catalog (a restart, or the same storage adopted by another process) can never mint the
    /// same id for different events. The other half is [`Self::next_eid`].
    eid_nonce: String,
    /// Monotonic within the Engine instance. Together with `eid_nonce` this is the whole identity of an
    /// append attempt — no clock, no hashing of the payload (two identical `Left`s ARE two
    /// different events; only a retry of the same one must be de-duplicated).
    next_eid: Arc<std::sync::atomic::AtomicU64>,
    /// Highest queue position that has landed, plus a wake-up for durability barriers.
    landed_seq: Arc<std::sync::atomic::AtomicU64>,
    landed_notify: Arc<tokio::sync::Notify>,
    /// The last `Offset` checkpoint that has actually **landed in storage** — what a restart would
    /// resume from, as opposed to what this process has processed in memory.
    ///
    /// It is the floor for change-log segment deletion (ADR-0006), and it has to be: the sequencer
    /// publishes its in-memory position the instant it crosses a segment boundary, but the
    /// checkpoint is an async append. Deleting on the in-memory position would let a sweep delete
    /// segment `n` while the durable checkpoint still says `(n, X)` — and a crash in that window
    /// leaves a boot resuming inside a stream that no longer exists.
    durable: Arc<std::sync::Mutex<Option<LogPosition>>>,
}

impl CatalogWriter {
    /// Enqueue an event. Infallible by design: a dead writer means this Engine is stopping, and
    /// no caller has a better answer than continuing (the previous code spelled this `let _ =`).
    ///
    /// For an event a CLIENT is being told about — a create, a join, a release, an explicit purge —
    /// use [`Self::send_durable`] instead: an acknowledged mutation the durable record does not
    /// contain is a shape that vanishes at the next restart.
    pub(crate) fn send(&self, ev: CatalogEvent) {
        if !self.enqueue(ev, None) {
            tracing::error!("durable catalog writer stopped before accepting an event; beginning fail-closed shutdown");
            self.shutdown.begin();
        }
    }

    /// Enqueue an event and hand back a future that resolves once it has **landed in storage**.
    ///
    /// The send happens here, synchronously, so callers keep enqueueing under the state lock and the
    /// log order still matches the state-mutation order; only the WAIT moves to the caller's own
    /// await point (after the lock is released, immediately before it answers its client).
    ///
    /// It resolves successfully only after the append lands. The writer retries a transient failure
    /// forever and exits [`EXIT_CATALOG_REFUSED`] on a definite refusal; if the writer task itself is
    /// lost, the typed error prevents any caller from manufacturing durability. There is deliberately
    /// no timeout — a create while storage is down waits, because the alternative is telling a client
    /// about a shape that will not exist after a restart.
    pub(crate) fn send_durable(
        &self,
        ev: CatalogEvent,
    ) -> impl std::future::Future<Output = std::result::Result<(), CatalogWriterGone>> + Send + 'static {
        let (done, wait) = tokio::sync::oneshot::channel();
        let queued = self.enqueue(ev, Some(done));
        let shutdown = self.shutdown.clone();
        async move {
            if !queued {
                tracing::error!(
                    "durable catalog writer stopped before accepting an acknowledged append; beginning fail-closed shutdown"
                );
                shutdown.begin();
                return Err(CatalogWriterGone);
            }
            wait.await.map_err(|_| {
                tracing::error!(
                    "durable catalog writer stopped before acknowledging an append; beginning fail-closed shutdown"
                );
                shutdown.begin();
                CatalogWriterGone
            })
        }
    }

    /// Assign the event's `eid` and queue it. The id is minted HERE, once, so a retry-in-place
    /// re-appends the same identity — the fold then applies it exactly once however many copies of
    /// the record a lost response left in the log (ADR-0008).
    fn enqueue(&self, ev: CatalogEvent, done: Option<tokio::sync::oneshot::Sender<()>>) -> bool {
        let n = self.next_eid.fetch_add(1, Ordering::SeqCst);
        let eid = format!("{}-{n:x}", self.eid_nonce);
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        if self.tx.send(CatalogSend { ev, seq: n, eid, done }).is_err() {
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            false
        } else {
            true
        }
    }

    /// Wait until every event enqueued before this call is durable. Used by an idempotent retry
    /// whose first request already changed memory and is still waiting on its own catalog append.
    pub(crate) async fn wait_durable(&self) -> std::result::Result<(), CatalogWriterGone> {
        let target = self.next_eid.load(Ordering::SeqCst).saturating_sub(1);
        loop {
            let notified = self.landed_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.landed_seq.load(Ordering::SeqCst) >= target {
                return Ok(());
            }
            if self.shutdown.is_shutting_down() {
                return Err(CatalogWriterGone);
            }
            tokio::select! {
                _ = notified => {}
                _ = self.shutdown.wait() => {
                    if self.landed_seq.load(Ordering::SeqCst) >= target {
                        return Ok(());
                    }
                    return Err(CatalogWriterGone);
                }
            }
        }
    }

    /// The last change-log position whose `Offset` checkpoint is durable (see [`Self::durable`]).
    pub(crate) fn durable_offset(&self) -> Option<LogPosition> {
        self.durable.lock().unwrap().clone()
    }

    /// Adopt the checkpoint the boot restored from the catalog: it is by definition already durable,
    /// and without it nothing could be deleted until this process wrote its first checkpoint.
    pub(crate) fn seed_durable_offset(&self, pos: LogPosition) {
        let mut g = self.durable.lock().unwrap();
        if g.as_ref().is_none_or(|cur| *cur < pos) {
            *g = Some(pos);
        }
    }

    /// Wait until every event sent so far has been appended (or `timeout` elapses — reported, never
    /// hung on). Returns whether the queue actually drained.
    pub(crate) async fn drain(&self, timeout: std::time::Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        while self.in_flight.load(Ordering::SeqCst) > 0 {
            if std::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        true
    }
}

/// Spawn the single catalog writer: events are appended strictly in send order (senders enqueue
/// while holding the engine-state lock, so the log order matches the state-mutation order).
///
/// **The writer never drops an event.** A transient failure (transport, timeout, 5xx) retries THAT
/// event in place, forever, so the log keeps its order and a restart cannot under-restore; a
/// definite refusal requests a source restart (standalone: [`EXIT_CATALOG_REFUSED`]), because memory and the durable
/// record disagree has no honest way to continue. The retry loop is bounded in practice by the
/// shutdown grace: while it is retrying the writer registers a `catalog writer` shutdown party, so
/// a `SIGTERM` during an outage exits 70 NAMING it rather than looking like a mystery hang.
pub(crate) fn spawn_catalog_writer(ds: DsClient, shutdown: crate::shutdown::ShutdownToken) -> CatalogWriter {
    let (tx, mut rx) = mpsc::unbounded_channel::<CatalogSend>();
    let in_flight = Arc::new(std::sync::atomic::AtomicI64::new(0));
    let counter = in_flight.clone();
    let durable: Arc<std::sync::Mutex<Option<LogPosition>>> = Arc::new(std::sync::Mutex::new(None));
    let landed = durable.clone();
    let landed_seq = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let writer_landed_seq = landed_seq.clone();
    let landed_notify = Arc::new(tokio::sync::Notify::new());
    let writer_landed_notify = landed_notify.clone();
    let writer_shutdown = shutdown.clone();
    shutdown.clone().spawn(async move {
        let mut ensured = false;
        // Latches the "catalog stream create failed" line to once per outage (see
        // `ensure_catalog_logged`); it outlives one event because the retry loop below runs per
        // attempt, and an outage spans many.
        let mut ensure_logged = false;
        while let Some(CatalogSend { ev, seq, eid, done }) = rx.recv().await {
            // Published only on a SUCCESSFUL append: an `Offset` whose write failed is not a
            // position a restart would resume from, and treating it as one would license deleting
            // the segment underneath it.
            let checkpoint = match &ev {
                CatalogEvent::Offset { pos, .. } => Some(pos.clone()),
                _ => None,
            };
            let json = match serde_json::to_value(&ev) {
                // The `eid` rides on the wire beside the event's own fields (the enum is
                // internally tagged, so the value is always a JSON object).
                Ok(serde_json::Value::Object(mut map)) => {
                    map.insert("eid".to_string(), serde_json::Value::String(eid.clone()));
                    serde_json::Value::Object(map)
                }
                Ok(other) => {
                    refuse(
                        &shutdown,
                        &ev,
                        &anyhow::anyhow!("catalog event serialized to a non-object ({other}); it cannot carry an eid"),
                    );
                    return;
                }
                // Not a storage problem and not one waiting fixes: the engine has state it cannot
                // describe. Same verdict as a refusal, for the same reason.
                Err(e) => {
                    refuse(&shutdown, &ev, &anyhow::Error::new(e).context("catalog event could not be serialized"));
                    return;
                }
            };
            // Held only while an outage is in progress, so the ordinary case leaves
            // `wait_for_parties` untouched (a permanently-registered party would never let a
            // shutdown finish).
            let mut party: Option<crate::shutdown::ShutdownParty> = None;
            let mut attempt = 0u32;
            loop {
                // Inside the retry loop, not before it: the PUT that creates the catalog stream is
                // itself a storage round trip, so an engine that started while durable-streams was
                // down has not made it yet — and appending to a stream that does not exist answers
                // 404, which would otherwise read as a refusal and exit the process. Until it has
                // succeeded ONCE, every failure here is transient by construction. (After that, a
                // catalog stream that has *vanished* is a genuine refusal: the durable record has
                // been destroyed under a running engine, and continuing would quietly start a
                // second history.)
                if !ensured {
                    ensured = self::ensure_catalog(&ds, &mut ensure_logged).await;
                }
                match ds.append_json(CATALOG_STREAM, std::slice::from_ref(&json)).await {
                    Ok(()) => break,
                    Err(e) => match if ensured { classify_append(&e) } else { AppendVerdict::Retry } {
                        AppendVerdict::Refused => {
                            refuse(&shutdown, &ev, &e);
                            return;
                        }
                        AppendVerdict::Retry => {
                            attempt += 1;
                            crate::metrics::metrics().catalog_append_retries.fetch_add(1, Ordering::Relaxed);
                            // Once per outage, not once per attempt: an unreachable storage server
                            // must not turn one event into a log flood.
                            if attempt == 1 {
                                party = Some(shutdown.party("catalog writer"));
                                tracing::warn!(
                                    "catalog append failed ({e:#}); durable-streams is unavailable, so \
                                     this event is retried until it lands — every catalog mutation \
                                     behind it waits, and a client-facing create/join/release waits \
                                     with it. No event is dropped."
                                );
                            }
                            let base = catalog_backoff(attempt);
                            tokio::time::sleep(crate::replication::jitter(base, crate::replication::clock_nanos()))
                                .await;
                        }
                    },
                }
            }
            if let Some(pos) = checkpoint {
                let mut g = landed.lock().unwrap();
                if g.as_ref().is_none_or(|cur| *cur < pos) {
                    *g = Some(pos);
                }
            }
            writer_landed_seq.store(seq, Ordering::SeqCst);
            writer_landed_notify.notify_waiters();
            if attempt > 0 {
                tracing::info!("catalog append landed after {attempt} retr(ies); durable-streams is back");
                drop(party.take());
            }
            // Only now: `send_durable`'s contract is "resolved after the append SUCCEEDED".
            if let Some(done) = done {
                let _ = done.send(());
            }
            counter.fetch_sub(1, Ordering::SeqCst);
        }
    });
    CatalogWriter {
        tx,
        shutdown: writer_shutdown,
        in_flight,
        durable,
        eid_nonce: process_nonce(),
        next_eid: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        landed_seq,
        landed_notify,
    }
}

/// A short nonce per Engine instance — the namespace half of both the catalog's event ids and the
/// subscription ids the engine mints (ADR-0008). Not cryptographic and not required to be: it only
/// has to differ between Engine instances writing to the same catalog, and the pair (start time, address of
/// a fresh allocation) does that on every platform the engine runs on.
pub(crate) fn process_nonce() -> String {
    let secs =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0);
    let here = Box::new(0u8);
    let addr = std::ptr::addr_of!(*here) as usize as u64;
    format!("{:x}", secs ^ addr.rotate_left(17))
}

/// The refusal half of [`spawn_catalog_writer`]: name the event and what storage answered, then
/// stop this Engine. A supervised source requests its own restart; standalone mode exits.
fn refuse(shutdown: &crate::shutdown::ShutdownToken, ev: &CatalogEvent, e: &anyhow::Error) {
    tracing::error!(
        "durable catalog REFUSED a {} event: {e:#}. Storage answered, and the answer will not change \
         — the engine is now serving state its durable record does not describe, which no restart of \
         the request and no amount of waiting can reconcile. Requesting restart ({EXIT_CATALOG_REFUSED} in standalone mode): a boot \
         re-folds the catalog, which is the only way memory becomes consistent with storage again.",
        event_kind(ev)
    );
    std::io::Write::flush(&mut std::io::stderr()).ok();
    shutdown.restart_or_exit(EXIT_CATALOG_REFUSED)
}

/// The event's variant name, for the refusal message (`serde` tags it, but only on the way out).
fn event_kind(ev: &CatalogEvent) -> &'static str {
    match ev {
        CatalogEvent::StoreBound(_) => "storeBound",
        CatalogEvent::Created { .. } => "created",
        CatalogEvent::Joined { .. } => "joined",
        CatalogEvent::Left { .. } => "left",
        CatalogEvent::Dormant { .. } => "dormant",
        CatalogEvent::Reactivated { .. } => "reactivated",
        CatalogEvent::BackfillSized { .. } => "backfillSized",
        CatalogEvent::Dropped { .. } => "dropped",
        CatalogEvent::Retired { .. } => "retired",
        CatalogEvent::Offset { .. } => "offset",
        CatalogEvent::ChangesRotated { .. } => "changesRotated",
        CatalogEvent::ChangesSegmentDeleted { .. } => "changesSegmentDeleted",
        CatalogEvent::ConsumerPinned { .. } => "consumerPinned",
        CatalogEvent::ConsumerEvicted { .. } => "consumerEvicted",
        CatalogEvent::SchemaChanged { .. } => "schemaChanged",
        CatalogEvent::SourceDrained(_) => "sourceDrained",
        CatalogEvent::SlotBound(_) => "slotBound",
    }
}

/// A catalog cannot be adopted unless its first event is the one immutable binding expected by
/// this engine. These are typed boot errors because they are operator decisions, not transient
/// storage failures: retrying cannot turn one namespace's history into another's.
#[derive(Debug)]
pub(crate) enum CatalogStoreBindingError {
    EmptyWithoutInitialization,
    Missing,
    DuplicateOrReordered,
    Malformed(String),
    Mismatch { expected: crate::store_identity::StoreBound, observed: Box<crate::store_identity::StoreBound> },
}

/// Linear proof that a ready Durable Streams client owns the expected catalog event zero.
pub struct StoreAdmission {
    pub(crate) ds: DsClient,
    pub(crate) binding: crate::store_identity::StoreBound,
}

async fn read_catalog_events_from(ds: &DsClient) -> Result<Vec<serde_json::Value>> {
    let mut events = Vec::new();
    let mut off = "-1".to_string();
    loop {
        let (page, next, up_to_date) = ds.read_json(CATALOG_STREAM, &off).await?;
        events.extend(page);
        match next {
            Some(next) if !up_to_date && next != off => off = next,
            _ => return Ok(events),
        }
    }
}

impl std::fmt::Display for CatalogStoreBindingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyWithoutInitialization => {
                f.write_str("catalog is empty; explicit namespace initialization is required")
            }
            Self::Missing => f.write_str("catalog event zero is missing StoreBound"),
            Self::DuplicateOrReordered => f.write_str("StoreBound must occur exactly once as catalog event zero"),
            Self::Malformed(detail) => write!(f, "StoreBound is malformed: {detail}"),
            Self::Mismatch { expected, observed } => {
                write!(f, "StoreBound mismatch: expected {expected:?}, observed {observed:?}")
            }
        }
    }
}

impl std::error::Error for CatalogStoreBindingError {}

#[derive(Debug, thiserror::Error)]
#[error("durable catalog writer stopped before acknowledging the append")]
pub(crate) struct CatalogWriterGone;

fn validate_store_binding(events: &[serde_json::Value], expected: &crate::store_identity::StoreBound) -> Result<()> {
    let Some(first) = events.first() else {
        return Err(anyhow::Error::new(CatalogStoreBindingError::Missing));
    };
    let observed = match serde_json::from_value::<CatalogEvent>(first.clone()) {
        Ok(CatalogEvent::StoreBound(bound)) => bound,
        Ok(_) => return Err(anyhow::Error::new(CatalogStoreBindingError::Missing)),
        Err(error) => {
            return Err(anyhow::Error::new(CatalogStoreBindingError::Malformed(error.to_string())));
        }
    };
    if &observed != expected {
        return Err(anyhow::Error::new(CatalogStoreBindingError::Mismatch {
            expected: expected.clone(),
            observed: Box::new(observed),
        }));
    }
    for event in events.iter().skip(1) {
        if event.get("t").and_then(serde_json::Value::as_str) == Some("storeBound") {
            return Err(anyhow::Error::new(CatalogStoreBindingError::DuplicateOrReordered));
        }
    }
    Ok(())
}

/// The durable catalog holds a record written **before** ADR-0002 (a bare `rec.table`).
///
/// A typed error because the boot path treats it differently from every other restore failure: an
/// ordinary failure is logged and the engine continues with an empty registry (the clients recreate
/// their shapes), but this one **refuses to boot**. Half-restoring a pre-qualification catalog is
/// the one outcome worse than not booting: the record's `sig` still carries the bare spelling, so an
/// identical post-cutover create would not share with it and the engine would quietly maintain two
/// streams for one table, forever. Recovery is a deliberate human act (reset the storage), not
/// something the engine gets to paper over.
#[derive(Debug)]
pub struct CatalogPredatesQualification {
    detail: String,
}

impl std::fmt::Display for CatalogPredatesQualification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "durable catalog predates ADR-0002 ({}); reset the durable-streams data directory", self.detail)
    }
}

impl std::error::Error for CatalogPredatesQualification {}

/// The durable catalog holds a change-log position written **before** ADR-0006 (a bare offset
/// string, from when the log was one un-segmented `changes` stream).
///
/// Fatal for the same reason [`CatalogPredatesQualification`] is: the value cannot be repaired, and
/// guessing would be worse than not booting. A bare offset is a byte position in a stream that no
/// longer exists under that name; adopting it as "segment 0" would resume the sequencer — or a
/// dormant shape — at an unrelated point in a different segment's byte space, silently replaying or
/// silently skipping an arbitrary span of changes. Greenfield: no such catalog can exist except
/// from a pre-cutover build, and recovery is a deliberate human act (reset the storage).
#[derive(Debug)]
pub struct CatalogPredatesSegmentation {
    detail: String,
}

impl std::fmt::Display for CatalogPredatesSegmentation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "durable catalog predates ADR-0006 ({}); reset the durable-streams data directory", self.detail)
    }
}

impl std::error::Error for CatalogPredatesSegmentation {}

/// A consumer pin names a physical change-log segment. Restoring a pin for a segment that the
/// folded retention history no longer retains would publish a reader that can only receive 410,
/// and—worse—would make the deletion floor appear protected by a non-existent stream.
#[derive(Debug)]
pub struct CatalogConsumerSegmentMissing {
    id: String,
    position: LogPosition,
}

impl std::fmt::Display for CatalogConsumerSegmentMissing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "durable catalog restores consumer {} at {}, but that change-log segment is not retained",
            self.id, self.position
        )
    }
}

impl std::error::Error for CatalogConsumerSegmentMissing {}

/// The durable catalog holds an event written **before** ADR-0008: no `eid`, or a shape-lifecycle
/// event with no `subscription`.
///
/// Fatal for the same reason the two above are, and it is the same class of thing: the value cannot
/// be reconstructed and guessing would be worse than not booting. Without an `eid` the fold cannot
/// tell a retried append from a second event, so a lost response in the old log has already been
/// applied twice and there is no way to know which; without a `subscription` the live SET cannot be
/// rebuilt, and inventing ids would restore a refcount that no client can ever release (nothing the
/// caller holds names it). Greenfield: no such catalog can exist except from a pre-cutover build,
/// and recovery is a deliberate human act (reset the storage).
#[derive(Debug)]
pub struct CatalogPredatesSubscriptions {
    detail: String,
}

impl std::fmt::Display for CatalogPredatesSubscriptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "durable catalog predates ADR-0008 ({}); reset the durable-streams data directory", self.detail)
    }
}

impl std::error::Error for CatalogPredatesSubscriptions {}

/// Is this raw catalog event missing what ADR-0008 requires of it? `Some(detail)` names it.
///
/// Positive-checking the raw JSON rather than trusting the deserializer, exactly like
/// [`predates_segmentation`]: an event the strict deserializer cannot read is otherwise silently
/// skipped by the fold, which for a `Created` would mean a shape that simply disappears at boot.
fn predates_subscriptions(ev: &serde_json::Value) -> Option<String> {
    let kind = ev.get("t").and_then(serde_json::Value::as_str).unwrap_or("<untagged>");
    if ev.get("eid").and_then(serde_json::Value::as_str).is_none() {
        return Some(format!("a {kind} event with no eid"));
    }
    if matches!(kind, "created" | "joined" | "left")
        && ev.get("subscription").and_then(serde_json::Value::as_str).is_none()
    {
        return Some(format!("a {kind} event with no subscription"));
    }
    if matches!(kind, "created" | "joined") && ev.get("at").and_then(serde_json::Value::as_u64).is_none() {
        return Some(format!("a {kind} event with no lease timestamp"));
    }
    None
}

/// The numeric half of a shape id (`s7` -> `7`). `None` for anything that is not one — the ids the
/// engine mints always are, and a foreign spelling must not silently reset the id counter.
fn shape_id_num(id: &str) -> Option<u64> {
    id.strip_prefix('s')?.parse().ok()
}

/// Is this raw catalog event a pre-ADR-0006 change-log position? `Some(detail)` names it.
///
/// Deliberately positive-checking the OLD spelling rather than trusting the strict deserializer to
/// fail: an unparseable event is otherwise silently skipped by the fold, which for an `Offset` would
/// silently restart the sequencer from the beginning of the log.
fn predates_segmentation(ev: &serde_json::Value) -> Option<String> {
    match ev.get("t").and_then(serde_json::Value::as_str) {
        Some("offset") if ev.get("offset").is_some_and(serde_json::Value::is_string) => {
            Some(format!("bare change-log offset {}", ev["offset"]))
        }
        Some("dormant") if ev.get("resume_offset").is_some_and(serde_json::Value::is_string) => Some(format!(
            "bare dormant resume offset {} for shape {}",
            ev["resume_offset"],
            ev.get("id").and_then(serde_json::Value::as_str).unwrap_or("<unknown>")
        )),
        _ => None,
    }
}

/// Did the shape's table move while the engine was down? `Some(description)` if so.
///
/// Only Postgres-mode tables can answer: a library-mode table has no fingerprint on either side and
/// is restored as before. A record with no fingerprint of its own, over a table that HAS one, cannot
/// be vouched for and is retired — greenfield, so that is a catalog written before the field
/// existed, not a format to keep compatibility with.
fn schema_moved_while_down(rec: &ShapeRecord, compiled: &HashMap<TableRef, TableSchema>) -> Option<String> {
    let now = compiled.get(&rec.table)?.fingerprint.as_ref()?;
    match &rec.fingerprint {
        Some(then) if then.still_serves(now) => None,
        Some(then) => Some(crate::schema::describe_drift(then, now).join("; ")),
        None => Some("the record predates schema fingerprinting".to_string()),
    }
}

/// Idempotently create the catalog stream, logging a failure only the FIRST time through an outage
/// — the writer retries this before every append attempt while the stream has never been created,
/// and one error line per attempt for as long as storage is down is a flood, not information.
/// `logged` is the caller's per-outage latch; it is cleared again on success.
async fn ensure_catalog(ds: &DsClient, logged: &mut bool) -> bool {
    match ds.ensure_stream(CATALOG_STREAM).await {
        Ok(()) => {
            *logged = false;
            true
        }
        Err(e) => {
            if !*logged {
                tracing::error!("catalog stream create failed: {e:#}");
                *logged = true;
            }
            false
        }
    }
}

/// One shape as the fold reconstructed it: (record, sharing signature, **live subscriptions**,
/// dormant resume state). The last `Dormant`/`Reactivated` event wins.
///
/// The subscriptions are a SET, not a count (ADR-0008): `subscription id -> the wall-clock second
/// its lease was last renewed at`. A repeated `Joined` for an id already in the set moves the
/// timestamp and nothing else; a `Left` for an id that is not in it does nothing at all. That is
/// what makes a duplicated record — a retry after a lost response, an over-eager client — harmless
/// where counter arithmetic was not.
type Restored = (
    ShapeRecord,
    Option<String>,
    std::collections::BTreeMap<String, u64>,
    Option<(LogPosition, crate::pg::SnapshotGate)>,
);

/// The durable catalog, folded — everything a boot needs before it decides what to *do* with it.
///
/// Reading and deciding are separate because the epoch check has to happen between them (ADR-0004):
/// the binding to verify against is in the log, and no shape may be resumed before the verdict is
/// in.
pub(crate) struct CatalogFold {
    recs: HashMap<String, Restored>,
    /// Shapes whose `Dropped` intent is in the log with no `Retired` completion after it: shape id →
    /// stream path. These are streams the engine promised to remove and did not — the boot hands
    /// them to the retirement queue (see [`Engine::apply_catalog`]).
    ///
    /// Bounded by the retirements actually outstanding, not by the log's length: the entry is
    /// inserted on `Dropped` and removed again on the matching `Retired`.
    pending_retire: HashMap<String, String>,
    /// The highest numeric shape id the log has EVER minted — from every `Created`, including the
    /// ones later dropped. `None` = the log created no shape.
    ///
    /// This, and not the surviving records, is the restart's id high-water mark. A dropped shape's
    /// id stays spoken for as long as anything of it survives: its `shape/sN` stream lives until the
    /// retirement lands, so re-minting `sN` would hand a brand-new shape the dead one's stream —
    /// `ensure_stream` is idempotent, so the PUT succeeds, the backfill appends to a stream holding
    /// pre-`TRUNCATE` rows, and the pending retirement then closes and deletes the LIVE shape's
    /// stream (and records `Retired` against a registered shape, whose appends are `Gone` from then
    /// on). Ids are never reused, full stop.
    max_shape_id: Option<u64>,
    /// The sequencer's change-log replay start (the last `Offset` checkpoint).
    start_pos: LogPosition,
    /// The `(lsn, seq)` de-duplication highwater recorded with that checkpoint (ADR-0003).
    start_highwater: Option<(u64, u64)>,
    /// The change log's current segment: the last `ChangesRotated`, else 0 (ADR-0006). A lower
    /// bound — a process can die between closing a segment and recording the rotation — so the
    /// boot walks forward from here (`changelog::resolve_current`).
    pub(crate) current_segment: u32,
    /// Every segment's start time (unix seconds), for the retain window that governs deletion.
    pub(crate) segment_starts: std::collections::BTreeMap<u32, u64>,
    /// Durable external consumer positions, folded last-write-wins by caller chosen id.
    pub(crate) consumers: std::collections::BTreeMap<String, LogPosition>,
    /// The last `SlotBound`: the epoch these shapes belong to. `None` = nothing ever claimed one,
    /// which is a genuine first boot.
    pub(crate) binding: Option<crate::engine::epoch::SlotBinding>,
    /// Durable source-fence receipts restored independently of shape state. A repeated event for
    /// the same commit id is idempotent; a controller asks for the exact id it wrote.
    source_receipts: HashMap<String, SourceDrainReceipt>,
    last_source_receipt: Option<SourceDrainReceipt>,
    /// Every `eid` this fold has already applied (ADR-0008). The writer retries a failed append in
    /// place, so a response lost after the append committed leaves the SAME event in the log twice;
    /// applying the second copy is exactly the double-count this set exists to prevent.
    ///
    /// Bounded by the catalog's length, like the fold itself — it lives only for the duration of
    /// one boot's read and is dropped with the fold.
    seen: HashSet<String>,
}

impl Default for CatalogFold {
    fn default() -> Self {
        CatalogFold {
            recs: HashMap::new(),
            pending_retire: HashMap::new(),
            max_shape_id: None,
            start_pos: LogPosition::start(),
            start_highwater: None,
            binding: None,
            source_receipts: HashMap::new(),
            last_source_receipt: None,
            current_segment: 0,
            segment_starts: std::collections::BTreeMap::new(),
            consumers: std::collections::BTreeMap::new(),
            seen: HashSet::new(),
        }
    }
}

impl CatalogFold {
    /// Fold one event in, **once**: an `eid` this fold has already applied is ignored (ADR-0008).
    ///
    /// The de-duplication belongs here rather than in [`Self::apply`] because it is a property of
    /// the LOG (the writer may have appended the same event twice), not of the event's meaning.
    fn apply_once(&mut self, eid: &str, ev: CatalogEvent) {
        if !self.seen.insert(eid.to_string()) {
            tracing::debug!("catalog restore: ignoring a repeated append of event {eid}");
            return;
        }
        self.apply(ev);
    }

    /// Fold one event in. Pure (no engine, no IO), so the log's semantics — last-writer-wins for the
    /// epoch and the offset, remove-on-drop for the shapes — are unit-testable.
    fn apply(&mut self, ev: CatalogEvent) {
        match ev {
            CatalogEvent::StoreBound(_) => {}
            CatalogEvent::Created { rec, sig, subscription, at } => {
                // Every create moves the id high-water mark, and nothing ever moves it back (see
                // `max_shape_id`). `max`, not "the last one wins": the engine mints ids
                // monotonically from one counter, so the log is monotonic anyway, and taking the
                // maximum means a log that somehow is not cannot lower the mark.
                if let Some(num) = shape_id_num(&rec.id) {
                    self.max_shape_id = Some(self.max_shape_id.map_or(num, |cur| cur.max(num)));
                }
                self.recs.insert(rec.id.clone(), (rec, sig, [(subscription, at)].into_iter().collect(), None));
            }
            // A join and a lease RENEWAL are the same record: insert wins for a new id, and only
            // moves the lease for one already held (ADR-0008). The restored `at` is what stops a
            // restart from handing every subscription a fresh idle window.
            CatalogEvent::Joined { id, subscription, at } => {
                if let Some(e) = self.recs.get_mut(&id) {
                    let lease = e.2.entry(subscription).or_insert(at);
                    *lease = (*lease).max(at);
                }
            }
            // Idempotent by construction: releasing an id the set does not hold is a no-op, which
            // is what makes a client's retried DELETE safe.
            CatalogEvent::Left { id, subscription, lapsed: _ } => {
                if let Some(e) = self.recs.get_mut(&id) {
                    e.2.remove(&subscription);
                }
            }
            CatalogEvent::Dormant { id, resume, gate } => {
                if let Some(e) = self.recs.get_mut(&id) {
                    e.3 = Some((resume, gate));
                }
            }
            CatalogEvent::Reactivated { id } => {
                if let Some(e) = self.recs.get_mut(&id) {
                    e.3 = None;
                }
            }
            CatalogEvent::BackfillSized { id, rows, bytes } => {
                if let Some((rec, ..)) = self.recs.get_mut(&id) {
                    rec.backfill_rows = Some(rows);
                    rec.backfill_bytes = Some(bytes);
                }
            }
            // The record goes; the obligation to retire its stream stays until a `Retired` says it
            // was honoured. The path is taken from the record itself (the event carries only the
            // id): a `Dropped` for a record this fold never saw is a duplicate, and there is
            // nothing left to retire.
            CatalogEvent::Dropped { id } => {
                if let Some((rec, ..)) = self.recs.remove(&id) {
                    self.pending_retire.insert(id, rec.stream_path);
                }
            }
            CatalogEvent::Retired { id } => {
                self.pending_retire.remove(&id);
            }
            CatalogEvent::Offset { pos, highwater } => {
                self.start_pos = pos;
                self.start_highwater = highwater;
            }
            // The LAST rotation is the current segment; every one of them is kept, because the
            // retain window needs to know when each segment began (see the variant).
            CatalogEvent::ChangesRotated { segment, at } => {
                self.current_segment = self.current_segment.max(segment);
                self.segment_starts.insert(segment, at);
            }
            // A deleted segment leaves the set but never moves `current_segment`: the current one
            // is never deleted, so the last rotation still names it.
            CatalogEvent::ChangesSegmentDeleted { segment } => {
                self.segment_starts.remove(&segment);
            }
            CatalogEvent::ConsumerPinned { id, position } => {
                self.consumers.insert(id, position);
            }
            CatalogEvent::ConsumerEvicted { id } => {
                self.consumers.remove(&id);
            }
            // Audit only (see the variant): the shapes it explains are already `Dropped`,
            // and the boot's own introspection is the authority on the schema.
            CatalogEvent::SchemaChanged { .. } => {}
            CatalogEvent::SourceDrained(receipt) => {
                if !self.source_receipts.contains_key(&receipt.source_commit_id) {
                    self.source_receipts.insert(receipt.source_commit_id.clone(), receipt.clone());
                    self.last_source_receipt = Some(receipt);
                }
            }
            // The LAST binding is the epoch in force. A reset appends its new one after the
            // `Dropped` records of the epoch it ended, so the two always agree.
            CatalogEvent::SlotBound(binding) => self.binding = Some(binding),
        }
    }

    /// The sequencer's restored change-log position — the segment the boot must vouch for before
    /// anything resumes (see `Engine::init_change_log`).
    pub(crate) fn start_pos(&self) -> LogPosition {
        self.start_pos.clone()
    }

    /// Every `Dropped` with no `Retired` after it: `(shape id, stream path)`, sorted by id so the
    /// boot's enqueue order is deterministic. This is the orphan-`shape/*` GC, bounded by the
    /// catalog rather than by a storage listing (durable-streams exposes no list API).
    pub(crate) fn pending_retirements(&self) -> Vec<(String, String)> {
        let mut v: Vec<(String, String)> =
            self.pending_retire.iter().map(|(id, path)| (id.clone(), path.clone())).collect();
        v.sort();
        v
    }

    /// Nothing was ever written (or everything was dropped and nothing checkpointed).
    ///
    /// Deliberately does NOT consider `pending_retire`: this asks "is there anything to INSTALL",
    /// and the boot enqueues outstanding retirements before it consults this at all.
    fn is_empty(&self) -> bool {
        self.recs.is_empty() && self.start_pos == LogPosition::start()
    }
}

/// How much of a folded catalog a boot actually installs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RestoreMode {
    /// The ordinary boot: restore the records and re-register them with the sequencer.
    Resume,
    /// The epoch broke (ADR-0004). Restore the shape RECORDS and nothing else — no resume, no
    /// sequencer registration, no retention lifecycle, and above all no teardown of its own.
    ///
    /// The records are what a reset needs in order to retire the shapes *properly* (close the
    /// stream, then delete it, and write `Dropped`), whether that reset happens immediately (the
    /// auto policy) or when an operator posts `/epoch/reset`. Nothing is resumed, so no old-epoch
    /// shape is ever maintained for an instant; nothing is destroyed either, so a refusing engine
    /// has not thrown anything away before the human said so.
    Park,
}

impl Engine {
    /// Admit a ready store namespace before an Engine can exist. The returned proof carries both
    /// the admitted DS client and the immutable event-zero binding.
    pub async fn admit_store(
        ds: DsClient,
        expected: crate::store_identity::StoreBound,
        initialize_namespace: bool,
    ) -> Result<StoreAdmission> {
        let events = read_catalog_events_from(&ds).await?;
        if events.is_empty() {
            if !initialize_namespace {
                return Err(anyhow::Error::new(CatalogStoreBindingError::EmptyWithoutInitialization));
            }
            ds.ensure_stream(CATALOG_STREAM).await?;
            let mut event = serde_json::to_value(CatalogEvent::StoreBound(expected.clone()))?;
            event
                .as_object_mut()
                .expect("catalog event serializes as object")
                .insert("eid".to_string(), serde_json::Value::String("store-bound-init".to_string()));
            ds.append_json(CATALOG_STREAM, std::slice::from_ref(&event)).await?;
            validate_store_binding(&read_catalog_events_from(&ds).await?, &expected)?;
        } else {
            validate_store_binding(&events, &expected)?;
        }
        Ok(StoreAdmission { ds, binding: expected })
    }

    /// Establish the coupled-pilot catalog identity before any Postgres setup. A blank namespace is
    /// not adoption evidence: only the explicit initialization flag licenses its first durable
    /// `StoreBound` append.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn prepare_store_binding(
        &self,
        expected: crate::store_identity::StoreBound,
        initialize_namespace: bool,
    ) -> Result<()> {
        let events = self.read_catalog_events().await?;
        if events.is_empty() {
            if !initialize_namespace {
                return Err(anyhow::Error::new(CatalogStoreBindingError::EmptyWithoutInitialization));
            }
            self.catalog_tx.send_durable(CatalogEvent::StoreBound(expected.clone())).await?;
            let written = self.read_catalog_events().await?;
            validate_store_binding(&written, &expected)?;
        } else {
            validate_store_binding(&events, &expected)?;
        }
        self.store_bound
            .set(expected)
            .map_err(|_| anyhow::anyhow!("store binding was already prepared for this engine"))?;
        Ok(())
    }

    async fn read_catalog_events(&self) -> Result<Vec<serde_json::Value>> {
        read_catalog_events_from(&self.ds).await
    }

    /// Vouch for every stream that Resume would install before changing the registry, routing or
    /// sequencer. Definitive missing/closed answers are returned for retirement; a transport error
    /// still aborts the restore (the boot boundary classifies it as retryable and backs off).
    /// Schema-drift and subquery records are intentionally dropped by the existing
    /// restore path, so their streams are retired below rather than treated as resumable state.
    async fn preflight_catalog_streams(
        &self,
        recs: &HashMap<String, Restored>,
        compiled: &HashMap<TableRef, TableSchema>,
    ) -> Result<Vec<(String, &'static str)>> {
        let mut retire = Vec::new();
        for (id, (rec, _, _, _)) in recs {
            if rec.is_subquery || schema_moved_while_down(rec, compiled).is_some() {
                continue;
            }
            let head = self
                .ds
                .head(&rec.stream_path)
                .await
                .with_context(|| format!("catalog restore: checking shape {id} stream {}", rec.stream_path))?;
            match head {
                None => retire.push((id.clone(), "missing")),
                Some(head) if head.closed => retire.push((id.clone(), "closed")),
                Some(_) => {}
            }
        }
        retire.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(retire)
    }

    /// Read the durable shape catalog and fold it. No engine state is touched — see
    /// [`Self::apply_catalog`] for that half.
    pub(crate) async fn fold_catalog(&self) -> Result<CatalogFold> {
        let mut fold = CatalogFold::default();
        let events = self.read_catalog_events().await?;
        if let Some(expected) = self.store_bound.get() {
            validate_store_binding(&events, expected)?;
        }
        for ev in events {
            // The catalog is engine-written, so `rec.table` is ALWAYS the canonical
            // `schema.name` (`ShapeRecord`'s strict deserializer enforces it). A bare one can
            // only be a catalog written before ADR-0002: refuse the boot naming the record,
            // rather than let the strict deserializer turn it into a silently skipped event.
            if let Some(raw) = ev.pointer("/rec/table").and_then(serde_json::Value::as_str)
                && crate::table_ref::TableRef::parse(raw).is_ok_and(|t| t.as_str() != raw)
            {
                let id = ev.pointer("/rec/id").and_then(serde_json::Value::as_str).unwrap_or("<unknown>");
                return Err(anyhow::Error::new(CatalogPredatesQualification {
                    detail: format!("bare table name '{raw}' in shape {id}"),
                }));
            }
            // Same stance for a pre-segmentation change-log position (ADR-0006): refuse the
            // boot naming it, rather than let the strict deserializer turn it into a silently
            // skipped event.
            if let Some(detail) = predates_segmentation(&ev) {
                return Err(anyhow::Error::new(CatalogPredatesSegmentation { detail }));
            }
            // ...and for an event written before ADR-0008 (no `eid`, no `subscription`): the
            // fold cannot de-duplicate or rebuild the live set, and both failures are silent.
            if let Some(detail) = predates_subscriptions(&ev) {
                return Err(anyhow::Error::new(CatalogPredatesSubscriptions { detail }));
            }
            let eid = ev["eid"].as_str().unwrap_or_default().to_string();
            let Ok(ev) = serde_json::from_value::<CatalogEvent>(ev) else { continue };
            fold.apply_once(&eid, ev);
        }
        Ok(fold)
    }

    /// Install a folded catalog: re-register every restorable shape with the (not yet spawned)
    /// sequencer — see [`CATALOG_STREAM`] for the restore semantics per shape kind — or, in
    /// [`RestoreMode::Park`], record them and stop there.
    pub(crate) async fn apply_catalog(
        &self,
        mut fold: CatalogFold,
        compiled: &HashMap<TableRef, TableSchema>,
        mode: RestoreMode,
    ) -> Result<()> {
        // Pins are independent of shapes: validate and install them before the empty-catalog fast
        // path and before the retention sweep can run after restore. `init_change_log` has already
        // rebuilt the retained segment set, but it validates only the sequencer checkpoint; every
        // external reader's independently durable position needs the same fail-closed check.
        let retained_segments = self.changes.state().segments();
        for (id, position) in &fold.consumers {
            if !retained_segments.contains_key(&position.segment) {
                return Err(anyhow::Error::new(CatalogConsumerSegmentMissing {
                    id: id.clone(),
                    position: position.clone(),
                }));
            }
        }
        *self.consumers.lock().unwrap() = fold.consumers.clone();
        // A re-fold can race a receipt appended after its read snapshot. Merge the durable snapshot
        // instead of replacing live receipts, and preserve the live `last` when the fold did not
        // contain it; otherwise the fold's last event is the newer durable order authority.
        let live_last = self.last_source_receipt.lock().unwrap().clone();
        let preserve_live_last =
            live_last.as_ref().is_some_and(|receipt| !fold.source_receipts.contains_key(&receipt.source_commit_id));
        {
            let mut progress = self.source_receipt_progress.lock().unwrap();
            for source_commit_id in fold.source_receipts.keys() {
                progress.record_restored(source_commit_id);
            }
        }
        self.source_receipts.lock().unwrap().extend(fold.source_receipts.clone());
        if !preserve_live_last {
            *self.last_source_receipt.lock().unwrap() = fold.last_source_receipt.clone().or(live_last);
        }
        // Resume must prove every retained stream is still appendable before any record, share or
        // sequencer state is installed. Park intentionally skips this: it records old-epoch shapes
        // for the reset's close-then-delete path and must preserve that existing semantics.
        let boot_retire = if mode == RestoreMode::Resume {
            self.preflight_catalog_streams(&fold.recs, compiled).await?
        } else {
            Vec::new()
        };
        // BEFORE anything else, and in both modes: a `Dropped` with no `Retired` is a shape stream a
        // previous process promised to remove and did not (its retirement was refused by storage,
        // or the process died between the two). The engine has already forgotten the shape, so
        // nothing else will ever notice — this is the only place it can be picked up. The queue
        // retries each one to completion and writes the `Retired` that closes it out.
        for (id, path) in fold.pending_retirements() {
            tracing::info!("restore: shape {id} was dropped but its stream {path} was never retired; queued");
            self.retirements.enqueue(&path, Some(&id));
        }
        // Also before anything else, and also in both modes: the id counter resumes past EVERY id
        // the log ever minted, not past the ones that survived (see `CatalogFold::max_shape_id`).
        // A catalog of nothing but creates and drops folds to `is_empty()`, so this cannot wait for
        // the branches below — that is exactly the case where re-minting would collide with a
        // `shape/*` stream whose retirement is still pending.
        if let Some(max) = fold.max_shape_id {
            let mut st = self.state.lock().await;
            st.next_shape_id = st.next_shape_id.max(max + 1);
        }
        // A definitive missing/closed HEAD is a durable shape record whose storage disappeared
        // while the engine was down. Remove it from the install set, enqueue the `Dropped` intent
        // (ordered ahead of the retirement's `Retired` on the same writer, not awaited — the same
        // stance as the drift and subquery drops below; retry-safety comes from the next boot's
        // HEAD, which re-retires anything whose pair never landed), and send its stream through the
        // normal close-then-delete retirement path.
        let mut dead_streams: Vec<(String, String)> = Vec::new();
        for (id, reason) in boot_retire {
            if let Some((rec, _, _, _)) = fold.recs.remove(&id) {
                tracing::warn!(
                    shape_id = %id,
                    stream_path = %rec.stream_path,
                    reason,
                    "catalog restore: retiring shape whose stream storage is gone"
                );
                self.catalog_tx.send(CatalogEvent::Dropped { id: id.clone() });
                crate::metrics::metrics().catalog_restore_retired.fetch_add(1, Ordering::Relaxed);
                crate::statsd::catalog_restore_retired(reason);
                dead_streams.push((id, rec.stream_path));
            }
        }
        if fold.is_empty() {
            for (id, path) in dead_streams {
                self.retire_shape_stream(&id, &path).await;
            }
            self.release_restore_reads().await?;
            return Ok(());
        }
        let CatalogFold { recs, start_pos, start_highwater, .. } = fold;
        let restored_ids: Vec<String> = recs.keys().cloned().collect();
        tracing::info!("catalog restore: {} shape(s), change-log replay from {start_pos}", recs.len());
        // The restored checkpoint IS durable (it was read back out of the log), so it is the
        // segment-deletion floor from boot rather than from this process's first checkpoint.
        self.catalog_tx.seed_durable_offset(start_pos.clone());
        *self.seq_start.lock().unwrap() = start_pos;
        // The de-duplication highwater is restored with the position (ADR-0003), so a prefix of a
        // chunked commit that was applied and checkpointed before a crash is not applied twice when
        // Postgres re-delivers the transaction.
        *self.seq_highwater.lock().unwrap() = start_highwater;

        if mode == RestoreMode::Park {
            // The epoch broke: hold the records, touch nothing else (see `RestoreMode::Park`).
            let mut st = self.state.lock().await;
            for (id, (rec, _, _, _)) in recs {
                st.shapes.insert(id, rec); // `next_shape_id` was moved past every minted id above
                // No share entries and therefore no subscriptions: a parked epoch's shapes exist
                // only to be retired, and nothing may join or renew one.
            }
            tracing::warn!(
                "catalog restore: {} shape(s) parked over a broken epoch — none is resumed or \
                 maintained; they exist only to be retired by the reset",
                st.shapes.len()
            );
            drop(st);
            self.release_restore_reads().await?;
            return Ok(());
        }

        // Resume registration must complete before the sequencer reads or checkpoints any history.
        // The reader is spawned paused on this path and released only after every shape succeeds.
        self.restore_reads_paused.store(true, std::sync::atomic::Ordering::Release);
        let existing_seq = self.state.lock().await.sequencer.as_ref().map(|s| s.cmd_tx.clone());
        if let Some(cmd_tx) = existing_seq {
            let (done_tx, done_rx) = tokio::sync::oneshot::channel();
            let pause = match cmd_tx.send(SequencerCmd::PauseReads { done: done_tx }) {
                Ok(()) => done_rx.await.map_err(|_| anyhow::anyhow!("sequencer pause acknowledgement dropped")),
                Err(_) => Err(anyhow::anyhow!("sequencer pause command receiver closed")),
            };
            if let Err(e) = pause {
                self.rollback_catalog_restore(&restored_ids, &cmd_tx).await;
                return Err(e.context("catalog restore: sequencer pause handshake failed"));
            }
        }

        // 2. Restore records + shares; subquery shapes are dropped (see CATALOG_STREAM docs).
        let mut resume: Vec<ShapeRecord> = Vec::new();
        {
            let mut st = self.state.lock().await;
            for (id, (rec, sig, subs, dormant)) in recs {
                // (`next_shape_id` was moved past every id the log ever minted, above.)
                // DDL while the engine was DOWN is seen by nothing on the live path: no `Relation`
                // message, no reconciler tick. The record's own fingerprint is the only witness —
                // if it no longer matches what boot introspected, the retained stream holds rows
                // shaped by the old schema and can never be brought up to date (ADR-0005).
                if let Some(what) = schema_moved_while_down(&rec, compiled) {
                    tracing::warn!(
                        "restore: retiring shape {id} on {} — its schema changed while the engine was \
                         down ({what}); subscribers observe the closed stream and recreate",
                        rec.table
                    );
                    self.catalog_tx.send(CatalogEvent::Dropped { id: id.clone() });
                    dead_streams.push((id.clone(), rec.stream_path.clone()));
                    continue;
                }
                if rec.is_subquery {
                    // Subquery shapes are registry-served and their inner-node contributor
                    // state is not persisted: a fresh-seeded node cannot detect flips that
                    // happened during downtime (stale move-outs would persist forever), so
                    // they are dropped loudly and clients recreate them.
                    tracing::warn!(
                        "restore: dropping subquery shape {id} (inner-node state is not persisted); subscribers observe the deleted stream and recreate"
                    );
                    self.catalog_tx.send(CatalogEvent::Dropped { id: id.clone() });
                    dead_streams.push((id.clone(), rec.stream_path.clone()));
                    continue;
                }
                st.shapes.insert(id.clone(), rec.clone());
                if let Some(sig) = sig {
                    // Restored feeds are live immediately (their streams already hold data).
                    let (ready_tx, ready_rx) = tokio::sync::watch::channel(ShareOutcome::Ready);
                    drop(ready_tx); // receivers keep observing `Ready`
                    st.feed_by_sig.insert(sig.clone(), id.clone());
                    st.feed_shares.insert(id.clone(), FeedShare { sig, subs: Default::default(), ready: ready_rx });
                    // The live set is restored WITH its lease ages (ADR-0008): a subscription that
                    // was already past the idle window when the process died is past it here too,
                    // so a restart neither forgets a subscription nor grants every one of them a
                    // fresh window to pin the shape with.
                    for (sub, at) in subs {
                        st.subscribe(&id, sub, at);
                    }
                }
                match dormant {
                    // A dormant shape restores AS dormant: record + stream retained, no routing,
                    // no replay at boot — the first touch reactivates it from its own resume
                    // offset. (Dormancy age restarts at boot; the TTL clock is conservative.)
                    Some((resume_at, gate)) => {
                        self.lives.lock().unwrap().insert(
                            id.clone(),
                            ShapeLife {
                                last_read: std::time::Instant::now(),
                                state: LifeState::Dormant { since: std::time::Instant::now(), resume: resume_at, gate },
                            },
                        );
                    }
                    None => {
                        self.lives.lock().unwrap().insert(id.clone(), ShapeLife::active());
                        resume.push(rec);
                    }
                }
            }
            self.ensure_sequencer(&mut st);
        }
        // Restored dormant shapes still need the TTL/eviction layers running.
        self.ensure_retention_sweeper();
        // Retirement: clients may still be tailing these streams from before the restart, so close
        // before deleting — their long-poll is released at once with `stream-closed`. A storage
        // failure here is not "lost": the `Dropped` above is the durable intent, and the retirement
        // is finished in the background (or by the next boot).
        for (id, path) in dead_streams {
            self.retire_shape_stream(&id, &path).await;
        }

        // 3. Re-register with the sequencer. Plain/routed shapes resume without a backfill and
        // with a passthrough gate (`changes_only = true` path): everything after the restored
        // offset replays, and re-emission across the crash window is idempotent. Aggregates
        // re-seed their fold from a fresh snapshot (fresh gate skips the replayed history).
        let cmd_tx = {
            let st = self.state.lock().await;
            st.sequencer.as_ref().expect("sequencer spawned above").cmd_tx.clone()
        };
        resume.sort_by(|a, b| a.id.cmp(&b.id));
        for rec in resume {
            let outcome = self.resume_shape(&cmd_tx, &rec, compiled).await;
            if let Err(e) = outcome {
                tracing::error!("restore: shape {} failed to resume ({e:#}); rolling back the restore", rec.id);
                self.rollback_catalog_restore(&restored_ids, &cmd_tx).await;
                return Err(e.context(format!("catalog restore failed for shape {}", rec.id)));
            }
        }
        if let Err(e) = self.release_restore_reads().await {
            self.rollback_catalog_restore(&restored_ids, &cmd_tx).await;
            return Err(e.context("catalog restore: sequencer resume handshake failed"));
        }
        Ok(())
    }

    /// Release the replay gate only after the sequencer acknowledges the command. A dropped
    /// receiver/ack must remain a setup error; clearing the shared gate would otherwise let a
    /// replacement task race ahead of an incomplete catalog restore.
    async fn release_restore_reads(&self) -> Result<()> {
        let existing_seq = self.state.lock().await.sequencer.as_ref().map(|s| s.cmd_tx.clone());
        if let Some(cmd_tx) = existing_seq {
            let (done_tx, done_rx) = tokio::sync::oneshot::channel();
            cmd_tx
                .send(SequencerCmd::ResumeReads { done: done_tx })
                .map_err(|_| anyhow::anyhow!("catalog restore: sequencer resume command receiver closed"))?;
            done_rx.await.map_err(|_| anyhow::anyhow!("catalog restore: sequencer resume acknowledgement dropped"))?;
        }
        self.restore_reads_paused.store(false, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// Undo every in-memory and sequencer registration made by one Resume attempt. The remove
    /// commands are fenced by a barrier so a failed boot cannot leave an earlier shape routed while
    /// the caller retries the same durable catalog.
    async fn rollback_catalog_restore(&self, ids: &[String], cmd_tx: &mpsc::UnboundedSender<SequencerCmd>) {
        let records: Vec<(String, TableRef)> = {
            let st = self.state.lock().await;
            ids.iter().filter_map(|id| st.shapes.get(id).map(|rec| (id.clone(), rec.table.clone()))).collect()
        };
        for (id, table) in records {
            let _ = cmd_tx.send(SequencerCmd::RemoveShape { table, shape_id: id });
        }
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        if cmd_tx.send(SequencerCmd::Barrier { done: done_tx }).is_ok() {
            let _ = done_rx.await;
        }
        let mut st = self.state.lock().await;
        let mut lives = self.lives.lock().unwrap();
        for id in ids {
            lives.remove(id);
            st.forget_subscriptions(id);
            if let Some(share) = st.feed_shares.remove(id) {
                st.feed_by_sig.remove(&share.sig);
            }
            st.circuit_placement.remove(id);
            st.shapes.remove(id);
        }
    }

    /// Re-register one restored shape with the sequencer (the resume half of `apply_catalog`).
    pub(crate) async fn resume_shape(
        &self,
        cmd_tx: &mpsc::UnboundedSender<SequencerCmd>,
        rec: &ShapeRecord,
        compiled: &HashMap<TableRef, TableSchema>,
    ) -> Result<()> {
        let ts = compiled.get(&rec.table).with_context(|| format!("table '{}' no longer exists", rec.table))?;
        let out_cols: Option<Arc<Vec<usize>>> = match &rec.columns {
            Some(names) => {
                let idx: Result<Vec<usize>> = names.iter().map(|n| ts.column_index(n)).collect();
                Some(Arc::new(idx?))
            }
            None => None,
        };
        let num_id: u64 = rec.id.trim_start_matches('s').parse().unwrap_or(0);
        // Circuit-served restore: re-register with the sequencer, seed=false for plain shapes
        // (the stream is already complete up to the resume offset; dynamic groups re-derive
        // from the router snapshot, which the catch-up replay has brought to the same point).
        // Aggregates re-seed from the counts snapshot (their fold is not persisted) — same
        // fresh-value semantics as the legacy aggregate resume.
        if let Some(arr) = self.arrangements.lock().unwrap().clone() {
            match &rec.aggregate {
                Some(a) if matches!(a.func, AggFn::Count) && a.col.is_none() => {
                    if let Some(gcols) = arr.counts_group_cols(&rec.table).map(|g| g.to_vec()) {
                        if let Some(constraints) = plan_circuit_agg(rec.where_json.as_ref(), ts, &gcols) {
                            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
                            cmd_tx
                                .send(SequencerCmd::CreateCircuitAgg {
                                    table: rec.table.clone(),
                                    shape_id: rec.id.clone(),
                                    stream_path: rec.stream_path.clone(),
                                    constraints,
                                    ready: ready_tx,
                                })
                                .map_err(|_| anyhow::anyhow!("sequencer is gone"))?;
                            ready_rx
                                .await
                                .unwrap_or_else(|_| Err("sequencer dropped".to_string()))
                                .map_err(|e| anyhow::anyhow!(e))?;
                            self.state.lock().await.circuit_placement.insert(
                                rec.id.clone(),
                                CircuitPlacement { label: "counts".into(), col: None, counts: true },
                            );
                            return Ok(());
                        }
                    }
                }
                _ => {}
            }
        }
        // Compiled lazily, after the circuit branch: a circuit-served subquery record never
        // needs (and could not build) a registry-free compiled predicate.
        let pred = Arc::new(CompiledPredicate::compile_opt(rec.where_json.as_ref(), ts)?);
        let (kind, changes_only, aggregate) = match &rec.aggregate {
            Some(a) => {
                let col = a.col.as_deref().map(|c| ts.column_index(c)).transpose()?;
                (CreateKind::Aggregate { func: a.func, col }, false, Some((a.func, col)))
            }
            None => (CreateKind::Plain, true, None),
        };
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        cmd_tx
            .send(SequencerCmd::BeginShape {
                table: rec.table.clone(),
                shape_id: rec.id.clone(),
                num_id,
                stream_path: rec.stream_path.clone(),
                pred: pred.clone(),
                out_cols: out_cols.clone(),
                kind,
                ack: ack_tx,
            })
            .map_err(|_| anyhow::anyhow!("sequencer is gone"))?;
        backfill_and_activate(
            &self.ds,
            &self.pg_pool,
            cmd_tx,
            ts,
            &rec.table,
            &rec.id,
            &rec.stream_path,
            &pred,
            out_cols.as_ref(),
            changes_only,
            aggregate,
            &self.shutdown_token(),
            ack_rx,
        )
        .await
        .map(|_| ())
    }
}

/// A minimal, faultable durable-streams stand-in for the catalog writer + retirement queue tests.
///
/// Deliberately an HTTP server rather than a trait double: the behaviour under test is entirely
/// about what the engine does with real statuses (`is_unavailable` reads the response, not a mock's
/// intent), and a double that returns pre-classified errors would test the test.
#[cfg(test)]
pub(crate) mod testing {
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    /// Wait for a request-scoped FakeDs blocking generation to be released. `Notify` only wakes
    /// waiters; the generation is the durable test-control state, so an early release cannot be
    /// lost and cannot release a request that began in a later generation.
    async fn wait_while_blocked(
        block: &AtomicBool,
        release_generation: &AtomicU64,
        observed_generation: u64,
        released: &tokio::sync::Notify,
    ) {
        loop {
            let notified = released.notified();
            if !block.load(Ordering::SeqCst) || release_generation.load(Ordering::SeqCst) != observed_generation {
                return;
            }
            notified.await;
        }
    }

    fn logical_path(path: &str) -> &str {
        path.rsplit_once("/queries/test-query/").map_or(path, |(_, logical)| logical)
    }

    #[derive(Default)]
    pub(crate) struct FakeDsState {
        /// Remaining `POST /meta/catalog` calls to answer 503 (transient).
        fail_appends: AtomicU32,
        /// Remaining `POST /meta/catalog` calls to COMMIT and then answer 503 — a response lost
        /// after the write landed, which is what makes the writer's retry append a second copy.
        lose_responses: AtomicU32,
        /// Remaining `DELETE` calls to answer 503.
        fail_deletes: AtomicU32,
        appends: AtomicU64,
        deletes: AtomicU64,
        closes: AtomicU64,
        block_deletes: AtomicBool,
        delete_release_generation: AtomicU64,
        delete_started: tokio::sync::Notify,
        delete_blocked: tokio::sync::Notify,
        pause_delete_after_start: AtomicBool,
        delete_after_start_paused: tokio::sync::Notify,
        continue_delete_after_start: tokio::sync::Notify,
        release_delete: tokio::sync::Notify,
        block_retired: AtomicBool,
        retired_release_generation: AtomicU64,
        retired_started: tokio::sync::Notify,
        release_retired: tokio::sync::Notify,
        /// Shape streams to answer as absent/closed to restore preflight HEADs.
        missing_heads: Mutex<HashSet<String>>,
        closed_heads: Mutex<HashSet<String>>,
        events: Mutex<Vec<serde_json::Value>>,
        /// Opt-in read cut: report the requested cursor, then hold the response forever.
        /// The engine can cancel the read, but no page can complete before shutdown.
        held_change_reads: Mutex<Option<tokio::sync::mpsc::UnboundedSender<crate::changelog::LogPosition>>>,
    }

    pub(crate) struct FakeDs {
        url: String,
        state: Arc<FakeDsState>,
    }

    impl FakeDs {
        pub(crate) async fn start() -> FakeDs {
            use axum::extract::State;
            use axum::response::IntoResponse;
            let state = Arc::new(FakeDsState::default());
            let app = axum::Router::new()
                .route(
                    "/{*path}",
                    axum::routing::put(|| async { axum::http::StatusCode::OK })
                        .get(
                            |State(st): State<Arc<FakeDsState>>,
                             axum::extract::Path(path): axum::extract::Path<String>,
                             axum::extract::Query(query): axum::extract::Query<
                                std::collections::HashMap<String, String>,
                            >| async move {
                                let logical = logical_path(&path);
                                if logical == "meta/catalog" {
                                    return (
                                        [("stream-next-offset", "catalog-tail"), ("stream-up-to-date", "true")],
                                        axum::Json(st.events.lock().unwrap().clone()),
                                    )
                                        .into_response();
                                }
                                let observer = st.held_change_reads.lock().unwrap().clone();
                                if let Some(observer) = observer
                                    && let Some(segment) = logical.strip_prefix("changes/")
                                {
                                    let _ = observer.send(crate::changelog::LogPosition {
                                        segment: segment.parse().unwrap(),
                                        offset: query.get("offset").expect("read must specify a cursor").clone(),
                                    });
                                    return std::future::pending::<axum::response::Response>().await;
                                }
                                axum::http::StatusCode::METHOD_NOT_ALLOWED.into_response()
                            },
                        )
                        .head(
                            |State(st): State<Arc<FakeDsState>>,
                             axum::extract::Path(path): axum::extract::Path<String>| async move {
                                let logical = logical_path(&path);
                                if st.missing_heads.lock().unwrap().contains(logical) {
                                    return (axum::http::StatusCode::NOT_FOUND, [("stream-next-offset", "-1")])
                                        .into_response();
                                }
                                let closed = st.closed_heads.lock().unwrap().contains(logical);
                                let mut response =
                                    (axum::http::StatusCode::OK, [("stream-next-offset", "0")]).into_response();
                                if closed {
                                    response
                                        .headers_mut()
                                        .insert("stream-closed", axum::http::HeaderValue::from_static("true"));
                                }
                                response
                            },
                        )
                        .post(
                            |State(st): State<Arc<FakeDsState>>,
                             axum::extract::Path(path): axum::extract::Path<String>,
                             body: String| async move {
                                if logical_path(&path) != "meta/catalog" {
                                    st.closes.fetch_add(1, Ordering::SeqCst);
                                    return axum::http::StatusCode::NO_CONTENT;
                                }
                                if st
                                    .fail_appends
                                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                                    .is_ok()
                                {
                                    return axum::http::StatusCode::SERVICE_UNAVAILABLE;
                                }
                                let is_retired = serde_json::from_str::<serde_json::Value>(&body)
                                    .ok()
                                    .and_then(|value| value.as_array().cloned())
                                    .is_some_and(|items| items.iter().any(|item| item["t"] == "retired"));
                                if is_retired && st.block_retired.load(Ordering::SeqCst) {
                                    let release_generation = st.retired_release_generation.load(Ordering::SeqCst);
                                    st.retired_started.notify_one();
                                    wait_while_blocked(
                                        &st.block_retired,
                                        &st.retired_release_generation,
                                        release_generation,
                                        &st.release_retired,
                                    )
                                    .await;
                                }
                                st.appends.fetch_add(1, Ordering::SeqCst);
                                if let Ok(serde_json::Value::Array(items)) = serde_json::from_str(&body) {
                                    st.events.lock().unwrap().extend(items);
                                }
                                // Committed, then the answer is lost: the caller cannot tell this
                                // from a write that never happened, and retries.
                                if st
                                    .lose_responses
                                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                                    .is_ok()
                                {
                                    return axum::http::StatusCode::SERVICE_UNAVAILABLE;
                                }
                                axum::http::StatusCode::NO_CONTENT
                            },
                        )
                        .delete(
                            |State(st): State<Arc<FakeDsState>>,
                             axum::extract::Path(path): axum::extract::Path<String>| async move {
                                let release_generation = st.delete_release_generation.load(Ordering::SeqCst);
                                st.deletes.fetch_add(1, Ordering::SeqCst);
                                // The real server answers 404 for a stream it does not have; the client's
                                // delete tolerance (`ds::delete_stream`) is what makes that a retirement.
                                if st.missing_heads.lock().unwrap().contains(logical_path(&path)) {
                                    return axum::http::StatusCode::NOT_FOUND;
                                }
                                st.delete_started.notify_one();
                                if st.pause_delete_after_start.load(Ordering::SeqCst) {
                                    st.delete_after_start_paused.notify_one();
                                    st.continue_delete_after_start.notified().await;
                                }
                                if st.block_deletes.load(Ordering::SeqCst) {
                                    st.delete_blocked.notify_one();
                                    wait_while_blocked(
                                        &st.block_deletes,
                                        &st.delete_release_generation,
                                        release_generation,
                                        &st.release_delete,
                                    )
                                    .await;
                                }
                                if st
                                    .fail_deletes
                                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                                    .is_ok()
                                {
                                    return axum::http::StatusCode::SERVICE_UNAVAILABLE;
                                }
                                axum::http::StatusCode::NO_CONTENT
                            },
                        ),
                )
                .with_state(state.clone());
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });
            FakeDs { url: format!("http://{addr}"), state }
        }

        pub(crate) fn url(&self) -> &str {
            &self.url
        }

        pub(crate) fn hold_change_reads(&self) -> tokio::sync::mpsc::UnboundedReceiver<crate::changelog::LogPosition> {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            *self.state.held_change_reads.lock().unwrap() = Some(tx);
            rx
        }

        /// Answer the next `n` catalog appends with 503.
        pub(crate) fn fail_appends(&self, n: u32) {
            self.state.fail_appends.store(n, Ordering::SeqCst);
        }

        /// Commit the next `n` catalog appends and then answer 503 (a lost success response).
        pub(crate) fn lose_responses(&self, n: u32) {
            self.state.lose_responses.store(n, Ordering::SeqCst);
        }

        /// Answer the next `n` stream deletes with 503.
        pub(crate) fn fail_deletes(&self, n: u32) {
            self.state.fail_deletes.store(n, Ordering::SeqCst);
        }

        pub(crate) fn mark_stream_missing(&self, path: &str) {
            self.state.missing_heads.lock().unwrap().insert(path.to_string());
        }

        pub(crate) fn mark_stream_closed(&self, path: &str) {
            self.state.closed_heads.lock().unwrap().insert(path.to_string());
        }

        pub(crate) fn block_deletes(&self, block: bool) {
            self.state.block_deletes.store(block, Ordering::SeqCst);
        }

        pub(crate) async fn wait_delete_started(&self) {
            self.state.delete_started.notified().await;
        }

        /// Wait until a DELETE has observed the closed gate and is about to wait for its release.
        pub(crate) async fn wait_delete_blocked(&self) {
            self.state.delete_blocked.notified().await;
        }

        /// Stop a DELETE just after it has published its arrival, before it registers its blocking
        /// release waiter. This lets lifecycle tests exercise the no-lost-wakeup contract.
        pub(crate) fn pause_delete_after_start(&self, pause: bool) {
            self.state.pause_delete_after_start.store(pause, Ordering::SeqCst);
        }

        pub(crate) async fn wait_delete_after_start_paused(&self) {
            self.state.delete_after_start_paused.notified().await;
        }

        pub(crate) fn continue_delete_after_start(&self) {
            self.state.continue_delete_after_start.notify_one();
        }

        pub(crate) fn release_delete(&self) {
            self.state.delete_release_generation.fetch_add(1, Ordering::SeqCst);
            self.state.release_delete.notify_waiters();
        }

        pub(crate) fn block_retired(&self, block: bool) {
            self.state.block_retired.store(block, Ordering::SeqCst);
        }

        pub(crate) async fn wait_retired_started(&self) {
            self.state.retired_started.notified().await;
        }

        pub(crate) fn release_retired(&self) {
            self.state.retired_release_generation.fetch_add(1, Ordering::SeqCst);
            self.state.release_retired.notify_waiters();
        }

        pub(crate) fn deletes(&self) -> u64 {
            self.state.deletes.load(Ordering::SeqCst)
        }

        pub(crate) fn closes(&self) -> u64 {
            self.state.closes.load(Ordering::SeqCst)
        }

        /// Every catalog event that LANDED, in order.
        pub(crate) fn catalog_events(&self) -> Vec<serde_json::Value> {
            self.state.events.lock().unwrap().clone()
        }

        /// Just the `t` tags of the events that landed — the order-and-once assertions.
        pub(crate) fn catalog_kinds(&self) -> Vec<String> {
            self.catalog_events().iter().map(|e| e["t"].as_str().unwrap_or("?").to_string()).collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use testing::FakeDs;

    const OTTO_5492_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);

    async fn otto_5492_read(reads: &mut tokio::sync::mpsc::UnboundedReceiver<LogPosition>) -> LogPosition {
        tokio::time::timeout(OTTO_5492_DEADLINE, reads.recv())
            .await
            .expect("sequencer must issue a change-log read")
            .expect("storage read observer must remain open")
    }

    async fn otto_5492_stop(engine: &Engine) {
        engine.shutdown.begin();
        assert!(engine.shutdown.wait_for_parties(OTTO_5492_DEADLINE).await, "sequencer must stop");
        assert!(engine.catalog_tx.drain(OTTO_5492_DEADLINE).await, "final catalog writes must land");
    }

    /// Persist a dormant shape and the incident's checkpoint through the actual catalog writer.
    /// The dormant record keeps this test independent of snapshot SQL and boot replay backfills.
    async fn otto_5492_saved_catalog(engine: &Engine) -> CatalogFold {
        let saved = pos(270, "0000000000000000_0000000529873332");
        let mut created = created_event("public.users");
        // Leave s1 available to the racing request, so an ID collision cannot hide the cursor bug.
        created["rec"]["id"] = serde_json::json!("s99");
        created["rec"]["stream_path"] = serde_json::json!("shape/s99");
        for event in [
            serde_json::from_value::<CatalogEvent>(created).unwrap(),
            CatalogEvent::Dormant {
                id: "s99".into(),
                resume: saved.clone(),
                gate: crate::pg::SnapshotGate::passthrough(),
            },
            CatalogEvent::ChangesRotated { segment: 270, at: crate::changelog::now_secs() },
            CatalogEvent::Offset { pos: saved.clone(), highwater: Some((0x10, 2)) },
        ] {
            engine.catalog_tx.send_durable(event).await.unwrap();
        }
        let fold = engine.fold_catalog().await.expect("read the saved catalog through storage");
        assert_eq!(fold.start_pos(), saved, "fixture must contain the incident's saved checkpoint");
        engine
            .init_change_log(fold.current_segment, fold.segment_starts.clone(), &fold.start_pos())
            .await
            .expect("the saved segment exists at boot");
        fold
    }

    async fn otto_5492_create(engine: &Engine) -> axum::response::Response {
        use tower::ServiceExt;
        tokio::time::timeout(
            OTTO_5492_DEADLINE,
            crate::http::router(engine.clone()).oneshot(
                axum::http::Request::post("/v1/shapes")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(r#"{"table":"public.users","changesOnly":true}"#))
                    .unwrap(),
            ),
        )
        .await
        .expect("create must complete or refuse retryably during boot")
        .unwrap()
    }

    async fn otto_5492_boot_with_request(race_request: bool) {
        let server = FakeDs::start().await;
        let mut reads = server.hold_change_reads();
        let engine = Engine::new_pg_for_in_process_test(
            DsClient::new_for_in_process_test(server.url()),
            // No SQL is needed: changesOnly skips snapshots. Never contact another lane's PG.
            "postgres://postgres@127.0.0.1:55532/postgres".into(),
        );
        let fold = otto_5492_saved_catalog(&engine).await;
        let saved = fold.start_pos();
        let def = serde_json::from_value(serde_json::json!({
            "columns": { "id": { "type": "int" } }, "primaryKey": "id"
        }))
        .unwrap();
        let table = TableRef::parse("public.users").unwrap();
        let compiled = HashMap::from([(table.clone(), TableSchema::from_def(&table, &def).unwrap())]);

        // Deterministically stop setup at its schema-installed / pre-arrangement-seed boundary.
        // The rest of boot runs only after the request returns; no timing window or sleep.
        engine.health.store(HEALTH_STARTING, Ordering::Relaxed);
        *engine.tables_shared.write().unwrap() = compiled.clone();
        engine.state.lock().await.tables = compiled.clone();
        engine.subqueries.lock().await.set_schemas(Arc::new(compiled.clone()));
        if race_request {
            let response = otto_5492_create(&engine).await;
            assert!(
                response.status().is_success() || response.status() == axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "boot admission must succeed safely or refuse retryably: {}",
                response.status()
            );
            if response.status() == axum::http::StatusCode::SERVICE_UNAVAILABLE {
                assert!(response.headers().contains_key("retry-after"), "boot refusal must be retryable");
            }
        }
        let premature = engine.state.lock().await.sequencer.is_some();
        tokio::time::timeout(OTTO_5492_DEADLINE, engine.apply_catalog(fold, &compiled, RestoreMode::Resume))
            .await
            .expect("catalog restore must complete")
            .unwrap();
        engine.ensure_sequencer(&mut *engine.state.lock().await);
        let read = otto_5492_read(&mut reads).await;
        otto_5492_stop(&engine).await;

        assert_eq!(
            (premature, read),
            (false, saved),
            "a pre-restore request must not start replay; the live reader must use the restored checkpoint"
        );
    }

    #[tokio::test]
    async fn otto_5492_create_before_restore_reads_from_saved_position() {
        otto_5492_boot_with_request(true).await;
    }

    #[tokio::test]
    async fn otto_5492_restore_without_request_reads_from_saved_position() {
        otto_5492_boot_with_request(false).await;
    }

    #[tokio::test]
    async fn otto_5492_unread_sequencer_shutdown_preserves_catalog_position() {
        let server = FakeDs::start().await;
        let mut reads = server.hold_change_reads();
        // Inject the already-stale task below admission, independently of the boot-latch fix.
        // Library construction allows this even if Postgres spawning is later boot-gated.
        let engine = Engine::new_for_in_process_test(DsClient::new_for_in_process_test(server.url()));
        let fold = otto_5492_saved_catalog(&engine).await;
        let saved = fold.start_pos();
        engine.ensure_sequencer(&mut *engine.state.lock().await);
        assert_eq!(otto_5492_read(&mut reads).await, LogPosition::start(), "inject a stale live cursor");

        tokio::time::timeout(OTTO_5492_DEADLINE, engine.apply_catalog(fold, &HashMap::new(), RestoreMode::Resume))
            .await
            .expect("restore must complete while the read is held")
            .unwrap();
        assert_eq!(*engine.seq_start.lock().unwrap(), saved, "restore installed its newer checkpoint");
        // The HTTP handler never completes a page. Stop only after the resumed read has arrived.
        otto_5492_read(&mut reads).await;
        otto_5492_stop(&engine).await;
        let next_boot = engine.fold_catalog().await.expect("fold the persisted catalog after shutdown");
        assert_eq!(
            next_boot.start_pos(),
            saved,
            "shutdown without a completed read must not overwrite the restored restart checkpoint"
        );
        assert_eq!(next_boot.start_highwater, Some((0x10, 2)), "preserve the matching de-duplication frontier");
    }

    #[tokio::test]
    async fn otto_5492_library_define_schema_still_starts_and_creates() {
        let server = FakeDs::start().await;
        let mut reads = server.hold_change_reads();
        let engine = Engine::new_for_in_process_test(DsClient::new_for_in_process_test(server.url()));
        let schema = serde_json::from_value(serde_json::json!({
            "tables": { "users": { "columns": { "id": { "type": "int" } }, "primaryKey": "id" } }
        }))
        .unwrap();
        engine.define_schema(&schema).await.unwrap();
        let response = otto_5492_create(&engine).await;
        let read = otto_5492_read(&mut reads).await;
        otto_5492_stop(&engine).await;
        assert_eq!(response.status(), axum::http::StatusCode::OK, "library mode needs no Postgres boot latch");
        assert_eq!(read, LogPosition::start());
    }

    #[tokio::test]
    async fn otto_5492_schema_route_during_pg_boot_cannot_start_sequencer() {
        use tower::ServiceExt;

        let server = FakeDs::start().await;
        let mut reads = server.hold_change_reads();
        let engine = Engine::new_pg_for_in_process_test(
            DsClient::new_for_in_process_test(server.url()),
            "postgres://postgres@127.0.0.1:55532/postgres".into(),
        );
        let fold = otto_5492_saved_catalog(&engine).await;
        let saved = fold.start_pos();
        engine.health.store(HEALTH_STARTING, Ordering::Release);
        // The server is listening, but setup has not installed the restored replay cursor.
        let response = tokio::time::timeout(
            OTTO_5492_DEADLINE,
            crate::http::router(engine.clone()).oneshot(
                axum::http::Request::post("/schema")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        r#"{"schema":{"tables":{"users":{"columns":{"id":{"type":"int"}},"primaryKey":"id"}}}}"#,
                    ))
                    .unwrap(),
            ),
        )
        .await
        .expect("schema request must finish without waiting for boot")
        .unwrap();
        let premature = engine.state.lock().await.sequencer.is_some();
        let installed_tables = engine.table_count().await;
        tokio::time::timeout(OTTO_5492_DEADLINE, engine.apply_catalog(fold, &HashMap::new(), RestoreMode::Resume))
            .await
            .expect("catalog restore must finish")
            .unwrap();
        let read = otto_5492_read(&mut reads).await;
        otto_5492_stop(&engine).await;

        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST, "POST /schema is library-mode only");
        assert!(!response.headers().contains_key("retry-after"), "wrong-mode requests cannot succeed on retry");
        assert!(!premature, "POST /schema must not start the PostgreSQL sequencer before restore");
        assert_eq!(installed_tables, 0, "POST /schema must not install caller-supplied PostgreSQL schemas");
        assert_eq!(read, saved, "boot must read from its restored checkpoint");
    }

    #[tokio::test]
    async fn otto_5492_define_schema_rejects_active_pg_at_engine_boundary() {
        let server = FakeDs::start().await;
        let _reads = server.hold_change_reads();
        let engine = Engine::new_pg_for_in_process_test(
            DsClient::new_for_in_process_test(server.url()),
            "postgres://postgres@127.0.0.1:55532/postgres".into(),
        );
        // This API is never supported in PostgreSQL mode, including after the boot gate opens.
        engine.health.store(HEALTH_ACTIVE, Ordering::Release);
        let schema = serde_json::from_value(serde_json::json!({
            "tables": { "users": { "columns": { "id": { "type": "int" } }, "primaryKey": "id" } }
        }))
        .unwrap();
        let result = tokio::time::timeout(OTTO_5492_DEADLINE, engine.define_schema(&schema))
            .await
            .expect("direct schema definition must return promptly");
        let spawned = engine.state.lock().await.sequencer.is_some();
        let installed_tables = engine.table_count().await;
        otto_5492_stop(&engine).await;

        assert!(result.is_err(), "define_schema must reject PostgreSQL mode even when ACTIVE");
        assert!(!spawned, "rejection must precede sequencer spawn");
        assert_eq!(installed_tables, 0, "rejection must precede schema installation");
        assert!(engine.tables_shared.read().unwrap().is_empty(), "the shared schema view must remain untouched");
    }

    fn expected_store_bound() -> crate::store_identity::StoreBound {
        crate::store_identity::StoreBound::coupled_v1(&crate::store_identity::StreamScope::in_process_test_scope())
    }

    #[test]
    fn store_bound_is_exactly_one_matching_event_zero() {
        let expected = expected_store_bound();
        let bound = serde_json::to_value(CatalogEvent::StoreBound(expected.clone())).unwrap();
        validate_store_binding(&[bound.clone(), serde_json::json!({ "t": "offset" })], &expected).unwrap();
        assert!(validate_store_binding(&[], &expected).is_err());
        assert!(validate_store_binding(&[serde_json::json!({ "t": "offset" }), bound.clone()], &expected).is_err());
        assert!(validate_store_binding(&[bound.clone(), bound], &expected).is_err());

        let mut conflicting = expected.clone();
        conflicting.query_generation = "other-query".to_string();
        let conflicting = serde_json::to_value(CatalogEvent::StoreBound(conflicting)).unwrap();
        assert!(validate_store_binding(&[conflicting], &expected).is_err());
    }

    fn created_event(table: &str) -> serde_json::Value {
        serde_json::json!({
            "t": "created",
            "rec": {
                "id": "s1",
                "table": table,
                "stream_path": "shape/s1",
                "changes_only": false,
                "where_json": null,
                "columns": null,
                "family_key": null,
                "is_subquery": false,
                "aggregate": null,
                "fingerprint": null,
            },
            "sig": null,
            "subscription": "sub-a",
            "at": 100,
        })
    }

    /// The catalog is strict where API ingress is lenient: a record must already carry the
    /// canonical `schema.name`. A bare one can only be a catalog written before ADR-0002, and
    /// resolving it would leave its (bare-spelled) sharing signature unable to match an identical
    /// post-cutover create — two maintained streams for one table.
    #[test]
    fn catalog_records_must_be_canonically_spelled() {
        let ok =
            serde_json::from_value::<CatalogEvent>(created_event("public.users")).expect("a canonical record restores");
        match ok {
            CatalogEvent::Created { rec, .. } => {
                assert_eq!(rec.table.to_string(), "public.users");
                assert_eq!(rec.table.schema(), "public");
            }
            other => panic!("expected Created, got {other:?}"),
        }

        let err = serde_json::from_value::<CatalogEvent>(created_event("users"))
            .expect_err("a bare record is refused, not resolved to public.users");
        assert!(err.to_string().contains("canonical"), "{err}");

        // The same rule applies to a non-public bare-impossible spelling: `a.b.c` is not a table.
        assert!(serde_json::from_value::<CatalogEvent>(created_event("a.b.c")).is_err());
        // ...and an explicitly qualified non-public table restores untouched.
        let ok = serde_json::from_value::<CatalogEvent>(created_event("other.users")).unwrap();
        match ok {
            CatalogEvent::Created { rec, .. } => assert_eq!(rec.table.to_string(), "other.users"),
            other => panic!("expected Created, got {other:?}"),
        }
    }

    fn bound(slot: &str, sysid: &str, at: &str) -> CatalogEvent {
        CatalogEvent::SlotBound(crate::engine::epoch::SlotBinding {
            system_identifier: sysid.to_string(),
            timeline_id: 1,
            slot: slot.to_string(),
            bound_at: at.to_string(),
        })
    }

    /// Fold a sequence of events, giving each its own `eid` — the shape the writer produces when
    /// nothing was retried. [`fold_of_with_eids`] is the one that repeats them.
    fn fold_of(events: Vec<CatalogEvent>) -> CatalogFold {
        fold_of_with_eids(events.into_iter().enumerate().map(|(i, ev)| (format!("e{i}"), ev)).collect())
    }

    fn fold_of_with_eids(events: Vec<(String, CatalogEvent)>) -> CatalogFold {
        let mut fold = CatalogFold::default();
        for (eid, ev) in events {
            fold.apply_once(&eid, ev);
        }
        fold
    }

    #[test]
    fn backfill_sized_updates_created_records_and_old_records_remain_compatible() {
        let rec = crate::engine::ShapeRecord {
            id: "s1".into(),
            table: "users".into(),
            stream_path: "shape/s1".into(),
            changes_only: false,
            where_json: None,
            columns: None,
            family_key: None,
            is_subquery: false,
            aggregate: None,
            fingerprint: None,
            backfill_rows: None,
            backfill_bytes: None,
        };
        let fold = fold_of(vec![
            CatalogEvent::Created { rec: rec.clone(), sig: None, subscription: "sub".into(), at: 1 },
            CatalogEvent::BackfillSized { id: "s1".into(), rows: 12, bytes: 3456 },
        ]);
        let restored = &fold.recs.get("s1").unwrap().0;
        assert_eq!(restored.backfill_rows, Some(12));
        assert_eq!(restored.backfill_bytes, Some(3456));
        let old = serde_json::json!({"t":"created","id":"s-old","table":"users","streamPath":"shape/s-old","changesOnly":false});
        assert!(serde_json::from_value::<CatalogEvent>(old).is_err() || rec.backfill_rows.is_none());
    }

    /// The three shape-lifecycle events, with the subscription and lease timestamp ADR-0008 gives
    /// them — spelled out here so the fold tests read as "who is subscribed", not as bookkeeping.
    fn joined(id: &str, sub: &str, at: u64) -> CatalogEvent {
        CatalogEvent::Joined { id: id.to_string(), subscription: sub.to_string(), at }
    }

    fn left(id: &str, sub: &str) -> CatalogEvent {
        CatalogEvent::Left { id: id.to_string(), subscription: sub.to_string(), lapsed: false }
    }

    fn subs_of(fold: &CatalogFold, id: &str) -> Vec<String> {
        fold.recs.get(id).map(|r| r.2.keys().cloned().collect()).unwrap_or_default()
    }

    fn pos(segment: u32, offset: &str) -> LogPosition {
        LogPosition { segment, offset: offset.to_string() }
    }

    /// A catalog with no `SlotBound` anywhere is a genuine first boot — the state that licenses the
    /// engine to create a slot from nothing (ADR-0004).
    #[test]
    fn a_catalog_without_a_binding_folds_to_no_epoch() {
        let fold = fold_of(vec![CatalogEvent::Offset { pos: pos(0, "42"), highwater: None }]);
        assert!(fold.binding.is_none());
        assert_eq!(fold.start_pos, pos(0, "42"));
        // Nothing at all folds to the empty catalog, which the restore skips outright.
        assert!(CatalogFold::default().is_empty());
    }

    #[test]
    fn source_drain_receipts_are_durable_and_idempotent_by_commit_id() {
        let first = SourceDrainReceipt {
            source_commit_id: "018f5f4d-70c2-7d70-a4d5-5f7355078f85".to_string(),
            commit_lsn: "0/16B6C50".to_string(),
        };
        let second = SourceDrainReceipt {
            source_commit_id: "018f5f4d-70c2-7d70-a4d5-5f7355078f86".to_string(),
            commit_lsn: "0/16B6D00".to_string(),
        };
        let fold = fold_of(vec![
            CatalogEvent::SourceDrained(first.clone()),
            CatalogEvent::SourceDrained(first.clone()),
            CatalogEvent::SourceDrained(second.clone()),
            CatalogEvent::SourceDrained(first.clone()),
        ]);
        assert_eq!(fold.source_receipts.len(), 2);
        assert_eq!(fold.source_receipts.get(&first.source_commit_id), Some(&first));
        assert_eq!(fold.source_receipts.get(&second.source_commit_id), Some(&second));
        assert_eq!(fold.last_source_receipt, Some(second));
    }

    #[test]
    fn source_drain_receipt_wire_shape_is_stable() {
        let receipt = SourceDrainReceipt {
            source_commit_id: "018f5f4d-70c2-7d70-a4d5-5f7355078f85".to_string(),
            commit_lsn: "0/16B6C50".to_string(),
        };
        let json = serde_json::to_value(CatalogEvent::SourceDrained(receipt.clone())).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "t": "sourceDrained",
                "sourceCommitId": receipt.source_commit_id,
                "commitLsn": receipt.commit_lsn,
            })
        );
        match serde_json::from_value::<CatalogEvent>(json).unwrap() {
            CatalogEvent::SourceDrained(decoded) => assert_eq!(decoded, receipt),
            _ => panic!("sourceDrained decoded as the wrong catalog event"),
        }
    }

    #[tokio::test]
    async fn source_drain_receipt_restores_even_without_any_shape_state() {
        let receipt = SourceDrainReceipt {
            source_commit_id: "018f5f4d-70c2-7d70-a4d5-5f7355078f85".to_string(),
            commit_lsn: "0/16B6C50".to_string(),
        };
        let fold = fold_of(vec![CatalogEvent::SourceDrained(receipt.clone())]);
        let engine = Engine::new_for_in_process_test(DsClient::new_for_in_process_test("http://127.0.0.1:1"));
        engine.apply_catalog(fold, &HashMap::new(), RestoreMode::Resume).await.unwrap();
        assert_eq!(engine.source_drain_receipt(&receipt.source_commit_id), Some(receipt.clone()));
        assert_eq!(engine.last_source_drain_receipt(), Some(receipt));
    }

    #[test]
    fn restored_receipts_cannot_cross_a_closure_that_happened_during_boot() {
        let old = SourceDrainReceipt {
            source_commit_id: "018f5f4d-70c2-7d70-a4d5-5f7355078f85".to_string(),
            commit_lsn: "0/16B6C50".to_string(),
        };
        let mut progress = super::super::SourceReceiptProgress::default();

        // HTTP can close admission before `apply_catalog` restores this old durable receipt.
        // The restore path calls `record_restored`, which must not advance across that frontier.
        progress.snapshot_closure();
        progress.record_restored(&old.source_commit_id);
        assert!(progress.has_closure());
        assert!(
            !progress.is_after_closure(&old.source_commit_id),
            "a receipt restored after close is historical, not a fresh source fence"
        );
        progress.record("018f5f4d-70c2-7d70-a4d5-5f7355078f86");
        assert!(
            progress.is_after_closure("018f5f4d-70c2-7d70-a4d5-5f7355078f86"),
            "the first live receipt after restore must advance beyond the closure"
        );
    }

    #[tokio::test]
    async fn stale_catalog_refold_does_not_erase_a_newer_live_source_receipt() {
        let first = SourceDrainReceipt {
            source_commit_id: "018f5f4d-70c2-7d70-a4d5-5f7355078f85".to_string(),
            commit_lsn: "0/16B6C50".to_string(),
        };
        let newer = SourceDrainReceipt {
            source_commit_id: "018f5f4d-70c2-7d70-a4d5-5f7355078f86".to_string(),
            commit_lsn: "0/16B6D00".to_string(),
        };
        let engine = Engine::new_for_in_process_test(DsClient::new_for_in_process_test("http://127.0.0.1:1"));
        engine.source_receipts.lock().unwrap().insert(newer.source_commit_id.clone(), newer.clone());
        *engine.last_source_receipt.lock().unwrap() = Some(newer.clone());

        let stale_fold = fold_of(vec![CatalogEvent::SourceDrained(first.clone())]);
        engine.apply_catalog(stale_fold, &HashMap::new(), RestoreMode::Resume).await.unwrap();

        assert_eq!(engine.source_drain_receipt(&first.source_commit_id), Some(first));
        assert_eq!(engine.source_drain_receipt(&newer.source_commit_id), Some(newer.clone()));
        assert_eq!(engine.last_source_drain_receipt(), Some(newer));
    }

    /// The de-duplication highwater is checkpointed WITH the position (ADR-0003), and the fold's
    /// last `Offset` wins for both together.
    ///
    /// It has to travel with the position because a commit too large for one request body is
    /// appended in several chunks: a crash can leave a prefix of a transaction applied and
    /// checkpointed while the rest is still to be re-delivered. Restoring the position without the
    /// highwater would re-apply that prefix — and aggregate/subquery contributor weights are not
    /// idempotent under duplicates.
    #[test]
    fn the_dedup_highwater_is_restored_with_the_checkpoint_it_was_taken_at() {
        let fold = fold_of(vec![
            CatalogEvent::Offset { pos: pos(0, "10"), highwater: Some((0x10, 3)) },
            CatalogEvent::Offset { pos: pos(0, "20"), highwater: Some((0x20, 7)) },
        ]);
        assert_eq!(fold.start_pos, pos(0, "20"));
        assert_eq!(fold.start_highwater, Some((0x20, 7)), "the last checkpoint's highwater, not the first");

        // A checkpoint taken before anything was applied carries none, and CLEARS an older one:
        // last-writer-wins for the pair, never a mix of a new position and a stale highwater.
        let fold = fold_of(vec![
            CatalogEvent::Offset { pos: pos(0, "20"), highwater: Some((0x20, 7)) },
            CatalogEvent::Offset { pos: pos(1, "0"), highwater: None },
        ]);
        assert_eq!(fold.start_pos, pos(1, "0"));
        assert_eq!(fold.start_highwater, None);

        // Nothing at all: no highwater, so a first boot de-duplicates from scratch.
        assert_eq!(CatalogFold::default().start_highwater, None);
    }

    /// The wire form: the highwater is omitted when absent (so a checkpoint that has applied
    /// nothing is the same bytes it always was) and round-trips when present.
    #[test]
    fn the_checkpoint_wire_form_carries_the_highwater_only_when_there_is_one() {
        let bare = serde_json::to_value(CatalogEvent::Offset { pos: pos(2, "9"), highwater: None }).unwrap();
        assert_eq!(bare.get("highwater"), None, "{bare}");
        let with = serde_json::to_value(CatalogEvent::Offset { pos: pos(2, "9"), highwater: Some((5, 6)) }).unwrap();
        assert_eq!(with["highwater"], serde_json::json!([5, 6]));
        match serde_json::from_value::<CatalogEvent>(with).unwrap() {
            CatalogEvent::Offset { pos: p, highwater } => {
                assert_eq!(p, pos(2, "9"));
                assert_eq!(highwater, Some((5, 6)));
            }
            other => panic!("expected Offset, got {other:?}"),
        }
    }

    /// The change log's segmentation folds out of the same log (ADR-0006): the last rotation is the
    /// current segment, and EVERY rotation is kept — segment `n` was closed when `n+1` began, which
    /// is the clock the retain window that governs deletion runs on.
    #[test]
    fn rotations_fold_to_the_current_segment_and_every_segments_start() {
        let fold = fold_of(vec![
            CatalogEvent::ChangesRotated { segment: 0, at: 100 },
            CatalogEvent::Offset { pos: pos(0, "9"), highwater: None },
            CatalogEvent::ChangesRotated { segment: 1, at: 200 },
            CatalogEvent::ChangesRotated { segment: 2, at: 300 },
        ]);
        assert_eq!(fold.current_segment, 2);
        assert_eq!(fold.segment_starts.get(&0), Some(&100));
        assert_eq!(fold.segment_starts.get(&1), Some(&200));
        assert_eq!(fold.segment_starts.get(&2), Some(&300));
        // A catalog that has only ever rotated has nothing to restore.
        assert_eq!(fold.start_pos, pos(0, "9"));

        // Nothing rotated = segment 0, which is also what a first boot creates.
        assert_eq!(CatalogFold::default().current_segment, 0);
    }

    /// A deleted segment leaves the folded set, so a restart neither re-plans retiring streams that
    /// are long gone nor reports a `changes_segments_retained` gauge counting them. It never moves
    /// the current segment — the current one is never deleted.
    #[test]
    fn deleted_segments_leave_the_fold() {
        let fold = fold_of(vec![
            CatalogEvent::ChangesRotated { segment: 0, at: 100 },
            CatalogEvent::ChangesRotated { segment: 1, at: 200 },
            CatalogEvent::ChangesRotated { segment: 2, at: 300 },
            CatalogEvent::ChangesSegmentDeleted { segment: 0 },
        ]);
        assert_eq!(fold.current_segment, 2);
        assert_eq!(fold.segment_starts.keys().copied().collect::<Vec<_>>(), vec![1, 2]);

        let json = serde_json::to_value(CatalogEvent::ChangesSegmentDeleted { segment: 7 }).unwrap();
        assert_eq!(json["t"], "changesSegmentDeleted");
        assert_eq!(json["segment"], 7);
    }

    #[test]
    fn consumer_pins_fold_last_write_wins_and_evictions_remove_them() {
        let fold = fold_of(vec![
            CatalogEvent::ConsumerPinned { id: "kernel".into(), position: pos(0, "10") },
            CatalogEvent::ConsumerPinned { id: "kernel".into(), position: pos(1, "20") },
            CatalogEvent::ConsumerPinned { id: "audit".into(), position: pos(1, "30") },
            CatalogEvent::ConsumerEvicted { id: "kernel".into() },
        ]);
        assert_eq!(fold.consumers.get("audit"), Some(&pos(1, "30")));
        assert!(!fold.consumers.contains_key("kernel"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn restore_refuses_a_consumer_pin_for_an_unknown_change_log_segment() {
        let fold = fold_of(vec![CatalogEvent::ConsumerPinned { id: "kernel".into(), position: pos(9, "10") }]);
        let engine = Engine::new_for_in_process_test(DsClient::new_for_in_process_test("http://127.0.0.1:1"));
        engine.changes.state().adopt(1, [(0, 100), (1, 200)].into_iter().collect(), None);

        let err = engine
            .apply_catalog(fold, &HashMap::new(), RestoreMode::Resume)
            .await
            .expect_err("a restored consumer cannot resume in a segment the catalog no longer retains");
        assert!(
            format!("{err:#}").contains("consumer kernel") && format!("{err:#}").contains("changes/9"),
            "the restore error must name both the consumer and its missing segment: {err:#}"
        );
        assert!(engine.consumers().is_empty(), "a rejected restore must not publish any pins");
    }

    /// The sequencer's checkpoint and a dormant shape's resume state carry the SEGMENT, so a
    /// restart after a rotation resumes in the right stream rather than at a byte position in a
    /// stream that no longer holds those bytes.
    #[test]
    fn checkpoints_and_dormant_resumes_carry_their_segment() {
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let gate = crate::pg::SnapshotGate::passthrough();
        let fold = fold_of(vec![
            created,
            CatalogEvent::Dormant { id: "s1".to_string(), resume: pos(3, "77"), gate },
            CatalogEvent::Offset { pos: pos(4, "12"), highwater: None },
        ]);
        assert_eq!(fold.start_pos, pos(4, "12"));
        assert_eq!(fold.recs.get("s1").and_then(|r| r.3.as_ref()).map(|(p, _)| p.clone()), Some(pos(3, "77")));

        // Wire form: an object, never a bare string.
        let json = serde_json::to_value(CatalogEvent::Offset { pos: pos(4, "12"), highwater: None }).unwrap();
        assert_eq!(json["t"], "offset");
        assert_eq!(json["pos"]["segment"], 4);
        assert_eq!(json["pos"]["offset"], "12");
    }

    /// A catalog written before ADR-0006 carries bare change-log offsets, which cannot be repaired:
    /// the byte position belongs to a stream that no longer exists under that name. It is named and
    /// refused at boot, never coerced to "segment 0" (which would resume at an unrelated point) and
    /// never silently skipped by the strict deserializer (which for an `Offset` would restart the
    /// sequencer from the beginning of the log).
    #[test]
    fn a_pre_segmentation_catalog_is_refused_by_name() {
        let bare_offset = serde_json::json!({ "t": "offset", "offset": "0000000000000000_0000000000000042" });
        let detail = predates_segmentation(&bare_offset).expect("a bare offset is recognised");
        assert!(detail.contains("bare change-log offset"), "{detail}");
        assert!(
            CatalogPredatesSegmentation { detail }.to_string().contains("predates ADR-0006"),
            "the operator is told which cutover the catalog is on the wrong side of"
        );

        let bare_dormant = serde_json::json!({
            "t": "dormant",
            "id": "s7",
            "resume_offset": "0000000000000000_0000000000000042",
            "gate": {},
        });
        let detail = predates_segmentation(&bare_dormant).expect("a bare dormant resume is recognised");
        assert!(detail.contains("s7"), "the refusal names the shape: {detail}");

        // The current spellings are not mistaken for the old ones.
        let now = serde_json::to_value(CatalogEvent::Offset { pos: pos(2, "9"), highwater: None }).unwrap();
        assert!(predates_segmentation(&now).is_none());
        let gate = crate::pg::SnapshotGate::passthrough();
        let now =
            serde_json::to_value(CatalogEvent::Dormant { id: "s7".to_string(), resume: pos(2, "9"), gate }).unwrap();
        assert!(predates_segmentation(&now).is_none());
        // ...and neither is an unrelated event.
        assert!(predates_segmentation(&created_event("public.users")).is_none());
    }

    /// The LAST binding is the epoch in force: a reset appends its new one after the `Dropped`
    /// records of the epoch it ended, so the fold must not stop at the first.
    #[test]
    fn the_last_slot_bound_wins() {
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let fold = fold_of(vec![
            bound("s", "7300000000000000001", "2026-08-20T09:00:00.000Z"),
            created,
            CatalogEvent::Dropped { id: "s1".to_string() },
            bound("s", "7300000000000000001", "2026-08-21T11:30:00.000Z"),
        ]);
        let b = fold.binding.expect("the epoch is folded out of the log");
        assert_eq!(b.bound_at, "2026-08-21T11:30:00.000Z", "the newest binding is the epoch in force");
        assert!(fold.recs.is_empty(), "the epoch it ended took its shapes with it");
    }

    /// The binding survives everything else in the log — dropping every shape does not drop the
    /// epoch, and the epoch does not disturb the shapes.
    #[test]
    fn shapes_and_the_binding_fold_independently() {
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let fold = fold_of(vec![
            bound("s", "7300000000000000001", "2026-08-21T11:30:00.000Z"),
            created,
            joined("s1", "sub-b", 100),
        ]);
        assert_eq!(
            subs_of(&fold, "s1"),
            vec!["sub-a".to_string(), "sub-b".to_string()],
            "create + join = two subscriptions"
        );
        assert_eq!(fold.binding.map(|b| b.slot), Some("s".to_string()));
    }

    /// The event is stored (and restored) as its own `t` case, alongside the shape lifecycle — a
    /// catalog is one log, and its epoch is part of it.
    #[test]
    fn slot_bound_round_trips_on_the_wire() {
        let json = serde_json::to_value(bound("electric_circuits", "73", "2026-08-21T11:30:00.000Z")).unwrap();
        assert_eq!(json["t"], "slotBound");
        assert_eq!(json["slot"], "electric_circuits");
        assert_eq!(json["system_identifier"], "73");
        assert_eq!(json["timeline_id"], 1);
        assert_eq!(json["bound_at"], "2026-08-21T11:30:00.000Z");
        match serde_json::from_value::<CatalogEvent>(json).unwrap() {
            CatalogEvent::SlotBound(b) => assert_eq!(b.system_identifier, "73"),
            other => panic!("expected SlotBound, got {other:?}"),
        }
    }

    // --- the writer never drops an event -------------------------------------------------------

    /// The classification the whole retry-or-die decision hangs on, without exiting anything: a 5xx
    /// or a dead transport is storage having a moment; a 4xx is an answer.
    #[test]
    fn a_5xx_retries_and_a_4xx_refuses() {
        let five =
            anyhow::Error::new(crate::ds::DsUnavailable { op: "POST", path: CATALOG_STREAM.to_string(), status: 503 });
        assert_eq!(classify_append(&five), AppendVerdict::Retry);
        let four = anyhow::anyhow!("POST meta/catalog -> 400 Bad Request: malformed event");
        assert_eq!(classify_append(&four), AppendVerdict::Refused);
        let serialization = anyhow::anyhow!("catalog event could not be serialized");
        assert_eq!(classify_append(&serialization), AppendVerdict::Refused);
    }

    #[test]
    fn the_catalog_schedule_climbs_to_five_seconds_and_stays() {
        let ms = |a: u32| catalog_backoff(a).as_millis();
        assert_eq!(ms(1), 100, "the first retry is immediate-ish: a create is waiting on it");
        assert_eq!(ms(2), 200);
        assert_eq!(ms(6), 3200);
        assert_eq!(ms(7), 5000, "capped");
        assert_eq!(ms(70), 5000, "and stays capped");
    }

    /// The regression this exists for: a 503 on a catalog append used to log "event lost" and drop
    /// the event, so an acknowledged shape simply was not in the log a restart folds.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_refused_append_is_retried_in_place_and_lands_exactly_once() {
        let server = FakeDs::start().await;
        server.fail_appends(3);
        let w = spawn_catalog_writer(
            DsClient::new_for_in_process_test(server.url()),
            crate::shutdown::ShutdownToken::new(),
        );
        w.send(joined("s1", "sub-a", 100));
        w.send(left("s1", "sub-a"));
        assert!(w.drain(std::time::Duration::from_secs(20)).await, "the queue must drain");
        assert_eq!(
            server.catalog_kinds(),
            vec!["joined".to_string(), "left".to_string()],
            "both events land, exactly once each, in send order"
        );
    }

    /// `send_durable` is the durable-before-ack primitive: the future must not resolve while the
    /// append is still being refused.
    #[tokio::test(flavor = "multi_thread")]
    async fn send_durable_resolves_only_after_the_append_lands() {
        let server = FakeDs::start().await;
        server.fail_appends(2);
        let w = spawn_catalog_writer(
            DsClient::new_for_in_process_test(server.url()),
            crate::shutdown::ShutdownToken::new(),
        );
        let landed = w.send_durable(joined("s1", "sub-a", 100));
        tokio::pin!(landed);
        // 100 ms + 200 ms of backoff to go, so the ack cannot have happened yet.
        tokio::select! {
            _ = &mut landed => panic!("acknowledged a create whose record storage had refused"),
            _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
        }
        assert!(server.catalog_kinds().is_empty(), "nothing has landed yet");
        tokio::time::timeout(std::time::Duration::from_secs(20), landed)
            .await
            .expect("the append lands once storage recovers")
            .expect("the catalog writer remains alive");
        assert_eq!(server.catalog_kinds(), vec!["joined".to_string()]);
    }

    /// Losing the writer task is not a successful append. In particular, a source-fence caller must
    /// never continue on to publish an in-memory receipt merely because its acknowledgement sender
    /// disappeared.
    #[tokio::test]
    async fn writer_loss_cannot_satisfy_a_durable_wait() {
        let (tx, rx) = mpsc::unbounded_channel();
        drop(rx);
        let shutdown = crate::shutdown::ShutdownToken::new();
        let writer = CatalogWriter {
            tx,
            shutdown: shutdown.clone(),
            in_flight: Arc::new(std::sync::atomic::AtomicI64::new(0)),
            eid_nonce: "writer-gone".to_string(),
            next_eid: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            landed_seq: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            landed_notify: Arc::new(tokio::sync::Notify::new()),
            durable: Arc::new(std::sync::Mutex::new(None)),
        };

        let outcome =
            tokio::time::timeout(std::time::Duration::from_millis(25), writer.send_durable(joined("s1", "sub-a", 100)))
                .await
                .expect("writer loss must be reported rather than hanging");
        assert!(outcome.is_err(), "a dropped writer acknowledgement must not manufacture durability");
        assert!(shutdown.is_shutting_down(), "writer loss must begin fail-closed process shutdown");
        let barrier = tokio::time::timeout(std::time::Duration::from_millis(25), writer.wait_durable())
            .await
            .expect("a failed enqueue must not leave an unreachable durability barrier");
        assert!(barrier.is_err(), "the barrier must report writer loss rather than manufacturing durability");
    }

    // --- retirement: intent and completion ------------------------------------------------------

    /// A `Dropped` with no `Retired` is the durable form of "this stream must still go".
    #[test]
    fn a_drop_without_a_retirement_stays_pending() {
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let fold = fold_of(vec![created, CatalogEvent::Dropped { id: "s1".to_string() }]);
        assert!(fold.recs.is_empty(), "the record is gone the moment the drop is recorded");
        assert_eq!(fold.pending_retirements(), vec![("s1".to_string(), "shape/s1".to_string())]);
    }

    /// ...and the completion clears it, so an ordinary retirement leaves the boot nothing to do.
    #[test]
    fn a_completed_retirement_leaves_nothing_pending() {
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let fold = fold_of(vec![
            created,
            CatalogEvent::Dropped { id: "s1".to_string() },
            CatalogEvent::Retired { id: "s1".to_string() },
        ]);
        assert!(fold.pending_retirements().is_empty());
    }

    /// A `Dropped` for a record this fold never saw (a duplicate, or a drop from before whatever
    /// the log still holds) has no stream path to retire and must not invent one.
    #[test]
    fn a_drop_with_no_record_is_not_a_pending_retirement() {
        let fold = fold_of(vec![CatalogEvent::Dropped { id: "s404".to_string() }]);
        assert!(fold.pending_retirements().is_empty());
    }

    /// A shape id is spoken for by its `shape/*` stream, which outlives the record whenever the
    /// retirement has not landed. The fold's high-water mark therefore comes from every `Created`,
    /// not from the survivors.
    #[test]
    fn the_id_high_water_mark_survives_the_drop() {
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let fold = fold_of(vec![created, CatalogEvent::Dropped { id: "s1".to_string() }]);
        assert!(fold.recs.is_empty());
        assert_eq!(fold.max_shape_id, Some(1), "s1 is still spoken for: its stream is being retired");
    }

    /// ...and it is a MAXIMUM, so nothing in the log can lower it (the engine mints monotonically,
    /// so this is belt-and-braces rather than a case that occurs).
    #[test]
    fn the_id_high_water_mark_never_goes_backwards() {
        let at = |id: &str| {
            let mut ev = created_event("public.users");
            ev["rec"]["id"] = serde_json::json!(id);
            ev["rec"]["stream_path"] = serde_json::json!(format!("shape/{id}"));
            serde_json::from_value::<CatalogEvent>(ev).unwrap()
        };
        let fold = fold_of(vec![at("s3"), CatalogEvent::Dropped { id: "s3".to_string() }, at("s1")]);
        assert_eq!(fold.max_shape_id, Some(3));
    }

    /// The end-to-end property the fold exists for: a restart must not re-mint an id whose stream is
    /// still being retired. `Created s1` + `Dropped s1` folds to `is_empty()`, so this is also the
    /// case that proves the counter is restored BEFORE that early return.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_boot_over_a_dropped_shape_mints_the_next_id_not_that_one() {
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let fold = fold_of(vec![created, CatalogEvent::Dropped { id: "s1".to_string() }]);
        assert!(fold.is_empty(), "nothing to install — only an id that must not come back");
        // No durable-streams behind it: the queued retirement retries in the background and this
        // test does not depend on it (the id counter is engine state, decided before any IO).
        let engine = Engine::new_for_in_process_test(DsClient::new_for_in_process_test("http://127.0.0.1:1"));
        engine.restore_reads_paused.store(true, std::sync::atomic::Ordering::Release);
        engine.apply_catalog(fold, &HashMap::new(), RestoreMode::Resume).await.unwrap();
        assert_eq!(engine.state.lock().await.next_shape_id, 2, "s1 is taken; the next shape is s2");
        assert!(!engine.restore_reads_paused.load(std::sync::atomic::Ordering::Acquire));
    }

    /// The same over a broken epoch: `Park` restores records to be retired, and it must not hand the
    /// reset a fresh create colliding with one of them either.
    #[tokio::test(flavor = "multi_thread")]
    async fn parking_a_broken_epoch_also_moves_the_id_counter() {
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let fold = fold_of(vec![created, CatalogEvent::Offset { pos: pos(0, "10"), highwater: None }]);
        let engine = Engine::new_for_in_process_test(DsClient::new_for_in_process_test("http://127.0.0.1:1"));
        engine.restore_reads_paused.store(true, std::sync::atomic::Ordering::Release);
        engine.apply_catalog(fold, &HashMap::new(), RestoreMode::Park).await.unwrap();
        let st = engine.state.lock().await;
        assert_eq!(st.next_shape_id, 2);
        assert_eq!(st.shapes.len(), 1, "the record is parked for the reset to retire");
        drop(st);
        assert!(!engine.restore_reads_paused.load(std::sync::atomic::Ordering::Acquire));
    }

    // --- subscriptions: identity, idempotence, leases (ADR-0008) --------------------------------

    /// The whole of the fold's subscription semantics: a create opens the set, a join adds to it, a
    /// REPEATED join is a renewal (one member, later lease), and a `Left` removes exactly one id —
    /// a second one for the same id changing nothing.
    #[test]
    fn subscriptions_fold_as_a_set_not_a_count() {
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let fold = fold_of(vec![
            created,
            joined("s1", "sub-b", 200),
            joined("s1", "sub-b", 300), // a renewal, not a second subscriber
            joined("s1", "sub-c", 400),
            left("s1", "sub-b"),
            left("s1", "sub-b"),   // the client retried its DELETE; nothing more to release
            left("s1", "sub-zzz"), // never held here at all
        ]);
        assert_eq!(subs_of(&fold, "s1"), vec!["sub-a".to_string(), "sub-c".to_string()]);

        // The renewal moved the lease, which is the only thing a repeated join may do.
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let fold = fold_of(vec![created, joined("s1", "sub-a", 900)]);
        assert_eq!(fold.recs["s1"].2.get("sub-a"), Some(&900), "the lease is restored at its LAST renewal");
    }

    /// A lapse is recorded as an ordinary `Left` with a marker: the fold treats it identically (the
    /// subscription is released either way), and the flag is there so the durable record explains a
    /// subscription that nobody deleted.
    #[test]
    fn a_lapsed_lease_folds_exactly_like_an_explicit_release() {
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let lapse = CatalogEvent::Left { id: "s1".to_string(), subscription: "sub-a".to_string(), lapsed: true };
        let json = serde_json::to_value(&lapse).unwrap();
        assert_eq!(json["lapsed"], true);
        assert_eq!(json["subscription"], "sub-a");
        // An ordinary release does not carry the flag at all (the wire form stays minimal).
        assert_eq!(serde_json::to_value(left("s1", "sub-a")).unwrap().get("lapsed"), None);

        let fold = fold_of(vec![created, lapse]);
        assert!(subs_of(&fold, "s1").is_empty(), "a lapsed subscription is released like any other");
        assert!(fold.recs.contains_key("s1"), "...and the shape itself stays: retention decides its fate");
    }

    /// The regression the `eid` exists for: the SAME event appended twice (a retry after a lost
    /// response) must be applied once. Without it, the second copy of a `Left` releases a
    /// subscription the caller never released — the double-decrement that stole another
    /// subscriber's claim.
    #[test]
    fn a_repeated_append_of_one_event_is_applied_once() {
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let fold = fold_of_with_eids(vec![
            ("e0".to_string(), created),
            ("e1".to_string(), joined("s1", "sub-b", 200)),
            ("e1".to_string(), joined("s1", "sub-b", 200)), // the writer's retry-in-place
        ]);
        assert_eq!(subs_of(&fold, "s1"), vec!["sub-a".to_string(), "sub-b".to_string()]);

        // ...and the case that used to corrupt the count: one release, appended twice.
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let fold = fold_of_with_eids(vec![
            ("e0".to_string(), created),
            ("e1".to_string(), joined("s1", "sub-b", 200)),
            ("e2".to_string(), left("s1", "sub-a")),
            ("e2".to_string(), left("s1", "sub-a")),
        ]);
        assert_eq!(subs_of(&fold, "s1"), vec!["sub-b".to_string()], "one release is one release");
    }

    /// A catalog written before ADR-0008 cannot be folded — the fold could neither de-duplicate a
    /// retried append nor rebuild the live set — so the boot refuses it by name rather than
    /// silently restoring something else.
    #[test]
    fn a_pre_subscription_catalog_is_refused_by_name() {
        let no_eid = serde_json::json!({ "t": "joined", "id": "s1", "subscription": "sub-a", "at": 1 });
        let detail = predates_subscriptions(&no_eid).expect("an event with no eid is recognised");
        assert!(detail.contains("no eid"), "{detail}");
        assert!(
            CatalogPredatesSubscriptions { detail }.to_string().contains("predates ADR-0008"),
            "the operator is told which cutover the catalog is on the wrong side of"
        );

        let no_sub = serde_json::json!({ "t": "left", "id": "s1", "eid": "e1" });
        assert!(predates_subscriptions(&no_sub).unwrap().contains("no subscription"));
        let no_at = serde_json::json!({ "t": "joined", "id": "s1", "subscription": "sub-a", "eid": "e1" });
        assert!(predates_subscriptions(&no_at).unwrap().contains("no lease timestamp"));

        // The current spellings pass, and an event that has no subscription of its own (an offset,
        // a rotation) needs only its eid.
        let now = serde_json::json!({ "t": "joined", "id": "s1", "subscription": "sub-a", "at": 1, "eid": "e1" });
        assert!(predates_subscriptions(&now).is_none());
        let offset = serde_json::json!({ "t": "offset", "pos": { "segment": 0, "offset": "1" }, "eid": "e2" });
        assert!(predates_subscriptions(&offset).is_none());
    }

    /// End to end through the real writer and a real storage response: an append that COMMITS and
    /// then loses its response is retried, so the log holds the event twice — with one `eid`, which
    /// is what makes the fold apply it once.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_lost_response_leaves_two_records_with_one_event_id() {
        let server = FakeDs::start().await;
        let w = spawn_catalog_writer(
            DsClient::new_for_in_process_test(server.url()),
            crate::shutdown::ShutdownToken::new(),
        );
        server.lose_responses(1);
        w.send(joined("s1", "sub-b", 200));
        assert!(w.drain(std::time::Duration::from_secs(20)).await, "the queue must drain");

        let events = server.catalog_events();
        assert_eq!(events.len(), 2, "the retry appended a second copy: {events:?}");
        let eids: Vec<&str> = events.iter().map(|e| e["eid"].as_str().unwrap_or("")).collect();
        assert_eq!(eids[0], eids[1], "both copies are the same event, and say so");
        assert!(!eids[0].is_empty(), "every event carries an eid");

        // Fold them exactly as a boot would: one join.
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let mut fold = fold_of(vec![created]);
        for ev in events {
            let eid = ev["eid"].as_str().unwrap().to_string();
            fold.apply_once(&eid, serde_json::from_value::<CatalogEvent>(ev).unwrap());
        }
        assert_eq!(subs_of(&fold, "s1"), vec!["sub-a".to_string(), "sub-b".to_string()]);
    }

    /// Two DIFFERENT events must never share an id (the dedup is per append attempt, not per
    /// payload): two identical-looking joins from two callers are two claims.
    #[tokio::test(flavor = "multi_thread")]
    async fn every_enqueued_event_gets_its_own_id() {
        let server = FakeDs::start().await;
        let w = spawn_catalog_writer(
            DsClient::new_for_in_process_test(server.url()),
            crate::shutdown::ShutdownToken::new(),
        );
        w.send(joined("s1", "sub-b", 200));
        w.send(joined("s1", "sub-c", 200));
        assert!(w.drain(std::time::Duration::from_secs(20)).await);
        let events = server.catalog_events();
        assert_ne!(events[0]["eid"], events[1]["eid"]);
    }

    /// The restore reads the live set back out of the log, with its lease ages — the two halves the
    /// engine needs to answer "who is subscribed, and when did they last say so".
    #[tokio::test(flavor = "multi_thread")]
    async fn a_restored_shape_keeps_its_subscriptions_and_their_lease_ages() {
        let mut created = created_event("public.users");
        created["sig"] = serde_json::json!("sig-1");
        created["at"] = serde_json::json!(1000);
        let fold = fold_of(vec![
            serde_json::from_value::<CatalogEvent>(created).unwrap(),
            joined("s1", "sub-b", 2000),
            // Restored as DORMANT, which is the one restore path that installs the shape without
            // replaying it — the point here is the live set, not the resume.
            CatalogEvent::Dormant {
                id: "s1".to_string(),
                resume: pos(0, "5"),
                gate: crate::pg::SnapshotGate::passthrough(),
            },
            CatalogEvent::Offset { pos: pos(0, "10"), highwater: None },
        ]);
        let server = FakeDs::start().await;
        let engine = Engine::new_for_in_process_test(DsClient::new_for_in_process_test(server.url()));
        engine.apply_catalog(fold, &HashMap::new(), RestoreMode::Resume).await.unwrap();
        let st = engine.state.lock().await;
        let share = st.feed_shares.get("s1").expect("the restored share entry");
        assert_eq!(share.subs.get("sub-a"), Some(&1000), "the creator's lease age survives the restart");
        assert_eq!(share.subs.get("sub-b"), Some(&2000));
        assert_eq!(share.refcount(), 2);
        assert_eq!(st.subscription_owner("sub-b"), Some(&"s1".to_string()), "the id index is restored too");
    }

    /// A shape that cannot be re-registered is a restore failure, not an empty-catalog success.
    /// The Postgres boot boundary must see this error before it starts serving, so it can apply
    /// the ordinary retry/fatal boot classification instead of resuming from missing state.
    #[tokio::test(flavor = "multi_thread")]
    async fn restore_failure_is_returned_to_the_boot_boundary() {
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let fold = fold_of(vec![created]);
        let server = FakeDs::start().await;
        let engine = Engine::new_for_in_process_test(DsClient::new_for_in_process_test(server.url()));

        let err = engine
            .apply_catalog(fold, &HashMap::new(), RestoreMode::Resume)
            .await
            .expect_err("a shape that cannot resume must fail catalog restore");
        assert!(format!("{err:#}").contains("public.users"), "restore failure keeps shape context: {err:#}");
    }

    /// A missing retained stream is a definitive storage answer: only that shape is retired and
    /// the rest of the catalog restores with its sharing state intact.
    #[tokio::test(flavor = "multi_thread")]
    async fn restore_retires_a_missing_shape_stream_and_restores_the_others() {
        fn dormant(id: &str, path: &str, table: &str) -> Vec<CatalogEvent> {
            let mut created = created_event(table);
            created["rec"]["id"] = serde_json::json!(id);
            created["rec"]["stream_path"] = serde_json::json!(path);
            created["sig"] = serde_json::json!(format!("sig-{id}"));
            let created = serde_json::from_value::<CatalogEvent>(created).unwrap();
            let gate = crate::pg::SnapshotGate::passthrough();
            vec![
                created,
                CatalogEvent::Joined { id: id.to_string(), subscription: format!("sub-{id}"), at: 200 },
                CatalogEvent::Dormant { id: id.to_string(), resume: pos(0, "5"), gate },
            ]
        }

        let missing_server = FakeDs::start().await;
        missing_server.mark_stream_missing("shape/s2");
        let fold = fold_of(
            dormant("s1", "shape/s1", "public.users")
                .into_iter()
                .chain(dormant("s2", "shape/s2", "public.accounts"))
                .chain(dormant("s3", "shape/s3", "public.events"))
                .chain([CatalogEvent::Offset { pos: pos(0, "10"), highwater: None }])
                .collect(),
        );
        let engine = Engine::new_for_in_process_test(DsClient::new_for_in_process_test(missing_server.url()));
        engine
            .apply_catalog(fold, &HashMap::new(), RestoreMode::Resume)
            .await
            .expect("a missing stream is retired while the rest of the catalog restores");
        let st = engine.state.lock().await;
        assert_eq!(st.shapes.len(), 2);
        assert!(st.shapes.contains_key("s1") && st.shapes.contains_key("s3"));
        assert_eq!(st.feed_shares.len(), 2, "the surviving shapes retain their sharing entries");
        assert_eq!(st.feed_shares["s1"].subs.get("sub-s1"), Some(&200));
        assert_eq!(st.feed_shares["s3"].subs.get("sub-s3"), Some(&200));
        assert!(!st.shapes.contains_key("s2"), "the missing shape is not installed");
        drop(st);
        assert!(engine.catalog_tx.drain(std::time::Duration::from_secs(20)).await);
        let events = missing_server.catalog_events();
        assert!(events.iter().any(|event| event["t"] == "dropped" && event["id"] == "s2"), "{events:?}");
        assert!(events.iter().any(|event| event["t"] == "retired" && event["id"] == "s2"), "{events:?}");
        assert_eq!(missing_server.deletes(), 1, "exactly the absent stream takes the (404-tolerant) delete path");
    }

    /// A closed retained stream is the same definitive storage answer as a missing one: retire
    /// just that shape and continue restoring the others.
    #[tokio::test(flavor = "multi_thread")]
    async fn restore_retires_a_closed_shape_stream_and_restores_the_others() {
        fn dormant(id: &str, table: &str) -> Vec<CatalogEvent> {
            let mut created = created_event(table);
            created["rec"]["id"] = serde_json::json!(id);
            created["rec"]["stream_path"] = serde_json::json!(format!("shape/{id}"));
            created["sig"] = serde_json::json!(format!("sig-{id}"));
            let created = serde_json::from_value::<CatalogEvent>(created).unwrap();
            vec![
                created,
                CatalogEvent::Joined { id: id.to_string(), subscription: format!("sub-{id}"), at: 200 },
                CatalogEvent::Dormant {
                    id: id.to_string(),
                    resume: pos(0, "5"),
                    gate: crate::pg::SnapshotGate::passthrough(),
                },
            ]
        }

        let closed_server = FakeDs::start().await;
        closed_server.mark_stream_closed("shape/s2");
        let closed_fold = fold_of(
            dormant("s1", "public.users")
                .into_iter()
                .chain(dormant("s2", "public.accounts"))
                .chain(dormant("s3", "public.events"))
                .chain([CatalogEvent::Offset { pos: pos(0, "10"), highwater: None }])
                .collect(),
        );
        let closed_engine = Engine::new_for_in_process_test(DsClient::new_for_in_process_test(closed_server.url()));
        closed_engine
            .apply_catalog(closed_fold, &HashMap::new(), RestoreMode::Resume)
            .await
            .expect("a closed stream is retired while the rest of the catalog restores");
        let st = closed_engine.state.lock().await;
        assert_eq!(st.shapes.len(), 2);
        assert!(st.shapes.contains_key("s1") && st.shapes.contains_key("s3"));
        assert_eq!(st.feed_shares.len(), 2);
        assert!(!st.shapes.contains_key("s2"));
        drop(st);
        assert!(closed_engine.catalog_tx.drain(std::time::Duration::from_secs(20)).await);
        let events = closed_server.catalog_events();
        assert!(events.iter().any(|event| event["t"] == "dropped" && event["id"] == "s2"), "{events:?}");
        assert!(events.iter().any(|event| event["t"] == "retired" && event["id"] == "s2"), "{events:?}");
        assert_eq!(closed_server.deletes(), 1, "exactly the closed stream is deleted");
    }

    /// A connection error while preflighting a stream is not a definitive missing/closed answer
    /// and therefore still aborts restore before any record is installed.
    #[tokio::test(flavor = "multi_thread")]
    async fn restore_transport_failure_is_returned_before_installation() {
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let fold = fold_of(vec![
            created,
            CatalogEvent::Dormant {
                id: "s1".to_string(),
                resume: pos(0, "5"),
                gate: crate::pg::SnapshotGate::passthrough(),
            },
        ]);
        let engine = Engine::new_for_in_process_test(DsClient::new_for_in_process_test("http://127.0.0.1:1"));
        let err = engine
            .apply_catalog(fold, &HashMap::new(), RestoreMode::Resume)
            .await
            .expect_err("a stream HEAD connection error must abort restore");
        assert!(format!("{err:#}").contains("checking shape s1 stream"));
        assert!(engine.state.lock().await.shapes.is_empty());
    }

    /// This is the production SIGKILL window: the catalog has a Created event, but the stream PUT
    /// never happened. Restore retires the orphan and still advances the id allocator past s7.
    #[tokio::test(flavor = "multi_thread")]
    async fn restore_retires_a_shape_left_by_the_create_boot_window() {
        let mut created = created_event("public.calendar_event");
        created["rec"]["id"] = serde_json::json!("s7");
        created["rec"]["stream_path"] = serde_json::json!("shape/s7");
        let created = serde_json::from_value::<CatalogEvent>(created).unwrap();
        let fold = fold_of(vec![created]);
        let server = FakeDs::start().await;
        server.mark_stream_missing("shape/s7");
        let engine = Engine::new_for_in_process_test(DsClient::new_for_in_process_test(server.url()));
        engine
            .apply_catalog(fold, &HashMap::new(), RestoreMode::Resume)
            .await
            .expect("the boot-window orphan is repaired by retirement");
        assert_eq!(engine.state.lock().await.next_shape_id, 8);
        assert!(engine.catalog_tx.drain(std::time::Duration::from_secs(20)).await);
        let events = server.catalog_events();
        assert!(events.iter().any(|event| event["t"] == "dropped" && event["id"] == "s7"), "{events:?}");
        assert!(events.iter().any(|event| event["t"] == "retired" && event["id"] == "s7"), "{events:?}");
    }

    /// Once Dropped and Retired are both in the durable catalog, a second boot has no pending
    /// retirement and does not probe or delete the stream again.
    #[tokio::test(flavor = "multi_thread")]
    async fn restore_is_idempotent_after_a_boot_retirement_pair() {
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let fold = fold_of(vec![
            created,
            CatalogEvent::Dropped { id: "s1".into() },
            CatalogEvent::Retired { id: "s1".into() },
        ]);
        let server = FakeDs::start().await;
        let engine = Engine::new_for_in_process_test(DsClient::new_for_in_process_test(server.url()));
        engine.apply_catalog(fold, &HashMap::new(), RestoreMode::Resume).await.unwrap();
        assert!(engine.catalog_tx.drain(std::time::Duration::from_secs(20)).await);
        assert_eq!(server.deletes(), 0);
        assert!(server.catalog_events().is_empty());
    }

    /// Every definitive missing answer is handled independently; there is no count threshold that
    /// turns a large orphan set back into a boot refusal.
    #[tokio::test(flavor = "multi_thread")]
    async fn restore_retires_three_missing_shape_streams_without_a_threshold() {
        let events = (1..=3)
            .flat_map(|n| {
                let mut created = created_event("public.users");
                created["rec"]["id"] = serde_json::json!(format!("s{n}"));
                created["rec"]["stream_path"] = serde_json::json!(format!("shape/s{n}"));
                let created = serde_json::from_value::<CatalogEvent>(created).unwrap();
                [
                    created,
                    CatalogEvent::Dormant {
                        id: format!("s{n}"),
                        resume: pos(0, "5"),
                        gate: crate::pg::SnapshotGate::passthrough(),
                    },
                ]
            })
            .collect::<Vec<_>>();
        let fold = fold_of(events);
        let server = FakeDs::start().await;
        for n in 1..=3 {
            server.mark_stream_missing(&format!("shape/s{n}"));
        }
        let engine = Engine::new_for_in_process_test(DsClient::new_for_in_process_test(server.url()));
        engine.apply_catalog(fold, &HashMap::new(), RestoreMode::Resume).await.unwrap();
        assert!(engine.state.lock().await.shapes.is_empty());
        assert!(engine.catalog_tx.drain(std::time::Duration::from_secs(20)).await);
        let catalog_events = server.catalog_events();
        assert_eq!(catalog_events.iter().filter(|event| event["t"] == "dropped").count(), 3);
        assert_eq!(catalog_events.iter().filter(|event| event["t"] == "retired").count(), 3);
        assert_eq!(server.deletes(), 3);
    }

    /// Even a dormant-only catalog must fail closed when the existing sequencer command receiver
    /// is gone: ResumeReads is a readiness handshake, not a best-effort hint.
    #[tokio::test(flavor = "multi_thread")]
    async fn dormant_resume_with_dead_sequencer_is_a_restore_error() {
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let fold = fold_of(vec![
            created,
            CatalogEvent::Dormant {
                id: "s1".to_string(),
                resume: pos(0, "5"),
                gate: crate::pg::SnapshotGate::passthrough(),
            },
            CatalogEvent::Offset { pos: pos(0, "10"), highwater: None },
        ]);
        let server = FakeDs::start().await;
        let engine = Engine::new_for_in_process_test(DsClient::new_for_in_process_test(server.url()));
        let cmd = {
            let mut st = engine.state.lock().await;
            engine.ensure_sequencer(&mut st).cmd_tx.clone()
        };
        engine.shutdown_token().begin();
        tokio::time::timeout(std::time::Duration::from_secs(2), cmd.closed())
            .await
            .expect("sequencer command receiver must close");

        let err = engine
            .apply_catalog(fold, &HashMap::new(), RestoreMode::Resume)
            .await
            .expect_err("a dead sequencer cannot acknowledge ResumeReads");
        assert!(format!("{err:#}").contains("sequencer"), "handshake failure is named: {err:#}");
        assert!(engine.restore_reads_paused.load(std::sync::atomic::Ordering::Acquire));
    }

    /// If shape A has resumed and shape B then fails, the rollback barrier removes both registrations
    /// before the retry. The durable records remain intact for the next Resume attempt.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_second_resume_failure_rolls_back_the_first_shape_too() {
        let mut first = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let mut second = created_event("public.accounts");
        second["rec"]["id"] = serde_json::json!("s2");
        second["rec"]["stream_path"] = serde_json::json!("shape/s2");
        let second = serde_json::from_value::<CatalogEvent>(second).unwrap();
        let first_rec = match &mut first {
            CatalogEvent::Created { rec, .. } => rec.clone(),
            _ => unreachable!(),
        };
        let second_rec = match second {
            CatalogEvent::Created { rec, .. } => rec,
            _ => unreachable!(),
        };
        let engine = Engine::new_for_in_process_test(DsClient::new_for_in_process_test("http://127.0.0.1:1"));
        {
            let mut st = engine.state.lock().await;
            st.shapes.insert(first_rec.id.clone(), first_rec.clone());
            st.shapes.insert(second_rec.id.clone(), second_rec.clone());
            st.circuit_placement
                .insert(first_rec.id.clone(), CircuitPlacement { label: "all".into(), col: None, counts: false });
            st.circuit_placement
                .insert(second_rec.id.clone(), CircuitPlacement { label: "all".into(), col: None, counts: false });
            engine.ensure_sequencer(&mut st);
        }
        let cmd_tx = engine.state.lock().await.sequencer.as_ref().unwrap().cmd_tx.clone();
        let ids = vec!["s1".to_string(), "s2".to_string()];

        // Model A's successful resume followed by B's failure, then invoke the same rollback
        // barrier the production Resume path uses.
        engine.rollback_catalog_restore(&ids, &cmd_tx).await;
        let st = engine.state.lock().await;
        assert!(st.shapes.is_empty(), "both restored records are removed from memory");
        assert!(st.circuit_placement.is_empty(), "partial circuit placement is removed");
        assert!(engine.lives.lock().unwrap().is_empty(), "lifecycle state is removed");
        drop(st);
        assert!(engine.table_stats(&first_rec.table).await.is_none(), "sequencer routing is fenced and empty");
        assert!(engine.table_stats(&second_rec.table).await.is_none(), "second table routing is fenced and empty");
    }

    /// A historical page cannot advance the processed offset while Resume registration is paused;
    /// releasing the gate then consumes the same page from the original checkpoint.
    #[tokio::test(flavor = "multi_thread")]
    async fn paused_resume_reader_does_not_advance_or_checkpoint_history() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let reads = Arc::new(AtomicUsize::new(0));
        let notify = Arc::new(tokio::sync::Notify::new());
        let app = {
            let reads = reads.clone();
            let notify = notify.clone();
            axum::Router::new()
                .route(
                    "/{*path}",
                    axum::routing::get(move |axum::extract::Path(path): axum::extract::Path<String>| {
                        let reads = reads.clone();
                        let notify = notify.clone();
                        async move {
                            if path.ends_with("/changes/0") {
                                reads.fetch_add(1, Ordering::SeqCst);
                                notify.notify_waiters();
                                (axum::http::StatusCode::OK, [("stream-next-offset", "1")], "[]".to_string())
                            } else {
                                (axum::http::StatusCode::NO_CONTENT, [("stream-next-offset", "-1")], String::new())
                            }
                        }
                    })
                    .post(|| async { axum::http::StatusCode::NO_CONTENT }),
                )
                .with_state(())
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let engine = Engine::new_for_in_process_test(DsClient::new_for_in_process_test(format!("http://{addr}")));
        engine.restore_reads_paused.store(true, Ordering::Release);
        let cmd = {
            let mut st = engine.state.lock().await;
            engine.ensure_sequencer(&mut st).cmd_tx.clone()
        };
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(reads.load(Ordering::SeqCst), 0, "paused Resume does not read history");
        assert_eq!(engine.changes_position(), LogPosition::start());
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let _ = cmd.send(SequencerCmd::ResumeReads { done: done_tx });
        engine.restore_reads_paused.store(false, Ordering::Release);
        let _ = done_rx.await;
        tokio::time::timeout(std::time::Duration::from_secs(2), notify.notified()).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let offset = engine.table_offset(&TableRef::parse("public.users").unwrap()).await.unwrap();
        assert_eq!(offset.offset, "1", "release consumes from the original checkpoint");
    }

    #[test]
    fn retired_round_trips_on_the_wire() {
        let json = serde_json::to_value(CatalogEvent::Retired { id: "s3".to_string() }).unwrap();
        assert_eq!(json["t"], "retired");
        assert_eq!(json["id"], "s3");
        match serde_json::from_value::<CatalogEvent>(json).unwrap() {
            CatalogEvent::Retired { id } => assert_eq!(id, "s3"),
            other => panic!("expected Retired, got {other:?}"),
        }
    }
}
