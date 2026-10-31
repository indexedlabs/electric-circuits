//! The LSN-ordered sequencer: command protocol, per-table executors, the main loop,
//! envelope processing, activation/backfill, and the reliable flush.

use super::*;

/// Handle to the engine's single **sequencer** task — the LSN-ordered executor consuming the
/// global `changes` stream (Electric's `ShapeLogCollector` pattern): one task processes every
/// table's changes in commit order and flushes each transaction's shape appends before the next
/// transaction, restoring per-transaction atomic emission across tables.
pub(crate) struct SequencerHandle {
    pub(crate) cmd_tx: mpsc::UnboundedSender<SequencerCmd>,
    /// Change-log position up to which every envelope has been processed AND fanned to every shape
    /// (appends landed) — `(segment, offset)`, since the log is segmented (ADR-0006). A harness
    /// polls this against the current segment's tail as the convergence barrier.
    pub(crate) processed: Arc<std::sync::Mutex<LogPosition>>,
    /// Per-table circuit topology (shared families + standalone count), for tests/observability.
    pub(crate) stats: Arc<std::sync::Mutex<HashMap<TableRef, TableStats>>>,
    /// Live per-node state summaries, merged across all tables, keyed by graph node id.
    /// Republished after every processed batch and on shape add/remove; read by `GET /state`.
    pub(crate) node_states: Arc<std::sync::Mutex<HashMap<String, NodeStateSummary>>>,
}

struct ReceiptWork {
    marker_lsn: u64,
    commit_lsn: Option<String>,
    source_receipt: Option<SourceDrainReceipt>,
    runtime_fences: Vec<crate::runtime_authority::RuntimeFence>,
    runtime_incarnation: uuid::Uuid,
}

/// Receipt work is small (runtime fences are bounded per transaction), but it must not be allowed
/// to park the source sequencer behind a group whose output is waiting on the same sequencer. The
/// ingress is therefore non-blocking. Receipt work remains pending until the group's lane lands
/// or the lane's cap retires it; neither a source fence nor a runtime marker is dropped because a
/// different group's output is temporarily holding the replay boundary.

#[derive(Default)]
struct ReceiptBarrier {
    pending: std::sync::Mutex<BTreeMap<u64, usize>>,
    wake: tokio::sync::Notify,
}

impl ReceiptBarrier {
    fn register(&self, marker: u64) {
        *self.pending.lock().unwrap().entry(marker).or_default() += 1;
    }

    fn complete(&self, marker: u64) {
        let mut pending = self.pending.lock().unwrap();
        if let Some(count) = pending.get_mut(&marker) {
            *count -= 1;
            if *count == 0 {
                pending.remove(&marker);
            }
        }
        drop(pending);
        self.wake.notify_waiters();
    }

    fn has_at_or_before(&self, marker: u64) -> bool {
        self.pending.lock().unwrap().keys().any(|value| *value <= marker)
    }

    async fn wait_until(&self, marker: u64, shutdown: &crate::shutdown::ShutdownToken) {
        loop {
            if !self.has_at_or_before(marker) || shutdown.is_shutting_down() {
                return;
            }
            let notified = self.wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.has_at_or_before(marker) || shutdown.is_shutting_down() {
                return;
            }
            tokio::select! {
                _ = notified => {}
                _ = shutdown.wait() => return,
            }
        }
    }
}

pub(crate) enum SequencerCmd {
    /// Phase 1 of shape creation: register a PENDING shape that buffers this table's deltas while
    /// the creator runs the Postgres backfill concurrently — the sequencer itself never blocks on
    /// Postgres, so one slow backfill cannot stall the whole change pipeline. Buffer registration
    /// is acknowledged BEFORE the creator takes its snapshot, so no change can fall between the
    /// snapshot and activation.
    BeginShape {
        table: TableRef,
        shape_id: String,
        num_id: u64,
        stream_path: String,
        pred: Arc<CompiledPredicate>,
        /// Output projection (column indices to emit), or `None` for the full row.
        out_cols: Option<Arc<Vec<usize>>>,
        kind: CreateKind,
        /// Acknowledges the buffer registration, carrying the sequencer's PUBLISHED change-log
        /// position at that instant — the exact point the pending buffer starts from. A
        /// reactivation replay fences its scan there: earlier envelopes are the replay's to
        /// deliver, later ones are the buffer's, and the two meet with no gap. Any fence captured
        /// before this ack (the ingestor's tail when the touch was accepted, say) leaves the
        /// envelopes processed in between to neither.
        ack: tokio::sync::oneshot::Sender<LogPosition>,
    },
    /// Phase 2: the creator's backfill snapshot has been appended chunk by chunk (plain) or folded
    /// into `agg_seed` (aggregates); drain the buffered deltas through the shape's snapshot gate
    /// and go live.
    /// `ready` mirrors the old add-shape handshake: `Ok(())` once the shape is live and its
    /// snapshot + gated buffer are on the stream, `Err(reason)` otherwise.
    ActivateShape {
        table: TableRef,
        shape_id: String,
        gate: crate::pg::SnapshotGate,
        /// An aggregate's fold, already seeded by the creator from the streamed backfill (`None`
        /// for plain shapes — the creator appended their snapshot envelopes chunk by chunk — and
        /// for a reactivation, whose state comes from the change-log replay).
        agg_seed: Option<AggSeed>,
        /// Snapshot envelopes the creator appended (seeds the shape's emit counter).
        emitted_seed: u64,
        ready: tokio::sync::oneshot::Sender<std::result::Result<(), ActivateFailure>>,
    },
    /// Creation failed after `BeginShape`: drop the pending buffer.
    AbortShape {
        table: TableRef,
        shape_id: String,
    },
    /// Retention: unregister a plain row shape's routing and hand back its resume state — the
    /// sequencer's fully-processed change-log position (the batch preceding this command was fully
    /// fanned out + flushed, so the shape's stream is complete up to here) and the shape's
    /// backfill-snapshot gate. `None` if the shape is unknown (or an aggregate — not parkable).
    DeactivateShape {
        table: TableRef,
        shape_id: String,
        resp: tokio::sync::oneshot::Sender<Option<(LogPosition, crate::pg::SnapshotGate)>>,
    },
    RemoveShape {
        table: TableRef,
        shape_id: String,
    },
    /// Restore rollback barrier: acknowledge after all preceding shape removals have been applied.
    Barrier {
        done: tokio::sync::oneshot::Sender<()>,
    },
    /// Release the boot replay gate after every catalog shape has resumed successfully.
    ResumeReads {
        done: tokio::sync::oneshot::Sender<()>,
    },
    /// Pause the change-log reader before a retrying Resume attempt.
    PauseReads {
        done: tokio::sync::oneshot::Sender<()>,
    },
    /// Schema drift (ADR-0005): forget everything the sequencer holds for this table. The executor
    /// is keyed by the OLD `TableSchema`, so it is dropped outright rather than patched; the next
    /// envelope for the table lazily rebuilds it from the (already swapped) shared schema view.
    /// Every shape it was routing has been retired by the same handler, so there is nothing to
    /// preserve.
    ResetTable {
        table: TableRef,
    },
    /// Create a **circuit-served** COUNT aggregate over the table's counts pipeline: seeded by
    /// summing matching groups, then updated from the pipeline's per-transaction group deltas.
    CreateCircuitAgg {
        table: TableRef,
        shape_id: String,
        stream_path: String,
        constraints: Vec<Option<std::collections::HashSet<Value>>>,
        ready: tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
    },
    /// Dump the full internal state of one node (`family:<t>:<cols>` → the routing index
    /// contents; an aggregate `shape:<sid>` → the fold internals incl. the MIN/MAX multiset).
    /// `None` if the node id is unknown. Serves `GET /state/node`.
    DumpNode {
        table: TableRef,
        node_id: String,
        resp: tokio::sync::oneshot::Sender<Option<serde_json::Value>>,
    },
    /// On-demand owned-heap byte-walk of every table's live executor state (see
    /// `introspection::exec_heap_bytes`) — the memory probe's `bytes_executors` term. Sent only
    /// from `Engine::mem_bytes` (called by `GET /memory` or the slower diagnostic logger — never
    /// the 500ms background sampler, which calls the cheap `Engine::mem_cardinalities` instead), never from the
    /// per-batch write path, so the walk's cost never lands on ingestion or on the sampler.
    MemBytes {
        resp: tokio::sync::oneshot::Sender<usize>,
    },
}

/// What kind of shape a pending creation becomes at activation.
#[derive(Clone)]
pub(crate) enum CreateKind {
    Plain,
    Aggregate { func: AggFn, col: Option<usize> },
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_sequencer(
    ds: DsClient,
    tables: SharedTables,
    start: LogPosition,
    restore_highwater: Option<(u64, u64)>,
    catalog_tx: CatalogWriter,
    source_receipts: Arc<std::sync::Mutex<HashMap<String, SourceDrainReceipt>>>,
    last_source_receipt: Arc<std::sync::Mutex<Option<SourceDrainReceipt>>>,
    source_receipt_progress: Arc<std::sync::Mutex<SourceReceiptProgress>>,
    runtime_receipts: Arc<std::sync::Mutex<crate::runtime_authority::RuntimeReceipts>>,
    subq: SubqueryHandle,
    trace_tx: tokio::sync::broadcast::Sender<Arc<String>>,
    arr: Option<crate::arrangements::Arrangements>,
    arr_gates: HashMap<TableRef, crate::pg::SnapshotGate>,
    // No Postgres behind the engine: writes arrive on the change log through the native write API
    // and carry no replication old-image, so the sequencer keeps the current row per key itself
    // (see `TableExec::library_rows`).
    library_mode: bool,
    start_paused: bool,
    pause_gate: Arc<std::sync::atomic::AtomicBool>,
    read_failure_latch: Arc<std::sync::atomic::AtomicBool>,
    // Per-shape ceiling for the pending-creation buffer (see `buffer_pending`).
    pending_buffer_max_bytes: u64,
    txn_cfg: crate::txn_buffer::TxnBufferConfig,
    // Postgres mode: where each transaction's xid is noted before it is fanned out, so a backfill or
    // subset snapshot that still excludes it can be retaken (`pg::SequencedXids`).
    sequenced: Option<Arc<crate::pg::SequencedXids>>,
    shutdown: crate::shutdown::ShutdownToken,
) -> SequencerHandle {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let processed = Arc::new(std::sync::Mutex::new(start.clone()));
    let stats = Arc::new(std::sync::Mutex::new(HashMap::new()));
    let node_states = Arc::new(std::sync::Mutex::new(HashMap::new()));
    // Registered before the spawn: the shutdown wait must never see "no parties" in the window
    // between deciding to run a sequencer and the task existing.
    let party = shutdown.party("sequencer");
    shutdown.clone().spawn(sequencer_loop(
        ds,
        tables,
        start,
        restore_highwater,
        catalog_tx,
        source_receipts,
        last_source_receipt,
        source_receipt_progress,
        runtime_receipts,
        cmd_rx,
        processed.clone(),
        stats.clone(),
        node_states.clone(),
        subq,
        trace_tx,
        arr,
        arr_gates,
        library_mode,
        start_paused,
        pause_gate,
        read_failure_latch,
        pending_buffer_max_bytes,
        txn_cfg,
        sequenced,
        shutdown,
        party,
    ));
    SequencerHandle { cmd_tx, processed, stats, node_states }
}

/// Rebuild + publish the merged node-state map and per-table stats to the sequencer's shared
/// handles and, when anyone is subscribed to `/trace`, broadcast the merged map (plus the
/// subquery registry's summaries) as a `{"type":"state"}` event.
pub(crate) async fn publish_all(
    execs: &HashMap<String, TableExec>,
    offset: &str,
    emitted: &HashMap<String, u64>,
    stats: &std::sync::Mutex<HashMap<TableRef, TableStats>>,
    node_states: &std::sync::Mutex<HashMap<String, NodeStateSummary>>,
    subqueries: &Arc<Mutex<SubqueryRegistry>>,
    trace_tx: &tokio::sync::broadcast::Sender<Arc<String>>,
) {
    let mut stats_map = HashMap::new();
    let mut merged: HashMap<String, NodeStateSummary> = HashMap::new();
    for exec in execs.values() {
        stats_map.insert(exec.ts.table.clone(), stats_of(exec));
        merged.extend(build_node_states(
            &exec.ts,
            offset,
            exec.envelopes_total,
            &exec.shapes,
            &exec.families,
            &exec.family_of,
            &exec.aggregates,
            &exec.circuit_aggs,
            emitted,
        ));
    }
    *stats.lock().unwrap() = stats_map;
    *node_states.lock().unwrap() = merged.clone();
    if trace_tx.receiver_count() == 0 {
        return;
    }
    let mut ev_nodes = merged;
    for (id, s) in subqueries.lock().await.state_summaries() {
        ev_nodes.insert(id, s);
    }
    if let Ok(json) = serde_json::to_string(&crate::trace::StateEvent::new(ev_nodes)) {
        let _ = trace_tx.send(Arc::new(json));
    }
}

/// Per-table executor state owned by the sequencer: the routing structures a table's changes fan
/// out through, plus any in-flight (pending) shape creations buffering deltas.
pub(crate) struct TableExec {
    pub(crate) ts: TableSchema,
    pub(crate) shapes: HashMap<String, StandaloneShape>,
    pub(crate) shape_index: StandaloneIndex,
    pub(crate) families: HashMap<Vec<usize>, KeyRouter>,
    pub(crate) family_of: HashMap<String, (Vec<usize>, u64, Row)>,
    pub(crate) aggregates: HashMap<String, AggShape>,
    /// Necessary-conjunct index over the aggregates' predicates (same structure as
    /// `shape_index`): per change only candidate aggregates are folded, so aggregate count
    /// stops being a linear per-change term. Match-all / un-indexable predicates land on the
    /// index's scan list and stay always-candidates.
    pub(crate) agg_index: StandaloneIndex,
    /// Circuit-served COUNT aggregates on this table (see [`CircuitAgg`]).
    pub(crate) circuit_aggs: HashMap<String, CircuitAgg>,
    pub(crate) pending: HashMap<String, PendingShape>,
    pub(crate) envelopes_total: u64,
    /// **Library mode only** — the current row per primary key, as last seen on the change log.
    ///
    /// Stays empty (and unallocated) in Postgres mode, because only [`Self::stamp_before_image`]
    /// ever writes to it and the sequencer calls that only when there is no Postgres. In Postgres
    /// mode the system of record supplies the before-image on every envelope (`REPLICA IDENTITY
    /// FULL`) — which is exactly why the engine's hot path holds no table copy. Library mode has
    /// no such source: the native write API takes `(table, op, pk, row)` and a delete/update
    /// carries no prior row, so the retraction half of every Z-set delta would be missing and a
    /// deleted row could never leave a shape.
    ///
    /// Scope: this is the sequencer's view of the change log as it consumes it, and it is EXACT
    /// from boot rather than best-effort. It is not persisted, but it does not need to be — library
    /// mode has no catalog checkpoint to resume from (`apply_catalog` runs only on the Postgres
    /// boot), so a starting process replays the log from `LogPosition::start()` and rebuilds the
    /// whole view before it serves anything. The one reader this cannot serve is
    /// the coalesced replay scanner, which reads the log at DORMANT SHAPE resume positions while
    /// the view is at the head; that path decides membership absolutely instead
    /// (`output::absolute_envelope`).
    pub(crate) library_rows: HashMap<String, serde_json::Value>,
}

impl TableExec {
    pub(crate) fn new(ts: TableSchema) -> TableExec {
        TableExec {
            ts,
            shapes: HashMap::new(),
            shape_index: StandaloneIndex::default(),
            families: HashMap::new(),
            family_of: HashMap::new(),
            aggregates: HashMap::new(),
            agg_index: StandaloneIndex::default(),
            circuit_aggs: HashMap::new(),
            pending: HashMap::new(),
            envelopes_total: 0,
            library_rows: HashMap::new(),
        }
    }

    /// Give a library-mode envelope the before-image a replicated one would already carry, and
    /// record its after-image for the next change to the same key. Called by the sequencer for
    /// every change-log envelope **when the engine runs without Postgres**, before anything else
    /// looks at it, so a library-mode change is indistinguishable from a replicated one from
    /// `apply_envelope` downwards.
    ///
    /// An envelope that already carries an `old` keeps it — a producer that knows the prior row is
    /// always believed over this cache.
    pub(crate) fn stamp_before_image(&mut self, env: &mut Envelope) {
        if env.old.is_none() {
            env.old = self.library_rows.get(&env.key).cloned();
        }
        match env.headers.operation.as_str() {
            "delete" => {
                self.library_rows.remove(&env.key);
            }
            _ => {
                // An insert/update with no row body says nothing about the key's current value;
                // dropping the remembered one would only lose the next retraction.
                if let Some(v) = env.value.as_ref() {
                    self.library_rows.insert(env.key.clone(), v.clone());
                }
            }
        }
    }
}

/// Why `ActivateShape` refused. Typed because one of the two answers is actionable in a way a
/// message is not: an overflowed pending buffer means this identity can never be brought up, and
/// the caller must recreate from a fresh backfill instead of retrying.
#[derive(Debug)]
pub(crate) enum ActivateFailure {
    /// The shape's pending buffer passed `ELECTRIC_CIRCUITS_PENDING_BUFFER_MAX_BYTES` and was
    /// dropped, so the deltas it was holding are not anywhere any more.
    PendingBufferOverflow,
    Failed(String),
}

impl std::fmt::Display for ActivateFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ActivateFailure::PendingBufferOverflow => f.write_str(
                "the pending-shape buffer exceeded ELECTRIC_CIRCUITS_PENDING_BUFFER_MAX_BYTES and was dropped; \
                 this shape must be recreated from a fresh backfill",
            ),
            ActivateFailure::Failed(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for ActivateFailure {}

/// A shape between `BeginShape` and `ActivateShape`: buffers every processed delta of its table so
/// activation can replay exactly what the backfill snapshot did not see (through the gate).
pub(crate) struct PendingShape {
    pub(crate) num_id: u64,
    pub(crate) stream_path: String,
    pub(crate) pred: Arc<CompiledPredicate>,
    pub(crate) out_cols: Option<Arc<Vec<usize>>>,
    pub(crate) kind: CreateKind,
    pub(crate) buffered: Vec<Envelope>,
    /// What `buffered` costs to hold, in the same unit `ELECTRIC_CIRCUITS_TXN_MEMORY_BYTES` uses.
    pub(crate) buffered_bytes: u64,
    /// The buffer passed its cap and was dropped. The shape can no longer be activated from it —
    /// its stream would be missing exactly the changes the buffer was holding — so activation
    /// refuses and the caller recreates from a fresh backfill.
    pub(crate) overflowed: bool,
}

impl crate::heap_size::HeapSize for PendingShape {
    fn heap_bytes(&self) -> usize {
        self.stream_path.heap_bytes() + self.buffered.heap_bytes()
    }
}

/// Buffer one processed envelope for a pending shape, or refuse it because the shape's buffer is
/// over its cap.
///
/// The buffer holds every delta of the table between `BeginShape` and `ActivateShape`. That window
/// is a Postgres backfill for a create, and for a wake it is the replay — which now also includes
/// the wait for a reactivation permit, so a burst of wakes behind two long scans lengthens it to
/// "replay duration x queue depth". Unbounded, it is a write-rate-driven allocation with no ceiling
/// and no owner. Bounded, the shape is retired and recreated from a fresh backfill, which is the
/// same outcome an over-budget replay produces and one every client surface already handles.
///
/// The whole buffer is dropped on overflow rather than truncated: a prefix of the deltas is not a
/// cheaper answer, it is a silently wrong stream.
pub(crate) fn buffer_pending(pending: &mut PendingShape, env: &Envelope, cap: u64) -> bool {
    if pending.overflowed {
        return false;
    }
    let cost = crate::ds::envelope_memory_bytes(env);
    if cap > 0 && pending.buffered_bytes.saturating_add(cost) > cap {
        pending.overflowed = true;
        pending.buffered = Vec::new();
        pending.buffered_bytes = 0;
        return false;
    }
    pending.buffered_bytes = pending.buffered_bytes.saturating_add(cost);
    pending.buffered.push(env.clone());
    true
}

/// Get (or lazily create) the executor for `table`; `None` if `table` is not a known table's
/// **canonical** `schema.name`.
///
/// `execs` is keyed by that canonical string, not by [`TableRef`]: every caller already holds it —
/// the envelope's `type` on the hot path, `cmd.table.as_str()` off it — so the steady-state lookup
/// is a plain `&str` probe with no parse and no allocation.
///
/// **Strict, deliberately.** A non-canonical spelling (a bare `users`) is refused rather than
/// resolved, because resolving it here would be resolved in only HALF the engine: the live fan-out
/// would route it while the dormant-reactivation / retained-stream
/// catch-up — compares `env.type_ != table.as_str()` and would skip the very same envelopes, so a
/// shape's live stream and its replayed stream would disagree. No legitimate writer produces a
/// non-canonical `type`: the replication ingestor stamps `TableRef::to_string()`, and library-mode
/// writes go through the protocol's `toTableEnvelope`/`canonicalTable`. One that somehow appears
/// falls into the caller's "unknown table" branch — logged, highwater-advanced, dropped.
pub(crate) fn exec_for<'a>(
    execs: &'a mut HashMap<String, TableExec>,
    tables: &SharedTables,
    table: &str,
) -> Option<&'a mut TableExec> {
    if !execs.contains_key(table) {
        // Cold miss: resolve the schema registry (keyed by `TableRef`), but only for a spelling
        // that IS already canonical — `parse` alone would accept the bare-name sugar.
        let tref = TableRef::parse(table).ok().filter(|t| t.as_str() == table)?;
        let ts = tables.read().unwrap().get(&tref).cloned()?;
        execs.insert(tref.to_string(), TableExec::new(ts));
    }
    execs.get_mut(table)
}

/// The engine's single LSN-ordered executor: consumes the global `changes` stream in commit order
/// and dispatches each envelope to its table's executor. Each transaction's shape appends are
/// flushed **before the next transaction is processed**, so every shape stream reflects source
/// transactions atomically and in commit order — cross-table included (Electric's
/// `ShapeLogCollector` pattern; the property the old per-table tailers lost).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn sequencer_loop(
    ds: DsClient,
    tables: SharedTables,
    start: LogPosition,
    // The `(lsn, seq)` de-duplication highwater restored with `start` (ADR-0003).
    restore_highwater: Option<(u64, u64)>,
    catalog_tx: CatalogWriter,
    source_receipts: Arc<std::sync::Mutex<HashMap<String, SourceDrainReceipt>>>,
    last_source_receipt: Arc<std::sync::Mutex<Option<SourceDrainReceipt>>>,
    source_receipt_progress: Arc<std::sync::Mutex<SourceReceiptProgress>>,
    runtime_receipts: Arc<std::sync::Mutex<crate::runtime_authority::RuntimeReceipts>>,
    mut cmd_rx: mpsc::UnboundedReceiver<SequencerCmd>,
    processed: Arc<std::sync::Mutex<LogPosition>>,
    stats: Arc<std::sync::Mutex<HashMap<TableRef, TableStats>>>,
    node_states: Arc<std::sync::Mutex<HashMap<String, NodeStateSummary>>>,
    subq: SubqueryHandle,
    trace_tx: tokio::sync::broadcast::Sender<Arc<String>>,
    arr: Option<crate::arrangements::Arrangements>,
    arr_gates: HashMap<TableRef, crate::pg::SnapshotGate>,
    library_mode: bool,
    start_paused: bool,
    pause_gate: Arc<std::sync::atomic::AtomicBool>,
    read_failure_latch: Arc<std::sync::atomic::AtomicBool>,
    pending_buffer_max_bytes: u64,
    txn_cfg: crate::txn_buffer::TxnBufferConfig,
    sequenced: Option<Arc<crate::pg::SequencedXids>>,
    shutdown: crate::shutdown::ShutdownToken,
    // Held for the task's lifetime: dropping it is what tells the shutdown "the sequencer is done".
    _party: crate::shutdown::ShutdownParty,
) {
    let mut execs: HashMap<String, TableExec> = HashMap::new();
    let receipt_barrier = Arc::new(ReceiptBarrier::default());
    let (receipt_tx, mut receipt_rx) = mpsc::unbounded_channel::<ReceiptWork>();
    {
        let subq = subq.clone();
        let source_receipts = source_receipts.clone();
        let last_source_receipt = last_source_receipt.clone();
        let source_receipt_progress = source_receipt_progress.clone();
        let runtime_receipts = runtime_receipts.clone();
        let worker_shutdown = shutdown.clone();
        let catalog_tx = catalog_tx.clone();
        let receipt_barrier = receipt_barrier.clone();
        shutdown.clone().spawn_background(async move {
            loop {
                let work = tokio::select! {
                    work = receipt_rx.recv() => match work { Some(work) => work, None => return },
                    _ = worker_shutdown.wait() => return,
                };
                if !wait_for_source_effects(&subq, &worker_shutdown, work.marker_lsn).await {
                    // A degraded engine must restart from the last durable checkpoint. Leaving
                    // this marker registered would pin the publish gate forever while the
                    // sequencer continues to read, so fail closed and let shutdown replay it.
                    worker_shutdown.begin();
                    break;
                }
                if !work.runtime_fences.is_empty() {
                    if let Some(commit_lsn) = work.commit_lsn.as_deref() {
                        runtime_receipts.lock().unwrap().publish(
                            work.runtime_incarnation,
                            work.runtime_fences,
                            commit_lsn,
                            std::time::Instant::now(),
                        );
                    }
                }
                let Some(receipt) = work.source_receipt else {
                    receipt_barrier.complete(work.marker_lsn);
                    continue;
                };
                let source_commit_id = receipt.source_commit_id.clone();
                source_receipt_progress.lock().unwrap().record(&source_commit_id);
                if let Err(error) = catalog_tx.send_durable(CatalogEvent::SourceDrained(receipt.clone())).await {
                    tracing::error!(%error, %source_commit_id, "sequencer: source fence catalog receipt was not acknowledged");
                    // Keep the receipt barrier held when the durable catalog did not accept it.
                    // Shutdown preserves the earlier checkpoint and the next boot replays this
                    // transaction rather than claiming a durable source fence.
                    worker_shutdown.begin();
                    break;
                }
                source_receipts.lock().unwrap().insert(source_commit_id.clone(), receipt.clone());
                *last_source_receipt.lock().unwrap() = Some(receipt);
                tracing::info!(%source_commit_id, commit_lsn = ?work.commit_lsn, "source transaction durably drained");
                receipt_barrier.complete(work.marker_lsn);
            }
        });
    }
    let mut paused = start_paused;
    // A missing segment or unrecoverable body breach is terminal for this live loop. Keep reads
    // halted while the fatal restart request shuts it down at the safe checkpoint boundary.
    let mut sequencer_read_failed = false;
    // Whether the store has been re-attested since the last successful read. A read failure is the
    // only reconnect signal this client gets (reqwest owns the connection pool and reports no
    // reconnect event), so the first failure of a streak re-reads readiness; the rest of the streak
    // does not, or a store that is down would be HEADed at the backoff frequency for as long as it
    // stays down.
    let mut reattested_since_read = false;
    // A task started before restore may hold a stale initial cursor. Until an unpaused read
    // succeeds it has no progress to checkpoint, especially when the first segment is gone.
    let mut completed_read = false;
    let mut pos = start;
    // Offset checkpointing: persist the processed position (the restart replay start) at most
    // every ~2s of change — and ALWAYS the moment a segment boundary is crossed, so a restart
    // resumes on the segment the log actually moved to.
    let mut last_ckpt = std::time::Instant::now();
    let mut ckpt_pos = pos.clone();
    // The highwater the last checkpoint carried. While a transaction is HELD the position is
    // pinned, so the position alone stops changing — but transactions completed before the hold
    // keep advancing the highwater, and that progress has to reach the catalog or a crash
    // re-applies them (ADR-0003).
    let mut ckpt_hw: Option<(u64, u64)> = restore_highwater;
    // The rotation pointer this segment carried (ADR-0006). It can arrive in a batch that is not
    // yet flagged `closed` — the pointer append and the close are two requests — so it is
    // remembered until the close actually lands, and the segment is only left once BOTH are in.
    let mut rotate_to: Option<u32> = None;
    // Envelopes appended per shape id — the counters behind the per-node state summaries.
    let mut emitted: HashMap<String, u64> = HashMap::new();
    // De-duplication highwater: the ingestor's delivery is at-least-once (unacknowledged commits
    // re-deliver after a reconnect), and deltas are NOT idempotent for aggregates/subquery
    // weights. Every ingestor envelope carries (commit lsn, seq = position in txn), strictly
    // increasing on the single ordered log, so anything at/below the highwater has already been
    // applied and is skipped. Envelopes without both stamps (library mode) bypass this.
    let mut highwater: Option<(u64, u64)> = restore_highwater;

    // The TRAILING RUN of a page whose last envelope is not marked `headers.last` (ADR-0003).
    //
    // Durable-streams exposes each append atomically, so a commit appended in several chunks
    // arrives as several pages. Splitting on `(txid, lsn)` alone would make chunk 1 look like a
    // whole transaction and flush it to shape streams on its own — a subscriber would see a
    // fraction of a commit, which per-transaction atomic emission forbids. So an unterminated
    // trailing run is HELD here and carried into the next read, and the transaction is processed
    // (and flushed) only once the marker arrives.
    let mut held: Option<super::held::Transaction> = None;
    // Where a restart must resume while a run is held: the read position from BEFORE the page the
    // held run started in. `processed` (and therefore the checkpoint, and therefore the segment
    // deletion floor) never moves past it, so a crash re-reads the whole held transaction. The read
    // cursor `pos` moves on normally — only what is *published* is pinned.
    let mut held_from: Option<LogPosition> = None;
    // When the current hold started, and how many times it has been reported. A hold is normal and
    // short (the ingestor is appending the next chunk), but it freezes `processed` — the restart
    // point, the convergence barrier and the segment-deletion floor — so a hold that does NOT end
    // has to be visible rather than looking like an idle engine.
    let mut held_since: Option<std::time::Instant> = None;
    let mut held_warnings: u32 = 0;
    // Own the positioned read across command arms. Cancelling/recreating it would abandon
    // admission and rejoin the FIFO behind later readers during a control-plane burst.
    let mut pending_read = None;
    // Group output lands on its own lanes after the loop has moved on (`GroupEmissionLanes::submit`),
    // so a full or stalled group never holds another group or a plain shape. What waits instead is
    // what a restart, a convergence barrier and the segment-deletion floor read: `processed` and
    // the checkpoint never pass a transaction whose group output has not landed.
    let mut publish_gate =
        PublishGate::new(processed.lock().unwrap().clone(), restore_highwater, receipt_barrier.clone());
    let group_wake = subq.group_lanes.wake();
    loop {
        let (read_path, read_off) = (pos.path(), pos.offset.clone());
        if pending_read.is_none() && !paused && !sequencer_read_failed {
            let ds = ds.clone();
            let (path, offset) = (read_path.clone(), read_off.clone());
            pending_read = Some(Box::pin(async move { ds.read_budgeted(&path, &offset, true).await }));
        }
        tokio::select! {
            biased;
            cmd = cmd_rx.recv() => match cmd {
                Some(SequencerCmd::BeginShape { table, shape_id, num_id, stream_path, pred, out_cols, kind, ack }) => {
                    match exec_for(&mut execs, &tables, table.as_str()) {
                        Some(exec) => {
                            exec.pending.insert(
                                shape_id,
                                PendingShape {
                                    num_id,
                                    stream_path,
                                    pred,
                                    out_cols,
                                    kind,
                                    buffered: Vec::new(),
                                    buffered_bytes: 0,
                                    overflowed: false,
                                },
                            );
                        }
                        None => tracing::error!("begin_shape: unknown table '{table}'"),
                    }
                    let _ = ack.send(published(&pos, &held_from));
                }
                Some(SequencerCmd::ActivateShape { table, shape_id, gate, agg_seed, emitted_seed, ready }) => {
                    let res = activate_shape(
                        &ds, &mut execs, &table, &shape_id, gate, agg_seed, emitted_seed, &mut emitted,
                        &shutdown,
                    ).await;
                    if let Err(e) = &res {
                        tracing::error!("activate_shape failed: {e:#}");
                    }
                    let _ = ready.send(res);
                    publish_all(&execs, &pos.to_string(), &emitted, &stats, &node_states, &subq.registry, &trace_tx).await;
                }
                Some(SequencerCmd::AbortShape { table, shape_id }) => {
                    if let Some(exec) = execs.get_mut(table.as_str()) {
                        exec.pending.remove(&shape_id);
                    }
                }
                Some(SequencerCmd::DeactivateShape { table, shape_id, resp }) => {
                    // Capture-and-unregister is atomic w.r.t. envelope processing (commands run
                    // between fully-flushed transactions), so the shape's stream is complete up to
                    // the PUBLISHED position — which is the read cursor unless a transaction is
                    // being held mid-append (ADR-0003), in which case it is where that run began.
                    // Handing back the read cursor there would park the shape past a transaction it
                    // never saw, and its reactivation replay would start after those rows. Resuming
                    // from the pin instead re-reads the completed run, which is idempotent: the
                    // replay appends absolute per-pk rows.
                    let gate = execs.get_mut(table.as_str()).and_then(|exec| {
                        if let Some(shape) = exec.shapes.remove(&shape_id) {
                            exec.shape_index.remove(&shape_id);
                            Some(shape.gate)
                        } else if let Some((key_cols, num_id, key_tuple)) = exec.family_of.remove(&shape_id) {
                            let mut gate = None;
                            if let Some(router) = exec.families.get_mut(&key_cols) {
                                if let Some(routed) = router.index.get_mut(&key_tuple) {
                                    if let Some(pos) = routed.iter().position(|rs| rs.num_id == num_id) {
                                        gate = Some(routed.remove(pos).gate);
                                    }
                                    if routed.is_empty() {
                                        router.index.remove(&key_tuple);
                                    }
                                }
                                if router.index.is_empty() {
                                    exec.families.remove(&key_cols);
                                }
                            }
                            gate
                        } else {
                            None // unknown, pending, or an aggregate — not parkable from here
                        }
                    });
                    if gate.is_some() {
                        emitted.remove(&shape_id);
                    }
                    let _ = resp.send(gate.map(|g| (published(&pos, &held_from), g)));
                    publish_all(&execs, &pos.to_string(), &emitted, &stats, &node_states, &subq.registry, &trace_tx).await;
                }
                Some(SequencerCmd::RemoveShape { table, shape_id }) => {
                    if let Some(exec) = execs.get_mut(table.as_str()) {
                        exec.pending.remove(&shape_id);
                        if exec.circuit_aggs.remove(&shape_id).is_some() {
                            // a circuit-served COUNT — nothing else to unwind
                        } else if exec.aggregates.remove(&shape_id).is_some() {
                            // an aggregation shape — drop its conjunct-index entry too
                            exec.agg_index.remove(&shape_id);
                        } else if exec.shapes.remove(&shape_id).map(|_| exec.shape_index.remove(&shape_id)).is_none()
                            && let Some((key_cols, num_id, key_tuple)) = exec.family_of.remove(&shape_id)
                            && let Some(router) = exec.families.get_mut(&key_cols)
                        {
                            // Drop the shape from its key's routing list (the shape stream is torn
                            // down elsewhere); discard the router once it routes to no shapes.
                            if let Some(routed) = router.index.get_mut(&key_tuple) {
                                routed.retain(|rs| rs.num_id != num_id);
                                if routed.is_empty() {
                                    router.index.remove(&key_tuple);
                                }
                            }
                            if router.index.is_empty() {
                                exec.families.remove(&key_cols);
                            }
                        }
                    }
                    emitted.remove(&shape_id);
                    publish_all(&execs, &pos.to_string(), &emitted, &stats, &node_states, &subq.registry, &trace_tx).await;
                }
                Some(SequencerCmd::Barrier { done }) => {
                    receipt_barrier
                        .wait_until(highwater.map_or(u64::MAX, |(lsn, _)| lsn), &shutdown)
                        .await;
                    let _ = done.send(());
                }
                Some(SequencerCmd::ResumeReads { done }) => {
                    paused = false;
                    let _ = done.send(());
                }
                Some(SequencerCmd::PauseReads { done }) => {
                    paused = true;
                    pending_read = None;
                    let _ = done.send(());
                }
                Some(SequencerCmd::ResetTable { table }) => {
                    if execs.remove(table.as_str()).is_some() {
                        tracing::warn!("sequencer: dropped the executor for '{table}' (schema drift)");
                    }
                    publish_all(&execs, &pos.to_string(), &emitted, &stats, &node_states, &subq.registry, &trace_tx).await;
                }
                Some(SequencerCmd::CreateCircuitAgg { table, shape_id, stream_path, constraints, ready }) => {
                    let res = create_circuit_agg(
                        &ds, arr.as_ref(), &mut execs, &tables, &table, &shape_id, &stream_path, constraints,
                        &shutdown,
                    )
                    .await;
                    if res.is_ok() {
                        emitted.insert(shape_id.clone(), 1);
                    }
                    let _ = ready.send(res.map_err(|e| format!("{e:#}")));
                    publish_all(&execs, &pos.to_string(), &emitted, &stats, &node_states, &subq.registry, &trace_tx).await;
                }
                Some(SequencerCmd::DumpNode { table, node_id, resp }) => {
                    let val =
                        execs.get(table.as_str()).and_then(|exec| dump_node_json(exec, &pos.to_string(), &emitted, &node_id));
                    let _ = resp.send(val);
                }
                Some(SequencerCmd::MemBytes { resp }) => {
                    // On-demand only (see the variant's doc comment): this sums `exec_heap_bytes`
                    // across every live table executor, which is exactly the walk `stats_of` used
                    // to do on every processed batch before it moved here.
                    let total: usize = execs.values().map(exec_heap_bytes).sum();
                    let _ = resp.send(total);
                }
                None => break,
            },
            // Graceful shutdown. Placed AFTER commands (so anything already queued is still
            // answered — a create waiting on `ready` gets its answer rather than a dropped channel)
            // and BEFORE the read, so the change-log long-poll below is abandoned at once instead
            // of holding the pod's termination grace for its full window. Abandoning a read loses
            // nothing: `pos` is only advanced inside the branch body, so the next boot re-reads
            // from the checkpoint written just below.
            _ = shutdown.wait() => {
                tracing::info!("sequencer: shutdown requested at {}", published(&pos, &held_from));
                break;
            }
            res = async { pending_read.as_mut().expect("unpaused read exists").await }, if !paused && !sequencer_read_failed => {
                pending_read = None;
                match res {
                Ok((rr, page_permit)) => {
                    // A PauseReads command may win immediately after an in-flight HTTP read returns.
                    // Discard that page before touching the cursor or checkpoint so an existing
                    // sequencer can be fenced without advancing the retry boundary.
                    if pause_gate.load(std::sync::atomic::Ordering::Acquire) {
                        continue;
                    }
                    completed_read = true;
                    reattested_since_read = false;
                    let next = rr.next_offset.clone();
                    // How much this read delivered, BEFORE control envelopes are filtered out: a
                    // closed segment is only left once a read comes back empty, which is the proof
                    // that everything in it has been consumed.
                    let delivered = rr.envelopes.len();
                    let advanced = rr.next_offset.as_deref().is_some_and(|n| n != pos.offset);
                    if let Some(n) = rr.next_offset { pos.offset = n; }
                    // Change-log CONTROL envelopes (the rotation pointer, ADR-0006) are routing
                    // metadata, not data: they are recognised by TYPE and removed here —
                    // UNCONDITIONALLY, before anything (transaction splitting, the arrangements
                    // feed, `exec_for`, `process_envelope`) looks at the batch. Not "only when a
                    // pointer is present": any control envelope, recognised or not, is ours and
                    // must never be mistaken for a table's change.
                    let mut envs = rr.envelopes;
                    if let Some(n) = crate::changelog::rotation_target_in(&envs) {
                        rotate_to = Some(n);
                    }
                    envs.retain(|e| !crate::changelog::is_control(e));
                    // Keep the pre-page replay boundary separately from `held_from`, which is
                    // cleared when a held transaction completes on this page. If a later
                    // transaction then fails, replay must still include that completed prefix.
                    let held_replay_from = held_from.clone();
                    let page_permit = Arc::new(page_permit);
                    let page_credit = ds.memory_budget().enabled().then(|| page_permit.clone());
                    let page = match prepare_transactions(envs, held.take(), &txn_cfg, ds.metrics().clone(), page_credit).await {
                        Ok(page) => page,
                        Err(error) => {
                            tracing::error!(%error, "sequencer held spill failed; preserving replay boundary");
                            pos = held_replay_from.clone().unwrap_or_else(|| LogPosition { segment: pos.segment, offset: read_off.clone() });
                            held_from = held_replay_from;
                            shutdown.begin();
                            break;
                        }
                    };
                    drop(page_permit);
                    held = page.held;
                    if let Some(run) = &held {
                        if page.new_hold {
                            held_from = Some(LogPosition { segment: pos.segment, offset: read_off.clone() });
                            held_since = Some(std::time::Instant::now());
                            held_warnings = 0;
                        }
                        ds.metrics().sequencer_held_run.store(1, Ordering::Relaxed);
                        let since = *held_since.get_or_insert_with(std::time::Instant::now);
                        if since.elapsed() >= HELD_RUN_WARN_AFTER * (held_warnings + 1) {
                            held_warnings += 1;
                            tracing::warn!(txid = ?run.key.0, lsn = ?run.key.1, envelopes = run.count,
                                elapsed = ?since.elapsed(), position = %published(&pos, &held_from),
                                "sequencer transaction remains incomplete; replay position is pinned");
                        }
                    } else {
                        // Processing below finishes before this released pin can be published.
                        held_from = None;
                        held_since = None;
                        held_warnings = 0;
                        ds.metrics().sequencer_held_run.store(0, Ordering::Relaxed);
                    }
                    // Split the read batch into transactions (runs of equal (txid, lsn) — the
                    // ingestor appends whole commits contiguously, in commit order) and flush each
                    // transaction's appends before processing the next: atomic per-transaction
                    // emission, across tables. Every run left here is COMPLETE: it ends on an
                    // envelope carrying `headers.last`, so a commit appended in several chunks
                    // (ADR-0003) is processed once, whole, not chunk by chunk.
                    let mut touched = false;
                    let mut processing_failed = false;
                    for mut transaction in page.ready {
                        // Commit highwater only after the complete transaction succeeds. If an
                        // envelope fails, staged output is discarded and replay must retry the
                        // whole transaction rather than skipping an earlier prefix.
                        let txn_highwater = highwater;
                        let (txid, lsn) = transaction.key.clone();
                        let mut source_fence: Option<String> = None;
                        let runtime_incarnation = runtime_receipts.lock().unwrap().incarnation();
                        let mut runtime_fences = crate::runtime_authority::PendingRuntimeFences::default();
                        // Before any of this transaction reaches a shape stream or a feed: a
                        // snapshot of a table it touched, taken from here on, that still excludes
                        // it must be retaken, or whatever it is paired with (a BeginShape point, a
                        // subset client's HEAD) would sit past a transaction that is in neither
                        // (`pg::SequencedXids`). Engine control envelopes name no table.
                        if let (Some(seen), Some(xid)) = (&sequenced, txid.as_deref().and_then(|t| t.parse::<u64>().ok())) {
                            seen.note(
                                xid,
                                transaction.tables.iter().map(String::as_str).filter(|t| !t.starts_with("__circuits.")),
                            );
                        }
                        // Keep one transaction's output and arrangement batch, but stream its
                        // INPUT from memory/file chunks. Neither chunk boundaries nor spill replay
                        // flush partial output or publish partial highwater.
                        let mut txn_arr_deltas = Vec::new();
                        let mut txn_pending: HashMap<String, Vec<Envelope>> = HashMap::new();
                        loop {
                            // Finish ready input on shutdown. Only a pending spill read or its
                            // admission wait yields the page back to the replay boundary.
                            let next_chunk = tokio::select! {
                                biased;
                                result = transaction.next_chunk() => result,
                                _ = shutdown.wait() => {
                                    tracing::info!("sequencer: shutdown while awaiting transaction input; preserving replay boundary");
                                    highwater = txn_highwater;
                                    processing_failed = true;
                                    break;
                                },
                            };
                            let chunk = match next_chunk {
                                Ok(Some(chunk)) => chunk,
                                Ok(None) => break,
                                Err(error) => {
                                    tracing::error!(%error, "reading held transaction failed; preserving replay boundary");
                                    highwater = txn_highwater;
                                    processing_failed = true;
                                    break;
                                }
                            };
                            for mut env in chunk {
                                // The circuit retains its own dedup highwater, as on the page path.
                                if let Some(arr) = &arr
                                    && let Some(delta) = stamped_delta_for_arrangements(&tables, arr, &arr_gates, &env)
                                {
                                    txn_arr_deltas.push(delta);
                                }
                                // Skip redelivered changes (see `highwater` above).
                                let pos = match (env.headers.lsn.as_deref(), env.headers.seq) {
                                    (Some(l), Some(seq)) => Some((crate::pg::lsn_to_u64(l), seq)),
                                    _ => None,
                                };
                                if let (Some(p), Some(hw)) = (pos, highwater) {
                                    if p <= hw {
                                        tracing::debug!("sequencer: skipping duplicate change at {p:?}");
                                        continue;
                                    }
                                }
                                if env.type_ == crate::runtime_authority::ENVELOPE_TYPE {
                                    runtime_fences.stage(&env, runtime_incarnation);
                                    touched = true;
                                    if let Some(position) = pos { highwater = Some(position); }
                                    continue;
                                }
                                if env.type_ == crate::replication::SOURCE_FENCE_ENVELOPE {
                                    if source_fence.replace(env.key.clone()).is_some() {
                                        tracing::error!(
                                            "sequencer: transaction {txid:?} at {lsn:?} contains more than one source fence"
                                        );
                                        highwater = txn_highwater;
                                        processing_failed = true;
                                        break;
                                    }
                                    touched = true;
                                    if let Some(position) = pos {
                                        highwater = Some(position);
                                    }
                                    continue;
                                }
                                let Some(exec) = exec_for(&mut execs, &tables, &env.type_) else {
                                    tracing::error!("sequencer: change for unknown table '{}'", env.type_);
                                    if let Some(p) = pos { highwater = Some(p); }
                                    continue;
                                };
                                // LIBRARY MODE: no Postgres wrote this change, so nothing stamped a
                                // before-image on it. Fill it in from the sequencer's own per-key view
                                // (`TableExec::library_rows`) HERE — after the de-duplication highwater
                                // (a re-delivered duplicate must never fold into the view twice) and
                                // ahead of everything downstream: the pending-shape buffers, the
                                // fan-out, the aggregate folds. From this point the envelope is
                                // indistinguishable from a replicated one.
                                //
                                // The counts pipelines were fed above, from the un-stamped page. That
                                // is not a gap: the circuit tier seeds from a Postgres snapshot and
                                // refuses to start without a `pg_url`, so `arr` is always `None` here
                                // in library mode.
                                if library_mode {
                                    exec.stamp_before_image(&mut env);
                                }
                                // Buffer for in-flight creations on this table: their `BeginShape` was
                                // acknowledged before the creator's snapshot, so everything the
                                // snapshot cannot contain lands in the buffer.
                                for (pending_id, pending) in exec.pending.iter_mut() {
                                    let was_overflowed = pending.overflowed;
                                    if !buffer_pending(pending, &env, pending_buffer_max_bytes) && !was_overflowed {
                                        ds.metrics().pending_buffer_overflows.fetch_add(1, Ordering::Relaxed);
                                        tracing::error!(
                                            shape_id = pending_id,
                                            cap_bytes = pending_buffer_max_bytes,
                                            "pending shape buffer exceeded ELECTRIC_CIRCUITS_PENDING_BUFFER_MAX_BYTES \
                                             while the shape was still being created or replayed; dropping the buffer \
                                             and refusing activation, so the caller recreates from a fresh backfill"
                                        );
                                    }
                                }
                                if let Err(e) = process_envelope(
                                    ds.metrics(), &exec.ts, &exec.shapes, &exec.shape_index, &exec.families,
                                    &mut exec.aggregates, &exec.agg_index, env, &mut txn_pending,
                                    &subq, &trace_tx, library_mode,
                                )
                                .await
                                {
                                    tracing::error!("process_envelope failed: {e:#}; stopping before publishing this transaction");
                                    highwater = txn_highwater;
                                    processing_failed = true;
                                    break;
                                }
                                exec.envelopes_total += 1;
                                touched = true;
                                if let Some(p) = pos {
                                    highwater = Some(p);
                                }
                            }
                            if processing_failed { break; }
                        }
                        if processing_failed {
                            // Do not run the count fold or flush the partial transaction. The
                            // failed transaction is retried from its page boundary on restart.
                            break;
                        }
                        // Do not mutate the arrangement until every envelope has fanned out
                        // successfully; a failed transaction must leave no circuit-side prefix.
                        let txn_count_deltas = if let Some(arr) = &arr {
                            arr.apply_batch(txn_arr_deltas).await
                        } else {
                            Vec::new()
                        };
                        // Counts pipeline → circuit-served aggregates.
                        if !txn_count_deltas.is_empty() {
                            apply_count_deltas(
                                &mut execs, txn_count_deltas, txid.clone(), lsn.clone(), &mut txn_pending,
                                &trace_tx,
                            );
                        }
                        emit_storage_txn_metrics(ds.metrics(), &txn_pending);
                        for (path, envs) in &txn_pending {
                            *emitted.entry(sid_of_path(path).to_string()).or_insert(0) += envs.len() as u64;
                        }
                        // Group members, including ordinary filters, use the same bounded lane as
                        // deferred subquery effects. The lane keeps each group's output in commit
                        // order and applies backpressure only to that group's stream; the loop
                        // never waits on it (`PublishGate` holds the replay boundary instead).
                        let group_lsn = lsn.as_deref().map(crate::pg::lsn_to_u64).unwrap_or(0);
                        let group_paths: Vec<String> = txn_pending
                            .keys()
                            .filter(|path| path.starts_with("shape/group-"))
                            .cloned()
                            .collect();
                        let group_batches: Vec<(String, Vec<Envelope>)> = group_paths
                            .into_iter()
                            .filter_map(|path| txn_pending.remove(&path).map(|envs| (path, envs)))
                            .collect();
                        // Transaction boundary: every plain append of this commit lands before the
                        // next commit is processed. Group output is handed to its lanes only once
                        // that flush succeeded, so a transaction that must be replayed leaves none.
                        let other_stream_output = !txn_pending.is_empty();
                        if let Err(error) = flush_pending(&ds, txn_pending).await {
                            tracing::warn!(%error, "sequencer append interrupted; preserving replay boundary");
                            highwater = txn_highwater;
                            processing_failed = true;
                            break;
                        }
                        for (path, envs) in group_batches {
                            subq.group_lanes.submit(&path, envs, group_lsn);
                        }
                        // A wait-cap clock advances only after a successful append to a plain
                        // stream. If storage is down and every append is stopped, no progress
                        // event is emitted and no group consumes its wait budget.
                        if other_stream_output {
                            subq.group_lanes.progress(group_lsn);
                        }
                        if runtime_fences.overflowed() {
                            tracing::warn!(commit_lsn = ?lsn, capacity = crate::runtime_authority::RECEIPT_CAPACITY,
                                "runtime marker staging full; excess receipts stay unknown; application transaction processed");
                        }
                        let source_receipt = source_fence.map(|source_commit_id| SourceDrainReceipt {
                            source_commit_id,
                            commit_lsn: lsn.clone().unwrap_or_else(|| "0/0".to_string()),
                        });
                        if source_receipt.is_some() || !runtime_fences.is_empty() {
                            let marker_lsn = lsn.as_deref().map(crate::pg::lsn_to_u64).unwrap_or(u64::MAX);
                            receipt_barrier.register(marker_lsn);
                            let work = ReceiptWork {
                                marker_lsn,
                                commit_lsn: lsn.clone(),
                                source_receipt,
                                runtime_fences: runtime_fences.into_fences(),
                                runtime_incarnation,
                            };
                            if receipt_tx.send(work).is_err() {
                                highwater = txn_highwater;
                                shutdown.begin();
                                processing_failed = true;
                            }
                            if processing_failed {
                                break;
                            }
                            // The worker owns the durable catalog write. Give it a scheduling
                            // turn before the sequencer continues to the next command/read so the
                            // existing command barrier observes the same receipt ordering.
                            tokio::task::yield_now().await;
                        }
                    }
                    if processing_failed {
                        // The read cursor advances before processing; rewind it to the page that
                        // began any held transaction, not merely this page. A completed held
                        // prefix may have been staged before the failure and must be replayed.
                        if let Some(from) = held_replay_from {
                            pos = from.clone();
                            held_from = Some(from);
                        } else {
                            pos.offset = read_off;
                        }
                        shutdown.begin();
                        break;
                    }
                    // Publish the processed position only after the whole batch is fanned out +
                    // flushed — and never past a run still being HELD (ADR-0003): a restart must
                    // re-read the whole incomplete transaction, and `processed` is also what
                    // `GET /tables/{name}/offset` and the segment-deletion floor read.
                    if next.is_some() {
                        publish_gate.publish(&subq.group_lanes, &processed, published(&pos, &held_from), highwater);
                    }
                    // The segment ended (ADR-0006). Cross only once a read of it comes back EMPTY
                    // (or stops advancing): "closed" can arrive alongside a page of data, and
                    // leaving on that page would skip whatever a partial page left behind. A closed
                    // stream answers a long-poll instantly, so the confirming read costs one round
                    // trip per rotation. The crossing is checkpointed immediately — a 2 s-lazy
                    // checkpoint still naming the closed segment's tail would make a restart
                    // re-derive it.
                    let mut crossed = false;
                    if rr.closed && (delivered == 0 || !advanced) {
                        match rotate_to.take() {
                            Some(n) => {
                                tracing::info!("sequencer: {} closed; continuing on {}", pos.path(), segment_path(n));
                                pos = LogPosition::start_of(n);
                                publish_gate.publish(&subq.group_lanes, &processed, published(&pos, &held_from), highwater);
                                crossed = true;
                            }
                            None => {
                                // No pointer in this run: the checkpoint this process resumed from
                                // was already past it, or storage lost it. Step to EXACTLY the next
                                // segment, verified to exist — never a walk to the first OPEN one,
                                // which would skip every closed segment in between and with it a
                                // whole span of unread changes.
                                match crate::changelog::next_segment_for_reader(&ds, pos.segment).await {
                                    Ok(n) => {
                                        tracing::warn!(
                                            "sequencer: {} is closed and carried no rotation pointer in this run; \
                                             stepping to {}",
                                            pos.path(),
                                            segment_path(n)
                                        );
                                        pos = LogPosition::start_of(n);
                                        publish_gate.publish(&subq.group_lanes, &processed, published(&pos, &held_from), highwater);
                                        crossed = true;
                                    }
                                    Err(e) if e.is::<crate::changelog::MissingSuccessor>() => {
                                        ds.metrics().sequencer_stream_gone_failures.fetch_add(1, Ordering::Relaxed);
                                        sequencer_read_failed = true;
                                        read_failure_latch.store(true, Ordering::SeqCst);
                                        tracing::error!(
                                            "sequencer: cannot leave the closed segment {}: {e:#}. \
                                             Live reads halted; requesting restart without skipping data.",
                                            pos.path()
                                        );
                                        shutdown.restart_or_exit(75);
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            "sequencer: cannot leave the closed segment {}: {e:#}. Backing off; \
                                             nothing is skipped.",
                                            pos.path()
                                        );
                                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                                    }
                                }
                            }
                        }
                    }
                    publish_gate.release(&subq.group_lanes, &processed, &shutdown);
                    let (ckpt, ckpt_highwater) = publish_gate.checkpoint(
                        &subq.group_lanes,
                        published(&pos, &held_from),
                        highwater,
                    );
                    // Either half moving is progress worth persisting: the position, or — while the
                    // position is pinned behind a held run — the de-duplication highwater.
                    let ckpt_moved = ckpt != ckpt_pos || ckpt_highwater != ckpt_hw;
                    if crossed || (next.is_some() && ckpt_moved && last_ckpt.elapsed() >= std::time::Duration::from_secs(2)) {
                        ckpt_pos = ckpt.clone();
                        ckpt_hw = ckpt_highwater;
                        last_ckpt = std::time::Instant::now();
                        // The de-duplication highwater rides WITH the position (ADR-0003): a crash
                        // after a prefix of a chunked commit was applied and checkpointed would
                        // otherwise re-apply that prefix on re-delivery, and aggregate/subquery
                        // weights are not idempotent under duplicates.
                        catalog_tx.send(CatalogEvent::Offset { pos: ckpt, highwater: ckpt_highwater });
                    }
                    if touched {
                        publish_all(&execs, &pos.to_string(), &emitted, &stats, &node_states, &subq.registry, &trace_tx).await;
                    }
                }
                Err(e) if crate::ds::is_stream_gone(&e) => {
                    ds.metrics().sequencer_stream_gone_failures.fetch_add(1, Ordering::Relaxed);
                    // A GET 404/410 is terminal for this process, even if a proxy misreported it.
                    // Preserve the replay boundary and withdraw readiness as for a terminal cap
                    // breach. The next boot verifies the whole chain: intact storage can resume,
                    // but an actual gap refuses boot instead of serving stale shapes.
                    sequencer_read_failed = true;
                    read_failure_latch.store(true, Ordering::SeqCst);
                    tracing::error!(
                        "sequencer: GET reported the change-log segment is GONE ({e:#}); live reads halted \
                         and readiness withdrawn. Requesting restart; boot will re-verify the segment chain.",
                    );
                    shutdown.restart_or_exit(75);
                }
                Err(e) => {
                    if let Some(cap) = e.downcast_ref::<crate::ds::ReadCapExceeded>() {
                        ds.metrics().sequencer_read_cap_failures.fetch_add(1, Ordering::Relaxed);
                        // A read bigger than the cap is ordinarily a large value, not a broken
                        // store: against a store that advertises no page the whole remainder of
                        // the stream comes back in one response, and against one that pages a
                        // single value larger than the page is framed whole, up to the value bound
                        // the store advertises. Halting there costs more than the memory it saves.
                        // The health check replaces the task, the restart re-reads from the same
                        // checkpoint, and the task cycles until an operator intervenes. So the
                        // ceiling is raised, the read retried, and the latch kept for an
                        // operator-named cap, a store that broke a value bound it advertised, or a
                        // value past the hard ceiling.
                        match ds.raise_read_cap_after_breach() {
                            crate::ds::CapBreachOutcome::Raise { limit } => {
                                ds.metrics().sequencer_read_cap_raised.fetch_add(1, Ordering::Relaxed);
                                tracing::warn!(path = %cap.path, observed = cap.observed, was = cap.limit, now = limit,
                                    max_value_bytes = cap.max_value_bytes,
                                    "a Durable Streams read exceeded the client body cap: the store either advertises \
                                     no page or is entitled to frame one value larger than its page; raising the cap \
                                     and retrying. Bound the values written to this stream, or name a cap this \
                                     process should not exceed");
                                back_off(&shutdown, std::time::Duration::from_millis(200)).await;
                            }
                            crate::ds::CapBreachOutcome::Latch => {
                                sequencer_read_failed = true;
                                read_failure_latch.store(true, Ordering::SeqCst);
                                tracing::error!(path = %cap.path, observed = cap.observed, limit = cap.limit,
                                    "sequencer halted live reads after a Durable Streams body-cap breach; restart required");
                                shutdown.restart_or_exit(75);
                            }
                        }
                    } else {
                        tracing::warn!("sequencer read error on {read_path}: {e:#}; backing off");
                        // The next read reconnects, and a store's page capability is boot state
                        // that can change under a running client (an upgrade that starts paging, a
                        // rollback that stops). Re-derive the verdict here so the cap in force is
                        // the one this store actually advertises now, rather than the one attested
                        // at a connection that no longer exists.
                        if !reattested_since_read {
                            reattested_since_read = true;
                            match ds.reattest_readiness().await {
                                Ok(verdict) => tracing::info!(
                                    ?verdict,
                                    "re-attested Durable Streams readiness after a read failure"
                                ),
                                Err(error) => tracing::error!(
                                    "re-attesting Durable Streams readiness after a read failure failed: {error:#}"
                                ),
                            }
                        }
                        back_off(&shutdown, std::time::Duration::from_millis(200)).await;
                    }
                }
                }
            },
            // Publish what group output has since landed, even while the change log is idle. The
            // timeout covers a completion that fired before this wait was registered.
            _ = tokio::time::timeout(std::time::Duration::from_millis(100), group_wake.notified()), if publish_gate.is_waiting() => {
                if publish_gate.release(&subq.group_lanes, &processed, &shutdown) {
                    let (ckpt, ckpt_highwater) = publish_gate.checkpoint(
                        &subq.group_lanes,
                        published(&pos, &held_from),
                        highwater,
                    );
                    let crossed = ckpt.segment != ckpt_pos.segment;
                    if crossed || ((ckpt != ckpt_pos || ckpt_highwater != ckpt_hw)
                        && last_ckpt.elapsed() >= std::time::Duration::from_secs(2))
                    {
                        ckpt_pos = ckpt.clone();
                        ckpt_hw = ckpt_highwater;
                        last_ckpt = std::time::Instant::now();
                        catalog_tx.send(CatalogEvent::Offset { pos: ckpt, highwater: ckpt_highwater });
                    }
                }
            }
        }
    }
    drop(pending_read);
    drop(held);
    ds.metrics().sequencer_held_run.store(0, Ordering::Relaxed);
    // The loop is only left on shutdown (or on the command channel closing, i.e. the engine going
    // away). A completed batch is fully flushed; a processing/append failure rewinds to its replay
    // boundary above. One last `Offset` makes that safe position (and the
    // de-duplication highwater riding with it) durable. Without it, everything since the last lazy
    // 2 s checkpoint would be replayed on the next boot: correct, but a needless storm, and for a
    // held run it would also re-read a transaction the ingestor never finished.
    if !paused && completed_read {
        let (ckpt, highwater) = publish_gate.checkpoint(&subq.group_lanes, published(&pos, &held_from), highwater);
        catalog_tx.send(CatalogEvent::Offset { pos: ckpt.clone(), highwater });
        tracing::info!("sequencer: stopped at {ckpt} (highwater {highwater:?})");
    } else if paused {
        tracing::info!("sequencer: stopped while replay was paused; no checkpoint published");
    } else {
        tracing::info!("sequencer: stopped before completing a read; no checkpoint published");
    }
}

/// A source receipt is stronger than "the sequencer task exited" or "the slot was released": it
/// waits for deferred propagation/emission batches originating at or before the marker's commit
/// LSN to land and refuses to publish after shutdown or degradation. Runtime and deployment markers
/// share this effect barrier, while their receipts and admission remain separate. Later origins
/// cannot delay this prefix; unknown origins (zero) hold every marker. The existing short poll
/// services completion without adding wake-ups to the hot paths.
async fn wait_for_source_effects(
    subq: &SubqueryHandle,
    shutdown: &crate::shutdown::ShutdownToken,
    marker_lsn: u64,
) -> bool {
    loop {
        if shutdown.is_shutting_down() {
            return false;
        }
        if subq.degrade.degraded.load(Ordering::Acquire) {
            return false;
        }
        if !subq.pending_flips.has_at_or_before(marker_lsn) {
            return true;
        }
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
            _ = shutdown.wait() => return false,
        }
    }
}

/// The replay boundary while group output is still landing. A completed batch publishes its
/// position (and the de-duplication highwater riding with it) at once when no group output at or
/// before its last commit is outstanding; otherwise it waits here, in order, behind the batches
/// before it. Only `processed` and the checkpoint wait; the sequencer keeps reading and fanning out.
struct PublishGate {
    waiting: std::collections::VecDeque<(u64, LogPosition, Option<(u64, u64)>)>,
    published: (LogPosition, Option<(u64, u64)>),
    receipts: Arc<ReceiptBarrier>,
}

impl PublishGate {
    fn new(pos: LogPosition, highwater: Option<(u64, u64)>, receipts: Arc<ReceiptBarrier>) -> Self {
        Self { waiting: std::collections::VecDeque::new(), published: (pos, highwater), receipts }
    }

    fn is_waiting(&self) -> bool {
        !self.waiting.is_empty()
    }

    /// The last commit covered by a batch is its highwater's LSN. Without one (library mode, or
    /// nothing stamped yet) every outstanding group origin holds it.
    fn publish(
        &mut self,
        lanes: &crate::engine::emission::GroupEmissionLanes,
        processed: &std::sync::Mutex<LogPosition>,
        pos: LogPosition,
        highwater: Option<(u64, u64)>,
    ) {
        let marker = highwater.map_or(u64::MAX, |(lsn, _)| lsn);
        if self.waiting.is_empty()
            && !lanes.has_any_origin_at_or_before(marker)
            && !self.receipts.has_at_or_before(marker)
        {
            *processed.lock().unwrap() = pos.clone();
            self.published = (pos, highwater);
        } else {
            self.waiting.push_back((marker, pos, highwater));
        }
    }

    /// Publish every waiting batch whose group output has landed. Nothing is published once
    /// shutdown has begun: a lane stopped by shutdown completes its batches without landing them.
    fn release(
        &mut self,
        lanes: &crate::engine::emission::GroupEmissionLanes,
        processed: &std::sync::Mutex<LogPosition>,
        shutdown: &crate::shutdown::ShutdownToken,
    ) -> bool {
        if shutdown.is_shutting_down() {
            return false;
        }
        let mut advanced = false;
        while let Some((marker, _, _)) = self.waiting.front() {
            if lanes.has_any_origin_at_or_before(*marker) || self.receipts.has_at_or_before(*marker) {
                break;
            }
            let (_, pos, highwater) = self.waiting.pop_front().expect("front exists");
            self.published = (pos, highwater);
            advanced = true;
        }
        if advanced {
            *processed.lock().unwrap() = self.published.0.clone();
        }
        advanced
    }

    /// What a checkpoint may carry: the live boundary when nothing waits, else the last
    /// published one.
    fn checkpoint(
        &self,
        lanes: &crate::engine::emission::GroupEmissionLanes,
        pos: LogPosition,
        highwater: Option<(u64, u64)>,
    ) -> (LogPosition, Option<(u64, u64)>) {
        let marker = highwater.map_or(u64::MAX, |(lsn, _)| lsn);
        if self.waiting.is_empty()
            && !lanes.has_any_origin_at_or_before(marker)
            && !self.receipts.has_at_or_before(marker)
        {
            (pos, highwater)
        } else {
            self.published.clone()
        }
    }
}

/// Wait out a read-error backoff, cut short by a shutdown. The backoff exists to stop a failing
/// read spinning; once the process is going away there is nothing left to pace, and sleeping it out
/// would spend the termination grace on dead time.
async fn back_off(shutdown: &crate::shutdown::ShutdownToken, d: std::time::Duration) {
    tokio::select! {
        _ = tokio::time::sleep(d) => {}
        _ = shutdown.wait() => {}
    }
}

/// How long a transaction may stay incomplete on the change log before the sequencer says so (and
/// again every interval after that). Long enough that appending the next chunk of a genuinely huge
/// commit is never reported, short enough that a stalled ingest is not silent.
const HELD_RUN_WARN_AFTER: std::time::Duration = std::time::Duration::from_secs(60);

/// The position a HELD run pins publication to: while a transaction is only partly on the log
/// (ADR-0003), `processed` — and therefore the restart point, `GET /tables/{name}/offset`, and the
/// segment-deletion floor — stays at the page the held run began in. The read cursor moves on
/// regardless; only what is published is pinned.
fn published(pos: &LogPosition, held_from: &Option<LogPosition>) -> LogPosition {
    held_from.clone().unwrap_or_else(|| pos.clone())
}

/// The `(txid, lsn)` pair identifying the transaction an envelope belongs to — the key the change
/// log's runs are split on.
fn run_key(env: &Envelope) -> (Option<&str>, Option<&str>) {
    (env.headers.txid.as_deref(), env.headers.lsn.as_deref())
}

/// [`run_key`], owned — needed to remember a held run's identity across the move that re-attaches it.
pub(super) fn run_key_owned(env: &Envelope) -> (Option<String>, Option<String>) {
    (env.headers.txid.clone(), env.headers.lsn.clone())
}

/// Borrow an owned run key back for comparison with [`run_key`].
fn key_ref(k: &(Option<String>, Option<String>)) -> (Option<&str>, Option<&str>) {
    (k.0.as_deref(), k.1.as_deref())
}

/// The index at which a page's trailing, INCOMPLETE transaction begins — `None` when the page ends
/// on a complete one, or carries nothing that could be incomplete.
///
/// "Complete" is the ingestor's transaction-end marker (`headers.last`), set on the final envelope
/// of the final chunk of every commit (ADR-0003). Without it a commit appended in several chunks
/// would be processed chunk by chunk, and each chunk flushed to shape streams as if it were a whole
/// transaction.
///
/// An envelope with no `seq` is NOT held: only the ingestor structures its envelopes into
/// transactions, and a library-mode write is a one-envelope transaction. Holding one would wait for
/// a marker no producer is going to send.
fn unterminated_tail(envs: &[Envelope]) -> Option<usize> {
    let last = envs.last()?;
    if last.headers.last == Some(true) {
        return None;
    }
    last.headers.seq?;
    let (txid, lsn) = (&last.headers.txid, &last.headers.lsn);
    let mut i = envs.len() - 1;
    while i > 0 && (&envs[i - 1].headers.txid, &envs[i - 1].headers.lsn) == (txid, lsn) {
        i -= 1;
    }
    Some(i)
}

struct TransactionPage {
    ready: Vec<super::held::Transaction>,
    held: Option<super::held::Transaction>,
    new_hold: bool,
}

/// Preserve ADR-0003's leading-run redelivery filter and trailing hold without ever loading an
/// existing spilled run into the page vector. Complete inputs retain their scratch owners until
/// processing (including the transaction flush) finishes.
async fn prepare_transactions(
    mut envs: Vec<Envelope>,
    mut held: Option<super::held::Transaction>,
    cfg: &crate::txn_buffer::TxnBufferConfig,
    metrics: Arc<crate::metrics::Metrics>,
    page_credit: Option<Arc<crate::memory_budget::Permit>>,
) -> Result<TransactionPage> {
    // A receipt needs query-back credits, and a previously held input needs a spill-reader
    // reservation. In either case spill the rest of this page before taking those dependencies.
    // Ordinary complete pages can consume their page owner directly, without scratch I/O.
    let mut spill_ready = envs.iter().any(|env| {
        env.type_ == crate::runtime_authority::ENVELOPE_TYPE || env.type_ == crate::replication::SOURCE_FENCE_ENVELOPE
    });
    let mut ready = Vec::new();
    if let Some(mut run) = held.take() {
        let lead = envs.iter().take_while(|env| run_key(env) == key_ref(&run.key)).count();
        if lead == 0 && !envs.is_empty() {
            tracing::warn!(held = ?run.key, next = ?run_key_owned(&envs[0]),
                "sequencer: discarding orphan held transaction fragment");
            metrics.sequencer_orphan_fragments.fetch_add(1, Ordering::Relaxed);
        } else {
            spill_ready = true;
            let rest = envs.split_off(lead);
            envs.retain(|env| match (env.headers.seq, run.last_seq) {
                (Some(seq), Some(last)) => seq > last,
                _ => true,
            });
            if !envs.is_empty() {
                run = run.with_page_permit(page_credit.clone()).append(envs).await?;
            }
            envs = rest;
            if run.complete || !envs.is_empty() {
                ready.push(run);
            } else {
                held = Some(run);
            }
        }
    }
    let mut new_hold = false;
    if let Some(cut) = unterminated_tail(&envs) {
        let tail = envs.split_off(cut);
        held = Some(
            super::held::Transaction::held(cfg.clone(), metrics.clone())
                .with_page_permit(page_credit.clone())
                .append(tail)
                .await?,
        );
        new_hold = true;
    }
    let mut iter = envs.into_iter().peekable();
    while let Some(first) = iter.next() {
        let key = run_key_owned(&first);
        let mut run = vec![first];
        while iter.peek().is_some_and(|env| run_key(env) == key_ref(&key)) {
            if let Some(env) = iter.next() {
                run.push(env);
            }
        }
        ready.push(super::held::Transaction::page(run, metrics.clone()).with_page_permit(page_credit.clone()));
    }
    if metrics.memory_budget.enabled() {
        if spill_ready {
            let mut spooled = Vec::with_capacity(ready.len());
            for txn in ready {
                spooled.push(txn.spool(cfg.clone()).await?);
            }
            ready = spooled;
        }
        if let Some(txn) = held.take() {
            held = Some(txn.spool(cfg.clone()).await?);
        }
    }
    Ok(TransactionPage { ready, held, new_hold })
}

/// Make a pending shape live: register its routing, then replay its buffered deltas through the
/// snapshot gate — emitting exactly the changes the backfill snapshot did not see. The buffered
/// replay is appended before the sequencer processes any further change, so the shape stream stays
/// in commit order.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn activate_shape(
    ds: &DsClient,
    execs: &mut HashMap<String, TableExec>,
    table: &TableRef,
    shape_id: &str,
    gate: crate::pg::SnapshotGate,
    agg_seed: Option<AggSeed>,
    emitted_seed: u64,
    emitted: &mut HashMap<String, u64>,
    shutdown: &crate::shutdown::ShutdownToken,
) -> std::result::Result<(), ActivateFailure> {
    let exec = execs
        .get_mut(table.as_str())
        .ok_or_else(|| ActivateFailure::Failed(format!("no executor for table '{table}'")))?;
    let p = exec
        .pending
        .remove(shape_id)
        .ok_or_else(|| ActivateFailure::Failed(format!("no pending shape '{shape_id}' (aborted?)")))?;
    if p.overflowed {
        return Err(ActivateFailure::PendingBufferOverflow);
    }
    if emitted_seed > 0 {
        emitted.insert(shape_id.to_string(), emitted_seed);
    }
    match p.kind {
        CreateKind::Plain => {
            // Register routing first (an equality template joins/creates its family's KeyRouter;
            // everything else is a standalone indexed filter)...
            match p.pred.equality_template() {
                Some(pairs) => {
                    let key_cols: Vec<usize> = pairs.iter().map(|(c, _)| *c).collect();
                    let key_tuple = Row(pairs.into_iter().map(|(_, v)| v).collect());
                    let router = exec
                        .families
                        .entry(key_cols.clone())
                        .or_insert_with(|| KeyRouter { key_cols: key_cols.clone(), index: HashMap::new() });
                    router.index.entry(key_tuple.clone()).or_default().push(RoutedShape {
                        num_id: p.num_id,
                        stream_path: p.stream_path.clone(),
                        gate: gate.clone(),
                        out_cols: p.out_cols.clone(),
                    });
                    exec.family_of.insert(shape_id.to_string(), (key_cols, p.num_id, key_tuple));
                }
                None => {
                    exec.shape_index.insert(shape_id, &p.pred);
                    exec.shapes.insert(
                        shape_id.to_string(),
                        StandaloneShape {
                            pred: p.pred.clone(),
                            stream_path: p.stream_path.clone(),
                            gate: gate.clone(),
                            out_cols: p.out_cols.clone(),
                        },
                    );
                }
            }
            // ...then drain the buffer through the gate. `matches()` evaluates equality templates
            // and standalone predicates alike, so one replay path covers both placements.
            let mut outs: Vec<Envelope> = Vec::new();
            for env in &p.buffered {
                let Ok((delta, txid, lsn)) = apply_envelope(&exec.ts, env) else { continue };
                if delta.is_empty() {
                    continue;
                }
                let lsn_u64 = lsn.as_deref().map(crate::pg::lsn_to_u64).unwrap_or(0);
                let xid = txid.as_deref().and_then(|s| s.parse::<u64>().ok());
                if gate.should_skip(lsn_u64, xid) {
                    continue;
                }
                let matched = eval_standalone(&p.pred, &delta);
                if matched.is_empty() {
                    continue;
                }
                outs.extend(translate_output(&exec.ts, matched, txid, lsn, p.out_cols.as_deref().map(Vec::as_slice)));
            }
            if !outs.is_empty() {
                *emitted.entry(shape_id.to_string()).or_insert(0) += outs.len() as u64;
                ds.append_reliable(&p.stream_path, &outs)
                    .await
                    .map_err(|error| ActivateFailure::Failed(error.to_string()))?;
            }
        }
        CreateKind::Aggregate { func, col } => {
            // Adopt the fold the creator built while STREAMING the backfill (no backfill rows ever
            // existed in one place), emit the initial value, then fold the gated buffer (emitting a
            // value envelope whenever the aggregate moves).
            let seed = agg_seed.unwrap_or_default();
            let mut agg = AggShape {
                pred: p.pred.clone(),
                func,
                col,
                stream_path: p.stream_path.clone(),
                gate: gate.clone(),
                count: seed.count,
                nn_count: seed.nn_count,
                sum: seed.sum,
                multiset: seed.multiset,
                last: None,
            };
            let mut outs = vec![agg.envelope(&exec.ts, None, None)];
            agg.last = Some(agg.value());
            for env in &p.buffered {
                let Ok((delta, txid, lsn)) = apply_envelope(&exec.ts, env) else { continue };
                if delta.is_empty() {
                    continue;
                }
                let lsn_u64 = lsn.as_deref().map(crate::pg::lsn_to_u64).unwrap_or(0);
                let xid = txid.as_deref().and_then(|s| s.parse::<u64>().ok());
                if gate.should_skip(lsn_u64, xid) {
                    continue;
                }
                if agg.apply(&delta) {
                    let val = agg.value();
                    if agg.last.as_ref() != Some(&val) {
                        agg.last = Some(val.clone());
                        outs.push(agg.envelope(&exec.ts, txid, lsn));
                    }
                }
            }
            *emitted.entry(shape_id.to_string()).or_insert(0) += outs.len() as u64;
            // Retried, not propagated on the first failure: at RESTORE this append's error is what
            // makes `apply_catalog` drop and retire an acknowledged aggregate, so one transient 503
            // during a boot used to delete a live subscription permanently.
            ds.append_retrying(&p.stream_path, &outs, DsClient::RESTORE_APPEND_BUDGET, shutdown)
                .await
                .map_err(|e| ActivateFailure::Failed(format!("{e:#}")))?;
            exec.agg_index.insert(shape_id, &agg.pred);
            exec.aggregates.insert(shape_id.to_string(), agg);
        }
    }
    Ok(())
}

/// Replay the global change log from `from` for one dormant shape: apply each of its table's
/// envelopes through the shape's snapshot gate + predicate + projection and append the matches to
/// the retained stream. Pages until the log reports up-to-date on the OPEN segment, following each
/// closed segment's rotation pointer on the way (ADR-0006) exactly as the live loop does. Appends
/// are direct (`ds.append`): a retired stream (404/410/closed) means it vanished (evicted/purged
/// mid-replay) and must fail the resume.
///
/// A resume segment that is GONE (the sweeper deleted it — which it only does once nothing can
/// resume inside it, so this should be unreachable) surfaces as a read error, which fails the
/// resume and drops the shape. That is the right outcome: a shape whose replay start no longer
/// exists can never be brought up to date, and its subscribers must recreate it.
///
/// It deliberately does NOT hold back an unterminated transaction the way the live loop does
/// (ADR-0003). Nothing here depends on transaction boundaries: it filters by table through the
/// shape's snapshot gate and appends **absolute per-pk** rows (`upsert`/`delete` by key), so a
/// partly-appended commit produces a prefix of the same absolute rows and the next page produces
/// the rest — no delta is counted twice and no intermediate state is wrong, only briefly
/// incomplete, on a stream the shape is not live on yet. The live loop's rule is what governs from
/// the moment the shape is registered.
/// Replay one exact cursor page for several shapes of the same table. The caller groups requests
/// with the same `(table, segment, offset)`; each target still applies its own predicate/gate and
/// appends to its own stream. A target append failure is isolated so other waiters can complete.
pub(crate) struct ReplayTarget {
    pub shape_id: String,
    /// Set when the shape is retired underneath the scan (see `Engine::reactivation_cancels`).
    pub purged: Arc<std::sync::atomic::AtomicBool>,
    pub ts: TableSchema,
    pub table: TableRef,
    pub pred: Arc<CompiledPredicate>,
    pub out_cols: Option<Arc<Vec<usize>>>,
    pub gate: crate::pg::SnapshotGate,
    pub stream_path: String,
    pub from: LogPosition,
    pub library_mode: bool,
    pub until: Option<LogPosition>,
}

pub(crate) async fn replay_changes_for_targets(
    ds: &DsClient,
    mut targets: Vec<ReplayTarget>,
    shutdown: &crate::shutdown::ShutdownToken,
) -> Vec<Result<u64>> {
    if targets.is_empty() {
        return Vec::new();
    }
    let table = targets[0].table.clone();
    // Start at the EARLIEST parked cursor in the batch. Targets arrive in touch order, not cursor
    // order, and each one is routed below by comparing its cursor to the page range — so a scan
    // that began at `targets[0]` would silently drop `[earliest, targets[0].from)` for every target
    // parked before it.
    let mut pos = targets
        .iter()
        .map(|target| target.from.clone())
        .min_by(|a, b| {
            a.segment
                .cmp(&b.segment)
                .then_with(|| crate::changelog::offset_bytes(&a.offset).cmp(&crate::changelog::offset_bytes(&b.offset)))
        })
        .expect("targets is non-empty");
    let mut emitted = vec![0u64; targets.len()];
    let mut errors: Vec<Option<anyhow::Error>> = (0..targets.len()).map(|_| None).collect();
    let mut rotate_to = None;
    loop {
        // A target whose shape has been retired is dropped HERE, at a page boundary, rather than at
        // its next append: a selective predicate can match nothing for the whole remaining span, and
        // until this scan ends it holds one of the engine's two reactivation permits.
        for (idx, target) in targets.iter().enumerate() {
            if errors[idx].is_none() && target.purged.load(Ordering::Relaxed) {
                errors[idx] = Some(anyhow::anyhow!(
                    "shape '{}' was retired while its change-log replay was still scanning",
                    target.shape_id
                ));
            }
        }
        if errors.iter().all(Option::is_some) {
            break;
        }
        let page_start = crate::changelog::offset_bytes(&pos.offset);
        let rr = match ds.read_for_table(&pos.path(), &pos.offset, false, table.as_str()).await {
            Ok(rr) => rr,
            Err(e) => {
                let msg = format!("{e:#}");
                for err in &mut errors {
                    if err.is_none() {
                        *err = Some(anyhow::anyhow!("coalesced replay read: {msg}"));
                    }
                }
                break;
            }
        };
        if let Some(n) = crate::changelog::rotation_target_in(&rr.envelopes) {
            rotate_to = Some(n);
        }
        for (idx, target) in targets.iter_mut().enumerate() {
            if errors[idx].is_some() {
                continue;
            }
            // Durable Streams offsets identify the byte cut *between pages*; individual change
            // envelopes do not carry offsets. A target whose parked cursor is after this page's
            // start must wait for a later page. Cursors before this page (including an earlier
            // segment) receive the whole page. This deliberately uses the page range rather than
            // the absent per-envelope `headers.offset` field, which would otherwise duplicate or
            // silently drop rows when coalescing different cursors.
            let target_is_after_page_start = target.from.segment > pos.segment
                || (target.from.segment == pos.segment
                    && match (crate::changelog::offset_bytes(&target.from.offset), page_start) {
                        (Some(target_offset), Some(page_offset)) => target_offset > page_offset,
                        _ => target.from.offset != pos.offset,
                    });
            if target_is_after_page_start {
                continue;
            }
            let mut outs = Vec::new();
            for env in &rr.envelopes {
                if env.type_ != target.table.as_str() {
                    continue;
                }
                let Ok((delta, txid, lsn)) = apply_envelope(&target.ts, env) else { continue };
                let absolute = target.library_mode && needs_absolute_emission(env);
                if delta.is_empty() && !absolute {
                    continue;
                }
                let lsn_u64 = lsn.as_deref().map(crate::pg::lsn_to_u64).unwrap_or(0);
                let xid = txid.as_deref().and_then(|s| s.parse::<u64>().ok());
                if target.gate.should_skip(lsn_u64, xid) {
                    continue;
                }
                if absolute {
                    let held =
                        delta.iter().find(|Tup2(_, w)| *w > 0).map(|Tup2(r, _)| r).filter(|r| target.pred.matches(r));
                    if let Some(e) = absolute_envelope(
                        &target.ts,
                        &env.key,
                        held,
                        txid,
                        lsn,
                        target.out_cols.as_deref().map(Vec::as_slice),
                    ) {
                        outs.push(e);
                    }
                } else {
                    let matched = eval_standalone(&target.pred, &delta);
                    if !matched.is_empty() {
                        outs.extend(translate_output(
                            &target.ts,
                            matched,
                            txid,
                            lsn,
                            target.out_cols.as_deref().map(Vec::as_slice),
                        ));
                    }
                }
            }
            if !outs.is_empty() {
                emitted[idx] += outs.len() as u64;
                if let Err(e) =
                    ds.append_retrying(&target.stream_path, &outs, DsClient::RESTORE_APPEND_BUDGET, shutdown).await
                {
                    errors[idx] = Some(e.context("append coalesced replay"));
                }
            }
        }
        let advanced = rr.next_offset.as_deref().is_some_and(|n| n != pos.offset);
        if let (Some(start), Some(end)) =
            (page_start, rr.next_offset.as_deref().and_then(crate::changelog::offset_bytes))
        {
            ds.metrics().reactivation_bytes_scanned.fetch_add(end.saturating_sub(start), Ordering::Relaxed);
        }
        if let Some(n) = rr.next_offset {
            pos.offset = n;
        }
        if targets.iter().all(|target| target.until.as_ref().is_some_and(|until| pos >= *until)) {
            break;
        }
        if rr.closed && (rr.envelopes.is_empty() || !advanced) {
            let next = match rotate_to.take() {
                Some(n) => n,
                None => match crate::changelog::next_segment_for_reader(ds, pos.segment).await {
                    Ok(n) => n,
                    Err(e) => {
                        let msg = format!("{e:#}");
                        for err in &mut errors {
                            if err.is_none() {
                                *err = Some(anyhow::anyhow!("{msg}"));
                            }
                        }
                        break;
                    }
                },
            };
            pos = LogPosition::start_of(next);
            continue;
        }
        if !advanced || (rr.up_to_date && !rr.closed) {
            break;
        }
    }
    errors.into_iter().zip(emitted).map(|(err, n)| err.map_or(Ok(n), Err)).collect()
}

/// Creator-side half of the two-phase shape creation: await the pending-buffer ack, **stream** the
/// Postgres backfill on a pooled connection (appending it chunk by chunk for a plain shape, folding
/// it for an aggregate), then activate. The sequencer keeps processing other work the whole time —
/// a slow backfill only delays THIS shape. Returns the creation outcome (`Err(reason)` mirrors the
/// old handshake).
///
/// **Nothing here ever holds a whole backfill.** The snapshot arrives in chunks bounded by
/// `ELECTRIC_CIRCUITS_BACKFILL_APPEND_BYTES`, and each chunk is appended (or folded) and dropped
/// before the next is read, so the creator's memory is one chunk for a table of any size. Chunking
/// needs no protocol change: the shape is PENDING until `ActivateShape` lands, so a partly-appended
/// snapshot is on a stream no subscriber is reading yet, and a failure aborts the pending shape and
/// rolls the whole creation back exactly as before.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn backfill_and_activate(
    ds: &DsClient,
    pg_url: &Option<crate::pg::Pool>,
    cmd_tx: &mpsc::UnboundedSender<SequencerCmd>,
    ts: &TableSchema,
    table: &TableRef,
    shape_id: &str,
    stream_path: &str,
    pred: &Arc<CompiledPredicate>,
    out_cols: Option<&Arc<Vec<usize>>>,
    changes_only: bool,
    // `Some((func, col))` makes this an aggregate: the backfill is FOLDED into an `AggSeed` rather
    // than appended as snapshot envelopes.
    aggregate: Option<(AggFn, Option<usize>)>,
    shutdown: &crate::shutdown::ShutdownToken,
    ack_rx: tokio::sync::oneshot::Receiver<LogPosition>,
) -> anyhow::Result<BackfillStats> {
    let abort = || {
        let _ = cmd_tx.send(SequencerCmd::AbortShape { table: table.clone(), shape_id: shape_id.to_string() });
    };
    if ack_rx.await.is_err() {
        anyhow::bail!("sequencer dropped the begin-shape ack");
    }
    // Backfill: current matching rows from a REPEATABLE READ snapshot, predicate pushed into the
    // SELECT; `matches()` is the final authority (a safety net if the SQL is ever a looser
    // superset). A `changes_only` feed skips the backfill and forwards only future matches
    // (passthrough gate) — the non-materialized live tail a subset query follows.
    let (gate, agg_seed, emitted_seed, stats) = if changes_only {
        (crate::pg::SnapshotGate::passthrough(), None, 0u64, BackfillStats::default())
    } else {
        let t0 = std::time::Instant::now();
        match stream_backfill(ds, pg_url, ts, pred, out_cols, stream_path, aggregate, shutdown, t0).await {
            Ok(v) => v,
            Err(e) => {
                abort();
                return Err(e);
            }
        }
    };
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    if cmd_tx
        .send(SequencerCmd::ActivateShape {
            table: table.clone(),
            shape_id: shape_id.to_string(),
            gate,
            agg_seed,
            emitted_seed,
            ready: ready_tx,
        })
        .is_err()
    {
        anyhow::bail!("sequencer is gone");
    }
    ready_rx
        .await
        .unwrap_or_else(|_| Err(ActivateFailure::Failed("sequencer dropped the ready channel".to_string())))
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    Ok(stats)
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct BackfillStats {
    pub rows: u64,
    pub estimated_bytes: u64,
}

/// What a create is refused with when a shutdown interrupts it. The client's move is to retry
/// against the next process, so it reads as a retry instruction, not as a failure of the request.
pub(crate) const SHUTTING_DOWN: &str = "engine is shutting down; retry the create against the next process";

/// Stream one shape's backfill: chunk in, chunk out, nothing accumulated.
///
/// For a plain shape each chunk is filtered, translated and appended to the (still pending) shape
/// stream; for an aggregate each chunk is folded into the seed. Returns the snapshot gate, the
/// aggregate seed, and how many envelopes the snapshot appended (which seeds the shape's emit
/// counter).
#[allow(clippy::too_many_arguments)]
async fn stream_backfill(
    ds: &DsClient,
    pg_url: &Option<crate::pg::Pool>,
    ts: &TableSchema,
    pred: &Arc<CompiledPredicate>,
    out_cols: Option<&Arc<Vec<usize>>>,
    stream_path: &str,
    aggregate: Option<(AggFn, Option<usize>)>,
    shutdown: &crate::shutdown::ShutdownToken,
    t0: std::time::Instant,
) -> anyhow::Result<(crate::pg::SnapshotGate, Option<AggSeed>, u64, BackfillStats)> {
    // Library/no-source mode: the shape simply starts empty (and an aggregate starts at its
    // empty-set value), exactly as the materialising version did.
    let Some(pool) = pg_url.as_ref() else {
        return Ok((
            crate::pg::SnapshotGate::passthrough(),
            aggregate.map(|_| AggSeed::default()),
            0,
            BackfillStats::default(),
        ));
    };
    // Typed errors all the way out: a snapshot that could not settle (`pg::SnapshotUnsettled`) must
    // reach the HTTP layer as itself — a retryable 503 — not as a string it can only answer 500 to.
    let mut client = pool.get().await?;
    let scope = crate::pg::SettleScope::request(&ts.table);
    let mut reader = crate::pg::backfill_reader(&mut client, ts, Some(pred.as_ref()), &scope).await?;

    let mut agg_seed = aggregate.map(|_| AggSeed::default());
    let mut rows_total = 0u64;
    let mut snapshot_bytes = 0u64;
    let mut emitted_seed = 0u64;
    let mut appends = 0u64;
    loop {
        // A chunk boundary is this loop's safe point. Without it a backfill over a large table is
        // an un-interruptible span of the termination grace — the exact shape of the boot's
        // un-raced connect. Aborting costs nothing: the shape is still PENDING, so the caller's
        // rollback removes the partly-appended stream and the client simply creates it again.
        if shutdown.is_shutting_down() {
            anyhow::bail!(SHUTTING_DOWN);
        }
        let Some(chunk) = reader.next_chunk().await? else { break };
        rows_total += chunk.len() as u64;
        match (&mut agg_seed, aggregate) {
            // The sequencer no longer receives rows for an aggregate: the fold happens here, one
            // chunk at a time, and the chunk is dropped.
            (Some(seed), Some((func, col))) => seed.fold_rows(pred.as_ref(), func, col, &chunk),
            _ => {
                let out: Vec<(Row, ZWeight)> = chunk.into_iter().filter(|r| pred.matches(r)).map(|r| (r, 1)).collect();
                if out.is_empty() {
                    continue;
                }
                let envs = translate_output(ts, out, None, None, out_cols.map(|c| c.as_slice()));
                if crate::statsd::enabled() {
                    snapshot_bytes += envs_bytes(&envs);
                }
                emitted_seed += envs.len() as u64;
                ds.append(stream_path, &envs).await.context("append snapshot")?;
                appends += 1;
            }
        }
    }
    // A backfill that fit in one append contributes 0, same accounting as a chunked commit's.
    if appends > 1 {
        ds.metrics().backfill_chunked_appends.fetch_add(appends, Ordering::Relaxed);
        tracing::info!(
            table = %ts.table,
            rows = rows_total,
            chunks = appends,
            duration_ms = t0.elapsed().as_secs_f64() * 1000.0,
            "large backfill appended in chunks"
        );
    }
    let estimated_bytes = reader.estimated_bytes_read();
    let fences = reader.finish().await;
    if agg_seed.is_none() {
        ds.metrics().emitter().snapshot_stored(rows_total, snapshot_bytes, t0.elapsed().as_secs_f64() * 1000.0);
    }
    Ok((fences.gate, agg_seed, emitted_seed, BackfillStats { rows: rows_total, estimated_bytes }))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_envelope(
    metrics: &crate::metrics::Metrics,
    ts: &TableSchema,
    shapes: &HashMap<String, StandaloneShape>,
    shape_index: &StandaloneIndex,
    families: &HashMap<Vec<usize>, KeyRouter>,
    aggregates: &mut HashMap<String, AggShape>,
    agg_index: &StandaloneIndex,
    env: Envelope,
    pending: &mut HashMap<String, Vec<Envelope>>,
    subq: &SubqueryHandle,
    trace_tx: &tokio::sync::broadcast::Sender<Arc<String>>,
    library_mode: bool,
) -> Result<()> {
    let (delta, txid, lsn) = apply_envelope(ts, &env)?;
    // No before-image (library mode only — see `needs_absolute_emission`): the plain tiers below
    // decide membership ABSOLUTELY per pk instead of from the delta, so a delete with no delta at
    // all still has work to do.
    let absolute = library_mode && needs_absolute_emission(&env);
    if delta.is_empty() && !absolute {
        return Ok(());
    }
    // Per-envelope trace collection (hops, reached shape ids). `None` when nobody is subscribed,
    // so the untraced hot path pays only this one atomic load — see `crate::trace`.
    let mut tr: Option<(Vec<crate::trace::TraceHop>, Vec<String>)> = if trace_tx.receiver_count() > 0 {
        Some((vec![crate::trace::TraceHop::new(format!("table:{}", ts.table), "passed")], Vec::new()))
    } else {
        None
    };
    // `lsn` (the commit-LSN string) is stamped onto output envelopes so a subset client can position
    // its live tail at the page snapshot (drop deltas with `lsn < snapshot_lsn`); `lsn_u64` is the
    // numeric fallback for the per-shape backfill-skip compare, and `xid` (the transaction id the
    // ingestor stamps as `txid`) is the primary fence — see `pg::SnapshotGate` for why xid visibility,
    // not LSN order, is the sound backfill↔replication reconciliation.
    let lsn_u64 = lsn.as_deref().map(crate::pg::lsn_to_u64).unwrap_or(0);
    let xid = txid.as_deref().and_then(|s| s.parse::<u64>().ok());
    metrics.envelopes.fetch_add(1, Ordering::Relaxed);
    let _t = Timer::new(&metrics.process_envelope);
    // Standalone shapes: evaluate each stateless filter directly on the delta (no thread, no clone).
    // Skip changes already visible to the shape's backfill snapshot (xid-visibility gate, LSN
    // fallback for changes without a parseable xid). On the untraced hot path only the index's
    // candidates are visited (a non-candidate's necessary conjunct fails, so it cannot match);
    // with a trace subscriber the full scan is kept so every filter node still reports a hop.
    //
    // ABSOLUTE emission takes over both plain tiers when the envelope has no before-image: the
    // row's current value (`row_now`, `None` for a delete) decides each shape's membership
    // outright — matches ⇒ `upsert`, otherwise ⇒ `delete <key>`. It costs a visit to EVERY shape
    // on the table (a shape the row no longer matches is exactly the one that must be told to drop
    // the key, so an index of *candidates* cannot find it), which is the price of not having an
    // old row. On the LIVE path that price is almost never paid: the per-key view is exact from
    // boot, so an old-less delete/update here means a key the change log never carried.
    let row_now: Option<&Row> = delta.iter().find(|Tup2(_, w)| *w > 0).map(|Tup2(r, _)| r);
    if absolute {
        for (sid, shape) in shapes.iter() {
            if shape.gate.should_skip(lsn_u64, xid) {
                if let Some((hops, _)) = tr.as_mut() {
                    hops.push(crate::trace::TraceHop::new(format!("filter:{sid}"), "dropped"));
                }
                continue;
            }
            let held = row_now.filter(|r| shape.pred.matches(r));
            if let Some((hops, ids)) = tr.as_mut() {
                hops.push(crate::trace::TraceHop::new(
                    format!("filter:{sid}"),
                    if held.is_some() { "passed" } else { "dropped" },
                ));
                hops.push(crate::trace::TraceHop::new(format!("shape:{sid}"), "passed"));
                ids.push(sid.clone());
            }
            if let Some(e) = absolute_envelope(
                ts,
                &env.key,
                held,
                txid.clone(),
                lsn.clone(),
                shape.out_cols.as_deref().map(Vec::as_slice),
            ) {
                pending.entry(shape.stream_path.clone()).or_default().push(e);
            }
        }
        let _s = Timer::new(&metrics.family_step);
        for router in families.values() {
            // A routed shape's membership IS the key match, so the row belongs to exactly the one
            // key group its CURRENT value names; every other group must drop it.
            let new_key = row_now.map(|r| key_of(r, &router.key_cols));
            for (key, routed) in router.index.iter() {
                let held = if new_key.as_ref() == Some(key) { row_now } else { None };
                for rs in routed {
                    if rs.gate.should_skip(lsn_u64, xid) {
                        continue;
                    }
                    if let Some((hops, ids)) = tr.as_mut() {
                        let sid = format!("s{}", rs.num_id);
                        hops.push(crate::trace::TraceHop::new(format!("shape:{sid}"), "passed"));
                        ids.push(sid);
                    }
                    if let Some(e) = absolute_envelope(
                        ts,
                        &env.key,
                        held,
                        txid.clone(),
                        lsn.clone(),
                        rs.out_cols.as_deref().map(Vec::as_slice),
                    ) {
                        pending.entry(rs.stream_path.clone()).or_default().push(e);
                    }
                }
            }
        }
    }
    let candidate_ids;
    let candidates: Box<dyn Iterator<Item = (&String, &StandaloneShape)>> = if absolute {
        Box::new(std::iter::empty())
    } else if tr.is_some() {
        Box::new(shapes.iter())
    } else {
        candidate_ids = shape_index.candidates(&delta);
        Box::new(candidate_ids.iter().filter_map(|sid| shapes.get_key_value(sid)))
    };
    for (sid, shape) in candidates {
        if shape.gate.should_skip(lsn_u64, xid) {
            if let Some((hops, _)) = tr.as_mut() {
                hops.push(crate::trace::TraceHop::new(format!("filter:{sid}"), "dropped"));
            }
            continue;
        }
        let out = eval_standalone(&shape.pred, &delta);
        if out.is_empty() {
            if let Some((hops, _)) = tr.as_mut() {
                hops.push(crate::trace::TraceHop::new(format!("filter:{sid}"), "dropped"));
            }
            continue;
        }
        if let Some((hops, ids)) = tr.as_mut() {
            hops.push(crate::trace::TraceHop::new(format!("filter:{sid}"), "passed"));
            hops.push(crate::trace::TraceHop::new(format!("shape:{sid}"), "passed"));
            ids.push(sid.clone());
        }
        let envs = translate_output(ts, out, txid.clone(), lsn.clone(), shape.out_cols.as_deref().map(Vec::as_slice));
        pending.entry(shape.stream_path.clone()).or_default().extend(envs);
    }
    // Equality routers: route each delta row by its key to exactly the shapes registered on that key.
    // No table copy, no join state — membership is the key match (an equality-template predicate matches a
    // row iff its key equals the shape's constants). Each shape's own snapshot gate is applied, so
    // changes already in that shape's backfill are skipped.
    let _s = Timer::new(&metrics.family_step);
    for router in families.values().filter(|_| !absolute) {
        type ShapeOut<'a> = (&'a str, Option<&'a [usize]>, Vec<(Row, ZWeight)>);
        let mut by_shape: HashMap<u64, ShapeOut> = HashMap::new();
        let mut routed_keys: Vec<Row> = Vec::new();
        for Tup2(row, w) in &delta {
            let key = key_of(row, &router.key_cols);
            let Some(routed) = router.index.get(&key) else { continue };
            if tr.is_some() && !routed_keys.contains(&key) {
                routed_keys.push(key);
            }
            for rs in routed {
                if rs.gate.should_skip(lsn_u64, xid) {
                    continue;
                }
                by_shape
                    .entry(rs.num_id)
                    .or_insert_with(|| (rs.stream_path.as_str(), rs.out_cols.as_deref().map(Vec::as_slice), Vec::new()))
                    .2
                    .push((row.clone(), *w));
            }
        }
        if let Some((hops, ids)) = tr.as_mut() {
            // Node id matches the visualizer's logical graph: family:<table>:<key cols by name>.
            let cols = router
                .key_cols
                .iter()
                .map(|i| ts.columns.get(*i).map(|(n, _)| n.clone()).unwrap_or_else(|| format!("col{i}")))
                .collect::<Vec<_>>()
                .join(",");
            let node = format!("family:{}:{cols}", ts.table);
            if by_shape.is_empty() {
                hops.push(crate::trace::TraceHop::new(node, "dropped"));
            } else {
                for key in &routed_keys {
                    let key_json = serde_json::Value::Array(key.0.iter().map(crate::value::Value::to_json).collect());
                    hops.push(crate::trace::TraceHop::routed(node.clone(), key_json));
                }
                for num_id in by_shape.keys() {
                    let sid = format!("s{num_id}");
                    hops.push(crate::trace::TraceHop::new(format!("shape:{sid}"), "passed"));
                    ids.push(sid);
                }
            }
        }
        if by_shape.is_empty() {
            continue;
        }
        metrics.family_steps.fetch_add(1, Ordering::Relaxed);
        for (_sid, (stream_path, out_cols, rows)) in by_shape {
            let envs = translate_output(ts, rows, txid.clone(), lsn.clone(), out_cols);
            if !envs.is_empty() {
                pending.entry(stream_path.to_string()).or_default().extend(envs);
            }
        }
    }
    // A delete with no before-image carries no delta at all, so the subquery registry and the
    // aggregate folds have nothing to apply — and nothing they COULD apply, since correcting them
    // needs the old row. That is sound rather than a gap: the per-key view supplies the old row for
    // every key the change log actually carried (so a real delete DOES reach the folds), an
    // aggregate never goes dormant so the replay path never feeds one, and a delete for a key that
    // was never inserted must move no aggregate anyway.
    if delta.is_empty() {
        publish_envelope_trace(trace_tx, tr, ts, &delta, &txid, &lsn);
        return Ok(());
    }
    // Subquery shapes/nodes: route this delta through the cross-table registry. Under the lock it
    // updates the shared inner-set nodes (in-memory) and emits outer-shape deltas; the flip-driven
    // Postgres query-backs are handed to the engine's flip-propagator task so they never block
    // this tailer. The convergence barrier is processed offsets + a drained flip queue
    // (`pending_flips == 0`).
    {
        let mut group_paths = HashSet::new();
        let (result, staged_group_emissions) = {
            let mut reg = subq.registry.lock().await;
            let result = if reg.touches(&ts.table) {
                let mut sq_hops: Option<Vec<crate::trace::TraceHop>> = tr.as_ref().map(|_| Vec::new());
                let result = reg.on_table_delta(ts, &delta, lsn_u64, xid, txid.clone(), sq_hops.as_mut()).await;
                if let Ok(result_work) = result.as_ref() {
                    group_paths = reg.group_paths_for_work(result_work);
                    for path in &group_paths {
                        subq.group_lanes.begin_origin(path, lsn_u64);
                    }
                    if let (Some((hops, ids)), Some(sq)) = (tr.as_mut(), sq_hops) {
                        for h in &sq {
                            if h.outcome == "passed"
                                && let Some(sid) = h.node.strip_prefix("shape:")
                                && !ids.iter().any(|i| i == sid)
                            {
                                ids.push(sid.to_string());
                            }
                        }
                        hops.extend(sq);
                    }
                }
                result
            } else {
                Ok(std::collections::VecDeque::new())
            };
            (result, reg.take_group_emissions())
        };
        let work = match result {
            Ok(work) => {
                // Reserved in commit order under the registry lock; the lane admits them in that
                // order, so the sequencer never waits on a group's window.
                subq.group_lanes.spawn_wait(staged_group_emissions);
                work
            }
            Err(error) => {
                drop(staged_group_emissions);
                return Err(error);
            }
        };
        if !work.is_empty() {
            subq.pending_flips.add(lsn_u64);
            if subq
                .flip_tx
                .send(FlipWork::Walk {
                    work,
                    txid: txid.clone(),
                    lsn: lsn.clone(),
                    group_paths: group_paths.iter().cloned().collect(),
                })
                .is_err()
            {
                // Propagator gone (shutdown) — don't leave the barrier stuck.
                subq.pending_flips.complete(lsn_u64);
                for path in &group_paths {
                    subq.group_lanes.complete_origin(path, lsn_u64);
                }
            }
        }
    }
    // Scalar aggregations: fold this delta into each *candidate* aggregate (necessary-conjunct
    // index — a non-candidate's predicate provably matches no delta row, so skipping it leaves
    // the fold unchanged); emit the new value when it changes. Skips changes already counted in
    // the seed (the aggregate's snapshot gate). Under an attached trace subscriber the index is
    // bypassed (like the standalone tier) so every aggregate node reports a folded/dropped hop.
    let agg_candidates: Option<HashSet<String>> =
        if tr.is_none() { Some(agg_index.candidates(&delta).into_iter().collect()) } else { None };
    for (sid, agg) in aggregates.iter_mut() {
        if let Some(c) = &agg_candidates {
            if !c.contains(sid) {
                continue;
            }
        }
        if agg.gate.should_skip(lsn_u64, xid) {
            if let Some((hops, _)) = tr.as_mut() {
                hops.push(crate::trace::TraceHop::new(format!("shape:{sid}"), "dropped"));
            }
            continue;
        }
        let mut folded = false;
        if agg.apply(&delta) {
            let val = agg.value();
            if agg.last.as_ref() != Some(&val) {
                agg.last = Some(val.clone());
                let env = agg.envelope(ts, txid.clone(), lsn.clone());
                pending.entry(agg.stream_path.clone()).or_default().push(env);
                folded = true;
            }
        }
        if let Some((hops, ids)) = tr.as_mut() {
            hops.push(crate::trace::TraceHop::new(format!("shape:{sid}"), if folded { "folded" } else { "dropped" }));
            if folded {
                ids.push(sid.clone());
            }
        }
    }
    publish_envelope_trace(trace_tx, tr, ts, &delta, &txid, &lsn);
    Ok(())
}

/// Publish one envelope's trace event (serialize once; lossy send — see `crate::trace`). No-op when
/// nothing was collected, i.e. when nobody is subscribed.
fn publish_envelope_trace(
    trace_tx: &tokio::sync::broadcast::Sender<Arc<String>>,
    tr: Option<(Vec<crate::trace::TraceHop>, Vec<String>)>,
    ts: &TableSchema,
    delta: &[Tup2<Row, ZWeight>],
    txid: &Option<String>,
    lsn: &Option<String>,
) {
    let Some((hops, shape_ids)) = tr else { return };
    let ev = crate::trace::TraceEvent {
        lsn: lsn.clone(),
        txid: txid.clone(),
        table: ts.table.clone(),
        delta: delta
            .iter()
            .take(crate::trace::DELTA_CAP)
            .map(|Tup2(row, w)| crate::trace::TraceDelta { row: ts.row_to_json(row), w: *w })
            .collect(),
        hops,
        shapes: shape_ids,
    };
    if let Ok(json) = serde_json::to_string(&ev) {
        let _ = trace_tx.send(Arc::new(json));
    }
}

/// Total serialized byte size of a set of output envelopes (for storage/snapshot byte metrics).
pub(crate) fn envs_bytes(envs: &[Envelope]) -> u64 {
    envs.iter().map(|e| serde_json::to_string(e).map(|s| s.len() as u64).unwrap_or(0)).sum()
}

/// Emit the per-source-transaction storage StatsD metrics from one txn's staged appends.
/// `affected_shape_count` = distinct shape streams the txn touched; `operations`/`bytes` = output
/// envelopes appended + their serialized size. (Subquery-registry appends go out synchronously inside
/// `process_envelope` and are not reflected here.) No-op when the txn produced no appends.
pub(crate) fn emit_storage_txn_metrics(
    metrics: &crate::metrics::Metrics,
    txn_pending: &HashMap<String, Vec<Envelope>>,
) {
    let ops: u64 = txn_pending.values().map(|v| v.len() as u64).sum();
    if ops == 0 {
        return;
    }
    let bytes: u64 =
        txn_pending.values().flatten().map(|e| serde_json::to_string(e).map(|s| s.len() as u64).unwrap_or(0)).sum();
    metrics.emitter().storage_txn(ops, bytes, txn_pending.len() as u64);
}

/// Flush the batch's staged appends, bounded-concurrently. Each envelope keeps its own txid, so
/// `awaitTxId` semantics are preserved; only the HTTP round-trips are coalesced + parallelized.
///
/// Appends are **reliable**: transient failures retry with backoff (`append_reliable`) rather than
/// being dropped — a lost shape append is a permanent divergence for that shape's subscribers, and
/// the tailer's processed-offset barrier (published after this returns) must mean "every subscriber
/// stream reflects the batch". The only non-retried case is a retired stream (404/410/closed — the
/// shape was dropped or evicted mid-flush), which discards cleanly.
pub(crate) async fn flush_pending(ds: &DsClient, pending: HashMap<String, Vec<Envelope>>) -> Result<()> {
    const CAP: usize = 32; // bound in-flight appends so we don't swamp the storage server
    let mut items: Vec<(String, Vec<Envelope>)> = pending.into_iter().collect();
    while !items.is_empty() {
        let take = items.len().min(CAP);
        let batch = items.split_off(items.len() - take);
        let mut set = tokio::task::JoinSet::new();
        for (path, envs) in batch {
            let ds = ds.clone();
            set.spawn(async move {
                let _t = Timer::new(&ds.metrics().append);
                ds.append_reliable(&path, &envs).await?;
                ds.metrics().shape_appends.fetch_add(1, Ordering::Relaxed);
                Ok::<_, anyhow::Error>(())
            });
        }
        while let Some(result) = set.join_next().await {
            result??;
        }
    }
    Ok(())
}

#[cfg(test)]
mod txn_boundary_tests {
    use super::*;
    use crate::ds::EnvelopeHeaders;

    /// One change-log envelope as the ingestor stamps it.
    fn env(lsn: &str, txid: &str, seq: u64, last: bool) -> Envelope {
        Envelope {
            type_: "public.t".into(),
            key: seq.to_string(),
            value: None,
            old: None,
            headers: EnvelopeHeaders {
                operation: "insert".into(),
                txid: Some(txid.into()),
                offset: None,
                lsn: Some(lsn.into()),
                seq: Some(seq),
                last: last.then_some(true),
            },
        }
    }

    /// A library-mode write: no transaction structure at all (no `seq`).
    fn lib_env(key: &str) -> Envelope {
        Envelope {
            type_: "public.t".into(),
            key: key.into(),
            value: None,
            old: None,
            headers: EnvelopeHeaders {
                operation: "insert".into(),
                txid: None,
                offset: None,
                lsn: None,
                seq: None,
                last: None,
            },
        }
    }

    /// A page that ends on a marked envelope is entirely processable; one that does not has its
    /// trailing run held back, from the run's FIRST envelope — never mid-transaction.
    #[test]
    fn a_page_ending_mid_transaction_holds_back_the_whole_trailing_run() {
        // Complete: nothing held.
        let done = vec![env("0/10", "1", 0, false), env("0/10", "1", 1, true)];
        assert_eq!(unterminated_tail(&done), None);

        // Incomplete: the trailing run starts at index 0.
        let open = vec![env("0/10", "1", 0, false), env("0/10", "1", 1, false)];
        assert_eq!(unterminated_tail(&open), Some(0));

        // A complete transaction followed by the first chunk of the next: only the second is held.
        let mixed = vec![env("0/10", "1", 0, true), env("0/20", "2", 0, false), env("0/20", "2", 1, false)];
        assert_eq!(unterminated_tail(&mixed), Some(1));

        // Empty page: nothing to hold.
        assert_eq!(unterminated_tail(&[]), None);
    }

    /// A library-mode envelope is a one-envelope transaction with no `seq`. Holding one would wait
    /// forever for a marker no producer sends, so it is never held.
    #[test]
    fn an_envelope_with_no_transaction_structure_is_never_held() {
        assert_eq!(unterminated_tail(&[lib_env("a")]), None);
        assert_eq!(unterminated_tail(&[env("0/10", "1", 0, true), lib_env("a")]), None);
    }

    /// While a run is held, everything PUBLISHED — the restart point, `GET /tables/{n}/offset`, the
    /// segment-deletion floor — stays at the page the run began in, even after the read cursor has
    /// moved on (and even across a segment crossing). Otherwise a crash would resume past a
    /// transaction that was never applied.
    #[test]
    fn publication_is_pinned_to_where_a_held_run_began() {
        let cursor = LogPosition { segment: 3, offset: "99".into() };
        assert_eq!(published(&cursor, &None), cursor, "nothing held: the read cursor is published");

        let held_from = LogPosition { segment: 2, offset: "40".into() };
        assert_eq!(published(&cursor, &Some(held_from.clone())), held_from);
    }
}

#[cfg(test)]
mod source_fence_tests {
    use super::*;

    // Exercise the real barrier with named commit positions and deterministic completion.
    fn commits_pending(positions: &[u64]) -> SubqueryHandle {
        let subq = handle(0);
        for &position in positions {
            subq.pending_flips.add(position);
        }
        subq
    }

    async fn drain_through(subq: &SubqueryHandle, shutdown: &crate::shutdown::ShutdownToken, marker: u64) -> bool {
        wait_for_source_effects(subq, shutdown, marker).await
    }

    fn complete_commit(subq: &SubqueryHandle, position: u64) {
        subq.pending_flips.complete(position);
    }

    #[test]
    fn replay_boundary_waits_for_durable_receipt_before_publishing() {
        let subq = handle(0);
        let receipts = Arc::new(ReceiptBarrier::default());
        receipts.register(0x20);
        let start = LogPosition::start();
        let next = LogPosition::start_of(1);
        let processed = Arc::new(std::sync::Mutex::new(start.clone()));
        let mut gate = PublishGate::new(start.clone(), None, receipts.clone());
        gate.publish(&subq.group_lanes, &processed, next.clone(), Some((0x20, 0)));
        assert_eq!(*processed.lock().unwrap(), start);
        let lanes = crate::engine::emission::GroupEmissionLanes::new(
            crate::ds::DsClient::with_test_store(
                "scripted://publish-gate".into(),
                Arc::new(crate::ds::ScriptedStore::default()),
            ),
            crate::shutdown::ShutdownToken::new(),
            Arc::new(PendingFlips::default()),
        );
        assert_eq!(gate.checkpoint(&lanes, next.clone(), Some((0x20, 0))), (start.clone(), None));
        receipts.complete(0x20);
        assert!(gate.release(&subq.group_lanes, &processed, &crate::shutdown::ShutdownToken::new()));
        assert_eq!(*processed.lock().unwrap(), next);
    }

    #[tokio::test(start_paused = true)]
    async fn a_later_flip_does_not_delay_an_earlier_marker() {
        use std::future::Future;
        let subquery = commits_pending(&[0x30]);
        let shutdown = crate::shutdown::ShutdownToken::new();
        let drain = drain_through(&subquery, &shutdown, 0x20);
        tokio::pin!(drain);
        std::future::poll_fn(|cx| {
            assert_eq!(
                drain.as_mut().poll(cx),
                std::task::Poll::Ready(true),
                "a marker at 0/20 must not wait for a pending flip at 0/30"
            );
            std::task::Poll::Ready(())
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn earlier_and_same_commit_flips_hold_only_their_marker_prefix() {
        use std::future::Future;
        let subquery = commits_pending(&[0x10, 0x20, 0x30]);
        let shutdown = crate::shutdown::ShutdownToken::new();
        let drain = drain_through(&subquery, &shutdown, 0x20);
        tokio::pin!(drain);
        std::future::poll_fn(|cx| {
            assert!(drain.as_mut().poll(cx).is_pending(), "earlier effects must hold the marker");
            std::task::Poll::Ready(())
        })
        .await;
        complete_commit(&subquery, 0x10);
        tokio::time::advance(std::time::Duration::from_millis(10)).await;
        std::future::poll_fn(|cx| {
            assert!(drain.as_mut().poll(cx).is_pending(), "same-transaction effects must hold the marker");
            std::task::Poll::Ready(())
        })
        .await;
        complete_commit(&subquery, 0x20);
        tokio::time::advance(std::time::Duration::from_millis(10)).await;
        std::future::poll_fn(|cx| {
            assert_eq!(
                drain.as_mut().poll(cx),
                std::task::Poll::Ready(true),
                "the completed prefix must release its marker while the later flip stays pending"
            );
            std::task::Poll::Ready(())
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_or_degradation_while_waiting_never_releases_a_receipt() {
        use std::future::Future;
        for degrade in [false, true] {
            let subquery = commits_pending(&[0x10]);
            let shutdown = crate::shutdown::ShutdownToken::new();
            let drain = drain_through(&subquery, &shutdown, 0x20);
            tokio::pin!(drain);
            std::future::poll_fn(|cx| {
                assert!(drain.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            if degrade {
                subquery.degrade.mark();
            } else {
                shutdown.begin();
            }
            complete_commit(&subquery, 0x10);
            tokio::time::advance(std::time::Duration::from_millis(10)).await;
            assert!(!drain.await, "finished work must not bypass shutdown or degradation");
        }
    }

    #[tokio::test]
    async fn later_work_cannot_bypass_a_degraded_or_stopping_engine() {
        for degrade in [false, true] {
            let subquery = commits_pending(&[0x30]);
            let shutdown = crate::shutdown::ShutdownToken::new();
            if degrade {
                subquery.degrade.mark();
            } else {
                shutdown.begin();
            }
            assert!(!drain_through(&subquery, &shutdown, 0x20).await);
        }
    }

    #[test]
    fn unknown_origins_are_conservative_and_commits_can_finish_out_of_order() {
        let subquery = commits_pending(&[0, 0x10, 0x10, 0x30]);
        complete_commit(&subquery, 0x30);
        complete_commit(&subquery, 0x10);
        assert!(subquery.pending_flips.has_at_or_before(0x20));
        complete_commit(&subquery, 0x10);
        assert!(subquery.pending_flips.has_at_or_before(0x01), "unknown work holds every prefix");
        complete_commit(&subquery, 0);
        assert!(!subquery.pending_flips.has_at_or_before(u64::MAX));
        assert_eq!(subquery.pending_flips.count(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn runtime_and_source_receipts_publish_at_the_same_commit_prefix() {
        use crate::runtime_authority::{AuthorityMarker, RuntimeFence};
        use std::future::Future;
        let store = Arc::new(crate::ds::ScriptedStore::default());
        let ds = DsClient::with_test_store("scripted://receipt-prefix".into(), store.clone());
        let engine = Engine::new_for_in_process_test(ds);
        let marker = AuthorityMarker::parse(
            "018f5f4d-70c2-7d70-a4d5-5f7355078f85",
            "018f5f4d-70c2-7d70-a4d5-5f7355078f81",
            &"a".repeat(64),
        )
        .unwrap();
        let mut runtime =
            RuntimeFence { marker: marker.clone(), incarnation: engine.runtime_receipts.lock().unwrap().incarnation() }
                .into_envelope();
        runtime.headers.lsn = Some("0/20".into());
        runtime.headers.txid = Some("2".into());
        runtime.headers.seq = Some(0);
        let mut source = runtime.clone();
        source.type_ = crate::replication::SOURCE_FENCE_ENVELOPE.into();
        source.key = "deployment-fence".into();
        source.headers.seq = Some(1);
        source.headers.last = Some(true);
        let start = engine.seq_start.lock().unwrap().clone();
        store.pages_by_offset.lock().unwrap().insert(
            start.offset.clone(),
            ("after-marker".into(), true, serde_json::to_string(&[runtime, source]).unwrap()),
        );
        engine.pending_flips.add(0x10);
        engine.pending_flips.add(0x30);
        let (_commands, rx) = mpsc::unbounded_channel();
        let sequencer = sequencer_loop(
            engine.ds.clone(),
            engine.tables_shared.clone(),
            start.clone(),
            None,
            engine.catalog_tx.clone(),
            engine.source_receipts.clone(),
            engine.last_source_receipt.clone(),
            engine.source_receipt_progress.clone(),
            engine.runtime_receipts.clone(),
            rx,
            Arc::new(std::sync::Mutex::new(start)),
            Default::default(),
            Default::default(),
            engine.subquery_handle(),
            engine.trace_tx.clone(),
            None,
            HashMap::new(),
            true,
            false,
            engine.restore_reads_paused.clone(),
            engine.sequencer_read_failed.clone(),
            engine.retention.pending_buffer_max_bytes,
            engine.txn_cfg.lock().unwrap().clone(),
            None,
            engine.shutdown.clone(),
            engine.shutdown.party("test sequencer"),
        );
        tokio::pin!(sequencer);
        std::future::poll_fn(|cx| {
            assert!(sequencer.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert!(engine.runtime_drain_receipt(&marker).unwrap().is_none());
        assert!(engine.source_drain_receipt("deployment-fence").is_none());
        engine.pending_flips.complete(0x10);
        tokio::time::advance(Duration::from_millis(10)).await;
        std::future::poll_fn(|cx| {
            assert!(sequencer.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert_eq!(engine.runtime_drain_receipt(&marker).unwrap().unwrap().commit_lsn, "0/20");
        // Runtime and source receipts may both have landed by the time the worker gets its
        // scheduling turn. The causal assertion is that the source receipt is only observable
        // after its durable catalog append; the barrier below still pins publication/checkpoint
        // progress until that append completes.
        tokio::time::timeout(Duration::from_secs(1), engine.catalog_tx.wait_durable()).await.unwrap().unwrap();
        std::future::poll_fn(|cx| {
            assert!(sequencer.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert_eq!(engine.source_drain_receipt("deployment-fence").unwrap().commit_lsn, "0/20");
        assert_eq!(engine.pending_flips(), 1, "later work is still pending after both receipts");
        engine.shutdown.begin();
        sequencer.await;
    }

    fn handle(pending: i64) -> SubqueryHandle {
        let (flip_tx, _flip_rx) = mpsc::unbounded_channel();
        let pending_flips = Arc::new(PendingFlips::default());
        for _ in 0..pending {
            pending_flips.add(0);
        }
        let ds = DsClient::new_for_in_process_test("http://127.0.0.1:1");
        let shutdown = crate::shutdown::ShutdownToken::new();
        SubqueryHandle {
            registry: Arc::new(Mutex::new(SubqueryRegistry::new(ds.clone(), None))),
            flip_tx,
            pending_flips: pending_flips.clone(),
            degrade: DegradeState::new(),
            group_lanes: crate::engine::emission::GroupEmissionLanes::new(ds, shutdown, pending_flips.clone()),
        }
    }

    #[tokio::test]
    async fn task_shutdown_cannot_manufacture_a_source_receipt() {
        let subquery = handle(0);
        let shutdown = crate::shutdown::ShutdownToken::new();
        shutdown.begin();
        assert!(!wait_for_source_effects(&subquery, &shutdown, 0x10).await);
    }

    #[tokio::test]
    async fn source_fence_waits_until_deferred_writes_land() {
        let subquery = handle(1);
        let pending = subquery.pending_flips.clone();
        let shutdown = crate::shutdown::ShutdownToken::new();
        let wait = tokio::spawn(async move { wait_for_source_effects(&subquery, &shutdown, 0x10).await });
        tokio::task::yield_now().await;
        assert!(!wait.is_finished(), "a pending deferred write must hold the receipt barrier");
        pending.complete(0);
        assert!(tokio::time::timeout(std::time::Duration::from_secs(1), wait).await.unwrap().unwrap());
    }

    #[tokio::test]
    async fn reset_while_runtime_drain_waits_rejects_its_completed_effects() {
        use crate::runtime_authority::{AuthorityMarker, RuntimeFence, RuntimeReceipts};
        use std::future::Future;
        let mut receipts = RuntimeReceipts::default();
        let incarnation = receipts.incarnation();
        let marker = AuthorityMarker::parse(
            "018f5f4d-70c2-7d70-a4d5-5f7355078f85",
            "018f5f4d-70c2-7d70-a4d5-5f7355078f81",
            &"a".repeat(64),
        )
        .unwrap();
        let subquery = handle(1);
        let shutdown = crate::shutdown::ShutdownToken::new();
        let drain = wait_for_source_effects(&subquery, &shutdown, 0x10);
        tokio::pin!(drain);
        std::future::poll_fn(|cx| {
            assert!(drain.as_mut().poll(cx).is_pending(), "the deferred effect has not landed");
            std::task::Poll::Ready(())
        })
        .await;
        receipts.invalidate();
        subquery.pending_flips.complete(0);
        assert!(drain.await);
        receipts.publish(
            incarnation,
            vec![RuntimeFence { marker: marker.clone(), incarnation }],
            "0/10",
            std::time::Instant::now(),
        );
        assert!(
            receipts.get(&marker, std::time::Instant::now()).is_none(),
            "finishing old work cannot mint current authority"
        );
    }

    #[tokio::test]
    async fn newly_admitted_ingress_after_reset_publishes_only_after_full_drain() {
        use crate::replication::EpochEvents;
        use crate::runtime_authority::{AuthorityMarker, RuntimeFence};
        use std::future::Future;
        let engine = Engine::new_for_in_process_test(DsClient::new_for_in_process_test("http://127.0.0.1:1"));
        let old = engine.before_connect().await.unwrap();
        drop(engine.force_epoch_reset_window().await);
        let current = engine.before_connect().await.unwrap();
        let marker = AuthorityMarker::parse(
            "018f5f4d-70c2-7d70-a4d5-5f7355078f85",
            "018f5f4d-70c2-7d70-a4d5-5f7355078f81",
            &"a".repeat(64),
        )
        .unwrap();
        let subquery = handle(1);
        let shutdown = crate::shutdown::ShutdownToken::new();
        let drain = wait_for_source_effects(&subquery, &shutdown, 0x10);
        tokio::pin!(drain);
        std::future::poll_fn(|cx| {
            assert!(drain.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert!(engine.runtime_drain_receipt(&marker).unwrap().is_none());
        subquery.pending_flips.complete(0);
        assert!(drain.await);
        let mut receipts = engine.runtime_receipts.lock().unwrap();
        receipts.publish(
            current,
            vec![RuntimeFence { marker: marker.clone(), incarnation: old }],
            "0/10",
            std::time::Instant::now(),
        );
        assert!(receipts.get(&marker, std::time::Instant::now()).is_none());
        receipts.publish(
            current,
            vec![RuntimeFence { marker: marker.clone(), incarnation: current }],
            "0/20",
            std::time::Instant::now(),
        );
        assert_eq!(receipts.get(&marker, std::time::Instant::now()).unwrap().commit_lsn, "0/20");
    }

    #[tokio::test]
    async fn degraded_effects_cannot_manufacture_a_source_receipt() {
        let subquery = handle(0);
        subquery.degrade.mark();
        assert!(!wait_for_source_effects(&subquery, &crate::shutdown::ShutdownToken::new(), 0x10).await);
    }
}

#[cfg(test)]
mod pending_buffer_tests {
    use super::*;
    use crate::schema::TableDef;

    fn users() -> TableSchema {
        let def: TableDef = serde_json::from_value(serde_json::json!({
            "columns": { "id": {"type":"int"}, "name": {"type":"text"} },
            "primaryKey": "id"
        }))
        .unwrap();
        TableSchema::from_def(&"users".into(), &def).unwrap()
    }

    fn envelope(key: u64, filler: usize) -> Envelope {
        Envelope {
            type_: "public.users".to_string(),
            key: key.to_string(),
            value: Some(serde_json::json!({ "id": key, "name": "x".repeat(filler) })),
            old: None,
            headers: crate::ds::EnvelopeHeaders {
                operation: "insert".to_string(),
                txid: None,
                offset: None,
                lsn: None,
                seq: None,
                last: None,
            },
        }
    }

    fn pending(ts: &TableSchema, buffered: Vec<Envelope>) -> PendingShape {
        PendingShape {
            num_id: 1,
            stream_path: "shape/s1".to_string(),
            pred: Arc::new(CompiledPredicate::compile_opt(None, ts).unwrap()),
            out_cols: None,
            kind: CreateKind::Plain,
            buffered_bytes: buffered.iter().map(crate::ds::envelope_memory_bytes).sum(),
            buffered,
            overflowed: false,
        }
    }

    /// The buffer is a ceiling, not a ration: past the cap the whole thing goes, because a prefix
    /// of a shape's missing deltas is not a cheaper answer — it is a stream that is silently wrong.
    /// The shape is retired and recreated from a fresh backfill instead.
    #[test]
    fn a_pending_buffer_stops_at_its_cap_and_drops_what_it_held() {
        let ts = users();
        let mut pending_shape = pending(&ts, Vec::new());
        let cap = 8 * 1024;
        let accepted = (0..100).filter(|key| buffer_pending(&mut pending_shape, &envelope(*key, 1024), cap)).count();
        assert!(accepted > 0, "a buffer under its cap still buffers");
        assert!(accepted < 100, "a buffer over its cap stops");
        assert!(pending_shape.overflowed);
        assert!(pending_shape.buffered.is_empty(), "the deltas it was holding are dropped, not truncated");
        assert_eq!(pending_shape.buffered_bytes, 0);
        assert!(
            !buffer_pending(&mut pending_shape, &envelope(101, 1), cap),
            "an overflowed buffer stays overflowed; it cannot half-recover"
        );
    }

    /// `0` is the documented escape hatch, and it has to mean "no ceiling" rather than "zero bytes".
    #[test]
    fn a_zero_cap_means_no_cap() {
        let ts = users();
        let mut pending_shape = pending(&ts, Vec::new());
        for key in 0..100 {
            assert!(buffer_pending(&mut pending_shape, &envelope(key, 1024), 0));
        }
        assert!(!pending_shape.overflowed);
        assert_eq!(pending_shape.buffered.len(), 100);
    }

    /// Everything the sequencer processes for a table while a shape of that table is between
    /// `BeginShape` and `ActivateShape` is cloned into that shape's buffer. That is the term that
    /// grows with the write rate times the replay duration times the permit queue depth, so it is
    /// exactly the term `/memory` has to show.
    #[test]
    fn a_pending_shapes_buffer_is_reported_as_engine_memory() {
        let ts = users();
        let mut exec = TableExec::new(ts.clone());
        let empty = crate::engine::introspection::exec_heap_bytes(&exec);
        let buffered: Vec<Envelope> = (0..100).map(|key| envelope(key, 1024)).collect();
        exec.pending.insert("s1".to_string(), pending(&ts, buffered));
        let with_buffer = crate::engine::introspection::exec_heap_bytes(&exec);
        assert!(
            with_buffer - empty >= 100 * 1024,
            "a pending shape's buffered deltas must be visible as engine memory, not invisible until OOM \
             (reported growth: {} bytes)",
            with_buffer - empty
        );
    }
}

#[cfg(test)]
mod held_spill_tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;

    #[tokio::test]
    async fn shutdown_finishes_a_ready_in_memory_page() {
        // No output streams: isolate page-input completion from independently interruptible
        // reliable appends (covered by shutdown_append_cannot_be_reported_as_a_completed_flush).
        let mut h = Harness::new(u64::MAX);
        let table: TableRef = "public.items".into();
        let def = serde_json::from_value(serde_json::json!({
            "columns":{"id":{"type":"int"},"blob":{"type":"text"}},"primaryKey":"id"
        }))
        .unwrap();
        h.engine.tables_shared.write().unwrap().insert(table.clone(), TableSchema::from_def(&table, &def).unwrap());
        let registry = h.engine.subqueries.clone();
        let held = registry.lock().await;
        let rows: Vec<_> = (1..=64).map(|seq| change(&seq.to_string(), "0/10", seq, true)).collect();
        h.store
            .pages_by_offset
            .lock()
            .unwrap()
            .insert("-1".into(), ("p1".into(), true, serde_json::to_string(&rows).unwrap()));
        std::future::poll_fn(|cx| {
            assert!(h.future.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert_eq!(h.engine.metrics.envelopes.load(Ordering::Relaxed), 1, "first row reached the held registry lock");
        h.engine.shutdown.begin();
        drop(held);
        let checkpoint = h.finish().await;
        assert_eq!(h.engine.metrics.envelopes.load(Ordering::Relaxed), 64, "shutdown must finish ready page input");
        assert_eq!(checkpoint, (LogPosition { segment: 0, offset: "p1".into() }, Some((0x10, 64))));
        assert!(h.files().is_empty());
        h.engine.close(Duration::from_secs(5)).await;
    }

    #[tokio::test]
    async fn memory_budget_sequencer_command_preserves_read_fifo_position() {
        let mut h = Harness::with_budget(u64::MAX, LogPosition::start(), None, Some(1024));
        let budget = h.engine.ds.memory_budget().clone();
        let owner = budget.acquire(1024).await;
        h.store.pages_by_offset.lock().unwrap().insert("-1".into(), ("p1".into(), true, "[]".into()));
        std::future::poll_fn(|cx| {
            assert!(h.future.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert_eq!(budget.waits.load(Ordering::Relaxed), 1, "sequencer queued first");
        let mut peer = Box::pin(budget.acquire(1024));
        std::future::poll_fn(|cx| {
            assert!(peer.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        let (resp, ack) = tokio::sync::oneshot::channel();
        h.commands.send(SequencerCmd::MemBytes { resp }).unwrap();
        std::future::poll_fn(|cx| {
            assert!(h.future.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert_eq!(ack.await.unwrap(), 0, "commands remain serviceable while admission waits");
        drop(owner);
        std::future::poll_fn(|cx| {
            assert!(h.future.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert_eq!(
            h.processed.lock().unwrap().offset,
            "p1",
            "the first queued sequencer page must proceed before the later peer"
        );
        assert_eq!(budget.waits.load(Ordering::Relaxed), 2, "commands must not requeue a read");
        drop(peer.await);
        h.engine.shutdown.begin();
        h.future.as_mut().await;
        h.engine.close(Duration::from_secs(5)).await;
    }

    struct Harness {
        engine: Engine,
        store: Arc<crate::ds::ScriptedStore>,
        dir: std::path::PathBuf,
        future: Pin<Box<dyn Future<Output = ()> + Send>>,
        commands: mpsc::UnboundedSender<SequencerCmd>,
        processed: Arc<std::sync::Mutex<LogPosition>>,
    }

    impl Harness {
        fn new(cap: u64) -> Self {
            Self::restore(cap, LogPosition::start_of(0), None)
        }

        fn restore(cap: u64, start: LogPosition, highwater: Option<(u64, u64)>) -> Self {
            Self::with_budget(cap, start, highwater, None)
        }

        fn with_budget(cap: u64, start: LogPosition, highwater: Option<(u64, u64)>, budget: Option<u64>) -> Self {
            Self::with_mode(cap, start, highwater, budget, false)
        }

        fn with_mode(
            cap: u64,
            start: LogPosition,
            highwater: Option<(u64, u64)>,
            budget: Option<u64>,
            postgres: bool,
        ) -> Self {
            let dir = std::env::temp_dir().join(format!("circuits-held-test-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir(&dir).unwrap();
            let cfg = crate::txn_buffer::TxnBufferConfig::resolve(|key| match key {
                "ELECTRIC_CIRCUITS_HELD_MEMORY_BYTES" => Some(cap.to_string()),
                "ELECTRIC_CIRCUITS_TXN_SPILL_DIR" => Some(dir.to_string_lossy().into_owned()),
                _ => None,
            })
            .unwrap();
            let store = Arc::new(crate::ds::ScriptedStore::default());
            // Keep unknown offsets parked, rather than returning synthetic empty pages in a loop.
            store.pages_by_offset.lock().unwrap().insert("unused".into(), ("unused".into(), true, "[]".into()));
            let mut ds = DsClient::with_test_store("scripted://held".into(), store.clone());
            if let Some(bytes) = budget {
                let config = crate::config::Config::resolve(|key| {
                    (key == "ELECTRIC_CIRCUITS_MEMORY_BUDGET_BYTES").then(|| bytes.to_string())
                })
                .unwrap();
                ds = ds.with_memory_budget(&config);
            }
            let engine = if postgres {
                Engine::new_pg_for_in_process_test(ds, "postgres://postgres@127.0.0.1:55532/postgres".into())
            } else {
                Engine::new_for_in_process_test(ds)
            };
            engine.set_txn_config(cfg);
            let processed = Arc::new(std::sync::Mutex::new(start.clone()));
            let (commands, rx) = mpsc::unbounded_channel();
            let future = Box::pin(sequencer_loop(
                engine.ds.clone(),
                engine.tables_shared.clone(),
                start,
                highwater,
                engine.catalog_tx.clone(),
                engine.source_receipts.clone(),
                engine.last_source_receipt.clone(),
                engine.source_receipt_progress.clone(),
                engine.runtime_receipts.clone(),
                rx,
                processed.clone(),
                Default::default(),
                Default::default(),
                engine.subquery_handle(),
                engine.trace_tx.clone(),
                None,
                HashMap::new(),
                false,
                false,
                engine.restore_reads_paused.clone(),
                engine.sequencer_read_failed.clone(),
                engine.retention.pending_buffer_max_bytes,
                engine.txn_cfg.lock().unwrap().clone(),
                None,
                engine.shutdown.clone(),
                engine.shutdown.party("held test"),
            ));
            Self { engine, store, dir, future, commands, processed }
        }

        async fn page(&mut self, from: &str, to: &str, envelopes: Vec<Envelope>) {
            self.store
                .pages_by_offset
                .lock()
                .unwrap()
                .insert(from.into(), (to.into(), true, serde_json::to_string(&envelopes).unwrap()));
            // Explicitly pause/cancel the fixture's permanently idle read; ordinary commands
            // preserve reads now. Resume at the same cursor with the newly supplied page.
            let (done, _) = tokio::sync::oneshot::channel();
            self.commands.send(SequencerCmd::PauseReads { done }).unwrap();
            let (done, _) = tokio::sync::oneshot::channel();
            self.commands.send(SequencerCmd::ResumeReads { done }).unwrap();
            std::future::poll_fn(|cx| {
                assert!(self.future.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            // The page has entered its processing branch. This command is served only after
            // that branch finishes, including any disk I/O, appends and checkpoint work.
            let (done, ack) = tokio::sync::oneshot::channel();
            self.commands.send(SequencerCmd::Barrier { done }).unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                tokio::select! {
                    _ = &mut self.future => panic!("sequencer stopped while processing page {from}"),
                    result = ack => result.unwrap(),
                }
            })
            .await
            .expect("page processing deadline");
        }

        async fn receive<T>(&mut self, ack: tokio::sync::oneshot::Receiver<T>) -> T {
            tokio::time::timeout(Duration::from_secs(5), async {
                tokio::select! {
                    _ = &mut self.future => panic!("sequencer stopped before command acknowledgement"),
                    result = ack => result.unwrap(),
                }
            })
            .await
            .expect("command deadline")
        }

        async fn shape(&mut self) {
            let table: TableRef = "public.items".into();
            let def = serde_json::from_value(serde_json::json!({
                "columns": {"id":{"type":"int"}, "blob":{"type":"text"}}, "primaryKey":"id"
            }))
            .unwrap();
            let ts = TableSchema::from_def(&table, &def).unwrap();
            self.engine.tables_shared.write().unwrap().insert(table.clone(), ts.clone());
            let (ack, received) = tokio::sync::oneshot::channel();
            self.commands
                .send(SequencerCmd::BeginShape {
                    table: table.clone(),
                    shape_id: "s1".into(),
                    num_id: 1,
                    stream_path: "shape/s1".into(),
                    pred: Arc::new(CompiledPredicate::compile_opt(None, &ts).unwrap()),
                    out_cols: None,
                    kind: CreateKind::Plain,
                    ack,
                })
                .unwrap();
            self.receive(received).await;
            let (ready, received) = tokio::sync::oneshot::channel();
            self.commands
                .send(SequencerCmd::ActivateShape {
                    table,
                    shape_id: "s1".into(),
                    gate: crate::pg::SnapshotGate::passthrough(),
                    agg_seed: None,
                    emitted_seed: 0,
                    ready,
                })
                .unwrap();
            self.receive(received).await.unwrap();
        }

        fn output(&self) -> Vec<Vec<Envelope>> {
            self.store
                .appended
                .lock()
                .unwrap()
                .iter()
                .filter(|(path, _, _)| path.ends_with("shape/s1"))
                .map(|(_, _, body)| serde_json::from_slice(body).unwrap())
                .collect()
        }

        async fn finish(&mut self) -> (LogPosition, Option<(u64, u64)>) {
            self.engine.shutdown.begin();
            tokio::time::timeout(Duration::from_secs(5), &mut self.future).await.expect("shutdown deadline");
            // Drain the catalog writer after the loop queues its final safe checkpoint.
            assert!(self.engine.catalog_tx.drain(Duration::from_secs(5)).await);
            self.store
                .appended
                .lock()
                .unwrap()
                .iter()
                .rev()
                .flat_map(|(_, _, body)| serde_json::from_slice::<Vec<serde_json::Value>>(body).unwrap())
                .filter_map(|value| serde_json::from_value::<CatalogEvent>(value).ok())
                .find_map(|ev| match ev {
                    CatalogEvent::Offset { pos, highwater } => Some((pos, highwater)),
                    _ => None,
                })
                .expect("durable final offset")
        }

        fn files(&self) -> Vec<std::path::PathBuf> {
            std::fs::read_dir(&self.dir).unwrap().map(|e| e.unwrap().path()).collect()
        }
    }

    impl Drop for Harness {
        fn drop(&mut self) {
            self.engine.shutdown.begin();
            // Drop the sequencer's scratch owner before removing its test directory.
            self.future = Box::pin(async {});
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    #[tokio::test]
    async fn boot_race_final_checkpoint_cannot_overwrite_seeded_floor() {
        let mut h = Harness::with_mode(4096, LogPosition::start(), None, None, true);
        let floor = LogPosition { segment: 1080, offset: "0000000000000000_0000001015737962".into() };
        h.engine.catalog_tx.seed_durable_offset(floor.clone(), Some((0x10, 2))).await;
        h.engine
            .catalog_tx
            .send_durable(CatalogEvent::Offset { pos: floor.clone(), highwater: Some((0x10, 2)) })
            .await
            .unwrap();
        // A completed stale read defeats the older unread-shutdown guard. Exercise the actual
        // sequencer final checkpoint and catalog writer, not just monotonic in-memory tracking.
        h.page("-1", "p1", vec![]).await;
        assert_eq!(h.processed.lock().unwrap().segment, 0);
        let checkpoint = h.finish().await;
        assert_eq!(
            checkpoint,
            (floor, Some((0x10, 2))),
            "shutdown must preserve the durable restart cursor and highwater"
        );
    }

    fn change(txid: &str, lsn: &str, seq: u64, last: bool) -> Envelope {
        Envelope {
            type_: "public.items".into(),
            key: seq.to_string(),
            value: Some(serde_json::json!({"id": seq, "blob": "x".repeat(1024)})),
            old: None,
            headers: EnvelopeHeaders {
                operation: "insert".into(),
                txid: Some(txid.into()),
                lsn: Some(lsn.into()),
                seq: Some(seq),
                last: last.then_some(true),
                offset: None,
            },
        }
    }

    #[tokio::test]
    async fn memory_budget_orphan_discard_does_not_spool_the_following_complete_page() {
        let h = Harness::with_budget(u64::MAX, LogPosition::start_of(0), None, Some(10000));
        let budget = h.engine.metrics.memory_budget.clone();
        let cfg = h.engine.txn_cfg.lock().unwrap().clone();
        let first = prepare_transactions(
            vec![change("1", "0/10", 0, false)],
            None,
            &cfg,
            h.engine.metrics.clone(),
            Some(Arc::new(budget.acquire(4000).await)),
        )
        .await
        .unwrap();
        assert_eq!(h.files().len(), 1);
        let page = prepare_transactions(
            vec![change("2", "0/20", 0, true)],
            first.held,
            &cfg,
            h.engine.metrics.clone(),
            Some(Arc::new(budget.acquire(4000).await)),
        )
        .await
        .unwrap();
        assert!(h.files().is_empty(), "an orphan discard leaves an ordinary complete page, which needs no scratch I/O");
        assert_eq!(page.ready.len(), 1);
        assert_eq!(budget.held.load(Ordering::Relaxed), 4000);
    }

    #[tokio::test]
    async fn memory_budget_ordinary_complete_input_keeps_its_credit_without_spilling() {
        let h = Harness::with_budget(u64::MAX, LogPosition::start_of(0), None, Some(100));
        let budget = h.engine.metrics.memory_budget.clone();
        let credit = Arc::new(budget.acquire(200).await);
        let cfg = h.engine.txn_cfg.lock().unwrap().clone();
        let page = prepare_transactions(
            vec![change("1", "0/10", 0, true)],
            None,
            &cfg,
            h.engine.metrics.clone(),
            Some(credit.clone()),
        )
        .await
        .unwrap();
        drop(credit);
        assert!(h.files().is_empty(), "a complete page with no receipt dependency needs no disk I/O");
        assert_eq!(budget.held.load(Ordering::Relaxed), 200, "decoded input still owns its page credit");
        drop(page);
        assert_eq!(budget.held.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn memory_budget_page_spills_before_a_receipt_waits_on_another_byte_owner() {
        let start = LogPosition::start_of(0);
        let mut h = Harness::with_budget(u64::MAX, start.clone(), None, Some(1));
        h.shape().await;
        let appended = Arc::new(tokio::sync::Notify::new());
        h.store.append_notifiers.lock().unwrap().insert("shape/s1".into(), appended.clone());
        h.engine.pending_flips.add(0x10);
        let engine = h.engine.clone();
        let budget = engine.metrics.memory_budget.clone();
        let worker = async {
            appended.notified().await;
            // A causally prior query-back cannot finish until it has memory. This is larger
            // than the entire budget, so input from this page must have been released/spilled.
            let _permit = budget.acquire(2).await;
            assert!(engine.source_drain_receipt("budget-fence").is_none());
            engine.pending_flips.complete(0x10);
        };
        let mut marker = change("1", "0/10", 1, true);
        marker.type_ = crate::replication::SOURCE_FENCE_ENVELOPE.into();
        marker.key = "budget-fence".into();
        let page = vec![change("1", "0/10", 0, false), marker, change("2", "0/20", 0, false)];
        tokio::join!(h.page(&start.offset, "p1", page), worker);
        assert!(h.engine.source_drain_receipt("budget-fence").is_some());
        assert_eq!(h.files().len(), 1, "later held input stays on disk across the receipt");
        assert_eq!(budget.held.load(Ordering::Relaxed), 0, "idle reads and held scratch own no input bytes");
        assert_eq!(h.engine.metrics.sequencer_held_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(*h.processed.lock().unwrap(), start, "the later incomplete transaction still pins replay");
        h.finish().await;
        assert!(h.files().is_empty());
    }

    #[tokio::test]
    async fn held_spill_waits_for_end_and_removes_the_file_after_processing() {
        let mut h = Harness::new(512);
        let start = LogPosition::start_of(0);
        h.page(&start.offset, "p1", vec![change("1", "0/10", 0, false)]).await;
        assert_eq!(h.files().len(), 1, "held input above the configured cap must move to a spill file");
        assert_eq!(*h.processed.lock().unwrap(), start, "a spill cannot advance the replay boundary");
        assert!(h.engine.source_drain_receipt("commit-1").is_none());
        let mut end = change("1", "0/10", 1, true);
        end.type_ = crate::replication::SOURCE_FENCE_ENVELOPE.into();
        end.key = "commit-1".into();
        h.page("p1", "p2", vec![end]).await;
        assert_eq!(h.engine.source_drain_receipt("commit-1").unwrap().commit_lsn, "0/10");
        assert_eq!(h.processed.lock().unwrap().offset, "p2");
        assert!(h.files().is_empty(), "completed transaction scratch must be removed");
    }

    #[tokio::test]
    async fn held_spill_is_removed_when_a_different_transaction_discards_the_fragment() {
        let mut h = Harness::new(512);
        let start = LogPosition::start_of(0);
        h.page(&start.offset, "p1", vec![change("1", "0/10", 0, false)]).await;
        assert_eq!(h.files().len(), 1, "the incomplete transaction must have spilled");
        h.page("p1", "p2", vec![change("2", "0/20", 0, true)]).await;
        assert!(h.files().is_empty(), "orphan/epoch-discarded scratch must be removed");
        assert_eq!(h.engine.metrics.sequencer_orphan_fragments.load(Ordering::Relaxed), 1);
        assert_eq!(h.processed.lock().unwrap().offset, "p2");
    }
    #[tokio::test]
    async fn held_spill_filters_only_leading_redelivery_and_flushes_once_per_transaction() {
        let mut h = Harness::new(512);
        h.shape().await;
        let start = LogPosition::start_of(0);
        h.page(&start.offset, "p1", vec![change("1", "0/10", 0, false), change("1", "0/10", 1, false)]).await;
        assert!(h.output().is_empty(), "no prefix is visible before the end marker");
        h.page("p1", "p2", vec![change("1", "0/10", 0, false), change("1", "0/10", 1, false)]).await;
        assert_eq!(h.files().len(), 1, "redelivery keeps one scratch owner");
        assert!(h.output().is_empty());
        h.page(
            "p2",
            "p3",
            vec![change("1", "0/10", 1, false), change("1", "0/10", 2, true), change("2", "0/20", 0, true)],
        )
        .await;
        let batches = h.output();
        assert_eq!(batches.len(), 2, "file read chunks must not become partial transaction appends");
        assert_eq!(batches[0].iter().map(|e| e.key.as_str()).collect::<Vec<_>>(), ["0", "1", "2"]);
        assert!(batches[0].iter().all(|e| e.headers.lsn.as_deref() == Some("0/10")));
        assert_eq!(batches[1].len(), 1, "a later run's low seq must survive the leading-run filter");
        assert_eq!(batches[1][0].headers.lsn.as_deref(), Some("0/20"));
        assert!(h.files().is_empty());
        assert_eq!(h.engine.metrics.sequencer_held_spills.load(Ordering::Relaxed), 1);
        assert_eq!(h.finish().await, (LogPosition { segment: 0, offset: "p3".into() }, Some((0x20, 0))));
    }

    #[tokio::test]
    async fn held_consecutive_spills_repin_and_checkpoint_only_completed_highwater() {
        let mut h = Harness::new(512);
        h.shape().await;
        let start = LogPosition::start_of(0);
        h.page(&start.offset, "p1", vec![change("1", "0/10", 0, false)]).await;
        let first_file = h.files().pop().unwrap();
        h.page("p1", "p2", vec![change("1", "0/10", 1, true), change("2", "0/20", 0, false)]).await;
        assert!(!first_file.exists());
        assert_eq!(h.files().len(), 1);
        assert_eq!(h.processed.lock().unwrap().offset, "p1");
        assert_eq!(h.output().len(), 1);
        let (resp, received) = tokio::sync::oneshot::channel();
        h.commands
            .send(SequencerCmd::DeactivateShape { table: "public.items".into(), shape_id: "s1".into(), resp })
            .unwrap();
        assert_eq!(h.receive(received).await.unwrap().0.offset, "p1", "dormancy also uses the held boundary");
        assert_eq!(h.finish().await, (LogPosition { segment: 0, offset: "p1".into() }, Some((0x10, 1))));
        assert!(h.files().is_empty(), "shutdown releases incomplete scratch");
        assert_eq!(h.engine.metrics.sequencer_held_run.load(Ordering::Relaxed), 0);
        assert_eq!(h.engine.metrics.sequencer_held_bytes.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn held_memory_below_cap_is_released_on_shutdown() {
        let mut h = Harness::new(64 * 1024 * 1024);
        let start = LogPosition::start_of(0);
        h.page(&start.offset, "p1", vec![change("1", "0/10", 0, false)]).await;
        assert!(h.files().is_empty());
        assert!(h.engine.metrics.sequencer_held_bytes.load(Ordering::Relaxed) > 0);
        assert_eq!(h.finish().await, (start, None));
        assert_eq!(h.engine.metrics.sequencer_held_bytes.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn held_missing_file_stops_without_output_receipt_or_checkpoint_advancement() {
        let mut h = Harness::new(512);
        h.shape().await;
        let start = LogPosition::start_of(0);
        h.page(&start.offset, "p1", vec![change("1", "0/10", 0, false)]).await;
        std::fs::remove_file(h.files().pop().unwrap()).unwrap();
        let mut end = change("1", "0/10", 1, true);
        end.type_ = crate::replication::SOURCE_FENCE_ENVELOPE.into();
        end.key = "failed-commit".into();
        h.store
            .pages_by_offset
            .lock()
            .unwrap()
            .insert("p1".into(), ("p2".into(), true, serde_json::to_string(&vec![end]).unwrap()));
        // This fixture's idle response is permanently pending; use the explicit cancellation
        // command to expose the next page without relying on ordinary commands dropping reads.
        let (done, _) = tokio::sync::oneshot::channel();
        h.commands.send(SequencerCmd::PauseReads { done }).unwrap();
        let (done, _) = tokio::sync::oneshot::channel();
        h.commands.send(SequencerCmd::ResumeReads { done }).unwrap();
        tokio::time::timeout(Duration::from_secs(5), &mut h.future).await.expect("read failure must stop sequencing");
        // The loop completed itself; use an inert future while draining its queued checkpoint.
        h.future = Box::pin(async {});
        assert_eq!(h.finish().await, (start.clone(), None));
        assert_eq!(*h.processed.lock().unwrap(), start);
        assert!(h.output().is_empty());
        assert!(h.engine.source_drain_receipt("failed-commit").is_none());
        assert!(h.files().is_empty());
    }
    #[tokio::test]
    async fn held_checkpoint_restart_skips_complete_prefix_and_replays_incomplete_input() {
        let start = LogPosition::start_of(0);
        let mut h = Harness::new(512);
        h.shape().await;
        let page = vec![change("1", "0/10", 0, true), change("2", "0/20", 0, false)];
        h.page(&start.offset, "p1", page.clone()).await;
        assert_eq!(h.output().len(), 1);
        let (saved, highwater) = h.finish().await;
        assert_eq!(saved, start);
        assert_eq!(highwater, Some((0x10, 0)), "complete prefix advances highwater while the position stays pinned");
        assert!(h.files().is_empty());
        let mut restored = Harness::restore(512, saved.clone(), highwater);
        restored.shape().await;
        restored.page(&saved.offset, "p1", page).await;
        assert!(restored.output().is_empty(), "already checkpointed prefix must not be applied twice");
        restored.page("p1", "p2", vec![change("2", "0/20", 1, true)]).await;
        let out = restored.output();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].iter().map(|e| e.key.as_str()).collect::<Vec<_>>(), ["0", "1"]);
        assert!(out[0].iter().all(|e| e.headers.lsn.as_deref() == Some("0/20")));
        assert_eq!(restored.finish().await.1, Some((0x20, 1)));
    }

    #[tokio::test]
    async fn held_write_failure_stops_at_page_boundary_and_hard_drop_removes_scratch() {
        let mut h = Harness::new(512);
        let start = LogPosition::start_of(0);
        // Make the configured directory unusable after the boot probe point.
        std::fs::remove_dir(&h.dir).unwrap();
        std::fs::write(&h.dir, b"not a directory").unwrap();
        h.store.pages_by_offset.lock().unwrap().insert(
            start.offset.clone(),
            ("p1".into(), true, serde_json::to_string(&vec![change("1", "0/10", 0, false)]).unwrap()),
        );
        tokio::time::timeout(Duration::from_secs(5), &mut h.future).await.expect("write failure must stop sequencing");
        h.future = Box::pin(async {});
        assert_eq!(h.finish().await, (start.clone(), None));
        assert_eq!(*h.processed.lock().unwrap(), start);
        assert_eq!(h.engine.metrics.sequencer_held_bytes.load(Ordering::Relaxed), 0);
        std::fs::remove_file(&h.dir).unwrap();
        std::fs::create_dir(&h.dir).unwrap();

        let mut h = Harness::new(512);
        h.page(&start.offset, "p1", vec![change("1", "0/10", 0, false)]).await;
        let path = h.files().pop().unwrap();
        h.future = Box::pin(async {}); // terminal cancellation/replacement, no outstanding disk operation
        assert!(!path.exists(), "dropping the sequencer must drop its scratch owner");
    }
}
