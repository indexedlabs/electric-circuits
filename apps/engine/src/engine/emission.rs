//! Ordered emission lanes for subquery-shape appends.
//!
//! The registry's correctness rule is that, per shape stream, **append order equals
//! membership-evaluation order** — a move evaluated at t₁ must never land after an emission
//! for the same pk evaluated at t₂ > t₁, or the stream's last word is stale (permanent
//! divergence). The old implementation guaranteed this by holding the registry lock across
//! the `append_reliable` network call; these lanes give the same guarantee without network
//! under the lock:
//!
//!   * emitters **enqueue under the registry lock** — so per-stream enqueue order is exactly
//!     eval order — and return immediately;
//!   * each stream path hashes to ONE lane (a FIFO drained by one writer task), so batches
//!     for a stream land in enqueue order;
//!   * appends use `append_reliable` (retry-until-landed; the only non-retried case is a retired
//!     stream — 404/410/`stream-closed` — the shape was dropped or evicted, discard is correct).
//!
//! The convergence barrier: every enqueued batch increments the shared `pending` counter
//! (the engine's `pendingFlips` term) and decrements it only after the append **landed**, so
//! "`pendingFlips == 0`" still means every subquery effect is on its stream — queued batches
//! included. Increment-before/at-enqueue happens while the emitter still holds its own
//! accounting (a `FlipWork` token stays >0 until its batches are enqueued), so the counter
//! never dips to zero with effects in flight. Each batch retains its originating commit LSN;
//! receipt barriers check only outstanding origins at or before their marker, while the metric
//! remains the total across all origins. The origin is internal accounting, not a wire LSN.

use super::*;

struct Batch {
    commit_lsn: u64,
    stream_path: String,
    envs: Vec<Envelope>,
}

/// Hash-sharded, per-stream-ordered append lanes. Cheap to clone.
#[derive(Clone)]
pub(crate) struct EmissionLanes {
    lanes: Arc<Vec<mpsc::UnboundedSender<Batch>>>,
    pending: Arc<PendingFlips>,
}

impl EmissionLanes {
    /// Spawn `n` writer tasks. `pending` is the engine's convergence-barrier counter
    /// (`pendingFlips`): incremented per enqueued batch, decremented after its append lands.
    pub(crate) fn spawn(
        ds: DsClient,
        n: usize,
        pending: Arc<PendingFlips>,
        shutdown: crate::shutdown::ShutdownToken,
    ) -> EmissionLanes {
        let n = n.max(1);
        let mut lanes = Vec::with_capacity(n);
        for _ in 0..n {
            let (tx, mut rx) = mpsc::unbounded_channel::<Batch>();
            let ds = ds.clone();
            let pending = pending.clone();
            shutdown.spawn_background(async move {
                while let Some(b) = rx.recv().await {
                    // Reliable: a dropped subquery envelope is permanent divergence for the
                    // shape's subscribers. A retired stream (shape dropped/evicted mid-flight)
                    // discards, correctly.
                    if ds.append_reliable(&b.stream_path, &b.envs).await.is_err() {
                        return;
                    }
                    pending.complete(b.commit_lsn);
                }
            });
            lanes.push(tx);
        }
        EmissionLanes { lanes: Arc::new(lanes), pending }
    }

    /// Which lane serves `path` (stable: one stream always drains through one FIFO).
    pub(crate) fn lane_for(&self, path: &str) -> usize {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        path.hash(&mut h);
        (h.finish() % self.lanes.len() as u64) as usize
    }

    /// Enqueue one batch for ordered delivery. Call while holding the registry lock so that
    /// per-stream enqueue order is evaluation order. Ordinary subquery lanes remain unbounded;
    /// grouped output uses [`GroupEmissionLanes`] for its explicit bounded backpressure contract.
    pub(crate) fn enqueue(&self, stream_path: &str, envs: Vec<Envelope>, commit_lsn: u64) {
        if envs.is_empty() {
            return;
        }
        self.pending.add(commit_lsn);
        let lane = self.lane_for(stream_path);
        if self.lanes[lane].send(Batch { commit_lsn, stream_path: stream_path.to_string(), envs }).is_err() {
            // Writer gone (engine teardown): don't leave the barrier stuck.
            self.pending.complete(commit_lsn);
        }
    }
}

const GROUP_LANE_CAPACITY: usize = 64;

struct GroupBatch {
    commit_lsn: u64,
    stream_path: String,
    envs: Vec<Envelope>,
}

struct GroupLane {
    queue: std::sync::Mutex<std::collections::VecDeque<Arc<QueuedGroupBatch>>>,
    wake: tokio::sync::Notify,
    slots: Arc<tokio::sync::Semaphore>,
    closed: std::sync::atomic::AtomicBool,
    pending: Arc<PendingFlips>,
    origins: Arc<std::sync::Mutex<BTreeMap<(String, u64), usize>>>,
    origin_wake: Arc<tokio::sync::Notify>,
}

struct QueuedGroupBatch {
    batch: std::sync::Mutex<Option<GroupBatch>>,
    permit: std::sync::Mutex<Option<tokio::sync::OwnedSemaphorePermit>>,
    ready: std::sync::atomic::AtomicBool,
    cancelled: std::sync::atomic::AtomicBool,
}

pub(crate) struct GroupBatchHandle {
    lane: Arc<GroupLane>,
    queued: Arc<QueuedGroupBatch>,
}

#[derive(Clone)]
pub(crate) struct GroupEmissionLanes {
    lanes: Arc<std::sync::Mutex<HashMap<String, Arc<GroupLane>>>>,
    ds: DsClient,
    shutdown: crate::shutdown::ShutdownToken,
    pending: Arc<PendingFlips>,
    origins: Arc<std::sync::Mutex<BTreeMap<(String, u64), usize>>>,
    wake: Arc<tokio::sync::Notify>,
}

impl GroupEmissionLanes {
    pub(crate) fn new(ds: DsClient, shutdown: crate::shutdown::ShutdownToken, pending: Arc<PendingFlips>) -> Self {
        Self {
            lanes: Arc::new(std::sync::Mutex::new(HashMap::new())),
            ds,
            shutdown,
            pending,
            origins: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            wake: Arc::new(tokio::sync::Notify::new()),
        }
    }

    fn lane(&self, path: &str) -> Arc<GroupLane> {
        let mut lanes = self.lanes.lock().unwrap();
        if let Some(sender) = lanes.get(path) {
            return sender.clone();
        }
        let lane = Arc::new(GroupLane {
            queue: std::sync::Mutex::new(std::collections::VecDeque::new()),
            wake: tokio::sync::Notify::new(),
            slots: Arc::new(tokio::sync::Semaphore::new(GROUP_LANE_CAPACITY)),
            closed: std::sync::atomic::AtomicBool::new(false),
            pending: self.pending.clone(),
            origins: self.origins.clone(),
            origin_wake: self.wake.clone(),
        });
        let worker_lane = lane.clone();
        let worker_ds = self.ds.clone();
        self.shutdown.clone().spawn_background(async move {
            loop {
                let notified = worker_lane.wake.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let queued = {
                    let mut queue = worker_lane.queue.lock().unwrap();
                    if worker_lane.closed.load(std::sync::atomic::Ordering::Acquire) && queue.is_empty() {
                        return;
                    }
                    if queue.front().is_some_and(|item| {
                        item.ready.load(std::sync::atomic::Ordering::Acquire)
                            || item.cancelled.load(std::sync::atomic::Ordering::Acquire)
                    }) {
                        queue.pop_front()
                    } else {
                        None
                    }
                };
                let Some(queued) = queued else {
                    notified.await;
                    continue;
                };
                let permit = queued.permit.lock().unwrap().take();
                let Some(permit) = permit else {
                    continue;
                };
                // The producer bound covers queued batches, like a bounded mpsc channel. Once
                // the worker owns the batch, release the slot while the reliable append runs.
                drop(permit);
                let batch = queued.batch.lock().unwrap().take();
                let Some(batch) = batch else {
                    continue;
                };
                if worker_lane.closed.load(std::sync::atomic::Ordering::Acquire) {
                    worker_lane.pending.complete(batch.commit_lsn);
                    complete_origin_entry(
                        &worker_lane.origins,
                        &worker_lane.origin_wake,
                        &batch.stream_path,
                        batch.commit_lsn,
                    );
                    continue;
                }
                if worker_ds.append_reliable(&batch.stream_path, &batch.envs).await.is_err() {
                    if worker_lane.closed.load(std::sync::atomic::Ordering::Acquire) {
                        worker_lane.pending.complete(batch.commit_lsn);
                        complete_origin_entry(
                            &worker_lane.origins,
                            &worker_lane.origin_wake,
                            &batch.stream_path,
                            batch.commit_lsn,
                        );
                    }
                    // Keep the pending count and origin held for a failed live append: a source
                    // receipt must never advance past an effect that did not land.
                    return;
                }
                worker_lane.pending.complete(batch.commit_lsn);
                complete_origin_entry(
                    &worker_lane.origins,
                    &worker_lane.origin_wake,
                    &batch.stream_path,
                    batch.commit_lsn,
                );
            }
        });
        lanes.insert(path.to_string(), lane.clone());
        lane
    }

    pub(crate) fn register(&self, path: &str) {
        let _ = self.lane(path);
    }

    fn existing_lane(&self, path: &str) -> Option<Arc<GroupLane>> {
        self.lanes.lock().unwrap().get(path).cloned()
    }

    pub(crate) fn begin_origin(&self, path: &str, commit_lsn: u64) {
        *self.origins.lock().unwrap().entry((path.to_string(), commit_lsn)).or_default() += 1;
    }

    pub(crate) fn complete_origin(&self, path: &str, commit_lsn: u64) {
        complete_origin_entry(&self.origins, &self.wake, path, commit_lsn);
    }

    pub(crate) fn has_origin_at_or_before(&self, paths: &HashSet<String>, marker_lsn: u64) -> bool {
        self.origins.lock().unwrap().iter().any(|((path, lsn), _)| *lsn <= marker_lsn && paths.contains(path))
    }

    pub(crate) fn paths_at_or_before(&self, marker_lsn: u64) -> HashSet<String> {
        self.origins
            .lock()
            .unwrap()
            .iter()
            .filter(|((_, lsn), _)| *lsn <= marker_lsn)
            .map(|((path, _), _)| path.clone())
            .collect()
    }

    pub(crate) fn wake(&self) -> Arc<tokio::sync::Notify> {
        self.wake.clone()
    }

    /// Reserve one complete group batch in evaluation order. This is synchronous so callers can
    /// record the order while holding the registry lock; [`GroupBatchHandle::wait`] performs the
    /// bounded capacity wait after that lock is released.
    pub(crate) fn reserve(&self, path: &str, envs: Vec<Envelope>, commit_lsn: u64) -> Option<GroupBatchHandle> {
        if envs.is_empty() {
            return None;
        }
        let path = path.to_string();
        let lane = self.existing_lane(&path)?;
        self.pending.add(commit_lsn);
        self.begin_origin(&path, commit_lsn);
        let queued = Arc::new(QueuedGroupBatch {
            batch: std::sync::Mutex::new(Some(GroupBatch { commit_lsn, stream_path: path.clone(), envs })),
            permit: std::sync::Mutex::new(None),
            ready: std::sync::atomic::AtomicBool::new(false),
            cancelled: std::sync::atomic::AtomicBool::new(false),
        });
        let mut queue = lane.queue.lock().unwrap();
        if lane.closed.load(std::sync::atomic::Ordering::Acquire) {
            drop(queue);
            self.pending.complete(commit_lsn);
            complete_origin_entry(&self.origins, &self.wake, &path, commit_lsn);
            return None;
        }
        queue.push_back(queued.clone());
        drop(queue);
        lane.wake.notify_one();
        Some(GroupBatchHandle { lane, queued })
    }

    /// Queue one complete group batch and await its producer-side capacity slot. The queue entry
    /// is reserved before the await, preserving evaluation order without holding the registry lock.
    pub(crate) async fn enqueue(&self, path: &str, envs: Vec<Envelope>, commit_lsn: u64) {
        if let Some(handle) = self.reserve(path, envs, commit_lsn) {
            handle.wait().await;
        }
    }

    pub(crate) fn remove(&self, path: &str) {
        let lane = self.lanes.lock().unwrap().remove(path);
        if let Some(lane) = lane {
            lane.closed.store(true, std::sync::atomic::Ordering::Release);
            let dropped = std::mem::take(&mut *lane.queue.lock().unwrap());
            for queued in dropped {
                queued.cancelled.store(true, std::sync::atomic::Ordering::Release);
                let _permit = queued.permit.lock().unwrap().take();
                let Some(batch) = queued.batch.lock().unwrap().take() else {
                    continue;
                };
                self.pending.complete(batch.commit_lsn);
                complete_origin_entry(&self.origins, &self.wake, &batch.stream_path, batch.commit_lsn);
            }
            lane.wake.notify_waiters();
        }
    }
}

impl GroupBatchHandle {
    pub(crate) async fn wait(self) {
        let Ok(permit) = self.lane.slots.clone().acquire_owned().await else { return };
        if self.queued.cancelled.load(std::sync::atomic::Ordering::Acquire) {
            return;
        }
        *self.queued.permit.lock().unwrap() = Some(permit);
        self.queued.ready.store(true, std::sync::atomic::Ordering::Release);
        self.lane.wake.notify_one();
    }
}

impl Drop for GroupBatchHandle {
    fn drop(&mut self) {
        if self.queued.ready.load(std::sync::atomic::Ordering::Acquire)
            || self.queued.cancelled.swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return;
        }
        let _permit = self.queued.permit.lock().unwrap().take();
        let Some(batch) = self.queued.batch.lock().unwrap().take() else {
            return;
        };
        self.lane.pending.complete(batch.commit_lsn);
        complete_origin_entry(&self.lane.origins, &self.lane.origin_wake, &batch.stream_path, batch.commit_lsn);
        self.lane.wake.notify_one();
        // The handle is the owner of a reserved-but-not-submitted batch. Cancellation must undo
        // the barrier entry just as a closed lane does, otherwise its front queue item strands
        // every later batch behind it.
    }
}

fn complete_origin_entry(
    origins: &std::sync::Mutex<BTreeMap<(String, u64), usize>>,
    wake: &tokio::sync::Notify,
    path: &str,
    commit_lsn: u64,
) {
    let mut origins = origins.lock().unwrap();
    let key = (path.to_string(), commit_lsn);
    if let Some(count) = origins.get_mut(&key) {
        *count -= 1;
        if *count == 0 {
            origins.remove(&key);
        }
    }
    drop(origins);
    wake.notify_waiters();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_worker_hands_its_commit_to_every_append_before_completing() {
        let pending = Arc::new(PendingFlips::default());
        let (tx, mut rx) = mpsc::unbounded_channel();
        let lanes = EmissionLanes { lanes: Arc::new(vec![tx]), pending: pending.clone() };
        let envelope: Envelope = serde_json::from_value(serde_json::json!({
            "type": "test", "key": "1", "headers": {"operation": "upsert"}
        }))
        .unwrap();
        pending.add(0x10); // The worker still owns the originating flip.
        lanes.enqueue("shape/a", vec![envelope.clone()], 0x10);
        lanes.enqueue("shape/b", vec![envelope.clone()], 0x10);
        lanes.enqueue("shape/c", vec![envelope], 0x30);
        pending.complete(0x10); // Worker has enqueued all of its effects.
        assert_eq!(pending.count(), 3);
        assert!(pending.has_at_or_before(0x20), "unlanded appends still hold this marker");
        let first = rx.try_recv().unwrap();
        let second = rx.try_recv().unwrap();
        assert_eq!((first.commit_lsn, second.commit_lsn), (0x10, 0x10));
        pending.complete(first.commit_lsn);
        assert!(pending.has_at_or_before(0x20), "every append at the origin must land");
        pending.complete(second.commit_lsn);
        assert!(!pending.has_at_or_before(0x20), "a later append cannot hold this prefix");
        assert_eq!(pending.count(), 1, "the aggregate metric still includes later work");
        pending.complete(rx.try_recv().unwrap().commit_lsn);
        assert_eq!(pending.count(), 0);
        assert!(pending.state.lock().unwrap().1.is_empty(), "completed origins consume no index memory");
    }

    #[test]
    fn ordinary_lanes_remain_unbounded_and_do_not_degrade_when_busy() {
        let pending = Arc::new(PendingFlips::default());
        let (tx, _rx) = mpsc::unbounded_channel();
        let lanes = EmissionLanes { lanes: Arc::new(vec![tx]), pending: pending.clone() };
        let envelope: Envelope = serde_json::from_value(serde_json::json!({
            "type": "public.items", "key": "1", "headers": {"operation": "upsert"}
        }))
        .unwrap();
        lanes.enqueue("shape/group-1", vec![envelope.clone()], 0x10);
        lanes.enqueue("shape/group-1", vec![envelope], 0x20);
        assert_eq!(pending.count(), 2);
    }

    #[test]
    fn a_closed_lane_undoes_only_its_own_origin() {
        let pending = Arc::new(PendingFlips::default());
        let (tx, rx) = mpsc::unbounded_channel();
        drop(rx);
        let lanes = EmissionLanes { lanes: Arc::new(vec![tx]), pending: pending.clone() };
        pending.add(0x30);
        let envelope = serde_json::from_value(serde_json::json!({
            "type": "test", "key": "1", "headers": {"operation": "upsert"}
        }))
        .unwrap();
        lanes.enqueue("shape/a", vec![envelope], 0x10);
        assert_eq!(pending.count(), 1);
        assert!(!pending.has_at_or_before(0x20));
    }

    #[tokio::test]
    async fn enqueue_after_remove_completes_its_origin() {
        let pending = Arc::new(PendingFlips::default());
        let lanes = GroupEmissionLanes::new(
            DsClient::new_for_in_process_test("http://127.0.0.1:1"),
            crate::shutdown::ShutdownToken::new(),
            pending.clone(),
        );
        let _lane = lanes.lane("shape/group");
        lanes.remove("shape/group");
        let envelope: Envelope = serde_json::from_value(serde_json::json!({
            "type": "shape/group", "key": "1", "headers": {"operation": "upsert"}
        }))
        .unwrap();
        lanes.enqueue("shape/group", vec![envelope], 0x42).await;
        assert_eq!(pending.count(), 0);
        assert!(!lanes.has_origin_at_or_before(&HashSet::from(["shape/group".to_string()]), 0x42));
    }

    #[tokio::test]
    async fn reserved_batches_waiting_out_of_order_land_in_reservation_order() {
        let store = Arc::new(crate::ds::ScriptedStore::default());
        let lanes = GroupEmissionLanes::new(
            DsClient::with_test_store("scripted://group-order".into(), store.clone()),
            crate::shutdown::ShutdownToken::new(),
            Arc::new(PendingFlips::default()),
        );
        lanes.register("shape/group");
        let env = |key: &str| Envelope {
            type_: "shape/group".into(),
            key: key.into(),
            value: None,
            old: None,
            headers: EnvelopeHeaders {
                operation: "upsert".into(),
                txid: None,
                offset: None,
                lsn: None,
                seq: None,
                last: None,
            },
        };
        let old = lanes.reserve("shape/group", vec![env("old")], 1).unwrap();
        let new = lanes.reserve("shape/group", vec![env("new")], 2).unwrap();
        new.wait().await;
        assert!(store.appended.lock().unwrap().is_empty());
        old.wait().await;
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if store.appended.lock().unwrap().len() == 2 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let keys = store
            .appended
            .lock()
            .unwrap()
            .iter()
            .map(|(_, _, body)| serde_json::from_slice::<Vec<serde_json::Value>>(body).unwrap()[0]["key"].clone())
            .collect::<Vec<_>>();
        assert_eq!(keys, vec![serde_json::json!("old"), serde_json::json!("new")]);
    }

    #[tokio::test]
    async fn removing_a_busy_lane_completes_queued_origins() {
        let store = Arc::new(crate::ds::ScriptedStore::default());
        let gate = Arc::new(crate::ds::BodyGate::default());
        *store.append_fault.lock().unwrap() =
            Some(("shape/group".into(), 0, crate::ds::AppendFault::Hold(gate.clone())));
        let pending = Arc::new(PendingFlips::default());
        let lanes = GroupEmissionLanes::new(
            DsClient::with_test_store("scripted://group-close".into(), store.clone()),
            crate::shutdown::ShutdownToken::new(),
            pending.clone(),
        );
        lanes.register("shape/group");
        let env = |key: &str| Envelope {
            type_: "shape/group".into(),
            key: key.into(),
            value: None,
            old: None,
            headers: EnvelopeHeaders {
                operation: "upsert".into(),
                txid: None,
                offset: None,
                lsn: None,
                seq: None,
                last: None,
            },
        };
        lanes.enqueue("shape/group", vec![env("in-flight")], 1).await;
        while gate.arrived.load(std::sync::atomic::Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
        let queued = lanes.reserve("shape/group", vec![env("queued")], 2).unwrap();
        queued.wait().await;
        assert_eq!(pending.count(), 2);
        lanes.remove("shape/group");
        assert_eq!(pending.count(), 1);
        gate.released.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while pending.count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!lanes.has_origin_at_or_before(&HashSet::from(["shape/group".to_string()]), 2));
    }

    #[tokio::test]
    async fn group_lane_capacity_waits_for_a_slot_and_preserves_batch_order() {
        let store = Arc::new(crate::ds::ScriptedStore::default());
        let gate = Arc::new(crate::ds::BodyGate::default());
        *store.append_fault.lock().unwrap() =
            Some(("shape/group".into(), 0, crate::ds::AppendFault::Hold(gate.clone())));
        let lanes = GroupEmissionLanes::new(
            DsClient::with_test_store("scripted://group-lane".into(), store.clone()),
            crate::shutdown::ShutdownToken::new(),
            Arc::new(PendingFlips::default()),
        );
        lanes.register("shape/group");
        let env = |commit_lsn: u64| Envelope {
            type_: "shape/group".into(),
            key: commit_lsn.to_string(),
            value: None,
            old: None,
            headers: EnvelopeHeaders {
                operation: "upsert".into(),
                txid: None,
                offset: None,
                lsn: None,
                seq: None,
                last: None,
            },
        };
        lanes.enqueue("shape/group", vec![env(0)], 0).await;
        while gate.arrived.load(std::sync::atomic::Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
        for commit_lsn in 1..=GROUP_LANE_CAPACITY as u64 {
            lanes.enqueue("shape/group", vec![env(commit_lsn)], commit_lsn).await;
        }
        let next = tokio::spawn({
            let lanes = lanes.clone();
            async move { lanes.enqueue("shape/group", vec![env(65)], 65).await }
        });
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        assert!(!next.is_finished(), "a full group lane accepted a batch without waiting");

        for _ in 0..GROUP_LANE_CAPACITY {
            let before = gate.arrived.load(std::sync::atomic::Ordering::Acquire);
            *store.append_fault.lock().unwrap() =
                Some(("shape/group".into(), 0, crate::ds::AppendFault::Hold(gate.clone())));
            gate.released.notify_one();
            while gate.arrived.load(std::sync::atomic::Ordering::Acquire) == before {
                tokio::task::yield_now().await;
            }
        }
        gate.released.notify_one();
        next.await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if store.appended.lock().unwrap().len() == GROUP_LANE_CAPACITY + 2 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let landed = store.appended.lock().unwrap().clone();
        let keys = landed
            .into_iter()
            .map(|(_, _, body)| {
                serde_json::from_slice::<Vec<serde_json::Value>>(&body).unwrap()[0]["key"]
                    .as_str()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(keys, (0..=65).collect::<Vec<_>>());
    }
}
