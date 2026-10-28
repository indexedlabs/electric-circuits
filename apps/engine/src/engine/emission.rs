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
    queue: std::sync::Mutex<std::collections::VecDeque<GroupBatch>>,
    wake: tokio::sync::Notify,
    closed: std::sync::atomic::AtomicBool,
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
        let (tx, mut rx) = mpsc::channel(GROUP_LANE_CAPACITY);
        let lane = Arc::new(GroupLane {
            queue: std::sync::Mutex::new(std::collections::VecDeque::new()),
            wake: tokio::sync::Notify::new(),
            closed: std::sync::atomic::AtomicBool::new(false),
        });
        let dispatcher_lane = lane.clone();
        let dispatcher_pending = self.pending.clone();
        let dispatcher_origins = self.origins.clone();
        let dispatcher_wake = self.wake.clone();
        let dispatcher_shutdown = self.shutdown.clone();
        let dispatcher = async move {
            loop {
                tokio::select! {
                    _ = dispatcher_lane.wake.notified() => {}
                    _ = dispatcher_shutdown.wait() => {
                        dispatcher_lane.closed.store(true, std::sync::atomic::Ordering::Release);
                    }
                }
                loop {
                    let batch = dispatcher_lane.queue.lock().unwrap().pop_front();
                    let Some(batch) = batch else { break };
                    if dispatcher_lane.closed.load(std::sync::atomic::Ordering::Acquire) {
                        let mut queue = dispatcher_lane.queue.lock().unwrap();
                        let mut dropped = std::collections::VecDeque::new();
                        std::mem::swap(&mut *queue, &mut dropped);
                        drop(queue);
                        for batch in std::iter::once(batch).chain(dropped) {
                            dispatcher_pending.complete(batch.commit_lsn);
                            complete_origin_entry(
                                &dispatcher_origins,
                                &dispatcher_wake,
                                &batch.stream_path,
                                batch.commit_lsn,
                            );
                        }
                        return;
                    }
                    if let Err(batch) = tx.send(batch).await {
                        let mut queue = dispatcher_lane.queue.lock().unwrap();
                        let mut dropped = std::collections::VecDeque::new();
                        std::mem::swap(&mut *queue, &mut dropped);
                        drop(queue);
                        for batch in std::iter::once(batch).chain(dropped) {
                            dispatcher_pending.complete(batch.commit_lsn);
                            complete_origin_entry(
                                &dispatcher_origins,
                                &dispatcher_wake,
                                &batch.stream_path,
                                batch.commit_lsn,
                            );
                        }
                        return;
                    }
                }
                if dispatcher_lane.closed.load(std::sync::atomic::Ordering::Acquire) {
                    return;
                }
            }
        };
        self.shutdown.clone().spawn_background(dispatcher);

        let worker_lane = lane.clone();
        let worker_ds = self.ds.clone();
        let worker_pending = self.pending.clone();
        let worker_origins = self.origins.clone();
        let worker_wake = self.wake.clone();
        self.shutdown.clone().spawn_background(async move {
            while let Some(batch) = rx.recv().await {
                let _ = worker_ds.append_reliable(&batch.stream_path, &batch.envs).await;
                worker_pending.complete(batch.commit_lsn);
                complete_origin_entry(&worker_origins, &worker_wake, &batch.stream_path, batch.commit_lsn);
            }
            worker_lane.closed.store(true, std::sync::atomic::Ordering::Release);
        });
        lanes.insert(path.to_string(), lane.clone());
        lane
    }

    pub(crate) fn has_origin_at_or_before(&self, marker_lsn: u64) -> bool {
        self.origins.lock().unwrap().iter().any(|((_, lsn), _)| *lsn <= marker_lsn)
    }

    pub(crate) fn wake(&self) -> Arc<tokio::sync::Notify> {
        self.wake.clone()
    }

    /// Queue one complete group batch. The bounded send is awaited by an engine-owned task rather
    /// than while the subquery registry mutex is held; a full group waits for its own lane and
    /// cannot drop, split, reorder or degrade unrelated shapes.
    pub(crate) fn enqueue(&self, path: &str, envs: Vec<Envelope>, commit_lsn: u64) {
        if envs.is_empty() {
            return;
        }
        self.pending.add(commit_lsn);
        let path = path.to_string();
        *self.origins.lock().unwrap().entry((path.clone(), commit_lsn)).or_default() += 1;
        let lane = self.lane(&path);
        lane.queue.lock().unwrap().push_back(GroupBatch { commit_lsn, stream_path: path, envs });
        lane.wake.notify_one();
    }

    pub(crate) fn remove(&self, path: &str) {
        let lane = self.lanes.lock().unwrap().remove(path);
        if let Some(lane) = lane {
            lane.closed.store(true, std::sync::atomic::Ordering::Release);
            let mut queue = lane.queue.lock().unwrap();
            let dropped = std::mem::take(&mut *queue);
            drop(queue);
            for batch in dropped {
                self.pending.complete(batch.commit_lsn);
                complete_origin_entry(&self.origins, &self.wake, &batch.stream_path, batch.commit_lsn);
            }
            lane.wake.notify_one();
        }
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
}
