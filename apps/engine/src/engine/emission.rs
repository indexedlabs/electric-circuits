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

#[derive(Clone, Copy, Debug)]
pub(crate) struct GroupLaneConfig {
    pub max_backlog_bytes: u64,
    pub max_stall: std::time::Duration,
}

impl Default for GroupLaneConfig {
    fn default() -> Self {
        Self { max_backlog_bytes: 64 * 1024 * 1024, max_stall: std::time::Duration::from_secs(60) }
    }
}

impl GroupLaneConfig {
    pub(crate) fn from_env() -> Self {
        let d = Self::default();
        let max_backlog_bytes = std::env::var("ELECTRIC_CIRCUITS_GROUP_BACKLOG_MAX_BYTES")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(d.max_backlog_bytes);
        let max_stall = std::env::var("ELECTRIC_CIRCUITS_GROUP_STALL_MAX_SECS")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .map(std::time::Duration::from_secs)
            .unwrap_or(d.max_stall);
        Self { max_backlog_bytes, max_stall }
    }
}

struct GroupLane {
    state: std::sync::Mutex<GroupLaneState>,
    wake: tokio::sync::Notify,
    worker_wake: tokio::sync::Notify,
    worker_done: tokio::sync::Notify,
    pending: Arc<PendingFlips>,
    origins: Arc<std::sync::Mutex<BTreeMap<(String, u64), usize>>>,
    origin_wake: Arc<tokio::sync::Notify>,
    config: GroupLaneConfig,
    metadata: std::sync::Mutex<GroupMetadata>,
    #[cfg(test)]
    failure_gate: std::sync::Mutex<Option<Arc<crate::ds::BodyGate>>>,
}

struct GroupLaneState {
    /// Waiting tickets contain only admission and accounting metadata. Their producers retain the
    /// envelope vectors until admission, so the lane owns payloads only after it admits them.
    waiting: std::collections::VecDeque<Arc<GroupTicket>>,
    /// At most GROUP_LANE_CAPACITY payloads are admitted ahead of the writer. The in-flight append
    /// is tracked separately, as it was with the old producer-side permit.
    admitted: std::collections::VecDeque<Arc<GroupTicket>>,
    in_flight: Option<Arc<GroupTicket>>,
    /// A failed reliable append is held until remove() claims it, so the pending/origin entry
    /// cannot be stranded in the race between append failure and group retirement.
    held_failed: Option<Arc<GroupTicket>>,
    /// Reserved batches not yet landed or dropped, per commit. An origin on this path with more
    /// entries than this is output still being produced (a flip query-back), not yet reserved.
    live: BTreeMap<u64, usize>,
    backlog_bytes: u64,
    peak_batches: usize,
    peak_bytes: u64,
    longest_wait: std::time::Duration,
    replay_held: bool,
    replay_held_since: Option<std::time::Instant>,
    threshold_bytes: [bool; 2],
    threshold_wait: [bool; 2],
    closed: bool,
}

#[derive(Clone, Default)]
struct GroupMetadata {
    stream_path: String,
    group_id: String,
    subscription: String,
    tables: Vec<String>,
}

struct GroupBatchEntry {
    commit_lsn: u64,
    stream_path: String,
    envs: std::sync::Mutex<Option<Vec<Envelope>>>,
    accounted: std::sync::atomic::AtomicBool,
    bytes: u64,
    enqueued_at: std::time::Instant,
}

struct GroupTicket {
    entry: Arc<GroupBatchEntry>,
    state: std::sync::atomic::AtomicU8,
}

pub(crate) struct GroupBatchHandle {
    lane: Arc<GroupLane>,
    ticket: Arc<GroupTicket>,
    batch: Option<Vec<Envelope>>,
}

const TICKET_WAITING: u8 = 0;
const TICKET_ADMITTED: u8 = 1;
const TICKET_CANCELLED: u8 = 2;

#[derive(Clone)]
pub(crate) struct GroupEmissionLanes {
    lanes: Arc<std::sync::Mutex<HashMap<String, Arc<GroupLane>>>>,
    ds: DsClient,
    shutdown: crate::shutdown::ShutdownToken,
    pending: Arc<PendingFlips>,
    origins: Arc<std::sync::Mutex<BTreeMap<(String, u64), usize>>>,
    wake: Arc<tokio::sync::Notify>,
    config: GroupLaneConfig,
    overflow_handler: Arc<std::sync::Mutex<Option<Arc<dyn Fn(String) + Send + Sync>>>>,
}

impl GroupEmissionLanes {
    pub(crate) fn new(ds: DsClient, shutdown: crate::shutdown::ShutdownToken, pending: Arc<PendingFlips>) -> Self {
        Self::new_with_config(ds, shutdown, pending, GroupLaneConfig::from_env())
    }

    pub(crate) fn new_with_config(
        ds: DsClient,
        shutdown: crate::shutdown::ShutdownToken,
        pending: Arc<PendingFlips>,
        config: GroupLaneConfig,
    ) -> Self {
        Self {
            lanes: Arc::new(std::sync::Mutex::new(HashMap::new())),
            ds,
            shutdown,
            pending,
            origins: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            wake: Arc::new(tokio::sync::Notify::new()),
            config,
            overflow_handler: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    pub(crate) fn set_overflow_handler(&self, handler: Arc<dyn Fn(String) + Send + Sync>) {
        *self.overflow_handler.lock().unwrap() = Some(handler);
    }

    fn lane(&self, path: &str) -> Arc<GroupLane> {
        let mut lanes = self.lanes.lock().unwrap();
        if let Some(sender) = lanes.get(path) {
            return sender.clone();
        }
        let lane = Arc::new(GroupLane {
            state: std::sync::Mutex::new(GroupLaneState {
                waiting: std::collections::VecDeque::new(),
                admitted: std::collections::VecDeque::new(),
                in_flight: None,
                held_failed: None,
                live: BTreeMap::new(),
                backlog_bytes: 0,
                peak_batches: 0,
                peak_bytes: 0,
                longest_wait: std::time::Duration::ZERO,
                replay_held: false,
                replay_held_since: None,
                threshold_bytes: [false; 2],
                threshold_wait: [false; 2],
                closed: false,
            }),
            wake: tokio::sync::Notify::new(),
            worker_wake: tokio::sync::Notify::new(),
            worker_done: tokio::sync::Notify::new(),
            pending: self.pending.clone(),
            origins: self.origins.clone(),
            origin_wake: self.wake.clone(),
            config: self.config,
            metadata: std::sync::Mutex::new(GroupMetadata {
                stream_path: path.to_string(),
                group_id: path.to_string(),
                ..Default::default()
            }),
            #[cfg(test)]
            failure_gate: std::sync::Mutex::new(None),
        });
        let worker_lane = lane.clone();
        let worker_ds = self.ds.clone();
        self.shutdown.clone().spawn_background(async move {
            loop {
                let notified = worker_lane.worker_wake.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let ticket = {
                    let mut state = worker_lane.state.lock().unwrap();
                    if state.closed
                        && state.waiting.is_empty()
                        && state.admitted.is_empty()
                        && state.in_flight.is_none()
                        && state.held_failed.is_none()
                    {
                        worker_lane.worker_done.notify_waiters();
                        return;
                    }
                    let ticket = state.admitted.pop_front();
                    if let Some(ticket) = &ticket {
                        state.in_flight = Some(ticket.clone());
                    }
                    drop(state);
                    if ticket.is_some() {
                        worker_lane.wake.notify_waiters();
                    }
                    ticket
                };
                let Some(ticket) = ticket else {
                    notified.await;
                    continue;
                };
                let envs = ticket.entry.envs.lock().unwrap().take();
                let Some(envs) = envs else {
                    let mut state = worker_lane.state.lock().unwrap();
                    if state.in_flight.as_ref().is_some_and(|active| Arc::ptr_eq(active, &ticket)) {
                        state.in_flight = None;
                    }
                    drop(state);
                    worker_lane.wake.notify_waiters();
                    continue;
                };
                let append = worker_ds.append_reliable(&ticket.entry.stream_path, &envs).await;
                if append.is_err() {
                    #[cfg(test)]
                    let failure_gate = worker_lane.failure_gate.lock().unwrap().clone();
                    #[cfg(test)]
                    if let Some(gate) = failure_gate {
                        gate.arrived.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        gate.entered.notify_one();
                        gate.released.notified().await;
                    }
                    let mut state = worker_lane.state.lock().unwrap();
                    let closed = state.closed;
                    if state.in_flight.as_ref().is_some_and(|active| Arc::ptr_eq(active, &ticket)) {
                        state.in_flight = None;
                    }
                    if !closed {
                        state.held_failed = Some(ticket.clone());
                    }
                    drop(state);
                    if closed {
                        complete_group_entry(&worker_lane, &ticket.entry);
                    }
                    // Keep the pending count and origin held for a failed live append until
                    // remove() claims the held entry, unless removal already won the race.
                    return;
                }
                let mut state = worker_lane.state.lock().unwrap();
                if state.in_flight.as_ref().is_some_and(|active| Arc::ptr_eq(active, &ticket)) {
                    state.in_flight = None;
                }
                drop(state);
                complete_group_entry(&worker_lane, &ticket.entry);
                worker_lane.wake.notify_waiters();
            }
        });
        lanes.insert(path.to_string(), lane.clone());
        lane
    }

    pub(crate) fn register(&self, path: &str) {
        let _ = self.lane(path);
    }

    pub(crate) fn set_metadata(&self, path: &str, group_id: &str, subscription: &str, tables: Vec<String>) {
        let lane = self.lane(path);
        *lane.metadata.lock().unwrap() = GroupMetadata {
            stream_path: path.to_string(),
            group_id: group_id.to_string(),
            subscription: subscription.to_string(),
            tables,
        };
    }

    fn existing_lane(&self, path: &str) -> Option<Arc<GroupLane>> {
        self.lanes.lock().unwrap().get(path).cloned()
    }

    #[cfg(test)]
    fn set_failure_gate(&self, path: &str, gate: Arc<crate::ds::BodyGate>) {
        if let Some(lane) = self.existing_lane(path) {
            *lane.failure_gate.lock().unwrap() = Some(gate);
        }
    }

    pub(crate) fn begin_origin(&self, path: &str, commit_lsn: u64) {
        *self.origins.lock().unwrap().entry((path.to_string(), commit_lsn)).or_default() += 1;
    }

    /// Complete an origin that no reserved batch carries: a flip or create batch that has
    /// finished reserving its output. A later commit's batch waiting on this group may now be
    /// admitted, so this lane's waiters re-check.
    pub(crate) fn complete_origin(&self, path: &str, commit_lsn: u64) {
        complete_origin_entry(&self.origins, &self.wake, path, commit_lsn);
        if let Some(lane) = self.existing_lane(path) {
            lane.wake.notify_waiters();
        }
    }

    #[cfg(test)]
    pub(crate) fn has_origin_at_or_before(&self, paths: &HashSet<String>, marker_lsn: u64) -> bool {
        self.origins.lock().unwrap().iter().any(|((path, lsn), _)| *lsn <= marker_lsn && paths.contains(path))
    }

    /// Whether any group output originating at or before `marker_lsn` has yet to land, on any
    /// group. The sequencer's published replay position waits on this; its loop never does.
    pub(crate) fn has_any_origin_at_or_before(&self, marker_lsn: u64) -> bool {
        self.origins.lock().unwrap().keys().any(|(_, lsn)| *lsn <= marker_lsn)
    }

    pub(crate) fn wake(&self) -> Arc<tokio::sync::Notify> {
        self.wake.clone()
    }

    /// Observe sequencer progress for the wait cap. A group only ages while another commit has
    /// landed, so a complete Durable Streams outage remains a transport stall rather than a group
    /// failure.
    pub(crate) fn progress(&self, commit_lsn: u64) {
        let lanes = self.lanes.lock().unwrap().values().cloned().collect::<Vec<_>>();
        for lane in lanes {
            let (overflow, waited) = {
                let mut state = lane.state.lock().unwrap();
                let oldest = state.waiting.front().or_else(|| state.admitted.front()).or(state.in_flight.as_ref());
                let Some(oldest) = oldest else { continue };
                if commit_lsn <= oldest.entry.commit_lsn {
                    continue;
                }
                let waited = oldest.entry.enqueued_at.elapsed();
                state.longest_wait = state.longest_wait.max(waited);
                for (i, fraction) in [0.5, 0.9].into_iter().enumerate() {
                    if self.config.max_stall > std::time::Duration::ZERO
                        && waited >= self.config.max_stall.mul_f64(fraction)
                        && !state.threshold_wait[i]
                    {
                        state.threshold_wait[i] = true;
                        emit_group_threshold(&lane, fraction, "wait", &state);
                    }
                }
                if waited >= std::time::Duration::from_secs(1) && !state.replay_held {
                    state.replay_held = true;
                    state.replay_held_since = Some(oldest.entry.enqueued_at + std::time::Duration::from_secs(1));
                }
                (self.config.max_stall > std::time::Duration::ZERO && waited > self.config.max_stall, waited)
            };
            if overflow {
                self.fail_lane(&lane, "wait", waited, commit_lsn);
            }
        }
    }

    fn fail_lane(&self, lane: &Arc<GroupLane>, cap: &str, stalled: std::time::Duration, lag_commits: u64) {
        let (path, dropped) = {
            let mut state = lane.state.lock().unwrap();
            if state.closed {
                return;
            }
            let meta = lane.metadata.lock().unwrap().clone();
            tracing::event!(tracing::Level::WARN, event = "group_backlog_overflow", group_id = %meta.group_id,
                subscription = %meta.subscription, tables = ?meta.tables, cap,
                backlog_batches = state.waiting.len() + state.admitted.len(), backlog_bytes = queued_bytes(&state),
                cap_bytes = lane.config.max_backlog_bytes, cap_secs = lane.config.max_stall.as_secs(),
                stalled_secs = stalled.as_secs_f64(), lag_commits, lag_lsn_bytes = 0u64);
            state.closed = true;
            let mut dropped = state.waiting.drain(..).collect::<Vec<_>>();
            dropped.extend(state.admitted.drain(..));
            if let Some(ticket) = state.held_failed.take() {
                dropped.push(ticket);
            }
            emit_group_high_water(lane, &state);
            (meta.stream_path, dropped)
        };
        for ticket in dropped {
            complete_group_entry(lane, &ticket.entry);
        }
        lane.wake.notify_waiters();
        lane.worker_wake.notify_one();
        if let Some(handler) = self.overflow_handler.lock().unwrap().clone() {
            handler(path);
        }
    }

    /// Reserve one complete group batch. This is synchronous so callers can record the order while
    /// holding the registry lock; [`GroupBatchHandle::wait`] performs the bounded capacity wait
    /// after that lock is released.
    ///
    /// Every producer of a group's output (direct member output, subquery member output from the
    /// registry, flip query-backs) reserves here, and the lane keeps commit order across them: a
    /// batch is queued behind every batch of its own or an earlier commit, and is admitted only
    /// when no earlier commit still has output being produced for this group (an origin no
    /// reserved batch accounts for). A flip for commit T that reserves after a batch for T+1 is
    /// therefore still appended first.
    pub(crate) fn reserve(&self, path: &str, envs: Vec<Envelope>, commit_lsn: u64) -> Option<GroupBatchHandle> {
        if envs.is_empty() {
            return None;
        }
        let path = path.to_string();
        let lane = self.existing_lane(&path)?;
        self.reserve_on_lane(lane, &path, envs, commit_lsn)
    }

    fn reserve_on_lane(
        &self,
        lane: Arc<GroupLane>,
        path: &str,
        envs: Vec<Envelope>,
        commit_lsn: u64,
    ) -> Option<GroupBatchHandle> {
        let bytes = serde_json::to_vec(&envs).map(|v| v.len() as u64).unwrap_or(0);
        {
            let state = lane.state.lock().unwrap();
            let active_bytes = state.in_flight.as_ref().map(|ticket| ticket.entry.bytes).unwrap_or(0);
            if !state.closed
                && self.config.max_backlog_bytes > 0
                && state.backlog_bytes.saturating_sub(active_bytes).saturating_add(bytes)
                    > self.config.max_backlog_bytes
            {
                drop(state);
                self.fail_lane(&lane, "bytes", std::time::Duration::ZERO, commit_lsn);
                return None;
            }
        }
        self.pending.add(commit_lsn);
        self.begin_origin(path, commit_lsn);
        let entry = Arc::new(GroupBatchEntry {
            commit_lsn,
            stream_path: path.to_string(),
            envs: std::sync::Mutex::new(None),
            accounted: std::sync::atomic::AtomicBool::new(false),
            bytes,
            enqueued_at: std::time::Instant::now(),
        });
        let mut state = lane.state.lock().unwrap();
        *state.live.entry(commit_lsn).or_default() += 1;
        state.backlog_bytes = state.backlog_bytes.saturating_add(bytes);
        state.peak_batches = state.peak_batches.max(state.waiting.len() + state.admitted.len() + 1);
        state.peak_bytes = state.peak_bytes.max(queued_bytes(&state));
        for (i, fraction) in [0.5, 0.9].into_iter().enumerate() {
            if self.config.max_backlog_bytes > 0
                && queued_bytes(&state) as f64 >= self.config.max_backlog_bytes as f64 * fraction
                && !state.threshold_bytes[i]
            {
                state.threshold_bytes[i] = true;
                emit_group_threshold(&lane, fraction, "bytes", &state);
            }
        }
        if state.closed {
            drop(state);
            complete_group_entry(&lane, &entry);
            return None;
        }
        let ticket = Arc::new(GroupTicket { entry, state: std::sync::atomic::AtomicU8::new(TICKET_WAITING) });
        // Commit order, then reservation order within a commit. Batches nearly always arrive in
        // commit order, so the position is found from the back.
        let at = state.waiting.iter().rposition(|queued| queued.entry.commit_lsn <= commit_lsn).map_or(0, |i| i + 1);
        state.waiting.insert(at, ticket.clone());
        drop(state);
        Some(GroupBatchHandle { lane, ticket, batch: Some(envs) })
    }

    /// Queue one complete group batch and await its queue-position admission. The queue entry is
    /// reserved before the await, preserving evaluation order without holding the registry lock.
    pub(crate) async fn enqueue(&self, path: &str, envs: Vec<Envelope>, commit_lsn: u64) {
        if let Some(handle) = self.reserve(path, envs, commit_lsn) {
            handle.wait().await;
        }
    }

    /// Hand one source transaction's direct output for a group to that group's lane without
    /// waiting on the caller, so a slow or full group never holds the source sequencer. The
    /// reservation is synchronous, so the batch's pending count and origin are visible at once to
    /// the next transaction, a source receipt and the published replay position.
    pub(crate) fn submit(&self, path: &str, envs: Vec<Envelope>, commit_lsn: u64) {
        if let Some(handle) = self.reserve(path, envs, commit_lsn) {
            self.spawn_wait([handle]);
        }
    }

    /// Keep a reserved batch's admission wait off the source sequencer. The lane still admits
    /// batches in commit order and retains the bounded payload window; only the producer's task
    /// waits when this group's window is full.
    pub(crate) fn spawn_wait(&self, handles: impl IntoIterator<Item = GroupBatchHandle>) {
        for handle in handles {
            let shutdown = self.shutdown.clone();
            shutdown.spawn_background(async move {
                handle.wait().await;
            });
        }
    }

    pub(crate) fn remove(&self, path: &str) {
        let lane = self.lanes.lock().unwrap().remove(path);
        if let Some(lane) = lane {
            let dropped = {
                let mut state = lane.state.lock().unwrap();
                state.closed = true;
                let mut dropped = Vec::new();
                for ticket in state.waiting.drain(..) {
                    ticket.state.store(TICKET_CANCELLED, std::sync::atomic::Ordering::Release);
                    dropped.push(ticket);
                }
                dropped.extend(state.admitted.drain(..));
                if let Some(ticket) = state.held_failed.take() {
                    dropped.push(ticket);
                }
                emit_group_high_water(&lane, &state);
                dropped
            };
            for ticket in dropped {
                complete_group_entry(&lane, &ticket.entry);
            }
            lane.wake.notify_waiters();
            lane.worker_wake.notify_one();
        }
    }
}

impl GroupBatchHandle {
    pub(crate) async fn wait(mut self) {
        loop {
            let notified = self.lane.wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            enum Admission {
                Admitted,
                Waiting,
                Cancelled,
            }
            let admission = {
                let mut state = self.lane.state.lock().unwrap();
                while state
                    .waiting
                    .front()
                    .is_some_and(|front| front.state.load(std::sync::atomic::Ordering::Acquire) == TICKET_CANCELLED)
                {
                    state.waiting.pop_front();
                }
                match self.ticket.state.load(std::sync::atomic::Ordering::Acquire) {
                    TICKET_CANCELLED => Admission::Cancelled,
                    TICKET_ADMITTED => Admission::Admitted,
                    _ if state.closed => {
                        self.ticket.state.store(TICKET_CANCELLED, std::sync::atomic::Ordering::Release);
                        Admission::Cancelled
                    }
                    _ if state.waiting.front().is_some_and(|front| Arc::ptr_eq(front, &self.ticket))
                        && state.admitted.len() < GROUP_LANE_CAPACITY
                        && !earlier_output_in_production(&self.lane, &state, &self.ticket.entry) =>
                    {
                        let batch = self.batch.take().expect("a waiting handle owns its batch");
                        *self.ticket.entry.envs.lock().unwrap() = Some(batch);
                        state.waiting.pop_front();
                        self.ticket.state.store(TICKET_ADMITTED, std::sync::atomic::Ordering::Release);
                        state.admitted.push_back(self.ticket.clone());
                        Admission::Admitted
                    }
                    _ => Admission::Waiting,
                }
            };
            match admission {
                Admission::Admitted => {
                    self.lane.worker_wake.notify_one();
                    return;
                }
                Admission::Cancelled => {
                    complete_group_entry(&self.lane, &self.ticket.entry);
                    self.lane.wake.notify_waiters();
                    return;
                }
                Admission::Waiting => notified.await,
            }
        }
    }
}

impl Drop for GroupBatchHandle {
    fn drop(&mut self) {
        let state = self.ticket.state.load(std::sync::atomic::Ordering::Acquire);
        if state == TICKET_ADMITTED || state == TICKET_CANCELLED {
            return;
        }
        if self
            .ticket
            .state
            .compare_exchange(
                TICKET_WAITING,
                TICKET_CANCELLED,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_ok()
        {
            complete_group_entry(&self.lane, &self.ticket.entry);
            self.lane.wake.notify_waiters();
        }
    }
}

fn complete_group_entry(lane: &GroupLane, entry: &GroupBatchEntry) {
    if entry.accounted.swap(true, std::sync::atomic::Ordering::AcqRel) {
        return;
    }
    let _ = entry.envs.lock().unwrap().take();
    lane.pending.complete(entry.commit_lsn);
    // The origin goes first: in between, the commit shows fewer origins than live batches, which
    // admission reads as nothing in production, never as a phantom flip.
    complete_origin_entry(&lane.origins, &lane.origin_wake, &entry.stream_path, entry.commit_lsn);
    let mut state = lane.state.lock().unwrap();
    state.backlog_bytes = state.backlog_bytes.saturating_sub(entry.bytes);
    if state.backlog_bytes < lane.config.max_backlog_bytes / 4 {
        state.threshold_bytes = [false; 2];
        state.threshold_wait = [false; 2];
    }
    if state.replay_held && state.waiting.is_empty() && state.admitted.is_empty() && state.in_flight.is_none() {
        if let Some(since) = state.replay_held_since.take() {
            emit_group_replay_held(lane, since.elapsed());
        }
        state.replay_held = false;
    }
    if state.longest_wait < entry.enqueued_at.elapsed() {
        state.longest_wait = entry.enqueued_at.elapsed();
    }
    if let Some(count) = state.live.get_mut(&entry.commit_lsn) {
        *count -= 1;
        if *count == 0 {
            state.live.remove(&entry.commit_lsn);
        }
    }
}

fn queued_bytes(state: &GroupLaneState) -> u64 {
    state.backlog_bytes.saturating_sub(state.in_flight.as_ref().map(|ticket| ticket.entry.bytes).unwrap_or(0))
}

fn emit_group_threshold(lane: &GroupLane, fraction: f64, cap: &str, state: &GroupLaneState) {
    let meta = lane.metadata.lock().unwrap().clone();
    tracing::event!(tracing::Level::INFO, event = "group_backlog_threshold", group_id = %meta.group_id,
        subscription = %meta.subscription, tables = ?meta.tables, cap, fraction,
        backlog_batches = state.waiting.len() + state.admitted.len(), backlog_bytes = queued_bytes(state),
        cap_bytes = lane.config.max_backlog_bytes, cap_secs = lane.config.max_stall.as_secs(),
        stalled_secs = state.longest_wait.as_secs_f64(), lag_commits = 0u64, lag_lsn_bytes = 0u64);
}

fn emit_group_high_water(lane: &GroupLane, state: &GroupLaneState) {
    let meta = lane.metadata.lock().unwrap().clone();
    tracing::event!(tracing::Level::INFO, event = "group_backlog_high_water", group_id = %meta.group_id,
        tables = ?meta.tables, peak_batches = state.peak_batches, peak_bytes = state.peak_bytes,
        longest_wait_secs = state.longest_wait.as_secs_f64());
}

fn emit_group_replay_held(lane: &GroupLane, waited: std::time::Duration) {
    let meta = lane.metadata.lock().unwrap().clone();
    tracing::event!(tracing::Level::INFO, event = "group_replay_held", group_id = %meta.group_id,
        tables = ?meta.tables, held_ms = waited.as_millis() as u64);
}

/// Whether an earlier commit than `entry`'s still has output being produced for this group: an
/// origin on the path that outnumbers the batches already reserved for that commit. Those batches
/// are ahead of `entry` in the lane, so only unreserved output can still overtake.
fn earlier_output_in_production(lane: &GroupLane, state: &GroupLaneState, entry: &GroupBatchEntry) -> bool {
    let origins = lane.origins.lock().unwrap();
    origins
        .range((entry.stream_path.clone(), 0)..(entry.stream_path.clone(), entry.commit_lsn))
        .any(|((_, lsn), count)| *count > state.live.get(lsn).copied().unwrap_or(0))
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
        let lane = lanes.lane("shape/group");
        lanes.remove("shape/group");
        let envelope: Envelope = serde_json::from_value(serde_json::json!({
            "type": "shape/group", "key": "1", "headers": {"operation": "upsert"}
        }))
        .unwrap();
        assert!(lanes.reserve_on_lane(lane, "shape/group", vec![envelope], 0x42).is_none());
        assert_eq!(pending.count(), 0);
        assert!(!lanes.has_origin_at_or_before(&HashSet::from(["shape/group".to_string()]), 0x42));
    }

    #[tokio::test]
    async fn backlog_byte_cap_closes_only_the_overflowed_lane() {
        let store = Arc::new(crate::ds::ScriptedStore::default());
        let pending = Arc::new(PendingFlips::default());
        let lanes = GroupEmissionLanes::new_with_config(
            DsClient::with_test_store("scripted://group-backlog-cap".into(), store),
            crate::shutdown::ShutdownToken::new(),
            pending.clone(),
            GroupLaneConfig { max_backlog_bytes: 1, max_stall: std::time::Duration::from_secs(60) },
        );
        lanes.register("shape/group-a");
        lanes.register("shape/group-b");
        let env = |key: &str| Envelope {
            type_: "public.items".into(),
            key: key.into(),
            value: Some(serde_json::json!({"payload": "012345678901234567890123456789"})),
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
        let failed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let failed_copy = failed.clone();
        lanes.set_overflow_handler(Arc::new(move |path| failed_copy.lock().unwrap().push(path)));
        let _ = lanes.reserve("shape/group-a", vec![env("a")], 1);
        let other = lanes.reserve("shape/group-b", vec![env("c")], 3).expect("other group unaffected");
        drop(other);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while failed.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("overflow handler did not run");
        assert_eq!(failed.lock().unwrap().as_slice(), ["shape/group-a"]);
        assert_eq!(pending.count(), 0, "dropping the test handle releases the unrelated group's pending output");
    }

    /// `submit` never waits on its caller. A batch behind an earlier commit's output still in
    /// production on the same group (here a flip still propagating) is reserved at once, counts
    /// toward the replay and receipt barriers, and is appended after that output even though the
    /// flip reserves later; another group's batch lands meanwhile.
    #[tokio::test]
    async fn a_submitted_batch_waits_behind_earlier_output_on_its_own_group_only() {
        let store = Arc::new(crate::ds::ScriptedStore::default());
        let pending = Arc::new(PendingFlips::default());
        let lanes = GroupEmissionLanes::new(
            DsClient::with_test_store("scripted://group-submit".into(), store.clone()),
            crate::shutdown::ShutdownToken::new(),
            pending.clone(),
        );
        lanes.register("shape/group-held");
        lanes.register("shape/group-free");
        let env = |key: &str| Envelope {
            type_: "public.items".into(),
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
        let keys_on = |suffix: &str| {
            store
                .appended
                .lock()
                .unwrap()
                .iter()
                .filter(|(path, _, _)| path.ends_with(suffix))
                .map(|(_, _, body)| serde_json::from_slice::<Vec<serde_json::Value>>(body).unwrap()[0]["key"].clone())
                .collect::<Vec<_>>()
        };
        // Commit 1's flip is still propagating on the held group, as the flip propagator records it.
        pending.add(1);
        lanes.begin_origin("shape/group-held", 1);
        lanes.submit("shape/group-held", vec![env("commit-2")], 2);
        lanes.submit("shape/group-free", vec![env("commit-2")], 2);
        assert!(lanes.has_any_origin_at_or_before(2), "the waiting batch holds the replay boundary");
        assert!(pending.has_at_or_before(2), "the waiting batch holds a receipt at its commit");
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while keys_on("shape/group-free").is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("another group's batch waited behind the held group");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(keys_on("shape/group-held").is_empty(), "commit 2 overtook commit 1's flip");
        // The flip enqueues its effect, then finishes propagating.
        lanes.enqueue("shape/group-held", vec![env("commit-1-flip")], 1).await;
        lanes.complete_origin("shape/group-held", 1);
        pending.complete(1);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while lanes.has_any_origin_at_or_before(2) || pending.count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the held group's output never landed");
        assert_eq!(
            keys_on("shape/group-held"),
            vec![serde_json::json!("commit-1-flip"), serde_json::json!("commit-2")]
        );
    }

    /// Two flips in production on one group at once (the propagator runs several): the later
    /// commit's query-back finishes and reserves first, yet the earlier commit's output, reserved
    /// after it, is appended first. Neither is admitted while the earlier flip is still running.
    #[tokio::test]
    async fn concurrent_flips_land_in_commit_order_whichever_reserves_first() {
        let store = Arc::new(crate::ds::ScriptedStore::default());
        let pending = Arc::new(PendingFlips::default());
        let lanes = GroupEmissionLanes::new(
            DsClient::with_test_store("scripted://group-flip-order".into(), store.clone()),
            crate::shutdown::ShutdownToken::new(),
            pending.clone(),
        );
        lanes.register("shape/group");
        let env = |key: &str| Envelope {
            type_: "public.items".into(),
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
        let keys = || {
            store
                .appended
                .lock()
                .unwrap()
                .iter()
                .map(|(_, _, body)| serde_json::from_slice::<Vec<serde_json::Value>>(body).unwrap()[0]["key"].clone())
                .collect::<Vec<_>>()
        };
        for commit_lsn in [3, 4] {
            pending.add(commit_lsn);
            lanes.begin_origin("shape/group", commit_lsn);
        }
        let later = lanes.reserve("shape/group", vec![env("flip-4")], 4).unwrap();
        lanes.spawn_wait([later]);
        lanes.complete_origin("shape/group", 4);
        pending.complete(4);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(keys().is_empty(), "commit 4's flip was admitted while commit 3's flip was still running");
        let earlier = lanes.reserve("shape/group", vec![env("flip-3")], 3).unwrap();
        lanes.spawn_wait([earlier]);
        lanes.complete_origin("shape/group", 3);
        pending.complete(3);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while keys().len() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the flips never landed");
        assert_eq!(keys(), vec![serde_json::json!("flip-3"), serde_json::json!("flip-4")]);
        assert!(!lanes.has_any_origin_at_or_before(u64::MAX));
        assert_eq!(pending.count(), 0);
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
        let mut new_wait = tokio::spawn(new.wait());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut new_wait).await.is_err(),
            "a later reservation was admitted before the front ticket"
        );
        assert!(store.appended.lock().unwrap().is_empty());
        tokio::time::timeout(std::time::Duration::from_secs(1), old.wait()).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), new_wait).await.unwrap().unwrap();
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
    async fn capacity_admission_stays_in_reservation_order_across_producers() {
        let store = Arc::new(crate::ds::ScriptedStore::default());
        let lanes = GroupEmissionLanes::new(
            DsClient::with_test_store("scripted://group-capacity-order".into(), store.clone()),
            crate::shutdown::ShutdownToken::new(),
            Arc::new(PendingFlips::default()),
        );
        lanes.register("shape/group");
        fn env(key: String) -> Envelope {
            Envelope {
                type_: "shape/group".into(),
                key,
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
            }
        }

        let mut first_producer = Vec::new();
        for i in 0..=128u64 {
            first_producer.push(lanes.reserve("shape/group", vec![env(format!("x{i}"))], i).unwrap());
        }
        let first_waiters = first_producer.split_off(GROUP_LANE_CAPACITY);
        for handle in first_producer {
            tokio::time::timeout(std::time::Duration::from_secs(10), handle.wait()).await.unwrap();
        }
        let mut second_producer = Vec::new();
        for i in 0..GROUP_LANE_CAPACITY {
            second_producer.push(lanes.reserve("shape/group", vec![env(format!("y{i}"))], 1000 + i as u64).unwrap());
        }
        let second_tasks = second_producer.into_iter().map(|handle| tokio::spawn(handle.wait())).collect::<Vec<_>>();
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        let first_task = tokio::spawn(async move {
            for handle in first_waiters {
                tokio::time::timeout(std::time::Duration::from_secs(10), handle.wait()).await.unwrap();
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(10), first_task).await.unwrap().unwrap();
        for task in second_tasks {
            tokio::time::timeout(std::time::Duration::from_secs(10), task).await.unwrap().unwrap();
        }
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if store.appended.lock().unwrap().len() == 129 + GROUP_LANE_CAPACITY {
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
            .map(|(_, _, body)| {
                serde_json::from_slice::<Vec<serde_json::Value>>(body).unwrap()[0]["key"].as_str().unwrap().to_string()
            })
            .collect::<Vec<_>>();
        let mut expected = (0..=128u64).map(|i| format!("x{i}")).collect::<Vec<_>>();
        expected.extend((0..GROUP_LANE_CAPACITY).map(|i| format!("y{i}")));
        assert_eq!(keys, expected);
    }

    #[tokio::test]
    async fn concurrent_producers_are_fifo_and_bounded_while_append_is_held() {
        let store = Arc::new(crate::ds::ScriptedStore::default());
        let gate = Arc::new(crate::ds::BodyGate::default());
        *store.append_fault.lock().unwrap() =
            Some(("shape/group".into(), 0, crate::ds::AppendFault::Hold(gate.clone())));
        let lanes = GroupEmissionLanes::new(
            DsClient::with_test_store("scripted://group-concurrent-capacity".into(), store.clone()),
            crate::shutdown::ShutdownToken::new(),
            Arc::new(PendingFlips::default()),
        );
        lanes.register("shape/group");
        let lane = lanes.existing_lane("shape/group").unwrap();
        let env = |key: u64| Envelope {
            type_: "shape/group".into(),
            key: key.to_string(),
            value: Some(serde_json::json!({ "payload": "x".repeat(4096) })),
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

        let first = lanes.reserve("shape/group", vec![env(0)], 0).unwrap();
        let first_wait = tokio::spawn(first.wait());
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while gate.arrived.load(std::sync::atomic::Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let mut producers = Vec::new();
        for key in 1..=130u64 {
            let handle = lanes.reserve("shape/group", vec![env(key)], key).unwrap();
            producers.push(tokio::spawn(handle.wait()));
        }
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if producers.iter().take(GROUP_LANE_CAPACITY).all(tokio::task::JoinHandle::is_finished) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!producers[GROUP_LANE_CAPACITY].is_finished(), "the 65th admitted batch bypassed capacity");
        assert_eq!(store.appended.lock().unwrap().len(), 0);
        {
            let state = lane.state.lock().unwrap();
            assert!(state.admitted.len() + usize::from(state.in_flight.is_some()) <= GROUP_LANE_CAPACITY + 1);
            assert!(state.waiting.iter().all(|ticket| ticket.entry.envs.lock().unwrap().is_none()));
        }

        gate.released.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(10), first_wait).await.unwrap().unwrap();
        for task in producers {
            tokio::time::timeout(std::time::Duration::from_secs(2), task).await.unwrap().unwrap();
        }
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if store.appended.lock().unwrap().len() == 131 {
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
            .map(|(_, _, body)| {
                serde_json::from_slice::<Vec<serde_json::Value>>(body).unwrap()[0]["key"]
                    .as_str()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(keys, (0..=130).collect::<Vec<_>>());
    }

    fn failure_test_envelope() -> Envelope {
        Envelope {
            type_: "shape/group".into(),
            key: "failure".into(),
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
        }
    }

    #[tokio::test]
    async fn remove_wins_append_failure_before_failed_batch_is_parked() {
        let store =
            Arc::new(crate::ds::ScriptedStore { fail_append_path: Some("shape/group".into()), ..Default::default() });
        let append_shutdown = crate::shutdown::ShutdownToken::new();
        let lane_shutdown = crate::shutdown::ShutdownToken::new();
        let gate = Arc::new(crate::ds::BodyGate::default());
        let pending = Arc::new(PendingFlips::default());
        let ds = DsClient::with_test_store("scripted://group-failure-before-park".into(), store)
            .with_shutdown(append_shutdown.clone());
        let lanes = GroupEmissionLanes::new(ds, lane_shutdown, pending.clone());
        lanes.register("shape/group");
        lanes.set_failure_gate("shape/group", gate.clone());
        let enqueue = tokio::spawn({
            let lanes = lanes.clone();
            async move { lanes.enqueue("shape/group", vec![failure_test_envelope()], 7).await }
        });
        append_shutdown.begin();
        tokio::time::timeout(std::time::Duration::from_secs(1), gate.entered.notified()).await.unwrap();
        assert_eq!(pending.count(), 1);
        lanes.remove("shape/group");
        gate.released.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(10), enqueue).await.unwrap().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while pending.count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!lanes.has_origin_at_or_before(&HashSet::from(["shape/group".to_string()]), 7));
    }

    #[tokio::test]
    async fn parked_append_failure_is_released_by_remove() {
        let store =
            Arc::new(crate::ds::ScriptedStore { fail_append_path: Some("shape/group".into()), ..Default::default() });
        let append_shutdown = crate::shutdown::ShutdownToken::new();
        let lane_shutdown = crate::shutdown::ShutdownToken::new();
        let gate = Arc::new(crate::ds::BodyGate::default());
        let pending = Arc::new(PendingFlips::default());
        let ds = DsClient::with_test_store("scripted://group-failure-parked".into(), store)
            .with_shutdown(append_shutdown.clone());
        let lanes = GroupEmissionLanes::new(ds, lane_shutdown, pending.clone());
        lanes.register("shape/group");
        lanes.set_failure_gate("shape/group", gate.clone());
        let enqueue = tokio::spawn({
            let lanes = lanes.clone();
            async move { lanes.enqueue("shape/group", vec![failure_test_envelope()], 8).await }
        });
        append_shutdown.begin();
        tokio::time::timeout(std::time::Duration::from_secs(1), gate.entered.notified()).await.unwrap();
        gate.released.notify_one();
        let lane = lanes.existing_lane("shape/group").unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if lane.state.lock().unwrap().held_failed.is_some() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(pending.count(), 1);
        lanes.remove("shape/group");
        tokio::time::timeout(std::time::Duration::from_secs(10), enqueue).await.unwrap().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while pending.count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!lanes.has_origin_at_or_before(&HashSet::from(["shape/group".to_string()]), 8));
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
        let lane = lanes.existing_lane("shape/group").unwrap();
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
        let enqueue = tokio::spawn({
            let lanes = lanes.clone();
            async move { lanes.enqueue("shape/group", vec![env("in-flight")], 1).await }
        });
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while gate.arrived.load(std::sync::atomic::Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let worker_done = lane.worker_done.notified();
        tokio::pin!(worker_done);
        worker_done.as_mut().enable();
        let queued = lanes.reserve("shape/group", vec![env("queued")], 2).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), queued.wait()).await.unwrap();
        assert_eq!(pending.count(), 2);
        lanes.remove("shape/group");
        assert_eq!(pending.count(), 1);
        gate.released.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(10), enqueue).await.unwrap().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while pending.count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), worker_done).await.unwrap();
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
        let first = tokio::spawn({
            let lanes = lanes.clone();
            async move { lanes.enqueue("shape/group", vec![env(0)], 0).await }
        });
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while gate.arrived.load(std::sync::atomic::Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        for commit_lsn in 1..=GROUP_LANE_CAPACITY as u64 {
            tokio::time::timeout(
                std::time::Duration::from_secs(10),
                lanes.enqueue("shape/group", vec![env(commit_lsn)], commit_lsn),
            )
            .await
            .unwrap();
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
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                while gate.arrived.load(std::sync::atomic::Ordering::Acquire) == before {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        gate.released.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(10), next).await.unwrap().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(10), first).await.unwrap().unwrap();
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
