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
    /// grouped output uses [`GroupEmissionLanes`], whose lanes detach instead of growing.
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

// ---------------------------------------------------------------------------------------------
// Group lanes: one barrier per group (OTTO-11131, `dec-circuits-group-per-group-barriers`).
//
// Every group stream has its own lane, and nothing outside the group waits on it: producers
// (the sequencer, the flip workers, a subquery create) hand a batch over and return at once, and
// the replay position, the checkpoint and the shared source receipts never wait for a group
// append. A commit is atomic within each group, not across all streams.
//
//   * **Commit order per group.** A batch is queued by `(commit LSN, reservation order)`. Work a
//     commit still has in production for the group (a deferred flip whose query-back has not
//     produced its rows yet) is registered with [`GroupEmissionLanes::begin_origin`] before the
//     sequencer moves on, and no batch of a later commit is appended while it is outstanding. So
//     T's delayed flip lands before T+1's plain output, without the sequencer waiting for it.
//   * **Coalesced appends.** The lane's one writer appends every appendable queued batch, up to
//     `append_bytes`, in one request. A stream whose appends are slow but answer therefore keeps
//     up with the source instead of falling further behind with every commit.
//   * **Detach.** A lane that has work and has landed nothing for `detach_after` while the
//     sequencer keeps moving, or whose queue outgrows `memory_bytes`, detaches: its unlanded
//     output is dropped, a hung append is abandoned, and the group keeps a resume point (the
//     oldest commit it failed to deliver). Each detach doubles the lane's `detach_after`, up to
//     `detach_after_max`, and a lane that then stays healthy for `detach_after_max` gets the base
//     back, so a merely slow group does not detach and reattach over and over. When the whole
//     engine stops (a Durable Streams outage stops the change log as well), no lane accrues stall
//     time and none detaches for the outage alone.
//   * **Reattach.** A detached lane probes its stream with a `HEAD`, backing off to
//     `probe_after_max`. Once the stream answers, the lane queues a must-refetch marker ahead of
//     every later batch and takes live output again; its readers reload the group from a snapshot
//     at the marker. The resume point is cleared only when the marker has landed.
//   * **Durable resume point.** Each checkpoint records every group with output at or before its
//     highwater that has not landed ([`GroupEmissionLanes::behind_at_or_before`]); a restart
//     restores such a group detached, so it reattaches behind a marker instead of silently
//     missing the commits the checkpoint already passed.

/// The must-refetch marker's envelope type. It names no table, so a group reader cannot mistake
/// it for a row; the SDK reloads the group from a snapshot when it reads one.
pub(crate) const GROUP_CONTROL_TYPE: &str = "__circuits.control";
pub(crate) const MUST_REFETCH: &str = "must-refetch";

/// Group lane limits, read once per engine from the environment.
#[derive(Clone, Debug)]
pub(crate) struct GroupLaneConfig {
    /// How long a lane with work may land nothing, while the sequencer keeps moving, before it
    /// detaches. Doubles with each detach up to `detach_after_max`.
    pub(crate) detach_after: std::time::Duration,
    pub(crate) detach_after_max: std::time::Duration,
    /// Queued bytes past which a lane detaches, not counting the largest queued commit (nor the
    /// in-flight append): one commit's output never trips it, however large, wherever it sits in
    /// the queue and however many batches it arrives in, so an oversized transaction still
    /// arrives whole.
    pub(crate) memory_bytes: u64,
    /// The slowest append throughput a lane waits out before it counts an in-flight append as
    /// stalled: an append of `b` bytes gets `b / append_min_bytes_per_sec` beyond the stall cap,
    /// so a transaction near the 512 MiB ceiling is never abandoned while it is still sending.
    pub(crate) append_min_bytes_per_sec: u64,
    /// The most queued bytes one coalesced append carries (a single larger batch goes alone).
    pub(crate) append_bytes: u64,
    /// How often the monitor measures stalls and probes detached lanes.
    pub(crate) tick: std::time::Duration,
    pub(crate) probe_timeout: std::time::Duration,
    pub(crate) probe_after_max: std::time::Duration,
}

impl Default for GroupLaneConfig {
    fn default() -> Self {
        Self {
            detach_after: std::time::Duration::from_secs(10),
            detach_after_max: std::time::Duration::from_secs(300),
            memory_bytes: 8 << 20,
            append_min_bytes_per_sec: 1 << 20,
            append_bytes: 1 << 20,
            tick: std::time::Duration::from_secs(1),
            probe_timeout: std::time::Duration::from_secs(3),
            probe_after_max: std::time::Duration::from_secs(5),
        }
    }
}

impl GroupLaneConfig {
    pub(crate) fn from_env() -> Self {
        fn env_u64(name: &str) -> Option<u64> {
            let raw = std::env::var(name).ok()?;
            match raw.trim().parse::<u64>() {
                Ok(value) => Some(value),
                Err(_) => {
                    tracing::warn!(name, raw, "ignoring an unparseable group lane setting");
                    None
                }
            }
        }
        let mut config = Self::default();
        if let Some(secs) = env_u64("ELECTRIC_CIRCUITS_GROUP_DETACH_AFTER_SECS") {
            config.detach_after = std::time::Duration::from_secs(secs.max(1));
        }
        if let Some(secs) = env_u64("ELECTRIC_CIRCUITS_GROUP_DETACH_AFTER_MAX_SECS") {
            config.detach_after_max = std::time::Duration::from_secs(secs);
        }
        config.detach_after_max = config.detach_after_max.max(config.detach_after);
        if let Some(bytes) = env_u64("ELECTRIC_CIRCUITS_GROUP_LANE_MEMORY_BYTES") {
            config.memory_bytes = bytes.max(1);
        }
        if let Some(bytes) = env_u64("ELECTRIC_CIRCUITS_GROUP_APPEND_BYTES") {
            config.append_bytes = bytes.max(1);
        }
        config
    }
}

/// The sequencer's progress, which a lane's stall clock runs against: a lane accrues stall time
/// only while the engine is otherwise moving.
#[derive(Default)]
pub(crate) struct SequencerProgress {
    commits: std::sync::atomic::AtomicU64,
    lsn: std::sync::atomic::AtomicU64,
}

/// What a group is, for its telemetry.
#[derive(Clone, Debug, Default)]
pub(crate) struct GroupMeta {
    pub(crate) subscription: String,
    pub(crate) tables: Vec<String>,
}

impl GroupMeta {
    pub(crate) fn of(group: &GroupRecord) -> Self {
        Self {
            subscription: group.subscription.clone(),
            tables: group.members.iter().map(|member| member.table.clone()).collect(),
        }
    }
}

/// A per-group source receipt: whether every output of this group at or before a commit is on
/// its stream. `must_refetch` means the group dropped output at or before it and its readers
/// reload from a snapshot at the must-refetch marker.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupDrain {
    pub group_id: String,
    pub drained: bool,
    pub must_refetch: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resume_lsn: Option<String>,
}

pub(crate) fn lsn_text(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xffff_ffff)
}

#[derive(Clone, Copy, Debug)]
struct Gap {
    resume_lsn: u64,
    through_lsn: u64,
    since: tokio::time::Instant,
    dropped_batches: u64,
}

impl Gap {
    fn widen(&mut self, lsn: u64) {
        self.resume_lsn = self.resume_lsn.min(lsn);
        self.through_lsn = self.through_lsn.max(lsn);
    }
}

#[derive(Clone, Copy, Debug)]
enum Phase {
    Attached,
    /// Output is dropped until the stream answers a probe.
    Detached(Gap),
    /// The must-refetch marker is queued or in flight; live output queues behind it.
    Reattaching(Gap),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Cap {
    Bytes,
    Stall,
}

impl Cap {
    fn name(self) -> &'static str {
        match self {
            Cap::Bytes => "bytes",
            Cap::Stall => "stall",
        }
    }
    fn index(self) -> usize {
        match self {
            Cap::Bytes => 0,
            Cap::Stall => 1,
        }
    }
}

struct Queued {
    envs: Vec<Envelope>,
    bytes: u64,
    /// Part of a member's activation drain, which the byte cap never counts.
    exempt: bool,
}

struct InFlight {
    /// The oldest and newest commit the append carries; `None` for the marker alone.
    lsns: Option<(u64, u64)>,
    bytes: u64,
    batches: u64,
    marker: bool,
    started: tokio::time::Instant,
}

impl InFlight {
    /// How long this append may take before it counts as hung: the base stall cap plus time for
    /// its size at the slowest throughput a lane waits out.
    fn allowance(&self, config: &GroupLaneConfig) -> std::time::Duration {
        config.detach_after + std::time::Duration::from_secs(self.bytes / config.append_min_bytes_per_sec.max(1))
    }
}

struct Probe {
    running: bool,
    next_at: tokio::time::Instant,
    delay: std::time::Duration,
}

struct GroupLaneState {
    /// Commits with output for this group still in production (deferred flips), by commit LSN.
    producing: BTreeMap<u64, usize>,
    queued: BTreeMap<(u64, u64), Queued>,
    queued_bytes: u64,
    /// Queued bytes from activation drains, still queued and outside the byte cap.
    exempt_bytes: u64,
    next_seq: u64,
    marker: Option<Envelope>,
    in_flight: Option<InFlight>,
    /// Bumped whenever an in-flight append must be abandoned (detach, close).
    epoch: u64,
    closed: bool,
    phase: Phase,
    /// The last gap whose must-refetch marker landed: a receipt for a commit inside it still says
    /// `mustRefetch`, since that commit's output was dropped and only the marker covers it.
    healed: Option<(u64, u64)>,
    /// Stall time accrued while the engine moved and this lane landed nothing.
    behind: std::time::Duration,
    landed: bool,
    detach_after: std::time::Duration,
    healthy_since: tokio::time::Instant,
    probe: Probe,
    meta: GroupMeta,
    /// Per cap, whether the 50% and 90% threshold events may fire again.
    armed: [[bool; 2]; 2],
    peak_batches: u64,
    peak_bytes: u64,
    longest_stall: std::time::Duration,
}

impl GroupLaneState {
    fn has_work(&self) -> bool {
        !self.queued.is_empty() || self.in_flight.is_some() || self.marker.is_some()
    }

    /// Whether the lane is waiting on its stream: an append is in flight, or one could start.
    /// Queued batches held behind the group's own work in production (a deferred flip's
    /// query-back) are not waiting on the stream, so they accrue no stall time.
    fn waiting_on_stream(&self) -> bool {
        let barrier = self.producing.keys().next().copied().unwrap_or(u64::MAX);
        self.in_flight.is_some()
            || self.marker.is_some()
            || self.queued.keys().next().is_some_and(|(lsn, _)| *lsn <= barrier)
    }

    /// Queued bytes the byte cap counts: everything but activation drains and the largest queued
    /// commit. A walk of the queue, so callers take it only once the total could matter (the
    /// counted bytes above a quarter of the cap, where threshold events re-arm).
    fn capped_bytes(&self) -> u64 {
        let mut largest = 0u64;
        let mut current: Option<(u64, u64)> = None;
        for (&(lsn, _), queued) in self.queued.iter().filter(|(_, queued)| !queued.exempt) {
            current = match current {
                Some((commit, bytes)) if commit == lsn => Some((commit, bytes + queued.bytes)),
                Some((_, bytes)) => {
                    largest = largest.max(bytes);
                    Some((lsn, queued.bytes))
                }
                None => Some((lsn, queued.bytes)),
            };
        }
        if let Some((_, bytes)) = current {
            largest = largest.max(bytes);
        }
        self.queued_bytes - self.exempt_bytes - largest
    }

    /// The stall cap now in force: the lane's backed-off cap, plus the time an in-flight append
    /// of its size may take at the slowest throughput the lane waits out.
    fn stall_cap(&self, config: &GroupLaneConfig) -> std::time::Duration {
        let in_flight = self.in_flight.as_ref().map_or(0, |flight| flight.bytes);
        self.detach_after + std::time::Duration::from_secs(in_flight / config.append_min_bytes_per_sec.max(1))
    }

    /// The oldest commit whose output for this group is not on its stream, if any.
    fn oldest_outstanding(&self) -> Option<u64> {
        let gap = match self.phase {
            Phase::Detached(gap) | Phase::Reattaching(gap) => Some(gap.resume_lsn),
            Phase::Attached => None,
        };
        [
            gap,
            self.producing.keys().next().copied(),
            self.queued.keys().next().map(|(lsn, _)| *lsn),
            self.in_flight.as_ref().and_then(|flight| flight.lsns).map(|(oldest, _)| oldest),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    fn lag_commits(&self) -> u64 {
        let mut commits = std::collections::BTreeSet::new();
        commits.extend(self.queued.keys().map(|(lsn, _)| *lsn));
        commits.extend(self.producing.keys().copied());
        commits.len() as u64 + self.in_flight.as_ref().map_or(0, |flight| flight.batches.min(1))
    }
}

struct GroupLane {
    path: String,
    state: std::sync::Mutex<GroupLaneState>,
    worker_wake: tokio::sync::Notify,
    epoch: tokio::sync::watch::Sender<u64>,
    shared: Arc<LaneShared>,
}

struct LaneShared {
    config: GroupLaneConfig,
    progress: SequencerProgress,
    metrics: Arc<crate::metrics::Metrics>,
}

/// Every group lane of one engine. Cheap to clone.
#[derive(Clone)]
pub(crate) struct GroupEmissionLanes {
    lanes: Arc<std::sync::Mutex<HashMap<String, Arc<GroupLane>>>>,
    ds: DsClient,
    shutdown: crate::shutdown::ShutdownToken,
    shared: Arc<LaneShared>,
    /// Set once the monitor runs: it starts with the first lane, inside the runtime.
    monitor: Arc<std::sync::atomic::AtomicBool>,
}

impl GroupEmissionLanes {
    pub(crate) fn new(ds: DsClient, shutdown: crate::shutdown::ShutdownToken, config: GroupLaneConfig) -> Self {
        let shared =
            Arc::new(LaneShared { config, progress: SequencerProgress::default(), metrics: ds.metrics().clone() });
        Self {
            lanes: Arc::new(std::sync::Mutex::new(HashMap::new())),
            ds,
            shutdown,
            shared,
            monitor: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// The sequencer finished a source transaction. A lane's stall clock runs only while this
    /// moves, which is what keeps a whole-engine outage from detaching every group.
    pub(crate) fn note_commit(&self, commit_lsn: u64) {
        self.shared.progress.commits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.shared.progress.lsn.fetch_max(commit_lsn, std::sync::atomic::Ordering::Relaxed);
    }

    fn lane(&self, path: &str) -> Arc<GroupLane> {
        let mut lanes = self.lanes.lock().unwrap();
        if let Some(lane) = lanes.get(path) {
            return lane.clone();
        }
        let now = tokio::time::Instant::now();
        let lane = Arc::new(GroupLane {
            path: path.to_string(),
            state: std::sync::Mutex::new(GroupLaneState {
                producing: BTreeMap::new(),
                queued: BTreeMap::new(),
                queued_bytes: 0,
                exempt_bytes: 0,
                next_seq: 0,
                marker: None,
                in_flight: None,
                healed: None,
                epoch: 0,
                closed: false,
                phase: Phase::Attached,
                behind: std::time::Duration::ZERO,
                landed: false,
                detach_after: self.shared.config.detach_after,
                healthy_since: now,
                probe: Probe { running: false, next_at: now, delay: self.shared.config.tick },
                meta: GroupMeta::default(),
                armed: [[true; 2]; 2],
                peak_batches: 0,
                peak_bytes: 0,
                longest_stall: std::time::Duration::ZERO,
            }),
            worker_wake: tokio::sync::Notify::new(),
            epoch: tokio::sync::watch::Sender::new(0),
            shared: self.shared.clone(),
        });
        self.shutdown.clone().spawn_background(run_group_lane(lane.clone(), self.ds.clone()));
        lanes.insert(path.to_string(), lane.clone());
        if !self.monitor.swap(true, std::sync::atomic::Ordering::AcqRel) {
            self.spawn_monitor();
        }
        lane
    }

    pub(crate) fn register(&self, path: &str) {
        let _ = self.lane(path);
    }

    /// Restore a group that a checkpoint recorded as behind: it starts detached at its durable
    /// resume point and reattaches behind a must-refetch marker once its stream answers.
    pub(crate) fn register_detached(&self, path: &str, resume_lsn: u64, meta: GroupMeta) {
        let lane = self.lane(path);
        let mut state = lane.state.lock().unwrap();
        if state.closed {
            return;
        }
        state.meta = meta;
        let gap = match state.phase {
            Phase::Detached(mut gap) | Phase::Reattaching(mut gap) => {
                gap.widen(resume_lsn);
                gap
            }
            Phase::Attached => {
                Gap { resume_lsn, through_lsn: resume_lsn, since: tokio::time::Instant::now(), dropped_batches: 0 }
            }
        };
        if matches!(state.phase, Phase::Attached) {
            self.shared.metrics.group_lanes_detached.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        state.phase = Phase::Detached(gap);
        self.shared.metrics.group_detaches.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let config = &self.shared.config;
        tracing::warn!(
            event = "group_detached",
            group_id = %path,
            subscription = %state.meta.subscription,
            member_tables = ?state.meta.tables,
            cap = "restart",
            backlog_batches = 0u64,
            backlog_bytes = 0u64,
            in_flight_bytes = 0u64,
            memory_cap_bytes = config.memory_bytes,
            stall_cap_secs = state.detach_after.as_secs(),
            next_stall_cap_secs = state.detach_after.as_secs(),
            seconds_stalled = 0u64,
            lag_commits = 0u64,
            lag_lsn_bytes = 0u64,
            resume_lsn = %lsn_text(gap.resume_lsn),
            dropped_batches = gap.dropped_batches,
            "group restored behind its checkpoint; it reattaches behind a must-refetch marker"
        );
    }

    pub(crate) fn set_meta(&self, path: &str, meta: GroupMeta) {
        if let Some(lane) = self.existing_lane(path) {
            lane.state.lock().unwrap().meta = meta;
        }
    }

    fn existing_lane(&self, path: &str) -> Option<Arc<GroupLane>> {
        self.lanes.lock().unwrap().get(path).cloned()
    }

    /// Work for `path` at `commit_lsn` is in production (a deferred flip): no later commit's batch
    /// is appended to the group until [`Self::complete_origin`].
    pub(crate) fn begin_origin(&self, path: &str, commit_lsn: u64) {
        if let Some(lane) = self.existing_lane(path) {
            let mut state = lane.state.lock().unwrap();
            if !state.closed {
                *state.producing.entry(commit_lsn).or_default() += 1;
            }
        }
    }

    pub(crate) fn complete_origin(&self, path: &str, commit_lsn: u64) {
        if let Some(lane) = self.existing_lane(path) {
            let mut state = lane.state.lock().unwrap();
            if let Some(count) = state.producing.get_mut(&commit_lsn) {
                *count -= 1;
                if *count == 0 {
                    state.producing.remove(&commit_lsn);
                }
            }
            drop(state);
            lane.worker_wake.notify_one();
        }
    }

    /// Hand one complete batch of a commit's group output to its lane. Never waits: the lane
    /// orders it by commit, and a lane that cannot keep up detaches instead of slowing its
    /// producer. Callers may hold the subquery registry lock, so evaluation order is queue order.
    pub(crate) fn push(&self, path: &str, envs: Vec<Envelope>, commit_lsn: u64) {
        if envs.is_empty() {
            return;
        }
        let Some(lane) = self.existing_lane(path) else { return };
        lane.push(envs, commit_lsn);
    }

    /// Queue a group member's activation drain: the changes that committed while the member was
    /// being created, by commit. They are bounded by the pending buffer's own cap and arrive in one
    /// burst before the lane's writer can run, so they are queued outside the byte cap; a lane
    /// that then cannot deliver them still detaches at its stall cap.
    pub(crate) fn push_drain(&self, path: &str, batches: Vec<(u64, Vec<Envelope>)>) {
        let Some(lane) = self.existing_lane(path) else { return };
        lane.push_uncapped(batches);
    }

    /// The restored checkpoint's highwater: every commit at or before it was sequenced by an
    /// earlier process, and its group output has either landed or been recorded as a resume point
    /// and restored detached, so a per-group receipt for it need not wait for a new commit.
    pub(crate) fn note_restored_highwater(&self, commit_lsn: u64) {
        self.shared.progress.lsn.fetch_max(commit_lsn, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn remove(&self, path: &str) {
        let lane = self.lanes.lock().unwrap().remove(path);
        if let Some(lane) = lane {
            lane.close();
        }
    }

    /// Every group with output at or before `marker_lsn` that is not on its stream, with the
    /// oldest such commit: the per-group resume points a checkpoint at that highwater records.
    pub(crate) fn behind_at_or_before(&self, marker_lsn: u64) -> BTreeMap<String, u64> {
        let lanes: Vec<_> = self.lanes.lock().unwrap().values().cloned().collect();
        lanes
            .into_iter()
            .filter_map(|lane| {
                let state = lane.state.lock().unwrap();
                if state.closed {
                    return None;
                }
                state
                    .oldest_outstanding()
                    .filter(|oldest| *oldest <= marker_lsn)
                    .map(|oldest| (lane.path.clone(), oldest))
            })
            .collect()
    }

    /// The per-group source receipt for a commit: `None` when the engine serves no such group.
    pub(crate) fn drain(&self, path: &str, marker_lsn: u64) -> Option<GroupDrain> {
        let lane = self.existing_lane(path)?;
        let state = lane.state.lock().unwrap();
        if state.closed {
            return None;
        }
        let gap = match state.phase {
            Phase::Detached(gap) | Phase::Reattaching(gap) if gap.resume_lsn <= marker_lsn => Some(gap),
            _ => None,
        };
        // A commit inside the last healed gap had its output dropped; the marker on the stream
        // covers it, so it is drained, but its readers must refetch.
        let healed = state.healed.filter(|(resume, through)| (*resume..=*through).contains(&marker_lsn));
        // Only a commit this process has sequenced can be on the lane's books at all: after a
        // crash, a durable receipt can name a commit whose group output the replay has not yet
        // re-derived.
        let sequenced = marker_lsn <= self.shared.progress.lsn.load(std::sync::atomic::Ordering::Relaxed);
        let drained = sequenced && gap.is_none() && state.oldest_outstanding().is_none_or(|oldest| oldest > marker_lsn);
        Some(GroupDrain {
            group_id: lane.path.clone(),
            drained,
            must_refetch: gap.is_some() || healed.is_some(),
            resume_lsn: gap.map(|gap| gap.resume_lsn).or(healed.map(|(resume, _)| resume)).map(lsn_text),
        })
    }

    fn spawn_monitor(&self) {
        let lanes = self.clone();
        self.shutdown.clone().spawn_background(async move {
            let mut last_commits = lanes.shared.progress.commits.load(std::sync::atomic::Ordering::Relaxed);
            let mut ticker = tokio::time::interval(lanes.shared.config.tick);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let commits = lanes.shared.progress.commits.load(std::sync::atomic::Ordering::Relaxed);
                let engine_moved = commits != last_commits;
                last_commits = commits;
                for lane in lanes.tick(engine_moved) {
                    let ds = lanes.ds.clone();
                    lanes.shutdown.spawn_background(async move { lane.probe(&ds).await });
                }
            }
        });
    }

    /// One monitor pass over every lane: accrue or clear stall time, detach a lane past its cap,
    /// and return the detached lanes whose next probe is due.
    fn tick(&self, engine_moved: bool) -> Vec<Arc<GroupLane>> {
        let all: Vec<_> = self.lanes.lock().unwrap().values().cloned().collect();
        all.into_iter().filter(|lane| lane.tick(engine_moved)).collect()
    }

    /// Run one monitor pass inline, probes included, without the background monitor.
    #[cfg(test)]
    async fn tick_for_test(&self, engine_moved: bool) {
        for lane in self.tick(engine_moved) {
            lane.probe(&self.ds).await;
        }
    }

    /// Lanes without the background monitor, for tests that drive [`Self::tick_for_test`].
    #[cfg(test)]
    fn unmonitored(ds: DsClient, config: GroupLaneConfig) -> Self {
        let shared =
            Arc::new(LaneShared { config, progress: SequencerProgress::default(), metrics: ds.metrics().clone() });
        // Most lane tests reason about one lane alone: treat every commit as already sequenced.
        shared.progress.lsn.store(u64::MAX / 2, std::sync::atomic::Ordering::Relaxed);
        Self {
            lanes: Arc::new(std::sync::Mutex::new(HashMap::new())),
            ds,
            shutdown: crate::shutdown::ShutdownToken::new(),
            shared,
            monitor: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        }
    }
}

impl GroupLane {
    fn push(&self, envs: Vec<Envelope>, commit_lsn: u64) {
        let bytes: u64 = envs.iter().map(crate::ds::envelope_memory_bytes).sum();
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return;
        }
        if let Phase::Detached(gap) = &mut state.phase {
            gap.widen(commit_lsn);
            gap.dropped_batches += 1;
            return;
        }
        let seq = state.next_seq;
        state.next_seq += 1;
        state.queued.insert((commit_lsn, seq), Queued { envs, bytes, exempt: false });
        state.queued_bytes += bytes;
        state.peak_batches = state.peak_batches.max(state.queued.len() as u64);
        state.peak_bytes = state.peak_bytes.max(state.queued_bytes);
        self.backlog_changed(bytes as i64, 1);
        let limit = self.shared.config.memory_bytes;
        let counted = state.queued_bytes - state.exempt_bytes;
        let capped = if counted >= limit / 4 { state.capped_bytes() } else { counted };
        // A push that jumps past the cap still reports the thresholds it crossed on the way.
        self.threshold(&mut state, Cap::Bytes, capped as f64 / limit as f64);
        if capped > limit {
            self.detach(&mut state, Cap::Bytes);
        } else {
            drop(state);
            self.worker_wake.notify_one();
        }
    }

    fn push_uncapped(&self, batches: Vec<(u64, Vec<Envelope>)>) {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return;
        }
        for (commit_lsn, envs) in batches {
            if envs.is_empty() {
                continue;
            }
            if let Phase::Detached(gap) = &mut state.phase {
                gap.widen(commit_lsn);
                gap.dropped_batches += 1;
                continue;
            }
            let bytes: u64 = envs.iter().map(crate::ds::envelope_memory_bytes).sum();
            let seq = state.next_seq;
            state.next_seq += 1;
            state.queued.insert((commit_lsn, seq), Queued { envs, bytes, exempt: true });
            state.queued_bytes += bytes;
            state.exempt_bytes += bytes;
            state.peak_batches = state.peak_batches.max(state.queued.len() as u64);
            state.peak_bytes = state.peak_bytes.max(state.queued_bytes);
            self.backlog_changed(bytes as i64, 1);
        }
        drop(state);
        self.worker_wake.notify_one();
    }

    fn backlog_changed(&self, bytes: i64, batches: i64) {
        use std::sync::atomic::Ordering::Relaxed;
        let metrics = &self.shared.metrics;
        if bytes >= 0 {
            metrics.group_lane_backlog_bytes.fetch_add(bytes as u64, Relaxed);
        } else {
            let _ = metrics
                .group_lane_backlog_bytes
                .fetch_update(Relaxed, Relaxed, |v| Some(v.saturating_sub((-bytes) as u64)));
        }
        if batches >= 0 {
            metrics.group_lane_backlog_batches.fetch_add(batches as u64, Relaxed);
        } else {
            let _ = metrics
                .group_lane_backlog_batches
                .fetch_update(Relaxed, Relaxed, |v| Some(v.saturating_sub((-batches) as u64)));
        }
    }

    /// Take the next append: the marker alone if one is due, otherwise every queued batch no
    /// earlier in-production commit holds back, oldest first, up to the append budget.
    fn take_appendable(&self, state: &mut GroupLaneState) -> Option<(Vec<Envelope>, u64)> {
        if state.closed || state.in_flight.is_some() || matches!(state.phase, Phase::Detached(_)) {
            return None;
        }
        if let Some(marker) = state.marker.take() {
            state.in_flight =
                Some(InFlight { lsns: None, bytes: 0, batches: 0, marker: true, started: tokio::time::Instant::now() });
            return Some((vec![marker], state.epoch));
        }
        let barrier = state.producing.keys().next().copied().unwrap_or(u64::MAX);
        let budget = self.shared.config.append_bytes;
        let mut keys = Vec::new();
        let mut bytes = 0u64;
        for (key, queued) in &state.queued {
            if key.0 > barrier || (!keys.is_empty() && bytes + queued.bytes > budget) {
                break;
            }
            bytes += queued.bytes;
            keys.push(*key);
        }
        let (first, last) = (keys.first()?.0, keys.last()?.0);
        let mut envs = Vec::new();
        for key in &keys {
            if let Some(queued) = state.queued.remove(key) {
                if queued.exempt {
                    state.exempt_bytes -= queued.bytes;
                }
                envs.extend(queued.envs);
            }
        }
        state.queued_bytes -= bytes;
        self.backlog_changed(-(bytes as i64), -(keys.len() as i64));
        state.in_flight = Some(InFlight {
            lsns: Some((first, last)),
            bytes,
            batches: keys.len() as u64,
            marker: false,
            started: tokio::time::Instant::now(),
        });
        Some((envs, state.epoch))
    }

    fn landed(&self, epoch: u64) {
        let mut state = self.state.lock().unwrap();
        if state.epoch != epoch {
            return;
        }
        let Some(flight) = state.in_flight.take() else { return };
        state.landed = true;
        if flight.marker
            && let Phase::Reattaching(gap) = state.phase
        {
            state.phase = Phase::Attached;
            state.healed = Some((gap.resume_lsn, gap.through_lsn));
            state.healthy_since = tokio::time::Instant::now();
            self.shared.metrics.group_lanes_detached.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            tracing::info!(
                event = "group_refetch_marker_landed",
                group_id = %self.path,
                resume_lsn = %lsn_text(gap.resume_lsn),
                through_lsn = %lsn_text(gap.through_lsn),
                "group's must-refetch marker is on its stream; its resume point is cleared"
            );
        }
    }

    /// One monitor tick. Returns whether to start a probe of a detached lane's stream.
    fn tick(&self, engine_moved: bool) -> bool {
        let config = &self.shared.config;
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return false;
        }
        let now = tokio::time::Instant::now();
        if let Phase::Detached(mut gap) = state.phase {
            // An append a byte-cap detach kept is still sending whole commits. It is abandoned
            // once past the time its size allows, and until it lands or is abandoned the lane
            // does not probe: reattaching would only queue output behind it again.
            if let Some((started, allowance, lsns, batches)) = state
                .in_flight
                .as_ref()
                .map(|flight| (flight.started, flight.allowance(config), flight.lsns, flight.batches))
            {
                if now.duration_since(started) < allowance {
                    return false;
                }
                if let Some((oldest, newest)) = lsns {
                    gap.widen(oldest);
                    gap.widen(newest);
                }
                gap.dropped_batches += batches;
                state.phase = Phase::Detached(gap);
                state.in_flight = None;
                state.epoch += 1;
                self.epoch.send_replace(state.epoch);
                tracing::warn!(
                    event = "group_append_abandoned",
                    group_id = %self.path,
                    resume_lsn = %lsn_text(gap.resume_lsn),
                    "a detached group's kept append did not land within the time its size allows; abandoned"
                );
            }
            if !state.probe.running && now >= state.probe.next_at {
                state.probe.running = true;
                return true;
            }
            return false;
        }
        if state.landed || !state.has_work() {
            state.behind = std::time::Duration::ZERO;
        } else if engine_moved && state.waiting_on_stream() {
            state.behind += config.tick;
            state.longest_stall = state.longest_stall.max(state.behind);
        }
        state.landed = false;
        let cap = state.stall_cap(config);
        let fraction = state.behind.as_secs_f64() / cap.as_secs_f64();
        self.threshold(&mut state, Cap::Stall, fraction);
        if state.behind >= cap {
            self.detach(&mut state, Cap::Stall);
            return false;
        }
        if matches!(state.phase, Phase::Attached)
            && state.behind.is_zero()
            && state.detach_after > config.detach_after
            && now.duration_since(state.healthy_since) >= config.detach_after_max
        {
            state.detach_after = config.detach_after;
        }
        false
    }

    /// Detach: drop what has not landed, abandon a hung append and keep the resume point.
    fn detach(&self, state: &mut GroupLaneState, cap: Cap) {
        // A byte-cap detach is about what queued behind the in-flight append, not the append
        // itself: an append of whole commits that is still sending (the stall cap, which gives it
        // time for its size, decides when it is hung) finishes, and the gap starts after it, so
        // an oversized transaction in flight still arrives whole.
        let config = &self.shared.config;
        let keep_in_flight = cap == Cap::Bytes
            && state.in_flight.as_ref().is_some_and(|flight| {
                !flight.marker && tokio::time::Instant::now().duration_since(flight.started) < flight.allowance(config)
            });
        let mut lsns: Vec<u64> = state.queued.keys().map(|(lsn, _)| *lsn).collect();
        lsns.extend(state.producing.keys().copied());
        if !keep_in_flight && let Some((oldest, newest)) = state.in_flight.as_ref().and_then(|flight| flight.lsns) {
            lsns.extend([oldest, newest]);
        }
        let progress = self.shared.progress.lsn.load(std::sync::atomic::Ordering::Relaxed);
        let (backlog_batches, backlog_bytes) = (state.queued.len() as u64, state.queued_bytes);
        let dropped = backlog_batches
            + if keep_in_flight { 0 } else { state.in_flight.as_ref().map_or(0, |flight| flight.batches) };
        let mut gap = match state.phase {
            Phase::Reattaching(gap) => gap,
            _ => Gap {
                resume_lsn: lsns.iter().copied().min().unwrap_or(progress),
                through_lsn: lsns.iter().copied().max().unwrap_or(progress),
                since: tokio::time::Instant::now(),
                dropped_batches: 0,
            },
        };
        for lsn in &lsns {
            gap.widen(*lsn);
        }
        gap.dropped_batches += dropped;
        let in_flight_bytes = state.in_flight.as_ref().map_or(0, |flight| flight.bytes);
        self.backlog_changed(-(backlog_bytes as i64), -(backlog_batches as i64));
        state.queued.clear();
        state.queued_bytes = 0;
        state.exempt_bytes = 0;
        state.marker = None;
        if !keep_in_flight {
            state.in_flight = None;
            state.epoch += 1;
            self.epoch.send_replace(state.epoch);
        }
        if matches!(state.phase, Phase::Attached) {
            self.shared.metrics.group_lanes_detached.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        state.phase = Phase::Detached(gap);
        let stalled = state.behind;
        state.behind = std::time::Duration::ZERO;
        state.landed = false;
        let config = &self.shared.config;
        let detached_after = state.detach_after;
        state.detach_after = (state.detach_after * 2).min(config.detach_after_max);
        // The first detach since a healthy spell probes at once. A repeat waits out the cap it
        // just hit, so a stream whose `HEAD` answers while its appends still hang cannot cycle
        // detach and reattach faster than the stall backoff.
        let first_probe = if detached_after > config.detach_after { detached_after } else { config.tick };
        state.probe = Probe { running: false, next_at: tokio::time::Instant::now() + first_probe, delay: config.tick };
        state.armed = [[true; 2]; 2];
        self.shared.metrics.group_detaches.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::warn!(
            event = "group_detached",
            group_id = %self.path,
            subscription = %state.meta.subscription,
            member_tables = ?state.meta.tables,
            cap = cap.name(),
            backlog_batches,
            backlog_bytes,
            in_flight_bytes,
            memory_cap_bytes = config.memory_bytes,
            stall_cap_secs = detached_after.as_secs(),
            next_stall_cap_secs = state.detach_after.as_secs(),
            seconds_stalled = stalled.as_secs(),
            lag_commits = lsns.iter().collect::<std::collections::BTreeSet<_>>().len(),
            lag_lsn_bytes = progress.saturating_sub(gap.resume_lsn),
            resume_lsn = %lsn_text(gap.resume_lsn),
            dropped_batches = gap.dropped_batches,
            "group fell behind and detached; nothing else waits on it, its output is dropped until its stream \
             answers, and its readers reload from a snapshot at the must-refetch marker"
        );
        self.high_water(state, "detached");
    }

    async fn probe(&self, ds: &DsClient) {
        let config = &self.shared.config;
        let head = tokio::time::timeout(config.probe_timeout, ds.head(&self.path)).await;
        let answered = matches!(&head, Ok(Ok(Some(head))) if !head.closed);
        let mut state = self.state.lock().unwrap();
        state.probe.running = false;
        if state.closed {
            return;
        }
        let Phase::Detached(gap) = state.phase else { return };
        if !answered {
            state.probe.delay = (state.probe.delay * 2).min(config.probe_after_max);
            state.probe.next_at = tokio::time::Instant::now() + state.probe.delay;
            return;
        }
        state.marker = Some(must_refetch_marker(&self.path, &gap));
        state.phase = Phase::Reattaching(gap);
        state.behind = std::time::Duration::ZERO;
        state.landed = false;
        self.shared.metrics.group_reattaches.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::warn!(
            event = "group_reattached",
            group_id = %self.path,
            subscription = %state.meta.subscription,
            member_tables = ?state.meta.tables,
            resume_lsn = %lsn_text(gap.resume_lsn),
            through_lsn = %lsn_text(gap.through_lsn),
            detached_secs = gap.since.elapsed().as_secs(),
            dropped_batches = gap.dropped_batches,
            "detached group's stream answers again; it reattached behind a must-refetch marker"
        );
        drop(state);
        self.worker_wake.notify_one();
    }

    fn threshold(&self, state: &mut GroupLaneState, cap: Cap, fraction: f64) {
        let armed = &mut state.armed[cap.index()];
        if fraction < 0.25 {
            *armed = [true; 2];
            return;
        }
        let mut crossed = Vec::new();
        if fraction >= 0.5 && armed[0] {
            armed[0] = false;
            crossed.push(0.5);
        }
        if fraction >= 0.9 && armed[1] {
            armed[1] = false;
            crossed.push(0.9);
        }
        if crossed.is_empty() {
            return;
        }
        let progress = self.shared.progress.lsn.load(std::sync::atomic::Ordering::Relaxed);
        let oldest = state.oldest_outstanding();
        for crossed in crossed {
            tracing::warn!(
            event = "group_backlog_threshold",
            group_id = %self.path,
            subscription = %state.meta.subscription,
            member_tables = ?state.meta.tables,
            cap = cap.name(),
            fraction = crossed,
            backlog_batches = state.queued.len(),
            backlog_bytes = state.queued_bytes,
            memory_cap_bytes = self.shared.config.memory_bytes,
            stall_cap_secs = state.detach_after.as_secs(),
            seconds_stalled = state.behind.as_secs(),
            lag_commits = state.lag_commits(),
            lag_lsn_bytes = oldest.map_or(0, |oldest| progress.saturating_sub(oldest)),
            "group lane is approaching a detach cap"
            );
        }
    }

    fn high_water(&self, state: &GroupLaneState, ended_by: &str) {
        tracing::info!(
            event = "group_backlog_high_water",
            group_id = %self.path,
            subscription = %state.meta.subscription,
            member_tables = ?state.meta.tables,
            ended_by,
            peak_backlog_batches = state.peak_batches,
            peak_backlog_bytes = state.peak_bytes,
            longest_stall_secs = state.longest_stall.as_secs(),
            "group lane high-water marks"
        );
    }

    fn close(&self) {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return;
        }
        state.closed = true;
        let (bytes, batches) = (state.queued_bytes, state.queued.len() as u64);
        self.backlog_changed(-(bytes as i64), -(batches as i64));
        if !matches!(state.phase, Phase::Attached) {
            self.shared.metrics.group_lanes_detached.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
        state.queued.clear();
        state.queued_bytes = 0;
        state.exempt_bytes = 0;
        state.producing.clear();
        state.marker = None;
        state.in_flight = None;
        state.epoch += 1;
        self.epoch.send_replace(state.epoch);
        self.high_water(&state, "released");
        drop(state);
        self.worker_wake.notify_one();
    }
}

fn must_refetch_marker(path: &str, gap: &Gap) -> Envelope {
    Envelope {
        type_: GROUP_CONTROL_TYPE.to_string(),
        key: MUST_REFETCH.to_string(),
        value: Some(serde_json::json!({
            "control": MUST_REFETCH,
            "reason": "group-detached",
            "groupId": path,
            "resumeLsn": lsn_text(gap.resume_lsn),
            "throughLsn": lsn_text(gap.through_lsn),
        })),
        old: None,
        headers: EnvelopeHeaders {
            operation: "control".to_string(),
            txid: None,
            offset: None,
            lsn: None,
            seq: None,
            last: Some(true),
            commit_ts: None,
        },
    }
}

/// A lane's one writer: appends in queue order, coalescing, and abandons an append when the lane
/// detaches or closes under it (a hung request never holds the lane past its detach).
async fn run_group_lane(lane: Arc<GroupLane>, ds: DsClient) {
    let mut epochs = lane.epoch.subscribe();
    loop {
        let notified = lane.worker_wake.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let next = {
            let mut state = lane.state.lock().unwrap();
            if state.closed {
                return;
            }
            lane.take_appendable(&mut state)
        };
        let Some((envs, epoch)) = next else {
            notified.await;
            continue;
        };
        let landed = tokio::select! {
            biased;
            _ = epochs.wait_for(|current| *current != epoch) => None,
            landed = ds.append_reliable(&lane.path, &envs) => Some(landed),
        };
        match landed {
            // Detached or closed mid-append: the detach already accounted for this batch.
            None => continue,
            // Only shutdown interrupts a reliable append.
            Some(Err(_)) => return,
            // A retired stream (`Ok(false)`) discards: the group is being released.
            Some(Ok(_)) => lane.landed(epoch),
        }
    }
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

    const GROUP: &str = "shape/group-7";

    fn group_env(key: &str) -> Envelope {
        Envelope {
            type_: "public.items".into(),
            key: key.into(),
            value: Some(serde_json::json!({ "id": key })),
            old: None,
            headers: EnvelopeHeaders {
                operation: "upsert".into(),
                txid: None,
                offset: None,
                lsn: None,
                seq: None,
                last: None,
                commit_ts: None,
            },
        }
    }

    fn test_config() -> GroupLaneConfig {
        GroupLaneConfig {
            detach_after: std::time::Duration::from_secs(3),
            detach_after_max: std::time::Duration::from_secs(12),
            ..GroupLaneConfig::default()
        }
    }

    /// Lanes over a scripted store whose group stream is held until the returned sender says
    /// otherwise, driven by explicit monitor ticks.
    fn held_lanes(
        config: GroupLaneConfig,
    ) -> (GroupEmissionLanes, Arc<crate::ds::ScriptedStore>, tokio::sync::watch::Sender<bool>) {
        let store = Arc::new(crate::ds::ScriptedStore::default());
        let (hold, held) = tokio::sync::watch::channel(true);
        *store.stream_hold.lock().unwrap() = Some((GROUP.into(), held));
        let lanes = GroupEmissionLanes::unmonitored(
            DsClient::with_test_store("scripted://group".into(), store.clone()),
            config,
        );
        lanes.register(GROUP);
        (lanes, store, hold)
    }

    /// Every envelope appended to the group stream, in order, and how many appends carried them.
    fn appended(store: &crate::ds::ScriptedStore) -> (Vec<String>, usize) {
        let log = store.appended.lock().unwrap();
        let appends: Vec<_> = log.iter().filter(|(path, _, _)| path.ends_with(GROUP)).collect();
        let keys = appends
            .iter()
            .flat_map(|(_, _, body)| serde_json::from_slice::<Vec<serde_json::Value>>(body).unwrap())
            .map(|event| event["key"].as_str().unwrap().to_string())
            .collect();
        (keys, appends.len())
    }

    async fn wait_appended(store: &crate::ds::ScriptedStore, count: usize) -> (Vec<String>, usize) {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let seen = appended(store);
                if seen.0.len() >= count {
                    return seen;
                }
                tokio::task::yield_now().await;
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("expected {count} group envelopes, saw {:?}", appended(store)))
    }

    async fn settle() {
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
    }

    fn detaches(lanes: &GroupEmissionLanes) -> u64 {
        lanes.shared.metrics.group_detaches.load(std::sync::atomic::Ordering::Relaxed)
    }

    #[tokio::test]
    async fn a_later_commit_waits_for_earlier_work_in_production_and_lands_in_commit_order() {
        let store = Arc::new(crate::ds::ScriptedStore::default());
        let lanes = GroupEmissionLanes::unmonitored(
            DsClient::with_test_store("scripted://group".into(), store.clone()),
            test_config(),
        );
        lanes.register(GROUP);
        // T (0x10) has a deferred flip in production for the group; T+1's plain output is ready.
        lanes.begin_origin(GROUP, 0x10);
        lanes.push(GROUP, vec![group_env("t+1")], 0x20);
        settle().await;
        assert!(appended(&store).0.is_empty(), "T+1 was appended ahead of T's outstanding flip");
        assert!(!lanes.drain(GROUP, 0x10).unwrap().drained);
        lanes.push(GROUP, vec![group_env("t-flip")], 0x10);
        lanes.complete_origin(GROUP, 0x10);
        let (keys, _) = wait_appended(&store, 2).await;
        assert_eq!(keys, ["t-flip", "t+1"]);
        assert!(lanes.drain(GROUP, 0x20).unwrap().drained);
    }

    #[tokio::test]
    async fn queued_commits_coalesce_so_a_slow_stream_keeps_up() {
        let (lanes, store, hold) = held_lanes(test_config());
        for i in 0..50u64 {
            lanes.push(GROUP, vec![group_env(&i.to_string())], i + 1);
        }
        settle().await;
        assert!(appended(&store).0.is_empty());
        hold.send_replace(false);
        let (keys, appends) = wait_appended(&store, 50).await;
        assert_eq!(keys, (0..50).map(|i| i.to_string()).collect::<Vec<_>>());
        assert!(appends <= 2, "50 queued commits took {appends} appends instead of coalescing");
    }

    #[tokio::test]
    async fn coalesced_appends_keep_each_commits_commit_time() {
        let (lanes, store, hold) = held_lanes(test_config());
        let at = |key: &str, commit_ts: i64| {
            let mut env = group_env(key);
            env.headers.commit_ts = Some(commit_ts);
            env
        };
        lanes.push(GROUP, vec![at("first", 1_793_404_800_001)], 1);
        settle().await;
        lanes.push(GROUP, vec![at("second", 1_793_404_800_002)], 2);
        lanes.push(GROUP, vec![at("third", 1_793_404_800_003)], 3);
        hold.send_replace(false);
        wait_appended(&store, 3).await;
        let stamps = store
            .appended
            .lock()
            .unwrap()
            .iter()
            .filter(|(path, _, _)| path.ends_with(GROUP))
            .flat_map(|(_, _, body)| serde_json::from_slice::<Vec<serde_json::Value>>(body).unwrap())
            .map(|event| event["headers"]["commit_ts"].as_i64())
            .collect::<Vec<_>>();
        assert_eq!(stamps, [Some(1_793_404_800_001), Some(1_793_404_800_002), Some(1_793_404_800_003)]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_but_answering_stream_never_detaches() {
        let store = Arc::new(crate::ds::ScriptedStore::default());
        *store.append_delay.lock().unwrap() = Some((GROUP.into(), std::time::Duration::from_millis(200)));
        let lanes = GroupEmissionLanes::unmonitored(
            DsClient::with_test_store("scripted://group".into(), store.clone()),
            test_config(),
        );
        lanes.register(GROUP);
        // 200 commits a second for 30 s against 200 ms appends, the prototype's `c-200`.
        for i in 0..6000u64 {
            lanes.push(GROUP, vec![group_env(&i.to_string())], i + 1);
            lanes.note_commit(i + 1);
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            if i % 200 == 199 {
                lanes.tick_for_test(true).await;
            }
        }
        let (keys, appends) = wait_appended(&store, 6000).await;
        assert_eq!(keys.len(), 6000);
        assert_eq!(detaches(&lanes), 0, "a slow stream that keeps answering detached");
        assert!(appends < 6000 / 10, "{appends} appends for 6000 commits");
    }

    #[tokio::test(start_paused = true)]
    async fn a_held_group_detaches_after_its_backoff_only_while_the_engine_moves() {
        let (lanes, store, _hold) = held_lanes(test_config());
        lanes.push(GROUP, vec![group_env("held")], 0x10);
        settle().await;
        // A whole-engine outage: nothing else moves either, so no stall time accrues.
        for _ in 0..30 {
            lanes.tick_for_test(false).await;
        }
        assert_eq!(detaches(&lanes), 0, "a group detached for an outage that stopped the engine");
        assert!(!lanes.drain(GROUP, 0x10).unwrap().must_refetch);
        // The engine moves on while this group lands nothing.
        for _ in 0..2 {
            lanes.tick_for_test(true).await;
        }
        assert_eq!(detaches(&lanes), 0, "detached before its backoff");
        lanes.tick_for_test(true).await;
        assert_eq!(detaches(&lanes), 1);
        let drain = lanes.drain(GROUP, 0x10).unwrap();
        assert!(drain.must_refetch && !drain.drained, "{drain:?}");
        assert_eq!(drain.resume_lsn.as_deref(), Some("0/10"));
        assert!(lanes.drain(GROUP, 0x0F).unwrap().drained, "a commit before the resume point is delivered");
        // A detached group drops its output: no producer waits and no memory grows.
        for i in 0..1000u64 {
            lanes.push(GROUP, vec![group_env("dropped")], 0x20 + i);
        }
        assert_eq!(lanes.shared.metrics.group_lane_backlog_batches.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert_eq!(lanes.behind_at_or_before(u64::MAX), BTreeMap::from([(GROUP.to_string(), 0x10)]));
        assert!(appended(&store).0.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_detached_group_reattaches_behind_a_must_refetch_marker() {
        let (lanes, store, hold) = held_lanes(test_config());
        lanes.push(GROUP, vec![group_env("lost")], 0x10);
        settle().await;
        for _ in 0..3 {
            lanes.tick_for_test(true).await;
        }
        assert_eq!(detaches(&lanes), 1);
        // The stream still holds requests: the probe times out and the group stays detached.
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        lanes.tick_for_test(true).await;
        assert!(lanes.drain(GROUP, 0x10).unwrap().must_refetch);
        assert_eq!(lanes.shared.metrics.group_reattaches.load(std::sync::atomic::Ordering::Relaxed), 0);
        // The stream answers again.
        hold.send_replace(false);
        tokio::time::advance(std::time::Duration::from_secs(5)).await;
        lanes.tick_for_test(true).await;
        assert_eq!(lanes.shared.metrics.group_reattaches.load(std::sync::atomic::Ordering::Relaxed), 1);
        lanes.push(GROUP, vec![group_env("after")], 0x30);
        let (keys, _) = wait_appended(&store, 2).await;
        assert_eq!(keys, [MUST_REFETCH, "after"], "the marker must precede every later batch");
        let marker = {
            let log = store.appended.lock().unwrap();
            serde_json::from_slice::<Vec<serde_json::Value>>(&log[0].2).unwrap()[0].clone()
        };
        assert_eq!(marker["type"], GROUP_CONTROL_TYPE);
        assert_eq!(marker["value"]["control"], MUST_REFETCH);
        assert_eq!(marker["value"]["resumeLsn"], "0/10");
        assert_eq!(marker["value"]["groupId"], GROUP);
        settle().await;
        let drain = lanes.drain(GROUP, 0x30).unwrap();
        assert!(drain.drained && !drain.must_refetch, "{drain:?}");
        assert!(lanes.behind_at_or_before(u64::MAX).is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn repeated_detaches_back_off_and_a_healthy_spell_restores_the_base() {
        let (lanes, _store, hold) = held_lanes(test_config());
        let stall = |lanes: GroupEmissionLanes, ticks: usize| async move {
            for _ in 0..ticks {
                lanes.tick_for_test(true).await;
            }
        };
        let reattach = |lanes: GroupEmissionLanes, hold: tokio::sync::watch::Sender<bool>| async move {
            hold.send_replace(false);
            tokio::time::advance(std::time::Duration::from_secs(7)).await;
            lanes.tick_for_test(true).await;
            settle().await;
            // Let one tick see the marker land, so the next stall starts from zero.
            lanes.tick_for_test(true).await;
            hold.send_replace(true);
        };
        lanes.push(GROUP, vec![group_env("a")], 1);
        settle().await;
        stall(lanes.clone(), 3).await;
        assert_eq!(detaches(&lanes), 1);
        reattach(lanes.clone(), hold.clone()).await;
        lanes.push(GROUP, vec![group_env("b")], 2);
        settle().await;
        stall(lanes.clone(), 5).await;
        assert_eq!(detaches(&lanes), 1, "the second detach must wait twice as long");
        stall(lanes.clone(), 1).await;
        assert_eq!(detaches(&lanes), 2);
        reattach(lanes.clone(), hold.clone()).await;
        // Healthy for the maximum backoff: the base comes back.
        hold.send_replace(false);
        settle().await;
        tokio::time::advance(std::time::Duration::from_secs(13)).await;
        lanes.tick_for_test(true).await;
        hold.send_replace(true);
        lanes.push(GROUP, vec![group_env("c")], 3);
        settle().await;
        stall(lanes.clone(), 3).await;
        assert_eq!(detaches(&lanes), 3);
    }

    fn big_env(key: &str) -> Envelope {
        let mut env = group_env(key);
        env.value = Some(serde_json::json!({ "id": key, "payload": "x".repeat(16 * 1024) }));
        env
    }

    #[tokio::test]
    async fn the_memory_cap_detaches_but_one_oversized_commit_never_does() {
        let config = GroupLaneConfig { memory_bytes: 4096, ..test_config() };
        let (lanes, _store, _hold) = held_lanes(config);
        lanes.push(GROUP, vec![big_env("in-flight")], 1);
        settle().await;
        // The largest queued commit is never counted, however many batches it arrives in.
        lanes.push(GROUP, vec![big_env("queued")], 2);
        lanes.push(GROUP, vec![big_env("queued-flip")], 2);
        assert_eq!(detaches(&lanes), 0, "one commit's output alone must never trip the memory cap");
        lanes.push(GROUP, vec![big_env("over")], 3);
        assert_eq!(detaches(&lanes), 1);
        // The in-flight append is whole commits still sending: it finishes, and the gap starts after it.
        assert_eq!(lanes.drain(GROUP, 3).unwrap().resume_lsn.as_deref(), Some("0/2"));
    }

    #[tokio::test]
    async fn an_oversized_commit_followed_at_once_by_a_small_one_lands_whole() {
        let store = Arc::new(crate::ds::ScriptedStore::default());
        let config = GroupLaneConfig { memory_bytes: 4096, ..test_config() };
        let lanes = GroupEmissionLanes::unmonitored(
            DsClient::with_test_store("scripted://group".into(), store.clone()),
            config,
        );
        lanes.register(GROUP);
        // Both are queued before the writer runs: the sequencer never waits between them.
        lanes.push(GROUP, vec![big_env("oversized")], 1);
        lanes.push(GROUP, vec![group_env("next")], 2);
        let (keys, _) = wait_appended(&store, 2).await;
        assert_eq!(keys, ["oversized", "next"]);
        assert_eq!(detaches(&lanes), 0);
    }

    #[tokio::test]
    async fn an_oversized_commit_queued_behind_smaller_ones_lands_whole() {
        let config = GroupLaneConfig { memory_bytes: 4096, ..test_config() };
        let (lanes, store, hold) = held_lanes(config);
        lanes.push(GROUP, vec![group_env("in-flight")], 1);
        settle().await;
        // T-1 is queued behind the held append when T's oversized output arrives.
        lanes.push(GROUP, vec![group_env("t-1")], 2);
        lanes.push(GROUP, vec![big_env("t-oversized")], 3);
        lanes.push(GROUP, vec![group_env("t+1")], 4);
        assert_eq!(detaches(&lanes), 0, "an oversized commit behind a smaller one tripped the cap");
        hold.send_replace(false);
        let (keys, _) = wait_appended(&store, 4).await;
        assert_eq!(keys, ["in-flight", "t-1", "t-oversized", "t+1"]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_byte_cap_detach_lets_an_oversized_append_in_flight_finish() {
        // 16 KiB at 1 KiB/s: the oversized append may take 19 s before it counts as hung.
        let config = GroupLaneConfig { memory_bytes: 4096, append_min_bytes_per_sec: 1024, ..test_config() };
        let (lanes, store, hold) = held_lanes(config);
        lanes.push(GROUP, vec![big_env("oversized")], 1);
        settle().await;
        // Ordinary writes keep coming behind the oversized append until they pass the cap.
        for i in 0..8u64 {
            lanes.push(GROUP, vec![big_env(&format!("small-{i}"))], 2 + i);
        }
        assert_eq!(detaches(&lanes), 1);
        assert_eq!(lanes.drain(GROUP, 2).unwrap().resume_lsn.as_deref(), Some("0/2"));
        // While it is still sending, the lane does not reattach, so nothing queues behind it
        // again and the one transaction costs one detach.
        for i in 0..5u64 {
            tokio::time::advance(std::time::Duration::from_secs(1)).await;
            lanes.tick_for_test(true).await;
            lanes.push(GROUP, vec![big_env(&format!("more-{i}"))], 20 + i);
        }
        assert_eq!(lanes.shared.metrics.group_reattaches.load(std::sync::atomic::Ordering::Relaxed), 0);
        hold.send_replace(false);
        let (keys, _) = wait_appended(&store, 1).await;
        assert_eq!(keys, ["oversized"], "the oversized append in flight must still land whole");
        assert!(lanes.drain(GROUP, 1).unwrap().drained, "the gap starts after the commit that landed");
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        lanes.tick_for_test(true).await;
        let (keys, _) = wait_appended(&store, 2).await;
        assert_eq!(keys, ["oversized", MUST_REFETCH], "the marker follows the landed append promptly");
        assert_eq!(detaches(&lanes), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_kept_append_that_never_lands_is_abandoned_at_its_allowance() {
        // The stream's HEAD answers but its appends never land: the kept append is given time
        // for its size (16 KiB at 1 KiB/s plus the 3 s cap) and then abandoned, so the marker
        // goes out instead of the lane cycling forever.
        let store = Arc::new(crate::ds::ScriptedStore::default());
        *store.append_delay.lock().unwrap() = Some((GROUP.into(), std::time::Duration::from_secs(24 * 3600)));
        let config = GroupLaneConfig { memory_bytes: 4096, append_min_bytes_per_sec: 1024, ..test_config() };
        let lanes = GroupEmissionLanes::unmonitored(
            DsClient::with_test_store("scripted://group".into(), store.clone()),
            config,
        );
        lanes.register(GROUP);
        lanes.push(GROUP, vec![big_env("stuck")], 1);
        settle().await;
        for i in 0..2u64 {
            lanes.push(GROUP, vec![big_env(&format!("behind-{i}"))], 2 + i);
        }
        assert_eq!(detaches(&lanes), 1);
        tokio::time::advance(std::time::Duration::from_secs(10)).await;
        lanes.tick_for_test(true).await;
        assert_eq!(lanes.shared.metrics.group_reattaches.load(std::sync::atomic::Ordering::Relaxed), 0);
        tokio::time::advance(std::time::Duration::from_secs(15)).await;
        lanes.tick_for_test(true).await;
        lanes.tick_for_test(true).await;
        assert_eq!(lanes.shared.metrics.group_reattaches.load(std::sync::atomic::Ordering::Relaxed), 1);
        let drain = lanes.drain(GROUP, 1).unwrap();
        assert!(drain.must_refetch && drain.resume_lsn.as_deref() == Some("0/1"), "{drain:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_receipt_inside_a_healed_gap_says_refetch_and_an_unsequenced_commit_is_not_drained() {
        let (lanes, store, hold) = held_lanes(test_config());
        lanes.push(GROUP, vec![group_env("lost")], 0x10);
        settle().await;
        for _ in 0..3 {
            lanes.tick_for_test(true).await;
        }
        hold.send_replace(false);
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        lanes.tick_for_test(true).await;
        wait_appended(&store, 1).await;
        settle().await;
        let inside = lanes.drain(GROUP, 0x10).unwrap();
        assert!(inside.drained && inside.must_refetch, "{inside:?}");
        assert_eq!(inside.resume_lsn.as_deref(), Some("0/10"));
        let after = lanes.drain(GROUP, 0x20).unwrap();
        assert!(after.drained && !after.must_refetch, "{after:?}");

        // After a restart, a durable receipt can name a commit this process has not sequenced yet.
        let restarted = GroupEmissionLanes::unmonitored(
            DsClient::with_test_store("scripted://group".into(), Arc::new(crate::ds::ScriptedStore::default())),
            test_config(),
        );
        restarted.shared.progress.lsn.store(0x08, std::sync::atomic::Ordering::Relaxed);
        restarted.register(GROUP);
        assert!(!restarted.drain(GROUP, 0x10).unwrap().drained, "an unsequenced commit was reported drained");
        restarted.note_commit(0x10);
        assert!(restarted.drain(GROUP, 0x10).unwrap().drained);
    }

    #[derive(Clone, Default)]
    struct CapturedLog(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for CapturedLog {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl CapturedLog {
        fn events(&self, name: &str) -> Vec<serde_json::Value> {
            String::from_utf8(self.0.lock().unwrap().clone())
                .unwrap()
                .lines()
                .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
                .map(|line| line["fields"].clone())
                .filter(|fields| fields["event"] == name)
                .collect()
        }
    }

    /// `#v-group-backlog-telemetry`: with small caps, a held group logs `group_backlog_threshold`
    /// at 50% and 90%, then `group_detached` and `group_backlog_high_water`, and `group_reattached`
    /// once its stream answers; a restart's detach names its subscription and counts as a detach.
    #[tokio::test(start_paused = true)]
    async fn group_events_carry_every_listed_field() {
        let log = CapturedLog::default();
        let writer = log.clone();
        let subscriber = tracing_subscriber::fmt().json().with_writer(move || writer.clone()).finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let config = GroupLaneConfig { memory_bytes: 8 * 1024, ..test_config() };
        let (lanes, store, hold) = held_lanes(config);
        lanes.set_meta(GROUP, GroupMeta { subscription: "scope-1".into(), tables: vec!["public.items".into()] });
        lanes.push(GROUP, vec![big_env("in-flight")], 1);
        settle().await;
        let small = |key: &str| {
            let mut env = group_env(key);
            env.value = Some(serde_json::json!({ "id": key, "payload": "x".repeat(900) }));
            env
        };
        for i in 0..20u64 {
            lanes.push(GROUP, vec![small(&format!("q{i}"))], 2 + i);
            if detaches(&lanes) == 1 {
                break;
            }
        }
        assert_eq!(detaches(&lanes), 1);
        let thresholds = log.events("group_backlog_threshold");
        let fractions: Vec<f64> = thresholds.iter().map(|event| event["fraction"].as_f64().unwrap()).collect();
        assert_eq!(fractions, [0.5, 0.9], "{thresholds:?}");
        let threshold_fields = [
            "group_id",
            "subscription",
            "member_tables",
            "cap",
            "fraction",
            "backlog_batches",
            "backlog_bytes",
            "memory_cap_bytes",
            "stall_cap_secs",
            "seconds_stalled",
            "lag_commits",
            "lag_lsn_bytes",
        ];
        for field in threshold_fields {
            assert!(thresholds.iter().all(|event| !event[field].is_null()), "threshold lacks {field}: {thresholds:?}");
        }
        let detach_fields = [
            "group_id",
            "subscription",
            "member_tables",
            "cap",
            "backlog_batches",
            "backlog_bytes",
            "in_flight_bytes",
            "memory_cap_bytes",
            "stall_cap_secs",
            "next_stall_cap_secs",
            "seconds_stalled",
            "lag_commits",
            "lag_lsn_bytes",
            "resume_lsn",
            "dropped_batches",
        ];
        let detached = log.events("group_detached");
        assert_eq!(detached.len(), 1);
        for field in detach_fields {
            assert!(!detached[0][field].is_null(), "group_detached lacks {field}: {detached:?}");
        }
        assert_eq!(detached[0]["subscription"], "scope-1");
        let high_water = log.events("group_backlog_high_water");
        for field in ["group_id", "member_tables", "peak_backlog_batches", "peak_backlog_bytes", "longest_stall_secs"] {
            assert!(high_water.iter().all(|event| !event[field].is_null()), "high water lacks {field}");
        }
        assert!(!high_water.is_empty());

        hold.send_replace(false);
        settle().await;
        tokio::time::advance(std::time::Duration::from_secs(20)).await;
        lanes.tick_for_test(true).await;
        lanes.tick_for_test(true).await;
        wait_appended(&store, 2).await;
        let reattached = log.events("group_reattached");
        assert_eq!(reattached.len(), 1, "{reattached:?}");
        for field in [
            "group_id",
            "subscription",
            "member_tables",
            "resume_lsn",
            "through_lsn",
            "detached_secs",
            "dropped_batches",
        ] {
            assert!(!reattached[0][field].is_null(), "group_reattached lacks {field}");
        }

        lanes.register_detached(
            "shape/group-8",
            0x40,
            GroupMeta { subscription: "scope-2".into(), tables: vec!["public.items".into()] },
        );
        let restart = log.events("group_detached").into_iter().find(|event| event["cap"] == "restart").unwrap();
        assert_eq!(restart["subscription"], "scope-2");
        for field in detach_fields {
            assert!(!restart[field].is_null(), "restart detach lacks {field}: {restart}");
        }
        assert_eq!(detaches(&lanes), 2, "a restart's detach counts as a detach");
    }

    #[tokio::test]
    async fn live_output_behind_an_activation_drain_does_not_trip_the_byte_cap() {
        let config = GroupLaneConfig { memory_bytes: 4096, ..test_config() };
        let (lanes, store, hold) = held_lanes(config);
        // A member's activation drain, several commits over the cap, then live output for the
        // group before the writer has worked any of it off.
        let drain = (1..=6u64).map(|lsn| (lsn, vec![big_env(&format!("drained-{lsn}"))])).collect();
        lanes.push_drain(GROUP, drain);
        lanes.push(GROUP, vec![group_env("live-7")], 7);
        lanes.push(GROUP, vec![group_env("live-8")], 8);
        assert_eq!(detaches(&lanes), 0, "live output behind an activation drain tripped the byte cap");
        hold.send_replace(false);
        let (keys, _) = wait_appended(&store, 8).await;
        let mut expected: Vec<String> = (1..=6).map(|lsn| format!("drained-{lsn}")).collect();
        expected.extend(["live-7".to_string(), "live-8".to_string()]);
        assert_eq!(keys, expected);
        // Once the drain has landed, ordinary output is capped again.
        hold.send_replace(true);
        lanes.push(GROUP, vec![big_env("in-flight")], 9);
        settle().await;
        lanes.push(GROUP, vec![big_env("queued")], 10);
        lanes.push(GROUP, vec![big_env("over")], 11);
        assert_eq!(detaches(&lanes), 1);
    }

    #[tokio::test]
    async fn waiting_on_its_own_deferred_flip_is_not_a_stall() {
        let store = Arc::new(crate::ds::ScriptedStore::default());
        let lanes = GroupEmissionLanes::unmonitored(
            DsClient::with_test_store("scripted://group".into(), store.clone()),
            test_config(),
        );
        lanes.register(GROUP);
        // T's query-back is slow; T+1's output waits behind it while the stream is healthy.
        lanes.begin_origin(GROUP, 5);
        lanes.push(GROUP, vec![group_env("t+1")], 6);
        for _ in 0..30 {
            lanes.tick_for_test(true).await;
        }
        assert_eq!(detaches(&lanes), 0, "a slow flip detached a group whose stream answers");
        lanes.complete_origin(GROUP, 5);
        wait_appended(&store, 1).await;
    }

    #[tokio::test]
    async fn an_append_still_sending_gets_time_for_its_size() {
        // 16 KiB at 1 KiB/s: the in-flight append gets 16 s beyond the 3 s stall cap.
        let config = GroupLaneConfig { append_min_bytes_per_sec: 1024, ..test_config() };
        let (lanes, _store, _hold) = held_lanes(config);
        lanes.push(GROUP, vec![big_env("large")], 1);
        settle().await;
        for _ in 0..10 {
            lanes.tick_for_test(true).await;
        }
        assert_eq!(detaches(&lanes), 0, "a large append was abandoned while it could still be sending");
        for _ in 0..20 {
            lanes.tick_for_test(true).await;
        }
        assert_eq!(detaches(&lanes), 1, "a hung append must still detach in the end");
    }

    #[tokio::test(start_paused = true)]
    async fn a_repeat_detach_waits_out_its_cap_before_probing() {
        let (lanes, store, hold) = held_lanes(test_config());
        lanes.push(GROUP, vec![group_env("a")], 1);
        settle().await;
        for _ in 0..3 {
            lanes.tick_for_test(true).await;
        }
        assert_eq!(detaches(&lanes), 1);
        hold.send_replace(false);
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        lanes.tick_for_test(true).await;
        wait_appended(&store, 1).await;
        lanes.tick_for_test(true).await;
        // The stream stops again: the second detach does not probe for the 3 s cap it hit.
        hold.send_replace(true);
        lanes.push(GROUP, vec![group_env("b")], 2);
        settle().await;
        for _ in 0..6 {
            lanes.tick_for_test(true).await;
        }
        assert_eq!(detaches(&lanes), 2);
        hold.send_replace(false);
        tokio::time::advance(std::time::Duration::from_secs(2)).await;
        lanes.tick_for_test(true).await;
        assert_eq!(lanes.shared.metrics.group_reattaches.load(std::sync::atomic::Ordering::Relaxed), 1);
        tokio::time::advance(std::time::Duration::from_secs(5)).await;
        lanes.tick_for_test(true).await;
        assert_eq!(lanes.shared.metrics.group_reattaches.load(std::sync::atomic::Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn a_checkpoint_and_a_group_receipt_name_work_still_in_production() {
        let store = Arc::new(crate::ds::ScriptedStore::default());
        let lanes =
            GroupEmissionLanes::unmonitored(DsClient::with_test_store("scripted://group".into(), store), test_config());
        lanes.register(GROUP);
        lanes.begin_origin(GROUP, 7);
        assert_eq!(lanes.behind_at_or_before(7), BTreeMap::from([(GROUP.to_string(), 7)]));
        assert!(lanes.behind_at_or_before(6).is_empty(), "a later origin does not hold an earlier checkpoint");
        assert!(!lanes.drain(GROUP, 7).unwrap().drained);
        assert!(lanes.drain(GROUP, 6).unwrap().drained);
        lanes.complete_origin(GROUP, 7);
        assert!(lanes.drain(GROUP, 7).unwrap().drained);
        assert!(lanes.behind_at_or_before(u64::MAX).is_empty());
        assert!(lanes.drain("shape/group-unknown", 7).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn a_restored_resume_point_reattaches_behind_a_marker() {
        let store = Arc::new(crate::ds::ScriptedStore::default());
        let lanes = GroupEmissionLanes::unmonitored(
            DsClient::with_test_store("scripted://group".into(), store.clone()),
            test_config(),
        );
        lanes.register_detached(GROUP, 0x2A, GroupMeta::default());
        assert_eq!(lanes.behind_at_or_before(0x30), BTreeMap::from([(GROUP.to_string(), 0x2A)]));
        assert!(lanes.drain(GROUP, 0x30).unwrap().must_refetch);
        lanes.push(GROUP, vec![group_env("replayed")], 0x31);
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        lanes.tick_for_test(true).await;
        lanes.push(GROUP, vec![group_env("live")], 0x32);
        let (keys, _) = wait_appended(&store, 2).await;
        assert_eq!(keys, [MUST_REFETCH, "live"]);
    }

    #[tokio::test]
    async fn removing_a_held_lane_abandons_its_append_and_queue() {
        let (lanes, store, hold) = held_lanes(test_config());
        lanes.push(GROUP, vec![group_env("in-flight")], 1);
        settle().await;
        lanes.push(GROUP, vec![group_env("queued")], 2);
        lanes.remove(GROUP);
        assert_eq!(lanes.shared.metrics.group_lane_backlog_batches.load(std::sync::atomic::Ordering::Relaxed), 0);
        hold.send_replace(false);
        settle().await;
        assert!(appended(&store).0.is_empty());
        assert!(lanes.drain(GROUP, 2).is_none());
        lanes.push(GROUP, vec![group_env("late")], 3);
        assert!(lanes.behind_at_or_before(u64::MAX).is_empty());
    }
}
