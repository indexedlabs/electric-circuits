//! Shape retention: the three-tier lifecycle (active / dormant / evicted) and its layered,
//! dormant eviction policy plus active retirement for non-parkable feeds.
//!
//! Replaces delete-on-refcount-0 (extended API) and the "handle TTL drops the shape" behavior of
//! the `/v1/shape` adapter as the primary lifecycle (a deliberate divergence from upstream
//! Electric, which keeps every retained shape actively maintained):
//!
//! - **Active** — maintained live by a tailer. Refcount-0 / client disconnect does NOT deactivate;
//!   brief reconnects stay warm and rejoin the same stream.
//! - **Dormant** — after sitting idle (no engine-visible reads and refcount 0) for
//!   [`RetentionConfig::idle_timeout`]: the tailer's routing state for the shape is dropped, while
//!   the durable stream and the shape record are retained at zero engine cost. Any touch
//!   reactivates by replaying the global change log from the captured resume **position** —
//!   `(segment, offset)`, following rotation pointers across segments (ADR-0006) — with no Postgres
//!   backfill (see `Engine::ensure_active`). While dormant a shape PINS its resume segment against
//!   deletion; one that would pin it past `ELECTRIC_CIRCUITS_CHANGES_RETAIN_SECS` is evicted
//!   ([`EvictReason::ChangeLogRetention`]) so the segment can go.
//! - **Evicted** — stream and record deleted; a returning `/v1/shape` client gets `409
//!   must-refetch` and re-snapshots, an extended-API client gets `404` and recreates.
//!
//! Eviction is **layered**: ordinary shapes are evicted from dormant, while **live-only** shapes
//! (the ones that cannot park: subqueries, aggregates, changes-only feeds) are retired straight
//! from active once nothing holds them:
//! 1. **Dormancy TTL** (hygiene): dormant longer than [`RetentionConfig::dormant_ttl`] → evict.
//!    Reaps dead shapes even with no resource pressure. A live-only shape with no subscription is
//!    retired after [`RetentionConfig::live_only_evict_after`] without a touch — the live-only
//!    idle period (`ELECTRIC_CIRCUITS_LIVE_ONLY_IDLE_SECS`, 24 hours by default), or the
//!    idle-plus-dormancy grace a parkable shape gets when that is shorter.
//! 2. **`max_shapes` count cap** (engine cost bound): when the total shape count exceeds
//!    [`RetentionConfig::max_shapes`], evict least-recently-read dormant shapes until under, then
//!    the longest-idle live-only shapes that have no subscription. Group-owned members are left
//!    to the group retention/re-join contract, and are never selected here. A subscribed shape
//!    is never evicted, and neither is a parkable shape that is still active.
//!    When nothing is left to evict, a create that would mint a NEW shape is refused
//!    (`503` + `Retry-After`); joining an existing shape is not.
//! 3. **Disk budget** (hard backstop): when the tracked shape-stream bytes exceed
//!    [`RetentionConfig::disk_budget_bytes`], evict least-recently-read dormant shapes until
//!    under. Byte accounting is engine-side ([`crate::ds::DsClient`] counts what it appends) —
//!    the durable-streams server exposes no per-stream sizes yet — so it undercounts streams
//!    written before the current process started.
//!
//! If a cap/budget is exceeded but nothing eligible is left to evict, the sweep logs loudly and
//! bumps a metric instead of evicting subscribed shapes.
//!
//! Live-only shapes are exempt from dormancy (a subquery's inner sets and an aggregate's running
//! fold cannot be rebuilt from a change-log replay alone, and a changes-only feed would lose the
//! changes of its dormant period), so they stay active while retained — evaluated on every change
//! to their tables, subscribed or not. So that an abandoned one stops costing that work, the
//! engine retires it **straight from active** (full teardown) once it has gone the live-only idle
//! period with no subscription and no touch. A **touch** is a lease create, join or renewal, or an
//! engine-visible read; a lease lapse is not one, so the period runs from the last renewal. The
//! period is also how long a client that went away can still catch up from the feed's stream, so
//! it is never shorter than the lease. Like any evicted shape, a returning client recreates it.
//!
//! This module holds the configuration, the lifecycle state machine types, and the **pure** sweep
//! planner ([`plan_sweep`]); `crate::engine` owns the state and executes plans. Persistence of the
//! lifecycle (catalog, `last_read` flushes, restart recovery) is the follow-up catalog work (GH
//! issue #8). Dormancy IS durable: the engine's `meta/catalog` records `Dormant`/`Reactivated`
//! events (with the resume position + snapshot gate), so a restart restores dormant shapes as
//! dormant — no re-registration, no backfill. Only the in-memory clocks reset (dormancy age
//! restarts at boot, so the TTL is conservative across restarts).

use std::time::{Duration, Instant};

use crate::changelog::LogPosition;
use crate::heap_size::HeapSize;
use crate::pg::SnapshotGate;

/// Retention tuning, read from the environment once at engine construction.
///
/// | Env var | Default | Meaning |
/// |---|---|---|
/// | `ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS` | `21600` (6 hours) | Idle time (no reads, refcount 0) before an active shape goes dormant. `0` disables dormancy. |
/// | `ELECTRIC_CIRCUITS_SUBSCRIPTION_LEASE_SECS` | `1800` (30 min) | Time since the last renewal before a native subscription lease lapses. `0` disables lease expiry. |
/// | `ELECTRIC_CIRCUITS_SHAPE_DORMANT_TTL_SECS` | `604800` (7 days) | Time a shape may stay dormant before it is evicted. `0` disables the TTL layer. |
/// | `ELECTRIC_CIRCUITS_LIVE_ONLY_IDLE_SECS` | `86400` (24 hours) | Time a live-only shape (subquery, aggregate, changes-only feed) may go without a subscription or a touch before it is retired. Never shorter than the lease. `0` leaves only the idle-plus-dormancy grace. |
/// | `ELECTRIC_CIRCUITS_REPLAY_MIN_BYTES` | `16777216` (16 MiB) | Minimum change-log span budget for dormant replay. |
/// | `ELECTRIC_CIRCUITS_REPLAY_MULTIPLIER` | `4` | Replay budget multiplier applied to the shape's recorded backfill bytes. |
/// | `ELECTRIC_CIRCUITS_MAX_SHAPES` | `10000` | Cap on registered shapes (active, dormant and live-only; each group member is one). Over it, least-recently-read dormant shapes are evicted, then the longest-idle unsubscribed live-only shapes; with nothing left to evict, new shapes are refused. `0` = unlimited. |
/// | `ELECTRIC_CIRCUITS_SHAPE_DISK_BUDGET_MB` | `0` (disabled) | Cap on tracked shape-stream bytes; over it, least-recently-read dormant shapes are evicted. |
/// | `ELECTRIC_CIRCUITS_RETENTION_SWEEP_SECS` | `60` | Sweep interval of the background retention task. |
/// | `ELECTRIC_CIRCUITS_REACTIVATION_JOIN_TIMEOUT_SECS` | `20` | How long a create/join/read may wait on a dormant shape's reactivation before it gives up with a typed recreate outcome. The reactivation itself continues. `0` = wait forever. |
#[derive(Clone, Debug)]
pub struct RetentionConfig {
    /// Active → dormant idle threshold (`ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS`, default 6 hours; 0 = never).
    pub idle_timeout: Duration,
    /// Native subscription lease window (`ELECTRIC_CIRCUITS_SUBSCRIPTION_LEASE_SECS`, default 30 min; 0 = never).
    pub subscription_lease_timeout: Duration,
    /// Dormant → evicted hygiene TTL (`ELECTRIC_CIRCUITS_SHAPE_DORMANT_TTL_SECS`, default 7 days; 0 = never).
    pub dormant_ttl: Duration,
    /// The live-only idle period as configured (`ELECTRIC_CIRCUITS_LIVE_ONLY_IDLE_SECS`, default
    /// 24 hours; 0 = no period of its own). Read it through [`Self::live_only_evict_after`], which
    /// applies the lease floor and the idle-plus-dormancy grace.
    pub live_only_idle: Duration,
    /// Minimum dormant replay span in bytes (`ELECTRIC_CIRCUITS_REPLAY_MIN_BYTES`).
    pub replay_min_bytes: u64,
    /// Backfill-size multiplier for replay admission (`ELECTRIC_CIRCUITS_REPLAY_MULTIPLIER`).
    pub replay_multiplier: u64,
    /// Cap on registered shapes of every kind and state (`ELECTRIC_CIRCUITS_MAX_SHAPES`, default
    /// 10000; 0 = unlimited).
    pub max_shapes: usize,
    /// Shape-stream disk budget in bytes (`ELECTRIC_CIRCUITS_SHAPE_DISK_BUDGET_MB`, default 0 = disabled).
    pub disk_budget_bytes: u64,
    /// Background sweep interval (`ELECTRIC_CIRCUITS_RETENTION_SWEEP_SECS`, default 60s).
    pub sweep_interval: Duration,
    /// How long a touch may wait on an in-flight reactivation before giving up with a typed
    /// recreate outcome (`ELECTRIC_CIRCUITS_REACTIVATION_JOIN_TIMEOUT_SECS`, default 20s; 0 = wait
    /// forever). The default sits under the API gateway's 30s read timeout: a large-span replay
    /// takes longer than the gateway will wait, and a caller that is going to be cut off anyway is
    /// better served by an answer it can act on than by the gateway's 503.
    pub reactivation_join_timeout: Duration,
    /// Per-shape ceiling for the buffer of live deltas a shape accumulates between `BeginShape` and
    /// `ActivateShape` (`ELECTRIC_CIRCUITS_PENDING_BUFFER_MAX_BYTES`, default 64 MiB; 0 = no cap).
    /// That window is a Postgres backfill for a create and a change-log replay for a wake — plus,
    /// since replays are permit-gated, the wait for a permit, which makes the window "replay
    /// duration x queue depth". Past the cap the buffer is dropped and the shape answers recreate.
    pub pending_buffer_max_bytes: u64,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        RetentionConfig {
            idle_timeout: Duration::from_secs(6 * 3600),
            subscription_lease_timeout: Duration::from_secs(1800),
            dormant_ttl: Duration::from_secs(7 * 24 * 3600),
            live_only_idle: Duration::from_secs(24 * 3600),
            replay_min_bytes: 16 * 1024 * 1024,
            replay_multiplier: 4,
            max_shapes: 10_000,
            disk_budget_bytes: 0,
            sweep_interval: Duration::from_secs(60),
            reactivation_join_timeout: Duration::from_secs(20),
            pending_buffer_max_bytes: 64 * 1024 * 1024,
        }
    }
}

impl RetentionConfig {
    pub fn replay_budget(&self, backfill_bytes: Option<u64>) -> u64 {
        self.replay_min_bytes.max(backfill_bytes.unwrap_or(0).saturating_mul(self.replay_multiplier))
    }

    /// How long a live-only shape may go with no subscription and no touch before it is retired
    /// from active, and the reason that retirement is counted under. `None` = never.
    ///
    /// It is the shorter of two windows. The **live-only idle period**
    /// (`ELECTRIC_CIRCUITS_LIVE_ONLY_IDLE_SECS`) is raised to the lease when set below it: a lease
    /// lapses a whole lease after the last renewal, so a shorter period would retire a feed in the
    /// sweep that lapsed it and leave a client that missed one renewal nothing to catch up from.
    /// The **idle-plus-dormancy grace** is the rule from before the period existed, kept so the
    /// period only ever shortens how long an abandoned shape lives: a deployment that evicts a
    /// parkable shape within the hour does not keep a live-only one for a day. The sweep plan and
    /// the eviction's recheck under the locks both read the threshold from here.
    pub fn live_only_evict_after(&self) -> Option<(Duration, EvictReason)> {
        let period = (!self.live_only_idle.is_zero())
            .then(|| (self.live_only_idle.max(self.subscription_lease_timeout), EvictReason::LiveOnlyIdle));
        let grace = (!self.dormant_ttl.is_zero())
            .then(|| (self.idle_timeout.saturating_add(self.dormant_ttl), EvictReason::DormantTtl));
        match (period, grace) {
            (Some(period), Some(grace)) => Some(if grace.0 < period.0 { grace } else { period }),
            (period, grace) => period.or(grace),
        }
    }

    pub fn from_env() -> Self {
        Self::resolve(|name| std::env::var(name).ok())
    }

    /// Resolve from an environment getter, so the boot configuration can check the retention
    /// windows against `ELECTRIC_CIRCUITS_STREAM_TTL_SECS` through the same values.
    pub fn resolve(get: impl Fn(&str) -> Option<String>) -> Self {
        let env_u64 =
            |name: &str, default: u64| get(name).and_then(|s| s.trim().parse::<u64>().ok()).unwrap_or(default);
        let d = RetentionConfig::default();
        // Backward compatibility: deployments and the conformance harness historically set the
        // idle knob to accelerate both dormancy and lease expiry. An explicit lease knob wins;
        // otherwise preserve that legacy coupling only when the old env var is actually supplied.
        let legacy_idle = get("ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS").and_then(|v| v.trim().parse::<u64>().ok());
        let lease_secs = get("ELECTRIC_CIRCUITS_SUBSCRIPTION_LEASE_SECS")
            .and_then(|v| v.trim().parse::<u64>().ok())
            .or(legacy_idle)
            .unwrap_or(d.subscription_lease_timeout.as_secs());
        RetentionConfig {
            idle_timeout: Duration::from_secs(legacy_idle.unwrap_or(d.idle_timeout.as_secs())),
            subscription_lease_timeout: Duration::from_secs(lease_secs),
            dormant_ttl: Duration::from_secs(env_u64(
                "ELECTRIC_CIRCUITS_SHAPE_DORMANT_TTL_SECS",
                d.dormant_ttl.as_secs(),
            )),
            live_only_idle: Duration::from_secs(env_u64(
                "ELECTRIC_CIRCUITS_LIVE_ONLY_IDLE_SECS",
                d.live_only_idle.as_secs(),
            )),
            replay_min_bytes: env_u64("ELECTRIC_CIRCUITS_REPLAY_MIN_BYTES", d.replay_min_bytes),
            replay_multiplier: env_u64("ELECTRIC_CIRCUITS_REPLAY_MULTIPLIER", d.replay_multiplier),
            max_shapes: env_u64("ELECTRIC_CIRCUITS_MAX_SHAPES", d.max_shapes as u64) as usize,
            disk_budget_bytes: env_u64("ELECTRIC_CIRCUITS_SHAPE_DISK_BUDGET_MB", 0).saturating_mul(1024 * 1024),
            sweep_interval: Duration::from_secs(env_u64("ELECTRIC_CIRCUITS_RETENTION_SWEEP_SECS", 60).max(1)),
            reactivation_join_timeout: Duration::from_secs(env_u64(
                "ELECTRIC_CIRCUITS_REACTIVATION_JOIN_TIMEOUT_SECS",
                d.reactivation_join_timeout.as_secs(),
            )),
            pending_buffer_max_bytes: env_u64("ELECTRIC_CIRCUITS_PENDING_BUFFER_MAX_BYTES", d.pending_buffer_max_bytes),
        }
    }
}

/// The shortest non-zero `ELECTRIC_CIRCUITS_STREAM_TTL_SECS` the engine starts with: twice the
/// larger of a shape's idle-plus-dormancy window and the change log's retain window, 14.5 days at
/// the defaults. Those windows bound how long the engine's own lifecycle holds a stream before it
/// deletes it, so the TTL stays well clear of them. `None` when idle or dormancy is `0`
/// (disabled): a shape may then stay forever, and no TTL is safe. A retain window of `0` adds
/// nothing to the floor, since dormancy eviction still unpins the segment a dormant shape holds.
///
/// The floor alone does not keep a tracked stream alive: a stream the engine neither appends to nor
/// reads, such as a restored dormant shape whose dormancy clock restarted at boot, would still
/// expire. The keep-alive sweep (`Engine::sweep_stream_ttl`, on its own timer at the retention
/// sweep interval) reads each of them at its tail at least once per quarter TTL; `HEAD` does not
/// slide the TTL.
pub fn stream_ttl_floor(retention: &RetentionConfig, changes: &crate::changelog::ChangeLogConfig) -> Option<Duration> {
    if retention.idle_timeout.is_zero() || retention.dormant_ttl.is_zero() {
        return None;
    }
    Some(retention.idle_timeout.saturating_add(retention.dormant_ttl).max(changes.retain).saturating_mul(2))
}

/// Where a shape is in the lifecycle. Held per shape id by the engine (`Engine::lives`).
pub enum LifeState {
    /// Maintained live by its tailer.
    Active,
    /// The active → dormant transition is in flight (the tailer is unregistering the shape and
    /// capturing the resume state). A touch waits for it to finish, then reactivates.
    Deactivating { done: tokio::sync::watch::Receiver<bool> },
    /// Engine state dropped; stream + record retained. `resume` is the change-log position
    /// (segment + offset — the log is segmented, ADR-0006) up to which the shape's stream is
    /// complete; `gate` is the shape's original backfill-snapshot fence (still needed if the shape
    /// went dormant with pre-backfill changes in flight). While dormant the shape **pins** its
    /// resume segment: the segment is not deleted until the shape is reactivated or evicted.
    Dormant { since: Instant, resume: LogPosition, gate: SnapshotGate },
    /// A touch is replaying the change log to bring the shape back. Concurrent touches await
    /// the same outcome (`Some(true)` = active again, `Some(false)` = reactivation failed).
    /// `resume` is the position the replay is running FROM: it keeps pinning its change-log segment
    /// for the whole replay, so the sweeper cannot delete the segment out from under a
    /// reactivation that outlives one sweep tick (ADR-0006).
    Reactivating { done: tokio::sync::watch::Receiver<Option<bool>>, resume: LogPosition },
}

impl HeapSize for LifeState {
    /// Only `Dormant`'s `resume` position (its offset `String`) and `gate` own heap; the
    /// `watch::Receiver` variants are channel handles (shared with the sender side), not uniquely
    /// owned data.
    fn heap_bytes(&self) -> usize {
        match self {
            LifeState::Dormant { since: _, resume, gate } => resume.heap_bytes() + gate.heap_bytes(),
            LifeState::Reactivating { done: _, resume } => resume.heap_bytes(),
            LifeState::Active | LifeState::Deactivating { .. } => 0,
        }
    }
}

/// Per-shape lifecycle record.
pub struct ShapeLife {
    /// Last engine-visible read/touch (shape create/join, `/v1/shape` request, stream read,
    /// rows/log fold). Drives both the idle timer and the LRU eviction order. Direct
    /// durable-streams reads bypass the engine and are NOT observed — but such readers hold a
    /// subscription (refcount ≥ 1), which also blocks dormancy.
    pub last_read: Instant,
    pub state: LifeState,
}

impl HeapSize for ShapeLife {
    /// `last_read` (`Instant`) is inline (no heap).
    fn heap_bytes(&self) -> usize {
        self.state.heap_bytes()
    }
}

impl ShapeLife {
    pub fn active() -> Self {
        ShapeLife { last_read: Instant::now(), state: LifeState::Active }
    }
}

/// One shape's sweep-relevant status, snapshotted by the engine for [`plan_sweep`].
pub struct SweepShape {
    pub id: String,
    /// Live subscriptions (shared-feed refcount; 0 for unshared shapes).
    pub refcount: usize,
    /// Time since the last engine-visible read.
    pub idle: Duration,
    /// `Some(time dormant)` iff the shape is dormant; `None` while active/transitioning.
    pub dormant_for: Option<Duration>,
    /// True while a deactivation or reactivation is in flight — the sweep leaves it alone.
    pub in_transition: bool,
    /// Eligible for dormancy at all (plain row shapes). A shape that is not is **live-only**: a
    /// subquery, an aggregate or a changes-only feed.
    pub dormancy_eligible: bool,
    /// The shape has a subscription set, so `refcount == 0` means every lease on it was released
    /// or lapsed. Every shape made through the HTTP surfaces has one. An unshared library shape
    /// does not: its refcount is always 0 and says nothing about whether it is in use, so the cap
    /// does not evict it from active.
    pub lease_tracked: bool,
    /// The id of the group this shape is a member of, if any. A group shares one stream and one
    /// lease, so this issue's evictor leaves every member to the group retention/re-join path.
    pub group: Option<String>,
    /// Tracked bytes appended to the shape's stream (engine-side accounting).
    pub stream_bytes: u64,
    /// Bytes from the dormant cursor to the current tail. `None` means the cursor's segment is gone.
    pub replay_span_bytes: Option<u64>,
    /// A transient HEAD failure prevented measuring the span this sweep; defer eviction.
    pub replay_span_deferred: bool,
    /// Last plain-shape backfill estimate. `None` is an old/unknown catalog record.
    pub backfill_bytes: Option<u64>,
}

/// Did an eviction actually happen?
///
/// The change log's sweeper needs the difference (ADR-0006). It evicts a dormant shape in order to
/// **unpin** the segment the shape resumes from, and then deletes that segment; a "nothing to do"
/// answer — the shape was touched between planning and eviction and is now reactivating, or it was
/// re-subscribed — must not be read as "the pin is released", or the segment would be deleted out
/// from under a live replay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Evicted {
    /// The shape is gone (or was already gone): nothing it held is held any more.
    Yes,
    /// Nothing was done — the shape is mid-transition, or has been touched/re-subscribed since the
    /// sweep's snapshot. Everything it pinned, it still pins.
    Skipped,
}

/// Why a shape is being evicted (for logs/metrics).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvictReason {
    DormantTtl,
    /// A live-only shape went the live-only idle period with no subscription and no touch.
    LiveOnlyIdle,
    MaxShapes,
    DiskBudget,
    /// The shape's dormant resume position sits in a change-log segment that was rotated out more
    /// than `ELECTRIC_CIRCUITS_CHANGES_RETAIN_SECS` ago (ADR-0006). Evicting it is what unpins the
    /// segment so it can be deleted — the change log must not be held hostage by a shape nobody has
    /// touched in a week.
    ChangeLogRetention,
    ReplayBudget,
}

impl EvictReason {
    /// Every reason, in the order the per-reason eviction counters are kept.
    pub const ALL: [EvictReason; 6] = [
        EvictReason::DormantTtl,
        EvictReason::LiveOnlyIdle,
        EvictReason::MaxShapes,
        EvictReason::DiskBudget,
        EvictReason::ChangeLogRetention,
        EvictReason::ReplayBudget,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            EvictReason::DormantTtl => "dormant-ttl",
            EvictReason::LiveOnlyIdle => "live-only-idle",
            EvictReason::MaxShapes => "max-shapes",
            EvictReason::DiskBudget => "disk-budget",
            EvictReason::ChangeLogRetention => "change-log-retention",
            EvictReason::ReplayBudget => "replay-budget",
        }
    }

    /// This reason's place in [`Self::ALL`].
    pub fn index(self) -> usize {
        self as usize
    }
}

/// What a shape costs the engine while it is live, for the live-shape gauges. A shape has one
/// kind: an aggregate, else a subquery shape (changes-only or not, since its membership work is
/// the same), else a changes-only feed, else a plain row shape. Only `Plain` can park.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShapeKind {
    Plain,
    ChangesOnly,
    Subquery,
    Aggregate,
}

impl ShapeKind {
    /// Every kind, in the order the live-shape gauges are kept.
    pub const ALL: [ShapeKind; 4] =
        [ShapeKind::Plain, ShapeKind::ChangesOnly, ShapeKind::Subquery, ShapeKind::Aggregate];

    pub fn of(is_subquery: bool, is_aggregate: bool, changes_only: bool) -> Self {
        if is_aggregate {
            ShapeKind::Aggregate
        } else if is_subquery {
            ShapeKind::Subquery
        } else if changes_only {
            ShapeKind::ChangesOnly
        } else {
            ShapeKind::Plain
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ShapeKind::Plain => "plain",
            ShapeKind::ChangesOnly => "changes-only",
            ShapeKind::Subquery => "subquery",
            ShapeKind::Aggregate => "aggregate",
        }
    }

    /// This kind's place in [`Self::ALL`].
    pub fn index(self) -> usize {
        self as usize
    }
}

/// What one retention sweep should do. Deactivations and evictions are executed by the engine;
/// the `over_*` flags mean a cap/budget is exceeded with nothing left that it may evict — surfaced
/// loudly (log + metric), never resolved by evicting a subscribed shape.
#[derive(Default)]
pub struct SweepPlan {
    pub deactivate: Vec<String>,
    pub evict: Vec<(String, EvictReason)>,
    pub over_capacity: bool,
    pub over_budget: bool,
}

/// Live-only shapes that nothing holds and are safe for this issue to retire. Group-owned shapes
/// are intentionally absent: their shared-stream retention and re-join contract belongs to
/// OTTO-13834.
struct Unheld<'a> {
    idle: Duration,
    members: Vec<&'a SweepShape>,
    lease_tracked: bool,
}

/// The unheld live-only units of a snapshot, longest idle first.
///
/// A group-owned shape is never selected here, even when all its members are unheld. The engine
/// snapshot marks records and in-flight reservations so this exclusion is reachable in production.
fn unheld_live_only(shapes: &[SweepShape]) -> Vec<Unheld<'_>> {
    let free = |s: &SweepShape| {
        s.group.is_none() && !s.dormancy_eligible && !s.in_transition && s.dormant_for.is_none() && s.refcount == 0
    };
    let mut units = Vec::new();
    for s in shapes {
        if free(s) {
            units.push(Unheld { idle: s.idle, members: vec![s], lease_tracked: s.lease_tracked });
        }
    }
    // Longest idle first; the id breaks a tie so the same snapshot always gives the same plan.
    units.sort_by(|a, b| b.idle.cmp(&a.idle).then_with(|| a.members[0].id.cmp(&b.members[0].id)));
    units
}

/// Evict until `count` is at most `target`: least-recently-read dormant shapes first, then the
/// longest-idle unheld live-only units. Returns whether the target was met. A subscribed shape is
/// never chosen, nor a parkable shape that is still active (it parks first, on the idle timer).
fn evict_down_to<'a, 'u>(
    target: usize,
    count: &mut usize,
    dormant_lru: &mut impl Iterator<Item = &'a SweepShape>,
    unheld: &mut impl Iterator<Item = &'u Unheld<'a>>,
    evicted: &mut std::collections::HashSet<&'a str>,
    out: &mut Vec<(String, EvictReason)>,
) -> bool
where
    'a: 'u,
{
    while *count > target {
        let unit: Vec<&'a SweepShape> = match dormant_lru.next() {
            Some(s) => vec![s],
            None => match unheld
                .find(|unit| unit.lease_tracked && !unit.members.iter().any(|s| evicted.contains(s.id.as_str())))
            {
                Some(unit) => unit.members.clone(),
                None => return false,
            },
        };
        for s in unit {
            out.push((s.id.clone(), EvictReason::MaxShapes));
            evicted.insert(&s.id);
            *count = count.saturating_sub(1);
        }
    }
    true
}

/// Decide one sweep's actions from a status snapshot. Pure — all engine state is passed in — so
/// the layered policy is unit-testable without tailers or storage.
pub fn plan_sweep(cfg: &RetentionConfig, shapes: &[SweepShape]) -> SweepPlan {
    let mut plan = SweepPlan::default();

    // Tier 0 — dormancy: idle, unsubscribed, eligible, settled shapes go dormant.
    if !cfg.idle_timeout.is_zero() {
        for s in shapes {
            if s.dormancy_eligible
                && !s.in_transition
                && s.dormant_for.is_none()
                && s.group.is_none()
                && s.refcount == 0
                && s.idle >= cfg.idle_timeout
            {
                plan.deactivate.push(s.id.clone());
            }
        }
    }

    let mut evicted: std::collections::HashSet<&str> = std::collections::HashSet::new();

    // A parked cursor that points into an expired segment, or whose replay span has grown beyond
    // the shape's bounded replay budget, is no longer safely reactivatable. Retire it now so the
    // next subscriber recreates from a fresh snapshot.
    for s in shapes {
        if s.group.is_none() && s.dormant_for.is_some() {
            if s.replay_span_deferred {
                continue;
            } else if s.replay_span_bytes.is_none() {
                plan.evict.push((s.id.clone(), EvictReason::ChangeLogRetention));
                evicted.insert(&s.id);
            } else if s.replay_span_bytes.unwrap_or(0) > cfg.replay_budget(s.backfill_bytes) {
                plan.evict.push((s.id.clone(), EvictReason::ReplayBudget));
                evicted.insert(&s.id);
            }
        }
    }

    // Tier 1 — dormancy TTL (hygiene, independent of pressure).
    if !cfg.dormant_ttl.is_zero() {
        for s in shapes {
            if s.group.is_none() && s.dormant_for.is_some_and(|d| d >= cfg.dormant_ttl) {
                plan.evict.push((s.id.clone(), EvictReason::DormantTtl));
                evicted.insert(&s.id);
            }
        }
    }
    // Shapes that cannot park (subquery / aggregate state is not rebuildable from a bounded replay;
    // changes_only feeds would lose their dormant-period history) are evaluated on every change for
    // as long as they are registered. One that nothing holds is evicted straight from active once
    // it has gone [`RetentionConfig::live_only_evict_after`] without a touch. They are recreatable:
    // a returning client gets 404 / must-refetch and recreates; changes_only callers receive a
    // fresh feed rather than a silently incomplete dormant replay.
    let unheld = unheld_live_only(shapes);
    if let Some((after, reason)) = cfg.live_only_evict_after() {
        for unit in unheld.iter().filter(|unit| unit.idle >= after) {
            for s in &unit.members {
                plan.evict.push((s.id.clone(), reason));
                evicted.insert(&s.id);
            }
        }
    }

    // Dormant shapes still standing after tier 1, least-recently-read first (largest idle first).
    let mut lru: Vec<&SweepShape> = shapes
        .iter()
        .filter(|s| s.group.is_none() && s.dormant_for.is_some() && !evicted.contains(s.id.as_str()))
        .collect();
    lru.sort_by_key(|s| std::cmp::Reverse(s.idle));
    let mut lru = lru.into_iter();

    // Tier 2 — max_shapes count cap: dormant shapes, then live-only shapes nothing holds.
    if cfg.max_shapes > 0 {
        let mut count = shapes.len() - evicted.len();
        let met =
            evict_down_to(cfg.max_shapes, &mut count, &mut lru, &mut unheld.iter(), &mut evicted, &mut plan.evict);
        plan.over_capacity = !met;
    }

    // Tier 3 — disk budget over the tracked shape-stream bytes.
    if cfg.disk_budget_bytes > 0 {
        let mut total: u64 = shapes.iter().filter(|s| !evicted.contains(s.id.as_str())).map(|s| s.stream_bytes).sum();
        while total > cfg.disk_budget_bytes {
            match lru.next() {
                Some(s) => {
                    plan.evict.push((s.id.clone(), EvictReason::DiskBudget));
                    evicted.insert(&s.id);
                    total = total.saturating_sub(s.stream_bytes);
                }
                None => {
                    plan.over_budget = true;
                    break;
                }
            }
        }
    }

    plan
}

/// What a create that would mint new shapes finds at the cap ([`plan_cap_room`]).
#[derive(Debug, PartialEq, Eq)]
pub enum CapRoom {
    /// The new shapes fit under the cap as things stand.
    Fits,
    /// They fit once these shapes are evicted (ids, in eviction order).
    Evict(Vec<String>),
    /// They do not fit and nothing more may be evicted: the create is refused.
    Full,
}

/// What a create that is about to mint `wanted` shapes needs under `ELECTRIC_CIRCUITS_MAX_SHAPES`.
///
/// The cap counts every shape in the snapshot: active, dormant and live-only, each group member
/// once, creates in flight included. The sweep only acts when the count is **over** the cap, and a
/// create at the cap would never get there, so room is made on demand from exactly what the sweep
/// may evict, in the same order. When that is not enough the answer is [`CapRoom::Full`] and
/// nothing is evicted: emptying shapes for a create that is refused anyway helps nobody.
pub fn plan_cap_room(cfg: &RetentionConfig, shapes: &[SweepShape], wanted: usize) -> CapRoom {
    if cfg.max_shapes == 0 || shapes.len().saturating_add(wanted) <= cfg.max_shapes {
        return CapRoom::Fits;
    }
    let Some(target) = cfg.max_shapes.checked_sub(wanted) else { return CapRoom::Full };
    let mut dormant: Vec<&SweepShape> =
        shapes.iter().filter(|s| s.group.is_none() && s.dormant_for.is_some()).collect();
    dormant.sort_by_key(|s| std::cmp::Reverse(s.idle));
    let unheld = unheld_live_only(shapes);
    let mut count = shapes.len();
    let mut evict = Vec::new();
    let met = evict_down_to(
        target,
        &mut count,
        &mut dormant.into_iter(),
        &mut unheld.iter(),
        &mut std::collections::HashSet::new(),
        &mut evict,
    );
    if met { CapRoom::Evict(evict.into_iter().map(|(id, _)| id).collect()) } else { CapRoom::Full }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> RetentionConfig {
        RetentionConfig {
            idle_timeout: Duration::from_secs(1800),
            subscription_lease_timeout: Duration::from_secs(1800),
            dormant_ttl: Duration::from_secs(7 * 24 * 3600),
            live_only_idle: Duration::ZERO,
            replay_min_bytes: 16 * 1024 * 1024,
            replay_multiplier: 4,
            max_shapes: 0,
            disk_budget_bytes: 0,
            sweep_interval: Duration::from_secs(60),
            reactivation_join_timeout: Duration::from_secs(20),
            pending_buffer_max_bytes: 64 * 1024 * 1024,
        }
    }

    fn shape(id: &str) -> SweepShape {
        SweepShape {
            id: id.into(),
            refcount: 0,
            idle: Duration::ZERO,
            dormant_for: None,
            in_transition: false,
            dormancy_eligible: true,
            lease_tracked: true,
            group: None,
            stream_bytes: 0,
            replay_span_bytes: None,
            replay_span_deferred: false,
            backfill_bytes: None,
        }
    }

    fn dormant(id: &str, idle_secs: u64, dormant_secs: u64) -> SweepShape {
        SweepShape {
            idle: Duration::from_secs(idle_secs),
            dormant_for: Some(Duration::from_secs(dormant_secs)),
            replay_span_bytes: Some(0),
            ..shape(id)
        }
    }

    #[test]
    fn idle_unsubscribed_shapes_go_dormant() {
        let shapes = vec![
            SweepShape { idle: Duration::from_secs(3600), ..shape("s1") }, // idle past the timeout
            SweepShape { idle: Duration::from_secs(60), ..shape("s2") },   // recently read
            SweepShape { idle: Duration::from_secs(3600), refcount: 2, ..shape("s3") }, // subscribed
            SweepShape { idle: Duration::from_secs(3600), dormancy_eligible: false, ..shape("s4") }, // aggregate/subquery
            SweepShape { idle: Duration::from_secs(3600), in_transition: true, ..shape("s5") },      // mid-transition
        ];
        let plan = plan_sweep(&cfg(), &shapes);
        assert_eq!(plan.deactivate, vec!["s1"]);
        assert!(plan.evict.is_empty());
    }

    #[test]
    fn zero_idle_timeout_disables_dormancy() {
        let c = RetentionConfig { idle_timeout: Duration::ZERO, ..cfg() };
        let shapes = vec![SweepShape { idle: Duration::from_secs(1 << 20), ..shape("s1") }];
        assert!(plan_sweep(&c, &shapes).deactivate.is_empty());
    }

    #[test]
    fn dormancy_ttl_reaps_old_dormant_shapes() {
        let shapes = vec![
            dormant("s1", 100, 8 * 24 * 3600), // dormant past the TTL
            dormant("s2", 100, 3600),          // dormant, young
            SweepShape { idle: Duration::from_secs(9 * 24 * 3600), ..shape("s3") }, // active (long idle but never dormanted — e.g. still subscribed elsewhere)
        ];
        let plan = plan_sweep(&cfg(), &shapes);
        assert_eq!(plan.evict, vec![("s1".to_string(), EvictReason::DormantTtl)]);
    }

    #[test]
    fn non_parkable_shapes_are_evicted_from_active_after_the_full_grace() {
        let grace = 1800 + 7 * 24 * 3600; // idle_timeout + dormant_ttl
        let shapes = vec![
            // An aggregate/subquery shape (not dormancy-eligible), unsubscribed and idle past the
            // full grace window → evicted straight from active.
            SweepShape { idle: Duration::from_secs(grace), dormancy_eligible: false, ..shape("agg-old") },
            // Same but still subscribed → protected.
            SweepShape { idle: Duration::from_secs(grace), dormancy_eligible: false, refcount: 1, ..shape("agg-held") },
            // Same but within the grace window → kept.
            SweepShape { idle: Duration::from_secs(grace - 1), dormancy_eligible: false, ..shape("agg-young") },
        ];
        let plan = plan_sweep(&cfg(), &shapes);
        assert_eq!(plan.evict, vec![("agg-old".to_string(), EvictReason::DormantTtl)]);
        assert!(plan.deactivate.is_empty(), "non-parkable shapes never deactivate");
    }

    #[test]
    fn non_parkable_retirement_still_runs_when_dormancy_is_disabled() {
        let c = RetentionConfig {
            idle_timeout: Duration::ZERO,
            subscription_lease_timeout: Duration::from_secs(1800),
            ..cfg()
        };
        let shapes = vec![SweepShape { idle: c.dormant_ttl, dormancy_eligible: false, ..shape("changes-only") }];
        assert_eq!(plan_sweep(&c, &shapes).evict, vec![("changes-only".to_string(), EvictReason::DormantTtl)]);
    }

    #[test]
    fn max_shapes_evicts_least_recently_read_dormant_first() {
        let c = RetentionConfig { max_shapes: 2, ..cfg() };
        let shapes = vec![
            shape("active"),
            dormant("cold", 5000, 60), // least recently read → goes first
            dormant("warm", 100, 60),
        ];
        let plan = plan_sweep(&c, &shapes);
        assert_eq!(plan.evict, vec![("cold".to_string(), EvictReason::MaxShapes)]);
        assert!(!plan.over_capacity);
    }

    /// A live-only shape (subquery, aggregate, changes-only feed) with the given subscriptions and
    /// time since its last touch.
    fn live_only(id: &str, refcount: usize, idle_secs: u64) -> SweepShape {
        SweepShape { dormancy_eligible: false, refcount, idle: Duration::from_secs(idle_secs), ..shape(id) }
    }

    fn group_member(id: &str, group: &str, refcount: usize, idle_secs: u64) -> SweepShape {
        SweepShape { group: Some(group.into()), ..live_only(id, refcount, idle_secs) }
    }

    /// The cap never takes a shape somebody holds, nor a parkable shape that is still active (it
    /// parks first, on the idle timer). With only those left the cap cannot be met, and that is
    /// surfaced as pressure: it is also when new shapes are refused.
    #[test]
    fn max_shapes_never_evicts_a_subscribed_shape_or_a_parkable_active_one() {
        let c = RetentionConfig { max_shapes: 1, ..cfg() };
        let shapes = vec![
            shape("parkable-active"),
            SweepShape { refcount: 1, ..shape("parkable-held") },
            live_only("feed-held", 1, 9 * 24 * 3600),
            SweepShape { in_transition: true, ..live_only("feed-mid-create", 0, 0) },
            SweepShape { lease_tracked: false, ..live_only("unshared-library-feed", 0, 60) },
        ];
        let plan = plan_sweep(&c, &shapes);
        assert!(plan.evict.is_empty(), "{:?}", plan.evict);
        assert!(plan.over_capacity, "over the cap with nothing evictable must be surfaced, not resolved");
    }

    /// Over the cap, once no dormant shape is left, the live-only shapes nobody subscribes to go,
    /// longest idle first and only as many as it takes.
    #[test]
    fn max_shapes_evicts_the_longest_idle_unsubscribed_live_only_shape() {
        let c = RetentionConfig { max_shapes: 3, ..cfg() };
        let shapes = vec![
            live_only("held", 1, 5000),
            live_only("old", 0, 3000),
            live_only("older", 0, 4000),
            live_only("young", 0, 10),
            dormant("parked", 100, 60),
        ];
        // Five shapes, three allowed: the dormant one goes first, then the longest-idle feed that
        // nobody holds. `held` has been idle longest of all and stays.
        let plan = plan_sweep(&c, &shapes);
        assert_eq!(
            plan.evict,
            vec![("parked".to_string(), EvictReason::MaxShapes), ("older".to_string(), EvictReason::MaxShapes)]
        );
        assert!(!plan.over_capacity);
    }

    /// A group shares one stream and one lease, so the cap takes all of it or none of it, and none
    /// of it while any member is subscribed.
    #[test]
    fn max_shapes_leaves_group_members_protected_and_never_evicts_a_subscribed_one() {
        let c = RetentionConfig { max_shapes: 4, ..cfg() };
        let shapes = vec![
            group_member("held-a", "g-held", 1, 9000),
            group_member("held-b", "g-held", 0, 9000),
            group_member("gone-a", "g-gone", 0, 500),
            group_member("gone-b", "g-gone", 0, 800),
            live_only("feed", 0, 600),
        ];
        // One over. `g-gone` was last touched 500 s ago (its most recent member), so the single
        // feed is the longer idle and goes alone.
        assert_eq!(plan_sweep(&c, &shapes).evict, vec![("feed".to_string(), EvictReason::MaxShapes)]);

        // Two over: after the feed, the grouped members remain protected for OTTO-13834. `g-held`
        // and `g-gone` are both left to that group's retention/rejoin implementation.
        let c = RetentionConfig { max_shapes: 3, ..cfg() };
        let plan = plan_sweep(&c, &shapes);
        assert_eq!(plan.evict, vec![("feed".to_string(), EvictReason::MaxShapes)]);
        assert!(plan.over_capacity);
    }

    /// The live-only idle period retires an unsubscribed live-only shape long before the
    /// idle-plus-dormancy grace does (7 days 30 minutes in this config).
    #[test]
    fn an_unsubscribed_live_only_shape_is_evicted_after_the_live_only_period() {
        let c = RetentionConfig { live_only_idle: Duration::from_secs(24 * 3600), ..cfg() };
        let shapes = vec![
            live_only("idle-feed", 0, 24 * 3600),
            live_only("recent-feed", 0, 24 * 3600 - 1),
            live_only("held-feed", 1, 30 * 24 * 3600),
            // A parkable shape is untouched by the period: it parks on the idle timer as before.
            SweepShape { idle: Duration::from_secs(24 * 3600), ..shape("parkable") },
        ];
        let plan = plan_sweep(&c, &shapes);
        assert_eq!(plan.evict, vec![("idle-feed".to_string(), EvictReason::LiveOnlyIdle)]);
        assert_eq!(plan.deactivate, vec!["parkable"]);
    }

    /// The period is how long a client that went away can still catch up from the stream, and a
    /// lease only lapses a whole lease after the last renewal: a period under the lease would
    /// retire the feed in the sweep that lapsed it.
    #[test]
    fn the_live_only_period_is_never_shorter_than_the_lease() {
        let c = RetentionConfig {
            live_only_idle: Duration::from_secs(60),
            subscription_lease_timeout: Duration::from_secs(1800),
            ..cfg()
        };
        assert_eq!(c.live_only_evict_after(), Some((Duration::from_secs(1800), EvictReason::LiveOnlyIdle)));
        assert!(plan_sweep(&c, &[live_only("lapsing", 0, 1799)]).evict.is_empty());
        assert_eq!(
            plan_sweep(&c, &[live_only("lapsed", 0, 1800)]).evict,
            vec![("lapsed".to_string(), EvictReason::LiveOnlyIdle)]
        );
    }

    /// `0` keeps the rule from before the period existed, and a grace shorter than the period
    /// still applies: the period only ever shortens how long an abandoned shape is kept.
    #[test]
    fn the_live_only_period_only_shortens_the_idle_plus_dormancy_grace() {
        let grace = Duration::from_secs(1800 + 7 * 24 * 3600);
        let off = RetentionConfig { live_only_idle: Duration::ZERO, ..cfg() };
        assert_eq!(off.live_only_evict_after(), Some((grace, EvictReason::DormantTtl)));
        let longer = RetentionConfig { live_only_idle: Duration::from_secs(30 * 24 * 3600), ..cfg() };
        assert_eq!(longer.live_only_evict_after(), Some((grace, EvictReason::DormantTtl)));
        let neither = RetentionConfig { live_only_idle: Duration::ZERO, dormant_ttl: Duration::ZERO, ..cfg() };
        assert_eq!(neither.live_only_evict_after(), None);
        assert!(plan_sweep(&neither, &[live_only("kept", 0, 1 << 30)]).evict.is_empty());
    }

    /// Group retention and re-join stay with OTTO-13834; this evictor leaves every member alone.
    #[test]
    fn the_live_only_period_leaves_group_members_to_group_retention() {
        let c = RetentionConfig { live_only_idle: Duration::from_secs(3600), ..cfg() };
        let shapes = vec![
            group_member("a", "g-recent", 0, 7200),
            group_member("b", "g-recent", 0, 60),
            group_member("c", "g-old", 0, 7200),
            group_member("d", "g-old", 0, 3600),
        ];
        assert!(plan_sweep(&c, &shapes).evict.is_empty());
    }

    #[test]
    fn grouped_live_only_shapes_are_not_eviction_candidates() {
        let c = RetentionConfig { max_shapes: 1, live_only_idle: Duration::from_secs(1), ..cfg() };
        let shapes =
            vec![group_member("group-member-a", "group", 0, 3600), group_member("group-member-b", "group", 0, 10)];
        let plan = plan_sweep(&c, &shapes);
        assert!(plan.evict.is_empty(), "group members must remain under OTTO-13834 retention");
        assert!(plan.over_capacity);
    }

    /// What a create needs before it mints `wanted` shapes: room under the cap, made from the
    /// same shapes the sweep may evict, and a plain "no" when there are none.
    #[test]
    fn room_under_the_cap_is_made_only_from_what_the_cap_may_evict() {
        let c = RetentionConfig { max_shapes: 3, ..cfg() };
        let shapes = vec![live_only("held", 1, 900), live_only("old", 0, 800), live_only("young", 0, 10)];
        // At the cap, not over it: the sweep has nothing to do, a create needs one slot.
        assert!(plan_sweep(&c, &shapes).evict.is_empty());
        assert_eq!(plan_cap_room(&c, &shapes, 1), CapRoom::Evict(vec!["old".to_string()]));
        assert_eq!(plan_cap_room(&c, &shapes, 2), CapRoom::Evict(vec!["old".to_string(), "young".to_string()]));
        assert_eq!(plan_cap_room(&c, &shapes, 3), CapRoom::Full, "a subscribed shape is never evicted");
        assert_eq!(plan_cap_room(&RetentionConfig { max_shapes: 4, ..cfg() }, &shapes, 1), CapRoom::Fits);
        assert_eq!(plan_cap_room(&cfg(), &shapes, 1000), CapRoom::Fits, "0 is no cap at all");
    }

    #[test]
    fn eviction_reasons_and_shape_kinds_index_their_own_tables() {
        for (i, reason) in EvictReason::ALL.into_iter().enumerate() {
            assert_eq!(reason.index(), i, "{reason:?}");
        }
        for (i, kind) in ShapeKind::ALL.into_iter().enumerate() {
            assert_eq!(kind.index(), i, "{kind:?}");
        }
        assert_eq!(ShapeKind::of(true, false, true), ShapeKind::Subquery);
        assert_eq!(ShapeKind::of(false, false, true), ShapeKind::ChangesOnly);
        assert_eq!(ShapeKind::of(false, true, false), ShapeKind::Aggregate);
        assert_eq!(ShapeKind::of(false, false, false), ShapeKind::Plain);
    }

    #[test]
    fn disk_budget_evicts_lru_dormant_until_under() {
        let c = RetentionConfig { disk_budget_bytes: 100, ..cfg() };
        let shapes = vec![
            SweepShape { stream_bytes: 80, ..shape("active") },
            SweepShape { stream_bytes: 30, ..dormant("cold", 5000, 60) },
            SweepShape { stream_bytes: 30, ..dormant("warm", 100, 60) },
        ];
        // 140 tracked > 100: evicting "cold" (LRU) brings it to 110; still over, evict "warm" → 80.
        let plan = plan_sweep(&c, &shapes);
        assert_eq!(
            plan.evict,
            vec![("cold".to_string(), EvictReason::DiskBudget), ("warm".to_string(), EvictReason::DiskBudget)]
        );
        assert!(!plan.over_budget);
    }

    #[test]
    fn disk_budget_over_with_all_active_flags_loudly() {
        let c = RetentionConfig { disk_budget_bytes: 10, ..cfg() };
        let shapes = vec![SweepShape { stream_bytes: 100, ..shape("a") }];
        let plan = plan_sweep(&c, &shapes);
        assert!(plan.evict.is_empty());
        assert!(plan.over_budget);
    }

    #[test]
    fn replay_budget_evicts_over_budget_and_unknown_uses_minimum() {
        let c = RetentionConfig { replay_min_bytes: 100, replay_multiplier: 4, ..cfg() };
        let over = SweepShape { replay_span_bytes: Some(401), backfill_bytes: Some(100), ..dormant("over", 1, 1) };
        let unknown_ok = SweepShape { replay_span_bytes: Some(100), backfill_bytes: None, ..dormant("unknown", 1, 1) };
        let plan = plan_sweep(&c, &[over, unknown_ok]);
        assert_eq!(plan.evict, vec![("over".to_string(), EvictReason::ReplayBudget)]);
    }

    #[test]
    fn missing_replay_segment_is_evicted_immediately() {
        let shape = SweepShape { replay_span_bytes: None, ..dormant("gone", 1, 1) };
        let plan = plan_sweep(&cfg(), &[shape]);
        assert_eq!(plan.evict, vec![("gone".to_string(), EvictReason::ChangeLogRetention)]);
    }

    #[test]
    fn ttl_evictions_count_toward_the_cap_and_budget() {
        // s1 falls to the TTL; that alone brings the count under max_shapes, so no cap eviction.
        let c = RetentionConfig { max_shapes: 1, ..cfg() };
        let shapes = vec![shape("active"), dormant("old", 5000, 8 * 24 * 3600)];
        let plan = plan_sweep(&c, &shapes);
        assert_eq!(plan.evict, vec![("old".to_string(), EvictReason::DormantTtl)]);
        assert!(!plan.over_capacity);
    }

    #[test]
    fn the_live_only_period_is_read_from_its_own_variable() {
        let set = |name: &str| (name == "ELECTRIC_CIRCUITS_LIVE_ONLY_IDLE_SECS").then(|| "7200".to_string());
        assert_eq!(RetentionConfig::resolve(set).live_only_idle, Duration::from_secs(7200));
        // The legacy idle knob still moves the idle timeout and the lease, and nothing else.
        let legacy = |name: &str| (name == "ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS").then(|| "1".to_string());
        assert_eq!(RetentionConfig::resolve(legacy).live_only_idle, Duration::from_secs(86400));
    }

    #[test]
    fn config_defaults_are_sensible() {
        let d = RetentionConfig::default();
        assert_eq!(d.idle_timeout, Duration::from_secs(6 * 3600));
        assert_eq!(d.subscription_lease_timeout, Duration::from_secs(1800));
        assert_eq!(d.dormant_ttl, Duration::from_secs(604800));
        assert_eq!(d.live_only_idle, Duration::from_secs(86400));
        assert_eq!(d.max_shapes, 10_000);
        assert_eq!(d.disk_budget_bytes, 0);
        assert_eq!(d.sweep_interval, Duration::from_secs(60));
    }
}
