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
//! Eviction is **layered**: ordinary shapes are evicted from dormant, while non-parkable active
//! shapes (subqueries, aggregates, changes-only) retire after the full idle+dormancy grace,
//! least-recently-read first:
//! 1. **Dormancy TTL** (hygiene): dormant longer than [`RetentionConfig::dormant_ttl`] → evict.
//!    Reaps dead shapes even with no resource pressure.
//! 2. **`max_shapes` count cap** (engine cost bound): when the total shape count exceeds
//!    [`RetentionConfig::max_shapes`], evict least-recently-read dormant shapes until under.
//! 3. **Disk budget** (hard backstop): when the tracked shape-stream bytes exceed
//!    [`RetentionConfig::disk_budget_bytes`], evict least-recently-read dormant shapes until
//!    under. Byte accounting is engine-side ([`crate::ds::DsClient`] counts what it appends) —
//!    the durable-streams server exposes no per-stream sizes yet — so it undercounts streams
//!    written before the current process started.
//!
//! If a cap/budget is exceeded but nothing eligible is left to evict, the sweep logs loudly and
//! bumps a metric instead of evicting active shapes.
//!
//! Subquery and aggregate shapes are exempt from dormancy (their engine state — inner-set
//! arrangements, running folds — cannot be rebuilt from a change-log replay alone), so they stay
//! active while retained. So that they cannot leak forever once unsubscribed, the TTL layer evicts
//! them **straight from active** (full teardown) after the same total grace an eligible shape gets
//! (idle timeout + dormancy TTL); like any evicted shape, a returning client recreates them.
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
/// | `ELECTRIC_CIRCUITS_REPLAY_MIN_BYTES` | `16777216` (16 MiB) | Minimum change-log span budget for dormant replay. |
/// | `ELECTRIC_CIRCUITS_REPLAY_MULTIPLIER` | `4` | Replay budget multiplier applied to the shape's recorded backfill bytes. |
/// | `ELECTRIC_CIRCUITS_MAX_SHAPES` | `10000` | Total shape-count cap; over it, least-recently-read dormant shapes are evicted. `0` = unlimited. |
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
    /// Minimum dormant replay span in bytes (`ELECTRIC_CIRCUITS_REPLAY_MIN_BYTES`).
    pub replay_min_bytes: u64,
    /// Backfill-size multiplier for replay admission (`ELECTRIC_CIRCUITS_REPLAY_MULTIPLIER`).
    pub replay_multiplier: u64,
    /// Total shape-count cap (`ELECTRIC_CIRCUITS_MAX_SHAPES`, default 10000; 0 = unlimited).
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
/// The floor alone does not keep a tracked stream alive. A stream the engine neither appends to nor
/// reads, such as a restored dormant shape whose dormancy clock restarted at boot, still expires
/// unless something reads it; `HEAD` does not slide the TTL.
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
    /// Eligible for dormancy at all (plain row shapes; subquery + aggregate shapes are not).
    pub dormancy_eligible: bool,
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
    pub fn as_str(&self) -> &'static str {
        match self {
            EvictReason::DormantTtl => "dormant-ttl",
            EvictReason::MaxShapes => "max-shapes",
            EvictReason::DiskBudget => "disk-budget",
            EvictReason::ChangeLogRetention => "change-log-retention",
            EvictReason::ReplayBudget => "replay-budget",
        }
    }
}

/// What one retention sweep should do. Deactivations and evictions are executed by the engine;
/// the `over_*` flags mean a cap/budget is exceeded with nothing dormant left to evict — surfaced
/// loudly (log + metric), never resolved by evicting active shapes.
#[derive(Default)]
pub struct SweepPlan {
    pub deactivate: Vec<String>,
    pub evict: Vec<(String, EvictReason)>,
    pub over_capacity: bool,
    pub over_budget: bool,
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
        if s.dormant_for.is_some() {
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
            if s.dormant_for.is_some_and(|d| d >= cfg.dormant_ttl) {
                plan.evict.push((s.id.clone(), EvictReason::DormantTtl));
                evicted.insert(&s.id);
            }
        }
        // Shapes that cannot park (subquery / aggregate state is not rebuildable from a bounded
        // replay; changes_only feeds would lose their dormant-period history) would otherwise be
        // immortal once unsubscribed: evict them straight from active after the same total grace
        // an eligible shape gets (idle timeout + dormancy TTL). They are recreatable — a returning
        // client gets 404 / must-refetch and recreates; changes_only callers receive a fresh feed
        // rather than a silently incomplete dormant replay.
        for s in shapes {
            if !s.dormancy_eligible
                && !s.in_transition
                && s.refcount == 0
                && s.idle >= cfg.idle_timeout + cfg.dormant_ttl
            {
                plan.evict.push((s.id.clone(), EvictReason::DormantTtl));
                evicted.insert(&s.id);
            }
        }
    }

    // Dormant shapes still standing after tier 1, least-recently-read first (largest idle first).
    let mut lru: Vec<&SweepShape> =
        shapes.iter().filter(|s| s.dormant_for.is_some() && !evicted.contains(s.id.as_str())).collect();
    lru.sort_by_key(|s| std::cmp::Reverse(s.idle));
    let mut lru = lru.into_iter();

    // Tier 2 — max_shapes count cap.
    if cfg.max_shapes > 0 {
        let mut count = shapes.len() - evicted.len();
        while count > cfg.max_shapes {
            match lru.next() {
                Some(s) => {
                    plan.evict.push((s.id.clone(), EvictReason::MaxShapes));
                    evicted.insert(&s.id);
                    count -= 1;
                }
                None => {
                    plan.over_capacity = true;
                    break;
                }
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> RetentionConfig {
        RetentionConfig {
            idle_timeout: Duration::from_secs(1800),
            subscription_lease_timeout: Duration::from_secs(1800),
            dormant_ttl: Duration::from_secs(7 * 24 * 3600),
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

    #[test]
    fn max_shapes_never_evicts_active_shapes() {
        let c = RetentionConfig { max_shapes: 1, ..cfg() };
        let shapes = vec![shape("a1"), shape("a2"), shape("a3")];
        let plan = plan_sweep(&c, &shapes);
        assert!(plan.evict.is_empty());
        assert!(plan.over_capacity, "over the cap with nothing dormant must be surfaced, not resolved");
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
    fn config_defaults_are_sensible() {
        let d = RetentionConfig::default();
        assert_eq!(d.idle_timeout, Duration::from_secs(6 * 3600));
        assert_eq!(d.subscription_lease_timeout, Duration::from_secs(1800));
        assert_eq!(d.dormant_ttl, Duration::from_secs(604800));
        assert_eq!(d.max_shapes, 10_000);
        assert_eq!(d.disk_budget_bytes, 0);
        assert_eq!(d.sweep_interval, Duration::from_secs(60));
    }
}
