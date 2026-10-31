//! Engine orchestration: schema/shape registries and one tailer task per table. A tailer holds only
//! per-shape routing metadata (no table data): it fans each change out to standalone filters and to
//! equality shapes routed by key, and appends the filtered deltas (as State-Protocol envelopes) to
//! the shape streams. Shapes backfill from Postgres on registration; see `add_shape_routed`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use crate::value::{Tup2, ZWeight};
use anyhow::{Context, Result, bail};
use tokio::sync::{Mutex, mpsc};

use std::sync::atomic::Ordering;

use crate::changelog::{ChangeLogWriter, ChangesState, LogPosition, segment_path};
use crate::ds::{DsClient, Envelope, EnvelopeHeaders};
use crate::heap_size::HeapSize;
use crate::metrics::Timer;
use crate::predicate::{CompiledPredicate, PredicateJson};
use crate::retention::{EvictReason, Evicted, LifeState, RetentionConfig, ShapeLife, SweepShape};
use crate::runtime_authority::{AuthorityMarker, RuntimeDrainReceipt, RuntimeReceipts};
use crate::schema::{Schema, SharedTables, TableSchema, compile_schema};
use crate::subquery::{SubqueryRegistry, predicate_has_subquery, referenced_tables};
use crate::table_ref::{TableRef, TableSelector};
use crate::value::{Row, Value};
use std::time::Duration;

#[cfg(test)]
mod boot_projection_tests;
mod catalog;
mod circuit_serving;
mod drift;
pub(crate) mod emission;
pub(crate) mod epoch;
mod executors;
pub(crate) mod group;
mod held;
mod introspection;
mod lifecycle;
pub(crate) mod membership;
mod output;
mod planning;
mod retirement;
mod sequencer;
mod subset_query_log;
#[cfg(test)]
mod tests;

use catalog::*;
use circuit_serving::*;
use epoch::*;
use executors::*;
use introspection::*;
use planning::*;
use retirement::*;
use sequencer::*;

/// Only active creates own entries; waiters hold no partial reservation. This is request-owned
/// coordination, not another worker queue. PostgreSQL work still uses the bounded engine pool.
struct SubqueryAdmission {
    nodes: std::sync::Mutex<HashSet<crate::predicate::SubquerySig>>,
    changed: tokio::sync::Notify,
    /// Bounds retained fresh initializer state across PG checkouts and cancellation cleanup.
    seed_slots: Arc<tokio::sync::Semaphore>,
}

impl SubqueryAdmission {
    fn new(capacity: usize) -> Self {
        Self {
            nodes: Default::default(),
            changed: Default::default(),
            seed_slots: Arc::new(tokio::sync::Semaphore::new(capacity.clamp(1, tokio::sync::Semaphore::MAX_PERMITS))),
        }
    }
}

impl Default for SubqueryAdmission {
    fn default() -> Self {
        let admission = Self::new(
            std::env::var("ELECTRIC_CIRCUITS_SUBQUERY_INIT_CONCURRENCY").ok().and_then(|v| v.parse().ok()).unwrap_or(4),
        );
        tracing::info!(
            subquery_init_concurrency = admission.seed_slots.available_permits(),
            "subquery initialization capacity configured"
        );
        admission
    }
}

pub use drift::ResolveLock;
pub use epoch::{EpochBreakReason, EpochBroken, EpochResetting, SlotBinding};
pub use executors::AggFn;
pub(crate) use group::{
    GroupClaims, GroupConflict, GroupFence, GroupLoadInFlight, GroupMember, GroupRecord, GroupRelease,
    GroupStreamClosed,
};
pub use introspection::{
    AggInfo, ArrConsumer, ArrCounts, ArrIndex, ArrInput, ArrangementGraph, EngineGraph, FamilyStat, GraphEdge,
    GraphNode, GraphShape, NodeIndex, NodeStateSummary, NodeValue, OpEdge, OpNode, ShapeRecord, StateSnapshot,
    TableColumnInfo, TableSchemaInfo, TableStats,
};
pub(crate) use output::{
    absolute_envelope, apply_envelope, delete_envelopes, needs_absolute_emission, translate_output,
};
pub use planning::CircuitPlacement;

/// `GET /v1/health` phases (see [`Engine::health`]).
const HEALTH_WAITING: u8 = 0;
const HEALTH_STARTING: u8 = 1;
const HEALTH_ACTIVE: u8 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ManagedRole {
    Active,
    Standby,
    Quiescing,
    Promoting,
}

#[derive(Clone, Debug)]
struct ManagedDeploymentState {
    config: crate::deployment::ManagedDeploymentConfig,
    coordination_key: String,
    role: ManagedRole,
    ownership: Option<crate::deployment::Ownership>,
}

/// Public data is fenced until boot completes and any managed ownership is proved.
#[derive(Debug)]
pub struct DeploymentNotReady;

impl std::fmt::Display for DeploymentNotReady {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("engine is not the active ready writer; retry")
    }
}

impl std::error::Error for DeploymentNotReady {}

#[derive(Debug, thiserror::Error)]
#[error("catalog restore found a sequencer at a different checkpoint; restart requested (75)")]
pub(crate) struct CatalogRestoreMismatch;

/// PostgreSQL owns its schemas; caller-defined schemas are a library-mode API only.
#[derive(Debug, thiserror::Error)]
#[error("define_schema is only supported in library mode; PostgreSQL schemas are introspected")]
pub(crate) struct SchemaDefinitionUnsupported;

#[derive(Clone, Debug, PartialEq, Eq)]
enum BootEpochAction {
    Restore,
    Park(EpochBreakReason),
    Wait(Option<i32>),
}

/// Decide what boot may do with the slot verdict before any catalog or schema state is installed.
fn boot_epoch_action(verdict: &Verdict) -> BootEpochAction {
    match verdict {
        Verdict::FirstBoot | Verdict::Ok { .. } => BootEpochAction::Restore,
        Verdict::Busy { active_pid } => BootEpochAction::Wait(*active_pid),
        Verdict::Break(reason) => BootEpochAction::Park(*reason),
    }
}

/// The engine computed membership effects it could not deliver (see [`DegradeState`]), so what it
/// serves is silently wrong. A typed error: the HTTP layer maps it to 503 by downcast, never by
/// matching on message text.
#[derive(Debug)]
pub struct Degraded;

impl std::fmt::Display for Degraded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("degraded: subquery membership effects were lost; restart required")
    }
}

impl std::error::Error for Degraded {}

/// Live replay has stopped permanently for this engine instance. Refuse data until restart;
/// unlike boot/ownership admission, retrying the same instance cannot restore progress.
#[derive(Debug, thiserror::Error)]
#[error("degraded: live replay cannot continue; restart required")]
pub struct ReplayFailed;

/// A create or join finished its **catalog durability wait** only to find that the shape it was
/// about to acknowledge is no longer the shape it made: retired by a schema drift / `TRUNCATE` /
/// purge while the wait was in flight, or belonging to a superseded epoch.
///
/// `send_durable` is an unbounded wait on external storage, so it opens a whole new — externally
/// controllable — interval between a create's last check and its answer. Typed, because the request
/// itself is still perfectly valid: only this attempt lost a race, so the create is redone rather
/// than making every client implement that retry (see `Engine::recheck_after_durability`). Reaching
/// a caller at all means the retries were exhausted, which is a 503: come back.
#[derive(Debug)]
pub struct CreateRaced(pub String);

impl std::fmt::Display for CreateRaced {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}; retry", self.0)
    }
}

impl std::error::Error for CreateRaced {}

/// A create named a **subscription id another shape already holds** (ADR-0008).
///
/// Typed because it is a 409, not a 500 and not a retry: nothing about waiting changes the answer,
/// and the request is not malformed either — the caller simply used one name for two different
/// shapes. Accepting it would leave that caller holding a single id against two subscriptions,
/// unable to release either without saying which, which is the ambiguity the id exists to remove.
#[derive(Debug)]
pub struct SubscriptionConflict {
    pub subscription: String,
    /// The shape that holds it now.
    pub shape: String,
}

/// Why a touch cannot hand back the shape it was asked for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecreateReason {
    /// The dormant replay span exceeded its budget, so the shape was deliberately retired. The
    /// shape is gone from the lifecycle map, so a create can fall through to a fresh one.
    OverBudget,
    /// The reactivation is admitted and still running, but the joining request reached
    /// `ELECTRIC_CIRCUITS_REACTIVATION_JOIN_TIMEOUT_SECS`. The old identity is retired so a create
    /// in the same request falls through to a fresh shape; the detached replay continues until it
    /// notices its shape is gone.
    JoinTimedOut,
}

/// The touch could not complete and the shape is still there: a transient change-log HEAD failure,
/// a replay page the store never finished, a reactivator that went away. The shape has been parked
/// back as dormant and the next touch retries it, so this is a RETRYABLE refusal — distinct from
/// [`ReactivationRecreate`], where the identity is gone and only a replacement will do, and from a
/// 500, which says the engine is broken.
#[derive(Debug)]
pub struct ReactivationDeferred {
    pub shape: String,
    /// What the failed attempt reported, kept for the response body and the log line.
    pub detail: String,
}

impl ReactivationDeferred {
    pub(crate) fn new(shape: impl Into<String>, detail: impl Into<String>) -> Self {
        Self { shape: shape.into(), detail: detail.into() }
    }
}

impl std::fmt::Display for ReactivationDeferred {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "shape '{}' reactivation was deferred ({}); retry the read", self.shape, self.detail)
    }
}

impl std::error::Error for ReactivationDeferred {}

/// The touch cannot return the retained shape; the caller should obtain a replacement rather than
/// receive a generic 500. See [`RecreateReason`] for which of the two situations produced it.
#[derive(Debug)]
pub struct ReactivationRecreate {
    pub shape: String,
    pub reason: RecreateReason,
}

/// A pending shape's buffer of live deltas passed its cap and was dropped. The shape cannot be
/// activated from it — its stream would be missing exactly those changes — so this is a recreate
/// outcome, not a retry.
#[derive(Debug)]
pub(crate) struct PendingBufferOverflow {
    pub shape: String,
}

impl std::fmt::Display for PendingBufferOverflow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "shape '{}' buffered more live change than ELECTRIC_CIRCUITS_PENDING_BUFFER_MAX_BYTES allows while it \
             was being replayed; recreate it from a fresh backfill",
            self.shape
        )
    }
}

impl std::error::Error for PendingBufferOverflow {}

/// A transient changelog HEAD failure. Unlike a missing segment, it is not evidence that a
/// dormant cursor is unrecoverable and must never trigger eviction.
#[derive(Debug)]
pub(crate) struct ReplayHeadUnavailable;

impl std::fmt::Display for ReplayHeadUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("change-log HEAD unavailable; retaining dormant shape for a later retry")
    }
}

impl std::error::Error for ReplayHeadUnavailable {}

/// A group health probe could not reach Durable Streams. The group remains owned and callers
/// should retry rather than tearing down a healthy claim on an inconclusive HEAD.
#[derive(Debug)]
pub(crate) struct GroupStreamUnavailable;

impl std::fmt::Display for GroupStreamUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("group stream health is temporarily unavailable; retrying preserves the group claim")
    }
}

impl std::error::Error for GroupStreamUnavailable {}

impl ReactivationRecreate {
    pub(crate) fn over_budget(shape: impl Into<String>) -> Self {
        Self { shape: shape.into(), reason: RecreateReason::OverBudget }
    }

    pub(crate) fn join_timed_out(shape: impl Into<String>) -> Self {
        Self { shape: shape.into(), reason: RecreateReason::JoinTimedOut }
    }
}

impl std::fmt::Display for ReactivationRecreate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.reason {
            RecreateReason::OverBudget => {
                write!(f, "shape '{}' was retired; recreate from a fresh backfill", self.shape)
            }
            RecreateReason::JoinTimedOut => write!(
                f,
                "shape '{}' is still replaying its change log; the join gave up before the gateway would. \
                 Recreate from a fresh backfill or retry later",
                self.shape
            ),
        }
    }
}

impl std::error::Error for ReactivationRecreate {}

impl std::fmt::Display for SubscriptionConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "subscription '{}' already belongs to shape '{}'; a subscription id names one shape — \
             release it first, or use a different id for this one",
            self.subscription, self.shape
        )
    }
}

impl std::error::Error for SubscriptionConflict {}

/// New control-plane mutations are paused for a deployment handoff while existing stream reads
/// remain healthy. Typed so HTTP answers 503 without conflating the gate with read readiness.
#[derive(Debug)]
pub struct ControlAdmissionClosed;

impl std::fmt::Display for ControlAdmissionClosed {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("control admission is closed for deployment handoff; retry")
    }
}

impl std::error::Error for ControlAdmissionClosed {}

struct ControlAdmission {
    open: std::sync::atomic::AtomicBool,
    in_flight: std::sync::atomic::AtomicU64,
    drained: tokio::sync::Notify,
}

impl ControlAdmission {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            open: std::sync::atomic::AtomicBool::new(true),
            in_flight: std::sync::atomic::AtomicU64::new(0),
            drained: tokio::sync::Notify::new(),
        })
    }

    fn release(&self) {
        if self.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.drained.notify_waiters();
        }
    }
}

/// Held for the complete lifetime of an admitted control mutation. Closing admission first flips
/// the gate, then waits for these guards to drop before a deployment writes its source fence.
pub struct ControlPermit {
    admission: Arc<ControlAdmission>,
}

impl Drop for ControlPermit {
    fn drop(&mut self) {
        self.admission.release();
    }
}

/// Durable proof that the named source transaction was completely derived into shape streams.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceDrainReceipt {
    pub source_commit_id: String,
    pub commit_lsn: String,
}

/// In-process ordering for durable source receipts. The catalog remains the durable authority for
/// the receipt itself; this ledger only distinguishes receipts observed before a control-admission
/// closure from those observed afterwards. It intentionally resets on process restart, where
/// managed admission is fail-closed and the controller must create a new fence.
#[derive(Default)]
pub(crate) struct SourceReceiptProgress {
    next_sequence: u64,
    sequences: HashMap<String, u64>,
    closure_watermark: Option<u64>,
}

impl SourceReceiptProgress {
    pub(crate) fn record(&mut self, source_commit_id: &str) {
        if !self.sequences.contains_key(source_commit_id) {
            self.next_sequence += 1;
            self.sequences.insert(source_commit_id.to_owned(), self.next_sequence);
        }
    }

    /// Installs a receipt read from the durable catalog. If admission was already closed while
    /// boot was still restoring that catalog, this receipt necessarily predates the close: keep
    /// it on (not across) that closure's frontier. Live sequencer receipts always use [`record`]
    /// and therefore advance past the frontier.
    pub(crate) fn record_restored(&mut self, source_commit_id: &str) {
        if self.sequences.contains_key(source_commit_id) {
            return;
        }
        if let Some(watermark) = self.closure_watermark {
            self.sequences.insert(source_commit_id.to_owned(), watermark);
        } else {
            self.record(source_commit_id);
        }
    }

    fn snapshot_closure(&mut self) {
        self.closure_watermark = Some(self.next_sequence);
    }

    fn clear_closure(&mut self) {
        self.closure_watermark = None;
    }

    fn is_after_closure(&self, source_commit_id: &str) -> bool {
        self.closure_watermark
            .zip(self.sequences.get(source_commit_id))
            .is_some_and(|(watermark, sequence)| *sequence > watermark)
    }

    fn has_closure(&self) -> bool {
        self.closure_watermark.is_some()
    }
}

/// Fail-closed degradation state, latched when a flip batch exhausts its retries.
///
/// The inner-set node was already reconciled under the registry lock before the batch's query-backs
/// ran, so an abandoned batch's effects can never be re-derived — only a restart, which re-seeds
/// every node from Postgres, makes the engine right again. Until then it refuses every read that
/// would carry membership rather than serve the lie.
pub(crate) struct DegradeState {
    degraded: std::sync::atomic::AtomicBool,
    /// Flip batches abandoned so far (`GET /replication/lsn` → `flipFailures`).
    failures: std::sync::atomic::AtomicU64,
    /// Wakes the stream reaper on the transition (see `Engine::ensure_degrade_reaper`).
    wake: tokio::sync::watch::Sender<bool>,
    reaper_started: std::sync::atomic::AtomicBool,
}

impl DegradeState {
    fn new() -> Arc<Self> {
        Arc::new(DegradeState {
            degraded: std::sync::atomic::AtomicBool::new(false),
            failures: std::sync::atomic::AtomicU64::new(0),
            wake: tokio::sync::watch::channel(false).0,
            reaper_started: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Count one abandoned batch and latch the engine degraded (idempotent; never cleared).
    fn mark(&self) {
        self.failures.fetch_add(1, Ordering::SeqCst);
        self.degraded.store(true, Ordering::SeqCst);
        let _ = self.wake.send(true); // no reaper subscribed = no subquery shape to reap
    }
}

/// One subset page (see [`Engine::query_subset`]): the projected rows and the fences of the settled
/// snapshot they were read in.
pub struct SubsetPage {
    pub rows: Vec<serde_json::Value>,
    /// `pg_current_wal_lsn()` at the snapshot.
    pub lsn: String,
    /// `pg_current_snapshot()::text` of the snapshot (full xid8 values).
    pub snapshot: String,
    /// `pg_current_wal_insert_lsn()` at the snapshot.
    pub horizon: String,
}

/// One read of a [`Engine::query_subset_batch`].
pub struct SubsetReadReq {
    pub table: TableRef,
    pub where_: Option<PredicateJson>,
    pub columns: Option<Vec<String>>,
    pub order_by: Option<(String, bool)>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

/// The pages of a batch, in request order, read in one snapshot whose fences they share.
pub struct SubsetBatchPage {
    pub pages: Vec<Vec<serde_json::Value>>,
    pub lsn: String,
    pub snapshot: String,
    pub horizon: String,
}

struct SubsetPlan {
    ts: TableSchema,
    out_cols: Option<Arc<Vec<usize>>>,
    where_sql: Option<(String, Vec<String>)>,
    scope: crate::pg::SettleScope,
    scope_tables: Vec<TableRef>,
    order: Option<(usize, bool)>,
    limit: Option<i64>,
    offset: Option<i64>,
}

impl SubsetPlan {
    fn project(&self, rows: &[crate::value::Row]) -> Vec<serde_json::Value> {
        let proj = self.out_cols.as_deref().map(Vec::as_slice);
        rows.iter().map(|r| self.ts.row_to_json_cols(r, proj)).collect()
    }
}

#[derive(Clone)]
pub struct Engine {
    pub(crate) metrics: Arc<crate::metrics::Metrics>,
    pub(crate) memory_gauges: Arc<crate::mem::Gauges>,
    pub(crate) telemetry: Arc<std::sync::Mutex<Option<Arc<crate::mem::Telemetry>>>>,
    pg_pool: Option<crate::pg::Pool>,
    publish_generated: Arc<std::sync::atomic::AtomicBool>,
    pub(crate) electric_handles: Arc<crate::electric::HandleRegistry>,
    close_result: Arc<Mutex<Option<crate::shutdown::ShutdownOutcome>>>,
    membership: crate::subq_circuit::MembershipCircuit,
    ds: DsClient,
    state: Arc<Mutex<EngineState>>,
    /// Postgres connection string when running in Postgres mode (logical replication + query-back
    /// backfill, no in-memory `table_state`). `None` keeps the engine usable only as a library shell.
    pg_url: Option<String>,
    /// Which authority owns PostgreSQL publication and replica-identity
    /// mutation for this source. Hosted control planes provision those
    /// objects before handing the narrowed source to the Engine, so the
    /// Engine validates them without acquiring DDL authority.
    postgres_setup: PostgresSetup,
    /// Last commit LSN the replication ingestor has appended (observability).
    repl_lsn: Arc<std::sync::Mutex<String>>,
    /// Highest `__el_sync` sentinel counter the ingestor has decoded-and-appended. The drain barrier
    /// bumps the sentinel and waits for this to catch up — robust under a shared multi-database
    /// Postgres (per-database, no dependence on server-global WAL LSNs).
    repl_sync: Arc<std::sync::atomic::AtomicI64>,
    /// Set once the replication ingestor has been spawned, so `setup_postgres` stays idempotent.
    replicator_started: Arc<std::sync::atomic::AtomicBool>,
    /// Boot readiness phase driving `GET /v1/health`: 0 = `waiting` (Postgres not connected), 1 =
    /// `starting` (connected; introspecting / creating slot / spawning ingest), 2 = `active` (ingest
    /// loop running). Library mode (no Postgres) is `active` from construction.
    health: Arc<std::sync::atomic::AtomicU8>,
    /// Opt-in ownership gate. Kept separate from health so legacy mode remains byte compatible.
    managed_deployment: Arc<std::sync::Mutex<Option<ManagedDeploymentState>>>,
    /// Wakes a standby boot retry immediately after a successful local promotion.
    managed_wakeup: Arc<tokio::sync::Notify>,
    /// Independent of read health: false refuses new control-plane mutations while restored stream
    /// reads continue. The deployment controller closes this before writing a source fence.
    control_admission: Arc<ControlAdmission>,
    /// Source-fence receipts that have landed in the durable catalog, restored at boot. The map is
    /// the only authority behind the private drained-through endpoint; task exit and slot state do
    /// not populate it.
    source_receipts: Arc<std::sync::Mutex<HashMap<String, SourceDrainReceipt>>>,
    last_source_receipt: Arc<std::sync::Mutex<Option<SourceDrainReceipt>>>,
    source_receipt_progress: Arc<std::sync::Mutex<SourceReceiptProgress>>,
    runtime_receipts: Arc<std::sync::Mutex<RuntimeReceipts>>,
    /// Cross-table subquery registry: maintained inner-set nodes (shared by canonical signature) + the
    /// outer subquery shapes that depend on them. Every tailer routes its deltas here so an inner-table
    /// change moves outer rows. `None`-free; empty until a subquery shape is created.
    subqueries: Arc<Mutex<SubqueryRegistry>>,
    /// Overlapping membership-node initializers serialize through phase C or rollback. Disjoint
    /// predicates can seed/backfill concurrently without holding engine state or the registry.
    subquery_init: Arc<SubqueryAdmission>,
    /// Best-effort per-envelope trace broadcast (see [`crate::trace`]). Events are serialized once
    /// and only when someone is subscribed; slow subscribers lag and drop.
    trace_tx: tokio::sync::broadcast::Sender<Arc<String>>,
    /// Sender to the single flip-propagator task: inner-set flips detected by a tailer are handed
    /// off here so their Postgres query-backs run off the tailer hot path (see
    /// [`crate::subquery::propagate_flips`]).
    flip_tx: mpsc::UnboundedSender<FlipWork>,
    /// Flip batches enqueued but not yet fully propagated. Part of the convergence barrier:
    /// drained change log + `pending_flips == 0` ⇒ all subquery effects have landed. An abandoned
    /// batch keeps its count forever, so the barrier can never report "everything landed" over
    /// effects that were lost — the engine degrades instead (see [`DegradeState`]). Each batch is
    /// indexed by its originating commit LSN so receipt barriers can wait for just their prefix.
    pending_flips: Arc<PendingFlips>,
    /// Fail-closed degradation latch + abandoned-batch count (see [`DegradeState`]).
    degrade: Arc<DegradeState>,
    /// Per-engine group transaction fence. It must never be process-global.
    group_fence: GroupFence,
    group_lanes: crate::engine::emission::GroupEmissionLanes,
    /// Group teardowns keyed by group identity. A reload waits for an in-flight release instead of
    /// renewing the dying record or allowing a stale task to drop a replacement under the same name.
    group_releases: Arc<std::sync::Mutex<HashMap<String, Arc<GroupRelease>>>>,
    /// Latched when live replay cannot continue: a missing segment or unrecoverable body-cap breach.
    pub(crate) sequencer_read_failed: Arc<std::sync::atomic::AtomicBool>,
    /// Table schemas shared with the sequencer task and the replication ingestor — the set of
    /// tables the engine can **currently decode and route**, and the single place a drift swaps a
    /// schema (ADR-0005).
    ///
    /// It is a subset of `EngineState::tables` (which is what the engine *knows about*, and what
    /// the reconciler reconciles): a table whose drift is unresolved is removed from here, so its
    /// changes are dropped at the decoder rather than decoded against a schema the engine knows is
    /// stale, while staying in `tables` so it is still reconciled and still gives API callers the
    /// specific "unresolved, retry" refusal rather than "unknown table".
    tables_shared: SharedTables,
    /// Ordered writer for the durable shape catalog (see [`CATALOG_STREAM`]).
    catalog_tx: CatalogWriter,
    /// Shape streams whose retirement (close, then delete — ADR-0007) storage refused, retried in
    /// the background until it lands. See [`crate::engine::retirement`]: the `Dropped` record is the
    /// durable intent, so nothing here is lost by a restart.
    retirements: RetirementQueue,
    /// Durable purge completion barriers keyed by shape id. A retry joins the original teardown,
    /// rather than launching a second completion task or acknowledging before retirement lands.
    purge_barriers: Arc<std::sync::Mutex<HashMap<String, Arc<PurgeBarrier>>>>,
    #[cfg(test)]
    pub(crate) purge_test_hook: Arc<PurgeTestHook>,
    #[cfg(feature = "test-support")]
    pub(crate) group_seed_test_gate: Arc<GroupSeedTestGate>,
    #[cfg(feature = "test-support")]
    pub(crate) group_abort_test_gate: Arc<GroupAbortTestGate>,
    #[cfg(feature = "test-support")]
    pub(crate) group_load_test_gate: Arc<GroupLoadTestGate>,
    #[cfg(feature = "test-support")]
    pub(crate) group_release_test_gate: Arc<GroupReleaseTestGate>,
    /// The segmented change log (ADR-0006): which segment the ingestor appends to, when each
    /// segment began, and the rotation policy. Held by the engine (not just the ingestor) because
    /// the retention sweeper deletes segments and the epoch reset rotates one.
    changes: ChangeLogWriter,
    /// Explicit, caller-named external replay positions. Unlike a page read these are durable
    /// retention claims, so only catalog-confirmed entries are allowed to constrain deletion.
    consumers: Arc<std::sync::Mutex<BTreeMap<String, LogPosition>>>,
    /// Change-log position the sequencer starts from (set by catalog restore before the spawn).
    seq_start: Arc<std::sync::Mutex<LogPosition>>,
    /// The `(lsn, seq)` de-duplication highwater the sequencer starts from, restored with the
    /// checkpoint (ADR-0003). `None` = start de-duplicating from nothing.
    seq_highwater: Arc<std::sync::Mutex<Option<(u64, u64)>>>,
    /// Keeps the change-log reader/checkpointer paused while a durable catalog Resume is incomplete.
    restore_reads_paused: Arc<std::sync::atomic::AtomicBool>,
    /// Per-shape retention lifecycle + last-read instant. A separate sync mutex (not
    /// `EngineState`) so hot read paths can touch it without the async engine lock. Lock order:
    /// when both are held, `state` first, then `lives`; never across `.await`.
    lives: Arc<std::sync::Mutex<HashMap<String, ShapeLife>>>,
    /// Retention policy knobs (see `crate::retention`).
    retention: Arc<RetentionConfig>,
    /// Bounds concurrent dormant replays; each permit covers one replay scan and its page-sized
    /// transient allocations. Shapes joining an in-flight replay wait on its lifecycle channel.
    pub(crate) reactivation_permits: Arc<tokio::sync::Semaphore>,
    /// Cancellation flags for in-flight replay scans, keyed by shape id. A scan that is still
    /// paging for a shape nothing is waiting for any more (a join timeout purged it, the sweeper
    /// evicted it) holds a reactivation permit until it happens to append — which a predicate that
    /// matches nothing in the remaining span never does. Retiring a shape sets its flag; the scan
    /// drops that target at the next page boundary and gives the permit back.
    pub(crate) reactivation_cancels: Arc<std::sync::Mutex<HashMap<String, Arc<std::sync::atomic::AtomicBool>>>>,
    /// Pending same-table/cursor replay requests coalesced into one page scan.
    pub(crate) reactivation_batches:
        Arc<std::sync::Mutex<HashMap<String, Arc<std::sync::Mutex<crate::engine::lifecycle::ReplayBatch>>>>>,
    /// Set once the background retention sweeper has been spawned (lazy, idempotent).
    retention_started: Arc<std::sync::atomic::AtomicBool>,
    /// Set once the background schema reconciler has been spawned (see `engine::drift`).
    reconciler_started: Arc<std::sync::atomic::AtomicBool>,
    /// Per-table serialisation of drift resolutions, plus the set currently running (see
    /// [`drift::Resolving`]). Every trigger runs its own resolution, one at a time per table, and a
    /// shape create on a table being resolved is refused rather than allowed to install against a
    /// schema that is mid-swap.
    resolving: Arc<drift::Resolving>,
    /// Tables with an unresolved-retry task running (coalescing — one task per table). A std lock
    /// so the task's drop guard can clear it without an await.
    retrying: Arc<std::sync::Mutex<HashSet<TableRef>>>,
    /// dbsp arrangement settings (`ELECTRIC_CIRCUITS_DBSP*`), set before `setup_postgres`.
    dbsp_cfg: Arc<std::sync::Mutex<Option<crate::config::DbspConfig>>>,
    /// Large-transaction settings for the ingestor's per-transaction buffer (ADR-0003), set before
    /// `setup_postgres` (which spawns the ingestor). Defaults apply when nothing sets them — the
    /// binary always does, from the boot config.
    txn_cfg: Arc<std::sync::Mutex<crate::txn_buffer::TxnBufferConfig>>,
    /// Deployment projection, separate from physical schema fingerprints and shape SELECT lists.
    pg_columns: Arc<std::collections::BTreeMap<TableRef, Vec<String>>>,
    /// Witnessed tables, with historical column names retained only when configured.
    pg_columns_seen: Arc<std::sync::RwLock<std::collections::BTreeMap<TableRef, HashSet<String>>>>,
    /// The dbsp arrangement layer, once started (see [`crate::arrangements`]).
    arrangements: Arc<std::sync::Mutex<Option<crate::arrangements::Arrangements>>>,
    /// Per-table seed-snapshot gates fencing the arrangement feed (fresh seeds only; empty
    /// after a checkpoint restore, where the highwater does the fencing instead).
    arr_gates: Arc<std::sync::RwLock<HashMap<TableRef, crate::pg::SnapshotGate>>>,
    /// Which replication slot, in which cluster, this engine is bound to — and what to do when that
    /// stops being true (see [`engine::epoch`], ADR-0004).
    epoch: Arc<EpochState>,
    /// Expected event-zero storage binding, installed by the binary before Postgres setup.
    store_bound: Arc<std::sync::OnceLock<crate::store_identity::StoreBound>>,
    /// This Engine's graceful-shutdown state (see [`crate::shutdown`]). Held here — not in a global
    /// — because every part that must join it (the sequencer's select, the ingestor, the `/v1/shape`
    /// live poll, `GET /ready`) already has an `Engine`.
    shutdown: crate::shutdown::ShutdownToken,
    /// Per-Engine-instance nonce for the subscription ids the engine mints for creates that named none
    /// (ADR-0008). The counter alone would not do: the catalog outlives the process, so a restart
    /// would re-mint ids a restored shape still holds.
    sub_nonce: Arc<str>,
}

/// PostgreSQL authority boundary selected when a source Engine is composed.
///
/// The standalone binary preserves its historical self-provisioning behavior.
/// Multi-source hosts instead select `ExternallyManaged` after their trusted
/// control plane has reconciled the exact publication, slot, and identities.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PostgresSetup {
    #[default]
    EngineManaged,
    ExternallyManaged,
}

pub(crate) struct PurgeBarrier {
    dropped_durable: std::sync::atomic::AtomicBool,
    dropped_failed: std::sync::atomic::AtomicBool,
    dropped_notify: tokio::sync::Notify,
    done: std::sync::atomic::AtomicBool,
    notify: tokio::sync::Notify,
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct PurgeTestHook {
    pub(crate) pause_before_remove: std::sync::atomic::AtomicBool,
    pub(crate) before_remove: tokio::sync::Notify,
    pub(crate) release_before_remove: tokio::sync::Notify,
    pub(crate) pause_after_remove: std::sync::atomic::AtomicBool,
    pub(crate) removed: tokio::sync::Notify,
    pub(crate) release: tokio::sync::Notify,
}

#[cfg(feature = "test-support")]
pub struct GroupSeedTestGate {
    armed: std::sync::atomic::AtomicBool,
    pub arrived: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
}

#[cfg(feature = "test-support")]
pub struct GroupAbortTestGate {
    armed: std::sync::atomic::AtomicBool,
    pub arrived: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
    pub finished: tokio::sync::Notify,
}

#[cfg(feature = "test-support")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupLoadAwait {
    BeforeReservation,
    AfterReservation,
    MemberCreate(usize),
    RenewalHealth,
    RenewalStreamTail,
    RenewalSnapshot,
    StreamTail,
    Snapshot,
}

#[cfg(feature = "test-support")]
pub struct GroupLoadTestGate {
    stage: tokio::sync::Mutex<Option<GroupLoadAwait>>,
    pub arrived: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
}

#[cfg(feature = "test-support")]
pub struct GroupReleaseTestGate {
    armed: std::sync::atomic::AtomicBool,
    pub arrived: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
}

#[cfg(feature = "test-support")]
impl Default for GroupAbortTestGate {
    fn default() -> Self {
        Self {
            armed: std::sync::atomic::AtomicBool::new(false),
            arrived: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            finished: tokio::sync::Notify::new(),
        }
    }
}

#[cfg(feature = "test-support")]
impl GroupAbortTestGate {
    pub fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }

    async fn wait_if_armed(&self) -> bool {
        if self.armed.swap(false, Ordering::AcqRel) {
            let notified = self.release.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            self.arrived.notify_one();
            notified.await;
            return true;
        }
        false
    }
}

#[cfg(feature = "test-support")]
impl Default for GroupLoadTestGate {
    fn default() -> Self {
        Self {
            stage: tokio::sync::Mutex::new(None),
            arrived: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        }
    }
}

#[cfg(feature = "test-support")]
impl GroupLoadTestGate {
    pub async fn arm(&self, stage: GroupLoadAwait) {
        *self.stage.lock().await = Some(stage);
    }

    async fn wait_if_armed(&self, stage: GroupLoadAwait) {
        let armed = self.stage.lock().await.as_ref() == Some(&stage);
        if armed {
            *self.stage.lock().await = None;
            let notified = self.release.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            self.arrived.notify_one();
            notified.await;
        }
    }
}

#[cfg(feature = "test-support")]
impl Default for GroupReleaseTestGate {
    fn default() -> Self {
        Self {
            armed: std::sync::atomic::AtomicBool::new(false),
            arrived: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        }
    }
}

#[cfg(feature = "test-support")]
impl GroupReleaseTestGate {
    pub fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }

    async fn wait_if_armed(&self) {
        if self.armed.swap(false, Ordering::AcqRel) {
            let notified = self.release.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            self.arrived.notify_one();
            notified.await;
        }
    }
}

#[cfg(feature = "test-support")]
impl Default for GroupSeedTestGate {
    fn default() -> Self {
        Self {
            armed: std::sync::atomic::AtomicBool::new(false),
            arrived: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        }
    }
}

#[cfg(feature = "test-support")]
impl GroupSeedTestGate {
    pub fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }

    async fn wait_if_armed(&self) {
        if self.armed.swap(false, Ordering::AcqRel) {
            let notified = self.release.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            self.arrived.notify_one();
            notified.await;
        }
    }
}

impl PurgeBarrier {
    fn new() -> Self {
        Self {
            dropped_durable: std::sync::atomic::AtomicBool::new(false),
            dropped_failed: std::sync::atomic::AtomicBool::new(false),
            dropped_notify: tokio::sync::Notify::new(),
            done: std::sync::atomic::AtomicBool::new(false),
            notify: tokio::sync::Notify::new(),
        }
    }

    fn mark_dropped_durable(&self) {
        self.dropped_durable.store(true, Ordering::Release);
        self.dropped_notify.notify_waiters();
    }

    fn mark_dropped_failed(&self) {
        self.dropped_failed.store(true, Ordering::Release);
        self.dropped_notify.notify_waiters();
    }

    async fn wait_dropped_durable(&self) -> std::result::Result<(), catalog::CatalogWriterGone> {
        loop {
            let notified = self.dropped_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.dropped_durable.load(Ordering::Acquire) {
                return Ok(());
            }
            if self.dropped_failed.load(Ordering::Acquire) {
                return Err(catalog::CatalogWriterGone);
            }
            notified.await;
        }
    }

    fn complete(&self) {
        self.done.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    pub(crate) fn is_done(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }

    async fn wait(&self) -> std::result::Result<(), catalog::CatalogWriterGone> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.done.load(Ordering::Acquire) {
                return if self.dropped_failed.load(Ordering::Acquire) {
                    Err(catalog::CatalogWriterGone)
                } else {
                    Ok(())
                };
            }
            notified.await;
        }
    }
}

/// A unit of deferred subquery propagation for the flip propagator (see [`Engine::flip_tx`]).
pub(crate) enum FlipWork {
    /// One tailer envelope's (or one create's replay's) worth of inner-set flips, walked up the
    /// dependency DAG from the flipped nodes.
    Walk {
        work: std::collections::VecDeque<(crate::predicate::SubquerySig, crate::subquery::Flip)>,
        txid: Option<String>,
        /// The originating write's commit lsn, threaded through to the deferred flip's trace event
        /// so it carries the same lsn/txid as the direct-change event that triggered the
        /// propagation — letting the activity log group them as one write (see
        /// `subquery::emit_flip_trace`).
        lsn: Option<String>,
        /// Group stream paths reachable from this walk. Registered before the work is sent so a
        /// following source transaction cannot pass the group fence while this walk is in flight.
        group_paths: Vec<String>,
    },
    /// Work that reached a shape while it was still being created, run against that one shape now
    /// that it is installed (see `SubqueryRegistry::finish_create`). Its dependency walk already
    /// happened, so there is nothing left to walk.
    Deferred {
        commit_lsn: u64,
        shape_id: String,
        work: std::collections::VecDeque<crate::subquery::DeferredShapeWork>,
        group_paths: Vec<String>,
    },
    /// Work that reached a fresh parent NODE while a create was still seeding it, re-derived now
    /// that its set is installed (see `SubqueryRegistry::finish_create`). Unlike `Deferred`, this
    /// one DOES walk: reconciling the node produces flips, and the dependents below it — the
    /// create's own shape included — have not seen them.
    DeferredNode {
        commit_lsn: u64,
        work: std::collections::VecDeque<(crate::predicate::SubquerySig, crate::subquery::DeferredNodeWork)>,
        group_paths: Vec<String>,
    },
}

impl FlipWork {
    fn commit_lsn(&self) -> u64 {
        match self {
            Self::Walk { lsn, .. } => lsn.as_deref().map(crate::pg::lsn_to_u64).unwrap_or(0),
            Self::Deferred { commit_lsn, .. } | Self::DeferredNode { commit_lsn, .. } => *commit_lsn,
        }
    }

    fn group_paths(&self) -> &[String] {
        match self {
            Self::Walk { group_paths, .. }
            | Self::Deferred { group_paths, .. }
            | Self::DeferredNode { group_paths, .. } => group_paths,
        }
    }
}

/// Outstanding propagation and append batches, indexed by their originating commit LSN.
/// Unknown origins use zero, so they hold every marker. Entries disappear after the last
/// batch at that position lands; storage is O(distinct outstanding commit positions).
/// The total and index share one short, non-async lock so receipts never see a partial update.
#[derive(Default)]
pub(crate) struct PendingFlips {
    state: std::sync::Mutex<(i64, BTreeMap<u64, usize>)>,
}

impl PendingFlips {
    fn add(&self, commit_lsn: u64) {
        self.add_all(std::iter::once(commit_lsn));
    }

    /// Reserve an entire handoff before any worker can complete one of its batches. The
    /// receipt check must not observe an empty prefix between two batches of the same create.
    fn add_all(&self, origins: impl IntoIterator<Item = u64>) {
        let mut state = self.state.lock().unwrap();
        for commit_lsn in origins {
            state.0 += 1;
            *state.1.entry(commit_lsn).or_default() += 1;
        }
    }

    fn complete(&self, commit_lsn: u64) {
        let mut state = self.state.lock().unwrap();
        let count = state.1.get_mut(&commit_lsn).expect("completed an untracked flip batch");
        *count -= 1;
        if *count == 0 {
            state.1.remove(&commit_lsn);
        }
        state.0 -= 1;
    }

    fn count(&self) -> i64 {
        self.state.lock().unwrap().0
    }

    fn has_at_or_before(&self, marker_lsn: u64) -> bool {
        self.state.lock().unwrap().1.first_key_value().is_some_and(|(&lsn, _)| lsn <= marker_lsn)
    }
}

/// Everything a tailer needs to route deltas through the subquery layer: the shared registry for
/// the synchronous node-reconcile + outer-emission phases, and the deferral channel + pending
/// counter for flip propagation.
#[derive(Clone)]
struct SubqueryHandle {
    registry: Arc<Mutex<SubqueryRegistry>>,
    flip_tx: mpsc::UnboundedSender<FlipWork>,
    pending_flips: Arc<PendingFlips>,
    degrade: Arc<DegradeState>,
    group_lanes: crate::engine::emission::GroupEmissionLanes,
}

/// Spawn the flip-propagation dispatcher: FlipWork batches run **concurrently**, bounded by a
/// semaphore (`ELECTRIC_CIRCUITS_FLIP_WORKERS`, default 8) — the Postgres round-trips are the
/// dominant cost and are independent across batches. Correctness does not depend on
/// propagation order: membership evaluation happens under the registry lock and the resulting
/// envelopes are **enqueued under that same lock** into per-stream FIFO emission lanes
/// (`engine::emission`), so per-shape append order equals eval order regardless of which
/// worker ran the query-back; absolute per-pk emission makes concurrent re-derivations
/// convergent (see `subquery.rs`).
fn spawn_flip_propagator(
    registry: Arc<Mutex<SubqueryRegistry>>,
    mut rx: mpsc::UnboundedReceiver<FlipWork>,
    pending: Arc<PendingFlips>,
    degrade: Arc<DegradeState>,
    trace_tx: tokio::sync::broadcast::Sender<Arc<String>>,
    group_lanes: crate::engine::emission::GroupEmissionLanes,
    shutdown: crate::shutdown::ShutdownToken,
) {
    let workers: usize =
        std::env::var("ELECTRIC_CIRCUITS_FLIP_WORKERS").ok().and_then(|v| v.parse().ok()).unwrap_or(8).max(1);
    shutdown.clone().spawn_background(async move {
        let sem = Arc::new(tokio::sync::Semaphore::new(workers));
        while let Some(fw) = rx.recv().await {
            let permit = sem.clone().acquire_owned().await.expect("flip semaphore");
            let registry = registry.clone();
            let pending = pending.clone();
            let degrade = degrade.clone();
            let trace_tx = trace_tx.clone();
            let group_lanes = group_lanes.clone();
            shutdown.spawn_background(async move {
                let commit_lsn = fw.commit_lsn();
                let group_paths = fw.group_paths().to_vec();
                // Both arms report `(error, items the batch never got through)`; a deferred batch
                // is treated exactly like a live one, because its effects are just as lost.
                let failed = match fw {
                    FlipWork::Walk { mut work, txid, lsn, .. } => {
                        crate::subquery::propagate_with_retry(&registry, &mut work, txid, lsn, &trace_tx)
                            .await
                            .err()
                            .map(|e| (e, work.len()))
                    }
                    FlipWork::Deferred { shape_id, mut work, .. } => {
                        crate::subquery::propagate_deferred_with_retry(&registry, &shape_id, &mut work)
                            .await
                            .err()
                            .map(|e| (e, work.len()))
                    }
                    FlipWork::DeferredNode { mut work, .. } => {
                        // The walk the re-derivations feed starts empty and is carried across the
                        // retries with them, so an exhausted batch reports both halves of what it
                        // never got through.
                        let mut walk = std::collections::VecDeque::new();
                        crate::subquery::propagate_deferred_node_with_retry(
                            &registry, &mut work, &mut walk, commit_lsn, &trace_tx,
                        )
                        .await
                        .err()
                        .map(|e| (e, work.len() + walk.len()))
                    }
                };
                if let Some((e, unfinished)) = failed {
                    // Fail closed. The inner-set node was reconciled before these query-backs ran,
                    // so nothing will ever re-derive the rows this batch was carrying: the effects
                    // are lost, not delayed. Keep the batch's pending count held — a consumer
                    // gating on `pendingFlips == 0` must never be told everything landed when it
                    // did not — and latch the engine degraded so every membership-bearing route
                    // refuses instead of serving membership the engine knows is wrong.
                    tracing::error!(
                        "subquery flip propagation ABANDONED after {} attempts ({} item(s) unfinished): {e:#}; \
                         membership effects lost; engine degraded",
                        crate::subquery::FLIP_ATTEMPTS,
                        unfinished
                    );
                    degrade.mark();
                    for path in &group_paths {
                        group_lanes.complete_origin(path, commit_lsn);
                    }
                    drop(permit);
                    return;
                }
                // Decremented only after propagation finished enqueueing every resulting
                // batch — each batch carries its own pending increment until it lands, so
                // the barrier never reads zero with effects in flight.
                pending.complete(commit_lsn);
                for path in &group_paths {
                    group_lanes.complete_origin(path, commit_lsn);
                }
                drop(permit);
            });
        }
    });
}

struct EngineState {
    tables: HashMap<TableRef, TableSchema>,
    sequencer: Option<SequencerHandle>,
    shapes: HashMap<String, ShapeRecord>,
    /// Group claims and their private member list live under the same state lock as
    /// ordinary subscription claims, making a group reservation atomic with a competing
    /// single-shape create.
    group_claims: GroupClaims,
    next_shape_id: u64,
    /// Shape sharing. Any two **equal** shapes — same kind and definition (see `shape_signature` /
    /// `agg_signature`: table + canonical predicate + columns + changes-only, or table + predicate +
    /// func + column for aggregates) — share ONE durable stream + ONE routed/standalone/registry entry,
    /// ref-counted, so the engine maintains + appends once for all subscribers instead of once each. A
    /// joiner positions itself with its own snapshot LSN (client-side `< S` drop), so sharing is safe.
    /// Covers plain, subquery, and aggregate shapes. `feed_by_sig`: signature -> shape_id;
    /// `feed_shares`: shape_id -> (sig, refcount).
    feed_by_sig: HashMap<String, String>,
    /// Circuit-served placement per shape id (label like `all` / `static:project_id` /
    /// `dynamic:project_id` / `counts`), plus the arrangement column serving it — feeds the
    /// graph payload so the visualizer can draw pipeline→shape edges.
    circuit_placement: HashMap<String, CircuitPlacement>,
    feed_shares: HashMap<String, FeedShare>,
    /// Per-table **schema generation**, bumped in the same critical section that enumerates a
    /// table's dependents for retirement (`Engine::retire_dependents`). A shape create captures the
    /// generation of every table it touches when it registers and re-checks it before returning
    /// success, so a create that overlapped a drift is rolled back instead of being installed
    /// against a schema that is already gone (ADR-0005) — the same shape as the degradation latch's
    /// two-check pattern (see [`Engine::ensure_create_not_degraded`]).
    schema_gen: HashMap<TableRef, u64>,
    /// Tables whose drift could not be resolved (Postgres unreachable, the catalog read failed, the
    /// `REPLICA IDENTITY FULL` re-assert timed out, or the wire keeps disagreeing with the
    /// catalog). Creates on them are refused with a retryable error and the ingestor decodes
    /// nothing for them; a per-table retry task keeps trying. Never a silent stale-serve.
    unresolved: HashSet<TableRef>,
    /// **Epoch generation**, bumped in the same critical section in which an epoch reset enumerates
    /// the shapes it is about to retire (`Engine::reset_epoch`, ADR-0004). The per-table
    /// `schema_gen` closes the create-overtaken-by-a-drift race; this one closes its whole-engine
    /// twin — a create that registered before the reset's enumeration would otherwise be purged by
    /// it, or (worse) backfill at a snapshot taken before the NEW slot's consistent point and
    /// permanently miss the window between them.
    epoch_gen: u64,
    /// Which shape each live subscription belongs to: `subscription id -> shape id` (ADR-0008).
    ///
    /// The reverse of `feed_shares[*].subs`, maintained by the same three methods
    /// ([`Self::subscribe`], [`Self::unsubscribe`], [`Self::forget_subscriptions`]) so the two can
    /// only move together. It exists for one question a create must answer before it does anything:
    /// **is this subscription id already someone else's?** Re-using one id for a second predicate is
    /// refused (409) rather than silently accepted, because the caller would then hold one name for
    /// two shapes and be unable to release either without ambiguity.
    subs_by_id: HashMap<String, String>,
    /// Counter behind the ids the engine mints for creates that named no subscription. Combined
    /// with a per-Engine-instance nonce (see [`Engine::mint_subscription`]), so a minted id is unique
    /// across restarts too — the catalog outlives the process that wrote it.
    next_minted_sub: u64,
}

/// The generations a create captured for everything that can invalidate it: the schema of each table
/// it depends on, and the engine's epoch.
///
/// A create reads several tables (a subquery shape reads its outer table AND every referenced inner
/// table), and any of them drifting invalidates the whole create — so the whole set travels
/// together and is re-checked as one. The epoch rides along for the same reason and is re-checked in
/// the same place: one closing check, whatever moved.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct SchemaGens {
    tables: Vec<(TableRef, u64)>,
    /// `EngineState::epoch_gen` as it was when this create registered (ADR-0004).
    epoch: u64,
}

impl EngineState {
    /// Capture the current generation of each table, and of the epoch. Call under the state lock, in
    /// the same critical section that registers the create.
    fn capture_gens(&self, tables: &[TableRef]) -> SchemaGens {
        SchemaGens {
            tables: tables.iter().map(|t| (t.clone(), self.schema_gen.get(t).copied().unwrap_or(0))).collect(),
            epoch: self.epoch_gen,
        }
    }

    /// The first table whose generation moved since [`capture_gens`](Self::capture_gens), if any.
    fn drifted_since(&self, gens: &SchemaGens) -> Option<TableRef> {
        gens.tables.iter().find(|(t, g)| self.schema_gen.get(t).copied().unwrap_or(0) != *g).map(|(t, _)| t.clone())
    }

    /// Did an epoch reset run since [`capture_gens`](Self::capture_gens)?
    fn epoch_reset_since(&self, gens: &SchemaGens) -> bool {
        self.epoch_gen != gens.epoch
    }

    /// The first of `tables` whose schema is unresolved, if any.
    fn first_unresolved(&self, tables: &[TableRef]) -> Option<TableRef> {
        tables.iter().find(|t| self.unresolved.contains(*t)).cloned()
    }

    // --- subscriptions (ADR-0008) ---------------------------------------------------------------

    /// Which shape holds this subscription id right now, if any.
    fn subscription_owner(&self, sub: &str) -> Option<&String> {
        self.subs_by_id.get(sub)
    }

    /// Add (or RENEW) a subscription on a shape's share entry. Renewing is the same call: an id the
    /// shape already holds keeps its place in the set and only moves its lease forward.
    ///
    /// Returns whether this was a NEW claim — the caller uses it to decide between a durable
    /// `Joined` (a claim the client is about to be told about) and a queued one (a renewal, which
    /// promises nothing new).
    fn subscribe(&mut self, shape: &str, sub: String, at: u64) -> bool {
        let Some(share) = self.feed_shares.get_mut(shape) else { return false };
        let fresh = match share.subs.get_mut(&sub) {
            Some(lease) => {
                *lease = (*lease).max(at);
                false
            }
            None => {
                share.subs.insert(sub.clone(), at);
                true
            }
        };
        self.subs_by_id.insert(sub, shape.to_string());
        fresh
    }

    /// Release one subscription. Returns whether it was actually held — a release for an id the
    /// shape does not hold changes nothing and writes nothing, which is exactly what makes a
    /// client's retried `DELETE` safe.
    fn unsubscribe(&mut self, shape: &str, sub: &str) -> bool {
        let removed = self.feed_shares.get_mut(shape).is_some_and(|s| s.subs.remove(sub).is_some());
        if removed {
            // Only if it still points HERE: an id released from one shape and immediately claimed
            // on another must keep the newer owner.
            if self.subs_by_id.get(sub).is_some_and(|owner| owner == shape) {
                self.subs_by_id.remove(sub);
            }
        }
        removed
    }

    /// The legacy anonymous release (`DELETE /shapes/{id}` with no `subscription`): drop ONE
    /// subscription without being told which.
    ///
    /// Engine-minted ids go first, oldest lease first, and only then caller-named ones — a caller
    /// that never learned the protocol must not be able to steal the claim of one that did. It is
    /// still not retry-safe (nothing identifies which claim the caller meant), which is why the
    /// route documents it as legacy.
    ///
    /// (A shape created with `share = false` has no share entry at all, so it holds no tracked
    /// subscriptions and there is nothing here to release — as before ADR-0008. Nothing in the tree
    /// creates one.)
    fn unsubscribe_anonymous(&mut self, shape: &str) -> Option<String> {
        let victim = {
            let share = self.feed_shares.get(shape)?;
            let pick = |minted: bool| {
                share
                    .subs
                    .iter()
                    .filter(|(id, _)| id.starts_with(MINTED_SUB_PREFIX) == minted)
                    // Oldest lease first, and a tie broken by the minted id's COUNTER rather than
                    // its spelling: `~n-10` is younger than `~n-9`, which a string compare has
                    // backwards. Ties only arise within one wall-clock second, so this is about
                    // being predictable rather than about correctness.
                    .min_by_key(|(id, at)| (**at, minted_seq(id), (*id).clone()))
                    .map(|(id, _)| id.clone())
            };
            pick(true).or_else(|| pick(false))?
        };
        self.unsubscribe(shape, &victim).then_some(victim)
    }

    /// Forget every subscription a shape held — the removal half of a purge/eviction/rollback,
    /// where the share entry itself goes. Without it the id index would keep pointing at a shape
    /// that no longer exists and refuse a perfectly good re-subscription with a 409.
    fn forget_subscriptions(&mut self, shape: &str) {
        if let Some(share) = self.feed_shares.get(shape) {
            let ids: Vec<String> = share.subs.keys().cloned().collect();
            for id in ids {
                if self.subs_by_id.get(&id).is_some_and(|owner| owner == shape) {
                    self.subs_by_id.remove(&id);
                }
            }
        }
    }

    /// Publish the live-subscription gauge. A gauge and not a counter: it describes how many claims
    /// are pinning shapes right now, which is the number an operator watching a shape that will not
    /// go dormant needs.
    fn publish_subscription_gauge(&self, metrics: &crate::metrics::Metrics) {
        let live: usize = self.feed_shares.values().map(FeedShare::refcount).sum();
        metrics.subscriptions_live.store(live as u64, std::sync::atomic::Ordering::Relaxed);
    }

    /// Every subscription whose lease has not been renewed within `window`, as
    /// `(shape id, subscription id)` (ADR-0008). Pure bookkeeping — the caller records the `Left`s
    /// and the retention sweep then treats the shape exactly as it would after an explicit release.
    ///
    /// `window == 0` disables leases entirely, together with dormancy: an engine that never parks
    /// a shape has no use for the liveness signal, and expiring subscriptions under it would only
    /// break sharing.
    ///
    /// The comparison is **strictly greater**, deliberately. The lease clock is wall-clock SECONDS,
    /// so an age of `n` means "somewhere in `[n, n+1)` of real time"; lapsing at `>=` would end a
    /// one-second window after as little as a few milliseconds, which is not the window the operator
    /// asked for. A client renewing at any fraction of its window is unaffected either way — this
    /// decides only the boundary case, and the boundary belongs to the client.
    fn lapsed_subscriptions(&self, window: std::time::Duration, now: u64) -> Vec<(String, String)> {
        if window.is_zero() {
            return Vec::new();
        }
        let secs = window.as_secs().max(1);
        let mut out = Vec::new();
        for (shape, share) in &self.feed_shares {
            for (sub, at) in &share.subs {
                if now.saturating_sub(*at) > secs {
                    out.push((shape.clone(), sub.clone()));
                }
            }
        }
        out.sort();
        out
    }
}

struct FeedShare {
    sig: String,
    /// The shape's **live subscriptions** (ADR-0008): `subscription id -> the wall-clock second its
    /// lease was last renewed at`. The refcount is this set's size, and it is a set precisely so
    /// that repeating a create or a release is one claim, not two.
    ///
    /// The lease clock is deliberately WALL CLOCK, not the `Instant` the rest of retention uses: a
    /// lease has to survive a restart, and only a wall-clock second can be written to the catalog
    /// and read back as an age. (`last_read` stays an `Instant` — it is process-local and must not
    /// move when the system clock does.)
    subs: std::collections::BTreeMap<String, u64>,
    /// Creation outcome, observed by joiners (see [`ShareOutcome`]).
    ready: tokio::sync::watch::Receiver<ShareOutcome>,
}

impl FeedShare {
    /// Live subscriptions — what the retention sweep calls the refcount.
    fn refcount(&self) -> usize {
        self.subs.len()
    }
}

/// The prefix of an **engine-minted** subscription id — a pure MARKER, not a namespace the engine
/// polices (`http::validate_new_subscription`).
///
/// A create that names no subscription still gets one (it is in the response, and the caller can
/// renew or release with it), but it is marked as un-named so the legacy anonymous
/// `DELETE /shapes/{id}` — which has no id to go on — releases one of THOSE first rather than
/// stealing an identified subscriber's claim.
///
/// A caller MAY name an id with this prefix; the engine does not check whether it minted it. All
/// such a caller achieves is making its own claim the expendable one — see
/// `http::validate_new_subscription`.
pub(crate) const MINTED_SUB_PREFIX: char = '~';

/// The counter out of an engine-minted id (`~<nonce>-<n>` -> `n`); `None` for anything else, which
/// sorts first. Used only to break a tie between two claims minted in the same second.
fn minted_seq(id: &str) -> Option<u64> {
    id.rsplit_once('-')?.1.parse().ok()
}

/// What a shared shape's creator publishes to the joiners waiting on it.
///
/// Typed rather than a bare `bool` because the REASON travels with it: a create refused because the
/// engine degraded must give every joiner the same typed [`Degraded`] refusal the creator returns
/// (503), not a generic initialization failure (500) — identical requests from identical clients
/// cannot be allowed to disagree about why the engine said no.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ShareOutcome {
    /// The creator's backfill/registration is still in flight.
    Pending,
    /// The shape is live: its snapshot is readable.
    Ready,
    /// Creation failed (the entry is removed; joiners must error, not return a dead stream).
    Failed,
    /// Creation failed because its snapshot could not settle ([`crate::pg::SnapshotUnsettled`]).
    /// Distinct from [`Self::Failed`] so a joiner answers the creator's retryable 503, not a 500.
    Unsettled,
    /// The create overlapped a degradation and refused (see [`Engine::ensure_create_not_degraded`]).
    Degraded,
    /// The creator's closing re-check after its catalog durability wait found the shape retired
    /// underneath it (see [`Engine::recheck_after_durability`]). Distinct from [`Self::Failed`]
    /// because the creator does not give up on that: it redoes the create. A joiner must reach the
    /// same conclusion — it arrives typed [`CreateRaced`], so the joiner's own attempt is redone
    /// too. Reported as `Failed` it would answer 500 while the creator quietly succeeded on its
    /// next attempt, which is two identical requests disagreeing about the same outcome.
    Raced,
}

impl ShareOutcome {
    /// What joiners are told when the creator's work failed with `error`.
    pub(crate) fn failed_by(error: &anyhow::Error) -> Self {
        if error.downcast_ref::<crate::pg::SnapshotUnsettled>().is_some() {
            ShareOutcome::Unsettled
        } else {
            ShareOutcome::Failed
        }
    }
}

/// Wait until a shared shape's creator reports the shape live (or failed). Joining before the
/// backfill lands would hand the caller a stream whose snapshot isn't readable yet.
async fn await_share_ready(mut rx: tokio::sync::watch::Receiver<ShareOutcome>, id: &str) -> Result<()> {
    loop {
        let state = *rx.borrow();
        match state {
            ShareOutcome::Ready => return Ok(()),
            // The creator's own refusal, verbatim: the HTTP layer downcasts it to 503 exactly as
            // it does for the creator's error.
            ShareOutcome::Degraded => return Err(anyhow::Error::new(Degraded)),
            // Likewise typed, so this joiner's attempt is redone exactly as the creator's is.
            ShareOutcome::Raced => {
                return Err(anyhow::Error::new(CreateRaced(format!(
                    "shared shape '{id}' was retired during its creator's catalog durability wait"
                ))));
            }
            ShareOutcome::Failed => bail!("shared shape '{id}' failed to initialize; retry the create"),
            ShareOutcome::Unsettled => {
                return Err(anyhow::Error::new(crate::pg::SnapshotUnsettled {
                    cause: crate::pg::UnsettledCause::Shared,
                    pending: Vec::new(),
                })
                .context(format!("shared shape '{id}' failed to initialize")));
            }
            ShareOutcome::Pending => {
                if rx.changed().await.is_err() {
                    bail!("shared shape '{id}' creator died before completing; retry the create");
                }
            }
        }
    }
}

/// Canonical signature for feed sharing: table + serialized predicate + sorted projection indices.
/// Two subset feeds with an equal signature are interchangeable and share one stream.
/// Order-insensitive predicate canonicalization (same form used for subquery-node sharing), so
/// `a AND b` and `b AND a` collapse to one shape.
fn canon_where(where_: &Option<PredicateJson>) -> String {
    where_.as_ref().map(crate::predicate::canonical_pred).unwrap_or_default()
}

/// The coarse engine column type as a stable string for the schema endpoint's JSON.
fn col_type_str(ty: crate::schema::ColumnType) -> &'static str {
    use crate::schema::ColumnType::*;
    match ty {
        Int => "int",
        Text => "text",
        Bool => "bool",
        Float => "float",
    }
}

fn canon_cols(out_cols: &Option<Arc<Vec<usize>>>) -> String {
    out_cols
        .as_ref()
        .map(|v| {
            let mut idx = v.as_ref().clone();
            idx.sort_unstable();
            idx.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(",")
        })
        .unwrap_or_default()
}

/// The sharing key for a **row shape** (materialized or changes-only feed, plain or subquery). Two
/// shapes are interchangeable — and so share one maintained stream — iff these all match. `changes_only`
/// is part of the key: a backfilled shape and a no-backfill feed over the same rows are NOT the same
/// stream.
fn shape_signature(
    table: &TableRef,
    where_: &Option<PredicateJson>,
    out_cols: &Option<Arc<Vec<usize>>>,
    changes_only: bool,
) -> String {
    format!("shape\u{1f}{}\u{1f}{table}\u{1f}{}\u{1f}{}", changes_only, canon_where(where_), canon_cols(out_cols))
}

/// The sharing key for an **aggregation shape**: table + predicate + function + column. Namespaced so it
/// never collides with a row shape's key.
fn agg_signature(table: &TableRef, where_: &Option<PredicateJson>, func: &AggFn, col_idx: Option<usize>) -> String {
    format!("agg\u{1f}{table}\u{1f}{}\u{1f}{:?}\u{1f}{:?}", canon_where(where_), func, col_idx)
}

/// Broadcast a graph-lifecycle event on the trace channel (zero cost with no subscribers).
fn trace_lifecycle(tx: &tokio::sync::broadcast::Sender<Arc<String>>, ev: crate::trace::GraphLifecycle) {
    if tx.receiver_count() == 0 {
        return;
    }
    if let Ok(json) = serde_json::to_string(&ev) {
        let _ = tx.send(Arc::new(json));
    }
}

/// The graph/trace node id of a family router: `family:<table>:<col,col>` (canonical `schema.name`
/// table + column NAMES, matching
/// the hop ids `process_envelope` emits and the ids the visualizer renders).
fn family_node_id(ts: &TableSchema, key_cols: &[usize]) -> String {
    let cols = key_cols
        .iter()
        .map(|i| ts.columns.get(*i).map(|(n, _)| n.clone()).unwrap_or_else(|| format!("col{i}")))
        .collect::<Vec<_>>()
        .join(",");
    format!("family:{}:{cols}", ts.table)
}

/// Shape id (`s<N>`) from its stream path (`shape/s<N>`) — the key `emitted` counters are kept by.
fn sid_of_path(stream_path: &str) -> &str {
    stream_path.strip_prefix("shape/").unwrap_or(stream_path)
}

impl Engine {
    pub(crate) fn group_stream_path(&self) -> String {
        format!("shape/group-{}-{}", self.sub_nonce, uuid::Uuid::new_v4())
    }
    /// Construct a library engine after checking store bindings across every catalog page.
    /// Fails before engine tasks or control surfaces exist if the read or binding check fails.
    /// Cancellation during validation releases the current page and starts no engine work.
    pub async fn new(admission: StoreAdmission) -> Result<Self> {
        let bytes = Self::check_catalog_binding(&admission.ds, &admission.binding).await?;
        let engine =
            Self::new_inner(admission.ds, None, Some(admission.binding), PostgresSetup::EngineManaged, None, None);
        engine.metrics.catalog_bytes.store(bytes, Ordering::Relaxed);
        Ok(engine)
    }

    /// Engine in Postgres mode: data lives in Postgres, ingested via logical replication and read
    /// back for backfill. Call [`setup_postgres`](Self::setup_postgres) before serving.
    pub fn new_pg(admission: StoreAdmission, pg_url: String) -> Self {
        Self::new_pg_with_setup(admission, pg_url, PostgresSetup::EngineManaged)
    }

    /// Engine in Postgres mode where a trusted control plane owns DDL.
    /// `setup_postgres` validates the exact existing publication and replica
    /// identity rather than trying to broaden the Engine's database grants.
    pub fn new_pg_with_setup(admission: StoreAdmission, pg_url: String, setup: PostgresSetup) -> Self {
        let e = Self::new_inner(admission.ds, Some(pg_url), Some(admission.binding), setup, None, None);
        // Postgres mode starts `waiting` until the connection + introspection + slot + ingest are up.
        e.health.store(HEALTH_WAITING, std::sync::atomic::Ordering::Relaxed);
        e
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn new_for_in_process_test(ds: DsClient) -> Self {
        Self::new_inner(ds, None, None, PostgresSetup::EngineManaged, None, None)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn new_pg_for_in_process_test(ds: DsClient, pg_url: String) -> Self {
        Self::new_pg_for_in_process_test_with_setup(ds, pg_url, PostgresSetup::EngineManaged)
    }

    /// Test-only Postgres constructor with an explicit DDL authority boundary.
    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub fn new_pg_for_in_process_test_with_setup(ds: DsClient, pg_url: String, setup: PostgresSetup) -> Self {
        let e = Self::new_inner(ds, Some(pg_url), None, setup, None, None);
        e.health.store(HEALTH_WAITING, std::sync::atomic::Ordering::Relaxed);
        e
    }

    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub fn new_supervised_for_test(notify: Arc<tokio::sync::Notify>) -> Self {
        Self::new_inner(
            DsClient::new_for_in_process_test("http://127.0.0.1:1"),
            None,
            None,
            PostgresSetup::EngineManaged,
            None,
            Some(notify),
        )
    }

    /// Validate the complete catalog before exposing a library-mode engine as ready. Postgres
    /// boot performs this fold during epoch validation instead, after event-zero admission.
    pub async fn boot_library(admission: StoreAdmission, config: &crate::config::Config) -> Result<Self> {
        Self::new_with_config(admission, None, PostgresSetup::EngineManaged, config).await
    }

    /// Construct using resolved storage/pool settings. Library mode checks bindings across all catalog
    /// pages before construction; errors/cancellation start no engine work. Postgres mode remains
    /// waiting until `setup_postgres` validates the catalog and completes boot.
    pub async fn new_with_config(
        mut admission: StoreAdmission,
        pg_url: Option<String>,
        setup: PostgresSetup,
        config: &crate::config::Config,
    ) -> Result<Self> {
        let bytes = if pg_url.is_none() {
            admission.ds = admission.ds.with_memory_budget(config);
            Some(Self::check_catalog_binding(&admission.ds, &admission.binding).await?)
        } else {
            None
        };
        let engine = Self::new_configured(admission, pg_url, setup, config, None);
        if let Some(bytes) = bytes {
            engine.metrics.catalog_bytes.store(bytes, Ordering::Relaxed);
        }
        Ok(engine)
    }

    pub(crate) fn new_supervised(
        admission: StoreAdmission,
        pg_url: Option<String>,
        setup: PostgresSetup,
        config: &crate::config::Config,
        restart_notify: Arc<tokio::sync::Notify>,
    ) -> Self {
        Self::new_configured(admission, pg_url, setup, config, Some(restart_notify))
    }

    fn new_configured(
        admission: StoreAdmission,
        pg_url: Option<String>,
        setup: PostgresSetup,
        config: &crate::config::Config,
        restart_notify: Option<Arc<tokio::sync::Notify>>,
    ) -> Self {
        let postgres = pg_url.is_some();
        if !postgres && !config.pg_columns.is_empty() {
            tracing::warn!("ELECTRIC_CIRCUITS_PG_COLUMNS is ignored in library mode");
        }
        let engine =
            Self::new_inner(admission.ds, pg_url, Some(admission.binding), setup, Some(config), restart_notify);
        if postgres {
            engine.health.store(HEALTH_WAITING, Ordering::Relaxed);
        }
        engine
    }

    fn new_inner(
        ds: DsClient,
        pg_url: Option<String>,
        binding: Option<crate::store_identity::StoreBound>,
        postgres_setup: PostgresSetup,
        config: Option<&crate::config::Config>,
        restart_notify: Option<Arc<tokio::sync::Notify>>,
    ) -> Self {
        let store_bound = Arc::new(std::sync::OnceLock::new());
        if let Some(binding) = binding {
            store_bound.set(binding).expect("fresh store binding proof");
        }
        let ds = match config {
            Some(config) => ds.with_memory_budget(config),
            None => ds,
        };
        let mut metrics = crate::metrics::Metrics::new(
            config.map_or_else(|| crate::config::stack_id().to_owned(), |c| c.stack_id.clone()),
            restart_notify.is_some(),
        );
        metrics.memory_budget = ds.memory_budget().clone();
        let metrics = Arc::new(metrics);
        let memory_gauges = Arc::new(crate::mem::Gauges::default());
        let source_id = restart_notify.as_ref().map(|_| metrics.stack_id.clone());
        let telemetry = crate::mem::init_otel(metrics.clone(), memory_gauges.clone(), source_id.as_deref());
        let shutdown = crate::shutdown::ShutdownToken::with_metrics(restart_notify, metrics.clone());
        let ds = ds.with_shutdown(shutdown.clone());
        let pg_pool = pg_url.as_ref().map(|url| {
            crate::pg::Pool::owned(
                url.clone(),
                config.map_or_else(crate::pg::configured_pool_size, |c| c.db_pool_size),
                shutdown.clone(),
            )
        });
        let membership = match config {
            Some(config) => crate::subq_circuit::MembershipCircuit::start_config(&config.subq_storage),
            None => crate::subq_circuit::MembershipCircuit::start(),
        }
        .expect("membership circuit failed to start");
        let subqueries = Arc::new(Mutex::new(SubqueryRegistry::with_pool(
            ds.clone(),
            pg_url.clone(),
            pg_pool.clone(),
            membership.clone(),
        )));
        let trace_tx = tokio::sync::broadcast::channel(crate::trace::CHANNEL_CAP).0;
        let (flip_tx, flip_rx) = mpsc::unbounded_channel();
        let pending_flips = Arc::new(PendingFlips::default());
        let degrade = DegradeState::new();
        let group_fence = GroupFence::default();
        let group_lanes = emission::GroupEmissionLanes::new(ds.clone(), shutdown.clone(), pending_flips.clone());
        // Ordered emission lanes for subquery-shape appends (network out from under the
        // registry lock; per-stream FIFO keeps append order = eval order). They share the
        // pendingFlips counter so the convergence barrier covers queued batches.
        let lanes = emission::EmissionLanes::spawn(
            ds.clone(),
            std::env::var("ELECTRIC_CIRCUITS_EMIT_LANES").ok().and_then(|v| v.parse().ok()).unwrap_or(8),
            pending_flips.clone(),
            shutdown.clone(),
        );
        subqueries.try_lock().expect("fresh registry").set_lanes(lanes);
        subqueries.try_lock().expect("fresh registry").set_group_lanes(group_lanes.clone());
        spawn_flip_propagator(
            subqueries.clone(),
            flip_rx,
            pending_flips.clone(),
            degrade.clone(),
            trace_tx.clone(),
            group_lanes.clone(),
            shutdown.clone(),
        );
        // Created before the writer so the writer can register a shutdown party while it is
        // retrying an append (see `spawn_catalog_writer`).
        let catalog_tx = spawn_catalog_writer(ds.clone(), shutdown.clone(), pg_url.is_some());
        let retirements = spawn_retirement_queue(ds.clone(), catalog_tx.clone(), shutdown.clone());
        // The change log's writer records every rotation in the durable catalog, so a restart knows
        // which segment is current (and when each one began, for the retain window).
        let changes = ChangeLogWriter::new(
            ds.clone(),
            Arc::new(ChangesState::default()),
            crate::changelog::ChangeLogConfig::from_env(),
            {
                let catalog_tx = catalog_tx.clone();
                Arc::new(move |segment, at| catalog_tx.send(CatalogEvent::ChangesRotated { segment, at }))
            },
        );
        let engine = Engine {
            metrics,
            memory_gauges,
            telemetry: Arc::new(std::sync::Mutex::new(Some(Arc::new(telemetry)))),
            pg_pool,
            publish_generated: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            electric_handles: Arc::new(crate::electric::HandleRegistry::default()),
            close_result: Arc::new(Mutex::new(None)),
            membership,
            ds,
            state: Arc::new(Mutex::new(EngineState {
                tables: HashMap::new(),
                sequencer: None,
                shapes: HashMap::new(),
                group_claims: GroupClaims::default(),
                next_shape_id: 1,
                feed_by_sig: HashMap::new(),
                feed_shares: HashMap::new(),
                circuit_placement: HashMap::new(),
                schema_gen: HashMap::new(),
                unresolved: HashSet::new(),
                epoch_gen: 0,
                subs_by_id: HashMap::new(),
                next_minted_sub: 1,
            })),
            pg_url,
            postgres_setup,
            repl_lsn: Arc::new(std::sync::Mutex::new("0/0".to_string())),
            repl_sync: Arc::new(std::sync::atomic::AtomicI64::new(0)),
            replicator_started: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            // Library mode: no Postgres to wait on, so report `active` immediately.
            health: Arc::new(std::sync::atomic::AtomicU8::new(HEALTH_ACTIVE)),
            managed_deployment: Arc::new(std::sync::Mutex::new(None)),
            managed_wakeup: Arc::new(tokio::sync::Notify::new()),
            control_admission: ControlAdmission::new(),
            source_receipts: Arc::new(std::sync::Mutex::new(HashMap::new())),
            last_source_receipt: Arc::new(std::sync::Mutex::new(None)),
            source_receipt_progress: Arc::new(std::sync::Mutex::new(SourceReceiptProgress::default())),
            runtime_receipts: Arc::new(std::sync::Mutex::new(RuntimeReceipts::default())),
            subqueries,
            subquery_init: Arc::new(SubqueryAdmission::default()),
            trace_tx,
            flip_tx,
            pending_flips,
            degrade,
            group_fence,
            group_lanes,
            group_releases: Arc::new(std::sync::Mutex::new(HashMap::new())),
            sequencer_read_failed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            tables_shared: Arc::new(std::sync::RwLock::new(HashMap::new())),
            catalog_tx,
            retirements,
            purge_barriers: Arc::new(std::sync::Mutex::new(HashMap::new())),
            #[cfg(test)]
            purge_test_hook: Arc::new(PurgeTestHook::default()),
            #[cfg(feature = "test-support")]
            group_seed_test_gate: Arc::new(GroupSeedTestGate::default()),
            #[cfg(feature = "test-support")]
            group_abort_test_gate: Arc::new(GroupAbortTestGate::default()),
            #[cfg(feature = "test-support")]
            group_load_test_gate: Arc::new(GroupLoadTestGate::default()),
            #[cfg(feature = "test-support")]
            group_release_test_gate: Arc::new(GroupReleaseTestGate::default()),
            changes,
            consumers: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            seq_start: Arc::new(std::sync::Mutex::new(LogPosition::start())),
            seq_highwater: Arc::new(std::sync::Mutex::new(None)),
            restore_reads_paused: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            lives: Arc::new(std::sync::Mutex::new(HashMap::new())),
            retention: Arc::new(RetentionConfig::from_env()),
            reactivation_permits: Arc::new(tokio::sync::Semaphore::new(
                std::env::var("ELECTRIC_CIRCUITS_REACTIVATION_CONCURRENCY")
                    .ok()
                    .and_then(|v| v.parse::<usize>().ok())
                    .unwrap_or(2)
                    .max(1),
            )),
            reactivation_cancels: Arc::new(std::sync::Mutex::new(HashMap::new())),
            reactivation_batches: Arc::new(std::sync::Mutex::new(HashMap::new())),
            retention_started: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            reconciler_started: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            resolving: Arc::new(drift::Resolving::default()),
            retrying: Arc::new(std::sync::Mutex::new(HashSet::new())),
            dbsp_cfg: Arc::new(std::sync::Mutex::new(None)),
            txn_cfg: Arc::new(std::sync::Mutex::new(crate::txn_buffer::TxnBufferConfig::default())),
            pg_columns: Arc::new(config.map(|c| c.pg_columns.clone()).unwrap_or_default()),
            pg_columns_seen: Arc::default(),
            arrangements: Arc::new(std::sync::Mutex::new(None)),
            arr_gates: Arc::new(std::sync::RwLock::new(HashMap::new())),
            epoch: EpochState::new(),
            store_bound,
            shutdown,
            sub_nonce: crate::engine::catalog::process_nonce().into(),
        };
        // The streams client must be able to ask the engine whether a terminal append answer is
        // real before a live shape's batch is discarded (see `Engine::install_gone_reconciler`).
        engine.install_gone_reconciler();
        engine.ensure_degrade_reaper();
        crate::electric::ensure_evictor(&engine);
        crate::mem::spawn_sampler(engine.clone(), Duration::from_millis(500));
        engine
    }

    /// Emit this Engine's shape counts over the shared StatsD transport.
    pub fn spawn_statsd_shape_sampler(&self, period: Duration) {
        if !crate::statsd::enabled() {
            return;
        }
        let engine = self.clone();
        self.shutdown.spawn_background(async move {
            loop {
                let (total, indexed, unindexed) = crate::mem::published_shape_counts(&engine.memory_gauges);
                engine.metrics.emitter().shape_gauges(total, indexed, unindexed);
                tokio::select! { _ = engine.shutdown.wait() => break, _ = tokio::time::sleep(period) => {} }
            }
        });
    }

    pub(crate) fn settle_waits_active(&self) -> u64 {
        self.pg_pool.as_ref().map_or(0, |pool| pool.sequenced().settle_waits_active())
    }
    pub(crate) fn settle_stats_json(&self) -> serde_json::Value {
        self.pg_pool
            .as_ref()
            .map_or_else(|| crate::pg::SequencedXids::default().stats_json(), |pool| pool.sequenced().stats_json())
    }

    /// This Engine's Prometheus series; an already closed Engine has no registry.
    pub fn prometheus_text(&self) -> String {
        self.telemetry.lock().unwrap().as_ref().map_or_else(String::new, |telemetry| telemetry.prometheus_text())
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn provider_alive_probe_for_test(&self) -> impl Fn() -> bool + Send + Sync + use<> {
        let weak = self.telemetry.lock().unwrap().as_ref().map(Arc::downgrade);
        move || weak.as_ref().is_some_and(|weak| weak.strong_count() != 0)
    }

    /// This Engine's shutdown token — close or the standalone signal handler flips it, and every
    /// long-running part of the engine joins it (see [`crate::shutdown`]).
    pub fn shutdown_token(&self) -> crate::shutdown::ShutdownToken {
        self.shutdown.clone()
    }

    /// Stop this Engine in either hosting mode. Safe to call repeatedly; all clones share completion.
    pub async fn close(&self, grace: std::time::Duration) -> crate::shutdown::ShutdownOutcome {
        use crate::shutdown::ShutdownOutcome;
        let mut result = self.close_result.lock().await;
        if let Some(outcome) = *result {
            return outcome;
        }
        self.shutdown.begin();
        self.close_control_admission();
        let remaining = || grace.saturating_sub(self.shutdown.elapsed().unwrap_or_default());
        let mut outcome = if self.shutdown.wait_for_parties(remaining()).await {
            ShutdownOutcome::Complete
        } else {
            tracing::warn!(parties = ?self.shutdown.outstanding(), "Engine close exceeded graceful drain");
            ShutdownOutcome::Forced
        };
        // Handles are volatile; durable subscriptions lapse by lease after restart.
        crate::electric::close_handles(self);
        if !self.drain_catalog(remaining().min(crate::shutdown::CATALOG_DRAIN)).await
            && outcome == ShutdownOutcome::Complete
        {
            outcome = ShutdownOutcome::CatalogIncomplete;
        }
        self.membership.shutdown().await;
        let arrangements = self.arrangements.lock().unwrap().take();
        if let Some(arrangements) = arrangements {
            arrangements.shutdown().await;
        }
        if let Some(pool) = &self.pg_pool {
            pool.close();
        }
        self.shutdown.join_tasks().await;
        for (_, barrier) in std::mem::take(&mut *self.purge_barriers.lock().unwrap()) {
            barrier.mark_dropped_failed();
            barrier.complete();
        }
        self.ds.clear_gone_reconciler();
        let telemetry = self.telemetry.lock().unwrap().take();
        if let Some(telemetry) = telemetry {
            if let Err(error) = tokio::task::spawn_blocking(move || telemetry.shutdown()).await {
                tracing::warn!(%error, "Engine telemetry shutdown task failed");
                outcome = ShutdownOutcome::Forced;
            }
        }
        *result = Some(outcome);
        outcome
    }

    /// Observe state ownership without keeping the Engine alive.
    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn state_alive_probe_for_test(&self) -> impl Fn() -> bool + Send + Sync + use<> {
        let state = Arc::downgrade(&self.state);
        move || state.upgrade().is_some()
    }

    /// Install the managed ownership gate before the HTTP server is exposed or Postgres setup runs.
    pub fn configure_managed_deployment(
        &self,
        config: crate::deployment::ManagedDeploymentConfig,
        slot: &str,
    ) -> Result<()> {
        let binding = self.store_bound.get().context("managed deployment requires an admitted StoreBound")?;
        let coordination_key = crate::deployment::coordination_key(binding, slot);
        *self.managed_deployment.lock().unwrap() =
            Some(ManagedDeploymentState { config, coordination_key, role: ManagedRole::Standby, ownership: None });
        self.close_control_admission();
        Ok(())
    }

    fn managed_status(&self) -> Option<(ManagedRole, Option<crate::deployment::Ownership>)> {
        self.managed_deployment.lock().unwrap().as_ref().map(|state| (state.role, state.ownership.clone()))
    }

    pub fn managed_deployment_enabled(&self) -> bool {
        self.managed_deployment.lock().unwrap().is_some()
    }

    /// Recovery controls may run on the persisted active owner even while its epoch is broken.
    /// Standby/quiescing processes must never reopen admission or reset the active writer's slot.
    pub fn managed_recovery_owner(&self) -> bool {
        self.managed_deployment.lock().unwrap().as_ref().is_none_or(|state| state.role == ManagedRole::Active)
    }

    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub fn install_managed_role_for_test(&self, active: bool) {
        *self.managed_deployment.lock().unwrap() = Some(ManagedDeploymentState {
            config: crate::deployment::ManagedDeploymentConfig {
                revision: "test-revision".into(),
                initial_active: false,
            },
            coordination_key: "a".repeat(64),
            role: if active { ManagedRole::Active } else { ManagedRole::Standby },
            ownership: None,
        });
    }

    /// Managed traffic may flow only after the owner has completed the normal engine boot.
    /// Claiming the database row is deliberately not enough: catalog restore, slot ownership, and
    /// ingestor startup still happen after that claim.
    pub fn managed_public_ready(&self) -> bool {
        !self.managed_deployment_enabled() || self.readiness_status() == "active"
    }

    pub fn managed_wakeup(&self) -> Arc<tokio::sync::Notify> {
        self.managed_wakeup.clone()
    }

    async fn claim_managed_ownership(&self, client: &tokio_postgres::Client) -> Result<()> {
        let (config, key) = {
            let guard = self.managed_deployment.lock().unwrap();
            let Some(state) = guard.as_ref() else { return Ok(()) };
            (state.config.clone(), state.coordination_key.clone())
        };
        match crate::deployment::claim_or_observe(client, &config, &key).await? {
            crate::deployment::Claim::Active(ownership) => {
                let mut guard = self.managed_deployment.lock().unwrap();
                let state = guard.as_mut().expect("managed state stays installed");
                state.role = ManagedRole::Active;
                state.ownership = Some(ownership);
                Ok(())
            }
            crate::deployment::Claim::Standby(ownership) => {
                let mut guard = self.managed_deployment.lock().unwrap();
                let state = guard.as_mut().expect("managed state stays installed");
                state.role = ManagedRole::Standby;
                state.ownership = ownership;
                Err(anyhow::Error::new(DeploymentNotReady))
            }
        }
    }

    pub(crate) async fn deployment_status(
        &self,
    ) -> Result<(String, String, ManagedRole, Option<crate::deployment::Ownership>)> {
        let (revision, key, role) = {
            let guard = self.managed_deployment.lock().unwrap();
            let state =
                guard.as_ref().ok_or_else(|| anyhow::Error::new(crate::deployment::OwnershipError::Disabled))?;
            (state.config.revision.clone(), state.coordination_key.clone(), state.role)
        };
        let url = self.pg_url.clone().context("managed deployment requires Postgres mode")?;
        let client = crate::deployment::connect(&url).await?;
        let ownership = crate::deployment::status(&client, &key).await?;
        Ok((key, revision, role, ownership))
    }

    pub(crate) async fn deployment_quiesce(
        &self,
        expected_key: &str,
        expected_owner: &str,
        generation: i64,
        handoff_id: uuid::Uuid,
        source_commit_id: uuid::Uuid,
    ) -> Result<crate::deployment::Ownership> {
        let (revision, key) = {
            let guard = self.managed_deployment.lock().unwrap();
            let state =
                guard.as_ref().ok_or_else(|| anyhow::Error::new(crate::deployment::OwnershipError::Disabled))?;
            (state.config.revision.clone(), state.coordination_key.clone())
        };
        if key != expected_key || revision != expected_owner {
            bail!(crate::deployment::OwnershipError::Conflict);
        }
        // A receipt that predates admission closure cannot fence a mutation which was already
        // admitted. Close and drain on a first attempt, then refuse it so the controller obtains
        // a fresh receipt and retries. The ordinary close endpoint is idempotent, so this is a
        // bounded controller round-trip rather than a new protocol surface.
        self.require_preclosed_control_drain().await?;
        if self.source_drain_receipt(&source_commit_id.to_string()).is_none() {
            bail!(crate::deployment::OwnershipError::Conflict);
        }
        if !self.source_receipt_progress.lock().unwrap().is_after_closure(&source_commit_id.to_string()) {
            bail!(crate::deployment::OwnershipError::FreshReceiptRequired);
        }
        let url = self.pg_url.clone().context("managed deployment requires Postgres mode")?;
        let client = crate::deployment::connect(&url).await?;
        let ownership =
            crate::deployment::quiesce(&client, &key, expected_owner, generation, handoff_id, source_commit_id).await?;
        if let Some(state) = self.managed_deployment.lock().unwrap().as_mut() {
            state.role = ManagedRole::Quiescing;
            state.ownership = Some(ownership.clone());
        }
        self.shutdown.begin();
        Ok(ownership)
    }

    pub(crate) async fn deployment_promote(
        &self,
        expected_key: &str,
        expected_owner: &str,
        successor_revision: &str,
        generation: i64,
        handoff_id: uuid::Uuid,
        source_commit_id: uuid::Uuid,
    ) -> Result<crate::deployment::Ownership> {
        // A promotion request names both ends of the transfer. Without the successor identity an
        // incumbent that is already quiescing could write its own revision back into ownership.
        // Only a standby process changes local readiness while it validates the CAS. An active
        // successor is already serving its persisted generation, so an idempotent retry must not
        // withdraw it during a transient coordinator outage.
        let restore_standby = {
            let mut guard = self.managed_deployment.lock().unwrap();
            let state =
                guard.as_mut().ok_or_else(|| anyhow::Error::new(crate::deployment::OwnershipError::Disabled))?;
            if state.coordination_key != expected_key
                || state.config.revision != successor_revision
                || state.role == ManagedRole::Quiescing
                || self.shutdown.is_shutting_down()
            {
                bail!(crate::deployment::OwnershipError::Conflict);
            }
            if state.role == ManagedRole::Standby {
                state.role = ManagedRole::Promoting;
                true
            } else {
                // Active idempotent retries stay active. A second overlap observes the first
                // promotion and leaves cleanup to the request that actually entered it.
                false
            }
        };
        let (revision, key) = {
            let guard = self.managed_deployment.lock().unwrap();
            let state = guard.as_ref().expect("managed state stays installed");
            (state.config.revision.clone(), state.coordination_key.clone())
        };
        let ownership = async {
            let url = self.pg_url.clone().context("managed deployment requires Postgres mode")?;
            let client = crate::deployment::connect(&url).await?;
            crate::deployment::promote(
                &client,
                &key,
                &revision,
                expected_owner,
                generation,
                handoff_id,
                source_commit_id,
            )
            .await
        }
        .await;
        match ownership {
            Ok(ownership) => {
                if let Some(state) = self.managed_deployment.lock().unwrap().as_mut() {
                    state.role = ManagedRole::Active;
                    state.ownership = Some(ownership.clone());
                }
                self.managed_wakeup.notify_one();
                Ok(ownership)
            }
            Err(error) => {
                if restore_standby {
                    self.restore_failed_promotion_role();
                }
                Err(error)
            }
        }
    }

    /// Refuse new control mutations while leaving health/readiness unchanged for existing readers.
    pub fn ensure_control_admitted(&self) -> Result<()> {
        if self.control_admission.open.load(Ordering::Acquire) && !self.shutdown.is_shutting_down() {
            Ok(())
        } else {
            Err(anyhow::Error::new(ControlAdmissionClosed))
        }
    }

    pub fn admit_control(&self) -> Result<ControlPermit> {
        self.ensure_control_admitted()?;
        self.control_admission.in_flight.fetch_add(1, Ordering::AcqRel);
        // Close can race between the first check and the increment. Re-check after publishing the
        // guard; a loser gives its count back and no mutation starts after close returned.
        if self.ensure_control_admitted().is_err() {
            self.control_admission.release();
            return Err(anyhow::Error::new(ControlAdmissionClosed));
        }
        Ok(ControlPermit { admission: self.control_admission.clone() })
    }

    pub fn close_control_admission(&self) {
        self.control_admission.open.store(false, Ordering::Release);
    }

    /// Close admission, wait for every previously admitted mutation, then snapshot the receipt
    /// frontier. A source fence recorded after this method returns has a sequence strictly above
    /// the watermark; an earlier receipt can never be replayed into a handoff.
    pub async fn close_control_admission_with_receipt_barrier(&self) -> bool {
        let transitioned = self.control_admission.open.swap(false, Ordering::AcqRel);
        self.wait_for_control_drain().await;
        let mut progress = self.source_receipt_progress.lock().unwrap();
        if transitioned || !progress.has_closure() {
            // Managed boot is already closed. Its first explicit close must still establish the
            // frontier so a controller can close, write a fence, and quiesce without an extra
            // round trip; later idempotent closes preserve that original frontier.
            progress.snapshot_closure();
        }
        transitioned
    }

    async fn require_preclosed_control_drain(&self) -> Result<()> {
        if self.close_control_admission_with_receipt_barrier().await {
            bail!(crate::deployment::OwnershipError::PrecloseRequired);
        }
        self.wait_for_control_drain().await;
        Ok(())
    }

    pub async fn wait_for_control_drain(&self) {
        loop {
            let notified = self.control_admission.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.control_admission.in_flight.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }

    pub fn open_control_admission(&self) {
        if !self.shutdown.is_shutting_down() {
            self.control_admission.open.store(true, Ordering::Release);
            self.source_receipt_progress.lock().unwrap().clear_closure();
        }
    }

    pub(crate) fn runtime_drain_receipt(&self, marker: &AuthorityMarker) -> Result<Option<RuntimeDrainReceipt>> {
        self.ensure_not_degraded()?;
        let receipt = self.runtime_receipts.lock().unwrap().get(marker, std::time::Instant::now());
        self.ensure_not_degraded()?;
        Ok(receipt)
    }

    pub fn source_drain_receipt(&self, source_commit_id: &str) -> Option<SourceDrainReceipt> {
        self.source_receipts.lock().unwrap().get(source_commit_id).cloned()
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn record_source_drain_receipt_for_test(&self, source_commit_id: &str) {
        let receipt = SourceDrainReceipt { source_commit_id: source_commit_id.to_owned(), commit_lsn: "0/0".into() };
        self.source_receipts.lock().unwrap().insert(source_commit_id.to_owned(), receipt.clone());
        *self.last_source_receipt.lock().unwrap() = Some(receipt);
        self.source_receipt_progress.lock().unwrap().record(source_commit_id);
    }

    /// Park a shape as dormant from outside the engine module, for tests that drive a wake through
    /// a public surface (an HTTP route) rather than through `Engine`'s private state.
    #[cfg(test)]
    pub(crate) async fn park_dormant_shape_for_test(&self, id: &str, table: &str, resume: LogPosition) {
        {
            let mut st = self.state.lock().await;
            st.shapes.insert(
                id.to_string(),
                ShapeRecord {
                    id: id.to_string(),
                    table: table.into(),
                    stream_path: format!("shape/{id}"),
                    changes_only: false,
                    where_json: None,
                    columns: None,
                    family_key: None,
                    is_subquery: false,
                    aggregate: None,
                    fingerprint: None,
                    backfill_rows: Some(1),
                    backfill_bytes: Some(1),
                },
            );
        }
        self.lives.lock().unwrap().insert(
            id.to_string(),
            crate::retention::ShapeLife {
                last_read: std::time::Instant::now(),
                state: crate::retention::LifeState::Dormant {
                    since: std::time::Instant::now(),
                    resume,
                    gate: crate::pg::SnapshotGate::passthrough(),
                },
            },
        );
    }

    fn restore_failed_promotion_role(&self) {
        if let Some(state) = self.managed_deployment.lock().unwrap().as_mut()
            && state.role == ManagedRole::Promoting
        {
            state.role = ManagedRole::Standby;
        }
    }

    pub fn last_source_drain_receipt(&self) -> Option<SourceDrainReceipt> {
        self.last_source_receipt.lock().unwrap().clone()
    }

    /// Wait for every durable-catalog event sent so far to be appended (or `timeout`). The last
    /// step of a graceful shutdown: the sequencer's final `Offset` is *queued* when it stops, and
    /// only a drained queue makes it the position the next boot resumes from.
    pub async fn drain_catalog(&self, timeout: std::time::Duration) -> bool {
        self.catalog_tx.drain(timeout).await
    }

    /// Configure the always-on dbsp arrangement layer (call before
    /// [`setup_postgres`](Self::setup_postgres), which builds and seeds it).
    pub fn set_dbsp_config(&self, cfg: crate::config::DbspConfig) {
        *self.dbsp_cfg.lock().unwrap() = Some(cfg);
    }

    /// Configure large-transaction handling on the ingest path — the per-transaction memory cap,
    /// the sequencer held-input cap, spill directory and append byte budget (ADR-0003). Call before
    /// [`setup_postgres`](Self::setup_postgres), which spawns the ingestor.
    pub fn set_txn_config(&self, cfg: crate::txn_buffer::TxnBufferConfig) {
        *self.txn_cfg.lock().unwrap() = cfg;
    }

    /// Start the dbsp counts layer and seed it, when configured. Seeds each counts pipeline
    /// from one group-aggregated Postgres snapshot per table (capturing the gate that fences the live
    /// feed); restored state skips seeding — the sequencer replays the change-log gap instead.
    async fn maybe_start_arrangements(&self, schemas: &HashMap<TableRef, TableSchema>) -> Result<()> {
        let Some(cfg) = self.dbsp_cfg.lock().unwrap().clone() else { return Ok(()) };
        if !cfg.indexes.is_empty() {
            tracing::warn!(
                "ELECTRIC_CIRCUITS_DBSP_INDEXES is deprecated and ignored: row data lives in Postgres \
                 (lookups are pooled queries); the circuit holds counts pipelines only"
            );
        }
        if cfg.cache_mib.is_some() || cfg.max_rss_bytes.is_some() {
            tracing::warn!(
                "ELECTRIC_CIRCUITS_DBSP_{{CACHE_MIB,MIN_STORAGE_KB,MAX_RSS_MB,CHECKPOINT_SECS,DIR}} are \
                 deprecated no-ops: the circuit is in-memory counts only (no storage layer)"
            );
        }
        if std::env::var("ELECTRIC_CIRCUITS_FEED_TRACE").is_ok() {
            tracing::warn!(
                "ELECTRIC_CIRCUITS_FEED_TRACE is deprecated and ignored: the feed relation now lives host-side"
            );
        }
        let mut counts: Vec<crate::arrangements::CountSpec> = Vec::new();
        for (t, cols) in &cfg.counts {
            let Some(ts) = schemas.get(t) else {
                tracing::warn!("ELECTRIC_CIRCUITS_DBSP_COUNTS: unknown table {t}; skipping");
                continue;
            };
            let resolved: Option<Vec<usize>> = cols.iter().map(|c| ts.index.get(c).copied()).collect();
            match resolved {
                Some(group_cols) => counts.push(crate::arrangements::CountSpec { table: t.clone(), group_cols }),
                None => tracing::warn!("ELECTRIC_CIRCUITS_DBSP_COUNTS: unknown column in {t}:{cols:?}; skipping"),
            }
        }
        if counts.is_empty() {
            return Ok(()); // nothing for the circuit to maintain
        }
        let arr = crate::arrangements::Arrangements::start(counts.clone())?;
        *self.arrangements.lock().unwrap() = Some(arr.clone());
        // Seed each counts pipeline from ONE group-aggregated query per table — O(groups),
        // not O(rows); row data stays in Postgres. State is in-memory only, so this runs on
        // every boot; the seed's SnapshotGate fences change-log replay exactly like a shape
        // backfill.
        let seeded = async {
            let mut client = self.pg_pool.as_ref().context("Postgres pool unavailable")?.get().await?;
            let mut gates = HashMap::new();
            for spec in &counts {
                let ts = schemas.get(&spec.table).expect("resolved above");
                let (groups, gate) = crate::pg::backfill_group_counts(&mut client, ts, &spec.group_cols).await?;
                let total = groups.len();
                arr.seed_groups(&spec.table, groups).await?;
                gates.insert(spec.table.clone(), gate);
                arr.finish_seed(&spec.table);
                tracing::info!("arrangements: seeded counts for '{}' ({total} groups)", spec.table);
            }
            Ok::<_, anyhow::Error>(gates)
        }
        .await;
        let gates = match seeded {
            Ok(gates) => gates,
            Err(error) => {
                self.arrangements.lock().unwrap().take();
                self.arr_gates.write().unwrap().clear();
                arr.shutdown().await;
                return Err(error);
            }
        };
        *self.arr_gates.write().unwrap() = gates;
        *self.arrangements.lock().unwrap() = Some(arr);
        Ok(())
    }

    /// Sender for the per-envelope trace broadcast — subscribe via `.subscribe()` (used by the
    /// `/trace` SSE endpoint); tailers publish through a clone.
    pub fn trace_sender(&self) -> tokio::sync::broadcast::Sender<Arc<String>> {
        self.trace_tx.clone()
    }

    /// Flip batches enqueued but not yet propagated (convergence-barrier term; see `flip_tx`).
    pub fn pending_flips(&self) -> i64 {
        self.pending_flips.count()
    }

    /// Flip batches abandoned after exhausting their retries — membership effects the engine
    /// computed and could not deliver. Reported as `flipFailures`; non-zero means degraded.
    pub fn flip_failures(&self) -> u64 {
        self.degrade.failures.load(Ordering::SeqCst)
    }

    /// Membership-loss latch only (see [`DegradeState`]), not overall readiness. Data admission
    /// also checks terminal replay failure, epoch state and boot/ownership readiness below.
    pub fn degraded(&self) -> bool {
        self.degrade.degraded.load(Ordering::SeqCst)
    }

    /// The guard every membership-bearing create/read path takes: once degraded, refuse rather than
    /// answer with membership the engine knows is wrong.
    ///
    /// Independent terminal refusals have typed 503 errors: lost membership effects ([`Degraded`]),
    /// halted replay ([`ReplayFailed`]), and a broken epoch under the refuse policy ([`EpochBroken`]).
    /// These precede temporary boot/ownership admission, so they never advertise Retry-After.
    pub fn ensure_not_degraded(&self) -> Result<()> {
        // A reset in flight is checked FIRST: it is the transient, actionable one, and while it runs
        // the epoch is also (correctly) latched broken — "retry in a moment" is the useful answer.
        // Taken under the engine-state lock by every create, so a create either registered before
        // the reset's enumeration (and is retired by it, then rolled back by its own closing check)
        // or is refused here — never installed against a slot that is being replaced.
        if self.epoch_resetting() {
            return Err(anyhow::Error::new(EpochResetting));
        }
        if let Some(reason) = self.epoch_broken() {
            return Err(anyhow::Error::new(EpochBroken { reason }));
        }
        if self.degraded() {
            return Err(anyhow::Error::new(Degraded));
        }
        if self.sequencer_read_failed.load(Ordering::SeqCst) {
            return Err(anyhow::Error::new(ReplayFailed));
        }
        // Schemas become visible before arrangement seeding and catalog restore finish. A
        // request admitted in that window could spawn the one-shot sequencer at changes/0,
        // before restore installs its saved cursor. ACTIVE is published only after restore and
        // sequencer setup succeed; library mode has no Postgres boot to wait for.
        if self.pg_url.is_some() && self.health.load(Ordering::Acquire) != HEALTH_ACTIVE {
            return Err(anyhow::Error::new(DeploymentNotReady));
        }
        if !self.managed_public_ready() {
            return Err(anyhow::Error::new(DeploymentNotReady));
        }
        Ok(())
    }

    /// Latch the engine degraded. Exposed for tests that drive the refusal surface without a lost
    /// flip to cause it; the engine itself degrades only from the propagator's abandonment path.
    #[doc(hidden)]
    pub fn force_degraded(&self) {
        self.degrade.mark();
    }

    /// Start this Engine's reaper once at construction. It waits for degradation or shutdown;
    /// request handlers may call this idempotently without starting work on an unowned runtime.
    pub(crate) fn ensure_degrade_reaper(&self) {
        if self.degrade.reaper_started.swap(true, Ordering::SeqCst) {
            return;
        }
        let engine = self.clone();
        let mut wake = self.degrade.wake.subscribe();
        self.shutdown.spawn_background(async move {
            while !engine.degraded() {
                if wake.changed().await.is_err() {
                    return; // engine gone
                }
            }
            engine.reap_subquery_streams().await;
        });
    }

    /// Delete every registered subquery shape's durable stream. Clients read durable-streams
    /// directly, past the HTTP surface that now answers 503, so a deleted stream is the only way
    /// they learn their shape is no longer maintained. The shape RECORDS stay registered (the
    /// engine answers 503, not 404). This destroys nothing a restart would have kept: a restart
    /// re-seeds every node from Postgres but DROPS every subquery shape — their inner-node state
    /// is not persisted, so the catalog restore deliberately does not restore them (see
    /// `Engine::apply_catalog`) — and clients recreate them with `POST /shapes`.
    async fn reap_subquery_streams(&self) {
        let paths: Vec<String> = {
            let st = self.state.lock().await;
            st.shapes.values().filter(|r| r.is_subquery).map(|r| r.stream_path.clone()).collect()
        };
        tracing::error!(
            "degraded: deleting {} subquery shape stream(s); clients must recreate against a restarted engine",
            paths.len()
        );
        for path in paths {
            // Retirement (close, then delete), retried until storage accepts the delete
            // (`delete_stream` counts a 404 as done): a stream left behind keeps serving rows the
            // engine can no longer maintain, and the close releases subscribers tailing it now.
            let mut attempt = 0u32;
            while let Err(e) = self.ds.retire_stream(&path).await {
                attempt += 1;
                let backoff = std::time::Duration::from_millis(100u64.saturating_mul(1 << attempt.min(5)).min(2000));
                tracing::warn!(
                    "degraded: deleting stream {path} failed (attempt {attempt}), retrying in {backoff:?}: {e:#}"
                );
                tokio::time::sleep(backoff).await;
            }
        }
    }

    fn subquery_handle(&self) -> SubqueryHandle {
        SubqueryHandle {
            registry: self.subqueries.clone(),
            flip_tx: self.flip_tx.clone(),
            pending_flips: self.pending_flips.clone(),
            degrade: self.degrade.clone(),
            group_lanes: self.group_lanes.clone(),
        }
    }

    /// Get (or spawn) the single sequencer task consuming the global change log.
    fn ensure_sequencer<'a>(&self, st: &'a mut EngineState) -> &'a SequencerHandle {
        if st.sequencer.is_none() {
            let start = self.seq_start.lock().unwrap().clone();
            let highwater = *self.seq_highwater.lock().unwrap();
            let txn_cfg = self.txn_cfg.lock().unwrap().clone();
            crate::txn_buffer::sweep_held_spill_dir(&txn_cfg.spill_dir);
            st.sequencer = Some(spawn_sequencer(
                self.ds.clone(),
                self.tables_shared.clone(),
                start,
                highwater,
                self.catalog_tx.clone(),
                self.source_receipts.clone(),
                self.last_source_receipt.clone(),
                self.source_receipt_progress.clone(),
                self.runtime_receipts.clone(),
                self.subquery_handle(),
                self.group_fence.clone(),
                self.trace_tx.clone(),
                self.arrangements.lock().unwrap().clone(),
                self.arr_gates.read().unwrap().clone(),
                self.pg_url.is_none(),
                self.restore_reads_paused.load(std::sync::atomic::Ordering::Acquire),
                self.restore_reads_paused.clone(),
                self.sequencer_read_failed.clone(),
                self.retention.pending_buffer_max_bytes,
                txn_cfg,
                self.pg_pool.as_ref().map(crate::pg::Pool::sequenced),
                self.shutdown.clone(),
            ));
        }
        st.sequencer.as_ref().expect("sequencer just spawned")
    }

    /// Number of tables with a known schema (tables being tailed) — for the boot `consumers_ready` metric.
    pub async fn table_count(&self) -> usize {
        self.state.lock().await.tables.len()
    }

    /// Every table the engine tracks, canonically sorted (`GET /tables`).
    pub async fn tracked_tables(&self) -> Vec<TableRef> {
        let mut v: Vec<TableRef> = self.state.lock().await.tables.keys().cloned().collect();
        v.sort();
        v
    }

    /// The `/v1/health` status string: `degraded` | `waiting` | `starting` | `active` (exact, no
    /// whitespace). `degraded` outranks every boot phase — an engine that has lost membership
    /// effects, cannot continue replay, or whose epoch broke under the refuse policy (ADR-0004)
    /// is not healthy however far along its boot got. These share one status word because the
    /// fleet healthcheck string-compares the body. Membership/epoch details and the `replayFailed`
    /// flag are exposed by `GET /replication/lsn`; the failing read logs the replay cause.
    pub fn health_status(&self) -> &'static str {
        if let Some((role, _)) = self.managed_status() {
            match role {
                ManagedRole::Standby => return "standby",
                ManagedRole::Quiescing => return "quiescing",
                ManagedRole::Promoting => return "promoting",
                ManagedRole::Active => {}
            }
        }
        if self.degraded()
            || self.epoch_broken().is_some()
            || self.sequencer_read_failed.load(std::sync::atomic::Ordering::Relaxed)
        {
            return "degraded";
        }
        match self.health.load(std::sync::atomic::Ordering::Relaxed) {
            HEALTH_WAITING => "waiting",
            HEALTH_STARTING => "starting",
            _ => "active",
        }
    }

    /// Put the boot phase back to `waiting` — "not connected to Postgres, and about to try again".
    ///
    /// The binary's connect loop calls this after a retryable failure, because a failed attempt
    /// leaves the phase wherever it got to (`starting`, once the connection was open) and a whole
    /// backoff reported as `starting` tells an orchestrator the engine is making progress when it
    /// is in fact waiting to redial.
    pub fn set_waiting(&self) {
        self.health.store(HEALTH_WAITING, std::sync::atomic::Ordering::Relaxed);
    }

    /// The `GET /ready` status word — the **readiness** probe, as distinct from the liveness one
    /// (`GET /health`, which is "the process is running" and nothing more).
    ///
    /// `active` (200) means every precondition for serving is met: Postgres connected, the slot
    /// verified against the epoch binding, the durable catalog restored, the ingestor spawned, no
    /// degradation, no broken epoch. Anything else is 503 with the word that says why —
    /// `waiting` (no Postgres yet), `starting` (introspecting / restoring), `degraded`,
    /// or `shutting_down`.
    ///
    /// Shutdown outranks everything: the instant a `SIGTERM` lands this reports `shutting_down`, so
    /// a load balancer stops routing to the pod BEFORE the engine starts winding anything down.
    pub fn readiness_status(&self) -> &'static str {
        if self.shutdown.is_shutting_down() {
            return "shutting_down";
        }
        self.health_status()
    }

    fn validate_ingest_tables(&self, tables: &[TableRef]) -> Result<()> {
        for table in self.pg_columns.keys() {
            if !tables.contains(table) {
                bail!("ELECTRIC_CIRCUITS_PG_COLUMNS: unknown tracked table '{table}'");
            }
        }
        Ok(())
    }

    fn compile_ingest_table(&self, table: &TableRef, def: &crate::schema::TableDef) -> Result<TableSchema> {
        let full = TableSchema::from_def(table, def)?;
        match self.pg_columns.get(table) {
            Some(columns) => {
                let seen = self.pg_columns_seen.read().unwrap();
                let columns: Vec<_> = columns.iter().filter(|column| {
                    if !def.columns.contains_key(*column) && seen.get(table).is_some_and(|names| names.contains(*column)) {
                        tracing::warn!(%table, %column, "configured ingest column was dropped; projecting without it");
                        false
                    } else {
                        true
                    }
                }).cloned().collect();
                full.project_ingest(&columns).context("ELECTRIC_CIRCUITS_PG_COLUMNS")
            }
            None => Ok(full),
        }
    }

    fn tracks_ingest_columns(&self, table: &TableRef) -> bool {
        self.pg_columns.contains_key(table) || self.pg_columns_seen.read().unwrap().contains_key(table)
    }

    fn remember_ingest_columns(&self, table: &TableRef, columns: &[String]) {
        let mut seen = self.pg_columns_seen.write().unwrap();
        let names = seen.entry(table.clone()).or_default();
        if let Some(configured) = self.pg_columns.get(table) {
            names.extend(columns.iter().filter(|name| configured.contains(name)).cloned());
        }
    }

    /// Introspect the configured tables from Postgres, set `REPLICA IDENTITY FULL`, create the
    /// replication slot, register the schema, and start the replication ingestor. Idempotent: a second
    /// call re-introspects but will NOT spawn a second ingestor (two ingestors would fight for the slot).
    pub async fn setup_postgres(&self, selectors: &[TableSelector], slot: &str) -> Result<()> {
        let url = self.pg_url.clone().context("setup_postgres called without a pg_url")?;
        // A retryable failure below brings the boot back here (see the binary's connect loop), so
        // the phase is reset rather than left wherever the last attempt stopped: `/ready` must say
        // `waiting` while the engine is trying to reach Postgres, not `starting`.
        self.health.store(HEALTH_WAITING, std::sync::atomic::Ordering::Relaxed);
        let client = crate::pg::connect_owned(&url, &self.shutdown).await?;
        self.setup_postgres_connected(client, selectors, slot).await
    }

    async fn setup_postgres_connected(
        &self,
        client: tokio_postgres::Client,
        selectors: &[TableSelector],
        slot: &str,
    ) -> Result<()> {
        let url = self.pg_url.clone().context("setup_postgres called without a pg_url")?;
        // `wal_level` is checked explicitly, first, rather than left to surface as a slot-creation
        // failure: it needs a Postgres RESTART to change, so it deserves its own named refusal.
        crate::pg::check_wal_level(&client).await?;
        // This is the sole managed-mode operation before normal boot. A standby only reads (or
        // wins the explicit generation-1 bootstrap insert); it cannot restore catalog state, alter
        // publication/identity, claim a slot, spawn ingest, or admit controls.
        self.claim_managed_ownership(&client).await?;
        // Postgres connection established: leave `waiting`, enter `starting` (introspection + slot +
        // ingest spawn still ahead). `/v1/health` reports 202 until the ingest loop is running.
        self.health.store(HEALTH_STARTING, std::sync::atomic::Ordering::Relaxed);
        // An empty setting means `*`, i.e. `public.*`: every table with a PK in `public` — NOT every
        // schema (introspect-all sets REPLICA IDENTITY FULL, which is not ours to do to managed
        // system schemas). `schema.*` opts another schema in explicitly.
        let default_all = [TableSelector::AllIn(crate::table_ref::PUBLIC_SCHEMA.to_string())];
        // Was the wildcard ASKED for, or is it the default? An explicit `schema.*` that finds
        // nothing is a misconfiguration (wrong schema name, or the migrations never ran) and aborts
        // the boot; the default `public.*` finding nothing is just an empty database, which is a
        // legitimate cold start.
        let explicit = !selectors.is_empty();
        let selectors: &[TableSelector] = if explicit { selectors } else { &default_all };
        // Resolve the selectors to a de-duplicated, ordered table set (a table named twice — say
        // `other.*` plus `other.items` — is introspected once).
        let mut tables: Vec<TableRef> = Vec::new();
        for sel in selectors {
            match sel {
                TableSelector::AllIn(schema) => {
                    let discovered = crate::pg::list_tables(&client, schema).await?;
                    if explicit && discovered.is_empty() {
                        bail!(
                            "ELECTRIC_CIRCUITS_PG_TABLES selects '{schema}.*', but schema '{schema}' \
                             has no base tables with a primary key"
                        );
                    }
                    tracing::info!("introspect-all '{schema}.*': {} table(s)", discovered.len());
                    tables.extend(discovered);
                }
                TableSelector::One(t) if t == crate::runtime_authority::marker_table() => {}
                TableSelector::One(t) => tables.push(t.clone()),
            }
        }
        tables.sort();
        tables.dedup();
        self.validate_ingest_tables(&tables)?;
        // Validate tracked-table visibility before any durable catalog, slot, publication,
        // identity or schema setup can mutate Postgres or storage. RLS is unsupported in the first
        // profile because it can make logical replication silently selective.
        crate::pg::reject_rls_tables(&client, &tables).await?;
        // --- The epoch (ADR-0004) ---
        //
        // Read the durable catalog and DECIDE before restoring anything: the epoch the catalog's
        // shapes belong to is recorded in that same log, and no shape may be resumed until the slot
        // it depends on has been vouched for. A slot the engine cannot vouch for is not recreated
        // quietly — every shape over it is missing an unknown span of WAL.
        self.set_epoch_slot(slot);
        // A catalog the engine could not READ is not a catalog with no epoch in it. Booting past an
        // unreadable one would take the `FirstBoot` branch — create a slot at the current WAL head,
        // append a `SlotBound` on top of whatever is already in the log — and the next boot, with
        // storage healthy again, would Resume-restore shapes that were never `Dropped` straight over
        // the gap. So it is fatal, exactly like a catalog written before ADR-0002: nothing may claim
        // an epoch unless the log was read and demonstrably contained none.
        let fold = self.fold_catalog().await.map_err(|e| {
            if e.downcast_ref::<catalog::CatalogPredatesQualification>().is_some() {
                return e;
            }
            e.context(
                "durable catalog unreadable; refusing to decide the epoch (an unreadable catalog is \
                 not an empty one — booting on would create a slot at the current WAL head and \
                 silently orphan every shape already in the log). Fix durable-streams and restart.",
            )
        })?;
        // The validated catalog supplies the history that distinguishes a dropped column from a
        // typo. Validate projection before epoch/slot/publication/identity writes. Compilation
        // uses all catalog columns regardless of generated-column publication policy; only the
        // physical fingerprint depends on that policy and is re-read after identity setup below.
        for table in self.pg_columns.keys() {
            let _permit = self.metrics.memory_budget.acquire(crate::pg::backfill_config().append_bytes).await;
            let def = crate::pg::introspect(&client, table, false).await?;
            self.compile_ingest_table(table, &def)?;
        }
        self.adopt_epoch_binding(fold.binding.clone());
        let verdict = self.verify_epoch_at_boot(&client, slot).await?;
        let restored = match boot_epoch_action(&verdict) {
            // Either the epoch is intact or this boot just started one. Restore as usual.
            BootEpochAction::Restore => {
                self.init_change_log(fold.current_segment, fold.segment_starts.clone(), &fold.start_pos()).await?;
                Some(fold)
            }
            BootEpochAction::Wait(active_pid) => {
                // A busy slot is not an epoch break, but ownership is not established. Do not
                // mutate durable state, restore shapes, spawn tasks, or claim readiness.
                self.health.store(HEALTH_WAITING, std::sync::atomic::Ordering::Relaxed);
                return Err(anyhow::Error::new(crate::replication::Refused::SlotBusy(active_pid)));
            }
            BootEpochAction::Park(reason) => {
                self.init_change_log(fold.current_segment, fold.segment_starts.clone(), &fold.start_pos()).await?;
                // Park the records (see `RestoreMode::Park`): nothing is resumed, so no old-epoch
                // shape is ever maintained, and the reset — now, or whenever the operator asks —
                // still retires each one properly instead of orphaning its stream. Parking BEFORE
                // the policy runs is what gives the auto reset something to retire. No compiled
                // schema is needed in Park mode.
                self.apply_catalog(fold, &HashMap::new(), catalog::RestoreMode::Park).await?;
                // The same latch/count/act path the ingestor's pre-connect check uses. A refusal is
                // the expected outcome under the refuse policy, and boot continues (every route
                // answers 503); a failed auto-reset leaves the break latched and the ingestor
                // retries it.
                if let Err(refused) = self.on_epoch_break(reason, slot).await {
                    tracing::warn!("boot: ingest will not start — {refused}");
                    self.health.store(HEALTH_WAITING, std::sync::atomic::Ordering::Relaxed);
                    return Err(anyhow::Error::new(refused));
                }
                None
            }
        };

        // Only after the durable catalog was readable and the slot ownership/epoch verdict was
        // obtained do we perform the Postgres setup writes. A busy, foreign, lost, or mismatched
        // slot therefore cannot cause publication/identity mutation before boot refuses or parks.
        let publication = format!("{slot}_pub");
        if self.postgres_setup == PostgresSetup::EngineManaged {
            crate::pg::ensure_publication(&client, &publication).await?;
        }
        let pubinfo = crate::pg::inspect_publication(&client, &publication, &tables).await?;
        self.publish_generated.store(pubinfo.publish_generated, Ordering::Release);
        if pubinfo.publish_generated {
            tracing::info!("publication '{publication}' publishes stored generated columns");
        }
        let mut compiled = HashMap::new();
        for t in &tables {
            // Identity FIRST, then introspect: the compiled schema carries a fingerprint that
            // includes `relreplident`, and reading it before the ALTER would record the pre-boot
            // identity — which the ingestor's first `Relation` message would then report as drift
            // on every single boot (ADR-0005).
            if self.postgres_setup == PostgresSetup::EngineManaged {
                crate::pg::ensure_replica_identity_full(&client, t).await?;
            }
            let def = crate::pg::introspect(&client, t, self.publish_generated.load(Ordering::Acquire)).await?;
            if self.postgres_setup == PostgresSetup::ExternallyManaged
                && def
                    .fingerprint
                    .as_ref()
                    .is_none_or(|fingerprint| fingerprint.replident != crate::schema::REPLICA_IDENTITY_FULL)
            {
                bail!("externally managed table '{t}' must already use REPLICA IDENTITY FULL");
            }
            let ts = self.compile_ingest_table(t, &def)?;
            compiled.insert(t.clone(), ts);
        }
        *self.tables_shared.write().unwrap() = compiled.clone();
        self.state.lock().await.tables = compiled.clone();
        self.subqueries.lock().await.set_schemas(Arc::new(compiled.clone()));
        // Start (and seed) the dbsp arrangement layer BEFORE the catalog restore: the restore spawns
        // the sequencer (which captures the handle + seed gates) and may re-register circuit-served
        // shapes, both of which need the layer up. It is also after the epoch step, so a reset's new
        // slot exists before the seed snapshot is taken — otherwise the circuit would miss the
        // changes between the two. A failure here degrades to Postgres query-backs (the engine still
        // runs), it does not abort boot.
        if let Err(e) = self.maybe_start_arrangements(&compiled).await {
            tracing::error!("dbsp arrangements failed to start (falling back to Postgres): {e:#}");
        }
        // Replay the durable shape catalog (restores shapes + the change-log replay offset), then
        // start the sequencer from the restored position. Runs before the ingestor so the restored
        // routing sees every replayed change.
        if let Some(fold) = restored {
            self.apply_catalog(fold, &compiled, catalog::RestoreMode::Resume).await.context("catalog restore")?;
        } else {
            // Epoch reset retired the old records already. Still commit the projection witness
            // before new ingest. The existing catalog reader holds Slice 5 page permits.
            let mut fold = self.fold_catalog().await?;
            self.reconcile_ingest_columns(&mut fold, &compiled).await?;
        }
        {
            let mut st = self.state.lock().await;
            self.ensure_sequencer(&mut st);
        }
        // Spawn the ingestor at most once, even if setup_postgres is called again.
        if self.replicator_started.swap(true, std::sync::atomic::Ordering::SeqCst) {
            tracing::warn!("setup_postgres called again; ingestor already running, not spawning another");
            self.health.store(HEALTH_ACTIVE, std::sync::atomic::Ordering::Release);
            return Ok(());
        }
        // The ingestor reads the LIVE schema view (not a boot-time copy) and reports schema drift
        // / TRUNCATE back through `SchemaEvents`, which `Engine` implements (see `engine::drift`).
        // It is spawned even when the epoch is broken and the policy is refuse: `EpochEvents`
        // refuses every connection until `POST /epoch/reset`, so it parks in its backoff loop and
        // starts streaming the moment the operator acts — no ingest happens in the meantime.
        // Registered BEFORE the spawn so the shutdown wait can never observe "no parties" in the
        // window between deciding to start the ingestor and the task actually running.
        let party = self.shutdown.party("replication ingestor");
        self.shutdown.spawn(crate::replication::run(
            url,
            slot.to_string(),
            publication,
            self.changes.clone(),
            self.tables_shared.clone(),
            Arc::new(self.clone()) as Arc<dyn crate::replication::SchemaEvents>,
            Arc::new(self.clone()) as Arc<dyn crate::replication::EpochEvents>,
            self.repl_lsn.clone(),
            self.repl_sync.clone(),
            self.txn_cfg.lock().unwrap().clone(),
            self.shutdown.clone(),
            party,
        ));
        // DDL with no following DML produces no `Relation` message; the reconciler catches it.
        self.ensure_schema_reconciler();
        // The retention sweeper is lazy elsewhere (a library user that never creates a shape never
        // needs it), but a Postgres-mode engine always has a change log growing under it: the same
        // sweep is what deletes the segments nothing can resume inside (ADR-0006), so it must run
        // whether or not anyone has ever created a shape.
        self.ensure_retention_sweeper();
        // Restore + sequencer + ingest are up: publish boot completion to membership admission
        // and report `active` (200 on `/v1/health`).
        self.health.store(HEALTH_ACTIVE, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// Adopt the change log's segmentation state at boot and make the CURRENT segment exist
    /// (ADR-0006).
    ///
    /// `from_fold` is the catalog's answer, which is a **lower bound**: a process can die between
    /// closing a segment and recording its `ChangesRotated`, so the boot walks forward over closed
    /// segments (`changelog::resolve_current`) and writes the rotation records the crashed
    /// predecessor missed. On a genuine first boot nothing is in the log at all, and segment 0's
    /// creation is recorded as `ChangesRotated { segment: 0, at }` so the age criterion has a start
    /// time to measure from.
    ///
    /// It also vouches for the **sequencer's** whole replay chain, from its start position through
    /// the resolved current segment. A segment is deleted only once the durable checkpoint is past
    /// it, so any gap in that range is fatal at boot, with a message naming the segment. Otherwise
    /// replay would repeatedly request a runtime restart when it reached the same missing data.
    async fn init_change_log(
        &self,
        from_fold: u32,
        starts: std::collections::BTreeMap<u32, u64>,
        seq_start: &LogPosition,
    ) -> Result<()> {
        // "Recorded" tells a first boot (nothing was ever written) from a segment the engine knows
        // existed having been deleted — see `resolve_current`.
        let recorded = starts.contains_key(&from_fold);
        let resolved = crate::changelog::resolve_current(&self.ds, from_fold, recorded)
            .await
            .context("resolving the change log's current segment")?;
        let mut starts = starts;
        self.ds
            .ensure_stream(&segment_path(resolved))
            .await
            .with_context(|| format!("creating {}", segment_path(resolved)))?;
        // Every segment from the fold's answer up to the resolved one began without its record
        // being written (or, for segment 0 on a first boot, has just been created here).
        for n in from_fold..=resolved {
            if starts.contains_key(&n) {
                continue;
            }
            let at = crate::changelog::now_secs();
            starts.insert(n, at);
            self.catalog_tx.send(CatalogEvent::ChangesRotated { segment: n, at });
        }
        // The sequencer resumes here; if that segment is gone it can never advance (see above).
        let start_head =
            self.ds.head(&segment_path(seq_start.segment)).await.with_context(|| {
                format!("checking the sequencer's start segment {}", segment_path(seq_start.segment))
            })?;
        if start_head.is_none() {
            bail!(
                "durable catalog resumes the change log at {seq_start}, but {} does not exist. A segment is \
                 deleted only once the durable checkpoint has passed it, so this cannot happen while the \
                 engine is the only writer — refusing to boot rather than spin on a deleted stream. Reset \
                 the durable-streams data directory.",
                segment_path(seq_start.segment)
            );
        }
        // Resolving the writer can start ahead of the checkpoint. Every intervening segment is
        // still needed for replay, even if the checkpoint already consumed its rotation pointer.
        // Keep unavailable HEAD errors intact so the outer boot loop retries them, not a gap.
        let mut tail = start_head.and_then(|h| h.next_offset);
        for predecessor in seq_start.segment..resolved {
            let path = segment_path(predecessor + 1);
            let head = self.ds.head(&path).await.with_context(|| format!("checking required replay segment {path}"))?;
            let Some(head) = head else {
                bail!(
                    "durable catalog resumes the change log at {seq_start}, but required segment {path} \
                     does not exist. A segment is deleted only once the durable checkpoint has passed \
                     it — refusing to boot with an incomplete replay chain."
                );
            };
            tail = head.next_offset;
        }
        // The last verified HEAD also seeds the writer's size budget from its current tail.
        self.changes.state().adopt(resolved, starts, tail);
        self.metrics.changes_segments_retained.store(self.changes.state().retained(), Ordering::Relaxed);
        tracing::info!("change log: current segment is {}", segment_path(resolved));
        Ok(())
    }

    /// Last commit LSN appended by the replication ingestor (text form, e.g. "0/1A2B3C").
    pub fn replication_lsn(&self) -> String {
        self.repl_lsn.lock().unwrap().clone()
    }

    /// In-flight reliable appends retrying after failure; diagnostic, not readiness or a drain fence.
    pub(crate) fn failing_appends(&self) -> usize {
        self.ds.failing_appends()
    }

    /// Highest `__el_sync` sentinel counter the ingestor has decoded-and-appended.
    pub fn replication_sync(&self) -> i64 {
        self.repl_sync.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn stream_url(&self, path: &str) -> String {
        self.ds.stream_url(path)
    }

    /// A stream's current tail offset (`stream-next-offset` of a `HEAD`); `None` if it is gone or
    /// closed, since nothing more can be appended after a closed tail.
    pub async fn stream_tail(&self, path: &str) -> Result<Option<String>> {
        Ok(self.ds.head(path).await?.filter(|h| !h.closed).and_then(|h| h.next_offset))
    }

    /// Define a library-mode schema. PostgreSQL mode configures schemas through introspection.
    pub async fn define_schema(&self, schema: &Schema) -> Result<()> {
        // Reject at the engine boundary, before any storage/schema mutation or sequencer spawn.
        // This must hold after boot too: callers must never replace PostgreSQL's schema view.
        if self.pg_url.is_some() {
            return Err(anyhow::Error::new(SchemaDefinitionUnsupported));
        }
        let compiled = compile_schema(schema)?;
        if compiled.contains_key(crate::runtime_authority::marker_table()) {
            bail!("runtime authority marker is private and cannot be an application table");
        }
        // Library construction validated the catalog but does not restore its Postgres state.
        // The current segment is whatever this process resolved (0 on a fresh engine); there is no
        // ingestor. The CURRENT segment stream is what gets created, never a bare `changes`.
        self.init_change_log(
            self.changes.state().current(),
            self.changes.state().segments(),
            &self.changes.state().position(),
        )
        .await?;
        self.subqueries.lock().await.set_schemas(Arc::new(compiled.clone()));
        *self.tables_shared.write().unwrap() = compiled.clone();
        {
            let mut st = self.state.lock().await;
            st.tables = compiled;
            self.ensure_sequencer(&mut st);
        }
        Ok(())
    }

    /// Run a one-shot **subset query** (the non-materialized counterpart to a shape): a single
    /// `SELECT … WHERE … ORDER BY … LIMIT … OFFSET …` against Postgres, returning the projected page
    /// rows (as JSON) + the snapshot LSN. Creates no shape, no stream, no live state — paging never
    /// becomes server-side range state, so a change can never fan out across ranges. The caller follows
    /// the live tail separately (a base-predicate feed) to keep the page live.
    pub async fn query_subset(
        &self,
        table: &TableRef,
        where_: Option<PredicateJson>,
        columns: Option<Vec<String>>,
        order_by: Option<(String, bool)>,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<SubsetPage> {
        // Every page logs where its time went once it ends; a page dropped mid-await logs as
        // cancelled from the guard's `Drop` (see `subset_query_log`).
        let mut log = subset_query_log::SubsetQueryLog::start(table);
        let page = self.query_subset_logged(&mut log, table, where_, columns, order_by, limit, offset).await;
        log.finish(&page);
        page
    }

    #[allow(clippy::too_many_arguments)]
    async fn query_subset_logged(
        &self,
        log: &mut subset_query_log::SubsetQueryLog,
        table: &TableRef,
        where_: Option<PredicateJson>,
        columns: Option<Vec<String>>,
        order_by: Option<(String, bool)>,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<SubsetPage> {
        let plan = self.plan_subset(Some(&mut *log), table, where_, columns, order_by, limit, offset).await?;
        let pool = self.pg_pool.as_ref().context("Postgres pool unavailable")?;
        log.pool = Some((pool.in_use(), pool.size()));
        let mut client = pool.get_timed(&mut log.checkout).await?;
        let sq = crate::pg::query_subset_where(
            &mut client,
            &plan.ts,
            plan.where_sql.clone(),
            &plan.scope,
            plan.order,
            plan.limit,
            plan.offset,
            &mut log.query,
        )
        .await?;
        log.rows = sq.rows.len();
        let rows = plan.project(&sq.rows);
        Ok(SubsetPage { rows, lsn: sq.lsn, snapshot: sq.snapshot, horizon: sq.horizon })
    }

    #[cfg(feature = "test-support")]
    pub async fn subscription_owner(&self, subscription: &str) -> Option<String> {
        self.state.lock().await.subscription_owner(subscription).cloned()
    }

    #[cfg(not(feature = "test-support"))]
    /// Return the current owner of a subscription claim. HTTP compound-load admission holds the
    /// control permit while using this to distinguish a new claim from a renewal.
    pub(crate) async fn subscription_owner(&self, subscription: &str) -> Option<String> {
        self.state.lock().await.subscription_owner(subscription).cloned()
    }

    /// Reserve a group claim before any private member shape is created.  The ordinary
    /// subscription map and the group map are checked while holding one state lock, so a
    /// concurrent single-shape create cannot win the same claim between member creates.
    pub(crate) async fn reserve_group(&self, subscription: &str, member_subscriptions: &[String]) -> Result<()> {
        self.reserve_group_with_id(subscription, member_subscriptions, &format!("group {subscription}")).await
    }

    /// Reserve a group with its stream/incarnation id. Cancellation compensation uses this token
    /// so a late abort cannot remove a newer reservation under the same subscription.
    pub(crate) async fn reserve_group_with_id(
        &self,
        subscription: &str,
        member_subscriptions: &[String],
        reservation_id: &str,
    ) -> Result<()> {
        let mut st = self.state.lock().await;
        // Checked first: while assembling, member 0 already owns the subscription, and a retry
        // must hear "retry" rather than a conflict with its own in-flight load.
        if st.group_claims.is_reserved(subscription) && st.group_claims.get(subscription).is_none() {
            return Err(anyhow::Error::new(crate::engine::GroupLoadInFlight {
                subscription: subscription.to_string(),
            }));
        }
        if let Some(owner) = st.subscription_owner(subscription) {
            return Err(anyhow::Error::new(crate::engine::GroupConflict {
                subscription: subscription.to_string(),
                owner: format!("shape {owner}"),
            }));
        }
        if let Some(owner) = st.group_claims.private_owner(subscription) {
            return Err(anyhow::Error::new(crate::engine::GroupConflict {
                subscription: subscription.to_string(),
                owner: format!("group {owner}"),
            }));
        }
        for member in member_subscriptions {
            if let Some(owner) = st.subscription_owner(member) {
                return Err(anyhow::Error::new(crate::engine::GroupConflict {
                    subscription: member.clone(),
                    owner: format!("shape {owner}"),
                }));
            }
            if let Some(owner) = st.group_claims.private_owner(member) {
                return Err(anyhow::Error::new(crate::engine::GroupConflict {
                    subscription: member.clone(),
                    owner: format!("group {owner}"),
                }));
            }
            if st.group_claims.is_reserved(member) || st.group_claims.get(member).is_some() {
                return Err(anyhow::Error::new(crate::engine::GroupConflict {
                    subscription: member.clone(),
                    owner: format!("group {member}"),
                }));
            }
        }
        st.group_claims.reserve(subscription, reservation_id)?;
        st.group_claims.reserve_private_members(subscription, member_subscriptions.iter().cloned());
        Ok(())
    }

    pub(crate) async fn complete_group(&self, record: GroupRecord) -> Result<()> {
        let mut tables = Vec::new();
        for member in &record.members {
            if let Ok(table) = TableRef::parse(&member.table) {
                tables.push(table);
            }
            if let Some(where_) = &member.where_ {
                tables.extend(referenced_tables(where_));
            }
        }
        tables.sort();
        tables.dedup();
        let gens = self.capture_schema_gens(&tables).await;
        self.complete_group_with_gens(record, &gens).await
    }

    /// Complete a group with the schema/epoch snapshot captured before member creation began.
    /// The catalog durability wait is an interval: the same health, generation, epoch,
    /// registration and stream checks must run after it, before the group is made visible.
    pub(crate) async fn complete_group_with_gens(&self, record: GroupRecord, gens: &SchemaGens) -> Result<()> {
        self.group_lanes.register(&record.stream_path);
        self.catalog_tx.send_durable(CatalogEvent::GroupCreated { group: record.clone() }).await?;
        let stream_open = match self.recheck_group_after_durability(&record, gens).await {
            Ok(open) => open,
            Err(error) => {
                self.remove_group_lane(&record.stream_path);
                // GroupCreated may already be in the durable catalog.  Leave a durable compensating
                // intent behind before the caller's cancellation guard is allowed to disappear.
                let _ = self
                    .catalog_tx
                    .send_durable(CatalogEvent::GroupDropped {
                        subscription: record.subscription.clone(),
                        group_id: Some(record.id.clone()),
                    })
                    .await;
                return Err(error);
            }
        };
        let (valid, renewals) = {
            let mut st = self.state.lock().await;
            let closing_error = if let Err(error) = self.ensure_not_degraded() {
                Some(error)
            } else if st.epoch_reset_since(gens) {
                Some(anyhow::Error::new(CreateRaced(
                    "the replication epoch was reset during group creation".to_string(),
                )))
            } else if let Some(table) = st.drifted_since(gens) {
                Some(anyhow::Error::new(CreateRaced(format!("schema of '{table}' changed during group creation"))))
            } else if !stream_open {
                Some(anyhow::Error::new(CreateRaced(format!(
                    "group stream '{}' closed during group activation",
                    record.stream_path
                ))))
            } else if self.group_retirement_started(&record.id) {
                Some(anyhow::Error::new(CreateRaced(format!(
                    "group '{}' began retirement during group activation",
                    record.id
                ))))
            } else {
                None
            };
            if let Some(error) = closing_error {
                drop(st);
                self.remove_group_lane(&record.stream_path);
                let _ = self
                    .catalog_tx
                    .send_durable(CatalogEvent::GroupDropped {
                        subscription: record.subscription.clone(),
                        group_id: Some(record.id.clone()),
                    })
                    .await;
                return Err(error);
            }
            let valid = record.members.iter().all(|member| {
                st.shapes.contains_key(&member.shape_id)
                    && st.subscription_owner(&member.subscription).is_some_and(|owner| owner == &member.shape_id)
            });
            if valid {
                let now = crate::changelog::now_secs();
                let renewals = record
                    .members
                    .iter()
                    .map(|member| {
                        st.subscribe(&member.shape_id, member.subscription.clone(), now);
                        CatalogEvent::Joined {
                            id: member.shape_id.clone(),
                            subscription: member.subscription.clone(),
                            at: now,
                        }
                    })
                    .collect::<Vec<_>>();
                st.group_claims.complete(record.clone());
                self.group_fence.activate();
                (true, renewals)
            } else {
                (false, Vec::new())
            }
        };
        if !valid {
            self.remove_group_lane(&record.stream_path);
            let _ = self
                .catalog_tx
                .send_durable(CatalogEvent::GroupDropped {
                    subscription: record.subscription.clone(),
                    group_id: Some(record.id.clone()),
                })
                .await;
            return Err(anyhow::Error::new(crate::engine::GroupStreamClosed { stream_path: record.stream_path }));
        }
        for event in renewals {
            self.catalog_tx.send(event);
        }
        Ok(())
    }

    pub(crate) async fn abort_group(&self, subscription: &str, group_id: &str) -> Result<()> {
        self.catalog_tx
            .send_durable(CatalogEvent::GroupDropped {
                subscription: subscription.to_string(),
                group_id: Some(group_id.to_string()),
            })
            .await?;
        self.state.lock().await.group_claims.release_reservation_if_matches(subscription, group_id);
        #[cfg(feature = "test-support")]
        let abort_was_gated = self.group_abort_test_gate.wait_if_armed().await;
        self.state.lock().await.group_claims.abort_if_matches(subscription, group_id);
        #[cfg(feature = "test-support")]
        if abort_was_gated {
            self.group_abort_test_gate.finished.notify_one();
        }
        Ok(())
    }

    /// Capture the generations for all tables a group reads.  The caller takes this before its
    /// first member create so a drift or epoch reset during any later await is observable by the
    /// closing check in `complete_group_with_gens`.
    pub(crate) async fn capture_schema_gens(&self, tables: &[TableRef]) -> SchemaGens {
        self.state.lock().await.capture_gens(tables)
    }

    async fn recheck_group_after_durability(&self, record: &GroupRecord, gens: &SchemaGens) -> Result<bool> {
        for member in &record.members {
            self.recheck_after_durability(&member.shape_id, &record.stream_path, gens).await?;
        }
        match self.ds.head(&record.stream_path).await {
            Ok(Some(head)) => Ok(!head.closed && head.next_offset.is_some()),
            Ok(None) => Err(anyhow::Error::new(CreateRaced(format!(
                "group stream '{}' disappeared during its catalog durability wait",
                record.stream_path
            )))),
            Err(error) => Err(anyhow::Error::new(crate::engine::GroupStreamUnavailable).context(error)),
        }
    }

    pub(crate) async fn group_record(&self, subscription: &str) -> Option<GroupRecord> {
        self.state.lock().await.group_claims.get(subscription).cloned()
    }

    #[cfg(feature = "test-support")]
    pub async fn has_group_record(&self, subscription: &str) -> bool {
        self.state.lock().await.group_claims.get(subscription).is_some()
    }

    #[cfg(feature = "test-support")]
    pub async fn group_id_for_test(&self, subscription: &str) -> Option<String> {
        let st = self.state.lock().await;
        st.group_claims
            .get(subscription)
            .map(|record| record.id.clone())
            .or_else(|| st.group_claims.reservation_id(subscription).cloned())
    }

    #[cfg(feature = "test-support")]
    pub async fn group_reservation_for_test(&self, subscription: &str) -> bool {
        self.state.lock().await.group_claims.is_reserved(subscription)
    }

    #[cfg(feature = "test-support")]
    pub fn group_seed_gate_for_test(&self) -> Arc<GroupSeedTestGate> {
        self.group_seed_test_gate.clone()
    }

    #[cfg(feature = "test-support")]
    pub fn group_abort_gate_for_test(&self) -> Arc<GroupAbortTestGate> {
        self.group_abort_test_gate.clone()
    }

    #[cfg(feature = "test-support")]
    pub fn group_load_gate_for_test(&self) -> Arc<GroupLoadTestGate> {
        self.group_load_test_gate.clone()
    }

    #[cfg(feature = "test-support")]
    pub fn group_release_gate_for_test(&self) -> Arc<GroupReleaseTestGate> {
        self.group_release_test_gate.clone()
    }

    #[cfg(feature = "test-support")]
    pub(crate) async fn wait_group_load_gate_for_test(&self, stage: GroupLoadAwait) {
        self.group_load_test_gate.wait_if_armed(stage).await;
    }

    #[cfg(feature = "test-support")]
    pub(crate) async fn wait_group_seed_gate_for_test(&self) {
        self.group_seed_test_gate.wait_if_armed().await;
    }

    pub(crate) async fn begin_group_release(
        &self,
        subscription: &str,
        expected_id: &str,
    ) -> Option<(GroupRecord, Arc<GroupRelease>, bool)> {
        let st = self.state.lock().await;
        let group = st.group_claims.get(subscription).cloned()?;
        if group.id != expected_id {
            return None;
        }
        let mut releases = self.group_releases.lock().unwrap();
        if let Some(existing) = releases.get(&group.id) {
            return Some((group, existing.clone(), true));
        }
        let release = GroupRelease::new();
        releases.insert(group.id.clone(), release.clone());
        Some((group, release, false))
    }

    /// The single retirement mark used by renewal and fresh group activation. Callers hold the
    /// engine state lock before invoking this helper, matching `group_is_current`'s ordering.
    fn group_retirement_started(&self, group_id: &str) -> bool {
        self.group_releases.lock().unwrap().contains_key(group_id)
    }

    pub(crate) fn group_release_in_flight(&self, group_id: &str) -> Option<Arc<GroupRelease>> {
        self.group_releases.lock().unwrap().get(group_id).cloned()
    }

    pub(crate) fn register_group_lane(&self, stream_path: &str) {
        self.group_lanes.register(stream_path);
    }

    pub(crate) fn remove_group_lane(&self, stream_path: &str) {
        self.group_lanes.remove(stream_path);
    }

    /// Renew the public group lease and every private member claim together. A retried load is
    /// one idempotent lease touch, so no member claim is duplicated and all member leases advance
    /// to the same wall-clock instant.
    pub(crate) async fn renew_group_claim(&self, group: &GroupRecord) -> Result<bool> {
        let now = crate::changelog::now_secs();
        let mut events = Vec::with_capacity(group.members.len());
        {
            let mut st = self.state.lock().await;
            let in_flight = self.group_releases.lock().unwrap().get(&group.id).cloned();
            if let Some(release) = in_flight {
                drop(st);
                release.wait().await?;
                return Ok(false);
            }
            for member in &group.members {
                if !(st.shapes.contains_key(&member.shape_id)
                    && st.subscription_owner(&member.subscription).is_some_and(|owner| owner == &member.shape_id))
                {
                    return Ok(false);
                }
                st.subscribe(&member.shape_id, member.subscription.clone(), now);
                events.push(CatalogEvent::Joined {
                    id: member.shape_id.clone(),
                    subscription: member.subscription.clone(),
                    at: now,
                });
            }
            // Registration is part of the same state-locked admission as the claim check. A
            // release that wins this lock removes the lane after the renew; a release already in
            // flight makes this branch return before it can recreate the dying lane.
            self.group_lanes.register(&group.stream_path);
            st.publish_subscription_gauge(&self.metrics);
        }
        for event in events {
            self.catalog_tx.send(event);
        }
        Ok(true)
    }

    /// Whether `group` is still its subscription's record, no release of it has begun, and every
    /// member still owns its claim. `begin_group_release` records a release under this same state
    /// lock, so a release this check misses starts after it.
    pub(crate) async fn group_is_current(&self, group: &GroupRecord) -> bool {
        let st = self.state.lock().await;
        st.group_claims.get(&group.subscription).is_some_and(|current| current.id == group.id)
            && !self.group_retirement_started(&group.id)
            && group.members.iter().all(|member| {
                st.shapes.contains_key(&member.shape_id)
                    && st.subscription_owner(&member.subscription).is_some_and(|owner| owner == &member.shape_id)
            })
    }

    pub(crate) async fn group_is_healthy(&self, group: &GroupRecord) -> Result<bool> {
        let members_healthy = {
            let st = self.state.lock().await;
            group.members.iter().all(|member| {
                st.shapes.contains_key(&member.shape_id)
                    && st.subscription_owner(&member.subscription).is_some_and(|owner| owner == &member.shape_id)
            })
        };
        if !members_healthy {
            return Ok(false);
        }
        match self.ds.head(&group.stream_path).await {
            Ok(Some(head)) => Ok(!head.closed && head.next_offset.is_some()),
            Ok(None) => Ok(false),
            Err(error) => Err(anyhow::Error::new(GroupStreamUnavailable).context(error)),
        }
    }

    /// Remove the public group record after its members have been released and its GroupDropped
    /// catalog event is durable. The caller performs member releases outside the state lock.
    pub(crate) async fn take_group(&self, subscription: &str, group_id: &str) -> Option<GroupRecord> {
        let mut st = self.state.lock().await;
        let record = st.group_claims.release_if_matches(subscription, group_id);
        if record.is_some() {
            self.group_fence.deactivate();
        }
        record
    }

    pub(crate) async fn finish_group_release(&self, subscription: &str, group_id: &str) -> Result<()> {
        self.catalog_tx
            .send_durable(CatalogEvent::GroupDropped {
                subscription: subscription.to_string(),
                group_id: Some(group_id.to_string()),
            })
            .await?;
        #[cfg(feature = "test-support")]
        self.group_release_test_gate.wait_if_armed().await;
        let _ = self.take_group(subscription, group_id).await;
        Ok(())
    }

    /// Validate a subset read without opening Postgres or changing engine state.
    ///
    /// HTTP compound loads use this before creating their changes-only feed so a malformed
    /// page (for example an unknown `orderBy` column) cannot leave a subscription claim behind.
    pub(crate) async fn validate_subset(
        &self,
        table: &TableRef,
        where_: Option<PredicateJson>,
        columns: Option<Vec<String>>,
        order_by: Option<(String, bool)>,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<()> {
        self.plan_subset(None, table, where_, columns, order_by, limit, offset).await.map(|_| ())
    }

    /// Several one-shot subset reads in **one** settled `REPEATABLE READ` snapshot (prototype: the
    /// scope load). The snapshot settles against every table any read depends on, so the pages are
    /// mutually consistent and share one set of fences.
    pub async fn query_subset_batch(&self, reads: Vec<SubsetReadReq>) -> Result<SubsetBatchPage> {
        anyhow::ensure!(!reads.is_empty(), "a batch needs at least one table");
        let mut plans = Vec::with_capacity(reads.len());
        for r in reads {
            plans.push(self.plan_subset(None, &r.table, r.where_, r.columns, r.order_by, r.limit, r.offset).await?);
        }
        let mut scope = plans[0].scope.clone();
        for p in &plans[1..] {
            scope = scope.with(p.scope_tables.iter());
        }
        let mut client = self.pg_pool.as_ref().context("Postgres pool unavailable")?.get().await?;
        let batch_reads: Vec<crate::pg::SubsetRead<'_>> = plans
            .iter()
            .map(|p| crate::pg::SubsetRead {
                ts: &p.ts,
                where_sql: p.where_sql.clone(),
                order: p.order,
                limit: p.limit,
                offset: p.offset,
            })
            .collect();
        let bq = crate::pg::query_subset_batch_where(&mut client, batch_reads, &scope).await?;
        let pages = plans.iter().zip(bq.pages.iter()).map(|(p, rows)| p.project(rows)).collect();
        Ok(SubsetBatchPage { pages, lsn: bq.lsn, snapshot: bq.snapshot, horizon: bq.horizon })
    }

    /// Validate a subset read and build its SQL, projection and settle scope without touching Postgres.
    ///
    /// `log`, when given, records the moment the engine-state lock is held.
    #[allow(clippy::too_many_arguments)]
    async fn plan_subset(
        &self,
        log: Option<&mut subset_query_log::SubsetQueryLog>,
        table: &TableRef,
        where_: Option<PredicateJson>,
        columns: Option<Vec<String>>,
        order_by: Option<(String, bool)>,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<SubsetPlan> {
        let (ts, schemas) = {
            let st = self.state.lock().await;
            if let Some(log) = log {
                log.state_held();
            }
            let ts = st.tables.get(table).cloned().ok_or_else(|| anyhow::anyhow!("unknown table '{table}'"))?;
            // Clone the table schemas so the subquery SQL emitter can cast each leaf's param to its
            // column's native Postgres type (query_subset is one-shot; the clone is off the hot path).
            (ts, st.tables.clone())
        };
        let out_cols = resolve_columns(&ts, columns)?;
        let order = match order_by {
            Some((col, desc)) => Some((ts.column_index(&col)?, desc)),
            None => None,
        };
        // Subquery predicates are evaluated natively by Postgres in the one-shot query-back (no engine
        // subquery state needed for a non-live page); other predicates use the compiled-form emitter.
        let where_sql = match where_.as_ref() {
            Some(p) if crate::subquery::predicate_has_subquery(p) => {
                // Reuse the predicate compiler's column/type checks without registering any
                // maintained nodes for this one-shot query. Every nested table is projected too.
                struct Validate<'a>(&'a HashMap<TableRef, TableSchema>);
                impl crate::predicate::SubqueryCollector for Validate<'_> {
                    fn collect(
                        &mut self,
                        table: &TableRef,
                        project: &str,
                        where_: Option<&PredicateJson>,
                    ) -> Result<crate::predicate::SubquerySig> {
                        let ts = self.0.get(table).with_context(|| format!("unknown subquery table '{table}'"))?;
                        ts.column_index(project)?;
                        CompiledPredicate::compile_opt_with(where_, ts, self)?;
                        Ok(crate::predicate::subquery_sig(table, project, where_))
                    }
                }
                CompiledPredicate::compile_with(p, &ts, &mut Validate(&schemas))?;
                Some(crate::sql::predicate_json_to_sql(p, 1, &schemas, table))
            }
            Some(p) => {
                let cp = CompiledPredicate::compile_opt(Some(p), &ts)?;
                crate::sql::predicate_to_sql(&cp, &ts)
            }
            None => None,
        };
        // The page's snapshot is settled against every table the query reads: the subset's table and
        // the inner tables of any subquery in its predicate.
        let scope = crate::pg::SettleScope::request(table)
            .with(where_.as_ref().map(crate::subquery::referenced_tables).unwrap_or_default().iter());
        let scope_tables: Vec<TableRef> = std::iter::once(table.clone())
            .chain(where_.as_ref().map(crate::subquery::referenced_tables).unwrap_or_default())
            .collect();
        Ok(SubsetPlan { ts, out_cols, where_sql, scope, scope_tables, order, limit, offset })
    }

    /// The column list + primary key of a replicated table, for the visualizer's add-row form. Reads the
    /// in-memory `TableSchema` (introspected at startup) — no Postgres round-trip.
    pub async fn table_schema_info(&self, table: &TableRef) -> Result<TableSchemaInfo> {
        let ts = {
            let st = self.state.lock().await;
            st.tables.get(table).cloned().ok_or_else(|| anyhow::anyhow!("unknown table '{table}'"))?
        };
        let pk_set: HashSet<usize> = ts.pk_cols.iter().copied().collect();
        let columns = ts
            .columns
            .iter()
            .enumerate()
            .map(|(i, (name, ty))| TableColumnInfo {
                name: name.clone(),
                ty: col_type_str(*ty),
                pg_type: ts.pg_types.get(i).cloned().flatten(),
                pk: pk_set.contains(&i),
                has_default: ts.has_defaults.get(i).copied().unwrap_or(false),
            })
            .collect();
        let primary_key = ts.pk_cols.iter().map(|&i| ts.columns[i].0.clone()).collect();
        Ok(TableSchemaInfo { table: ts.table.clone(), columns, primary_key })
    }

    /// Insert one row into a replicated table's Postgres relation, so the change is captured by logical
    /// replication and flows through the pipeline (backing the visualizer's add-row action). `values`
    /// maps column name → value; only known columns are accepted (unknown ⇒ error), omitted columns take
    /// their Postgres default / NULL. Identifiers are quoted and values are **bound parameters** cast to
    /// each column's native type — no string-concatenated SQL.
    pub async fn insert_row(
        &self,
        table: &TableRef,
        values: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<serde_json::Value> {
        let ts = {
            let st = self.state.lock().await;
            st.tables.get(table).cloned().ok_or_else(|| anyhow::anyhow!("unknown table '{table}'"))?
        };
        if values.is_empty() {
            bail!("no columns provided");
        }
        let mut cols: Vec<String> = Vec::with_capacity(values.len());
        let mut placeholders: Vec<String> = Vec::with_capacity(values.len());
        let mut params: Vec<String> = Vec::new();
        for (col, val) in values {
            // Reject unknown columns (also closes the identifier-injection surface: only catalog columns
            // are ever emitted, each independently quoted).
            if !ts.index.contains_key(col) {
                bail!("unknown column '{col}' on table '{table}'");
            }
            cols.push(crate::pg::quote_ident(col));
            if val.is_null() {
                placeholders.push("NULL".to_string());
                continue;
            }
            // Bind the value as a text parameter, then cast it to the column's native Postgres type
            // (uuid/int8/bool/timestamptz/…). The leading `::text` pins the parameter's inferred type to
            // text so any value serializes as a string; the second cast converts it to the column type
            // (a bare `$n::int8` would instead make Postgres infer the param itself as int8 and reject a
            // String). A JSON string binds its contents; other scalars bind their compact text form.
            let n = params.len() + 1;
            let placeholder = match ts.pg_type_of(col) {
                Some(t) => format!("${n}::text::{}", crate::pg::quote_ident(t)),
                None => format!("${n}::text"),
            };
            placeholders.push(placeholder);
            let s = match val {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            params.push(s);
        }
        let sql = format!(
            "insert into {} ({}) values ({})",
            table.quote_qualified(),
            cols.join(", "),
            placeholders.join(", "),
        );
        let client = self.pg_pool.as_ref().context("Postgres pool unavailable")?.get().await?;
        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            params.iter().map(|s| s as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
        let n = client.execute(&sql, &param_refs).await.with_context(|| format!("insert into {table}"))?;
        Ok(serde_json::json!({ "ok": true, "inserted": n }))
    }

    /// Delete rows from a replicated table's Postgres relation by primary key, so the deletes are
    /// captured by logical replication and flow through the pipeline (backing the visualizer's
    /// delete-rows action). `keys` holds one map per row: primary-key column → value. Every key must
    /// supply exactly the table's primary-key columns, non-NULL. All rows go in one parameterized
    /// statement (identifiers quoted, values bound and cast to the columns' native types), so a
    /// multi-row delete is a single transaction — one replication batch, one pipeline delta.
    pub async fn delete_rows(
        &self,
        table: &TableRef,
        keys: &[serde_json::Map<String, serde_json::Value>],
    ) -> Result<serde_json::Value> {
        const MAX_KEYS: usize = 1000;
        let ts = {
            let st = self.state.lock().await;
            st.tables.get(table).cloned().ok_or_else(|| anyhow::anyhow!("unknown table '{table}'"))?
        };
        if keys.is_empty() {
            bail!("no keys provided");
        }
        if keys.len() > MAX_KEYS {
            bail!("too many keys ({}); at most {MAX_KEYS} rows per delete", keys.len());
        }
        if ts.pk_cols.is_empty() {
            bail!("table '{table}' has no primary key");
        }
        let pk_names: Vec<&str> = ts.pk_cols.iter().map(|&i| ts.columns[i].0.as_str()).collect();
        let mut clauses: Vec<String> = Vec::with_capacity(keys.len());
        let mut params: Vec<String> = Vec::with_capacity(keys.len() * pk_names.len());
        for key in keys {
            // Only primary-key columns are accepted (as with insert, this also closes the
            // identifier-injection surface: every emitted identifier comes from the catalog).
            for col in key.keys() {
                if !pk_names.contains(&col.as_str()) {
                    bail!("column '{col}' is not in table '{table}''s primary key");
                }
            }
            let mut conj: Vec<String> = Vec::with_capacity(pk_names.len());
            for &col in &pk_names {
                let val = key.get(col).ok_or_else(|| anyhow::anyhow!("key is missing primary-key column '{col}'"))?;
                if val.is_null() {
                    bail!("primary-key column '{col}' must not be NULL");
                }
                // Same bind-as-text-then-cast scheme as insert_row (see the comment there).
                let n = params.len() + 1;
                let placeholder = match ts.pg_type_of(col) {
                    Some(t) => format!("${n}::text::{}", crate::pg::quote_ident(t)),
                    None => format!("${n}::text"),
                };
                conj.push(format!("{} = {placeholder}", crate::pg::quote_ident(col)));
                params.push(match val {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                });
            }
            clauses.push(format!("({})", conj.join(" and ")));
        }
        let sql = format!("delete from {} where {}", table.quote_qualified(), clauses.join(" or "));
        let client = self.pg_pool.as_ref().context("Postgres pool unavailable")?.get().await?;
        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            params.iter().map(|s| s as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
        let n = client.execute(&sql, &param_refs).await.with_context(|| format!("delete from {table}"))?;
        Ok(serde_json::json!({ "ok": true, "deleted": n }))
    }

    /// Number of maintained subquery nodes (for the sharing-topology introspection endpoint).
    pub async fn subquery_node_count(&self) -> usize {
        self.subqueries.lock().await.node_count()
    }

    /// Per-node subquery topology (signature, inner table, distinct values, refcount).
    pub async fn subquery_stats(&self) -> Vec<crate::subquery::NodeStat> {
        self.subqueries.lock().await.stats()
    }

    /// The schema for `table`, if known (used by the Electric-protocol adapter for the schema header and
    /// value encoding).
    pub async fn table_schema(&self, table: &TableRef) -> Option<TableSchema> {
        self.state.lock().await.tables.get(table).cloned()
    }

    /// Read a shape's durable stream (catch-up or long-poll live) — used by the Electric adapter to turn
    /// the engine's shape output into Electric `/v1/shape` change messages.
    pub async fn read_shape_stream(&self, path: &str, offset: &str, live: bool) -> Result<crate::ds::ReadResult> {
        // A data read is a full retention touch: reactivate a dormant shape before reading (so a
        // parked stream is never served stale) and refresh `last_read`. `ensure_active` is a cheap
        // lifecycle-map check when the shape is active (the common case).
        self.ensure_active(sid_of_path(path)).await?;
        self.ds.read(path, offset, live).await
    }

    /// The stream scope this Engine's shape ids and offsets mean anything in.
    pub(crate) fn stream_scope_tag(&self) -> String {
        self.ds.scope_tag()
    }

    /// Resolve lifecycle before holding a fold reservation: wake/replay can itself need budget.
    pub(crate) async fn shape_fold_input(&self, path: &str) -> Result<DsClient> {
        self.ensure_active(sid_of_path(path)).await?;
        Ok(self.ds.clone())
    }

    /// Engine-internal cardinalities for the memory probe — the structures whose growth drives RSS:
    /// registered shapes, per-table tailers, shared **family circuits** (the M× join-trace amplifier:
    /// each holds the base table once), standalone per-shape circuits, and the subquery registry's
    /// nodes/contributor-pks. Read directly from in-memory state (cheap; no tailer round-trip, no
    /// byte-walk, no sequencer round-trip).
    ///
    /// This is the ONLY cardinality path the 500ms background sampler (`mem::spawn_sampler`) is
    /// allowed to call. It deliberately never touches `HeapSize::heap_bytes` or sends
    /// `SequencerCmd::MemBytes` — every `bytes_*` field on the returned [`crate::mem::Cardinalities`]
    /// is left at its `Default` zero. Byte-level self-accounting (Phase 0 of the memory-reduction
    /// effort) lives in the sibling [`Self::mem_bytes`], called by `GET /memory` and the slower
    /// diagnostic memory logger; see its doc comment for why that split exists (a prior regression:
    /// this method used to do the walk
    /// inline, which meant a ~100MB recursive walk + a `MemBytes` sequencer round-trip ran twice a
    /// second at 50k+ shapes).
    pub async fn mem_cardinalities(&self) -> crate::mem::Cardinalities {
        let (shapes, tailers, tables, families, family_shapes, standalone) = {
            let st = self.state.lock().await;
            let mut families = 0usize;
            let mut family_shapes = 0usize;
            let mut standalone = 0usize;
            let mut tables_with_execs = 0usize;
            if let Some(seq) = st.sequencer.as_ref()
                && let Ok(per_table) = seq.stats.lock()
            {
                tables_with_execs = per_table.len();
                for s in per_table.values() {
                    families += s.families.len();
                    family_shapes += s.families.iter().map(|f| f.shapes).sum::<usize>();
                    standalone += s.standalone;
                }
            }
            (st.shapes.len(), tables_with_execs, st.tables.len(), families, family_shapes, standalone)
        };
        let sq = {
            let reg = self.subqueries.lock().await;
            reg.mem_totals()
        };
        let shapes_dormant =
            self.lives.lock().unwrap().values().filter(|l| matches!(l.state, LifeState::Dormant { .. })).count();
        crate::mem::Cardinalities {
            shapes,
            shapes_dormant,
            tailers,
            tables,
            families,
            family_shapes,
            standalone,
            subquery_nodes: sq.nodes,
            subquery_contributors: sq.contributors,
            subquery_distinct_values: sq.distinct,
            subquery_shapes: sq.shapes,
            subquery_edges: sq.edges,
            subquery_feed_entries: sq.feed_entries,
            ..Default::default()
        }
    }

    /// Diagnostic only: full dbsp profiler dumps for EVERY dbsp circuit the engine runs — the
    /// (single, engine-wide) subquery membership circuit plus the counts/arrangements circuit
    /// when configured. The engine's "family" and "standalone" circuits are host-side executor
    /// structures (no dbsp runtime; sized by `bytes_executors`), so they have no profile here.
    /// Heavy (profiler round-trip through each circuit thread, holds the subquery-registry lock
    /// for the membership dump); serves `GET /debug/dbsp-profile` on demand ONLY — never call
    /// this from the 500 ms sampler (see `mem::spawn_sampler`).
    pub async fn dbsp_profile_dump(&self) -> serde_json::Value {
        fn entry(used: usize, stored: usize, json: String) -> serde_json::Value {
            serde_json::json!({
                "total_used_bytes": used,
                "total_storage_bytes": stored,
                "profile": serde_json::from_str::<serde_json::Value>(&json)
                    .unwrap_or(serde_json::Value::String(json)),
            })
        }
        let membership = {
            let reg = self.subqueries.lock().await;
            let (used, stored, json) = reg.circuit_profile_dump().await;
            entry(used, stored, json)
        };
        let arr = self.arrangements.lock().unwrap().clone();
        let counts = match arr {
            Some(a) => {
                let (used, stored, json) = a.profile_dump().await;
                entry(used, stored, json)
            }
            None => serde_json::Value::Null,
        };
        serde_json::json!({ "membership_circuit": membership, "counts_circuit": counts })
    }

    /// On-demand byte-level self-accounting (Phase 0 of the memory-reduction effort): a
    /// [`crate::heap_size::HeapSize`] lower-bound owned-heap estimate per major structure. These
    /// are LOWER BOUNDS (owned heap, not allocator slack) — the gap vs. `process.rss_bytes` is the
    /// allocator/pinning term this phase is instrumenting to measure.
    ///
    /// Expensive: locks engine state, round-trips a one-off `SequencerCmd::MemBytes` to the
    /// sequencer task (mirroring the `DumpNode` command's pattern — see `dump_node` below), locks
    /// the subquery registry, and walks roughly the engine's entire owned heap. Call this only
    /// from the `GET /memory` HTTP handler or the slower diagnostic memory logger — never from the
    /// 500ms background sampler
    /// (`mem::spawn_sampler`), which calls `mem_cardinalities` instead. Mixing this into the
    /// sampler's path was exactly the prior regression (a large peak/steady RSS increase from
    /// twice-a-second byte walks); see `mem::spawn_sampler`'s doc comment.
    ///
    /// `bytes_executors` (standalone shapes + their conjunct index, family routers, aggregate
    /// folds + their index) is the one term this method cannot read out of already-published
    /// state: those structures are privately owned by the sequencer task's `execs` map, never
    /// exposed through a shared mutex (unlike `stats`/`node_states`, which are republished after
    /// every batch specifically so other tasks can read them cheaply). Walking them for real bytes
    /// is not cheap enough to piggyback on every batch (see `sequencer::publish_all`/`stats_of`),
    /// so instead this method round-trips the one-off `SequencerCmd::MemBytes` so the byte-walk
    /// itself only ever runs on an explicit diagnostic path, never per batch.
    pub async fn mem_bytes(&self) -> crate::mem::HeapBytes {
        let (bytes_shape_records, cmd_tx) = {
            let st = self.state.lock().await;
            (st.shapes.heap_bytes(), st.sequencer.as_ref().map(|seq| seq.cmd_tx.clone()))
        };
        // Byte-walk every table's live executor state: ask the sequencer task directly, since it
        // privately owns `execs`.
        let bytes_executors = match cmd_tx {
            Some(tx) => {
                let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
                if tx.send(SequencerCmd::MemBytes { resp: resp_tx }).is_ok() { resp_rx.await.unwrap_or(0) } else { 0 }
            }
            None => 0,
        };
        let (circuit_bytes, bytes_feed_sets, bytes_subquery_registry, bytes_pk_dict) = {
            let reg = self.subqueries.lock().await;
            (reg.circuit_bytes(), reg.feed_sets_bytes(), reg.heap_bytes(), reg.pk_dict_bytes())
        };
        let bytes_retention = self.lives.lock().unwrap().heap_bytes();
        let bytes_electric_adapter = crate::electric::ttl_registry_heap_bytes(self).await;
        crate::mem::HeapBytes {
            bytes_shape_records,
            bytes_executors,
            bytes_retention,
            bytes_subquery_registry,
            bytes_membership_circuit: circuit_bytes.total_bytes(),
            bytes_circuit_integral: circuit_bytes.integral_bytes(),
            bytes_circuit_snapshots: circuit_bytes.snapshot_bytes(),
            bytes_feed_sets,
            bytes_pk_dict,
            bytes_electric_adapter,
        }
    }

    pub async fn get_shape(&self, id: &str) -> Option<ShapeRecord> {
        self.state.lock().await.shapes.get(id).cloned()
    }

    /// How many live subscriptions a shape has (`GET /shapes/{id}` — ADR-0008).
    ///
    /// The COUNT, not the ids: an operator needs to know why a shape will not go dormant, and the
    /// ids are other callers' handles. Deliberately not a retention touch, like the rest of that
    /// route.
    pub async fn subscription_count(&self, id: &str) -> usize {
        self.state.lock().await.feed_shares.get(id).map(FeedShare::refcount).unwrap_or(0)
    }

    /// The lease window a subscription must renew within
    /// (`ELECTRIC_CIRCUITS_SUBSCRIPTION_LEASE_SECS`), handed to clients in every create response.
    /// This is independent from the dormancy idle timeout.
    pub fn lease_seconds(&self) -> u64 {
        self.retention.subscription_lease_timeout.as_secs()
    }

    /// The change-log position up to which the sequencer has processed (global — all tables share
    /// the single ordered log), or `None` if the sequencer is not running yet.
    pub async fn table_offset(&self, _table: &TableRef) -> Option<LogPosition> {
        let st = self.state.lock().await;
        st.sequencer.as_ref().map(|s| s.processed.lock().unwrap().clone())
    }

    /// The ingestor's own change-log position: the CURRENT segment and the tail offset of its last
    /// append (`GET /replication/lsn` → `changes`). Additive observability — and the only place a
    /// consumer can learn which segment is current, which is what a convergence barrier HEADs for
    /// the tail.
    pub fn changes_position(&self) -> LogPosition {
        self.changes.state().position()
    }

    /// Immutable query generation that names the current change-log namespace.  Consumers bind
    /// a stored position to this value; a mismatch is a terminal re-sync signal, never a guess at
    /// whether an identically numbered segment exists in a different generation.
    pub fn changes_generation(&self) -> &str {
        self.changes.ds().query_generation()
    }

    /// Every retained change-log segment and the unix second it became current.
    pub fn changes_segments(&self) -> std::collections::BTreeMap<u32, u64> {
        self.changes.state().segments()
    }

    /// Record a caller-owned replay position after its catalog event is durable. A retried PUT at
    /// the same position is idempotent; a backwards position is refused rather than widening the
    /// deletion floor after a caller has declared it finished.
    pub async fn pin_consumer(&self, id: String, position: LogPosition) -> Result<()> {
        if id.is_empty() || id.len() > 128 || !id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-') {
            bail!("consumer id must use only alphanumeric, '_' or '-', and be at most 128 bytes");
        }
        if position.segment > self.changes.state().current()
            || (position.segment != self.changes.state().current()
                && !self.changes.state().segments().contains_key(&position.segment))
        {
            bail!("consumer position names an unretained segment: {}", position.segment);
        }
        let previous = self.consumers.lock().unwrap().get(&id).cloned();
        if previous.as_ref().is_some_and(|old| position < *old) {
            bail!("consumer position cannot move backwards");
        }
        if previous.as_ref() == Some(&position) {
            return Ok(());
        }
        self.catalog_tx
            .send_durable(CatalogEvent::ConsumerPinned { id: id.clone(), position: position.clone() })
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        self.consumers.lock().unwrap().insert(id, position);
        Ok(())
    }

    /// Release an explicit external consumer pin, durably before the registry forgets it.
    pub async fn remove_consumer(&self, id: &str) -> Result<()> {
        if !self.consumers.lock().unwrap().contains_key(id) {
            return Ok(());
        }
        self.catalog_tx
            .send_durable(CatalogEvent::ConsumerEvicted { id: id.to_string() })
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        self.consumers.lock().unwrap().remove(id);
        Ok(())
    }

    /// Snapshot consumers with their segment lag against the current open segment.
    pub fn consumers(&self) -> Vec<(String, LogPosition, u32)> {
        let current = self.changes.state().current();
        self.consumers
            .lock()
            .unwrap()
            .iter()
            .map(|(id, pos)| (id.clone(), pos.clone(), current.saturating_sub(pos.segment)))
            .collect()
    }

    /// Read one positioned page of the external change-log contract. The ingestor remains its only
    /// writer; this is deliberately a byte-level view over the same segment the sequencer reads.
    pub async fn read_changes(&self, segment: u32, offset: &str, live: bool) -> anyhow::Result<crate::ds::ReadResult> {
        self.changes.ds().read(&crate::changelog::segment_path(segment), offset, live).await
    }

    /// The table's current circuit topology (shared families + standalone count), or `None` if no
    /// tailer exists.
    pub async fn table_stats(&self, table: &TableRef) -> Option<TableStats> {
        let st = self.state.lock().await;
        st.sequencer.as_ref().and_then(|s| s.stats.lock().unwrap().get(table).cloned())
    }
}
