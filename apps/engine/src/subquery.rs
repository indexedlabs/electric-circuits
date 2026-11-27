//! Subquery support: shared, incrementally-maintained inner-set **nodes** and the cross-table
//! registry that moves outer rows in/out as inner sets change.
//!
//! A shape whose `WHERE` contains `col IN (SELECT proj FROM inner WHERE pred)` (or `NOT IN`) cannot be
//! evaluated row-locally — membership depends on the inner subquery's result set. We maintain that set
//! once per distinct subquery (keyed by a canonical [`SubquerySig`]) as a [`SubqueryNode`]: a map from
//! projected value to the set of inner-row primary keys producing it. A value is "in the set" iff its
//! contributor set is non-empty; tracking contributor pks (not a bare count) makes maintenance
//! reconcile-by-identity — set a row's presence to equal `match(row)` regardless of history.
//!
//! Identical subqueries share one node (the memory win + the sharing the design calls for). Nodes feed
//! dependents — outer shapes or *parent* nodes (a node whose inner `pred` itself references this node) —
//! along edges recorded by connecting column. When a value flips (a bucket goes empty↔non-empty), the
//! registry queries the dependent rows referencing that value and re-evaluates them (see
//! `on_table_delta`, added in a later step). This file (step 6) is the pure in-memory core: node
//! maintenance + the [`SubqueryEval`] read view. No Postgres, no streams yet.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;

use crate::value::{Tup2, ZWeight};
use anyhow::{Context, Result};

use crate::ds::{DsClient, Envelope};
use crate::heap_size::HeapSize;
use crate::pk_dict::PkDict;
use crate::predicate::{CompiledPredicate, PredicateJson, SubqueryCollector, SubqueryEval, SubquerySig, subquery_sig};
use crate::schema::TableSchema;
use crate::subq_circuit::{Assert, Assertions, PkKey};
use crate::table_ref::TableRef;
use crate::value::{Row, Value};

/// Direction of a value-membership change on a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlipDir {
    /// A value's contributor set went empty → non-empty (the value entered the inner set).
    Enter,
    /// A value's contributor set went non-empty → empty (the value left the inner set).
    Leave,
}

/// A single value-membership change emitted by [`SubqueryNode::reconcile_row`]. `value` may be
/// [`Value::Null`] (the null bucket — relevant to `NOT IN`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Flip {
    pub value: Value,
    pub dir: FlipDir,
}

/// One maintained inner subquery: `SELECT proj_col FROM inner_table WHERE pred`, as a value set.
///
/// The set itself lives in the **membership circuit** (`crate::subq_circuit`) as the
/// `(node_id, value)` slice of one shared relation; the node keeps only the host-side reverse
/// index (`pk_value`) that makes maintenance reconcile-by-identity — evaluation can depend on
/// *other* nodes' current sets (nested `IN`), so a row's tuple is not a pure function of the
/// row and exact retraction needs the remembered old value.
pub struct SubqueryNode {
    pub sig: SubquerySig,
    pub inner_table: TableRef,
    /// Column index (in `inner_table`) of the projected value.
    pub proj_col: usize,
    /// Column index (in `inner_table`) of the primary key — used to key contributors.
    pub pk_col: usize,
    /// The inner predicate; may reference deeper nodes (evaluated via [`SubqueryEval`]).
    pub pred: Arc<CompiledPredicate>,
    /// The inner `where` as raw JSON (for seeding SQL, which must emit nested `IN (SELECT …)`).
    pub where_json: Option<PredicateJson>,
    /// The node's backfill-snapshot fence: inner deltas already visible to the seed snapshot are
    /// skipped (xid visibility, LSN fallback — see [`crate::pg::SnapshotGate`]).
    pub gate: crate::pg::SnapshotGate,
    /// `Some` while the node is being seeded (three-phase create): raw inner-table deltas that
    /// arrive mid-seed are buffered here and replayed through the seed gate at install — never
    /// applied to a half-seeded set (a snapshot row landing after a fresher delta would be a
    /// stale overwrite). `None` = live.
    ///
    /// Each buffered delta keeps the commit stamp it arrived under ([`BufferedDelta`], the same
    /// type the outer shape's buffer uses), because the replay is a LIVE per-pk decision and is
    /// stamped like every other one (see [`SubqueryNode::recent`]). Today no re-derivation can be
    /// in flight for a node that is mid-seed — [`SubqueryRegistry::queue_node_deferred`] defers it,
    /// and only a freshly minted node ever gets a seed buffer — so the stamp makes the replay's
    /// freshness explicit instead of an unstated consequence of that ordering.
    pub(crate) seed_buffer: Option<Vec<BufferedDelta>>,
    /// Re-derivations a CHILD node's flip aimed at this node while it was still seeding
    /// (`seed_buffer.is_some()`), replayed after the seed is installed — see
    /// [`SubqueryRegistry::queue_node_deferred`]. The parent-node analogue of
    /// [`PendingSubqueryShape::deferred`]: reconciling against a pre-seed (empty) set would
    /// derive nothing and the seed would then overwrite the change with the older snapshot.
    pub(crate) deferred: VecDeque<DeferredNodeWork>,
    /// The node's key in the membership circuit (registry-assigned, unique per live node).
    pub node_id: i64,
    /// The template this node is a bind of (see [`crate::predicate::subquery_template`]).
    pub template_key: String,
    /// The lifted parameter literals, positionally aligned with the template's `param_cols`.
    pub(crate) bind: Row,
    /// Number of dependents (shapes + parent nodes) referencing this node; drop the node at 0.
    pub refcount: usize,
    /// **Per-pk recency, for the duration of a node query-back only** — inner-row pk (the key
    /// [`SubqueryRegistry::assert_node_row`] and [`SubqueryRegistry::apply_node_evals`] work in)
    /// → the commit stamp `(lsn, xid)` of the LIVE contribution decision most recently taken for
    /// that pk on this node.
    ///
    /// The node analogue of [`SubqueryShape::recent`], and needed for the same reason: a parent
    /// node's re-derivation ([`requery_and_reconcile_parent`]) reads its inner rows from Postgres
    /// with the registry lock RELEASED, so a direct inner-table change committed after that read's
    /// snapshot is reconciled by [`SubqueryRegistry::on_table_delta`] step 1 in between — and the
    /// re-derivation would then re-assert the old contribution last. That is not a stream append,
    /// but it changes maintained node state, and the DEPENDENT shape appends what the node's flips
    /// say: the divergence is the same permanent one, one level down.
    ///
    /// Populated ONLY while [`inflight_querybacks`](Self::inflight_querybacks) `> 0` and cleared
    /// when it returns to zero — the steady state carries no per-pk map at all.
    pub(crate) recent: HashMap<String, (u64, Option<u64>)>,
    /// How many re-derivations of this node are between their registry-lock release and their
    /// reconcile. Non-zero is exactly the window `recent` has to cover.
    pub(crate) inflight_querybacks: u32,
}

impl HeapSize for SubqueryNode {
    /// `pred` (`Arc<CompiledPredicate>`) is shared with the registry's compiled evaluators, not
    /// uniquely owned by this node — skipped, like every other `Arc<...>` field in this module.
    /// `recent` is bounded by the pks touched while a re-derivation is in flight and is dropped
    /// when the last one finishes, but it is owned heap while it lives, so it is counted;
    /// `inflight_querybacks` (`u32`) is inline.
    fn heap_bytes(&self) -> usize {
        self.sig.heap_bytes()
            + self.inner_table.heap_bytes()
            + self.where_json.heap_bytes()
            + self.gate.heap_bytes()
            + self.seed_buffer.heap_bytes()
            + self.deferred.heap_bytes()
            + self.template_key.heap_bytes()
            + self.bind.heap_bytes()
            + self.recent.heap_bytes()
    }
}

impl SubqueryNode {
    pub fn new(
        sig: SubquerySig,
        inner_table: TableRef,
        proj_col: usize,
        pk_col: usize,
        pred: Arc<CompiledPredicate>,
        node_id: i64,
    ) -> Self {
        SubqueryNode {
            sig,
            inner_table,
            proj_col,
            pk_col,
            pred,
            where_json: None,
            gate: crate::pg::SnapshotGate::passthrough(),
            seed_buffer: None,
            deferred: VecDeque::new(),
            node_id,
            template_key: String::new(),
            bind: Row(Vec::new()),
            refcount: 0,
            recent: HashMap::new(),
            inflight_querybacks: 0,
        }
    }
}

/// One subquery template: the shared evaluation structure for every node (bind) whose inner
/// query differs only in the lifted equality literals — the KeyRouter factoring applied to
/// subqueries. A delta on the inner table is evaluated ONCE per template (residual + param
/// projection), then routed to the single affected bind by hash lookup, instead of one full
/// predicate eval per literal-keyed node.
pub(crate) struct TemplateGroup {
    pub(crate) inner_table: TableRef,
    /// Column index (in `inner_table`) of the projected value (same for every bind).
    proj_col: usize,
    /// The compiled residual (lifted equalities removed, other literals baked in). May contain
    /// nested `IN` leaves, resolved via [`SubqueryEval`] against already-collected child nodes.
    residual: Arc<CompiledPredicate>,
    /// Column indices of the lifted parameters, aligned with each bind `Row`.
    param_cols: Vec<usize>,
    /// bind literals -> the node serving that bind.
    pub(crate) binds: HashMap<Row, SubquerySig>,
    /// pk id -> nodes of this template currently holding a contribution from that pk. An exact
    /// inverted index over the nodes' contributor sets (maintained in lockstep by
    /// `reconcile_node_row`), so a row that stops matching finds its old bind in O(1) instead
    /// of scanning every bind. Keyed by the pk's dictionary id (see [`crate::pk_dict::PkDict`]),
    /// not the pk string — the string lives once in the shared dictionary.
    pk_nodes: HashMap<u32, HashSet<SubquerySig>>,
}

impl HeapSize for TemplateGroup {
    /// `residual` (`Arc<CompiledPredicate>`) is shared across every bind of this template —
    /// skipped, like other `Arc<...>` fields in this module.
    fn heap_bytes(&self) -> usize {
        self.inner_table.heap_bytes()
            + self.param_cols.heap_bytes()
            + self.binds.heap_bytes()
            + self.pk_nodes.heap_bytes()
    }
}

/// Identifies a dependent of a node: an outer shape or a parent node, plus the connecting column on the
/// dependent's table whose value `= the flipped node value` selects the affected rows.
#[derive(Debug, Clone)]
pub enum Dependent {
    /// An outer subquery shape (by registry shape id).
    Shape(String),
    /// A parent node (by signature) whose inner `pred` references this node.
    Node(SubquerySig),
}

impl HeapSize for Dependent {
    fn heap_bytes(&self) -> usize {
        match self {
            Dependent::Shape(id) => id.heap_bytes(),
            Dependent::Node(sig) => sig.heap_bytes(),
        }
    }
}

/// An edge from a node to a dependent: when the node flips `value`, rows of the dependent's table with
/// `connecting_col = value` may change membership.
#[derive(Debug, Clone)]
pub struct Edge {
    pub node_sig: SubquerySig,
    pub dependent: Dependent,
    /// Column index (in the dependent's table) connecting to the node.
    pub connecting_col: usize,
    pub negated: bool,
    /// True iff a NULL entering/leaving the node's set can change this dependent's membership. That is
    /// the case when the `IN` leaf is itself negated (`NOT IN` — SQL: a NULL in the set makes it
    /// UNKNOWN) **or** sits under any `Not{…}` wrapper: with no negation anywhere above the leaf, a
    /// NULL only moves the leaf between FALSE and UNKNOWN, and AND/OR are monotone in
    /// FALSE < UNKNOWN < TRUE, so overall TRUE-ness (inclusion) cannot change. Any negation breaks the
    /// monotonicity, so those dependents must be fully re-derived on a NULL flip.
    pub null_sensitive: bool,
}

impl HeapSize for Edge {
    /// `connecting_col`/`negated`/`null_sensitive` are inline.
    fn heap_bytes(&self) -> usize {
        self.node_sig.heap_bytes() + self.dependent.heap_bytes()
    }
}

/// A registered outer subquery shape: an ordinary materialized shape whose predicate contains
/// `IN (SELECT …)`. The engine emits `upsert`/`delete` envelopes to `stream_path` as membership
/// changes (from outer-row deltas and from inner-set flips).
pub struct SubqueryShape {
    pub shape_id: String,
    pub outer_table: TableRef,
    pub stream_path: String,
    /// The outer predicate (with `InSubquery` leaves resolving against this registry's nodes).
    pub pred: Arc<CompiledPredicate>,
    pub out_cols: Option<Arc<Vec<usize>>>,
    /// The shape's backfill-snapshot fence; outer deltas already visible to the backfill are skipped.
    pub gate: crate::pg::SnapshotGate,
    /// A `changes_only` feed: no backfill, so its stream — and its key set — began EMPTY while its
    /// client holds the rows of a separate snapshot query. For such a feed the key set is not what
    /// the client holds, and the delete gate also opens on proof that a row was a member just
    /// before the change ([`Prior`]). A materialized shape's key set is seeded with its backfill
    /// and is already complete, so it keeps the key set as its only gate.
    pub changes_only: bool,
    /// Envelopes appended to this shape's stream (backfill + live), for the visualizer's per-node
    /// state. Atomic because the append paths hold `&self`.
    pub emitted: std::sync::atomic::AtomicU64,
    /// This shape's key into the host-side per-feed key set ([`crate::subq_feed::FeedSet`],
    /// `feed_sets`). The set replaces the old `known_members` set: a delete is delivered iff
    /// the set actually retracts, so a "not a member" verdict for a pk the stream never
    /// contained is structurally a no-op — the wake-storm gate (PR #30) with no filter to keep
    /// in sync.
    pub(crate) feed_id: i64,
    /// **Per-pk recency, for the duration of a query-back only** — `pk_dict` id → the commit
    /// stamp `(lsn, xid)` of the LIVE outer-row decision most recently applied to this shape's
    /// stream for that pk.
    ///
    /// A membership query-back reads its candidate rows from Postgres *without* the registry
    /// lock and evaluates them when it reacquires it, so a direct outer-row change committed
    /// after that read's snapshot can be evaluated and emitted in between — and the query-back's
    /// older row would then land last on the stream. Native consumers fold by durable-stream
    /// offset, so "last" is "final": permanent divergence from Postgres. Recording the live
    /// decision's stamp here lets the query-back drop exactly those candidates whose live
    /// decision its own snapshot could not have seen (see [`EmissionSource`]).
    ///
    /// Populated ONLY while [`inflight_querybacks`](Self::inflight_querybacks) `> 0` and cleared
    /// when it returns to zero: with no query-back in flight there is no older read to lose to,
    /// so the steady state carries no per-pk map at all.
    pub(crate) recent: HashMap<u32, (u64, Option<u64>)>,
    /// How many query-backs for this shape are between their registry-lock release and their
    /// evaluation. Non-zero is exactly the window `recent` has to cover.
    pub(crate) inflight_querybacks: u32,
    /// **The stream's position**: the highest `headers.lsn` this shape has appended to its own
    /// stream (`0` = nothing placed yet). Every envelope is placed at `max(its commit's LSN, this)`
    /// ([`place`](Self::place)), so a position never falls along the stream.
    ///
    /// A live change is evaluated in commit order and lands at its own commit. A flip's verdict
    /// does not: its query-back runs after the commit that caused it, so the verdict can follow a
    /// NEWER live change to the same key. At its own commit's LSN it would sit below that change,
    /// and a consumer that applies a delta only at or above the position it holds for the key —
    /// every consumer that orders by position — would discard the stream's last word. Offset order
    /// is the engine's truth (append order is evaluation order, and the recency fence keeps the
    /// last evaluation of a key its freshest), and raising the verdict to the stream's position
    /// makes position order agree with it. `docs/ARCHITECTURE.md` §7, *Native envelope positions*,
    /// is the contract consumers read.
    ///
    /// Kept on the shape because the shape is its stream's only writer once the backfill is on it,
    /// and the two share a lifetime: a subquery shape is never dormant, and neither it nor its
    /// stream outlives the engine run that created it. A group member's stream is shared and
    /// ordered by its lane, which keeps the position instead (`engine::emission`); for a group
    /// member this stays `0`.
    pub(crate) position: u64,
}

impl SubqueryShape {
    /// The position of the envelopes the commit at `commit_lsn` caused on this shape's stream, as
    /// `headers.lsn` spells it — advancing [`position`](Self::position) to it. `None` only when
    /// there is no position at all: library mode, where no change has an LSN.
    fn place(&mut self, commit_lsn: u64) -> Option<String> {
        let position = if self.stream_path.starts_with("shape/group-") {
            commit_lsn
        } else {
            self.position = self.position.max(commit_lsn);
            self.position
        };
        (position != 0).then(|| crate::engine::emission::lsn_text(position))
    }
}

impl HeapSize for SubqueryShape {
    /// `pred`/`out_cols` are `Arc`-shared, not uniquely owned; `emitted` (`AtomicU64`),
    /// `feed_id` (`i64`), `inflight_querybacks` (`u32`) and `position` (`u64`) are inline. `recent` is bounded by the
    /// pks touched while a query-back is in flight and is dropped when the last one finishes, but
    /// it is owned heap while it lives, so it is counted.
    fn heap_bytes(&self) -> usize {
        self.shape_id.heap_bytes()
            + self.outer_table.heap_bytes()
            + self.stream_path.heap_bytes()
            + self.gate.heap_bytes()
            + self.recent.heap_bytes()
    }
}

/// Where the candidates handed to [`SubqueryRegistry::emit_for_shapes`] came from — which decides
/// how that evaluation relates in TIME to the other evaluations racing for the same stream.
///
/// The emission tail is absolute per pk (upsert if the row matches now, else an idempotent gated
/// delete), so convergence never depended on ordering *between* pks. It does depend on the last
/// evaluation of a given pk being the freshest one, and that is exactly what a query-back cannot
/// promise on its own: it reads Postgres at one snapshot with the registry lock released.
///
/// * `Live`/`Replay` carry the commit stamp of the outer-table change being evaluated. They are
///   the freshest possible verdict for their pks, so while a query-back is in flight they RECORD
///   that stamp in [`SubqueryShape::recent`].
/// * `QueryBack` carries the gate of the read its candidates came from, and DROPS every candidate
///   whose recorded live stamp is not visible to that gate — the live decision was taken on a
///   commit this read could not have seen, so this row is stale and re-emitting it would make an
///   older read the stream's last word.
pub(crate) enum EmissionSource<'a> {
    /// A live outer-table delta from [`SubqueryRegistry::on_table_delta`] step 2.
    Live { lsn: u64, xid: Option<u64> },
    /// A buffered outer-table delta replayed at install ([`SubqueryRegistry::finish_create`]),
    /// with the stamp it was buffered under. Identical to `Live` in effect — a create's deferred
    /// query-back can run immediately after install, so these decisions must be protected the
    /// same way — and kept a distinct variant because the two call sites are not interchangeable.
    Replay { lsn: u64, xid: Option<u64> },
    /// Candidates read from Postgres by a query-back, under `gate`'s snapshot. `commit_lsn`
    /// belongs to the triggering flip, not the snapshot: its resulting appends hold that receipt,
    /// and are placed at that commit — or at the stream's position, when the stream already
    /// carries a newer one ([`SubqueryShape::position`]).
    QueryBack { gate: &'a crate::pg::SnapshotGate, commit_lsn: u64 },
}

/// Inner-set transitions to look past: `sig -> value -> was the value in the set BEFORE the
/// transition`. A value that left was present before; one that entered was absent. When a value
/// moves more than once the FIRST transition wins, because that is the state before all of them.
pub(crate) type Undone = HashMap<SubquerySig, HashMap<Value, bool>>;

/// What the caller of [`SubqueryRegistry::emit_for_shapes_with`] can prove about its candidates'
/// membership **immediately before** the change being evaluated — the second way the delete gate
/// opens, used only for a [`changes_only`](SubqueryShape::changes_only) feed.
///
/// The first way is the feed's key set ([`crate::subq_feed`]): a delete for a pk the stream itself
/// delivered. A changes-only feed delivered nothing at open, yet its client holds every row a
/// snapshot query returned, and is owed each one that later leaves the scope. The key set cannot
/// say which those are. The change itself can: a row that satisfied the predicate just before the
/// change and does not satisfy it now has left because of it. That is as selective as the key
/// set — a row that was never in the scope proves nothing and still emits nothing, so a busy
/// table cannot wake every idle feed on it (the wake storm the gate exists to prevent).
///
/// The proof is exact for one cause at a time. What it can miss — and what the next snapshot
/// repairs, as it repairs everything today — is a row with TWO causes that do not meet in one
/// evaluation: its membership rests on two inner sets that both drop it in one commit and a later
/// commit reaches the registry before that commit's query-backs run, or the row itself changes
/// in a later commit while a flip that already removed it is still waiting for its query-back.
/// A commit too large for its record ([`CommitScope::saturated`]) reaches the same limit inside
/// itself. Only a key set seeded with what the client holds — a materialized shape — is exact.
#[derive(Clone, Copy)]
pub(crate) enum Prior<'a> {
    /// Nothing beyond the key set.
    Unknown,
    /// An outer-row change: each touched pk's row before this commit (absent: the row did not
    /// exist). The old row is judged against the inner sets as they stood before the commit.
    Images(&'a HashMap<String, Row>),
    /// An inner-set flip's query-back: `value` just left (`left`) or entered node `sig`'s set. The
    /// candidates are the rows that depend on it; one was a member before the flip iff it
    /// satisfies the predicate with the flip — and whatever else its commit did to the inner
    /// sets, while that commit is still the one being ingested — looked past. Where the commit's
    /// record knows the flipped value it decides, not the flip (see [`Before`]).
    Flip { sig: &'a SubquerySig, value: &'a Value, left: bool },
}

impl Prior<'_> {
    /// Was this candidate a member of `pred` just before the change? `row` is its current image;
    /// `commit` is what the commit being evaluated has done to the inner sets so far (empty when
    /// the evaluation belongs to an earlier commit, whose record is gone).
    fn was_member(
        &self,
        pk: &str,
        row: &Row,
        pred: &CompiledPredicate,
        reg: &SubqueryRegistry,
        commit: &Undone,
    ) -> bool {
        match *self {
            Prior::Unknown => false,
            Prior::Images(images) => {
                images.get(pk).is_some_and(|old| pred.matches_ctx(old, &Before { reg, undone: commit, flip: None }))
            }
            Prior::Flip { sig, value, left } => {
                pred.matches_ctx(row, &Before { reg, undone: commit, flip: Some((sig, value, left)) })
            }
        }
    }
}

/// How much one commit's record ([`CommitScope`]) may hold, in estimated heap bytes.
///
/// The record grows with every inner value the commit flips and every removal it proves, and one
/// transaction can do either any number of times: a bulk delete that takes a thousand values out
/// of each of a few hundred inner sets is some hundred thousand transitions. Typical commits
/// record a handful. An entry that would take the record past this is not copied into it: the
/// record is given up for the rest of the commit instead — see [`CommitScope::saturated`] for what
/// that costs. The figure is what the registry's heap accounting reports for the record, hash
/// tables included, so it bounds what the record owns at every moment, not only at rest.
pub(crate) const COMMIT_PROOF_MAX_BYTES: usize = 8 * 1024 * 1024;

/// What the registry remembers about the ONE commit it is ingesting, for [`Prior`]. Replaced when
/// the next commit's first change arrives, so nothing accumulates across commits; a change with no
/// LSN (0) keeps nothing.
#[derive(Default)]
struct CommitScope {
    lsn: u64,
    /// The inner-set transitions the commit has applied so far: what an evaluation looks past to
    /// see the sets as they stood before the commit. Every transition is here, wherever it was
    /// derived — a set whose inner table the commit changed, and a parent set that a flip's walk
    /// re-derived afterwards ([`SubqueryRegistry::note_commit_flip`]).
    undone: Undone,
    /// The removals a changes-only feed has already emitted for this commit, as
    /// `feed -> pk -> still removed`. One commit can prove the same departure more than once — an
    /// outer-row change and the flip behind it, or two flips that each took the row out — and the
    /// stream needs to hear it once.
    ///
    /// A row that comes back into the scope keeps its entry, with the flag cleared, until the
    /// commit ends. Taking the entry out would leave a tombstone in the table, and a table with
    /// tombstones reports less room than it has: its next growth can be far more than the
    /// doubling the budget is asked for ([`table_growth`]). No entry ever leaves, so no table
    /// here has tombstones, and what a returned row holds stays counted against the budget.
    removed: HashMap<i64, HashMap<String, bool>>,
    /// What `undone` and `removed` own — exactly the figure [`HeapSize`] reports for them, hash
    /// tables included — kept as they change, so the budget is asked without walking them.
    bytes: usize,
    /// The record outgrew [`COMMIT_PROOF_MAX_BYTES`] and was given up: for the rest of this commit
    /// nothing is recorded, and an evaluation is judged as one that belongs to an earlier commit
    /// is — by its own cause alone (the flip being propagated, or the row's old image against the
    /// sets as they are now). That is the proof's documented limit, reached early: a row with two
    /// causes in this commit can be missed, a value that entered and left inside it can produce a
    /// delete for a row that was never in the scope, and a departure proven twice is said twice.
    /// None of them removes a row that is in the scope: a delete is only ever sent for a row that
    /// is not a member now.
    saturated: bool,
    /// The most the record has been charged with at any moment.
    #[cfg(test)]
    peak: usize,
}

/// What a hash table with room for `capacity` entries of type `T` owns, as [`crate::heap_size`]
/// counts it.
fn table_bytes<T>(capacity: usize) -> usize {
    (capacity * (std::mem::size_of::<T>() + 1) * 11) / 10
}

/// What one more entry of type `T` can make a hash table of `len` entries and room for `capacity`
/// grow by. An upper bound for a table nothing was ever removed from, which is every table of a
/// [`CommitScope`]: a full one at most doubles (and starts at no more than 8).
fn table_growth<T>(len: usize, capacity: usize) -> usize {
    if len < capacity {
        return 0;
    }
    table_bytes::<T>((capacity * 2).max(8)) - table_bytes::<T>(capacity)
}

type UndoneEntry = (SubquerySig, HashMap<Value, bool>);
type RemovedEntry = (i64, HashMap<String, bool>);

impl CommitScope {
    /// Record one inner-set transition. When a value moves more than once the first transition is
    /// kept: that is its state before all of them.
    ///
    /// The budget is asked BEFORE the value is copied: what the entry would own, and what the
    /// tables can grow by to hold it. A value larger than the whole budget — Postgres text can be
    /// — is therefore never in the record, not even between the copy and the check.
    fn note_transition(&mut self, sig: &SubquerySig, flip: &Flip) {
        if self.saturated {
            return;
        }
        let values = self.undone.get(sig);
        if values.is_some_and(|values| values.contains_key(&flip.value)) {
            return;
        }
        let (len, capacity) = values.map_or((0, 0), |values| (values.len(), values.capacity()));
        let mut more = flip.value.heap_bytes() + table_growth::<(Value, bool)>(len, capacity);
        if values.is_none() {
            more += sig.len() + table_growth::<UndoneEntry>(self.undone.len(), self.undone.capacity());
        }
        if !self.admit(more) {
            return;
        }
        // Charged with what was actually taken: the copies' own heap, and each table's real growth.
        let mut taken = 0;
        if !self.undone.contains_key(sig) {
            let before = self.undone.capacity();
            let sig = sig.clone();
            taken += sig.heap_bytes();
            self.undone.insert(sig, HashMap::new());
            taken += table_bytes::<UndoneEntry>(self.undone.capacity()) - table_bytes::<UndoneEntry>(before);
        }
        let values = self.undone.get_mut(sig).expect("inserted above");
        let before = values.capacity();
        let value = flip.value.clone();
        taken += value.heap_bytes();
        values.insert(value, matches!(flip.dir, FlipDir::Leave));
        taken += table_bytes::<(Value, bool)>(values.capacity()) - table_bytes::<(Value, bool)>(before);
        self.charge(taken);
    }

    /// Whether this is the first time the commit removes `pk` from `feed_id`, recording it if so.
    /// A removal the record has no room to remember — or that comes after the record was given
    /// up — is the first every time: it is said, perhaps twice, never swallowed.
    fn first_removal(&mut self, feed_id: i64, pk: &str) -> bool {
        if self.saturated {
            return true;
        }
        if let Some(removed) = self.removed.get_mut(&feed_id).and_then(|pks| pks.get_mut(pk)) {
            // Known to the record: said already, or back in the scope since and so news again.
            return !std::mem::replace(removed, true);
        }
        let pks = self.removed.get(&feed_id);
        let (len, capacity) = pks.map_or((0, 0), |pks| (pks.len(), pks.capacity()));
        let mut more = pk.len() + table_growth::<(String, bool)>(len, capacity);
        if pks.is_none() {
            more += table_growth::<RemovedEntry>(self.removed.len(), self.removed.capacity());
        }
        if !self.admit(more) {
            return true;
        }
        let mut taken = 0;
        if !self.removed.contains_key(&feed_id) {
            let before = self.removed.capacity();
            self.removed.insert(feed_id, HashMap::new());
            taken += table_bytes::<RemovedEntry>(self.removed.capacity()) - table_bytes::<RemovedEntry>(before);
        }
        let pks = self.removed.get_mut(&feed_id).expect("inserted above");
        let before = pks.capacity();
        let pk = pk.to_string();
        taken += pk.heap_bytes();
        pks.insert(pk, true);
        taken += table_bytes::<(String, bool)>(pks.capacity()) - table_bytes::<(String, bool)>(before);
        self.charge(taken);
        true
    }

    /// `pk` is back in `feed_id`'s scope: a later departure in this commit is news again. The entry
    /// stays where it is (see [`Self::removed`]); only its flag changes.
    fn forget_removal(&mut self, feed_id: i64, pk: &str) {
        if let Some(removed) = self.removed.get_mut(&feed_id).and_then(|pks| pks.get_mut(pk)) {
            *removed = false;
        }
    }

    /// Whether `more` bytes still fit. When they do not the record is given up — before the entry
    /// that would not fit is copied into it.
    fn admit(&mut self, more: usize) -> bool {
        if self.bytes.saturating_add(more) <= COMMIT_PROOF_MAX_BYTES {
            return true;
        }
        tracing::warn!(
            event = "subquery_commit_record_given_up",
            commit_lsn = %crate::engine::emission::lsn_text(self.lsn),
            record_bytes = self.bytes,
            refused_bytes = more,
            budget_bytes = COMMIT_PROOF_MAX_BYTES,
            "one commit's record of inner-set transitions would outgrow its budget; for the rest of this commit \
             a changes-only feed's removals are proven one cause at a time"
        );
        self.undone = Undone::new();
        self.removed = HashMap::new();
        self.bytes = 0;
        self.saturated = true;
        false
    }

    fn charge(&mut self, taken: usize) {
        self.bytes += taken;
        #[cfg(test)]
        {
            self.peak = self.peak.max(self.bytes);
        }
    }
}

impl HeapSize for CommitScope {
    fn heap_bytes(&self) -> usize {
        self.undone.heap_bytes() + self.removed.heap_bytes()
    }
}

/// The registry's inner sets as they stood before some transitions (see [`Undone`]).
///
/// The commit's record decides first: it holds each value's state before ALL of the commit's
/// transitions, which is the state the client's snapshot saw. The flip being propagated decides
/// its own value only where the record has nothing for it — the evaluation belongs to an earlier
/// commit, whose record is gone. Read the other way round, a value that entered and left inside
/// one commit would count as present before it (its `Leave` flip says so), and a row that was
/// never in the scope would be removed.
struct Before<'a> {
    reg: &'a SubqueryRegistry,
    undone: &'a Undone,
    flip: Option<(&'a SubquerySig, &'a Value, bool)>,
}

impl SubqueryEval for Before<'_> {
    fn contains(&self, sig: &SubquerySig, value: &Value) -> bool {
        if let Some(&was_present) = self.undone.get(sig).and_then(|values| values.get(value)) {
            return was_present;
        }
        if let Some((flipped, flipped_value, left)) = self.flip
            && flipped == sig
            && flipped_value == value
        {
            return left;
        }
        self.reg.contains(sig, value)
    }
    fn has_null(&self, sig: &SubquerySig) -> bool {
        self.contains(sig, &Value::Null)
    }
}

/// One table delta buffered by a mid-create shape (outer table) or a mid-seed node (inner table),
/// WITH the commit stamp it arrived under.
///
/// The stamp is retained (it used to be dropped, leaving the install replay unstamped) because the
/// replay is a live decision like any other: a create hands back deferred work that can run a
/// query-back the moment the shape is installed — or a node re-derivation the moment the seed is —
/// and that query-back must be able to tell that the replayed decision is newer than its own read.
/// See [`EmissionSource::Replay`] for the shape half and [`SubqueryNode::recent`] for the node half.
pub(crate) struct BufferedDelta {
    /// Commit LSN of the change (`0` = unknown).
    lsn: u64,
    /// Commit xid, when the source stamped one — the exact half of the visibility test.
    xid: Option<u64>,
    delta: Vec<Tup2<Row, ZWeight>>,
}

impl HeapSize for BufferedDelta {
    /// `lsn`/`xid` are inline.
    fn heap_bytes(&self) -> usize {
        self.delta.heap_bytes()
    }
}

/// A `TableSchema` lookup shared with the engine's compiled schema.
pub type SchemaMap = Arc<HashMap<TableRef, TableSchema>>;

/// Cheap, sampler-safe registry cardinalities (see [`SubqueryRegistry::mem_totals`]).
pub struct MemTotals {
    pub nodes: usize,
    /// Total contributor pks across all nodes (the dominant per-node state).
    pub contributors: usize,
    pub distinct: usize,
    pub shapes: usize,
    pub edges: usize,
    /// Total pks delivered across all shapes' host-side feed sets (Task 2.2).
    pub feed_entries: usize,
}

/// Per-node introspection (served at `GET /subqueries`).
#[derive(Clone, serde::Serialize)]
pub struct NodeStat {
    pub sig: SubquerySig,
    pub inner_table: TableRef,
    pub distinct_values: usize,
    pub refcount: usize,
    /// The template this node is a bind of — equal across nodes that differ only in lifted
    /// equality literals (template-level sharing).
    pub template: String,
}

/// Propagation work that reached a shape while it was still being created, deferred until it is
/// installed (see [`PendingSubqueryShape::deferred`]). Every item retains its originating
/// commit — its LSN and, where known, its transaction id — and coalescing keeps the earliest origin
/// whose effects this re-derivation covers, as a PAIR: the emission stamps the LSN into
/// `headers.lsn` and the txid into `headers.txid`, so the two must name the same transaction.
pub enum DeferredShapeWork {
    /// An inner-set value flipped: re-derive the outer rows with `connecting_col = value`.
    Value { connecting_col: usize, value: Value, txid: Option<String>, commit_lsn: u64 },
    /// A NULL flip on a NULL-sensitive edge: re-derive every outer row.
    Full { txid: Option<String>, commit_lsn: u64 },
}

impl DeferredShapeWork {
    pub(crate) fn commit_lsn(&self) -> u64 {
        match self {
            Self::Value { commit_lsn, .. } | Self::Full { commit_lsn, .. } => *commit_lsn,
        }
    }

    fn txid(&self) -> &Option<String> {
        match self {
            Self::Value { txid, .. } | Self::Full { txid, .. } => txid,
        }
    }

    /// Coalesce `other` into this item, keeping whichever origin is earlier — its LSN and its txid
    /// together. Lowering the LSN alone would leave the txid of a LATER commit beside the earlier
    /// position whenever flip workers queue out of commit order, and the envelope this item becomes
    /// would then name two different transactions. An earlier origin whose txid is unknown (`None`)
    /// replaces a known one: the emission then resolves the txid from that earlier LSN.
    fn retain_earliest(&mut self, other: &DeferredShapeWork) {
        let (other_lsn, other_txid) = (other.commit_lsn(), other.txid());
        match self {
            Self::Value { commit_lsn, txid, .. } | Self::Full { commit_lsn, txid, .. } => {
                if other_lsn < *commit_lsn {
                    *commit_lsn = other_lsn;
                    txid.clone_from(other_txid);
                }
            }
        }
    }
}

impl HeapSize for DeferredShapeWork {
    fn heap_bytes(&self) -> usize {
        match self {
            DeferredShapeWork::Value { value, txid, .. } => value.heap_bytes() + txid.heap_bytes(),
            DeferredShapeWork::Full { txid, .. } => txid.heap_bytes(),
        }
    }
}

/// Propagation work that reached a PARENT NODE while it was still being seeded, deferred until its
/// seed is installed (see [`SubqueryNode::deferred`]).
///
/// The node analogue of [`DeferredShapeWork`], minus the `txid`: re-deriving a node reconciles an
/// in-memory value set and emits nothing, so there is no envelope for a txid to travel on — the
/// flips it produces carry the change onward to the dependents that do emit. The originating
/// commit LSN is retained for those descendants and their receipt barrier.
pub enum DeferredNodeWork {
    /// A child value flipped: re-derive the parent's inner rows with `connecting_col = value`.
    Value { connecting_col: usize, value: Value, commit_lsn: u64 },
    /// A NULL flip on a NULL-sensitive edge: re-derive every inner row of the parent.
    Full { commit_lsn: u64 },
}

impl DeferredNodeWork {
    pub(crate) fn commit_lsn(&self) -> u64 {
        match self {
            Self::Value { commit_lsn, .. } | Self::Full { commit_lsn, .. } => *commit_lsn,
        }
    }

    fn retain_earliest(&mut self, origin: u64) {
        match self {
            Self::Value { commit_lsn, .. } | Self::Full { commit_lsn, .. } => *commit_lsn = (*commit_lsn).min(origin),
        }
    }
}

impl HeapSize for DeferredNodeWork {
    fn heap_bytes(&self) -> usize {
        match self {
            DeferredNodeWork::Value { value, .. } => value.heap_bytes(),
            DeferredNodeWork::Full { .. } => 0,
        }
    }
}

/// A subquery shape between `begin_create` and `finish_create`: registration exists (so its
/// nodes are refcounted, its edges recorded, and its deltas buffered) but seeding/backfill runs
/// outside the registry lock.
pub struct PendingSubqueryShape {
    pub shape_id: String,
    pub outer_table: TableRef,
    pub stream_path: String,
    pub pred: Arc<CompiledPredicate>,
    pub out_cols: Option<Arc<Vec<usize>>>,
    pub changes_only: bool,
    /// This create's node-refcount log (for exact rollback on failure).
    collect_log: Vec<SubquerySig>,
    /// Outer-table deltas buffered while the backfill runs; replayed through the gate at install,
    /// each with the commit stamp it arrived under (see [`BufferedDelta`]).
    buffer: Vec<BufferedDelta>,
    /// Flips that reached this shape's edges before it was installed, replayed at install.
    ///
    /// Phase A commits the shape's edges, so a flip on a node it SHARES with an already-live shape
    /// is propagated immediately — while phase B is still backfilling and there is no shape to
    /// move. The inner change may have committed after the backfill's snapshot, so those outer rows
    /// are in neither the snapshot nor the flip's reach: dropping the flip loses them for good.
    /// Buffering it here is the same answer the outer `buffer` gives to outer deltas.
    deferred: VecDeque<DeferredShapeWork>,
}

impl HeapSize for PendingSubqueryShape {
    /// `pred`/`out_cols` are `Arc`-shared, not uniquely owned; `changes_only` is inline.
    fn heap_bytes(&self) -> usize {
        self.shape_id.heap_bytes()
            + self.outer_table.heap_bytes()
            + self.stream_path.heap_bytes()
            + self.collect_log.heap_bytes()
            + self.buffer.heap_bytes()
            + self.deferred.heap_bytes()
    }
}

/// What phase B (Postgres I/O, run WITHOUT the registry lock) needs from `begin_create`.
pub struct BeginCreate {
    /// Fresh nodes this create must seed: `(sig, inner_table, inner where-JSON)`.
    pub seeds: Vec<(SubquerySig, TableRef, Option<PredicateJson>)>,
    /// Schema map snapshot for SQL emission.
    pub schemas: SchemaMap,
}

/// What phase C hands the caller to propagate once the create's registry lock is released. All
/// three are effects the create computed but did not deliver, so all three are barrier-covered
/// (`pendingFlips`) and fail closed exactly like a live flip batch.
pub struct FinishedCreate {
    /// Flips produced by replaying the fresh nodes' buffered inner deltas through their seed gates,
    /// each with the buffered change's original commit LSN.
    pub work: VecDeque<(u64, SubquerySig, Flip)>,
    /// Work that reached the SHAPE while it was pending (see [`PendingSubqueryShape::deferred`]),
    /// to run against the now-installed shape.
    pub deferred: VecDeque<DeferredShapeWork>,
    /// Work that reached a fresh PARENT NODE while it was still seeding (see
    /// [`SubqueryNode::deferred`]), to re-derive now that the node's set is real. Each item's flips
    /// are then walked on down the DAG like any other.
    pub node_work: VecDeque<(SubquerySig, DeferredNodeWork)>,
}

/// The cross-table registry of subquery nodes + shapes + edges. Implements [`SubqueryEval`] so a
/// predicate's subquery leaves resolve against the maintained node sets. One per engine, behind a
/// `tokio::Mutex`; every table tailer calls [`on_table_delta`](Self::on_table_delta).
pub struct SubqueryRegistry {
    /// Nodes by canonical signature (shared across identical subqueries).
    pub nodes: HashMap<SubquerySig, SubqueryNode>,
    /// Edges from each node to its dependents, keyed by the node's signature — flip
    /// propagation looks up ONE node's dependents per flip, so this must not be a scan over
    /// every edge in the registry (the propagation-side analogue of template-grouped eval).
    edges: HashMap<SubquerySig, Vec<Edge>>,
    /// Edges appended by the in-flight `begin_create` compile, committed into `edges` only
    /// when the whole registration succeeds (a failed/conflicted compile just clears this —
    /// exact rollback without index bookkeeping).
    staged_edges: Vec<Edge>,
    /// Registered outer subquery shapes by engine shape id.
    pub shapes: HashMap<String, SubqueryShape>,
    /// Necessary-conjunct index over `shapes`, bucketed by outer table — what makes an outer-table
    /// delta cost `O(candidates)` instead of a scan over every registered subquery shape. Kept in
    /// lockstep with `shapes` by [`install_shape`](Self::install_shape) /
    /// [`drop_subquery_shape`](Self::drop_subquery_shape); see [`crate::subq_index`] for the
    /// old-image ∪ new-image probe rule that keeps absolute emission correct.
    shape_index: crate::subq_index::SubqueryShapeIndex,
    /// The membership circuit: every node's value set as one dbsp relation; flip detection is
    /// the circuit's incremental distinct (see `crate::subq_circuit`).
    pub(crate) circuit: crate::subq_circuit::MembershipCircuit,
    /// The global pk dictionary shared by the circuit tier: every contributor / feed key is
    /// `(id, pk_id)` where `pk_id = pk_dict.get_or_insert(pk)`. Ids are minted here (when building
    /// assertions) and resolved back to pk strings here (at the emission seam), so the circuit and
    /// its indexes never store a heap pk string. One instance per engine (per registry).
    pk_dict: Arc<PkDict>,
    /// Next circuit node id (monotonic; ids of dropped nodes are never reused, so a stale
    /// snapshot read can never alias a new node's slice).
    next_node_id: i64,
    /// circuit node id -> node signature (maps circuit flip deltas back to nodes).
    node_by_id: HashMap<i64, SubquerySig>,
    /// Next feed id (per-shape key; monotonic, never reused).
    next_feed_id: i64,
    /// circuit feed id -> shape id (maps feed deltas back to shapes).
    feed_by_id: HashMap<i64, String>,
    /// Host-side per-feed key sets (Task 2.2, dbsp-ds-dh6): per-feed Roaring bitmaps over pk_ids,
    /// implementing the delete gate. A delete is emitted iff `remove(feed_id, pk_id)` returns true.
    /// Mutated only under this registry's lock, synchronously (no `.await`), in the same critical
    /// section as the emission decision — so the emission decision and the bitmap transition are
    /// one indivisible step.
    feed_sets: crate::subq_feed::FeedSet,
    /// Shared evaluation templates (see [`TemplateGroup`]), keyed by
    /// [`crate::predicate::subquery_template`]'s key.
    pub(crate) templates: HashMap<String, TemplateGroup>,
    /// Nodes created but not yet seeded from Postgres (deepest-first).
    pending_seed: Vec<SubquerySig>,
    /// Shapes between `begin_create` and `finish_create` (the three-phase create): their
    /// outer-table deltas are buffered here and replayed through the shape's gate at install.
    pending_shapes: Vec<PendingSubqueryShape>,
    /// Every node-refcount increment made by the in-flight `create_subquery_shape` (one entry per
    /// `collect()` call). On failure the create is rolled back exactly: each logged sig is decremented
    /// once, and nodes that return to zero are removed. The registry mutex is held for the phase-A
    /// compile; the log then moves into that create's pending entry before releasing the lock.
    collect_log: Vec<SubquerySig>,
    /// The commit now being ingested, as [`Prior`] needs it (see [`CommitScope`]).
    commit: CommitScope,
    ds: DsClient,
    pg_url: Option<String>,
    pg_pool: Option<crate::pg::Pool>,
    /// Test-only stand-in for Postgres behind every flip query-back (see [`CandidateReader`]).
    #[cfg(test)]
    fake_candidates: Option<Arc<tests::FakeCandidates>>,
    /// Test-only hook run under the lock right after every delivery is handed off: lets a test
    /// observe (or act on) the registry exactly as the emission left it, and fail that delivery.
    #[cfg(test)]
    delivery_probe: Option<tests::DeliveryProbe>,
    /// Test-only hook run under the lock right after each flip-driven parent reconcile.
    #[cfg(test)]
    reconcile_probe: Option<tests::ReconcileProbe>,
    schemas: SchemaMap,
    /// Ordered emission lanes (see `engine::emission`): membership envelopes are enqueued
    /// under this registry's lock — per-stream enqueue order = eval order — and land on their
    /// streams asynchronously, covered by the `pendingFlips` barrier. `None` (unit tests)
    /// falls back to a direct reliable append.
    lanes: Option<crate::engine::emission::EmissionLanes>,
    group_lanes: Option<crate::engine::emission::GroupEmissionLanes>,
    /// Postgres commit time (ms since the Unix epoch) of recent live commits, by commit LSN, so a
    /// delivery stamps `headers.commit_ts` on the envelopes a commit caused — including a deferred
    /// query-back's, which carries its triggering commit's LSN. The newest
    /// [`COMMIT_TIMES_RETAINED`] commits are kept; a delivery for an older one goes unstamped,
    /// which costs only its telemetry.
    commit_times: BTreeMap<u64, i64>,
    /// The transaction id of recent commits the registry ingested, by commit LSN, so an emission
    /// that reaches the tail with its commit's LSN but no transaction in hand — a create's replayed
    /// seed flips and deferred node re-derivations carry only their origin — still names the
    /// transaction that caused it (`headers.txid`). The newest [`COMMIT_TIMES_RETAINED`] commits
    /// are kept; an emission for an older one is placed by `headers.lsn` alone.
    commit_txids: BTreeMap<u64, String>,
}

/// How many live commits' times and transaction ids [`SubqueryRegistry`] keeps for stamping
/// deliveries.
const COMMIT_TIMES_RETAINED: usize = 4096;

impl HeapSize for SubqueryRegistry {
    /// `circuit` (the dbsp membership relation) is accounted separately — see
    /// [`SubqueryRegistry::circuit_bytes`]'s `bytes_membership_circuit` measurement — so it is
    /// deliberately excluded here to avoid double-counting the same state under two `/memory`
    /// fields. The `pk_dict` is likewise accounted separately (`bytes_pk_dict`, see
    /// [`SubqueryRegistry::pk_dict_bytes`]) — `Arc`-shared and reported once. `feed_sets` is
    /// likewise excluded, accounted separately as `bytes_feed_sets` (see
    /// [`SubqueryRegistry::feed_sets_bytes`]). `ds` (a client
    /// handle) and `schemas` (`Arc`-shared with the engine's compiled
    /// schema) are not uniquely owned; `lanes` holds channel senders, not owned data;
    /// `next_node_id`/`next_feed_id` are inline counters. `commit` is the record of the commit
    /// being ingested: owned, bounded by [`COMMIT_PROOF_MAX_BYTES`], and replaced per commit.
    fn heap_bytes(&self) -> usize {
        self.nodes.heap_bytes()
            + self.edges.heap_bytes()
            + self.staged_edges.heap_bytes()
            + self.shapes.heap_bytes()
            + self.shape_index.heap_bytes()
            + self.node_by_id.heap_bytes()
            + self.feed_by_id.heap_bytes()
            + self.templates.heap_bytes()
            + self.pending_seed.heap_bytes()
            + self.pending_shapes.heap_bytes()
            + self.collect_log.heap_bytes()
            + self.commit.heap_bytes()
            + self.pg_url.heap_bytes()
            + self.commit_times.heap_bytes()
            + self.commit_txids.heap_bytes()
    }
}

impl SubqueryRegistry {
    pub fn new(ds: DsClient, pg_url: Option<String>) -> Self {
        let pool = pg_url.as_ref().map(|url| crate::pg::Pool::new(url.clone(), crate::pg::configured_pool_size()));
        Self::with_pool(
            ds,
            pg_url,
            pool,
            crate::subq_circuit::MembershipCircuit::start().expect("membership circuit failed to start"),
        )
    }

    pub(crate) fn with_pool(
        ds: DsClient,
        pg_url: Option<String>,
        pg_pool: Option<crate::pg::Pool>,
        circuit: crate::subq_circuit::MembershipCircuit,
    ) -> Self {
        SubqueryRegistry {
            nodes: HashMap::new(),
            edges: HashMap::new(),
            staged_edges: Vec::new(),
            shapes: HashMap::new(),
            shape_index: crate::subq_index::SubqueryShapeIndex::default(),
            circuit,
            pk_dict: Arc::new(PkDict::new()),
            next_node_id: 1,
            node_by_id: HashMap::new(),
            next_feed_id: 1,
            feed_by_id: HashMap::new(),
            feed_sets: crate::subq_feed::FeedSet::new(),
            templates: HashMap::new(),
            pending_seed: Vec::new(),
            pending_shapes: Vec::new(),
            collect_log: Vec::new(),
            commit: CommitScope::default(),
            ds,
            pg_url,
            pg_pool,
            #[cfg(test)]
            fake_candidates: None,
            #[cfg(test)]
            delivery_probe: None,
            #[cfg(test)]
            reconcile_probe: None,
            schemas: Arc::new(HashMap::new()),
            lanes: None,
            group_lanes: None,
            commit_times: BTreeMap::new(),
            commit_txids: BTreeMap::new(),
        }
    }

    /// Record a live commit's time before its delta is evaluated, so every delivery it causes — now
    /// or from a later query-back — carries `headers.commit_ts`.
    pub(crate) fn note_commit_time(&mut self, commit_lsn: u64, commit_ts: i64) {
        self.commit_times.insert(commit_lsn, commit_ts);
        while self.commit_times.len() > COMMIT_TIMES_RETAINED {
            self.commit_times.pop_first();
        }
    }

    /// Apply a contributor assertion batch to the membership circuit and map its member deltas
    /// back to node signatures. Callers hold the registry lock across the await — the circuit
    /// thread never takes this lock, and awaiting the step is what gives every later membership
    /// read read-your-writes over this batch. (The delete gate is no longer a circuit relation —
    /// it lives in [`crate::subq_feed::FeedSet`], mutated synchronously under this same lock.)
    async fn apply_asserts(&mut self, asserts: Assertions) -> Vec<(SubquerySig, Flip)> {
        if asserts.is_empty() {
            return Vec::new();
        }
        self.circuit
            .apply(asserts)
            .await
            .into_iter()
            .filter_map(|d| {
                let sig = self.node_by_id.get(&d.node_id)?.clone();
                let dir = if d.delta > 0 { FlipDir::Enter } else { FlipDir::Leave };
                Some((sig, Flip { value: d.value, dir }))
            })
            .collect()
    }

    /// Build one node's contributor assertion for `pk` and keep the template's `pk_nodes`
    /// inverted index in lockstep. `pk_nodes` IS the record of presence per template (the
    /// circuit's upsert map is the record of the value), so all presence transitions go
    /// through here. Inserts always assert (idempotent; a changed value must flow); absent
    /// stays quiet unless the node actually held the pk.
    fn assert_node_row(&mut self, sig: &SubquerySig, pk: &str, present: Option<Value>) -> Option<Tup2<PkKey, Assert>> {
        let (node_id, tkey) = {
            let node = self.nodes.get(sig)?;
            (node.node_id, node.template_key.clone())
        };
        let pk_id = self.pk_dict.get_or_insert(pk);
        let key = PkKey { id: node_id, pk: pk_id };
        match present {
            Some(v) => {
                if let Some(tpl) = self.templates.get_mut(&tkey) {
                    tpl.pk_nodes.entry(pk_id).or_default().insert(sig.clone());
                }
                Some(Tup2(key, Assert::Insert(v)))
            }
            None => {
                let had = self
                    .templates
                    .get_mut(&tkey)
                    .and_then(|tpl| {
                        let set = tpl.pk_nodes.get_mut(&pk_id)?;
                        let had = set.remove(sig);
                        if set.is_empty() {
                            tpl.pk_nodes.remove(&pk_id);
                        }
                        Some(had)
                    })
                    .unwrap_or(false);
                had.then_some(Tup2(key, Assert::Delete))
            }
        }
    }

    /// Assert a batch of per-pk evaluations against ONE node and return the resulting flips
    /// (seeding replay and flip-driven parent re-derivations, where the caller already
    /// evaluated the node's full predicate per row).
    async fn apply_node_evals(&mut self, sig: &SubquerySig, evals: Vec<(String, Option<Value>)>) -> Vec<Flip> {
        let mut asserts = Assertions::default();
        for (pk, pv) in evals {
            asserts.contributors.extend(self.assert_node_row(sig, &pk, pv));
        }
        // Every assertion belongs to `sig`, so the sig on each flip is redundant here.
        let flips = self.apply_asserts(asserts).await;
        flips.into_iter().map(|(_, f)| f).collect()
    }

    /// Test seam (never compiled into the engine): assert one contributor onto a node exactly as
    /// phase C's seed install does, so a cancellation test can park a create with a fresh node
    /// already holding real membership-circuit state.
    #[cfg(test)]
    pub(crate) async fn assert_seed_row_for_test(&mut self, sig: &SubquerySig, pk: &str, value: Value) {
        self.apply_node_evals(sig, vec![(pk.to_string(), Some(value))]).await;
    }

    /// The node's current distinct-value count, read from the circuit snapshot.
    pub(crate) fn circuit_distinct(&self, node_id: i64) -> usize {
        self.circuit.values_for_node(node_id, 0).0
    }

    pub(crate) fn set_lanes(&mut self, lanes: crate::engine::emission::EmissionLanes) {
        self.lanes = Some(lanes);
    }

    pub(crate) fn set_group_lanes(&mut self, lanes: crate::engine::emission::GroupEmissionLanes) {
        self.group_lanes = Some(lanes);
    }

    /// Deliver membership envelopes to a shape stream in **evaluation order**: enqueue on the
    /// stream's emission lane while the caller holds this registry's lock (per-stream FIFO ⇒
    /// append order = eval order — the "data in the right place" invariant). Without lanes
    /// (unit tests) this awaits a direct reliable append, the pre-lane behavior.
    async fn deliver(&mut self, stream_path: &str, envs: Vec<Envelope>, commit_lsn: u64) -> Result<()> {
        self.deliver_envelopes(stream_path, envs, commit_lsn).await?;
        #[cfg(test)]
        if let Some(probe) = self.delivery_probe.clone() {
            probe(self, stream_path)?;
        }
        Ok(())
    }

    async fn deliver_envelopes(&mut self, stream_path: &str, mut envs: Vec<Envelope>, commit_lsn: u64) -> Result<()> {
        if let Some(&commit_ts) = self.commit_times.get(&commit_lsn) {
            for env in &mut envs {
                env.headers.commit_ts = Some(commit_ts);
            }
        }
        if stream_path.starts_with("shape/group-") {
            if let Some(lanes) = &self.group_lanes {
                lanes.push(stream_path, envs, commit_lsn);
                return Ok(());
            }
        }
        match &self.lanes {
            Some(l) => l.enqueue(stream_path, envs, commit_lsn),
            None => {
                self.ds.append_reliable(stream_path, &envs).await?;
            }
        }
        Ok(())
    }

    pub fn set_schemas(&mut self, schemas: SchemaMap) {
        self.schemas = schemas;
    }

    /// Does any node's inner table or any shape's outer table equal `table`? (Fast skip for tailers of
    /// tables not involved in any subquery.)
    pub fn touches(&self, table: &TableRef) -> bool {
        // Called once per replicated envelope, so the shape half is answered by the conjunct
        // index's table buckets in O(1) rather than a scan over every registered shape — the same
        // reason step 2 of `on_table_delta` is indexed. Nodes are shared (one per distinct inner
        // query, orders of magnitude fewer than shapes) and `pending_shapes` holds only in-flight
        // creates, so those two stay linear.
        self.shape_index.has_table(table.as_str())
            || self.nodes.values().any(|n| &n.inner_table == table)
            || self.pending_shapes.iter().any(|p| &p.outer_table == table)
    }

    /// Number of maintained nodes (shared inner sets).
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// The live **inner-set index** of one node (the visualizer's "see the index" view): up to `cap`
    /// `(value, contributor-count)` pairs, most-shared first, plus the true distinct count, refcount, and
    /// whether the list was truncated. This is the actual engine-maintained set, not derivable from topology.
    pub fn node_value_index(
        &self,
        sig: &str,
        cap: usize,
    ) -> Option<(usize, usize, Vec<(serde_json::Value, usize)>, bool)> {
        let n = self.nodes.get(sig)?;
        let (distinct, vals) = self.circuit.values_for_node(n.node_id, cap);
        let vals: Vec<(serde_json::Value, usize)> = vals.into_iter().map(|(v, c)| (v.to_json(), c)).collect();
        Some((distinct, n.refcount, vals, distinct > cap))
    }

    /// Memory-relevant registry totals: maintained nodes, total contributor pks across all nodes (the
    /// dominant per-node state — one entry per inner row producing a value), distinct values, shapes,
    /// edges, and total feed entries (pks delivered across all shapes' host-side feed sets). Used by
    /// the memory probe to attribute subquery state growth — cheap enough for the 500ms background
    /// sampler (`mem::spawn_sampler`): everything here is already published/derivable per-node index
    /// state or an O(containers) bitmap-length sum, the same class of walk this method always did.
    ///
    /// Does NOT include the byte-level measurements (`bytes_membership_circuit`, `bytes_feed_sets`)
    /// — those are on-demand diagnostic measurements (`GET /memory` or the slower logger); see
    /// [`Self::circuit_bytes`].
    pub fn mem_totals(&self) -> MemTotals {
        let mut contributors = 0;
        let mut distinct = 0;
        for n in self.nodes.values() {
            let (d, vals) = self.circuit.values_for_node(n.node_id, usize::MAX);
            contributors += vals.iter().map(|(_, c)| c).sum::<usize>();
            distinct += d;
        }
        MemTotals {
            nodes: self.nodes.len(),
            contributors,
            distinct,
            shapes: self.shapes.len(),
            edges: self.edges_count(),
            feed_entries: self.feed_sets.total_entries(),
        }
    }

    /// Measured owned/on-disk bytes of the membership circuit's published snapshots
    /// (`bytes_membership_circuit` and its `bytes_circuit_integral` / `bytes_circuit_snapshots`
    /// split) — the on-demand diagnostic (`GET /memory` or slower logger) counterpart to
    /// [`Self::mem_totals`]. Never
    /// called from the 500ms background sampler.
    ///
    /// Replaces the former `key_count × 88 B` estimate with dbsp's exact per-batch
    /// `approximate_byte_size` (columnar bytes when resident; on-disk file size when spilled —
    /// see `subq_circuit`'s `SpillConfig`). Cheap: reads the two snapshot slots the circuit
    /// already publishes, no circuit round-trip. See [`crate::subq_circuit::CircuitBytes`] for
    /// what each term covers and the (profiler-only) non-published state it does not. Covers only
    /// the contributor relation now — the per-feed key sets left the circuit (see
    /// [`Self::feed_sets_bytes`]).
    pub fn circuit_bytes(&self) -> crate::subq_circuit::CircuitBytes {
        self.circuit.snapshot_bytes()
    }

    /// Diagnostic only: the membership circuit's full dbsp profiler dump —
    /// `(total_used_bytes, total_storage_size, per-operator profile JSON)`. Heavy (profiler
    /// round-trip through the circuit thread); on-demand only (`GET /debug/dbsp-profile`),
    /// never from the 500 ms sampler.
    pub async fn circuit_profile_dump(&self) -> (usize, usize, String) {
        self.circuit.profile_dump().await
    }

    /// Estimated owned heap of the host-side per-feed key sets (`bytes_feed_sets` in `GET /memory`)
    /// — the delete gate's Roaring bitmaps, moved out of the membership circuit in Task 2.2
    /// (dbsp-ds-dh6). A lower-bound owned floor (serialized bitmap payloads + the outer HashMap
    /// backing store). On-demand only (a per-bitmap payload walk); never on the 500ms sampler path.
    pub fn feed_sets_bytes(&self) -> usize {
        self.feed_sets.heap_bytes()
    }

    /// Estimated owned heap of the global pk dictionary (`bytes_pk_dict` in `GET /memory`) — the
    /// once-per-distinct-pk string storage plus its forward/reverse index. Accounted separately
    /// from the registry's own `heap_bytes` (the dictionary is `Arc`-shared and append-only; this
    /// makes the string-interning trade visible). On-demand only — never on the sampler path.
    pub fn pk_dict_bytes(&self) -> usize {
        self.pk_dict.heap_bytes()
    }

    /// Per-node topology for the introspection endpoint: signature, inner table, current distinct value
    /// count, and the dependent refcount. Two shapes referencing the same subquery show one node with
    /// `refcount == 2` (proves sharing).
    pub fn stats(&self) -> Vec<NodeStat> {
        let mut out: Vec<NodeStat> = self
            .nodes
            .values()
            .map(|n| NodeStat {
                sig: n.sig.clone(),
                inner_table: n.inner_table.clone(),
                distinct_values: self.circuit_distinct(n.node_id),
                refcount: n.refcount,
                template: n.template_key.clone(),
            })
            .collect();
        out.sort_by(|a, b| a.sig.cmp(&b.sig));
        out
    }

    /// Live state summaries for every registry-owned graph node (`node:<sig>` inner sets and
    /// subquery `shape:<sid>` sinks), keyed by graph node id — merged into `GET /state` snapshots
    /// and the tailers' SSE `state` events.
    pub fn state_summaries(&self) -> Vec<(String, crate::engine::NodeStateSummary)> {
        let mut out = Vec::with_capacity(self.nodes.len() + self.shapes.len());
        for (sig, n) in &self.nodes {
            out.push((
                format!("node:{sig}"),
                crate::engine::NodeStateSummary::SubqueryNode {
                    distinct_values: self.circuit_distinct(n.node_id),
                    refcount: n.refcount,
                },
            ));
        }
        for (sid, s) in &self.shapes {
            out.push((
                format!("shape:{sid}"),
                crate::engine::NodeStateSummary::Shape {
                    emitted: s.emitted.load(std::sync::atomic::Ordering::Relaxed),
                },
            ));
        }
        out
    }

    /// Outgoing edges for a node signature. O(that node's own edge list).
    fn edges_of(&self, sig: &SubquerySig) -> Vec<Edge> {
        self.edges.get(sig).cloned().unwrap_or_default()
    }

    /// Return group stream paths reachable from a set of node flips. This is evaluated while the
    /// registry lock is held, before the flip work is handed to a worker, so the source fence can
    /// cover the entire in-flight walk rather than only the append it eventually produces.
    pub(crate) fn group_paths_for_work(&self, work: &VecDeque<(SubquerySig, Flip)>) -> HashSet<String> {
        let mut pending = work.iter().map(|(sig, _)| sig.clone()).collect::<VecDeque<_>>();
        let mut visited = HashSet::new();
        let mut paths = HashSet::new();
        while let Some(sig) = pending.pop_front() {
            if !visited.insert(sig.clone()) {
                continue;
            }
            for edge in self.edges_of(&sig) {
                match edge.dependent {
                    Dependent::Shape(id) => {
                        if let Some(shape) = self.shapes.get(&id)
                            && shape.stream_path.starts_with("shape/group-")
                        {
                            paths.insert(shape.stream_path.clone());
                        }
                    }
                    Dependent::Node(parent) => pending.push_back(parent),
                }
            }
        }
        paths
    }

    pub(crate) fn group_paths_for_shape(&self, shape_id: &str) -> HashSet<String> {
        self.shapes
            .get(shape_id)
            .filter(|shape| shape.stream_path.starts_with("shape/group-"))
            .map(|shape| std::iter::once(shape.stream_path.clone()).collect())
            .unwrap_or_default()
    }

    pub(crate) fn all_group_paths(&self) -> HashSet<String> {
        self.shapes
            .values()
            .filter(|shape| shape.stream_path.starts_with("shape/group-"))
            .map(|shape| shape.stream_path.clone())
            .collect()
    }

    /// Every edge in the registry (introspection only — hot paths use [`edges_of`]).
    pub(crate) fn all_edges(&self) -> impl Iterator<Item = &Edge> {
        self.edges.values().flatten()
    }

    /// Total edge count (memory probe + tests).
    pub fn edges_count(&self) -> usize {
        self.edges.values().map(Vec::len).sum()
    }

    /// Commit one edge (staged during creates; direct in tests).
    fn add_edge(&mut self, e: Edge) {
        self.edges.entry(e.node_sig.clone()).or_default().push(e);
    }

    /// Remove a dying node's edge entries: its outgoing list, plus the incoming edges that
    /// point at it from its children's lists (a dependent edge lives under the CHILD's key).
    fn remove_node_edges(&mut self, sig: &SubquerySig, child_sigs: &[SubquerySig]) {
        self.edges.remove(sig);
        for c in child_sigs {
            if let Some(v) = self.edges.get_mut(c) {
                v.retain(|e| !matches!(&e.dependent, Dependent::Node(s) if s == sig));
                if v.is_empty() {
                    self.edges.remove(c);
                }
            }
        }
    }

    // --- registration -------------------------------------------------------------------------

    /// Register an outer subquery shape: compile the outer predicate (creating/deduping nodes + edges),
    /// seed any new nodes from Postgres, backfill the shape, and record it. Idempotent per shape id.
    ///
    /// **Atomic**: on any failure (unknown table, seed error, backfill/append error) every refcount
    /// increment, node, edge, and pending-seed entry made by this call is rolled back, so a failed
    /// create leaves the registry exactly as it was — no half-registered node that would silently
    /// serve wrong (unseeded) membership to a later identical create.
    /// Phase A of the three-phase create (call under the registry lock; brief, in-memory):
    /// compile the predicate (discovering/refcounting nodes — fresh ones start buffering),
    /// record edges, and register a pending shape that buffers its outer-table deltas from this
    /// moment on (no delta can fall between registration and the phase-B snapshot). Returns
    /// what phase B needs. The engine holds subquery initialization admission through phase C or
    /// rollback: an existing node must already be seeded before a new create can reference it.
    pub fn begin_create(
        &mut self,
        shape_id: &str,
        outer_table: &TableRef,
        stream_path: &str,
        where_json: &PredicateJson,
        out_cols: Option<Arc<Vec<usize>>>,
        changes_only: bool,
    ) -> Result<BeginCreate> {
        let outer_ts = self.schemas.get(outer_table).cloned().context("subquery shape: unknown outer table")?;
        // Compile on a scratch collector so any compile/invariant failure has no effects.
        self.staged_edges.clear();
        self.collect_log.clear();
        let pred = match CompiledPredicate::compile_with(where_json, &outer_ts, self) {
            Ok(p) => Arc::new(p),
            Err(e) => {
                let log = std::mem::take(&mut self.collect_log);
                self.rollback_refs(log);
                return Err(e);
            }
        };
        let log = std::mem::take(&mut self.collect_log);
        // A shared node still seeding means initialization admission was released too early.
        // Fail closed rather than evaluating against incomplete membership.
        let fresh: Vec<SubquerySig> = std::mem::take(&mut self.pending_seed);
        let conflicted =
            log.iter().any(|sig| !fresh.contains(sig) && self.nodes.get(sig).is_some_and(|n| n.seed_buffer.is_some()));
        if conflicted {
            self.rollback_refs(log);
            anyhow::bail!("subquery initialization invariant: shared node is still seeding");
        }
        // Fresh nodes start buffering their inner-table deltas.
        let mut seeds = Vec::with_capacity(fresh.len());
        for sig in &fresh {
            if let Some(n) = self.nodes.get_mut(sig) {
                n.seed_buffer = Some(Vec::new());
                seeds.push((sig.clone(), n.inner_table.clone(), n.where_json.clone()));
            }
        }
        // Shape-level edges (staged with the compile's child edges; committed below).
        for leaf in collect_in_leaves(&pred) {
            self.staged_edges.push(Edge {
                node_sig: leaf.sig,
                dependent: Dependent::Shape(shape_id.to_string()),
                connecting_col: leaf.col,
                negated: leaf.negated,
                null_sensitive: leaf.null_sensitive,
            });
        }
        // Registration is definitely happening: commit the staged edges.
        for e in std::mem::take(&mut self.staged_edges) {
            self.add_edge(e);
        }
        // Pending shape: outer-table deltas buffer from HERE (before the phase-B snapshot).
        self.pending_shapes.push(PendingSubqueryShape {
            shape_id: shape_id.to_string(),
            outer_table: outer_table.clone(),
            stream_path: stream_path.to_string(),
            pred,
            out_cols,
            changes_only,
            collect_log: log,
            buffer: Vec::new(),
            deferred: VecDeque::new(),
        });
        Ok(BeginCreate { seeds, schemas: self.schemas.clone() })
    }

    /// Queue propagation work for a shape that is still being created, if it is one. Returns
    /// whether it was queued — `false` means the shape is installed (or gone) and the caller
    /// proceeds normally.
    ///
    /// Deduped, because the queue is replayed against Postgres one item at a time and a hot inner
    /// table can flip the same value repeatedly during one backfill: a re-derive is absolute, so
    /// running it once at install is running it enough. A full re-derive covers every value, so it
    /// subsumes — and is not added to — anything else in the queue.
    fn queue_deferred(&mut self, shape_id: &str, mut work: DeferredShapeWork) -> bool {
        let Some(pending) = self.pending_shapes.iter_mut().find(|p| p.shape_id == shape_id) else {
            return false;
        };
        if let Some(full) = pending.deferred.iter_mut().find(|w| matches!(w, DeferredShapeWork::Full { .. })) {
            full.retain_earliest(&work);
            return true;
        }
        match &work {
            DeferredShapeWork::Full { .. } => {
                for previous in &pending.deferred {
                    work.retain_earliest(previous);
                }
                pending.deferred.clear();
                pending.deferred.push_back(work);
            }
            DeferredShapeWork::Value { connecting_col, value, .. } => {
                let dup = pending.deferred.iter_mut().find(|w| {
                    matches!(w, DeferredShapeWork::Value { connecting_col: c, value: v, .. }
                        if c == connecting_col && v == value)
                });
                if let Some(dup) = dup {
                    dup.retain_earliest(&work);
                } else {
                    pending.deferred.push_back(work);
                }
            }
        }
        true
    }

    /// Queue a re-derivation aimed at a parent node that is still being SEEDED, if it is one.
    /// Returns whether it was queued — `false` means the node is live (or gone) and the caller
    /// reconciles now.
    ///
    /// A flip must never reconcile a mid-seed node: its set is still empty until phase C installs
    /// the seed, so the re-derivation finds nothing to move — and the seed, taken from a snapshot
    /// OLDER than the flip, is then installed over the change. The node and every dependent below
    /// it stay stale for the life of the shape. Deferring is the same answer
    /// [`queue_deferred`](Self::queue_deferred) gives for a not-yet-installed shape.
    ///
    /// Exactly ONE create can own any seeding node: engine admission serializes creates with overlapping node signatures,
    /// so every seeding node is fresh for one in-flight create and that
    /// create's `finish_create` is the single place this queue is handed back — or, if the create
    /// dies, the node goes and the queue with it.
    ///
    /// Deduped like the shape queue: the replay is absolute, so running a value's re-derive once is
    /// running it enough, and a `Full` re-derive covers every value, so it subsumes the rest.
    fn queue_node_deferred(&mut self, sig: &SubquerySig, mut work: DeferredNodeWork) -> bool {
        let Some(node) = self.nodes.get_mut(sig) else { return false };
        if node.seed_buffer.is_none() {
            return false;
        }
        if let Some(full) = node.deferred.iter_mut().find(|w| matches!(w, DeferredNodeWork::Full { .. })) {
            full.retain_earliest(work.commit_lsn());
            return true;
        }
        match &work {
            DeferredNodeWork::Full { .. } => {
                for previous in &node.deferred {
                    work.retain_earliest(previous.commit_lsn());
                }
                node.deferred.clear();
                node.deferred.push_back(work);
            }
            DeferredNodeWork::Value { connecting_col, value, .. } => {
                let dup = node.deferred.iter_mut().find(|w| {
                    matches!(w, DeferredNodeWork::Value { connecting_col: c, value: v, .. }
                        if c == connecting_col && v == value)
                });
                if let Some(dup) = dup {
                    dup.retain_earliest(work.commit_lsn());
                } else {
                    node.deferred.push_back(work);
                }
            }
        }
        true
    }

    /// Phase C (under the registry lock; brief, in-memory + lane enqueues): install the seeds,
    /// replay every buffered delta through the seed gates, register the shape, and return the work
    /// the caller must propagate (see [`FinishedCreate`]). `seeded` counts the phase-B snapshot
    /// envelopes (for the shape's emitted counter). `seeded_pks` is the backfilled outer rows' pks,
    /// seeding the feed so a later delta that finds one of them no longer matching correctly emits a
    /// delete (not silently dropped as "never known").
    ///
    /// **The invariant that makes the deferred hand-off lossless**: taking the queue off the pending
    /// entry and installing the shape both happen inside ONE synchronous `&mut self` step below, and
    /// the registry mutex is held exclusively for the whole call. So a live flip either queues onto
    /// the pending entry before that step or finds the shape installed after it — there is no state
    /// in between for it to fall through.
    ///
    /// **The invariant that makes it cancellable**: the pending entry stays IN `pending_shapes`
    /// across every await here, so the registry — not this future's stack — owns the create's
    /// rollback state (compile log, buffers, fresh nodes) for the whole of phase C. A create whose
    /// future is dropped at one of these awaits therefore leaves a pending entry
    /// [`abort_create`](Self::abort_create) can unwind exactly, node seeds already asserted into the
    /// membership circuit included. After the atomic tail the create is an ordinary installed shape,
    /// which the same `abort_create` unwinds through the ordinary drop path.
    pub async fn finish_create(
        &mut self,
        shape_id: &str,
        node_seeds: Vec<(SubquerySig, Vec<Row>, crate::pg::SnapshotGate)>,
        outer_gate: crate::pg::SnapshotGate,
        seeded: u64,
        seeded_pks: std::collections::HashSet<String>,
    ) -> Result<FinishedCreate> {
        anyhow::ensure!(
            self.pending_shapes.iter().any(|p| p.shape_id == shape_id),
            "finish_create: pending shape vanished"
        );
        let mut work: VecDeque<(u64, SubquerySig, Flip)> = VecDeque::new();
        let mut node_work: VecDeque<(SubquerySig, DeferredNodeWork)> = VecDeque::new();
        // 1. Install node seeds, then replay each node's buffered deltas through its gate.
        for (sig, rows, gate) in node_seeds {
            let (ts, proj_col) = {
                let n = self.nodes.get(&sig).context("finish_create: node vanished")?;
                (self.schemas.get(&n.inner_table).cloned().context("unknown inner table")?, n.proj_col)
            };
            if let Some(n) = self.nodes.get_mut(&sig) {
                n.gate = gate;
            }
            let mut seed = Assertions::default();
            for r in &rows {
                let pk = ts.key_string(r).unwrap_or_default();
                let pv = r.0.get(proj_col).cloned().unwrap_or(Value::Null);
                seed.contributors.extend(self.assert_node_row(&sig, &pk, Some(pv)));
            }
            // Initial state: the seed's flips are meaningless (every dependent's backfill
            // already reflects the seeded set), so this step's deltas are discarded — only
            // the replay below propagates.
            let _ = self.apply_asserts(seed).await;
            let buffered = self.nodes.get_mut(&sig).and_then(|n| n.seed_buffer.take()).unwrap_or_default();
            // Replayed one buffered delta at a time, in arrival order, so each keeps its own
            // commit stamp and is recorded as the live decision it is (the outer buffer's replay
            // below does the same). Membership is re-evaluated by identity per pk and is
            // idempotent against the seed for snapshot-visible rows, so replaying every delta is
            // convergent — replaying per delta just also lands the intermediate states.
            for b in buffered {
                let evals = self.node_present_values(&sig, &ts, &b.delta);
                self.record_node_recency(&sig, evals.iter().map(|(pk, _)| pk), b.lsn, b.xid);
                for f in self.apply_node_evals(&sig, evals).await {
                    work.push_back((b.lsn, sig.clone(), f));
                }
            }
            // The node is live from here (its seed and its raw deltas are both in), so a child
            // flip that arrived while it was seeding can finally be re-derived against a set that
            // is not a lie. Taking the queue drains it for good: the node no longer defers.
            if let Some(n) = self.nodes.get_mut(&sig) {
                for w in std::mem::take(&mut n.deferred) {
                    node_work.push_back((sig.clone(), w));
                }
            }
        }
        // 2. Everything that can still fail happens BEFORE the pending entry leaves the registry,
        //    so a failure here returns with the create still exactly unwindable.
        let idx = self
            .pending_shapes
            .iter()
            .position(|p| p.shape_id == shape_id)
            .context("finish_create: pending shape vanished")?;
        let ts = self
            .schemas
            .get(&self.pending_shapes[idx].outer_table)
            .cloned()
            .context("finish_create: unknown outer table")?;
        // The atomic tail: ONE synchronous `&mut self` step, no `.await` inside it — take the
        // pending entry (and with it the deferred queue), register the shape, seed its feed. This
        // is the step the deferred hand-off's losslessness rests on (see the doc comment).
        let mut pending = self.pending_shapes.remove(idx);
        let deferred = std::mem::take(&mut pending.deferred);
        let feed_id = self.next_feed_id;
        self.next_feed_id += 1;
        self.install_shape(SubqueryShape {
            shape_id: shape_id.to_string(),
            outer_table: pending.outer_table.clone(),
            stream_path: pending.stream_path.clone(),
            pred: pending.pred.clone(),
            out_cols: pending.out_cols.clone(),
            gate: outer_gate,
            changes_only: pending.changes_only,
            emitted: std::sync::atomic::AtomicU64::new(seeded),
            feed_id,
            recent: HashMap::new(),
            inflight_querybacks: 0,
            position: 0,
        });
        // Seed the feed with the backfilled pks (the stream already carries the snapshot) —
        // replaces the old known_members hand-off. This is phase C, under the registry lock,
        // BEFORE the shape is discoverable by any live delta (the spike's §6 riskiest transition):
        // the host-side FeedSet is seeded synchronously here — a `&mut self` op with no `.await` —
        // so no live delta can slip between "shape registered" and "feed seeded".
        for pk in seeded_pks {
            let pk_id = self.pk_dict.get_or_insert(&pk);
            self.feed_sets.insert(feed_id, pk_id);
        }
        // 3. Replay the shape's buffered outer deltas through the gate (absolute emission;
        //    idempotent against the backfill for snapshot-visible rows). Replayed one buffered
        //    delta at a time, in arrival order, so each keeps its own commit stamp: the deferred
        //    work returned below can start a query-back the instant this call releases the lock,
        //    and a replayed decision it must not undo is recognisable only by its stamp. Per-pk
        //    emission is absolute, so replaying per delta rather than folding them into one
        //    verdict lands the same final state — it just also lands the intermediate ones.
        for buffered in std::mem::take(&mut pending.buffer) {
            let candidates = crate::engine::membership::latest_rows_by_pk(&ts, &buffered.delta);
            self.emit_for_shapes(
                &ts,
                vec![(shape_id.to_string(), candidates)],
                None,
                EmissionSource::Replay { lsn: buffered.lsn, xid: buffered.xid },
            )
            .await?;
        }
        Ok(FinishedCreate { work, deferred, node_work })
    }

    /// Install a fully-built outer shape. The shapes map, the feed-id reverse map and the
    /// necessary-conjunct index MUST move together — an index entry outliving its shape is a
    /// wasted probe, but a shape with no index entry is invisible to every later delta (silently
    /// dropped envelopes). One call site so they cannot drift, and one synchronous `&mut self`
    /// step so no live delta can observe a half-installed shape. Registration is only reachable
    /// from `finish_create`, the atomic tail of shape creation: a create that fails or is cancelled
    /// earlier never got here, so [`abort_create`](Self::abort_create) unwinds its still-registered
    /// pending entry — one abandoned after it undoes this install through
    /// [`drop_subquery_shape`](Self::drop_subquery_shape).
    fn install_shape(&mut self, shape: SubqueryShape) {
        self.feed_by_id.insert(shape.feed_id, shape.shape_id.clone());
        self.shape_index.insert(&shape.shape_id, shape.outer_table.as_str(), &shape.pred);
        self.shapes.insert(shape.shape_id.clone(), shape);
    }

    /// The subquery shapes on `table` that a change described by `delta` can possibly move —
    /// exactly the set [`on_table_delta`](Self::on_table_delta) step 2 visits, exposed for tests
    /// and introspection so "shapes visited per change" is assertable.
    ///
    /// `delta` must be the RAW Z-set delta (old `-1` images included), not the per-pk latest fold:
    /// the candidate set is the union over old ∪ new images, which is what keeps a move-out's
    /// absolute delete alive. See [`crate::subq_index`].
    pub(crate) fn outer_candidates(&self, table: &TableRef, delta: &[Tup2<Row, ZWeight>]) -> Vec<String> {
        self.shape_index.candidates(table.as_str(), delta)
    }

    /// Abort a create in whichever phase it died in. Before the atomic tail of `finish_create` that
    /// means unwinding its pending entry (edges, refcounts, fresh nodes with their buffers and their
    /// deferred queues, and any seed already asserted into the membership circuit); after it the
    /// shape is installed like any other, so the ordinary drop path is the correct undo. A create
    /// cancelled between `finish_create` returning and the engine publishing the shape lands in that
    /// second case.
    ///
    /// The pending branch must cover a create that died ANYWHERE in phase C, not just before it:
    /// phase C installs node seeds one await at a time, so a dropped future can leave a fresh node
    /// holding a full seed. Removing it without retracting those contributor tuples would leave the
    /// membership circuit carrying a dead node's set forever, so removal goes through the same
    /// retracting path a refcount-0 drop uses — unconditionally, because a node with no state
    /// retracts nothing and the check would cost the same scan as the retraction.
    pub async fn abort_create(&mut self, shape_id: &str) {
        if self.shapes.contains_key(shape_id) {
            self.drop_subquery_shape(shape_id).await;
            return;
        }
        let Some(idx) = self.pending_shapes.iter().position(|p| p.shape_id == shape_id) else {
            return;
        };
        // Taking the whole entry discards its deferred queue with it: work aimed at a shape that
        // will never exist has nowhere to land and nothing to correct. The same goes for the
        // deferred queue of each fresh node removed below — it dies with the node it was waiting on.
        let pending = self.pending_shapes.remove(idx);
        // Shape edges live under the pred's leaf sigs — remove only there, not a global scan.
        self.remove_shape_edges(&pending.pred, &pending.shape_id);
        let mut asserts = Assertions::default();
        for sig in pending.collect_log {
            let Some(n) = self.nodes.get_mut(&sig) else { continue };
            n.refcount = n.refcount.saturating_sub(1);
            if n.refcount > 0 {
                continue;
            }
            // No cascade here (unlike `decref_nodes`): the compile log has one entry per `collect()`
            // call, deeper nodes included, so this walk already visits every node the create
            // referenced — cascading would decrement a child a second time.
            self.remove_node_with_state(&sig, &mut asserts);
            self.pending_seed.retain(|s| s != &sig);
        }
        // The nodes are gone and their ids are never reused, so these retractions have no dependent
        // left to move and their flips are discarded.
        if !asserts.is_empty() {
            let _ = self.circuit.apply(asserts).await;
        }
    }

    /// Remove a shape's dependency edges: they live under the keys of the predicate's IN
    /// leaves, so removal touches only those nodes' lists.
    fn remove_shape_edges(&mut self, pred: &CompiledPredicate, shape_id: &str) {
        for leaf in collect_in_leaves(pred) {
            if let Some(v) = self.edges.get_mut(&leaf.sig) {
                v.retain(|e| !matches!(&e.dependent, Dependent::Shape(id) if id == shape_id));
                if v.is_empty() {
                    self.edges.remove(&leaf.sig);
                }
            }
        }
    }

    /// Drop a removed node's id/template bookkeeping. The circuit retraction is the caller's job —
    /// see [`remove_node_with_state`](Self::remove_node_with_state), which does both for a node
    /// that may hold state.
    fn remove_node_entry(&mut self, node: &SubqueryNode) {
        self.node_by_id.remove(&node.node_id);
        if let Some(tpl) = self.templates.get_mut(&node.template_key) {
            tpl.binds.remove(&node.bind);
            if tpl.binds.is_empty() {
                self.templates.remove(&node.template_key);
            }
        }
    }

    /// Rollback helper for a failed/conflicted `begin_create` compile: drop the staged edges
    /// and undo the node refs made by the aborted compile.
    fn rollback_refs(&mut self, log: Vec<SubquerySig>) {
        self.staged_edges.clear();
        for sig in log {
            if let Some(n) = self.nodes.get_mut(&sig) {
                n.refcount = n.refcount.saturating_sub(1);
                if n.refcount == 0 {
                    if let Some(node) = self.nodes.remove(&sig) {
                        self.remove_node_entry(&node);
                    }
                    self.pending_seed.retain(|s| s != &sig);
                }
            }
        }
    }

    /// Remove a subquery shape: drop its edges and decref the nodes it referenced (removing nodes whose
    /// refcount hits zero, and their edges, recursively).
    pub async fn drop_subquery_shape(&mut self, shape_id: &str) {
        let Some(shape) = self.shapes.remove(shape_id) else { return };
        // Sigs this shape pointed at, then drop the shape's edges.
        let sigs: Vec<SubquerySig> = collect_in_leaves(&shape.pred).into_iter().map(|l| l.sig).collect();
        self.remove_shape_edges(&shape.pred, shape_id);
        // Un-file it from the conjunct index in the same step it leaves `shapes` (the inverse of
        // `install_shape`) — a posting for a shape that no longer exists would route deltas at a
        // dead stream path.
        self.shape_index.remove(shape_id);
        // Drop the shape's whole feed (O(1) — no per-pk enumeration, no circuit round-trip) and
        // its id mapping. The stream is being torn down; the `feed_id` is never reused.
        self.feed_by_id.remove(&shape.feed_id);
        self.feed_sets.drop_feed(shape.feed_id);
        self.decref_nodes(sigs).await;
    }

    /// Decrement each sig's refcount, removing (and cascading into the children of) nodes
    /// that reach zero. Removed nodes retract their contributor tuples from the circuit in
    /// one batch at the end; the resulting Leave flips are discarded — a refcount-0 node has
    /// no dependents left to move.
    async fn decref_nodes(&mut self, sigs: Vec<SubquerySig>) {
        let mut stack = sigs;
        let mut asserts = Assertions::default();
        while let Some(sig) = stack.pop() {
            let Some(node) = self.nodes.get_mut(&sig) else { continue };
            node.refcount = node.refcount.saturating_sub(1);
            if node.refcount > 0 {
                continue;
            }
            // Refcount hit zero: retract state, remove node + edges, recurse into its children.
            stack.extend(self.remove_node_with_state(&sig, &mut asserts));
        }
        // Refcount-0 removal ⇒ no dependents remain; the flips are discarded.
        if !asserts.is_empty() {
            let _ = self.circuit.apply(asserts).await;
        }
    }

    /// Remove a node that has reached the end of its life: retract its contributor tuples into
    /// `asserts` (the caller applies one batch), drop its id/template/edge bookkeeping, and return
    /// its child sigs. The one removal path for a node that may hold circuit state — shared by the
    /// refcount-0 drop ([`decref_nodes`](Self::decref_nodes), which recurses into the returned
    /// children) and by [`abort_create`](Self::abort_create), whose fresh node may have taken its
    /// seed before the create was cancelled.
    ///
    /// The contributor slice comes from the circuit's own integral (prefix scan) — there is no host
    /// pk list to drain. Keys are pk ids; the retraction re-asserts the same id, so no dictionary
    /// round-trip is needed here.
    fn remove_node_with_state(&mut self, sig: &SubquerySig, asserts: &mut Assertions) -> Vec<SubquerySig> {
        let Some(node) = self.nodes.remove(sig) else { return Vec::new() };
        let child_sigs: Vec<SubquerySig> = collect_in_leaves(&node.pred).into_iter().map(|l| l.sig).collect();
        for (pk_id, _v) in self.circuit.contributor_entries(node.node_id) {
            if let Some(tpl) = self.templates.get_mut(&node.template_key) {
                if let Some(set) = tpl.pk_nodes.get_mut(&pk_id) {
                    set.remove(sig);
                    if set.is_empty() {
                        tpl.pk_nodes.remove(&pk_id);
                    }
                }
            }
            asserts.contributors.push(Tup2(PkKey { id: node.node_id, pk: pk_id }, Assert::Delete));
        }
        self.remove_node_entry(&node);
        self.remove_node_edges(sig, &child_sigs);
        child_sigs
    }

    // --- live maintenance ---------------------------------------------------------------------

    /// Process one table delta: update affected nodes (in-memory) and emit outer-shape deltas
    /// synchronously, then **return** the inner-set flips for deferred propagation (the caller
    /// hands them to the engine's flip-propagator task — see [`propagate_flips`]). Deferring the
    /// flip-driven Postgres query-backs is safe because outer membership is emitted absolutely
    /// (upsert-if-matches-now / idempotent delete), so cross-table convergence is order-independent;
    /// the convergence barrier is the processed offset **plus** a drained flip queue. `lsn` is the
    /// change's commit LSN (0 = unknown/never skip).
    pub async fn on_table_delta(
        &mut self,
        ts: &TableSchema,
        delta: &[Tup2<Row, ZWeight>],
        lsn: u64,
        xid: Option<u64>,
        txid: Option<String>,
        mut trace: Option<&mut Vec<crate::trace::TraceHop>>,
    ) -> Result<VecDeque<(SubquerySig, Flip)>> {
        let table = ts.table.clone();
        // A new commit: the transitions kept for the previous one are no longer "this commit's".
        if lsn == 0 || self.commit.lsn != lsn {
            self.commit = CommitScope { lsn, ..CommitScope::default() };
            // Remember whose commit this is, for the emissions it causes later that arrive with
            // its LSN alone (see `commit_txids`).
            if lsn != 0
                && let Some(txid) = &txid
            {
                self.commit_txids.insert(lsn, txid.clone());
                while self.commit_txids.len() > COMMIT_TIMES_RETAINED {
                    self.commit_txids.pop_first();
                }
            }
        }
        // Work queue of (node sig, flip) pairs to propagate (BFS up the dependency DAG).
        let mut work: VecDeque<(SubquerySig, Flip)> = VecDeque::new();
        // Trace helper: record a hop once per node id (a shape reached via several flips is one hop).
        let hop = |trace: &mut Option<&mut Vec<crate::trace::TraceHop>>, node: String, outcome: &'static str| {
            if let Some(t) = trace.as_mut() {
                if let Some(prev) = t.iter_mut().find(|h| h.node == node) {
                    if outcome == "passed" {
                        prev.outcome = "passed"; // an earlier dropped hop upgraded by a later emit
                    }
                } else {
                    t.push(crate::trace::TraceHop::new(node, outcome));
                }
            }
        };

        // 1. Templates whose inner table is this table: one residual eval + one bind lookup
        // per touched pk (instead of one full-predicate eval per literal-keyed node), then one
        // circuit step for the whole delta — the circuit's distinct reports the flips.
        let tkeys: Vec<String> =
            self.templates.iter().filter(|(_, t)| t.inner_table == table).map(|(k, _)| k.clone()).collect();
        let mut asserts = Assertions::default();
        let mut live_sigs: Vec<SubquerySig> = Vec::new();
        for tkey in &tkeys {
            let sigs: Vec<SubquerySig> =
                self.templates.get(tkey).map(|t| t.binds.values().cloned().collect()).unwrap_or_default();
            for sig in sigs {
                // Mid-seed: buffer the raw delta for gated replay at install (a half-seeded
                // set must not be reconciled — the snapshot could stale-overwrite a fresher
                // delta).
                if let Some(buf) = self.nodes.get_mut(&sig).and_then(|n| n.seed_buffer.as_mut()) {
                    // Keep this commit's stamp with the delta: the install replay is a live
                    // decision and has to be able to out-rank a node query-back's older read
                    // (see [`BufferedDelta`]).
                    buf.push(BufferedDelta { lsn, xid, delta: delta.to_vec() });
                    hop(&mut trace, format!("node:{sig}"), "buffered");
                } else if self.nodes.get(&sig).is_some_and(|n| n.gate.should_skip(lsn, xid)) {
                    hop(&mut trace, format!("node:{sig}"), "dropped");
                } else {
                    live_sigs.push(sig);
                }
            }
            let evals = self.template_present(tkey, ts, delta);
            asserts.contributors.extend(self.template_assertions(tkey, evals, lsn, xid));
        }
        let flips = self.apply_asserts(asserts).await;
        for (sig, flip) in &flips {
            self.note_commit_flip(lsn, sig, flip);
        }
        for sig in live_sigs {
            let flipped = flips.iter().any(|(s, _)| s == &sig);
            hop(&mut trace, format!("node:{sig}"), if flipped { "passed" } else { "dropped" });
        }
        for f in flips {
            work.push_back(f);
        }

        // 2. Subquery shapes whose outer table is this table: one batch of candidates across
        // every shape — assertions feed the circuit in ONE step, its feed deltas are the
        // deletes, matching candidates are the upserts.
        // Candidates come from the necessary-conjunct index (`crate::subq_index`), bucketed by
        // outer table: a shape whose conjunct no image in this delta satisfies provably cannot
        // change membership for any touched pk, so visiting it would be a full predicate eval
        // that can only conclude "nothing". The probe unions the delta's OLD (`-1`) and NEW
        // (`+1`) images, which is what keeps a move-out's absolute delete alive — see the module
        // docs of `subq_index` for the argument. With a trace subscriber attached the index is
        // bypassed (like the standalone/aggregate tiers) so every shape node still reports a hop.
        let shape_ids: Vec<String> = if trace.is_some() {
            self.shapes.iter().filter(|(_, s)| s.outer_table == table).map(|(id, _)| id.clone()).collect()
        } else {
            self.outer_candidates(&table, delta)
        };
        let mut groups: Vec<(String, Vec<(Row, bool)>)> = Vec::new();
        for id in shape_ids {
            if self.shapes.get(&id).is_some_and(|s| s.gate.should_skip(lsn, xid)) {
                continue;
            }
            groups.push((id, crate::engine::membership::latest_rows_by_pk(ts, delta)));
        }
        // This is the freshest verdict any path can have for these pks: it is derived from the
        // commit itself. `EmissionSource::Live` carries that commit's stamp so a query-back
        // already reading Postgres cannot later overwrite the decision with an older row.
        //
        // The delta also carries each row's image BEFORE the commit, which is the proof a
        // changes-only feed needs to remove a row it never delivered (see [`Prior`]). The old
        // image is judged against the inner sets as they stood before this commit, so a commit
        // that first withdraws a row's inner value and then changes the row still removes it.
        let images =
            if groups.is_empty() { HashMap::new() } else { crate::engine::membership::prior_rows_by_pk(ts, delta) };
        for (id, emitted, _net) in self
            .emit_for_shapes_with(ts, groups, txid.clone(), EmissionSource::Live { lsn, xid }, Prior::Images(&images))
            .await?
        {
            hop(&mut trace, format!("shape:{id}"), if emitted { "passed" } else { "dropped" });
        }

        // 2b. Pending shapes (mid-create) on this table: buffer for gated replay at install,
        // keeping this commit's stamp with it (the replay is a live decision — see
        // [`BufferedDelta`]).
        for p in self.pending_shapes.iter_mut().filter(|p| p.outer_table == table) {
            p.buffer.push(BufferedDelta { lsn, xid, delta: delta.to_vec() });
        }

        // 3. Flip propagation (the Postgres query-backs) is deferred: the caller enqueues `work`
        // onto the engine's flip-propagator task, which runs [`propagate_flips`] without holding
        // this registry lock across round-trips.
        Ok(work)
    }

    /// For each inner-row pk touched by `delta`, its desired contribution (`Some(proj)` if the row now
    /// matches the node predicate, else `None`). Immutable (reads node sets for `matches_ctx`).
    fn node_present_values(
        &self,
        sig: &SubquerySig,
        ts: &TableSchema,
        delta: &[Tup2<Row, ZWeight>],
    ) -> Vec<(String, Option<Value>)> {
        let (pred, proj) = match self.nodes.get(sig) {
            Some(n) => (n.pred.clone(), n.proj_col),
            None => return Vec::new(),
        };
        // The +1 row (if any) is the row's new state; a pk seen only with -1 was deleted.
        let mut newrow: HashMap<String, Row> = HashMap::new();
        let mut seen: Vec<String> = Vec::new();
        for Tup2(row, w) in delta {
            let pk = ts.key_string(row).unwrap_or_default();
            if !seen.contains(&pk) {
                seen.push(pk.clone());
            }
            if *w > 0 {
                newrow.insert(pk, row.clone());
            }
        }
        seen.into_iter()
            .map(|pk| match newrow.get(&pk) {
                Some(r) => {
                    let pv = if pred.matches_ctx(r, self) {
                        Some(r.0.get(proj).cloned().unwrap_or(Value::Null))
                    } else {
                        None
                    };
                    (pk, pv)
                }
                None => (pk, None),
            })
            .collect()
    }

    /// The per-pk contributions a node RE-DERIVATION should assert: its Postgres rows evaluated
    /// against the node's predicate, MINUS every pk whose live decision the read could not have
    /// seen. `None` = the node vanished while the read ran.
    ///
    /// This is the node tier of the query-back recency fence, the exact analogue of the candidate
    /// filter [`emit_for_shapes`](Self::emit_for_shapes) applies to a shape's query-back, with the
    /// same predicate: `gate.should_skip(lsn, xid)` is "this commit was already visible to that
    /// read's snapshot", so the rows in hand already reflect it and the candidate is current. Its
    /// negation is "the live decision is NEWER than this read": the row is stale, the live verdict
    /// already stands, and re-deriving that pk would re-assert the contribution the verdict just
    /// removed (or withhold one it just added). The node appends nothing itself, but its dependent
    /// shape appends what the node's flips say — so a stale re-assertion is the same permanent
    /// divergence, one level down. (An unstamped live decision — no LSN and no xid — reads as
    /// "newer" and is dropped: only non-Postgres sources leave a change unstamped, and a
    /// re-derivation cannot run without Postgres.)
    fn node_queryback_evals(
        &self,
        sig: &SubquerySig,
        ts: &TableSchema,
        rows: &[Row],
        gate: &crate::pg::SnapshotGate,
    ) -> Option<Vec<(String, Option<Value>)>> {
        let (pred, proj, recent) = match self.nodes.get(sig) {
            Some(n) => (n.pred.clone(), n.proj_col, &n.recent),
            None => return None,
        };
        Some(
            rows.iter()
                .filter_map(|r| {
                    let pk = ts.key_string(r).unwrap_or_default();
                    // `recent` empty (nothing decided live during this read) is the common case
                    // and costs nothing: no candidate is re-examined at all.
                    if let Some(&(lsn, xid)) = recent.get(&pk)
                        && !gate.should_skip(lsn, xid)
                    {
                        return None;
                    }
                    let pv = if pred.matches_ctx(r, self) {
                        Some(r.0.get(proj).cloned().unwrap_or(Value::Null))
                    } else {
                        None
                    };
                    Some((pk, pv))
                })
                .collect(),
        )
    }

    /// For each touched pk, the row's target contribution under one template: `Some((node
    /// sig, projected value))` when the latest row matches the residual AND its projected
    /// params hit a registered bind, else `None`. One residual eval + one hash lookup per pk —
    /// the template-sharing eval win over per-node full-predicate evaluation.
    fn template_present(
        &self,
        tkey: &str,
        ts: &TableSchema,
        delta: &[Tup2<Row, ZWeight>],
    ) -> Vec<(String, Option<(SubquerySig, Value)>)> {
        let Some(tpl) = self.templates.get(tkey) else { return Vec::new() };
        crate::engine::membership::latest_rows_by_pk(ts, delta)
            .into_iter()
            .map(|(row, is_new)| {
                let pk = ts.key_string(&row).unwrap_or_default();
                let target = if is_new && tpl.residual.matches_ctx(&row, self) {
                    let params =
                        Row(tpl.param_cols.iter().map(|&i| row.0.get(i).cloned().unwrap_or(Value::Null)).collect());
                    tpl.binds
                        .get(&params)
                        .map(|sig| (sig.clone(), row.0.get(tpl.proj_col).cloned().unwrap_or(Value::Null)))
                } else {
                    None
                };
                (pk, target)
            })
            .collect()
    }

    /// Turn one template's per-pk targets into contributor assertions: absent for nodes that
    /// held the pk but are no longer its target, present for the new target. Per node, the
    /// delta is skipped when the node is mid-seed (its raw buffer replays at install) or when
    /// the node's seed gate says the snapshot already contains this change — in both cases
    /// the node's seed is (or will be) the authority, and absolute assertion absorbs any
    /// overlap idempotently.
    ///
    /// This is the per-(node, pk) LIVE decision point, so it is also where node recency is
    /// recorded (see [`SubqueryNode::recent`]): for every bind of this template that has a
    /// re-derivation in flight, this delta decides the pk's contribution ABSOLUTELY — target
    /// bind or not — so the stamp is recorded for each of them, including the pks whose verdict
    /// is "no contribution". Recording only the pks that produced an assertion would miss
    /// exactly the case the fence exists for: a pk the node does not hold yet, whose live
    /// verdict is "still not a contributor", which a stale re-derivation would then admit.
    fn template_assertions(
        &mut self,
        tkey: &str,
        evals: Vec<(String, Option<(SubquerySig, Value)>)>,
        lsn: u64,
        xid: Option<u64>,
    ) -> Vec<Tup2<PkKey, Assert>> {
        let node_applies = |reg: &Self, sig: &SubquerySig| {
            reg.nodes.get(sig).is_some_and(|n| n.seed_buffer.is_none() && !n.gate.should_skip(lsn, xid))
        };
        // The binds whose decisions have an older read to beat. Empty in the steady state (no
        // node re-derivation outstanding), so the recording below costs nothing then.
        let fenced: Vec<SubquerySig> = self
            .templates
            .get(tkey)
            .map(|t| {
                t.binds
                    .values()
                    .filter(|sig| {
                        self.nodes.get(*sig).is_some_and(|n| n.inflight_querybacks > 0) && node_applies(self, sig)
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        let mut asserts = Vec::new();
        for (pk, target) in evals {
            // This commit decided this pk's contribution on every fenced bind (it is the target
            // of at most one of them, and a non-contributor to the rest), so all of them record
            // it — a re-derivation reading Postgres older than this commit must drop the pk
            // whichever bind it is re-deriving.
            for sig in &fenced {
                if let Some(n) = self.nodes.get_mut(sig) {
                    n.recent.insert(pk.clone(), (lsn, xid));
                }
            }
            // A pk with no interned id was never asserted, so it can hold no contribution — probe
            // without minting (a never-member delete must not grow the dictionary).
            let holders: Vec<SubquerySig> = self
                .pk_dict
                .get(&pk)
                .and_then(|pk_id| self.templates.get(tkey).and_then(|t| t.pk_nodes.get(&pk_id)))
                .map(|s| s.iter().cloned().collect())
                .unwrap_or_default();
            for sig in holders {
                if target.as_ref().is_some_and(|(tsig, _)| tsig == &sig) {
                    continue; // still the target; the insert below carries the fresh value
                }
                if node_applies(self, &sig) {
                    asserts.extend(self.assert_node_row(&sig, &pk, None));
                }
            }
            if let Some((sig, v)) = target {
                if node_applies(self, &sig) {
                    asserts.extend(self.assert_node_row(&sig, &pk, Some(v)));
                }
            }
        }
        asserts
    }

    /// Mark a query-back for `shape_id` as in flight — called under the lock the query-back is
    /// about to RELEASE for its Postgres read, so every live decision taken while that read (and
    /// the evaluation that follows it) is outstanding lands in [`SubqueryShape::recent`].
    /// Increment first, release second: the reverse order would leave a hole exactly the size of
    /// the race being closed.
    fn begin_queryback(&mut self, shape_id: &str) {
        if let Some(s) = self.shapes.get_mut(shape_id) {
            s.inflight_querybacks = s.inflight_querybacks.saturating_add(1);
        }
    }

    /// Balance [`begin_queryback`](Self::begin_queryback) — under the lock, on EVERY exit path of
    /// the query-back (including the read's error path and the "nothing came back" path).
    /// A shape dropped in the meantime has nothing to decrement.
    ///
    /// The last in-flight query-back to finish clears `recent`: with no outstanding read there is
    /// nothing left that could overwrite a live decision, so keeping per-pk stamps would only be
    /// unbounded growth. This is what makes the map cost proportional to a query-back window
    /// rather than to the shape.
    fn end_queryback(&mut self, shape_id: &str) {
        if let Some(s) = self.shapes.get_mut(shape_id) {
            s.inflight_querybacks = s.inflight_querybacks.saturating_sub(1);
            if s.inflight_querybacks == 0 {
                s.recent = HashMap::new();
            }
        }
    }

    /// [`begin_queryback`](Self::begin_queryback), one tier down: mark a re-derivation of `sig`'s
    /// inner rows as in flight, under the lock it is about to RELEASE for its Postgres read, so
    /// every live contribution decision taken while that read is outstanding lands in
    /// [`SubqueryNode::recent`]. Increment first, release second — the reverse order would leave a
    /// hole exactly the size of the race being closed.
    fn begin_node_queryback(&mut self, sig: &SubquerySig) {
        if let Some(n) = self.nodes.get_mut(sig) {
            n.inflight_querybacks = n.inflight_querybacks.saturating_add(1);
        }
    }

    /// Balance [`begin_node_queryback`](Self::begin_node_queryback) — under the lock, on EVERY exit
    /// path of the re-derivation (the read's error path and the "node vanished" path included), or
    /// the node would keep recording recency (and dropping candidates) forever. A node dropped in
    /// the meantime has nothing to decrement. The last one out clears `recent`: with no outstanding
    /// read there is nothing left that could re-assert a stale contribution.
    fn end_node_queryback(&mut self, sig: &SubquerySig) {
        if let Some(n) = self.nodes.get_mut(sig) {
            n.inflight_querybacks = n.inflight_querybacks.saturating_sub(1);
            if n.inflight_querybacks == 0 {
                n.recent = HashMap::new();
            }
        }
    }

    /// Record a batch of LIVE per-pk contribution decisions for one node — a no-op unless a
    /// re-derivation of that node is in flight, which is the only window the map has to cover.
    /// (The template path records inline; this is for the seed-buffer replay, which decides one
    /// node's pks directly rather than through a template's binds. See
    /// [`SubqueryNode::seed_buffer`] for why that replay is stamped at all.)
    fn record_node_recency<'a>(
        &mut self,
        sig: &SubquerySig,
        pks: impl Iterator<Item = &'a String>,
        lsn: u64,
        xid: Option<u64>,
    ) {
        let Some(n) = self.nodes.get_mut(sig) else { return };
        if n.inflight_querybacks == 0 {
            return;
        }
        for pk in pks {
            n.recent.insert(pk.clone(), (lsn, xid));
        }
    }

    /// Deferred-propagation helper: snapshot what a query-back needs (brief lock scope at the
    /// call site — see the free functions below).
    fn snapshot_for_table(&self, table: &TableRef) -> Result<TableSchema> {
        self.schemas.get(table).cloned().with_context(|| format!("unknown table '{table}'"))
    }

    /// Where this registry's flip query-backs read their candidate rows (pooled Postgres).
    fn candidate_reader(&self) -> CandidateReader {
        CandidateReader {
            pool: self.pg_pool.clone(),
            #[cfg(test)]
            fake: self.fake_candidates.clone(),
        }
    }

    /// The ONE emission tail (spec §5): evaluate each shape's candidates against the current
    /// membership snapshot, transition the host-side per-feed key set ([`crate::subq_feed::FeedSet`])
    /// in the same synchronous step, and deliver — **upserts for every matching candidate** (an
    /// update to a continuing member must flow; upserts are always safe for readers), **deletes
    /// only where the feed actually retracts** (`FeedSet::remove` returns true — a "not a member"
    /// verdict for a pk the stream never contained gates to nothing, so the spurious delete that
    /// used to wake idle long-polls is structurally impossible). Emission is absolute per pk, so
    /// deferred flip propagation converges regardless of timing.
    ///
    /// Callers hold the registry lock for the whole call (eval + FeedSet mutation + lane enqueue),
    /// which is what keeps per-stream append order = evaluation order AND makes each emission
    /// decision atomic with its bitmap transition (borrow-checker-enforced — no `.await` between).
    /// `candidates` are each shape's touched rows: `(latest row, still-exists)`.
    /// Returns per shape whether anything was delivered (trace hops).
    ///
    /// **Per-stream order is not enough on its own.** Holding the lock makes append order equal
    /// evaluation order, but a query-back's candidates were READ before it took the lock, so
    /// "evaluated last" and "freshest" are different things for exactly the pks a live commit
    /// touched in between. `source` closes that gap ([`EmissionSource`]): a live/replayed
    /// decision records its commit stamp for every pk it evaluates while a query-back is in
    /// flight, and a query-back drops every candidate whose recorded stamp its own snapshot could
    /// not see. Both halves are needed — recording only the pks that produced an envelope would
    /// let a stale query-back upsert re-add a row whose live verdict was "not a member, and it
    /// was never in the feed", which emits nothing and is precisely the divergence seen in
    /// practice.
    async fn emit_for_shapes(
        &mut self,
        ts: &TableSchema,
        groups: Vec<(String, Vec<(Row, bool)>)>,
        txid: Option<String>,
        source: EmissionSource<'_>,
    ) -> Result<Vec<(String, bool, i64)>> {
        self.emit_for_shapes_with(ts, groups, txid, source, Prior::Unknown).await
    }

    /// Record one inner-set transition of the commit at `commit_lsn` in that commit's record
    /// ([`CommitScope`]), wherever it was derived: by the commit's own change to the set's inner
    /// table, or later, by a flip's walk re-deriving a parent set. A transition of any other
    /// commit — an earlier one whose walk ran late, or a change with no LSN — is not recorded: the
    /// record is the one commit's, and such an evaluation is judged without it.
    fn note_commit_flip(&mut self, commit_lsn: u64, sig: &SubquerySig, flip: &Flip) {
        if commit_lsn != 0 && self.commit.lsn == commit_lsn {
            self.commit.note_transition(sig, flip);
        }
    }

    /// [`emit_for_shapes`](Self::emit_for_shapes) with what the caller can prove about the
    /// candidates' membership just before this change. For a `changes_only` feed a non-member
    /// candidate is deleted when the key set held it **or** `prior` shows it was a member; every
    /// other shape ignores `prior` and gates on the key set alone, as before.
    async fn emit_for_shapes_with(
        &mut self,
        ts: &TableSchema,
        groups: Vec<(String, Vec<(Row, bool)>)>,
        txid: Option<String>,
        source: EmissionSource<'_>,
        prior: Prior<'_>,
    ) -> Result<Vec<(String, bool, i64)>> {
        // Phase 1: evaluate each candidate against the current membership snapshot and, in the
        // SAME synchronous step (no `.await`), transition the host-side FeedSet — building the
        // member upserts and the gated deletes. A delete is emitted for a shape's pk **iff**
        // `feed_sets.remove` returns true (the pk was actually in the feed): the check-and-set IS
        // the emission decision, in one indivisible expression under the registry lock, so the
        // spurious delete that used to wake idle long-polls is structurally impossible (the
        // wake-storm gate, PR #30). Upserts are delivered for every current member unconditionally.
        let mut staged: Vec<(String, Vec<Row>)> = Vec::new();
        let mut deletes: HashMap<String, Vec<String>> = HashMap::new();
        // Receipt accounting follows the triggering commit even when candidates came from a
        // later query-back snapshot. This is separate from the per-pk recency stamp below.
        let commit_lsn = match &source {
            EmissionSource::Live { lsn, .. } | EmissionSource::Replay { lsn, .. } => *lsn,
            EmissionSource::QueryBack { commit_lsn, .. } => *commit_lsn,
        };
        // Whether this evaluation belongs to the commit the registry is ingesting (see
        // [`CommitScope`]): only then can it share that commit's record of removals already said.
        let same_commit = commit_lsn != 0 && self.commit.lsn == commit_lsn;
        // That commit's inner-set transitions, lent to the proof for the synchronous evaluation
        // below and put back before anything awaits. Moved out and back, never copied: a commit
        // can hold thousands of flips, and each reaches hundreds of shapes.
        // A record that was given up lends nothing: the evaluation is judged by its own cause.
        let lent = (same_commit && !self.commit.saturated && !matches!(prior, Prior::Unknown))
            .then(|| std::mem::take(&mut self.commit.undone));
        let no_record = Undone::new();
        // The commit stamp a live/replayed decision records for the pks it evaluates; `None` for a
        // query-back, which reads this map instead of writing it.
        let live_stamp: Option<(u64, Option<u64>)> = match &source {
            EmissionSource::Live { lsn, xid } | EmissionSource::Replay { lsn, xid } => Some((*lsn, *xid)),
            EmissionSource::QueryBack { .. } => None,
        };
        for (shape_id, candidates) in groups {
            let Some(shape) = self.shapes.get(&shape_id) else { continue };
            let (pred, feed_id) = (shape.pred.clone(), shape.feed_id);
            let changes_only = shape.changes_only;
            let prior = if changes_only { prior } else { Prior::Unknown };
            // Recency is only worth recording while some query-back for THIS shape is between its
            // Postgres read and its evaluation; outside that window there is no older read that
            // could overwrite this decision, so the map stays empty (and no pk id is minted for a
            // non-member that would otherwise never need one).
            let record_recency = shape.inflight_querybacks > 0 && live_stamp.is_some();
            // A query-back's candidates are pre-filtered against the live decisions taken since
            // its snapshot. `should_skip(lsn, xid)` is "this commit was already visible to that
            // snapshot" — i.e. the read already reflects it, so the candidate row is current and
            // the query-back may evaluate it. Its negation is "the live decision is NEWER than
            // this read": the row in hand is stale, the live verdict already stands, and this
            // evaluation must not touch the pk at all. (An unstamped live decision — no LSN and
            // no xid — reads as "newer" and is dropped: only non-Postgres sources leave a change
            // unstamped, and a query-back cannot run without Postgres.)
            let candidates: Vec<(Row, bool)> = match &source {
                // `recent` empty (nothing decided live during this read) is the common case and
                // costs nothing: no pk is recomputed for the whole candidate set.
                EmissionSource::QueryBack { gate, .. } if !shape.recent.is_empty() => {
                    let recent = &shape.recent;
                    candidates
                        .into_iter()
                        .filter(|(row, _)| {
                            let Ok(pk) = ts.key_string(row) else { return true };
                            match self.pk_dict.get(&pk).and_then(|id| recent.get(&id)) {
                                Some(&(lsn, xid)) => gate.should_skip(lsn, xid),
                                None => true,
                            }
                        })
                        .collect()
                }
                EmissionSource::QueryBack { .. } | EmissionSource::Live { .. } | EmissionSource::Replay { .. } => {
                    candidates
                }
            };
            let mut members: Vec<Row> = Vec::new();
            let mut evaluated: Vec<u32> = Vec::new();
            for (row, exists) in candidates {
                let pk = match ts.key_string(&row) {
                    Ok(pk) => pk,
                    Err(_) => continue,
                };
                if exists && pred.matches_ctx(&row, self) {
                    // Current member: deliver the upsert and record presence in the feed.
                    let pk_id = self.pk_dict.get_or_insert(&pk);
                    self.feed_sets.insert(feed_id, pk_id);
                    if same_commit {
                        // Back in the scope: a later departure in this commit is news again.
                        self.commit.forget_removal(feed_id, &pk);
                    }
                    if record_recency {
                        evaluated.push(pk_id);
                    }
                    members.push(row);
                } else {
                    // Non-member. The delete is emitted if the pk was actually in the feed; the
                    // dictionary is probed WITHOUT minting — a never-interned pk can never have
                    // been delivered, so it gates with no id allocated (the same
                    // probe-without-mint rationale as `template_assertions`). The key set is
                    // retracted FIRST and unconditionally, so it never keeps a pk the stream has
                    // been told is gone.
                    let pk_id = self.pk_dict.get(&pk);
                    let delivered = pk_id.is_some_and(|pk_id| self.feed_sets.remove(feed_id, pk_id));
                    // A changes-only feed also owes the delete for a row it never delivered but
                    // its client holds from the snapshot: the row was a member until this change.
                    let proven =
                        !delivered && prior.was_member(&pk, &row, &pred, self, lent.as_ref().unwrap_or(&no_record));
                    if delivered || proven {
                        // Said once per commit, however many of the commit's changes prove it. A
                        // key-set retraction is never held back: it is the stream's own record.
                        let first = !(changes_only && same_commit) || self.commit.first_removal(feed_id, &pk);
                        if delivered || first {
                            deletes.entry(shape_id.clone()).or_default().push(pk.clone());
                        }
                    }
                    if record_recency {
                        // A never-interned non-member still has to win over a query-back holding
                        // an older row for this pk, which WOULD emit an upsert (the empty-shape
                        // row-moved-away case). Recording it needs an id, so this is the one place
                        // a non-member mints one; it is bounded by the pks a live commit touches
                        // while a query-back is in flight.
                        evaluated.push(pk_id.unwrap_or_else(|| self.pk_dict.get_or_insert(&pk)));
                    }
                }
            }
            if let (Some(stamp), Some(shape)) = (live_stamp, self.shapes.get_mut(&shape_id)) {
                for pk_id in evaluated {
                    shape.recent.insert(pk_id, stamp);
                }
            }
            staged.push((shape_id, members));
        }
        // Not put back into a record this evaluation's own removals just pushed past its budget.
        if let Some(undone) = lent
            && !self.commit.saturated
        {
            self.commit.undone = undone;
        }
        // The transaction every envelope of this emission names is the one that CAUSED it: the
        // commit a live or replayed change belongs to, or the commit whose flip a query-back
        // serves — never a later transaction its rows may also reflect. A replay knows its xid, not
        // the text; work a create handed back knows only its commit's LSN, which the registry
        // remembers the transaction of.
        let txid = txid
            .or_else(|| match &source {
                EmissionSource::Live { xid, .. } | EmissionSource::Replay { xid, .. } => xid.map(|xid| xid.to_string()),
                EmissionSource::QueryBack { .. } => None,
            })
            .or_else(|| self.commit_txids.get(&commit_lsn).cloned());
        // Phase 3: build + deliver per shape (still under the caller's lock — enqueue order
        // on each stream's FIFO lane is evaluation order).
        let mut results = Vec::with_capacity(staged.len());
        for (shape_id, members) in staged {
            let dels = deletes.remove(&shape_id).unwrap_or_default();
            let Some(shape) = self.shapes.get_mut(&shape_id) else { continue };
            if members.is_empty() && dels.is_empty() {
                results.push((shape_id, false, 0));
                continue;
            }
            let net = members.len() as i64 - dels.len() as i64;
            // Placed here, under the lock that fixes this stream's append order, so the position
            // an envelope carries can never fall behind one already enqueued ahead of it.
            let lsn = shape.place(commit_lsn);
            let mut envs = crate::engine::translate_output(
                ts,
                members.into_iter().map(|r| (r, 1)).collect(),
                txid.clone(),
                lsn.clone(),
                shape.out_cols.as_deref().map(Vec::as_slice),
            );
            envs.extend(crate::engine::delete_envelopes(ts, dels, txid.clone(), lsn));
            if envs.is_empty() {
                results.push((shape_id, false, 0));
                continue;
            }
            shape.emitted.fetch_add(envs.len() as u64, std::sync::atomic::Ordering::Relaxed);
            let path = shape.stream_path.clone();
            self.deliver(&path, envs, commit_lsn).await?;
            results.push((shape_id, true, net));
        }
        Ok(results)
    }
}

// --- deferred flip propagation ------------------------------------------------------------------
//
// Runs on the engine's flip-worker pool (semaphore-bounded, `ELECTRIC_CIRCUITS_FLIP_WORKERS`), NOT
// inside the table tailers, so the flip-driven Postgres query-backs neither sit on the tailer
// hot path nor serialize on a single task. Two invariants make this sound:
//
//  * **Deferral**: every emission is absolute (per pk: upsert if the row matches *now*, else
//    idempotent delete), so a propagation that runs later re-derives from the then-current
//    Postgres and node state and converges regardless of when — or on which worker — it runs.
//    The convergence barrier gains one term: the engine's pending counter must drain to zero
//    (`GET /replication/lsn` → `pendingFlips`), and that counter also covers emission-lane
//    batches until they LAND on their streams.
//  * **Eval+enqueue atomicity + per-stream FIFO**: membership evaluation and the enqueue of the
//    resulting envelopes happen under one registry-lock scope, and each shape stream drains
//    through exactly one ordered emission lane (`engine::emission`). Per-stream append order
//    therefore equals evaluation order — a move evaluated at time t1 can never land *after* an
//    emission evaluated at t2 > t1 for the same pk (which would leave the stream's last word
//    stale — permanent divergence). This is the same guarantee the old
//    hold-the-lock-across-append design gave, without network under the lock and without a
//    single-task bottleneck. Postgres round-trips run outside the lock, concurrently. A group
//    member's stream is shared and its lane orders it by commit, but never one key against this
//    rule: a key's envelopes land in the order they were enqueued here (`GroupLaneState::order_for`).
//  * **Per-pk recency across the read**: evaluation order is not freshness order for a query-back,
//    whose candidate rows were READ before it took the lock. A live outer-row change committed
//    after that read's snapshot can be evaluated in between, and the query-back would then
//    evaluate its older row last. So while a query-back is in flight the shape records the commit
//    stamp of every live decision it applies, and the query-back drops the candidates whose stamp
//    its own snapshot could not see (`SubqueryShape::recent`, `EmissionSource`). Without this the
//    two invariants above still leave the stream's last word for that pk stale forever — native
//    consumers fold by durable-stream offset, so an older LSN on the envelope repairs nothing.
//  * **A position never falls along a stream**: those three make OFFSET order the truth, and a
//    consumer that orders by `headers.lsn` has to arrive at the same place. A flip's verdict is
//    evaluated after the commit that caused it, so it can follow a newer live change on its
//    stream; it is placed at `max(its own commit, the stream's position)`
//    (`SubqueryShape::position`) and keeps its own `txid`, so that such a consumer never ranks
//    the stream's last word for a pk below an earlier one.

/// How many times a flip batch's propagation is attempted before the engine gives up on the batch
/// — and on itself (see [`propagate_with_retry`]).
pub const FLIP_ATTEMPTS: u32 = 5;
/// Delay after the first failed attempt; doubled per attempt, capped at [`FLIP_BACKOFF_MAX`]. The
/// point is to sit out a transient Postgres fault (a killed backend, a failover, a lock timeout)
/// long enough that the retry has a different answer. Shrunk under `cfg(test)` so the exhaustion
/// test costs milliseconds instead of spending the production schedule's several seconds waiting.
const FLIP_BACKOFF_START: std::time::Duration = std::time::Duration::from_millis(if cfg!(test) { 1 } else { 50 });
const FLIP_BACKOFF_MAX: std::time::Duration = std::time::Duration::from_millis(if cfg!(test) { 8 } else { 2000 });

/// [`propagate_flips`], retried on failure — the only form callers should use.
///
/// A retry RESUMES the walk rather than restarting it: `work` is drained in place and a failed
/// attempt leaves behind exactly what it did not finish (the failing item back at the front), so
/// the next attempt picks up where the last one stopped. Restarting from the original roots would
/// be silently wrong — reconciling a parent node CONSUMES the transition that produced its flips,
/// so a re-walk from the roots finds the parent already moved, derives nothing, and reports `Ok`
/// while the dependents those flips would have moved never move. Re-walking the finished edges of
/// the restored item is harmless: emission is absolute per pk.
pub async fn propagate_with_retry(
    registry: &tokio::sync::Mutex<SubqueryRegistry>,
    work: &mut VecDeque<(SubquerySig, Flip)>,
    txid: Option<String>,
    lsn: Option<String>,
    trace_tx: &tokio::sync::broadcast::Sender<Arc<String>>,
) -> Result<()> {
    let mut backoff = FLIP_BACKOFF_START;
    let mut attempt = 1u32;
    loop {
        let e = match propagate_flips(registry, work, txid.clone(), lsn.clone(), trace_tx).await {
            Ok(()) => return Ok(()),
            Err(e) => e,
        };
        tracing::warn!(
            "subquery flip propagation failed (attempt {attempt} of {FLIP_ATTEMPTS}, {} flip(s) left to walk): {e:#}",
            work.len()
        );
        if attempt >= FLIP_ATTEMPTS {
            return Err(e);
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(FLIP_BACKOFF_MAX);
        attempt += 1;
    }
}

/// [`propagate_deferred_shape_work`], on the same retry schedule (and with the same fail-closed
/// treatment by the caller) as the live walk: work queued during a create is exactly as
/// unreproducible once dropped as work derived from a live flip, so it gets the same guarantees.
pub async fn propagate_deferred_with_retry(
    registry: &tokio::sync::Mutex<SubqueryRegistry>,
    shape_id: &str,
    work: &mut VecDeque<DeferredShapeWork>,
) -> Result<()> {
    let mut backoff = FLIP_BACKOFF_START;
    let mut attempt = 1u32;
    loop {
        let e = match propagate_deferred_shape_work(registry, shape_id, work).await {
            Ok(()) => return Ok(()),
            Err(e) => e,
        };
        tracing::warn!(
            "deferred propagation for shape '{shape_id}' failed (attempt {attempt} of {FLIP_ATTEMPTS}, {} item(s) left): {e:#}",
            work.len()
        );
        if attempt >= FLIP_ATTEMPTS {
            return Err(e);
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(FLIP_BACKOFF_MAX);
        attempt += 1;
    }
}

/// Run the work that reached a shape while it was being created, now that it is installed.
///
/// Only that shape: the flip's walk already visited every other dependent of the flipped node at
/// the time, so re-walking `edges_of` would redo their query-backs for nothing. Drained in place
/// with the failed item restored at the front, exactly like [`propagate_flips`], so the retry
/// resumes rather than restarts.
///
/// No trace is emitted. A flip trace lights the whole path — source `table:` → flipped `node:` →
/// dependent — and a deferred item deliberately carries neither the flipped node's signature nor
/// the table the change entered through; the walk that queued it already lit that path for the
/// dependents it could move.
pub async fn propagate_deferred_shape_work(
    registry: &tokio::sync::Mutex<SubqueryRegistry>,
    shape_id: &str,
    work: &mut VecDeque<DeferredShapeWork>,
) -> Result<()> {
    while let Some(item) = work.pop_front() {
        let res = match &item {
            DeferredShapeWork::Value { connecting_col, value, txid, commit_lsn } => {
                move_shape_for_value(registry, shape_id, *connecting_col, value, txid.clone(), *commit_lsn)
                    .await
                    .map(|_| ())
            }
            DeferredShapeWork::Full { txid, commit_lsn } => {
                rederive_shape(registry, shape_id, txid.clone(), *commit_lsn).await
            }
        };
        if let Err(e) = res {
            work.push_front(item);
            return Err(e);
        }
    }
    Ok(())
}

/// [`propagate_deferred_node_work`], on the same retry schedule (and with the same fail-closed
/// treatment by the caller) as every other propagation: a re-derivation a create deferred is
/// exactly as unreproducible once dropped as a live flip's.
pub async fn propagate_deferred_node_with_retry(
    registry: &tokio::sync::Mutex<SubqueryRegistry>,
    work: &mut VecDeque<(SubquerySig, DeferredNodeWork)>,
    walk: &mut VecDeque<(SubquerySig, Flip)>,
    commit_lsn: u64,
    trace_tx: &tokio::sync::broadcast::Sender<Arc<String>>,
) -> Result<()> {
    let mut backoff = FLIP_BACKOFF_START;
    let mut attempt = 1u32;
    loop {
        let e = match propagate_deferred_node_work(registry, work, walk, commit_lsn, trace_tx).await {
            Ok(()) => return Ok(()),
            Err(e) => e,
        };
        tracing::warn!(
            "deferred node propagation failed (attempt {attempt} of {FLIP_ATTEMPTS}, {} re-derivation(s) + {} flip(s) left): {e:#}",
            work.len(),
            walk.len()
        );
        if attempt >= FLIP_ATTEMPTS {
            return Err(e);
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(FLIP_BACKOFF_MAX);
        attempt += 1;
    }
}

/// Run the re-derivations that reached a parent NODE while its create was still seeding it, now
/// that its set is installed — then walk whatever they flip on down the DAG, so the dependents
/// below (the create's own shape included, installed by now) move too.
///
/// Both queues are drained in place and both are resumable, so the retry picks up where the failed
/// attempt stopped rather than restarting: a failed `requery_and_reconcile_parent` fails at its
/// Postgres read, BEFORE it consumes the transition it was called for, so its item goes back at the
/// front unchanged; and once a re-derivation has produced flips they live on `walk`, which
/// [`propagate_flips`] drains with the same discipline. Restarting instead would find the parent
/// already reconciled and derive nothing while its dependents never moved.
pub async fn propagate_deferred_node_work(
    registry: &tokio::sync::Mutex<SubqueryRegistry>,
    work: &mut VecDeque<(SubquerySig, DeferredNodeWork)>,
    walk: &mut VecDeque<(SubquerySig, Flip)>,
    commit_lsn: u64,
    trace_tx: &tokio::sync::broadcast::Sender<Arc<String>>,
) -> Result<()> {
    while let Some((sig, item)) = work.pop_front() {
        let filter = match &item {
            DeferredNodeWork::Value { connecting_col, value, .. } => Some((*connecting_col, value)),
            DeferredNodeWork::Full { .. } => None,
        };
        match requery_and_reconcile_parent(registry, &sig, filter, commit_lsn).await {
            Ok(Some((_inner, flips))) => {
                for f in flips {
                    walk.push_back((sig.clone(), f));
                }
            }
            // The node was dropped while the work waited: nothing to re-derive, nothing to walk.
            Ok(None) => {}
            Err(e) => {
                work.push_front((sig, item));
                return Err(e);
            }
        }
    }
    // No trace is emitted for the re-derivation itself, for the same reason the deferred shape
    // replay emits none: the walk that queued it already lit the source table → flipped node path.
    // The flips it produced light their own hops from this node down.
    propagate_flips(
        registry,
        walk,
        None,
        Some(format!("{:X}/{:X}", commit_lsn >> 32, commit_lsn & 0xffff_ffff)),
        trace_tx,
    )
    .await
}

/// Propagate a batch of inner-set flips up the dependency DAG (BFS), querying back affected rows.
/// `work` is the walk's whole state and is drained IN PLACE: on failure the item being processed
/// goes back at the front and the rest is untouched, so the caller's retry can resume (see
/// [`propagate_with_retry`], which is what callers should use).
pub async fn propagate_flips(
    registry: &tokio::sync::Mutex<SubqueryRegistry>,
    work: &mut VecDeque<(SubquerySig, Flip)>,
    txid: Option<String>,
    lsn: Option<String>,
    trace_tx: &tokio::sync::broadcast::Sender<Arc<String>>,
) -> Result<()> {
    while let Some((sig, flip)) = work.pop_front() {
        if let Err(e) = propagate_one(registry, &sig, &flip, &txid, &lsn, trace_tx, work).await {
            work.push_front((sig, flip));
            return Err(e);
        }
    }
    Ok(())
}

/// One flip's worth of the walk: move every dependent of the flipped node, pushing any parent-node
/// flips it derives onto `work`. Split out of [`propagate_flips`] so a failure anywhere inside it
/// unwinds to exactly one place — the caller that puts the flip back on the queue.
///
/// Every dependent the flip reaches through the same connecting column on the same table
/// re-evaluates the SAME candidate rows (`<table> WHERE <col> = <value>`), so they share one read
/// instead of issuing one each — one read per (tier, table, connecting column): shapes are read
/// per outer table and parent nodes per inner table, in separate tiers, and dependents reached
/// through different connecting columns are read separately. The read is the expensive part (a pooled connection, a settled
/// `REPEATABLE READ` snapshot, a SELECT), the per-dependent evaluation is in memory. Without this a
/// node with N dependents costs N identical query-backs per flip — the 2026-11-11 backfill storm,
/// where one flip reached 445 shapes and ~1,000 flips kept the flip workers (and every drain
/// receipt waiting behind them) busy for 23 minutes. Shapes are moved before parent nodes; order
/// across dependents carries no meaning, since each shape's emission is absolute per pk and each
/// parent reconciles its own set.
///
/// A NULL flip is batched the same way, through the same two bodies: its NULL-sensitive dependents
/// each need a full re-derive (`SELECT` every row of their table), and that read depends only on
/// the table, so they share one full read per (tier, table) — whatever column reached them.
async fn propagate_one(
    registry: &tokio::sync::Mutex<SubqueryRegistry>,
    sig: &SubquerySig,
    flip: &Flip,
    txid: &Option<String>,
    lsn: &Option<String>,
    trace_tx: &tokio::sync::broadcast::Sender<Arc<String>>,
    work: &mut VecDeque<(SubquerySig, Flip)>,
) -> Result<()> {
    let _measurement = crate::stall_profile::Timer::start(crate::stall_profile::FLIP_ONE);
    let commit_lsn = lsn.as_deref().map(crate::pg::lsn_to_u64).unwrap_or(0);
    // The flipped inner-set node's dependents, plus the table its change entered through: the
    // head of the propagation path each dependent's trace lights (`table:<t>` → `node:<sig>` →
    // dependent). Fetched under one lock so a concurrent drop can't split them.
    let (edges, source_table) = {
        let reg = crate::stall_profile::flip_lock(registry).await;
        (reg.edges_of(sig), reg.nodes.get(sig).map(|n| n.inner_table.clone()))
    };
    // A NULL-value flip only matters to NULL-sensitive dependents — a `NOT IN` leaf, or an `IN`
    // leaf under any `Not{…}` (SQL: a NULL in the set makes the leaf UNKNOWN, which negation turns
    // into a membership change). It can shift *every* dependent row, so re-derive the dependent
    // fully; NULL-insensitive dependents can't change (AND/OR are monotone over
    // FALSE < UNKNOWN < TRUE), so skip. The full reads are shared per table, like a value flip's.
    if matches!(flip.value, Value::Null) {
        let (shapes, parents) = null_sensitive_dependents(edges);
        requery_and_move_shapes(registry, &shapes, None, Some((sig, flip)), txid.clone(), commit_lsn).await?;
        for (parent_sig, _inner, flips) in requery_and_reconcile_parents(registry, &parents, None, commit_lsn).await? {
            for f in flips {
                work.push_back((parent_sig.clone(), f));
            }
        }
        return Ok(());
    }
    let (shapes, parents) = group_dependents_by_column(edges);
    for (col, ids) in shapes {
        let moved = requery_and_move_shapes(
            registry,
            &ids,
            Some((col, &flip.value)),
            Some((sig, flip)),
            txid.clone(),
            commit_lsn,
        )
        .await?;
        // Light the whole path only for the shapes that actually moved rows: source `table:<t>` →
        // the flipped `node:<sig>` → that `shape:<id>`.
        let Some(src) = source_table.as_ref() else { continue };
        for (id, outer, net) in moved {
            emit_flip_trace(
                trace_tx,
                &outer,
                src,
                sig,
                format!("shape:{id}"),
                vec![id],
                net,
                lsn.clone(),
                txid.clone(),
            );
        }
    }
    for (col, parent_sigs) in parents {
        // A parent missing from the result vanished, or is a fresh node its own create is still
        // seeding — in which case the re-derivation was queued on the node and runs when that
        // create installs the seed (see `queue_node_deferred`).
        let reconciled =
            requery_and_reconcile_parents(registry, &parent_sigs, Some((col, &flip.value)), commit_lsn).await?;
        for (parent_sig, _inner, flips) in reconciled {
            // A nested `IN`: connect the flipped child `node:<sig>` to the parent
            // `node:<parent_sig>` it re-derived, so the propagation reads through. The parent's
            // own downstream shape lights when its flips reach a shape edge.
            if let (false, Some(src)) = (flips.is_empty(), source_table.as_ref()) {
                emit_flip_trace(
                    trace_tx,
                    src,
                    src,
                    sig,
                    format!("node:{parent_sig}"),
                    Vec::new(),
                    flip_net(&flips),
                    lsn.clone(),
                    txid.clone(),
                );
            }
            for f in flips {
                work.push_back((parent_sig.clone(), f));
            }
        }
    }
    Ok(())
}

/// A flipped node's dependents, grouped by the connecting column they are reached through (the
/// column a query-back filters on), shapes and parent nodes apart, each dependent once per column
/// and in edge order.
#[allow(clippy::type_complexity)]
fn group_dependents_by_column(edges: Vec<Edge>) -> (Vec<(usize, Vec<String>)>, Vec<(usize, Vec<SubquerySig>)>) {
    fn push(groups: &mut Vec<(usize, Vec<String>)>, seen: &mut HashSet<(usize, String)>, col: usize, id: String) {
        if !seen.insert((col, id.clone())) {
            return;
        }
        match groups.iter_mut().find(|(c, _)| *c == col) {
            Some((_, ids)) => ids.push(id),
            None => groups.push((col, vec![id])),
        }
    }
    let (mut shapes, mut parents) = (Vec::new(), Vec::new());
    let (mut seen_shapes, mut seen_parents) = (HashSet::new(), HashSet::new());
    for edge in edges {
        match edge.dependent {
            Dependent::Shape(id) => push(&mut shapes, &mut seen_shapes, edge.connecting_col, id),
            Dependent::Node(parent) => push(&mut parents, &mut seen_parents, edge.connecting_col, parent),
        }
    }
    (shapes, parents)
}

/// A NULL flip's dependents: only the NULL-sensitive ones, shapes and parent nodes apart, each once
/// and in edge order. A full re-derive reads every row of the dependent's table, so the connecting
/// column an edge reached it through makes no difference and a dependent reached through two is
/// re-derived once.
fn null_sensitive_dependents(edges: Vec<Edge>) -> (Vec<String>, Vec<SubquerySig>) {
    let (mut shapes, mut parents) = (Vec::new(), Vec::new());
    let (mut seen_shapes, mut seen_parents) = (HashSet::new(), HashSet::new());
    for edge in edges.into_iter().filter(|e| e.null_sensitive) {
        match edge.dependent {
            Dependent::Shape(id) => {
                if seen_shapes.insert(id.clone()) {
                    shapes.push(id);
                }
            }
            Dependent::Node(parent) => {
                if seen_parents.insert(parent.clone()) {
                    parents.push(parent);
                }
            }
        }
    }
    (shapes, parents)
}

/// Re-query outer shapes' candidate rows — `Some((col, v))` = only rows with `connecting_col = v`
/// (an inner-set value `v` flipped), `None` = every row (a NULL flip's full re-derive, or a `Full`
/// deferred item) — ONCE per outer table, however many shapes share it, re-evaluate each shape's
/// full predicate against them, and append `upsert` (matches) / `delete` (doesn't, and was in the
/// feed) by pk. Returns `(shape_id, outer_table, net_weight)` for each shape that appended
/// envelopes — the shape's own table (the event's `table`) and the net membership change (for the
/// trace dot's label/colour). A shape still being created gets the work queued on its pending entry
/// instead, and a shape that vanished is skipped. The shared body of every flip-driven shape path,
/// as [`requery_and_reconcile_parents`] is for parent nodes: the fetch differs, the evaluation and
/// emission never do.
///
/// `cause` is the flip being propagated, when there is one (deferred work replayed at install has
/// none): it is what lets a changes-only feed remove a row it never delivered — the row was a
/// member until this flip iff it satisfies the predicate with the flip looked past (see [`Prior`]).
async fn requery_and_move_shapes(
    registry: &tokio::sync::Mutex<SubqueryRegistry>,
    shape_ids: &[String],
    filter: Option<(usize, &Value)>,
    cause: Option<(&SubquerySig, &Flip)>,
    txid: Option<String>,
    commit_lsn: u64,
) -> Result<Vec<(String, TableRef, i64)>> {
    // Brief lock: snapshot what the query-backs need, grouped by outer table.
    let (groups, reader) = {
        let mut reg = crate::stall_profile::flip_lock(registry).await;
        let mut groups: Vec<(TableSchema, Vec<String>)> = Vec::new();
        for shape_id in shape_ids {
            // A shape whose create is still in flight has edges (phase A) but no installed shape to
            // move: the flip waits on its pending entry and runs at install, instead of falling
            // through to a `shapes` miss that would silently drop it.
            let deferred = match filter {
                Some((connecting_col, value)) => {
                    DeferredShapeWork::Value { connecting_col, value: value.clone(), txid: txid.clone(), commit_lsn }
                }
                None => DeferredShapeWork::Full { txid: txid.clone(), commit_lsn },
            };
            if reg.queue_deferred(shape_id, deferred) {
                continue;
            }
            let Some(shape) = reg.shapes.get(shape_id) else { continue };
            let table = shape.outer_table.clone();
            match groups.iter_mut().find(|(ts, _)| ts.table == table) {
                Some((_, ids)) => ids.push(shape_id.clone()),
                None => groups.push((reg.snapshot_for_table(&table)?, vec![shape_id.clone()])),
            }
        }
        // In flight from here until the evaluation below: live decisions taken in that window are
        // stamped on each shape and beat whatever the shared read returns. Marked only after every
        // fallible step above, so an early return leaves nothing to balance.
        for (_, ids) in &groups {
            for shape_id in ids {
                reg.begin_queryback(shape_id);
            }
        }
        (groups, reg.candidate_reader())
    };
    // Every exit from here on must decrement every marked shape — including a read's error path —
    // or the shape would keep recording recency (and dropping candidates) forever. There is no
    // guard object that can do it: the registry mutex is async and cannot be taken in `Drop`.
    let end_all = |reg: &mut SubqueryRegistry| {
        for (_, ids) in &groups {
            for shape_id in ids {
                reg.end_queryback(shape_id);
            }
        }
    };
    let mut reads = Vec::with_capacity(groups.len());
    for (ts, _) in &groups {
        match reader.read(ts, filter).await {
            Ok(read) => reads.push(read),
            Err(e) => {
                end_all(&mut *crate::stall_profile::flip_lock(registry).await);
                return Err(e);
            }
        }
    }
    // Evaluate + assert + deliver through the ONE emission tail (candidates from a query-back all
    // still exist), each shape filtered against the live decisions taken since the shared read's
    // snapshot, with its own copy of the shared candidates (copying rows in memory is cheap next to
    // another read). ONE shape per lock hold, released from in flight as soon as it is evaluated:
    // one hold across every dependent — hundreds of them, each over a whole table on a NULL flip —
    // would keep the live sequencer path and every other flip worker off the registry for all N
    // evaluations at once. Each shape's eval + FeedSet transition + enqueue stays atomic under its
    // own hold, and the shapes still waiting stay marked, so live decisions taken in between are
    // stamped on them and beat the shared read exactly as before.
    let mut moved = Vec::new();
    for (g, ((ts, ids), (rows, gate))) in groups.iter().zip(reads).enumerate() {
        // The emission tail only ever touches the candidates it is given, so an empty read moves
        // nothing for any shape (a full re-derive included): release the whole group in one hold.
        if rows.is_empty() {
            let mut reg = crate::stall_profile::flip_lock(registry).await;
            for shape_id in ids {
                reg.end_queryback(shape_id);
            }
            continue;
        }
        let candidates: Vec<(Row, bool)> = rows.into_iter().map(|r| (r, true)).collect();
        for (k, shape_id) in ids.iter().enumerate() {
            let mut reg = crate::stall_profile::flip_lock(registry).await;
            // The proof a changes-only feed needs: the flip itself. The emission tail adds whatever
            // else the flip's commit did to the inner sets, while that commit is still the one
            // being ingested, so a row whose membership rested on two of them is still seen to
            // have been a member.
            let prior = match cause {
                Some((sig, flip)) => Prior::Flip { sig, value: &flip.value, left: matches!(flip.dir, FlipDir::Leave) },
                None => Prior::Unknown,
            };
            let emitted = reg
                .emit_for_shapes_with(
                    ts,
                    vec![(shape_id.clone(), candidates.clone())],
                    txid.clone(),
                    EmissionSource::QueryBack { gate: &gate, commit_lsn },
                    prior,
                )
                .await;
            reg.end_queryback(shape_id);
            match emitted {
                Ok(emitted) => moved.extend(
                    emitted
                        .into_iter()
                        .filter(|(_, appended, _)| *appended)
                        .map(|(id, _, net)| (id, ts.table.clone(), net)),
                ),
                Err(e) => {
                    // This shape is released above; release every one not evaluated yet.
                    let later = groups[g + 1..].iter().flat_map(|(_, ids)| ids);
                    for rest in ids[k + 1..].iter().chain(later) {
                        reg.end_queryback(rest);
                    }
                    return Err(e);
                }
            }
        }
    }
    Ok(moved)
}

/// [`requery_and_move_shapes`] for one shape and one flipped value: `Some((outer_table,
/// net_weight))` when it appended envelopes, `None` when nothing moved (or the shape is gone or still
/// being created).
async fn move_shape_for_value(
    registry: &tokio::sync::Mutex<SubqueryRegistry>,
    shape_id: &str,
    connecting_col: usize,
    value: &Value,
    txid: Option<String>,
    commit_lsn: u64,
) -> Result<Option<(TableRef, i64)>> {
    let shape_ids = [shape_id.to_string()];
    let mut moved =
        requery_and_move_shapes(registry, &shape_ids, Some((connecting_col, value)), None, txid, commit_lsn).await?;
    Ok(moved.pop().map(|(_, table, net)| (table, net)))
}

/// Re-query parent nodes' inner rows — `Some((col, v))` = only rows with `connecting_col = v` (a
/// value flip), `None` = every row (a NULL re-derive) — ONCE per inner table, however many parents
/// share it, then re-evaluate each parent's full predicate and reconcile. Returns
/// `(parent, inner_table, flips)` for every parent reconciled; a parent that vanished, or is still
/// being seeded (its work is queued on it), is absent. The shared body of every flip-driven parent
/// path: the fetch differs, the eval+reconcile never does.
///
/// Every read happens before any parent is reconciled, so a failed read consumes no transition and
/// the caller's retry finds every parent exactly as it was.
async fn requery_and_reconcile_parents(
    registry: &tokio::sync::Mutex<SubqueryRegistry>,
    parent_sigs: &[SubquerySig],
    filter: Option<(usize, &Value)>,
    commit_lsn: u64,
) -> Result<Vec<(SubquerySig, TableRef, Vec<Flip>)>> {
    let (groups, reader) = {
        let mut reg = crate::stall_profile::flip_lock(registry).await;
        let mut groups: Vec<(TableSchema, Vec<SubquerySig>)> = Vec::new();
        for parent_sig in parent_sigs {
            // Same as `requery_and_move_shapes`'s pending-shape check, one tier down: a parent node
            // whose own create is still seeding it has edges (phase A) but no set to reconcile
            // against. The work waits on the node and runs at install, instead of re-deriving
            // against an empty set whose seed — older than this flip — would then be installed
            // over the change.
            if reg.queue_node_deferred(
                parent_sig,
                match filter {
                    Some((col, value)) => {
                        DeferredNodeWork::Value { connecting_col: col, value: value.clone(), commit_lsn }
                    }
                    None => DeferredNodeWork::Full { commit_lsn },
                },
            ) {
                continue;
            }
            let Some(n) = reg.nodes.get(parent_sig) else { continue };
            let table = n.inner_table.clone();
            match groups.iter_mut().find(|(ts, _)| ts.table == table) {
                Some((_, sigs)) => sigs.push(parent_sig.clone()),
                None => groups.push((reg.snapshot_for_table(&table)?, vec![parent_sig.clone()])),
            }
        }
        // In flight from here until the reconcile below: live contribution decisions taken in that
        // window are stamped on each node and beat whatever the shared read returns. Marked only
        // after every fallible step above.
        for (_, sigs) in &groups {
            for parent_sig in sigs {
                reg.begin_node_queryback(parent_sig);
            }
        }
        (groups, reg.candidate_reader())
    };
    // Every exit from here on must decrement every marked node — the read's error path included —
    // or the node would keep recording recency forever (same reason as `requery_and_move_shapes`).
    let mut reads = Vec::with_capacity(groups.len());
    for (ts, _) in &groups {
        match reader.read(ts, filter).await {
            Ok(read) => reads.push(read),
            Err(e) => {
                let mut reg = crate::stall_profile::flip_lock(registry).await;
                for (_, sigs) in &groups {
                    for parent_sig in sigs {
                        reg.end_node_queryback(parent_sig);
                    }
                }
                return Err(e);
            }
        }
    }
    // ONE parent per lock hold, released from in flight as soon as it is reconciled — the same reason
    // as `requery_and_move_shapes`: one hold across every parent would keep the registry locked for
    // all N re-derivations. The parents still waiting stay marked. Reconciling cannot fail, so there
    // is no partial-exit path to balance.
    let mut reconciled = Vec::new();
    for ((ts, sigs), (rows, gate)) in groups.iter().zip(reads) {
        // An empty read asserts nothing for any parent: reconcile (to no flips) and release the
        // whole group in one hold.
        if rows.is_empty() {
            let mut reg = crate::stall_profile::flip_lock(registry).await;
            for parent_sig in sigs {
                if reg.nodes.contains_key(parent_sig) {
                    reconciled.push((parent_sig.clone(), ts.table.clone(), Vec::new()));
                }
                reg.end_node_queryback(parent_sig);
            }
            continue;
        }
        for parent_sig in sigs {
            let mut reg = crate::stall_profile::flip_lock(registry).await;
            if let Some(evals) = reg.node_queryback_evals(parent_sig, ts, &rows, &gate) {
                let flips = reg.apply_node_evals(parent_sig, evals).await;
                // A parent's set moves here, not in `on_table_delta`: the commit changed a row of a
                // deeper inner table, and this re-derivation is where the parent learns of it. It
                // is that commit's transition all the same, and a row that rests on two such
                // parents is proven a member only if both are in the commit's record.
                for flip in &flips {
                    reg.note_commit_flip(commit_lsn, parent_sig, flip);
                }
                reconciled.push((parent_sig.clone(), ts.table.clone(), flips));
            }
            reg.end_node_queryback(parent_sig);
            #[cfg(test)]
            if let Some(probe) = reg.reconcile_probe.clone() {
                probe(&mut *reg, parent_sig.as_str());
            }
        }
    }
    Ok(reconciled)
}

/// [`requery_and_reconcile_parents`] for one parent: `Some((inner_table, flips))`, or `None` if the
/// parent vanished or is still being seeded.
async fn requery_and_reconcile_parent(
    registry: &tokio::sync::Mutex<SubqueryRegistry>,
    parent_sig: &SubquerySig,
    filter: Option<(usize, &Value)>,
    commit_lsn: u64,
) -> Result<Option<(TableRef, Vec<Flip>)>> {
    let mut reconciled =
        requery_and_reconcile_parents(registry, std::slice::from_ref(parent_sig), filter, commit_lsn).await?;
    Ok(reconciled.pop().map(|(_, table, flips)| (table, flips)))
}

/// Re-derive one outer shape from scratch: [`requery_and_move_shapes`] with no filter, so every row
/// of its table is a candidate and the ONE emission tail decides each one's membership — except for
/// pks a live commit decided after the read's snapshot, which it drops. Replays a `Full` deferred
/// item; a live NULL flip re-derives all its NULL-sensitive shapes in one call instead.
async fn rederive_shape(
    registry: &tokio::sync::Mutex<SubqueryRegistry>,
    shape_id: &str,
    txid: Option<String>,
    commit_lsn: u64,
) -> Result<()> {
    let shape_ids = [shape_id.to_string()];
    requery_and_move_shapes(registry, &shape_ids, None, None, txid, commit_lsn).await.map(|_| ())
}

// Candidate-row resolution (arrangement snapshot → pooled Postgres fallback) is the shared
// membership kernel's — one implementation for this registry and for circuit cohort serving.
use crate::engine::membership::{query_rows_all as query_all, query_rows_by_col as query_candidates};

/// The read behind every flip query-back: pooled Postgres, through the shared membership kernel.
/// Unit tests swap in an in-memory table that counts its reads, which is how the query-back
/// fan-out is asserted without a live database.
#[derive(Clone)]
struct CandidateReader {
    pool: Option<crate::pg::Pool>,
    #[cfg(test)]
    fake: Option<Arc<tests::FakeCandidates>>,
}

impl CandidateReader {
    /// `<ts> WHERE <col> = <value>`, with the read's snapshot gate.
    async fn by_col(&self, ts: &TableSchema, col: usize, value: &Value) -> Result<(Vec<Row>, crate::pg::SnapshotGate)> {
        #[cfg(test)]
        if let Some(fake) = &self.fake {
            return fake.read(ts, Some((col, value)));
        }
        query_candidates(&self.pool, ts, col, value).await
    }

    /// Every row of `ts` (a full re-derive), with the read's snapshot gate.
    async fn all(&self, ts: &TableSchema) -> Result<(Vec<Row>, crate::pg::SnapshotGate)> {
        #[cfg(test)]
        if let Some(fake) = &self.fake {
            return fake.read(ts, None);
        }
        query_all(&self.pool, ts).await
    }

    async fn read(
        &self,
        ts: &TableSchema,
        filter: Option<(usize, &Value)>,
    ) -> Result<(Vec<Row>, crate::pg::SnapshotGate)> {
        match filter {
            Some((col, value)) => self.by_col(ts, col, value).await,
            None => self.all(ts).await,
        }
    }
}

/// Net membership change carried by a batch of parent-node flips (enters +1, leaves −1), for the
/// trace dot's label/colour.
fn flip_net(flips: &[Flip]) -> i64 {
    flips
        .iter()
        .map(|f| match f.dir {
            FlipDir::Enter => 1,
            FlipDir::Leave => -1,
        })
        .sum()
}

/// Broadcast a lossy trace event lighting the WHOLE path a deferred inner-set flip travelled: the
/// source inner `table:<t>` the change entered through, the flipped inner-set `node:<sig>` (its
/// `IN-SET ARRANGE`/distinct), and the re-derived `dependent` (`shape:<sid>` for an outer subquery
/// shape, or a parent `node:<sig>` for nested `IN`). The originating envelope's own trace stops at
/// the inner-set node — the propagator moves the dependent out of band, after the query-backs — so
/// without this the visualizer flashes the source, fades the moved shape off that direct change's
/// path, and never pulses the serving edge (an edge pulses only when both endpoints flash). One
/// synthetic weighted row carries the net membership change so the travelling dot is labelled +1 /
/// −1 and coloured. Best-effort and zero-cost when no one is subscribed, mirroring the in-engine
/// trace path.
///
/// `event_table` is the table the event is *about* (the dependent shape's own table, matching the
/// direct-change trace's `table`); `source_table` is where the change entered and heads the hop path
/// (they differ: a `project_members` change moves an `issues` shape).
fn emit_flip_trace(
    trace_tx: &tokio::sync::broadcast::Sender<Arc<String>>,
    event_table: &TableRef,
    source_table: &TableRef,
    node_sig: &SubquerySig,
    dependent: String,
    shapes: Vec<String>,
    net: i64,
    lsn: Option<String>,
    txid: Option<String>,
) {
    if trace_tx.receiver_count() == 0 {
        return;
    }
    let ev = crate::trace::TraceEvent {
        lsn,
        txid,
        table: event_table.clone(),
        // One synthetic weighted row carrying the net change: a single flip can move many outer
        // rows, so the payload is not one table row — left empty, weighted by the net.
        delta: vec![crate::trace::TraceDelta { row: serde_json::json!({}), w: net }],
        hops: vec![
            crate::trace::TraceHop::new(format!("table:{source_table}"), "passed"),
            crate::trace::TraceHop::new(format!("node:{node_sig}"), "passed"),
            crate::trace::TraceHop::new(dependent, "passed"),
        ],
        shapes,
    };
    if let Ok(json) = serde_json::to_string(&ev) {
        let _ = trace_tx.send(Arc::new(json));
    }
}

impl SubqueryCollector for SubqueryRegistry {
    /// Discover (or dedupe) a subquery node: compile its inner predicate (recursively collecting deeper
    /// nodes), record its child edges, and queue it for seeding. Returns the canonical signature.
    fn collect(&mut self, table: &TableRef, project: &str, where_: Option<&PredicateJson>) -> Result<SubquerySig> {
        let sig = subquery_sig(table, project, where_);
        if let Some(n) = self.nodes.get_mut(&sig) {
            n.refcount += 1;
            self.collect_log.push(sig.clone());
            return Ok(sig);
        }
        let inner_ts = self.schemas.get(table).cloned().context("subquery: unknown inner table")?;
        let inner_pred = match where_ {
            Some(w) => CompiledPredicate::compile_with(w, &inner_ts, self)?,
            None => CompiledPredicate::MatchAll,
        };
        // Record edges from each child node to THIS node (so a child flip re-derives this node's rows).
        for leaf in collect_in_leaves(&inner_pred) {
            self.staged_edges.push(Edge {
                node_sig: leaf.sig,
                dependent: Dependent::Node(sig.clone()),
                connecting_col: leaf.col,
                negated: leaf.negated,
                null_sensitive: leaf.null_sensitive,
            });
        }
        let proj_col = inner_ts.column_index(project)?;
        let node_id = self.next_node_id;
        self.next_node_id += 1;
        // Template registration: lift the equality literals into a bind (coerced to the
        // column types, same as leaf compilation) and share the residual across binds.
        let (tkey, bind_literals, residual_json) = crate::predicate::subquery_template(table, project, where_);
        let mut param_cols = Vec::with_capacity(bind_literals.len());
        let mut bind_vals = Vec::with_capacity(bind_literals.len());
        for (col, lit) in &bind_literals {
            let idx = inner_ts.column_index(col)?;
            param_cols.push(idx);
            bind_vals.push(Value::literal_for_column(lit, inner_ts.column_type(idx), inner_ts.pg_type_of(col))?);
        }
        let bind = Row(bind_vals);
        if !self.templates.contains_key(&tkey) {
            // The residual is compiled with a sig-only collector: any nested IN inside it was
            // already collected (and refcounted) by the full-pred compile above; collecting
            // again would double-count.
            struct SigOnly;
            impl SubqueryCollector for SigOnly {
                fn collect(&mut self, t: &TableRef, p: &str, w: Option<&PredicateJson>) -> Result<SubquerySig> {
                    Ok(subquery_sig(t, p, w))
                }
            }
            let residual = if residual_json.is_empty() {
                CompiledPredicate::MatchAll
            } else {
                CompiledPredicate::compile_with(&PredicateJson::And { and: residual_json }, &inner_ts, &mut SigOnly)?
            };
            self.templates.insert(
                tkey.clone(),
                TemplateGroup {
                    inner_table: table.clone(),
                    proj_col,
                    residual: Arc::new(residual),
                    param_cols: param_cols.clone(),
                    binds: HashMap::new(),
                    pk_nodes: HashMap::new(),
                },
            );
        }
        if let Some(tpl) = self.templates.get_mut(&tkey) {
            tpl.binds.insert(bind.clone(), sig.clone());
        }
        let mut node =
            SubqueryNode::new(sig.clone(), table.clone(), proj_col, inner_ts.pk_index, Arc::new(inner_pred), node_id);
        node.where_json = where_.cloned();
        node.template_key = tkey;
        node.bind = bind;
        node.refcount = 1;
        self.nodes.insert(sig.clone(), node);
        self.node_by_id.insert(node_id, sig.clone());
        self.collect_log.push(sig.clone());
        self.pending_seed.push(sig.clone());
        Ok(sig)
    }
}

impl SubqueryEval for SubqueryRegistry {
    fn contains(&self, sig: &SubquerySig, value: &Value) -> bool {
        self.nodes.get(sig).is_some_and(|n| self.circuit.contains(n.node_id, value))
    }
    fn has_null(&self, sig: &SubquerySig) -> bool {
        self.nodes.get(sig).is_some_and(|n| self.circuit.contains(n.node_id, &Value::Null))
    }
}

/// One `IN (SELECT …)` leaf found in a compiled predicate, with the context needed to build its
/// dependency edge.
pub struct InLeaf {
    pub col: usize,
    pub sig: SubquerySig,
    pub negated: bool,
    /// leaf negated OR under any `Not{…}` wrapper — see [`Edge::null_sensitive`].
    pub null_sensitive: bool,
}

/// Find all `IN (SELECT …)` leaves in a compiled predicate, tracking whether each sits under a `Not`
/// (which makes it NULL-sensitive even when the leaf itself isn't negated — `NOT (x IN S)` flips
/// membership when a NULL enters `S`, exactly like `x NOT IN S`).
pub fn collect_in_leaves(p: &CompiledPredicate) -> Vec<InLeaf> {
    let mut out = Vec::new();
    fn go(p: &CompiledPredicate, under_not: bool, out: &mut Vec<InLeaf>) {
        match p {
            CompiledPredicate::And(v) | CompiledPredicate::Or(v) => v.iter().for_each(|c| go(c, under_not, out)),
            CompiledPredicate::Not(b) => go(b, true, out),
            CompiledPredicate::InSubquery { col, sig, negated } => out.push(InLeaf {
                col: *col,
                sig: sig.clone(),
                negated: *negated,
                null_sensitive: *negated || under_not,
            }),
            _ => {}
        }
    }
    go(p, false, &mut out);
    out
}

/// Does a JSON predicate contain any `IN (SELECT …)` subquery?
pub fn predicate_has_subquery(p: &PredicateJson) -> bool {
    match p {
        PredicateJson::In { .. } => true,
        PredicateJson::And { and } => and.iter().any(predicate_has_subquery),
        PredicateJson::Or { or } => or.iter().any(predicate_has_subquery),
        PredicateJson::Not { not } => predicate_has_subquery(not),
        PredicateJson::Leaf { .. } | PredicateJson::IsNull { .. } => false,
    }
}

/// Every table referenced by a JSON predicate's subqueries (inner tables, recursively).
pub fn referenced_tables(p: &PredicateJson) -> Vec<TableRef> {
    let mut out = Vec::new();
    fn go(p: &PredicateJson, out: &mut Vec<TableRef>) {
        match p {
            PredicateJson::In { subquery, .. } => {
                if !out.contains(&subquery.table) {
                    out.push(subquery.table.clone());
                }
                if let Some(w) = &subquery.where_ {
                    go(w, out);
                }
            }
            PredicateJson::And { and } => and.iter().for_each(|c| go(c, out)),
            PredicateJson::Or { or } => or.iter().for_each(|c| go(c, out)),
            PredicateJson::Not { not } => go(not, out),
            PredicateJson::Leaf { .. } | PredicateJson::IsNull { .. } => {}
        }
    }
    go(p, &mut out);
    out
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A registry with one MatchAll node registered the way `collect()` would: node map,
    /// node_by_id, and a unit-bind template — so `on_table_delta`, `apply_node_evals`, and the
    /// `SubqueryEval` reads all work.
    fn registry_with_node(sig: &str) -> SubqueryRegistry {
        let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test("http://unused"), None);
        insert_test_node(&mut reg, sig);
        reg
    }

    pub(crate) fn insert_test_node(reg: &mut SubqueryRegistry, sig: &str) {
        insert_test_node_on(reg, sig, "t", 0);
    }

    /// [`insert_test_node`] over a named inner table, projecting `proj_col`.
    fn insert_test_node_on(reg: &mut SubqueryRegistry, sig: &str, inner_table: &str, proj_col: usize) {
        let node_id = reg.next_node_id;
        reg.next_node_id += 1;
        let mut node = SubqueryNode::new(
            sig.into(),
            inner_table.into(),
            proj_col,
            1,
            Arc::new(CompiledPredicate::MatchAll),
            node_id,
        );
        let tkey = format!("tpl:{sig}");
        node.template_key = tkey.clone();
        node.refcount = 1;
        reg.nodes.insert(sig.into(), node);
        reg.node_by_id.insert(node_id, sig.into());
        reg.templates.insert(
            tkey,
            TemplateGroup {
                inner_table: inner_table.into(),
                proj_col,
                residual: Arc::new(CompiledPredicate::MatchAll),
                param_cols: Vec::new(),
                binds: [(Row(Vec::new()), sig.to_string())].into_iter().collect(),
                pk_nodes: HashMap::new(),
            },
        );
    }

    /// The trace reports an inner-table delta's effect on a subquery node: `passed` when the
    /// inner set flipped (a value entered/left), `dropped` when it didn't change.
    #[tokio::test]
    async fn trace_subquery_node_hops() {
        use crate::schema::TableDef;
        let ts = {
            let def: TableDef = serde_json::from_value(serde_json::json!({
                "columns": { "id": {"type":"int"} }, "primaryKey": "id"
            }))
            .unwrap();
            crate::schema::TableSchema::from_def(&"t".into(), &def).unwrap()
        };
        let mut reg = SubqueryRegistry::new(crate::ds::DsClient::new_for_in_process_test("http://127.0.0.1:1"), None);
        insert_test_node(&mut reg, "sig1");

        // A new row projects value 1 into the inner set -> Enter flip -> passed.
        let delta = vec![Tup2(Row(vec![Value::Int(1)]), 1)];
        let mut hops = Vec::new();
        reg.on_table_delta(&ts, &delta, 0, None, None, Some(&mut hops)).await.unwrap();
        assert!(
            hops.iter().any(|h| h.node == "node:sig1" && h.outcome == "passed"),
            "expected passed node hop, got {hops:?}"
        );

        // The same row again: the value is already present -> no flip -> dropped.
        let mut hops = Vec::new();
        reg.on_table_delta(&ts, &delta, 0, None, None, Some(&mut hops)).await.unwrap();
        assert!(
            hops.iter().any(|h| h.node == "node:sig1" && h.outcome == "dropped"),
            "expected dropped node hop, got {hops:?}"
        );
    }

    /// A deferred inner-set flip that moves a dependent shape must light the WHOLE propagation path
    /// — the source inner `table:`, the flipped `node:<sig>` (IN-SET ARRANGE/distinct), and the
    /// re-derived `shape:<sid>` — so the visualizer animates the moved shape instead of fading it
    /// off the direct change's path (the "leaving a project" bug).
    #[test]
    fn flip_trace_lights_source_node_and_shape() {
        let (trace_tx, mut trace_rx) = tokio::sync::broadcast::channel::<Arc<String>>(8);
        // A `project_members` delete flipped inner-set value; the `issues` shape s1 lost 103 rows.
        let sig = "project_members|project_id|L(user_id,Eq,1)".to_string();
        emit_flip_trace(
            &trace_tx,
            &"issues".into(),
            &"project_members".into(),
            &sig,
            "shape:s1".into(),
            vec!["s1".into()],
            -103,
            Some("0/1A2B3C".into()),
            Some("777".into()),
        );

        let ev: serde_json::Value = serde_json::from_str(&trace_rx.try_recv().unwrap()).unwrap();
        // The event is about the dependent shape's table; the path still heads at the source.
        assert_eq!(ev["table"], "public.issues");
        // Carries the originating write's lsn/txid, so the activity log can group this deferred
        // flip event together with the direct-change event that triggered it (same commit).
        assert_eq!(ev["lsn"], "0/1A2B3C");
        assert_eq!(ev["txid"], "777");
        let outcome = |node: &str| {
            ev["hops"].as_array().unwrap().iter().find(|h| h["node"] == node).map(|h| h["outcome"].clone())
        };
        assert_eq!(outcome("table:public.project_members"), Some(serde_json::json!("passed")), "source lit");
        assert_eq!(outcome(&format!("node:{sig}")), Some(serde_json::json!("passed")), "subquery node lit");
        assert_eq!(outcome("shape:s1"), Some(serde_json::json!("passed")), "dependent shape lit");
        assert_eq!(ev["shapes"].as_array().unwrap(), &vec![serde_json::json!("s1")]);
        assert_eq!(ev["delta"][0]["w"], -103, "the dot carries the real net −103 leave, not 0");
    }

    /// **A failed walk keeps its own remains.** The DAG walk is not restartable from its roots:
    /// reconciling a parent node consumes the transition that produced its flips, so a retry from
    /// the roots finds the parent already moved, derives nothing, and reports success while the
    /// dependents those flips would have moved never move — lost effects that also bypass the
    /// fail-closed path, because nothing errored the second time.
    ///
    /// What makes the retry sound is that the queue survives the failure: whatever the attempt did
    /// not finish, including the item that failed, is still there for the next attempt. Asserted
    /// here directly, since a partial walk through a nested DAG needs a live Postgres to reach.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_propagation_leaves_its_work_for_the_retry() {
        let ts = issues_ts();
        // No `pg_url`: the first query-back fails, as an outage makes it fail.
        let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test("http://unused"), None);
        let sig: SubquerySig = "project_members|project_id|L(user_id,Eq,1)".into();
        insert_test_node(&mut reg, &sig);
        insert_membership_shape(&mut reg, "s1", &sig, 1);
        reg.add_edge(Edge {
            node_sig: sig.clone(),
            dependent: Dependent::Shape("s1".into()),
            connecting_col: 1,
            negated: false,
            null_sensitive: false,
        });
        reg.set_schemas(Arc::new([(TableRef::parse("issues").unwrap(), ts)].into_iter().collect()));
        let registry = tokio::sync::Mutex::new(reg);
        let (trace_tx, _rx) = tokio::sync::broadcast::channel(16);

        let mut work: VecDeque<(SubquerySig, Flip)> = [
            (sig.clone(), Flip { value: Value::Int(100), dir: FlipDir::Enter }),
            (sig.clone(), Flip { value: Value::Int(200), dir: FlipDir::Enter }),
        ]
        .into_iter()
        .collect();
        let err = propagate_flips(&registry, &mut work, None, None, &trace_tx).await;

        assert!(err.is_err(), "the query-back cannot succeed without Postgres");
        assert_eq!(work.len(), 2, "a failed attempt must not consume the work it did not finish");
        assert_eq!(work[0].1.value, Value::Int(100), "the failed item goes back at the front");
        assert_eq!(work[1].1.value, Value::Int(200), "and the untouched item keeps its place");
    }

    /// In-memory candidate rows standing in for Postgres behind flip query-backs (see
    /// [`CandidateReader`]). Logs every read, so a test can assert how many query-backs a walk
    /// issued and of which table — the quantity that multiplied in production — and can make one
    /// table's reads fail, as an outage reaching that table's query-back does.
    #[derive(Default)]
    pub(crate) struct FakeCandidates {
        rows: HashMap<TableRef, Vec<Row>>,
        /// Every read in order: its table, and its filter column (`None` = a full read).
        log: std::sync::Mutex<Vec<(TableRef, Option<usize>)>>,
        failing: Option<TableRef>,
        /// The snapshot gate every read reports (passthrough when unset).
        gate: Option<crate::pg::SnapshotGate>,
    }

    impl FakeCandidates {
        pub(crate) fn with_rows(tables: impl IntoIterator<Item = (&'static str, Vec<Row>)>) -> Self {
            FakeCandidates {
                rows: tables.into_iter().map(|(t, rows)| (TableRef::from(t), rows)).collect(),
                ..Default::default()
            }
        }

        /// Every read of `table` fails (after it is logged).
        pub(crate) fn failing_reads_of(mut self, table: &str) -> Self {
            self.failing = Some(TableRef::from(table));
            self
        }

        /// Every read reports `gate` as its snapshot.
        pub(crate) fn with_gate(mut self, gate: crate::pg::SnapshotGate) -> Self {
            self.gate = Some(gate);
            self
        }

        pub(crate) fn reads(&self) -> usize {
            self.log.lock().unwrap().len()
        }

        /// Filtered reads of `table` on column `col`.
        pub(crate) fn reads_on(&self, table: &str, col: usize) -> usize {
            let table = TableRef::from(table);
            self.log.lock().unwrap().iter().filter(|(t, c)| *t == table && *c == Some(col)).count()
        }

        /// `(filtered, full)` read counts for one table.
        pub(crate) fn reads_of(&self, table: &str) -> (usize, usize) {
            let table = TableRef::from(table);
            let log = self.log.lock().unwrap();
            let full = log.iter().filter(|(t, col)| *t == table && col.is_none()).count();
            let all = log.iter().filter(|(t, _)| *t == table).count();
            (all - full, full)
        }

        pub(crate) fn read(
            &self,
            ts: &TableSchema,
            filter: Option<(usize, &Value)>,
        ) -> Result<(Vec<Row>, crate::pg::SnapshotGate)> {
            self.log.lock().unwrap().push((ts.table.clone(), filter.map(|(col, _)| col)));
            if self.failing.as_ref() == Some(&ts.table) {
                anyhow::bail!("injected read failure for '{}'", ts.table);
            }
            let rows = self
                .rows
                .get(&ts.table)
                .map(|rows| {
                    rows.iter().filter(|r| filter.is_none_or(|(col, v)| r.0.get(col) == Some(v))).cloned().collect()
                })
                .unwrap_or_default();
            Ok((rows, self.gate.clone().unwrap_or_else(crate::pg::SnapshotGate::passthrough)))
        }
    }

    fn flip_edge(node: &SubquerySig, dependent: Dependent, connecting_col: usize) -> Edge {
        Edge { node_sig: node.clone(), dependent, connecting_col, negated: false, null_sensitive: false }
    }

    /// **Incident 2026-11-11 (backfill storm): one flip, one read.** Every dependent a flipped
    /// node reaches through the same connecting column re-evaluates the SAME candidate rows
    /// (`<outer table> WHERE <col> = <value>`), so the walk must read them once and evaluate them
    /// against each shape. In production one flip reached 445 shapes and issued 445 identical
    /// query-backs (each a `REPEATABLE READ` snapshot + settle + SELECT on a pooled connection);
    /// ~1,000 such flips kept the 8 flip workers busy for 23 minutes, and every drain receipt
    /// behind them waited out the backlog.
    #[tokio::test(flavor = "multi_thread")]
    async fn one_flip_reads_its_candidates_once_for_every_dependent_shape() {
        const SHAPES: usize = 64;
        let ts = issues_ts();
        let (ds_url, store) = spawn_fake_ds().await;
        let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test(&ds_url), None);
        let sig: SubquerySig = "project_members|project_id|L(user_id,Eq,1)".into();
        insert_test_node(&mut reg, &sig);
        // Project 100 is in the set, so issue 1 (project 100) is a member of every shape.
        reg.apply_node_evals(&sig, vec![("pm-1".into(), Some(Value::Int(100)))]).await;
        for i in 0..SHAPES {
            let id = format!("s{i}");
            insert_membership_shape(&mut reg, &id, &sig, 1);
            reg.add_edge(flip_edge(&sig, Dependent::Shape(id), 1));
        }
        reg.set_schemas(Arc::new([(TableRef::parse("issues").unwrap(), ts)].into_iter().collect()));
        let fake = Arc::new(FakeCandidates::with_rows([("issues", vec![issue(1, 100), issue(2, 200)])]));
        reg.fake_candidates = Some(fake.clone());
        let registry = tokio::sync::Mutex::new(reg);
        let (trace_tx, _rx) = tokio::sync::broadcast::channel(16);

        let mut work: VecDeque<(SubquerySig, Flip)> =
            [(sig.clone(), Flip { value: Value::Int(100), dir: FlipDir::Enter })].into_iter().collect();
        propagate_with_retry(&registry, &mut work, None, None, &trace_tx).await.unwrap();

        assert_eq!(fake.reads(), 1, "one flip must issue one query-back, not one per dependent shape");
        for i in 0..SHAPES {
            assert_eq!(ops_for(&store, &format!("shape/s{i}")), vec!["upsert"], "shape s{i} moves issue 1 in");
        }
        let reg = registry.lock().await;
        assert!(reg.shapes.values().all(|s| s.inflight_querybacks == 0), "every query-back is balanced");
    }

    /// The same one tier down: parent NODES reached through one connecting column share the
    /// re-derivation read, and each is still reconciled (its own flips derived and walked on).
    #[tokio::test(flavor = "multi_thread")]
    async fn one_flip_reads_its_candidates_once_for_every_dependent_node() {
        use crate::schema::TableDef;
        const PARENTS: usize = 32;
        let def: TableDef = serde_json::from_value(serde_json::json!({
            "columns": { "id": {"type":"int"}, "gid": {"type":"int"} }, "primaryKey": "id"
        }))
        .unwrap();
        let t = TableSchema::from_def(&TableRef::from("t"), &def).unwrap();
        let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test("http://unused"), None);
        let child: SubquerySig = "child".into();
        insert_test_node(&mut reg, &child);
        let parents: Vec<SubquerySig> = (0..PARENTS).map(|i| SubquerySig::from(format!("parent-{i}"))).collect();
        for parent in &parents {
            insert_test_node(&mut reg, parent);
            reg.add_edge(flip_edge(&child, Dependent::Node(parent.clone()), 1));
        }
        reg.set_schemas(Arc::new([(TableRef::from("t"), t)].into_iter().collect()));
        // `t(id 7, gid 100)`: the child flips gid 100 in, so every parent's `id` set gains 7.
        let fake = Arc::new(FakeCandidates::with_rows([("t", vec![Row(vec![Value::Int(7), Value::Int(100)])])]));
        reg.fake_candidates = Some(fake.clone());
        let registry = tokio::sync::Mutex::new(reg);
        let (trace_tx, _rx) = tokio::sync::broadcast::channel(16);

        let mut work: VecDeque<(SubquerySig, Flip)> =
            [(child.clone(), Flip { value: Value::Int(100), dir: FlipDir::Enter })].into_iter().collect();
        propagate_with_retry(&registry, &mut work, None, None, &trace_tx).await.unwrap();

        assert_eq!(fake.reads(), 1, "one flip must issue one re-derivation read, not one per parent node");
        let reg = registry.lock().await;
        for parent in &parents {
            assert!(reg.contains(parent, &Value::Int(7)), "parent {parent} reconciled from the shared read");
            assert_eq!(reg.nodes[parent].inflight_querybacks, 0, "every re-derivation is balanced");
        }
    }

    /// `<name>(id, gid)`: a two-column table for the multi-table flip tests.
    fn id_gid_ts(name: &str) -> TableSchema {
        use crate::schema::TableDef;
        let def: TableDef = serde_json::from_value(serde_json::json!({
            "columns": { "id": {"type":"int"}, "gid": {"type":"int"} }, "primaryKey": "id"
        }))
        .unwrap();
        TableSchema::from_def(&TableRef::from(name), &def).unwrap()
    }

    /// A row of an [`id_gid_ts`] table. `TableSchema` sorts its columns, so `gid` comes first.
    fn id_gid_row(ts: &TableSchema, id: i64, gid: i64) -> Row {
        let mut cells = vec![Value::Null; 2];
        cells[ts.column_index("id").unwrap()] = Value::Int(id);
        cells[ts.column_index("gid").unwrap()] = Value::Int(gid);
        Row(cells)
    }

    fn null_sensitive_edge(node: &SubquerySig, dependent: Dependent, connecting_col: usize) -> Edge {
        Edge { node_sig: node.clone(), dependent, connecting_col, negated: true, null_sensitive: true }
    }

    /// `(type, key, operation)` of every envelope delivered to one stream path, in append order.
    fn envs_for(store: &DsStore, path: &str) -> Vec<(String, String, String)> {
        store
            .0
            .lock()
            .unwrap()
            .get(path)
            .map(|v| v.iter().map(|e| (e.type_.clone(), e.key.clone(), e.headers.operation.clone())).collect())
            .unwrap_or_default()
    }

    /// The same incident, for a NULL flip: a NULL leaving (or entering) an inner set re-derives every
    /// NULL-sensitive dependent from ALL the rows of its table — the same per-dependent fan-out as a
    /// value flip, on a more expensive read. The NULL-sensitive shapes share one full read of their
    /// outer table and the NULL-sensitive parents one full read of their inner table; every one of
    /// them still moves, and a plain-`IN` dependent (which a NULL cannot change) is neither read for
    /// nor moved.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_null_flip_reads_each_table_once_for_every_null_sensitive_dependent() {
        const SHAPES: usize = 16;
        const PARENTS: usize = 8;
        let (ds_url, store) = spawn_fake_ds().await;
        let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test(&ds_url), None);
        let sig: SubquerySig = "project_members|project_id|L(user_id,Eq,1)".into();
        insert_test_node(&mut reg, &sig);
        // The set was {100, NULL} and the NULL just left: `project_id NOT IN set` goes from UNKNOWN
        // (no member) to TRUE for issue 2 (project 200), and stays FALSE for issue 1 (project 100).
        reg.apply_node_evals(&sig, vec![("pm-1".into(), Some(Value::Int(100))), ("pm-2".into(), Some(Value::Null))])
            .await;
        let flips = reg.apply_node_evals(&sig, vec![("pm-2".into(), None)]).await;
        assert_eq!(flips, vec![Flip { value: Value::Null, dir: FlipDir::Leave }]);
        for i in 0..SHAPES {
            let id = format!("s{i}");
            insert_outer_shape(
                &mut reg,
                &id,
                "issues",
                CompiledPredicate::InSubquery { col: 1, sig: sig.clone(), negated: true },
            );
            reg.add_edge(null_sensitive_edge(&sig, Dependent::Shape(id), 1));
        }
        insert_membership_shape(&mut reg, "plain", &sig, 1);
        reg.add_edge(flip_edge(&sig, Dependent::Shape("plain".into()), 1));
        let parents: Vec<SubquerySig> = (0..PARENTS).map(|i| SubquerySig::from(format!("parent-{i}"))).collect();
        let t = id_gid_ts("t");
        let (t_id, t_gid) = (t.column_index("id").unwrap(), t.column_index("gid").unwrap());
        for parent in &parents {
            insert_test_node_on(&mut reg, parent, "t", t_id);
            reg.add_edge(null_sensitive_edge(&sig, Dependent::Node(parent.clone()), t_gid));
        }
        let t_rows = vec![id_gid_row(&t, 7, 100), id_gid_row(&t, 8, 200)];
        reg.set_schemas(Arc::new(
            [(TableRef::parse("issues").unwrap(), issues_ts()), (TableRef::from("t"), t)].into_iter().collect(),
        ));
        let fake = Arc::new(FakeCandidates::with_rows([("issues", vec![issue(1, 100), issue(2, 200)]), ("t", t_rows)]));
        reg.fake_candidates = Some(fake.clone());
        let registry = tokio::sync::Mutex::new(reg);
        let (trace_tx, _rx) = tokio::sync::broadcast::channel(16);

        let mut work: VecDeque<(SubquerySig, Flip)> = [(sig.clone(), flips[0].clone())].into_iter().collect();
        propagate_with_retry(&registry, &mut work, None, None, &trace_tx).await.unwrap();

        assert_eq!(fake.reads(), 2, "one full read per table, not one per NULL-sensitive dependent");
        assert_eq!(fake.reads_of("issues"), (0, 1), "the shapes share one full read of their outer table");
        assert_eq!(fake.reads_of("t"), (0, 1), "the parents share one full read of their inner table");
        let issue_2 = issues_ts().key_string(&issue(2, 200)).unwrap();
        for i in 0..SHAPES {
            let envs = envs_for(&store, &format!("shape/s{i}"));
            assert_eq!(envs.len(), 1, "shape s{i} moves exactly issue 2 in, got {envs:?}");
            assert_eq!((envs[0].1.as_str(), envs[0].2.as_str()), (issue_2.as_str(), "upsert"));
        }
        assert!(ops_for(&store, "shape/plain").is_empty(), "a NULL cannot move a plain-IN shape");
        let reg = registry.lock().await;
        for parent in &parents {
            assert!(
                reg.contains(parent, &Value::Int(7)) && reg.contains(parent, &Value::Int(8)),
                "{parent} re-derived"
            );
            assert_eq!(reg.nodes[parent].inflight_querybacks, 0, "every re-derivation is balanced");
        }
        assert!(reg.shapes.values().all(|s| s.inflight_querybacks == 0), "every query-back is balanced");
    }

    /// A node whose dependents live on TWO outer tables (`issues`, `comments`) and TWO inner tables
    /// (`t`, `t2`), interleaved in edge order, every one reached through column 1 and NULL-sensitive
    /// (a `NOT IN`), so the same registry serves a value flip and a NULL flip. The set is empty: the
    /// flip is a value (or the NULL) that just left it. `failing` makes that table's reads fail.
    #[allow(clippy::type_complexity)]
    async fn two_table_registry(
        failing: Option<&str>,
    ) -> (tokio::sync::Mutex<SubqueryRegistry>, DsStore, Arc<FakeCandidates>, SubquerySig, Vec<String>, Vec<SubquerySig>)
    {
        let (ds_url, store) = spawn_fake_ds().await;
        let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test(&ds_url), None);
        let sig: SubquerySig = "child".into();
        insert_test_node(&mut reg, &sig);
        let ts = id_gid_ts("any");
        let (id_col, gid_col) = (ts.column_index("id").unwrap(), ts.column_index("gid").unwrap());
        let mut shapes = Vec::new();
        for i in 0..3 {
            for table in ["issues", "comments"] {
                let id = format!("{table}-{i}");
                insert_outer_shape(
                    &mut reg,
                    &id,
                    table,
                    CompiledPredicate::InSubquery { col: gid_col, sig: sig.clone(), negated: true },
                );
                reg.add_edge(null_sensitive_edge(&sig, Dependent::Shape(id.clone()), gid_col));
                shapes.push(id);
            }
        }
        let mut parents = Vec::new();
        for i in 0..3 {
            for table in ["t", "t2"] {
                let parent = SubquerySig::from(format!("{table}-parent-{i}"));
                insert_test_node_on(&mut reg, &parent, table, id_col);
                reg.add_edge(null_sensitive_edge(&sig, Dependent::Node(parent.clone()), gid_col));
                parents.push(parent);
            }
        }
        reg.set_schemas(Arc::new(
            ["issues", "comments", "t", "t2"].into_iter().map(|t| (TableRef::from(t), id_gid_ts(t))).collect(),
        ));
        let row = |id: i64, gid: i64| id_gid_row(&ts, id, gid);
        let fake = FakeCandidates::with_rows([
            ("issues", vec![row(1, 100), row(2, 200)]),
            ("comments", vec![row(10, 100), row(20, 200)]),
            ("t", vec![row(7, 100), row(8, 200)]),
            ("t2", vec![row(70, 100), row(80, 200)]),
        ]);
        let fake = Arc::new(match failing {
            Some(table) => fake.failing_reads_of(table),
            None => fake,
        });
        reg.fake_candidates = Some(fake.clone());
        (tokio::sync::Mutex::new(reg), store, fake, sig, shapes, parents)
    }

    /// One read per TABLE, not one per flip: dependents on different tables each get their own
    /// table's read, and only its rows — a shape on `comments` never sees an `issues` row, a parent
    /// on `t2` never reconciles a `t` row. For a value flip and a NULL flip alike.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_flip_reads_each_table_once_and_shares_no_rows_across_tables() {
        // Per flip: the ids each table's dependents must end up with. Rows: issues 1 (gid 100) and
        // 2 (gid 200), comments 10/20, t 7/8, t2 70/80; `NOT IN` an empty set admits every gid.
        for (value, ids) in [
            (Value::Int(100), [("issues", vec![1]), ("comments", vec![10]), ("t", vec![7]), ("t2", vec![70])]),
            (
                Value::Null,
                [("issues", vec![1, 2]), ("comments", vec![10, 20]), ("t", vec![7, 8]), ("t2", vec![70, 80])],
            ),
        ] {
            let ids_of = |table: &str| ids.iter().find(|(t, _)| *t == table).map(|(_, ids)| ids.clone()).unwrap();
            let full = matches!(value, Value::Null);
            let (registry, store, fake, sig, shapes, parents) = two_table_registry(None).await;
            let (trace_tx, _rx) = tokio::sync::broadcast::channel(64);
            let mut work: VecDeque<(SubquerySig, Flip)> =
                [(sig.clone(), Flip { value: value.clone(), dir: FlipDir::Leave })].into_iter().collect();
            propagate_with_retry(&registry, &mut work, None, None, &trace_tx).await.unwrap();

            assert_eq!(fake.reads(), 4, "{value:?}: one read per table, four tables");
            for table in ["issues", "comments", "t", "t2"] {
                let expected = if full { (0, 1) } else { (1, 0) };
                assert_eq!(fake.reads_of(table), expected, "{value:?}: exactly one read of {table}");
            }
            let ts = id_gid_ts("any");
            for id in &shapes {
                let table = id.split('-').next().unwrap();
                let expected: Vec<(String, String, String)> = ids_of(table)
                    .into_iter()
                    .map(|pk| {
                        let key = ts.key_string(&id_gid_row(&ts, pk, 0)).unwrap();
                        (TableRef::from(table).to_string(), key, "upsert".to_string())
                    })
                    .collect();
                assert_eq!(envs_for(&store, &format!("shape/{id}")), expected, "{value:?}: shape {id}");
            }
            let reg = registry.lock().await;
            for parent in &parents {
                let table = parent.split('-').next().unwrap();
                for (t, ids) in &ids {
                    for id in ids {
                        assert_eq!(
                            reg.contains(parent, &Value::Int(*id)),
                            *t == table,
                            "{value:?}: parent {parent} holds exactly its own table's rows (id {id} of {t})"
                        );
                    }
                }
                assert_eq!(reg.nodes[parent].inflight_querybacks, 0);
            }
            assert!(reg.shapes.values().all(|s| s.inflight_querybacks == 0));
        }
    }

    /// A read that fails AFTER another table's shared read succeeded unwinds the whole flip: every
    /// shape and node it marked in flight is released (or it would record recency, and drop
    /// candidates, forever), no shape of the failed group emits and no parent is reconciled (so the
    /// retry finds every transition still unconsumed), and the flip goes back at the FRONT of the
    /// walk with the rest untouched — the resume contract of `propagate_with_retry`. Failing the
    /// second outer table fails before any shape emits; failing the second inner table fails after
    /// the shapes moved (harmless: emission is absolute) but before any parent reconciles.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_second_table_read_releases_every_dependent_and_keeps_the_flip() {
        for value in [Value::Int(100), Value::Null] {
            for failing in ["comments", "t2"] {
                let (registry, store, fake, sig, shapes, parents) = two_table_registry(Some(failing)).await;
                let (trace_tx, _rx) = tokio::sync::broadcast::channel(64);
                let flip = Flip { value: value.clone(), dir: FlipDir::Leave };
                let next = Flip { value: Value::Int(999), dir: FlipDir::Enter };
                let mut work: VecDeque<(SubquerySig, Flip)> =
                    [(sig.clone(), flip.clone()), (sig.clone(), next.clone())].into_iter().collect();
                let res = propagate_flips(&registry, &mut work, None, None, &trace_tx).await;

                let case = format!("{value:?} failing {failing}");
                assert!(res.is_err(), "{case}: the failed read fails the walk");
                assert_eq!(work.len(), 2, "{case}: the failed flip is not consumed");
                assert_eq!(work[0], (sig.clone(), flip.clone()), "{case}: it goes back at the front");
                assert_eq!(work[1], (sig.clone(), next.clone()), "{case}: the rest keeps its place");
                let first = if failing == "comments" { "issues" } else { "t" };
                assert_eq!(fake.reads_of(first).0 + fake.reads_of(first).1, 1, "{case}: {first} was read first");
                assert_eq!(fake.reads_of(failing).0 + fake.reads_of(failing).1, 1, "{case}: then {failing}");
                if failing == "comments" {
                    for id in &shapes {
                        assert!(ops_for(&store, &format!("shape/{id}")).is_empty(), "{case}: {id} emitted nothing");
                    }
                }
                let reg = registry.lock().await;
                for parent in &parents {
                    for id in [7, 8, 70, 80] {
                        assert!(!reg.contains(parent, &Value::Int(id)), "{case}: {parent} was not reconciled");
                    }
                    assert_eq!(reg.nodes[parent].inflight_querybacks, 0, "{case}: {parent} released");
                }
                for (id, s) in &reg.shapes {
                    assert_eq!(s.inflight_querybacks, 0, "{case}: shape {id} released");
                }
            }
        }
    }

    /// See [`SubqueryRegistry::delivery_probe`].
    pub(crate) type DeliveryProbe = Arc<dyn Fn(&mut SubqueryRegistry, &str) -> Result<()> + Send + Sync>;
    /// See [`SubqueryRegistry::reconcile_probe`].
    pub(crate) type ReconcileProbe = Arc<dyn Fn(&mut SubqueryRegistry, &str) + Send + Sync>;

    /// **The shared read must not become one long lock hold.** Each shape is evaluated under its
    /// own hold and released from in flight the moment it is — so at every delivery the shapes
    /// before it are already released and the ones after it still marked — and an emission error
    /// midway releases every shape (those never evaluated included), emits nothing further and
    /// leaves the flip at the front of the walk. For a value flip and a NULL flip alike.
    #[tokio::test(flavor = "multi_thread")]
    async fn each_shape_is_released_as_soon_as_it_is_evaluated() {
        const SHAPES: usize = 5;
        for value in [Value::Int(100), Value::Null] {
            let (ds_url, store) = spawn_fake_ds().await;
            let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test(&ds_url), None);
            let sig: SubquerySig = "child".into();
            insert_test_node(&mut reg, &sig);
            for i in 0..SHAPES {
                let id = format!("s{i}");
                insert_outer_shape(
                    &mut reg,
                    &id,
                    "issues",
                    CompiledPredicate::InSubquery { col: 1, sig: sig.clone(), negated: true },
                );
                reg.add_edge(null_sensitive_edge(&sig, Dependent::Shape(id), 1));
            }
            reg.set_schemas(Arc::new([(TableRef::parse("issues").unwrap(), issues_ts())].into_iter().collect()));
            let fake = Arc::new(FakeCandidates::with_rows([("issues", vec![issue(1, 100), issue(2, 200)])]));
            reg.fake_candidates = Some(fake.clone());
            // At every delivery: each shape's in-flight count as the emission sees it. s2's fails.
            let seen: Arc<std::sync::Mutex<Vec<(String, Vec<u32>)>>> = Default::default();
            let log = seen.clone();
            reg.delivery_probe = Some(Arc::new(move |reg: &mut SubqueryRegistry, path: &str| {
                let counts = (0..SHAPES).map(|i| reg.shapes[&format!("s{i}")].inflight_querybacks).collect();
                log.lock().unwrap().push((path.to_string(), counts));
                if path == "shape/s2" {
                    anyhow::bail!("injected emission failure");
                }
                Ok(())
            }));
            let registry = tokio::sync::Mutex::new(reg);
            let (trace_tx, _rx) = tokio::sync::broadcast::channel(64);
            let flip = Flip { value: value.clone(), dir: FlipDir::Leave };
            let mut work: VecDeque<(SubquerySig, Flip)> = [(sig.clone(), flip.clone())].into_iter().collect();
            let res = propagate_flips(&registry, &mut work, None, None, &trace_tx).await;

            assert!(res.is_err(), "{value:?}: the failed emission fails the walk");
            assert_eq!(work, VecDeque::from([(sig.clone(), flip.clone())]), "{value:?}: the flip goes back");
            assert_eq!(fake.reads(), 1, "{value:?}: still one shared read");
            let seen = seen.lock().unwrap().clone();
            let paths: Vec<&str> = seen.iter().map(|(p, _)| p.as_str()).collect();
            assert_eq!(paths, ["shape/s0", "shape/s1", "shape/s2"], "{value:?}: s3 and s4 are never evaluated");
            for (k, (path, counts)) in seen.iter().enumerate() {
                for (i, count) in counts.iter().enumerate() {
                    let expected = if i < k { 0 } else { 1 };
                    assert_eq!(*count, expected, "{value:?}: at {path}, s{i} is released iff evaluated earlier");
                }
            }
            for i in 3..SHAPES {
                assert!(ops_for(&store, &format!("shape/s{i}")).is_empty(), "{value:?}: s{i} emitted nothing");
            }
            let reg = registry.lock().await;
            for (id, s) in &reg.shapes {
                assert_eq!(s.inflight_querybacks, 0, "{value:?}: shape {id} released");
            }
        }
    }

    /// One tier down: each parent node is reconciled under its own hold and released from in
    /// flight right after its own reconcile — the parents before it already released, the ones
    /// after it still marked. For a value flip and a NULL flip alike.
    #[tokio::test(flavor = "multi_thread")]
    async fn each_parent_is_released_as_soon_as_it_is_reconciled() {
        const PARENTS: usize = 5;
        for value in [Value::Int(100), Value::Null] {
            let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test("http://unused"), None);
            let child: SubquerySig = "child".into();
            insert_test_node(&mut reg, &child);
            let t = id_gid_ts("t");
            let (id_col, gid_col) = (t.column_index("id").unwrap(), t.column_index("gid").unwrap());
            let parents: Vec<SubquerySig> = (0..PARENTS).map(|i| format!("parent-{i}")).collect();
            for parent in &parents {
                insert_test_node_on(&mut reg, parent, "t", id_col);
                reg.add_edge(null_sensitive_edge(&child, Dependent::Node(parent.clone()), gid_col));
            }
            let rows = vec![id_gid_row(&t, 7, 100), id_gid_row(&t, 8, 200)];
            reg.set_schemas(Arc::new([(TableRef::from("t"), t)].into_iter().collect()));
            reg.fake_candidates = Some(Arc::new(FakeCandidates::with_rows([("t", rows)])));
            // Right after each reconcile: every parent's in-flight count, as that hold left it.
            let seen: Arc<std::sync::Mutex<Vec<(String, Vec<u32>)>>> = Default::default();
            let log = seen.clone();
            reg.reconcile_probe = Some(Arc::new(move |reg: &mut SubqueryRegistry, sig: &str| {
                let counts = (0..PARENTS).map(|i| reg.nodes[&format!("parent-{i}")].inflight_querybacks).collect();
                log.lock().unwrap().push((sig.to_string(), counts));
            }));
            let registry = tokio::sync::Mutex::new(reg);
            let (trace_tx, _rx) = tokio::sync::broadcast::channel(64);
            let mut work: VecDeque<(SubquerySig, Flip)> =
                [(child.clone(), Flip { value: value.clone(), dir: FlipDir::Leave })].into_iter().collect();
            propagate_with_retry(&registry, &mut work, None, None, &trace_tx).await.unwrap();

            let seen = seen.lock().unwrap().clone();
            let order: Vec<&str> = seen.iter().map(|(sig, _)| sig.as_str()).collect();
            assert_eq!(order, parents.iter().map(String::as_str).collect::<Vec<_>>(), "{value:?}: one reconcile each");
            for (k, (sig, counts)) in seen.iter().enumerate() {
                for (i, count) in counts.iter().enumerate() {
                    let expected = if i <= k { 0 } else { 1 };
                    assert_eq!(*count, expected, "{value:?}: after {sig}'s reconcile, parent-{i}");
                }
            }
            let reg = registry.lock().await;
            for parent in &parents {
                assert!(reg.contains(parent, &Value::Int(7)), "{value:?}: {parent} reconciled");
            }
        }
    }

    /// `<name>(id, a, b)`: a table two connecting columns can reach.
    fn id_a_b_ts(name: &str) -> TableSchema {
        use crate::schema::TableDef;
        let def: TableDef = serde_json::from_value(serde_json::json!({
            "columns": { "id": {"type":"int"}, "a": {"type":"int"}, "b": {"type":"int"} }, "primaryKey": "id"
        }))
        .unwrap();
        TableSchema::from_def(&TableRef::from(name), &def).unwrap()
    }

    fn id_a_b_row(ts: &TableSchema, id: i64, a: i64, b: i64) -> Row {
        let mut cells = vec![Value::Null; 3];
        for (col, v) in [("id", id), ("a", a), ("b", b)] {
            cells[ts.column_index(col).unwrap()] = Value::Int(v);
        }
        Row(cells)
    }

    /// The read is shared per (tier, table, CONNECTING COLUMN), not per table: dependents reached
    /// through column `a` and through column `b` of the same table get one read each, and neither
    /// sees the other's rows — a row read for `b = 100` reaching an `a`-dependent would make it a
    /// member there (`a = 0 NOT IN {}`). A NULL flip's full read ignores the column, so it is one
    /// read per table that every dependent shares.
    #[tokio::test(flavor = "multi_thread")]
    async fn dependents_on_two_connecting_columns_read_once_per_column() {
        for value in [Value::Int(100), Value::Null] {
            let full = matches!(value, Value::Null);
            let (ds_url, store) = spawn_fake_ds().await;
            let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test(&ds_url), None);
            let sig: SubquerySig = "child".into();
            insert_test_node(&mut reg, &sig);
            let ts = id_a_b_ts("items");
            let col = |name: &str| ts.column_index(name).unwrap();
            for i in 0..2 {
                for name in ["a", "b"] {
                    let shape = format!("{name}-shape-{i}");
                    insert_outer_shape(
                        &mut reg,
                        &shape,
                        "items",
                        CompiledPredicate::InSubquery { col: col(name), sig: sig.clone(), negated: true },
                    );
                    reg.add_edge(null_sensitive_edge(&sig, Dependent::Shape(shape), col(name)));
                    let parent = format!("{name}-parent-{i}");
                    insert_test_node_on(&mut reg, &parent, "inner3", col("id"));
                    reg.add_edge(null_sensitive_edge(&sig, Dependent::Node(parent), col(name)));
                }
            }
            reg.set_schemas(Arc::new(
                ["items", "inner3"].into_iter().map(|t| (TableRef::from(t), id_a_b_ts(t))).collect(),
            ));
            // Rows 1 and 7 are reached through `a = 100`, rows 2 and 8 through `b = 100`.
            let fake = Arc::new(FakeCandidates::with_rows([
                ("items", vec![id_a_b_row(&ts, 1, 100, 0), id_a_b_row(&ts, 2, 0, 100)]),
                ("inner3", vec![id_a_b_row(&ts, 7, 100, 0), id_a_b_row(&ts, 8, 0, 100)]),
            ]));
            reg.fake_candidates = Some(fake.clone());
            let registry = tokio::sync::Mutex::new(reg);
            let (trace_tx, _rx) = tokio::sync::broadcast::channel(64);
            let mut work: VecDeque<(SubquerySig, Flip)> =
                [(sig.clone(), Flip { value: value.clone(), dir: FlipDir::Leave })].into_iter().collect();
            propagate_with_retry(&registry, &mut work, None, None, &trace_tx).await.unwrap();

            if full {
                assert_eq!(fake.reads(), 2, "NULL: one full read per table, whatever the column");
                assert_eq!(fake.reads_of("items"), (0, 1));
                assert_eq!(fake.reads_of("inner3"), (0, 1));
            } else {
                assert_eq!(fake.reads(), 4, "value: one read per (tier, table, column)");
                for table in ["items", "inner3"] {
                    for name in ["a", "b"] {
                        assert_eq!(fake.reads_on(table, col(name)), 1, "one read of {table} on {name}");
                    }
                }
            }
            let key = |id: i64| ts.key_string(&id_a_b_row(&ts, id, 0, 0)).unwrap();
            let reg = registry.lock().await;
            for i in 0..2 {
                for (name, own, other) in [("a", (1, 7), (2, 8)), ("b", (2, 8), (1, 7))] {
                    let keys: Vec<String> =
                        envs_for(&store, &format!("shape/{name}-shape-{i}")).into_iter().map(|(_, k, _)| k).collect();
                    let expected = if full { vec![key(1), key(2)] } else { vec![key(own.0)] };
                    assert_eq!(keys, expected, "{value:?}: {name}-shape-{i} sees only its own column's rows");
                    let parent = format!("{name}-parent-{i}");
                    assert!(reg.contains(&parent, &Value::Int(own.1)), "{value:?}: {parent} reconciled");
                    assert_eq!(
                        reg.contains(&parent, &Value::Int(other.1)),
                        full,
                        "{value:?}: {parent} sees the other column's row only on a full read"
                    );
                }
            }
        }
    }

    /// **A live decision between two holds still wins.** Shapes are evaluated one hold at a time, so
    /// a live commit can land after s0's evaluation and before s1's. The test applies it inside s0's
    /// hold, right after s0's delivery — the same point in the walk, made deterministic (that another
    /// task really gets the lock there is `another_task_takes_the_lock_between_dependents`). The
    /// shapes still waiting are marked in flight, so the live verdict is stamped on them, and the
    /// shared read's older row for that pk — from a snapshot that cannot see the commit — is dropped
    /// when they evaluate it. s1.. carry only the live row; s0 keeps its query-back upsert, followed
    /// by the live one.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_live_change_between_holds_beats_the_shared_read_for_later_shapes() {
        const SHAPES: usize = 4;
        let ts = issues_ts();
        let (ds_url, store) = spawn_fake_ds().await;
        let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test(&ds_url), None);
        let sig: SubquerySig = "child".into();
        insert_test_node(&mut reg, &sig);
        for i in 0..SHAPES {
            let id = format!("s{i}");
            insert_outer_shape(
                &mut reg,
                &id,
                "issues",
                CompiledPredicate::InSubquery { col: 1, sig: sig.clone(), negated: true },
            );
            reg.add_edge(flip_edge(&sig, Dependent::Shape(id), 1));
        }
        reg.set_schemas(Arc::new([(TableRef::parse("issues").unwrap(), ts.clone())].into_iter().collect()));
        // The shared read's snapshot (xmin 40, xmax 45) cannot see the live commit (xid 50).
        let fake = FakeCandidates::with_rows([("issues", vec![issue(1, 100)])])
            .with_gate(crate::pg::SnapshotGate::parse("40:45:", "0/100"));
        reg.fake_candidates = Some(Arc::new(fake));
        // Inside s0's hold, right after its query-back delivery (before s1's hold): issue 1 moves from project
        // 100 to 300 in xid 50, through the live outer-table path.
        let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let live_ts = ts.clone();
        reg.delivery_probe = Some(Arc::new(move |reg: &mut SubqueryRegistry, path: &str| {
            if path != "shape/s0" || fired.swap(true, std::sync::atomic::Ordering::SeqCst) {
                return Ok(());
            }
            let delta = [Tup2(issue(1, 100), -1), Tup2(issue(1, 300), 1)];
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(reg.on_table_delta(
                    &live_ts,
                    &delta,
                    0x200,
                    Some(50),
                    None,
                    None,
                ))
            })
            .map(|_| ())
        }));
        let registry = Arc::new(tokio::sync::Mutex::new(reg));
        // Walk on a worker thread, where the probe may block in place.
        let walk = {
            let (registry, sig) = (registry.clone(), sig.clone());
            tokio::spawn(async move {
                let (trace_tx, _rx) = tokio::sync::broadcast::channel(64);
                let mut work: VecDeque<(SubquerySig, Flip)> =
                    [(sig, Flip { value: Value::Int(100), dir: FlipDir::Leave })].into_iter().collect();
                propagate_flips(&registry, &mut work, None, None, &trace_tx).await
            })
        };
        walk.await.unwrap().unwrap();

        let (stale, live) = (ts.row_to_json_cols(&issue(1, 100), None), ts.row_to_json_cols(&issue(1, 300), None));
        let delivered = |path: &str| -> Vec<(String, Option<serde_json::Value>)> {
            store
                .0
                .lock()
                .unwrap()
                .get(path)
                .map(|v| v.iter().map(|e| (e.headers.operation.clone(), e.value.clone())).collect())
                .unwrap_or_default()
        };
        let upsert = |v: &serde_json::Value| ("upsert".to_string(), Some(v.clone()));
        assert_eq!(
            delivered("shape/s0"),
            vec![upsert(&stale), upsert(&live)],
            "s0: its query-back row, then the live one"
        );
        for i in 1..SHAPES {
            assert_eq!(delivered(&format!("shape/s{i}")), vec![upsert(&live)], "s{i}: the stale shared row is dropped");
        }
        let reg = registry.lock().await;
        assert!(reg.shapes.values().all(|s| s.inflight_querybacks == 0), "every query-back is balanced");
    }

    /// Apply a live change through the table tailer's path from inside a (sync) probe. The probe
    /// runs under the registry lock on a walk spawned onto a worker thread, so it can block in place.
    fn apply_live(reg: &mut SubqueryRegistry, ts: &TableSchema, delta: &[Tup2<Row, ZWeight>], lsn: u64, xid: u64) {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(reg.on_table_delta(ts, delta, lsn, Some(xid), None, None))
        })
        .expect("live change");
    }

    /// Walk one flip on a worker thread (where a probe may block in place), handing back the result
    /// and whatever the walk left on its queue.
    async fn walk_on_worker(
        registry: &Arc<tokio::sync::Mutex<SubqueryRegistry>>,
        sig: &SubquerySig,
        flip: &Flip,
    ) -> (Result<()>, VecDeque<(SubquerySig, Flip)>) {
        let (registry, item) = (registry.clone(), (sig.clone(), flip.clone()));
        tokio::spawn(async move {
            let (trace_tx, _rx) = tokio::sync::broadcast::channel(64);
            let mut work: VecDeque<(SubquerySig, Flip)> = [item].into_iter().collect();
            let res = propagate_flips(&registry, &mut work, None, None, &trace_tx).await;
            (res, work)
        })
        .await
        .unwrap()
    }

    /// The parent tier of [`a_live_change_between_holds_beats_the_shared_read_for_later_shapes`]:
    /// inside parent 0's hold, right after it reconciles from the shared read (before parent 1's
    /// hold), the inner row it read moves (`gid` 100 → 300, xid 50) through the live template path. The parents
    /// still waiting are marked, so the live contribution is stamped on them and the shared read's
    /// older row — from a snapshot that cannot see xid 50 — is dropped when they re-derive. Every
    /// parent, parent 0 included, ends with the live contribution and not the stale one.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_live_change_between_holds_beats_the_shared_read_for_later_parents() {
        const PARENTS: usize = 4;
        let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test("http://unused"), None);
        let child: SubquerySig = "child".into();
        insert_test_node(&mut reg, &child);
        let pt = id_gid_ts("pt");
        let gid = pt.column_index("gid").unwrap();
        let parents: Vec<SubquerySig> = (0..PARENTS).map(|i| format!("parent-{i}")).collect();
        for parent in &parents {
            // Each parent projects `gid`, so the row's move changes the value it contributes.
            insert_test_node_on(&mut reg, parent, "pt", gid);
            reg.add_edge(flip_edge(&child, Dependent::Node(parent.clone()), gid));
        }
        reg.set_schemas(Arc::new([(TableRef::from("pt"), pt.clone())].into_iter().collect()));
        let fake = FakeCandidates::with_rows([("pt", vec![id_gid_row(&pt, 7, 100)])])
            .with_gate(crate::pg::SnapshotGate::parse("40:45:", "0/100"));
        reg.fake_candidates = Some(Arc::new(fake));
        let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let live_pt = pt.clone();
        reg.reconcile_probe = Some(Arc::new(move |reg: &mut SubqueryRegistry, sig: &str| {
            if sig != "parent-0" || fired.swap(true, std::sync::atomic::Ordering::SeqCst) {
                return;
            }
            let delta = [Tup2(id_gid_row(&live_pt, 7, 100), -1), Tup2(id_gid_row(&live_pt, 7, 300), 1)];
            apply_live(reg, &live_pt, &delta, 0x200, 50);
        }));
        let registry = Arc::new(tokio::sync::Mutex::new(reg));
        let flip = Flip { value: Value::Int(100), dir: FlipDir::Leave };
        let (res, work) = walk_on_worker(&registry, &child, &flip).await;

        res.unwrap();
        assert!(work.is_empty(), "the walk finished");
        let reg = registry.lock().await;
        for parent in &parents {
            assert!(reg.contains(parent, &Value::Int(300)), "{parent} holds the live contribution");
            assert!(!reg.contains(parent, &Value::Int(100)), "{parent} does not hold the stale one");
            assert_eq!(reg.nodes[parent].inflight_querybacks, 0, "{parent} released");
            assert!(reg.nodes[parent].recent.is_empty(), "{parent}'s recency cleared");
        }
    }

    /// From inside a probe (which runs under the walker's registry lock): spawn a task that waits on
    /// that lock and logs `"contender"` once it gets it, and return only once that task is queued on
    /// the mutex. The task signals right before `lock().await` and the probe then gives it time to
    /// reach the wait queue; the tokio mutex is FIFO-fair, so it is next in line when this hold ends.
    fn queue_a_contender(
        registry: &std::sync::OnceLock<std::sync::Weak<tokio::sync::Mutex<SubqueryRegistry>>>,
        log: &Arc<std::sync::Mutex<Vec<String>>>,
    ) {
        let (registry, log) = (registry.get().expect("registry set").upgrade().expect("registry alive"), log.clone());
        let (queued_tx, queued_rx) = std::sync::mpsc::channel();
        tokio::spawn(async move {
            queued_tx.send(()).unwrap();
            let _held = registry.lock().await;
            log.lock().unwrap().push("contender".to_string());
        });
        tokio::task::block_in_place(|| {
            queued_rx.recv().unwrap();
            std::thread::sleep(std::time::Duration::from_millis(200));
        });
    }

    /// **Another task really gets the registry between two dependents** — the point of evaluating
    /// each under its own hold. From the first dependent's probe (shape s0's delivery, parent 0's
    /// reconcile) a contender is queued on the registry lock; it must acquire it before the second
    /// dependent's probe runs. Under one hold across the loop it could only get in after the last.
    #[tokio::test(flavor = "multi_thread")]
    async fn another_task_takes_the_lock_between_dependents() {
        let (ds_url, _store) = spawn_fake_ds().await;
        let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test(&ds_url), None);
        let sig: SubquerySig = "child".into();
        insert_test_node(&mut reg, &sig);
        reg.apply_node_evals(&sig, vec![("pm-1".into(), Some(Value::Int(100)))]).await;
        for i in 0..3 {
            let id = format!("s{i}");
            insert_membership_shape(&mut reg, &id, &sig, 1);
            reg.add_edge(flip_edge(&sig, Dependent::Shape(id), 1));
        }
        let t = id_gid_ts("t");
        let (id_col, gid_col) = (t.column_index("id").unwrap(), t.column_index("gid").unwrap());
        for i in 0..3 {
            let parent = format!("parent-{i}");
            insert_test_node_on(&mut reg, &parent, "t", id_col);
            reg.add_edge(flip_edge(&sig, Dependent::Node(parent), gid_col));
        }
        let t_rows = vec![id_gid_row(&t, 7, 100)];
        reg.set_schemas(Arc::new(
            [(TableRef::parse("issues").unwrap(), issues_ts()), (TableRef::from("t"), t)].into_iter().collect(),
        ));
        reg.fake_candidates =
            Some(Arc::new(FakeCandidates::with_rows([("issues", vec![issue(1, 100)]), ("t", t_rows)])));
        let cell: Arc<std::sync::OnceLock<std::sync::Weak<tokio::sync::Mutex<SubqueryRegistry>>>> = Default::default();
        let log: Arc<std::sync::Mutex<Vec<String>>> = Default::default();
        let (probe_cell, probe_log) = (cell.clone(), log.clone());
        reg.delivery_probe = Some(Arc::new(move |_: &mut SubqueryRegistry, path: &str| {
            probe_log.lock().unwrap().push(path.to_string());
            if path == "shape/s0" {
                queue_a_contender(&probe_cell, &probe_log);
            }
            Ok(())
        }));
        let (probe_cell, probe_log) = (cell.clone(), log.clone());
        reg.reconcile_probe = Some(Arc::new(move |_: &mut SubqueryRegistry, parent: &str| {
            probe_log.lock().unwrap().push(parent.to_string());
            if parent == "parent-0" {
                queue_a_contender(&probe_cell, &probe_log);
            }
        }));
        let registry = Arc::new(tokio::sync::Mutex::new(reg));
        cell.set(Arc::downgrade(&registry)).unwrap();
        let flip = Flip { value: Value::Int(100), dir: FlipDir::Enter };
        let (res, work) = walk_on_worker(&registry, &sig, &flip).await;

        res.unwrap();
        assert!(work.is_empty(), "the walk finished");
        let log = log.lock().unwrap().clone();
        assert_eq!(
            log,
            ["shape/s0", "contender", "shape/s1", "shape/s2", "parent-0", "contender", "parent-1", "parent-2"],
            "the contender gets the lock right after the first dependent's hold, in both tiers"
        );
    }

    /// **An empty shared read releases its group and moves nothing.** Shapes on `issues` and
    /// `comments`, parents on `t` and `t2`, where `comments` and `t2` have no rows: their reads come
    /// back empty, for a value flip and a NULL flip alike. While the empty groups wait, a live change
    /// lands on each of their tables (at the first `issues` delivery and after the first `t`
    /// reconcile), so those dependents hold recency stamps; releasing the empty group must clear
    /// them. The empty groups emit and reconcile nothing beyond the live change; the non-empty
    /// groups move as usual. With an emission failure in the `issues` group instead, every
    /// dependent — the empty group's included — is still released and the flip stays at the front.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_empty_shared_read_releases_its_group_and_moves_nothing() {
        for value in [Value::Int(100), Value::Null] {
            for fail_at in [None, Some("shape/issues-1")] {
                let case = format!("{value:?} failing at {fail_at:?}");
                let (ds_url, store) = spawn_fake_ds().await;
                let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test(&ds_url), None);
                let sig: SubquerySig = "child".into();
                insert_test_node(&mut reg, &sig);
                let ts = id_gid_ts("any");
                let (id_col, gid_col) = (ts.column_index("id").unwrap(), ts.column_index("gid").unwrap());
                let mut shapes = Vec::new();
                for table in ["issues", "comments"] {
                    for i in 0..2 {
                        let id = format!("{table}-{i}");
                        insert_outer_shape(
                            &mut reg,
                            &id,
                            table,
                            CompiledPredicate::InSubquery { col: gid_col, sig: sig.clone(), negated: true },
                        );
                        reg.add_edge(null_sensitive_edge(&sig, Dependent::Shape(id.clone()), gid_col));
                        shapes.push(id);
                    }
                }
                let mut parents = Vec::new();
                for table in ["t", "t2"] {
                    for i in 0..2 {
                        let parent = format!("{table}-parent-{i}");
                        insert_test_node_on(&mut reg, &parent, table, id_col);
                        reg.add_edge(null_sensitive_edge(&sig, Dependent::Node(parent.clone()), gid_col));
                        parents.push(parent);
                    }
                }
                reg.set_schemas(Arc::new(
                    ["issues", "comments", "t", "t2"].into_iter().map(|t| (TableRef::from(t), id_gid_ts(t))).collect(),
                ));
                let fake = FakeCandidates::with_rows([
                    ("issues", vec![id_gid_row(&ts, 1, 100)]),
                    ("comments", Vec::new()),
                    ("t", vec![id_gid_row(&ts, 7, 100)]),
                    ("t2", Vec::new()),
                ])
                .with_gate(crate::pg::SnapshotGate::parse("40:45:", "0/100"));
                let fake = Arc::new(fake);
                reg.fake_candidates = Some(fake.clone());
                let (comments, t2) = (id_gid_ts("comments"), id_gid_ts("t2"));
                let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
                reg.delivery_probe = Some(Arc::new(move |reg: &mut SubqueryRegistry, path: &str| {
                    if path == "shape/issues-0" && !fired.swap(true, std::sync::atomic::Ordering::SeqCst) {
                        apply_live(reg, &comments, &[Tup2(id_gid_row(&comments, 10, 300), 1)], 0x200, 50);
                    }
                    if Some(path) == fail_at {
                        anyhow::bail!("injected emission failure");
                    }
                    Ok(())
                }));
                let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
                reg.reconcile_probe = Some(Arc::new(move |reg: &mut SubqueryRegistry, parent: &str| {
                    if parent == "t-parent-0" && !fired.swap(true, std::sync::atomic::Ordering::SeqCst) {
                        apply_live(reg, &t2, &[Tup2(id_gid_row(&t2, 70, 300), 1)], 0x300, 51);
                    }
                }));
                let registry = Arc::new(tokio::sync::Mutex::new(reg));
                let flip = Flip { value: value.clone(), dir: FlipDir::Leave };
                let (res, work) = walk_on_worker(&registry, &sig, &flip).await;

                let key = |id: i64| ts.key_string(&id_gid_row(&ts, id, 0)).unwrap();
                let keys = |id: &str| -> Vec<String> {
                    envs_for(&store, &format!("shape/{id}")).into_iter().map(|(_, k, _)| k).collect()
                };
                let reg = registry.lock().await;
                // Released, and no recency left behind, whatever happened.
                for (id, s) in &reg.shapes {
                    assert_eq!(s.inflight_querybacks, 0, "{case}: shape {id} released");
                    assert!(s.recent.is_empty(), "{case}: shape {id}'s recency cleared");
                }
                for parent in &parents {
                    assert_eq!(reg.nodes[parent].inflight_querybacks, 0, "{case}: {parent} released");
                    assert!(reg.nodes[parent].recent.is_empty(), "{case}: {parent}'s recency cleared");
                }
                // The empty `comments` group emitted nothing of its own: only the live insert.
                for i in 0..2 {
                    assert_eq!(keys(&format!("comments-{i}")), vec![key(10)], "{case}: comments-{i}");
                }
                match fail_at {
                    None => {
                        res.unwrap();
                        assert!(work.is_empty(), "{case}: the walk finished");
                        assert_eq!(fake.reads(), 4, "{case}: one read per table, empty ones included");
                        for i in 0..2 {
                            assert_eq!(keys(&format!("issues-{i}")), vec![key(1)], "{case}: issues-{i} moved");
                            let (t_parent, t2_parent) = (format!("t-parent-{i}"), format!("t2-parent-{i}"));
                            assert!(reg.contains(&t_parent, &Value::Int(7)), "{case}: {t_parent} reconciled");
                            assert!(reg.contains(&t2_parent, &Value::Int(70)), "{case}: {t2_parent} has the live row");
                            assert!(!reg.contains(&t2_parent, &Value::Int(7)), "{case}: and nothing from `t`");
                        }
                    }
                    Some(_) => {
                        assert!(res.is_err(), "{case}: the failed emission fails the walk");
                        assert_eq!(work, VecDeque::from([(sig.clone(), flip.clone())]), "{case}: the flip goes back");
                        assert_eq!(fake.reads_of("t"), (0, 0), "{case}: the parent tier is never reached");
                        for parent in &parents {
                            for id in [7, 70] {
                                assert!(!reg.contains(parent, &Value::Int(id)), "{case}: {parent} not reconciled");
                            }
                        }
                    }
                }
            }
        }
    }

    /// A flip reaching installed AND still-being-created dependents: the pending ones get the work
    /// queued on them (a `Value` item for a value flip, `Full` for a NULL), never a read; the
    /// installed ones move from the one shared read. For shapes (a create's pending shape beside a
    /// live one) and parent nodes (a nested create's seeding node beside a live one).
    #[tokio::test(flavor = "multi_thread")]
    async fn a_flip_defers_pending_dependents_and_moves_installed_ones() {
        for value in [Value::Int(7), Value::Null] {
            let full = matches!(value, Value::Null);
            let flip = Flip { value: value.clone(), dir: FlipDir::Leave };
            let (trace_tx, _rx) = tokio::sync::broadcast::channel(64);

            // Shapes: `s1` is mid-create on node `x`; `live` is installed on it (`gid NOT IN x`).
            let (mut reg, begin) = registry_mid_create();
            let (ds_url, store) = spawn_fake_ds().await;
            reg.ds = DsClient::new_for_in_process_test(&ds_url);
            let x = begin.seeds[0].0.clone();
            let outer = reg.schemas[&TableRef::parse("outer_t").unwrap()].clone();
            let gid = outer.column_index("gid").unwrap();
            insert_outer_shape(
                &mut reg,
                "live",
                "outer_t",
                CompiledPredicate::InSubquery { col: gid, sig: x.clone(), negated: true },
            );
            reg.add_edge(null_sensitive_edge(&x, Dependent::Shape("s1".into()), gid));
            reg.add_edge(null_sensitive_edge(&x, Dependent::Shape("live".into()), gid));
            let fake = Arc::new(FakeCandidates::with_rows([(
                "outer_t",
                vec![id_gid_row(&outer, 1, 7), id_gid_row(&outer, 2, 8)],
            )]));
            reg.fake_candidates = Some(fake.clone());
            let registry = tokio::sync::Mutex::new(reg);
            let mut work: VecDeque<(SubquerySig, Flip)> = [(x.clone(), flip.clone())].into_iter().collect();
            propagate_with_retry(&registry, &mut work, None, None, &trace_tx).await.unwrap();

            assert_eq!(fake.reads(), 1, "{value:?}: only the installed shape reads");
            let keys: Vec<String> = envs_for(&store, "shape/live").into_iter().map(|(_, key, _)| key).collect();
            let key = |id: i64, g: i64| outer.key_string(&id_gid_row(&outer, id, g)).unwrap();
            let expected = if full { vec![key(1, 7), key(2, 8)] } else { vec![key(1, 7)] };
            assert_eq!(keys, expected, "{value:?}: the installed shape moves");
            let reg = registry.lock().await;
            let queued = &reg.pending_shapes[0].deferred;
            assert_eq!(queued.len(), 1, "{value:?}: the pending shape gets the work queued once");
            match (&queued[0], full) {
                (DeferredShapeWork::Full { .. }, true) => {}
                (DeferredShapeWork::Value { connecting_col, value: v, .. }, false) => {
                    assert_eq!((*connecting_col, v), (gid, &value));
                }
                (_, _) => panic!("{value:?}: wrong deferred shape work"),
            }
            assert!(!reg.shapes.contains_key("s1"), "{value:?}: and nothing was installed for it");
            assert_eq!(reg.shapes["live"].inflight_querybacks, 0);
            drop(reg);

            // Parent nodes: `mid` is still seeding (nested create); `live-parent` is installed on the
            // same inner table, both dependents of `deep`.
            let (mut reg, deep, mid) = registry_mid_nested_create();
            let mid_ts = reg.schemas[&TableRef::parse("mid_t").unwrap()].clone();
            let gid = mid_ts.column_index("gid").unwrap();
            let live: SubquerySig = "live-parent".into();
            insert_test_node_on(&mut reg, &live, "mid_t", mid_ts.column_index("id").unwrap());
            reg.add_edge(null_sensitive_edge(&deep, Dependent::Node(mid.clone()), gid));
            reg.add_edge(null_sensitive_edge(&deep, Dependent::Node(live.clone()), gid));
            let fake = Arc::new(FakeCandidates::with_rows([(
                "mid_t",
                vec![id_gid_row(&mid_ts, 5, 7), id_gid_row(&mid_ts, 6, 8)],
            )]));
            reg.fake_candidates = Some(fake.clone());
            let registry = tokio::sync::Mutex::new(reg);
            let mut work: VecDeque<(SubquerySig, Flip)> = [(deep.clone(), flip.clone())].into_iter().collect();
            propagate_with_retry(&registry, &mut work, None, None, &trace_tx).await.unwrap();

            assert_eq!(fake.reads(), 1, "{value:?}: only the installed parent reads");
            let reg = registry.lock().await;
            // `live-parent` is MatchAll projecting `id`.
            assert!(reg.contains(&live, &Value::Int(5)), "{value:?}: the installed parent reconciles");
            assert_eq!(reg.contains(&live, &Value::Int(6)), full, "{value:?}: only a NULL re-derives every row");
            let queued = &reg.nodes[&mid].deferred;
            assert_eq!(queued.len(), 1, "{value:?}: the seeding parent gets the work queued once");
            match (&queued[0], full) {
                (DeferredNodeWork::Full { .. }, true) => {}
                (DeferredNodeWork::Value { connecting_col, value: v, .. }, false) => {
                    assert_eq!((*connecting_col, v), (gid, &value));
                }
                (_, _) => panic!("{value:?}: wrong deferred node work"),
            }
            assert!(reg.nodes[&mid].seed_buffer.is_some(), "{value:?}: and it is still seeding");
            assert_eq!(reg.nodes[&live].inflight_querybacks, 0);
            assert_eq!(reg.nodes[&mid].inflight_querybacks, 0);
        }
    }

    /// The same, through the retrying entry point every caller uses: exhausting the attempts still
    /// hands the whole unfinished walk back, so the caller can report exactly what was lost.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_exhausted_retry_still_returns_the_unfinished_work() {
        let ts = issues_ts();
        let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test("http://unused"), None);
        let sig: SubquerySig = "project_members|project_id|L(user_id,Eq,1)".into();
        insert_test_node(&mut reg, &sig);
        insert_membership_shape(&mut reg, "s1", &sig, 1);
        reg.add_edge(Edge {
            node_sig: sig.clone(),
            dependent: Dependent::Shape("s1".into()),
            connecting_col: 1,
            negated: false,
            null_sensitive: false,
        });
        reg.set_schemas(Arc::new([(TableRef::parse("issues").unwrap(), ts)].into_iter().collect()));
        let registry = tokio::sync::Mutex::new(reg);
        let (trace_tx, _rx) = tokio::sync::broadcast::channel(16);

        let mut work: VecDeque<(SubquerySig, Flip)> =
            [(sig.clone(), Flip { value: Value::Int(100), dir: FlipDir::Enter })].into_iter().collect();
        let err = propagate_with_retry(&registry, &mut work, None, None, &trace_tx).await;

        assert!(err.is_err(), "postgres never comes back; every attempt fails");
        assert_eq!(work.len(), 1, "the abandoned flip is still on the queue for the report");
    }

    /// `outer_t(gid, id)` + `inner_t(gid, id)`: enough for `begin_create` to compile
    /// `gid IN (SELECT gid FROM inner_t)` and register a pending shape.
    fn creating_schemas() -> HashMap<TableRef, TableSchema> {
        use crate::schema::TableDef;
        let mk = |name: &str| {
            let def: TableDef = serde_json::from_value(serde_json::json!({
                "columns": { "id": {"type":"int"}, "gid": {"type":"int"} }, "primaryKey": "id"
            }))
            .unwrap();
            TableSchema::from_def(&TableRef::parse(name).unwrap(), &def).unwrap()
        };
        [("outer_t", mk("outer_t")), ("inner_t", mk("inner_t"))]
            .into_iter()
            .map(|(t, ts)| (TableRef::parse(t).unwrap(), ts))
            .collect()
    }

    fn in_inner_t() -> PredicateJson {
        serde_json::from_value(serde_json::json!({
            "col":"gid","in":{"table":"inner_t","project":"gid"}
        }))
        .unwrap()
    }

    /// A registry parked exactly where phase B runs: edges committed, shape not installed.
    fn registry_mid_create() -> (SubqueryRegistry, BeginCreate) {
        let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test("http://unused"), None);
        reg.set_schemas(Arc::new(creating_schemas()));
        let begin = reg.begin_create("s1", &"outer_t".into(), "shape/s1", &in_inner_t(), None, false).unwrap();
        (reg, begin)
    }

    #[tokio::test]
    async fn deferred_rederivations_retain_the_earliest_coalesced_commit() {
        let (reg, _) = registry_mid_create();
        let registry = tokio::sync::Mutex::new(reg);
        for lsn in [0x30, 0x10, 0x40] {
            move_shape_for_value(&registry, "s1", 0, &Value::Int(7), None, lsn).await.unwrap();
        }
        assert_eq!(registry.lock().await.pending_shapes[0].deferred[0].commit_lsn(), 0x10);
        rederive_shape(&registry, "s1", None, 0x50).await.unwrap();
        move_shape_for_value(&registry, "s1", 0, &Value::Int(8), None, 0x08).await.unwrap();
        {
            let reg = registry.lock().await;
            let queue = &reg.pending_shapes[0].deferred;
            assert_eq!(queue.len(), 1);
            assert!(matches!(queue[0], DeferredShapeWork::Full { commit_lsn: 0x08, .. }));
        }

        let (reg, _, mid) = registry_mid_nested_create();
        let registry = tokio::sync::Mutex::new(reg);
        for lsn in [0x30, 0x10, 0x40] {
            requery_and_reconcile_parent(&registry, &mid, Some((0, &Value::Int(7))), lsn).await.unwrap();
        }
        assert_eq!(registry.lock().await.nodes[&mid].deferred[0].commit_lsn(), 0x10);
        requery_and_reconcile_parent(&registry, &mid, None, 0x50).await.unwrap();
        requery_and_reconcile_parent(&registry, &mid, Some((0, &Value::Int(8))), 0x08).await.unwrap();
        let reg = registry.lock().await;
        let queue = &reg.nodes[&mid].deferred;
        assert_eq!(queue.len(), 1);
        assert!(matches!(queue[0], DeferredNodeWork::Full { commit_lsn: 0x08 }));
    }

    /// **Coalescing keeps the earliest origin as a pair.** Flip workers can take the registry lock
    /// out of commit order, so a later commit's flip may queue first. Whatever survives coalescing
    /// is stamped into one envelope's `headers.lsn` AND `headers.txid`, so the txid must be the
    /// earliest commit's, not the first-queued one's.
    #[tokio::test]
    async fn deferred_coalescing_keeps_the_earliest_commits_txid_with_its_lsn() {
        let origin = |w: &DeferredShapeWork| (w.commit_lsn(), w.txid().clone());

        // Same value, queued later commit first: T2 (0x30), then T1 (0x10), then T3 (0x40).
        let (reg, _) = registry_mid_create();
        let registry = tokio::sync::Mutex::new(reg);
        for (lsn, txid) in [(0x30, "t2"), (0x10, "t1"), (0x40, "t3")] {
            move_shape_for_value(&registry, "s1", 0, &Value::Int(7), Some(txid.into()), lsn).await.unwrap();
        }
        assert_eq!(origin(&registry.lock().await.pending_shapes[0].deferred[0]), (0x10, Some("t1".into())));

        // A full re-derive that arrives later but subsumes earlier per-value work takes that work's
        // origin, txid included…
        rederive_shape(&registry, "s1", Some("t5".into()), 0x50).await.unwrap();
        assert_eq!(origin(&registry.lock().await.pending_shapes[0].deferred[0]), (0x10, Some("t1".into())));
        // …and one already queued takes an earlier flip's origin, an unknown txid included.
        move_shape_for_value(&registry, "s1", 0, &Value::Int(8), None, 0x08).await.unwrap();
        assert_eq!(origin(&registry.lock().await.pending_shapes[0].deferred[0]), (0x08, None));
        // A LATER flip changes nothing.
        move_shape_for_value(&registry, "s1", 0, &Value::Int(9), Some("t6".into()), 0x60).await.unwrap();
        let reg = registry.lock().await;
        assert_eq!(reg.pending_shapes[0].deferred.len(), 1);
        assert_eq!(origin(&reg.pending_shapes[0].deferred[0]), (0x08, None));
    }

    #[tokio::test]
    async fn node_seed_replay_retains_each_buffered_commits_origin() {
        let (mut reg, begin) = registry_mid_create();
        let inner = reg.schemas[&TableRef::parse("inner_t").unwrap()].clone();
        for (lsn, gid) in [(0x10, 7), (0x30, 8)] {
            let work = reg
                .on_table_delta(&inner, &[Tup2(Row(vec![Value::Int(gid), Value::Int(gid)]), 1)], lsn, None, None, None)
                .await
                .unwrap();
            assert!(work.is_empty(), "the node is still seeding");
        }
        let seeds = begin
            .seeds
            .into_iter()
            .map(|(sig, _, _)| (sig, Vec::new(), crate::pg::SnapshotGate::passthrough()))
            .collect();
        let finished = reg
            .finish_create("s1", seeds, crate::pg::SnapshotGate::passthrough(), 0, Default::default())
            .await
            .unwrap();
        let origins: Vec<_> = finished.work.iter().map(|(lsn, _, _)| *lsn).collect();
        assert_eq!(origins, vec![0x10, 0x30]);
    }

    /// **A flip that arrives mid-create waits, it is not dropped.** Phase A commits the shape's
    /// edges, so a flip on a node the shape SHARES with a live sibling reaches this shape while
    /// phase B is still backfilling — and the backfill's snapshot may predate the inner change.
    /// Finding no installed shape and moving on would lose those rows for the life of the shape.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_flip_reaching_a_shape_mid_create_waits_on_its_pending_entry() {
        let (reg, _begin) = registry_mid_create();
        let registry = tokio::sync::Mutex::new(reg);

        // No `pg_url`: had the flip fallen through to its query-back instead of being queued, this
        // would be an error rather than "nothing moved".
        let moved = move_shape_for_value(&registry, "s1", 0, &Value::Int(7), None, 0).await.unwrap();
        assert!(moved.is_none(), "nothing moves: the shape is not installed yet");
        // A hot inner table flips the same value repeatedly; the replay is absolute, so once is enough.
        move_shape_for_value(&registry, "s1", 0, &Value::Int(7), None, 0).await.unwrap();
        move_shape_for_value(&registry, "s1", 0, &Value::Int(8), None, 0).await.unwrap();
        {
            let reg = registry.lock().await;
            assert!(reg.shapes.is_empty(), "the create has not installed anything");
            let queued = &reg.pending_shapes[0].deferred;
            assert_eq!(queued.len(), 2, "both distinct values queued; the repeat deduped");
            assert!(matches!(queued[0], DeferredShapeWork::Value { value: Value::Int(7), .. }));
            assert!(matches!(queued[1], DeferredShapeWork::Value { value: Value::Int(8), .. }));
        }

        // A NULL flip re-derives every row, so it subsumes anything queued before it.
        rederive_shape(&registry, "s1", None, 0).await.unwrap();
        let reg = registry.lock().await;
        let queued = &reg.pending_shapes[0].deferred;
        assert_eq!(queued.len(), 1, "the full re-derive replaced the per-value work");
        assert!(matches!(queued[0], DeferredShapeWork::Full { .. }));
    }

    /// `finish_create` hands the queue back with the pending entry removed and the shape installed,
    /// all in one `&mut self` step: after it, a flip finds the shape and runs straight through, and
    /// the caller has the only copy of what arrived while it could not.
    #[tokio::test(flavor = "multi_thread")]
    async fn finish_create_hands_back_the_queued_work() {
        let (reg, begin) = registry_mid_create();
        let seeds: Vec<(SubquerySig, Vec<Row>, crate::pg::SnapshotGate)> = begin
            .seeds
            .iter()
            .map(|(sig, _, _)| (sig.clone(), Vec::new(), crate::pg::SnapshotGate::passthrough()))
            .collect();
        let registry = tokio::sync::Mutex::new(reg);
        move_shape_for_value(&registry, "s1", 0, &Value::Int(7), None, 0).await.unwrap();

        let mut reg = registry.into_inner();
        let finished = reg
            .finish_create("s1", seeds, crate::pg::SnapshotGate::passthrough(), 0, Default::default())
            .await
            .unwrap();

        assert!(finished.work.is_empty(), "no buffered inner deltas to replay");
        assert_eq!(finished.deferred.len(), 1, "the queued flip comes back for the caller to run");
        assert!(finished.node_work.is_empty(), "no flip reached a node mid-seed");
        assert!(reg.pending_shapes.is_empty(), "the pending entry is gone, and its queue with it");
        assert!(reg.shapes.contains_key("s1"), "the shape is installed: later flips run straight through");
    }

    /// `outer_t(gid, id)` + `mid_t(gid, id)` + `deep_t(gid, id)`: enough to compile the NESTED
    /// `gid IN (SELECT gid FROM mid_t WHERE gid IN (SELECT gid FROM deep_t))`, which registers two
    /// fresh nodes — a deepest one and a PARENT node depending on it.
    fn nested_schemas() -> HashMap<TableRef, TableSchema> {
        use crate::schema::TableDef;
        let mk = |name: &str| {
            let def: TableDef = serde_json::from_value(serde_json::json!({
                "columns": { "id": {"type":"int"}, "gid": {"type":"int"} }, "primaryKey": "id"
            }))
            .unwrap();
            TableSchema::from_def(&TableRef::parse(name).unwrap(), &def).unwrap()
        };
        ["outer_t", "mid_t", "deep_t"].into_iter().map(|t| (TableRef::parse(t).unwrap(), mk(t))).collect()
    }

    /// A registry parked in a NESTED create's phase B: both fresh nodes seeding (deepest first),
    /// the child→parent edge committed, no shape installed. Returns the registry, the deepest
    /// node's sig and the parent node's.
    fn registry_mid_nested_create() -> (SubqueryRegistry, SubquerySig, SubquerySig) {
        let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test("http://unused"), None);
        reg.set_schemas(Arc::new(nested_schemas()));
        let where_json: PredicateJson = serde_json::from_value(serde_json::json!({
            "col": "gid",
            "in": {
                "table": "mid_t",
                "project": "gid",
                "where": { "col": "gid", "in": { "table": "deep_t", "project": "gid" } }
            }
        }))
        .unwrap();
        let begin = reg.begin_create("s1", &"outer_t".into(), "shape/s1", &where_json, None, false).unwrap();
        assert_eq!(begin.seeds.len(), 2, "two fresh nodes, deepest first");
        let (deep, mid) = (begin.seeds[0].0.clone(), begin.seeds[1].0.clone());
        (reg, deep, mid)
    }

    /// **A flip that reaches a still-seeding parent NODE waits, it is not reconciled.** The node's
    /// set is empty until phase C installs its seed, so re-deriving now derives nothing — and the
    /// seed, taken from a snapshot older than the flip, is then installed over the change, leaving
    /// the node and everything below it stale for the life of the shape.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_flip_reaching_a_seeding_parent_node_waits_on_it() {
        let (reg, deep, mid) = registry_mid_nested_create();
        let mid_id = reg.nodes[&mid].node_id;
        let registry = tokio::sync::Mutex::new(reg);
        let (trace_tx, _rx) = tokio::sync::broadcast::channel(16);

        // No `pg_url`: had the flip fallen through to its query-back instead of being deferred,
        // this walk would fail rather than complete quietly.
        let flip = |v: i64| -> VecDeque<(SubquerySig, Flip)> {
            [(deep.clone(), Flip { value: Value::Int(v), dir: FlipDir::Enter })].into_iter().collect()
        };
        propagate_flips(&registry, &mut flip(7), None, None, &trace_tx).await.unwrap();
        // A hot inner table flips the same value repeatedly during one seed; the re-derive is
        // absolute, so running it once at install is running it enough.
        propagate_flips(&registry, &mut flip(7), None, None, &trace_tx).await.unwrap();
        {
            let reg = registry.lock().await;
            let queued = &reg.nodes[&mid].deferred;
            assert_eq!(queued.len(), 1, "queued once on the parent node; the repeat deduped");
            assert!(matches!(queued[0], DeferredNodeWork::Value { connecting_col: 0, value: Value::Int(7), .. }));
            assert_eq!(reg.circuit_distinct(mid_id), 0, "the seeding node's set is untouched");
            assert!(reg.nodes[&mid].seed_buffer.is_some(), "and it is still seeding");
        }

        // A full re-derive covers every value, so it subsumes what was queued before it.
        assert!(requery_and_reconcile_parent(&registry, &mid, None, 0).await.unwrap().is_none());
        let reg = registry.lock().await;
        let queued = &reg.nodes[&mid].deferred;
        assert_eq!(queued.len(), 1, "the full re-derive replaced the per-value work");
        assert!(matches!(queued[0], DeferredNodeWork::Full { .. }));
    }

    /// `finish_create` hands the node's deferred work back once that node's seed and its buffered
    /// raw deltas are both in — the first moment re-deriving it against Postgres means anything.
    #[tokio::test(flavor = "multi_thread")]
    async fn finish_create_hands_back_the_node_work() {
        let (reg, deep, mid) = registry_mid_nested_create();
        let seeds: Vec<(SubquerySig, Vec<Row>, crate::pg::SnapshotGate)> = [&deep, &mid]
            .into_iter()
            .map(|sig| (sig.clone(), Vec::new(), crate::pg::SnapshotGate::passthrough()))
            .collect();
        let registry = tokio::sync::Mutex::new(reg);
        let (trace_tx, _rx) = tokio::sync::broadcast::channel(16);
        let mut work: VecDeque<(SubquerySig, Flip)> =
            [(deep.clone(), Flip { value: Value::Int(7), dir: FlipDir::Enter })].into_iter().collect();
        propagate_flips(&registry, &mut work, None, None, &trace_tx).await.unwrap();

        let mut reg = registry.into_inner();
        let finished = reg
            .finish_create("s1", seeds, crate::pg::SnapshotGate::passthrough(), 0, Default::default())
            .await
            .unwrap();

        assert_eq!(finished.node_work.len(), 1, "the parent node's re-derivation comes back");
        assert_eq!(finished.node_work[0].0, mid, "aimed at the parent node that deferred it");
        assert!(reg.nodes[&mid].deferred.is_empty(), "taken, not copied");
        assert!(reg.nodes[&mid].seed_buffer.is_none(), "and the node is live, so nothing defers again");
    }

    /// A create cancelled in the MIDDLE of phase C — one fresh node's seed already asserted into
    /// the membership circuit, the shape not installed — leaves nothing behind. The rollback state
    /// is the registry's (the pending entry never left it), so the abort can retract the partial
    /// seed as well as unwind the refcounts, edges and templates.
    #[tokio::test(flavor = "multi_thread")]
    async fn abort_after_a_partial_phase_c_leaves_no_state() {
        let (mut reg, deep, mid) = registry_mid_nested_create();
        let (deep_id, mid_id) = (reg.nodes[&deep].node_id, reg.nodes[&mid].node_id);
        // Phase C got as far as installing the deepest node's seed, then the request was dropped.
        reg.apply_node_evals(&deep, vec![("1".to_string(), Some(Value::Int(7)))]).await;
        assert_eq!(reg.circuit_distinct(deep_id), 1, "the partial seed really landed");

        reg.abort_create("s1").await;

        assert!(reg.pending_shapes.is_empty(), "no pending entry");
        assert!(reg.shapes.is_empty(), "and no installed shape either");
        assert!(reg.nodes.is_empty(), "both fresh nodes are gone");
        assert_eq!(reg.edges_count(), 0, "no edge outlives the create that staged it");
        assert!(reg.templates.is_empty(), "no template bind outlives its only node");
        assert!(reg.pending_seed.is_empty(), "nothing is left waiting to be seeded");
        assert_eq!(reg.circuit_distinct(deep_id), 0, "the partial seed was retracted");
        assert_eq!(reg.circuit_distinct(mid_id), 0);
    }

    /// The deferred replay is resumable for the same reason the live walk is: a failed item goes
    /// back at the front, so the retry redoes exactly what did not land and nothing else.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_deferred_item_is_restored_for_the_retry() {
        let ts = issues_ts();
        // No `pg_url`: the first query-back fails, as an outage makes it fail.
        let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test("http://unused"), None);
        let sig: SubquerySig = "project_members|project_id|L(user_id,Eq,1)".into();
        insert_test_node(&mut reg, &sig);
        insert_membership_shape(&mut reg, "s1", &sig, 1);
        reg.set_schemas(Arc::new([(TableRef::parse("issues").unwrap(), ts)].into_iter().collect()));
        let registry = tokio::sync::Mutex::new(reg);

        let mut work: VecDeque<DeferredShapeWork> = [
            DeferredShapeWork::Value { connecting_col: 1, value: Value::Int(100), txid: None, commit_lsn: 0 },
            DeferredShapeWork::Full { txid: None, commit_lsn: 0 },
        ]
        .into_iter()
        .collect();
        let res = propagate_deferred_shape_work(&registry, "s1", &mut work).await;

        assert!(res.is_err(), "the query-back cannot succeed without Postgres");
        assert_eq!(work.len(), 2, "a failed replay must not consume the work it did not finish");
        assert!(
            matches!(work[0], DeferredShapeWork::Value { value: Value::Int(100), .. }),
            "the failed item goes back at the front"
        );
    }

    /// Gating: a flip that changes no dependent membership emits nothing. Here a NULL flip reaches a
    /// plain (non-negated) `IN` dependent, which NULL can't move — `propagate_flips` skips it, so no
    /// path lights and the visualizer fades nothing spuriously.
    #[tokio::test]
    async fn flip_no_op_emits_no_trace() {
        let mut reg = registry_with_node("sig1");
        reg.add_edge(Edge {
            node_sig: "sig1".into(),
            dependent: Dependent::Shape("s7".into()),
            connecting_col: 0,
            negated: false,
            null_sensitive: false,
        });
        let reg = tokio::sync::Mutex::new(reg);
        let (trace_tx, mut trace_rx) = tokio::sync::broadcast::channel::<Arc<String>>(8);

        let mut work: VecDeque<(SubquerySig, Flip)> = VecDeque::new();
        work.push_back(("sig1".into(), Flip { value: Value::Null, dir: FlipDir::Enter }));
        propagate_flips(&reg, &mut work, None, None, &trace_tx).await.unwrap();
        assert!(trace_rx.try_recv().is_err(), "a NULL flip on a non-null-sensitive dependent emits nothing");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reconcile_enter_and_leave_on_first_and_last_contributor() {
        let sig: SubquerySig = "sig".into();
        let mut reg = registry_with_node(&sig);
        let evals = |pk: &str, v: Option<Value>| vec![(pk.to_string(), v)];
        assert_eq!(
            reg.apply_node_evals(&sig, evals("a", Some(Value::Int(5)))).await,
            vec![Flip { value: Value::Int(5), dir: FlipDir::Enter }]
        );
        assert!(reg.contains(&sig, &Value::Int(5)));
        // second contributor to the same value -> no flip
        assert_eq!(reg.apply_node_evals(&sig, evals("b", Some(Value::Int(5)))).await, vec![]);
        // removing one of two -> still present, no flip
        assert_eq!(reg.apply_node_evals(&sig, evals("a", None)).await, vec![]);
        assert!(reg.contains(&sig, &Value::Int(5)));
        // removing the last -> Leave
        assert_eq!(
            reg.apply_node_evals(&sig, evals("b", None)).await,
            vec![Flip { value: Value::Int(5), dir: FlipDir::Leave }]
        );
        assert!(!reg.contains(&sig, &Value::Int(5)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reconcile_value_change_emits_leave_then_enter() {
        let sig: SubquerySig = "sig".into();
        let mut reg = registry_with_node(&sig);
        reg.apply_node_evals(&sig, vec![("a".into(), Some(Value::Int(5)))]).await;
        let mut flips = reg.apply_node_evals(&sig, vec![("a".into(), Some(Value::Int(7)))]).await;
        flips.sort_by(|a, b| a.value.cmp(&b.value));
        assert_eq!(
            flips,
            vec![
                Flip { value: Value::Int(5), dir: FlipDir::Leave },
                Flip { value: Value::Int(7), dir: FlipDir::Enter },
            ]
        );
        assert!(!reg.contains(&sig, &Value::Int(5)));
        assert!(reg.contains(&sig, &Value::Int(7)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reconcile_same_value_is_a_noop() {
        let sig: SubquerySig = "sig".into();
        let mut reg = registry_with_node(&sig);
        reg.apply_node_evals(&sig, vec![("a".into(), Some(Value::Int(5)))]).await;
        assert_eq!(reg.apply_node_evals(&sig, vec![("a".into(), Some(Value::Int(5)))]).await, vec![]);
        // unchanged absence is also a no-op
        assert_eq!(reg.apply_node_evals(&sig, vec![("z".into(), None)]).await, vec![]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn null_bucket_tracks_has_null() {
        let sig: SubquerySig = "sig".into();
        let mut reg = registry_with_node(&sig);
        assert_eq!(
            reg.apply_node_evals(&sig, vec![("a".into(), Some(Value::Null))]).await,
            vec![Flip { value: Value::Null, dir: FlipDir::Enter }]
        );
        assert!(reg.has_null(&sig));
        assert_eq!(
            reg.apply_node_evals(&sig, vec![("a".into(), None)]).await,
            vec![Flip { value: Value::Null, dir: FlipDir::Leave }]
        );
        assert!(!reg.has_null(&sig));
    }

    /// `NOT (x IN S)` is exactly as NULL-sensitive as `x NOT IN S`: a NULL entering `S` turns the leaf
    /// UNKNOWN, and the enclosing NOT converts that into a membership change. The edge must record it,
    /// or a NULL flip silently skips the re-derivation and members go stale.
    #[test]
    fn null_sensitivity_tracks_not_wrappers_and_negated_leaves() {
        use crate::schema::TableDef;
        let ts = {
            let def: TableDef = serde_json::from_value(serde_json::json!({
                "columns": { "id": {"type":"int"}, "gid": {"type":"int"} }, "primaryKey": "id"
            }))
            .unwrap();
            crate::schema::TableSchema::from_def(&"outer_t".into(), &def).unwrap()
        };
        struct Rec;
        impl crate::predicate::SubqueryCollector for Rec {
            fn collect(&mut self, t: &TableRef, p: &str, w: Option<&PredicateJson>) -> Result<SubquerySig> {
                Ok(crate::predicate::subquery_sig(t, p, w))
            }
        }
        let compile = |j: serde_json::Value| {
            CompiledPredicate::compile_with(&serde_json::from_value(j).unwrap(), &ts, &mut Rec).unwrap()
        };
        let in_sub = serde_json::json!({"col":"gid","in":{"table":"outer_t","project":"gid"}});

        // plain IN: not NULL-sensitive (FALSE↔UNKNOWN can't change inclusion without negation)
        let leaves = collect_in_leaves(&compile(in_sub.clone()));
        assert!(!leaves[0].negated && !leaves[0].null_sensitive);

        // NOT IN leaf: NULL-sensitive
        let mut neg = in_sub.clone();
        neg["negated"] = serde_json::json!(true);
        let leaves = collect_in_leaves(&compile(neg));
        assert!(leaves[0].negated && leaves[0].null_sensitive);

        // IN under a Not wrapper: NULL-sensitive even though the leaf isn't negated
        let leaves = collect_in_leaves(&compile(serde_json::json!({"not": in_sub.clone()})));
        assert!(!leaves[0].negated && leaves[0].null_sensitive);

        // IN nested under Not(And(...)): still NULL-sensitive
        let leaves = collect_in_leaves(&compile(serde_json::json!({
            "not": {"and": [ {"col":"id","op":"gt","value":0}, in_sub ]}
        })));
        assert!(leaves[0].null_sensitive);
    }

    /// A failed create (here: no Postgres to seed from) must roll the registry back to exactly its
    /// prior state — no orphaned node, edge, or pending-seed entry that a later identical create
    /// would silently join and read unseeded (wrong) membership from.
    #[tokio::test]
    async fn failed_create_rolls_back_nodes_and_edges() {
        use crate::schema::TableDef;
        let mk = |name: &str| {
            let def: TableDef = serde_json::from_value(serde_json::json!({
                "columns": { "id": {"type":"int"}, "gid": {"type":"int"} }, "primaryKey": "id"
            }))
            .unwrap();
            crate::schema::TableSchema::from_def(&TableRef::parse(name).unwrap(), &def).unwrap()
        };
        let mut schemas = HashMap::new();
        schemas.insert(TableRef::parse("outer_t").unwrap(), mk("outer_t"));
        schemas.insert(TableRef::parse("inner_t").unwrap(), mk("inner_t"));
        // No pg_url: node seeding must fail after collect() has already registered the node.
        let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test("http://unused"), None);
        reg.set_schemas(Arc::new(schemas));
        let where_json: PredicateJson = serde_json::from_value(serde_json::json!({
            "col":"gid","in":{"table":"inner_t","project":"gid"}
        }))
        .unwrap();
        // Three-phase: begin registers (nodes buffering, edges, pending shape); a phase-B
        // failure is rolled back exactly by abort_create.
        let begin = reg.begin_create("s1", &"outer_t".into(), "shape/s1", &where_json, None, false).unwrap();
        assert_eq!(begin.seeds.len(), 1, "one fresh node to seed");
        assert_eq!(reg.nodes.len(), 1);
        assert!(reg.nodes.values().all(|n| n.seed_buffer.is_some()), "fresh node buffers");
        assert!(reg.touches(&"outer_t".into()), "pending shape routes its outer table");
        reg.abort_create("s1").await;
        assert_eq!(reg.nodes.len(), 0, "aborted create left an orphaned node");
        assert_eq!(reg.edges_count(), 0, "aborted create left orphaned edges");
        assert_eq!(reg.pending_seed.len(), 0, "aborted create left a pending seed");
        assert!(reg.shapes.is_empty());
        assert!(reg.pending_shapes.is_empty());
    }

    /// The per-feed key set is the fix for the live-poll wake-storm bug, now structural:
    /// `emit_for_shapes` computes an *absolute* membership verdict for every touched pk, but a
    /// delete envelope is built ONLY where the host-side FeedSet actually retracts — a "not a
    /// member" verdict for a pk the stream never contained gates to nothing (`remove` returns
    /// false), so the spurious delete that used to wake every idle long-poll cannot be emitted at
    /// all (there is no filter left to get out of sync). The end-to-end drop is asserted via
    /// `emit_for_shapes`; the gate proper (a known member's delete opens, a repeat nets nothing)
    /// is asserted by poking the FeedSet directly — its verdicts are the delete gate.
    #[tokio::test(flavor = "multi_thread")]
    async fn feed_relation_drops_deletes_for_never_known_pks() {
        use crate::schema::TableDef;
        let def: TableDef = serde_json::from_value(serde_json::json!({
            "columns": { "id": {"type":"int"}, "gid": {"type":"int"} }, "primaryKey": "id"
        }))
        .unwrap();
        let ts = crate::schema::TableSchema::from_def(&"t".into(), &def).unwrap();
        let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test("http://unused"), None);
        // A shape whose predicate never matches (Not(MatchAll)): every candidate verdict is
        // "not a member".
        insert_outer_shape(&mut reg, "s1", "t", CompiledPredicate::Not(Box::new(CompiledPredicate::MatchAll)));
        let row = |id: i64| Row(vec![Value::Int(id), Value::Int(0)]);

        // A "leave" for a pk this feed never contained: nothing is emitted (no lanes are
        // configured, so an emission would attempt a real append and fail loudly; emitted
        // stays 0 and the result reports nothing delivered).
        let results = reg
            .emit_for_shapes(
                &ts,
                vec![("s1".to_string(), vec![(row(1), true)])],
                None,
                EmissionSource::Live { lsn: 1, xid: None },
            )
            .await
            .unwrap();
        assert_eq!(results, vec![("s1".to_string(), false, 0)], "never-member delete must be dropped");
        assert_eq!(reg.shapes["s1"].emitted.load(std::sync::atomic::Ordering::Relaxed), 0);

        // Seed pk 1 as a member (backfill hand-off), then the same verdict is a GENUINE leave.
        // The pk id must match the one `emit_for_shapes` would probe for row(1)'s pk string, so
        // build the key through the SAME dictionary.
        let pk_id = reg.pk_dict.get_or_insert("1");
        let feed_id = reg.shapes["s1"].feed_id;
        reg.feed_sets.insert(feed_id, pk_id);
        assert!(
            reg.feed_sets.remove(feed_id, pk_id),
            "a known member's delete must gate OPEN (remove returns true → retraction)"
        );

        // And once retracted, a repeat delete gates closed again.
        assert!(
            !reg.feed_sets.remove(feed_id, pk_id),
            "repeat delete for an already-removed pk must gate closed (remove returns false)"
        );
    }

    /// Focused regression: `emit_for_shapes` must mint a `pk_dict` id ONLY for a genuine member
    /// (mirrors the existing probe-without-minting rationale for `template_assertions`'s
    /// `pk_dict.get` lookups). A delete or non-matching candidate for a pk never seen before must
    /// leave the dictionary's `len()` unchanged — no forward/reverse slot for a pk that can never
    /// be present in the feed relation — while a genuinely matching candidate DOES mint.
    #[tokio::test(flavor = "multi_thread")]
    async fn never_member_candidate_does_not_mint_pk_dict_id() {
        use crate::schema::TableDef;
        let def: TableDef = serde_json::from_value(serde_json::json!({
            "columns": { "id": {"type":"int"}, "gid": {"type":"int"} }, "primaryKey": "id"
        }))
        .unwrap();
        let ts = crate::schema::TableSchema::from_def(&"t".into(), &def).unwrap();
        // A real (fake) DS server: the final phase genuinely emits an upsert, which would retry
        // forever against an unreachable URL.
        let (ds_url, _store) = spawn_fake_ds().await;
        let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test(&ds_url), None);
        insert_outer_shape(&mut reg, "s1", "t", CompiledPredicate::MatchAll);
        let row = |id: i64| Row(vec![Value::Int(id), Value::Int(0)]);
        assert_eq!(reg.pk_dict.len(), 0);

        // A delete for a brand-new pk (42), never interned before: `exists = false` forces
        // `member = false` regardless of the (always-true) predicate. The fix must skip minting.
        reg.emit_for_shapes(
            &ts,
            vec![("s1".to_string(), vec![(row(42), false)])],
            None,
            EmissionSource::Live { lsn: 1, xid: None },
        )
        .await
        .unwrap();
        assert_eq!(reg.pk_dict.len(), 0, "a never-member delete candidate must not mint a pk_dict id");

        // A non-matching (but existing) candidate for another brand-new pk (43) must likewise
        // skip minting: swap in a never-matching predicate for this check. (Poking `pred` in
        // place would desync the conjunct index — safe ONLY because this test calls
        // `emit_for_shapes` directly and never probes the index. Production always re-files
        // through `install_shape`.)
        reg.shapes.get_mut("s1").unwrap().pred =
            Arc::new(CompiledPredicate::Not(Box::new(CompiledPredicate::MatchAll)));
        reg.emit_for_shapes(
            &ts,
            vec![("s1".to_string(), vec![(row(43), true)])],
            None,
            EmissionSource::Live { lsn: 1, xid: None },
        )
        .await
        .unwrap();
        assert_eq!(reg.pk_dict.len(), 0, "a non-matching candidate must not mint a pk_dict id");

        // A genuinely matching insert (pk 44) DOES mint exactly one id.
        reg.shapes.get_mut("s1").unwrap().pred = Arc::new(CompiledPredicate::MatchAll);
        reg.emit_for_shapes(
            &ts,
            vec![("s1".to_string(), vec![(row(44), true)])],
            None,
            EmissionSource::Live { lsn: 1, xid: None },
        )
        .await
        .unwrap();
        assert_eq!(reg.pk_dict.len(), 1, "a matching candidate must mint exactly one pk_dict id");
    }

    // --- Task 0.3 harness: a minimal fake durable-streams server ------------------------------
    //
    // `emit_for_shapes`'s `deliver()` falls back to `DsClient::append_reliable` when no lanes are
    // configured (unit tests): a genuinely non-empty emission would otherwise retry forever
    // against `http://unused`. `tests/live_poll_deadline.rs` and `tests/params_shape.rs` solve
    // this the same way — spawn a real (local) axum server and point a real `DsClient` at it —
    // reused verbatim here so appends actually land and can be inspected, instead of asserting
    // only the `emitted` counter.

    #[derive(Clone, Default)]
    pub(crate) struct DsStore(pub(crate) Arc<std::sync::Mutex<HashMap<String, Vec<Envelope>>>>);

    async fn fake_ds_handler(
        axum::extract::State(store): axum::extract::State<DsStore>,
        req: axum::extract::Request,
    ) -> axum::response::Response {
        use axum::http::{Method, StatusCode};
        use axum::response::IntoResponse;
        match *req.method() {
            Method::PUT | Method::DELETE => StatusCode::OK.into_response(),
            Method::POST => {
                let path = req.uri().path().trim_start_matches('/').rsplit_once("/queries/test-query/").map_or_else(
                    || req.uri().path().trim_start_matches('/').to_string(),
                    |(_, logical)| logical.to_string(),
                );
                let body = axum::body::to_bytes(req.into_body(), 1024 * 1024).await.unwrap_or_default();
                if let Ok(envs) = serde_json::from_slice::<Vec<Envelope>>(&body) {
                    store.0.lock().unwrap().entry(path).or_default().extend(envs);
                }
                StatusCode::OK.into_response()
            }
            Method::GET => ([("stream-next-offset", "tip"), ("stream-up-to-date", "1")], "[]").into_response(),
            _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
        }
    }

    /// Boots the fake server on an ephemeral port; returns its base URL and the shared store of
    /// every envelope POSTed to it, keyed by stream path (e.g. `"shape/s1"`).
    pub(crate) async fn spawn_fake_ds() -> (String, DsStore) {
        let store = DsStore::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = axum::Router::new().fallback(fake_ds_handler).with_state(store.clone());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}"), store)
    }

    /// The operations (`"upsert"`/`"delete"`) delivered to one stream path so far, in append order.
    fn ops_for(store: &DsStore, path: &str) -> Vec<String> {
        store
            .0
            .lock()
            .unwrap()
            .get(path)
            .map(|v| v.iter().map(|e| e.headers.operation.clone()).collect())
            .unwrap_or_default()
    }

    /// `issues(id, project_id)`, matching the brief's example shape:
    /// `issues WHERE project_id IN (SELECT project_id FROM project_members WHERE user_id = ...)`.
    pub(crate) fn issues_ts() -> TableSchema {
        use crate::schema::TableDef;
        let def: TableDef = serde_json::from_value(serde_json::json!({
            "columns": { "id": {"type":"int"}, "project_id": {"type":"int"} },
            "primaryKey": "id"
        }))
        .unwrap();
        TableSchema::from_def(&"issues".into(), &def).unwrap()
    }

    pub(crate) fn issue(id: i64, project_id: i64) -> Row {
        Row(vec![Value::Int(id), Value::Int(project_id)])
    }

    /// Registers a shape whose predicate is `project_id IN (node sig)` — the outer half of the
    /// brief's example, wired to a membership node inserted with [`insert_test_node`].
    fn insert_membership_shape(reg: &mut SubqueryRegistry, shape_id: &str, sig: &SubquerySig, project_col: usize) {
        insert_outer_shape(
            reg,
            shape_id,
            "issues",
            CompiledPredicate::InSubquery { col: project_col, sig: sig.clone(), negated: false },
        );
    }

    /// Register an outer shape with an arbitrary predicate through the SAME installation path
    /// production uses (`install_shape`), so the necessary-conjunct index is populated exactly as
    /// `finish_create` would populate it.
    pub(crate) fn insert_outer_shape(
        reg: &mut SubqueryRegistry,
        shape_id: &str,
        outer_table: &str,
        pred: CompiledPredicate,
    ) {
        install_outer_shape(reg, shape_id, outer_table, pred, false);
    }

    /// [`insert_outer_shape`] for a `changes_only` feed, exactly as its create installs it: a
    /// passthrough gate, nothing emitted, an empty key set.
    pub(crate) fn insert_changes_only_feed(
        reg: &mut SubqueryRegistry,
        shape_id: &str,
        outer_table: &str,
        pred: CompiledPredicate,
    ) {
        install_outer_shape(reg, shape_id, outer_table, pred, true);
    }

    fn install_outer_shape(
        reg: &mut SubqueryRegistry,
        shape_id: &str,
        outer_table: &str,
        pred: CompiledPredicate,
        changes_only: bool,
    ) {
        let feed_id = reg.next_feed_id;
        reg.next_feed_id += 1;
        reg.install_shape(SubqueryShape {
            shape_id: shape_id.to_string(),
            outer_table: outer_table.into(),
            stream_path: format!("shape/{shape_id}"),
            pred: Arc::new(pred),
            out_cols: None,
            gate: crate::pg::SnapshotGate::passthrough(),
            changes_only,
            emitted: std::sync::atomic::AtomicU64::new(0),
            feed_id,
            recent: HashMap::new(),
            inflight_querybacks: 0,
            position: 0,
        });
    }

    /// A realistic LinearLite-style outer predicate: `project_id = k AND project_id IN (SELECT …)`.
    /// `access_leaf` lifts the equality as the necessary conjunct; the `IN` leaf is not row-local
    /// and can never be one.
    pub(crate) fn keyed_membership_pred(project_id: i64, sig: &SubquerySig) -> CompiledPredicate {
        CompiledPredicate::And(vec![
            CompiledPredicate::Cmp { col: 1, op: crate::predicate::LeafOp::Eq, value: Value::Int(project_id) },
            CompiledPredicate::InSubquery { col: 1, sig: sig.clone(), negated: false },
        ])
    }

    /// bead dbsp-ds-g5p, regression 1/2 — **THE correctness trap of the conjunct index.**
    ///
    /// Outer membership is emitted ABSOLUTELY (§3.3): every touched pk gets its *current*
    /// verdict, and a move-out is a delete the feed gate opens. So a candidate probe that only
    /// looked at the row's NEW image would skip the shape the row just LEFT — and the delete
    /// would never be built. The probe must union the old (`-1`) and new (`+1`) images of the
    /// delta; this test moves a row out of the indexed conjunct's value and demands the delete.
    #[tokio::test(flavor = "multi_thread")]
    async fn move_out_delete_survives_the_conjunct_index() {
        let ts = issues_ts();
        let (ds_url, store) = spawn_fake_ds().await;
        let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test(&ds_url), None);
        let sig: SubquerySig = "project_members|project_id|L(user_id,Eq,1)".into();
        insert_test_node(&mut reg, &sig);
        // User 1 is a member of BOTH projects, so the only thing that moves the row is the
        // indexed equality conjunct — isolating exactly what the index decides.
        reg.apply_node_evals(
            &sig,
            vec![("pm-1".into(), Some(Value::Int(100))), ("pm-2".into(), Some(Value::Int(200)))],
        )
        .await;
        insert_outer_shape(&mut reg, "s100", "issues", keyed_membership_pred(100, &sig));
        insert_outer_shape(&mut reg, "s200", "issues", keyed_membership_pred(200, &sig));

        // Enter s100: issue 1 lands in project 100.
        reg.on_table_delta(&ts, &[Tup2(issue(1, 100), 1)], 1, None, None, None).await.unwrap();
        assert_eq!(ops_for(&store, "shape/s100"), vec!["upsert"]);

        // The move: UPDATE project_id 100 -> 200, as `apply_envelope` builds it (old `-1`, new
        // `+1`). The NEW image fails s100's indexed conjunct entirely — s100 is only reachable
        // through the old image.
        let delta = vec![Tup2(issue(1, 100), -1), Tup2(issue(1, 200), 1)];
        assert!(
            reg.outer_candidates(&"issues".into(), &delta).contains(&"s100".to_string()),
            "the shape the row LEFT must be a candidate — via the delta's old image"
        );
        reg.on_table_delta(&ts, &delta, 2, None, None, None).await.unwrap();
        assert_eq!(
            ops_for(&store, "shape/s100"),
            vec!["upsert", "delete"],
            "the move-out delete must still be emitted after the index skips non-candidates"
        );
        assert_eq!(ops_for(&store, "shape/s200"), vec!["upsert"], "and the move-in upsert lands");
    }

    /// A subquery shape's live emission carries the commit time of the commit that caused it
    /// (`headers.commit_ts`), looked up by commit LSN; a commit the registry no longer remembers
    /// (or never had a time for) delivers unstamped rather than with another commit's time.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_delivery_carries_its_commits_time() {
        let ts = issues_ts();
        let (ds_url, store) = spawn_fake_ds().await;
        let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test(&ds_url), None);
        let sig: SubquerySig = "project_members|project_id|L(user_id,Eq,1)".into();
        insert_test_node(&mut reg, &sig);
        reg.apply_node_evals(&sig, vec![("pm-1".into(), Some(Value::Int(100)))]).await;
        insert_outer_shape(&mut reg, "s100", "issues", keyed_membership_pred(100, &sig));
        let commit_ts_for = |path: &str| -> Vec<Option<i64>> {
            store
                .0
                .lock()
                .unwrap()
                .get(path)
                .map(|v| v.iter().map(|e| e.headers.commit_ts).collect())
                .unwrap_or_default()
        };

        reg.note_commit_time(0x10, 1_793_404_800_123);
        reg.on_table_delta(&ts, &[Tup2(issue(1, 100), 1)], 0x10, None, None, None).await.unwrap();
        assert_eq!(commit_ts_for("shape/s100"), vec![Some(1_793_404_800_123)]);

        // Commit 0x20 has no recorded time (an older change log, or it was evicted).
        reg.on_table_delta(&ts, &[Tup2(issue(2, 100), 1)], 0x20, None, None, None).await.unwrap();
        assert_eq!(commit_ts_for("shape/s100"), vec![Some(1_793_404_800_123), None]);

        // A deferred query-back delivers under the LSN of the commit that triggered it.
        reg.note_commit_time(0x30, 1_793_404_800_456);
        let gate = crate::pg::SnapshotGate::passthrough();
        reg.emit_for_shapes(
            &ts,
            vec![("s100".to_string(), vec![(issue(3, 100), true)])],
            None,
            EmissionSource::QueryBack { gate: &gate, commit_lsn: 0x30 },
        )
        .await
        .unwrap();
        assert_eq!(commit_ts_for("shape/s100"), vec![Some(1_793_404_800_123), None, Some(1_793_404_800_456)]);

        // Only the newest COMMIT_TIMES_RETAINED commits are kept.
        for lsn in 0..=COMMIT_TIMES_RETAINED as u64 {
            reg.note_commit_time(0x100 + lsn, 1);
        }
        assert_eq!(reg.commit_times.len(), COMMIT_TIMES_RETAINED);
        assert!(!reg.commit_times.contains_key(&0x10), "the oldest commit is evicted first");
    }

    /// bead dbsp-ds-g5p, regression 2/2 — per-change cost is `O(candidates)`, not
    /// `O(#subquery shapes)`. `on_table_delta` step 2 visits exactly `outer_candidates`, so
    /// asserting on that set IS asserting shapes-visited. Also pins the two other bucketing
    /// properties: shapes on another outer table are never even considered, and a shape with no
    /// indexable conjunct (a bare `IN`, or a `NOT IN` under a negation) stays an unconditional
    /// candidate — skipping those would be unsound.
    #[tokio::test(flavor = "multi_thread")]
    async fn outer_candidates_do_not_scale_with_shape_count() {
        let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test("http://unused"), None);
        let sig: SubquerySig = "project_members|project_id|L(user_id,Eq,1)".into();
        insert_test_node(&mut reg, &sig);

        const N: i64 = 500;
        for k in 0..N {
            insert_outer_shape(&mut reg, &format!("s{k}"), "issues", keyed_membership_pred(k, &sig));
        }
        // A shape on a DIFFERENT outer table: the `shapes.iter().filter(outer_table == table)`
        // global scan this replaces would have walked past all of these.
        insert_outer_shape(&mut reg, "other", "comments", keyed_membership_pred(7, &sig));
        assert_eq!(reg.shapes.len(), N as usize + 1);

        let cands = reg.outer_candidates(&"issues".into(), &[Tup2(issue(1, 7), 1)]);
        assert_eq!(cands, vec!["s7".to_string()], "1 of {N} shapes visited, not {N}");

        // Un-indexable predicates must stay unconditional candidates.
        insert_outer_shape(
            &mut reg,
            "bare_in",
            "issues",
            CompiledPredicate::InSubquery { col: 1, sig: sig.clone(), negated: false },
        );
        insert_outer_shape(
            &mut reg,
            "not_in",
            "issues",
            CompiledPredicate::Not(Box::new(keyed_membership_pred(7, &sig))),
        );
        let mut cands = reg.outer_candidates(&"issues".into(), &[Tup2(issue(1, 4242), 1)]);
        cands.sort();
        assert_eq!(
            cands,
            vec!["bare_in".to_string(), "not_in".to_string()],
            "a predicate with no necessary conjunct can never be skipped"
        );

        // Dropping a shape un-files it: no stale candidate, and the freed id is safe to re-mint.
        reg.drop_subquery_shape("s7").await;
        assert!(
            reg.outer_candidates(&"issues".into(), &[Tup2(issue(1, 7), 1)]).iter().all(|s| s != "s7"),
            "a dropped shape must leave no index entry behind"
        );
    }

    /// G2 loop test 1/2 — the delete-gate's OPEN-failure half: a delete for a pk that was
    /// **never** a member of the feed must be dropped (zero appends, zero wake), never a
    /// spurious delete. `project_members` (user 1) only ever contains project 100; an issue in
    /// project 999 is written then deleted without ever entering the `issues` shape's feed.
    #[tokio::test(flavor = "multi_thread")]
    async fn never_member_delete_is_dropped() {
        let ts = issues_ts();
        let (ds_url, store) = spawn_fake_ds().await;
        let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test(&ds_url), None);
        let sig: SubquerySig = "project_members|project_id|L(user_id,Eq,1)".into();
        insert_test_node(&mut reg, &sig);
        // The only project user 1 is a member of.
        reg.apply_node_evals(&sig, vec![("pm-1".into(), Some(Value::Int(100)))]).await;
        insert_membership_shape(&mut reg, "s1", &sig, 1);

        // Write an issue in a NON-matching project (999): never a member, so nothing is emitted.
        let work = reg.on_table_delta(&ts, &[Tup2(issue(1, 999), 1)], 1, None, None, None).await.unwrap();
        assert!(work.is_empty(), "an outer-table delta never queues node-flip propagation");
        assert_eq!(reg.shapes["s1"].emitted.load(std::sync::atomic::Ordering::Relaxed), 0);

        // Delete it: still never a member -> the delete-gate must drop it (no spurious wake).
        reg.on_table_delta(&ts, &[Tup2(issue(1, 999), -1)], 2, None, None, None).await.unwrap();
        assert_eq!(
            reg.shapes["s1"].emitted.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "never-member delete must be dropped"
        );
        assert!(
            ops_for(&store, "shape/s1").is_empty(),
            "the shape's stream must receive ZERO appends for a never-member pk"
        );
    }

    /// G2 loop test 2/2 — the delete-gate's CLOSED-failure half: a delete for a pk that WAS a
    /// genuine member must never be dropped, on either exit path — (a) the row itself is
    /// deleted, (b) the row survives but the membership node it depended on flips the pk out
    /// (`project_members`'s row for user 1 is removed: "the user loses the project") — and a pk
    /// that re-enters after leaving must re-emit (the feed relation, not a one-shot latch).
    #[tokio::test(flavor = "multi_thread")]
    async fn genuine_member_delete_is_never_dropped() {
        let ts = issues_ts();
        let (ds_url, store) = spawn_fake_ds().await;
        let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test(&ds_url), None);
        let sig: SubquerySig = "project_members|project_id|L(user_id,Eq,1)".into();
        insert_test_node(&mut reg, &sig);
        reg.apply_node_evals(&sig, vec![("pm-1".into(), Some(Value::Int(100)))]).await;
        insert_membership_shape(&mut reg, "s2", &sig, 1);
        fn emitted(reg: &SubqueryRegistry) -> u64 {
            reg.shapes["s2"].emitted.load(std::sync::atomic::Ordering::Relaxed)
        }

        // Enter: an issue in the matching project.
        reg.on_table_delta(&ts, &[Tup2(issue(1, 100), 1)], 1, None, None, None).await.unwrap();
        assert_eq!(emitted(&reg), 1);
        assert_eq!(ops_for(&store, "shape/s2"), vec!["upsert"]);

        // Exit (a): the row itself is deleted -> exactly one delete emission.
        reg.on_table_delta(&ts, &[Tup2(issue(1, 100), -1)], 2, None, None, None).await.unwrap();
        assert_eq!(emitted(&reg), 2, "exactly one more envelope: the row-delete emission");
        assert_eq!(ops_for(&store, "shape/s2"), vec!["upsert", "delete"]);

        // A pk that re-enters after leaving must re-emit — the feed relation gates on current
        // membership, it is not a one-shot "already told you" latch.
        reg.on_table_delta(&ts, &[Tup2(issue(1, 100), 1)], 3, None, None, None).await.unwrap();
        assert_eq!(emitted(&reg), 3, "a re-entering pk must re-emit");
        assert_eq!(ops_for(&store, "shape/s2"), vec!["upsert", "delete", "upsert"]);

        // Exit (b): the row is untouched, but the user loses the project — the membership node's
        // only contributor is withdrawn, flipping project 100 out of the inner set. In
        // production `propagate_flips`/`move_shape_for_value` discovers this and queries
        // Postgres back for the outer rows with `project_id = 100` to re-evaluate; this harness
        // has no Postgres, so the flip is driven directly and the still-present candidate row is
        // handed to the same `emit_for_shapes` re-derivation tail that query-back would call —
        // exactly the "membership flip" exit path, distinct from a row delete.
        let flips = reg.apply_node_evals(&sig, vec![("pm-1".into(), None)]).await;
        assert_eq!(flips, vec![Flip { value: Value::Int(100), dir: FlipDir::Leave }]);
        let results = reg
            .emit_for_shapes(
                &ts,
                vec![("s2".to_string(), vec![(issue(1, 100), true)])],
                None,
                EmissionSource::Live { lsn: 1, xid: None },
            )
            .await
            .unwrap();
        assert_eq!(
            results,
            vec![("s2".to_string(), true, -1)],
            "exactly one delete emission for the membership-flip exit path"
        );
        assert_eq!(emitted(&reg), 4);
        assert_eq!(ops_for(&store, "shape/s2"), vec!["upsert", "delete", "upsert", "delete"]);
    }

    // --- query-back vs. live ordering (per-pk recency) -----------------------------------------
    //
    // A membership query-back reads its candidate rows from Postgres with the registry lock
    // RELEASED, so a direct outer-row change committed after that read can be evaluated and
    // emitted in between. Without a fence the query-back's older row is evaluated last and
    // becomes the stream's last word — and native consumers fold by durable-stream offset, so
    // "last" is final. These three tests pin the fence: live decisions stamp the pks they touch
    // while a query-back is in flight, and a query-back drops the pks whose stamp its own
    // snapshot could not have seen.

    /// A shape over `project_id = 100 AND project_id IN <node>`, its node containing 100, with a
    /// fake durable-streams server so appends actually land and can be inspected.
    async fn recency_fixture() -> (TableSchema, DsStore, SubqueryRegistry) {
        let ts = issues_ts();
        let (ds_url, store) = spawn_fake_ds().await;
        let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test(&ds_url), None);
        let sig: SubquerySig = "project_members|project_id|L(user_id,Eq,1)".into();
        insert_test_node(&mut reg, &sig);
        reg.apply_node_evals(&sig, vec![("pm-1".into(), Some(Value::Int(100)))]).await;
        insert_outer_shape(&mut reg, "s1", "issues", keyed_membership_pred(100, &sig));
        (ts, store, reg)
    }

    /// (a) With a query-back in flight, a live decision for pk 1 is stamped, and a query-back
    /// holding an OLDER read of that pk drops it — while one whose snapshot already saw the live
    /// commit applies its row as usual.
    ///
    /// The live decision here emits nothing at all (the row moved out of the shape and the feed
    /// never contained it), which is exactly the case an emission-driven fence would miss: the
    /// stale query-back's upsert would be the stream's only word about pk 1, and Postgres says
    /// the row is not in the shape.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stale_query_back_cannot_overwrite_a_newer_live_decision() {
        let (ts, store, mut reg) = recency_fixture().await;

        // A query-back for the flipped value is between its Postgres read and its evaluation.
        reg.begin_queryback("s1");

        // Meanwhile the row moves out of project 100, in a transaction (xid 50) the read below
        // cannot see. Absolute verdict: not a member — and nothing is emitted, because the feed
        // never contained pk 1.
        reg.on_table_delta(&ts, &[Tup2(issue(1, 100), -1), Tup2(issue(1, 999), 1)], 0x200, Some(50), None, None)
            .await
            .unwrap();
        assert!(ops_for(&store, "shape/s1").is_empty(), "a never-member verdict emits nothing");
        let pk_id = reg.pk_dict.get("1").expect("the live decision must record recency for pk 1");
        assert_eq!(
            reg.shapes["s1"].recent.get(&pk_id),
            Some(&(0x200u64, Some(50u64))),
            "the live decision's commit stamp must be recorded while a query-back is in flight"
        );

        // The in-flight read: its snapshot (xmin 40, xmax 45) predates xid 50, so its row for
        // pk 1 — still in project 100 — is stale. It must be dropped, not emitted.
        let stale = crate::pg::SnapshotGate::parse("40:45:", "0/100");
        reg.emit_for_shapes(
            &ts,
            vec![("s1".to_string(), vec![(issue(1, 100), true)])],
            None,
            EmissionSource::QueryBack { gate: &stale, commit_lsn: 0 },
        )
        .await
        .unwrap();
        assert!(
            ops_for(&store, "shape/s1").is_empty(),
            "a query-back row older than the live decision for that pk must be dropped"
        );
        assert_eq!(reg.shapes["s1"].emitted.load(std::sync::atomic::Ordering::Relaxed), 0);

        // A query-back whose snapshot (xmin 60) DID see xid 50 is not stale: whatever it read is
        // at least as new as the live decision (here: the row moved back into project 100), so
        // it applies normally.
        let fresh = crate::pg::SnapshotGate::parse("60:70:", "0/300");
        reg.emit_for_shapes(
            &ts,
            vec![("s1".to_string(), vec![(issue(1, 100), true)])],
            None,
            EmissionSource::QueryBack { gate: &fresh, commit_lsn: 0 },
        )
        .await
        .unwrap();
        assert_eq!(
            ops_for(&store, "shape/s1"),
            vec!["upsert"],
            "a query-back that already saw the live commit must still apply its row"
        );
    }

    /// (b) The map is bounded by the in-flight window: nothing is recorded with no query-back
    /// outstanding, and the last one to finish clears what was recorded.
    #[tokio::test(flavor = "multi_thread")]
    async fn recency_is_recorded_only_while_a_query_back_is_in_flight() {
        let (ts, store, mut reg) = recency_fixture().await;

        // No query-back in flight: an ordinary live delta records nothing.
        reg.on_table_delta(&ts, &[Tup2(issue(1, 100), 1)], 1, Some(10), None, None).await.unwrap();
        assert_eq!(ops_for(&store, "shape/s1"), vec!["upsert"]);
        assert!(reg.shapes["s1"].recent.is_empty(), "with nothing to protect against, no per-pk state is kept");

        // Two query-backs in flight; a live delta now records for the pks it evaluates.
        reg.begin_queryback("s1");
        reg.begin_queryback("s1");
        reg.on_table_delta(&ts, &[Tup2(issue(1, 100), 1)], 2, Some(11), None, None).await.unwrap();
        assert_eq!(reg.shapes["s1"].recent.len(), 1);

        // The first to finish does NOT clear: the other read is still outstanding.
        reg.end_queryback("s1");
        assert_eq!(reg.shapes["s1"].recent.len(), 1, "one query-back is still in flight");

        // The last one does.
        reg.end_queryback("s1");
        assert!(reg.shapes["s1"].recent.is_empty(), "the last query-back out clears the map");
        assert_eq!(reg.shapes["s1"].inflight_querybacks, 0);

        // An unbalanced decrement (a shape whose query-back failed twice, or a dropped shape)
        // must not wrap the counter into a permanently "in flight" state.
        reg.end_queryback("s1");
        assert_eq!(reg.shapes["s1"].inflight_querybacks, 0);
        reg.end_queryback("gone");
    }

    /// (c) The same fence protects a live LEAVE: once the delete has been emitted, a stale
    /// query-back must not resurrect the row with an upsert — the stream's last word for that pk
    /// has to stay the newer verdict.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_live_leave_is_not_undone_by_a_stale_query_back() {
        let (ts, store, mut reg) = recency_fixture().await;
        reg.on_table_delta(&ts, &[Tup2(issue(1, 100), 1)], 1, Some(10), None, None).await.unwrap();
        assert_eq!(ops_for(&store, "shape/s1"), vec!["upsert"]);

        reg.begin_queryback("s1");
        // The row is deleted in a newer transaction (xid 50): the feed retracts, a delete lands.
        reg.on_table_delta(&ts, &[Tup2(issue(1, 100), -1)], 0x200, Some(50), None, None).await.unwrap();
        assert_eq!(ops_for(&store, "shape/s1"), vec!["upsert", "delete"]);

        // The in-flight read still has the row. Applying it would make "upsert" the last word.
        let stale = crate::pg::SnapshotGate::parse("40:45:", "0/100");
        reg.emit_for_shapes(
            &ts,
            vec![("s1".to_string(), vec![(issue(1, 100), true)])],
            None,
            EmissionSource::QueryBack { gate: &stale, commit_lsn: 0 },
        )
        .await
        .unwrap();
        reg.end_queryback("s1");
        assert_eq!(
            ops_for(&store, "shape/s1"),
            vec!["upsert", "delete"],
            "the stale query-back must not resurrect a row the live path just retracted"
        );
    }

    // --- node re-derivation vs. live ordering (per-pk recency, one tier down) ------------------
    //
    // The same race as above, on a membership NODE: `requery_and_reconcile_parent` reads the
    // node's inner rows from Postgres with the registry lock released, so a direct inner-table
    // change committed after that read is reconciled by `on_table_delta` step 1 in between, and
    // the re-derivation would re-assert the old contribution last. The node appends nothing
    // itself, but its dependent shape appends what the node's flips say, so the divergence is the
    // same permanent one.

    /// `t(gid, id)` — the projected value and the pk of a membership node's inner table (columns
    /// are positioned in sorted order, so `gid` is 0 and `id` is 1, matching `insert_test_node`).
    fn inner_ts() -> TableSchema {
        use crate::schema::TableDef;
        let def: TableDef = serde_json::from_value(serde_json::json!({
            "columns": { "gid": {"type":"int"}, "id": {"type":"int"} },
            "primaryKey": "id"
        }))
        .unwrap();
        TableSchema::from_def(&"t".into(), &def).unwrap()
    }

    fn inner_row(gid: i64, id: i64) -> Row {
        Row(vec![Value::Int(gid), Value::Int(id)])
    }

    /// (a) With a re-derivation of the node in flight, the live contribution decisions it races
    /// are stamped per pk, and a re-derivation holding an OLDER read of those pks drops them —
    /// while one whose snapshot already saw the live commit applies its rows as usual.
    ///
    /// pk 2 is the case the conformance schedule hits and an assertion-driven fence would miss:
    /// the node never held it, so the live verdict ("still not a contributor") asserts nothing at
    /// all — and the stale read's row for it would otherwise be admitted as a fresh contribution.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stale_node_requery_cannot_overwrite_a_newer_live_contribution() {
        let ts = inner_ts();
        let sig: SubquerySig = "t|gid|MatchAll".into();
        let mut reg = registry_with_node(&sig);
        reg.apply_node_evals(&sig, vec![("1".into(), Some(Value::Int(100)))]).await;
        assert!(reg.contains(&sig, &Value::Int(100)));

        // A re-derivation of this node is between its Postgres read and its reconcile.
        reg.begin_node_queryback(&sig);

        // Meanwhile both inner rows are deleted in a transaction (xid 50) that read cannot see.
        // Absolute verdict: neither pk contributes — and for pk 2, which the node never held,
        // that verdict asserts nothing.
        reg.on_table_delta(
            &ts,
            &[Tup2(inner_row(100, 1), -1), Tup2(inner_row(200, 2), -1)],
            0x200,
            Some(50),
            None,
            None,
        )
        .await
        .unwrap();
        assert!(!reg.contains(&sig, &Value::Int(100)), "the live delta retracted pk 1");
        assert_eq!(
            reg.nodes[&sig].recent.get("1"),
            Some(&(0x200u64, Some(50u64))),
            "the live decision's commit stamp must be recorded while a re-derivation is in flight"
        );
        assert_eq!(
            reg.nodes[&sig].recent.get("2"),
            Some(&(0x200u64, Some(50u64))),
            "including the pk whose verdict is 'no contribution', which asserts nothing"
        );

        // The in-flight read still has both rows; its snapshot (xmin 40, xmax 45) predates xid 50.
        let stale = crate::pg::SnapshotGate::parse("40:45:", "0/100");
        let evals = reg
            .node_queryback_evals(&sig, &ts, &[inner_row(100, 1), inner_row(200, 2)], &stale)
            .expect("the node is live");
        assert!(evals.is_empty(), "every candidate older than its live decision must be dropped");
        reg.apply_node_evals(&sig, evals).await;
        assert!(!reg.contains(&sig, &Value::Int(100)), "a stale re-derivation must not re-admit a value");
        assert!(!reg.contains(&sig, &Value::Int(200)), "nor admit one the live verdict refused");

        // A read whose snapshot (xmin 60) DID see xid 50 is not stale: whatever it read is at
        // least as new as the live decision (here: both rows are back), so it applies normally.
        let fresh = crate::pg::SnapshotGate::parse("60:70:", "0/300");
        let evals = reg
            .node_queryback_evals(&sig, &ts, &[inner_row(100, 1), inner_row(200, 2)], &fresh)
            .expect("the node is live");
        reg.apply_node_evals(&sig, evals).await;
        assert!(reg.contains(&sig, &Value::Int(100)));
        assert!(reg.contains(&sig, &Value::Int(200)));
        reg.end_node_queryback(&sig);
    }

    /// (b) The node's map is bounded by the in-flight window exactly like the shape's: nothing is
    /// recorded with no re-derivation outstanding, and the last one to finish clears what was.
    #[tokio::test(flavor = "multi_thread")]
    async fn node_recency_is_recorded_only_while_a_requery_is_in_flight() {
        let ts = inner_ts();
        let sig: SubquerySig = "t|gid|MatchAll".into();
        let mut reg = registry_with_node(&sig);

        // Nothing in flight: an ordinary live delta records nothing.
        reg.on_table_delta(&ts, &[Tup2(inner_row(100, 1), 1)], 1, Some(10), None, None).await.unwrap();
        assert!(reg.contains(&sig, &Value::Int(100)));
        assert!(reg.nodes[&sig].recent.is_empty(), "with nothing to protect against, no per-pk state is kept");

        // Two re-derivations in flight; a live delta now records for the pks it decides.
        reg.begin_node_queryback(&sig);
        reg.begin_node_queryback(&sig);
        reg.on_table_delta(&ts, &[Tup2(inner_row(100, 1), 1)], 2, Some(11), None, None).await.unwrap();
        assert_eq!(reg.nodes[&sig].recent.len(), 1);

        // The first to finish does NOT clear: the other read is still outstanding.
        reg.end_node_queryback(&sig);
        assert_eq!(reg.nodes[&sig].recent.len(), 1, "one re-derivation is still in flight");

        // The last one does.
        reg.end_node_queryback(&sig);
        assert!(reg.nodes[&sig].recent.is_empty(), "the last re-derivation out clears the map");
        assert_eq!(reg.nodes[&sig].inflight_querybacks, 0);

        // An unbalanced decrement (a failed read, or a node dropped mid-flight) must not wrap the
        // counter into a permanently "in flight" state.
        reg.end_node_queryback(&sig);
        assert_eq!(reg.nodes[&sig].inflight_querybacks, 0);
        reg.end_node_queryback(&"gone".to_string());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn registry_eval_reads_node_sets() {
        let sig: SubquerySig = "sig".into();
        let mut reg = registry_with_node(&sig);
        reg.apply_node_evals(&sig, vec![("a".into(), Some(Value::Int(1))), ("b".into(), Some(Value::Null))]).await;
        assert!(reg.contains(&sig, &Value::Int(1)));
        assert!(!reg.contains(&sig, &Value::Int(2)));
        assert!(reg.has_null(&sig));
        // unknown sig -> empty
        assert!(!reg.contains(&"other".to_string(), &Value::Int(1)));
    }

    /// The equality literals of an inner predicate are lifted into its template's bind, and a
    /// replicated inner row finds its node by looking its own cells up among the binds. For a
    /// timestamp column the bind therefore has to be in the cells' spelling — Postgres's output for
    /// that instant — or no row ever matches it and the inner set stays empty.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_timestamp_bind_is_spelled_as_the_cells_it_is_matched_against() {
        use crate::schema::TableDef;
        let mut def: TableDef = serde_json::from_value(serde_json::json!({
            "columns": { "id": {"type":"int"}, "gid": {"type":"int"}, "at": {"type":"text"} }, "primaryKey": "id"
        }))
        .unwrap();
        def.columns.get_mut("at").unwrap().pg_type = Some("timestamptz".into());
        let slots = TableSchema::from_def(&TableRef::from("slots"), &def).unwrap();
        let mut schemas = creating_schemas();
        schemas.insert(TableRef::from("slots"), slots.clone());
        let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test("http://unused"), None);
        reg.set_schemas(Arc::new(schemas));
        let pred: PredicateJson = serde_json::from_value(serde_json::json!({
            "col": "gid",
            "in": {"table": "slots", "project": "gid", "where": {"col": "at", "op": "eq", "value": "2026-10-19T00:00:00-07:00"}}
        }))
        .unwrap();
        let begin = reg.begin_create("s1", &"outer_t".into(), "shape/s1", &pred, None, true).unwrap();
        let seeds = begin
            .seeds
            .into_iter()
            .map(|(sig, _, _)| (sig, Vec::new(), crate::pg::SnapshotGate::passthrough()))
            .collect();
        reg.finish_create("s1", seeds, crate::pg::SnapshotGate::passthrough(), 0, Default::default()).await.unwrap();
        let node = reg.nodes.values().next().expect("the inner set");
        assert_eq!(node.bind, Row(vec![Value::Text("2026-10-19 07:00:00+00".into())]));

        // A slot at that instant, as replication hands it over, puts its `gid` into the inner set.
        let slot = slots
            .row_from_json(serde_json::json!({"id": 1, "gid": 7, "at": "2026-10-19 07:00:00+00"}).as_object().unwrap())
            .unwrap();
        let flips = reg.on_table_delta(&slots, &[Tup2(slot, 1)], 0x10, None, None, None).await.unwrap();
        let entered: Vec<Flip> = flips.into_iter().map(|(_, flip)| flip).collect();
        assert_eq!(entered, vec![Flip { value: Value::Int(7), dir: FlipDir::Enter }]);
    }

    // --- changes-only feeds: removals of rows the feed never delivered -------------------------
    //
    // A native subset load is two requests: a changes-only feed (`POST /v1/subset-feeds`) and a
    // snapshot query. The feed skips its backfill, so `finish_create` installs it with no seeded
    // pks and its key set starts EMPTY — while the client holds every row the snapshot returned.
    // The feed still owes that client each row that leaves the scope after it opened. Opening the
    // delete gate only for a pk the feed itself delivered drops every one of them. Observed
    // against the gateway on 2026-11-16: 10 soft-deleted events and 12 re-dated Records stayed on
    // the client, in the events' own scope and in the two scopes that reach a Record's date
    // through a subquery, and the streams carried upserts only.
    //
    // `insert_changes_only_feed` installs a shape exactly as a changes-only create does: a
    // passthrough gate, nothing emitted, an empty key set.

    /// `events(id, record_id, starts, deleted_at)`: an app calendar scope in miniature — a window
    /// on a row-local column, a soft-delete column, and an access check that is a subquery.
    fn events_ts() -> TableSchema {
        use crate::schema::TableDef;
        let def: TableDef = serde_json::from_value(serde_json::json!({
            "columns": {
                "id": {"type":"int"}, "record_id": {"type":"int"}, "starts": {"type":"int"},
                "deleted_at": {"type":"int"}
            },
            "primaryKey": "id"
        }))
        .unwrap();
        TableSchema::from_def(&TableRef::from("events"), &def).unwrap()
    }

    fn event(ts: &TableSchema, id: i64, record_id: i64, starts: i64, deleted_at: Option<i64>) -> Row {
        ts.row_from_json(
            serde_json::json!({"id": id, "record_id": record_id, "starts": starts, "deleted_at": deleted_at})
                .as_object()
                .unwrap(),
        )
        .unwrap()
    }

    /// `starts >= lo AND starts < hi AND deleted_at IS NULL AND record_id IN <visible records>`.
    fn window_pred(ts: &TableSchema, lo: i64, hi: i64, visible: &SubquerySig) -> CompiledPredicate {
        use crate::predicate::LeafOp;
        let starts = ts.column_index("starts").unwrap();
        CompiledPredicate::And(vec![
            CompiledPredicate::Cmp { col: starts, op: LeafOp::Gte, value: Value::Int(lo) },
            CompiledPredicate::Cmp { col: starts, op: LeafOp::Lt, value: Value::Int(hi) },
            CompiledPredicate::IsNull { col: ts.column_index("deleted_at").unwrap(), is_null: true },
            CompiledPredicate::InSubquery {
                col: ts.column_index("record_id").unwrap(),
                sig: visible.clone(),
                negated: false,
            },
        ])
    }

    /// Two changes-only feeds over `events` — week one `[10, 20)` and week two `[20, 30)` — for a
    /// caller who can see Records 7 and 8. Neither feed has delivered anything.
    async fn two_week_feeds() -> (TableSchema, DsStore, SubqueryRegistry, SubquerySig) {
        let ts = events_ts();
        let (ds_url, store) = spawn_fake_ds().await;
        let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test(&ds_url), None);
        let visible: SubquerySig = "visible_records".into();
        insert_test_node(&mut reg, &visible);
        reg.apply_node_evals(
            &visible,
            vec![("grant-7".into(), Some(Value::Int(7))), ("grant-8".into(), Some(Value::Int(8)))],
        )
        .await;
        let record_col = ts.column_index("record_id").unwrap();
        for (id, lo, hi) in [("week1", 10, 20), ("week2", 20, 30)] {
            insert_changes_only_feed(&mut reg, id, "events", window_pred(&ts, lo, hi, &visible));
            reg.add_edge(flip_edge(&visible, Dependent::Shape(id.to_string()), record_col));
        }
        reg.set_schemas(Arc::new([(TableRef::from("events"), ts.clone())].into_iter().collect()));
        (ts, store, reg, visible)
    }

    fn envelope_of(ts: &TableSchema, row: &Row, operation: &str) -> (String, String, String) {
        (TableRef::from("events").to_string(), ts.key_string(row).unwrap(), operation.to_string())
    }

    /// Soft delete: the row is still in Postgres, and its own `deleted_at` takes it out of the
    /// scope. The client has it from the snapshot; the feed has never sent it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_changes_only_feed_removes_a_snapshot_row_that_is_soft_deleted() {
        let (ts, store, mut reg, _) = two_week_feeds().await;
        let (before, after) = (event(&ts, 1, 7, 12, None), event(&ts, 1, 7, 12, Some(99)));

        reg.on_table_delta(&ts, &[Tup2(before.clone(), -1), Tup2(after.clone(), 1)], 1, None, None, None)
            .await
            .unwrap();
        assert_eq!(
            envs_for(&store, "shape/week1"),
            vec![envelope_of(&ts, &before, "delete")],
            "the row was in week one until this commit, so the feed owes its removal"
        );
        assert!(envs_for(&store, "shape/week2").is_empty(), "week two never held it");

        // A later change to the already-removed row says nothing more.
        reg.on_table_delta(&ts, &[Tup2(after, -1), Tup2(event(&ts, 1, 7, 13, Some(99)), 1)], 2, None, None, None)
            .await
            .unwrap();
        assert_eq!(envs_for(&store, "shape/week1").len(), 1, "one removal, not one per later change");
    }

    /// A re-dated row leaves one week and enters the next: the removal and the arrival both land.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_changes_only_feed_removes_a_snapshot_row_that_moves_to_another_window() {
        let (ts, store, mut reg, _) = two_week_feeds().await;
        let (before, after) = (event(&ts, 1, 7, 12, None), event(&ts, 1, 7, 22, None));

        reg.on_table_delta(&ts, &[Tup2(before.clone(), -1), Tup2(after.clone(), 1)], 1, None, None, None)
            .await
            .unwrap();
        assert_eq!(envs_for(&store, "shape/week1"), vec![envelope_of(&ts, &before, "delete")], "it left week one");
        assert_eq!(envs_for(&store, "shape/week2"), vec![envelope_of(&ts, &after, "upsert")], "and entered week two");
    }

    /// A hard delete carries only the row's old image.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_changes_only_feed_removes_a_snapshot_row_that_is_deleted() {
        let (ts, store, mut reg, _) = two_week_feeds().await;
        let row = event(&ts, 1, 7, 12, None);

        reg.on_table_delta(&ts, &[Tup2(row.clone(), -1)], 1, None, None, None).await.unwrap();
        assert_eq!(envs_for(&store, "shape/week1"), vec![envelope_of(&ts, &row, "delete")]);
        assert!(envs_for(&store, "shape/week2").is_empty());
    }

    /// The row itself is untouched: the caller loses access to Record 7, so the value leaves the
    /// inner set and the flip's query-back finds the rows that depended on it. Each one that was
    /// in a scope until this flip is removed from that scope — and nothing else is: not the
    /// soft-deleted row, not the row outside both windows, not the rows of Record 8.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_changes_only_feed_removes_snapshot_rows_when_their_access_is_withdrawn() {
        let (ts, store, mut reg, visible) = two_week_feeds().await;
        let in_week1 = event(&ts, 1, 7, 12, None);
        let in_week2 = event(&ts, 2, 7, 22, None);
        let fake = Arc::new(FakeCandidates::with_rows([(
            "events",
            vec![
                in_week1.clone(),
                in_week2.clone(),
                event(&ts, 3, 7, 12, Some(99)), // Record 7, week one, already soft-deleted
                event(&ts, 4, 8, 13, None),     // Record 8: still visible
                event(&ts, 5, 7, 40, None),     // Record 7, outside both windows
            ],
        )]));
        reg.fake_candidates = Some(fake.clone());

        let flips = reg.apply_node_evals(&visible, vec![("grant-7".into(), None)]).await;
        assert_eq!(flips, vec![Flip { value: Value::Int(7), dir: FlipDir::Leave }]);
        let registry = tokio::sync::Mutex::new(reg);
        let (trace_tx, _rx) = tokio::sync::broadcast::channel(16);
        let mut work: VecDeque<(SubquerySig, Flip)> = flips.into_iter().map(|f| (visible.clone(), f)).collect();
        propagate_with_retry(&registry, &mut work, None, None, &trace_tx).await.unwrap();

        assert_eq!(envs_for(&store, "shape/week1"), vec![envelope_of(&ts, &in_week1, "delete")]);
        assert_eq!(envs_for(&store, "shape/week2"), vec![envelope_of(&ts, &in_week2, "delete")]);
    }

    /// The same through a `NOT IN`: there a value ENTERING the inner set is what takes rows out.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_changes_only_feed_removes_snapshot_rows_when_a_not_in_set_gains_their_value() {
        let ts = events_ts();
        let (ds_url, store) = spawn_fake_ds().await;
        let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test(&ds_url), None);
        let hidden: SubquerySig = "hidden_records".into();
        insert_test_node(&mut reg, &hidden);
        let record_col = ts.column_index("record_id").unwrap();
        insert_changes_only_feed(
            &mut reg,
            "unhidden",
            "events",
            CompiledPredicate::InSubquery { col: record_col, sig: hidden.clone(), negated: true },
        );
        reg.add_edge(null_sensitive_edge(&hidden, Dependent::Shape("unhidden".into()), record_col));
        reg.set_schemas(Arc::new([(TableRef::from("events"), ts.clone())].into_iter().collect()));
        let (leaving, staying) = (event(&ts, 1, 7, 12, None), event(&ts, 2, 8, 12, None));
        reg.fake_candidates = Some(Arc::new(FakeCandidates::with_rows([("events", vec![leaving.clone(), staying])])));

        let flips = reg.apply_node_evals(&hidden, vec![("hide-7".into(), Some(Value::Int(7)))]).await;
        assert_eq!(flips, vec![Flip { value: Value::Int(7), dir: FlipDir::Enter }]);
        let registry = tokio::sync::Mutex::new(reg);
        let (trace_tx, _rx) = tokio::sync::broadcast::channel(16);
        let mut work: VecDeque<(SubquerySig, Flip)> = flips.into_iter().map(|f| (hidden.clone(), f)).collect();
        propagate_with_retry(&registry, &mut work, None, None, &trace_tx).await.unwrap();

        assert_eq!(envs_for(&store, "shape/unhidden"), vec![envelope_of(&ts, &leaving, "delete")]);
    }

    /// The other half of the gate, which must keep holding: a row that was NOT in a scope just
    /// before the change gets no delete there, however it changes. This is what stops one busy
    /// table from waking every idle feed on it (the wake storm the gate exists for).
    #[tokio::test(flavor = "multi_thread")]
    async fn a_changes_only_feed_sends_nothing_for_rows_that_were_not_in_its_scope() {
        let (ts, store, mut reg, visible) = two_week_feeds().await;
        let mut lsn = 0;
        for (before, after) in [
            // Outside both windows, before and after.
            (Some(event(&ts, 1, 7, 40, None)), Some(event(&ts, 1, 7, 41, None))),
            // In week one's window, but a Record the caller cannot see.
            (Some(event(&ts, 2, 9, 12, None)), Some(event(&ts, 2, 9, 12, Some(99)))),
            // Already soft-deleted, then edited and finally deleted.
            (Some(event(&ts, 3, 7, 12, Some(50))), Some(event(&ts, 3, 7, 13, Some(50)))),
            (Some(event(&ts, 3, 7, 13, Some(50))), None),
            // An insert that matches nothing.
            (None, Some(event(&ts, 4, 7, 99, None))),
        ] {
            lsn += 1;
            let delta: Vec<Tup2<Row, ZWeight>> =
                before.into_iter().map(|r| Tup2(r, -1)).chain(after.into_iter().map(|r| Tup2(r, 1))).collect();
            reg.on_table_delta(&ts, &delta, lsn, None, None, None).await.unwrap();
        }
        // The caller gains a Record with no rows in either window, and loses one it never had.
        reg.fake_candidates =
            Some(Arc::new(FakeCandidates::with_rows([("events", vec![event(&ts, 2, 9, 12, Some(99))])])));
        let mut flips = reg.apply_node_evals(&visible, vec![("grant-9".into(), Some(Value::Int(9)))]).await;
        flips.extend(reg.apply_node_evals(&visible, vec![("grant-9".into(), None)]).await);
        let registry = tokio::sync::Mutex::new(reg);
        let (trace_tx, _rx) = tokio::sync::broadcast::channel(16);
        let mut work: VecDeque<(SubquerySig, Flip)> = flips.into_iter().map(|f| (visible.clone(), f)).collect();
        propagate_with_retry(&registry, &mut work, None, None, &trace_tx).await.unwrap();

        for week in ["week1", "week2"] {
            assert!(
                envs_for(&store, &format!("shape/{week}")).is_empty(),
                "{week} must stay silent, got {:?}",
                envs_for(&store, &format!("shape/{week}"))
            );
        }
    }

    /// A row the feed DID deliver is removed exactly once, by whichever rule sees it first.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_changes_only_feed_removes_a_delivered_row_exactly_once() {
        let (ts, store, mut reg, _) = two_week_feeds().await;
        let (live, gone) = (event(&ts, 1, 7, 12, None), event(&ts, 1, 7, 12, Some(99)));

        reg.on_table_delta(&ts, &[Tup2(live.clone(), 1)], 1, None, None, None).await.unwrap();
        reg.on_table_delta(&ts, &[Tup2(live.clone(), -1), Tup2(gone.clone(), 1)], 2, None, None, None).await.unwrap();
        reg.on_table_delta(&ts, &[Tup2(gone, -1)], 3, None, None, None).await.unwrap();
        assert_eq!(
            envs_for(&store, "shape/week1"),
            vec![envelope_of(&ts, &live, "upsert"), envelope_of(&ts, &live, "delete")]
        );
    }

    // The cases above have one cause each. A transaction often has two: the app removes a Record's
    // date row and soft-deletes the Record, or withdraws a grant and a schedule together. Each
    // change is then judged against inner sets the SAME commit has already moved, so the proof has
    // to look past all of that commit's transitions — and the removal must still be said once.

    /// `grants(id, gid)` / `schedule(id, gid)`: inner tables whose `gid` is a Record id. Their
    /// nodes are driven through `on_table_delta`, as replication drives them.
    fn record_link(ts: &TableSchema, id: i64, record_id: i64) -> Row {
        id_gid_row(ts, id, record_id)
    }

    /// One changes-only feed over `events` whose rows need BOTH a grant and a schedule entry for
    /// their Record: `record_id IN <granted> AND record_id IN <scheduled>`. Record 7 has both.
    async fn granted_and_scheduled_feed()
    -> (TableSchema, TableSchema, TableSchema, DsStore, SubqueryRegistry, SubquerySig, SubquerySig) {
        let events = events_ts();
        let (grants, schedule) = (id_gid_ts("grants"), id_gid_ts("schedule"));
        let (ds_url, store) = spawn_fake_ds().await;
        let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test(&ds_url), None);
        let (granted, scheduled): (SubquerySig, SubquerySig) = ("granted".into(), "scheduled".into());
        let gid = grants.column_index("gid").unwrap();
        insert_test_node_on(&mut reg, &granted, "grants", gid);
        insert_test_node_on(&mut reg, &scheduled, "schedule", gid);
        let record_col = events.column_index("record_id").unwrap();
        insert_changes_only_feed(
            &mut reg,
            "feed",
            "events",
            CompiledPredicate::And(vec![
                CompiledPredicate::InSubquery { col: record_col, sig: granted.clone(), negated: false },
                CompiledPredicate::InSubquery { col: record_col, sig: scheduled.clone(), negated: false },
            ]),
        );
        for sig in [&granted, &scheduled] {
            reg.add_edge(flip_edge(sig, Dependent::Shape("feed".into()), record_col));
        }
        reg.set_schemas(Arc::new(
            [("events", events.clone()), ("grants", grants.clone()), ("schedule", schedule.clone())]
                .into_iter()
                .map(|(name, ts)| (TableRef::from(name), ts))
                .collect(),
        ));
        reg.on_table_delta(&grants, &[Tup2(record_link(&grants, 1, 7), 1)], 1, None, None, None).await.unwrap();
        reg.on_table_delta(&schedule, &[Tup2(record_link(&schedule, 1, 7), 1)], 2, None, None, None).await.unwrap();
        assert!(envs_for(&store, "shape/feed").is_empty(), "setup delivers nothing: no outer row has changed");
        (events, grants, schedule, store, reg, granted, scheduled)
    }

    async fn propagate(
        registry: &tokio::sync::Mutex<SubqueryRegistry>,
        work: VecDeque<(SubquerySig, Flip)>,
        lsn: &str,
    ) {
        let (trace_tx, _rx) = tokio::sync::broadcast::channel(16);
        let mut work = work;
        propagate_with_retry(registry, &mut work, None, Some(lsn.to_string()), &trace_tx).await.unwrap();
    }

    /// One commit withdraws the grant and THEN edits the event. By the time the event's change is
    /// evaluated the grant is already gone, so its old image only counts as a member if the
    /// commit's own transition is looked past. The flip's query-back can prove the same departure
    /// a second time, and must not say it again.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_changes_only_feed_removes_a_snapshot_row_changed_after_its_inner_value_left_in_the_same_commit() {
        let (events, grants, _, store, mut reg, _, _) = granted_and_scheduled_feed().await;
        let (before, after) = (event(&events, 1, 7, 12, None), event(&events, 1, 7, 13, None));

        let work =
            reg.on_table_delta(&grants, &[Tup2(record_link(&grants, 1, 7), -1)], 0x10, None, None, None).await.unwrap();
        assert_eq!(work.len(), 1, "the grant's withdrawal flips Record 7 out of the granted set");
        reg.on_table_delta(&events, &[Tup2(before.clone(), -1), Tup2(after.clone(), 1)], 0x10, None, None, None)
            .await
            .unwrap();
        assert_eq!(envs_for(&store, "shape/feed"), vec![envelope_of(&events, &before, "delete")]);

        reg.fake_candidates = Some(Arc::new(FakeCandidates::with_rows([("events", vec![after])])));
        let registry = tokio::sync::Mutex::new(reg);
        propagate(&registry, work, "0/10").await;
        assert_eq!(envs_for(&store, "shape/feed").len(), 1, "the flip's query-back must not repeat the removal");
    }

    /// One commit withdraws the grant AND the schedule entry; the event row is untouched. Either
    /// flip alone, looked past on its own, still finds the other set without Record 7 — only the
    /// commit's transitions together show the row was a member. Two flips, one removal.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_changes_only_feed_removes_a_snapshot_row_once_when_one_commit_empties_both_inner_sets() {
        let (events, grants, schedule, store, mut reg, _, _) = granted_and_scheduled_feed().await;
        let row = event(&events, 1, 7, 12, None);
        reg.fake_candidates = Some(Arc::new(FakeCandidates::with_rows([("events", vec![row.clone()])])));

        let mut work =
            reg.on_table_delta(&grants, &[Tup2(record_link(&grants, 1, 7), -1)], 0x10, None, None, None).await.unwrap();
        work.extend(
            reg.on_table_delta(&schedule, &[Tup2(record_link(&schedule, 1, 7), -1)], 0x10, None, None, None)
                .await
                .unwrap(),
        );
        assert_eq!(work.len(), 2, "both inner sets lose Record 7");
        let registry = tokio::sync::Mutex::new(reg);
        propagate(&registry, work, "0/10").await;

        assert_eq!(envs_for(&store, "shape/feed"), vec![envelope_of(&events, &row, "delete")]);
    }

    /// A row the feed DID deliver, in a commit that withdraws its grant and then renames it: the
    /// key set removes it at the rename, and the flip's query-back — which can also prove the row
    /// was a member before the commit — must not say so again.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_changes_only_feed_removes_a_delivered_row_once_when_its_commit_proves_it_twice() {
        let (events, grants, _, store, mut reg, _, _) = granted_and_scheduled_feed().await;
        let (delivered, renamed) = (event(&events, 1, 7, 12, None), event(&events, 1, 7, 13, None));
        reg.on_table_delta(&events, &[Tup2(delivered.clone(), 1)], 3, None, None, None).await.unwrap();

        let work =
            reg.on_table_delta(&grants, &[Tup2(record_link(&grants, 1, 7), -1)], 0x10, None, None, None).await.unwrap();
        reg.on_table_delta(&events, &[Tup2(delivered.clone(), -1), Tup2(renamed.clone(), 1)], 0x10, None, None, None)
            .await
            .unwrap();
        reg.fake_candidates = Some(Arc::new(FakeCandidates::with_rows([("events", vec![renamed])])));
        let registry = tokio::sync::Mutex::new(reg);
        propagate(&registry, work, "0/10").await;

        assert_eq!(
            envs_for(&store, "shape/feed"),
            vec![envelope_of(&events, &delivered, "upsert"), envelope_of(&events, &delivered, "delete")]
        );
    }

    /// The commit's record ends with the commit: the next one starts clean, so a row that comes
    /// back and leaves again in a later commit is removed again.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_changes_only_feed_removes_a_row_again_after_it_returns_in_a_later_commit() {
        let (ts, store, mut reg, _) = two_week_feeds().await;
        let (inside, outside) = (event(&ts, 1, 7, 12, None), event(&ts, 1, 7, 40, None));

        reg.on_table_delta(&ts, &[Tup2(inside.clone(), -1), Tup2(outside.clone(), 1)], 0x10, None, None, None)
            .await
            .unwrap();
        reg.on_table_delta(&ts, &[Tup2(outside.clone(), -1), Tup2(inside.clone(), 1)], 0x20, None, None, None)
            .await
            .unwrap();
        reg.on_table_delta(&ts, &[Tup2(inside.clone(), -1), Tup2(outside, 1)], 0x30, None, None, None).await.unwrap();
        assert_eq!(
            envs_for(&store, "shape/week1"),
            vec![
                envelope_of(&ts, &inside, "delete"),
                envelope_of(&ts, &inside, "upsert"),
                envelope_of(&ts, &inside, "delete"),
            ]
        );
    }

    /// A materialized shape is untouched by all of this: its key set, seeded with the backfill, is
    /// already what its client holds, so a row it never held still gets no delete — exactly the
    /// existing gate.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_materialized_shape_still_gates_on_its_key_set_alone() {
        let ts = events_ts();
        let (ds_url, store) = spawn_fake_ds().await;
        let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test(&ds_url), None);
        let visible: SubquerySig = "visible_records".into();
        insert_test_node(&mut reg, &visible);
        reg.apply_node_evals(&visible, vec![("grant-7".into(), Some(Value::Int(7)))]).await;
        insert_outer_shape(&mut reg, "materialized", "events", window_pred(&ts, 10, 20, &visible));
        let (before, after) = (event(&ts, 1, 7, 12, None), event(&ts, 1, 7, 12, Some(99)));

        reg.on_table_delta(&ts, &[Tup2(before, -1), Tup2(after, 1)], 1, None, None, None).await.unwrap();
        assert!(envs_for(&store, "shape/materialized").is_empty());
    }

    // The proof judges a row against the inner sets as they stood BEFORE the commit. Two things
    // decide whether that view is the true one: every transition of the commit has to be in it,
    // the nested ones included, and it has to outrank the single flip being propagated.

    /// `<name>(id, gid)` tables for a scope behind two NESTED inner sets: an event's Record (its
    /// `gid`) must be linked in `links1` AND in `links2`, and a link counts only while its id is
    /// granted in `grants1` / `grants2`.
    fn two_link_schemas() -> HashMap<TableRef, TableSchema> {
        ["events", "links1", "links2", "grants1", "grants2"]
            .into_iter()
            .map(|name| (TableRef::from(name), id_gid_ts(name)))
            .collect()
    }

    /// `gid IN (SELECT gid FROM links<n> WHERE id IN (SELECT gid FROM grants<n>))`.
    fn linked_through(n: u8) -> serde_json::Value {
        serde_json::json!({"col": "gid", "in": {"table": format!("links{n}"), "project": "gid",
            "where": {"col": "id", "in": {"table": format!("grants{n}"), "project": "gid"}}}})
    }

    /// One commit withdraws the grant behind BOTH links, and the event row is untouched. The
    /// grants are the inner sets the commit's changes touch directly; the links are parent sets,
    /// re-derived later by the flips' walk. Each parent's flip, looked past on its own, still
    /// finds the other parent without Record 7, so the row is seen as a member before the commit
    /// only if the parents' transitions are part of the commit's record too. Two flips, one removal.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_changes_only_feed_removes_a_snapshot_row_once_when_one_commit_empties_two_nested_inner_sets() {
        let schemas = two_link_schemas();
        let table = |name: &str| schemas[&TableRef::from(name)].clone();
        let (ds_url, store) = spawn_fake_ds().await;
        let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test(&ds_url), None);
        reg.set_schemas(Arc::new(schemas.clone()));
        let pred: PredicateJson =
            serde_json::from_value(serde_json::json!({"and": [linked_through(1), linked_through(2)]})).unwrap();
        let begin = reg.begin_create("feed", &TableRef::from("events"), "shape/feed", &pred, None, true).unwrap();
        assert_eq!(begin.seeds.len(), 4, "two link sets, each over its own grant set");
        // Link 5 ties Record 7 in both tables, and grant 1 grants link 5 in both.
        let seeds = begin
            .seeds
            .iter()
            .map(|(sig, inner, _)| {
                let ts = &schemas[inner];
                let granting = [TableRef::from("grants1"), TableRef::from("grants2")].contains(inner);
                let row = if granting { id_gid_row(ts, 1, 5) } else { id_gid_row(ts, 5, 7) };
                (sig.clone(), vec![row], crate::pg::SnapshotGate::passthrough())
            })
            .collect();
        reg.finish_create("feed", seeds, crate::pg::SnapshotGate::passthrough(), 0, Default::default()).await.unwrap();
        let row = id_gid_row(&table("events"), 1, 7);
        reg.fake_candidates = Some(Arc::new(FakeCandidates::with_rows([
            ("links1", vec![id_gid_row(&table("links1"), 5, 7)]),
            ("links2", vec![id_gid_row(&table("links2"), 5, 7)]),
            ("events", vec![row.clone()]),
        ])));

        let (grants1, grants2) = (table("grants1"), table("grants2"));
        let mut work = reg
            .on_table_delta(&grants1, &[Tup2(id_gid_row(&grants1, 1, 5), -1)], 0x10, None, None, None)
            .await
            .unwrap();
        work.extend(
            reg.on_table_delta(&grants2, &[Tup2(id_gid_row(&grants2, 1, 5), -1)], 0x10, None, None, None)
                .await
                .unwrap(),
        );
        assert_eq!(work.len(), 2, "both grant sets lose link 5");
        let registry = tokio::sync::Mutex::new(reg);
        propagate(&registry, work, "0/10").await;

        let reg = registry.lock().await;
        assert!(
            reg.nodes.values().all(|node| !reg.contains(&node.sig, &Value::Int(7))),
            "both link sets lost Record 7"
        );
        assert_eq!(
            envs_for(&store, "shape/feed"),
            vec![(TableRef::from("events").to_string(), "1".to_string(), "delete".to_string())]
        );
    }

    /// A value that enters an inner set and leaves it again inside one commit was not in the set
    /// before the commit and is not in it after. A row that depends on it was never in the scope
    /// at any commit boundary, so the client never held it and the feed owes it nothing: the
    /// commit's record (the value was absent before) outranks the flip being propagated (it left).
    #[tokio::test(flavor = "multi_thread")]
    async fn a_changes_only_feed_sends_nothing_when_a_value_enters_and_leaves_within_one_commit() {
        let (events, grants, schedule, store, mut reg, _, _) = granted_and_scheduled_feed().await;
        // Record 9 is scheduled but not granted, so its event is outside the scope.
        reg.on_table_delta(&schedule, &[Tup2(record_link(&schedule, 2, 9), 1)], 3, None, None, None).await.unwrap();
        let outside = event(&events, 1, 9, 12, None);
        reg.fake_candidates = Some(Arc::new(FakeCandidates::with_rows([("events", vec![outside])])));

        let mut work =
            reg.on_table_delta(&grants, &[Tup2(record_link(&grants, 2, 9), 1)], 0x10, None, None, None).await.unwrap();
        work.extend(
            reg.on_table_delta(&grants, &[Tup2(record_link(&grants, 2, 9), -1)], 0x10, None, None, None).await.unwrap(),
        );
        assert_eq!(work.len(), 2, "the grant's arrival and its withdrawal are two flips");
        let registry = tokio::sync::Mutex::new(reg);
        propagate(&registry, work, "0/10").await;

        assert!(
            envs_for(&store, "shape/feed").is_empty(),
            "the row was outside the scope before the commit and after it, got {:?}",
            envs_for(&store, "shape/feed")
        );
    }

    // The commit's record is owned heap for as long as that commit is the one being ingested, and
    // one transaction can flip any number of inner values. So it is counted, and it is bounded.

    /// `grants(id, name)` and `count` of its rows: an inner table whose projected value is text,
    /// so a test can make the values of one commit as large as it needs (`bytes` each).
    fn named_grants(count: usize, bytes: usize) -> (TableSchema, Vec<Row>) {
        use crate::schema::TableDef;
        let def: TableDef = serde_json::from_value(serde_json::json!({
            "columns": { "id": {"type":"int"}, "name": {"type":"text"} }, "primaryKey": "id"
        }))
        .unwrap();
        let ts = TableSchema::from_def(&TableRef::from("grants"), &def).unwrap();
        let rows = (0..count)
            .map(|i| {
                let name = format!("{i:08}{}", "x".repeat(bytes - 8));
                ts.row_from_json(serde_json::json!({"id": i, "name": name}).as_object().unwrap()).unwrap()
            })
            .collect();
        (ts, rows)
    }

    /// A registry with one inner set, `granted`, over [`named_grants`]' table.
    fn registry_with_named_grants(ds_url: &str, ts: &TableSchema) -> SubqueryRegistry {
        let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test(ds_url), None);
        insert_test_node_on(&mut reg, "granted", "grants", ts.column_index("name").unwrap());
        reg
    }

    /// What the registry's heap accounting attributes to the commit being ingested: its total now,
    /// less its total once the next commit's first change has replaced the record.
    async fn counted_for_the_commit(reg: &mut SubqueryRegistry, ts: &TableSchema, next_lsn: u64) -> usize {
        let during = reg.heap_bytes();
        reg.on_table_delta(ts, &[], next_lsn, None, None, None).await.unwrap();
        during.saturating_sub(reg.heap_bytes())
    }

    /// The registry's heap accounting (`/memory`) includes the record of the commit being ingested.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_commits_proof_record_is_counted_in_the_registrys_heap() {
        const VALUE_BYTES: usize = 1024;
        let (ts, rows) = named_grants(100, VALUE_BYTES);
        let mut reg = registry_with_named_grants("http://unused", &ts);
        reg.set_schemas(Arc::new([(TableRef::from("grants"), ts.clone())].into_iter().collect()));
        let delta: Vec<Tup2<Row, ZWeight>> = rows.into_iter().map(|row| Tup2(row, 1)).collect();
        let flips = reg.on_table_delta(&ts, &delta, 0x10, None, None, None).await.unwrap();
        assert_eq!(flips.len(), 100, "every grant is a transition the commit's record keeps");

        let counted = counted_for_the_commit(&mut reg, &ts, 0x20).await;
        assert!(counted >= 100 * VALUE_BYTES, "100 transitions of {VALUE_BYTES} bytes each, counted {counted} bytes");
    }

    /// A commit whose record would outgrow [`COMMIT_PROOF_MAX_BYTES`] does not keep it: the record
    /// is given up for the rest of that commit, and nothing more is recorded. What is lost is the
    /// view of the commit's OTHER transitions, never the proof itself: each flip is then judged on
    /// its own, as it is for an earlier commit, so a row with one cause is still removed.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_commits_proof_record_stays_inside_its_budget_and_then_judges_one_cause_at_a_time() {
        use crate::schema::TableDef;
        const VALUE_BYTES: usize = 512 * 1024;
        let (grants, rows) = named_grants(COMMIT_PROOF_MAX_BYTES / VALUE_BYTES + 4, VALUE_BYTES);
        let def: TableDef = serde_json::from_value(serde_json::json!({
            "columns": { "id": {"type":"int"}, "label": {"type":"text"} }, "primaryKey": "id"
        }))
        .unwrap();
        let events = TableSchema::from_def(&TableRef::from("events"), &def).unwrap();
        let (ds_url, store) = spawn_fake_ds().await;
        let mut reg = registry_with_named_grants(&ds_url, &grants);
        let label = events.column_index("label").unwrap();
        let granted: SubquerySig = "granted".into();
        insert_changes_only_feed(
            &mut reg,
            "feed",
            "events",
            CompiledPredicate::InSubquery { col: label, sig: granted.clone(), negated: false },
        );
        reg.add_edge(flip_edge(&granted, Dependent::Shape("feed".into()), label));
        reg.set_schemas(Arc::new(
            [(TableRef::from("grants"), grants.clone()), (TableRef::from("events"), events.clone())]
                .into_iter()
                .collect(),
        ));

        // One commit grants every name: more transitions than the budget holds.
        let grant: Vec<Tup2<Row, ZWeight>> = rows.iter().cloned().map(|row| Tup2(row, 1)).collect();
        reg.on_table_delta(&grants, &grant, 0x10, None, None, None).await.unwrap();
        let counted = counted_for_the_commit(&mut reg, &grants, 0x18).await;
        assert!(
            counted <= COMMIT_PROOF_MAX_BYTES,
            "the record of one commit must stay inside {COMMIT_PROOF_MAX_BYTES} bytes, counted {counted}"
        );

        // The client holds event 1 from its snapshot: its label is the first granted name. Another
        // commit, as large, withdraws every grant; the flip that takes event 1 out still removes it.
        let name = grants.column_index("name").unwrap();
        let first = rows[0].0[name].clone();
        let Value::Text(label_text) = &first else { panic!("a text projection") };
        let held = events
            .row_from_json(serde_json::json!({"id": 1, "label": label_text.to_string()}).as_object().unwrap())
            .unwrap();
        reg.fake_candidates = Some(Arc::new(FakeCandidates::with_rows([("events", vec![held])])));
        let withdraw: Vec<Tup2<Row, ZWeight>> = rows.iter().cloned().map(|row| Tup2(row, -1)).collect();
        let mut work = reg.on_table_delta(&grants, &withdraw, 0x20, None, None, None).await.unwrap();
        work.retain(|(_, flip)| flip.value == first);
        assert_eq!(work.len(), 1, "the withdrawal of the first name is one flip");
        let registry = tokio::sync::Mutex::new(reg);
        propagate(&registry, work, "0/20").await;

        assert_eq!(
            envs_for(&store, "shape/feed"),
            vec![(TableRef::from("events").to_string(), "1".to_string(), "delete".to_string())]
        );
    }

    // The budget is a bound on what the record OWNS, at every moment. Three ways it could be one in
    // name only: the running figure is not what the heap accounting reports; the hash tables' own
    // storage is left out of it; or an entry is copied in first and the budget asked afterwards.

    fn transition(value: Value) -> Flip {
        Flip { value, dir: FlipDir::Leave }
    }

    /// The figure the budget is checked against is exactly what [`HeapSize`] reports for the
    /// record, through inserts into several inner sets, repeats, removals said and taken back.
    #[test]
    fn a_commits_proof_record_charges_itself_exactly_what_its_heap_accounting_reports() {
        let mut scope = CommitScope { lsn: 0x10, ..CommitScope::default() };
        for i in 0..6_000i64 {
            let sig: SubquerySig = format!("inner-set-{}", i % 7);
            let value = if i % 3 == 0 { Value::Text(format!("value-{i}")) } else { Value::Int(i / 2) };
            scope.note_transition(&sig, &transition(value));
            scope.first_removal(i % 5, &format!("pk-{}", i / 3));
            if i % 11 == 0 {
                scope.forget_removal(i % 5, &format!("pk-{}", i / 3));
            }
            if i % 97 == 0 {
                assert_eq!(scope.bytes, scope.heap_bytes(), "after {i} steps");
            }
        }
        assert!(!scope.saturated, "this much fits");
        assert_eq!(scope.bytes, scope.heap_bytes());
    }

    /// Many small entries: most of what such a record owns is its hash tables, which grow by
    /// doubling. The record gives up before the tables outgrow the budget, not once the entries'
    /// own sizes add up to it.
    #[test]
    fn a_commits_proof_record_counts_its_tables_against_the_budget() {
        let sig: SubquerySig = "granted".into();
        let mut scope = CommitScope { lsn: 0x10, ..CommitScope::default() };
        let mut largest = 0;
        for i in 0..400_000i64 {
            scope.note_transition(&sig, &transition(Value::Int(i)));
            if i % 1024 == 0 {
                largest = largest.max(scope.heap_bytes());
            }
        }
        assert!(scope.saturated, "400,000 transitions do not fit in {COMMIT_PROOF_MAX_BYTES} bytes");
        assert!(largest <= COMMIT_PROOF_MAX_BYTES, "the record owned {largest} bytes on the way");

        let mut scope = CommitScope { lsn: 0x10, ..CommitScope::default() };
        let mut largest = 0;
        for i in 0..400_000i64 {
            scope.first_removal(1, &i.to_string());
            if i % 1024 == 0 {
                largest = largest.max(scope.heap_bytes());
            }
        }
        assert!(scope.saturated, "400,000 removals do not fit in {COMMIT_PROOF_MAX_BYTES} bytes");
        assert!(largest <= COMMIT_PROOF_MAX_BYTES, "the record owned {largest} bytes on the way");
    }

    /// Removals a commit takes back, and then more of them. Taking an entry out of a hash table
    /// leaves a tombstone, and a table with tombstones reports less room than it has: asked for
    /// one more entry when that room is used up, it rehashes to its full size, which can be many
    /// times the doubling the budget was asked for. Here one feed's rows leave until its table is
    /// full, nearly all come back, others leave until the reported room is gone, a second feed
    /// brings the record close to its budget, and then one more row of the first feed leaves.
    #[test]
    fn a_commits_proof_record_stays_inside_its_budget_when_removals_are_taken_back() {
        let mut scope = CommitScope { lsn: 0x10, ..CommitScope::default() };
        let full = |scope: &CommitScope| scope.removed[&1].len() == scope.removed[&1].capacity();
        let mut left = 0usize;
        while left <= 100_000 || !full(&scope) {
            scope.first_removal(1, &format!("pk-{left}"));
            left += 1;
        }
        for i in 1_000..left {
            scope.forget_removal(1, &format!("pk-{i}"));
        }
        while !full(&scope) {
            scope.first_removal(1, &format!("pk-{left}"));
            left += 1;
        }
        assert_eq!(scope.bytes, scope.heap_bytes(), "the running figure is the record's heap");
        let mut other = 0usize;
        while !scope.saturated && scope.bytes < COMMIT_PROOF_MAX_BYTES - 3_000_000 {
            scope.first_removal(2, &format!("another-feeds-primary-key-{other:06}"));
            other += 1;
        }
        scope.first_removal(1, "one more");
        assert!(
            scope.peak <= COMMIT_PROOF_MAX_BYTES,
            "the record owned {} bytes at its largest, over its {COMMIT_PROOF_MAX_BYTES}",
            scope.peak
        );
        assert_eq!(scope.bytes, scope.heap_bytes(), "the running figure is the record's heap");

        // A row that left, came back and leaves again is news again, and is said once.
        let mut scope = CommitScope { lsn: 0x10, ..CommitScope::default() };
        assert!(scope.first_removal(1, "a"));
        assert!(!scope.first_removal(1, "a"));
        scope.forget_removal(1, "a");
        assert!(scope.first_removal(1, "a"));
        assert!(!scope.first_removal(1, "a"));
        assert_eq!(scope.bytes, scope.heap_bytes());
    }

    /// One projected value, or one primary key, larger than the whole budget — Postgres text can be
    /// far larger. The record is asked whether the entry fits BEFORE the entry is copied into it,
    /// so it never owns it, not even for the moment between the copy and the check.
    #[test]
    fn an_entry_larger_than_the_budget_is_never_copied_into_a_commits_proof_record() {
        let sig: SubquerySig = "granted".into();
        let oversized = "x".repeat(COMMIT_PROOF_MAX_BYTES + 1024);

        let mut scope = CommitScope { lsn: 0x10, ..CommitScope::default() };
        scope.note_transition(&sig, &transition(Value::Int(1)));
        scope.note_transition(&sig, &transition(Value::Text(oversized.clone())));
        assert!(scope.saturated && scope.undone.is_empty(), "the record is given up, not grown past its budget");
        assert!(scope.peak <= COMMIT_PROOF_MAX_BYTES, "the value was in the record first: {} bytes", scope.peak);

        let mut scope = CommitScope { lsn: 0x10, ..CommitScope::default() };
        assert!(scope.first_removal(1, "small"));
        assert!(scope.first_removal(1, &oversized), "a removal that cannot be remembered is said");
        assert!(scope.first_removal(1, &oversized), "and said again, since nothing remembers it");
        assert!(scope.saturated && scope.removed.is_empty());
        assert!(scope.peak <= COMMIT_PROOF_MAX_BYTES, "the key was in the record first: {} bytes", scope.peak);
    }

    // --- envelope positions (`headers.lsn`, `headers.txid`) ------------------------------------
    //
    // Every envelope the registry emits is placed: it carries the commit LSN and the transaction id
    // of the change that caused it. A plain shape's envelopes always were (the ingestor stamps
    // them); the registry's carried `operation`, `txid` and `commit_ts` only, and a consumer that
    // orders by position cannot place such an envelope at all.
    //
    // Presence is the easy half. A flip's verdict is taken out of commit order, so it can follow a
    // NEWER live change on the same stream, and at its own commit's LSN it would sit below what the
    // stream already carries: a consumer that applies a delta only at or above the key's applied
    // position would discard a correct verdict. So a position never falls along a stream.

    /// `(key, operation, lsn, txid)` of every envelope delivered to one stream path, in append order.
    fn positions_for(store: &DsStore, path: &str) -> Vec<(String, String, Option<String>, Option<String>)> {
        store
            .0
            .lock()
            .unwrap()
            .get(path)
            .map(|v| {
                v.iter()
                    .map(|e| {
                        (e.key.clone(), e.headers.operation.clone(), e.headers.lsn.clone(), e.headers.txid.clone())
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn placed(key: &str, operation: &str, lsn: &str, txid: &str) -> (String, String, Option<String>, Option<String>) {
        (key.into(), operation.into(), Some(lsn.into()), Some(txid.into()))
    }

    const MEMBERS: &str = "project_members|project_id|L(user_id,Eq,1)";

    /// A live arrival and a live removal (the key set's own: the stream delivered the row) each
    /// carry their commit's LSN, spelled as Postgres spells one, and their transaction.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_live_change_carries_its_commits_position() {
        let (ts, store, mut reg) = recency_fixture().await;

        reg.on_table_delta(&ts, &[Tup2(issue(1, 100), 1)], 0x1_0000_0010, Some(700), Some("700".into()), None)
            .await
            .unwrap();
        reg.on_table_delta(&ts, &[Tup2(issue(1, 100), -1)], 0x1_0000_00A0, Some(701), Some("701".into()), None)
            .await
            .unwrap();

        assert_eq!(
            positions_for(&store, "shape/s1"),
            vec![placed("1", "upsert", "1/10", "700"), placed("1", "delete", "1/A0", "701")]
        );
    }

    /// A changes-only feed's removal of a row it never delivered — opened by proof, not by the key
    /// set — is placed like any other live envelope.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_proven_removal_carries_its_commits_position() {
        let (ts, store, mut reg, _) = two_week_feeds().await;
        let (before, after) = (event(&ts, 1, 7, 12, None), event(&ts, 1, 7, 12, Some(99)));

        reg.on_table_delta(&ts, &[Tup2(before, -1), Tup2(after, 1)], 0x30, Some(702), Some("702".into()), None)
            .await
            .unwrap();

        assert_eq!(positions_for(&store, "shape/week1"), vec![placed("1", "delete", "0/30", "702")]);
    }

    /// A change buffered while its shape was being created is replayed at install under the commit
    /// it arrived with, transaction included: it is that commit's live envelope, delivered late.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_replayed_change_carries_the_position_it_was_buffered_under() {
        let (ds_url, store) = spawn_fake_ds().await;
        let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test(&ds_url), None);
        reg.set_schemas(Arc::new(creating_schemas()));
        let begin = reg.begin_create("s1", &"outer_t".into(), "shape/s1", &in_inner_t(), None, true).unwrap();
        let outer = reg.schemas[&TableRef::parse("outer_t").unwrap()].clone();
        let inner = reg.schemas[&TableRef::parse("inner_t").unwrap()].clone();

        for (lsn, xid, id) in [(0x40u64, 703u64, 5i64), (0x50, 704, 6)] {
            let delta = [Tup2(id_gid_row(&outer, id, 7), 1)];
            reg.on_table_delta(&outer, &delta, lsn, Some(xid), Some(xid.to_string()), None).await.unwrap();
        }
        assert!(positions_for(&store, "shape/s1").is_empty(), "a pending shape buffers; it emits nothing yet");

        // The inner set holds 7, so both buffered rows are members when the shape is installed.
        let seeds = begin
            .seeds
            .into_iter()
            .map(|(sig, _, _)| (sig, vec![id_gid_row(&inner, 1, 7)], crate::pg::SnapshotGate::passthrough()))
            .collect();
        reg.finish_create("s1", seeds, crate::pg::SnapshotGate::passthrough(), 0, Default::default()).await.unwrap();

        assert_eq!(
            positions_for(&store, "shape/s1"),
            vec![placed("5", "upsert", "0/40", "703"), placed("6", "upsert", "0/50", "704")]
        );
    }

    /// A flip's verdict names the commit that caused it: the triggering transaction's LSN and id,
    /// however much later its query-back ran.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_flips_verdict_carries_the_commit_that_caused_it() {
        let (ts, store, mut reg) = recency_fixture().await;
        let sig: SubquerySig = MEMBERS.into();
        reg.add_edge(flip_edge(&sig, Dependent::Shape("s1".into()), 1));
        reg.set_schemas(Arc::new([(TableRef::parse("issues").unwrap(), ts)].into_iter().collect()));
        reg.fake_candidates =
            Some(Arc::new(FakeCandidates::with_rows([("issues", vec![issue(1, 100), issue(2, 100)])])));
        let registry = tokio::sync::Mutex::new(reg);
        let (trace_tx, _rx) = tokio::sync::broadcast::channel(16);

        let mut work: VecDeque<(SubquerySig, Flip)> =
            [(sig, Flip { value: Value::Int(100), dir: FlipDir::Enter })].into_iter().collect();
        propagate_with_retry(&registry, &mut work, Some("705".into()), Some("0/50".into()), &trace_tx).await.unwrap();

        assert_eq!(
            positions_for(&store, "shape/s1"),
            vec![placed("1", "upsert", "0/50", "705"), placed("2", "upsert", "0/50", "705")]
        );
    }

    /// **The ordering rule.** Commit 0x50 flips an inner value; before its query-back runs, commit
    /// 0x60 changes row 1 and its live envelope lands at 0x60. The query-back's verdict for row 1 is
    /// the later word on the stream and must not be placed below the earlier one: it takes the
    /// stream's position (0x60) and keeps its own transaction. A later commit is placed at itself.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_flips_verdict_is_never_placed_below_what_its_stream_already_carries() {
        let (ts, store, mut reg) = recency_fixture().await;

        reg.on_table_delta(&ts, &[Tup2(issue(1, 100), 1)], 0x60, Some(706), Some("706".into()), None).await.unwrap();
        let gate = crate::pg::SnapshotGate::passthrough();
        reg.emit_for_shapes(
            &ts,
            vec![("s1".to_string(), vec![(issue(1, 100), true), (issue(2, 100), true)])],
            Some("705".into()),
            EmissionSource::QueryBack { gate: &gate, commit_lsn: 0x50 },
        )
        .await
        .unwrap();
        reg.on_table_delta(&ts, &[Tup2(issue(1, 100), -1)], 0x70, Some(707), Some("707".into()), None).await.unwrap();

        assert_eq!(
            positions_for(&store, "shape/s1"),
            vec![
                placed("1", "upsert", "0/60", "706"),
                placed("1", "upsert", "0/60", "705"),
                placed("2", "upsert", "0/60", "705"),
                placed("1", "delete", "0/70", "707"),
            ]
        );
    }

    /// A group stream's order is the group lane's to decide (commit order, and evaluation order
    /// for one key — `engine::emission`), so a group member's verdict leaves the registry at its
    /// own commit and the lane places it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_group_members_verdict_leaves_the_registry_at_its_own_commit() {
        let ts = issues_ts();
        let (ds_url, store) = spawn_fake_ds().await;
        let mut reg = SubqueryRegistry::new(DsClient::new_for_in_process_test(&ds_url), None);
        let sig: SubquerySig = MEMBERS.into();
        insert_test_node(&mut reg, &sig);
        reg.apply_node_evals(&sig, vec![("pm-1".into(), Some(Value::Int(100)))]).await;
        insert_changes_only_feed(&mut reg, "group-g", "issues", keyed_membership_pred(100, &sig));

        reg.on_table_delta(&ts, &[Tup2(issue(1, 100), 1)], 0x60, Some(706), Some("706".into()), None).await.unwrap();
        let gate = crate::pg::SnapshotGate::passthrough();
        reg.emit_for_shapes(
            &ts,
            vec![("group-g".to_string(), vec![(issue(1, 100), true)])],
            Some("705".into()),
            EmissionSource::QueryBack { gate: &gate, commit_lsn: 0x50 },
        )
        .await
        .unwrap();

        assert_eq!(
            positions_for(&store, "shape/group-g"),
            vec![placed("1", "upsert", "0/60", "706"), placed("1", "upsert", "0/50", "705")]
        );
    }

    /// Work a create hands back (a seed replay's flips, a deferred node re-derivation) reaches the
    /// emission tail with its commit's LSN but no transaction id in hand. The registry remembers
    /// the transaction of each commit it ingests, so the verdict still names it. A commit the
    /// registry never ingested has no transaction to name, and is placed by LSN alone.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_verdict_handed_no_transaction_names_the_one_its_commit_was_ingested_under() {
        let (ts, store, mut reg) = recency_fixture().await;
        // Commit 0x50 reaches the registry (this row is in no shape, so nothing is emitted).
        reg.on_table_delta(&ts, &[Tup2(issue(9, 999), 1)], 0x50, Some(705), Some("705".into()), None).await.unwrap();
        assert!(positions_for(&store, "shape/s1").is_empty());

        let gate = crate::pg::SnapshotGate::passthrough();
        for (commit_lsn, id) in [(0x50, 1), (0x58, 2)] {
            reg.emit_for_shapes(
                &ts,
                vec![("s1".to_string(), vec![(issue(id, 100), true)])],
                None,
                EmissionSource::QueryBack { gate: &gate, commit_lsn },
            )
            .await
            .unwrap();
        }

        assert_eq!(
            positions_for(&store, "shape/s1"),
            vec![placed("1", "upsert", "0/50", "705"), ("2".into(), "upsert".into(), Some("0/58".into()), None)]
        );
    }

    /// With no commit at all — library mode, where no change has an LSN — nothing is stamped: an
    /// envelope without a position is applied by key, as before.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_change_with_no_commit_position_is_left_unplaced() {
        let (ts, store, mut reg) = recency_fixture().await;
        reg.on_table_delta(&ts, &[Tup2(issue(1, 100), 1)], 0, None, None, None).await.unwrap();
        assert_eq!(positions_for(&store, "shape/s1"), vec![("1".into(), "upsert".into(), None, None)]);
    }
}
