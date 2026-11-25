# Scratch measurement changes — never merge

The production-source baseline is the immutable release artifact recorded in
RESULTS.md. Observational builds are separate artifacts with separate hashes.

The `stall_profile` module adds bounded atomic counters and RAII wall timers:
marker waits, membership calls, registry acquisition/holding, and active flip
batches/walks. It changes no receipt condition, replay position, append ordering,
snapshot gate, retry policy or task ownership. Existing shutdown-owned tasks own
the timers; cancellation drops them. Mutex guards retain the original lifetime.
No new task, queue, network operation, durable record or dependency is introduced.
The counters have eight fixed categories and no per-row retained state.

Wall categories overlap and may sum beyond elapsed time because eight flip tasks
can be waiting concurrently. They are not a partition of CPU. Perf supplies CPU
samples independently. A timer around a registry hold includes CPU and awaited
circuit work while that guard is actually retained. Flip registry counters cover
the production propagation/requery functions, not catalog creation or tests.

Focused guard tests check mutation, release and completed-wait counting. Release
builds and those tests run only in Actions. This is characterization, not a
production-readiness regression or permission to merge. Full product/client,
restart, fault and authorization qualification gates are intentionally not
claimed for these scratch observations.

## Audience-key prototypes

`B` is the user's all-plain idealization, not a claim that every proposed access
path can be replaced by a Space filter. Every tracked non-fence table gets nullable
`audience_key uuid` and `reader_class text`. The synthetic writer derives a row's
Space from its synthetic owning account, or its Calendar for `calendar_event`,
and writes both columns on insert. This workload contains only live, home-owned
rows; updates do not move parents and deletions remove the keyed rows normally.
No additional parent lookup is charged where the importing writer already knows
its Space/Calendar. General parent moves, hidden/deleted classes, share changes,
constraint repair and migration costs are not measured.

Historical windowed requests collapse to one equality feed per table/key/class;
we retain the original gateway projection plus the two measured columns. The
gateway token/access-list/relay implementation is not prototyped, and the original
marker probe intentionally remains to compare engine work on the same basis.
Only owner-key access is exercised. Equal payloads share a canonical shape.

`B-retained` is an additional cautionary control: chat, graph edges, Resources,
Files and User audiences retain their original gateway predicates and historical
windows. Other scopes use the prototype keys. It does not claim complete proposal
parity either; in particular it does not implement live share/revocation enforcement.
Comparing B with B-retained prevents an all-plain best case being mislabeled as
proof that the proposal's explicitly retained subqueries are free.

Both use the same source accounts, row counts, transaction boundaries, synthetic
payloads, disabled FK/cascade triggers, and other-account probe as A. The extra
columns add real stored/replicated bytes. The prototype is not an authorization
qualification and must never be deployed.

## Unsafe marker bypass

`STALL_REPRO_UNSAFE_SKIP_MARKER_WAIT=1` makes `wait_for_source_effects` return true
immediately in the observational binary. This deliberately gives up the receipt's
guarantee that earlier flip walks and their stream appends have completed, along
with the wait function's shutdown/degradation checks. Other guards are unchanged.
It is a measurement switch explicitly requested for this task, not a bridge safe
to ship. The harness still requires zero pending flips and failing appends after
the workload marker; an early receipt alone is never its completion oracle.

## Per-commit batching experiment contract, before implementation

The proposal's explicit reading is first-old/last-new folding per primary key,
one registry acquisition and membership evaluation per touched table per commit.
The frozen observer run at engine 894b2cc actually calls production membership
11,941 times for the six-transaction import. `check_membership_batch.py` asserts
an upper bound of 44 tables times six commits, plus every possible probe write
and 16 boundary/sampler allowance calls. This is a cost/structure contract using
the real engine's captured counters, not a customer-facing correctness oracle.
Its red artifact must be recorded before the prototype is implemented.

The proposed scratch implementation will buffer only this transaction, bound
charged retained images to 512 MiB and 100,000 primary keys, and fail closed at
the existing replay boundary on overflow. That deliberate scratch rejection is
NOT the production arbitrary-transaction-size contract. No new durable record,
task, network client, retry policy, snapshot gate or dedup rule is introduced.
The transaction's existing last-envelope boundary owns and drops the buffer.
Membership flush precedes ordinary appends and receipt publication. Errors reset
the highwater through the existing sequencer error path. The prototype will not
claim time slicing, scratch spill, fair round scheduling, crash-safe deferred
walk recovery, or comprehensive intermediate authorization equivalence. Full
production invariants require those missing parts; this experiment is unmergeable.

Red provenance: Actions `45363068569`, test-only harness `57b23ec`, checked the
immutable real observer evidence and failed the intended cost assertion
(`11941` membership calls above the per-table/commit bound). The job verifies
that specific assertion failure; its green job status means red was retained,
not that production already batches. The prototype preserves the same oracle.

The implementation adds private `stall_batch::Commit`, selected only by
`STALL_REPRO_COMMIT_MEMBERSHIP=1`, and folds first-old/last-new by key before
calling existing `on_table_delta` once per nonempty touched table. It still
evaluates each candidate shape against every relevant row, and still performs
per-value Postgres walks: it is NOT a membership-result cache or PR68's round
coalescing. It enqueues one walk batch per table rather than per row, which also
changes available walk parallelism. The ordinary filters/aggregates retain their
per-row path. It deliberately omits time slicing and spill, and therefore can
hold the registry longer in a single acquisition. No production performance or
correctness claim follows merely from the structural cost oracle passing.

## Shape-count controls

The half-subquery control keeps 2,144 of 4,287 subquery shapes (eight import/probe
feeds included), replacing the removed half with distinct plain shapes to keep
10,571 total. Selection is deterministic SHA256 order of request bodies, not a
convenient subset chosen after timing. The all-plain control replaces all 4,287.
Both retain table, projection and subscription identity, but replace permission
subqueries with an existing owner/Space column equality plus a unique harmless
primary-key inequality. For tables without a usable owner column, the equality
selects one synthetic parent/key. Nullable owner columns can match fewer rows.
These are shape-cost ablations, NOT authorization-equivalent app models; they
must not be mistaken for B's maintained keys or a safe gateway change. The
original plain shapes and the underlying synthetic data/writes are unchanged.

## Bridge knobs (takeover, `[bridges]` build)

One scratch binary built from this branch carries every knob below; each is off by default,
and `bridge-control` runs the same binary with all of them off.

- Lock-hold histograms: every `stall_profile` category also records its longest interval
  since boot and counts in six buckets (≤1 ms, ≤10 ms, ≤100 ms, ≤1 s, ≤5 s, >5 s). The memory
  sampler's registry acquisition is now measured too (`sampler_registry_wait`/`_hold`). Pure
  observation.
- `STALL_REPRO_SEQUENCER_YIELD=1` (`yield-rows`): after each release of the registry lock the
  sequencer calls `tokio::task::yield_now()`. Prod already releases the lock after every row
  (`process_envelope_deferred`); this only lets other ready tasks run before the sequencer
  queues for the lock again. It gives up nothing.
- `STALL_REPRO_LOCK_SLICE_ROWS=N` (`commit-membership-slices`, N=256): only on the per-commit
  membership path, which otherwise holds the lock for a whole table's folded delta. The flush
  releases and re-takes the lock every N delta rows and never cuts between an update's old
  and new images. It gives up a commit being applied to membership in one step: between slices
  a flip walk, a subset or the sampler can see membership sets that hold part of a commit, and
  outer-shape emissions from an earlier slice can be appended before a later slice's
  retractions. For example, if one commit moves a Record from Space X to Space Y, readers of Y
  can get it before readers of X lose it. Stream order within one shape is unchanged. No
  answer from snapshots is prototyped: `GET /_runtime/drained-through` already reads the
  receipt map without the registry lock, and what a drain waits for is the sequencer reaching
  the fence's commit in log order plus `wait_for_source_effects`. `marker-bypass` measures
  removing the second half.
- `STALL_REPRO_MEM_SAMPLER_MS` (`sampler-off` = 0, `sampler-10x` = 5000): period of the
  hard-coded 500 ms OTel sampler (`mem::spawn_sampler`), which takes the registry lock and
  walks every node's values in `SubqueryRegistry::mem_totals`. The 30 s structured memory log
  and `GET /memory` still compute cardinalities. Off loses only the OTel memory gauges' freshness.

## Retained-scope ablation (`B-retained-key-<class>`)

Same as `B-retained`, except that one exception class is keyed like the rest: its historical
requests become `audience_key`/`reader_class` equality feeds, and the import account's feed on
that table is keyed. Classes: `chat` = message, workflow_run; `edge` = graph_edge; `resource` =
resource, resource_fact; `file` = file; `user` = user. The writer maintains audience keys on
every table in all keyed variants, as for B. These runs show how much each retained class costs.
They do not claim that keying the class is authorization-equivalent.

## Engine bisect, B-cold and E variants (`[paired] [paired-2] [bisect-build]`)

- **Engine bisect.** Release builds (debug level 1, frame pointers) of PR 70 (`e10c4bd`), PR 71 (`a248886`) and PR 69 (`b42f70d`, prod since 08:33 UTC Nov 24), built from `git archive` with no source change. They run beside the existing `1be10fb` and `5b2cf3e` binaries on one runner.
- **B-cold.** See `variant-b-cold.md`.
- **E-only and E-plus-R.** Today's catalog (A). Only `graph_edge` (E-only), or `graph_edge` plus the six Record-family tables (E-plus-R), get B's audience-key columns and keyed bodies, collapsed to one feed per account as in B. Every other table and the probe's feed stay as today, except that the probe's Record feed is keyed in E-plus-R.

## B-per-account

### As built on B (`[paired-4]`, superseded)

Harness `bada4e3` built B-per-account on B's `audience_key` columns and paired it with B (results in `paired/paired-per-account-*`). The proposal's control is variant S, not B, so it was rebuilt below. The earlier rows stay for provenance only.

### As built on S (`[paired-5]`, OTTO-14032 question `q-keyed-stream-sharing`)

Built from S (`simple_keys.py`, merged from `profile/simple-keys-2026-11-24`), so that the only difference from S is one stream per account instead of one per Space. For every account and keyed table (`SIMPLE_KEYED`: the Record family, `calendar_event`, `graph_edge`), `per_account_feed` ORs S's feeds for that account into one plain filter:

- **Record family:** `space_id IN (the account's Spaces, partner Spaces included)` with S's row conditions (hidden record types; assignment variables), `OR id/record_id IN (direct shares)`.
- **`calendar_event`:** `(user_id = U OR calendar_id IN (partner calendars))` with S's event row state.
- **`graph_edge`:** S's six edge branches with each `= space` written as `IN (spaces)`. S as written has no edge share list, so neither does this.

Lists are S's own, from `account_lists`, read from the database when the shape is built. The engine has no literal-list operator, so an `IN` list is an OR of equality leaves. There is no cross-account dedupe. Every other table, the data (share seed, `assignment.space_id`, audience columns on the tables S doesn't key), the probe and the import feeds are exactly S's.
