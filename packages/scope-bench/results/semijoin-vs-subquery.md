# Key-only semijoin versus subquery members

Work in progress, not for merge. The real DBSP candidate and common group correctness
fixes for the reproduced hosted-review regressions are being validated by CI; no performance
results are claimed until the five-repetition PostgreSQL 18 benchmark job completes. The
experiment must finish before this report can support changing the membership decision.

## Pinned inputs and method

- Group baseline: ec#44, `8f7f07add3f3fd23cc92f94df021eca4c3b240c8`.
- Benchmark harness: `prototype/shape-groups-perf2`,
  `585e318410b53439cb03e832c3336a313f867f03`.
- Baseline DBSP is **0.318**; the requested proposal target is **0.365**. A version
  upgrade would change more than membership relative to ec#44, so this experiment keeps
  **0.318 in both modes**. Its results will not establish performance on 0.365.
- Planned workload: roughly 12,000 Records, five independent repetitions of each
  mode, 30-phone steady writes and cold launch on real PostgreSQL 18 in CI.
- The CI benchmark runs the same `scope-bench` command twice per mode: `ONLY_CONFIG='scope
  group'`, `PHONES=30`, `OTHER_USERS=40` (about 12k Records), `REPS=5`, once for
  `ONLY='cold launch'` and once for `ONLY='chat open' STEADY=1`. Each run records the
  membership mode, engine boot time, group load time, `/memory` RSS/cardinality/owned-byte
  snapshot, actual `pg_stat_statements` calls, provider delivered/empty reads, gateway CPU,
  engine CPU, downlink bytes and steady write p50/p90. The uploaded JSON is the raw evidence;
  this Markdown table is updated only from those artifacts.
- Keep group claim, lease, stream and commit order identical between modes. Record
  total engine RSS and its peak during restart, DBSP-reported state and spill bytes,
  incremental bytes per group, seed and reload times, actual reads per change,
  write-to-delivery p50/p90, and engine/gateway CPU separately.
- Per-group RSS from a difference divided by group count is an estimate; allocator
  retention, shared state and cache effects must not be called owned group bytes.
- The perf2 streaming mode models one open response using provider long polls. Its
  charged read count is not a measurement of native Durable Streams SSE. Preserve
  actual provider reads separately; do not use that model for fd qualification.

| Metric | Subquery members | Key-only semijoin | Evidence status |
| --- | --- | --- | --- |
| Total / incremental per-group memory | Pending | Pending | No new measurement |
| Restart peak RSS and ready + group reload time | Pending | Pending | No new measurement |
| New-group seed time | Pending | Pending | No new measurement |
| Actual reads per change | Pending | Pending | No new measurement |
| Delivery p50 / p90 | Pending | Pending | No new measurement |
| Engine / gateway CPU | Pending | Pending | No new measurement |

`engineBootMs` is a cold process boot plus catalog/shape reload measurement for this harness;
it is not a SIGTERM restart or a persisted durable-stream restore. `groupLoadMs` is the
end-to-end group request through the simulated gateway, including the public page and private
member seed. `/memory`'s process RSS is total engine memory; its `cardinalities` byte fields are
owned lower bounds, and per-group memory is estimated as a change divided by the number of
simultaneous groups. A provider read is a Durable Streams read counted by the gateway; the
gateway's modeled request/RTT/downlink charges are reported separately. `readsPerChange` is
`pg_stat_statements` calls divided by committed steady writes and includes query-backs and seed
work, so it is not a wire-level read count.

## Correctness contract before implementation

This is a prototype implementation regression lane, not release qualification. The
existing ec#44 group tests are inherited controls. Two added contracts exercise
production code through real PostgreSQL 18, the native group API, the sequencer,
and ec#44's observed in-memory DS fixture:

1. `ownership::group_snapshot_only_member_emits_move_out`: take a group snapshot
   with the inner Record table and a separate companion member whose snapshot contains one row;
   delete its inner key and write a source marker in
   the same transaction. Once the marker's receipt is visible, require the
   table-qualified companion delete. No intermediate live upsert is allowed to
   populate the delete gate. No other member covers the companion row. Require
   every resulting event's wire LSN to equal the source receipt's commit LSN.
   A missing delete fails the semantic assertion rather than timing out waiting
   for an event.
2. `ownership::group_membership_events_carry_commit_lsn`: insert an inner key and
   source marker in one transaction; after its receipt require the companion
   upsert and require every resulting event's wire LSN to equal that receipt's
   commit LSN. A snapshot/seed LSN or any other stale nonempty LSN is a failure.

Run both with the existing source-table command:

```sh
cargo test -p electric-circuits-engine --features test-support \
  --test sources_table group_ -- --ignored
```

The receipt covers deferred emission, but this lane has no real gateway, Swift
`CircuitsSubsetSource`, or GRDB cache receipt. It cannot qualify those seams.

The implementation must preserve xid-based snapshot fencing, absolute emission,
table-qualified identity, the sequencer's deduplication and source-commit ordering,
durable-before-ack lifecycle, and retirement on drift/TRUNCATE. Membership state
is derived state rebuilt from PostgreSQL. No new durable protocol is proposed.
Before adding a circuit worker, record its queue/byte bounds, error escalation,
shutdown owner and join behavior alongside the implementation.

### Genuine-red evidence

[CI run 40655076393](https://github.com/indexedlabs/electric-circuits/actions/runs/40655076393),
job `122704029701`, tested head `8b584ef2d8b303a3c589b9c4c8de6b2184a059b2`
through merge checkout `0aeb9fb43a319649da9d33484114f1ae54ce63ee` on PostgreSQL
**18.6**. Exactly the two added contracts failed; the other 19 source-table
tests passed. The first divergences were:

- Move-out: only the inner Record delete was emitted; the snapshot-only companion
  delete was missing after the source receipt.
- Move-in: the companion upsert existed, but had no `headers.lsn`; its receipt's
  commit LSN was `0/18FB1B8`.

The raw job log SHA-256 is
`b5019b50b186c61ba21a5df4e562b0d7e15b9e14447215b8c1d9a7ca0027cafb`.
Formatting and 739 library tests passed. Typecheck/Node/Vitest were not reached
after the intended Rust failure. Both hosted reviews accepted this test-only
checkpoint; their verdicts do not cover the subsequent implementation.

### Candidate and controls

**Review checkpoint `6d7551e`: not correct yet.** Both hosted reviewers found that
unconditional group deletes wake streams for never-member rows; a deferred
query-back can also delete a row belonging to a disjoint same-table sibling.
They also found that killing the shared DBSP worker on a Select error can leave
standalone shapes reading frozen membership. The current implementation must
not be benchmarked as a correct alternative. CI run
[40660381572](https://github.com/indexedlabs/electric-circuits/actions/runs/40660381572)
stopped at one rustfmt line wrap; no runtime tests executed in that job. Engine,
Node and Electric container builds passed in run `40660381475`.

The next test checkpoint fixes only that formatting and adds two PostgreSQL
contracts: a never-member insert/delete must leave the group stream unchanged
after source receipts, and an inner-key move-out must preserve a disjoint plain
sibling on the same table while deleting the companion. A focused fault test
injects a Select transaction failure during `finish_create` buffered replay,
rolls back the group, then applies an unrelated standalone membership change
and demands typed degradation on the runtime receipt query. The injection is
compiled only into unit tests and traverses the production error branch. These
tests await genuine-red execution before the behavior fixes. CI runs both
membership modes even if one fails, and separately runs library tests so the
fault regression is not hidden behind the PostgreSQL failures.

The `b83da4e` run
[40663786869](https://github.com/indexedlabs/electric-circuits/actions/runs/40663786869)
also stopped at rustfmt (the fault-command enum layout and one fixture call).
It supplies no runtime or genuine-red evidence for the new regressions. Both
hosted reviews accepted the regression designs while retaining the open behavior
findings. The next checkpoint applies rustfmt's exact output and fixes the
reviewed fail-fast issue: Cargo uses `--no-fail-fast`, each Rust command records
its status independently, and typecheck/Node/Vitest run when their setup succeeds
even after a Rust failure. Formatting and every failed test still fail the job;
no test is disabled or treated as an expected pass.

### Review-regression red checkpoint: `7cbda3f`

[CI run 40667076997](https://github.com/indexedlabs/electric-circuits/actions/runs/40667076997),
job `122744154957`, tested head `7cbda3f5991799e2cb2aa5e75fa37915c67a5c93`
through merge checkout `256df353c108ed16917aab0bbcf01c247f3f2d53` with PostgreSQL
**18.6** (`Ubuntu 18.6-1.pgdg24.04+2`). The raw job log SHA-256 is
`b3e2cd7015e646e4c07c2be7abbb8fc83d7774d0f25e0a9c96fbc3e0f1e42bde`.

| Gate | Observed result |
| --- | --- |
| Group contracts, subquery | 10 passed, 2 failed |
| Group contracts, semijoin | 10 passed, 2 failed |
| Full ignored source-table contracts | 22 passed, 2 failed |
| Rust library | 741 passed, 1 failed, 3 ignored |
| Other Rust integration targets | Passed (their ignored tests remain ignored) |
| Deployment ownership, explicitly ignored lane | 1 passed |
| PostgreSQL projection, explicitly ignored lane | 2 passed |
| Formatting, TypeScript typecheck | Passed |
| Supported Node gate | 9 passed |
| Full Vitest | 79 files, 357 tests passed |

Both group modes fail at the same intended semantic assertions:
`group_disjoint_same_table_sibling_survives_move_out` receives deletes for both
keys 1 and 2, although key 1 still belongs to the plain sibling;
`group_never_member_insert_and_delete_do_not_wake_stream` receives a delete for
out-of-scope key 3 after the insert's source receipt. The library fault contract
fails at “create replay killed shared membership but engine still accepts receipt
queries”. The original snapshot-only move-out and wire-LSN contracts, and the
new reparenting control, pass in both modes. This is genuine-red evidence for the
review defects; no regression assertion is changed in the correction.

### Correction candidate and current handoff implementation

The `9cffab2` CI run later supplied the intended green engine evidence: both
membership-mode group loops passed all 12 selected group contracts, including the
never-member, disjoint-sibling, snapshot-only move-out, LSN and reparenting cases;
the library passed 743 tests with 3 ignored. The full ignored source-table lane
passed 24 tests, and the existing integration lanes passed. Full Vitest then ran
80 files / 358 tests with exactly one failure: the new streaming phone regression
at its first timing assertion (`onWake` occurred at t=125 ms). This is the genuine
red for the remaining benchmark-model defect, not an engine failure. The same run
also confirms TypeScript, the supported Node gate and image builds passed.

The streaming fix routes each received chunk through the shared return half-RTT/downlink
queue and increments `bytesDown`. The implementation also seeds keys from the actual
public group page into each subquery member's FeedSet before the group claim becomes
durable. The private key seed and this public-page seed both retain only keys; the extra
bounded reads and feed entries belong in both modes' measurements.



Group companion creates now seed their FeedSet through the existing streamed
outer backfill and phase-C seed/replay handoff before becoming live. For these
changes-only members the backfill retains only primary keys and appends no public
snapshot payload; the later shared group snapshot stays unchanged. Its xid fence
and pending replay close the seed/live boundary. All deletes again require actual
prior membership in that member's gate, preserving disjoint same-table siblings
and keeping never-member writes quiet. This minimal correction reads full matching
rows one bounded chunk at a time before discarding non-key columns; it does not
yet project the SQL seed to keys. Include the extra read, transient payload work,
and retained feed keys in both modes' memory and seed/restart measurements.

The engine installs a synchronous degradation callback before the shared DBSP
worker accepts any commands. The worker latches it before reporting a failed
Select or Batch, independently of response cancellation; an unwind guard also
latches unexpected worker failure. The worker then stops. Batch channel/response
failures now propagate as errors through node seeding, live deltas and parent
query-backs instead of returning zero flips. Ordinary shutdown disarms the guard
and joins the same thread. No new worker, queue, retry loop or durable state is
introduced; restart uses the existing reseed/drop-and-reload policy. An additional
focused test drops a Select response receiver before submission, joins the failed
worker, and checks that its owner was notified and subsequent Batch calls fail.

The new `phone.test.ts` regression exercises production `Phone.hold` with a
controlled streaming chunk and virtual time: return propagation, serialization
at 1 Mbps, and byte accounting must precede delivery. Its first CI execution was
genuinely red at t=125 ms; the working tree now applies the shared downlink model
before delivery.

### Current exact-head correction

Head `8185b7b` adds the missing real PostgreSQL page-handoff controls. The test-only
sequencer pause keeps the source reader behind the page; an outer-row delete committed
after the page is visible but unsequenced, so the live path cannot seed the member's
FeedSet. Only the page-key handoff can emit the table-qualified tombstone, and the
assertion checks the delete's source commit LSN. A second contract aborts the group
request at both handoff barriers and checks that `inflight_querybacks` returns to zero.
The test bodies use distinct names from their isolated wrappers.

The benchmark now captures `/memory` and `/debug/dbsp-profile` both after group load
and at the end of the hold. The end profile checks the requested mode and requires
`selectionBatches > 0` for semijoin runs after an eligible write. Cold launch also
updates an assignment companion every 200 ms to exercise that path; this makes cold
launch a corrected workload rather than a byte-for-byte replay of perf2. `readsPerChange`
subtracts the benchmark writer's exact `BEGIN`/`COMMIT`/`ROLLBACK` utility statement
count in addition to filtering its DML. The exact-head CI run `40717261548` (test job
`122918228568`) reached the Rust and PostgreSQL lanes but failed before benchmark
artifacts: rustfmt failed, the library tombstone assertion used an unqualified table
name, and one new page contract failed before its event wait was made deterministic.
No measurement is claimed from it.

Earlier exact jobs are retained: group contracts passed in both modes at `8c0c6f0`
(job `122831109853`), `88b00cc` (`122836183365`) and `1c2926a`
(`122838729992`); the Rust job remained red there on formatting and the unqualified
tombstone assertion. At `8185b7b`, the subquery group loop passed its handoff contract
but `ownership::group_page_cancellation_releases_queryback_window` failed during its
cleanup with `the slot is held by walsender pid 17649`; the semijoin loop passed. The
library then failed `subquery::tests::page_handoff_emits_tombstone_for_newer_negative_queryback`
at `group snapshot table disappeared`. `327653b` fixes the split cancellation cases and
installs the missing unit-test schema; this head also applies the pinned rustfmt output.

The page/live handoff keeps ordinary stale-query-back recency separate from a bounded
`page_decisions` map. Query-back decisions use their read LSN for snapshot ordering and retain
the triggering commit LSN for synthetic tombstone headers. The handoff guard balances its state
on request cancellation, and tombstones are delivered in commit-LSN order. Empty inner-flip
query-backs do not scan the whole companion table; they remain a no-op unless a scoped decision
exists.

The perf2 `gateway.ts`, `phone.ts`, and `run.ts` harness files have now been
imported directly from the pinned perf2 commit, without its older engine code.
Their source blobs are respectively `ec9699db0c3393d55601a5996ec4e27fe4e69af9`,
`81251167803ff4bb307f1d0913afe721381c11cc`, and
`d5dfd0aa57372b90b820ba39f0ef0982468e6b88`. The subsequent `run.ts` change
repairs its Markdown table delimiter to match the 18 header/row cells.
The old Node sketch is explicitly named `semijoin:model` in package scripts.
This import supplies the starting harness only; mode comparison, resource/restart
instrumentation, five-repetition CI execution and resulting numbers are still
pending. Its modeled streaming charges must not be mistaken for provider reads.

Set `ELECTRIC_CIRCUITS_GROUP_MEMBERSHIP=subquery|semijoin` when starting the
engine; the default is `subquery`. The baseline already keeps shared scope keys
incrementally in DBSP. The candidate adds a DBSP incremental join of the distinct
`(scope node, Record key)` relation with transient `(scope node, companion foreign
key, ordinal)` probes. Returned ordinals select actual emitted rows. No companion
payload enters the circuit. It is a real join, but **not** a persistent companion
arrangement or a replacement for the baseline contributor map. In particular it
does not eliminate flip-driven PostgreSQL query-backs.

One positive `IN` under conjunctions is eligible, with local residual filtering.
`NOT IN`, subqueries under `OR`, and multiple independent `IN` leaves keep the
existing evaluator. The benchmark must report eligible members and nonzero
`selectionBatches` for cases advertised as exercising the join. The perf2
single-Record scenario uses direct foreign-key equality predicates, so that
unchanged scenario is a control, not evidence of join throughput.

The existing owned DBSP worker and 256-command channel are reused; selection
awaits capacity and limits each command to 1024 keys/ordinals. This is an item
bound, **not** a hard byte cap on arbitrary SQL text keys. Each chunk inserts,
steps, reads output, retracts and steps again. The registry mutex spans this
local-worker round trip, preventing membership changes mid-selection. Probe
cleanup completes even if its response receiver is dropped. A probe transaction
error synchronously degrades the engine and stops the worker before returning an
error, preventing later decisions from a half-cleared left relation. Engine close
joins the worker; its disposable spill cleanup is unchanged.

Both modes receive the same correctness fixes: mark group membership before
seed I/O, seed the member delete gate before live replay, and stamp group
membership events with their triggering source commit LSN. The two frozen red
contracts are unchanged. An additional PostgreSQL contract reparents one companion
key out/in/out after separate source receipts and checks the diagnostic mode and
join-execution counter. Focused operator coverage includes distinct contributors,
scope isolation, key moves/removal, NULL and reused ordinals across 1024-key chunks.
The join and reparenting controls passed at `7cbda3f`; the delete-gate, shared-worker,
page-handoff and cancellation corrections are implemented on `8185b7b` and the latest
fix is pending CI. Existing claim/lease, public snapshot query, group stream and
commit fence code is unchanged.

Use the complete `GET /debug/dbsp-profile` plus RSS to measure memory: the
published-snapshot byte counters in `/memory` do not include the new join's
arrangements. Wire LSNs identify the triggering commit, while deferred query-backs
may read newer database state; this is not proof of exact-prefix Swift/GRDB
materialization. The workload has one member per table. Overlapping same-table
members still need a union-semantics contract before claiming general group
correctness.

## Production incident matrix

These are required checks, not passing claims. Record both modes and exact failures
as the experiment progresses.

| Edge | Required observation | Current status |
| --- | --- | --- |
| Values over 16 MiB and append budget | Snapshot/live event delivered whole without degradation | Pending |
| 100 MB transaction and unrelated groups | Other delivery and drain latency while the large commit is held | Pending; shared sequencer is unchanged |
| Thousands of groups plus held-log replay | Peak RSS against configured memory budget, rebuild + reload time | Pending |
| Many groups and held SSE responses | Engine/DS/gateway fds and connections; release after close | Pending; native gateway/SSE topology absent from this checkout |
| 30-phone reconnect burst | Seed columns, conflicts/timeouts, racing and retried claims | Pending; baseline rejects a repeated group claim with 409 |
| Snapshot-only move-out and wire LSN | Exact delete after receipt and LSN on each membership event | Both green in both modes at `7cbda3f`; exact-head rerun pending |
| Companion reparenting | Table-qualified delete/upsert/delete after individual source receipts | Passed both modes at `7cbda3f`; exact-head rerun pending |
| Snapshot/LSN boundary through Swift/GRDB | Same source prefix materialized across the real client stack | Unavailable in this repository alone |
| Slot/WAL retention | No extra slots per group, lag and cleanup after drop/restart | Pending |
| Five-second authority rechecks | Measured recheck load under peak held streams | Pending; perf2 has a modeled gateway |
| Restart/SIGTERM with held streams | Resume correctness, no loss, bounded reconnect rate | Pending |
| Claim during catalog restore; unread checkpoint | Boot admission refuses until ready; shutdown cannot regress checkpoint | Added from incident review; pending |

Incident sources read: Indexed Incidents revision 20, commit
`3fb562ffe2cccec4c8dd33307f25e73da36f3221`, pages
`incidents/2026-10-13-sync-stalled-behind-large-transactions.md` and
`incidents/2026-10-15-circuits-restart-sync-down.md`. The latter adds boot admission
and checkpoint regression to the supplied list. This artifact revision contains no
September read-cap or October 23 fd-exhaustion writeup; those scenarios come from
the task's supplied incident details. Mighty Incidents revision 7 concerns Run
infrastructure and adds no membership case.

## Execution notes

Remote execution is unavailable: `mighty remote --provider tensorlake` returned
`invalid: remote project profile is unavailable`. Compilation and PostgreSQL
checks therefore run in GitHub CI. The accepted proposal and decision were not
modified. Both requested document paths currently return `document not found in
scope` for `mighty/180ad1d2-ace4-5530-838d-dd14a60777f4`; that branch currently lists
only the October 25 scope-load/read-scaling changes.

## Recommendation

Pending real DBSP measurements. Keep the accepted subquery-members decision in
place until a correct semijoin demonstrates lower memory and restart cost. This
is an evidence threshold, not a measured performance verdict. ec#44's documented
lifecycle, cancellation and burst-pressure gaps also prevent treating either
membership experiment as production-ready.
