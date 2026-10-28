# Key-only semijoin versus subquery members

Work in progress, not for merge. Correctness is still being validated by CI; the first
five-repetition PostgreSQL 18 benchmark completed on `48501cd`. The measurements below are
prototype evidence, not release qualification. The first measured semijoin run uses more DBSP
state and has slower delivery and higher engine CPU; restart cost remains unavailable.

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

| Metric (5-rep median) | Subquery members | Key-only semijoin | Evidence |
| --- | ---: | ---: | --- |
| Cold group load (ms) | 13,082 | 13,017 | Measured; artifact 12844722320 |
| Cold engine RSS (MiB) | 73.7 | 75.1 | Measured process RSS |
| Cold DBSP used bytes | 34,212 | 60,232 | Measured profile; semijoin arrangement state |
| Steady group load (ms) | 4,340 | 4,209 | Measured; artifact 12844722320 |
| Steady engine RSS (MiB) | 76.8 | 75.9 | Measured process RSS |
| Steady DBSP used bytes | 65,092 | 94,512 | Measured profile |
| Actual reads/change | 2.667 | 2.694 | Measured pg_stat_statements calls / 180 writes |
| Delivery p50 / p90 (ms) | 495 / 734 | 1,373 / 1,981 | Measured steady writes |
| Engine CPU (ms) | 1,920 | 3,070 | Measured steady hold |
| Gateway CPU (ms) | 4,864 | 4,588 | Measured steady hold |
| Restart peak RSS / reload time | Unavailable | Unavailable | Harness boots fresh process; no persisted restart lane |
| New-group seed time | Included in group load | Included in group load | No separate seed timer |

The measured benchmark is run `40747358562`, test job `123024579773`, benchmark job
`123037684019`, artifact `12844722320` (`semijoin-vs-subquery-5ebc3f863a377fca22d5ca55346348a3c92ae44`).
It ran five repetitions for each mode and both scenarios with PostgreSQL 18.6. `engineBootMs` is a
cold process boot plus catalog/shape reload measurement for this harness; it is not a SIGTERM
restart or a persisted durable-stream restore. `groupLoadMs` is the
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

Commit `aa3c90b` routes each received streaming chunk through the shared return
half-RTT/downlink queue and increments `bytesDown`. It also seeds keys from the actual
public group page into each subquery member's FeedSet before the group claim becomes
durable, closing the page/live handoff that could otherwise leave a snapshot row without
a delete gate. The private key seed and this public-page seed both retain only keys; the
extra bounded reads and feed entries belong in both modes' measurements. Its focused
phone regression was genuinely red before that correction; the exact-head benchmark
artifact is still pending.



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

Head `8185b7b` carries the real PostgreSQL page-handoff controls. The test-only
sequencer pause keeps the source reader behind the page; an outer-row delete committed
after the page is visible but unsequenced, so the live path cannot seed the member's
FeedSet. Only the page-key handoff can emit the table-qualified tombstone, and the
assertion checks the delete's source commit LSN. A second contract aborts the group
request at both handoff barriers and checks that `inflight_querybacks` returns to zero.
Head `327653b` makes the event wait deterministic; `6a58456` installs the missing library-test schema
and passes its group/library tests, while its formatting gate remains red. Head `53dd7e3` adds a clean-close/slot-reuse assertion,
member-claim compensation on canceled creates, and failed-boot result capture.
Head `9e9171e` makes the member releases durable across cancellation, records a durable
`GroupDropped` after `GroupCreated` is queued, and adds a catalog-completion cancellation
barrier. Commit `ccb02c6` extends that completion case to an all-plain group, closes the
original engine, boots a replacement on the same slot/store, and retries the same claim only
after restore; this is the restart assertion for the durable drop path. Merge head `d7cd60c`
records the concurrent-party reconciliation with the remote branch and retains this tree.
The handoff wrapper keeps the established `group_page_handoff_preserves_newer_move_out`
name so the CI `group_` filter remains stable, while its body uses a deterministic outer-row
delete. The event wait subscribes before observing the stream and waits for the exact
table-qualified delete/key/LSN tuple.

The benchmark now captures `/memory` and `/debug/dbsp-profile` both after group load
and at the end of the hold. The end profile checks the requested mode and requires
`selectionBatches > 0` for semijoin runs after an eligible write. Cold launch updates an assignment companion every 200 ms without changing scope membership.
Steady writes toggle the eligible `record_date_index` day between two bounded values so
transcript groups exercise the semijoin path without accumulating drift across repetitions;
this makes the workload a corrected run rather than a byte-for-byte replay of perf2. `readsPerChange`
uses a per-process benchmark writer role and excludes that role's DML and transaction
utility statements by `userid`; defensive text filters remain for SQL issued through the
admin session. The exact-head `327653b` run `40720275270` (test job `122928965091`) ran
the group contracts successfully in both modes, but its Rust library lane failed in
`subquery::tests::page_handoff_emits_tombstone_for_newer_negative_queryback` with
`group snapshot table disappeared`; rustfmt also failed on the three pinned `subquery.rs`
formatting hunks. The `6a58456` run `40723397869` (test job `122940089900`) passed the
group contracts in both modes and the Rust library lane but failed rustfmt on the two
cancellation-wrapper hunks; no benchmark artifact or measurement is claimed from either
head. The `53dd7e3` run `40726852161` (test job `122952317914`) passed every test-job step,
including Vitest, but its scope benchmark job `122969188541` failed all five semijoin
`chat open` repetitions with `selectionBatches: 0`: that scenario mutated assignment/tagged
items while its eligible companion was `record_date_index`. The partial artifact is
`12836273892` (SHA-256 `b55b35af476e2606de62afe6e955099becf926dbc3397e850cae7702ac4a0953`),
so no performance result is claimed. The `9e9171e` run `40730004632`
(test job `122963430874`) failed the new cancellation-compensation compile at
`apps/engine/src/engine/mod.rs:2917` (`expected Result<(), anyhow::Error>, found
Result<(), CatalogWriterGone>`); its rustfmt step was also red on the new rollback hunk.
The same compile failure made the group and Rust lanes red and caused Vitest setup to fail;
the Electric image job `122963429315` was red as well, so no benchmark artifact was produced.

The follow-up `ccb02c6` run `40733299001` (test job `122975056498`) repeated the
`drop_group_durable` type mismatch at `apps/engine/src/engine/mod.rs:2917`
(`expected Result<(), anyhow::Error>, found Result<(), CatalogWriterGone>`) before running
the cancellation contracts; its benchmark `122977296090` was skipped and the Electric image
job `122975057785` failed. The subsequent `89a0b97` head added a shutdown party around detached
compensation, a one-shot member-release re-enqueue, and the steady companion touch, but
retained the same `drop_group_durable` type mismatch. Its test job `122988431056` failed at
that compile error, its scope benchmark `122993623042` was skipped, and its engine image job
`122988431228` failed. The `dba8b19` run `40741829458` (test job `123005145542`) was compile/red in formatting,
group and Rust lanes; its Electric image was also red and Vitest was still running. The
follow-up `c9ccae9` applied the barrier wiring and workload corrections; its exact run
`40743452180` (test job `123010867174`) was red in group and Rust lanes because cleanup used
invalid PostgreSQL publication syntax, while formatting, TypeScript and Node gates were green;
no benchmark result was produced. The subsequent `fb6c7ae` run `40746277581` was red only in the formatting gate; its group,
Rust, TypeScript and Node steps passed. `48501cd` then made compensation single-shot after
writer loss, retained failed members in the guard, and collected concurrent-contract errors;
its test run `40747358562` (job `123024579773`) passed formatting, both membership-mode group
contracts, Rust engine tests, TypeScript, Node and Vitest. Its benchmark job `123037684019`
also completed successfully. The current candidate keeps the valid plain table cleanup,
preserves the one re-enqueue attempt during clean shutdown, and retains each member in place
until its release succeeds.

Earlier exact jobs remain part of the ledger: group contracts passed in both modes at
`8c0c6f0` (job `122831109853`), `88b00cc` (`122836183365`) and `1c2926a`
(`122838729992`), while their rustfmt and library lanes were red. Heads `661babb` and
`87076ff` were compile-red. These entries are evidence boundaries, not a claim that the
current candidate has passed. The follow-up adds completion durability compensation, shutdown-owned retry coverage and
cancellation-stage retry coverage. The c78804a run `40748888867` compiled and passed
formatting, Rust, TypeScript and Node but its group step failed in
`concurrent_group_cancellations_preserve_both_durable_drops` with `the slot is held by
walsender pid 5267` during replacement restart; the restart helper now retries that transient
`SlotBusy` state. Heads `82f2cd2` and `9c5b821` then exposed compile/format failures in the new
rollback contract. Head `9086025` fixed those blockers. Reviewed head `8a93697` adds the bounded
slot-busy retry and the second member-release cancellation barrier; its exact-head CI is the
remaining correctness gate. No performance claim is made beyond the completed `48501cd`
characterization above.

The page/live handoff keeps ordinary stale-query-back recency separate from a bounded
`page_decisions` map. Query-back decisions use their read LSN for snapshot ordering and retain
the triggering commit LSN for synthetic tombstone headers. The handoff guard balances its state
on request cancellation, and tombstones are delivered in commit-LSN order. Empty inner-flip
query-backs do not scan the whole companion table; they remain a no-op unless a scoped decision
exists. The cancellation contract now also aborts two all-plain group creates at the completion
barrier, closes the engine, and retries both subscriptions after restore. The counted-party
unit test is the direct proof of multiplicity; the concurrent contract covers end-to-end
cleanup and restart but is not claimed as an isolated red/green proof of the count.

The perf2 `gateway.ts`, `phone.ts`, and `run.ts` harness files have now been
imported directly from the pinned perf2 commit, without its older engine code.
Their source blobs are respectively `ec9699db0c3393d55601a5996ec4e27fe4e69af9`,
`81251167803ff4bb307f1d0913afe721381c11cc`, and
`d5dfd0aa57372b90b820ba39f0ef0982468e6b88`. The subsequent `run.ts` change
repairs its Markdown table delimiter to match the 18 header/row cells.
The old Node sketch is explicitly named `semijoin:model` in package scripts. The corrected
five-repetition mode comparison is now measured in artifact 12844722320. Its modeled
streaming charges must not be mistaken for provider reads.

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
The join and reparenting controls passed at `7cbda3f`; the delete-gate and shared-worker
corrections are implemented through `9cffab2`, while the page-handoff and cancellation
corrections are carried through the subsequent reviewed heads. Head `48501cd` passed the
full test job and produced the benchmark artifact; head `8a93697` adds the slot-busy stabilization
and second-barrier rollback contract, with its full exact-head qualification still running.
Existing claim/lease, public snapshot query,
group stream and commit fence code is unchanged.

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
| Snapshot-only move-out and wire LSN | Exact delete after receipt and LSN on each membership event | Both green in both modes at `327653b` (run `40720275270`); `53dd7e3` green, `9e9171e` fmt-red |
| Companion reparenting | Table-qualified delete/upsert/delete after individual source receipts | Passed both modes at `327653b` (run `40720275270`); `53dd7e3` green, `9e9171e` fmt-red |
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

The first completed five-repetition run does not meet the decision threshold. Semijoin DBSP
used bytes are about 76% higher cold and 45% higher steady, delivery p50/p90 is about 2.7–2.8×
slower, and engine CPU is about 60% higher; process RSS is similar and restart cost is
unavailable. Keep the accepted subquery-members decision in place. Revisit semijoin only with a
restart lane or a per-group scaling run that demonstrates lower DBSP memory and restart cost on
the same target-version inputs. ec#44's documented lifecycle, cancellation and burst-pressure
gaps also prevent treating either membership experiment as production-ready.
