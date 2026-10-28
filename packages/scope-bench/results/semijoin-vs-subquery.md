# Key-only semijoin versus subquery members

Work in progress, not for merge. No semijoin implementation or performance results are
claimed by this initial test-only commit. The experiment must finish before this report
can support changing the membership decision.

## Pinned inputs and method

- Group baseline: ec#44, `8f7f07add3f3fd23cc92f94df021eca4c3b240c8`.
- Benchmark harness: `prototype/shape-groups-perf2`,
  `585e318410b53439cb03e832c3336a313f867f03`.
- Baseline DBSP is **0.318**; the requested proposal target is **0.365**. A version
  difference must be resolved or reported before comparing the two membership modes.
- Planned workload: roughly 12,000 Records, five independent repetitions of each
  mode, 30-phone steady writes and cold launch on real PostgreSQL 18 in CI.
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

## Correctness contract before implementation

This is a prototype implementation regression lane, not release qualification. The
existing ec#44 group tests are inherited controls. Two added contracts exercise
production code through real PostgreSQL 18, the native group API, the sequencer,
and ec#44's observed in-memory DS fixture:

1. `ownership::group_snapshot_only_member_emits_move_out`: take a group snapshot
   with the inner Record table and a separate companion table containing one row;
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
| Snapshot-only move-out and wire LSN | Exact delete after receipt and LSN on each membership event | Added regression contracts; execution pending |
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
