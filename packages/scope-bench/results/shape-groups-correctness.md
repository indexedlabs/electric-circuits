# Shape groups correctness prototype

This branch starts at `c1d035e` (`prototype/shape-groups`). The engine now has
real PostgreSQL 18 contracts for a group containing a plain member and an
`IN (SELECT ...)` member, plus the lifecycle and restart cases. It is still a
prototype and nothing here is merge-ready.

## Verdict

The tests are green in CI, but the hosted review found lifecycle and backpressure gaps that make the current group implementation incorrect under lease lapse, cancellation, and high fan-out. Treat the CI rows as regression coverage only; do not recommend groups for production until those gaps are fixed. The key-only semijoin was not started as a DBSP runtime, so this round gives no semijoin performance conclusion.

## CI evidence

The genuine-red run is [40605454944](https://github.com/indexedlabs/electric-circuits/actions/runs/40605454944), where the delayed-flip and equal-key-delete contracts timed out with the group stream containing only one member envelope. The green code run is [40607189616](https://github.com/indexedlabs/electric-circuits/actions/runs/40607189616); the final report head also passed in [40610470515](https://github.com/indexedlabs/electric-circuits/actions/runs/40610470515). These runs compiled the engine, ran all 19 ignored source-table contracts against PostgreSQL 18, typecheck, the Node gate, and Vitest.

## Correctness cases

All tests live in `apps/engine/tests/sources_table/ownership.rs` and are run by
the CI command `cargo test -p electric-circuits-engine --features test-support
--test sources_table -- --ignored`. Each child process creates fresh tables,
publication, replication slot, and Durable Streams store. The source-drain
case writes a real `circuits_source_fence` row in the same PostgreSQL
transaction and waits for the engine's durable receipt.

| Case | Test | Result in green run | Fix and size |
| --- | --- | --- | --- |
| Delayed inner flip from T before plain output from T+1 | `ownership::group_delayed_subquery_flip_is_ordered_after_plain_output` | Pass; two envelopes carry distinct commit LSNs in order | Active-group previous-transaction fence plus bounded emission lane; M |
| Source-drain receipt covers the group lane | `ownership::group_source_drain_receipt_covers_deferred_lane` | Pass; receipt is observed only after the group envelope lands | Reused the pending-flip barrier and fail-closed queue; S |
| Equal key deleted from two member tables | `ownership::group_delete_names_table_for_equal_keys` | Pass; both table-qualified delete envelopes arrive | Table-qualified delete envelope regression; S |
| Group claim overlaps an existing group | `ownership::group_claim_overlap_is_atomic_and_returns_409` | Pass; second claim is 409 and a later claim succeeds | Atomic reservation/rollback under the engine state lock; M |
| One snapshot, horizon, and replay fence | `ownership::group_members_share_one_snapshot_horizon_and_replay_fence` | Pass; one response fence covers both pages | Durable `GroupRecord` stores the shared fence; M |
| Member retirement / TRUNCATE reload | `ownership::group_retirement_reloads_and_closed_stream_is_409` | Pass for TRUNCATE retirement and fresh group reload | Group teardown releases all private members and records `GroupDropped`; M. The old closed-stream read must still be checked through the gateway/client. |
| Plain and mixed groups across restart | `ownership::group_restart_restores_plain_and_drops_mixed_groups` | Pass; plain claim conflicts after restore, mixed claim can be recreated | Durable member list; mixed groups with non-persisted subquery state are dropped as one unit; L |

The queue is bounded by `ELECTRIC_CIRCUITS_EMIT_QUEUE` (default 1024). A full
lane degrades the engine instead of dropping a membership batch or reporting a
false drain receipt.

## Key-only semijoin

Not started. `packages/scope-bench/src/semijoin.ts` is only a rejected-
`NOT IN`/NULL sketch; it is not a DBSP runtime template. There are no engine
state bytes, RSS, reseed-time, move-in-latency, or production-size measurements.

## Hosted review findings

The hosted static review ([5676655751](https://github.com/indexedlabs/electric-circuits/pull/44#pullrequestreview-5676655751)) identified five implementation gaps that supersede the optimistic table results: group claims do not renew or release on lease lapse/retirement, the bounded emission lane degrades on ordinary burst pressure, group reservation and completion are not cancellation-safe, completion lacks a post-durability generation check, and group stream paths can be re-minted after restart. The delayed-flip test also used a source receipt between T and T+1, so it did not prove the new fence. These are accepted as prototype blockers, not waived correctness risks.

The benchmark semijoin result artifact and config have been removed; the semijoin remains explicitly not started.

## What remains unproven

- Gateway `subsets/load` to a real routed batch, SDK envelope `type` routing,
  and one transaction across two GRDB tables.
- Capability tokens and gateway re-authorization on every group long-poll.
- The client-visible 409 behavior after a group stream is closed by schema
  drift/TRUNCATE.
- A DBSP key-only semijoin, including explicit `NOT IN`/NULL semantics,
  checkpoint/reseed, and 12k-record production-size memory and restart data.

## Recommendation

Do not recommend or publish the current group implementation. First fix the hosted-review lifecycle, cancellation, per-engine fencing, and backpressure findings and rerun the red/green contracts without an intermediate receipt. Then build the real semijoin runtime and compare measured state/reseed/move-in/write costs against corrected groups before choosing the membership strategy.
