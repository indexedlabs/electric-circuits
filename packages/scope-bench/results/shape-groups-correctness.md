# Shape groups correctness prototype

This branch starts at `c1d035e` (`prototype/shape-groups`). The engine now has
real PostgreSQL 18 contracts for a group containing a plain member and an
`IN (SELECT ...)` member, plus the lifecycle and restart cases. It is still a
prototype and nothing here is merge-ready.

## Verdict

The group correctness slice is green in CI. Keep groups as the transport and
lease abstraction for the next round, but do not enable them for production:
the gateway/SDK route, capability re-authorization, and closed-stream client
reload contract are still unproven. The key-only semijoin was not started as a
DBSP runtime, so this round gives no semijoin performance conclusion.

## CI evidence

The genuine-red run is [40605454944](https://github.com/indexedlabs/electric-circuits/actions/runs/40605454944), where the delayed-flip and equal-key-delete contracts timed out with the group stream containing only one member envelope. The green run is [40607189616](https://github.com/indexedlabs/electric-circuits/actions/runs/40607189616); its Rust job ran all 19 ignored source-table contracts against PostgreSQL 18, followed by typecheck and the Node gate.

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

## What remains unproven

- Gateway `subsets/load` to a real routed batch, SDK envelope `type` routing,
  and one transaction across two GRDB tables.
- Capability tokens and gateway re-authorization on every group long-poll.
- The client-visible 409 behavior after a group stream is closed by schema
  drift/TRUNCATE.
- A DBSP key-only semijoin, including explicit `NOT IN`/NULL semantics,
  checkpoint/reseed, and 12k-record production-size memory and restart data.

## Recommendation

Continue with groups as the shared transport and lease layer. Build the real
semijoin runtime next and compare its measured state/reseed/move-in/write costs
against groups-with-subquery-members before choosing the membership strategy.
Do not publish or merge either path from this prototype.
