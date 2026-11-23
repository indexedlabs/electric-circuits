# @electric-circuits/conformance

The end-to-end conformance suite. The invariant it asserts, through the **real** stack (Postgres →
replication → engine → durable-streams → client), against the
[oracle](../oracle/README.md):

> For any shape and any op stream, the client-materialized set equals the oracle's
> `SELECT … WHERE <predicate>`.

## What the harness boots (`src/harness.ts`)

A vitest `globalSetup` builds the engine once and starts **one ephemeral Postgres** with
`wal_level=logical` (each harness then creates its own database + replication slot inside it, so
test files stay isolated). Per harness:

- a per-test Postgres database (the system of record *and*, via `createPgOracle`, the truth),
- a `DurableStreamTestServer` (the log),
- the Rust engine as a child process in Postgres mode (spawned from `target/debug/`, discovered
  via its `ENGINE_LISTENING` stdout line),
- the tRPC API (`createApiServer`) and a real `@electric-circuits/client`.

Comparison (`src/compare.ts`) is set equality over declared columns, keyed by stringified pk.

## Test areas

| Files | What they pin down |
|---|---|
| `conformance.test.ts`, `-postgres`, `-backfill` | core invariant: live replication, batched mutations, backfill ↔ live fencing |
| `conformance-concurrency.test.ts` | concurrent writers |
| `conformance-nulls.test.ts` | NULL three-valued logic (`NOT (col = x)` over NULL, `IS [NOT] NULL`) |
| `conformance-subquery*.test.ts` | `IN (SELECT …)`: scenarios, cross-table matrix, nested subqueries, inner-node sharing |
| `conformance-sharing`, `-shape-sharing` | identical shapes/feeds collapse to one stream, refcounted create/drop |
| `conformance-subset-positioning.test.ts` | subset LSN positioning: a change in the page/feed overlap window is counted exactly once |
| `conformance-subquery-positions.test.ts` | every live envelope of a subquery shape, a changes-only feed and a group stream carries `headers.lsn` + `txid`, positions never fall along a stream, and a flip's verdict that follows a newer live change still reaches the real client |
| `conformance-group-evaluation-order.test.ts` | a group stream (`/v1/subsets/load`) carries each key's envelopes in evaluation order: a nested flip's verdict that follows a newer live change to the same key is the stream's last word, and a verdict about other keys still lands in commit order |
| `conformance-group-rejoin.test.ts` | `POST /v1/subsets/rejoin` takes a released, lapsed or held group's claim again and answers a load's answer less the snapshot while every member table is locked (the same request as a load parks on the lock); a subscription with no group for those members is `412` and nothing is created or changed; a group re-joined inside each lease period never lapses; a release that names its claim (`&claim=…`) and arrives after a later session re-joined changes nothing, for a group, a feed and an aggregate, and a renewal that names its claim keeps it; a release that names no claim drops the later session's claim until its next re-join, and the cap may evict the group meanwhile |
| `conformance-swift-client.test.ts` | the Swift SDK's real collection stack, driven through its probe (`swift-probe.ts`; build it with `swift build --product ElectricCircuitsSwiftProbe` in the SDK checkout), against scopes shaped like the app's. One test per root cause, each ending equal to SQL with the probe's request counts asserted: a live change on a subquery scope applied in place; a relaunch that resumes a scope and a group under locks that would park any member query; a graceful engine restart during and between sessions with no snapshot, and one snapshot after a `kill -9`; removal of rows held only from the snapshot; an edit on the window's first UTC day on a non-UTC database; the nested-subquery ordering case on a group stream; idle retirement then exactly one snapshot, and a create refused at the shape cap retried after a release. Also writes the SDK's envelope fixture on request |
| `conformance-swift-client-resume.test.ts` | the same probe after a resume: a resumed scope and group report live at once when the stream holds nothing new and only after every missed change otherwise, under long-poll and SSE; a snapshot refresh reads the source again where a plain refresh continues from the cursor (a group by re-join, not by load); the SDK names the claim it holds when it renews and releases, so a replayed release from an earlier launch leaves a later launch's claim |
| `conformance-subquery-restart.test.ts` | subquery shapes survive a graceful restart: a materialized subquery shape, a changes-only subquery feed and a group with subquery members are opened, the engine is stopped with `SIGTERM`, and while it is down a row is edited, inserted, deleted and soft-deleted, access is revoked and granted on the inner table, and a row is moved to a hidden scope before its old scope is revoked; a client reading the SAME stream URLs from its saved offsets ends equal to SQL with no snapshot, no must-refetch marker, no re-sent untouched row and no falling position — again after a second stop and after a stop that follows a boot with no new change; a flip whose query-back is still in flight at the signal lands inside the subquery drain window and the shapes survive, while with `ELECTRIC_CIRCUITS_SHUTDOWN_SUBQUERY_DRAIN_SECS=0` it is abandoned and they are dropped; after a `SIGKILL` the shapes are dropped as before and the read reports the stream gone |
| `conformance-live-only-retention.test.ts` | live-only shapes stay bounded between restarts: an unsubscribed feed is retired after `ELECTRIC_CIRCUITS_LIVE_ONLY_IDLE_SECS` (reader released with `stream-closed`, then no subquery node and no Postgres query for it); at `ELECTRIC_CIRCUITS_MAX_SHAPES` every create route answers `503` + `Retry-After`, a join does not, and a release makes room; `DELETE /v1/shapes/{groupId}` releases a group's lease and the group is kept: the same subscription re-joins the same stream and reads what it missed from its earlier offset, also across a restart, until the idle period retires it as one unit |
| `conformance-subset-feed-scope.test.ts` | a subset feed judges a scope as its snapshot's SQL did, and removes rows the client holds only from the snapshot |
| `conformance-timestamp-zones.test.ts` | timestamps under a non-UTC database default (a DST zone, a half-hour zone): window predicates, and one spelling of an instant in backfill and live rows; a non-ISO `DateStyle` is refused at boot |
| `conformance-expressiveness`, `-transitions` | full predicate grammar, rows entering/leaving shapes |
| `conformance-fuzz`, `-fuzz-wide`, `-counterexample` | random-predicate fuzz vs the oracle + pinned counterexample replays |
| `harness-mechanics.test.ts` | the harness itself (boot, teardown, engine discovery) |

## Running

Requires PostgreSQL 18 binaries on `PATH` (`initdb`/`pg_ctl`) — the suite boots its own cluster.
From the repo root:

```bash
pnpm test:conformance     # the whole suite
pnpm test:fuzz            # the random-predicate fuzz test
pnpm loop [N]             # run the fuzz test repeatedly until failure (default 50 iterations)

# a fuzz failure prints `FAILED seed=<n>`; replay it exactly:
SEED=<n> pnpm exec vitest run packages/conformance/src/conformance-fuzz.test.ts
```

Fuzz tunables: `FUZZ_SEEDS` (scenarios per run, default 5), `FUZZ_SHAPES`, `FUZZ_OPS`, `SEED`
(base seed). `src/simulator.ts` generates the random schemas/predicates/op streams.

This suite tests the **extended** API; Electric's own protocol tests run separately from
[`electric-conformance/`](../../electric-conformance/README.md). Consistency model:
[docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md).
