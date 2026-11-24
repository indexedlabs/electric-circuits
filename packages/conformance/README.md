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
| `conformance-timestamp-zones.test.ts` | timestamps under a non-UTC database default (a DST zone, a half-hour zone) and a `SQL, DMY` role date style: window predicates, and one UTC ISO spelling of every `timestamptz`, `timestamp` and `date` — scalar, in an array, a range and a composite — in backfill rows and in live inserts, updates and deletes; a server-wide zone and style reloaded under a running engine do not reach its open replication connection; a literal for an array or range of timestamps is refused |
| `conformance-subset-feed-scope.test.ts` | a subset feed judges a `timestamptz` window as its snapshot's SQL did: a change to a row on the window's first UTC date reaches the client, in each spelling of the bounds the engine reads; a bound it cannot read (`… PST`, `Oct 19 2026`, `tomorrow`) is refused `400` by name on the feed, the subset query and the shape create |
| `conformance-subquery*.test.ts` | `IN (SELECT …)`: scenarios, cross-table matrix, nested subqueries, inner-node sharing |
| `conformance-sharing`, `-shape-sharing` | identical shapes/feeds collapse to one stream, refcounted create/drop |
| `conformance-subset-positioning.test.ts` | subset LSN positioning: a change in the page/feed overlap window is counted exactly once |
| `conformance-subquery-positions.test.ts` | every live envelope of a subquery shape, a changes-only feed and a group stream carries `headers.lsn` + `txid`, positions never fall along a stream, and a flip's verdict that follows a newer live change still reaches the real client |
| `conformance-group-evaluation-order.test.ts` | a group stream (`/v1/subsets/load`) carries each key's envelopes in evaluation order: a nested flip's verdict that follows a newer live change to the same key is the stream's last word, and a verdict about other keys still lands in commit order |
| `conformance-group-claim.test.ts` | a group load (`/v1/subsets/load`) answers `claim`; a load that names it (`?claim=`) keeps it, one that names none is answered a new one, and a release naming an earlier claim leaves the group a later load took |
| `conformance-subset-feed-removals.test.ts` | a subset feed removes rows the client holds only from the snapshot: soft-deleted, moved out of the window, deleted, or hidden through the inner table |
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
