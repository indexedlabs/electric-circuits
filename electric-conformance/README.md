# Electric protocol conformance

Runs ElectricSQL's formerly-official oracle harness (`Support.OracleHarness` / `ShapeChecker` — its
comparison-against-Postgres logic) against electric-circuits's `GET /v1/shape` adapter, driven by Electric's
official Elixir `Electric.Client`. Current Electric main no longer ships those internal test-support modules, so
the exact last upstream copies required by the deterministic oracle suite are vendored under
[`vendor/electric-oracle-harness`](vendor/electric-oracle-harness/PROVENANCE.md), with source hashes and
Apache-2.0 license. The runner verifies their bytes before installing them into the disposable Electric checkout.

## Run

```bash
electric-conformance/run.sh            # all suites (oracle + property + subqueries)
electric-conformance/run.sh property   # or: oracle | subqueries
```

The script locates an ElectricSQL checkout (`ELECTRIC_DIR`, default `../electric`; cloned from
`ELECTRIC_REPO` when absent) at the pinned `ELECTRIC_REF` default
`2f11f91d6c580e47fb57924f5d3f7954329314d8`, builds our release engine, copies the test files and verified
vendored oracle support into `packages/sync-service/test/{integration,support}/`, and runs `mix test`.
Requirements: elixir/mix, Rust, and PostgreSQL binaries (`initdb`/`pg_ctl`) on `PATH`.
Each invocation owns an initially empty cleanup manifest. The adapter records its engine and durable-stream
PIDs plus a fresh durable-stream root, waits for `ds.stop()` on graceful exit, and the runner uses those
exact entries as a forced-exit fallback—never a process-name sweep or shared temporary directory cleanup.

Both boot our stack (durable-streams + engine + adapter) via `packages/bench/src/electric-adapter.ts`,
point Electric's official `Electric.Client` at it, and run Electric's own `OracleHarness`/`ShapeChecker`.
The property test additionally reuses Electric's `WhereClauseGenerator` + `StandardSchema` (DDL/seed/
mutation generators); the launcher's two-phase mode (`ADAPTER_WAIT_TABLE`) lets the test apply
StandardSchema's exact schema+seed before the engine introspects. `ELECTRIC_CIRCUITS_DIR` overrides the path.
Tunables: `ORACLE_RUNS`, `ORACLE_SHAPE_COUNT`, `ORACLE_BATCH_COUNT`, `ORACLE_MUTATIONS_PER_TXN`.

## Adapter behavior notes (`/v1/shape`)

- **Handle idle-TTL.** Every initial `GET /v1/shape` (offset=-1) creates or **rejoins** the engine
  shape for that definition (identical definitions share one shape, so the `electric-handle` is the
  shared shape id). Handles idle longer than **`ELECTRIC_HANDLE_TTL`** seconds (default `600`) have
  their handle state evicted by a background task; the shape and its stream are retained under the
  engine's retention lifecycle (idle shapes go dormant and are eventually evicted — see
  `apps/engine/README.md`). A later request on an evicted handle resumes it when it still names
  that shape, and otherwise gets the standard `409` + `must-refetch` control (the client
  re-snapshots, rejoining the retained shape). Set the env var on
  the engine process to tune it (e.g. `ELECTRIC_HANDLE_TTL=60`).
- **Errors.** Validation failures (bad `where`, unknown table/column, `offset` without a `handle`,
  table not matching the handle's shape) are `400` with an Electric-style `{"message": …}` body;
  transient/internal failures (durable-streams hiccups, backfill errors) are `500` so the client
  retries instead of killing its sync loop.
- **Live long-poll deadline** is the adapter's own **`ELECTRIC_LIVE_TIMEOUT_MS`** (default `20000`,
  matching Electric's ~20s live long-poll), decoupled from the durable-streams server's long-poll
  timeout (which just paces the adapter's re-poll loop). At the deadline the engine directly returns
  the current Electric-compatible `200` `up-to-date` control body with unchanged handle/offset and
  the `electric-up-to-date` header; a plain `200 []` would make the client busy-loop. The launcher
  (`electric-adapter.ts`) exposes that direct engine URL and sets the deadline from
  `ADAPTER_LIVE_TIMEOUT_MS`, defaulting to `ADAPTER_LONGPOLL_MS` (1000) so the oracle
  harness keeps its fast up-to-date detection; the fleet benchmark runner sets `ADAPTER_LIVE_TIMEOUT_MS=20000`.
- **Live-request coalescing.** Concurrent `live=true` requests at the same (handle, offset) are
  identical, so one leader performs the serialized read+apply and every concurrent request at that
  offset receives the same response (write-fanout: N clients long-poll one handle at one offset —
  serializing them behind the per-handle mutex would hand each a full long-poll timeout in turn).
  Non-live requests and other offsets keep the serialized per-handle-mutex path; only the leader
  mutates per-handle state.
- **`IS NULL` / `IS NOT NULL`** in `where` are fully supported: they map to the engine's native
  null-test predicate leaf (`PredicateJson::IsNull` — the one leaf that evaluates TRUE on a NULL
  cell), so they compose correctly under `NOT`/`AND`/`OR` and match Postgres semantics exactly.

## Electric's subquery integration tests (`subquery_move_out_test.exs`, `subquery_dependency_update_test.exs`)

These are Electric's **hand-written** subquery integration tests, run against electric-circuits via a setup
swap: `el_ivm_setup.ex` (`el_ivm_pg` + `el_ivm_client`) replaces `with_unique_db` + `with_complete_stack`
+ `with_electric_client` with our launcher-booted stack (engine introspects all tables via
`ELECTRIC_CIRCUITS_PG_TABLES=*`); **the test bodies and assertions are unchanged**. Run with
`electric-conformance/run.sh subqueries`.

**Result: 13 / 15 pass.** The 13 cover the real subquery behaviors — synthetic move-out deletes (parent
deactivation/deletion), move-in via a different parent, negated move-in/out, dependency tracking (team
moves between premium orgs with no spurious deletes), combined-condition move-in, resume-preserves-move-out,
and the stale-tag no-spurious-delete case. The **2 failures both assert Electric's row-`tags` mechanism**
(`assert %{headers: %{tags: [_]}}` / `assert new_tags != initial_tags`) — an Electric-internal protocol
detail electric-circuits deliberately doesn't emit (we use absolute membership emission, not row tags). They
are not membership/correctness failures.

## Compatibility scope

The repaired `oracle` lane preserves the independent Postgres comparison and official `Electric.Client` at
Electric `2f11f91d6c580e47fb57924f5d3f7954329314d8`. Its zero-restart grouped mutations are flattened only to
match the vendored harness's official flat-sequence API; the SQL journal and independent oracle are unchanged.
The optional generated `property` lane still references
`WhereClauseGenerator`, which is absent both from that pinned checkout and from the reachable last upstream
OracleHarness source. It is deliberately outside this minimal compatibility vendor; a future repair must source
that generator from a reachable official revision and add its own provenance before claiming property coverage.
