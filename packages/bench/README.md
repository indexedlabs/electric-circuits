# @electric-circuits/bench

Benchmark and measurement tooling for [electric-circuits](../../README.md). Every runner boots the full
stack itself (durable-streams + engine, plus Postgres/API where relevant); results are written to
files under `docs/bench/` or the package directory.

## ElectricSQL benchmarking-fleet (`src/electric-bench-runner.ts`)

Runs the **unmodified** `.exs` benchmarks from
[electric-sql/benchmarking-fleet](https://github.com/electric-sql/benchmarking-fleet)
(`byo_electric` mode) against our `/v1/shape` adapter: boots the stack on an ephemeral Postgres,
seeds each benchmark's schema at scale, points the script at our engine, collects its statsd/UDP
telemetry, and reports latency percentiles + throughput as markdown.

```bash
pnpm bench:fleet          # from the repo root — auto-clones the fleet repo on first run
```

| Env | Default | Meaning |
|---|---|---|
| `BENCH_SCALE` | `1` | workload multiplier |
| `BENCH_ONLY` | *(all)* | comma list of benchmark names to run |
| `FLEET_DIR` | `../benchmarking-fleet` | path to a benchmarking-fleet clone (auto-cloned when absent) |
| `FLEET_REPO` | electric-sql/benchmarking-fleet | clone source |
| `BENCH_OUT` | `docs/bench/electric-fleet-results.md` | report path |
| `EXTERNAL_ELECTRIC_URL` | *(unset)* | run against an external Electric-compatible server instead of booting our stack (requires `EXTERNAL_DATABASE_URL`) |
| `EXTERNAL_DATABASE_URL` | *(unset)* | the external target's Postgres; benchmark tables are dropped/recreated there |

**macOS latency note:** the stack boots the real `durable-streams-server` binary (`@electric-circuits/ds-rust`)
instead of an in-process store. That binary's `--durability memory` mode is Linux-only (a zero-copy
socket→file path); on macOS every append falls back to disk-durable `wal` mode, which noticeably
inflates latency on shape-creation-heavy benchmarks (e.g. `concurrent_shape_creation_with_subqueries`).
This is a platform artifact, not an engine regression — verified by running the same workload in a
Linux container with `ELECTRIC_STORAGE=MEMORY`, which reproduced the original (pre-macOS-fallback)
latency. Don't read macOS-local numbers on these benchmarks as regressions; compare against the
hosted fleet (Linux) instead. (fresh benchmarks pending)

## Shape-memory matrix (`src/shape-mem-matrix.ts`)

How engine memory evolves as shapes are created, across deployment sizes: seeds N issues + a
project/membership graph (the LinearLite visibility model), creates subquery-visibility and
status-equality shapes in batches, and samples the engine's `GET /memory` probes (RSS +
cardinalities) at each milestone. Results are written to `docs/bench/shape-memory-matrix.md`.

```bash
pnpm --filter @electric-circuits/bench shape-mem
MATRIX_SIZES=1000,10000,100000 MATRIX_USERS=10,25,50,100 MATRIX_PROJECTS=20 \
  pnpm --filter @electric-circuits/bench shape-mem
```

### Direct per-table live transport comparison

`src/shape-mem-scale.ts` can hold the same per-table feeds against Durable Streams with either
transport. `SCALE_LIVE_MODE=long-poll` is the default; set `SCALE_LIVE_MODE=sse` for a real
`text/event-stream` read. The SSE worker advances only from `control` events' `streamNextOffset`
and reconnects from that opaque cursor when the server ends a response. Both modes report upstream
read count, peak held connections, total reconnects per ramp increment and an estimated final-level
rate (the sum of per-increment rates), reconnects per subscription minute and write-to-delivery p50/p90
when writes are enabled. For gateway sizing at the peak, run one level at the peak subscription count
without a ramp; that single-level total reconnects/min is measured over one common observation window.

```bash
SCALE_LIVE_MODE=sse SCALE_LIVE_SUBS=100 SCALE_LIVE_WRITES=20 \
  SCALE_LIVE_WRITE_INTERVAL_MS=500 SCALE_OUT=../../docs/bench/per-table-sse.md \
  pnpm --filter @electric-circuits/bench exec tsx src/shape-mem-scale.ts
```

The default observation hold is 120 s so an SSE response that the server closes at about 60 s can
be observed reconnecting; override `SCALE_LIVE_HOLD_MS` only when the observation window is part of
the recorded comparison. Before that hold begins, every subscription must report its stream
up-to-date fence. `SCALE_LIVE_READY_TIMEOUT_MS` defaults to 120 s (20k long-poll subscriptions
may need more time for catch-up). A run that misses the fence exits 1 and does not write a results file.
Run the same command with `SCALE_LIVE_MODE=long-poll` and the same seed, shape and write settings
to compare direct engine/Durable Streams behavior. This bench does not model gateway CPU or pool
limits; those measurements require the real Indexed dev load balancer.

## Other runners

- **`src/run.ts`** (`pnpm bench` from the root) — local stress benchmark: many equality shapes,
  a write firehose, a write→shape-update latency prober (p50/p99), and a resource sampler.
  Env: `BENCH_SHAPES` (1000), `BENCH_SUBS` (100), `BENCH_DURATION` (10 s), `BENCH_CONC` (64),
  `BENCH_REGCONC` (64), `BENCH_OUT`.
- **`src/electric-adapter.ts`** — boots the stack with the `/v1/shape` adapter for external
  drivers: standalone (seeds Electric's standard `level_1..4` schema, for curl smoke tests) or
  driven by Electric's Elixir harness (`ADAPTER_PG_URL`/`ADAPTER_PG_TABLES` provided by it; prints
  `ADAPTER_LISTENING <url>`). Used by [electric-conformance/](../../electric-conformance/README.md).

Companion: [packages/loadgen](../loadgen) (state-machine load-generator clients,
Docker-scalable). Benchmark reports are written under `docs/bench/`.
