# Performance and capacity audit

Status: research note, 2026-08-22. This is a static audit: it did not run a long benchmark,
load test, or soak. `Verified` means source, configuration, test, or retained historical artifact;
it does **not** mean production performance has been demonstrated. Numbers labelled *proposed* are
acceptance criteria to agree before launch, not current product SLOs.

## Bottom line

There is good capacity *mechanism* and observability, but no current, reproducible performance
evidence for this revision or a like-for-like upstream comparison. The repository has deliberately
removed the old `docs/bench/` reports as stale ([commit ad330f8](https://github.com/mwildehahn/electric-circuits/commit/ad330f877e513163b99b1563576f37dc7f35f028)); the current docs repeatedly say “fresh benchmarks pending.”
CI runs correctness/type tests only; it does not execute a benchmark, load generator, or regression
budget. Do not derive a launch capacity, latency SLO, or “faster than upstream” claim from the
present tree.

## Evidence ledger

### Current, verified implementation facts

| Area | Verified fact | Capacity implication |
|---|---|---|
| Measurement tooling | `packages/bench` has a local firehose/latency runner, a shape-memory matrix/scale runner, and an unmodified Electric `benchmarking-fleet` driver. `packages/loadgen` exercises real Postgres writes and client long-polls and records CPU/RSS/PG/DS disk every 2 s by default. | The right harnesses exist, but their output directories (`docs/bench/` and `results/`) contain no committed current run. |
| Replication | The engine exports slot retained-WAL bytes and confirmed-flush lag bytes, sampled roughly every 10 s on a pooled connection. | Ingest lag and source-WAL pressure are observable, but no observed lag-at-rate curve exists. |
| Large transactions | The ingestor spills a transaction after **128 MiB** by default and sends change-log chunks of at most **64 MiB** (hard maximum request body: **1 GiB**). It acknowledges the slot only after the final chunk lands. | This bounds ingestor memory plus one chunk, but **not** the sequencer's held transaction/read-page/pending-output memory or its transaction HOL delay. Spill storage must fit the largest source transaction. |
| Backfills | Native shape backfill streams with a **16 MiB** append budget; a per-create Postgres timeout is off by default. | Native backfill avoids collecting all matching rows. A subquery seed still retains its inner set; `/v1/shape` snapshot construction can require a whole protocol response body. Concurrent creates contend for the same PG pool. |
| PG/query-back concurrency | Backfills and query-backs use a **20**-connection pool by default. Deferred flip propagation has **8** workers by default. | This prevents unlimited DB fan-out, but gives no demonstrated queueing/latency budget under a membership-flip storm. |
| Output flush | The sequencer waits for every source transaction's output to land before processing the next transaction and permits at most **32** concurrent durable-stream appends per flush wave. | Durable-stream latency and fan-out are on the critical write path; a high-fan-out commit causes head-of-line blocking. |
| Shape lifecycle | Default `MAX_SHAPES` is **10,000**; idle is **30 min**, dormant TTL **7 days**, sweep **60 s**. Only dormant shapes are evicted; `MAX_SHAPES=0` is unlimited. | This is a useful admission/retention control, not an absolute active-shape memory or disk bound. If all shapes remain active, the cap produces pressure/rejections rather than eviction. |
| Change-log / output disk | Input segments rotate at **1 GiB** or **1 day** and a dormant shape can pin them for **7 days** by default. Shape-stream disk budget is disabled by default and its accounting resets on restart. | Size durable storage from measured input rate, retention/pins, and unbounded active shape output—not only the 1 GiB segment value. No current write-amplification or fsync rate is measured. |
| Circuit state | Counts are O(distinct configured groups); equality routing is per template plus a per-shape entry; subquery contributor state is O(inner result) and shared by query signature. Feed membership is a per-shape Roaring pk set. | The circuit does not structurally grow per subscription, but shape metadata/feed sets, high-cardinality aggregate groups, unique inner queries, and output streams still grow with workload cardinality. |
| Connection model | Each client subscription normally holds one DS long-poll. The loadgen estimates `USERS × FEEDS_PER_USER`; with four feeds it documents roughly **3–4k users per client node** before a macOS single-destination ephemeral-port ceiling. Engine `/v1/shape` live polls have a **20 s** deadline; DS test default is **30 s**. | Connection/FD/proxy limits are a separate capacity plane from engine CPU. The 3–4k figure is a client-host rule of thumb, not a server scalability measurement. |
| Client memory | The client maintains subscribed result rows locally; subset queries are the bounded/windowed alternative. | No browser/mobile RSS, retained-row, reconnect, or slow-consumer measurement is present. Large materialized feeds must be budgeted at the client as well as the server. |

Sources: `apps/engine/src/{txn_buffer.rs,pg.rs,retention.rs,metrics.rs,engine/{mod.rs,sequencer.rs}}`,
`docs/{ARCHITECTURE.md,deployment-postgres.md,ivm-engine-internals.md,memory-model.md}`, and
`packages/{bench,loadgen}/`.

### Historical measurements: retained only as invalidated context

The removed July 2026 reports are available in Git history, not the working tree. They ran on
macOS/arm64 and predate material changes (notably the feed-set representation and benchmark reset),
so they cannot establish current capacity or an upstream comparison.

| Historical artifact | What it recorded | Why it is not launch evidence |
|---|---|---|
| `docs/bench/electric-fleet-results.md` before ad330f8 | Five `benchmarking-fleet` workloads against this adapter: p99 **27 ms** (diverse fanout), **33 ms** (write fanout), **25 ms** (one-client latency), **556 ms** (concurrent creation), and **2,321 ms** (subquery creation). | One macOS run on 2026-07-13. It contains no same-machine stock-Electric row, commit/version pin, durability mode parity, repeated samples, or current-code rerun. |
| `shape-memory-matrix.md` before ad330f8 | A 10k-shape, 10k-issue row reported **645.8 MiB** RSS and a 3k-row materialized backfill peak of **55.3 MiB**. | The report itself contains contradictory generated prose about per-shape cost, only one deployment-size row survived, and all current documentation marks fresh measurements pending. |
| `shape-memory-scale.md` / `mem-attribution-100k.md` before ad330f8 | Earlier configurations reached 100k subscriptions / 50,005 distinct shapes and reported several hundreds of MiB to roughly 1 GiB of macOS footprint; PR #39's subject says “−40% @100k subs.” | Explicitly superseded by the host-side Roaring feed-set change and bounded cache changes. Old process footprint is allocator/platform sensitive and cannot be compared to current Linux production. |

The current `packages/bench/README.md` also records a qualitative macOS-versus-Linux durability
difference: macOS uses disk-durable WAL where Linux can use memory durability for a test run. That
is a warning against comparing those timings, not a measurement of production fsync throughput.

### Upstream comparison status

`pnpm bench:fleet` can point at any Electric-compatible endpoint via
`EXTERNAL_ELECTRIC_URL` plus `EXTERNAL_DATABASE_URL`; this is the correct comparison mechanism.
There is no checked-in paired report for this repository revision versus an upstream Electric image,
and the only configured Git remote points to `mwildehahn/electric-circuits` (there is no separately
configured upstream reference). Therefore:

- **Verified:** the harness supports an apples-to-apples protocol workload when both targets use
  the same benchmark seed and host class.
- **Unmeasured:** current Circuits-versus-upstream throughput, create/read/update percentiles,
  memory, CPU, disk, and failure/recovery behavior.
- **Required discipline:** pin upstream image digest/commit, Circuits SHA, durable-stream mode,
  Postgres version/configuration, instance size/kernel, benchmark-fleet revision, and all env
  variables. Run at least three repetitions per point and publish raw samples and percentiles.

## Bottlenecks and bounds to prove

1. **Single sequencer and transaction atomicity.** The sequencer intentionally completes all
   output appends for transaction N before N+1. It protects visibility but makes large source
   transactions, slow durable storage, and high fan-out global latency amplifiers. The 128 MiB
   spill threshold protects only the receiver; test sequencer RSS and small-write latency behind
   transactions above that threshold.

2. **Durable-stream write path and disk.** `append_reliable` retries; correctness wins over
   latency when DS is slow. Measure fsync/group-commit throughput, append size distribution,
   write amplification (input log + every output feed + catalog), tail/read concurrency, and
   recovery time. The current loadgen records DS directory bytes and append p99, but not fsync
   latency, DS CPU/FDs, queue depth, or output-feed retention/compaction.

3. **Replication lag and Postgres WAL.** Confirmed-flush lag is only sampled roughly every ten
   seconds, which is too coarse as the sole fast alert for bursts. Correlate it with actual
   end-to-end commit-to-client delay and `restart_lsn` retained WAL. Test DB failover, DS outage,
   and catch-up while the source keeps writing; capacity is insufficient if lag grows at the
   planned steady load.

4. **Backfill/create storms.** Streaming bounds a *single* native backfill chunk, but each create
   uses the 20-connection PG pool, may hold a repeatable-read snapshot, and performs durable
   writes. Test cold starts and reconnect waves with realistic result widths/cardinalities.
   Separately test Electric snapshot requests, whose response is not equivalent to the streamed
   native path.

5. **Shape cardinality and sharing.** Identical native shapes share maintenance/stream; the
   Electric adapter deliberately uses `share=false`. Measure both distinct predicates and repeated
   equivalent subscriptions, active versus dormant ratios, and `MAX_SHAPES` behavior when nothing
   is dormant. Do not extrapolate from subscriptions to distinct shapes without measuring the
   application’s sharing ratio.

6. **Fallback and subquery fan-out.** Equality/range indexes avoid work for unrelated shapes;
   unindexable `OR`/`NOT`/`LIKE`/`!=` predicates remain an O(K) fallback scan. An inner-value
   flip can query and emit every matching outer row for every dependent edge. Exercise
   low-selectivity values, many unique inner signatures, NULL/negated predicates, and bursty
   membership changes; capture candidate count, query-back queue/wait, rows fetched, and pending
   flips. Those queue metrics are not currently exported as a complete capacity surface.

7. **Aggregates/circuit groups.** A counts pipeline grows with distinct group keys, not total rows,
   but group cardinality can approach row cardinality. Dynamic aggregate folds and fallback also
   have per-shape candidate work. Measure configured and unconfigured aggregate families over
   high-cardinality keys, not only LinearLite's small project/status cohorts.

8. **Connection and client limits.** Exercise DS/proxy/server FDs, keep-alives, long-poll timeout
   churn, reconnect storms, slow readers, and browser/mobile retained result memory. The existing
   loadgen simulates user actions but is not evidence for tens of thousands of real internet
   connections or client device bounds.

## Benchmark and soak matrix

Run the following on Linux production-like hardware with file-backed durable streams and the real
Postgres configuration. Each row should report workload input/output bytes, p50/p95/p99/p999/max
for create and commit-to-client propagation, sustained rate, lag, RSS/CPU, PG CPU/IO/WAL,
DS CPU/IO/FDs/disk, client RSS, errors/retries, and recovery time. Keep raw CSV/JSON plus a
machine-readable manifest in version control or durable CI artifacts.

| Test | Matrix dimensions | Required comparison / assertion |
|---|---|---|
| Protocol parity | Every `benchmarking-fleet` workload at scale 1 and target scale; current Circuits and pinned upstream | Same host/config/seed; compare percentile distributions and correctness, not one wall-clock run. |
| Core write throughput | 1×/2× planned steady writes; hot key versus uniform key; fan-out 1/10/100/target max; small and wide rows | Find sustainable rate before confirmed-flush lag grows; include DS durable WAL and memory-only only as separately labelled modes. |
| Shape/register memory | 1k/10k/100k **distinct** shapes and separately 1k/10k/100k subscriptions to shared shapes; changes-only and materialized; active/dormant split | Re-run the matrix/scale harness with current code; report owned bytes and RSS after warm-up, not allocator-sensitive peak alone. |
| Connection scale | 1k/10k/target concurrent long-polls, several client nodes; idle, update fan-out, reconnect storm, slow readers | Prove DS/proxy/engine FD and connection budgets; measure client node port exhaustion and listener recovery. |
| Backfill/create storm | Result sizes 10k/100k/production maximum rows and narrow/wide projections; 1/20/100 concurrent creates; native and `/v1/shape` | Bound create p99, PG snapshot/vacuum impact, pool queueing, DS disk burst, and failure/timeout cleanup. |
| Large transaction / HOL | 64 MiB, 128 MiB, 2× cap, and the largest allowed production transaction; concurrent small writes | Assert spill/chunk counters, no lost/partial visibility, sequencer peak RSS, scratch disk use, and small-write latency while it drains. |
| Subquery / fallback | Inner cardinality/selectivity from 1 to hot value; dependent edges 1/10/100; outer fan-out up to target; indexed versus fallback predicates | Measure query-back pool wait, `pendingFlips`, catch-up time, and p99 propagation. No capacity claim without this row. |
| Aggregate groups | Low, medium, and near-row-cardinality group keys; circuit-served and dynamic aggregate predicates | Establish memory/disk/CPU per group and per aggregate subscriber; confirm circuit sharing assumptions. |
| Disk/WAL retention | Sustained writes plus dormant shapes at several resume ages; output feed consumers on/off; retention caps on/off | Measure actual bytes per input byte, output growth, rotation/deletion, source retained WAL, and behavior when a disk fills or DS restarts. |
| Soak and recovery | At target concurrent users/shapes and 70% planned steady write rate for **72 h**; a separate **24 h** 2× burst/catch-up run | No monotonic RSS/FD/disk/lag growth after warm-up; inject PG/DS/network restarts, pod termination, slot reconnect, and client reconnect storms. |

## Proposed launch acceptance envelope (not observed SLOs)

Product owners should replace these starter numbers with user-facing requirements. Until then, a
reasonable minimum evidence gate is:

- At the declared capacity unit (concurrent active subscriptions, distinct shapes, result rows,
  write rate, fan-out, and row width), sustain **70%** of planned peak for **72 h** and **2×** it for
  **24 h** without correctness divergence, unbounded resource growth, or manual intervention.
- At 70% load, end-to-end Postgres commit to durable shape append p99 **≤ 2 s** and p999 **≤ 10 s**;
  at 2× load, lag may rise but must recover to the 70%-load band within **5 min** after the burst.
  Report direct client-observed latency separately from engine append timing.
- Confirmed-flush lag should be **< 30 s** at steady state and retained WAL should remain below
  **25%** of the configured `max_slot_wal_keep_size`; warning at **10%**, page at **25%** are
  conservative starting alert thresholds. Calibrate them to WAL generation rate and recovery SLO.
- Provision at least **30%** memory and disk headroom at the 72-hour high-water mark; durable-stream
  disk must additionally cover the measured worst-case retention/input window and output-feed
  growth. Do not use the disabled/default shape-disk budget as proof of a quota.
- Demonstrate **zero lost committed changes** and convergence to the SQL oracle through every
  injected restart/failure, including a transaction over the spill threshold and a high-fan-out
  membership flip. Define the actual RPO/RTO explicitly; the architecture aims for replay rather
  than a numeric recovery guarantee.
- Publish a capacity table giving, for each deployment size, the measured maximum sustainable
  writes/s, output appends/s and bytes/s, active long-polls, distinct shapes, active/dormant ratio,
  group cardinality, peak engine/PG/DS CPU/RSS/disk/FDs, and the limiting component. No number
  should be advertised as a server limit until this table exists for the production topology.

## Recommended operational dashboard additions

Existing `/metrics`, `/memory`, Prometheus output, and loadgen CSV cover useful foundations:
envelopes/appends, process/append histograms, replication lag, spill/chunk counters, retained
segments, shape/subquery cardinalities, engine RSS/CPU, and PG/DS directory bytes. Add or derive
the following before relying on the system at scale: sequencer transaction age/bytes and queue
depth, flush-wave wait/in-flight counts, PG pool in-use/wait time, flip queue depth/age and
query-back rows/latency, DS append/read queue/FD/fsync/IO metrics, active long-polls, per-stream
backlog/age, backfill concurrency/rows/bytes/duration, client reconnect/error rates, and capacity
rejection reasons. Alert on slope as well as absolute values; an ever-growing lag, disk, or FD
series is more actionable than one sampled p99.
