M1: Synthetic cross-account starvation reproduced: Nov24 import took 528.49 s from first delivery at 98.50% CPU with 0/529 drain and live successes, versus Nov23 165.12 s; this reproduces the mechanism, not the incident's absolute duration.

# OTTO-13916 — scratch measurements, never merge

M2, M3 and M4 are not yet complete. The corrected production-binary baseline
below supersedes the provisional timing runs. All data are synthetic; no prod or
dev service was accessed. Nov23 also starves drain/live in this fixture, and clears
in 2.75 minutes, not the observed 15 minutes. Nov24 clears in 8.81 minutes, not
50 minutes. Thus M1 is a **partial reproduction of CPU-bound cross-account
starvation and catalog sensitivity**, not a claim to have recreated the whole
production incident. The calibrated case already stalls for many minutes;
the user's conditional 1.5x/2x escalation is not needed to create starvation.

## Corrected baseline — M1

Actions run `45357274431`; harness `8a60659dbc68c3118954715e0b559d05e3b6de99`;
engine source `5b2cf3e2731d7fcecc3f95b9c05a5963b9c6b363`, unchanged binary with
SHA256 in Provenance. One CPU quota **from process start**, eight flip tasks,
release symbols/frame pointers. PostgreSQL and store are outside the quota.
Each catalog has a fresh database and runs import, large deletion, then small
deletion in that order. Counts exclude probe writes and transaction fence rows.

| Catalog / workload | Rows | SQL start → empty s | First delivery → empty s | Changes/s, SQL clock | CPU %, one core | Backfills / minute | Pinned | Drain ok/attempts; p50/p95/max s | Subset ok/attempts; p50/p95/max s | Live ok/attempts; p50/p95/max s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| Nov23 / import | 13,081 | 165.85 | 165.12 | 78.87 | 95.85 | 15,420 / 5,578.50 | 0 | 2/165; 0.923/0.923/1.924 | 146/165; 0.196/1.195/1.778 | 2/165; 0.869/0.869/1.827 |
| Nov23 / large deletion | 29,793 | 69.38 | 61.69 | 429.43 | 85.07 | 15,973 / 13,813.90 | 0 | 6/69; 0.035/0.037/0.070 | 64/69; 0.091/0.941/1.963 | 6/69; 0.081/0.090/0.112 |
| Nov23 / small deletion | 7,114 | 91.42 | 89.37 | 77.82 | 96.36 | 15,422 / 10,122.17 | 0 | 1/91; 0.046/0.046/0.046 | 61/91; 0.146/0.236/1.213 | 1/91; 0.096/0.096/0.096 |
| Nov24 / import | 13,081 | 529.15 | 528.49 | 24.72 | 98.50 | 15,420 / 1,748.46 | 0 | 0/529; — | 362/529; 0.532/1.573/1.982 | 0/529; — |
| Nov24 / large deletion | 29,793 | 161.20 | 153.14 | 184.81 | 93.84 | 17,717 / 6,594.23 | 0 | 6/161; 0.046/0.058/0.236 | 127/161; 0.253/0.855/1.853 | 6/161; 0.166/0.244/0.245 |
| Nov24 / small deletion | 7,114 | 257.78 | 254.60 | 27.60 | 99.09 | 15,422 / 3,589.58 | 0 | 0/257; — | 149/257; 0.324/0.471/1.582 | 0/257; — |

Each row uses `python tools/stall-repro/run.py --engine tools/stall-repro/bin/electric-circuits-engine --store tools/stall-repro/bin/durable-streams-server --name run-CATALOG --catalog CATALOG`
with `CATALOG=nov23` or `nov24`, default `--cpus 1 --timeout 3600`, and
`STALL_REPRO_PROFILE=1`. Exact commands and all three workload results are in
`baseline-8a60659/CATALOG/manifest.json` and `*-result.json`. Raw probe/CPU samples,
write manifests and compressed engine logs are beside them. A successful probe
must complete in two seconds; latency percentiles exclude censored failures.
These are single runs, not confidence intervals. First delivery is the first
completed proxy response containing a workload xid, not an instrumented
sequencer-entry timestamp. Completion requires the exact final receipt plus
zero pending flips and failing appends.

| Catalog metric | Nov23 target | Nov23 actual | Nov24 target | Nov24 actual |
| --- | ---: | ---: | ---: | ---: |
| Shapes | 8,376 | 8,376 | 10,571 | 10,571 |
| Standalone | 5,991 | 6,022 | 6,251 | 6,284 |
| Family shapes / families | 31 / 1 | 0 / 0 | 33 / 1 | 0 / 0 |
| Subquery shapes | 2,354 | 2,354 | 4,287 | 4,287 |
| Subquery nodes | 808 | 856 | 1,382 | 1,406 |
| Subquery edges | 14,339 | 14,340 | 25,219 | 26,367 |
| Contributors | 141,628 | 144,765 | 235,310 | 237,341 |
| Distinct values | 141,614 | 144,765 | 235,292 | 237,341 |
| Feed entries | 5,850 | 4,620 | 16,388 | 13,656 |
| Tailers | 34 | 12 | 37 | 12 |
| Tables | 44 | 44 | 44 | 44 |

The engine's complete memory snapshots are retained as `catalog-calibration.json`.
The Nov24 import is 3.20x slower than Nov23 by first-delivery clock. It has
1,748 backfills/minute rather than the incident's approximately 200. No pinned
warning occurred. In particular, this pinned file-backed store does not support
prod's 4 MiB read-page cap; absence of page-spanning pinned warnings is not proof
that the production replay pin is irrelevant. Family/tailer and feed-entry
counts remain unmatched. The fixture is sufficiently close to expose the
membership cost, not a faithful reproduction of every production scheduling detail.

## CPU profile evidence — M2 wall-time supplement pending

The unchanged Nov24 engine was sampled for 529.15 seconds with
`sudo perf record -e cpu-clock -F 99 -g --call-graph fp -p PID`, producing 51,471
samples. `baseline-8a60659/nov24/import.svg` and `import.folded.gz` retain the
flamegraph and all folded stacks; Nov23 equivalents are alongside its results.
Raw `perf.data` and `perf.script` remain in Actions artifacts. Folded compression
command: `gzip -n -c import.folded > import.folded.gz`.

Preliminary deepest-recognized-frame attribution is 91.630% row/membership,
5.257% lock-related CPU, 0.861% flip propagation, 0.820% PostgreSQL client,
0.041% stream append and 1.391% other/unresolved. No sample was attributed to
marker waiting. These CPU categories are not off-CPU wall time and are not a
claim that every sampled mutex is the registry mutex. Async ancestry can be
missing. The separate observational build supplies registry/marker/worker wall
times before M2 is claimed.

## Provenance

| Input | Identity |
| --- | --- |
| Production engine base | `5b2cf3e2731d7fcecc3f95b9c05a5963b9c6b363` |
| Scratch branch | `profile/stall-repro-2026-11-24` |
| Data | Synthetic only; no prod/dev access permitted |
| Engine profile | Release, debug level 1, force-frame-pointers |
| Required engine limit | 1 CPU quota, at least two visible cores, 8 flip workers |
| Required shape limit | `ELECTRIC_CIRCUITS_MAX_SHAPES=20000` |
| Indexed source snapshot | `da746b7dce4e4b7aa89215eb5e5cdad2d2d947ce` (not asserted to be Nov 24 prod) |
| Release binary SHA256 | `c5f6eca87a80ddd2fccb4de97dc8cf8603021705e891af1f6ab170681c2358e9` |
| Build harness commit | `d6f1b0a9fc1f437a357ac259eb13f436256bc937`; engine sources unchanged from production base |

## Environment evidence

| Attempt | Command | Outcome |
| --- | --- | --- |
| Mighty Linux, 8 CPU / 16 GiB | Exact preflight in `../README.md`; command `01a20c4b-6aab-7c63-9c3a-2b08991a1af1` | Admission failed: `fleet mighty.agents.run.fleet-missing-capability: mighty.compute.os.linux`. No workload ran. Switched to GitHub Actions immediately rather than spending the environment budget retrying an absent capability. |
| GitHub Actions build, run `45342910224` | `CARGO_PROFILE_RELEASE_DEBUG=1 RUSTFLAGS='-C force-frame-pointers=yes' cargo build --release --locked -p electric-circuits-engine` at `d6f1b0a9fc1f437a357ac259eb13f436256bc937` | Passed, Rust 1.96.1; binary hash above. |
| Actions cgroup preflight, same run/commit | `docker run --rm --cpus=1 postgres:18 bash -c 'nproc; cat /sys/fs/cgroup/cpu.max; postgres --version'` | 4 visible logical CPUs, `100000 100000`, PostgreSQL 18.7. |
| Stream store, run `45347325335` | `cargo install durable-streams --version 0.1.5 --locked` at `bbc2b436ac47183c4050938a742968cf1906802c` | Passed; file-backed Rust store, matching repository harness version, not asserted to be prod store revision. |
| Schema/catalog validation, run `45349127147` | Workflow `fixture` job at `4a5e35bddbc5fcfa1777c4172a59413747591bf5` | Passed: tests, original gateway predicate construction, real exported schema applied to PG18, SQL-only writes. |
| First engine smoke, run `45348978663` | `run.py --engine ... --store ... --name run-nov24 --catalog nov24 --smoke --timeout 120` at `a244247a8e6a562fdaff5a63f3ee3551e12ee69c` | Boot refused, exit 78: `ELECTRIC_CIRCUITS_DS_IN_PROCESS_TEST requires an engine built with the test-support feature`. No engine measurement. Fix uses normal production client config plus repository loopback identity facade, not a test-support rebuild. |

Machine evidence: `preflight/machine.txt`, binary evidence: `preflight/binary.sha256`.
The hosted runner is AMD EPYC 9V74, 4 logical CPUs (2 cores, 2 threads/core),
not prod's m6i.large. No CPU-equivalence conversion is assumed. PG image digest:
`sha256:f5cbc081c25690710f3fc6f4c61a7565382bae01ca0d0f2028a53d7926b85a20`.

## SQL-only controls — not M1

| Control | Row changes | Command | Commit / run |
| --- | ---: | --- | --- |
| Import, six transactions | 13,081 (2,086; 2,134; 2,160; 2,101; 2,139; 2,461) | `python tools/stall-repro/data_smoke.py` | `a244247a8e6a562fdaff5a63f3ee3551e12ee69c` / `45348978663` |
| Large account deletion, one transaction | 29,793 | Same command | Same commit / run |

These count executed synthetic mutations against PG18. They do not establish
decoded engine change counts, WAL/log bytes, processing time or a stall.

## Fidelity limits and unfinished calibration

- No production catalog or customer rows were read. Shape requests use original
  gateway predicate builders and registry projections from the pinned source.
  Historical date windows and unique-ID filters are synthetic, not a captured
  Swift request sequence. Shape-request totals are not engine catalog calibration.
- The pinned app configuration lists **45** entries, including internal fences;
  the engine's own smoke snapshot reports **44 tables**, matching the incident's
  metric. No configured table was silently removed. The engine's default
  publication is `FOR ALL TABLES`; configured filtering and memory table counts
  are separate quantities.
- The SQLAlchemy export preserves storage columns, checks, indexes and FKs;
  application ORM relationships/serializers are not executed. Encrypted values
  are synthetic bytes. FKs/cascades are suppressed with
  `session_replication_role=replica` during synthetic writes. This is not an app
  integrity qualification.
- Workloads currently match transaction row-count targets, not the original
  per-table statement mix. COPY batches and individual updates/deletes replace
  app writes. Unpublished app writes are omitted. JSON/bytes are synthetic padding,
  not valid Loro documents. The 8.4 MB/transaction and 111 MB log sizes are not yet
  calibrated.
- There are 19 weighted synthetic background accounts and one new import account;
  the probe account is distinct from both import and deletion accounts. Actual
  subquery nodes, edges, contributors and feed entries are not yet measured.
- Both SQL submission and first change-log delivery are timed. First delivery
  means the first completed HTTP response at the external loopback proxy that
  contains a workload xid, not an instrumented sequencer function entry. The
  proxy records xids/counts, never row payloads, but parsing the extra copy costs
  uncapped-host CPU. Successful probe latencies exclude censored timeouts;
  success denominators include them. These limits remain visible in tables.
- Fixture readiness metadata comes from the existing conformance proxy. Stream
  data goes to the real file-backed Rust store outside the engine quota. This is
  not evidence about production store identity, layout or latency.

No flamegraphs, CPU attribution, model B results or bridge verdict exist yet.

## Preliminary full catalogs — not the final M1 control

These runs used 1 CPU during workloads but started/warmed the engine at 4 CPUs.
That can change runtime thread selection, so they are not interchangeable with
the corrected run that enforces its quota from process start. All use the same
immutable production-source binary. No pinned-position warnings were observed.

| Catalog / workload | First delivery → empty s | SQL → empty s | SQL-clock changes/s | Mean CPU % | Backfills | Backfills/min | Drain successes | Subset successes | Live successes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| Preliminary Nov 23 / import | 365.29 | 366.19 | 35.72 | 99.83 | 15,420 | 2,526.53 | 1/367 | 367/367 | 1/367 |
| Preliminary Nov 23 / large delete | 129.40 | 153.52 | 194.07 | 93.24 | 21,461 | 8,387.74 | 0/154 | 154/154 | 0/154 |
| Preliminary Nov 23 / small delete | 182.69 | 190.76 | 37.29 | 99.35 | 15,422 | 4,850.76 | 0/191 | 191/191 | 0/191 |
| Preliminary Nov 24 / import | 669.83 | 670.50 | 19.51 | 99.94 | 15,420 | 1,379.86 | 1/671 | 671/671 | 1/671 |
| Preliminary Nov 24 / large delete | 307.81 | 320.25 | 93.03 | 95.93 | 24,525 | 4,594.90 | 0/320 | 320/320 | 0/320 |
| Preliminary Nov 24 / small delete | 350.85 | 394.30 | 18.04 | 99.40 | 15,422 | 2,346.77 | 0/395 | 395/395 | 0/395 |

Every row: harness `672b8f03f1deb63c44fe6aaead15722715a70b61`, Actions
`45351634450`; command `python tools/stall-repro/run.py --engine tools/stall-repro/bin/electric-circuits-engine --store tools/stall-repro/bin/durable-streams-server --name run-DAY --catalog DAY`, replacing DAY with `nov23`/`nov24`.
Full p50/p95/max probe latencies, writes, CPU series, exact commands and engine
logs are retained in `precalibration-672b8f0/nov23/` and `nov24/`.

The preliminary membership state was too large: Nov 23 had 1,091 nodes and
364,381 contributors; Nov 24 had 1,708 nodes and 571,489 contributors. Synthetic
row density and date-window diversity were reduced before the next run.

## Near-target profiling attempt — artifacts incomplete

Harness `0923ed79f35dc745352e6f99b6e37b37c42ab08a`, Actions `45355440591`.
Same command pattern as above, with `STALL_REPRO_PERF` pointing to installed perf.
The warmup still used 4 CPUs. A root-owned `perf.data` file prevented artifact
upload; summaries were recovered from immutable job logs in `provisional-0923ed7/`.
Folded stacks/SVGs from this attempt are unavailable, so this is not M2.

| Metric | Nov 23 target | Nov 23 actual | Nov 24 target | Nov 24 actual |
| --- | ---: | ---: | ---: | ---: |
| Shapes | 8,376 | 8,376 | 10,571 | 10,571 |
| Subquery shapes | 2,354 | 2,354 | 4,287 | 4,287 |
| Nodes | 808 | 856 | 1,382 | 1,406 |
| Edges | 14,339 | 14,340 | 25,219 | 26,367 |
| Contributors | 141,628 | 144,765 | 235,310 | 237,341 |
| Distinct values | 141,614 | 144,765 | 235,292 | 237,341 |

Nov 24 import took 331.558 s from first delivery (331.981 s SQL clock), at 99.83%
mean CPU. Drain and live probes each passed 1/332; subsets passed 332/332.
The 32653 CPU samples attributed 89.06% to row/membership evaluation by deepest
recognized frame, 5.97% to lock CPU, 1.10% to flip frames, 1.11% to PostgreSQL
client frames, 0.037% to append frames, 0.003% to marker-wait frames and 2.71% to
other/unresolved frames. These categories are approximate async-stack attribution,
not lock hold/wait wall time, and cannot replace retained stacks or the wall timers.

Nov 23 import took 87.808 s from first delivery (88.189 s SQL clock), not the
incident's roughly fifteen minutes. This is a severity/timing mismatch, even
though the other account's drain and live changes were starved in both runs.

## End-to-end smoke — not M1

Actions run `45351487929`, harness `c84f2fdcd1be61f11f6a8394ec5968b5626052bb`,
unchanged release engine hash above. This deliberately uses only **48 shapes**
(28 subquery, 20 standalone), not a historical catalog. The engine reports 48
nodes, 204 edges, 13,442 contributors and 4 feed entries before workloads.
`cpu.max` is `100000 100000`. All three independent-account probes pass 3/3 idle
attempts before each workload. Raw evidence is in `smoke-c84f2fd/`.

Command: `python tools/stall-repro/run.py --engine tools/stall-repro/bin/electric-circuits-engine --store tools/stall-repro/bin/durable-streams-server --name run-nov24 --catalog nov24 --smoke --timeout 120`.

| Workload | Changes | SQL start → empty s | First delivery → empty s | Changes/s (SQL clock) | Mean CPU % (one core) | Backfills / minute | Pinned warnings |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Import | 13,081 | 21.604 | 20.938 | 605.50 | 74.80 | 15,420 / 42,825.84 | 0 |
| Large deletion | 29,793 | 9.860 | 1.866 | 3,021.63 | 27.89 | 202 / 1,229.22 | 0 |
| Small deletion | 7,114 | 14.143 | 12.045 | 503.00 | 80.18 | 15,422 / 65,425.81 | 0 |

Each probe cell is successes/attempts, then successful p50/p95/max in seconds.
Timeouts are censored at 2 s. Short drain/live failures are observable even here;
they do **not** reproduce the requested many-minute stall.

| Workload | Drain | Subset | Live change |
| --- | --- | --- | --- |
| Import | 3/22; 0.748/0.748/1.826 | 22/22; 0.039/0.053/0.089 | 1/22; 0.101/0.101/0.101 |
| Large deletion | 9/10; 0.031/0.952/1.768 | 10/10; 0.012/0.014/0.014 | 9/10; 0.029/0.783/1.765 |
| Small deletion | 4/15; 0.034/0.180/1.165 | 15/15; 0.021/0.033/0.034 | 2/15; 0.029/0.029/0.030 |

An earlier full-catalog attempt (`8f333ac11d9e20b3470d81fc337fffb5461abfc4`,
run `45351083884`) did not measure either workload: Nov 23 boot raced the stream
store (catalog GET 502); Nov 24 rejected a synthetic `workflow_run.record_id`
window (that column does not exist). Fixed by real-store readiness and a
created-at window. A later fixture-only run `45353004075` caught PostgreSQL's
temporary bootstrap server shutting down after Unix-socket readiness; the
workflow now waits for TCP readiness. None of these failures is stall evidence.

## Important interpretation limit

The requested model B is a measurement idealization where hot rows carry an
audience key. The attached proposal explicitly retains scopes for chat, edges,
Resources, and direct shares. An all-plain-shape result must not be described as
proof that the proposal's retained subqueries are harmless. The measured schema,
retained scopes, and any omitted costs must be stated with the eventual results.
