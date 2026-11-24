Today vs yesterday (Nov 24, same runner): prod's engine today (`b42f70d`, 2 vCPU) stalls as long as yesterday's (`5b2cf3e`, 1 vCPU). Import 547 s against 532 s (1.03x), large deletion 153 s against 159 s (0.96x), small deletion 265 s against 253 s (1.04x). The other account's drains and live changes are 0% on both; only subsets improve (100% against 58-84%). Bisect: PR 70 (`e10c4bd`) carries the slowdown since `1be10fb`, 1.29x on the import and 3.8x on the large deletion; PRs 71, 72 and 69 change nothing.
M4: Today the time goes to per-row membership evaluation (about 19 to 23 ms per changed row, 69% of CPU), serialized on one registry mutex. The other account starves because its drain and live change wait behind the workload in the single change log. No engine-side bridge keeps that account served (drain and live success at most 22% in every A variant, including 2 CPUs). The audience-key model removes the stall only if graph edges are keyed as well: B-retained takes 132 s with drains at 0 to 1%, against 3.9 s with graph edges keyed. Build that first. Prod's second vCPU does not help this path (0.96x), and the estimate is that prod is safe tonight but exposed to hour-scale outages from Nov 26 to 28, depending on regrowth.
M1: Synthetic cross-account starvation reproduced: Nov24 import took 528.49 s from first delivery at 98.50% CPU with 0/529 drain and live successes, versus Nov23 165.12 s; this reproduces the mechanism, not the incident's absolute duration.
M2: A 529-second, 51,471-sample profile assigns 68.74% CPU to per-row membership, 19.27% to flip walks and 8.12% to memory sampling; a separate 373-second observer run spends 82.26 s in marker waits and 276.61 s holding the sequencer registry lock.
M3: The all-plain audience-key prototype processes import/deletion in 3.18/1.79 s with zero backfills, but retaining the proposal's exceptional scopes still starves drain/live for the 148.42-second import.

# OTTO-13916 — scratch measurements, never merge

M4 is complete; its answer is the last section. The corrected production-binary baseline
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

## CPU and wall profile — M2

The unchanged Nov24 engine was sampled for 529.15 seconds with
`sudo perf record -e cpu-clock -F 99 -g --call-graph fp -p PID`, producing 51,471
samples. `baseline-8a60659/nov24/import.svg` and `import.folded.gz` retain the
flamegraph and all folded stacks; Nov23 equivalents are alongside its results.
Raw `perf.data` and `perf.script` remain in Actions artifacts. Folded compression
command: `gzip -n -c import.folded > import.folded.gz`.

The initial deepest-frame classifier in `import-result.json` reported 91.630%
membership and 5.257% lock CPU. **Do not use that as a responsibility split**:
flip walks evaluate membership too, and compiler inline symbols place a mutex
guard above memory sampling. Inspection of the full stacks gives this exclusive
owning-operation table (`profile_report.py`), assigning recognized PG/append
frames first. Async ancestry can be missing; unknown samples are not invented.

| CPU responsibility | Samples | CPU % | Production function / file |
| --- | ---: | ---: | --- |
| Per-row shape/membership evaluation | 35,379 | 68.736 | `SubqueryRegistry::on_table_delta`, `emit_for_shapes_with`, `apps/engine/src/subquery.rs` |
| Flip walks, including their requery predicate/membership evaluation | 9,916 | 19.265 | `propagate_one`, `requery_and_move_shapes`, `apps/engine/src/subquery.rs` |
| PostgreSQL client | 423 | 0.822 | `pg` / `tokio_postgres` frames |
| Stream append client | 157 | 0.305 | `append_reliable`, `append_retrying`, `append_batch`, `append_commit` |
| Marker wait CPU | 0 | 0.000 | `wait_for_source_effects`, `apps/engine/src/engine/sequencer.rs` |
| Memory sampler | 4,180 | 8.121 | `mem::spawn_sampler`, `Engine::mem_cardinalities`, `SubqueryRegistry::mem_totals` |
| Other lock-related CPU outside those owners | 16 | 0.031 | Mutex/rwlock/futex frames; not all registry locks |
| Everything else / unresolved | 1,400 | 2.720 | Retained stacks |

Command: `python tools/stall-repro/profile_report.py tools/stall-repro/results/baseline-8a60659/nov24/import.folded.gz`.
The resulting `cpu-responsibility.json` also preserves overlapping inner-frame
counts: `MembershipCircuit::contains` occurs in **76.857%** of all samples and
`subq_circuit::seek` in **72.728%**, across both sequencer and flip paths.
`VarintReader::get` alone is the leaf of 9,732 samples (18.91%). This is repeated
DBSP membership-snapshot lookup cost, not time spent executing SQL on Postgres.
`mem_totals` traverses the same circuit snapshots for every node and contributes
8.05% inclusively, despite its "cheap" source comment. `parking_lot` frames occur
in 2.92% of samples inclusively; those also include DBSP/cache locks. Off-CPU
registry waiting cannot honestly be assigned a CPU percentage.

For wall time, Actions run `45360019209`, harness
`346e9f970cc38beb50ff5c2ec26107b9ee90ec8a`, uses the separately verified observer
engine `894b2cca2ec9c1ba547e130e275159c61e178e59`, SHA256
`71fc1df3f0294ce0580d639e9001f239076bdbb92b64abf5b33faabbd8737311`.
Command: `python tools/stall-repro/run.py --engine tools/stall-repro/bin/electric-circuits-engine --store tools/stall-repro/bin/durable-streams-server --name run-A-observed-nov24-1cpu --catalog nov24 --variant A-observed --cpus 1 --workloads import --engine-source 894b2cca2ec9c1ba547e130e275159c61e178e59`.
This is **not the same run as the CPU sample**: no perf, 372.996 s SQL-to-empty,
372.650 s first-delivery-to-empty, 98.829% CPU, 15,420 backfills; drain/live both
0/373, subset 289/373. Different sampling overhead, code layout and runner
variation are not distinguished. An unprofiled unchanged-engine control is
included with B so improvement ratios need not use the profiled baseline.

| Observer interval, import | Completed calls | Aggregate wall s | Interpretation |
| --- | ---: | ---: | --- |
| Sequencer membership | 11,941 | 276.529 | Inside `on_table_delta` |
| Sequencer registry hold | 11,941 | 276.606 | Includes membership; not additive |
| Sequencer registry acquisition wait | 11,941 | 16.905 | Off-CPU and scheduling included |
| `wait_for_source_effects` | 66 | 82.265 | Sequencer waits, with 10 ms polls; not 82 s of CPU |
| Flip registry hold | 103,620 | 69.690 | Across all flip tasks |
| Flip registry acquisition wait | 103,620 | 570.797 | Concurrent waits overlap |
| Flip batches | 2,220 | 2,916.898 | Peak 8 active, zero at endpoints |
| Individual walks | 6,780 | 2,916.889 | Includes IO, appends, scheduling and lock waits |

The eight workers are semaphore-bounded async tasks, not eight dedicated
threads. Their aggregate occupied time is 7.82 worker-seconds per elapsed
second; they spend 570.80 worker-seconds waiting for the registry and 69.69
holding it. The remainder is not instrumented finely enough to split network
wait from runtime scheduling or other work. CPU samples show their executed
work includes substantial membership evaluation. The sequencer dominates a
single CPU and frequently holds the shared registry; raising worker count is
not evidence of eight-way CPU capacity. Wall categories overlap and must never
be summed into a CPU pie chart. Evidence is in `observer-346e9f9/`; the RAII guard
test passed in build run `45358360702`, with `cargo fmt --check` clean.

## Provenance

B's first attempt, run `45362175258` at harness `b187502`, failed before
measurement: the prototype reused one subscription identity for different
tables/calendars, correctly rejected by the engine with HTTP 409. The fix gives
each keyed table/key/class a distinct gateway-built subscription id. No workload
result from that failed B attempt is counted; its unmodified-A control is separate.

## Audience-key prototype — M3

Run `45363301660`, harness `1489d8bbc474e4f6ebe7736d97827061f58f5845`, unchanged
production engine `5b2cf3e`, 1 CPU. Exact command:
`python tools/stall-repro/run.py --engine tools/stall-repro/bin/electric-circuits-engine --store tools/stall-repro/bin/durable-streams-server --name run-B-nov24-1cpu --catalog nov24 --variant B --cpus 1`.
Data/writes are the same synthetic fixture, with stored/replicated audience key
and class columns added by the writer. Historical requests collapse to 725
plain shapes in 24 equality families, zero subquery nodes/edges/contributors.
The original 10,571 total is intentionally NOT retained in B; the separate
all-plain cost control holds total shapes constant. Raw evidence and SQL schema
additions: `model-B-1489d8b/`.

| Workload | Rows | SQL → empty s | First delivery → empty s | Changes/s, SQL clock | CPU %, SQL window | Backfills / minute | Pinned | Drain ok/attempts; p50/p95/max s | Subset ok/attempts; p50/p95/max s | Live ok/attempts; p50/p95/max s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| Import | 13,081 | 3.89 | 3.18 | 3,365.16 | 26.24 | 0 / 0 | 0 | 3/3; 0.034/0.034/0.200 | 3/3; 0.013/0.013/0.014 | 3/3; 0.044/0.044/0.137 |
| Large deletion | 29,793 | 9.56 | 1.79 | 3,116.14 | 21.86 | 0 / 0 | 0 | 8/9; 0.032/0.550/1.428 | 9/9; 0.012/0.072/0.187 | 8/9; 0.030/0.406/1.406 |
| Small deletion | 7,114 | 2.30 | 0.29 | 3,096.44 | 17.41 | 0 / 0 | 0 | 2/2; 0.030/0.030/0.347 | 2/2; 0.009/0.009/0.012 | 2/2; 0.028/0.028/0.222 |

This removes minute-scale starvation **in the all-plain idealization**, not
every two-second failure: large-deletion drain/live success is 88.9%, subset
100%. With only three import observations, 100% is not a reliability estimate.
The writer takes 6.24 s for the large deletion, before the engine's first
completed delivery; the low CPU average includes that source time. The new
log transaction is 121,912,564 bytes including its marker and new key columns.

`B-retained`, which keeps the proposal's explicit chat/edge/Resource/File/User
exceptions, is now measured in the same run/commit: import **148.42 s** from first
delivery, **98.94% CPU**, **7,020 backfills**, drain/live **0/148**, subset
**107/148**. Large deletion takes **34.67 s**, small deletion **58.98 s**.
It improves work volume but does **not** remove cross-account starvation.
The unprofiled unchanged-engine A control (run `45362175258`) took 530.75 s; see the matrices below.
Do not treat B's ideal result as the full proposal's gain. This prototype has
no gateway token/relay revocation enforcement, direct-share equivalence,
parent-move cascades, hidden/deleted cases or migration measurement. See
`../MEASUREMENT_PATCHES.md` for the exact cuts; it is not safe to deploy.

Early bridge result: unmodified A at **2 CPU** (run `45362388808`, harness
`33929b5`, exact command in `variants/A-nov24-2.0cpu-33929b5/manifest.json`)
takes **323.59 s** for import, consumes **102.64% of one core**, and still has
drain/live **0/324**, but subset **324/324** (p95 **0.011 s**). More CPU provides
serving capacity; it does not make the serial membership path twice as fast or
remove the global drain dependency. On one runner, 2 CPUs take 0.96x as long as 1 CPU (see the same-runner pairs below); the 0.61x here compared two different CPU models.

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

(Written before M2–M4; flamegraphs, attribution, B and the bridge verdict are in later sections.)

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


## Full variant matrix — same-runner pairs (takeover, harness `b4ab93e`)

**Why pairs.** Hosted runners in this pool have six CPU models (AMD EPYC 7763, 9V45, 9V74;
Intel Xeon 8370C, 8573C, 6975P-C), and the unchanged A import ranges from 330 s (EPYC 9V45) to
567 s (EPYC 7763) across them. Single runs on different runners mostly measure the CPU, so the
earlier cross-runner ratios (A 2 CPU at 0.61x, PR 68's base against prod, the per-class B-retained
rows) are superseded by this section. Each Actions job in run `45377863650` (workflow job `paired`,
harness `b4ab93e`) ran a control and its variant(s) back to back on one runner, each with a fresh
PostgreSQL 18 container, the Nov 24 catalog at 1 CPU unless marked, 8 flip workers and
`ELECTRIC_CIRCUITS_MAX_SHAPES=20000`. Exact command per row: `python tools/stall-repro/run.py
--engine tools/stall-repro/bin/BINARY/electric-circuits-engine --store
tools/stall-repro/bin/durable-streams-server --name run-LABEL --label LABEL --catalog nov24
--variant VARIANT --cpus CPUS --engine-source SHA`, recorded in each `paired/LABEL/manifest.json`.
Binaries: production `5b2cf3e` (SHA256 `c5f6eca8…`, Actions `45342910224`); bridge scratch build
`caf9e2d` (Actions `45372211215`, knobs off by default; `bridge-control` shows it is within 3–7%
of production); PR 68 base `1be10fb` and head `3a0ce40` (Actions `45362388808`).

The ratio column compares with the first A run on the same runner (for PR 68, with the A run in
that job; for the retained-scope group, with B-retained). "Served" means drain, subset and live-change
probes each succeeded in at least 90% of attempts during the workload, with a 2 s limit. Rows that
finish in a few seconds have only 2–9 probe attempts, so a "no" there is a seconds-long gap, not an
outage. **Noise:** two identical A runs on one runner differed by 0.95x on import and 1.01–1.03x
on the deletions. The small deletion is noisier (the A control varies from 96 to 261 s across
runners and sometimes within a model), so weigh import and large-deletion ratios. Each row is a
single run. Command for this table: `python3 tools/stall-repro/matrix.py --control
paired/paired-PAIR-1-… paired/paired-PAIR-*`.

| Variant | Runner CPU | Workload | First delivery → empty s | × same-runner control | CPU % | Backfills | Drain | Subset | Live change | Served? |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- | --- | --- |
| A | EPYC 7763 | import | 530.2 | 1.00 | 99 | 15,420 | 0% (0/530); p95 — | 69% (365/530); p95 1.74 | 0% (0/530); p95 — | no |
| A | EPYC 7763 | large-delete | 155.1 | 1.00 | 95 | 17,717 | 0% (0/206); p95 — | 83% (172/206); p95 1.03 | 0% (0/206); p95 — | no |
| A | EPYC 7763 | small-delete | 249.4 | 1.00 | 99 | 15,422 | 0% (1/253); p95 1.92 | 58% (146/253); p95 0.51 | 0% (1/253); p95 1.93 | no |
| A | EPYC 7763 | import | 505.4 | 0.95 | 98 | 15,420 | 0% (0/506); p95 — | 69% (350/506); p95 1.65 | 0% (0/506); p95 — | no |
| A | EPYC 7763 | large-delete | 156.4 | 1.01 | 95 | 17,717 | 0% (0/204); p95 — | 83% (170/204); p95 0.99 | 0% (0/204); p95 — | no |
| A | EPYC 7763 | small-delete | 256.7 | 1.03 | 99 | 15,422 | 0% (0/260); p95 — | 58% (151/260); p95 0.50 | 0% (0/260); p95 — | no |
| A | Xeon 6975P-C | import | 377.7 | 1.00 | 99 | 15,420 | 0% (0/378); p95 — | 78% (294/378); p95 1.62 | 0% (0/378); p95 — | no |
| A | Xeon 6975P-C | large-delete | 100.4 | 1.00 | 97 | 17,717 | 2% (2/104); p95 0.30 | 78% (81/104); p95 0.42 | 2% (2/104); p95 0.31 | no |
| A | Xeon 6975P-C | small-delete | 96.2 | 1.00 | 98 | 15,422 | 0% (0/97); p95 — | 67% (65/97); p95 0.23 | 0% (0/97); p95 — | no |
| A, 2 CPU | Xeon 6975P-C | import | 362.3 | 0.96 | 102 | 15,420 | 0% (0/362); p95 — | 100% (362/362); p95 0.01 | 0% (0/362); p95 — | no |
| A, 2 CPU | Xeon 6975P-C | large-delete | 94.7 | 0.94 | 102 | 17,717 | 2% (2/98); p95 0.77 | 100% (98/98); p95 0.02 | 2% (2/98); p95 0.72 | no |
| A, 2 CPU | Xeon 6975P-C | small-delete | 163.3 | 1.70 | 105 | 15,422 | 0% (0/164); p95 — | 100% (164/164); p95 0.01 | 0% (0/164); p95 — | no |
| A | Xeon 6975P-C | import | 369.5 | 1.00 | 99 | 15,420 | 0% (0/369); p95 — | 75% (276/369); p95 1.64 | 0% (0/369); p95 — | no |
| A | Xeon 6975P-C | large-delete | 100.5 | 1.00 | 97 | 17,717 | 2% (2/103); p95 0.49 | 79% (81/103); p95 0.76 | 2% (2/103); p95 0.41 | no |
| A | Xeon 6975P-C | small-delete | 175.3 | 1.00 | 99 | 15,422 | 1% (1/177); p95 0.51 | 57% (101/177); p95 0.35 | 1% (1/177); p95 0.52 | no |
| bridge-control | Xeon 6975P-C | import | 380.8 | 1.03 | 99 | 15,420 | 0% (0/381); p95 — | 78% (296/381); p95 1.64 | 0% (0/381); p95 — | no |
| bridge-control | Xeon 6975P-C | large-delete | 105.1 | 1.05 | 96 | 17,717 | 2% (2/108); p95 0.04 | 80% (86/108); p95 0.60 | 2% (2/108); p95 0.11 | no |
| bridge-control | Xeon 6975P-C | small-delete | 187.7 | 1.07 | 98 | 15,422 | 1% (2/188); p95 0.50 | 60% (112/188); p95 0.30 | 1% (2/188); p95 0.48 | no |
| A | EPYC 7763 | import | 550.1 | 1.00 | 99 | 15,420 | 0% (0/550); p95 — | 65% (355/550); p95 1.63 | 0% (0/550); p95 — | no |
| A | EPYC 7763 | large-delete | 154.8 | 1.00 | 95 | 17,717 | 0% (0/208); p95 — | 83% (173/208); p95 1.02 | 0% (0/208); p95 — | no |
| A | EPYC 7763 | small-delete | 150.3 | 1.00 | 98 | 15,422 | 1% (2/153); p95 0.03 | 67% (103/153); p95 0.36 | 1% (2/153); p95 0.19 | no |
| sampler-off | EPYC 7763 | import | 489.5 | 0.89 | 98 | 15,420 | 0% (0/490); p95 — | 67% (328/490); p95 1.62 | 0% (0/490); p95 — | no |
| sampler-off | EPYC 7763 | large-delete | 143.1 | 0.92 | 94 | 17,717 | 3% (5/151); p95 1.01 | 78% (118/151); p95 0.82 | 3% (5/151); p95 0.93 | no |
| sampler-off | EPYC 7763 | small-delete | 234.1 | 1.56 | 99 | 15,422 | 0% (1/236); p95 0.03 | 54% (128/236); p95 0.42 | 0% (1/236); p95 0.19 | no |
| A | EPYC 7763 | import | 519.6 | 1.00 | 99 | 15,420 | 0% (0/520); p95 — | 68% (351/520); p95 1.70 | 0% (0/520); p95 — | no |
| A | EPYC 7763 | large-delete | 161.8 | 1.00 | 95 | 17,717 | 0% (0/209); p95 — | 85% (178/209); p95 1.06 | 0% (0/209); p95 — | no |
| A | EPYC 7763 | small-delete | 251.0 | 1.00 | 99 | 15,422 | 0% (0/254); p95 — | 57% (145/254); p95 0.52 | 0% (0/254); p95 — | no |
| sampler-10x | EPYC 7763 | import | 499.2 | 0.96 | 98 | 15,420 | 0% (0/500); p95 — | 68% (338/500); p95 1.80 | 0% (0/500); p95 — | no |
| sampler-10x | EPYC 7763 | large-delete | 141.9 | 0.88 | 95 | 17,717 | 3% (5/151); p95 1.02 | 80% (121/151); p95 0.82 | 3% (5/151); p95 1.03 | no |
| sampler-10x | EPYC 7763 | small-delete | 230.0 | 0.92 | 98 | 15,422 | 0% (1/232); p95 0.04 | 54% (126/232); p95 0.45 | 0% (1/232); p95 0.17 | no |
| A | EPYC 7763 | import | 566.9 | 1.00 | 99 | 15,420 | 0% (0/567); p95 — | 66% (373/567); p95 1.65 | 0% (0/567); p95 — | no |
| A | EPYC 7763 | large-delete | 158.3 | 1.00 | 95 | 17,717 | 0% (0/213); p95 — | 84% (179/213); p95 0.98 | 0% (0/213); p95 — | no |
| A | EPYC 7763 | small-delete | 261.3 | 1.00 | 99 | 15,422 | 0% (1/265); p95 1.95 | 58% (153/265); p95 0.51 | 0% (1/265); p95 1.91 | no |
| yield-rows | EPYC 7763 | import | 569.3 | 1.00 | 99 | 15,420 | 0% (0/570); p95 — | 92% (523/570); p95 1.38 | 0% (0/570); p95 — | no |
| yield-rows | EPYC 7763 | large-delete | 164.7 | 1.04 | 97 | 17,717 | 1% (1/174); p95 1.08 | 100% (174/174); p95 0.58 | 1% (2/174); p95 1.09 | no |
| yield-rows | EPYC 7763 | small-delete | 269.3 | 1.03 | 99 | 15,422 | 0% (1/271); p95 0.05 | 100% (271/271); p95 0.72 | 0% (1/271); p95 0.16 | no |
| A | Xeon 8573C | import | 493.6 | 1.00 | 98 | 15,420 | 0% (0/494); p95 — | 69% (339/494); p95 1.77 | 0% (0/494); p95 — | no |
| A | Xeon 8573C | large-delete | 135.6 | 1.00 | 95 | 17,717 | 0% (0/178); p95 — | 84% (149/178); p95 0.85 | 0% (0/178); p95 — | no |
| A | Xeon 8573C | small-delete | 223.5 | 1.00 | 99 | 15,422 | 0% (1/226); p95 1.71 | 57% (129/226); p95 0.48 | 0% (1/226); p95 1.63 | no |
| commit-membership-slices | Xeon 8573C | import | 446.3 | 0.90 | 98 | 15,420 | 0% (0/447); p95 — | 34% (150/447); p95 0.99 | 0% (0/447); p95 — | no |
| commit-membership-slices | Xeon 8573C | large-delete | 126.2 | 0.93 | 94 | 17,717 | 3% (4/132); p95 0.04 | 29% (38/132); p95 0.10 | 3% (4/132); p95 0.14 | no |
| commit-membership-slices | Xeon 8573C | small-delete | 197.2 | 0.88 | 98 | 15,422 | 1% (1/199); p95 0.03 | 5% (9/199); p95 0.54 | 1% (1/199); p95 0.22 | no |
| commit-membership | Xeon 8573C | import | 420.1 | 0.85 | 97 | 15,420 | 0% (0/420); p95 — | 30% (124/420); p95 0.95 | 0% (0/420); p95 — | no |
| commit-membership | Xeon 8573C | large-delete | 132.8 | 0.98 | 96 | 17,717 | 1% (1/139); p95 1.22 | 26% (36/139); p95 0.22 | 1% (1/139); p95 1.23 | no |
| commit-membership | Xeon 8573C | small-delete | did not complete: RuntimeError('Engine exited while workload effects were outstanding; inspect eng | | | | | | | |
| A | EPYC 9V74 | import | 443.6 | 1.00 | 97 | 15,420 | 0% (0/444); p95 — | 73% (323/444); p95 1.64 | 0% (0/444); p95 — | no |
| A | EPYC 9V74 | large-delete | 124.4 | 1.00 | 96 | 17,717 | 2% (2/131); p95 0.09 | 79% (104/131); p95 0.65 | 2% (3/131); p95 1.07 | no |
| A | EPYC 9V74 | small-delete | 215.5 | 1.00 | 98 | 15,422 | 0% (1/218); p95 1.42 | 57% (124/218); p95 0.41 | 0% (1/218); p95 1.26 | no |
| marker-bypass | EPYC 9V74 | import | 499.1 | 1.13 | 98 | 15,420 | 20% (98/499); p95 1.44 | 74% (370/499); p95 1.59 | 12% (59/499); p95 1.87 | no |
| marker-bypass | EPYC 9V74 | large-delete | 138.8 | 1.12 | 95 | 17,717 | 22% (31/144); p95 0.13 | 81% (117/144); p95 0.83 | 22% (31/144); p95 0.23 | no |
| marker-bypass | EPYC 9V74 | small-delete | 237.8 | 1.10 | 99 | 15,422 | 3% (8/239); p95 0.96 | 59% (140/239); p95 0.76 | 3% (7/239); p95 0.84 | no |
| A | EPYC 7763 | import | 548.4 | 1.00 | 98 | 15,420 | 0% (0/549); p95 — | 68% (371/549); p95 1.72 | 0% (0/549); p95 — | no |
| A | EPYC 7763 | large-delete | 163.7 | 1.00 | 96 | 17,717 | 1% (1/173); p95 1.45 | 80% (138/173); p95 0.69 | 1% (1/173); p95 1.35 | no |
| A | EPYC 7763 | small-delete | 251.4 | 1.00 | 99 | 15,422 | 0% (1/255); p95 1.91 | 58% (147/255); p95 0.51 | 0% (1/255); p95 1.88 | no |
| half-subqueries | EPYC 7763 | import | 262.9 | 0.48 | 97 | 15,420 | 0% (0/263); p95 — | 84% (221/263); p95 1.46 | 0% (0/263); p95 — | no |
| half-subqueries | EPYC 7763 | large-delete | 75.9 | 0.46 | 91 | 10,777 | 7% (6/85); p95 0.43 | 86% (73/85); p95 0.48 | 7% (6/85); p95 0.44 | no |
| half-subqueries | EPYC 7763 | small-delete | 120.2 | 0.48 | 98 | 15,422 | 2% (2/123); p95 0.04 | 63% (78/123); p95 0.28 | 2% (2/123); p95 0.12 | no |
| coalesced-base | EPYC 9V45 | import | 270.2 | 0.82 | 96 | 15,420 | 0% (0/270); p95 — | 78% (211/270); p95 1.22 | 0% (0/270); p95 — | no |
| coalesced-base | EPYC 9V45 | large-delete | 28.4 | 0.27 | 84 | 17,717 | 9% (3/32); p95 0.04 | 100% (32/32); p95 0.08 | 6% (2/32); p95 0.08 | no |
| coalesced-base | EPYC 9V45 | small-delete | 30.0 | 0.19 | 94 | 15,422 | 3% (1/31); p95 1.92 | 100% (31/31); p95 0.46 | 0% (0/31); p95 — | no |
| coalesced-head | EPYC 9V45 | import | 252.7 | 0.76 | 99 | 42 | 0% (0/253); p95 — | 69% (174/253); p95 1.59 | 0% (0/253); p95 — | no |
| coalesced-head | EPYC 9V45 | large-delete | 20.6 | 0.20 | 88 | 35 | 12% (3/24); p95 0.06 | 100% (24/24); p95 0.38 | 8% (2/24); p95 0.08 | no |
| coalesced-head | EPYC 9V45 | small-delete | 27.7 | 0.17 | 94 | 21 | 7% (2/28); p95 0.80 | 100% (28/28); p95 0.47 | 7% (2/28); p95 0.66 | no |
| A | EPYC 9V45 | import | 330.3 | 1.00 | 99 | 15,420 | 0% (0/330); p95 — | 76% (251/330); p95 1.60 | 0% (0/330); p95 — | no |
| A | EPYC 9V45 | large-delete | 103.5 | 1.00 | 94 | 17,717 | 4% (4/108); p95 1.35 | 82% (89/108); p95 0.47 | 3% (3/108); p95 0.43 | no |
| A | EPYC 9V45 | small-delete | 158.7 | 1.00 | 98 | 15,422 | 0% (0/159); p95 — | 57% (91/159); p95 0.30 | 0% (0/159); p95 — | no |
| B-retained | EPYC 9V45 | import | 131.7 | 1.00 | 98 | 7,020 | 1% (1/132); p95 1.58 | 80% (106/132); p95 1.72 | 0% (0/132); p95 — | no |
| B-retained | EPYC 9V45 | large-delete | 31.8 | 1.00 | 88 | 9,701 | 9% (3/35); p95 0.03 | 86% (30/35); p95 0.28 | 6% (2/35); p95 0.03 | no |
| B-retained | EPYC 9V45 | small-delete | 58.6 | 1.00 | 98 | 7,022 | 2% (1/59); p95 1.63 | 32% (19/59); p95 0.83 | 0% (0/59); p95 — | no |
| B-retained-key-edge | EPYC 9V45 | import | 3.9 | 0.03 | 64 | 120 | 75% (3/4); p95 1.40 | 100% (4/4); p95 0.22 | 75% (3/4); p95 1.24 | no |
| B-retained-key-edge | EPYC 9V45 | large-delete | 5.5 | 0.17 | 60 | 504 | 44% (4/9); p95 0.36 | 100% (9/9); p95 0.14 | 44% (4/9); p95 0.29 | no |
| B-retained-key-edge | EPYC 9V45 | small-delete | 1.0 | 0.02 | 48 | 122 | 100% (2/2); p95 0.01 | 100% (2/2); p95 0.01 | 100% (2/2); p95 0.02 | **yes** |
| B-retained-key-chat | EPYC 9V45 | import | 131.8 | 1.00 | 98 | 7,020 | 1% (1/132); p95 1.78 | 82% (108/132); p95 1.75 | 0% (0/132); p95 — | no |
| B-retained-key-chat | EPYC 9V45 | large-delete | 27.3 | 0.86 | 85 | 9,701 | 10% (3/31); p95 0.03 | 87% (27/31); p95 0.08 | 6% (2/31); p95 0.03 | no |
| B-retained-key-chat | EPYC 9V45 | small-delete | 58.2 | 0.99 | 98 | 7,022 | 2% (1/59); p95 1.41 | 36% (21/59); p95 0.82 | 0% (0/59); p95 — | no |
| B-retained-key-resource | EPYC 9V45 | import | 139.8 | 1.06 | 99 | 6,900 | 0% (0/140); p95 — | 78% (109/140); p95 1.80 | 0% (0/140); p95 — | no |
| B-retained-key-resource | EPYC 9V45 | large-delete | 31.5 | 0.99 | 87 | 9,301 | 9% (3/35); p95 0.03 | 86% (30/35); p95 0.22 | 6% (2/35); p95 0.04 | no |
| B-retained-key-resource | EPYC 9V45 | small-delete | 60.9 | 1.04 | 98 | 6,902 | 2% (1/62); p95 1.23 | 32% (20/62); p95 0.95 | 0% (0/62); p95 — | no |
| B-retained-key-file | EPYC 9V45 | import | 137.6 | 1.05 | 98 | 7,020 | 1% (1/138); p95 1.65 | 78% (108/138); p95 1.74 | 0% (0/138); p95 — | no |
| B-retained-key-file | EPYC 9V45 | large-delete | 33.3 | 1.05 | 87 | 9,701 | 11% (4/37); p95 0.79 | 86% (32/37); p95 0.24 | 5% (2/37); p95 0.06 | no |
| B-retained-key-file | EPYC 9V45 | small-delete | 56.6 | 0.96 | 98 | 7,022 | 2% (1/57); p95 1.82 | 32% (18/57); p95 0.67 | 0% (0/57); p95 — | no |
| B-retained-key-user | EPYC 9V45 | import | 135.3 | 1.03 | 99 | 7,020 | 0% (0/135); p95 — | 75% (101/135); p95 1.64 | 0% (0/135); p95 — | no |
| B-retained-key-user | EPYC 9V45 | large-delete | 30.6 | 0.96 | 87 | 9,701 | 9% (3/34); p95 0.03 | 88% (30/34); p95 0.29 | 6% (2/34); p95 0.03 | no |
| B-retained-key-user | EPYC 9V45 | small-delete | 51.6 | 0.88 | 98 | 7,022 | 2% (1/52); p95 1.40 | 31% (16/52); p95 0.33 | 0% (0/52); p95 — | no |

Pairs, in order: noise (A, A); 2 CPU (A, A 2 CPU); binary (A, bridge-control); sampler-off;
sampler-10x; yield-rows; commit (A, commit-membership-slices, commit-membership); marker-bypass;
half-subqueries; PR 68 (base, head, A); retained (B-retained, then each class keyed).

**commit-membership is OOM-killed on the small deletion, reproducibly.** Engine RSS reached 14.7 GiB
on the 16 GiB runner (13.9 GiB in the earlier run on another runner) before the process vanished.
Folding a whole table's commit delta into one `on_table_delta` call multiplies candidate-row
allocations. With 256-row slices the same deletion peaked at 3.3 GiB and completed.

### Registry lock holds (bridge batch, run `45372211215`, import only)

Counts of individual registry-lock holds by duration, from the `caf9e2d` build's histograms. They
are not comparable across rows for speed (different runners), only for hold shape.

| Variant | Workload | Lock interval | Calls | Wall s | ≤1 ms | ≤10 ms | ≤100 ms | ≤1 s | ≤5 s | >5 s |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| bridge-control | import | sequencer_registry_hold | 11,931 | 224.1 | 466 | 5,407 | 6,058 | 0 | 0 | 0 |
| bridge-control | import | flip_registry_hold | 103,620 | 59.1 | 102,260 | 1,359 | 1 | 0 | 0 | 0 |
| bridge-control | import | sampler_registry_hold | 388 | 16.5 | 0 | 0 | 388 | 0 | 0 | 0 |
| bridge-control | import | marker_wait | 56 | 71.0 | 4 | 0 | 46 | 1 | 0 | 5 |
| commit-membership | import | sequencer_registry_hold | 105 | 359.4 | 6 | 0 | 58 | 5 | 19 | 17 |
| commit-membership | import | flip_registry_hold | 103,620 | 104.6 | 33,302 | 70,318 | 0 | 0 | 0 | 0 |
| commit-membership | import | sampler_registry_hold | 202 | 15.4 | 0 | 0 | 201 | 1 | 0 | 0 |
| commit-membership | import | marker_wait | 63 | 139.9 | 9 | 0 | 47 | 1 | 1 | 5 |
| commit-membership-slices | import | sequencer_registry_hold | 134 | 352.2 | 6 | 0 | 60 | 10 | 36 | 22 |
| commit-membership-slices | import | flip_registry_hold | 103,620 | 105.0 | 33,292 | 70,328 | 0 | 0 | 0 | 0 |
| commit-membership-slices | import | sampler_registry_hold | 197 | 15.9 | 0 | 0 | 196 | 1 | 0 | 0 |
| commit-membership-slices | import | marker_wait | 65 | 132.1 | 8 | 0 | 50 | 1 | 1 | 5 |

On prod's row path the longest sequencer hold in the import was **79 ms**, and none of the
11,931 holds exceeded 100 ms. The lock is already released after every row
(`process_envelope_deferred`, `apps/engine/src/engine/sequencer.rs`). The memory sampler
takes it every 500 ms for ~42 ms (388 holds, 16.5 s of a 306 s import, 5.4% of wall time).
The per-commit prototype is where long holds appear (17 over 5 s). 256-row slices do not remove
them (22 over 5 s), because evaluating 256 rows still costs about 256 × 19 ms.

### Earlier single-run rows across runners (kept for provenance; not for ratios)

| Variant | Runner CPU | Workload | First delivery → empty s | × A control (Xeon 8573C) | CPU % | Backfills | Drain | Subset | Live change | Served? |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- | --- | --- |
| A | Xeon 8573C | import | 530.8 | 1.00 | 98 | 15,420 | 1% (3/531); p95 1.20 | 72% (381/531); p95 1.62 | 1% (3/531); p95 1.05 | no |
| A | Xeon 8573C | large-delete | 137.1 | 1.00 | 95 | 17,717 | 3% (4/142); p95 0.05 | 80% (113/142); p95 0.75 | 3% (4/142); p95 0.16 | no |
| A | Xeon 8573C | small-delete | 225.0 | 1.00 | 99 | 15,422 | 0% (1/227); p95 1.49 | 56% (128/227); p95 0.47 | 0% (1/227); p95 1.49 | no |
| A, 2 CPU | EPYC 9V45 | import | 323.6 | 0.61 | 103 | 15,420 | 0% (0/324); p95 — | 100% (324/324); p95 0.01 | 0% (0/324); p95 — | no |
| A, 2 CPU | EPYC 9V45 | large-delete | 91.8 | 0.67 | 101 | 17,717 | 2% (2/95); p95 0.03 | 100% (95/95); p95 0.02 | 2% (2/95); p95 0.09 | no |
| A, 2 CPU | EPYC 9V45 | small-delete | 159.2 | 0.71 | 105 | 15,422 | 0% (0/160); p95 — | 100% (160/160); p95 0.01 | 0% (0/160); p95 — | no |
| A, 4 CPU | Xeon 8370C | import | 520.5 | 0.98 | 103 | 15,420 | 0% (0/521); p95 — | 100% (521/521); p95 0.02 | 0% (0/521); p95 — | no |
| A, 4 CPU | Xeon 8370C | large-delete | 144.0 | 1.05 | 101 | 17,717 | 0% (0/188); p95 — | 100% (188/188); p95 0.02 | 0% (0/188); p95 — | no |
| A, 4 CPU | Xeon 8370C | small-delete | 247.0 | 1.10 | 105 | 15,422 | 0% (0/249); p95 — | 99% (246/249); p95 0.02 | 0% (0/249); p95 — | no |
| A-observed | EPYC 7763 | import | 542.4 | 1.02 | 98 | 15,420 | 0% (0/543); p95 — | 66% (360/543); p95 1.70 | 0% (0/543); p95 — | no |
| A-observed | EPYC 7763 | large-delete | 165.2 | 1.20 | 96 | 17,717 | 1% (1/174); p95 1.11 | 80% (140/174); p95 0.90 | 1% (2/174); p95 1.03 | no |
| A-observed | EPYC 7763 | small-delete | 253.9 | 1.13 | 99 | 15,422 | 0% (1/256); p95 0.04 | 57% (145/256); p95 0.52 | 0% (1/256); p95 0.18 | no |
| marker-bypass | EPYC 9V74 | import | 497.6 | 0.94 | 98 | 15,420 | 21% (103/498); p95 1.05 | 75% (375/498); p95 1.64 | 6% (32/498); p95 1.52 | no |
| marker-bypass | EPYC 9V74 | large-delete | 139.4 | 1.02 | 95 | 17,717 | 23% (33/145); p95 0.11 | 81% (117/145); p95 0.82 | 22% (32/145); p95 0.21 | no |
| marker-bypass | EPYC 9V74 | small-delete | 230.7 | 1.03 | 99 | 15,422 | 3% (8/232); p95 0.90 | 59% (138/232); p95 0.77 | 3% (6/232); p95 0.47 | no |
| coalesced-base | Xeon 6975P-C | import | 285.6 | 0.54 | 98 | 15,420 | 0% (0/286); p95 — | 77% (221/286); p95 1.26 | 0% (0/286); p95 — | no |
| coalesced-base | Xeon 6975P-C | large-delete | 32.5 | 0.24 | 87 | 17,717 | 11% (4/36); p95 0.45 | 100% (36/36); p95 0.14 | 11% (4/36); p95 0.29 | no |
| coalesced-base | Xeon 6975P-C | small-delete | 30.1 | 0.13 | 94 | 15,422 | 6% (2/31); p95 0.14 | 94% (29/31); p95 0.37 | 6% (2/31); p95 0.14 | no |
| coalesced-head | EPYC 9V45 | import | 235.3 | 0.44 | 99 | 42 | 0% (0/235); p95 — | 69% (163/235); p95 1.34 | 0% (0/235); p95 — | no |
| coalesced-head | EPYC 9V45 | large-delete | 19.2 | 0.14 | 88 | 35 | 13% (3/23); p95 0.03 | 100% (23/23); p95 0.33 | 9% (2/23); p95 0.07 | no |
| coalesced-head | EPYC 9V45 | small-delete | 25.8 | 0.11 | 95 | 21 | 7% (2/27); p95 1.07 | 100% (27/27); p95 0.43 | 0% (0/27); p95 — | no |
| bridge-control | EPYC 9V45 | import | 305.9 | 0.58 | 98 | 15,420 | 0% (0/306); p95 — | 78% (239/306); p95 1.44 | 0% (0/306); p95 — | no |
| bridge-control | EPYC 9V45 | large-delete | 86.6 | 0.63 | 95 | 17,717 | 3% (3/91); p95 0.25 | 80% (73/91); p95 0.67 | 3% (3/91); p95 0.26 | no |
| bridge-control | EPYC 9V45 | small-delete | 158.9 | 0.71 | 99 | 15,422 | 0% (0/160); p95 — | 59% (94/160); p95 0.29 | 0% (0/160); p95 — | no |
| yield-rows | EPYC 9V74 | import | 460.5 | 0.87 | 97 | 15,420 | 0% (0/461); p95 — | 94% (434/461); p95 1.48 | 0% (0/461); p95 — | no |
| yield-rows | EPYC 9V74 | large-delete | 123.7 | 0.90 | 95 | 17,717 | 3% (4/129); p95 0.04 | 100% (129/129); p95 0.47 | 3% (4/129); p95 0.13 | no |
| yield-rows | EPYC 9V74 | small-delete | 209.8 | 0.93 | 99 | 15,422 | 0% (1/211); p95 0.03 | 100% (211/211); p95 0.55 | 0% (1/211); p95 0.21 | no |
| sampler-off | Xeon 8573C | import | 368.7 | 0.69 | 98 | 15,420 | 0% (0/369); p95 — | 73% (271/369); p95 1.59 | 0% (0/369); p95 — | no |
| sampler-off | Xeon 8573C | large-delete | 102.1 | 0.74 | 95 | 17,717 | 4% (4/107); p95 0.63 | 78% (83/107); p95 0.75 | 4% (4/107); p95 0.64 | no |
| sampler-off | Xeon 8573C | small-delete | 184.1 | 0.82 | 98 | 15,422 | 1% (2/185); p95 0.45 | 57% (106/185); p95 0.34 | 1% (2/185); p95 0.46 | no |
| sampler-10x | EPYC 9V45 | import | 275.2 | 0.52 | 98 | 15,420 | 0% (0/275); p95 — | 80% (219/275); p95 1.67 | 0% (0/275); p95 — | no |
| sampler-10x | EPYC 9V45 | large-delete | 75.3 | 0.55 | 94 | 17,717 | 3% (2/78); p95 0.03 | 79% (62/78); p95 0.62 | 3% (2/78); p95 0.08 | no |
| sampler-10x | EPYC 9V45 | small-delete | 128.4 | 0.57 | 99 | 15,422 | 0% (0/129); p95 — | 56% (72/129); p95 0.25 | 0% (0/129); p95 — | no |
| commit-membership | EPYC 7763 | import | 496.6 | 0.94 | 97 | 15,420 | 0% (0/497); p95 — | 30% (147/497); p95 1.15 | 0% (0/497); p95 — | no |
| commit-membership | EPYC 7763 | large-delete | 160.4 | 1.17 | 96 | 17,717 | 1% (2/170); p95 0.41 | 30% (51/170); p95 0.14 | 1% (2/170); p95 0.49 | no |
| commit-membership | EPYC 7763 | small-delete | did not complete: RuntimeError('Engine exited while workload effects were outstanding; inspect eng | | | | | | | |
| commit-membership-slices | EPYC 7763 | import | 484.2 | 0.91 | 99 | 15,420 | 0% (0/485); p95 — | 29% (140/485); p95 1.18 | 0% (0/485); p95 — | no |
| commit-membership-slices | EPYC 7763 | large-delete | 134.7 | 0.98 | 96 | 17,717 | 2% (3/144); p95 1.24 | 28% (40/144); p95 0.12 | 2% (3/144); p95 1.12 | no |
| commit-membership-slices | EPYC 7763 | small-delete | 127.8 | 0.57 | 97 | 15,422 | 1% (1/130); p95 0.04 | 10% (13/130); p95 0.64 | 1% (1/130); p95 0.11 | no |
| half-subqueries | Xeon 8573C | import | 239.2 | 0.45 | 97 | 15,420 | 0% (0/239); p95 — | 86% (205/239); p95 1.28 | 0% (0/239); p95 — | no |
| half-subqueries | Xeon 8573C | large-delete | 66.7 | 0.49 | 92 | 10,777 | 7% (5/73); p95 0.83 | 88% (64/73); p95 0.60 | 7% (5/73); p95 0.77 | no |
| half-subqueries | Xeon 8573C | small-delete | 111.7 | 0.50 | 98 | 15,422 | 1% (1/114); p95 0.39 | 61% (70/114); p95 0.25 | 1% (1/114); p95 0.41 | no |
| plain-only | EPYC 7763 | import | 3.4 | 0.01 | 30 | 0 | 100% (4/4); p95 0.07 | 100% (4/4); p95 0.04 | 100% (4/4); p95 0.19 | **yes** |
| plain-only | EPYC 7763 | large-delete | 2.0 | 0.01 | 30 | 0 | 78% (7/9); p95 0.05 | 100% (9/9); p95 0.01 | 67% (6/9); p95 0.13 | no |
| plain-only | EPYC 7763 | small-delete | 0.4 | 0.00 | 24 | 0 | 100% (2/2); p95 0.04 | 100% (2/2); p95 0.01 | 100% (2/2); p95 0.12 | **yes** |
| B | EPYC 7763 | import | 3.2 | 0.01 | 26 | 0 | 100% (3/3); p95 0.03 | 100% (3/3); p95 0.01 | 100% (3/3); p95 0.04 | **yes** |
| B | EPYC 7763 | large-delete | 1.8 | 0.01 | 22 | 0 | 89% (8/9); p95 0.55 | 100% (9/9); p95 0.07 | 89% (8/9); p95 0.41 | no |
| B | EPYC 7763 | small-delete | 0.3 | 0.00 | 17 | 0 | 100% (2/2); p95 0.03 | 100% (2/2); p95 0.01 | 100% (2/2); p95 0.03 | **yes** |
| B-retained | Xeon 6975P-C | import | 148.4 | 0.28 | 99 | 7,020 | 0% (0/148); p95 — | 72% (107/148); p95 1.83 | 0% (0/148); p95 — | no |
| B-retained | Xeon 6975P-C | large-delete | 34.7 | 0.25 | 91 | 9,701 | 8% (3/37); p95 0.09 | 86% (32/37); p95 0.41 | 5% (2/37); p95 0.03 | no |
| B-retained | Xeon 6975P-C | small-delete | 59.0 | 0.26 | 99 | 7,022 | 2% (1/59); p95 1.83 | 32% (19/59); p95 1.00 | 0% (0/59); p95 — | no |
| B-retained-key-edge | Xeon 6975P-C | import | 3.2 | 0.01 | 82 | 120 | 100% (3/3); p95 1.39 | 100% (3/3); p95 0.04 | 100% (3/3); p95 1.18 | **yes** |
| B-retained-key-edge | Xeon 6975P-C | large-delete | 5.3 | 0.04 | 66 | 504 | 50% (4/8); p95 0.61 | 100% (8/8); p95 0.11 | 50% (4/8); p95 0.53 | no |
| B-retained-key-edge | Xeon 6975P-C | small-delete | 0.9 | 0.00 | 52 | 122 | 100% (1/1); p95 0.64 | 100% (1/1); p95 0.16 | 100% (1/1); p95 0.64 | **yes** |
| B-retained-key-chat | EPYC 9V45 | import | 125.9 | 0.24 | 98 | 7,020 | 1% (1/126); p95 1.57 | 77% (97/126); p95 1.68 | 0% (0/126); p95 — | no |
| B-retained-key-chat | EPYC 9V45 | large-delete | 26.1 | 0.19 | 86 | 9,701 | 10% (3/29); p95 0.03 | 83% (24/29); p95 0.04 | 7% (2/29); p95 0.04 | no |
| B-retained-key-chat | EPYC 9V45 | small-delete | 56.3 | 0.25 | 98 | 7,022 | 2% (1/57); p95 1.17 | 33% (19/57); p95 0.94 | 0% (0/57); p95 — | no |
| B-retained-key-resource | EPYC 7763 | import | 222.6 | 0.42 | 99 | 6,900 | 0% (0/223); p95 — | 58% (130/223); p95 1.72 | 0% (0/223); p95 — | no |
| B-retained-key-resource | EPYC 7763 | large-delete | 52.0 | 0.38 | 85 | 9,301 | 12% (7/60); p95 0.04 | 87% (52/60); p95 0.40 | 10% (6/60); p95 0.05 | no |
| B-retained-key-resource | EPYC 7763 | small-delete | 89.9 | 0.40 | 98 | 6,902 | 1% (1/92); p95 0.05 | 28% (26/92); p95 0.23 | 1% (1/92); p95 0.05 | no |
| B-retained-key-file | EPYC 7763 | import | 221.1 | 0.42 | 99 | 7,020 | 0% (0/221); p95 — | 56% (124/221); p95 1.87 | 0% (0/221); p95 — | no |
| B-retained-key-file | EPYC 7763 | large-delete | 55.5 | 0.40 | 86 | 9,701 | 11% (7/63); p95 0.08 | 87% (55/63); p95 0.47 | 10% (6/63); p95 0.05 | no |
| B-retained-key-file | EPYC 7763 | small-delete | 87.8 | 0.39 | 98 | 7,022 | 1% (1/90); p95 0.05 | 32% (29/90); p95 0.55 | 1% (1/90); p95 0.05 | no |
| B-retained-key-user | Xeon 6975P-C | import | 150.1 | 0.28 | 99 | 7,020 | 0% (0/150); p95 — | 69% (104/150); p95 1.82 | 0% (0/150); p95 — | no |
| B-retained-key-user | Xeon 6975P-C | large-delete | 34.0 | 0.25 | 90 | 9,701 | 11% (4/37); p95 0.60 | 89% (33/37); p95 0.37 | 5% (2/37); p95 0.03 | no |
| B-retained-key-user | Xeon 6975P-C | small-delete | 58.2 | 0.26 | 98 | 7,022 | 3% (2/59); p95 1.02 | 34% (20/59); p95 1.38 | 0% (0/59); p95 — | no |

Runs: unprofiled A `45362175258` (`b187502`); A 2/4 CPU, A-observed, marker-bypass `45362388808`
(`33929b5`); PR 68 `45363890232` (`9f5ad6d`); B/B-retained `45363301660` (`1489d8b`); bridge
knobs, controls and per-class keys `45372211215` (`caf9e2d`). Commands are in each folder's
`manifest.json`; `summarize_results.py` prints them.

## What each bridge buys (from the pairs)

| Bridge | Duration vs same-runner A (import / large del.) | Other account served? | What it gives up |
| --- | --- | --- | --- |
| 1. Sequencer releases the lock between bounded batches | Not applicable on prod: the sequencer already releases after every row (max hold 79 ms). On the per-commit path, 256-row slices: 0.90 / 0.93 (no faster than the unsliced 0.85 / 0.98), and the OOM goes away | No: drain 0–3%, live 0–3%, subset falls to 5–34% | A commit is no longer applied to membership in one step. Between slices, flips, subsets and the sampler see part of a commit, and outer-shape output from an early slice can be appended before a later slice's retractions (a move between Spaces can show in the new one before it leaves the old) |
| 1b. Yield after each row's lock release | 1.00 / 1.04 | Subsets yes (92–100%); drains/live no (0–1%) | Nothing |
| 1c. Answer drains without the lock | Not applicable: `GET /_runtime/drained-through` already reads the receipt map without the registry lock. The closest measurement is marker-bypass: 1.13 / 1.12 | No: drain 20–22%, live 12–22% | The receipt's guarantee that earlier flips and appends landed. Unsafe |
| 2. Memory sampler off / 10x less often | 0.89 / 0.92 off; 0.96 / 0.88 at 5 s | No | Freshness of the OTel memory gauges; the 30 s log line stays |
| 2 vCPU | 0.96 / 0.94 (engine still uses 1.02 cores) | Subsets yes (100%); drains/live no (0–2%) | Cost |
| Membership once per commit | 0.85 / 0.98, OOM on the small deletion | No; subsets worse (26–30%) | Bounded memory; see `MEASUREMENT_PATCHES.md` |
| Half the subquery shapes | 0.48 / 0.46 | No | Authorization equivalence (ablation only) |
| Plain shapes only | ~0.01 | Yes on import/small; large deletion 78%/67% over 9 attempts | Authorization equivalence (ablation only) |
| PR 68 coalesced flips (vs its base `1be10fb`) | 0.94 / 0.73, backfills 15,420 → 42 | No | See PR 68 |
| PR 68's base `1be10fb` vs prod `5b2cf3e` | 0.82 / **0.27** (small deletion 0.19) | No | PRs 70, 71, 72 are what differ; something in them makes deletions about 4x slower on this workload |
| B all-plain | 3.2 s import (~0.006) | Yes | See M3 |
| B-retained (proposal's exceptions) | 0.28 (cross-runner) | No: drain 0–1% | — |
| B-retained, graph edges keyed | **0.03** (3.9 s), 120 backfills | Yes in effect: 3/4 import probes and 4/9 large-deletion probes succeed within a 4–6 s burst | Edge reads become audience-key equality; not shown authorization-equivalent |
| B-retained, chat / Resources / Files / Users keyed one at a time | 1.00 / 1.06 / 1.05 / 1.03 | No | — |

## M4 — the answer

Assumptions, for all three answers: the shapes are synthetic, built with the Indexed gateway's
own predicate builders (pinned source `da746b7`) and calibrated to prod's logged catalog counts.
They are not prod's catalog, and the date windows and the per-table mix of historical requests
are synthesized. The rows are synthetic. Hosted Actions runners have 4 visible CPUs, the engine
runs under a 1 CPU quota (2 where marked), and PostgreSQL and the stream store run outside the
quota. Every result is a single run per row; the same-runner noise is about 5%. Estimates are
marked.

**1. Where does the time go today?** Into evaluating each changed row against the membership
circuit, under one registry lock. The profile assigns 68.7% of CPU to the sequencer's
`SubqueryRegistry::on_table_delta` (`apps/engine/src/subquery.rs`), 19.3% to flip walks
(`propagate_one`, `requery_and_move_shapes`), 8.1% to the memory sampler (`mem_totals`), 0.8% to
the PostgreSQL client, 0.3% to stream appends, and 0% to the marker wait. One changed row costs
19–23 ms of membership work against the Nov 24 catalog (224 s across 11,931 rows). Within the
A-family catalogs the import time tracks the catalog's feed entries almost linearly: 0.036, 0.037
and 0.039 s per entry for Nov 23, half-subqueries and Nov 24 on EPYC 7763.

The registry mutex serializes all of this onto one core. At 2 and 4 CPUs the engine still uses
1.02–1.05 cores, and 2 CPUs on the same runner take 0.96x as long. The other account starves for
a structural reason, not because of a long lock hold. A gateway drain (fence row, then
`drained-through`) gets a receipt only after the sequencer has applied every earlier commit in
the single change log and `wait_for_source_effects` has seen their flips finish (71–82 s of
sequencer wall time per import, with single waits up to 14 s). A live change is a later commit
in the same log. While the backlog ahead of it is longer than 2 s, both fail, whichever account
they belong to.

**2. Does the proposed model remove the stall, and by how much?** All-plain B removes it: the
import takes 3.2 s against 531 s for the unprofiled A control (99.4% less), the large deletion
1.8 s against 137 s, zero backfills, and the other account is served (drain, subset and live
100% on import; 89% over 9 attempts on the large deletion). With the proposal's exceptions kept
as subquery scopes, B-retained takes 132 s against 330 s for A on the same runner model (0.40x;
0.28x against the A control). Drains and live changes stay at 0–1%, so it shortens the outage
about 2.5–3.6x but does not remove it.

Keying the exceptions one class at a time on one runner shows that **graph edges carry nearly
all of it**. With edges keyed the import takes 3.9 s (0.03x of B-retained), backfills fall from
7,020 to 120, and the subquery state falls from 1,284 nodes, 14,822 edges and 223k contributors
to 276, 1,047 and 41.6k. Keying chats, Resources, Files or Users alone changes nothing
(0.86–1.06x). So the model removes the stall only if graph edges are not windowed subquery scopes.

Reading used: the harness's retained edge scope is the gateway's real predicate plus the
synthetic date windows (`from_id` inside `record` inside `record_date_index`). Whether edges can
carry an audience key is the proposal's question; it is not settled here.

**3. Which bridge should be built first, and what does each buy?** Only changes that remove
subquery work keep the other account served: B all-plain, plain-only, and B with graph edges
keyed. Every engine-side bridge leaves drains and live changes at 0–22%. So the one to build
first is **the proposal's model with graph-edge reads keyed like the other hot tables** (or
otherwise not windowed nested subqueries). It is the only proposal-shaped variant measured that
removes the outage rather than shortening it.

Until then, these make a slow night shorter, not an outage shorter, with no change to drains:

- Memory sampler at 5 s or off: 4–12% faster, and almost free.
- Yield after each row: subsets served (92–100%); drains are not.
- PR 68: 6% faster on the import and 27% faster on the large deletion against its own base.
- The deletion regression between PR 68's base and prod (PRs 70–72, about 4x on deletions on the
  same runner) deserves its own look.

Do not build sequencer lock slicing on the row path: the lock is already per row (max hold
79 ms). Do not build per-commit membership as prototyped: it is 0.85–0.98x and runs out of memory
on a 7,114-row deletion unless sliced, and sliced it gains almost nothing. More CPU buys subset
service only.

### What prod is exposed to now (estimate)

Inputs from the brief: prod's engine has had 2 vCPUs since 08:33 UTC on Nov 24, and its catalog
was reset to nearly empty at 05:36 UTC. Subquery shapes regrow by about 740–1,900 a day (2,354
three days after a restart, 4,287 four days after). Nothing retires an idle shape. The nightly iOS
end-to-end run (a first-calendar import plus an account deletion) ran at 03:20 and 07:00 UTC on
Nov 23.

**Points on the curve.** Import seconds at 1 CPU on EPYC 7763:

| Subquery shapes | Import s |
| --- | ---: |
| 0 (plain-only) | 3.4 |
| 2,144 (half-subqueries) | 263 |
| 2,354 (Nov 23 catalog) | 165 |
| 4,287 (Nov 24 catalog) | 505–567 (median ~535 over 7 runs) |

At 2 CPUs the import takes 0.96x as long. So **prod's second vCPU should not change tonight's
answer for drains and live changes**; it does keep subset queries served (100% vs 66–78%).

Prod's own two points are steeper than the synthetic curve:

| Prod night | Subquery shapes | Import + deletion | CPU | Outcome |
| --- | ---: | --- | ---: | --- |
| Nov 23 | 2,354 | cleared in ~15 min | ~60% | no outage; at most ~9 CPU-minutes, paced by the writer |
| Nov 24 | 4,287 | 1 h 48 min | 100% | outage |

That is at least 12x the work for 1.8x the shapes, an exponent of about 4.1, where the synthetic
Nov 23 → Nov 24 curve has about 2.0. The bands below use the prod-anchored curve
(W = 108 CPU-min × (S / 4,287)^4.1) as the central estimate, and the synthetic exponent anchored at
Nov 24 as the other bound where the catalog is larger than Nov 24's. They assume an outage roughly
when the engine's work exceeds the ~15 minutes over which the writer spreads the import (about
15 CPU-min, around 2,700 subquery shapes), and 2 vCPU ≈ 1 vCPU for this path.

| Nightly run (03:20 UTC) | Days since reset | Subquery shapes (740–1,900/day) | Engine work, estimate | Other accounts, estimate |
| --- | ---: | --- | --- | --- |
| Tonight, Nov 25 | 0.9 | 670–1,720 | under 3 CPU-min (under 5 on the synthetic slope) | Served. Below Nov 23's size, which cleared with no outage; at most seconds-long drain gaps while each of the six transactions is applied |
| Nov 26 | 1.9 | 1,410–3,610 | 1 CPU-min at the low rate; 54–78 CPU-min at the high rate | Low regrowth: served. High regrowth: drains and live changes fail for roughly 40–80 minutes |
| Nov 27 | 2.9 | 2,150–5,510 | 6 to 180–310 CPU-min | Low: served or marginal. High: 3–5 hour outage |
| Nov 28 | 3.9 | 2,890–7,420 | 13–21 to 310–1,050 CPU-min | Low: at the threshold, possibly a 10–20 min outage. High: the engine stays behind for most of the day |
| Nov 29 and later | 4.9+ | 3,630–9,320+ | 54+ CPU-min; growing about as the 2nd–4th power of shapes | An outage every night; at high regrowth the engine does not catch up between nightly runs |

At the low regrowth rate the catalog reaches Nov 24's stalled size (4,287) about 5.8 days after
the reset (around Nov 30); at the high rate about 2.3 days after (around midday Nov 26). A restart
that empties the catalog resets this clock; nothing else measured here does.

These are estimates with wide error. The synthetic engine is 2.5–12x cheaper per change than prod
was, and the gap grows with catalog size. Prod's write pacing, its real row densities and its
second deletion are not modelled. The total-shape cap (20,000) is not modelled either.


## Takeover round 2: today's prod engine, the PR 70–72 bisect, B-cold, E-only and E+R

Actions run `45388936382`, harness `5169054`, workflow job `paired` with `[paired-2]`. Every group
ran back to back on one runner, each run with a fresh PostgreSQL 18, the Nov 24 catalog, 8 flip
workers and `ELECTRIC_CIRCUITS_MAX_SHAPES=20000`. Binaries:

| Binary | Commit | Built by Actions run |
| --- | --- | --- |
| `prod` | `5b2cf3e` | `45342910224` |
| `base` | `1be10fb` | `45362388808` |
| `pr70` | `e10c4bd` | `45388936382` |
| `pr71` | `a248886` | `45388936382` |
| `pr69` | `b42f70d` (prod since 08:33 UTC Nov 24) | `45388936382` |

Each new build is a `git archive` of its commit with no source change, release profile with
debug level 1 and frame pointers. Per-run commands are in `paired2/LABEL/manifest.json`.

### Is today's prod engine's stall longer or shorter than yesterday's?

About the same. Same runner (EPYC 7763), same catalog and workloads:

| Engine, vCPU | Workload | First delivery → empty s | × yesterday | CPU % | Drain | Subset | Live | Served? |
| --- | --- | ---: | ---: | ---: | --- | --- | --- | --- |
| `5b2cf3e`, 1 (yesterday) | import | 532.3 | 1.00 | 98 | 0% (0/533) | 68% (365/533) | 0% (0/533) | no |
| `5b2cf3e`, 1 | large-delete | 159.1 | 1.00 | 95 | 0% (0/210) | 84% (176/210) | 0% (0/210) | no |
| `5b2cf3e`, 1 | small-delete | 253.4 | 1.00 | 99 | 0% (0/257) | 58% (148/257) | 0% (0/257) | no |
| `b42f70d`, 2 (today) | import | 547.1 | 1.03 | 103 | 0% (0/548) | 100% (548/548) | 0% (0/548) | no |
| `b42f70d`, 2 | large-delete | 153.3 | 0.96 | 100 | 0% (0/202) | 100% (202/202) | 0% (0/202) | no |
| `b42f70d`, 2 | small-delete | 264.8 | 1.04 | 104 | 0% (0/285) | 100% (285/285) | 0% (0/285) | no |

The difference is within the 5% same-runner noise. PR 69 (timestamp-bound comparison) neither
helps nor hurts this workload, and the second vCPU still goes unused by the serialized path
(the engine uses 1.00–1.04 cores). The `cpu-split` job (`5b2cf3e` at 2 CPU, `b42f70d` at 1 CPU)
separates the two changes and is added below when it lands.

### Bisect: PR 70 carries the slowdown since `1be10fb`

Commands: `run.py … --variant A --workloads import` on Xeon 8573C, and `--workloads large-delete`
on Xeon 6975P-C, one runner each, in commit order.

| Engine | Import s (8573C) | × `1be10fb` | Large deletion s (6975P-C) | × `1be10fb` |
| --- | ---: | ---: | ---: | ---: |
| `1be10fb` (PR 68's base) | 294.8 | 1.00 | 26.6 | 1.00 |
| `e10c4bd` (after PR 70, exact removals) | 379.7 | **1.29** | 99.9 | **3.76** |
| `a248886` (after PR 71, commit positions) | 383.8 | 1.30 | 94.9 | 3.57 |
| `5b2cf3e` (after PR 72, boot lock) | 381.9 | 1.30 | 91.9 | 3.45 |
| `b42f70d` (after PR 69, today's prod) | 385.2 | 1.31 | 93.4 | 3.51 |

Backfills are identical on every row (15,420 import; 17,717 deletion), so the added cost is
inside the engine, not more Postgres reads. No row keeps the other account served: drains and
live changes are 0% on the imports and 2–10% on the deletions.

The orchestrator's "roughly doubled" compared runs on two CPU models; the inputs were otherwise
identical. Both runs used Indexed source `da746b7`, the same 10,563 requests, the same engine
cardinalities (4,287 subquery shapes, 1,406 nodes, 26,367 edges, 237,341 contributors) and the
same 13,081 import changes. The harness diff `b187502..9f5ad6d` touches only B's subscription
identity, the commit-membership flag and its check. What differed was the engine binary and the
runner (6975P-C for the 286 s run, 8573C for the 531 s run). On one runner the import gap is
1.29x, and the deletion gap is 3.8x.

PR 70 is "Remove rows a changes-only feed's client holds only from its snapshot". A profile of the
large deletion on `1be10fb` and `e10c4bd` is running (`[paired-3]`, `pr70-profile`) to name the
function that got more expensive.
