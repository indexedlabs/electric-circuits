# OTTO-10992: stalled-group backlog on the real stack

Status: **baseline on `main` measured; variants (#53 at `56f3dd1`, S1, S2, S3) running.** This page is regenerated as each variant lands.

## How it is measured

Every row is one scenario on a fresh GitHub Actions `ubuntu-latest` runner (4 vCPU, 16 GiB; the runner id and CPU are on each row). Each runner builds the engine at the variant's commit, with the observation-only measurement patch applied identically (`harness/otto-10992/measurement.patch`, or `measurement-56f3dd1.patch` for #53, whose files it rewrote). It then runs PostgreSQL 18.6, Durable Streams 0.1.5 (file-backed) and the engine, with a fault proxy on the engine's DS connection. Nothing is mocked.

- **Phases.** 30 s warmup, then the hold (the fault, or the burst), then 120 s of recovery. `pgbench` writes at a fixed rate the whole time. Each transaction is 4 rows: `record`, `plain_events`, `record_items`, `record_meta`.
- **Streams.**
  - The **faulted group** is a Record-scope group: `record` where `group_id = 1`, plus `record_items` and `record_meta`.
  - The **unaffected plain shape** is `plain_events`.
  - The **unaffected group** is `plain_events` + `sparse_events`.
  - A client-style tailer reads the unaffected streams directly from DS (not through the proxy) and counts the envelopes they receive.
- **Markers.** A marker row is inserted into `plain_events` 1 s into the hold ("during") and at the clear ("after"). Its latency runs from the Postgres commit to its appearance in each unaffected stream.
- **Faults.** The proxy switches faults in place through `/__fault`; it never restarts.
  - `a`: every request to the faulted group's stream is held open.
  - `b-N`: every engine request to DS is held for N minutes, the change-log append included.
  - `c-MS`: MS ms of delay on each request to the group's stream.
  - `d`: like `a`, on a group whose only source, `sparse_events`, gets one write per 10 s.
  - `e`: 200 groups registered back to back with no fault. Each is `plain_events` + `record` with no filter, so every commit fans out to every group.
- **Scenario code:** `prototype/experiment.sh`. **Per-row numbers:** `prototype/summarize.py`. **Table:** `prototype/table.py`.

**Columns**
- **env/s:** the engine's `envelopes_processed` rate.
- **Unaffected env in hold:** envelopes the tailer received during the hold.
- **Resumes after clear:** the first envelope after the clear on the unaffected plain stream and on the faulted group's stream.
- **Caught up after clear:** the after-marker's latency when it was seen within the 120 s recovery window. Otherwise it is estimated from envelope rates: backlog = (writer rate × 4 − hold rate) × hold, divided by (recovery rate − writer rate × 4). "Never" means recovery processed slower than the writers produce.
- **Processed−checkpoint lag:** the sequencer's processed offset minus its durable checkpoint.
- **Change-log lag:** the `changes/0` head minus the processed offset.
- **Retained WAL:** the slot's `pg_wal_lsn_diff(current, restart_lsn)`.

## Harness corrections (what earlier numbers on this branch got wrong)

1. **The fault proxy broke the no-fault control.** It opened a new TCP connection to DS for every request and ran out of ephemeral ports (`EADDRNOTAVAIL`) about 40 s into control-200 (run 41209890967). From then on every engine append failed. It now keeps one upstream connection per downstream connection.
2. **Toggling a fault restarted the proxy,** which dropped every engine connection, the `changes/0` append included, at hold start and at the clear. Faults now switch in place, and held requests are dropped unanswered when a fault clears.
3. **The unaffected-delivery counters could not count.** The HEAD probe always reported 0 envelopes, and DS 0.1.5 answers `live=false` with `400 invalid live mode`. The tailer now uses catch-up reads.
4. **Retractions.** The earlier claims that `changes/0` and the slot's `confirmed_flush` freeze under one stalled group, and that the stall is engine-wide at 1000 txn/s with a frozen slot, came from items 1 and 2. They are retracted. With a clean proxy, ingest keeps going during a group stall: `confirmed_flush` advances, the change log grows by about 125 MiB in 5 min at 200 txn/s, and retained WAL stays small. The **sequencer** is what stops.

## Baseline: `main` (`ac68597`), run [41215671062](https://github.com/indexedlabs/electric-circuits/actions/runs/41215671062)

| scenario | runner | rate (txn/s) | hold | env/s warmup → hold | unaffected plain env in hold | unaffected group env in hold | marker during (plain / group) | marker after (plain / group) | resumes after clear (plain / faulted group) | caught up after clear | RSS peak MiB (hold / recovery) | lane backlog peak (batches / MiB) | receipt queue peak | processed−checkpoint lag peak MiB | change-log lag at clear MiB | retained WAL MiB (hold start → peak) | host load1 peak |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `control` | #1000065354 · EPYC 9V74 80-Core · 4 vCPU | 200 | 299 s | 792 → 793 | 59,353 | 59,353 | 206 ms / 207 ms | 55 ms / 56 ms | 216 ms / – | – | 39.0 / 38.7 | 1 / 0.0 | 1 | 0.9 | 0.0 | 1.2 → 25.5 | 3.6 |
| `control-900` | #1000065357 · EPYC 9V74 80-Core · 4 vCPU | 200 | 899 s | 789 → 799 | 179,527 | 179,527 | 255 ms / 256 ms | 331 ms / 334 ms | 496 ms / – | – | 38.9 / 39.0 | 1 / 0.0 | 1 | 1.1 | 0.0 | 1.2 → 26.2 | 3.8 |
| `a` | #1000065350 · EPYC 7763 · 4 vCPU | 200 | 299 s | 792 → 0 | 20 | 20 | 303.7 s / 303.8 s | not seen / not seen | 2.2 s / 2.2 s | ≈958 s (est.) | 37.9 / 673.2 | 1 / 0.0 | 1 | 0.6 | 124.6 | 2.2 → 11.6 | 3.7 |
| `b-1` | #1000065362 · EPYC 9V74 80-Core · 4 vCPU | 200 | 59 s | 789 → 0 | 13 | 13 | 60.3 s / 60.3 s | 39.6 s / 39.6 s | 772 ms / 772 ms | 40 s | 39.1 / 67.4 | 1 / 0.0 | 1 | 1.3 | 0.0 | 3.0 → 30.3 | 3.5 |
| `b-5` | #1000065359 · EPYC 9V74 80-Core · 4 vCPU | 200 | 299 s | 797 → 0 | 21 | 20 | 300.3 s / 300.3 s | not seen / not seen | 274 ms / 274 ms | ≈1,552 s (est.) | 40.9 / 61.0 | 1 / 0.0 | 1 | 1.0 | 0.0 | 1.2 → 141.4 | 3.4 |
| `b-15` | #1000065356 · EPYC 9V74 80-Core · 4 vCPU | 200 | 899 s | 798 → 0 | 13 | 12 | 901.5 s / 901.5 s | not seen / not seen | 2.5 s / 2.5 s | ≈1,566 s (est.) | 41.5 / 79.7 | 1 / 0.0 | 1 | 1.2 | 1.0 | 1.8 → 447.7 | 3.0 |
| `c-200` | #1000065351 · EPYC 7763 · 4 vCPU | 200 | 299 s | 818 → 7 | 519 | 519 | 155.2 s / 155.2 s | not seen / not seen | 263 ms / 263 ms | ≈887 s (est.) | 639.8 / 639.6 | 1 / 0.0 | 1 | 0.0 | 122.0 | 8.7 → 13.1 | 3.4 |
| `c-2000` | #1000065352 · EPYC 9V74 80-Core · 4 vCPU | 200 | 299 s | 799 → 1 | 50 | 49 | 299.9 s / 299.9 s | not seen / not seen | 1.3 s / 1.3 s | ≈246 s (est.) | 43.8 / 668.7 | 1 / 0.0 | 1 | 0.0 | 123.6 | 1.8 → 15.6 | 3.5 |
| `d` | #1000065358 · EPYC 9V74 80-Core · 4 vCPU | 200 | 299 s | 794 → 0 | 178 | 179 | 302.3 s / 302.3 s | 58.7 s / 58.7 s | 4.3 s / 2.3 s | 59 s | 38.3 / 659.3 | 1 / 0.0 | 1 | 4.3 | 124.0 | 2.1 → 14.3 | 2.8 |
| `e` | #1000065355 · EPYC 9V74 80-Core · 4 vCPU | 200 | 299 s | 793 → 113 | 8,524 | 8,523 | 2.2 s / 2.2 s | not seen / not seen | 262 ms / – | never (not draining) | 439.4 / 524.7 | 53 / 0.0 | 53 | 0.1 | 4.6 | 1.2 → 181.9 | 4.8 |
| `control:1000` | #1000065353 · EPYC 9V74 80-Core · 4 vCPU | 1,000 | 299 s | 1,426 → 1,480 | 110,641 | 110,641 | 53.8 s / 53.8 s | not seen / not seen | 331 ms / – | never (not draining) | 41.8 / 41.8 | 1 / 0.0 | 1 | 1.6 | 0.0 | 66.0 → 714.3 | 4.2 |
| `a:1000` | #1000065361 · EPYC 9V74 80-Core · 4 vCPU | 1,000 | 299 s | 1,077 → 0 | 28 | 28 | 384.8 s / 384.8 s | not seen / not seen | 2.2 s / 2.2 s | never (not draining) | 48.8 / 1,843.5 | 1 / 0.0 | 1 | 1.0 | 349.9 | 73.6 → 568.3 | 5.5 |

Per-row summaries: [`harness/otto-10992/raw/main-41215671062/`](../../harness/otto-10992/raw/main-41215671062/). The full time series are in that run's artifacts, one per job.

### What the baseline shows

- **One held group stream stops delivery to every stream** (`a`). Unaffected plain and group streams received 20 envelopes in 300 s, against 59,353 in the control. The "during" marker arrived 303.7 s after commit, that is 3.7 s after the clear. `envelopes_processed` is flat for the whole hold. Ingest keeps up the whole time, so the cost lands on the change-log backlog: 125 MiB at the clear, about 16 min to drain at the recovery rate.
  - Mechanism: every commit with group output waits until that group's append lands (`wait_for_group_effects`, `sequencer.rs:987`). While any group exists, the next commit also waits on the previous commit's group paths (the group fence, `:789`). `append_reliable` retries a held append forever.
- **A slow group throttles the whole engine** (`c-200`, `c-2000`). With 200 ms per append to one group, unaffected delivery fell from 198/s to 1.7/s (519 envelopes in 300 s) and the during-marker took 155 s. With 2 s per append, 50 envelopes got through. Nothing failed, so nothing anywhere signals a stall.
- **A sparse source stalls as hard as a busy one** (`d`). The first sparse write in the hold blocks the sequencer. Only 178 unaffected envelopes arrived. Catch-up took 59 s, because the backlog has few group outputs to emit.
- **Whole-table group fan-out collapses throughput without any fault** (`e`). While 200 unfiltered groups register, processing drops from 793 to 113 env/s and is still at 100 env/s after the window, below the writer rate, so it never catches up. Lane backlog and receipt depth reach 53 and RSS reaches 439–525 MiB. These groups match every commit; Record-scope groups with a `where` would fan out far less.
- **DS outage** (`b-N`): ingest is held too, so `confirmed_flush` freezes and retained WAL grows (141 MiB at 5 min, 448 MiB at 15 min). Recovery from a 1-min outage took 40 s. The 5- and 15-min outages take about 26 min to drain at the recovery rate.
- **Memory during a stall stays flat** (38–44 MiB, versus 39 MiB in the control), because nothing is buffered: work waits in the change log. The **recovery** peaks of 640–1,840 MiB come from catch-up. DS 0.1.5 advertises no `max_chunk_bytes`, so the engine reads up to 64 MiB pages of backlog at once. That cost depends on the DS version and is the same for every variant.
- **1000 txn/s is above this runner's capacity even without a fault** (`control:1000`): about 1,450 env/s processed against 4,000/s produced, and the during-marker took 54 s. The 1000 rows are for comparison with the earlier headline only. The verdicts use 200 txn/s, where the control keeps up with about 200 ms marker latency.

## Variants

Pending: #53 (`56f3dd1`), S1 (`proto/otto-10992-s1`), S2 (`proto/otto-10992-s2`) and S3 (`proto/otto-10992-s3`) are queued or running on the same scenarios.
