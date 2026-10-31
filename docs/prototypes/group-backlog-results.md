# OTTO-10992: stalled-group backlog on the real stack

Status: **all four variants measured.** Raw time series and per-row summaries are archived under `harness/otto-10992/raw/`.

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

Each variant ran the same ten scenarios on the same `ubuntu-latest` image. The per-row table includes the runner, write rate, hold, unaffected delivery, faulted delivery, recovery, lane and receipt peaks, replay lag, retained WAL, RSS, host load, and a box-limit flag for the 1,000 txn/s rows. `a`, `a:1000`, `b-*`, `c-*`, and `d` write the same four-table Record workload; at 200 txn/s for 59 s / 299 s / 899 s that is approximately 9.1 / 45.9 / 137.9 MiB of source rows (45.96 MiB was measured for the 299 s case), and at 1,000 txn/s for 299 s it is approximately 229.8 MiB. The source write continues during a Durable Streams outage.

| variant | scenario | rate | hold | env/s hold | unaffected env | faulted env | resume | lane bytes / batches | receipt depth | proc−checkpoint lag | WAL start → peak | RSS peak | load1 | box-limited | raw |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|
| #53 (56f3dd1) | `control` | 200 | 299s | 804 | 60094 | — | — | 0.0 MiB / 4 | 4 | 0.9 MiB | 0.9 → 15.5 MiB | 39.5 MiB | 2.7 | — | [`../../harness/otto-10992/raw/actions-41215755747/artifact-13047849659`](../../harness/otto-10992/raw/actions-41215755747/artifact-13047849659) |
| #53 (56f3dd1) | `a` | 200 | 299s | 798 | 59645 | 36 | — | 19.7 MiB / 35380 | 35380 | 0.9 MiB | 1.4 → 15.5 MiB | 184.1 MiB | 3.7 | — | [`../../harness/otto-10992/raw/actions-41215755747/artifact-13049200428`](../../harness/otto-10992/raw/actions-41215755747/artifact-13049200428) |
| #53 (56f3dd1) | `a:1000` | 1000 | 299s | 1042 | 77902 | 72 | — | 25.9 MiB / 46541 | 46541 | 1.1 MiB | 67.8 → 781.6 MiB | 231.9 MiB | 6.6 | yes | [`../../harness/otto-10992/raw/actions-41215755747/artifact-13049595507`](../../harness/otto-10992/raw/actions-41215755747/artifact-13049595507) |
| #53 (56f3dd1) | `b-1` | 200 | 59s | 0 | 30 | 69 | 0.71s | 0.0 MiB / 35 | 35 | 0.8 MiB | 2.2 → 32.2 MiB | 41.5 MiB | 3.5 | — | [`../../harness/otto-10992/raw/actions-41215755747/artifact-13048752942`](../../harness/otto-10992/raw/actions-41215755747/artifact-13048752942) |
| #53 (56f3dd1) | `b-5` | 200 | 299s | 0 | 23 | 69 | 2.36s | 1.1 MiB / 2057 | 2057 | 1.9 MiB | 1.0 → 138.9 MiB | 49.1 MiB | 3.0 | — | [`../../harness/otto-10992/raw/actions-41215755747/artifact-13048829918`](../../harness/otto-10992/raw/actions-41215755747/artifact-13048829918) |
| #53 (56f3dd1) | `b-15` | 200 | 899s | 0 | 52 | 156 | 1.00s | 0.1 MiB / 141 | 141 | 1.8 MiB | 1.0 → 419.8 MiB | 42.9 MiB | 2.7 | — | [`../../harness/otto-10992/raw/actions-41215755747/artifact-13049197940`](../../harness/otto-10992/raw/actions-41215755747/artifact-13049197940) |
| #53 (56f3dd1) | `c-200` | 200 | 299s | 799 | 59767 | 1116 | — | 81.1 MiB / 145644 | 145644 | 0.9 MiB | 1.2 → 25.9 MiB | 644.9 MiB | 4.3 | — | [`../../harness/otto-10992/raw/actions-41215755747/artifact-13049116711`](../../harness/otto-10992/raw/actions-41215755747/artifact-13049116711) |
| #53 (56f3dd1) | `c-2000` | 200 | 299s | 800 | 59803 | 201 | — | 81.3 MiB / 146022 | 146022 | 1.0 MiB | 1.0 → 15.2 MiB | 643.7 MiB | 4.5 | — | [`../../harness/otto-10992/raw/actions-41215755747/artifact-13049101524`](../../harness/otto-10992/raw/actions-41215755747/artifact-13049101524) |
| #53 (56f3dd1) | `d` | 200 | 299s | 798 | 59637 | 0 | — | 0.0 MiB / 8 | 8 | 0.9 MiB | 1.4 → 15.6 MiB | 40.1 MiB | 4.4 | — | [`../../harness/otto-10992/raw/actions-41215755747/artifact-13048934690`](../../harness/otto-10992/raw/actions-41215755747/artifact-13048934690) |
| #53 (56f3dd1) | `e` | 200 | 299s | 22 | 1846 | — | — | 3.8 MiB / 4758 | 4758 | 0.2 MiB | 1.7 → 189.0 MiB | 639.5 MiB | 8.3 | — | [`../../harness/otto-10992/raw/actions-41215755747/artifact-13048923904`](../../harness/otto-10992/raw/actions-41215755747/artifact-13048923904) |
| S1 (cc4d78d) | `control` | 200 | 299s | 801 | 59908 | — | — | 0.0 MiB / 5 | 5 | 0.9 MiB | 0.9 → 15.1 MiB | 38.6 MiB | 3.6 | — | [`../../harness/otto-10992/raw/actions-41215799086/artifact-13047984596`](../../harness/otto-10992/raw/actions-41215799086/artifact-13047984596) |
| S1 (cc4d78d) | `a` | 200 | 299s | 290 | 21679 | 56 | 2.26s | 37.6 MiB / 67627 | 67627 | 0.1 MiB | 1.3 → 156.8 MiB | 116.1 MiB | 4.2 | — | [`../../harness/otto-10992/raw/actions-41215799086/artifact-13049227057`](../../harness/otto-10992/raw/actions-41215799086/artifact-13049227057) |
| S1 (cc4d78d) | `a:1000` | 1000 | 299s | 276 | 20723 | 70 | 2.68s | 37.1 MiB / 66692 | 66692 | 0.1 MiB | 69.3 → 1007.4 MiB | 122.2 MiB | 6.9 | yes | [`../../harness/otto-10992/raw/actions-41215799086/artifact-13049511443`](../../harness/otto-10992/raw/actions-41215799086/artifact-13049511443) |
| S1 (cc4d78d) | `b-1` | 200 | 59s | 0 | 26 | 87 | 0.70s | 0.0 MiB / 32 | 32 | 0.9 MiB | 2.1 → 44.2 MiB | 41.3 MiB | 4.0 | — | [`../../harness/otto-10992/raw/actions-41215799086/artifact-13049177251`](../../harness/otto-10992/raw/actions-41215799086/artifact-13049177251) |
| S1 (cc4d78d) | `b-5` | 200 | 299s | 0 | 14 | 31 | 2.36s | 0.8 MiB / 1403 | 1403 | 2.1 MiB | 1.2 → 139.3 MiB | 45.1 MiB | 3.5 | — | [`../../harness/otto-10992/raw/actions-41215799086/artifact-13049815094`](../../harness/otto-10992/raw/actions-41215799086/artifact-13049815094) |
| S1 (cc4d78d) | `b-15` | 200 | 899s | 0 | 12 | 82 | 2.48s | 0.7 MiB / 1181 | 1181 | 1.3 MiB | 1.0 → 447.9 MiB | 44.8 MiB | 3.9 | — | [`../../harness/otto-10992/raw/actions-41215799086/artifact-13050075852`](../../harness/otto-10992/raw/actions-41215799086/artifact-13050075852) |
| S1 (cc4d78d) | `c-200` | 200 | 299s | 306 | 23052 | 1517 | 0.23s | 38.7 MiB / 69539 | 69539 | 0.2 MiB | 1.6 → 156.4 MiB | 117.5 MiB | 3.3 | — | [`../../harness/otto-10992/raw/actions-41215799086/artifact-13049337688`](../../harness/otto-10992/raw/actions-41215799086/artifact-13049337688) |
| S1 (cc4d78d) | `c-2000` | 200 | 299s | 379 | 28299 | 194 | 1.85s | 50.9 MiB / 91524 | 91524 | 0.5 MiB | 7.2 → 89.5 MiB | 265.2 MiB | 3.1 | — | [`../../harness/otto-10992/raw/actions-41215799086/artifact-13049117503`](../../harness/otto-10992/raw/actions-41215799086/artifact-13049117503) |
| S1 (cc4d78d) | `d` | 200 | 299s | 801 | 59882 | 0 | 2.67s | 0.0 MiB / 31 | 31 | 78.6 MiB | 1.5 → 15.2 MiB | 80.1 MiB | 3.9 | — | [`../../harness/otto-10992/raw/actions-41215799086/artifact-13049870696`](../../harness/otto-10992/raw/actions-41215799086/artifact-13049870696) |
| S1 (cc4d78d) | `e` | 200 | 299s | 23 | 1886 | — | — | 3.8 MiB / 4750 | 4750 | 0.2 MiB | 1.6 → 188.5 MiB | 525.3 MiB | 8.3 | — | [`../../harness/otto-10992/raw/actions-41215799086/artifact-13049418071`](../../harness/otto-10992/raw/actions-41215799086/artifact-13049418071) |
| S2 | `control` | 200 | 299s | 797 | 59643 | — | — | 0.0 MiB / 3 | 3 | 0.9 MiB | 1.6 → 6.7 MiB | 38.4 MiB | 3.9 | — | [`../../harness/otto-10992/raw/actions-41216147178/artifact-13050560098`](../../harness/otto-10992/raw/actions-41216147178/artifact-13050560098) |
| S2 | `a` | 200 | 299s | 806 | 60228 | 77 | 1.26s | 3.4 MiB / 6190 | 6190 | 0.9 MiB | 1.7 → 14.6 MiB | 51.9 MiB | 3.7 | — | [`../../harness/otto-10992/raw/actions-41216147178/artifact-13049971724`](../../harness/otto-10992/raw/actions-41216147178/artifact-13049971724) |
| S2 | `a:1000` | 1000 | 299s | 954 | 71365 | 66 | 0.57s | 3.3 MiB / 5908 | 5908 | 1.0 MiB | 69.1 → 867.0 MiB | 53.3 MiB | 7.0 | yes | [`../../harness/otto-10992/raw/actions-41216147178/artifact-13049584453`](../../harness/otto-10992/raw/actions-41216147178/artifact-13049584453) |
| S2 | `b-1` | 200 | 59s | 0 | 12 | 48 | 1.23s | 0.1 MiB / 150 | 150 | 1.2 MiB | 1.3 → 30.0 MiB | 41.7 MiB | 3.2 | — | [`../../harness/otto-10992/raw/actions-41216147178/artifact-13049401997`](../../harness/otto-10992/raw/actions-41216147178/artifact-13049401997) |
| S2 | `b-5` | 200 | 299s | 0 | 36 | 77 | 1.27s | 0.5 MiB / 906 | 906 | 1.0 MiB | 2.0 → 144.1 MiB | 45.1 MiB | 4.0 | — | [`../../harness/otto-10992/raw/actions-41216147178/artifact-13050066179`](../../harness/otto-10992/raw/actions-41216147178/artifact-13050066179) |
| S2 | `b-15` | 200 | 899s | 0 | 18 | 66 | 0.49s | 0.0 MiB / 65 | 65 | 2.0 MiB | 1.1 → 441.3 MiB | 42.3 MiB | 2.9 | — | [`../../harness/otto-10992/raw/actions-41216147178/artifact-13050067948`](../../harness/otto-10992/raw/actions-41216147178/artifact-13050067948) |
| S2 | `c-200` | 200 | 299s | 805 | 60173 | 1488 | 0.37s | 8.2 MiB / 14793 | 14793 | 0.9 MiB | 1.0 → 15.4 MiB | 72.8 MiB | 4.4 | — | [`../../harness/otto-10992/raw/actions-41216147178/artifact-13049284225`](../../harness/otto-10992/raw/actions-41216147178/artifact-13049284225) |
| S2 | `c-2000` | 200 | 299s | 791 | 59139 | 224 | 0.70s | 8.1 MiB / 14535 | 14535 | 1.2 MiB | 1.2 → 18.6 MiB | 73.9 MiB | 4.6 | — | [`../../harness/otto-10992/raw/actions-41216147178/artifact-13050331467`](../../harness/otto-10992/raw/actions-41216147178/artifact-13050331467) |
| S2 | `d` | 200 | 299s | 797 | 59596 | 0 | 0.87s | 0.0 MiB / 4 | 4 | 0.9 MiB | 0.9 → 6.8 MiB | 38.8 MiB | 2.6 | — | [`../../harness/otto-10992/raw/actions-41216147178/artifact-13050021418`](../../harness/otto-10992/raw/actions-41216147178/artifact-13050021418) |
| S2 | `e` | 200 | 299s | 23 | 1872 | — | — | 4.5 MiB / 5614 | 5614 | 0.2 MiB | 1.8 → 185.4 MiB | 521.3 MiB | 7.4 | — | [`../../harness/otto-10992/raw/actions-41216147178/artifact-13050505621`](../../harness/otto-10992/raw/actions-41216147178/artifact-13050505621) |
| S3 | `control` | 200 | 299s | 800 | 59818 | — | — | 0.0 MiB / 1 | 1 | 0.9 MiB | 1.5 → 6.8 MiB | 38.5 MiB | 3.4 | — | [`../../harness/otto-10992/raw/actions-41215777583/artifact-13049503670`](../../harness/otto-10992/raw/actions-41215777583/artifact-13049503670) |
| S3 | `a` | 200 | 299s | 801 | 59827 | 55 | — | 0.0 MiB / 1 | 1 | 1.5 MiB | 2.1 → 15.1 MiB | 113.5 MiB | 3.5 | — | [`../../harness/otto-10992/raw/actions-41215777583/artifact-13049463901`](../../harness/otto-10992/raw/actions-41215777583/artifact-13049463901) |
| S3 | `a:1000` | 1000 | 299s | 994 | 73913 | 40 | — | 0.0 MiB / 1 | 1 | 1.3 MiB | 75.0 → 792.6 MiB | 159.9 MiB | 7.7 | yes | [`../../harness/otto-10992/raw/actions-41215777583/artifact-13049423620`](../../harness/otto-10992/raw/actions-41215777583/artifact-13049423620) |
| S3 | `b-1` | 200 | 59s | 0 | 18 | 52 | — | 0.0 MiB / 1 | 1 | 2.6 MiB | 1.6 → 27.9 MiB | 42.6 MiB | 2.9 | — | [`../../harness/otto-10992/raw/actions-41215777583/artifact-13049526960`](../../harness/otto-10992/raw/actions-41215777583/artifact-13049526960) |
| S3 | `b-5` | 200 | 299s | 0 | 26 | 78 | 2.26s | 0.0 MiB / 1 | 1 | 0.9 MiB | 1.9 → 142.1 MiB | 62.3 MiB | 3.3 | — | [`../../harness/otto-10992/raw/actions-41215777583/artifact-13049681893`](../../harness/otto-10992/raw/actions-41215777583/artifact-13049681893) |
| S3 | `b-15` | 200 | 899s | 0 | 22 | 67 | 2.44s | 0.0 MiB / 1 | 1 | 1.2 MiB | 3.2 → 449.6 MiB | 75.6 MiB | 4.0 | — | [`../../harness/otto-10992/raw/actions-41215777583/artifact-13050101694`](../../harness/otto-10992/raw/actions-41215777583/artifact-13050101694) |
| S3 | `c-200` | 200 | 299s | 7 | 508 | 1519 | 0.32s | 0.0 MiB / 1 | 1 | 0.6 MiB | 1.9 → 25.8 MiB | 646.9 MiB | 4.0 | — | [`../../harness/otto-10992/raw/actions-41215777583/artifact-13050085554`](../../harness/otto-10992/raw/actions-41215777583/artifact-13050085554) |
| S3 | `c-2000` | 200 | 299s | 1 | 67 | 200 | 1.27s | 0.0 MiB / 1 | 1 | 0.6 MiB | 2.2 → 14.2 MiB | 664.7 MiB | 3.7 | — | [`../../harness/otto-10992/raw/actions-41215777583/artifact-13050085874`](../../harness/otto-10992/raw/actions-41215777583/artifact-13050085874) |
| S3 | `d` | 200 | 299s | 799 | 59664 | 0 | — | 0.0 MiB / 1 | 1 | 1.8 MiB | 1.9 → 15.2 MiB | 95.5 MiB | 2.6 | — | [`../../harness/otto-10992/raw/actions-41215777583/artifact-13049787440`](../../harness/otto-10992/raw/actions-41215777583/artifact-13049787440) |
| S3 | `e` | 200 | 299s | 95 | 7177 | — | — | 0.0 MiB / 49 | 49 | 0.2 MiB | 1.5 → 168.5 MiB | 485.7 MiB | 5.5 | — | [`../../harness/otto-10992/raw/actions-41215777583/artifact-13049822045`](../../harness/otto-10992/raw/actions-41215777583/artifact-13049822045) |


### Variant findings

- **#53 (`56f3dd1`, run `41215755747`)** bounds the in-memory group lane in the 200 txn/s stall to 19.7 MiB and the 1,000 txn/s stall to 25.9 MiB, with receipt depth 35,380 and 46,541. Unaffected streams continue at about 800 and 1,042 env/s. The 2 s delay case still reaches 81.3 MiB and 146,022 receipts, so the 64 MiB/60 s cap is not a robust bound under a slow but healthy append. Outage rows retain WAL like the baseline (139, 420 MiB at 5/15 min).
- **S1 (`cc4d78d`, run `41215799086`)** spills group-lane work to disk (`group_spill_bytes` reaches about 10 MiB in the raw samples), but the shared sequencer still throttles the stalled case: `a` delivers 21,679 unaffected envelopes, close to `main`'s 20. Its receipt queue reaches 67,627 at 200 txn/s and 66,692 at 1,000 txn/s, and the lane reaches 39.5/38.9 MiB. This is a useful bounded-memory scheduler prototype, but it needs a separately bounded receipt queue and a scheduling change that lets unaffected groups progress.
- **S2 (`a72a5fe`, run `41216147178`) gives each group its own replay/checkpoint barrier.** In `a`, unaffected delivery is 60,228 envelopes and the held lane peaks at 3.4 MiB/6,190 receipts; in `c-200`, unaffected delivery is 60,173 and the delayed lane is 8.2 MiB/14,793 receipts. The 15-minute outage still retains 441.4 MiB of WAL, so per-group replay does not remove the outage retention cost.
- **S3 (`04e04ca`, run `41215777583`) retires a group under receipt pressure.** In `a`, unaffected delivery stays at 59,827 envelopes, the faulted group stops at 55, and the lane/receipt gauges remain at 842 bytes/1. The after marker arrives in 80 ms, but the group client must reload; this is an explicit failure rule and must be surfaced as a terminal group retirement.

### Recommendation

Choose **S2 as the design to carry into a fresh PR**, with S1's disk-spill scheduler as an implementation option for the per-group replay queue. S2 is the only prototype that both preserves unaffected delivery and keeps the stalled group's queue observable and bounded without silently dropping receipts. Its WAL cost is the same PostgreSQL slot cost already measured for a Durable Streams outage, and the 15-minute case is 441.4 MiB on this runner. Add an explicit group-reload contract and a progress-gap rule before implementation.

S3 is a viable alternative when the product accepts group retirement and client reloads; it does not fit #53's no-contract-change goal. #53 itself does not give a reliable cap under slow healthy appends, and S1 alone leaves the receipt queue unbounded. The recommendation therefore belongs in a **fresh PR** after Michael answers the contract question; do not implement it in #53.

Instrumentation commit: `135c183` (`harness/otto-10992/measurement.patch`). Variant #53 required the identical observation patch with its source offsets adjusted in `harness/otto-10992/measurement-56f3dd1.patch`.
