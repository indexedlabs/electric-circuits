# OTTO-10992: stalled-group backlog on the real stack

**Recommendation: S2, per-group barriers with detach and reattach, in a fresh PR rather than a reworked #53.** It is the only variant that does all four of these:
- keeps unaffected streams at control rates during a stall;
- keeps a merely slow group alive;
- holds memory under about 75 MiB;
- never holds the shared receipts or replay position on a group.

It changes the contract, and the reasons are below. If the contract can't change, S1 is the fallback, at the cost of an unbounded spill and a pinned replay position.

Every number on this page comes from the runs cited in its table headings, made with the corrected harness described below. An earlier writer's tables and recommendation on this branch (commits up to `3e0753b`) mixed in numbers from before the harness fixes, and nothing from them is used here.

## How it is measured

Every row is one scenario on a fresh GitHub Actions `ubuntu-latest` runner (4 vCPU, 16 GiB). The runner id and CPU are on each row. Each runner builds the engine at the variant's commit, with the observation-only measurement patch applied identically:
- main uses `harness/otto-10992/measurement.patch`;
- #53 uses `measurement-56f3dd1.patch`, the same hooks re-applied to the files #53 rewrote;
- each spike branch carries `measurement.patch` as its first commit on top of `ac68597`.

It then runs PostgreSQL 18.6, Durable Streams 0.1.5 (file-backed) and the engine, with a fault proxy on the engine's DS connection. Nothing is mocked. The workflow that ran it (`.github/workflows/otto-10992-prototype.yml`) is removed in the final commit; it is in the history at `4f3013b`.

- **Phases.** 30 s warmup, then the hold (the fault, or the burst), then 120 s of recovery. `pgbench` writes 200 transactions/s the whole time, except in the `:1000` rows. Each transaction is 4 rows: `record`, `plain_events`, `record_items`, `record_meta`.
- **Streams.**
  - The **faulted group** is a Record-scope group: `record` where `group_id = 1`, plus `record_items` and `record_meta`. Every transaction writes to it.
  - The **unaffected plain shape** is `plain_events`.
  - The **unaffected group** is `plain_events` + `sparse_events`.
  - A client-style tailer reads the unaffected streams and the faulted group's stream directly from DS (not through the proxy) and counts the envelopes delivered.
- **Markers.** A marker row is committed to `plain_events` 1 s into the hold ("during") and at the clear ("after"). Its latency runs from the Postgres commit to the marker's appearance in each unaffected stream.
- **Faults.** The proxy switches faults in place.
  - `a`: every request to the faulted group's stream is held open.
  - `b-N`: every engine request to DS is held for N minutes, the change-log append included.
  - `c-MS`: MS ms of delay on each request to the group's stream.
  - `d`: like `a`, on a group whose only source, `sparse_events`, gets one write per 10 s.
  - `e`: 200 groups are registered back to back with no fault. Each is `plain_events` + `record` with no filter, so every commit fans out to every registered group.
- **Code.** Scenarios: `prototype/experiment.sh`. Per-row numbers: `prototype/summarize.py`. Tables: `prototype/table.py`.

**Columns**
- **env/s:** the engine's `envelopes_processed` rate.
- **Resumes after clear:** the first envelope after the clear on the unaffected plain stream and on the faulted group's stream. "Not seen" means it never came within 120 s.
- **Caught up:** the after-marker's latency when it was seen. Otherwise it is estimated as (writer rate × 4 − hold rate) × hold ÷ (recovery rate − writer rate × 4). "Never" means recovery processed slower than the writers produce.
- **Receipt queue:** the depth of `PendingFlips`, the batches whose receipts are outstanding.
- **Processed−checkpoint lag:** processed offset minus durable checkpoint.
- **Change-log lag:** the `changes/0` head minus the *published* processed offset. In S1 that offset is pinned to the oldest unlanded group commit, so this column is how far its replay position trails.
- **Last column:** the spike counters. Groups retired (S3), detached and reattached (S2), and peak bytes spilled to disk (S1).

## Harness corrections (why earlier numbers on this branch are not used)

1. **The fault proxy broke the no-fault control.** It opened a TCP connection to DS per request and ran out of ephemeral ports about 40 s into control-200 (run 41209890967). It now reuses one upstream connection per downstream connection.
2. **Toggling a fault restarted the proxy,** which dropped every engine connection, the `changes/0` append included. Faults now switch in place, and held requests are dropped unanswered when a fault clears.
3. **The unaffected-delivery counters couldn't count.** The HEAD probe always reported 0, and DS 0.1.5 answers `live=false` with `400 invalid live mode`. The tailer now uses catch-up reads.
4. **Retractions.**
   - The claims that one stalled group freezes `changes/0` and the slot's `confirmed_flush` came from items 1 and 2, and are retracted. With a clean proxy, ingest keeps going during a group stall (`confirmed_flush` advances); only the sequencer stops.
   - The `client.ts` "changes" counts are dropped: that client subscribed to the first 100 ids, so it never saw new rows.

## Results

Rate is 200 txn/s unless the scenario says `:1000`. At that rate the control delivers about 198 envelopes/s to each unaffected stream, about 59,500 in a 300 s hold.

### `main` `ac68597`: run [41215671062](https://github.com/indexedlabs/electric-circuits/actions/runs/41215671062)

| scenario | runner | rate (txn/s) | hold | env/s warmup → hold | unaffected plain env in hold | unaffected group env in hold | marker during (plain / group) | marker after (plain / group) | resumes after clear (plain / faulted group) | caught up after clear | RSS peak MiB (hold / recovery) | lane backlog peak (batches / MiB) | receipt queue peak | processed−checkpoint lag peak MiB | change-log lag at clear MiB | retained WAL MiB (hold start → peak) | host load1 peak | group retired / detached / reattached; spill peak MiB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `control` | #1000065354 · EPYC 9V74 80-Core · 4 vCPU | 200 | 299 s | 792 → 793 | 59,353 | 59,353 | 206 ms / 207 ms | 55 ms / 56 ms | 216 ms / – | – | 39.0 / 38.7 | 1 / 0.0 | 1 | 0.9 | 0.0 | 1.2 → 25.5 | 3.6 | – |
| `control-900` | #1000065357 · EPYC 9V74 80-Core · 4 vCPU | 200 | 899 s | 789 → 799 | 179,527 | 179,527 | 255 ms / 256 ms | 331 ms / 334 ms | 496 ms / – | – | 38.9 / 39.0 | 1 / 0.0 | 1 | 1.1 | 0.0 | 1.2 → 26.2 | 3.8 | – |
| `a` | #1000065350 · EPYC 7763 · 4 vCPU | 200 | 299 s | 792 → 0 | 20 | 20 | 303.7 s / 303.8 s | not seen / not seen | 2.2 s / 2.2 s | ≈958 s (est.) | 37.9 / 673.2 | 1 / 0.0 | 1 | 0.6 | 124.6 | 2.2 → 11.6 | 3.7 | – |
| `b-1` | #1000065362 · EPYC 9V74 80-Core · 4 vCPU | 200 | 59 s | 789 → 0 | 13 | 13 | 60.3 s / 60.3 s | 39.6 s / 39.6 s | 772 ms / 772 ms | 40 s | 39.1 / 67.4 | 1 / 0.0 | 1 | 1.3 | 0.0 | 3.0 → 30.3 | 3.5 | – |
| `b-5` | #1000065359 · EPYC 9V74 80-Core · 4 vCPU | 200 | 299 s | 797 → 0 | 21 | 20 | 300.3 s / 300.3 s | not seen / not seen | 274 ms / 274 ms | ≈1,552 s (est.) | 40.9 / 61.0 | 1 / 0.0 | 1 | 1.0 | 0.0 | 1.2 → 141.4 | 3.4 | – |
| `b-15` | #1000065356 · EPYC 9V74 80-Core · 4 vCPU | 200 | 899 s | 798 → 0 | 13 | 12 | 901.5 s / 901.5 s | not seen / not seen | 2.5 s / 2.5 s | ≈1,566 s (est.) | 41.5 / 79.7 | 1 / 0.0 | 1 | 1.2 | 1.0 | 1.8 → 447.7 | 3.0 | – |
| `c-200` | #1000065351 · EPYC 7763 · 4 vCPU | 200 | 299 s | 818 → 7 | 519 | 519 | 155.2 s / 155.2 s | not seen / not seen | 263 ms / 263 ms | ≈887 s (est.) | 639.8 / 639.6 | 1 / 0.0 | 1 | 0.0 | 122.0 | 8.7 → 13.1 | 3.4 | – |
| `c-2000` | #1000065352 · EPYC 9V74 80-Core · 4 vCPU | 200 | 299 s | 799 → 1 | 50 | 49 | 299.9 s / 299.9 s | not seen / not seen | 1.3 s / 1.3 s | ≈246 s (est.) | 43.8 / 668.7 | 1 / 0.0 | 1 | 0.0 | 123.6 | 1.8 → 15.6 | 3.5 | – |
| `d` | #1000065358 · EPYC 9V74 80-Core · 4 vCPU | 200 | 299 s | 794 → 0 | 178 | 179 | 302.3 s / 302.3 s | 58.7 s / 58.7 s | 4.3 s / 2.3 s | 59 s | 38.3 / 659.3 | 1 / 0.0 | 1 | 4.3 | 124.0 | 2.1 → 14.3 | 2.8 | – |
| `e` (55 of 200 registered) | #1000065355 · EPYC 9V74 80-Core · 4 vCPU | 200 | 299 s | 793 → 113 | 8,524 | 8,523 | 2.2 s / 2.2 s | not seen / not seen | 262 ms / – | never (not draining) | 439.4 / 524.7 | 53 / 0.0 | 53 | 0.1 | 4.6 | 1.2 → 181.9 | 4.8 | – |
| `control:1000` | #1000065353 · EPYC 9V74 80-Core · 4 vCPU | 1,000 | 299 s | 1,426 → 1,480 | 110,641 | 110,641 | 53.8 s / 53.8 s | not seen / not seen | 331 ms / – | never (not draining) | 41.8 / 41.8 | 1 / 0.0 | 1 | 1.6 | 0.0 | 66.0 → 714.3 | 4.2 | – |
| `a:1000` | #1000065361 · EPYC 9V74 80-Core · 4 vCPU | 1,000 | 299 s | 1,077 → 0 | 28 | 28 | 384.8 s / 384.8 s | not seen / not seen | 2.2 s / 2.2 s | never (not draining) | 48.8 / 1,843.5 | 1 / 0.0 | 1 | 1.0 | 349.9 | 73.6 → 568.3 | 5.5 | – |

Per-row summaries: [`harness/otto-10992/raw/main-41215671062/`](../../harness/otto-10992/raw/main-41215671062/).


### #53 `56f3dd1`: run [41215755747](https://github.com/indexedlabs/electric-circuits/actions/runs/41215755747)

| scenario | runner | rate (txn/s) | hold | env/s warmup → hold | unaffected plain env in hold | unaffected group env in hold | marker during (plain / group) | marker after (plain / group) | resumes after clear (plain / faulted group) | caught up after clear | RSS peak MiB (hold / recovery) | lane backlog peak (batches / MiB) | receipt queue peak | processed−checkpoint lag peak MiB | change-log lag at clear MiB | retained WAL MiB (hold start → peak) | host load1 peak | group retired / detached / reattached; spill peak MiB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `control` | #1000065373 · EPYC 9V45 96-Core · 4 vCPU | 200 | 299 s | 780 → 804 | 60,094 | 60,094 | 234 ms / 235 ms | 184 ms / 186 ms | 311 ms / – | – | 39.5 / 39.3 | 4 / 0.0 | 4 | 0.9 | 0.0 | 0.9 → 15.5 | 2.7 | – |
| `a` | #1000065363 · EPYC 9V74 80-Core · 4 vCPU | 200 | 299 s | 794 → 798 | 59,645 | 59,645 | 261 ms / 262 ms | 158 ms / 160 ms | 326 ms / not seen | 0 s | 184.1 / 42.8 | 35,380 / 19.7 | 35,380 | 0.9 | 0.0 | 1.4 → 15.5 | 3.7 | – |
| `b-1` | #1000065365 · EPYC 7763 · 4 vCPU | 200 | 59 s | 806 → 0 | 30 | 27 | 60.7 s / 60.7 s | 60.1 s / 60.1 s | 712 ms / 712 ms | 60 s | 41.5 / 41.1 | 35 / 0.0 | 35 | 0.8 | 0.0 | 2.2 → 32.2 | 3.5 | – |
| `b-5` | #1000065376 · EPYC 9V45 96-Core · 4 vCPU | 200 | 299 s | 769 → 0 | 23 | 23 | 299.4 s / 299.4 s | not seen / not seen | 359 ms / 2.4 s | ≈239 s (est.) | 40.9 / 49.1 | 2,057 / 1.1 | 2,057 | 1.9 | 0.0 | 1.0 → 138.9 | 3.0 | – |
| `b-15` | #1000065374 · EPYC 9V45 96-Core · 4 vCPU | 200 | 899 s | 800 → 0 | 52 | 52 | 900.0 s / 900.0 s | not seen / not seen | 996 ms / 996 ms | ≈800 s (est.) | 41.7 / 42.9 | 141 / 0.1 | 141 | 1.8 | 0.0 | 1.0 → 419.8 | 2.7 | – |
| `c-200` | #1000065368 · INTEL XEON PLATINUM · 4 vCPU | 200 | 299 s | 801 → 799 | 59,767 | 59,768 | 200 ms / 201 ms | 131 ms / 133 ms | 289 ms / not seen | 0 s | 644.9 / 53.7 | 145,644 / 81.1 | 145,644 | 0.9 | 0.0 | 1.2 → 25.9 | 4.3 | – |
| `c-2000` | #1000065366 · EPYC 9V74 80-Core · 4 vCPU | 200 | 299 s | 786 → 800 | 59,803 | 59,803 | 217 ms / 218 ms | 144 ms / 145 ms | 287 ms / not seen | 0 s | 643.7 / 52.6 | 146,022 / 81.3 | 146,022 | 1.0 | 0.0 | 1.0 → 15.2 | 4.5 | – |
| `d` | #1000065375 · EPYC 7763 · 4 vCPU | 200 | 299 s | 775 → 798 | 59,637 | 59,667 | 154 ms / 156 ms | 484 ms / 486 ms | 228 ms / not seen | 0 s | 40.1 / 40.1 | 8 / 0.0 | 8 | 0.9 | 0.0 | 1.4 → 15.6 | 4.4 | – |
| `e` (200 of 200 registered) | #1000065364 · EPYC 7763 · 4 vCPU | 200 | 299 s | 806 → 22 | 1,846 | 1,846 | 3.1 s / 3.1 s | not seen / not seen | 270 ms / – | never (not draining) | 639.5 / 98.5 | 4,758 / 3.8 | 4,758 | 0.2 | 0.1 | 1.7 → 189.0 | 8.3 | – |
| `a:1000` | #1000065371 · EPYC 9V74 80-Core · 4 vCPU | 1,000 | 299 s | 732 → 1,042 | 77,902 | 77,902 | 97.7 s / 97.7 s | not seen / not seen | 241 ms / not seen | never (not draining) | 231.9 / 45.6 | 46,541 / 25.9 | 46,541 | 1.1 | 0.0 | 67.8 → 781.6 | 6.6 | – |

Per-row summaries: [`harness/otto-10992/raw/pr53-41215755747/`](../../harness/otto-10992/raw/pr53-41215755747/).


### S1 `faa9ee1` (`proto/otto-10992-s1`): run [41220115072](https://github.com/indexedlabs/electric-circuits/actions/runs/41220115072)

| scenario | runner | rate (txn/s) | hold | env/s warmup → hold | unaffected plain env in hold | unaffected group env in hold | marker during (plain / group) | marker after (plain / group) | resumes after clear (plain / faulted group) | caught up after clear | RSS peak MiB (hold / recovery) | lane backlog peak (batches / MiB) | receipt queue peak | processed−checkpoint lag peak MiB | change-log lag at clear MiB | retained WAL MiB (hold start → peak) | host load1 peak | group retired / detached / reattached; spill peak MiB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `control` | #1000065484 · Xeon 6973P-C · 4 vCPU | 200 | 299 s | 799 → 797 | 59,581 | 59,582 | 298 ms / 300 ms | 236 ms / 238 ms | 373 ms / – | – | 38.7 / 38.5 | 5 / 0.0 | 5 | 0.9 | 0.0 | 0.8 → 15.2 | 2.9 | 0 / 0 / 0; 0.0 |
| `a` | #1000065486 · INTEL XEON PLATINUM · 4 vCPU | 200 | 299 s | 796 → 797 | 59,632 | 59,633 | 206 ms / 208 ms | 66 ms / 68 ms | 289 ms / 2.3 s | 0 s | 189.7 / 191.9 | 179,961 / 100.2 | 179,961 | 1.4 | 122.7 | 1.1 → 11.9 | 4.2 | 0 / 0 / 0; 72.5 |
| `b-1` | #1000065474 · EPYC 7763 · 4 vCPU | 200 | 59 s | 796 → 0 | 37 | 36 | 60.2 s / 60.2 s | 58.0 s / 58.0 s | 691 ms / 691 ms | 58 s | 41.2 / 41.5 | 35 / 0.0 | 35 | 0.8 | 0.0 | 1.1 → 31.4 | 3.4 | 0 / 0 / 0; 0.0 |
| `b-5` | #1000065485 · EPYC 7763 · 4 vCPU | 200 | 299 s | 779 → 0 | 23 | 22 | 300.3 s / 300.3 s | not seen / not seen | 772 ms / 2.3 s | ≈7,610 s (est.) | 41.6 / 42.5 | 861 / 0.5 | 861 | 0.8 | 0.0 | 1.2 → 140.9 | 3.9 | 0 / 0 / 0; 0.0 |
| `b-15` | #1000065482 · EPYC 7763 · 4 vCPU | 200 | 899 s | 791 → 0 | 22 | 22 | 900.4 s / 900.4 s | not seen / not seen | 420 ms / 2.4 s | ≈32,657 s (est.) | 41.3 / 42.6 | 950 / 0.5 | 950 | 0.8 | 0.0 | 1.1 → 443.4 | 4.0 | 0 / 0 / 0; 0.0 |
| `c-200` | #1000065476 · EPYC 9V45 96-Core · 4 vCPU | 200 | 299 s | 788 → 797 | 59,555 | 59,554 | 294 ms / 294 ms | 206 ms / 207 ms | 375 ms / 375 ms | 0 s | 189.2 / 189.3 | 177,209 / 98.7 | 177,209 | 3.6 | 122.0 | 1.2 → 14.9 | 3.2 | 0 / 0 / 0; 71.3 |
| `c-2000` | #1000065477 · EPYC 7763 · 4 vCPU | 200 | 299 s | 789 → 802 | 59,992 | 59,992 | 95 ms / 97 ms | 460 ms / 463 ms | 176 ms / 1.2 s | 0 s | 190.7 / 194.2 | 180,417 / 100.5 | 180,417 | 1.0 | 123.6 | 1.3 → 14.7 | 3.7 | 0 / 0 / 0; 72.7 |
| `d` | #1000065480 · EPYC 7763 · 4 vCPU | 200 | 299 s | 799 → 796 | 59,509 | 59,538 | 131 ms / 134 ms | 439 ms / 441 ms | 217 ms / 2.2 s | 0 s | 79.2 / 79.5 | 30 / 0.0 | 30 | 107.4 | 122.4 | 1.5 → 15.1 | 3.8 | 0 / 0 / 0; 0.0 |
| `e` (200 of 200 registered) | #1000065473 · INTEL XEON PLATINUM · 4 vCPU | 200 | 299 s | 785 → 26 | 2,031 | 2,031 | 3.2 s / 3.2 s | not seen / not seen | 314 ms / – | never (not draining) | 428.4 / 95.1 | 6,662 / 5.3 | 6,662 | 0.7 | 0.0 | 0.9 → 189.4 | 7.5 | 0 / 0 / 0; 0.0 |
| `a:1000` | #1000065479 · EPYC 9V74 80-Core · 4 vCPU | 1,000 | 299 s | 1,035 → 1,470 | 109,935 | 109,936 | 63.2 s / 63.2 s | not seen / not seen | 275 ms / 2.3 s | never (not draining) | 295.0 / 298.1 | 332,224 / 185.1 | 332,224 | 1.3 | 227.0 | 68.4 → 746.4 | 7.3 | 0 / 0 / 0; 139.3 |

Per-row summaries: [`harness/otto-10992/raw/s1-41220115072/`](../../harness/otto-10992/raw/s1-41220115072/).


### S2 `a72a5fe` (`proto/otto-10992-s2`): run [41216147178](https://github.com/indexedlabs/electric-circuits/actions/runs/41216147178)

| scenario | runner | rate (txn/s) | hold | env/s warmup → hold | unaffected plain env in hold | unaffected group env in hold | marker during (plain / group) | marker after (plain / group) | resumes after clear (plain / faulted group) | caught up after clear | RSS peak MiB (hold / recovery) | lane backlog peak (batches / MiB) | receipt queue peak | processed−checkpoint lag peak MiB | change-log lag at clear MiB | retained WAL MiB (hold start → peak) | host load1 peak | group retired / detached / reattached; spill peak MiB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `control` | #1000065430 · EPYC 7763 · 4 vCPU | 200 | 299 s | 786 → 797 | 59,643 | 59,643 | 171 ms / 172 ms | 520 ms / 524 ms | 248 ms / – | – | 38.4 / 38.2 | 3 / 0.0 | 3 | 0.9 | 0.0 | 1.6 → 6.7 | 3.9 | 0 / 0 / 0; 0.0 |
| `a` | #1000065427 · EPYC 9V74 80-Core · 4 vCPU | 200 | 299 s | 807 → 806 | 60,228 | 60,228 | 181 ms / 182 ms | 530 ms / 531 ms | 262 ms / 1.3 s | 1 s | 51.9 / 39.4 | 6,190 / 3.4 | 6,190 | 0.9 | 0.0 | 1.7 → 14.6 | 3.7 | 0 / 1 / 1; 0.0 |
| `b-1` | #1000065421 · EPYC 9V74 80-Core · 4 vCPU | 200 | 59 s | 790 → 0 | 12 | 11 | 60.2 s / 60.3 s | 40.6 s / 40.6 s | 732 ms / 1.2 s | 41 s | 41.0 / 41.7 | 150 / 0.1 | 150 | 1.2 | 0.0 | 1.3 → 30.0 | 3.2 | 0 / 1 / 1; 0.0 |
| `b-5` | #1000065422 · EPYC 9V74 80-Core · 4 vCPU | 200 | 299 s | 785 → 0 | 36 | 34 | 300.8 s / 300.8 s | not seen / not seen | 2.3 s / 1.3 s | ≈2,545 s (est.) | 41.1 / 45.1 | 906 / 0.5 | 906 | 1.0 | 0.3 | 2.0 → 144.1 | 4.0 | 0 / 2 / 2; 0.0 |
| `b-15` | #1000065439 · EPYC 9V45 96-Core · 4 vCPU | 200 | 899 s | 783 → 0 | 18 | 17 | 900.0 s / 900.0 s | not seen / not seen | 492 ms / 492 ms | ≈714 s (est.) | 41.2 / 42.3 | 65 / 0.0 | 65 | 2.0 | 0.0 | 1.1 → 441.3 | 2.9 | 0 / 1 / 1; 0.0 |
| `c-200` | #1000065424 · EPYC 9V45 96-Core · 4 vCPU | 200 | 299 s | 812 → 805 | 60,173 | 60,173 | 309 ms / 310 ms | 242 ms / 243 ms | 371 ms / 371 ms | 0 s | 72.8 / 59.5 | 14,793 / 8.2 | 14,793 | 0.9 | 0.0 | 1.0 → 15.4 | 4.4 | 0 / 11 / 11; 0.0 |
| `c-2000` | #1000065437 · EPYC 7763 · 4 vCPU | 200 | 299 s | 785 → 791 | 59,139 | 59,141 | 119 ms / 121 ms | 914 ms / 916 ms | 209 ms / 704 ms | 1 s | 73.9 / 67.1 | 14,535 / 8.1 | 14,535 | 1.2 | 0.0 | 1.2 → 18.6 | 4.6 | 0 / 9 / 9; 0.0 |
| `d` | #1000065429 · EPYC 9V45 96-Core · 4 vCPU | 200 | 299 s | 798 → 797 | 59,596 | 59,626 | 306 ms / 306 ms | 200 ms / 202 ms | 370 ms / 869 ms | 0 s | 38.8 / 38.6 | 4 / 0.0 | 4 | 0.9 | 0.0 | 0.9 → 6.8 | 2.6 | 0 / 1 / 1; 0.0 |
| `e` (200 of 200 registered) | #1000065435 · EPYC 7763 · 4 vCPU | 200 | 299 s | 795 → 23 | 1,872 | 1,872 | 3.1 s / 3.1 s | not seen / not seen | 193 ms / – | never (not draining) | 521.3 / 99.9 | 5,614 / 4.5 | 5,614 | 0.2 | 0.0 | 1.8 → 185.4 | 7.4 | 0 / 0 / 0; 0.0 |
| `a:1000` | #1000065432 · EPYC 7763 · 4 vCPU | 1,000 | 299 s | 676 → 954 | 71,365 | 71,367 | 109.0 s / 109.0 s | not seen / not seen | 77 ms / 570 ms | never (not draining) | 53.3 / 40.0 | 5,908 / 3.3 | 5,908 | 1.0 | 0.0 | 69.1 → 867.0 | 7.0 | 0 / 1 / 1; 0.0 |

Per-row summaries: [`harness/otto-10992/raw/s2-41216147178/`](../../harness/otto-10992/raw/s2-41216147178/).


S2 is repeatable. The same tree ran twice more (runs [41217225982](https://github.com/indexedlabs/electric-circuits/actions/runs/41217225982) and [41218997194](https://github.com/indexedlabs/electric-circuits/actions/runs/41218997194), started by duplicate pushes):
- unaffected plain envelopes in `a` were 59,656 and 60,196;
- in `c-200`, 59,318 and 59,636;
- hold RSS was 51–74 MiB;
- the faulted group resumed 0.3–1.3 s after the clear.

Summaries are under `harness/otto-10992/raw/s2-repeat-*`.

### S3 `04e04ca` (`proto/otto-10992-s3`): run [41215777583](https://github.com/indexedlabs/electric-circuits/actions/runs/41215777583)

| scenario | runner | rate (txn/s) | hold | env/s warmup → hold | unaffected plain env in hold | unaffected group env in hold | marker during (plain / group) | marker after (plain / group) | resumes after clear (plain / faulted group) | caught up after clear | RSS peak MiB (hold / recovery) | lane backlog peak (batches / MiB) | receipt queue peak | processed−checkpoint lag peak MiB | change-log lag at clear MiB | retained WAL MiB (hold start → peak) | host load1 peak | group retired / detached / reattached; spill peak MiB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `control` | #1000065417 · EPYC 7763 · 4 vCPU | 200 | 299 s | 783 → 800 | 59,818 | 59,818 | 181 ms / 184 ms | 477 ms / 478 ms | 249 ms / – | – | 38.4 / 38.5 | 1 / 0.0 | 1 | 0.9 | 0.0 | 1.5 → 6.8 | 3.4 | 0 / 0 / 0; 0.0 |
| `a` | #1000065416 · Xeon Platinum 8370C · 4 vCPU | 200 | 299 s | 781 → 801 | 59,827 | 59,828 | 29.7 s / 29.7 s | 80 ms / 83 ms | 290 ms / not seen | 0 s | 113.5 / 41.2 | 1 / 0.0 | 1 | 1.5 | 0.0 | 2.1 → 15.1 | 3.5 | 1 / 0 / 0; 0.0 |
| `b-1` | #1000065399 · EPYC 9V45 96-Core · 4 vCPU | 200 | 59 s | 795 → 0 | 18 | 17 | 59.8 s / 59.8 s | 19.7 s / 19.7 s | 792 ms / not seen | 20 s | 39.9 / 42.6 | 1 / 0.0 | 1 | 2.6 | 0.0 | 1.6 → 27.9 | 2.9 | 1 / 0 / 0; 0.0 |
| `b-5` | #1000065415 · EPYC 7763 · 4 vCPU | 200 | 299 s | 794 → 0 | 26 | 26 | 301.3 s / 301.3 s | not seen / not seen | 2.3 s / 2.3 s | ≈2,763 s (est.) | 40.2 / 62.3 | 1 / 0.0 | 1 | 0.9 | 0.0 | 1.9 → 142.1 | 3.3 | 0 / 0 / 0; 0.0 |
| `b-15` | #1000065410 · EPYC 9V74 80-Core · 4 vCPU | 200 | 899 s | 802 → 0 | 22 | 22 | 901.5 s / 901.5 s | not seen / not seen | 2.4 s / 2.4 s | ≈1,544 s (est.) | 41.8 / 75.6 | 1 / 0.0 | 1 | 1.2 | 0.0 | 3.2 → 449.6 | 4.0 | 0 / 0 / 0; 0.0 |
| `c-200` | #1000065403 · EPYC 9V74 80-Core · 4 vCPU | 200 | 299 s | 798 → 7 | 508 | 508 | 149.3 s / 149.3 s | not seen / not seen | 322 ms / 322 ms | ≈256 s (est.) | 110.8 / 646.9 | 1 / 0.0 | 1 | 0.6 | 122.6 | 1.9 → 25.8 | 4.0 | 0 / 0 / 0; 0.0 |
| `c-2000` | #1000065405 · INTEL XEON PLATINUM · 4 vCPU | 200 | 299 s | 790 → 1 | 67 | 67 | 300.3 s / 300.3 s | not seen / not seen | 1.3 s / 1.3 s | ≈676 s (est.) | 90.3 / 664.7 | 1 / 0.0 | 1 | 0.6 | 124.4 | 2.2 → 14.2 | 3.7 | 0 / 0 / 0; 0.0 |
| `d` | #1000065414 · EPYC 9V45 96-Core · 4 vCPU | 200 | 299 s | 790 → 799 | 59,664 | 59,693 | 29.8 s / 29.8 s | 197 ms / 198 ms | 332 ms / not seen | 0 s | 95.5 / 40.6 | 1 / 0.0 | 1 | 1.8 | 0.0 | 1.9 → 15.2 | 2.6 | 1 / 0 / 0; 0.0 |
| `e` (50 of 200 registered) | #1000065409 · EPYC 9V74 80-Core · 4 vCPU | 200 | 299 s | 785 → 95 | 7,177 | 7,177 | 1.7 s / 1.7 s | not seen / not seen | 232 ms / – | never (not draining) | 485.7 / 406.0 | 49 / 0.0 | 49 | 0.2 | 4.2 | 1.5 → 168.5 | 5.5 | 0 / 0 / 0; 0.0 |
| `a:1000` | #1000065406 · EPYC 7763 · 4 vCPU | 1,000 | 299 s | 714 → 994 | 73,913 | 73,913 | 101.1 s / 101.1 s | not seen / not seen | 192 ms / not seen | never (not draining) | 159.9 / 45.1 | 1 / 0.0 | 1 | 1.3 | 0.0 | 75.0 → 792.6 | 7.7 | 1 / 0 / 0; 0.0 |

Per-row summaries: [`harness/otto-10992/raw/s3-41215777583/`](../../harness/otto-10992/raw/s3-41215777583/).


An earlier S1 run (41215799086, at `cc4d78d`) delivered only about 21,700 envelopes in `a`. It spent its time in an O(n) scan of unlanded origins on every commit, which was a bug in the spike. It is superseded by `faa9ee1`, and its numbers are not used.

## What each variant bounds, and what it costs

All figures are from the tables above (200 txn/s, 300 s hold unless stated).

| | Unaffected streams during a stall (`a`; control ≈ 59,500) | Faulted group: held (`a`, `d`) | Faulted group: merely slow (`c-200`, `c-2000`) | Peak memory (hold) | Receipts / replay position | 200-group burst (`e`) |
|---|---|---|---|---|---|---|
| **main** | **Stop.** 20 delivered; during-marker 303.7 s | Resumes 2.2 s after clear; whole engine about 16 min to catch up (est.) | **Throttles everything.** 519 / 50 unaffected delivered (−99%) | 38–44 MiB (work waits in the change log) | Shared barrier; replay position frozen with processing | 55 of 200 groups registered (registration blocks too); 8,524 delivered |
| **#53** | Keep delivering: 59,645 | **Retired** at its 60 s wait cap; never resumes | **Retired** by its 64 MiB byte cap; never resumes | **645 MiB** in `c-200` (146k queued batches, 81 MiB lane) before the cap trips | Receipt queue reaches the backlog (146k); replay not pinned | 200 registered; 1,846 delivered, 22 env/s |
| **S1** | Keep delivering: 59,632 | **Survives with all output.** Lane drains after clear; resumes 2.2–2.3 s | Survives with all output, but its backlog grows without bound while it stays slow (72 MiB spilled per 5 min) | 190 MiB (`a`, `c`), 298 MiB at 1000 txn/s; plus spill on disk (72 MiB, 139 MiB at 1000) | Receipt queue = whole backlog (180k; 332k at 1000). **Replay position pinned ~122 MiB behind** (`d`: checkpoint lag 107 MiB); a restart re-derives that for every stream | 200 registered; 2,031 delivered |
| **S2** | Keep delivering: 60,228 | **Survives.** Detaches after 10 s; reattaches 0.9–1.3 s after clear behind a must-refetch gap marker | **Survives**, but cycles detach/reattach (11 times in `c-200`, 9 in `c-2000`), each costing its readers a snapshot reload | **52–74 MiB** (lane ≤ about 8 MiB) | Never waits on a group: receipt queue ≤ 15k, change-log lag ≤ 0.3 MiB at clear. The group keeps its own resume point | 200 registered; 1,872 delivered |
| **S3** | **Stop for the gap,** then deliver: 59,827; during-marker 29.7 s | **Retired** after 30 s without progress (claim released, stream retired) | **Throttles everything like main** (508 / 67 delivered); a moving group never trips the gap | 90–114 MiB hold; 647–665 MiB catch-up in `c` | Contract unchanged; shared receipts and replay wait for the gap | 50 of 200 registered; 7,177 delivered |

**Notes that apply to every variant**
- **Outages (`b-N`)** hold the change-log append as well, so every variant stops for the outage. Recovery afterwards is the same order everywhere: 40–60 s after a 1-min outage. The catch-up estimates for 5 and 15 min outages are extrapolated from a 120 s window and are noisy: S2 alone ranges from 12 to 42 min. S1 drained slowest (about 825 env/s against 800 written, so ≈2 h and ≈9 h estimated). That is probably its per-commit epoch and spill bookkeeping, which I didn't investigate. S3 also retired the faulted group in `b-1` because it was mid-append when the outage started; in `b-5` and `b-15` it was idle and survived.
- **The burst (`e`) is not a backlog-design result.** On main and S3, the sequencer blocks registration itself, so only 50–55 of the 200 groups existed. That is why main's 8,524 looks better. With all 200 whole-table groups registered (#53, S1, S2), each commit fans out to 200 streams. A 4-vCPU runner sustains about 25 env/s of source progress against that, and none of the designs changes it. It needs its own admission or fan-out bound, separate from the stall question.
- **Catch-up RSS of 640 MiB–1.8 GiB** after a main- or S3-style stall comes from DS 0.1.5 serving uncapped 64 MiB catch-up pages. It depends on the DS version, not the variant.
- **1000 txn/s is past this runner's no-fault capacity** (about 1,450 env/s processed against 4,000/s produced), so the verdicts use 200.

## Recommendation

**S2, in a fresh PR, not a fit for #53.**

- **Why not #53.** It already isolates unaffected streams. But it still treats receipts and the replay position as one shared boundary, and it gets out of a stall only by retiring the group. So it kills a group that is merely slow (`c-200`), and before the cap trips it holds up to 645 MiB and 146k receipts. That is the conflict review rounds 6–10 kept finding, and the numbers confirm it is structural rather than a bug to patch: making receipts and replay per-group is a model change, not a review fix.
- **Why S2.** The barrier moves to the group:
  - unaffected streams are untouched;
  - memory stays bounded at about 75 MiB;
  - the shared checkpoint and receipts never wait on a group;
  - a stalled or slow group survives and recovers about 1 s after its stream answers.
- **The contract change S2 needs** (the fresh PR's spec):
  - per-commit atomicity across *all* streams becomes per-group;
  - receipts become per-group;
  - a detached group keeps a durable resume point (the spike keeps it in memory only);
  - readers handle a must-refetch gap marker by reloading the group from a snapshot.

  Engine-side replay from the change log isn't available for Record-scope groups: `replay_changes_for_targets` evaluates standalone shapes only, which is why reattach means a reload.
- **What the spike leaves for the PR:**
  - hysteresis or backoff on detach, so a merely slow group doesn't cycle 11 times in 5 min;
  - making the resume point durable;
  - per-group source-drain receipts;
  - a separate fan-out admission bound for the burst case.
- **Fallback, if the contract must not change: S1.** It is the only variant that keeps every byte of group output and the existing contract. The price:
  - an unbounded per-group spill on disk;
  - a receipt queue the size of the backlog;
  - a replay position pinned behind the oldest unlanded group commit, about 122 MiB after 5 min at 200 txn/s, all of it re-derived for every stream on restart.
- **S3 is not enough on its own.** It bounds only how long a hard stall lasts (30 s for everyone), retires the group, and does nothing for slow groups.

## Branches and raw data

- Harness and this page: `proto/otto-10992-harness`.
- Spikes, each on `ac68597` plus the measurement commit: `proto/otto-10992-s1` (`faa9ee1`), `proto/otto-10992-s2` (`a72a5fe`), `proto/otto-10992-s3` (`04e04ca`).
- Per-row summaries: `harness/otto-10992/raw/<variant>-<run>/`.
- Full time series (`samples.jsonl`, `delivery.jsonl`, engine logs): each run's Actions artifacts, kept 30 days.
- Other directories under `harness/otto-10992/raw/` (`actions-*`, `artifact-*`) were added by the earlier writer. They aren't used here.
