# OTTO-10992 group backlog prototype

Baseline evidence is from `main` (`ac68597`) with observation-only instrumentation from `135c183`, applied by `harness/otto-10992/measurement.patch`. Runs use GitHub Actions `ubuntu-latest` (4 vCPU, about 16 GiB). Raw time series are under [`harness/otto-10992/raw`](../../harness/otto-10992/raw); the authoritative Actions artifacts are runs `41205833395` (a-200), `41208546679` (control-200), and `41211316081` (b-1/b-5/b-15).

The delivery sampler records `envelopes_processed` and source replay positions. The real client marker in the healthy control was delivered in 1.941 s. The direct unaffected-stream probe is retained in the raw files, but its stream tail did not advance reliably on the current DS HEAD/GET probe; source replay and processed-envelope counters are the authoritative delivery measures for these rows.

| Scenario | Rate | Hold | Group bytes | Processed start → end (envelopes/s) | Peak lane bytes / batches | Receipt peak | Processed/checkpoint lag peak | WAL retained start → peak | RSS peak | Host load1 | Marker latency |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| control-200 | 200/s | 60s sampled; writer 300s | 46.6 MiB | 10,696 → 20,855 (172/s) | 835 / 1 | 1 | 2.4 MiB | ~4 → 99 MiB | 42 MiB | 4.1 | 1.941s |
| a-200 | 200/s | 300s | 45.9 MiB | 10,003 → 10,003 (0/s) | 832 / 1 | 1 | 24.1 MiB | ~4 → 99 MiB | 39 MiB | 2.7 | client setup failed |
| b-1 | outage 1m | 180s | n/a | 10,002 → 10,002 (0/s) | 0 / 0 | 0 | 24.1 MiB | see raw artifact | 39 MiB | — | client setup failed |
| b-5 | outage 5m | 420s | n/a | 18,830 → 18,830 (0/s) | 0 / 0 | 0 | 43.0 MiB | see raw artifact | 40 MiB | — | client setup failed |
| b-15 | outage 15m | 1,020s | n/a | 30,744 → 30,744 (0/s) | 0 / 0 | 0 | 145.9 MiB | see raw artifact | 44 MiB | — | client setup failed |

All three outage rows resumed processing after the proxy was restored. Recovery samples advanced the source position and processed envelopes at approximately 14.5/s (`b-1`), 33.6/s (`b-5`) and 85.4/s (`b-15`) while replaying the retained backlog. The marker reader failed to create its feed during outage setup, so those rows have no client latency.

The a-200 proxy held 10 appends on the exact group stream path and the group stream offset stayed `0 → 0` while 45.9 MiB accumulated in the source tables. This proves that `main` stalls the shared sequencer behind one held group; the low in-engine backlog is a consequence of stopped consumption, not a bounded group queue.

## Corrected baseline matrix

The corrected matrix is Actions run `41215671062`; each row ran on its own `ubuntu-latest` 4-vCPU runner. Delivery counts are cumulative envelopes received by direct Durable Streams readers during the hold. `resume` is the first growth after the fault was cleared.

| Scenario | Rate | Hold | Envelopes/s warmup → hold | Unaffected plain / group envelopes | Faulted group envelopes | Resume | Peak RSS | Lane / receipt peak | Change-log lag peak | Retained WAL start → peak |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| control | 200/s | 299s | 792 → 793 | 59,353 / 59,353 | — | — | 39 MiB | 842 B / 1; 1 | 0.1 MiB | 1.2 → 25.5 MiB |
| control-900 | 200/s | 899s | 789 → 799 | 179,527 / 179,527 | — | — | 39 MiB | 844 B / 1; 1 | 0.1 MiB | 1.2 → 26.2 MiB |
| control:1000 | 1,000/s | 299s | 1,426 → 1,480 | 110,641 / 110,641 | — | — | 42 MiB | 844 B / 1; 1 | 0.1 MiB | 66.0 → 714.3 MiB |
| a | 200/s | 299s | 792 → 0 | 20 / 20 | 57 | 2.2s | 673 MiB | 842 B / 1; 1 | 173.0 MiB | 2.2 → 11.6 MiB |
| a:1000 | 1,000/s | 299s | 1,077 → 0 | 28 / 28 | 81 | 2.2s | 1,843 MiB | 842 B / 1; 1 | 433.2 MiB | 73.6 → 568.3 MiB |
| b-1 | outage 1m | 59s | 789 → 0 | 13 / 13 | 39 | 772ms | 67 MiB | 842 B / 1; 1 | 5.5 MiB | 3.0 → 30.3 MiB |
| b-5 | outage 5m | 299s | 797 → 0 | 21 / 20 | 61 | 274ms | 61 MiB | 842 B / 1; 1 | 4.8 MiB | 1.2 → 141.4 MiB |
| b-15 | outage 15m | 899s | 798 → 0 | 13 / 12 | 32 | 2.5s | 80 MiB | 842 B / 1; 1 | 9.4 MiB | 1.8 → 447.7 MiB |
| c-200 | 200/s | 299s | 818 → 7 | 519 / 519 | 1,555 | 263ms | 640 MiB | 842 B / 1; 1 | 171.8 MiB | 8.7 → 13.1 MiB |
| c-2000 | 200/s | 299s | 799 → 1 | 50 / 49 | 149 | 1.3s | 669 MiB | 842 B / 1; 1 | 172.3 MiB | 1.8 → 15.6 MiB |
| d | 200/s | 299s | 794 → 0 | 178 / 179 | 0 | 2.3s | 659 MiB | 842 B / 1; 1 | 147.1 MiB | 2.1 → 14.3 MiB |
| e | 200/s, 200 groups | 299s | 793 → 113 | 8,524 / 8,523 | — | — | 525 MiB | 44.6 KiB / 53; 53 | 8.5 MiB | 1.2 → 181.9 MiB |

The `a` and `a:1000` rows prove the held group path blocks the shared sequencer. Slow append is degraded rather than completely stopped. Sparse input leaves unaffected delivery moving while the sparse group itself receives no envelopes. The burst row is the only baseline row that raises the per-lane and receipt gauges materially.

Variant runs are now queued on the same workflow for `56f3dd1`, S1, S2 and S3. Their raw artifacts will be added under this branch before the final recommendation. The workflow is intentionally retained until the final-results commit, where it will be deleted as requested.
