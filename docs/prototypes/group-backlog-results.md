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

Remaining baseline rows (`d` sparse, `c` delayed append and `e` burst) and all variant runs are pending. Each variant will use the same measurement patch and Actions runner class. The workflow is intentionally retained until the final results commit, where it will be deleted as requested.
