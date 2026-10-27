# Shape groups performance rerun (PostgreSQL 18)

## Verdict

The rerun used a real PostgreSQL 18.6 server and five repetitions for the one-phone matrix and both 30-phone cold-launch CPU settings. The earlier 250 ms chat-reopen result (groups 1069 ms versus scope load 637 ms) was noise in the first-arrival metric: with five reps, groups were 644 ms `[638–1075]` and scope load 1055 ms `[646–1059]`; the complete-screen medians were 1067 ms and 1059 ms. Groups were consistently cheaper to hold, but their 30-phone cold launch was still slower on the current gateway CPU.

Streaming is the only measured option that gets below one charged upstream read per Record change: one open stream per scope (30 reads for 600 changes at the 500 ms workload, 30 for 90 changes at the 5 s workload), with p50/p90 write latency 149/172 ms and 190/197 ms respectively. Coalescing at 25, 50, or 100 ms did not reduce reads below the plain group in this workload; it added latency, especially at 100 ms. Recommendation: pursue a real Durable Streams live/chunked primitive and periodic authority rechecks; keep coalescing as an optional latency tradeoff rather than the primary read reduction.

## Environment and method

- PostgreSQL: `PostgreSQL 18.6 (Ubuntu 18.6-1.pgdg24.04+2) on x86_64-pc-linux-gnu` (the exact `select version()` value is recorded in every JSON artifact).
- Rust: pinned 1.96.0 release engine with `test-support`.
- Host model: 2 vCPU, 7 GB; RTT 100 or 250 ms; 20 Mbps modeled downlink.
- One-phone matrix: five reps over cold launch, chat open, chat reopen, agenda fling, Stack visit, and Record open. Values below are median first content; brackets are the min–max across reps.
- Cold launch: five reps, 30 distinct phones, current gateway burn (`8/12/4 ms`) and half burn (`4/6/2 ms`).
- Steady: 30 distinct phones, Record open, 10 s hold, 20 s long-poll timeout. The 500 ms run has five reps with the existing assistant/message traffic; the 5 s run has three reps with that traffic disabled so Record writes are isolated. Empty timeouts are counted separately; all reported runs had zero empty timeouts because each stream received a write before its 20 s deadline.
- The benchmark now records delivered reads, empty reads, phone requests, gateway CPU and periodic streaming authority checks. Streaming is charged as one open upstream stream per scope plus rechecks; the installed Durable Streams test server has long-poll but no native live mode, so its internal wake polls are retained only as a diagnostic counter.

## One-phone matrix

| RTT | Scenario | Scope load first (ms) | Scope group first (ms) | Scope load reads | Group reads |
| ---: | --- | ---: | ---: | ---: | ---: |
| 100 | cold launch | 919 `[914–926]` | 922 `[918–2012]` | 26 | 13 |
| 100 | chat open | 929 `[926–942]` | 512 `[509–513]` | 26 | 23 |
| 100 | chat reopen | 904 `[491–935]` | 919 `[485–924]` | 26 | 23 |
| 100 | agenda fling | 1119 `[1115–1119]` | 1129 `[1118–1133]` | 8 | 1 |
| 100 | Stack visit | 684 `[678–691]` | 691 `[686–696]` | 8 | 1 |
| 100 | Record open | 233 `[230–235]` | 233 `[231–236]` | 8 | 1 |
| 250 | cold launch | 1074 `[1063–1102]` | 1082 `[1073–1091]` | 24 | 11 |
| 250 | chat open | 1084 `[1081–1087]` | 672 `[663–1100]` | 20 | 18 |
| 250 | chat reopen | 1055 `[646–1059]` | 644 `[638–1075]` | 21 | 19 |
| 250 | agenda fling | 1267 `[1264–1277]` | 1271 `[1269–1273]` | 8 | 1 |
| 250 | Stack visit | 832 `[827–844]` | 841 `[833–847]` | 8 | 1 |
| 250 | Record open | 381 `[379–385]` | 386 `[384–388]` | 8 | 1 |

The 100 ms cold-launch group range contains one 2012 ms outlier; the other four reps were 918–925 ms. No comparable outlier appeared at 250 ms.

## Thirty-phone cold launch

| Gateway CPU | Design | First content median (ms) | Spread (ms) | First-content p90 median (ms) | Gateway load CPU (ms) | Hold reads |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| current | scope load | 5235 | 5199–5269 | 9624 | 11928 | 69 |
| current | scope group | 5627 | 5547–5698 | 10105 | 10076 | 1 |
| half | scope load | 2820 | 2799–2964 | 5060 | 5958 | 81 |
| half | scope group | 3021 | 2948–3152 | 5399 | 5038 | 1 |

Groups reduced the gateway load CPU by 16% on current burn and 15% on half burn, but first content was 392 ms and 201 ms slower respectively. The current-CPU “groups slightly slower” result is therefore real, not two-rep noise.

## Steady writes: 500 ms per Record

Five reps, 10 s hold, 600 Record transactions across 30 phones. “Reads” is the charged upstream read count; `delivered/empty` is reported separately.

| Design | Reads (median, spread) | Delivered / empty | Gateway hold CPU (ms) | Phone requests | Write p50/p90 (ms) |
| --- | ---: | ---: | ---: | ---: | ---: |
| scope load | 1895 `[1847–1937]` | 1776 / 0 | 7580 | 1896 | 220 / 256 |
| scope group | 626 `[604–630]` | 600 / 0 | 2504 | 627 | 169 / 218 |
| group + coalesce 25 ms | 604 `[604–628]` | 600 / 0 | 2416 | 605 | 193 / 214 |
| group + coalesce 50 ms | 629 `[624–630]` | 600 / 0 | 2516 | 630 | 244 / 261 |
| group + coalesce 100 ms | 628 `[603–630]` | 600 / 0 | 2512 | 629 | 291 / 315 |
| group + streaming | **30** `[30]` | 600 / 0 | 0 charged recheck CPU | 26 charged hold requests | 149 / 172 |

The streaming row charges one open response per scope. The diagnostic provider-poll counter was high because this test server emulates the open response with repeated long-polls; those internal polls are not the proposed protocol's upstream read charge.

## Steady writes: one transaction every 5 s

Three reps, 12 s hold, 90 Record transactions across 30 phones, with assistant/message traffic disabled. This isolates the lower write rate and makes the timeout counters meaningful.

| Design | Reads (median, spread) | Delivered / empty | Gateway hold CPU (ms) | Write p50/p90 (ms) |
| --- | ---: | ---: | ---: | ---: |
| scope group | 90 `[90–90]` | 90 / 0 | 376 | 170 / 212 |
| group + coalesce 25 ms | 90 `[90–90]` | 90 / 0 | 480 | 131 / 239 |
| group + coalesce 100 ms | 90 `[90–90]` | 90 / 0 | 480 | 241 / 317 |
| group + streaming | **30** `[30]` | 90 / 0 | 0 charged recheck CPU | 190 / 197 |

There were no empty long-poll timeouts in these runs: every stream received a Record write before the 20 s server deadline. The separate empty counter is present in the raw artifacts and should be exercised with an idle/no-write control before production capacity planning.

## Files and validation

Raw artifacts are `shape-groups-perf2-rtt100.json`, `shape-groups-perf2-rtt250.json`, `shape-groups-perf2-30-current.json`, `shape-groups-perf2-30-halved.json`, `shape-groups-perf2-steady-500.json`, `shape-groups-perf2-steady-2000.json`, `shape-groups-perf2-steady-5000-idle.json`, and `shape-groups-perf2-streaming-500.json` in this directory. TypeScript validation passed with `pnpm exec tsc --noEmit -p tsconfig.json`; the release engine was built once with Rust 1.96.0 and reused for all runs.
