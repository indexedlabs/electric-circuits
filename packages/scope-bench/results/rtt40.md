RTT 40 ms, downlink 20 Mbps, 2 reps (medians)
| Scenario | Design | First content | Everything | Load requests | Phone requests incl. live | Gateway CPU | Gateway lag p99 | Engine CPU | PG statements | PG exec time | Long-polls held | MB down |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| cold launch | today | 2407 ms | 2407 ms | 96 | 104 | 1184 ms | 436 ms | 50 ms | 112 | 22 ms | 24 | 4.00 |
| cold launch | scopes + gate | 1153 ms | 2594 ms | 96 | 104 | 1184 ms | 101 ms | 50 ms | 112 | 24 ms | 24 | 4.00 |
| cold launch | gateway fan-in | 960 ms | 2056 ms | 27 | 34 | 436 ms | 169 ms | 40 ms | 111 | 35 ms | 24 | 3.98 |
| cold launch | scope load + mux | 851 ms | 1947 ms | 6 | 14 | 352 ms | 128 ms | 40 ms | 48 | 18 ms | 3 | 3.98 |
| chat open | today | 11384 ms | 12151 ms | 938 | 1008 | 11408 ms | 2034 ms | 350 ms | 1388 | 28 ms | 231 | 2.17 |
| chat open | scopes + gate | 1065 ms | 1065 ms | 20 | 27 | 268 ms | 38 ms | 30 ms | 31 | 9 ms | 5 | 1.87 |
| chat open | gateway fan-in | 473 ms | 898 ms | 7 | 14 | 124 ms | 27 ms | 30 ms | 31 | 6 ms | 5 | 1.86 |
| chat open | scope load + mux | 454 ms | 880 ms | 4 | 11 | 112 ms | 28 ms | 20 ms | 22 | 7 ms | 2 | 1.86 |
| agenda fling | today | 2780 ms | 3686 ms | 384 | 392 | 4640 ms | 152 ms | 140 ms | 487 | 81 ms | 96 | 9.15 |
| agenda fling | scopes + gate | 1076 ms | 5084 ms | 408 | 432 | 4800 ms | 99 ms | 160 ms | 487 | 76 ms | 96 | 9.16 |
| agenda fling | gateway fan-in | 1096 ms | 1096 ms | 108 | 116 | 1664 ms | 102 ms | 120 ms | 487 | 75 ms | 96 | 9.07 |
| agenda fling | scope load + mux | 1063 ms | 1063 ms | 24 | 25 | 1300 ms | 100 ms | 110 ms | 235 | 62 ms | 12 | 9.06 |
| stack visit | today | 2190 ms | 3175 ms | 224 | 224 | 2688 ms | 722 ms | 90 ms | 287 | 44 ms | 56 | 4.08 |
| stack visit | scopes + gate | 839 ms | 839 ms | 32 | 32 | 384 ms | 101 ms | 20 ms | 47 | 7 ms | 8 | 1.10 |
| stack visit | gateway fan-in | 665 ms | 665 ms | 9 | 9 | 136 ms | 101 ms | 20 ms | 48 | 7 ms | 8 | 1.10 |
| stack visit | scope load + mux | 633 ms | 633 ms | 2 | 2 | 108 ms | 98 ms | 20 ms | 26 | 8 ms | 1 | 1.10 |
| chat reopen | today | 1017 ms | 1017 ms | 20 | 28 | 272 ms | 34 ms | 20 ms | 28 | 5 ms | 5 | 1.92 |
| chat reopen | scopes + gate | 597 ms | 1020 ms | 20 | 29 | 272 ms | 33 ms | 20 ms | 28 | 6 ms | 5 | 1.92 |
| chat reopen | gateway fan-in | 442 ms | 865 ms | 7 | 16 | 132 ms | 33 ms | 30 ms | 28 | 4 ms | 5 | 1.91 |
| chat reopen | scope load + mux | 430 ms | 856 ms | 4 | 12 | 116 ms | 43 ms | 20 ms | 19 | 5 ms | 2 | 1.92 |
| chat reopen | scope load + mux + resume | 62 ms | 63 ms | 4 | 13 | 48 ms | 2 ms | 0 ms | 8 | 0 ms | 2 | 0.13 |
| record open | today | 506 ms | 506 ms | 32 | 32 | 384 ms | 41 ms | 10 ms | 39 | 0 ms | 8 | 0.02 |
| record open | scopes + gate | 522 ms | 522 ms | 32 | 32 | 384 ms | 100 ms | 10 ms | 40 | 0 ms | 8 | 0.02 |
| record open | gateway fan-in | 204 ms | 204 ms | 9 | 9 | 136 ms | 105 ms | 10 ms | 39 | 1 ms | 8 | 0.01 |
| record open | scope load + mux | 176 ms | 176 ms | 2 | 2 | 108 ms | 100 ms | 10 ms | 19 | 0 ms | 1 | 0.01 |
