RTT 100 ms, downlink 20 Mbps, 3 reps (medians)
| Scenario | Design | First content | Everything | Load requests | Phone requests incl. live | Gateway CPU | Gateway lag p99 | Engine CPU | PG statements | PG exec time | Long-polls held | MB down |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| cold launch | scope load + mux | 931 ms | 2027 ms | 6 | 14 | 352 ms | 165 ms | 50 ms | 48 | 20 ms | 3 | 3.98 |
| cold launch | scope load + mux + series | 926 ms | 2024 ms | 8 | 16 | 388 ms | 210 ms | 50 ms | 58 | 16 ms | 4 | 3.88 |
| chat open | scope load + mux | 510 ms | 936 ms | 4 | 12 | 112 ms | 19 ms | 30 ms | 22 | 7 ms | 2 | 1.87 |
| chat open | scope load + mux + series | 514 ms | 939 ms | 4 | 12 | 116 ms | 10 ms | 20 ms | 23 | 7 ms | 2 | 1.87 |
| agenda fling | scope load + mux | 1124 ms | 1124 ms | 24 | 25 | 1300 ms | 101 ms | 110 ms | 236 | 66 ms | 12 | 9.06 |
| agenda fling | scope load + mux + series | 326 ms | 336 ms | 24 | 24 | 1152 ms | 67 ms | 80 ms | 223 | 21 ms | 12 | 3.30 |
| stack visit | scope load + mux | 697 ms | 697 ms | 2 | 2 | 108 ms | 80 ms | 20 ms | 27 | 7 ms | 1 | 1.10 |
| stack visit | scope load + mux + series | 478 ms | 478 ms | 2 | 2 | 96 ms | 69 ms | 20 ms | 26 | 4 ms | 1 | 0.61 |
| chat reopen | scope load + mux | 906 ms | 907 ms | 4 | 13 | 116 ms | 39 ms | 20 ms | 19 | 5 ms | 2 | 1.92 |
| chat reopen | scope load + mux + series | 911 ms | 911 ms | 4 | 13 | 120 ms | 37 ms | 20 ms | 20 | 6 ms | 2 | 1.92 |
| record open | scope load + mux | 236 ms | 236 ms | 2 | 2 | 108 ms | 81 ms | 10 ms | 19 | 0 ms | 1 | 0.01 |
| record open | scope load + mux + series | 236 ms | 236 ms | 2 | 2 | 108 ms | 80 ms | 10 ms | 18 | 0 ms | 1 | 0.01 |
