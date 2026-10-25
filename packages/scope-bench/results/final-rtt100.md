RTT 100 ms, downlink 20 Mbps, 1 phone(s), 3 reps (medians)
| Scenario | Design | First content | First content p90 (phones) | First table | Everything | Load requests | Gateway CPU (load) | Gateway lag p99 | Hold: gateway CPU | Hold: requests | Hold: upstream reads | Long-polls held | PG statements | MB down |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| cold launch | today | 2571 ms | 2571 ms | 983 ms | 2571 ms | 96 | 1096 ms | 440 ms | 76 ms | 20 | 19 | 24 | 109 | 3.98 |
| cold launch | scopes + gate | 1424 ms | 1424 ms | 546 ms | 3001 ms | 96 | 1096 ms | 86 ms | 76 ms | 20 | 19 | 24 | 109 | 3.97 |
| cold launch | table load (gate per table) | 813 ms | 813 ms | 390 ms | 1996 ms | 48 | 616 ms | 101 ms | 76 ms | 20 | 19 | 24 | 109 | 3.95 |
| cold launch | table load (gate per table) + series | 935 ms | 935 ms | 375 ms | 2160 ms | 52 | 672 ms | 58 ms | 76 ms | 20 | 19 | 26 | 121 | 3.87 |
| chat open | today | 11259 ms | 11259 ms | 6898 ms | 12227 ms | 936 | 11104 ms | 414 ms | 260 ms | 66 | 65 | 231 | 1386 | 2.14 |
| chat open | scopes + gate | 1222 ms | 1222 ms | 614 ms | 1234 ms | 20 | 228 ms | 38 ms | 32 ms | 9 | 8 | 5 | 29 | 1.85 |
| chat open | table load (gate per table) | 1002 ms | 1002 ms | 284 ms | 1002 ms | 10 | 128 ms | 37 ms | 32 ms | 9 | 8 | 5 | 29 | 1.85 |
| chat open | table load (gate per table) + series | 994 ms | 994 ms | 272 ms | 996 ms | 10 | 128 ms | 38 ms | 32 ms | 9 | 8 | 5 | 29 | 1.85 |
| agenda fling | today | 2642 ms | 2642 ms | 1003 ms | 3469 ms | 384 | 4544 ms | 179 ms | 68 ms | 18 | 17 | 96 | 485 | 9.15 |
| agenda fling | scopes + gate | 1453 ms | 1453 ms | 939 ms | 6107 ms | 416 | 4704 ms | 110 ms | 96 ms | 25 | 24 | 96 | 486 | 9.16 |
| agenda fling | table load (gate per table) | 896 ms | 896 ms | 773 ms | 3174 ms | 192 | 2656 ms | 108 ms | 160 ms | 41 | 40 | 96 | 485 | 9.10 |
| agenda fling | table load (gate per table) + series | 394 ms | 394 ms | 214 ms | 474 ms | 168 | 2324 ms | 63 ms | 28 ms | 8 | 7 | 84 | 437 | 3.33 |
| stack visit | today | 2642 ms | 2642 ms | 1797 ms | 3525 ms | 224 | 2656 ms | 740 ms | 32 ms | 9 | 8 | 56 | 285 | 4.08 |
| stack visit | scopes + gate | 1023 ms | 1023 ms | 517 ms | 1023 ms | 32 | 352 ms | 37 ms | 32 ms | 9 | 8 | 8 | 45 | 1.10 |
| stack visit | table load (gate per table) | 799 ms | 799 ms | 364 ms | 800 ms | 16 | 192 ms | 101 ms | 32 ms | 9 | 8 | 8 | 45 | 1.10 |
| stack visit | table load (gate per table) + series | 562 ms | 562 ms | 328 ms | 562 ms | 14 | 168 ms | 59 ms | 28 ms | 8 | 7 | 7 | 41 | 0.62 |
| chat reopen | today | 831 ms | 831 ms | 735 ms | 1241 ms | 20 | 232 ms | 61 ms | 32 ms | 9 | 8 | 5 | 26 | 1.89 |
| chat reopen | scopes + gate | 807 ms | 807 ms | 635 ms | 1226 ms | 20 | 232 ms | 36 ms | 28 ms | 8 | 7 | 5 | 26 | 1.85 |
| chat reopen | table load (gate per table) | 958 ms | 958 ms | 242 ms | 962 ms | 10 | 128 ms | 33 ms | 36 ms | 10 | 9 | 5 | 26 | 1.90 |
| chat reopen | table load (gate per table) + series | 959 ms | 959 ms | 253 ms | 959 ms | 10 | 128 ms | 36 ms | 36 ms | 10 | 9 | 5 | 26 | 1.89 |
| record open | today | 681 ms | 681 ms | 549 ms | 681 ms | 32 | 352 ms | 79 ms | 32 ms | 9 | 8 | 8 | 37 | 0.02 |
| record open | scopes + gate | 705 ms | 705 ms | 572 ms | 705 ms | 32 | 352 ms | 101 ms | 32 ms | 9 | 8 | 8 | 37 | 0.02 |
| record open | table load (gate per table) | 371 ms | 371 ms | 353 ms | 371 ms | 16 | 192 ms | 101 ms | 32 ms | 9 | 8 | 8 | 37 | 0.02 |
| record open | table load (gate per table) + series | 369 ms | 369 ms | 351 ms | 369 ms | 16 | 192 ms | 101 ms | 32 ms | 9 | 8 | 8 | 37 | 0.02 |
