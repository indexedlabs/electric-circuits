RTT 100 ms, downlink 20 Mbps, 30 phone(s), 2 reps (medians)
| Scenario | Design | First content | First content p90 (phones) | First table | Everything | Load requests | Gateway CPU (load) | Gateway lag p99 | Hold: gateway CPU | Hold: requests | Hold: upstream reads | Long-polls held | PG statements | MB down |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| cold launch | scopes + gate | 13673 ms | 17435 ms | 3386 ms | 21561 ms | 3119 | 20788 ms | 71 ms | 656 ms | 165 | 164 | 720 | 3127 | 33.38 |
| cold launch | table load (gate per table) | 6986 ms | 11137 ms | 4295 ms | 13583 ms | 1566 | 12720 ms | 185 ms | 1096 ms | 275 | 274 | 720 | 3126 | 32.95 |
| cold launch | scope load | 2936 ms | 5345 ms | 2829 ms | 3268 ms | 810 | 7400 ms | 67 ms | 412 ms | 104 | 103 | 720 | 1236 | 32.67 |
