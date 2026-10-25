RTT 100 ms, downlink 20 Mbps, 1 phone(s), 1 reps (medians)
| Scenario | Design | First content | First content p90 (phones) | First table | Everything | Load requests | Gateway CPU (load) | Gateway lag p99 | Hold: gateway CPU | Hold: requests | Hold: upstream reads | Long-polls held | PG statements | MB down |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| cold launch | table load (gate per table) | 791 ms | 791 ms | 362 ms | 2119 ms | 48 | 616 ms | 1 ms | 2360 ms | 591 | 590 | 24 | 1631 | 7.46 |
| cold launch | table load (gate per table) + mux | 870 ms | 870 ms | 416 ms | 2187 ms | 27 | 616 ms | 2 ms | 3728 ms | 380 | 932 | 3 | 1634 | 7.48 |
