RTT 100 ms, downlink 20 Mbps, 30 phone(s), 2 reps (medians)
| Scenario | Design | First content | First content p90 (phones) | First table | Everything | Load requests | Gateway CPU (load) | Gateway lag p99 | Hold: gateway CPU | Hold: requests | Hold: upstream reads | Long-polls held | PG statements | MB down |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| cold launch | scopes + gate | 14654 ms | 19903 ms | 5114 ms | 22398 ms | 3220 | 21260 ms | 83 ms | 880 ms | 221 | 220 | 720 | 2895 | 119.19 |
| cold launch | table load (gate per table) + mux | 7702 ms | 8497 ms | 4151 ms | 10970 ms | 830 | 13080 ms | 218 ms | 1040 ms | 77 | 260 | 90 | 2894 | 118.10 |
| cold launch | scope load + mux | 6367 ms | 6606 ms | 5753 ms | 8684 ms | 180 | 7280 ms | 4671 ms | 1016 ms | 98 | 254 | 90 | 1005 | 118.04 |
