RTT 100 ms, downlink 20 Mbps, 1 phone(s), 1 reps (medians)
| Scenario | Design | First content | First content p90 (phones) | First table | Everything | Load requests | Gateway CPU (load) | Gateway lag p99 | Hold: gateway CPU | Hold: requests | Hold: upstream reads | Long-polls held | PG statements | MB down |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| chat open | table load (gate per table) | 655 ms | 655 ms | 377 ms | 1080 ms | 10 | 132 ms | 1 ms | 6640 ms | 1661 | 1660 | 5 | 1554 | 16.77 |
| chat open | table load (gate per table) + mux | 722 ms | 722 ms | 336 ms | 1150 ms | 7 | 132 ms | 1 ms | 13584 ms | 1632 | 3396 | 2 | 1558 | 17.30 |
