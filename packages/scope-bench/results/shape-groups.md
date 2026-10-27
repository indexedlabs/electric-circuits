# Shape groups prototype results

This is a performance prototype on `prototype/shape-groups`, built from
`prototype/scope-load@026a041`. The release engine was built with Rust 1.96.0
and the checked in toolchain. The sandbox had PostgreSQL 16 client/server
binaries but no PostgreSQL 18 package; the run used an explicit local
`initdb`/`pg_ctl` shim that reports 18 while executing PostgreSQL 16.15.
These numbers are therefore PG16 compatibility evidence, not PG18 qualification.

## Verdict

Groups beat scope loads in the cost that motivated them: one stream and one
long-poll per scope. At 30 phones in the steady write phase, median gateway
hold CPU was 1.29 s for groups versus 4.74 s for scope load, and write-to-phone
latency was 208/265 ms (p50/p90) versus 511/869 ms. Cold launch stayed within
the scope-load first-content range at one phone; at 30 phones with the gateway
CPU halved, groups reached 2.94 s median first content versus 5.60 s for scope
load. The prototype does not prove the design is merge-ready.

## One-phone scenario matrix

Medians of two reps; “reads” is upstream Durable Streams reads during the hold,
and “streams” is the number of held streams. Gateway CPU is load-only.

| RTT | Scenario | Scope load first / ms | Scope group first / ms | Scope load CPU / ms | Group CPU / ms | Scope reads / group | Scope streams / group |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | cold launch | 914 | 920 | 352 | 332 | 24 / 3 | 24 / 3 |
| 100 | chat open | 505 | 505 | 84 | 88 | 5 / 2 | 5 / 2 |
| 100 | chat reopen | 485 | 489 | 84 | 88 | 5 / 2 | 5 / 2 |
| 100 | agenda fling | 1128 | 1121 | 1964 | 1388 | 96 / 12 | 96 / 12 |
| 100 | Stack visit | 682 | 695 | 104 | 108 | 8 / 1 | 8 / 1 |
| 100 | Record open | 229 | 233 | 104 | 108 | 8 / 1 | 8 / 1 |
| 250 | cold launch | 1065 | 1065 | 352 | 332 | 24 / 3 | 24 / 3 |
| 250 | chat open | 652 | 653 | 84 | 88 | 5 / 2 | 5 / 2 |
| 250 | chat reopen | 637 | 1069 | 84 | 88 | 5 / 2 | 5 / 2 |
| 250 | agenda fling | 1278 | 1277 | 1856 | 1372 | 42 / 11 | 42 / 11 |
| 250 | Stack visit | 837 | 842 | 104 | 108 | 8 / 1 | 8 / 1 |
| 250 | Record open | 380 | 384 | 104 | 108 | 8 / 1 | 8 / 1 |

The 100 ms and 250 ms JSON artifacts include `today` and
`table load (gate per table)` reference designs. The separate resume probe
(`shape-groups-resume.json`) measured 111 ms first content, two held streams,
and three upstream reads for `scope group + resume` on chat reopen.

## 30-phone cold launch

Thirty distinct accounts, two reps, RTT 100 ms.

| Gateway | Design | First content | First content p90 | Load CPU | Hold CPU | Hold reads | Streams | Engine RSS |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| current | scope load | 5199 ms | 10382 ms | 11968 ms | 300 ms | 75 | 720 | 42.2 MB |
| current | scope load + mux | 5663 ms | 15282 ms | 19200 ms | 832 ms | 208 | 90 | 42.0 MB |
| current | scope group | 5529 ms | 11640 ms | 12160 ms | 188 ms | 47 | 90 | 43.5 MB |
| halved | scope load | 5604 ms | 5684 ms | 10116 ms | 664 ms | 332 | 720 | 42.5 MB |
| halved | scope load + mux | 5410 ms | 5453 ms | 6176 ms | 272 ms | 136 | 90 | 74.4 MB |
| halved | scope group | 2942 ms | 5841 ms | 5672 ms | 82 ms | 41 | 90 | 57.9 MB |

The group stream count is one per scope per phone. The engine RSS samples are
process RSS at the end of each run; the 30-phone runs show more variance under
gateway contention, especially for mux.

## Steady-state writes

Thirty distinct accounts, two reps, RTT 100 ms, Record open held for 5 seconds.
Each write transaction updates the Record title and inserts/updates one
assignment and one tagged item for that account. Latency is from commit to a
stream response containing the unique marker.

| Design | Load CPU | Hold CPU | Hold reads | Streams | Engine RSS | Write p50 / p90 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| scope load | 3440 ms | 4736 ms | 1184 | 240 | 37.4 MB | 511 / 869 ms |
| scope load + mux | 3120 ms | 4800 ms | 1200 | 30 | 37.3 MB | 732 / 1235 ms |
| scope group | 3240 ms | 1292 ms | 323 | 30 | 37.7 MB | 208 / 265 ms |

## Correctness shortcuts and limits

- Each member is still created through the existing plain or `IN`-subquery
  shape path. The prototype overrides the member stream path, keeps one
  settled Postgres snapshot for the member pages, and exposes the first
  member's subscription as the group's single live claim; internal member
  claims are released after the page is read.
- There is no durable group catalog record, group identity in the lifecycle
  fold, group retirement/retention/dormancy/restart handling, or atomic
  cross-member 409/conflict check. A later member can fail after earlier
  members have been created; cleanup releases their ordinary claims.
- There is no per-group transaction barrier between plain sequencer appends
  and deferred subquery emission lanes, no bounded group emission queue, and
  no atomic snapshot fence that includes subquery seed work. These are the
  main correctness gaps called out by the proposal.
- Deletes retain today’s key-only envelope behavior; the benchmark does not add
  table-qualified delete keys or a client materializer that routes decoded rows
  into separate collections. The group phone holds one stream and measures
  routing at the benchmark boundary.
- Authorization is the existing gateway predicate rewrite per member. The
  group gateway route charges one request, one engine call, and one upstream
  stream HEAD, but it does not implement a durable group capability token.
- The steady write phase uses the seeded synthetic schema and PG16 shim, not
  production traffic or PG18. RSS is sampled once at the end of a run, and
  gateway CPU is a calibrated busy-loop stand-in. Results are sensitive to
  two vCPUs, the 20 Mbps modeled downlink, short 300 ms long-polls, and the
  existing benchmark’s synthetic row sizes.
- The prototype was not run through the full conformance, Swift, or production
  gateway suites and is intentionally not suitable for merge.

Artifacts: `shape-groups-rtt100.json`, `shape-groups-rtt250.json`,
`shape-groups-30u-current.json`, `shape-groups-30u-halved.json`,
`shape-groups-steady-30u.json`, and `shape-groups-resume.json`.

