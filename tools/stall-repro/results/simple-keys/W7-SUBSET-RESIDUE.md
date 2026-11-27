# W7: does a subset query leave anything in the engine? (Main, via circuits-engine-watch)

**Answer.** No shape, circuit or subquery node stays behind, on prod's binary (`9bc1563`) or on
main (`46d15563`, list rule off). After 100 subset queries that each carry a subquery, every count
the engine exposes is what it was before, right after the queries and again two minutes later.

- **Shapes:** 1,147 before, after and two minutes later (510 standalone, 637 subquery shapes).
- **Subquery registry:** 566 nodes, 4,464 edges and 78,883 contributors throughout.
- **Standalone indexes:** `record` 64, `record_recurrence` 60, `calendar_event` 40 throughout.
- **Memory:** RSS moves by about 3 MiB up right after the queries and is 1.7–1.8 MiB below the
  start two minutes later, on both engines.
- **The one number that differs:** the engine's byte estimate for the subquery registry
  (`bytes_subquery_registry`) is 1,300 bytes higher at the two-minute read than before, on both
  engines, with no count changed. It is unchanged right after the queries, so the queries did not
  put it there at once; the bystander's probe was writing during the wait. What grew is not named
  by anything the engine exposes.

Actions run `45973476075`, harness `80c80bfd`, 1 CPU. Folders `paired-w7-*` beside this file; the
counts are in each folder's `w7-subsets-writes.json`.

## The run

- **Catalog:** S-main3-live-plus, what phones hold under main's lowering: 1,147 shapes, 637 of
  them subquery shapes.
- **The queries:** 100 one-shot `/v1/subsets/query` requests, one after another. Each is a member
  of the app's day window as main lowers a one-shot query (`simple_keys.window_subsets`), taken in
  turn from `record_date_index`, `record`, `assignment` and `graph_edge`, over 19 accounts and 30
  dates. Every one holds `… IN (SELECT …)`. All 100 answered 200, 8,201 rows in all.
- **The counts,** read before, right after and 120 s later (`run.engine_counts`):
  - shapes and the subquery registry from `GET /memory`;
  - each table's standalone index from `GET /tables/{table}/families`;
  - the `shapes_live_*` gauges from `GET /metrics`;
  - the node list from `GET /subqueries`;
  - RSS from the process.
- **The bystander's probe** runs through the whole phase, every 5 s: a marker write, a one-row
  update and a one-row subset query with no subquery.
- **Idle retirement is off** in the harness, so nothing was retired during the wait either.

## prod's binary `9bc1563` (Intel(R) Xeon(R) 6975P-C, `paired-w7-1-S-main3-live-plus-prod9bc-1cpu`)

| | Before | Right after the 100 queries | 2 minutes later |
| --- | ---: | ---: | ---: |
| Shapes (`/memory`) | 1,147 | 1,147 | 1,147 |
| Standalone shapes | 510 | 510 | 510 |
| Subquery shapes | 637 | 637 | 637 |
| Subquery nodes | 566 | 566 | 566 |
| Subquery edges | 4,464 | 4,464 | 4,464 |
| Subquery contributors | 78,883 | 78,883 | 78,883 |
| Subquery feed entries | 3,025 | 3,025 | 3,025 |
| Families / family shapes | 0 / 0 | 0 / 0 | 0 / 0 |
| Dormant shapes | 0 | 0 | 0 |
| Tailers | 11 | 11 | 11 |
| `record` standalone index | 64 | 64 | 64 |
| `record_recurrence` standalone index | 60 | 60 | 60 |
| `calendar_event` standalone index | 40 | 40 | 40 |
| `record_date_index`, `assignment`, `graph_edge` | no tailer (every shape on them is a subquery shape) | no tailer (every shape on them is a subquery shape) | no tailer (every shape on them is a subquery shape) |
| `/metrics` `shapes_live_changes_only_subscribed` | 510 | 510 | 510 |
| `/metrics` `shapes_live_subquery_subscribed` | 637 | 637 | 637 |
| `/metrics` other `shapes_live_*` gauges | 0 | 0 | 0 |
| `/subqueries` nodes listed | 566 | 566 | 566 |
| RSS | 255.2 MiB | 258.2 MiB | 253.5 MiB |
| `bytes_subquery_registry` | 61,348,568 | 61,348,568 | 61,349,868 |
| Every other `bytes_*` estimate | same as before | same as before | same as before |

## main `46d15563`, list rule off (Intel(R) Xeon(R) 6975P-C, `paired-w7-2-S-main3-live-plus-main46d-1cpu`)

| | Before | Right after the 100 queries | 2 minutes later |
| --- | ---: | ---: | ---: |
| Shapes (`/memory`) | 1,147 | 1,147 | 1,147 |
| Standalone shapes | 510 | 510 | 510 |
| Subquery shapes | 637 | 637 | 637 |
| Subquery nodes | 566 | 566 | 566 |
| Subquery edges | 4,464 | 4,464 | 4,464 |
| Subquery contributors | 78,883 | 78,883 | 78,883 |
| Subquery feed entries | 3,025 | 3,025 | 3,025 |
| Families / family shapes | 0 / 0 | 0 / 0 | 0 / 0 |
| Dormant shapes | 0 | 0 | 0 |
| Tailers | 11 | 11 | 11 |
| `record` standalone index (on the scan list) | 64 (64) | 64 (64) | 64 (64) |
| `record_recurrence` standalone index (on the scan list) | 60 (0) | 60 (0) | 60 (0) |
| `calendar_event` standalone index (on the scan list) | 40 (0) | 40 (0) | 40 (0) |
| `record_date_index`, `assignment`, `graph_edge` | no tailer (every shape on them is a subquery shape) | no tailer (every shape on them is a subquery shape) | no tailer (every shape on them is a subquery shape) |
| `/metrics` `shapes_live_changes_only_subscribed` | 510 | 510 | 510 |
| `/metrics` `shapes_live_subquery_subscribed` | 637 | 637 | 637 |
| `/metrics` other `shapes_live_*` gauges | 0 | 0 | 0 |
| `/subqueries` nodes listed | 566 | 566 | 566 |
| RSS | 252.6 MiB | 255.0 MiB | 250.8 MiB |
| `bytes_subquery_registry` | 61,348,568 | 61,348,568 | 61,349,868 |
| Every other `bytes_*` estimate | same as before | same as before | same as before |

