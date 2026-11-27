# B2: where the engine files the gateway's single-key filters (Main, via circuits-engine-watch)

**Answer.** All 22 rows land where expected, observed from the engine. With the single-key flag
on, the six collections that read through one key are filed under the reader's own key, and a
1,000-row commit costs the same against 1,000 readers as against 40. With it off they are on the
scan list (`calendar_event`: filed under a value every reader shares), and the commit grows with
readers. The five collections the flag does not change are on the scan list both ways.

- **What rests on what.** "Scan list" or "filed" is the engine's own count on main `46d15563`
  (`scan` in `GET /tables/{t}/families`). "Own key" or "shared value" is the cost on prod's binary
  `9bc1563`: flat means the reader's own key, growing means a value every reader shares or the
  scan list, and the count says which of those two.
- **Prod's binary agrees where it can say anything.** It has no `scan` count, but with single
  key on it puts the four index tables in an equality family, a hash lookup by key, as main does.
- **Which key, by name, is not reported by either engine.** For `record` and `calendar_event`
  the engine says "filed" and the cost says "own" or "shared"; the key's name follows because the
  filter has only one equality that fits (`space_id`, `user_id`, or `is_busy_block = false`).
  For the index tables the engine does report the family's key column by position, which is
  `space_id` in the engine's column order. The rule that picks the key is read, not verified
  (`apps/engine/src/predicate.rs`, `access_leaf`).

Actions run `45982458707`, harness `f4bec85c`, branch `profile/w6-list-keys`, 1 CPU. Folders
`paired-b2-*` beside this file; the table is printed by `tools/stall-repro/b2_report.py`.

## The table

| Collection | Single key | Expected placement | Observed on main (counts) | Observed on 9bc1563 (cost, 40 → 1,000 readers) | Match |
| --- | --- | --- | --- | --- | --- |
| `record` | off | scan list (no plain equality in the top-level AND chain) | standalone +1, scan +1: on the scan list | 0.05 s → 0.27 s: grows; its count: standalone +1 | yes |
| `record` | on | filed under the reader's own key: space_id = <SPACE> | standalone +1, scan +0: filed, not on the scan list | 0.05 s → 0.05 s: flat; its count: standalone +1 | yes |
| `record_assignee_index` | off | scan list (no plain equality in the top-level AND chain) | standalone +1, scan +1: on the scan list | 0.03 s → 0.20 s: grows; its count: standalone +1 | yes |
| `record_assignee_index` | on | filed under the reader's own key: space_id = <SPACE> | equality family +1 (key column 2), standalone +0 | 0.01 s → 0.02 s: flat; its count: equality family +1 (key column 2), standalone +0 | yes |
| `record_completed_index` | off | scan list (no plain equality in the top-level AND chain) | standalone +1, scan +1: on the scan list | 0.01 s → 0.20 s: grows; its count: standalone +1 | yes |
| `record_completed_index` | on | filed under the reader's own key: space_id = <SPACE> | equality family +1 (key column 2), standalone +0 | 0.01 s → 0.02 s: flat; its count: equality family +1 (key column 2), standalone +0 | yes |
| `record_date_index` | off | scan list (no plain equality in the top-level AND chain) | standalone +1, scan +1: on the scan list | 0.04 s → 0.20 s: grows; its count: standalone +1 | yes |
| `record_date_index` | on | filed under the reader's own key: space_id = <SPACE> | equality family +1 (key column 7), standalone +0 | 0.01 s → 0.02 s: flat; its count: equality family +1 (key column 7), standalone +0 | yes |
| `record_recurrence` | off | scan list (no plain equality in the top-level AND chain) | standalone +1, scan +1: on the scan list | 0.04 s → 0.21 s: grows; its count: standalone +1 | yes |
| `record_recurrence` | on | filed under the reader's own key: space_id = <SPACE> | equality family +1 (key column 7), standalone +0 | 0.01 s → 0.02 s: flat; its count: equality family +1 (key column 7), standalone +0 | yes |
| `assignment` | off | scan list (no plain equality in the top-level AND chain) | standalone +1, scan +1: on the scan list | 0.04 s → 0.24 s: grows; its count: standalone +1 | yes |
| `assignment` | on | scan list (no plain equality in the top-level AND chain) | standalone +1, scan +1: on the scan list | 0.03 s → 0.14 s: grows; its count: standalone +1 | yes |
| `calendar_event` | off | filed under a shared value: is_busy_block = False | standalone +1, scan +0: filed, not on the scan list | 0.06 s → 0.26 s: grows; its count: standalone +1 | yes |
| `calendar_event` | on | filed under the reader's own key: user_id = <USER> | standalone +1, scan +0: filed, not on the scan list | 0.05 s → 0.05 s: flat; its count: standalone +1 | yes |
| `graph_edge` | off | scan list (no plain equality in the top-level AND chain) | standalone +1, scan +1: on the scan list | 0.05 s → 0.69 s: grows; its count: standalone +1 | yes |
| `graph_edge` | on | scan list (no plain equality in the top-level AND chain) | standalone +1, scan +1: on the scan list | 0.05 s → 0.57 s: grows; its count: standalone +1 | yes |
| `user` | off | scan list (no plain equality in the top-level AND chain) | standalone +1, scan +1: on the scan list | 0.03 s → 0.22 s: grows; its count: standalone +1 | yes |
| `user` | on | scan list (no plain equality in the top-level AND chain) | standalone +1, scan +1: on the scan list | 0.02 s → 0.15 s: grows; its count: standalone +1 | yes |
| `resource_fact` | off | scan list (no plain equality in the top-level AND chain) | standalone +1, scan +1: on the scan list | 0.03 s → 0.23 s: grows; its count: standalone +1 | yes |
| `resource_fact` | on | scan list (no plain equality in the top-level AND chain) | standalone +1, scan +1: on the scan list | 0.03 s → 0.14 s: grows; its count: standalone +1 | yes |
| `resource` | off | scan list (no plain equality in the top-level AND chain) | standalone +1, scan +1: on the scan list | 0.04 s → 0.24 s: grows; its count: standalone +1 | yes |
| `resource` | on | scan list (no plain equality in the top-level AND chain) | standalone +1, scan +1: on the scan list | 0.03 s → 0.14 s: grows; its count: standalone +1 | yes |

22 of 22 rows match.

## How it was run

- **Filters:** the 22 literal authorization filters from circuits-coord (11 collections, single
  key off and on, built by the gateway in prod with ids replaced), kept verbatim in
  `tools/stall-repro/b2_filters.json`. Nothing is added: no phone filter, no stream identity.
  Each reader's `<SPACE>`, `<USER>` and `<ID_1>` are its own synthetic values.
- **Counts:** one reader on a fresh engine. Each filter is created on its own, and its table's
  `GET /tables/{t}/families` is read just before and just after, so every count is that one
  filter's. Run on main `46d15563` with the list rule off, and on prod's binary `9bc1563`, whose
  answer has `families` and `standalone` only.
- **Cost:** prod's binary `9bc1563`. 40 readers and then 1,000 on one runner, each holding all 11
  filters (441 and 11,001 shapes with the probe). Per table, three commits of 1,000 new rows for
  an account whose Space and user no reader holds; the time is the median, from the first
  change-log delivery to the commit's receipt, polled every 20 ms.
- **The floor here is 0.01–0.06 s, not 0.2 s:** the rows match no reader, so nothing is written
  to any stream. The 0.2 s floor of earlier runs includes delivering matched rows.
- **Growth is about the same size everywhere it appears:** 0.1–0.2 µs per row per reader for
  the short filters (0.11–0.22 s for 960 more readers), and 0.5–0.7 µs for `graph_edge`'s long one.
- **The single-key-on cost cells ran on a different runner from the off cells** (EPYC 9V74 and
  EPYC 7763). Each row compares 40 with 1,000 readers on one runner; do not compare an off row's
  seconds with an on row's.
- **One column added to the harness schema:** `resource_fact.resource_space_id`, which the
  filter reads and the harness's pinned schema (indexed `52e79b8`) does not have. The writer
  fills it with the fact's Space.
- **Not covered:** phone `where` clauses, the per-thread chat tables, readers with more than one
  Space, and prod itself. No prod read and no prod setting was touched.
