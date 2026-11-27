# B2b: where the engine files served predicates for the six single-key tables (Main, via circuits-engine-watch)

**Answer.** Yes for the claim under test. With the phone's filter and the stream identity around
it, a feed or subset query with single key applied still costs the same against 1,000 readers as
against 40, on all six tables. The same requests as group members, as served today, behave as
expected too: on the scan list, or filed under a value every reader shares, and growing with
readers.

- **8 of 12 rows match on both observations:** `record_recurrence`, `calendar_event`,
  `record_completed_index` and `record_assignee_index`, both forms.
- **4 rows rest on cost alone:** `record` and `record_date_index`, both forms. Their served
  predicates look into another table, so the engine makes each a subquery shape, and neither
  engine's counts say where a subquery shape is filed. Their cost is what the expected placement
  would give: flat with single key applied (0.04 s → 0.04 s and 0.04 s → 0.06 s), growing as a
  group member (0.04 s → 0.74 s and 0.06 s → 1.52 s).
- **Why that is consistent (read, not verified):** subquery shapes are filed in their own index
  by the same rule as standalone shapes, the first plain equality in the top-level AND
  (`apps/engine/src/subq_index.rs:139-168`, which calls `CompiledPredicate::access_leaf`). No
  endpoint exposes that index's counts.
- **No predicate was refused.** All 12 registered on both engines.
- **The group form is the expensive one today.** A day-window group load of `record` costs
  1.5 ms per changed row at 1,000 readers; OTTO-14415 would move it to the flat form.

Actions run `45987319597`, harness `1bf49bf8`, branch `profile/w6-list-keys`, 1 CPU. Folders
`paired-b2b-*` beside this file; the table is printed by `tools/stall-repro/b2b_report.py`.

## The table

| Table | Form | Expected placement | Observed on main (counts) | Observed on 9bc1563 (cost, 40 → 1,000 readers) | Match |
| --- | --- | --- | --- | --- | --- |
| `record_date_index` | group member, as served today | scan list (no plain equality in the top-level AND chain) | subquery shape +1, standalone +0: not in the standalone index, so no count says where it is filed | 0.04 s → 0.74 s: grows; its count: subquery shape +1, standalone +0 | cost only: as expected |
| `record_date_index` | feed or subset query, single key applied | filed under the reader's own key: space_id = <SPACE> | subquery shape +1, standalone +0: not in the standalone index, so no count says where it is filed | 0.04 s → 0.04 s: flat; its count: subquery shape +1, standalone +0 | cost only: as expected |
| `record` | group member, as served today | scan list (no plain equality in the top-level AND chain) | subquery shape +1, standalone +0: not in the standalone index, so no count says where it is filed | 0.06 s → 1.52 s: grows; its count: subquery shape +1, standalone +0 | cost only: as expected |
| `record` | feed or subset query, single key applied | filed under the reader's own key: space_id = <SPACE> | subquery shape +1, standalone +0: not in the standalone index, so no count says where it is filed | 0.04 s → 0.06 s: flat; its count: subquery shape +1, standalone +0 | cost only: as expected |
| `record_recurrence` | group member, as served today | range on dtstart | standalone +1, scan +0: filed, not on the scan list | 0.02 s → 0.04 s: flat; its count: standalone +1 | yes |
| `record_recurrence` | feed or subset query, single key applied | filed under the reader's own key: space_id = <SPACE> | standalone +1, scan +0: filed, not on the scan list | 0.04 s → 0.03 s: flat; its count: standalone +1 | yes |
| `calendar_event` | group member, as served today | filed under a shared value: is_busy_block = False | standalone +1, scan +0: filed, not on the scan list | 0.06 s → 0.25 s: grows; its count: standalone +1 | yes |
| `calendar_event` | feed or subset query, single key applied | filed under the reader's own key: user_id = <USER> | standalone +1, scan +0: filed, not on the scan list | 0.05 s → 0.03 s: flat; its count: standalone +1 | yes |
| `record_completed_index` | group member, as served today | scan list (no plain equality in the top-level AND chain) | standalone +1, scan +1: on the scan list | 0.01 s → 0.19 s: grows; its count: standalone +1 | yes |
| `record_completed_index` | feed or subset query, single key applied | filed under the reader's own key: space_id = <SPACE> | standalone +1, scan +0: filed, not on the scan list | 0.02 s → 0.02 s: flat; its count: standalone +1 | yes |
| `record_assignee_index` | group member, as served today | scan list (no plain equality in the top-level AND chain) | standalone +1, scan +1: on the scan list | 0.03 s → 0.19 s: grows; its count: standalone +1 | yes |
| `record_assignee_index` | feed or subset query, single key applied | filed under the reader's own key: space_id = <SPACE> | standalone +1, scan +0: filed, not on the scan list | 0.02 s → 0.02 s: flat; its count: standalone +1 | yes |

8 of 12 rows match on both observations. 4 rows rest on cost alone (record_date_index, group; record_date_index, feed; record, group; record, feed): 4 of them cost what the expected placement would.

## How it was run

- **Predicates:** the 12 served predicates from circuits-coord (built by the gateway in prod
  from Nov 27 request bodies, every id and date replaced), kept in
  `tools/stall-repro/b2b_filters.json` and registered as given. The feed form differs from the
  group form only in the reader's own key being a plain equality.
- **Placeholders:** `<SPACE>`, `<USER>`, `<RECORD>` and `<STREAM>` are each reader's own synthetic
  values. Every `<DATE_n>` is the same for all readers: one day window, 2026-09-15, with the
  app's two-day margins on the recurrence and original-start bounds.
- **Counts:** one reader on a fresh engine, each predicate created on its own with
  `GET /tables/{t}/families` and the engine's shape counts read just before and after. On main
  `46d15563` with the list rule off, and on prod's binary `9bc1563` (no `scan` count).
- **Cost:** prod's binary `9bc1563`. Each engine holds one table's predicate only, for 40
  readers and then 1,000 on one runner. One table per engine, because a commit to one table
  would otherwise feed the subquery nodes of another table's predicates. Three commits of 1,000
  new rows of that table for an account no reader holds; the time is the median, from the first
  change-log delivery to the receipt, polled every 20 ms.
- **The floor is 0.01–0.06 s,** as in B2: the written rows match no reader.
- **`record_recurrence` as a group member** is filed under its `dtstart <` bound, which every
  reader shares. It barely grows here (0.02 s → 0.04 s) because only a fifth of the written rows
  fall under the bound; rows that do are checked against every reader's filter.
- **Not covered:** readers with more than one Space, a real group load (`subsets/load`; each
  predicate here is registered as a feed), and prod itself. No prod read and no prod setting was
  touched.
