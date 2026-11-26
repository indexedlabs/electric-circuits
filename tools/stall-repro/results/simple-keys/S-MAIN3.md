Result: Monday's stall can still happen through the subquery shapes that main at `52e79b875b` leaves on today's filters, even with all three keyed flags on. The day-window group members that read through `record_date_index` or `record_recurrence` (`record`, `record_date_index`, `assignment`, `graph_edge`) stay on that path. A first import pins the engine (95–100% of one CPU) and drains and live changes for other families time out:

- **Phones' live demands only:** for about 55 s per import, against 2.5 s on S.
- **Every day window kept** (the fixture's history, as if idle shapes were never retired): for 6–7.5 minutes.

A paced deletion on its own stays mostly served. Run beside an import, its receipts wait 47 s (live demands) to 298 s (every day window).

# S-main3: the demands phones send, lowered as indexed main lowers them

**Runs.** Actions runs `45850851465` (harness `677d676`; engine build plus the import and paced
deletion), `45860702038` (`e03dca2`; import plus paced deletion together, first job only, because
the second job never got a runner) and `45861147470` (`ec12ec8`; S-main3-live). Engine `9bc1563`
(prod's binary, built from source in run `45850851465`), Nov 24 catalog, 1 CPU. Every row is
paired with S on the same runner. S runs on the pinned Indexed source as before; S-main3 runs on
indexed main `52e79b875b` (gateway and schema). Run folders `paired-main3-*` are beside this file.

**Variant.** `simple_keys.main3_lower` makes `_authorize_collection_read`'s keyed decision, then
calls main's own `authorize_predicate` and `_chat_authorization_predicate`, with
`circuits_keyed_scopes`, `circuits_keyed_index_tables` and `circuits_keyed_chat_rows` on:

- A **feed** drops its date window before the test.
- A **group member** keeps it, so it keys only without `in`.
- **Chat** keys by `_keyed_chat_demand`.

**Demands** are the app's own predicates from `ios/Index` at the same commit
(`main3_app_demands`):

- **Home groups:** today and tomorrow (six members each), and the recurring groups.
- **Day-window groups.**
- **Thread-rail and thread groups.**
- **Session feeds:** workflow-root `record`, `resource`, the pending deck, account tables, and the
  family graph by Space.

**Mapping.** Each account keeps its fixture demand counts. The i-th fixture demand on a
day-window table becomes that table's member of the day window i days into the fixture's 84-day
range. Index-table demands become thread-rail groups, chat demands become thread groups, and
everything else is held once as the session's feeds.

**S-main3-live** drops the fixture's earlier day windows, keeping only what phones hold now. Prod's
`9bc1563` retires idle live-only shapes (PR 73) and the harness switches retirement off, so the
full catalog is the case where no earlier window has been retired yet.

**Approximations.**
- **Groups:** group members are created as changes-only feeds with the member's lowered filter;
  engine group lanes are not used.
- **Keyed lists:** built from the fixture by main's `_read_keyed_audience_lists` rules, with
  derived access off.
- **Card scopes:** for a card scoped to one Record, the harness applies main's post-validation
  filter, `_card_thread_predicate`, directly.

**Load.**
- **`import`:** the harness's first import at its largest size (13,081 changes in six commits,
  unpaced, as prod commits it today).
- **`paced-delete`:** the large account's 29,793 rows in 2,980 commits of at most 10 tracked and
  500 total rows (`BulkWriteSettings`). After each commit the writer polls the engine's receipt
  every 50 ms (`PacedCommits`), so 0.052 s is the poll floor.
- **`import+paced-delete`:** both at once on two writers, in a fresh engine session.

**Pass line** (S's): each workload's other-family drains, subsets and live changes all succeed.
The run label is the same-runner pair.

| Pair · variant | Runner CPU | Workload | First delivery → empty s | × S (same runner) | CPU % | Backfills | Drain, p95 | Subset, p95 | Live change, p95 | Pass? |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- | --- | --- |
| main3-1 · S | EPYC 9V74 | import | 2.9 | 1.00 | 29 | 0 | 100% (3/3); p95 0.36 | 100% (3/3); p95 0.01 | 100% (3/3); p95 0.29 | **yes** |
| main3-1 · S | EPYC 9V74 | paced-delete | 203.7 | 1.00 | 3 | 0 | 100% (203/203); p95 0.16 | 100% (203/203); p95 0.01 | 100% (203/203); p95 0.16 | **yes** |
| main3-1 · S-main3 | EPYC 9V74 | import | 454.9 | 158.18 | 96 | 21,420 | 0% (0/455); p95 — | 67% (303/455); p95 1.69 | 0% (0/455); p95 — | no |
| main3-1 · S-main3 | EPYC 9V74 | paced-delete | 366.6 | 1.80 | 46 | 17,618 | 91% (351/386); p95 0.51 | 100% (386/386); p95 0.22 | 91% (351/386); p95 0.45 | **yes** |
| main3-2 · S-main3 | Xeon 6975P-C | import | 360.9 | 172.79 | 100 | 21,420 | 0% (0/361); p95 — | 70% (252/361); p95 1.60 | 0% (0/361); p95 — | no |
| main3-2 · S-main3 | Xeon 6975P-C | paced-delete | 265.9 | 1.49 | 44 | 17,618 | 97% (261/269); p95 0.39 | 100% (269/269); p95 0.16 | 97% (261/269); p95 0.32 | **yes** |
| main3-2 · S | Xeon 6975P-C | import | 2.1 | 1.00 | 29 | 0 | 100% (2/2); p95 0.10 | 100% (2/2); p95 0.01 | 100% (2/2); p95 0.09 | **yes** |
| main3-2 · S | Xeon 6975P-C | paced-delete | 178.5 | 1.00 | 3 | 0 | 100% (178/178); p95 0.03 | 100% (178/178); p95 0.01 | 100% (178/178); p95 0.03 | **yes** |
| main3-both-1 · S | Xeon 6975P-C | import+paced-delete | 182.7 | 1.00 | 4 | 0 | 100% (182/182); p95 0.03 | 100% (182/182); p95 0.01 | 100% (182/182); p95 0.03 | **yes** |
| main3-both-1 · S-main3 | Xeon 6975P-C | import+paced-delete | 652.8 | 3.57 | 75 | 39,038 | 40% (262/652); p95 0.39 | 85% (554/652); p95 1.58 | 40% (262/652); p95 0.29 | no |
| main3-live-1 · S | EPYC 9V74 | import | 2.5 | 1.00 | 30 | 0 | 100% (3/3); p95 0.10 | 100% (3/3); p95 0.01 | 100% (3/3); p95 0.07 | **yes** |
| main3-live-1 · S | EPYC 9V74 | paced-delete | 183.8 | 1.00 | 3 | 0 | 100% (183/183); p95 0.03 | 100% (183/183); p95 0.01 | 100% (183/183); p95 0.03 | **yes** |
| main3-live-1 · S-main3-live | EPYC 9V74 | import | 55.1 | 21.87 | 96 | 21,420 | 2% (1/55); p95 1.77 | 98% (54/55); p95 0.65 | 0% (0/55); p95 — | no |
| main3-live-1 · S-main3-live | EPYC 9V74 | paced-delete | 198.0 | 1.08 | 16 | 14,941 | 97% (193/198); p95 0.05 | 100% (198/198); p95 0.02 | 97% (193/198); p95 0.06 | **yes** |
| main3-live-2 · S-main3-live | EPYC 9V74 | import | 56.7 | 21.88 | 95 | 21,420 | 2% (1/57); p95 1.39 | 98% (56/57); p95 0.64 | 0% (0/57); p95 — | no |
| main3-live-2 · S-main3-live | EPYC 9V74 | paced-delete | 201.8 | 1.07 | 16 | 14,941 | 98% (196/201); p95 0.06 | 100% (201/201); p95 0.02 | 98% (196/201); p95 0.07 | **yes** |
| main3-live-2 · S | EPYC 9V74 | import | 2.6 | 1.00 | 31 | 0 | 100% (3/3); p95 0.19 | 100% (3/3); p95 0.03 | 100% (3/3); p95 0.17 | **yes** |
| main3-live-2 · S | EPYC 9V74 | paced-delete | 188.7 | 1.00 | 4 | 0 | 100% (188/188); p95 0.03 | 100% (188/188); p95 0.01 | 100% (188/188); p95 0.03 | **yes** |
| main3-live-both · S | EPYC 9V74 | import+paced-delete | 213.2 | 1.00 | 3 | 0 | 100% (213/213); p95 0.17 | 100% (213/213); p95 0.01 | 100% (213/213); p95 0.21 | **yes** |
| main3-live-both · S-main3-live | EPYC 9V74 | import+paced-delete | 281.5 | 1.32 | 31 | 36,361 | 78% (218/281); p95 0.21 | 100% (280/281); p95 0.33 | 78% (218/281); p95 0.29 | no |

**Paced deletion receipts** (commit to receipt, 2,980 commits each, 50 ms polling):

| Variant | p50 s | p95 s | Max s | Sum s |
| --- | ---: | ---: | ---: | ---: |
| S (four runs) | 0.052 | 0.052–0.10 | 0.06–0.85 | 154–165 alone |
| S-main3 alone | 0.052 | 0.26–0.42 | 8.6–19.6 | 243–348 |
| S-main3, with the import | 0.052 | 0.26 | **298** | — |
| S-main3-live alone | 0.052 | 0.067–0.071 | 7.2–7.5 | — |
| S-main3-live, with the import | 0.052 | 0.10 | **46.8** | — |

## Engine: the subquery shapes that remain

**How many, and on which tables** (engine count; catalog breakdown from `main3-demands.json`):

| | S-main3 | S-main3-live | S |
| --- | ---: | ---: | ---: |
| Shapes | 5,262 | 1,125 | 860 |
| Subquery shapes | **2,816** | **615** | 71 |
| Subquery nodes | 2,244 | 544 | 84 |
| Subquery edges | 26,679 | 4,442 | 103 |
| Contributors | 230,866 | 77,519 | 2,499 |
| `record` (day-window members, three `in`s; Home and recurring) | 926 | 76 | — |
| `graph_edge` (attendee members `in` `record_date_index`; rail, family graph: today's filter) | 767 | 164 | — |
| `record_date_index` (`allRecordDateIndicesForDayWindow`, `recurrenceDateIndicesForDayWindow`) | 470 | 57 | — |
| `assignment` (day-window `in` `record_date_index`; rail: today's filter) | 461 | 126 | — |
| `workflow_run` (keyed run threads, one `workflow` node each) | 44 | 44 | — |
| `resource`, `resource_fact`, `user`, `calendar` (today's filter) | 132 | 132 | 71 |

Every `record_recurrence`, `calendar_event`, `record_assignee_index`, `record_completed_index`,
`message` and `suggestion` demand keys (plain). So does the session `record` feed.

**Membership work per workload** (S-main3 / S-main3-live):

| | Import | Paced deletion | Both |
| --- | --- | --- | --- |
| Subquery contributor change | +7,198 / +7,198 | −6,444 / −5,363 | +754 / +1,835 |
| Subquery edge change | 0 / 0 | 0 / 0 | 0 / 0 |
| Move-in queries (backfills) | 21,420 / 21,420 | 17,618 / 14,941 | 39,038 / 36,361 |
| Engine CPU (one core) | 96–100% / 95–96% | 44–46% / 16% | 75% / 31% |
| `engine_sequencer_held` peak / longest `held_run` | 0 B / 0 s | 0 B / 0 s | 0 B / 0 s |

- **Import move-ins by table:** `graph_edge` 6,958, `record_date_index` 5,402, `record` 4,562,
  `assignment` 2,158, `record_recurrence` 2,100, `resource` and `resource_fact` 120 each.
- **Flips are not counted** by prod's binary. Each flip's query-back is one move-in query, logged
  as `backfill finished`.
- **The sequencer-held gauges measure something else.** `engine_sequencer_held` counts bytes of
  an incomplete transaction the sequencer holds back, and stayed at 0: these stalls are not held
  transactions. The engine's longest stall shows instead as the longest commit-to-receipt wait
  (19.6 s alone, 298 s beside the import) and as the import's whole duration with drains at 0%.

## Can Monday's stall still happen?

Yes, through the day-window group members and the today-filter tables. Main keys a group member
only when its filter reads no other table, but every day-window `record`, `record_date_index`,
`assignment` and attendee `graph_edge` member the app sends reads through `record_date_index` or
`record_recurrence`. `assignment` and `graph_edge` keep today's filters everywhere. A first import
writes exactly those tables, so every inserted row is checked against those subquery shapes and
flips their membership sets (21,420 move-in queries).

- **Live demands only** (615 subquery shapes, as if idle shapes are retired): one first import
  pins the engine for about 55 s, with other families' drains and live changes failing throughout.
- **Every earlier day window kept** (2,816): 6–7.5 minutes, the size of Monday's stall.
- **A paced deletion alone** keeps receipts around 0.05 s with occasional 7–20 s stalls, and loses
  2–9% of drains.
- **A deletion beside a first import:** the deletion's receipts wait behind the import, 47 s
  (live) to 298 s (full), and drains fall to 78% and 40%.

**Under what load:** any unpaced first import (thousands of tracked rows in a few commits) while
day-window groups are held, which every phone does for Home today and tomorrow. How long it lasts
grows with how many day-window subquery members are registered: about 55 s at today's live set,
minutes once earlier windows accumulate. Keying the day-window members (filters without `in`), or
pacing the import, are the levers the next round (the commit-size sweep) measures.
