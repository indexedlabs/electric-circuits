# W6: the list rule against the gateway's literal filters (Main, via circuits-engine-watch)

**Answer.** Yes, at every size measured. With the list rule on, the gateway's literal filters
cost what plain keys cost, and they stay correct through the restart and the access changes.

| Keyed `graph_edge` shapes | 1,000-edge commit, rule on | Rule off (control) | Shapes whose rows differ from Postgres |
| ---: | ---: | ---: | --- |
| 100 | 0.18 s (0.17–0.36) | 0.35 s (0.16–0.37) | 0 of 100, both cells |
| 1,000 | 0.21 s (0.20–0.39) | 1.32 s (1.31–1.32) | 0 of 1,000, both cells |
| 5,000 | 0.19 s (0.17–0.36) | 11.0 s (8.9–13.4) | 0 of 4,995, both cells |

- **Cost:** with the rule on the commit does not grow with the number of filters. 0.17–0.39 s is
  the floor a 1,000-row commit has with nothing extra to check: PLAIN-WORDS-3 measured 0.16–0.22 s
  for plain `record` keys (run 1) and 0.17–0.29 s for edge filters with a selective equality
  written first (run 2). With the rule off the same filters cost 11 s at 5,000 and the bystander
  loses 116 of 138 drains; with it on, none.
- **Restart, rule on:** the engine is ready again in 11.2 s with all 5,001 shapes back from its
  catalog and the same number filed as before (5,000 filed, 0 on the scan list). The first
  1,000-edge commit after it takes 0.31 s.
- **Access changes, rule on:** 10 accounts gain a partner Space and 10 lose theirs, each
  re-subscribed with its new OR, and 100 edges change Space. The commit after them takes 0.31 s.
- **Correct through both:** at the end of each 5,000 cell, 0 of 4,995 live shapes hold rows that
  differ from a direct Postgres query of their filter. The moved edges both left shapes (98 rails
  still hold a row; the rails on the moved Records hold none) and entered shapes (78 rails of the
  39 partnered accounts that kept their partner Space each gained one; the 10 that lost it gained
  none).
- **Released feeds stay in the engine in this harness:** all 115 old feeds were released (every
  release answered 200), yet the engine's count went from 5,000 to 5,110. The harness runs with
  the subscription lease and idle retirement off, so nothing retires a released shape here.

**What the engine does not expose (measurement stopped there, engine not patched).** Engine main
says, per table, how many standalone shapes it holds and how many of them are on the scan list
(`GET /tables/{table}/families`: `standalone`, `scan`). It does not say which key or keys a
standalone shape is filed under, or how many shapes share a key. So:

- **From the engine:** the totals. All shapes are filed and none is on the scan list, in both
  cells. Rule off, they are filed too, under one equality every account shares, which is why the
  control is slow without anything being on the scan list.
- **From the harness's own copy of the rule** (`simple_keys.access_plan`, a Python copy of
  `CompiledPredicate::access_plan`; read, not verified against the engine shape by shape): the
  key columns each filter is filed under, and the largest bucket.
- **What it needs:** an engine endpoint that lists `StandaloneIndex.placed` per shape and the
  number of shapes under each key (`apps/engine/src/engine/executors.rs`, read).

Engine electric-circuits `46d15563` (main) in both cells, 1 CPU. Actions run `45977881934`,
harness `ec8e045d`, on branch `profile/w6-list-keys` (the stall-repro branch with origin/main
merged in). Folders `paired-w6-*` beside this file; tables by `tools/stall-repro/w6_report.py`.

## The setup

- **Filters, exactly as the gateway writes them** (`simple_keys.w6_account_filters`): main's
  `keyed_authorization_predicate(table="graph_edge")` (`_keyed_graph_edge_predicate`, `_any_equal`),
  then the phone's filter, then the stream identity, in `authorize_predicate`'s order. No
  unwrapping: one Space is a one-item OR inside another.
- **Accounts:** 19 in 20 have one Space. 1 in 20 has its own home, one partner Space (two for
  every other such account) and one Record shared with it.
- **Shapes per account,** as PLAIN-WORDS-3 run 2: one session Resource-edge feed per Space it
  reads, and four thread rails' attendee members. 100 shapes are 20 accounts, 1,000 are 198 and
  5,000 are 990. A partnered account's first two rails are on the Records whose edges the access
  round moves into its partner Space.
- **The commit:** 1,000 `graph_edge` updates, 20 in each of 50 Spaces, one transaction, three
  times on one engine. The first 49 one-Space accounts live in written Spaces and hold rails on
  Records whose edges change.
- **The two cells** run back to back on one runner and differ only in
  `ELECTRIC_CIRCUITS_LIST_KEYS`.
- **The final check** (`w6-check`): these are changes-only feeds, so each live shape's delivered
  changes are replayed and the rows it ends up holding are compared with a direct Postgres query
  of its filter over the 1,000 edges the run changed.
- **The 5,000 runs** add, at the same points in both cells: an engine restart
  (`systemctl restart` of the engine's unit), then a round of access changes. 10 one-Space
  accounts gain a partner Space and 10 partnered accounts lose theirs, each re-subscribed with
  its new filters and its old feeds released; and 2 edges in each written Space move to the next
  written Space.

## The cells

### ON: `ELECTRIC_CIRCUITS_LIST_KEYS=1`

| Keyed `graph_edge` shapes | Runner | Sequencer time, 3 commits: median (range) s | Per row | Per row per shape | Engine CPU % | Peak RSS | Bystander receipts (drains ok/attempts) | Bystander live changes | Engine: shapes filed under a key | Harness's copy of the rule: filed under | Largest key bucket (harness's copy) | Final check: shapes whose rows differ from Postgres |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | --- | --- | --- |
| 100 | EPYC 9V74 | 0.18 (0.17–0.36) | 0.18 ms | 1.84 µs | 18 | 46 MiB | 8/8; p95 0.35 s, max 0.63 s | 8/8; p95 0.42 s, max 0.61 s | 100 filed, 0 on the scan list (of 100) | `from_id` 79, `from_space_id+to_space_id` 20, `from_id+from_space_id+partner_hidden+to_space_id` 1 | `from_id = 0ef534d0…`: 2 shapes | 0 of 100 (76 hold rows, 76 rows) |
| 1,000 | EPYC 7763 | 0.21 (0.20–0.39) | 0.21 ms | 0.21 µs | 26 | 91 MiB | 9/9; p95 0.06 s, max 0.35 s | 9/9; p95 0.07 s, max 0.32 s | 1,000 filed, 0 on the scan list (of 1,000) | `from_id` 789, `from_space_id+to_space_id` 211 | `from_id = c9eb65f8…`: 3 shapes | 0 of 1,000 (196 hold rows, 196 rows) |
| 5,000 | EPYC 9V74 | 0.19 (0.17–0.36) | 0.19 ms | 0.04 µs | 29 | 286 MiB | 7/7; p95 0.05 s, max 0.35 s | 7/7; p95 0.06 s, max 0.24 s | 5,000 filed, 0 on the scan list (of 5,000) | `from_id` 3,931, `from_space_id+to_space_id` 1,069 | `from_space_id = 11cea902…`: 11 shapes | 0 of 4,995 (176 hold rows, 176 rows) |

The 5,000 run's restart and access changes (paired-w6-5000-1-S-main3-w6-5000-on-main46d-1cpu):

| Step | What happened | 1,000-edge commit after it (s) | Bystander receipts in that commit | Engine: shapes filed under a key | Harness's copy: filed under |
| --- | --- | ---: | --- | --- | --- |
| Before | 5,001 shapes | 0.19 (0.17–0.36) | 7/7; p95 0.05 s, max 0.35 s | 5,000 filed, 0 on the scan list (of 5,000) | `from_id` 3,931, `from_space_id+to_space_id` 1,069 |
| Engine restart | ready again in 11.2 s; 5,001 shapes back from the catalog | 0.31 | 3/3; p95 0.18 s, max 0.56 s | 5,000 filed, 0 on the scan list (of 5,000) | `from_id` 3,931, `from_space_id+to_space_id` 1,069 |
| Access changes | 10 accounts gain a partner Space and 10 lose theirs (new feeds made; 115 old feeds released, 115 answered 200); 100 edges change Space | 0.31 | 3/3; p95 0.09 s, max 0.34 s | 5,110 filed, 0 on the scan list (of 5,110) | `from_id` 3,913, `from_space_id+to_space_id` 1,062, `from_id+from_space_id+to_space_id` 20 |

Final check after both: 0 of 4,995 shapes differ from Postgres (176 hold rows, 176 rows expected, 976 changes replayed).

### OFF: the setting unset (control)

| Keyed `graph_edge` shapes | Runner | Sequencer time, 3 commits: median (range) s | Per row | Per row per shape | Engine CPU % | Peak RSS | Bystander receipts (drains ok/attempts) | Bystander live changes | Engine: shapes filed under a key | Harness's copy of the rule: filed under | Largest key bucket (harness's copy) | Final check: shapes whose rows differ from Postgres |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | --- | --- | --- |
| 100 | EPYC 9V74 | 0.35 (0.16–0.37) | 0.35 ms | 3.52 µs | 24 | 46 MiB | 8/8; p95 0.21 s, max 0.60 s | 8/8; p95 0.20 s, max 0.45 s | 100 filed, 0 on the scan list (of 100) | `from_type` 79, `to_type` 21 | `from_type = 'record'`: 79 shapes | 0 of 100 (76 hold rows, 76 rows) |
| 1,000 | EPYC 7763 | 1.32 (1.31–1.32) | 1.32 ms | 1.32 µs | 66 | 114 MiB | 21/21; p95 1.24 s, max 1.48 s | 21/21; p95 1.11 s, max 1.34 s | 1,000 filed, 0 on the scan list (of 1,000) | `from_type` 789, `to_type` 211 | `from_type = 'record'`: 789 shapes | 0 of 1,000 (196 hold rows, 196 rows) |
| 5,000 | EPYC 9V74 | 11.01 (8.93–13.36) | 11.01 ms | 2.20 µs | 95 | 282 MiB | 22/138; p95 1.92 s, max 1.98 s | 1/138; p95 0.03 s, max 0.03 s | 5,000 filed, 0 on the scan list (of 5,000) | `from_type` 3,941, `to_type` 1,059 | `from_type = 'record'`: 3,941 shapes | 0 of 4,995 (176 hold rows, 176 rows) |

The 5,000 run's restart and access changes (paired-w6-5000-2-S-main3-w6-5000-off-main46d-1cpu):

| Step | What happened | 1,000-edge commit after it (s) | Bystander receipts in that commit | Engine: shapes filed under a key | Harness's copy: filed under |
| --- | --- | ---: | --- | --- | --- |
| Before | 5,001 shapes | 11.01 (8.93–13.36) | 22/138; p95 1.92 s, max 1.98 s | 5,000 filed, 0 on the scan list (of 5,000) | `from_type` 3,941, `to_type` 1,059 |
| Engine restart | ready again in 10.2 s; 5,001 shapes back from the catalog | 13.51 | 7/55; p95 1.61 s, max 1.86 s | 5,000 filed, 0 on the scan list (of 5,000) | `from_type` 3,941, `to_type` 1,059 |
| Access changes | 10 accounts gain a partner Space and 10 lose theirs (new feeds made; 115 old feeds released, 115 answered 200); 100 edges change Space | 12.25 | 5/50; p95 1.57 s, max 1.83 s | 5,110 filed, 0 on the scan list (of 5,110) | `from_type` 3,941, `to_type` 1,054 |

Final check after both: 0 of 4,995 shapes differ from Postgres (176 hold rows, 176 rows expected, 976 changes replayed).

