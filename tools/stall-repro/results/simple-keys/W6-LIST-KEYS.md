# W6: the list rule against the gateway's literal filters (Main, via circuits-engine-watch)

**Answer so far (100 and 1,000 filters; both 5,000 cells are running again).** Yes at both
sizes: with the list rule on, the gateway's literal filters cost what plain keys cost, and every
shape's rows match Postgres.

- **Rule on:** a 1,000-edge commit takes 0.16 s at 100 filters and 0.18 s at 1,000, which is the
  floor a 1,000-row commit has with nothing to scan (PLAIN-WORDS-3 run 1 and run 2's
  selective-equality cells: 0.16–0.22 s).
- **Rule off, same engine, same runner:** 0.30 s at 100 and 1.17 s at 1,000, and a bystander's
  receipt p95 goes from 0.17 s to 1.17 s.
- **Correct at both sizes, both cells:** 0 of 100 and 0 of 1,000 shapes differ from Postgres.
- **5,000, the restart and the access changes:** not reported yet. The first pass of both 5,000
  cells stopped at the access-change step on a harness mistake (the edge move hit a foreign key on
  a Space the fixture never creates) before the final check; it is fixed and they are running.

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

Engine electric-circuits `46d15563` (main) in both cells, 1 CPU. Actions run `45973476075`,
harness `80c80bfd`, on branch `profile/w6-list-keys` (the stall-repro branch with origin/main
merged in). Folders `paired-w6-*` beside this file; tables by `tools/stall-repro/w6_report.py`.

## The setup

- **Filters, exactly as the gateway writes them** (`simple_keys.w6_account_filters`): main's
  `keyed_authorization_predicate(table="graph_edge")` (`_keyed_graph_edge_predicate`, `_any_equal`),
  then the phone's filter, then the stream identity, in `authorize_predicate`'s order. No
  unwrapping: one Space is a one-item OR inside another.
- **Accounts:** 19 in 20 have one Space. 1 in 20 has its own home, one partner Space (two for
  every other such account) and one Record shared with it.
- **Shapes per account,** as PLAIN-WORDS-3 run 2: one session Resource-edge feed per Space it
  reads, and four thread rails' attendee members. 100 shapes are 20 accounts, 1,000 are 198.
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
| 100 | EPYC 9V74 | 0.16 (0.15–0.19) | 0.16 ms | 1.63 µs | 21 | 47 MiB | 6/6; p95 0.19 s, max 0.39 s | 6/6; p95 0.13 s, max 0.37 s | 100 filed, 0 on the scan list (of 100) | `from_id` 79, `from_space_id+to_space_id` 20, `from_id+from_space_id+partner_hidden+to_space_id` 1 | `from_space_id = 2b55ad11…`: 2 shapes | 0 of 100 (76 hold rows, 76 rows) |
| 1,000 | Xeon 6973P-C | 0.18 (0.17–0.19) | 0.18 ms | 0.18 µs | 22 | 90 MiB | 6/6; p95 0.17 s, max 0.37 s | 6/6; p95 0.11 s, max 0.35 s | 1,000 filed, 0 on the scan list (of 1,000) | `from_id` 789, `from_space_id+to_space_id` 211 | `from_space_id = 11cea902…`: 3 shapes | 0 of 1,000 (196 hold rows, 196 rows) |

### OFF: the setting unset (control)

| Keyed `graph_edge` shapes | Runner | Sequencer time, 3 commits: median (range) s | Per row | Per row per shape | Engine CPU % | Peak RSS | Bystander receipts (drains ok/attempts) | Bystander live changes | Engine: shapes filed under a key | Harness's copy of the rule: filed under | Largest key bucket (harness's copy) | Final check: shapes whose rows differ from Postgres |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | --- | --- | --- |
| 100 | EPYC 9V74 | 0.30 (0.16–0.34) | 0.30 ms | 2.99 µs | 25 | 46 MiB | 8/8; p95 0.40 s, max 0.60 s | 8/8; p95 0.16 s, max 0.41 s | 100 filed, 0 on the scan list (of 100) | `from_type` 79, `to_type` 21 | `from_type = 'record'`: 79 shapes | 0 of 100 (76 hold rows, 76 rows) |
| 1,000 | Xeon 6973P-C | 1.17 (1.15–1.26) | 1.17 ms | 1.17 µs | 65 | 107 MiB | 18/18; p95 1.17 s, max 1.42 s | 18/18; p95 1.10 s, max 1.35 s | 1,000 filed, 0 on the scan list (of 1,000) | `from_type` 789, `to_type` 211 | `from_type = 'record'`: 789 shapes | 0 of 1,000 (196 hold rows, 196 rows) |

