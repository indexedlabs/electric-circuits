# Coverage audit, round 1 (circuits-coord and Main)

Engine `9bc1563` (prod's binary) everywhere. Every number is a single run on a GitHub-hosted
runner; same-runner noise is about 5% on long runs. The bystander probe ticks once a second, so a
commit that finishes in under a second may have no bystander request inside it, and the tables say
so instead of reporting a receipt.

Runs:

| Actions run | Harness | Contents |
| --- | --- | --- |
| `45879502778` | `91a68b2` | H2, reruns 1–3 |
| `45883673774` | `453acfb` | `resource_share`, merge and Files reruns |
| `45885330921` | `dae3ea0` | H1 |

Run folders are `paired-h1b-*`, `paired-h2-*`, `paired-r1-*`, `paired-r2-*` and `paired-r3-*`
beside this file.

## H3: the drain probe and the authority settings

The harness's numbers stand. Its drain probe (`probes.py:49-69`) is the drain-on path prod runs,
and neither `SYNC_AUTHORITY_*` setting is read on the lowering path the harness uses. `SUMMARY.md`
now says, under "Not modelled", that the gateway's own Postgres authority reads and rechecks
(single read off) are not modelled: they load the database, not the engine.

## H4: shapes per table in the keyed catalogs

**Neither "Prototype" nor "Everything keyed" names a run of mine.** The closest matches:

- **"Prototype":** B, which M3 calls the audience-key prototype (all plain, keyed on
  `audience_key`).
- **"Everything keyed":** S-main3-keyed-all, main's lowering with `graph_edge`, `assignment` and
  the day-window `record`/`record_date_index` members keyed too.

All of them, plus catalog A (the Nov 24 fixture), S and S-main3, are below.

**Placement rule.** It mirrors `CompiledPredicate::access_leaf` (`predicate.rs:369-416` at
`9bc1563`). A plain shape is **indexed** when its top-level AND chain (nested ANDs included) has an
equality or range leaf on a non-NULL value. Otherwise it is on the **scan list**: a top-level OR,
NOT, IN list, `!=`, LIKE, IS NULL, or no filter.

The engine's compile does not collapse a one-child OR. So main's keyed filters, which always wrap
even one Space in an OR (`_any_equal`), stay on the scan list unless the demand adds its own
equality or range leaf.

**How the counts were made.** They are computed offline from the catalogs, rebuilt with the code
each run used (`h4_catalog_shapes.py`). Catalog A is exact: the fixture's own deterministic
requests. The other catalogs read stand-in account lists (the runs read theirs from the database),
so their totals can differ slightly from the engine's counts:

| Catalog | Engine count | Offline count |
| --- | ---: | ---: |
| S | 860 | 866 |
| S-main3 | 5,262 | 5,235 |
| S-main3-keyed-all | 3,374 | 3,352 |

Each engine count includes the import account's and the probe's feeds.

| Catalog | Shapes (offline requests) | Plain, indexed | Plain, scan list | Subquery |
| --- | ---: | ---: | ---: | ---: |
| A, Nov 24 fixture (today's gateway) | 10,563 | 4,883 | 1,401 | 4,279 |
| B (M3's audience-key prototype, all-plain) | 704 | 704 | 0 | 0 |
| S (simplified plain keys) | 866 | 739 | 57 | 70 |
| S-main3 (main's lowering, keyed flags on) | 5,235 | 2,157 | 278 | 2,800 |
| S-main3-keyed-all (graph_edge, assignment, day-window record/rdi keyed too) | 3,352 | 2,390 | 786 | 176 |

**Catalog A (Nov 24), per outer table** (offline, `h4_catalog_shapes.py detail`):

| Outer table | Plain, indexed (leaf column: shapes) | Plain, scan list | Subquery | Inner tables the subquery shapes reach into |
| --- | --- | ---: | ---: | --- |
| `activity` | 583 (`is_hidden`: 583) | 0 | 0 | — |
| `assignment` | 0 (—) | 0 | 351 | `record`, `record_date_index`, `record_share`, `space`, `standing_grant` |
| `assistant_state` | 311 (`source_type`: 311) | 0 | 0 | — |
| `calendar` | 379 (`user_id`: 379) | 0 | 0 | — |
| `calendar_event` | 1,905 (`is_busy_block`: 1905) | 0 | 483 | `calendar`, `partner_calendar_event_mirror`, `standing_grant` |
| `external_account` | 412 (`user_id`: 379, `id`: 33) | 0 | 0 | — |
| `feedback` | 311 (`user_id`: 311) | 0 | 0 | — |
| `file` | 0 (—) | 0 | 44 | `file_share` |
| `graph_edge` | 0 (—) | 0 | 702 | `record`, `record_date_index`, `record_share`, `resource`, `resource_share`, `space`, `standing_grant` |
| `invite` | 0 (—) | 360 | 0 | — |
| `message` | 0 (—) | 0 | 44 | `composer_submission`, `record`, `record_share`, `space`, `standing_grant` |
| `record` | 0 (—) | 0 | 1,185 | `record`, `record_date_index`, `record_share`, `space`, `standing_grant` |
| `record_assignee_index` | 0 (—) | 0 | 44 | `record`, `record_date_index`, `record_share`, `space`, `standing_grant` |
| `record_completed_index` | 0 (—) | 0 | 44 | `record`, `record_date_index`, `record_share`, `space`, `standing_grant` |
| `record_date_index` | 0 (—) | 0 | 439 | `record`, `record_date_index`, `record_share`, `space`, `standing_grant` |
| `record_recurrence` | 0 (—) | 0 | 395 | `record`, `record_date_index`, `record_share`, `space`, `standing_grant` |
| `resource` | 0 (—) | 0 | 285 | `resource`, `space`, `standing_grant` |
| `resource_fact` | 0 (—) | 0 | 175 | `resource`, `space`, `standing_grant` |
| `space` | 0 (—) | 331 | 0 | — |
| `space_member` | 0 (—) | 486 | 0 | — |
| `standing_grant` | 0 (—) | 224 | 0 | — |
| `suggestion` | 982 (`status`: 982) | 0 | 0 | — |
| `user` | 0 (—) | 0 | 44 | `space_member`, `standing_grant` |
| `workflow_run` | 0 (—) | 0 | 44 | `record`, `record_share`, `space`, `standing_grant`, `workflow` |
| **Total** | **4,883** | **1,401** | **4,279** | |

**Catalog A (Nov 24), binds per inner table** (engine `GET /subqueries` after warmup, run `paired-h2-a-1-A-prod9bc-1cpu`):

| Inner table | Subquery nodes | Edges (references) | Contributors (distinct values) |
| --- | ---: | ---: | ---: |
| `record` | 600 | 18,592 | 136,344 |
| `record_date_index` | 507 | 1,609 | 85,400 |
| `resource` | 81 | 4,036 | 15,200 |
| `standing_grant` | 54 | 609 | 53 |
| `space` | 41 | 300 | 58 |
| `record_share` | 20 | 60 | 0 |
| `resource_share` | 20 | 20 | 0 |
| `calendar` | 18 | 481 | 270 |
| `composer_submission` | 16 | 44 | 0 |
| `file_share` | 16 | 44 | 0 |
| `space_member` | 16 | 44 | 16 |
| `workflow` | 16 | 44 | 0 |
| `partner_calendar_event_mirror` | 1 | 484 | 0 |
| **Total** | **1,406** | **26,367** | **237,341** |

**S, per outer table** (offline, `h4_catalog_shapes.py detail`):

| Outer table | Plain, indexed (leaf column: shapes) | Plain, scan list | Subquery | Inner tables the subquery shapes reach into |
| --- | --- | ---: | ---: | --- |
| `activity` | 19 (`audience_key`: 19) | 0 | 0 | — |
| `assignment` | 38 (`space_id`: 38) | 0 | 0 | — |
| `assistant_state` | 19 (`audience_key`: 19) | 0 | 0 | — |
| `calendar` | 19 (`audience_key`: 19) | 0 | 0 | — |
| `calendar_event` | 304 (`calendar_id`: 285, `is_busy_block`: 19) | 0 | 0 | — |
| `external_account` | 19 (`audience_key`: 19) | 0 | 0 | — |
| `feedback` | 19 (`audience_key`: 19) | 0 | 0 | — |
| `file` | 0 (—) | 0 | 16 | `file_share` |
| `graph_edge` | 0 (—) | 38 | 0 | — |
| `invite` | 19 (`audience_key`: 19) | 0 | 0 | — |
| `message` | 32 (`audience_key`: 16, `target_type`: 16) | 0 | 0 | — |
| `record` | 38 (`space_id`: 38) | 0 | 0 | — |
| `record_assignee_index` | 32 (`space_id`: 32) | 0 | 0 | — |
| `record_completed_index` | 32 (`space_id`: 32) | 0 | 0 | — |
| `record_date_index` | 38 (`space_id`: 38) | 0 | 0 | — |
| `record_recurrence` | 38 (`space_id`: 38) | 0 | 0 | — |
| `resource` | 0 (—) | 0 | 19 | `resource_share` |
| `resource_fact` | 0 (—) | 0 | 19 | `resource_share` |
| `space` | 19 (`audience_key`: 19) | 0 | 0 | — |
| `space_member` | 19 (`audience_key`: 19) | 0 | 0 | — |
| `standing_grant` | 0 (—) | 19 | 0 | — |
| `suggestion` | 19 (`audience_key`: 19) | 0 | 0 | — |
| `user` | 0 (—) | 0 | 16 | `space_member`, `standing_grant` |
| `workflow_run` | 16 (`audience_key`: 16) | 0 | 0 | — |
| **Total** | **739** | **57** | **70** | |

**S, binds per inner table** (engine `GET /subqueries` after warmup, run `paired-r1-noresource-1-S-prod9bc-1cpu`):

| Inner table | Subquery nodes | Edges (references) | Contributors (distinct values) |
| --- | ---: | ---: | ---: |
| `standing_grant` | 32 | 32 | 32 |
| `resource_share` | 20 | 39 | 460 |
| `file_share` | 16 | 16 | 1,147 |
| `space_member` | 16 | 16 | 860 |
| **Total** | **84** | **103** | **2,499** |

**S-main3-keyed-all, binds per inner table** (engine `GET /subqueries` after warmup, run `paired-r1-noresource-2-S-main3-keyed-all-prod9bc-1cpu`):

| Inner table | Subquery nodes | Edges (references) | Contributors (distinct values) |
| --- | ---: | ---: | ---: |
| `resource` | 60 | 138 | 9,500 |
| `standing_grant` | 58 | 78 | 56 |
| `workflow` | 44 | 44 | 0 |
| `space` | 21 | 60 | 39 |
| `space_member` | 20 | 20 | 1,019 |
| **Total** | **203** | **340** | **10,614** |

**Against prod's 7-day peak** (10,660 shapes at 04:35:15 UTC Nov 24: 6,269 plain, 4,358 subquery,
33 family, 1,408 nodes, 25,576 edges), catalog A in the engine:

| | Catalog A | Prod peak |
| --- | ---: | ---: |
| Shapes | 10,571 | 10,660 |
| Subquery shapes | 4,287 | 4,358 |
| Nodes | 1,406 | 1,408 |
| Edges | 26,367 | 25,576 |
| Family shapes | 0 | 33 |
| Plain (standalone) | 6,284 | 6,269 |

Of A's plain shapes, 4,883 are indexed. They are mostly `calendar_event` on `is_busy_block`, then
`suggestion` on `status`, `activity` on `is_hidden`, and the account tables on `user_id`. The other
1,401 are on the scan list (`invite`, `space`, `space_member`, `standing_grant`).

**Group members against standalone streams:** the fixture doesn't record this. Catalog A's demands
are all changes-only feeds, synthesized from the logged shape counts. S-main3's day-window and
thread groups exist only as tags in `main3-demands.json`, and the harness creates group members as
feeds too.

## H1: a 1,000-row `record` commit against 40 to 20,000 keyed `record` shapes

**Answer.** Whole-table keyed `record` feeds sit on the scan list, and their cost grows about
linearly with their number:

- **Per row:** about 1.8 ms per 1,000 shapes, roughly 1.7–2 µs per row per shape.
- **The commit:** 0.15 s at 40 shapes, 1.8 s at 1,000, 16.8 s at 10,000 and 41 s at 20,000.
- **Other readers:** from 10,000 shapes they time out for the whole commit (drains 0/17 and 0/41).
- **The index:** giving half of the 10,000 shapes a client `id = X` equality, which the engine
  indexes, halves the time (9.0 s), but still loses 7 of 9 drains.
- **Memory:** peak RSS grows with the shape count, to 4.5 GiB at 20,000.

**Setup** (`S-main3-h1-*`, `simple_keys.h1_catalog`):

- **Shapes:** N keyed `record` feeds, lowered by main's `authorize_predicate` with a keyed authority
  (`keyed_authorization_predicate`, `52e79b875b`), one whole-table feed per synthetic account.
- **Each account's lists:** its own home Space (unique), 0–2 of the 50 Spaces that hold Records
  (about 1–3 Spaces in all), and two Records shared with it alone. So every filter differs; the
  engine folds identical filters into one shape, which a first attempt with too few distinct
  filters showed.
- **The commit:** 1,000 `record` updates, 20 in each of 50 Spaces (the 19 fixture accounts plus 31
  added ones), in one transaction.
- **The shape cap:** raised to 25,000 for these cells only.
- **Catalog:** these shapes and the bystander's probe feed only.
- **The indexed cell:** a Record has no `record_id` column, so the client equality is on its own
  key, `id = X`.

| Keyed `record` shapes | Engine shapes (with probe) | Runner | Sequencer time for the commit (s) | Per row | Engine CPU % | Peak RSS | Bystander during the commit |
| --- | ---: | --- | ---: | ---: | ---: | ---: | --- |
| 40 | 41 | Xeon 8573C | 0.15 | 0.15 ms | 25 | 41 MiB | no bystander request fell in the commit (1 s probe) |
| 1,000 | 1,001 | EPYC 9V74 | 1.83 | 1.83 ms | 45 | 274 MiB | drains 2/2, live 2/2; drain p95 0.19 s, max 1.21 s |
| 10,000 | 10,001 | Xeon 8370C | 16.83 | 16.83 ms | 85 | 2,265 MiB | drains 0/17, live 0/17 |
| 20,000 | 20,001 | Xeon 6975P-C | 41.40 | 41.40 ms | 60 | 4,478 MiB | drains 0/41, live 0/41 |
| 10,000 (half indexed) | 10,001 | Xeon 8370C | 9.03 | 9.03 ms | 69 | 1,252 MiB | drains 2/9, live 2/9; drain p95 0.96 s, max 1.80 s |

The sequencer-held gauges stayed at 0, and there were no move-in queries (no subquery shapes).
Runners differ across cells, so compare cells by their order of magnitude.

## H2: 1,000-row commits to six cold tables, on catalog A

**Answer.**

- **`record_share` is the hot one.** On catalog A, a 1,000-row share insert stalls the engine for
  55 s: 33,553 membership walks across the Record family, and the bystander's drains and live
  changes 0 of 55.
- **The other cold tables are cheap:**
  - `space_member` (1.9 s, 843 `user` walks), `resource_share` (1.0 s, 1,000 `resource` walks),
    `file_share` (0.6 s, 300 `file` walks): 2 s or less.
  - `suggestion` and `tag`: no subquery reaches them.
  - The `space_member` flips: 5 walks in about 0.3 s.

**Setup.**

- **Catalog A** (Nov 24, today's gateway, pinned Indexed source), 1 CPU.
- **Commit:** each is one transaction of 1,000 inserts for account 3.
- **Binds:** prod's bind counts are taken from A in the engine (`GET /subqueries` after warmup).
  A's runs don't seed shares, so A's `record_share`, `resource_share` and `file_share` nodes start
  with no values. The 1,000 inserted rows fill them at once, which is why `record_share` walks
  every Record-family table: each share adds a Record to its recipient's share node, and every
  Record-family shape of that recipient walks it.
- **Shares:** `resource_share` rows use one Resource each, because the table's active-provenance
  key allows one live direct share per Resource and target.
- **The flip cell:** account 3's user added to, then removed from, the Spaces of accounts 4–8. In A,
  `record`, `assignment`, `graph_edge` and `file` filters take the Space list as literals, so only
  the `user` shapes depend on `space_member`.

| Commit | Runner | Binds on that table (A) | Sequencer time (s) | Walk drain time (s) and walks | Another account during the commit | CPU % |
| --- | --- | --- | ---: | --- | --- | ---: |
| `space_member` insert, 1,000 rows | EPYC 9V74 | 16 nodes, 44 references, 16 values | 1.90 | 1.38 (843 walks: user 843) | drains 2/2, live 2/2; drain p95 0.10 s | 99 |
| `file_share` insert, 1,000 rows | EPYC 9V74 | 16 nodes, 44 references, 0 values | 0.62 | 0.33 (300 walks: file 300) | no bystander request fell in the commit (1 s probe) | 95 |
| `resource_share` insert, 1,000 rows | EPYC 9V45 | 20 nodes, 20 references, 0 values | 1.04 | 0.85 (1,000 walks: resource 1,000) | drains 1/1, live 1/1; drain p95 0.18 s | 95 |
| `record_share` insert, 1,000 rows | Xeon 8370C | 20 nodes, 60 references, 0 values | 55.28 | 53.59 (33,553 walks: record 11,447, graph_edge 6,429, record_date_index 4,786, record_recurrence 3,573, assignment 3,358, record_completed_index 1,188, record_assignee_index 1,188, message 792, workflow 792) | drains 0/55, live 0/55 (none succeeded) | 106 |
| `suggestion` insert, 1,000 rows | Xeon 8370C | 0 nodes, 0 references, 0 values | 2.25 | 0.00 (0 walks: none) | drains 2/2, live 2/2; drain p95 0.70 s | 82 |
| `tag` insert, 1,000 rows | Xeon 8370C | 0 nodes, 0 references, 0 values | 0.13 | 0.00 (0 walks: none) | no bystander request fell in the commit (1 s probe) | 75 |
| `space_member` add: account 3 into or out of 5 Spaces | EPYC 7763 | 16 nodes, 44 references, 16 values | 0.34 | 0.01 (5 walks: user 5) | no bystander request fell in the commit (1 s probe) | 72 |
| `space_member` remove: account 3 into or out of 5 Spaces | EPYC 7763 | 16 nodes, 44 references, 16 values | 0.12 | 0.01 (5 walks: user 5) | no bystander request fell in the commit (1 s probe) | 109 |

"Walk drain time" is first to last `backfill finished` in the engine log during the commit.

## Rerun 1: `sweep-keyed-all` unpaced without the fixture's `resource` rows

**Answer.**

- **The run:** without the 40 `resource` rows per commit, the keyed-all unpaced import takes
  2.80 s with zero move-in queries, and every bystander probe succeeds (3 of 3). With them it took
  5.4 s, with 240 move-ins and drains 2 of 6. So those rows caused the
  remaining loss.
- **Which is right:** neither "none" nor 240. Main binds one calendar Resource per synced calendar
  when calendars connect (`bind_calendar_resource` calls `ensure_calendar_resource`,
  `api/src/api/models/calendar/__init__.py:757-780` at `52e79b875b`). That is about 15 here, and it
  matches the incident trace's `resource` insert 10 / update 10. The fixture's 240 is too many, and
  the audit's "none" is too few.

| Variant | Runner | Workload | First delivery → empty s | CPU % | Backfills | Drain | Live change | Subset | Receipt p95 / max s |
| --- | --- | --- | ---: | ---: | ---: | --- | --- | --- | --- |
| S | EPYC 9V45 | import-noresource | 1.73 | 33 | 0 | 2/2; p95 0.02 | 2/2; p95 0.02 | 2/2 | — |
| keyed-all, no `resource` rows | EPYC 9V45 | import-noresource | 2.80 | 70 | 0 | 3/3; p95 0.76 | 3/3; p95 0.69 | 3/3 | — |

12,841 changes: the 13,081 import less its 240 `resource` rows.

## Rerun 2: S-main3-live with the `file` and `standing_grant_backfill_status` feeds

**Answer.** Adding them changes nothing material: the first import is 21× S on its runner (20.9×,
against 21–22× for S-main3-live). Its engine subquery count grows from 615 to 637.

| Workload | S-main3-live-plus | S-main3-live |
| --- | --- | --- |
| Import | 73 s, drains 1 of 73 | 55–57 s on another CPU model |
| Paced deletion | 215 s against S's 191 s, drains 96%, longest receipt 10.6 s | 198–202 s against S's 184–189 s, drains 96–98%, longest receipt 7.2–7.5 s |
| Import plus deletion | 288 s, drains 72%, longest receipt 60 s | 282 s, drains 78%, longest receipt 47 s |

- **`file`:** a nil-predicate feed for the session on every account (the shell's TierThree
  inventory, `RecordRailCircuitsSubscription.inventoryTables`).
- **`standing_grant_backfill_status`:** a nil-predicate feed on the Settings family screen
  (`SettingsFamilyView.swift:455`), on one account in ten.
- Both are lowered by main.

| Variant | Runner | Workload | First delivery → empty s | CPU % | Backfills | Drain | Live change | Subset | Receipt p95 / max s |
| --- | --- | --- | ---: | ---: | ---: | --- | --- | --- | --- |
| S | EPYC 7763 | import | 3.49 | 29 | 0 | 4/4; p95 0.18 | 4/4; p95 0.15 | 4/4 | — |
| S-main3-live-plus | EPYC 7763 | import | 72.82 | 97 | 21,420 | 1/73; p95 1.98 | 1/73; p95 1.87 | 71/73 | — |
| S | EPYC 7763 | paced-delete | 191.25 | 4 | 0 | 191/191; p95 0.03 | 191/191; p95 0.03 | 191/191 | 0.05 / 0.06 |
| S-main3-live-plus | EPYC 7763 | paced-delete | 214.70 | 20 | 14,941 | 205/214; p95 0.06 | 205/214; p95 0.06 | 214/214 | 0.08 / 10.61 |
| S | EPYC 7763 | import+paced-delete | 191.49 | 4 | 0 | 191/191; p95 0.03 | 191/191; p95 0.03 | 191/191 | 0.05 / 0.35 |
| S-main3-live-plus | EPYC 7763 | import+paced-delete | 287.53 | 39 | 36,361 | 206/287; p95 0.08 | 206/287; p95 0.07 | 286/287 | 0.08 / 59.91 |

## Rerun 3: cold-table commits as their writers make them

**Answer.** None stalls the engine:

- **Under 1.2 s:** the `space_member` subtree rewrite (30 rows, 0.20 s, 60 `user` walks), the
  global `user` update (1,020 rows, 0.56–1.19 s) and the 500-File soft delete (0.59 s).
- **The Resource merge:** 0.19 s on S, but 3.9 s on S-main3-live-plus, where it walks 810
  `graph_edge`, 297 `resource` and 198 `resource_fact` query-backs and loses one of three drains.

**How each was written.**

- **Subtree rewrite:** 30 `space_member` rows seeded in Space 5, moved to Space 6 in one update.
- **Global `user` update:** `updated_at` on every user row (1,020, the share seed's people
  included).
- **Resource merge:** 99 of account 5's Resources marked superseded by its first. 99 facts of
  accounts 5 and 6 moved onto that Resource and soft-deleted in the same update. A live fact is
  unique per (Resource, fact type), so a merge has to absorb duplicates; a first attempt that only
  moved them hit that key.
- **Files:** 500 of account 6's Files soft-deleted.

| Commit | Variant | Runner | Rows | Sequencer time (s) | Longest stall: walk window (s) | Walks | Other readers during the commit | Drain timeouts |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- | ---: |
| writer-subtree | S | EPYC 9V74 | 30 | 0.20 | 0.03 | 60 | no bystander request fell in the commit (1 s probe) | 0 |
| writer-subtree | S-main3-live-plus | EPYC 9V74 | 30 | 0.20 | 0.03 | 60 | no bystander request fell in the commit (1 s probe) | 0 |
| writer-user | S | EPYC 9V74 | 1020 | 0.56 | 0.00 | 0 | no bystander request fell in the commit (1 s probe) | 0 |
| writer-user | S-main3-live-plus | EPYC 9V74 | 1020 | 1.19 | 0.00 | 0 | drains 1/1, live 1/1; drain p95 0.17 s | 0 |
| writer-merge | S | EPYC 7763 | 198 | 0.19 | 0.00 | 0 | no bystander request fell in the commit (1 s probe) | 0 |
| writer-merge | S-main3-live-plus | EPYC 7763 | 198 | 3.86 | 3.69 | 1,305 | drains 2/3, live 2/3; drain p95 1.02 s | 1 |
| writer-files | S | EPYC 7763 | 500 | 0.59 | 0.00 | 0 | no bystander request fell in the commit (1 s probe) | 0 |
| writer-files | S-main3-live-plus | EPYC 7763 | 500 | 0.59 | 0.00 | 0 | no bystander request fell in the commit (1 s probe) | 0 |
