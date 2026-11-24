# Variant B-cold: what the proposal now keeps as subquery scopes, to measure on the root-cause harness

Spec as received from orchestrator `circuits-fix-proposal` (via `circuits-rollout`), Nov 24, in
answer to circuits-rollout's question after milestone 3 (B-retained: 148 s, drains 0 of 148).
Copied verbatim below; results in `results/RESULTS.md` cite this file. How the harness reads
each line is in "Harness reading" at the end.

## The decision this tests

Every table whose access filter today runs through a table that bulk writers touch gets a plain key. A shape may keep a subquery only when its inner table is a share, grant or membership table. Those tables are changed by small, paced writers, not by imports or erasures of content.

So B-cold is B with these scopes kept as subqueries, and everything else keyed as in B.

## Keyed (plain equality), as in B

- `record`, `record_date_index`, `record_recurrence`, `record_assignee_index`, `record_completed_index`, `assignment`: `space_id = S` with a reader-class condition (home readers: not `hidden`; partners: `live` only).
- `calendar_event`: owner `user_id = U`, partner `calendar_id = C`, with today's busy-block and cancelled-exception conditions and `is_partner_mirror = false`.

## Newly keyed in B-cold (kept as subqueries in B-retained)

- `message`: `space_id = S AND target_type = 'record'`, where `space_id` is the target Record's Space, written on the row. A pending composer message adds a per-account scope, `user_id = <caller> AND <pending condition>`. A Space-targeted message read by id stays `id = X`.
- `workflow`: `space_id = S AND record_id IS NOT NULL`.
- `workflow_run`: `space_id = S`, with `space_id` copied from its workflow (new column in the harness).
- `suggestion`: `space_id = S AND status = 'pending' AND removed_at IS NULL`.
- `graph_edge`: `from_space_id = S AND to_space_id = T` for each pair of Spaces the account reads (S = T for same-Space edges), plus `from_space_id = S AND edge_type = 'has_attendee'` for attendee edges whose other end may be unreadable.

If the harness cannot carry the reader class on these tables, leave it out; it does not change the cost.

## Kept as subquery scopes in B-cold (inner tables are share, grant or membership only)

- Direct Record shares, per recipient, on the six Record-family tables: `record_id IN (SELECT record_id FROM record_share WHERE target_type = 'user' AND target_id = <caller> AND deleted_at IS NULL)` (`id IN (...)` on `record`).
- `file`: `user_id = <caller> OR id IN (SELECT file_id FROM file_share WHERE target_space_id IN <caller's Space list>)`.
- `user`: `id = <caller> OR id IN (SELECT user_id FROM space_member WHERE space_id IN <caller's Space list>)`, plus today's standing-grant participant branches.
- `resource`, `resource_fact`: `space_id IN <caller's Space list>` (plain) OR `id IN (SELECT resource_id FROM resource_share WHERE target_id = <caller> AND deleted_at IS NULL)` (`resource_id IN (...)` on `resource_fact`).
- `standing_grant`, `standing_grant_backfill_status`, `monitored_url_subscription`, `monitored_url_source`: as today.

## Runs

1. The three workloads as in milestone 3: first calendar import (13,081 changes), large deletion (29,793), small deletion (7,114).
2. One extra worst case for the kept subqueries: a deletion commit that also deletes one account's `record_share`, `file_share` and `space_member` rows, about 1,000 of each, in one transaction.

## Pass

- Each workload finishes in under 10 s from first delivery to empty queue (B took 3.2 s, 1.8 s and 0.3 s).
- The second account's drains, subset queries and live changes all succeed for the whole run.
- Report CPU and the per-row membership share, as in milestone 2.

## If it fails

Variant (a), each exception class keyed one at a time, names the class that carries the cost; the proposal then keys that class as well. Variant (b), a separate lane for the exceptions, is not preferred. On one CPU they would still burn the same core, and receipts would have to be scoped to keep drains clear. Run it only if B-cold fails and a class cannot be keyed.

## Amendment after the Astra re-review (Nov 24, about 10:45 UTC)

Measure the predicates as they would really be:

- **Chat rail:** no server scope. The phone computes it from rows it already holds, so leave the `chat_record_rail` scope out of B-cold.
- **Monitored sources:** keep today's filter over subscriptions and Resources, as listed above.
- **`message`, `suggestion`:** keep today's row conditions as plain conditions on the keyed shape: `deleted_at IS NULL`, the typed target, and for the deck `status = 'pending' AND removed_at IS NULL`.
- **`graph_edge`:** key only `has_attendee` and `has_subject` edges, by the Record's Space. Keep every other edge branch on today's filter, so the run shows whether those branches carry cost.
- **`workflow`:** not published as a feed. It stays only as the dependency of `workflow_run`, keyed by its Workflow's Record's Space.

B-cold had not started when this amendment arrived, so only the amended version is measured.

## Harness reading (how `catalog.py` `cold_bodies` builds each shape)

Where the spec leaves a choice, these are the readings used.

- **One shape per account per scope.** Today's catalog has date-windowed historical requests; like B, every account gets one shape per scope, deduplicated (847 shapes for the Nov 24 catalog's accounts, against 10,563 requests today).
- **Spaces and the reader class.** Every synthetic account reads its own Space only, so a "caller's Space list" is one Space. The reader class is `live` on every keyed shape.
- **Record family.** Keyed `audience_key = S AND reader_class = 'live'` (B's body), plus the direct-share subquery per account on `record_share`.
- **`calendar_event`.** B's body: one keyed shape per calendar (15).
- **`message`.** Two plain shapes:
  - Record chats: `audience_key = S`, `reader_class = 'live'`, `deleted_at IS NULL`, `target_type = 'record'`.
  - The pending composer: `user_id = caller AND deleted_at IS NULL AND target_type = 'composer_submission'`. Today's composer branch is a subquery on `composer_submission`; here it is a plain row condition, which is the reading of "pending condition" used.
- **`suggestion`.** Keyed, plus `deleted_at IS NULL AND status = 'pending' AND removed_at IS NULL`.
- **`workflow_run`.** Keyed. The writer stores the Space as `audience_key`, which stands for "copied from its workflow". `workflow` is not in the synthetic catalog as a feed, today or here.
- **`graph_edge`.** Two shapes:
  - A plain keyed shape: `audience_key = S`, `reader_class = 'live'`, `from_type = 'record'`, `to_type = 'resource'`, `edge_type IN (has_attendee, has_subject)`.
  - Today's gateway filter with only its `has_attendee`/`has_subject` branch removed: four branches stay, two of them subqueries through `record` and `resource`.
- **`file`, `user`, `standing_grant`.** Today's gateway filter, which already has the spec's form: `file` is the owner or a `file_share` to the caller's Space; `user` is self, `space_member` of the caller's Space, or standing-grant participants.
- **`resource`, `resource_fact`.** `deleted_at IS NULL AND (space_id = S OR id/resource_id IN resource_share(target_id = caller, deleted_at IS NULL))`.
- **Not in the catalog.** `chat_record_rail` does not exist in the pinned Indexed source (`da746b7`) or in the synthetic catalog, so leaving it out changes nothing. Monitored sources and `standing_grant_backfill_status` are not in today's synthetic catalog either; the pinned gateway cannot build the monitored-source filter (`monitored_source_authorization_predicate` is missing from the bundle).
- **Share seed (B-cold only).** Every background account shares 20 Records, 20 Files and 20 Resources with other accounts. Account 17 (`SHARE_ACCOUNT`) has 1,000 `record_share`, 1,000 `file_share` and 100 `resource_share` rows, plus 1,000 extra `space_member` rows: synthetic people of account 17 in every other account's Space, the probe account's included. Those people have `user` rows, which stay when the memberships go.
- **Extra run `share-delete`.** One transaction deletes all of account 17's rows: its own content, the share rows, all 1,001 `space_member` rows and its own `user` row.
- **Comparisons.** B runs first on the same runner as B-cold's reference. The pass line is absolute: each workload under 10 s, and every probe succeeds.
