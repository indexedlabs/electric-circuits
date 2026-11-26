Result: with only the chat demands phones send, keyed as main lowers them, S-chat-thread costs the same as S: import 0.99x, large deletion 1.04–1.29x, share deletion 1.00x, with zero backfills of its own. It loses exactly S's bystander probes: one drain and one live change of 9 on every large deletion, for both S and S-chat-thread. So it clears chat's share of the stall but inherits S's strict-line miss on the large deletion.

# S-chat-thread, paired with S

**Runs.** Actions run `45761924550`, harness `8fdf7db` (`[paired-7]`) on
`profile/stall-repro-2026-11-24`. Engine `b42f70d` (the `pr69` binary, as in every S run), Nov 24
catalog, 1 CPU, with S's share seed loaded as in S's runs. Workloads: import, large deletion and
share deletion. Two jobs, each on an EPYC 7763 runner:

- `s-chat-thread-1`: S, then S-chat-thread.
- `s-chat-thread-2`: S-chat-thread, then S.

Run folders are beside this file (`paired-s-chat-thread-*`). Code is `chat_thread_bodies` and
`keyed_message_authorization` in `simple_keys.py`.

**Pass line** (S's): each workload under 10 s, and every bystander drain, subset and live change
succeeds.

**What changes from S.** Chat is keyed as indexed main lowers thread-named demands (`2cd606c5`):

- **Record thread:** the phone's `target_type = 'record' AND target_id = X`, ANDed with main's
  keyed `message` filter: `deleted_at IS NULL` and either a Record target with
  `space_id IN (own and partner Spaces) OR target_id IN (shared Records)`, or a composer target
  with `user_id` = caller.
- **Single Message id:** `id = M`, with the same filter plus main's Space-posted branch for that id.
- **Run thread:** the phone's `workflow_id IN (workflow WHERE record_id = X)`, with main's keyed
  Workflow filter injected (`record_id IS NOT NULL AND space_id IN (Spaces)`), and the run's Space
  on the `audience_key` column S's data writes. It keeps one `workflow` node per thread, as main's
  does.
- **Cards** (`suggestion`) stay as S keys them.

**Mapping.** The fixture's 44 `message` and 44 `workflow_run` demands (accounts 0–15, 9 down to 1
per account) each become one phone demand on the same account, in order. The i-th `message`
demand becomes the Record thread of the account's i-th Record, and every third one a
single-Message-id demand (29 threads, 15 by id). The i-th `workflow_run` demand becomes that
Record's run thread.

**Catalog:** S has 860 engine shapes (71 with subqueries); S-chat-thread has 900 (115: the 44 run
threads are the extra subquery shapes).

| Variant | Runner CPU | Workload | First delivery → empty s | × S (same runner) | CPU % | Backfills | Drain, p95 | Subset, p95 | Live change, p95 | Pass? |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- | --- | --- |
| S | EPYC 7763 | import | 3.4 | 1.00 | 28 | 0 | 100% (4/4); p95 0.15 | 100% (4/4); p95 0.06 | 100% (4/4); p95 0.14 | **yes** |
| S | EPYC 7763 | large-delete | 1.5 | 1.00 | 20 | 0 | 89% (8/9); p95 0.18 | 100% (9/9); p95 0.01 | 89% (8/9); p95 0.15 | no |
| S | EPYC 7763 | share-delete | 2.5 | 1.00 | 61 | 1,888 | 100% (3/3); p95 0.77 | 100% (3/3); p95 0.02 | 100% (3/3); p95 0.59 | **yes** |
| S-chat-thread | EPYC 7763 | import | 3.4 | 1.00 | 28 | 0 | 100% (4/4); p95 0.13 | 100% (4/4); p95 0.04 | 100% (4/4); p95 0.11 | **yes** |
| S-chat-thread | EPYC 7763 | large-delete | 2.0 | 1.28 | 23 | 0 | 89% (8/9); p95 0.36 | 100% (9/9); p95 0.01 | 89% (8/9); p95 0.36 | no |
| S-chat-thread | EPYC 7763 | share-delete | 2.5 | 1.00 | 60 | 1,888 | 100% (3/3); p95 0.78 | 100% (3/3); p95 0.02 | 100% (3/3); p95 0.64 | **yes** |
| S-chat-thread | EPYC 7763 | import | 3.4 | 0.99 | 28 | 0 | 100% (4/4); p95 0.18 | 100% (4/4); p95 0.09 | 100% (4/4); p95 0.18 | **yes** |
| S-chat-thread | EPYC 7763 | large-delete | 1.7 | 1.05 | 23 | 0 | 89% (8/9); p95 0.35 | 100% (9/9); p95 0.03 | 89% (8/9); p95 0.23 | no |
| S-chat-thread | EPYC 7763 | share-delete | 2.6 | 1.00 | 60 | 1,888 | 100% (3/3); p95 0.76 | 100% (3/3); p95 0.01 | 100% (3/3); p95 0.60 | **yes** |
| S | EPYC 7763 | import | 3.5 | 1.00 | 29 | 0 | 100% (4/4); p95 0.15 | 100% (4/4); p95 0.06 | 100% (4/4); p95 0.14 | **yes** |
| S | EPYC 7763 | large-delete | 1.6 | 1.00 | 21 | 0 | 89% (8/9); p95 0.18 | 100% (9/9); p95 0.01 | 89% (8/9); p95 0.18 | no |
| S | EPYC 7763 | share-delete | 2.6 | 1.00 | 61 | 1,888 | 67% (2/3); p95 0.57 | 100% (3/3); p95 0.01 | 67% (2/3); p95 0.54 | no |

**Against the pass line:**

- **Import:** S-chat-thread passes on both runners, at 0.99x S.
- **Large deletion:** it fails strictly on both runners, losing one drain and one live change of
  9, exactly as S does in the same jobs. That is S's known loss, not chat's.
- **Share deletion:** it passes on both runners (S lost one of three drains in job 2).

All of S-chat-thread's backfills are S's own: 1,888 on the share deletion from the kept `file`,
`user` and `resource` filters, and none from chat. Cost against S is 0.99x on the import,
1.04–1.29x on the large deletion and 1.00x on the share deletion.

**Not modelled:** the gateway's per-read thread check (`_validate_exact_chat_scope`, outside the
engine), and chat rows for the 16 demanding accounts (only account 18 holds Messages and
Workflows in the fixture, and it sends no chat demands).
