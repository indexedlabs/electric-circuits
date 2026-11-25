Result: neither variant passes S's line. S-assign-today breaks the stall fix: its import reaches 6.5–10.0 s with drains at 20–33%. S-chat-narrow keeps the import served (1.19–1.44x S) but loses drains on the large and share deletions (2.3–2.8x S).

# S-assign-today and S-chat-narrow, each paired with S

**Runs.** Actions run `45674851412`, harness `7a18647` (workflow job `paired`, `[paired-6]`) on
`profile/stall-repro-2026-11-24`. Engine `b42f70d` (the `pr69` binary, the one S's runs used),
Nov 24 catalog, 1 CPU, all four workloads, with S's share seed loaded as in S's own runs.

- Job `s-variants-1`, EPYC 7763: S, S-assign-today, S-chat-narrow.
- Job `s-variants-2`, Xeon 6975P-C: S-chat-narrow, S-assign-today, S.

Run folders are beside this file (`paired-s-variants-*`, each with `manifest.json`). Code is in
`simple_keys.py`.

**Pass line** (S's): each workload under 10 s, and every bystander drain, subset and live change
succeeds.

**"Today's filter"** is one shape per account and table, built by the gateway with no client
window, as `S-rest-today` builds it. The pinned Indexed source (`da746b7`) gives byte-identical
`assignment`, `message`, `workflow_run`, `suggestion` and `record` filters to indexed main
(`90893c5`), checked by building both.

| Variant | Engine shapes | Subquery shapes |
| --- | ---: | ---: |
| S | 860 | 71 |
| S-assign-today | 841 | 91 |
| S-chat-narrow | 844 | 103 |

## S-assign-today

`assignment` on today's gateway filter (`record_authorization_predicate` lowered onto
`assignment.record_id`) for every account, the importing account included. Everything else is as
S.

| Variant | Runner CPU | Workload | First delivery → empty s | × S (same runner) | CPU % | Backfills | Drain, p95 | Subset, p95 | Live change, p95 | Pass? |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- | --- | --- |
| S | EPYC 7763 | import | 3.5 | 1.00 | 28 | 0 | 100% (4/4); p95 0.22 | 100% (4/4); p95 0.01 | 100% (4/4); p95 0.21 | **yes** |
| S | EPYC 7763 | large-delete | 1.6 | 1.00 | 21 | 0 | 89% (8/9); p95 0.35 | 100% (9/9); p95 0.01 | 89% (8/9); p95 0.27 | no |
| S | EPYC 7763 | small-delete | 0.6 | 1.00 | 21 | 0 | 100% (2/2); p95 0.03 | 100% (2/2); p95 0.01 | 100% (2/2); p95 0.03 | **yes** |
| S | EPYC 7763 | share-delete | 2.5 | 1.00 | 61 | 1,888 | 100% (3/3); p95 0.77 | 100% (3/3); p95 0.02 | 100% (3/3); p95 0.68 | **yes** |
| S-assign-today | EPYC 7763 | import | 10.0 | 2.82 | 83 | 2,100 | 20% (2/10); p95 0.81 | 100% (10/10); p95 0.08 | 30% (3/10); p95 1.64 | no |
| S-assign-today | EPYC 7763 | large-delete | 4.8 | 2.98 | 37 | 1,523 | 67% (8/12); p95 0.76 | 100% (12/12); p95 0.04 | 67% (8/12); p95 0.66 | no |
| S-assign-today | EPYC 7763 | small-delete | 5.2 | 9.39 | 71 | 2,101 | 43% (3/7); p95 0.38 | 100% (7/7); p95 0.02 | 43% (3/7); p95 0.30 | no |
| S-assign-today | EPYC 7763 | share-delete | 7.1 | 2.81 | 78 | 5,392 | 38% (3/8); p95 0.34 | 100% (8/8); p95 0.02 | 38% (3/8); p95 0.32 | no |
| S | Xeon 6975P-C | import | 2.0 | 1.00 | 33 | 0 | 100% (2/2); p95 0.22 | 100% (2/2); p95 0.01 | 100% (2/2); p95 0.20 | **yes** |
| S | Xeon 6975P-C | large-delete | 1.1 | 1.00 | 30 | 0 | 100% (4/4); p95 0.09 | 100% (4/4); p95 0.01 | 100% (4/4); p95 0.08 | **yes** |
| S | Xeon 6975P-C | small-delete | 0.3 | 1.00 | 29 | 0 | 100% (1/1); p95 0.18 | 100% (1/1); p95 0.02 | 100% (1/1); p95 0.12 | **yes** |
| S | Xeon 6975P-C | share-delete | 1.4 | 1.00 | 71 | 1,888 | 100% (1/1); p95 0.98 | 100% (1/1); p95 0.01 | 100% (1/1); p95 0.85 | **yes** |
| S-assign-today | Xeon 6975P-C | import | 6.5 | 3.30 | 85 | 2,100 | 33% (2/6); p95 0.97 | 100% (6/6); p95 0.04 | 33% (2/6); p95 0.80 | no |
| S-assign-today | Xeon 6975P-C | large-delete | 2.7 | 2.50 | 49 | 1,523 | 60% (3/5); p95 0.03 | 100% (5/5); p95 0.01 | 80% (4/5); p95 0.87 | no |
| S-assign-today | Xeon 6975P-C | small-delete | 3.2 | 10.41 | 81 | 2,101 | 50% (2/4); p95 0.14 | 100% (4/4); p95 0.06 | 50% (2/4); p95 0.12 | no |
| S-assign-today | Xeon 6975P-C | share-delete | 4.1 | 3.02 | 82 | 5,392 | 50% (2/4); p95 0.55 | 100% (4/4); p95 0.05 | 50% (2/4); p95 0.47 | no |

**Fails on both runners, every workload.** The import takes 10.0 s on EPYC 7763 and 6.5 s on Xeon
6975P-C (2.8–3.3x S), with drains at 20–33% and live changes at 30–33%. The deletions take
2.5–3.0x S and the small deletion 9–10x, with drains at 38–67%.

Every import backfill (2,100) is an `assignment` walk. Today's assignment filter registers
`record` membership nodes, which the import's Record writes flip. **Cost against S:** about 3x on
the import and the deletions, and about 10x on the small deletion.

## S-chat-narrow

Chat keyed only where indexed main keys it: `_keyed_chat_demand` over `KEYED_CHAT_COLLECTIONS =
{message, suggestion, workflow_run}` in `api/src/api/services/circuits_sync.py` and
`circuits_sync_authority.py` at `90893c5`.

- `suggestion` is keyed for any demand without a subquery.
- `message` is keyed only for a thread's target pair (`record`/`composer_submission` with
  `target_id`, no `client_id`) or one message id.
- `workflow_run` is keyed only for one thread's runs.

Every synthetic `message` and `workflow_run` demand is a `created_at`-windowed history read that
names no thread, so they all keep today's filter (16 shapes each). `suggestion` stays keyed as in
S. The importing account holds no chat feeds, as in S. Everything else is as S.

| Variant | Runner CPU | Workload | First delivery → empty s | × S (same runner) | CPU % | Backfills | Drain, p95 | Subset, p95 | Live change, p95 | Pass? |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- | --- | --- |
| S | EPYC 7763 | import | 3.5 | 1.00 | 28 | 0 | 100% (4/4); p95 0.22 | 100% (4/4); p95 0.01 | 100% (4/4); p95 0.21 | **yes** |
| S | EPYC 7763 | large-delete | 1.6 | 1.00 | 21 | 0 | 89% (8/9); p95 0.35 | 100% (9/9); p95 0.01 | 89% (8/9); p95 0.27 | no |
| S | EPYC 7763 | small-delete | 0.6 | 1.00 | 21 | 0 | 100% (2/2); p95 0.03 | 100% (2/2); p95 0.01 | 100% (2/2); p95 0.03 | **yes** |
| S | EPYC 7763 | share-delete | 2.5 | 1.00 | 61 | 1,888 | 100% (3/3); p95 0.77 | 100% (3/3); p95 0.02 | 100% (3/3); p95 0.68 | **yes** |
| S-chat-narrow | EPYC 7763 | import | 4.2 | 1.19 | 70 | 0 | 100% (4/4); p95 0.90 | 100% (4/4); p95 0.28 | 100% (4/4); p95 0.80 | **yes** |
| S-chat-narrow | EPYC 7763 | large-delete | 4.1 | 2.57 | 38 | 73 | 73% (8/11); p95 0.98 | 100% (11/11); p95 0.07 | 73% (8/11); p95 0.81 | no |
| S-chat-narrow | EPYC 7763 | small-delete | 0.8 | 1.39 | 32 | 1 | 100% (2/2); p95 0.03 | 100% (2/2); p95 0.01 | 100% (2/2); p95 0.03 | **yes** |
| S-chat-narrow | EPYC 7763 | share-delete | 5.9 | 2.30 | 78 | 5,261 | 43% (3/7); p95 0.03 | 100% (7/7); p95 0.04 | 57% (4/7); p95 0.96 | no |
| S | Xeon 6975P-C | import | 2.0 | 1.00 | 33 | 0 | 100% (2/2); p95 0.22 | 100% (2/2); p95 0.01 | 100% (2/2); p95 0.20 | **yes** |
| S | Xeon 6975P-C | large-delete | 1.1 | 1.00 | 30 | 0 | 100% (4/4); p95 0.09 | 100% (4/4); p95 0.01 | 100% (4/4); p95 0.08 | **yes** |
| S | Xeon 6975P-C | small-delete | 0.3 | 1.00 | 29 | 0 | 100% (1/1); p95 0.18 | 100% (1/1); p95 0.02 | 100% (1/1); p95 0.12 | **yes** |
| S | Xeon 6975P-C | share-delete | 1.4 | 1.00 | 71 | 1,888 | 100% (1/1); p95 0.98 | 100% (1/1); p95 0.01 | 100% (1/1); p95 0.85 | **yes** |
| S-chat-narrow | Xeon 6975P-C | import | 2.8 | 1.44 | 75 | 0 | 100% (3/3); p95 1.04 | 100% (3/3); p95 0.25 | 100% (3/3); p95 0.99 | **yes** |
| S-chat-narrow | Xeon 6975P-C | large-delete | 3.0 | 2.71 | 52 | 73 | 80% (4/5); p95 1.00 | 100% (5/5); p95 0.03 | 80% (4/5); p95 0.87 | no |
| S-chat-narrow | Xeon 6975P-C | small-delete | 0.7 | 2.26 | 39 | 1 | 100% (1/1); p95 0.59 | 100% (1/1); p95 0.03 | 100% (1/1); p95 0.41 | **yes** |
| S-chat-narrow | Xeon 6975P-C | share-delete | 3.8 | 2.81 | 83 | 5,261 | 50% (2/4); p95 0.34 | 100% (4/4); p95 0.02 | 50% (2/4); p95 0.20 | no |

**Import and small deletion pass on both runners**: import 4.2 and 2.8 s (1.19–1.44x S) with every
drain and live change served, and no backfills.

**Large and share deletions fail on both runners.** The large deletion takes 3.0–4.1 s (2.6–2.7x
S), with drains 73–80%. The share deletion takes 3.8–5.9 s (2.3–2.8x S), with drains 43–50%.

The deletions pay for today's chat filters through their `record` subquery nodes: the large
deletion walks `message`, `workflow` and `workflow_run` (73 backfills). The share deletion adds
843 each on `message` and `workflow`, and 1,687 on `record`, on top of S's 1,888.

**Cost against S:** about 1.2–1.4x on the import and 2.3–2.8x on deletions.

**Not measured:** thread-named chat demands. The synthetic catalog has none, because the logged
history it is built from records no thread scopes, so this variant keys no `message` or
`workflow_run` feed at all. Where prod's phones send mostly thread-named demands, more of chat
would be keyed than here.
