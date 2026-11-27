# Plain-words sync, round 3: three measurements (circuits-engine-watch and Main)

Engine `9bc1563` (prod's binary), 1 CPU, GitHub-hosted runners. Lowering is indexed main's own
gateway code from the harness's pinned copy (`52e79b875b`). `_any_equal`,
`keyed_authorization_predicate`'s `record` branch, `_keyed_graph_edge_predicate` and
`_member_space_edges` are the same text there as on main `8fe2aadd`.

Every commit cell is three 1,000-row commits on one engine; the tables give the median and the
range. The bystander's probe ticks every 0.25 s here (1 s in AUDIT-1), so sub-second commits have
bystander samples. "During the commit" is from the writer's SQL start to the commit's receipt.
Tables are printed by `tools/stall-repro/plain_words_report.py`.

| Run | Actions run | Harness | Run folders |
| --- | --- | --- | --- |
| 1 | `45917533904` | `6532584` | `paired-pw1-*` |

## Run 1: the 1,000-row `record` write, with one-family filters written as a plain test

**Answer.** The fix does what it claims for the accounts it covers, but it does not make prod's mix
flat.

- **Every account one-family, fixed:** the commit takes about 0.2 s at every size, 40 to 20,000
  filters (0.16, 0.21, 0.21, 0.20 s). Bystander receipts all arrive, p95 0.03–0.10 s.
- **Prod's mix (19 in 20 fixed, 1 in 20 still on the scan list):** the commit grows with the
  accounts the fix does not cover. It is 0.21 s at 1,000 accounts, 0.87 s at 10,000 and 1.71 s at
  20,000. It stays under 0.5 s up to about 5,000 accounts (about 250 filters left on the scan
  list), and passes 2 s at about 23,000.
- **Against H1:** at 10,000 filters 0.87 s against 16.8 s, at 20,000 1.71 s against 41.4 s, and
  no bystander drain is lost where H1 lost all of them.
- **So:** the gateway fix alone covers the 19 in 20. The other 1 in 20 still cost about 1.5 µs per
  changed row each (H1's rate), so they need their own answer before about 5,000 accounts.

**What I changed in the harness's lowering (hypothetical: main does not do this).** One rewrite,
`simple_keys.unwrap_single_or`, applied to the filter main's `authorize_predicate` returns: every
`{"or": [x]}` with exactly one child becomes `x`, at every level. Main's `_any_equal` writes a
one-value list as a one-child OR, and `keyed_authorization_predicate` wraps its paths in another,
so this one rewrite is both halves of the fix. A one-family account's filter goes from

`AND(AND(OR(OR(space_id = X)), type test), stream identity)` to
`AND(AND(space_id = X, type test), stream identity)`,

which the engine indexes under `space_id` (`simple_keys.access_leaf` mirrors the engine's rule, and
`test_simple_keys.PlainWordsTests` checks the placement). The rewrite changes no row a filter
admits. It is applied to every account; an account with several Spaces or shares keeps a
two-child OR and stays on the scan list.

**Setup** (`S-main3-h1p-N-*`, `simple_keys.h1p_catalog`).

- **The commit:** H1's. 1,000 `record` updates, 20 in each of 50 Spaces, one transaction.
- **(a) one-family accounts:** N accounts, each with one Space, no partner Spaces and no shared
  Records. Each has its own home Space, so the N filters differ. 49 of the homes are Spaces the
  commit writes to, and the bystander's is the 50th, so every changed row matches one filter.
  Two members of one family with the same lists send the same filter, which the engine folds
  into one shape.
- **(b) prod's mix:** 19 accounts in 20 as in (a). 1 in 20 keeps H1's lists: its own home, 0–2
  of the 50 written Spaces and two shared Records.
- **Control:** (a)'s accounts with main's lowering as it is today, on the same runner. Only the
  lowering differs from (a).
- **(c) H1's old numbers** are from other runners, one commit each, with a 1 s probe. H1's
  accounts are not (a)'s: each reads 1–3 Spaces drawn from the 50 written ones, so at 20,000
  filters each changed row matched about 400 of them. That is why (c) is above the control.

| Keyed `record` shapes | Accounts and lowering | Runner | Sequencer time, 3 commits: median (range) s | Per row | Engine CPU % | Peak RSS | Bystander receipts (drains ok/attempts) | Bystander live changes | Under 0.5 s |
| ---: | --- | --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| 40 | (a) every account one-family, fix | EPYC 9V74 | 0.16 (0.14–0.17) | 0.16 ms | 26 | 43 MiB | 6/6; p95 0.10 s, max 0.38 s | 6/6; p95 0.10 s, max 0.35 s | yes |
| 40 | (b) prod mix, fix | EPYC 9V74 | 0.16 (0.15–0.17) | 0.16 ms | 25 | 42 MiB | 6/6; p95 0.11 s, max 0.38 s | 6/6; p95 0.11 s, max 0.35 s | yes |
| 40 | control: (a)'s accounts, today's lowering | EPYC 9V74 | 0.17 (0.16–0.33) | 0.17 ms | 25 | 65 MiB | 8/8; p95 0.46 s, max 0.78 s | 8/8; p95 0.44 s, max 0.69 s | yes |
| 40 | (c) H1, Nov 26 | Xeon 8573C | 0.15 | 0.15 ms | 25 | 41 MiB | none fell in the commit | none fell in the commit | yes |
| 1,000 | (a) every account one-family, fix | EPYC 7763 | 0.21 (0.20–0.22) | 0.21 ms | 26 | 52 MiB | 6/6; p95 0.03 s, max 0.21 s | 6/6; p95 0.17 s, max 0.42 s | yes |
| 1,000 | (b) prod mix, fix | EPYC 7763 | 0.21 (0.21–0.39) | 0.21 ms | 33 | 53 MiB | 7/7; p95 0.03 s, max 0.36 s | 7/7; p95 0.27 s, max 0.52 s | yes |
| 1,000 | control: (a)'s accounts, today's lowering | EPYC 7763 | 0.45 (0.43–0.46) | 0.45 ms | 42 | 52 MiB | 9/9; p95 0.16 s, max 0.43 s | 9/9; p95 0.40 s, max 0.65 s | yes |
| 1,000 | (c) H1, Nov 26 | EPYC 9V74 | 1.83 | 1.83 ms | 45 | 274 MiB | 2/2; p95 0.19 s, max 1.21 s | 2/2; p95 0.17 s, max 1.17 s | no |
| 10,000 | (a) every account one-family, fix | EPYC 7763 | 0.21 (0.20–0.22) | 0.21 ms | 30 | 144 MiB | 7/7; p95 0.04 s, max 0.20 s | 7/7; p95 0.19 s, max 0.44 s | yes |
| 10,000 | (b) prod mix, fix | EPYC 7763 | 0.87 (0.85–0.89) | 0.87 ms | 58 | 266 MiB | 15/15; p95 0.74 s, max 1.03 s | 15/15; p95 0.90 s, max 1.15 s | no |
| 10,000 | control: (a)'s accounts, today's lowering | EPYC 7763 | 3.43 (3.39–3.73) | 3.43 ms | 89 | 168 MiB | 27/47; p95 1.69 s, max 1.92 s | 25/47; p95 1.76 s, max 1.99 s | no |
| 10,000 | (c) H1, Nov 26 | Xeon 8370C | 16.83 | 16.83 ms | 85 | 2,265 MiB | 0/17 | 0/17 | no |
| 20,000 | (a) every account one-family, fix | EPYC 7763 | 0.20 (0.20–0.20) | 0.20 ms | 47 | 239 MiB | 9/9; p95 0.05 s, max 0.38 s | 9/9; p95 0.25 s, max 0.49 s | yes |
| 20,000 | (b) prod mix, fix | EPYC 7763 | 1.71 (1.69–1.76) | 1.71 ms | 74 | 465 MiB | 27/27; p95 1.72 s, max 1.85 s | 25/27; p95 1.70 s, max 1.95 s | no |
| 20,000 | control: (a)'s accounts, today's lowering | EPYC 7763 | 13.98 (13.87–16.43) | 13.98 ms | 97 | 284 MiB | 8/182; p95 1.74 s, max 1.97 s | 0/182 | no |
| 20,000 | (c) H1, Nov 26 | Xeon 6975P-C | 41.40 | 41.40 ms | 60 | 4,478 MiB | 0/41 | 0/41 | no |

**Reading.**

- **The floor:** about 0.15–0.2 s is what a 1,000-row commit costs with nothing on the scan
  list. That is (a) at every size, and the 40-filter cells of every lowering.
- **The mix's slope:** from 1,000 to 20,000 accounts, (b) adds 1.5 s for 950 more scan-list
  filters, about 1.5 µs per changed row per filter. The 0.5 s and 2 s account counts are read off
  that line.
- **The control's slope:** a one-Space filter left on the scan list costs 0.25–0.7 µs per row
  (0.45 s at 1,000, 3.4 s at 10,000, 14 s at 20,000), a third of H1's rate. H1's filters are
  bigger (a Space list and a share list) and match more rows.
- **Other readers in (b):** every drain arrives up to 20,000 accounts, but at 10,000 and 20,000
  they wait for the commit (p95 0.74 s and 1.72 s against the 2 s limit, and 2 of 27 live changes
  lost at 20,000).
- **Memory:** peak RSS is 43–239 MiB for (a) and 465 MiB for (b) at 20,000, against H1's
  4.5 GiB.
- The sequencer-held gauges stayed at 0, and there were no move-in queries.
