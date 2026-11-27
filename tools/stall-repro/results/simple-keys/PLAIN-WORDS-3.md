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
| 2 | `45917699001`, repeats `45919688237` | `b4b6b41`, `644272b` | `paired-pw2-*` |
| 3 | `45920764926` | `eea63a7` | `paired-pw3-*` |

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

## Run 2: the same for links (`graph_edge`)

**Answer.** Run 1's fix does nothing for links. A 1,000-link write passes 0.5 s at about 100
accounts and 2 s at about 340–450 accounts, with or without it.

- **Today:** 0.30 s at 100 filters (20 accounts), 0.78 s at 1,000 (199 accounts) and 4.7–7.6 s at
  5,000 (991 accounts). At 5,000 the bystander loses more than half its drains.
- **With the plain-test fix:** the same: 0.35 s, 0.67 s and 5.5–9.0 s. In the paired repeats it
  is 10–20% faster than today at 5,000, because the filters are a little smaller. No filter moves.
- **What would index them (hypothetical, more than the fix):** a selective equality written
  first. Then the commit is 0.17 s, 0.22 s and 0.29 s, every drain arrives, and the 1 in 20
  accounts left as they are set the slope.
- **Against the old estimate** (about 2 s at around 200 accounts): 2 s comes at about 340–450
  accounts, because an edge filter costs 0.6–1.5 µs per changed row, not `record`'s 2 µs. The
  0.5 s line comes at about 100.

**Which edge filters the fix can and can't make indexable.** None of them, selectively. The engine
takes the first `=` in the top-level AND, and no filter has a Space or Record equality there:

| Edge filter | Leaf today | With run 1's fix | What would index it |
| --- | --- | --- | --- |
| Main's edge authorization alone (`_keyed_graph_edge_predicate`) | none: `AND(deleted_at IS NULL, detached_at IS NULL, OR(six branches))` | none. Each branch's Space test becomes a plain `=`, but the six branches stay an OR: five read `from_space_id` and one (`bidirectional` Resource edges) reads `to_space_id` | two filters, one per end |
| Session Resource-edge feed, one per Space | `to_type = 'resource'`, from the phone's filter | the same | two filters, one per end |
| Thread rail's attendee member | `from_type = 'record'`, from the phone's filter | the same. The phone's `OR(from_id = R)` becomes `from_id = R`, but `from_type = 'record'` is written before it | `from_id = R` written first |
| Day-window attendee member | not keyed: it looks into `record_date_index`, so main keeps today's subquery filter | not applicable | — |

So today's keyed edge filters are not on the scan list. They are indexed under a value every
account shares, which costs the same: a changed attendee edge (`record` to `resource`) is checked
against every session feed and every rail filter.

**Setup** (`S-main3-e1-N-*`, `simple_keys.edge_catalog`).

- **Filters:** what a phone holds live (S-main3-live's phone model): one session Resource-edge
  feed per Space and four open thread rails' attendee members, 5 per one-Space account. Each is
  `AND(AND(main's edge authorization, the phone's filter), stream identity)`, the order main's
  `authorize_predicate` writes. The fixture's own count is 7.6 edge filters per account (2 session
  feeds, 4.6 rails, 1 day-window member); at that count the account numbers here are a third lower.
- **Accounts:** prod's mix, as in run 1 (b). N counts today's filters: 100 is 20 accounts,
  1,000 is 199 and 5,000 is 991.
- **The commit:** 1,000 `graph_edge` updates, 20 in each of 50 Spaces, one transaction. They are
  attendee edges. The first 49 one-Space accounts live in written Spaces and hold rails on four
  Records whose edges change.
- **`today`:** main's lowering as it is.
- **`fix` (hypothetical):** run 1's rewrite over every filter.
- **`split` (hypothetical, more than the fix):** `fix`, and for one-Space accounts a rail filter
  starts with its own `from_id = R`, and a session feed becomes two filters, one starting
  `from_space_id = S` and one starting `to_space_id = S AND from_space_id != S`. Together they
  admit the rows `fix` admits. It adds one shape per session feed. The 1 in 20 keep `fix`.
- **Catalog:** these filters and the bystander's `record` probe feed only.

| Keyed `graph_edge` filters today | Lowering | Engine shapes (with probe) | Runner | Sequencer time, 3 commits: median (range) s | Per row | Per row per filter | Engine CPU % | Peak RSS | Bystander receipts (drains ok/attempts) | Bystander live changes | Under 0.5 s |
| ---: | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| 100 | today | 101 | Xeon 8573C | 0.30 (0.16–0.34) | 0.30 ms | 3.03 µs | 25 | 47 MiB | 8/8; p95 0.36 s, max 0.61 s | 8/8; p95 0.23 s, max 0.48 s | yes |
| 100 | plain-test fix | 101 | Xeon 8573C | 0.35 (0.17–0.37) | 0.35 ms | 3.46 µs | 26 | 46 MiB | 8/8; p95 0.22 s, max 0.60 s | 8/8; p95 0.21 s, max 0.45 s | yes |
| 100 | selective equality first | 120 | Xeon 8573C | 0.17 (0.15–0.28) | 0.17 ms | 1.66 µs | 22 | 47 MiB | 7/7; p95 0.19 s, max 0.41 s | 7/7; p95 0.16 s, max 0.41 s | yes |
| 1,000 | today | 1,001 | EPYC 9V45 | 0.78 (0.65–0.78) | 0.78 ms | 0.78 µs | 58 | 104 MiB | 11/11; p95 0.55 s, max 0.89 s | 11/11; p95 0.49 s, max 0.74 s | no |
| 1,000 | plain-test fix | 1,001 | EPYC 9V45 | 0.67 (0.66–0.73) | 0.67 ms | 0.67 µs | 58 | 97 MiB | 11/11; p95 0.54 s, max 0.85 s | 11/11; p95 0.52 s, max 0.77 s | no |
| 1,000 | selective equality first | 1,191 | EPYC 9V45 | 0.22 (0.21–0.25) | 0.22 ms | 0.22 µs | 25 | 89 MiB | 6/6; p95 0.02 s, max 0.34 s | 6/6; p95 0.02 s, max 0.22 s | yes |
| 5,000 | today | 5,001 | EPYC 9V45 | 4.65 (4.53–4.66) | 4.65 ms | 0.93 µs | 91 | 275 MiB | 25/58; p95 1.72 s, max 1.97 s | 26/58; p95 1.74 s, max 1.99 s | no |
| 5,000 | plain-test fix | 5,001 | EPYC 9V45 | 9.04 (8.06–9.48) | 9.04 ms | 1.81 µs | 95 | 252 MiB | 23/109; p95 1.69 s, max 1.93 s | 13/109; p95 1.65 s, max 1.90 s | no |
| 5,000 | selective equality first | 5,943 | EPYC 9V45 | 0.29 (0.29–0.45) | 0.29 ms | 0.06 µs | 50 | 274 MiB | 7/7; p95 0.27 s, max 0.62 s | 7/7; p95 0.20 s, max 0.44 s | yes |

"Per row per filter" divides the whole commit by rows and by today's filter count, floor included;
the 100-filter cells are mostly floor.

**The 5,000-filter repeats.** The first pass had the fix at twice today's time, which the rewrite
cannot cause. Repeated in both orders on two more runners, the fix is 10–20% faster than today on
each, and the same lowering differs by up to 1.6× between runners (today: 4.65, 6.15, 7.6 s). So
the first pass's 9.04 s was that runner, and a 5,000-filter commit is 4.7–7.6 s.

| Job | Order | Lowering | Runner | Sequencer time, 3 commits: median (range) s | Engine CPU % | Bystander receipts (drains ok/attempts) |
| --- | ---: | --- | --- | ---: | ---: | --- |
| b | 1 | plain-test fix | EPYC 9V74 | 6.10 (6.09–6.11) | 92 | 25/78; p95 1.75 s, max 2.00 s |
| b | 2 | today | EPYC 9V74 | 7.56 (7.43–7.58) | 94 | 25/95; p95 1.75 s, max 1.99 s |
| b | 3 | plain-test fix | EPYC 9V74 | 6.07 (6.06–6.08) | 92 | 26/78; p95 1.77 s, max 1.99 s |
| b | 4 | today | EPYC 9V74 | 7.61 (7.61–7.66) | 94 | 23/96; p95 1.68 s, max 1.94 s |
| c | 1 | today | Xeon 8573C | 6.13 (6.06–6.21) | 93 | 25/78; p95 1.72 s, max 1.97 s |
| c | 2 | plain-test fix | Xeon 8573C | 5.54 (5.54–5.57) | 92 | 25/70; p95 1.73 s, max 1.98 s |
| c | 3 | today | Xeon 8573C | 6.18 (6.15–6.22) | 93 | 24/78; p95 1.73 s, max 1.98 s |
| c | 4 | plain-test fix | Xeon 8573C | 5.61 (5.52–5.91) | 91 | 24/72; p95 1.70 s, max 1.95 s |

**Where the commit passes 0.5 s and 2 s.** A straight line through the three sizes, with the
5,000-filter point the median of its passes, at 5 filters per account:

| Lowering | Fit: commit s = a + b × filters | Passes 0.5 s at | Passes 2 s at |
| --- | --- | --- | --- |
| today | −0.11 + 1.25 ms × filters | 492 filters, about 100 accounts | 1,696 filters, about 340 accounts |
| plain-test fix | −0.13 + 1.22 ms × filters | 512 filters, about 100 accounts | 1,737 filters, about 350 accounts |
| selective equality first | 0.18 + 0.022 ms × filters | about 14,000 filters, 2,900 accounts (extrapolated; 5,000 is the largest measured) | not reached; far outside what was measured |

The cost per filter rises with size (0.6 µs per row at 1,000 filters, 0.9–1.5 µs at 5,000), so the
line overstates the middle: read between the measured 1,000 and 5,000 points, 2 s falls at
1,700–2,260 filters, about 340–450 accounts.

**Reading.**

- **Other readers, today:** at 1,000 filters every drain arrives but waits about 0.5 s; at 5,000
  the bystander gets 25 of 58 drains (a quarter to a half across passes).
- **The split's remaining slope** is the 1 in 20 accounts left on shared values (294 filters at
  5,000). Splitting their feeds per Space, as S does, is not measured here.
- **Not measured:** the split's extra shapes on phones, and whether the gateway can address two
  streams for one session feed. This run only shows what the engine does with those filters.

## Run 3: a day and a week, table by table against the bundled group load

**Answer.** In the engine, turning bundles off costs nothing per load and makes nothing worse for
other readers. What it changes is outside what this harness measures: the gateway makes twelve
requests for a window where it made one.

- **Per load, wall time:** table by table in turn takes the same time as the bundle: 0.09–0.11 s
  at p50 and 0.10–0.13 s at p95 for both, at either row count, day and week. The six members at
  once take 0.055–0.065 s (p95 0.07–0.08 s), about 40% less.
- **Per load, engine and Postgres:** table by table uses less of both. Engine CPU is 55–67 ms per
  load against 67–92 ms; Postgres time is 50–65 ms against 129–132 ms, over 7 row selects against 8.
- **Per window, shapes:** a bundle creates 6 shapes for every window, 4 of them subquery shapes.
  Table by table creates 2 per window (both subquery: `assignment` and `graph_edge`) and 4 per
  account once, which every window of that account then shares.
- **These are job b's numbers,** the quieter of two runners; job a's are 30–50% higher and order
  the same way.
- **Other readers:** no request was lost in any cell. Their p95 during the loads is about 2× S
  idle whichever way the windows are loaded (0.04–0.05 s against 0.023 s).
- **Not measured, and it decides this:** the gateway's side. Each of the twelve requests is
  authorized on its own, where a bundle's members share one authority read. The bundle also reads
  its six members in one snapshot; table by table reads six.

**Why table by table is cheaper in Postgres.** It is main's keyed rule, not the bundling. A
one-shot query is keyed whenever its table is; a group member is keyed only when its filter reads
no other table. So the bundle's `record` and `record_date_index` members keep today's
authorization, and their selects take 42 ms and 24 ms. The same two tables as one-shot queries
take 3–5 ms and 2–3 ms. `assignment` and `graph_edge` are keyed neither way and cost the same in
both (15–25 ms each).

**What table by table leaves in the engine.** Per account, four whole-account keyed feeds:
`record`, `record_date_index` and `record_recurrence` on the scan list, and `calendar_event`
indexed under `is_busy_block`, a value every account shares. Those are run 1's filters: without
run 1's fix they cost every `record` write as run 1's control shows. The bundle's members are
per-window subquery shapes, which is what the first-import sweep found stalls imports. This run
made no writes while the windows were held, so it does not say which is worse at write time.

**Setup** (`simple_keys.window_plan`, `run.window_operation`).

- **The windows:** `app_day_window`'s six members (`record_date_index`, `record`,
  `record_recurrence`, `assignment`, `graph_edge`, `calendar_event`) for 1 day and for 7 days.
- **Rows:** for each of the 19 fixture accounts, the start dates whose windows return closest to
  prod's sizes, within 25%: 109 and 392 rows for a day, 172 and 584 for a week. That gives 140
  loads: 37 and 38 day windows, 27 and 38 week windows. The medians returned are 108, 392, 172 and
  584 rows, and every load returned exactly the rows Postgres counts for it.
- **(a) bundle:** one `/v1/subsets/load` with six members.
- **(b) table by table:** per member, a changes-only feed (`/v1/subset-feeds`) and then its rows
  (`/v1/subsets/query`). "In turn" is the six members one after another; "at once" is the six
  together, each still feed then query.
- **Lowering:** main's (`52e79b875b`), keyed flags on, each request as `_authorize_collection_read`
  lowers that operation. A group member keeps its window. A feed drops the window before the keyed
  test, so four of the six feeds are whole-account. A one-shot query keeps it.
- **Each way on its own engine**, with the same accounts and dates, on S-main3-live-plus's
  catalog (1,147 shapes, 637 of them subquery shapes). Loads run one after another, 0.5 s apart,
  day windows first and then week windows.
- **S** is S's catalog idle on the same runner, 60 s. Each engine also idles 60 s before its loads.
- **Fixture change:** calendar events get an hour on their creation day. The fixture's events had
  no time, so a window's `calendar_event` member returned nothing.
- **Postgres** is counted with `pg_stat_statements`, less the bystander's own probe queries.

**Job a (EPYC 9V74), the noisier runner.** Its idle engine was already 1.5–2.3× S before any load.

| Window | Rows wanted | Way | Loads | Rows returned, median | Wall to a complete window p50 / p95 s | Engine CPU inside the request, median ms | Shapes created per load (subquery) | Subquery nodes per load |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| day | p50 | (a) bundled group load | 37 | 108 | 0.138 / 0.379 | 32.2 | 6.0 (4.0) | 2.0 |
| day | p95 | (a) bundled group load | 38 | 392 | 0.143 / 0.377 | 52.6 | 6.0 (4.0) | 2.0 |
| week | p50 | (a) bundled group load | 27 | 172 | 0.150 / 0.305 | 54.2 | 6.0 (4.0) | 2.0 |
| week | p95 | (a) bundled group load | 38 | 584 | 0.154 / 0.192 | 50.0 | 6.0 (4.0) | 2.0 |
| day | p50 | (b) table by table, in turn | 37 | 108 | 0.119 / 0.245 | 23.2 | 4.1 (2.0) | 1.0 |
| day | p95 | (b) table by table, in turn | 38 | 392 | 0.132 / 0.240 | 27.8 | 2.0 (2.0) | 1.0 |
| week | p50 | (b) table by table, in turn | 27 | 172 | 0.131 / 0.202 | 25.7 | 2.0 (2.0) | 1.0 |
| week | p95 | (b) table by table, in turn | 38 | 584 | 0.152 / 0.221 | 37.7 | 2.0 (2.0) | 1.0 |
| day | p50 | (b) table by table, at once | 37 | 108 | 0.088 / 0.256 | 26.4 | 4.1 (2.0) | 1.0 |
| day | p95 | (b) table by table, at once | 38 | 392 | 0.085 / 0.236 | 30.7 | 2.0 (2.0) | 1.0 |
| week | p50 | (b) table by table, at once | 27 | 172 | 0.078 / 0.105 | 23.6 | 2.0 (2.0) | 1.0 |
| week | p95 | (b) table by table, at once | 38 | 584 | 0.085 / 0.111 | 30.7 | 2.0 (2.0) | 1.0 |

| Window | Way | Loads | Phase s | Engine CPU % | Engine CPU per load, net of idle, ms | Postgres row selects per load | Read transactions per load | Postgres time per load ms | Other readers' receipts: ok/attempts, p95 s (× S idle) | Subset p95 s (× S idle) | Live change p95 s (× S idle) | Shapes added (subquery) | Query-backs |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | ---: | ---: |
| — | idle before the loads (1) | 0 | 60 | 7 | — | — | — | — | 240/240; 0.056 (2.3×) | 240/240; 0.009 (1.1×) | 240/240; 0.090 (3.7×) | 0 | 0 |
| — | idle before the loads (2) | 0 | 60 | 7 | — | — | — | — | 241/241; 0.045 (1.8×) | 241/241; 0.009 (1.1×) | 241/241; 0.124 (5.1×) | 0 | 0 |
| — | idle before the loads (3) | 0 | 60 | 7 | — | — | — | — | 240/240; 0.036 (1.5×) | 240/240; 0.009 (1.1×) | 240/240; 0.073 (3.0×) | 0 | 0 |
| — | S idle (4) | 0 | 60 | 2 | — | — | — | — | 240/240; 0.024 (1.0×) | 240/240; 0.008 (1.0×) | 240/240; 0.024 (1.0×) | 0 | 0 |
| day | (a) bundled group load | 75 | 57 | 19 | 93 | 8.0 | 5.0 | 181 | 227/227; 0.123 (5.1×) | 227/227; 0.039 (4.7×) | 227/227; 0.227 (9.4×) | 450 (300) | 150 |
| week | (a) bundled group load | 65 | 48 | 25 | 130 | 8.0 | 5.0 | 190 | 192/192; 0.143 (5.9×) | 192/192; 0.041 (5.1×) | 192/192; 0.164 (6.8×) | 390 (260) | 130 |
| day | (b) table by table, in turn | 75 | 53 | 17 | 73 | 7.0 | 7.0 | 70 | 211/211; 0.067 (2.7×) | 211/211; 0.027 (3.3×) | 211/211; 0.127 (5.3×) | 226 (150) | 75 |
| week | (b) table by table, in turn | 65 | 46 | 19 | 84 | 7.0 | 7.0 | 74 | 185/185; 0.074 (3.0×) | 185/185; 0.024 (2.9×) | 185/185; 0.113 (4.7×) | 130 (130) | 65 |
| day | (b) table by table, at once | 75 | 51 | 19 | 75 | 7.0 | 7.0 | 84 | 203/203; 0.173 (7.1×) | 203/203; 0.036 (4.4×) | 203/203; 0.279 (11.6×) | 226 (150) | 75 |
| week | (b) table by table, at once | 65 | 42 | 20 | 84 | 7.0 | 7.0 | 88 | 168/168; 0.062 (2.5×) | 168/168; 0.026 (3.2×) | 168/168; 0.063 (2.6×) | 130 (130) | 65 |

**Job b (EPYC 9V45).**

| Window | Rows wanted | Way | Loads | Rows returned, median | Wall to a complete window p50 / p95 s | Engine CPU inside the request, median ms | Shapes created per load (subquery) | Subquery nodes per load |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| day | p50 | (a) bundled group load | 37 | 108 | 0.087 / 0.105 | 15.0 | 6.0 (4.0) | 2.0 |
| day | p95 | (a) bundled group load | 38 | 392 | 0.104 / 0.115 | 29.5 | 6.0 (4.0) | 2.0 |
| week | p50 | (a) bundled group load | 27 | 172 | 0.099 / 0.127 | 27.9 | 6.0 (4.0) | 2.0 |
| week | p95 | (a) bundled group load | 38 | 584 | 0.104 / 0.123 | 32.2 | 6.0 (4.0) | 2.0 |
| day | p50 | (b) table by table, in turn | 37 | 108 | 0.087 / 0.100 | 15.4 | 4.1 (2.0) | 1.0 |
| day | p95 | (b) table by table, in turn | 38 | 392 | 0.096 / 0.115 | 21.1 | 2.0 (2.0) | 1.0 |
| week | p50 | (b) table by table, in turn | 27 | 172 | 0.093 / 0.116 | 15.6 | 2.0 (2.0) | 1.0 |
| week | p95 | (b) table by table, in turn | 38 | 584 | 0.105 / 0.132 | 23.5 | 2.0 (2.0) | 1.0 |
| day | p50 | (b) table by table, at once | 37 | 108 | 0.059 / 0.079 | 16.0 | 4.1 (2.0) | 1.0 |
| day | p95 | (b) table by table, at once | 38 | 392 | 0.061 / 0.076 | 22.2 | 2.0 (2.0) | 1.0 |
| week | p50 | (b) table by table, at once | 27 | 172 | 0.055 / 0.066 | 14.2 | 2.0 (2.0) | 1.0 |
| week | p95 | (b) table by table, at once | 38 | 584 | 0.065 / 0.081 | 21.6 | 2.0 (2.0) | 1.0 |

| Window | Way | Loads | Phase s | Engine CPU % | Engine CPU per load, net of idle, ms | Postgres row selects per load | Read transactions per load | Postgres time per load ms | Other readers' receipts: ok/attempts, p95 s (× S idle) | Subset p95 s (× S idle) | Live change p95 s (× S idle) | Shapes added (subquery) | Query-backs |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | ---: | ---: |
| — | idle before the loads (1) | 0 | 60 | 5 | — | — | — | — | 240/240; 0.028 (1.2×) | 240/240; 0.007 (0.9×) | 240/240; 0.030 (1.4×) | 0 | 0 |
| — | idle before the loads (2) | 0 | 60 | 6 | — | — | — | — | 240/240; 0.026 (1.1×) | 240/240; 0.008 (1.1×) | 240/240; 0.022 (1.0×) | 0 | 0 |
| — | idle before the loads (3) | 0 | 60 | 6 | — | — | — | — | 240/240; 0.028 (1.2×) | 240/240; 0.008 (1.1×) | 240/240; 0.025 (1.1×) | 0 | 0 |
| — | S idle (4) | 0 | 60 | 1 | — | — | — | — | 240/240; 0.023 (1.0×) | 240/240; 0.007 (1.0×) | 240/240; 0.022 (1.0×) | 0 | 0 |
| day | (a) bundled group load | 75 | 48 | 16 | 67 | 8.0 | 5.0 | 129 | 192/192; 0.047 (2.0×) | 192/192; 0.023 (3.2×) | 192/192; 0.051 (2.4×) | 450 (300) | 150 |
| week | (a) bundled group load | 65 | 43 | 19 | 92 | 8.0 | 5.0 | 132 | 171/171; 0.046 (2.0×) | 171/171; 0.022 (3.0×) | 171/171; 0.054 (2.5×) | 390 (260) | 130 |
| day | (b) table by table, in turn | 75 | 48 | 14 | 55 | 7.0 | 7.0 | 50 | 190/190; 0.043 (1.9×) | 190/190; 0.022 (3.0×) | 190/190; 0.041 (1.9×) | 226 (150) | 75 |
| week | (b) table by table, in turn | 65 | 42 | 16 | 65 | 7.0 | 7.0 | 54 | 169/169; 0.039 (1.7×) | 169/169; 0.018 (2.5×) | 169/169; 0.042 (1.9×) | 130 (130) | 65 |
| day | (b) table by table, at once | 75 | 46 | 16 | 63 | 7.0 | 7.0 | 63 | 182/182; 0.045 (2.0×) | 182/182; 0.019 (2.6×) | 182/182; 0.043 (2.0×) | 226 (150) | 75 |
| week | (b) table by table, at once | 65 | 40 | 17 | 67 | 7.0 | 7.0 | 65 | 158/158; 0.044 (1.9×) | 158/158; 0.022 (3.1×) | 158/158; 0.043 (2.0×) | 130 (130) | 65 |

**Reading the tables.**

- **Engine CPU, two ways.** "Inside the request" is the engine's CPU between the request and its
  answer. "Net of idle" is the phase's CPU less the idle rate, per load: it also holds the work
  after the answer (filling the new subquery nodes) and the probe's higher cost as shapes pile up.
- **The first load per account** table by table creates 6 shapes (the 4 whole-account feeds and
  the window's 2); every later one creates 2. That is the 4.1 in the day p50 row.
- **Where the bundle's time goes** (the engine's own `group load finished` events, job b medians):
  92 ms in all, 26 ms creating the six members and 53 ms in the selects, over three connections on
  one exported snapshot.
- **Where table by table's time goes** (job b, in turn): `graph_edge`'s query 29–31 ms,
  `assignment`'s feed 12 ms and query 18 ms, `record`'s query 8–11 ms. Every other step is under
  6 ms.
- **Job a** is shown for the spread. Its bundle cells look worse for other readers (5–6× S against
  about 3× in turn), but its idle engines differed by as much before any load, so job b is the fairer
  comparison.

**Not measured.**

- **The gateway:** its authority reads, drains and rechecks per request (`SUMMARY.md`, "Not
  modelled"). Unkeyed members need the drain either way; a bundle does it once for the group.
- **Writes while the windows are held**, each way.
- **Retirement:** idle-shape retirement is off in the harness, so nothing let go of a window.
- **Renewing a window** the phone already holds: every load here is a first load.
