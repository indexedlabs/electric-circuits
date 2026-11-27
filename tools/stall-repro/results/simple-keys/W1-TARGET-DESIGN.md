# W1: the target design under one combined load (Main, via circuits-coord)

**Answer so far (20 and 200 active accounts; the 2,000 cell is still running).** The target design
holds at both sizes with room to spare.

- **Other accounts:** no drain, load or live change is lost in either cell, with or without the
  import and the deletion running. Under the combined load the bystander's receipt p95 goes from
  0.026 s to 0.07–0.10 s, and its worst is 0.5–0.7 s.
- **The import:** 5,000 events at 100 a commit land in 12 s; a commit's receipt takes 0.10 s at
  p50 and 0.21 s at p95.
- **The deletion:** a prod-p95 family (12,419 rows, 1,242 commits) takes 81–83 s. Each commit's
  receipt is 0.052 s, which is the harness's 50 ms receipt poll, so the engine is not what paces it.
- **The engine:** 15% of one CPU and 115 MiB at 200 accounts under the combined load. The steady
  loads alone are 8% at 200 accounts, so by that line the loads, not the writes, are what will
  fill the CPU first as accounts grow.
- **A whole-family follower** in the importing family is sent all 30,000 rows, 164 MB in 12 s. A
  phone on a day window would be sent 359 of them (2.0 MB), on a week window 2,513 (13.7 MB).

Engine `9bc1563` (prod's binary), 1 CPU, GitHub-hosted runners (4 cores, shared by the engine,
Postgres and the load generator). Actions run `45963541607`, harness `60bb84c`. Folders
`paired-w1-*` beside this file; tables by `tools/stall-repro/w1_report.py`.

## The setup

- **Subscriptions.** Every phone holds one feed per table, `audience_key = <its family's Space>`,
  on all 38 tables the gateway registers: 38 shapes a phone, one `=` each. The engine puts them in
  38 equality families, one per table, so a changed row finds its readers by a hash lookup.
- **What B left out.** B, the audience-key prototype, covered 24 tables and keyed
  `calendar_event` by calendar (15 feeds a phone). W1 keys `calendar_event` by the family, and
  adds the 14 tables B did not have: `assistant_stream_event`, `calendar_highlight`,
  `composer_submission`, `email_inbox`, `external_link`, `generated_image`,
  `monitored_url_source`, `monitored_url_subscription`, `standing_grant_backfill_status`,
  `suggestion_seen`, `tag`, `tagged_item`, `upcoming_summary` and `workflow`. Each is keyed the
  same way: the harness gives every published table an `audience_key` column and its writer fills
  it with the row's family Space. B's second test, `reader_class = 'live'`, is dropped.
- **Not decided here:** how a row with more than one audience gets its key (a `user` in two
  Spaces, a shared Record, a partner's calendar, `monitored_url_source`). In this run every row
  has one family.
- **Families.** N synthetic families, each its own Space. 19 in 20 have prod's median rows
  (Records 3, date index 491, edges 7, assignments 196, files 5); 1 in 20 has prod's p95 (2,200,
  4,800, 560, 4,200, 13). The harness adds recurrences (2 and 100), calendar events (30 and 300)
  and messages (10 and 200), which the prod reads do not give. Dates spread over a year. Rows
  carry the fixture's 3.5 KB padding on every blob column, as in every earlier run.
- **Postgres** has an index on `audience_key` on each table a load reads (and on
  `audience_key, date_value` for the date index). The target design reads by key, so prod's
  tables would need the same.
- **Active accounts.** Each family's phone, every minute at its own fixed offsets: one day load
  and one week load (six one-shot queries each) and two Record detail opens (five each, the
  Record's thread included). Every filter starts with the family's `audience_key = K`. The
  schedule is open-loop. Two detail opens a minute is the harness's choice. At 2,000 accounts
  that is 133 loads and 733 requests a second.
- **The two phases, on one engine.** *Loads only* for 120 s, then *loads, import and deletion*:
  - one family's first import, 5,000 events at 100 a commit (6 tracked rows an event), each
    commit waiting for the engine's receipt;
  - one family at prod's p95 size deleted at 10 rows a commit, each waiting for its receipt;
  - both at once, on top of the same steady loads.
- **Bystander:** a family with its own 38 feeds whose probe makes a drain, a one-row load and a
  one-row change four times a second. "Timed out" is past the probe's 2 s.
- **Followers.** A second phone in the importing family, and one in the deleted family, are read
  after the phase: everything their 38 feeds delivered. The window columns count, from those same
  deliveries, the rows whose date falls in one day or one week of the fixture's 84 days. A real
  first import spans years, so its window share would be smaller.
- **Not modelled:** the gateway, phones re-subscribing when a key changes, and any write by the
  active families other than the import and the deletion.

## The cells

### 20 active accounts (Xeon 6973P-C, `paired-w1-20-1-W1-20-prod9bc-1cpu`)

Engine shapes: 874, all of them in the engine's 38 equality families (one per table, a hash lookup by key); subquery shapes 0, scan-list shapes 0. Family rows seeded: 26,509.

| | loads only | loads, import and deletion |
| --- | --- | --- |
| Phase length | 121 s | 95 s |
| Bystander drains (receipts) | 483/483; p50 0.024 s, p95 0.026 s, max 0.18 s; 0 timed out | 381/381; p50 0.024 s, p95 0.100 s, max 0.74 s; 0 timed out |
| Bystander subset loads | 483/483; p50 0.006 s, p95 0.008 s, max 0.05 s; 0 timed out | 381/381; p50 0.007 s, p95 0.017 s, max 0.09 s; 0 timed out |
| Bystander live changes | 483/483; p50 0.022 s, p95 0.026 s, max 0.17 s; 0 timed out | 381/381; p50 0.023 s, p95 0.090 s, max 0.58 s; 0 timed out |
| Longest run of lost drains | 0.0 s | 0.0 s |
| Other accounts' day loads | 40/40 answered; p50 0.007 s, p95 0.009 s, max 0.01 s; 0 timed out, 0 shed, 0 failed | 34/34 answered; p50 0.008 s, p95 0.011 s, max 0.01 s; 0 timed out, 0 shed, 0 failed |
| Other accounts' week loads | 40/40 answered; p50 0.011 s, p95 0.015 s, max 0.03 s; 0 timed out, 0 shed, 0 failed | 32/32 answered; p50 0.012 s, p95 0.017 s, max 0.02 s; 0 timed out, 0 shed, 0 failed |
| Other accounts' record detail opens | 80/80 answered; p50 0.006 s, p95 0.008 s, max 0.02 s; 0 timed out, 0 shed, 0 failed | 62/62 answered; p50 0.007 s, p95 0.010 s, max 0.02 s; 0 timed out, 0 shed, 0 failed |
| Loads started late by, p95 / max | 0.002 s / 0.003 s | 0.002 s / 0.002 s |
| Loads served | 7 requests/s, 0.7 MB/s | 8 requests/s, 0.7 MB/s |
| Rows per load, p50 / p95 | day 71 / 141 ; week 215 / 227 ; detail 235 / 238 | day 71 / 141 ; week 215 / 227 ; detail 235 / 238 |
| Engine CPU (of 1) | 2% | 6% |
| Engine peak RSS | 59 MiB | 68 MiB |
| Runner CPU, all 4 cores | 5% mean, 11% peak | 16% mean, 37% peak |
| Sequencer held: peak / longest run | 0 B / 0 s | 0 B / 0 s |
| Import: wall time and per-commit receipt | — | 12 s; 50 commits, receipt p50 0.104 s, p95 0.215 s, max 0.51 s |
| Deletion: wall time and per-commit receipt | — | 83 s; 1,242 commits, receipt p50 0.052 s, p95 0.058 s, max 0.38 s |

| A phone that follows | Rows delivered | Bytes delivered | Over | Rows a minute | Bytes a minute | Of which a day-window phone would get | Of which a week-window phone would get |
| --- | ---: | ---: | --- | ---: | ---: | --- | --- |
| the importing family, all tables | 30,000 | 163.9 MB | 12 s | 146,488 | 800.3 MB | 359 rows, 2.0 MB | 2,513 rows, 13.7 MB |
| the deleted account's family, all tables | 12,419 | 2.1 MB | 83 s | 8,940 | 1.5 MB | 147 rows, 0.0 MB | 1,019 rows, 0.2 MB |
| its own (bystander) family, all tables | 374 | 3.0 MB | 95 s | 236 | 1.9 MB | — | — |

By table, import: `assignment` 5,000 rows / 3.5 MB, `calendar_event` 5,000 rows / 40.0 MB, `graph_edge` 10,000 rows / 78.6 MB, `record` 5,000 rows / 39.3 MB, `record_date_index` 5,000 rows / 2.4 MB.

By table, delete: `assignment` 4,200 rows / 0.7 MB, `calendar` 2 rows / 0.0 MB, `calendar_event` 300 rows / 0.1 MB, `external_account` 1 rows / 0.0 MB, `file` 13 rows / 0.0 MB, `graph_edge` 560 rows / 0.1 MB, `message` 200 rows / 0.0 MB, `record` 2,200 rows / 0.4 MB, `record_date_index` 4,800 rows / 0.9 MB, `record_recurrence` 100 rows / 0.0 MB, `resource` 40 rows / 0.0 MB, `space` 1 rows / 0.0 MB, `space_member` 1 rows / 0.0 MB, `user` 1 rows / 0.0 MB.

### 200 active accounts (EPYC 9V45, `paired-w1-200-1-W1-200-prod9bc-1cpu`)

Engine shapes: 7,714, all of them in the engine's 38 equality families (one per table, a hash lookup by key); subquery shapes 0, scan-list shapes 0. Family rows seeded: 265,090.

| | loads only | loads, import and deletion |
| --- | --- | --- |
| Phase length | 120 s | 93 s |
| Bystander drains (receipts) | 482/482; p50 0.022 s, p95 0.026 s, max 0.04 s; 0 timed out | 372/372; p50 0.024 s, p95 0.067 s, max 0.52 s; 0 timed out |
| Bystander subset loads | 482/482; p50 0.006 s, p95 0.009 s, max 0.02 s; 0 timed out | 372/372; p50 0.007 s, p95 0.014 s, max 0.10 s; 0 timed out |
| Bystander live changes | 482/482; p50 0.020 s, p95 0.024 s, max 0.26 s; 0 timed out | 372/372; p50 0.022 s, p95 0.073 s, max 0.55 s; 0 timed out |
| Longest run of lost drains | 0.0 s | 0.0 s |
| Other accounts' day loads | 400/400 answered; p50 0.005 s, p95 0.009 s, max 0.03 s; 0 timed out, 0 shed, 0 failed | 299/299 answered; p50 0.007 s, p95 0.015 s, max 0.07 s; 0 timed out, 0 shed, 0 failed |
| Other accounts' week loads | 400/400 answered; p50 0.008 s, p95 0.012 s, max 0.02 s; 0 timed out, 0 shed, 0 failed | 299/299 answered; p50 0.010 s, p95 0.016 s, max 0.07 s; 0 timed out, 0 shed, 0 failed |
| Other accounts' record detail opens | 800/800 answered; p50 0.005 s, p95 0.009 s, max 0.03 s; 0 timed out, 0 shed, 0 failed | 609/609 answered; p50 0.005 s, p95 0.012 s, max 0.15 s; 0 timed out, 0 shed, 0 failed |
| Loads started late by, p95 / max | 0.002 s / 0.003 s | 0.002 s / 0.003 s |
| Loads served | 74 requests/s, 6.7 MB/s | 73 requests/s, 6.6 MB/s |
| Rows per load, p50 / p95 | day 71 / 141 ; week 215 / 227 ; detail 235 / 238 | day 71 / 141 ; week 215 / 227 ; detail 235 / 238 |
| Engine CPU (of 1) | 8% | 15% |
| Engine peak RSS | 104 MiB | 115 MiB |
| Runner CPU, all 4 cores | 9% mean, 38% peak | 19% mean, 42% peak |
| Sequencer held: peak / longest run | 0 B / 0 s | 0 B / 0 s |
| Import: wall time and per-commit receipt | — | 12 s; 50 commits, receipt p50 0.104 s, p95 0.212 s, max 0.41 s |
| Deletion: wall time and per-commit receipt | — | 81 s; 1,242 commits, receipt p50 0.052 s, p95 0.057 s, max 0.60 s |

| A phone that follows | Rows delivered | Bytes delivered | Over | Rows a minute | Bytes a minute | Of which a day-window phone would get | Of which a week-window phone would get |
| --- | ---: | ---: | --- | ---: | ---: | --- | --- |
| the importing family, all tables | 30,000 | 163.9 MB | 12 s | 153,672 | 839.7 MB | 359 rows, 2.0 MB | 2,513 rows, 13.7 MB |
| the deleted account's family, all tables | 12,419 | 2.1 MB | 81 s | 9,163 | 1.6 MB | 147 rows, 0.0 MB | 1,019 rows, 0.2 MB |
| its own (bystander) family, all tables | 366 | 2.9 MB | 93 s | 236 | 1.9 MB | — | — |

By table, import: `assignment` 5,000 rows / 3.5 MB, `calendar_event` 5,000 rows / 40.0 MB, `graph_edge` 10,000 rows / 78.6 MB, `record` 5,000 rows / 39.3 MB, `record_date_index` 5,000 rows / 2.4 MB.

By table, delete: `assignment` 4,200 rows / 0.7 MB, `calendar` 2 rows / 0.0 MB, `calendar_event` 300 rows / 0.1 MB, `external_account` 1 rows / 0.0 MB, `file` 13 rows / 0.0 MB, `graph_edge` 560 rows / 0.1 MB, `message` 200 rows / 0.0 MB, `record` 2,200 rows / 0.4 MB, `record_date_index` 4,800 rows / 0.9 MB, `record_recurrence` 100 rows / 0.0 MB, `resource` 40 rows / 0.0 MB, `space` 1 rows / 0.0 MB, `space_member` 1 rows / 0.0 MB, `user` 1 rows / 0.0 MB.

