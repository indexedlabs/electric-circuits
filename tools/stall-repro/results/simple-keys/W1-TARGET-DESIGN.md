# W1: the target design under one combined load (Main, via circuits-coord)

**Answer.** No. At 2,000 active accounts under that load, one engine CPU cannot serve the screen
loads. Nothing written is lost or late; what gives way is reads. The design holds at 200 accounts,
is full at 1,600, and sheds loads at 2,000.

| Active accounts (shapes) | Engine CPU, loads only | Engine CPU, with import and deletion | Screen loads with import and deletion | Bystander receipts with import and deletion |
| ---: | ---: | ---: | --- | --- |
| 20 (874) | 2% | 6% | all answered, p95 0.01–0.02 s | all arrive, p95 0.10 s |
| 200 (7,714) | 8% | 15% | all answered, p95 0.02 s | all arrive, p95 0.07 s |
| 1,600 (60,914) | 63% | 92% | all answered, p95 1.6 s, started up to 17 s late | all arrive, p95 0.13 s |
| 2,000 (76,114) | 85% | 95% | 60% answered, p95 2.6 s; 40% not started within 30 s | all arrive, p95 0.19 s |

- **At 2,000, loads only:** the engine is at 85%. Every load is answered, p95 0.26–0.29 s
  (0.07–0.08 s at 1,600).
- **At 2,000, with the import and the deletion:** the engine is at 95–96% for the whole
  deletion. It serves 442 of the 734 requests a second asked of it. Answered loads take 2.2–2.6 s,
  and 8,121 of 20,465 loads could not start within 30 s of their time and were dropped by the
  generator. None of the answered ones timed out or failed.
- **The writers hold at every size.** At 2,000 the import takes 21 s (12 s at 200), a commit's
  receipt p95 is 0.27 s, the deletion takes 159 s (81 s at 200), and a bystander's drains and live
  changes all arrive (p95 0.19 s and 0.15 s; none of 743 timed out). The sequencer-held gauges
  stay at 0 and no drain is lost in any cell.

**Where it bends first: two costs on the engine's one CPU.** A line with two terms fits all eight
phases (predicted against measured CPU: 8/8 and 14/15 at 200, 63/63 and 90/92 at 1,600, 79/85 and
84/95 at 2,000):

- **Reads:** about 0.6 ms of engine CPU per load request, at this fixture's row sizes. At 2,000
  accounts' 734 requests a second that is about half the CPU.
- **Every commit, however small:** about 0.5 µs for every shape the engine holds. That is 4 ms a
  commit at 7,700 shapes and 40 ms at 76,000. The paced deletion makes 8–10 ten-row commits a
  second and the harness's bystander another 8, so at 2,000 accounts commits alone take over half
  the CPU, and the import and the deletion push the loads out.
- **Why this matters for the design:** 38 feeds a phone makes the shape count 38 times the
  phones, and pacing writes into small commits multiplies it. Fewer shapes per phone, bigger paced
  commits, or an engine pass that does not touch every shape on every commit would each move the
  bend. The second term is read off the fit, not profiled; a CPU profile of the 2,000 cell is the
  next run.

- **A whole-family follower** in the importing family is sent all 30,000 rows, 164 MB, as fast as
  the import commits (12–21 s). A phone on a day window would be sent 359 of them (2.0 MB), on a
  week window 2,513 (13.7 MB). An unrelated family's phone is sent nothing from either.

**The harness's stream store stopped the 2,000 cell twice before it ran.**

- **First run:** the file-backed stream store refused its 65,513th stream. It keeps one open
  file per stream, a hosted runner allows 65,536, and 2,000 phones at 38 feeds each are 76,114
  streams. With the limit raised to 1,048,576 the store held all 76,114.
- **Second run:** 22,790 feeds in, the fixture's store proxy closed a connection under one stream
  create and the harness gave up on that feed. Feed creation now retries; the third run needed no
  retry.
- **What this says about the design:** one stream per family per table means 38 streams a
  family. Whatever store prod runs has to hold that many open (76,000 at 2,000 families) and
  create them: this fixture creates about 110 feeds a second, so 2,000 phones' feeds take about
  11 minutes.

**Read these with three limits in mind.**

- **Row size:** rows carry the fixture's 3.5 KB padding on every blob column, so a median
  family's load is 71–235 rows and about 0.4 MB, and the engine serves 67 MB a second at 2,000
  accounts. Smaller real rows would cost less than 0.6 ms a request.
- **Load rate and the bystander:** every account is active at once and makes four loads a
  minute, and the bystander's probe commits 8 times a second, which the per-commit cost makes
  expensive at 76,000 shapes (about a third of the loads-only CPU at 2,000).
- **The generator's caps:** at most 128 loads in flight, and a load more than 30 s late is
  dropped. Under saturation those caps set the 1.3–2.6 s a load takes and the 40% shed; an
  uncapped client would see longer waits and no drops.

Engine `9bc1563` (prod's binary), 1 CPU, GitHub-hosted runners (4 cores, shared by the engine,
Postgres and the load generator). Actions runs `45963541607` (20 and 200, harness `60bb84c`) and
`45966333495` (1,600, harness `58f3443`) and `45969526371` (2,000, harness `819f883`). Folders
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

### 1,600 active accounts (Xeon 6975P-C, `paired-w1-1600-1-W1-1600-prod9bc-1cpu`)

Engine shapes: 60,914, all of them in the engine's 38 equality families (one per table, a hash lookup by key); subquery shapes 0, scan-list shapes 0. Family rows seeded: 2,120,720.

| | loads only | loads, import and deletion |
| --- | --- | --- |
| Phase length | 120 s | 142 s |
| Bystander drains (receipts) | 481/481; p50 0.047 s, p95 0.076 s, max 0.11 s; 0 timed out | 569/569; p50 0.068 s, p95 0.134 s, max 0.27 s; 0 timed out |
| Bystander subset loads | 481/481; p50 0.007 s, p95 0.014 s, max 0.15 s; 0 timed out | 569/569; p50 0.499 s, p95 0.645 s, max 0.68 s; 0 timed out |
| Bystander live changes | 481/481; p50 0.054 s, p95 0.066 s, max 0.27 s; 0 timed out | 569/569; p50 0.072 s, p95 0.113 s, max 0.28 s; 0 timed out |
| Longest run of lost drains | 0.0 s | 0.0 s |
| Other accounts' day loads | 3,195/3,195 answered; p50 0.014 s, p95 0.074 s, max 0.20 s; 0 timed out, 0 shed, 0 failed | 3,485/3,485 answered; p50 1.341 s, p95 1.564 s, max 1.71 s; 0 timed out, 0 shed, 0 failed |
| Other accounts' week loads | 3,194/3,194 answered; p50 0.017 s, p95 0.080 s, max 0.21 s; 0 timed out, 0 shed, 0 failed | 3,457/3,457 answered; p50 1.350 s, p95 1.570 s, max 1.71 s; 0 timed out, 0 shed, 0 failed |
| Other accounts' record detail opens | 6,386/6,386 answered; p50 0.012 s, p95 0.073 s, max 0.20 s; 0 timed out, 0 shed, 0 failed | 6,948/6,948 answered; p50 1.335 s, p95 1.559 s, max 1.74 s; 0 timed out, 0 shed, 0 failed |
| Loads started late by, p95 / max | 0.002 s / 0.005 s | 15.594 s / 16.869 s |
| Loads served | 587 requests/s, 53.7 MB/s | 586 requests/s, 53.4 MB/s |
| Rows per load, p50 / p95 | day 71 / 141 ; week 215 / 227 ; detail 235 / 238 | day 71 / 141 ; week 215 / 227 ; detail 235 / 238 |
| Engine CPU (of 1) | 63% | 92% |
| Engine peak RSS | 475 MiB | 557 MiB |
| Runner CPU, all 4 cores | 43% mean, 55% peak | 57% mean, 71% peak |
| Sequencer held: peak / longest run | 0 B / 0 s | 0 B / 0 s |
| Import: wall time and per-commit receipt | — | 17 s; 50 commits, receipt p50 0.154 s, p95 0.226 s, max 0.23 s |
| Deletion: wall time and per-commit receipt | — | 121 s; 1,242 commits, receipt p50 0.076 s, p95 0.119 s, max 0.31 s |

| A phone that follows | Rows delivered | Bytes delivered | Over | Rows a minute | Bytes a minute | Of which a day-window phone would get | Of which a week-window phone would get |
| --- | ---: | ---: | --- | ---: | ---: | --- | --- |
| the importing family, all tables | 30,000 | 163.9 MB | 17 s | 105,254 | 575.2 MB | 359 rows, 2.0 MB | 2,513 rows, 13.7 MB |
| the deleted account's family, all tables | 12,419 | 2.1 MB | 121 s | 6,180 | 1.1 MB | 147 rows, 0.0 MB | 1,019 rows, 0.2 MB |
| its own (bystander) family, all tables | 564 | 4.5 MB | 142 s | 238 | 1.9 MB | — | — |

By table, import: `assignment` 5,000 rows / 3.5 MB, `calendar_event` 5,000 rows / 40.0 MB, `graph_edge` 10,000 rows / 78.6 MB, `record` 5,000 rows / 39.3 MB, `record_date_index` 5,000 rows / 2.4 MB.

By table, delete: `assignment` 4,200 rows / 0.7 MB, `calendar` 2 rows / 0.0 MB, `calendar_event` 300 rows / 0.1 MB, `external_account` 1 rows / 0.0 MB, `file` 13 rows / 0.0 MB, `graph_edge` 560 rows / 0.1 MB, `message` 200 rows / 0.0 MB, `record` 2,200 rows / 0.4 MB, `record_date_index` 4,800 rows / 0.9 MB, `record_recurrence` 100 rows / 0.0 MB, `resource` 40 rows / 0.0 MB, `space` 1 rows / 0.0 MB, `space_member` 1 rows / 0.0 MB, `user` 1 rows / 0.0 MB.

### 2,000 active accounts (Xeon 6975P-C, `paired-w1-2000-1-W1-2000-prod9bc-1cpu`)

Engine shapes: 76,114, all of them in the engine's 38 equality families (one per table, a hash lookup by key); subquery shapes 0, scan-list shapes 0. Family rows seeded: 2,650,900.

| | loads only | loads, import and deletion |
| --- | --- | --- |
| Phase length | 121 s | 186 s |
| Bystander drains (receipts) | 483/483; p50 0.067 s, p95 0.118 s, max 0.22 s; 0 timed out | 743/743; p50 0.082 s, p95 0.185 s, max 0.67 s; 0 timed out |
| Bystander subset loads | 483/483; p50 0.012 s, p95 0.231 s, max 0.41 s; 0 timed out | 743/743; p50 0.867 s, p95 1.007 s, max 1.18 s; 0 timed out |
| Bystander live changes | 483/483; p50 0.075 s, p95 0.088 s, max 0.24 s; 0 timed out | 743/743; p50 0.084 s, p95 0.148 s, max 0.38 s; 0 timed out |
| Longest run of lost drains | 0.0 s | 0.0 s |
| Other accounts' day loads | 3,996/3,996 answered; p50 0.075 s, p95 0.280 s, max 0.44 s; 0 timed out, 0 shed, 0 failed | 3,135/5,130 answered; p50 2.234 s, p95 2.648 s, max 2.88 s; 0 timed out, 1995 shed, 0 failed |
| Other accounts' week loads | 3,994/3,994 answered; p50 0.077 s, p95 0.285 s, max 0.45 s; 0 timed out, 0 shed, 0 failed | 3,073/5,123 answered; p50 2.242 s, p95 2.649 s, max 2.88 s; 0 timed out, 2050 shed, 0 failed |
| Other accounts' record detail opens | 7,989/7,989 answered; p50 0.071 s, p95 0.263 s, max 0.45 s; 0 timed out, 0 shed, 0 failed | 6,136/10,212 answered; p50 2.229 s, p95 2.625 s, max 2.89 s; 0 timed out, 4076 shed, 0 failed |
| Loads started late by, p95 / max | 0.002 s / 0.017 s | 30.064 s / 30.219 s |
| Loads served | 734 requests/s, 67.0 MB/s | 442 requests/s, 40.2 MB/s |
| Rows per load, p50 / p95 | day 71 / 141 ; week 215 / 227 ; detail 235 / 238 | day 71 / 141 ; week 215 / 227 ; detail 235 / 238 |
| Engine CPU (of 1) | 85% | 95% |
| Engine peak RSS | 610 MiB | 613 MiB |
| Runner CPU, all 4 cores | 57% mean, 64% peak | 51% mean, 69% peak |
| Sequencer held: peak / longest run | 0 B / 0 s | 0 B / 0 s |
| Import: wall time and per-commit receipt | — | 21 s; 50 commits, receipt p50 0.224 s, p95 0.273 s, max 0.30 s |
| Deletion: wall time and per-commit receipt | — | 159 s; 1,242 commits, receipt p50 0.098 s, p95 0.160 s, max 0.35 s |

| A phone that follows | Rows delivered | Bytes delivered | Over | Rows a minute | Bytes a minute | Of which a day-window phone would get | Of which a week-window phone would get |
| --- | ---: | ---: | --- | ---: | ---: | --- | --- |
| the importing family, all tables | 30,000 | 163.9 MB | 21 s | 87,738 | 479.4 MB | 359 rows, 2.0 MB | 2,513 rows, 13.7 MB |
| the deleted account's family, all tables | 12,419 | 2.1 MB | 159 s | 4,696 | 0.8 MB | 147 rows, 0.0 MB | 1,019 rows, 0.2 MB |
| its own (bystander) family, all tables | 736 | 5.8 MB | 186 s | 238 | 1.9 MB | — | — |

By table, import: `assignment` 5,000 rows / 3.5 MB, `calendar_event` 5,000 rows / 40.0 MB, `graph_edge` 10,000 rows / 78.6 MB, `record` 5,000 rows / 39.3 MB, `record_date_index` 5,000 rows / 2.4 MB.

By table, delete: `assignment` 4,200 rows / 0.7 MB, `calendar` 2 rows / 0.0 MB, `calendar_event` 300 rows / 0.1 MB, `external_account` 1 rows / 0.0 MB, `file` 13 rows / 0.0 MB, `graph_edge` 560 rows / 0.1 MB, `message` 200 rows / 0.0 MB, `record` 2,200 rows / 0.4 MB, `record_date_index` 4,800 rows / 0.9 MB, `record_recurrence` 100 rows / 0.0 MB, `resource` 40 rows / 0.0 MB, `space` 1 rows / 0.0 MB, `space_member` 1 rows / 0.0 MB, `user` 1 rows / 0.0 MB.

