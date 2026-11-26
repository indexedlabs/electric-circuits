Result: under main's lowering at `52e79b875b`, keyed or not, only 1 event per commit (6 tracked rows) keeps every other reader served. Receipts are 0.26–0.28 s at p50, but bystander p95 latency is 7–26× S's, and a 5,000-event import takes about 23–27 minutes. At 10 events per commit receipts land in the 2–3 s band, but 5–22% of drains are lost. With `graph_edge`, `assignment` and the day-window `record` / `record_date_index` members keyed too (sweep-keyed-all), every size up to 100 events per commit (600 tracked rows) keeps every reader served with 0.05–0.30 s receipts, and 5,000 events take about 15 s. Pacing is then needed only against a fully unpaced import (still 2 of 6 drains in its 5 s burst).

# First-import commit-size sweep (indexed #8955, `calendar_first_import_pacing`)

**Runs.** Actions runs `45866702047` (harness `492b646`, the sweep) and `45870425206` (`45e8573`,
sweep-keyed-all). Engine `9bc1563` (prod's binary), Nov 24 catalog, 1 CPU, and S-main3's full
bystander catalog. Each cell runs back to back with S on the same runner. Run folders are
`paired-sweep-*` and `paired-keyed-all-*` beside this file.

**Workload (`paced-import-N`).** One first import of 1,000 calendar events. Each event writes 6
tracked rows (`calendar_event`, `record`, `record_date_index`, `assignment`, 2 attendee
`graph_edge`s); its untracked rows are not published and are left out. The writer commits N events
at a time and waits for each commit's receipt before the next, polling the engine's
`/_runtime/drained-through/{marker}` every 50 ms, so 0.05 s is the floor.

**Filter states, all lowered by main's own gateway code (`52e79b875b`):**

- **flags off:** `S-main3-off`, all three keyed flags off.
- **keyed (main):** `S-main3`, `circuits_keyed_scopes`, `circuits_keyed_index_tables` and
  `circuits_keyed_chat_rows` on.
- **keyed-all:** below.

**Columns.** "× S" compares the bystander's p95 latency with S's on the same runner. At 50 and 100
events per commit, most bystander attempts fail, so their p95 is over the few that succeeded:
read the success rate. CPU is the engine's share of one core over the run. Sequencer held is
`engine_sequencer_held` (incomplete-transaction bytes) and its longest `held_run`; both stayed at 0
in every cell, so they do not show these stalls.

## The eight cells

| Events per commit (tracked rows) | Lowering | Runner | Receipt p50 / p95 s | Import wall s (S) | Bystander drain (p95 × S) | Subset (p95 × S) | Live change (p95 × S) | CPU % | Sequencer held peak / longest run | Backfills | Subquery shapes |
| --- | --- | --- | ---: | ---: | --- | --- | --- | ---: | --- | ---: | ---: |
| 1 (6) | flags off | EPYC 9V74 | 0.28 / 0.57 | 320 (66) | 100% (320/320); p95 0.48 (14.3×) | 100% (320/320); p95 0.26 (25.7×) | 100% (320/320); p95 0.47 (12.3×) | 69 | 0 B / 0 s | 9,072 | 5,136 |
| 1 (6) | keyed (main) | EPYC 9V74 | 0.26 / 0.37 | 278 (66) | 100% (277/277); p95 0.38 (11.1×) | 100% (277/277); p95 0.19 (19.2×) | 100% (277/277); p95 0.28 (7.3×) | 78 | 0 B / 0 s | 8,072 | 2,816 |
| 10 (60) | flags off | EPYC 9V74 | 2.47 / 2.69 | 232 (8) | 78% (182/232); p95 1.86 (59.8×) | 100% (232/232); p95 1.44 (162.6×) | 81% (189/232); p95 1.90 (66.3×) | 94 | 0 B / 0 s | 9,072 | 5,136 |
| 10 (60) | keyed (main) | EPYC 9V74 | 1.93 / 2.62 | 203 (8) | 94% (190/203); p95 1.79 (57.5×) | 100% (203/203); p95 1.39 (157.1×) | 95% (192/203); p95 1.75 (60.8×) | 92 | 0 B / 0 s | 8,072 | 2,816 |
| 50 (300) | flags off | EPYC 7763 | 15.42 / 16.13 | 256 (3) | 1% (2/255); p95 0.61 (8.3×) | 61% (155/255); p95 1.72 (94.8×) | 3% (8/255); p95 1.67 (24.5×) | 92 | 0 B / 0 s | 9,072 | 5,136 |
| 50 (300) | keyed (main) | EPYC 7763 | 11.82 / 12.40 | 240 (3) | 1% (3/240); p95 1.07 (14.6×) | 51% (123/240); p95 1.52 (83.6×) | 1% (3/240); p95 1.08 (15.8×) | 92 | 0 B / 0 s | 8,072 | 2,816 |
| 100 (600) | flags off | EPYC 9V74 | 21.00 / 30.55 | 200 (3) | 1% (2/199); p95 0.42 (2.3×) | 62% (123/199); p95 1.78 (185.1×) | 1% (2/199); p95 0.31 (2.8×) | 80 | 0 B / 0 s | 9,072 | 5,136 |
| 100 (600) | keyed (main) | EPYC 9V74 | 20.44 / 23.06 | 209 (3) | 1% (3/209); p95 1.08 (6.0×) | 46% (97/209); p95 1.31 (136.5×) | 1% (3/209); p95 1.09 (9.9×) | 90 | 0 B / 0 s | 8,072 | 2,816 |

**Reading.** The engine's work per event is about the same at every size: 0.20–0.32 s per event,
the whole import 200–320 s against S's 2–66 s. A larger commit only gives other families fewer
turns.

- **1 event per commit:** every bystander request succeeds, but each waits behind one commit's
  work (p95 0.38–0.48 s, against 0.03 s on S).
- **10 events:** receipts are 1.9–2.7 s, and 5–6% (keyed) or 19–22% (off) of drains and live
  changes time out at 2 s.
- **50 and 100 events:** receipts are 12–31 s, and nearly every drain and live change times out.
  Keyed is a little faster than off at every size (0.87–1.05×), because main still leaves the
  stalling shapes on subquery filters.

## Proposal

**The size, under main's lowering today (both filter states).** 1 event (6 tracked rows) per
commit. It is the only size where other readers stay served. They are not within 1.1× S (their p95
is 7–26× S, though well inside the 2 s limit), and no size meets that. 10 events is the largest
size with a 2–3 s receipt, but it loses drains.

| At 1 event per commit | flags off | keyed (main) |
| --- | ---: | ---: |
| Receipt p50 / p95 | 0.28 / 0.57 s | 0.26 / 0.37 s |
| 500-event first import (receipt waits included) | about 160 s (2.7 min) | about 139 s (2.3 min) |
| 5,000-event first import | about 27 min | about 23 min |
| First near-term events on the phone (first commit, near-term window first) | about 0.3 s | about 0.3 s |
| Per 100 near-term events (−7 to +60 days, committed first) | about 32 s | about 28 s |

At 10 events per commit, 500 events take about 116 s (off) / 102 s (keyed) and 5,000 about 19 / 17
minutes. Near-term events arrive in 2–2.5 s batches of 10, with the drain losses above.

**With the three pieces keyed (keyed-all):** 100 events (600 tracked rows) per commit, the largest
size measured. Every reader is served, receipts are 0.16 s at p50 and 0.30 s at p95, the whole
1,000 events take about 3 s, a 500-event import about 1.5 s and a 5,000-event import about 15 s.
The first near-term events arrive after one commit (about 0.2 s), and 100 near-term events in one
commit.

All wall times scale each cell's measured 1,000-event time linearly. They are estimates.

## sweep-keyed-all: the three pieces that stay subquery, keyed too

**Lowering 1** is main's, the sweep's keyed column above (reused, not rerun). **Lowering 2**
(`S-main3-keyed-all`, `simple_keys.keyed_all_lowering`) keys three more pieces:

- **`graph_edge`:** main's `keyed_authorization_predicate(table="graph_edge")`, which is PR 8879's
  `_keyed_graph_edge_predicate` as merged into main, lowered but not served at `52e79b875b`
  (`detached_at` and `partner_hidden` included). It is ANDed with the phone's edge predicate. For
  the day-window attendee members, the phone's `in` into `record_date_index` is dropped
  (**hypothetical**: an edge has no date, so the member reads the account's attendee edges).
- **`assignment` (hypothetical, OTTO-14247's plan):** main's index-table keyed filter
  (`keyed_authorization_predicate(table="record_date_index")`), applied to `assignment`. That is
  `space_id` in the caller's Spaces, or `record_id` in its shared Records, on columns `assignment`
  has. Rail members keep their `record_id` list. Day-window members drop their `in` and read the
  account's assignments.
- **`record_date_index` members:**
  - `allRecordDateIndicesForDayWindow` keeps its own date window and drops its branch into
    `record_recurrence`, keyed by main.
  - `recurrenceDateIndicesForDayWindow`, whose window lives only in `record_recurrence`, becomes
    the account's keyed index stream (**hypothetical**).
- **Day-window `record` members:** the account's keyed Record stream, by main's `record` lowering
  (**hypothetical**: a Record has no date of its own, so the window cannot be kept on it).

**What stays subquery:** 180 shapes. They are `resource`, `resource_fact`, `user` and `calendar`,
which are outside the keyed program, and the 44 keyed run threads with their one `workflow` node.
Main's lowering leaves 2,816 subquery shapes; flags off leaves 5,136.

| Events per commit (tracked rows) | Lowering | Runner | Receipt p50 / p95 s | Import wall s (S) | Bystander drain (p95 × S) | Subset (p95 × S) | Live change (p95 × S) | CPU % | Sequencer held peak / longest run | Backfills | Subquery shapes |
| --- | --- | --- | ---: | ---: | --- | --- | --- | ---: | --- | ---: | ---: |
| 1 (6) | keyed-all | EPYC 9V45 | 0.05 / 0.05 | 62 (61) | 100% (61/61); p95 0.03 (1.0×) | 100% (61/61); p95 0.01 (1.6×) | 100% (61/61); p95 0.03 (1.0×) | 12 | 0 B / 0 s | 0 | 180 |
| 10 (60) | keyed-all | EPYC 9V45 | 0.05 / 0.05 | 8 (7) | 100% (7/7); p95 0.08 (2.5×) | 100% (7/7); p95 0.01 (2.1×) | 100% (7/7); p95 0.06 (2.3×) | 25 | 0 B / 0 s | 0 | 180 |
| 50 (300) | keyed-all | EPYC 7763 | 0.15 / 0.16 | 5 (3) | 100% (5/5); p95 0.05 (1.7×) | 100% (5/5); p95 0.02 (1.6×) | 100% (5/5); p95 0.08 (2.4×) | 40 | 0 B / 0 s | 0 | 180 |
| 100 (600) | keyed-all | EPYC 9V45 | 0.16 / 0.30 | 3 (2) | 100% (2/2); p95 0.06 (2.5×) | 100% (2/2); p95 0.01 (1.0×) | 100% (2/2); p95 0.04 (2.1×) | 41 | 0 B / 0 s | 0 | 180 |
| unpaced | keyed-all | EPYC 9V74 | 0.00 / 0.00 | 6 (4) | 33% (2/6); p95 0.02 (0.0×) | 100% (6/6); p95 0.16 (6.0×) | 50% (3/6); p95 0.97 (1.2×) | 60 | 0 B / 0 s | 240 | 180 |

Same-runner S for the keyed-all jobs:

| Events per commit | S import wall | S bystander drains |
| --- | --- | --- |
| 1 | 61 s | 61/61 |
| 10 | 7.2 s | 7/7 |
| 50 | 3.0 s | 3/3 |
| 100 | 2.1 s | 2/2 |
| Unpaced (13,081 changes, six commits) | 3.45 s | 4/4, p95 0.90 s |

**Is pacing still needed once the three pieces are keyed?** Not at any measured size.

- **Paced:** at 1–100 events per commit, every bystander drain, subset and live change succeeds,
  and receipts take 0.05–0.30 s with zero move-in queries. The import finishes within a second or
  two of S (1.0–1.7×, on 2–7 probe attempts at the larger sizes).
- **Unpaced:** the harness's import, about 2,180 events' worth in six large commits, still drops
  bystander requests for its 5.4 s burst: drains 2 of 6, live changes 3 of 6, against 4 of 4 on S.
  Its 240 move-ins come from `resource` and `resource_fact`, which stay outside the keyed program.
- **So pacing guards the transition.** It is needed while main's lowering leaves those pieces on
  subquery filters, where only 1 event per commit keeps readers served. Once they are keyed, a
  bound like 100 events per commit is cheap (about 15 s for 5,000 events) and still covers
  unpaced bursts.

**Largest size within about 1.1× S with a 2–3 s receipt, per lowering:**

| Lowering | Largest size | 500-event import | 5,000-event import |
| --- | --- | ---: | ---: |
| Main today (keyed flags on) | None within 1.1× S. 1 event per commit serves everyone; 10 is the largest with a 2–3 s receipt but loses drains | 139 s at 1, 102 s at 10 | 23 min at 1, 17 min at 10 |
| Main today, flags off | Same | 160 s at 1, 116 s at 10 | 27 min at 1, 19 min at 10 |
| Keyed-all | 100 events (600 tracked rows) or more: receipt 0.16 / 0.30 s, every reader served, bystander p95 a few hundredths of a second (1.0–2.5× S's) | about 1.5 s | about 15 s |

## 2 CPU

Actions run `45878203381`, harness `69e396a` (`[paired-13]`). The same paced first import of 1,000
events at 1 and 10 events per commit, the same bystander load (S-main3's full catalog), Nov 24
catalog, engine `9bc1563`, 50 ms receipt polling.

**How the engine gets 2 CPUs.** `run.py --cpus 2` starts it under systemd with `CPUQuota=200%`
from process start, on 4-core runners. That is how `--cpus 1` pins 1 CPU (`CPUQuota=100%`), and
each run's `cpu.max` records the quota. Prod's 7 GB memory is not limited in the harness.

**Pairing.** Each job ran S and both of main's flag states on one runner, and a second job reversed
the order. I can't read prod's flag state from here (no prod access), so both are reported.

**The bar** for other readers: no timeouts, and drain, subset and live-change p95 each under 0.5 s.

| Events per commit (tracked rows) | Lowering | Runner | Timeouts (all probes) | Drain p95 s | Subset p95 s | Live p95 s | CPU % (one core) | Sequencer held peak / longest | Receipt p50 / p95 s | Bar |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| 1 (6) | S | EPYC 7763 | 0 | 0.03 | 0.01 | 0.03 | 5 | 0 B / 0 s | 0.05 / 0.05 | **pass** |
| 1 (6) | main, flags off | EPYC 7763 | 0 | 0.42 | 0.02 | 0.36 | 86 | 0 B / 0 s | 0.26 / 0.42 | **pass** |
| 1 (6) | main, keyed flags on | EPYC 7763 | 0 | 0.34 | 0.02 | 0.36 | 92 | 0 B / 0 s | 0.31 / 0.41 | **pass** |
| 1 (6) | main, keyed flags on | Xeon 6975P-C | 0 | 0.33 | 0.01 | 0.22 | 88 | 0 B / 0 s | 0.16 / 0.26 | **pass** |
| 1 (6) | main, flags off | Xeon 6975P-C | 0 | 0.33 | 0.01 | 0.25 | 84 | 0 B / 0 s | 0.19 / 0.31 | **pass** |
| 1 (6) | S | Xeon 6975P-C | 0 | 0.03 | 0.01 | 0.02 | 4 | 0 B / 0 s | 0.05 / 0.05 | **pass** |
| 10 (60) | S | EPYC 9V45 | 0 | 0.03 | 0.01 | 0.03 | 6 | 0 B / 0 s | 0.05 / 0.05 | **pass** |
| 10 (60) | main, flags off | EPYC 9V45 | 0 | 1.55 | 0.01 | 1.49 | 102 | 0 B / 0 s | 1.59 / 1.73 | fail |
| 10 (60) | main, keyed flags on | EPYC 9V45 | 0 | 1.15 | 0.01 | 1.13 | 103 | 0 B / 0 s | 1.22 / 1.28 | fail |
| 10 (60) | main, keyed flags on | Xeon 6975P-C | 0 | 1.54 | 0.01 | 1.44 | 102 | 0 B / 0 s | 1.53 / 1.64 | fail |
| 10 (60) | main, flags off | Xeon 6975P-C | 4 | 1.95 | 0.01 | 1.89 | 101 | 0 B / 0 s | 1.02 / 2.00 | fail |
| 10 (60) | S | Xeon 6975P-C | 0 | 0.02 | 0.01 | 0.02 | 7 | 0 B / 0 s | 0.05 / 0.05 | **pass** |

**At 1 event per commit, both flag states pass at 2 CPUs on both runners.** There are no timeouts;
drain p95 is 0.33–0.42 s, live-change p95 0.22–0.36 s, and subset p95 0.01–0.02 s. On 1 CPU the
same cell measured drain p95 0.38–0.48 s, live 0.28–0.47 s and subset 0.19–0.26 s. The second CPU
helps subsets the most. Drains and live changes gain only about 0.05–0.1 s, because the engine's
serialized membership path still uses about one core (84–92% of one core under a 200% quota). The
margin under 0.5 s stays thin: 0.08–0.17 s on drains.

**At 10 events per commit, both fail at 2 CPUs.** With the keyed flags on there are no timeouts,
but drain p95 is 1.15–1.54 s. With flags off, one runner also times out 2 drains and 2 live
changes. Receipts take 1.0–1.6 s at p50 and 1.3–2.0 s at p95. The engine sits at 101–103% of one
core.

The sequencer-held gauges stayed at 0 throughout.
