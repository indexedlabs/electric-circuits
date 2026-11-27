# W1-NR: the 2,000-account cell without the per-page state rebuild (OTTO-14463)

**Answer.** The rebuild was the bend. Without it `publish_all` takes 1% of the engine's CPU under
the combined load, down from 70%, and 0.5 ms a commit, down from 48–50 ms: about 1% of the
per-commit cost is left. Every load is answered, 19,459 of 19,459, where today's engine on the
same runner answered 8,962 of 28,824.

| Loads, import and deletion | Today (W1-PROFILE) | Today, same runner as the next column | Without the per-page rebuild |
| --- | ---: | ---: | ---: |
| `publish_all` share, combined phase | 69.5% | 69.6% | 1.0% |
| `publish_all` CPU per commit | 48 ms | 50 ms | 0.5 ms |
| Loads answered of those due, combined phase | 10,601 of 26,586 | 8,962 of 28,824 | 19,459 of 19,459 |
| Engine CPU, loads only / combined | 93% / 95% | 93% / 95% | 85% / 83% |
| Bystander receipt p95 | 0.19–0.27 s | 0.25 s | 0.16 s |
| Import and deletion wall time | 21–27 s / 159–200 s | 31 s / 222 s | 36 s / 139 s |

| Loads only | Today (W1-PROFILE) | Today, same runner as the next column | Without the per-page rebuild |
| --- | ---: | ---: | ---: |
| `publish_all` share | 40.7% | 42.4% | 0.5% |
| `publish_all` CPU per commit | 48 ms | 49 ms | 0.5 ms |
| Loads answered of those due | 14,529 of 14,529 | 10,350 of 13,842 | 15,958 of 15,958 |
| Bystander receipt p95 | 0.15 s | 0.16 s | 0.08 s |

- **Measured three times:** the changed engine ran the cell on two runners (0.9% and 1.0%; all
  19,416 and all 19,459 loads answered), and today's engine ran it again on the second of them
  (69.6%), back to back with the changed one.
- **What the engine's CPU goes to now:** the screen loads. Subset queries are 53% of its samples
  under the combined load and 55% with loads only; the sequencer loop outside `publish_all` is
  about 1% and 0.1–0.2%.
- **The engine is still busy:** 83–85% of one CPU at 2,000 accounts, almost all of it serving 732
  load requests a second. That is the read cost W1 measured, now the only large term, so 2,000
  accounts at this load rate and row size is near what one CPU serves.
- **Loads are answered but not fast in these cells:** 1.0 s at p50 and 1.2–1.3 s at p95, with the
  schedule up to 17 s late under the combined load. The runner is the likely limit, not the
  engine: its four cores were 84–86% busy on average with the changed engine (53–63% with today's,
  which serves a third of the requests), carrying Postgres, the load generator and the profiler
  beside the engine. An unprofiled cell on a faster runner would say; none was run.
- **Not asked, and large:** bringing up the 76,114 feeds took 177–180 s from engine start to the
  first phase on the changed engine, against 1,455–1,528 s on today's. Each feed creation calls
  `publish_all` too (read, not verified: the publish after each create in `sequencer.rs`), so
  today's cost of bringing phones' feeds up grows with the square of their number.
- **The import is not faster:** 36 s on the changed engine against 31 s for today's on the same
  runner, though each commit's receipt is quicker (p95 0.25 s against 0.38 s). The import's writer
  and its 160 MB of rows share the busy runner with 732 load requests a second, which today's
  engine was not serving. The deletion is faster: 139 s against 222 s.

## The engine change

Branch `profile/w1-no-state-rebuild`, cut from prod's binary `9bc1563`, head `19f3f834`. Harness
only: never merged, nothing deployed. OTTO-14463 carries the real change.

- **Which form: on request.** `publish_all` still publishes per-table stats every time
  (`GET /tables/{t}/families` reads them). It builds the node-state map only when `/trace` has a
  subscriber. `GET /state` asks the sequencer for a fresh map with a new `SequencerCmd::NodeStates`,
  as `DumpNode` does, and falls back to the last map broadcast if the sequencer does not take the
  command.
- **Diff against `9bc1563`:** `sequencer.rs` +39 −15, `introspection.rs` +24 −6, `tests.rs` +30.
- **Test first, measured in CI** (`cargo test --release --features test-support --lib engine::`,
  output in `w1-nr-engine-tests/` beside this file):
  - `273b8b78`, the test and a call counter only: the new test
    `publish_all_without_a_trace_subscriber_does_not_build_node_states` fails as intended (`left: 1,
    right: 0`); 382 passed, 1 failed, 5 ignored.
  - `19f3f834`, the change: 383 passed, 0 failed, 5 ignored.
- **Not tested:** `GET /state` against a running engine, and `/trace` with a live subscriber beyond
  the unit test. The harness reads neither.

## The run

W1's 2,000-account cell as before: 76,114 feeds, 120 s of loads only, then the loads with a
5,000-event import and a prod-p95 family deleted, 1 CPU, `perf record -e cpu-clock -F 99 -g
--call-graph fp` on the engine in both phases. Actions run `45994130788`, harness `9c70c632` on
`profile/w6-list-keys`. One job ran the changed engine alone; a second ran the changed engine and
then prod's binary `9bc1563` on one runner. Folders `paired-w1-2000-nr-*` and
`paired-w1-2000-profile-*` beside this file; tables by `tools/stall-repro/w1_nr_report.py`.

## Every measured column

**Loads, import and deletion**

| | Today, W1-PROFILE (EPYC 9V74) | Today, same runner as the next column (EPYC 7763) | Without the rebuild, same runner (EPYC 7763) | Without the rebuild (EPYC 7763) |
| --- | ---: | ---: | ---: | ---: |
| `publish_all` share of engine CPU | 69.5% | 69.6% | 1.0% | 0.9% |
| `publish_all` CPU per commit | 48.5 ms | 50.1 ms | 0.5 ms | 0.5 ms |
| Subset queries' share (the loads) | 15.1% | 14.7% | 53.0% | 52.7% |
| Sequencer loop outside `publish_all` | 0.7% | 0.6% | 0.9% | 1.0% |
| Engine CPU (of 1) | 95% | 95% | 83% | 82% |
| Loads answered of those due | 10,601 of 26,586 | 8,962 of 28,824 | 19,459 of 19,459 | 19,416 of 19,416 |
| Loads not started within 30 s | 15,985 | 19,862 | 0 | 0 |
| Load time p50 / p95 | 3.07 s / 3.84 s | 3.98 s / 4.50 s | 1.05 s / 1.28 s | 1.04 s / 1.29 s |
| Loads started late by, p95 | 30.10 s | 30.11 s | 16.87 s | 16.54 s |
| Requests served a second | 291 | 227 | 732 | 732 |
| Bystander receipt p95 (drains ok/attempts) | 0.27 s (933/933) | 0.25 s (1003/1003) | 0.16 s (670/670) | 0.16 s (668/668) |
| Bystander live change p95 | 0.25 s (933/933) | 0.22 s (1003/1003) | 0.15 s (670/670) | 0.15 s (668/668) |
| Import wall time (commit receipt p95) | 26 s (0.44 s) | 31 s (0.38 s) | 36 s (0.25 s) | 36 s (0.24 s) |
| Deletion wall time (commit receipt p50) | 206 s (0.160 s) | 222 s (0.169 s) | 139 s (0.062 s) | 139 s (0.062 s) |
| Phase length; commits; samples | 233 s; 3,158; 21,748 | 251 s; 3,298; 23,408 | 167 s; 2,632; 13,560 | 167 s; 2,628; 13,491 |

**Loads only**

| | Today, W1-PROFILE (EPYC 9V74) | Today, same runner as the next column (EPYC 7763) | Without the rebuild, same runner (EPYC 7763) | Without the rebuild (EPYC 7763) |
| --- | ---: | ---: | ---: | ---: |
| `publish_all` share of engine CPU | 40.7% | 42.4% | 0.5% | 0.5% |
| `publish_all` CPU per commit | 47.5 ms | 49.2 ms | 0.5 ms | 0.6 ms |
| Subset queries' share (the loads) | 32.1% | 30.0% | 55.5% | 55.4% |
| Sequencer loop outside `publish_all` | 0.2% | 0.2% | 0.1% | 0.2% |
| Engine CPU (of 1) | 93% | 93% | 85% | 85% |
| Loads answered of those due | 14,529 of 14,529 | 10,350 of 13,842 | 15,958 of 15,958 | 15,970 of 15,970 |
| Loads not started within 30 s | 0 | 3,492 | 0 | 0 |
| Load time p50 / p95 | 1.19 s / 1.30 s | 1.67 s / 1.80 s | 0.96 s / 1.17 s | 0.99 s / 1.23 s |
| Loads started late by, p95 | 23.43 s | 30.05 s | 4.37 s | 7.39 s |
| Requests served a second | 731 | 544 | 734 | 734 |
| Bystander receipt p95 (drains ok/attempts) | 0.15 s (543/543) | 0.16 s (544/544) | 0.08 s (504/504) | 0.09 s (517/517) |
| Bystander live change p95 | 0.11 s (543/543) | 0.12 s (544/544) | 0.08 s (504/504) | 0.08 s (517/517) |
| Phase length; commits; samples | 136 s; 1,086; 12,409 | 136 s; 1,088; 12,431 | 126 s; 1,008; 10,367 | 129 s; 1,034; 10,625 |

