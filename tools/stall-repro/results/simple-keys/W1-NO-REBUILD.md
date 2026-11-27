# W1-NR: the 2,000-account cell without the per-page state rebuild (OTTO-14463)

**Answer.** The rebuild was the bend. Without it `publish_all` takes 0.9% of the engine's CPU under
the combined load, down from 69.5%, and 0.5 ms a commit, down from 48 ms: about 1% of the
per-commit cost is left. Every load is answered, 19,416 of 19,416, where today's engine answered
10,601 of 26,586.

| Loads, import and deletion | Today (W1-PROFILE) | Without the per-page rebuild |
| --- | ---: | ---: |
| `publish_all` share, combined phase | 69.5% | 0.9% |
| `publish_all` CPU per commit | 48 ms | 0.5 ms |
| Loads answered of those due, combined phase | 10,601 of 26,586 | 19,416 of 19,416 |
| Engine CPU, loads only / combined | 93% / 95% | 85% / 82% |
| Bystander receipt p95 | 0.19–0.27 s | 0.16 s |
| Import and deletion wall time | 21–27 s / 159–200 s | 36 s / 139 s |

| Loads only | Today (W1-PROFILE) | Without the per-page rebuild |
| --- | ---: | ---: |
| `publish_all` share | 40.7% | 0.5% |
| `publish_all` CPU per commit | 48 ms | 0.6 ms |
| Loads answered of those due | 14,529 of 14,529 | 15,970 of 15,970 |
| Bystander receipt p95 | 0.15 s | 0.09 s |

- **What the engine's CPU goes to now:** the screen loads. Subset queries are 53% of its samples
  under the combined load and 55% with loads only; the sequencer loop outside `publish_all` is 1.0%
  and 0.2%.
- **The engine is still busy:** 82–85% of one CPU at 2,000 accounts, almost all of it serving 732
  load requests a second. That is the read cost W1 measured (about 0.6 ms a request), now the only
  large term.
- **Loads are answered but not fast in this cell:** 1.0 s at p50 and 1.3 s at p95, and the
  schedule runs up to 17 s late. Here the runner is the likely limit, not the engine: its four
  cores were 84–85% busy on average (49–60% in W1-PROFILE's cell), with Postgres, the load
  generator and the profiler beside the engine. The same cell on today's engine on one runner is
  still running; it will say how much of that is the runner.
- **Not asked, and large:** creating the 76,114 feeds took 180 s from engine start to the first
  phase, against 1,455 s on today's engine. Each feed creation calls `publish_all` too (read, not
  verified: `sequencer.rs`, the publish after each create), so today's cost of bringing phones'
  feeds up grows with the square of their number.
- **The import took longer here (36 s against 21–27 s)** while its commits' receipts were faster
  (p95 0.24 s against 0.27–0.46 s). The import's writer shares the busy runner; the same-runner
  cell will show whether that holds.

**The columns are from different runners** (today: EPYC 9V74; without: EPYC 7763), so the shares
and the per-commit cost compare cleanly and the seconds and CPU percentages only roughly.

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
`profile/w6-list-keys`. Folders `paired-w1-2000-nr-*` and `paired-w1-2000-profile-*` beside this
file; tables by `tools/stall-repro/w1_nr_report.py`.

**Loads, import and deletion**

| | Today, W1-PROFILE (EPYC 9V74) | Without the rebuild (EPYC 7763) |
| --- | ---: | ---: |
| `publish_all` share of engine CPU | 69.5% | 0.9% |
| `publish_all` CPU per commit | 48.5 ms | 0.5 ms |
| Subset queries' share (the loads) | 15.1% | 52.7% |
| Sequencer loop outside `publish_all` | 0.7% | 1.0% |
| Engine CPU (of 1) | 95% | 82% |
| Loads answered of those due | 10,601 of 26,586 | 19,416 of 19,416 |
| Loads not started within 30 s | 15,985 | 0 |
| Load time p50 / p95 | 3.07 s / 3.84 s | 1.04 s / 1.29 s |
| Loads started late by, p95 | 30.10 s | 16.54 s |
| Requests served a second | 291 | 732 |
| Bystander receipt p95 (drains ok/attempts) | 0.27 s (933/933) | 0.16 s (668/668) |
| Bystander live change p95 | 0.25 s (933/933) | 0.15 s (668/668) |
| Import wall time (commit receipt p95) | 26 s (0.44 s) | 36 s (0.24 s) |
| Deletion wall time (commit receipt p50) | 206 s (0.160 s) | 139 s (0.062 s) |
| Phase length; commits; samples | 233 s; 3,158; 21,748 | 167 s; 2,628; 13,491 |

**Loads only**

| | Today, W1-PROFILE (EPYC 9V74) | Without the rebuild (EPYC 7763) |
| --- | ---: | ---: |
| `publish_all` share of engine CPU | 40.7% | 0.5% |
| `publish_all` CPU per commit | 47.5 ms | 0.6 ms |
| Subset queries' share (the loads) | 32.1% | 55.4% |
| Sequencer loop outside `publish_all` | 0.2% | 0.2% |
| Engine CPU (of 1) | 93% | 85% |
| Loads answered of those due | 14,529 of 14,529 | 15,970 of 15,970 |
| Loads not started within 30 s | 0 | 0 |
| Load time p50 / p95 | 1.19 s / 1.30 s | 0.99 s / 1.23 s |
| Loads started late by, p95 | 23.43 s | 7.39 s |
| Requests served a second | 731 | 734 |
| Bystander receipt p95 (drains ok/attempts) | 0.15 s (543/543) | 0.09 s (517/517) |
| Bystander live change p95 | 0.11 s (543/543) | 0.08 s (517/517) |
| Phase length; commits; samples | 136 s; 1,086; 12,409 | 129 s; 1,034; 10,625 |

