# W1-NR: the 2,000-account cell without the per-page state rebuild (OTTO-14463)

**Status: running, no result yet (written 2026-11-27 12:02 UTC so the work is not only in a sandbox).**
This file is replaced by the answer when the cell lands.

## What is pushed

- **Engine change:** branch `profile/w1-no-state-rebuild`, cut from prod's binary `9bc1563`. Harness
  only, never merged, nothing deployed.
  - `273b8b78`: the test and a call counter only. It should fail on today's code.
  - `19f3f834`: the change. It has not been compiled outside CI.
- **Which form:** on request. `publish_all` (`apps/engine/src/engine/sequencer.rs`) publishes
  per-table stats every time and builds the node-state map only when `/trace` has a subscriber.
  `GET /state` (`Engine::state_snapshot`, `introspection.rs`) asks the sequencer for a fresh map with
  a new `SequencerCmd::NodeStates`, as `DumpNode` does, and falls back to the last map broadcast if
  the sequencer does not take the command.
- **Diff against `9bc1563`:** `sequencer.rs` +39 −15, `introspection.rs` +24 −6, `tests.rs` +30.
- **Harness:** `9c70c632` on `profile/w6-list-keys` builds both engine commits, runs the engine
  module's unit tests on each (`cargo test --release --features test-support --lib engine::`, output
  in the artifact `stall-historical-build-<sha>/tests.txt`), and runs two cells.

## The engine tests (measured in CI, 12:05 UTC)

`cargo test --release --features test-support --lib engine::` on each commit; full output in
`w1-nr-engine-tests/` beside this file.

- `273b8b78` (test only): the new test fails as intended, `left: 1, right: 0` on "no subscriber: the
  node-state map must not be rebuilt". 382 passed, 1 failed, 5 ignored.
- `19f3f834` (the change): 383 passed, 0 failed, 5 ignored. The new test passes.

## The run in flight

Actions run `45994130788` (started 11:53 UTC):

- `w1-2000-nr`: W1's 2,000-account cell on the changed engine, `perf` on the engine in both phases.
- `w1-2000-nr-pair`: the changed engine and then prod's binary on one runner, same cell.

## To carry on from a fresh checkout

1. Collect: `STALL_REPRO_ATTACHMENT=<repo attachment> bash tools/stall-repro/collect.sh 45994130788 <dir>`.
2. Read `tests.txt` for both commits: the new test
   `publish_all_without_a_trace_subscriber_does_not_build_node_states` fails on `273b8b78` and
   passes on `19f3f834`; the other `engine::` tests pass on `19f3f834`.
3. If the build of `19f3f834` fails: fix on the engine branch with a new commit, put the new shas in
   `.github/workflows/stall-repro.yml` (the `[nr-build]` matrix, the `nr:` mapping and `SOURCE[nr]`)
   and push with `[nr-build] [paired] [paired-29]` in the commit message.
4. Retain: `python3 tools/stall-repro/retain_results.py <dir> tools/stall-repro/results/simple-keys`.
5. From each `w1-load.folded.gz` and `w1-control.folded.gz`: the share of samples under
   `sequencer::publish_all`, under `query_subset`, and in the sequencer loop outside `publish_all`.
   Commits in a phase are the writers' receipts plus two per bystander drain attempt;
   `publish_all` CPU per commit is engine CPU seconds times its share, over commits.
6. Fill this table and its loads-only twin, answer first:

| | Today (W1-PROFILE) | Without the per-page rebuild |
| --- | --- | --- |
| `publish_all` share, combined phase | 69.5% | |
| `publish_all` CPU per commit | 48 ms | |
| Loads answered of those due, combined phase | 10,601 of 26,586 | |
| Engine CPU, both phases | 93% / 95% | |
| Bystander receipt p95 | 0.19–0.27 s | |
| Import and deletion wall time | 21–27 s / 159–200 s | |

Loads-only, today: `publish_all` 40.7%, 48 ms a commit, 14,529 of 14,529 loads answered.
