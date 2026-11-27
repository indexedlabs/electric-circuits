# W1 profile: where commit time goes at 2,000 accounts (Main, via circuits-engine-watch)

**Answer.** The per-commit cost is real, and it is not the filter lookup. After every page of
commits the engine's sequencer rebuilds a state summary for its visualizer, and that rebuild walks
every shape the engine holds. At 76,114 shapes it costs about 48 ms of engine CPU per commit, and
it is 70% of the engine's CPU in the combined phase.

- **Measured (profile):** `sequencer::publish_all` holds 69.5% of the engine's CPU samples in the
  combined phase and 40.7% with loads only. Subset queries, the screen loads, hold 15.1% and 32.1%.
- **Measured (profile):** the rest of the sequencer loop, which includes finding a changed row's
  shapes and writing to their streams, holds 0.7% and 0.2%. The hash lookup is as cheap as the
  engine rule says; W1's fit was right about the size of the cost and silent about its cause.
- **Per commit:** 153 s of engine CPU in `publish_all` over about 3,160 commits is 48 ms a
  commit, and 52 s over about 1,090 is 48 ms with loads only. W1's fit said 0.5 µs a shape a
  commit, 38 ms at 76,114 shapes.
- **It walks every family shape on every publish. Read, not verified by a counter:**
  - `apps/engine/src/engine/sequencer.rs:206-232` (`publish_all`) loops over every table's
    executor, calls `build_node_states` for each, merges the results into one map
    (`merged.extend`, line 219) and then clones the whole map (`merged.clone()`, line 232).
  - `apps/engine/src/engine/introspection.rs:561-618` (`build_node_states`) inserts one entry per
    standalone shape (lines 578-582) and one per family shape (lines 589-594:
    `for sid in family_of.keys()`, each a `format!("shape:{sid}")` and a map insert).
  - `apps/engine/src/engine/sequencer.rs:1127-1128`: the loop calls `publish_all` after each page
    of transactions that touched anything; five other call sites in the same loop do it after
    shape and group changes.
  - Engine main `46d15563` has the same loop (`sequencer.rs:217-232`, `introspection.rs:592-597`).
- **What it serves (read):** the merged map is what `GET /state` returns and what a `/trace`
  subscriber is sent. It is built whether or not anyone reads it; only the broadcast is skipped
  when no one is subscribed (`sequencer.rs:233-235`).
- **So for the target design:** the bend W1 found at 1,600–2,000 accounts is this rebuild, times
  small paced commits, not plain-key filters. Everything else the engine did in the combined
  phase, while serving 40% of the loads asked of it, took about 30% of its CPU. What the cell
  would do without the rebuild is not measured.

Engine `9bc1563` (prod's binary, built with frame pointers), 1 CPU, 2,000 active accounts, 76,114
shapes. `perf record -e cpu-clock -F 99 -g --call-graph fp` on the engine for each whole phase.
Actions run `45975936329`, harness `14ac7628` (branch `profile/w6-list-keys`). Folder
`paired-w1-2000-profile-1-W1-2000-prod9bc-1cpu` beside this file holds the folded stacks
(`w1-load.folded.gz`, `w1-control.folded.gz`) and flame graphs (`w1-load.svg`, `w1-control.svg`).

## Where the engine's CPU goes

| | Loads only | Loads, import and deletion |
| --- | ---: | ---: |
| Phase length | 136 s | 233 s |
| Engine CPU (of 1) | 93% | 95% |
| Samples | 12,409 | 21,748 |
| `sequencer::publish_all` | 40.7% | 69.5% |
| Subset queries (`http::query_subset` and below) | 32.1% | 15.1% |
| Other HTTP work (feeds, receipts, probes) | 16.4% | 8.3% |
| Sequencer loop outside `publish_all` | 0.2% | 0.7% |
| Everything else | 10.6% | 6.4% |
| Commits in the phase (writers plus the bystander's probe) | about 1,090 | about 3,160 |
| `publish_all` CPU per commit | 48 ms | 48 ms |
| Loads answered of those due | 14,529 of 14,529 | 10,601 of 26,586 |

Commits are counted from the writers' receipts and the probe's two commits a tick; a page can hold
more than one commit, so 48 ms is per commit, not per call.

## Inside `publish_all`, combined phase

| Share of all engine CPU | Where | What it is |
| ---: | --- | --- |
| 27.9% | `introspection::build_node_states` | one map entry per shape |
| 17.9% | → `alloc::fmt::format::format_inner` | `format!("shape:{sid}")` for every shape |
| 7.2% | → `HashMap::insert` | inserting each entry |
| 1.8% | → `HashMap::get` | each shape's emitted count |
| 24.6% | `HashMap::extend` in `publish_all` | merging each table's entries into one map |
| 8.5% | `clone` in `publish_all` | cloning the merged map for the shared handle |
| 6.7% | `drop` and the allocator's free | dropping the previous map |
| 0.7% | `introspection::stats_of` | per-table counts |

## What this does and does not show

- **Shown:** which functions hold the engine's CPU, by sampling at 99 Hz with frame pointers.
  20% of samples end in a frame `perf` could not name; they are counted under the caller it could
  name.
- **Not shown:** a count of how many shapes each call walks. The walk over every shape is read
  from the source at the lines above; the profile agrees with it (the time is in formatting and
  inserting, in proportion to shape count across W1's cells) but does not count iterations.
- **Not run:** the same cell with the rebuild skipped. That needs an engine change, which this
  task does not make.
- **The profiled cell is slower than W1's unprofiled ones** (loads only 93% against 85%; 40% of
  loads answered against 60%), on a slower runner and with the profiler attached.
