# OTTO-13916 continuation checkpoint — 2026-11-24 09:30 UTC

Not the final verdict. M1, M2 and M3 are pushed; M4 is pending. All execution is
isolated GitHub Actions, never the Mighty Run or prod/dev. No PR exists. Remain
on `profile/stall-repro-2026-11-24`; no merge/review/deploy/rebase/force-push.
Indexed and the attached proposal graph remain read-only. Linear was claimed.

## Next actions

1. At the durable wake, collect completed Actions evidence once, not in a polling
   loop. Mighty refused repeated GET `/actions/runs/{n}/jobs` reads with HTTP 429
   (`repeated_read`) at 09:26 UTC. No access request is available or pending.
2. Verify observer build **45363678502** succeeded, with a binary hash and focused
   `stall_` tests. Its engine source is
   **0bdc7a2f6e5614f87246b66534a786c5aea792b6**. At checkpoint it was compiling/testing,
   not failed. `collect.sh` downloads its build evidence, not the huge binary.
3. If verified, launch `[measure] [final-variants]` on a code/workflow-changing
   commit (empty/doc-only pushes may be path-filtered). The current workflow is
   already prepared and pins that artifact. It runs commit-membership, then
   half-subqueries, then plain-only at 1 CPU, all three workloads, max-parallel 1.
   A small useful next code change is a final matrix renderer including
   engine-clock throughput and explicit success percentages. Do not rebuild or
   replace the immutable production binary. If the batch build failed, obtain
   its completed job logs, fix narrowly or record that variant blocked.
4. Collect every pending row below and retain with `retain_results.py`. Inspect
   any incomplete/failed variant; do not hide it or stop all other variants.
5. Produce ONE full variant/workload matrix in RESULTS.md with commands, full
   engine/harness provenance, first-read time/rate, CPU, backfills, warnings and
   all three probe success rates/latencies. Compare improvements against the
   **unprofiled A** control, not the 529 s perf run. Mark single-run uncertainty.
6. Write the three plain verdict answers, limitations and M4, commit/push; update
   Linear with final evidence and reply with M1–M4 lines. No review or merge.

## Live/completed run ledger

| Run | Harness | Work | Checkpoint status |
| --- | --- | --- | --- |
| 45357274431 | 8a60659 | Corrected profiled prod A, Nov23/Nov24 | Complete, retained `baseline-8a60659/` |
| 45360019209 | 346e9f9 | Old observer894b import wall times | Complete, retained `observer-346e9f9/` |
| 45362175258 | b187502 | Unprofiled unchanged A plus first failed B attempts | A still running at last collection; failed B409 is disclosed |
| 45363301660 | 1489d8b | Fixed B and B-retained | Both complete, retained `variants/` and B milestone folder |
| 45362388808 | 33929b5 | A2/A4 CPU, observer cbb control, unsafe marker bypass, historical builds | A2 complete retained; other measurements pending; both historical builds passed |
| 45363890232 | 9f5ad6d | PR68 base1be10fb vs head3a0ce40 | Both measurements running at last check |
| 45363678502 | 0bdc7a2 | Per-commit scratch Rust build and focused tests | Build/test in progress at last check |
| 45363068569 | 57b23ec | Red batching-cost assertion on immutable real observer evidence | Passed expected-red check (11941 calls exceeds bound) |

## Collection commands

Run from the engine repo. These download/analyze already remote-produced data,
not local engine tests:

```sh
bash tools/stall-repro/collect.sh RUN_ID /tmp/stall-RUN_ID
python3 tools/stall-repro/retain_results.py /tmp/stall-RUN_ID tools/stall-repro/results/variants
python3 tools/stall-repro/summarize_results.py tools/stall-repro/results/variants
```

Existing temporary downloads: `/tmp/stall-baseline33`, `/tmp/stall-observe41`,
`/tmp/stall-B43`, `/tmp/stall-cheap44`, `/tmp/stall-B46`, `/tmp/stall-batchbuild47`.
Re-running the collector after completion gets new artifacts; do not repeatedly
poll artifacts either. Completed job logs require a GitHub signed redirect;
in-progress logs return404. Save raw Mighty responses before jq so refusals are
visible. `.headers` is an array with `{name,value}`, not an object. `gh` is not
authenticated; use the attached repository's Mighty GitHub App.

## Measured facts to preserve

- Profiled prod Nov24 import: first delivery→empty528.49 s, SQL529.15 s,
  CPU98.50%, backfills15420, drain/live0/529, subset362/529. Nov23 first165.12 s.
  Partial mechanism reproduction, not incident duration: Nov23 also starves,
  and no pinned warnings occur. The pinned store0.1.5 lacks prod4MiB read pages.
- Retained CPU profile: 51471 samples/529 s; owning-operation split68.736%
  `on_table_delta`,19.265% flip walks,8.121% memory sampler,0.822%PG client,
  0.305%appends,0 marker CPU. Inner `MembershipCircuit::contains`76.857% overlaps.
  The first deepest-frame classifier conflated flip membership and sampler mutex
  symbols; use `cpu-responsibility.json`, not its original rough bucket labels.
- Observer894b run373 s: membership276.529 s, registry hold276.606 s,
  sequencer lock wait16.905 s, marker wait82.265 s; flip hold69.690 s,
  flip wait570.797 worker-seconds,2220 batches/6780 walks/peak8. Overlapping wall
  times are NOT CPU percentages. No perf in observer run; runner/code/perf
  differences are not controlled enough for a causal timing ratio.
- B all-plain: import3.18 s, large deletion1.79 s, small0.29 s after delivery;
  zero backfills. Import probes3/3 each; large drain/live8/9, subset9/9.725
  equality-family shapes, zero subqueries. No strict two-second isolation proof.
- B-retained: import148.42 s, CPU98.94%,7020 backfills, drain/live0/148,
  subset107/148; deletion34.67 s and small58.98 s. Retained chat/edge/Resource/
  File/User scopes still cause starvation. Do NOT sell ideal B as full proposal.
- A2 CPU: import323.59 s, CPU102.64% of ONE core, drain/live0/324,
  subset324/324,p95.011 s; large91.81 s, small159.15 s. More CPU protects serving,
  does not double serial membership throughput or eliminate drain dependence.

## Prototype caveats

`MEASUREMENT_PATCHES.md` is authoritative. Per-commit scratch code is in
`stall_batch.rs` + sequencer: first-old/last-new per key, once per table,
512MiB charged-image/100000-key bound, fail closed on overflow. It does NOT
implement time slicing/spill, fair coalesced rounds, or crash recovery. One walk
batch per table changes walk parallelism; per-value backfills remain. Large
table batches may amplify candidate-row allocations; no deployment claim.
Default flag unset preserves row path. The harness now records engine exit
promptly, instead of waiting an hour and losing evidence. If it fails/OOMs,
record the failure; do not silently reduce the workload.

Half/plain controls keep10571 shapes but are NOT authorization-equivalent:
replaced subqueries use literal owner/Space equality or one synthetic key.
Half retains2144 subqueries including8 workload/probe feeds. Nullable owner
columns can match fewer rows. B instead adds and maintains actual key columns
and intentionally collapses historical requests. All schema FK/cascade triggers
are disabled for synthetic writes; Loro payloads/statement mix are not faithful.

PR68 is compared only with its own base; prod differs from that base by PR70,
71,72. Historical binary hashes are in `/tmp/stall-cheap44/stall-historical-build-*`.
No PR is needed for CI. Any normal code push runs the fixture tests; doc/results
pushes are path-filtered. Avoid `[engine]` for scratch code; production build is
now explicitly archived from5b2cf3e, and variants use separate immutable artifacts.
