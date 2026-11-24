# OTTO-13916 continuation checkpoint — 2026-11-24 13:30 UTC (takeover, rounds 2–3 done)

M1–M4 are pushed in RESULTS.md. No Actions run on this branch is still running. No PR exists.
Nothing here is merged, deployed or reviewed. Linear has the takeover claim and the final comment.

## Run ledger (all collected and retained)

| Run | Harness | Work | Retained in |
| --- | --- | --- | --- |
| 45357274431 | 8a60659 | Profiled prod A, Nov23/Nov24 (EPYC 7763) | `baseline-8a60659/` |
| 45360019209 | 346e9f9 | Observer 894b import | `observer-346e9f9/` |
| 45362175258 | b187502 | Unprofiled A control (B attempt failed, 409) | `variants/A-nov24-1.0cpu-b187502` |
| 45363301660 | 1489d8b | B, B-retained | `variants/`, `model-B-1489d8b/` |
| 45362388808 | 33929b5 | A 2/4 CPU, A-observed, marker-bypass, PR68 builds | `variants/` |
| 45363890232 | 9f5ad6d | PR68 base/head | `variants/` |
| 45363678502 | 0bdc7a2 | Per-commit build + tests (passed) | not needed further |
| 45372211215 | caf9e2d | Bridge build + 13 cross-runner variants | `variants/*-caf9e2d` |
| 45377863650 | b4ab93e | 11 same-runner pairs (28 runs) | `paired/` |
| 45388936382 | 5169054 | Today-vs-yesterday, CPU split, PR 70–72 bisect, B-cold, E-only, E+R | `paired2/` |
| 45395594116 | ffc01e8 | B-cold attribution and profile, PR 70 profile, A edge-branch attribution | `paired3/` |

## If more is asked

- Hosted runners have six CPU models; only same-runner pairs (`paired` job, `[paired]` tag) give
  ratios. Add a pair to its matrix rather than a single run.
- `matrix.py` renders the served-or-not table; `--holds` renders the lock histograms.
- Collect with `collect.sh RUN_ID DIR`, then `retain_results.py DIR results/paired`; never poll.
