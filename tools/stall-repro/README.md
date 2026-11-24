# OTTO-13916 — isolated stall reproduction (scratch only)

This branch starts at engine commit `5b2cf3e2731d7fcecc3f95b9c05a5963b9c6b363`.
No changes here are intended for merge, deployment, or review. Never point this
harness at production or development; all rows and identities must be synthetic.

The harness is under construction. No workload measurements have been made.
See `results/RESULTS.md` for the evidence status, including environment failures.

## Execution route

The initial remote admission command was:

```sh
mighty remote --provider tensorlake --cpu 8 --mem 16 --timeout 300 -- bash -lc \
  'date -u; uname -a; nproc; command -v docker || true; docker version 2>&1; command -v perf || true; cat /proc/sys/kernel/perf_event_paranoid; command -v sudo || true; sudo -n true 2>&1; command -v cargo; rustc --version; command -v psql || true'
```

Admission failed before any command executed: the fleet has no Linux capability.
The authorized fallback is `.github/workflows/stall-repro.yml`, triggered by pushes
to this scratch branch. It builds the unmodified production engine in release mode
with debug symbols and frame pointers, and records its machine and binary hash.
PostgreSQL and Durable Streams will run outside the engine's CPU cgroup.
