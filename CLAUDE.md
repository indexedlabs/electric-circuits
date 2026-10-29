# Project Instructions for AI Agents

This file provides instructions and context for AI coding agents working on this project.

## Git Policy

Do not commit or push unless explicitly asked. At handoff, report changed files, validation run,
and suggested next commands.

## Build & Test

```bash
pnpm engine:test                          # Rust unit + integration (fast)
ELECTRIC_CIRCUITS_ENGINE_PREBUILT=1 pnpm test  # full vitest suite incl. oracle conformance (boots its own Postgres)
pnpm demo:linearlite                      # demo stack: PG + engine + LinearLite + pipeline visualizer
```

**Finishing an engine-touching task requires both suites green (the Rust engine tests and the vitest
suite with its engine-vs-oracle conformance), plus driving the demo (browser e2e) for
live-path/visualizer changes — see "Testing checklist before handoff or qualification" in AGENTS.md.
Electric's own oracle (`./electric-conformance/run.sh oracle`) is optional, not a gate.**

Full commands, the demo/visualizer runbook (incl. driving the visualizer with the Playwright MCP),
invariants, and gotchas live in **AGENTS.md** — read it before touching the engine or the apps.

## Architecture Overview

See AGENTS.md (layout + docs index) and `docs/ARCHITECTURE.md`.

## Conventions & Patterns

See the Invariants and Gotchas sections of AGENTS.md.
