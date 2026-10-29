# Tier selection and current commands

Choose the cheapest test that can falsify the risk, then keep one higher-tier proof whenever a real
boundary is part of the promised behavior.

| Risk | Primary evidence | Add when needed |
| --- | --- | --- |
| Parser, codec, predicate, fold, ID, retry classification | focused unit plus property/model/metamorphic test | fuzz untrusted bytes; corpus case for protocol compatibility |
| Small shared-state or unsafe core | model/property; Loom for a reduced synchronization core; Miri for reduced unsafe/FFI cases | real-stack regression for the resulting public behavior |
| Snapshot/live fence, replication, durable append, lifecycle, drift/epoch, restart | real Postgres + engine + durable-streams + actual client materialization | SQL/reference oracle and external fault gate |
| Public authorization, revocation, template admission, TLS | real authenticated gateway and client/network boundary | adversarial negatives before forbidden downstream work |
| Swift/app cache, lifecycle, protected data, account switch | actual package/app seam and normal cache reader | device/app-host coverage where OS behavior matters |
| Candidate release claim | immutable image stack, target profile, complete fault/resource corpus | independent qualification runner and reviewer |

Do not use E2E as the main test for exhaustive scheduling, local parser cases, pure algebra, or every
interleaving; E2E is slow and cannot enumerate them. Do not use a fake collaborator as qualification
for Postgres replication, durable-stream behavior, gateway authorization, restart, or the real cache.
Mocks, virtual clocks, and proxies remain valuable for controlled focused mechanics and fault gates.

## Current repository lanes

These are development/regression tools, not target launch acceptance:

- `pnpm typecheck` checks the TypeScript workspace; Vitest transpilation alone does not typecheck.
- `pnpm engine:test` runs Rust unit and integration coverage.
- `pnpm exec vitest run packages/conformance/src/conformance-<area>.test.ts` is the focused
  process-level conformance lane.
- `ELECTRIC_CIRCUITS_ENGINE_PREBUILT=1 pnpm test` runs the full current Vitest/conformance suite;
  set `PREBUILT` only after a successful engine build.
- `SEED=90210 FUZZ_SEEDS=5 FUZZ_SHAPES=10 FUZZ_OPS=250 pnpm exec vitest run packages/conformance/src/conformance-fuzz.test.ts`
  is a reproducible bounded fuzz invocation. Store the resulting journal, not only the seed.
- `ELECTRIC_DIR=/absolute/throwaway/electric ./electric-conformance/run.sh oracle` exercises the
  external Electric oracle in an isolated pinned checkout. It is optional and never a gate; run it
  (and `property` or `subqueries`) only when a `/v1/shape` change warrants it.
- `pnpm demo:linearlite` is a manual/external-browser smoke for live engine, shapes, or visualizer
  changes. Record the actions, public result and screenshot, or the missing-tool blocker.

Follow `AGENTS.md` for the complete engine-touching checklist: format, typecheck, engine tests,
full prebuilt Vitest, and applicable live browser verification. Report
unavailable prerequisites and any existing baseline failure as `blocked` or `fail`; do not call the
remaining lanes a substitute.

## Current versus target acceptance

The checked-in conformance harness is strong process-level regression evidence: it uses a real
Postgres database, logical slot, engine child process, test durable-stream server and TypeScript
client, then compares materialized state with source SQL. It currently uses direct internal
surfaces and the PostgreSQL 18 development baseline. It does not establish a pinned
PG18 major, file-backed durable storage, authenticated public gateway, Swift/app target receipt,
immutable candidate image, or release-profile qualification.

Target acceptance must instead start isolated, digest-pinned PostgreSQL 18 and file-backed storage,
assert the server version and exact publication/TLS profile, use the causal-fence protocol, and
operate candidate images without an implicit build or pull. PG16 remains useful only as a declared
compatibility/characterization lane. Never silently fall back from PG18; reject virtual generated
columns and treat slot/timeline continuity as an explicit fail-closed or authorized-reset decision.

## Qualification discipline

Before a qualifying run, freeze a manifest with source/tree SHA, dirty state, toolchains/lockfiles,
candidate digests, canonical redacted configuration, selected profile/hash, scenario/contract/cut
hashes, publication/schema facts, seeds/journals, fixed operation counts/floors, limits, deadlines,
and divergence-allowlist hash. Preserve raw logs, receipts, comparator output, resource samples,
cleanup and first divergence. Qualification has no automatic retries: a retry-pass is flaky and
fails the gate. Missing, stale, filtered, zero-test, under-run, mismatched, or blocked evidence
does not promote.
