---
name: electric-circuits-testing
description: Write, debug, review, or design Electric Circuits tests and behavior using genuine-red TDD, causally fenced black-box E2E contracts, and proportionate focused tests. Use for any test change, regression, test plan, fault injection, flake, or qualification.
---

# Electric Circuits testing

Use this skill for any test authoring, debugging or review, and for behavior changes, regressions,
test plans, fault injection, flake investigations, or qualification work. It applies to Rust,
TypeScript, protocol, conformance, and real-stack work; the contract decides the test location, not
the implementation language.

This repository is **not production-ready today**. Current direct-engine surfaces, the PostgreSQL 18
Compose fixture, and host-selected CI PostgreSQL are characterization tools. The target launch
acceptance profile is pinned PostgreSQL 18 with file-backed storage and an authenticated gateway.
Never turn a current green regression into a PG18, gateway, Swift/app, or release claim without that
target's named evidence.

Do not use E2E as the primary proof for a parser, codec, SQL/predicate translation, pure fold,
retry classification, identifier allocator, or exhaustive schedule. Start with focused
unit/property/model/concurrency/fuzz evidence for those. Retain one real-boundary integration test
when the local behavior participates in a larger product promise.

## Read first

Read `AGENTS.md`, then the affected public contract and neighboring tests. For a new supported
behavior or qualification, read [the contract protocol](references/contract-protocol.md). For the
appropriate tier, executable commands, and the current-versus-target boundary, read
[tier selection and commands](references/tier-selection-and-commands.md). Consult
[sources and evidence](references/sources-and-evidence.md) for the authoritative planning material,
profile rules, and handoff record.

## Workflow

1. Classify the work: characterization, implementation regression, or profile qualification. Name
   the selected lane and capabilities. A disabled capability needs an explicit public rejection
   before downstream work; do not infer support from a neighboring test.
2. Choose the highest stable black-box contract that can prove the product behavior. For a
   local-only risk, this is the focused unit/property/model boundary; do not add E2E merely to
   satisfy this step. For a replicated result, define the source-to-server-to-target receipt chain
   and an independently authored SQL/reference oracle before changing code. Keep the assertion
   outside circuit, catalog, offsets, task topology, and retry-count details.
3. Establish genuine red provenance. Register or select the scenario and semantic hash, add only
   the contract test, and run it on the exact frozen red-patch tree descended from the pinned base,
   before the implementation exists. Capture the intended assertion's first divergence, command,
   profile/config/digest, seed and raw output. A skip, timeout, setup error, `xfail`, changed contract,
   or mock-only failure is not red.
4. Add the smallest supporting proof at the causal risk: unit or property/model for a local law;
   Loom/Miri for a reduced synchronization or unsafe core; fuzzing for untrusted bytes; real PG,
   streams, process, gateway, or app collaborators where their seam is the promise. Use real
   collaborators to qualify replication, durability, authorization, restart, and cache behavior.
5. Implement narrowly. Keep the stable contract unchanged while making it green. Give races a
   named external gate and a diagnostic deadline, not a sleep. Include negative, restart and
   resource-bound cases where the behavior can cross those boundaries.
6. Qualify the selected tier with unique fixture ownership and no retry laundering. Capture
   immutable hashes and raw evidence. A separate reviewer verifies the red patch, oracle
   independence, unchanged contract, limits, and results; qualification does not repair what it
   judges.

Every red, green, direct, qualification and reviewer command uses the repository's canonical clean
evidence runner, not the author/control checkout: a new detached worktree at the exact commit, or a
newly empty export of a verified prepared-patch tree. Retain pre/post source-attestation and effective-
config hashes; bind immutable dependency/tool/fixture inputs and any read-only package-manager mount by
content plus resolver topology; and use a unique newly empty external root for writable build/cache/
fixture/artifact state. Dirty, undeclared, mutable, stale, reused or unidentified test bytes fail
provenance even if the assertion passes.

## Non-negotiable checks

- Compare target materialization only after a named causal fence. A later SQL query, a separate
  sentinel feed, private LSN/offset, or empty result is not proof that the target applied the change.
- Preserve the engine invariants in `AGENTS.md`: xid-based snapshot/live fencing, at-least-once
  ingest with exactly-once effect, complete transaction visibility, durable-before-ack lifecycle,
  reconcile-before-discard, safe drift/epoch handling, and bounded shutdown/recovery.
- Exercise failures that matter to the changed promise: response loss, cancellation, replay,
  restart, storage/network loss, schema/slot continuity, authorization/revocation, and
  limit-1/limit/limit+1 admission as applicable. Accept only documented continuation, safe replay,
  retirement, typed reset/refetch, or rejection—not stale-looking success.
- Every real-stack run owns its database, slot/publication, storage/volume, namespace/ports,
  process group, cache, journal and fault proxy. Cleanup is part of the result.
- Qualification is zero-retry. A retry-pass is a flake, not a pass. Missing, filtered, stale,
  under-run, wrong-profile/digest/config, or blocked evidence cannot be promoted.

## Completion

Record the contract/scenario hash, profile, exact candidate identity, seeds/journal, source and
target receipts, oracle, cut matrix, commands, raw artifact hashes, cleanup result, first
divergence, clean-source/external-input/mount/run-root/effective-config hashes, and every unrun gate
with its reason. Run the applicable repository gates; for engine/live-shape/client-path changes,
report the full `AGENTS.md` suite and any required browser blockers honestly.
