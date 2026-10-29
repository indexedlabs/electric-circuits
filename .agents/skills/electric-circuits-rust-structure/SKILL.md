---
name: electric-circuits-rust-structure
description: Evolve the Rust workspace, Cargo packages, module boundaries, public facade, features, or toolchain policy without weakening Electric Circuits engine invariants. Use for crate/module/API-boundary decisions; not localized Rust implementation within an established boundary.
---

# Electric Circuits Rust Structure

Use this skill for a proposed Cargo workspace or package change, module reorganization, public API
or visibility cleanup, dependency-direction problem, feature/platform/build-script/proc-macro
decision, or Rust toolchain/reproducibility policy change. It governs structural decisions, not
routine code inside an existing module: hand that work to
[`electric-circuits-rust-code`](../electric-circuits-rust-code/SKILL.md). For a change that does
both, settle the boundary here, then implement its local behavior there.

## Read first

1. Read `AGENTS.md`, then the relevant parts of `docs/ARCHITECTURE.md` and
   `docs/ivm-engine-internals.md`. A structural improvement is invalid if it obscures or breaks a
   durability, fencing, transaction, lifecycle, or boundedness invariant.
2. Read the applicable `Cargo.toml` files, `Cargo.lock`, `rust-toolchain.toml`, the current public
   facade (`apps/engine/src/lib.rs`), affected targets/tests, and the working-tree status.
3. Use [the repository map](references/repository-map.md) to orient yourself. Load the relevant
   [decision checklist](references/decision-checklists.md) and [official source](references/official-sources.md)
   before changing a manifest, public boundary, feature, or build mechanism.

## Workflow

### 1. Inventory and classify

State the desired seam and its invariant owner before editing. Inspect the current package graph,
public surface, feature activation, platform conditionals, and direct dependents. Useful evidence:

```bash
cargo metadata --no-deps --format-version 1
cargo tree -d
cargo tree -e features
cargo doc -p electric-circuits-engine --no-deps
```

Classify the request as one or more of: module ownership, public facade/visibility, package or
workspace topology, dependency direction, error boundary, feature/platform/build mechanism, or
toolchain/reproducibility. Do not turn runtime entities—shapes, users, tables, pipelines, or query
parameters—into Cargo structure.

### 2. Decide the smallest durable boundary

Default to a **private module facade inside the existing package**. The current one-crate engine is
not a defect by itself. Make `lib.rs` a deliberate list of supported names, not a mirror of source
files; keep the binary as a thin composition root. Move a boundary one dependency edge at a time.

Dependencies point inward: HTTP/Electric and Postgres/streams/dbsp adapters depend on the engine
kernel; the kernel depends on query/domain contracts; contracts/domain types import no adapter merely
to name a type. Infrastructure may implement a kernel-owned port. Do not introduce a trait solely to
hide a cycle.

An extraction is permitted only when all of these are true:

- It has a named contract and invariant owner that have survived real callers.
- Its dependency graph is acyclic, and the extracted crate avoids unnecessary dependency on the
  engine handle and heavy adapters such as HTTP, Postgres, streams, Tokio, or dbsp.
- Its public contract can be tested independently through supported APIs.
- Its package ownership and SemVer/publication intent are explicit.
- Before/after evidence shows a worthwhile dependency, compile, reuse, or deployment benefit.

If a gate is missing, create or improve the module facade instead and record what evidence would
justify revisiting extraction. Never split a crate for file size alone or make a catch-all
`engine-core` that imports every adapter.

### 3. Make boundary rules explicit

- Default implementation items to private; use `pub(crate)` only for an intentional intra-crate
  contract. Public APIs are compact facade re-exports with documented errors, cancellation,
  concurrency, and invariant semantics. Do not expose locks, maps, task handles, or storage
  internals merely because tests currently reach them.
- At a supported operation boundary, return meaningful domain errors and preserve relevant sources.
  Keep opaque contextual errors for internal tasks/startup; translate domain errors to wire statuses
  in one adapter layer. Prefer validated newtypes over repeated ambiguous strings at boundaries.
- Features are additive, owned, documented, and tested. Do not use them to select correctness
  semantics or serving tiers. Treat a default feature as compatibility policy, not a convenience
  switch. Before editing, record a concrete supported feature/target matrix: host rows for
  `--no-default-features`, each deliberately supported named combination, and `--all-features` only
  when it is itself a supported combination; plus the applicable row for every supported non-host
  target. Do not demand a feature powerset. Compile the host rows with
  `cargo check -p electric-circuits-engine --all-targets --locked` and the appropriate feature
  flags; compile each non-host row with
  `cargo check -p electric-circuits-engine --target <triple> --all-targets --locked`. Test the
  public outcome on a real supported host, and record an unavailable host as unavailable rather
  than treating a cross-compile as behavior evidence. Inspect activation with
  `cargo tree -e features --target <triple>` where relevant.
- A platform-specific behavior needs a small port and an explicit supported-platform outcome. Use
  target dependency tables and `#[cfg]` when the alternative cannot compile; do not make Unix-only
  dependencies universal by accident. Target dependency tables select targets; they do not evaluate
  feature predicates. Model an optional capability with an optional dependency and `[features]`,
  then combine target and feature predicates in code when needed.
- Avoid local `build.rs` and proc macros unless a maintained upstream solution or checked-in source
  cannot meet the need. A necessary build script is deterministic, offline, hermetic, writes only
  `OUT_DIR`, and declares narrow rerun inputs; a proc macro has a measured safety/maintenance case.
- Preserve the pinned toolchain. An exact toolchain pin and an MSRV are separate declarations: state
  their relationship explicitly and never infer an MSRV from a pin. Do not invent or change either
  incidentally. A dedicated MSRV task must set `[workspace.package] rust-version`, have every member
  inherit `rust-version.workspace = true`, identify intentionally unsupported targets, and prove the
  exact declared compiler with `cargo +<msrv> check --workspace --all-targets --locked` plus its
  supported feature/target matrix. A missing compiler or target is `blocked`, not proof. Keep
  `Cargo.lock` committed and use `--locked` in normal reproducible CI/container paths; review any
  resolver or lockfile refresh separately from that policy task. Isolate a resolver, toolchain,
  MSRV, or dependency refresh from unrelated structural work.

### 4. Migrate and prove it

Use an incremental path: establish the facade and its outcome tests, redirect callers, reduce
visibility or move one edge, then remove the old path. A focused test should fail for the intended
new boundary or regression before the behavioral change; purely mechanical moves still need tests
that show the supported API and engine invariants did not change. Integration tests exercise only
the supported facade; keep pure implementation tests near their logic.

For a structural proposal, measure rather than assume: capture relevant `cargo build --timings`,
`cargo test -p electric-circuits-engine --no-run`, `cargo tree -e features -i <crate>`, duplicate
tree, public rustdoc, and binary/image size before and after when they can change. Do not remove a
transitive duplicate or upgrade the dbsp compatibility island without tracing the cause and running
the full evidence matrix.

Run `cargo fmt --check` and the applicable checks in `AGENTS.md`. Engine or live-path structure
changes run and report its complete Rust, TypeScript, conformance, and relevant demo/
browser matrix under the validator-owned author/merge/qualification phases; an external blocker
never becomes release evidence or an author-chosen direct gate. Report every command not run and why.
Hand off the supported facade, package and
dependency direction, public/error/feature/platform/toolchain effects, measurements, invariant
coverage, and deferred extraction gates.
