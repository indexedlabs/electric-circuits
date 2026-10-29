# Structural decision checklists

Load only the section matching the proposed change.

## Module and public-facade change

- Name the supported callers and the invariant the facade owns.
- Re-export only supported types and functions from a small facade; redirect integration tests to
  it before narrowing implementation visibility.
- Keep implementation details private: locks, maps, channels, task handles, codecs, and transport
  state are not compatibility promises.
- Document public operation preconditions, `# Errors`, cancellation, concurrency, and any durable
  acknowledgement/retirement semantics.
- Put domain error mapping at one operation/adapter boundary. Preserve sources and avoid exposing
  transport-specific errors as the public decision vocabulary.

## New crate or workspace change

- Identify the contract, its owner, callers, and compatibility/publishing intent.
- Draw the post-change graph and prove it has no cycle; do not add a trait merely to conceal one.
- Confirm the new crate is dependency-light and does not pull `Engine`, axum, Postgres, streams,
  Tokio, or dbsp without a concrete reason.
- Provide black-box contract tests and a migration sequence that leaves a working facade after each
  step.
- Compare `cargo metadata`, `cargo tree -d`, feature activation, cold/incremental build timings,
  test compile time, and relevant artifact size before and after. State the benefit and cost.

## Feature, platform, build-script, or macro change

- Make a feature additive; name its owner, default policy, supported combinations, compile cost,
  and CI coverage. It must not select a correctness or serving mode.
- Record a concrete supported feature/target matrix before editing: host rows for
  `--no-default-features`, every deliberately supported named combination, and `--all-features`
  only where it is supported; plus the applicable row for each non-host target. Do not require a
  feature powerset. Run each host row as
  `cargo check -p electric-circuits-engine --all-targets --locked <feature flags>` and each non-host
  row as `cargo check -p electric-circuits-engine --target <triple> --all-targets --locked <feature
  flags>`; inspect activation with `cargo tree -e features --target <triple>` when relevant. Test a
  feature's public outcome on a real supported host; a cross-compile only proves compilation.
- Isolate platform behavior behind a small interface. Use `[target.'cfg(...)'.dependencies]` and
  `#[cfg]`; define conservative fallback or explicit unsupported-platform behavior. Target
  dependency tables select targets, not `cfg(feature = ...)`; use an optional dependency plus
  `[features]` for an optional capability, then combine target and feature predicates in code.
- Prefer source checked into the repository or an explicit generator. If `build.rs` is unavoidable,
  make it deterministic, offline, hermetic, `OUT_DIR`-only, and precise about rerun inputs.
- Add a proc macro only with a demonstrated safety or maintenance advantage over ordinary Rust or a
  maintained upstream derive; account for host-tool build dependencies.

## Toolchain, lockfile, or dependency change

- Read the exact pin rationale in `rust-toolchain.toml`. An exact pin is not an MSRV: state their
  relationship explicitly and do not infer one from the other.
- Establish or change an MSRV only in a dedicated task. Set `[workspace.package] rust-version`, have
  every member inherit `rust-version.workspace = true`, identify intentionally unsupported targets,
  and prove the declared compiler with `cargo +<msrv> check --workspace --all-targets --locked` plus
  the supported feature/target matrix. A missing compiler or target is `blocked`, not evidence.
- Keep the lockfile under version control. Use `--locked` for normal reproducible CI/container
  build/test/image paths; make an intentional resolver or lockfile refresh a separate, reviewed
  change.
- Inspect reverse feature/dependency trees before changing direct dbsp compatibility dependencies
  or “cleaning up” duplicate major versions. Incompatibility can be valid transitively.
- Keep resolver, edition, toolchain, and broad dependency updates independently reviewable.

## Validation and handoff

- Start with a focused failing test when observable behavior or a supported boundary changes; then
  make the smallest change and retain a regression/contract test. Mechanical moves still need
  facade and invariant evidence.
- Always run `cargo fmt --check` and scope-appropriate Rust checks. For engine/live-path work,
  run and report the full `AGENTS.md` matrix under its generated gate phases: `pnpm typecheck`,
  `pnpm engine:test`, prebuilt `pnpm test`, plus the demo/browser pass when
  it exercises the changed seam. Do not move an unavailable qualification lane into direct gates.
- Report: public API delta, dependency direction/package graph, error and feature/platform impact,
  toolchain/lockfile effect, before/after measurements, invariant evidence, commands run, and
  commands not run with the reason. Name deferred extraction gates rather than implying completion.
