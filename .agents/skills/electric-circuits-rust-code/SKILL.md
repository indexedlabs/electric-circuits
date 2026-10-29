---
name: electric-circuits-rust-code
description: Implement, debug, refactor, or review any Rust in Electric Circuits, including data paths, protocols, Tokio ownership, failures, observability, resource bounds, and unsafe code. Not for TypeScript-only or prose-only work.
---

# Electric Circuits Rust Code

Use this skill for work in `apps/engine` (and its Rust/Cargo/CI support) when a change can affect the engine's data path, lifecycle, failure handling, concurrency, memory or `unsafe`. Repository invariants in `AGENTS.md` are higher authority than this skill. For a behavior change, read `electric-circuits-testing` first; for a structural decision, also read `electric-circuits-rust-structure`, in the order `AGENTS.md` requires.

## Use when

- Editing or reviewing any Rust under `apps/engine`, including replication, backfill, shapes, durable streams, catalog/epoch/retention, circuits, parsing, shutdown, Tokio tasks, locks/channels, resource limits, or FFI/`unsafe`.
- Diagnosing a Rust engine failure where replay, cancellation, retries, stream retirement, ordering, or load may be involved.

## Do not use when

- The work is confined to TypeScript, docs, demo styling, or a mechanical non-Rust change. Use the skill that owns that surface instead.
- The main decision is a new persistent protocol, storage boundary, serving-tier contract, or public consistency model. Stop and get/record the architectural decision first; this skill governs its Rust implementation.
- The work is only test strategy or a read-only audit **and does not review Rust implementation**. Use the dedicated testing/audit workflow when available. If it reviews Rust under `apps/engine`, this skill remains required by `AGENTS.md`.

## Read first

1. Read `AGENTS.md`, then the relevant area of `docs/ARCHITECTURE.md` and any matching ADR in `docs/adr/` before proposing an engine change. Those documents define the durable behavior; do not restate or weaken them.
2. Read the changed module, its neighboring tests, `rust-toolchain.toml`, root and engine `Cargo.toml`, `rustfmt.toml`, and `.github/workflows/ci.yml`. Keep the Rust 1.96.1 pin to meet dbsp 0.365's MSRV. The repository records `dbsp` ICEs on the tested newer releases named in `rust-toolchain.toml`; newer compilers still need re-checking against the current dbsp version and CI before changing the pin.
3. Before editing, read [the production checklist's Boundary review, Test and evidence selection, and Validation matrix](references/production-rust.md). Then load its applicable errors/tasks/bounds/unsafe/observability section for the risk.

## Pre-edit workflow

Before code, write a short change note in the task/PR or plan:

1. Name the affected observable contract and the exact `AGENTS.md` invariant(s).
2. State the durable point, ordering/deduplication rule, and recovery/replay outcome where the path crosses a stream, catalog, snapshot, or lifecycle transition.
3. Name each fallible boundary, its classification and owner; name every spawned task, cancellation path, safe point, and join/error policy.
4. Give a concrete bound and overload behavior for any added queue, buffer, retry, permit, task, retention state, or blocking work.
5. Identify the trace/metric evidence and the smallest test that can fail for the intended reason.

If any answer changes an established invariant or needs a new durable state/protocol, pause for an architectural decision instead of encoding an assumption in a local refactor.

## Production Rust rules

- Model lifecycle transitions with types, private state, and guards where practical. Do not expose success before its required durable write and post-wait race check.
- Return contextual, typed errors from anticipated failures. Do not use `unwrap`, log-and-continue, or `panic!` for storage, network, malformed input, cancellation, capacity, or Postgres outcomes.
- A correctness-critical task is owned: it has cancellation, a safe shutdown point, and observed completion/panic behavior. Do not detach it merely to preserve a request future.
- Treat every `.await` and losing `select!` branch as a cancellation point. Persist or compensate multi-step state, and do not re-create a cancellation-sensitive future accidentally in a loop.
- Preserve boundedness and backpressure. A limit must include semantics on saturation; do not turn a large transaction, slow peer, or retry storm into unbounded memory, work, or task growth.
- Never hold a lock across network I/O or `.await` unless the design explicitly requires an async lock and proves the contention/liveness trade-off. Keep blocking or substantial CPU work behind a bounded worker boundary.
- Prefer safe Rust. A new `unsafe` block needs a minimal scope, a local `// SAFETY:` argument tied to current invariants, focused tests, and knowledgeable review; do not expand existing unsafe abstractions casually.
- Instrument correctness boundaries with structured identifiers and saturation/retry/state-transition signals, without logging credentials or row payloads by default.

For the detailed questions and authoritative Rust/Tokio/Cargo links, see [production-rust.md](references/production-rust.md).

## TDD and implementation handoff

For every behavior change or bug, first use `electric-circuits-testing` and record the repository-required genuine-red contract at the highest stable boundary on the exact frozen red-patch tree. For product behavior this is the public contract; for a local-only law it is the focused boundary selected by the testing skill. The red patch is the test-only descendant of the pinned base, before behavior implementation. It must fail at the intended semantic assertion; if that proof cannot be demonstrated, stop rather than implement. Then add the smallest focused fault, cancellation, or state-machine test and make the unchanged contract green. A spike, generated code, migration, or thin wiring may vary the sequencing, but never waives genuine-red evidence for a behavior change.

For protocol behavior, ordinary unit coverage is insufficient unless it can demonstrate the relevant ordering, crash/replay, cancellation, retry/gone, or real Postgres/stream boundary. Prefer the repository oracle/conformance machinery and deterministic faults or controlled time over sleeps.

## Validate and hand off

Run the focused evidence first, then the applicable repository gates in [the validation matrix](references/production-rust.md#validation-matrix). Current required gates and proposed future hardening are deliberately separated there: do not present Clippy, Miri, sanitizers, docs, or fuzzing as existing CI requirements. Run or plan risk-triggered hardening only when its prerequisites and a meaningful baseline exist; before strict Clippy has one, report its result honestly only when the task packet asks for it.

At handoff, report changed files; invariant and behavior covered; error/cancellation/bound choices; telemetry; commands run with outcome; and any required command not run with the reason and next command. Do not commit or push unless explicitly asked.
