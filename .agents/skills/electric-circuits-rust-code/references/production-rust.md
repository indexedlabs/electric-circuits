# Production Rust checklist

Use this reference after reading the repository invariants. It supplies implementation questions; it does not replace the durable-stream, SnapshotGate, transaction, epoch, retention, catalog, or shutdown rules in `AGENTS.md`.

## Boundary review

For each affected boundary, record the answers rather than relying on implied control flow.

| Concern | Questions to answer |
|---|---|
| Contract and state | What may the caller observe? Which states and transitions are valid? What type, guard, or private API prevents an impossible transition? |
| Durability and ordering | What record/append/checkpoint makes the effect durable? Is it before acknowledgement? What recheck is needed after an unbounded wait? Which key makes replay safe, and where is transaction visibility preserved? |
| Errors | Which outcomes are unavailable/retryable, definite refusal or gone, conflict, malformed/configuration, and invariant breach? Does the error keep causal context for the layer that owns the policy? |
| Ownership and cancellation | Who owns the resource and any child task? What happens if the request future, child future, or process is cancelled at every await? Is compensation idempotent and durable when required? |
| Bounds | What is the maximum bytes, items, permits, tasks, retry duration/rate, long-poll time, and retained state? On saturation does the system backpressure, spill, defer, retry, reject, or fail closed—and is that permitted by the invariant? |
| Shutdown | Which token interrupts waits? What safe point does the task reach? Who joins it and observes failure/panic? Does a stop preserve replay semantics rather than advancing a source position or retiring live state? |
| Operations | Which structured fields identify the transition (for example table, shape/stream, segment, LSN/xid, attempt)? Which counter, gauge, histogram, trace, health state, or debug endpoint demonstrates progress and saturation? |

## Rust and Tokio checks

### Errors and state

- Use `Result` for expected runtime faults; reserve panics for internal invariant violations that cannot honestly be recovered at that layer.
- Classify a boundary error before choosing retry, retirement, rollback, or process exit. Never discard a committed change merely because one transport response looks terminal; apply the repository reconciliation rule.
- Carry sources and stable context. Avoid `anyhow`-style erasure at an API/policy boundary when callers need to distinguish unavailable, conflict, malformed, or gone.
- Prefer a named state enum or RAII guard when cleanup depends on which part of a multi-step create/join/retire operation completed.

### Tasks and cancellation

- Before `spawn`, name its owner, resource budget, shutdown signal, safe point, and how a `JoinHandle` error/panic is surfaced. Long-lived engine work uses the local shutdown-party protocol.
- A dropped future is an input to the design. In `select!`, non-winning branches are dropped; do not put a half-applied transition behind them. Pin or reuse an in-flight future if losing a select iteration must not restart it.
- Do not use a detached task as a substitute for ownership. A deliberately detached completion task still needs durable intent, retry/escalation, bounded lifetime, and observability.
- For ingest/sequencer append changes, identify the append mode before adding cancellation. `append_commit_chunked` and the sequencer's in-flight batch are graceful-shutdown safe sections: finish them, then checkpoint; never acknowledge a partial commit. Live shape `append_reliable` retries until landing or reconciled retirement. Only restore/activation `append_retrying` has its bounded budget and shutdown token; do not substitute these policies. See [ingest](../../../../docs/ARCHITECTURE.md#3-ingest-logical-replication-exactly-once-effect), [reliability](../../../../docs/ARCHITECTURE.md#55-reliability-appends-never-drop-silently), [threading/shutdown](../../../../docs/ARCHITECTURE.md#10-threading-model), and [ADR-0003](../../../../docs/adr/0003-ingest-pgoutput-v1-with-spill.md).

### Bounded work, blocking, and locks

- Keep permit ownership aligned with the work it limits, including cancellation and retries. A semaphore/channel without an overload policy is not a complete design.
- Do not materialize a whole backfill or transaction when the established streaming/spill mechanism applies. For ADR-0003, name the component that owns the bound: the spill limit applies to ingestor buffering (cap plus one append chunk), not the sequencer's transaction-sized page, held run, or pending work needed for atomic visibility. Preserve `last`, re-delivery, and high-water semantics; never use transaction size to reject, purge, or retire valid work.
- Keep `std::sync::Mutex` regions short, low-contention, and free of `.await`; release registry/state locks before Postgres, durable-stream, or other network work. Use an async mutex only when the state truly must span an await and its liveness is understood.
- Move blocking I/O or meaningful CPU work out of async workers through a bounded, owned execution boundary. Make queue wait and saturation observable.

### Unsafe

- First establish why safe Rust, a narrower API, or a validated crate abstraction cannot meet the need.
- Keep the block minimal. Place `// SAFETY:` immediately beside it, spelling out the preconditions and why this call site satisfies them now.
- Document every caller obligation on an `unsafe fn`; use explicit inner unsafe blocks. Maintain `unsafe_op_in_unsafe_fn` discipline.
- Add targeted tests for the abstraction and request review from someone familiar with the DBSP/FFI boundary. Consider Miri or sanitizers when the code and platform support them.

### Observability

- Trace ingress, dedup/skip, durable wait, retry/backoff, reconciliation, lifecycle transition, checkpoint/segment movement, and shutdown safe point when the change affects them.
- Record attempts, queue depth/age or permits, bytes/items processed, duration, failure category, and terminal outcome where relevant. Avoid secrets and arbitrary row data.
- Make a partial failure diagnosable: an operator should distinguish a duplicate, a delay under retry/backpressure, a retirement, and an invariant failure.

## Test and evidence selection

Start with the smallest test that would fail if the proposed behavior were absent. Keep outcome assertions at a boundary the user or another component can observe; use mocks for controlled failure/clock/transport seams, not as proof of the engine's own replication semantics.

| Change risk | Minimum useful evidence in addition to focused Rust tests |
|---|---|
| Pure local transformation or parser branch | Focused unit/regression test; malformed and boundary inputs where relevant. |
| Error mapping, retry, task, channel, lock, or shutdown path | Deterministic fault/cancellation test; assert ownership, retry/terminal classification, and the declared limit. |
| Backfill/live, envelopes, transaction markers, dedup/checkpoints, appends, catalog, lifecycle, drift, epoch, or segments | Integration/conformance proof at the real Postgres/durable-stream/client boundary; include replay or restart/interleaving evidence when the contract has one. |
| Chunked ingest or reliable append | Deterministic cuts at each chunk, shutdown safe point, and ambiguous/terminal response; assert no source acknowledgement or drain barrier before the last chunk, and no processed/checkpoint position, deletion floor, or dormant resume position past a held run's page, while retaining every required high-water-only checkpoint. Also assert no partial client-visible transaction, correct replay/dedup and retry classification, and the declared ingestor bound (cap plus one append chunk). Pair with the real Postgres/durable-stream/client contract. |
| Shape live path or pipeline visualizer behavior | Run the demo/browser flow from `AGENTS.md`: drive a write, verify the stream/canvas/state, inspect errors, and capture a screenshot. |
| New or changed unsafe/FFI/concurrency ownership | Focused tests plus an appropriate Miri/sanitizer plan if supportable; do not claim those tools prove distributed protocol correctness. |
| Decoder, parser, fold, or state-machine attack surface | Regression corpus plus property/fuzz testing when justified; use an invariant or oracle as the property. |

For a bug, preserve a regression that fails for the intended reason. When practical, confirm causation by temporarily removing the fix locally; never weaken a test simply to accommodate the implementation.

## Validation matrix

### Current repository gates

These are the current repository requirements for an engine-touching task, not recommendations invented by this skill. Run them or report precisely why a gate could not run:

```bash
cargo fmt --check
pnpm typecheck
pnpm engine:test
ELECTRIC_CIRCUITS_ENGINE_PREBUILT=1 pnpm test
```

Electric's own oracle (`./electric-conformance/run.sh oracle`, which needs the pinned Elixir/Erlang toolchain and a sibling Electric checkout) is an optional check, not a gate. For a live engine/shape/visualizer path, also run the demo/browser validation specified in `AGENTS.md`. A focused test is useful early, but it does not replace the packet's phase-classified author/merge gates or final qualification. Follow `AGENTS.md` for the narrow owned-baseline repair rule; an inherited blocker remains visible and never becomes release evidence.

`pnpm typecheck` is required because Vitest transpiles without type checking. `pnpm test` includes the repository conformance suite; the Electric oracle is a separate, optional suite. CI currently runs formatting, engine tests, typecheck, and the full test suite—check `.github/workflows/ci.yml` if it changes.

### Proposed or risk-triggered hardening (not current CI gates)

The research recommends these as future/conditional evidence. Do not claim they are required CI unless the repository adopts them:

- `cargo clippy -p electric-circuits-engine --all-targets -- -D warnings` only after a dedicated baseline/CI task retains the exact source/toolchain result. It is currently non-green; before that baseline, report the command honestly when the task packet asks and do not treat it as a green gate.
- `cargo doc -p electric-circuits-engine --no-deps` and `cargo test --doc` for public Rust API/documentation changes.
- `cargo tree -d` and `cargo tree -e features` when dependency resolution changes; inspect and commit `Cargo.lock` intentionally.
- Miri for supportable unsafe or ownership-critical pure tests; Linux sanitizer runs for supported unsafe/FFI/cross-thread paths.
- `cargo-fuzz` or model/property/interleaving tests for decoders, catalog/envelope folds, transaction buffering, and replay/cancellation state machines.

## Primary references

- [Rust Book: deciding between `panic!` and `Result`](https://doc.rust-lang.org/stable/book/ch09-03-to-panic-or-not-to-panic.html)
- [`core::error` API](https://doc.rust-lang.org/stable/core/error/index.html)
- [Tokio: spawning tasks](https://tokio.rs/tokio/tutorial/spawning)
- [Tokio: `select!` and cancellation](https://tokio.rs/tokio/tutorial/select)
- [Tokio: shared state and mutex choice](https://tokio.rs/tokio/tutorial/shared-state)
- [Rust Reference: unsafe keyword](https://doc.rust-lang.org/stable/reference/unsafe-keyword.html)
- [Rust 2024 guide: unsafe operations in unsafe functions](https://doc.rust-lang.org/edition-guide/rust-2024/unsafe-op-in-unsafe-fn.html)
- [Cargo diagnostic attributes](https://doc.rust-lang.org/reference/attributes/diagnostics.html)
- [Cargo profiles and overflow behavior](https://doc.rust-lang.org/cargo/reference/profiles.html)
- [Cargo dependencies guide](https://doc.rust-lang.org/cargo/guide/dependencies.html) and [`cargo tree`](https://doc.rust-lang.org/cargo/commands/cargo-tree.html)
- [Rustdoc writing guide](https://doc.rust-lang.org/rustdoc/how-to-write-documentation.html) and [doctests](https://doc.rust-lang.org/rustdoc/write-documentation/documentation-tests.html)
- [Miri](https://github.com/rust-lang/miri), [Rust sanitizer flags](https://doc.rust-lang.org/nightly/unstable-book/compiler-flags/sanitizer.html), and the [Rust Fuzz Book](https://rust-fuzz.github.io/book/)
