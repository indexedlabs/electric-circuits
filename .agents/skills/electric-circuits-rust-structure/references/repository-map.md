# Rust structure map

## Current baseline

- The root is a virtual Cargo workspace with one member: `apps/engine`.
- `apps/engine` supplies both the `electric_circuits_engine` library and the
  `electric-circuits-engine` binary. Keeping this as one deployable package is an intentional
  option, not technical debt by definition.
- `rust-toolchain.toml` pins Rust 1.96.1 to meet dbsp 0.365's MSRV. The repository records upstream
  compiler ICEs with earlier dbsp on newer stable compilers; newer compilers still need re-checking
  against the current dbsp version and CI. Do not float the pin or change related Cargo policy in an
  incidental refactor.
- `Cargo.lock` is committed. The engine directly pins a dbsp/rkyv/size-of compatibility group;
  inspect its feature graph before changing any member.
- The current package uses edition 2024, while the virtual workspace explicitly selects resolver
  2. Treat a resolver move as a separately reviewed graph change.

## Engine ownership map

| Area | Principal code | Structural concern |
| --- | --- | --- |
| Engine kernel | `apps/engine/src/engine/` | Sequencing, lifecycle, catalog, memberships, planning, and delivery have shared atomicity/lifecycle rules. Do not split them casually. |
| Circuit tier | `arrangements.rs`, `subq_circuit.rs` | Fixed deploy-time pipelines; their structure must not scale with shapes or subscriptions. |
| Query/domain representation | `predicate.rs`, `schema.rs`, `value.rs`, `table_ref.rs`, `sql.rs`, `where_sql.rs` | Candidate low-level contract boundary only after real dependency and API evidence. |
| Source/storage adapters | `pg.rs`, `replication.rs`, `pgoutput.rs`, `ds.rs`, `changelog.rs` | Postgres remains authoritative; durable-stream semantics retain ordering and retirement guarantees. |
| Client/wire adapters | `http.rs`, `electric.rs`, `params.rs`, `main.rs` | Composition and protocol edges should depend inward; a wire surface need not expose implementation helpers. |
| Cross-table runtime registry | `subquery.rs` | Shares inner-set nodes and coordinates absolute emission; preserve atomic rollback and deferred-flip convergence. |

Read `docs/ARCHITECTURE.md` §14 and `docs/ivm-engine-internals.md` §7 for the current detailed
file map. For changes touching a row path, re-read the architecture consistency table and the
applicable ADR before deciding the package/module seam.

## Intended direction

```text
contracts and domain values
          ↑
query semantics and engine kernel
          ↑                 ↑
Postgres/streams adapters   dbsp circuit adapter
          ↑                 ↑
HTTP/Electric adapters and binary composition root
```

The arrows mean “depends on.” A port belongs at the boundary that consumes it. Runtime shape,
cohort, and query-template cardinality belongs in data/routing, never in Cargo topology.
