# Shape groups correctness prototype

This branch starts at `c1d035e` (`prototype/shape-groups`). It adds the first
correctness slice for a group with a plain member and an `IN (SELECT …)` member,
plus an executable key-only semijoin sketch. Nothing here is merge-ready or a
PG18 qualification.

## Verdict

The group shape can be made safe enough to keep prototyping: claims are reserved
atomically, the member list and one snapshot fence are durable, a group-wide
transaction fence closes the delayed-subquery ordering hole, and bounded emission
queues fail closed instead of losing membership effects. The semijoin remains
unstarted as a real DBSP runtime operator, so no semijoin performance conclusion
is included. Keep groups as the transport and lease abstraction, then compare a
real key-only semijoin only after its engine state and restart behavior are measured.

## Correctness cases

The focused Rust cases are in `apps/engine/src/engine/group.rs`; the existing
sequencer, source-receipt and output tests cover the shared paths they exercise.
The HTTP group path is `/v1/subset-loads` with `members`.

These are prototype characterization results, not release qualification: the
sandbox could not complete the engine dependency build, so no full red/green
run against PostgreSQL and Durable Streams was possible. “Pass” below means the
focused invariant is implemented and covered by a deterministic test/model.

| Case | Result | Fix | Size |
| --- | --- | --- | --- |
| Delayed inner flip from T after plain output from T+1 | Pass in the focused barrier test; live path now waits for all pending flip work from the previous commit while any group is active | Added an active-group transaction fence in the sequencer, preserving the existing source LSN barrier | M (about 20 Rust lines plus the barrier primitive) |
| Source-drain receipt covers the group lane | Pass by construction: source receipts already wait on `pending_flips`, and bounded emission batches remain in that counter until append | Reused the existing receipt barrier; emission queue overflow marks the engine degraded | S |
| Delete for a key present in two member tables | Pass for the wire contract: deletes carry `type = schema.table` and `key`, so equal keys cannot collide | Regression test for table-qualified delete envelopes | S |
| Group claim overlaps an existing shape/group | Pass for the claim path: reservation runs under the same state lock as ordinary subscriptions and returns 409 before member creation | Added `GroupClaims`, `GroupConflict`, and reservation rollback | M |
| One snapshot / horizon / replay fence, including subquery seed inputs | Pass for the load response: all pages come from `query_subset_batch`, and the group record stores one `lsn`, `snapshot`, `horizon`, and replay offset | Durable `GroupRecord` carries the common fence | M |
| Member table retirement or TRUNCATE reloads the group; closed group stream is 409 | Partial: a missing group stream is now a typed 409 and group member releases are all-or-nothing; schema retirement still originates per table and needs the gateway/app reload loop | Added `GroupStreamClosed`; group teardown records `GroupDropped` and releases every private member | M; full drift-to-client reload remains unproven |
| Mixed group across retention and restart | Partial: plain-only groups restore their durable member list; any group containing a subquery member is dropped as a unit so the client recreates it | Added `GroupCreated`/`GroupDropped` catalog events and restore filtering | L; no real restart/retention run in this sandbox |

The bounded subquery emission lane is controlled by
`ELECTRIC_CIRCUITS_EMIT_QUEUE` (default 1024). A full queue degrades the engine;
it never drops a membership batch and reports a false drained receipt.

## Key-only semijoin

`src/semijoin.ts` is a non-authoritative sketch only. It is not a DBSP runtime
template, and no semijoin state bytes, reseed time, or move-in latency are evidence.
`NOT IN`/NULL semantics are therefore also not an engine contract yet.

## What remains unproven

- A real PostgreSQL 18 + Durable Streams group run, including delayed subquery
  flips, schema drift, TRUNCATE, retention eviction and process restart.
- A group capability token and gateway re-authorization on every long-poll.
- Gateway-to-Swift/GRDB routing and one transaction across two collections. The
  requested stretch was not started.
- A DBSP key-only semijoin operator, its checkpoint/reseed path, and production
  memory/RSS under multiple heavy accounts.
- Full Rust, vitest/oracle, gateway and Swift suites. The sandbox has no Docker;
  `cargo check -p electric-circuits-engine --lib --locked` was attempted with
  Rust 1.96.0 and a downloaded Zig C toolchain; it reached `tikv-jemalloc-sys`
  and stopped because the sandbox has no `make`/`ld` toolchain. `cargo fmt --all
  -- --check` and `git diff --check` pass.

## Recommendation

Keep the group API and bounded lifecycle work as the shared transport layer: it
removes per-table leases and gives the phone one cursor. For the engine's
membership strategy, prefer a key-only semijoin over adding row joins or more
deferred query-backs; it has a small, predictable key state and one batched
move-in read. Do not enable either path in production until the real-stack cases
above and the gateway/SDK contract are green.

