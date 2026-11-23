# Upstream open-issue inventory — 2026-08-22

Scope: `electric-sql/electric-circuits`, inspected through the GitHub REST API with
pagination on 2026-08-22.  The open-issues endpoint returned 15 items: **14 issues and
one pull request**.  Every issue was created during the 2026-07-06 production-readiness
sweep; the only post-creation activity is one clarifying comment on #11.  “Local status”
below is an evidence-based comparison with this checkout at `474577a` (dated 2026-08-22),
not a claim that upstream has closed or accepted the work.

Classification uses the requested dimensions.  “Correctness” includes stale/wrong result
sets; “durability” includes restart, WAL and output-log guarantees.  Age is calendar age as
of the inspection date.

## Executive assessment

* The upstream tracker is materially stale: the readiness wave is still represented as 14
  open tickets even though upstream's closed PRs #21 and #22, and the local commits after
  them, implement most of the critical safety work.  Do **not** use GitHub open-count as a
  readiness measure.
* The local fork has strong, test-backed coverage for the P0 class: restart/catalog,
  slot-epoch loss, schema drift/TRUNCATE, bounded transactions, streamed backfills,
  segmented log retention, and graceful shutdown/readiness.
* The real remaining launch decisions are exposure/security (#14–#15), caching/CDN (#10–#11),
  and the deliberate pgoutput protocol-v2 deferral in #17.  The meaningful engineering
  residuals are the absence of an explicit PG advisory single-writer lock (#13) and the
  absence of a storage-wide orphan scan (#8; the durable-streams API has no list primitive).

## Open issues (GitHub state is verified)

All entries have no assignee and no milestone.  Unless stated otherwise, labels are empty.
The URLs are the primary-source records.

| Issue | Tracker metadata | Classification | Verified local-fork status and evidence | Dependency / priority reading |
|---|---|---|---|---|
| [#3 — Shape id reuse after engine restart can append onto stale `shape/<id>` streams](https://github.com/electric-sql/electric-circuits/issues/3) | Created/updated 2026-07-06; age 47d; labels `bug` | correctness, durability | **Solved.** Catalog folding advances `next_shape_id` past every historical `Created`, including dropped shapes (`apps/engine/src/engine/catalog.rs`); the unit cases around lines 1660–1710 and the restart conformance in `packages/conformance/src/conformance-catalog-durability.test.ts` prove no reused stream/id. | Foundational P0. It was a prerequisite for safe restart/retirement and is now covered by #8's catalog implementation. Upstream should close it. |
| [#4 — Engine restart silently freezes extended-API shapes](https://github.com/electric-sql/electric-circuits/issues/4) | Created/updated 2026-07-06; age 47d; labels `bug` | correctness, durability, client | **Solved for registered native shapes.** The catalog is explicitly the restart contract and restores routing/claims (`engine/catalog.rs`, especially restore near 894–1030); `conformance-catalog-durability.test.ts:193–199` restarts and verifies an acknowledged shape remains registered. Retired streams are closed before deletion, so clients can observe terminal stream state rather than tail a silently dead feed. | Depends on #8. Client retry/re-subscribe behavior after terminal `Stream-Closed`/404/410 is still a local follow-up; that is recovery UX, not the original silent-staleness failure. |
| [#5 — Failover/slot loss unhandled](https://github.com/electric-sql/electric-circuits/issues/5) | Created/updated 2026-07-06; age 47d; labels `bug` | correctness, durability, ops | **Substantively solved, with an intentional policy difference.** `engine/epoch.rs` persists `SlotBound`, verifies the slot/cluster before every connection, degrades or resets on loss, and `conformance-epoch.test.ts` exercises reset/refusal. Reconnect backoff is exponential/jittered in `replication.rs:167–308`. A timeline change is recorded rather than treated as an epoch break; system identifier/slot loss is the destructive boundary. | P0 was “never silently adopt a fresh slot at the head”; solved. Timeline-only failover policy is a conscious residual to document/revisit, not evidence that data-loss detection is absent. |
| [#6 — Unbounded memory on large transactions](https://github.com/electric-sql/electric-circuits/issues/6) | Created/updated 2026-07-06; age 47d; labels `bug` | correctness, durability, performance, ops | **Solved for the stated OOM risk.** `TxnBuffer` spills beyond `ELECTRIC_CIRCUITS_TXN_MEMORY_BYTES` (`apps/engine/src/txn_buffer.rs`), while `replication.rs` appends bounded chunks and preserves transaction-end atomicity. `packages/conformance/src/conformance-large-txn.test.ts` and `apps/engine/tests/txn_dormant_while_held.rs` cover spill/held-transaction semantics. | P0. #17/v2 is not required to make this safe: commit atomicity still requires receiver-side handling of a transaction. Upstream should reword/close #6 rather than treat protocol v2 as its blocker. |
| [#7 — DDL and TRUNCATE leave shapes silently stale](https://github.com/electric-sql/electric-circuits/issues/7) | Created/updated 2026-07-06; age 47d; labels `bug` | correctness, durability | **Solved.** `engine/drift.rs` fingerprints schema/identity, retires dependents on pgoutput relation drift, TRUNCATE and identity regression, and runs a reconciler for silent DDL. `conformance-schema-drift.test.ts` covers ADD COLUMN, TRUNCATE, replica identity and background reconciliation. | P0 and independent of #17 because Relation messages exist in pgoutput v1. A newly created/re-created table still needs a restart/reconciliation follow-up, but stale existing shapes are fail-closed. |
| [#8 — Persistent shape catalog + lazy restart recovery](https://github.com/electric-sql/electric-circuits/issues/8) | Created/updated 2026-07-06; age 47d; labels — | correctness, durability, ops | **Mostly solved.** Durable `meta/catalog` events restore live and dormant shapes, checkpoints and id high-water (`engine/catalog.rs`); all client-promised creates/releases wait for durability. The catalog durability conformance suite includes restart and retry/retirement failure paths. **Residual:** it can complete catalog-known unfinished retirements, but cannot enumerate all DS `shape/*` streams for a global orphan sweep because the storage API offers no list operation. | Foundation for #3, #4, #9 and #10. Incomplete-create handling is now transactional/durable rather than the issue's original simple “drop incomplete on boot” proposal; verify this contract before closing upstream. |
| [#10 — `/v1/shape` HTTP caching](https://github.com/electric-sql/electric-circuits/issues/10) | Created/updated 2026-07-06; age 47d; labels — | API, client, performance, ops | **Open by choice.** `apps/engine/src/electric.rs` still emits `cache-control: no-store` (around 953/1007), accepts but does not implement the cache-collapsing cursor, and has no ETag/304 or 10 MB response-tier scheme. | Depends conceptually on #8 (now available). Not a native-path correctness blocker, but it blocks Electric edge/CDN parity and should be prioritized before a high-fanout public Electric-compatible deployment. |
| [#11 — Extended API caching through the Rust DS CDN protocol](https://github.com/electric-sql/electric-circuits/issues/11) | Created 2026-07-06; updated 2026-07-06; age 47d; labels —; **1 comment** | API, client, performance, ops | **Deferred/partial infrastructure only.** The Rust DS server is in use, but the client does not send the DS cursor or handle 304, and there is no authenticated feed proxy that preserves DS cache headers. The sole comment changes the intended topology: DS stays internal and a CDN fronts the API proxy. | Blocked by #15's authenticated proxy/route decision; the stale issue body’s direct client-facing DS wording is superseded by its comment. This is a delivery-scaling feature, not a correctness prerequisite. |
| [#12 — DS disk growth / table-stream prefix trimming](https://github.com/electric-sql/electric-circuits/issues/12) | Created/updated 2026-07-06; age 47d; labels — | durability, ops, performance | **Solved in a redesigned form, not literally.** The former table streams became segmented `changes/N` log streams; `changelog.rs` rotates only at transaction boundaries and reclaims only after the durable checkpoint and dormant-shape pins permit it. `retention.rs` evicts dormant shapes under TTL/count/disk pressure. This replaces the proposed DS prefix-trim primitive. | #9 supplies the dormant lifecycle; #8 supplies durable resume positions. The requested per-stream-size API/shape-stream rotation is not demonstrated here, so public docs should describe bounded segmented input retention, not claim complete DS-wide compaction. |
| [#13 — Postgres connection management](https://github.com/electric-sql/electric-circuits/issues/13) | Created/updated 2026-07-06; age 47d; labels — | ops, performance, durability | **Mostly solved.** `pg.rs:247–347` provides a bounded shared pool (`ELECTRIC_DB_POOL_SIZE`); `BackfillReader` streams cursor rows; `ELECTRIC_CIRCUITS_BACKFILL_STATEMENT_TIMEOUT_MS` protects slow snapshots; boot classification and 1s→30s jittered retries are covered by `conformance-boot-errors.test.ts` and `conformance-backfill-streaming.test.ts`. **Residual:** no `pg_advisory_lock` call exists, so the requested explicit cross-engine single-writer guard is not evidenced. | The pool/backfill fixes remove the main resource hazard. The advisory lock remains an ops/durability hardening item for deployments that might run more than one engine against one slot. TLS is separately #14. |
| [#14 — TLS support for Postgres connections](https://github.com/electric-sql/electric-circuits/issues/14) | Created/updated 2026-07-06; age 47d; labels — | security, ops | **Unsolved / deliberately deferred.** `apps/engine/src/pg.rs` uses `tokio_postgres::NoTls`; a URL with `sslmode=disable` is tolerated, not implemented as full psql SSL semantics. | Blocks direct use with managed Postgres or any untrusted network path. It is not a blocker only under the documented cluster-internal/Postgres-side TLS termination assumption. |
| [#15 — Auth, CORS and debug-surface isolation](https://github.com/electric-sql/electric-circuits/issues/15) | Created/updated 2026-07-06; age 47d; labels — | security, API, client, ops | **Partially solved.** `ELECTRIC_SECRET` guards `/v1/shape` query secrets and the router has an OPTIONS/preflight path (`apps/engine/tests/http_endpoints.rs`). But there is no Bearer auth for native control/API routes, no API feed proxy, and debug/metrics endpoints remain on the main router rather than a utility port. | **Highest unresolved exposure blocker.** #11's CDN plan explicitly depends on this proxy boundary. Treat it as P0 before exposing engine/API/DS beyond a trusted internal network. |
| [#16 — Graceful shutdown, readiness probe and production metrics](https://github.com/electric-sql/electric-circuits/issues/16) | Created/updated 2026-07-06; age 47d; labels — | ops, durability, performance, testing | **Largely solved.** `main.rs` drains SIGTERM/SIGINT; `/health` is liveness and `/ready` is readiness (`http.rs`, `conformance-readiness.test.ts`, `conformance-shutdown.test.ts`). Prometheus/OpenTelemetry output includes retained-WAL and flush-lag gauges (`metrics.rs`, `mem.rs`; verified by `conformance-backfill-streaming.test.ts`). **Residual:** the configured separate Prometheus port is warned/ignored, and a separate utility port remains #15 work. | Should be closed or split: core safe shutdown/readiness/critical WAL signals are present; isolation/topology belongs with #15. |
| [#17 — Streaming logical replication / pgoutput proto v2+](https://github.com/electric-sql/electric-circuits/issues/17) | Created/updated 2026-07-06; age 47d; labels — | correctness, durability, ops, performance | **Partially solved, v2 deferred.** Ingest is already push-based streaming pgoutput with standby acknowledgements (`replication.rs`, `pgoutput.rs`), replacing the ticket’s poll/test_decoding premise. It is protocol v1 rather than v2+, because the client dependency does not expose v2; large transactions are safely handled by #6’s spill/chunk path. | The original “must use v2 to avoid buffering” rationale no longer holds for commit-atomic output. Keep a narrower enhancement issue only if v2’s streaming/in-progress transaction semantics or other PG-version capabilities are independently valuable. |

## Open PRs

| PR | Tracker metadata | Assessment |
|---|---|---|
| [#47 — Add license scan report and status](https://github.com/electric-sql/electric-circuits/pull/47) | Open, ready (not draft), created 2026-07-30 and last updated 2026-07-30; age 23d; author `fossabot`; no labels/assignee/milestone; 1 commit; 5 additions, 0 deletions, only `README.md`; no comments, reviews or reported check runs. GitHub reports mergeable `true`, mergeable state `unstable`. | Adds a FOSSA badge/report link only. It neither fixes nor blocks any readiness issue. Security/license-governance useful, but review it separately from runtime security (#14–#15); the bot-generated external links should receive normal supply-chain/documentation review. |

## Recently closed items that define readiness

### Closed issue

* [#9 — Shape retention: active / dormant / evicted](https://github.com/electric-sql/electric-circuits/issues/9) was created 2026-07-06 and closed as completed on 2026-07-07 (age at close: 1d; no labels, assignee or milestone).  Its model is implemented locally in `apps/engine/src/retention.rs`: idle unsubscribed shapes go dormant, retained streams/replay reactivate them, and dormant-only TTL/count/disk-budget eviction applies. `packages/conformance/src/conformance-retention.test.ts` proves dormant replay, read reactivation, and eviction.  This is the direct dependency behind #12’s reclamation policy and a key mitigation for output-stream growth.

### Closed PRs relevant to interpreting the still-open tracker

* [#21](https://github.com/electric-sql/electric-circuits/pull/21) (closed 2026-07-07) carried the three-tier retention design corresponding to #9.
* [#22](https://github.com/electric-sql/electric-circuits/pull/22) (closed 2026-07-08) is the major stale-tracker signal: its title explicitly covers sequencer architecture, streaming pgoutput, durable catalog and rebuilt retention. It explains why #3/#4/#8 and the streaming half of #17 no longer describe the local implementation.
* [#25](https://github.com/electric-sql/electric-circuits/pull/25) (closed 2026-07-13) moved to the Rust durable-streams server, relevant to #11/#12 but not proof that cache-proxy work is complete.
* [#37](https://github.com/electric-sql/electric-circuits/pull/37) and [#39](https://github.com/electric-sql/electric-circuits/pull/39) (both closed 2026-07-15/16) add disk spilling and memory reductions. They reinforce the local performance posture but do not substitute for the durability tests cited above.

## Prioritized dependency map (analysis, not tracker status)

```text
P0 if any public/untrusted exposure is planned
  #15 auth / API feed proxy / utility isolation
    └── #11 authenticated CDN/proxy delivery
  #14 PostgreSQL TLS (unless the entire DB path is already trusted and encrypted)

P1 production-hardening residuals
  #13 explicit PG advisory single-writer lock
  #8 storage-wide orphan enumeration, if DS gains a safe list/GC primitive
  #10 Electric HTTP cache tiers, before relying on CDN fan-out

P2 capability / optimization work
  #11 client cursor + conditional reads
  #17 pgoutput protocol v2, only after a concrete benefit beyond current spill/atomicity

Already safety-complete locally; close/re-scope upstream
  #3 #4 #5 #6 #7 #12 #16; #8 mostly; #13 mostly; #17 streaming-v1 half
```

## Verification boundaries

This inventory establishes source and code/test evidence; it does not rerun the full engine or
Electric conformance suites.  Assertions marked “solved” are based on the current code paths and
named regression/conformance tests, not on a fresh green test run in this documentation task.
The local fork has additional implementation commits after the upstream PRs, so upstream issue
closure still requires an upstream maintainer to compare/merge the relevant changes.
