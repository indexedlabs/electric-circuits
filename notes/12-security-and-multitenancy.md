# Security and multitenancy audit — production/mobile go-live

**Assessment date:** 2026-08-22. **Scope:** current engine, extended tRPC API, TypeScript client,
Docker deployment, and the direct Durable Streams transport. This is a source audit, not a
penetration test. Severity assumes an Internet-facing mobile app with mutually untrusted tenants.

## Decision

**Do not expose the shipped engine, tRPC API, or Durable Streams (DS) server directly to a Swift
client or the public Internet. This is a release blocker.** The project is a data-plane/query
engine, not an authenticated multitenant sync service. There is no request identity, tenant policy,
ownership check, or stream ACL spanning its three HTTP surfaces. A client-supplied predicate can
therefore request any configured table/column, and the returned stream URL is a direct data path.

It can run behind a purpose-built authenticated gateway/BFF and private service network after the
required controls below are implemented and tested. The TypeScript package is not a Swift SDK; a
native client should consume only that gateway's narrow, authenticated contract, not imitate the
package's direct engine/stream calls.

## Threat boundary

```
mobile app ── public TLS gateway/BFF ── private API/engine ── private DS ── Postgres
                  ^ identity, tenant policy, quotas, audit       ^ privileged replication role
```

The repository instead ships separately reachable engine, tRPC API and DS endpoints. The client
receives `streamUrl`/`streamPath` and opens DS directly
([`packages/client/src/index.ts`](/Users/bozilabs/labs/electric-circuits/packages/client/src/index.ts),
[`apps/engine/src/http.rs`](/Users/bozilabs/labs/electric-circuits/apps/engine/src/http.rs)). The
sample Compose file publishes **Postgres, DS, engine and API** to the host and defaults to every
PK-bearing `public` table ([`docker/compose.yaml`](/Users/bozilabs/labs/electric-circuits/docker/compose.yaml)).

Durable Streams' protocol explicitly leaves authentication/authorization to deployments and requires
TLS in production ([protocol security considerations](https://github.com/durable-streams/durable-streams/blob/main/PROTOCOL.md)).

## Verified controls worth preserving

| Area | Evidence | Security value and limit |
|---|---|---|
| Table identity | [`table_ref.rs`](/Users/bozilabs/labs/electric-circuits/apps/engine/src/table_ref.rs) canonicalizes bare names, rejects malformed/dotted/quoted references, and downstream SQL quotes qualified identifiers. | Prevents table ambiguity/identifier injection; not a caller or tenant allowlist. |
| Predicate/SQL injection | Finite JSON AST/op enum and schema compilation in [`predicate.rs`](/Users/bozilabs/labs/electric-circuits/apps/engine/src/predicate.rs); text binds/quoted identifiers in [`sql.rs`](/Users/bozilabs/labs/electric-circuits/apps/engine/src/sql.rs) and [`pg.rs`](/Users/bozilabs/labs/electric-circuits/apps/engine/src/pg.rs); `/v1/shape` parses restricted SQL WHERE in [`where_sql.rs`](/Users/bozilabs/labs/electric-circuits/apps/engine/src/where_sql.rs). | Good reviewed SQLi posture. Valid SQL/predicates still need authorization. |
| Projection/order checks | `resolve_columns`/`column_index` reject unknown columns ([`executors.rs`](/Users/bozilabs/labs/electric-circuits/apps/engine/src/engine/executors.rs)). | Prevents arbitrary identifiers, but every known sensitive column remains selectable. |
| Visualizer writes | Row insert/delete validates catalog fields, quotes identifiers and binds values ([`engine/mod.rs`](/Users/bozilabs/labs/electric-circuits/apps/engine/src/engine/mod.rs)). | SQLi-safe, but an exposed unauthenticated write primitive. |
| Correctness controls | Snapshot fencing, epoch/drift retirement, durable lifecycle and identified releases are documented in [`AGENTS.md`](/Users/bozilabs/labs/electric-circuits/AGENTS.md) and [`ARCHITECTURE.md`](/Users/bozilabs/labs/electric-circuits/docs/ARCHITECTURE.md). | Strong sync correctness; establishes no principal authorization. |
| Secret logging | Engine config uses `redacted()` and the fleet entrypoint redacts the DB password ([`main.rs`](/Users/bozilabs/labs/electric-circuits/apps/engine/src/main.rs), [`electric-entrypoint.sh`](/Users/bozilabs/labs/electric-circuits/docker/electric-entrypoint.sh)). | Retain; use a managed secret store in production. |

## Findings and required mitigations

### Critical — no authentication or authorization on native/API/control planes

The engine registers `/shapes`, `/aggregate`, `/query`, table schema/row mutations, metrics reset and
`/epoch/reset` with no middleware ([`http.rs`](/Users/bozilabs/labs/electric-circuits/apps/engine/src/http.rs)).
The tRPC context contains only `core`; every `t.procedure` is public, including `schema.define`,
`ingest.write`, shape reads/deletes, subset query/live and aggregates
([`apps/api/src/router.ts`](/Users/bozilabs/labs/electric-circuits/apps/api/src/router.ts)).

Impact: any reachable caller can enumerate/query configured data, create arbitrary feeds, mutate
application tables through visualizer routes, inject library-mode change events, alter schema state,
reset metrics or trigger an epoch reset. This is cross-tenant disclosure plus integrity/availability
compromise.

Required before go-live:

1. Make DS/Postgres/engine/API private-only; expose one HTTPS gateway/BFF.
2. Authenticate gateway requests with mobile OIDC/OAuth access tokens; validate issuer, audience,
   signature, expiry and authorized party. Derive user, tenant, roles and device/session server-side.
3. Expose a fixed server-owned query catalog/templates with projection and aggregate allowlists. Inject
   a non-bypassable tenant/membership rule from identity; never accept a client tenant ID as policy.
4. Put `/schema`, `/table/*/rows`, `/epoch/reset`, `/metrics/reset`, trace/debug and library-mode
   write surfaces on a separate mTLS/RBAC admin listener, or remove them from production.
5. Add negative authorization tests for every table, column, projection, subquery hop and admin route.

### Critical — direct DS access bypasses all prospective API checks

`ShapeResp` returns a raw stream URL. The client creates DS readers from that URL
([`http.rs`](/Users/bozilabs/labs/electric-circuits/apps/engine/src/http.rs),
[`packages/client/src/index.ts`](/Users/bozilabs/labs/electric-circuits/packages/client/src/index.ts)).
Paths are predictable `shape/sN`; DS also contains `changes/<n>` and `meta/catalog`. Compose publishes
DS on `8791` ([`docker/compose.yaml`](/Users/bozilabs/labs/electric-circuits/docker/compose.yaml)).
The DS protocol's access control is deliberately deployment-owned
([official protocol](https://github.com/durable-streams/durable-streams/blob/main/PROTOCOL.md)).

Impact: a caller reaching DS can read/modify arbitrary streams, bypass tenant policy, replay data,
forge change-log events, delete/close streams, or inspect catalog metadata. Randomizing IDs would
not solve the missing ACL.

Required before go-live: deny public DS access. Use mTLS/service-mesh policy between gateway, engine
and DS. Do not give a mobile app a shared DS bearer token. Either proxy/relay reads through the
authorized gateway, or issue short-lived signed capabilities bound to tenant, subject, exact path,
read-only method and audience. Reserve `changes/` and `meta/` plus all write/delete methods to the
engine identity. Test guessed, stale and cross-tenant paths.

### Critical — tenant isolation is a query convention, not an invariant

The guide presents `tenant = 7` and membership subqueries as usage patterns, not enforced policy
([`docs/live-queries-guide.md`](/Users/bozilabs/labs/electric-circuits/docs/live-queries-guide.md)).
Any caller can omit/change the filter, select any tracked column, or request a subquery over another
tracked table. Equal definitions share one global stream, while subscriptions are free-form strings
with no authenticated owner binding ([`engine/lifecycle.rs`](/Users/bozilabs/labs/electric-circuits/apps/engine/src/engine/lifecycle.rs),
[`http.rs`](/Users/bozilabs/labs/electric-circuits/apps/engine/src/http.rs)).

Postgres RLS alone is insufficient unless the complete design proves it: query-backs execute as one
engine role rather than a request-scoped end user, and table owners commonly bypass RLS.

Required before go-live: use an engine/database/schema per trust domain, or make tenant policy
structural. For shared databases, expose only reviewed sync views/tables with safe columns and a
mandatory tenant key; gateway-owned templates bind it from claims; the engine database role has no
access to base relations/columns that must never sync. Test membership changes and NULL/`NOT IN`
semantics for every server-authored visibility rule.

### High — shape IDs, leases and purge are lifecycle controls, not security capabilities

`GET /shapes/{id}`, row/log previews and `DELETE /shapes/{id}` have no principal check;
`?purge=true` force-deletes the shared stream. The legacy delete decrements an anonymous claim, and
a supplied subscription is format-validated but not ownership-validated
([`http.rs`](/Users/bozilabs/labs/electric-circuits/apps/engine/src/http.rs)). The documented lease
protocol intentionally accepts any well-formed ID ([`AGENTS.md`](/Users/bozilabs/labs/electric-circuits/AGENTS.md)).

Impact: an exposed caller can inspect another shape, purge it, release/renew a known claim and pin
retention. Idempotency is correct but is not authorization.

Required mitigation: do not expose engine shape IDs/subscriptions to untrusted clients. The gateway
maps an opaque high-entropy handle to `{tenant, subject, device/session, engine shape, subscription}`
and enforces that binding for read/renew/release. Public purge must not exist; use operator RBAC only.

### High — introspection and errors disclose data/topology

With default `ELECTRIC_CIRCUITS_TRACE=true`, `/trace`, `/graph`, `/state`, `/state/node`,
`/graph/node`, shape rows/logs, table metadata and profiling are registered unauthenticated. State and
trace can expose row data, as deployment docs explicitly warn
([`http.rs`](/Users/bozilabs/labs/electric-circuits/apps/engine/src/http.rs),
[`deployment-postgres.md`](/Users/bozilabs/labs/electric-circuits/docs/deployment-postgres.md)).
`AppError` returns formatted internal errors; `/v1/shape` logs returned error messages
([`http.rs`](/Users/bozilabs/labs/electric-circuits/apps/engine/src/http.rs),
[`electric.rs`](/Users/bozilabs/labs/electric-circuits/apps/engine/src/electric.rs)).

Required mitigation: set `ELECTRIC_CIRCUITS_TRACE=0` on production data-plane pods. Put diagnostics
behind separate admin auth. Return stable external error codes plus correlation IDs; retain detailed
causes only in protected structured logs. Avoid exporting tenant/table identifiers in telemetry labels.

### High — transport security is absent in the shipped topology

Engine/API bind plain HTTP. The engine image explicitly has no TLS backend, and normal Postgres
connections use `NoTls` ([`docker/Dockerfile.engine`](/Users/bozilabs/labs/electric-circuits/docker/Dockerfile.engine),
[`apps/engine/src/main.rs`](/Users/bozilabs/labs/electric-circuits/apps/engine/src/main.rs),
[`apps/engine/src/pg.rs`](/Users/bozilabs/labs/electric-circuits/apps/engine/src/pg.rs)). Compose
publishes cleartext ports and an example password. `ELECTRIC_SECRET` protects only `/v1/shape`, uses
`secret`/`api_secret` query parameters, and does not protect native/control/tRPC routes
([`config.rs`](/Users/bozilabs/labs/electric-circuits/apps/engine/src/config.rs),
[`electric.rs`](/Users/bozilabs/labs/electric-circuits/apps/engine/src/electric.rs)). URL secrets can
reach logs/monitoring/referrers; a deployment-wide secret cannot represent a user or tenant.

Required mitigation: public TLS 1.2+ gateway; mTLS or authenticated service mesh for every internal
hop; default-deny network policy. Configure Postgres TLS and use a TLS-capable connector/sidecar
before crossing a protected local network. Use managed secret injection/rotation. Never use
`ELECTRIC_SECRET` for mobile auth; if Electric compatibility is retained, authenticate at the proxy
with a short-lived `Authorization` credential, not a query string secret.

### High — abusive query/shape cost is not constrained by a principal-aware admission policy

No application-level authentication-aware rate limit, request concurrency limit, per-principal
shape quota, predicate depth/width limit, or response-size policy was found in the Axum/tRPC server
construction ([`http.rs`](/Users/bozilabs/labs/electric-circuits/apps/engine/src/http.rs),
[`apps/api/src/server.ts`](/Users/bozilabs/labs/electric-circuits/apps/api/src/server.ts)). Subset
`limit` has no application maximum and snapshot shapes/`/v1/shape` can materialize unbounded results.
Backfill statement timeout defaults to off ([`deployment-postgres.md`](/Users/bozilabs/labs/electric-circuits/docs/deployment-postgres.md)).

Retention's `MAX_SHAPES` and disk budget only evict **dormant** shapes. Active subscriptions, and
subquery/aggregate shapes that are not dormant, can exceed the cap while the engine logs pressure
rather than shedding active work ([`retention.rs`](/Users/bozilabs/labs/electric-circuits/apps/engine/src/retention.rs),
[`engine/lifecycle.rs`](/Users/bozilabs/labs/electric-circuits/apps/engine/src/engine/lifecycle.rs)).
Fallback predicates such as `OR`, `NOT` and `LIKE` retain a per-change scan path
([`ivm-engine-internals.md`](/Users/bozilabs/labs/electric-circuits/docs/ivm-engine-internals.md)).

Required mitigation: enforce gateway quotas per tenant/user/device and globally: request byte/JSON
depth/node caps; projection count; rows/page and response bytes; predicate/subquery-depth/`LIKE`
policy; concurrent backfills/long-polls; active shapes/streams; renewal rate; request/egress budgets.
Admit only indexed/template-approved predicates, set a finite backfill statement timeout, and
queue/reject rather than exhaust the shared Postgres pool. Alert on active shapes, backfill age, DS
bytes, retention pressure, query latency and fallback shape count.

### Medium — browser CORS is incomplete; Swift is not protected by CORS

`OPTIONS /v1/shape` sends only `Access-Control-Allow-Methods`; there is no router-wide
`Access-Control-Allow-Origin`, allowed-header, credential or exposed-header policy
([`http.rs`](/Users/bozilabs/labs/electric-circuits/apps/engine/src/http.rs)). A browser will not gain
cross-origin access from this alone, but CORS is not authorization and iOS/Swift does not enforce it.
If web clients are later supported, configure a precise origin allowlist at the gateway; Tower's CORS
API requires an allowed origin before it sends CORS headers
([tower-http documentation](https://docs.rs/tower-http/latest/tower_http/cors/struct.CorsLayer.html)).

### Medium — Postgres replication privileges and default table selection widen blast radius

The engine role needs `SELECT`, table ownership to set `REPLICA IDENTITY FULL`, and `REPLICATION`;
the project creates a `FOR ALL TABLES` publication
([`deployment-postgres.md`](/Users/bozilabs/labs/electric-circuits/docs/deployment-postgres.md)).
The default selector is `public.*`, which can include a newly created sensitive table at restart.
Compromise of the engine/database credential is broader than a typical application read role.

Required mitigation: enumerate reviewed sync tables/views instead of `*`; use a dedicated role,
database/schema and slot per environment/trust domain. Split DDL ownership/replication setup from
the runtime role where feasible; restrict DB network access and rotate the replication credential.
Treat every new replicated table/column as security-reviewed and exclude tokens, payment data,
internal notes and audit records from replication.

### Medium — auditability and incident response are inadequate for public access

Current logs/metrics record operational state and `/v1/shape` failures, but no durable security audit
event contains actor, tenant, client handle, authorization decision, stream grant/revocation, source
device/IP or administrative action ([`http.rs`](/Users/bozilabs/labs/electric-circuits/apps/engine/src/http.rs),
[`apps/api/src/router.ts`](/Users/bozilabs/labs/electric-circuits/apps/api/src/router.ts)). Without
identity, attribution and targeted revocation are impossible.

Required mitigation: make the gateway produce privacy-minimized, tamper-resistant audit events for
authentication, authorization denials, query-template/version, shape create/read/release, stream
grant/revocation, quota decisions and every admin/data mutation. Propagate a correlation ID only—no
bearer tokens or raw PII predicates—to engine/DS logs. Define audit retention/access, alerts and an
incident playbook for credential compromise/cross-tenant exposure.

### Medium — container and supply-chain posture is demo-grade

Positive: lockfiles exist; CI uses `pnpm install --frozen-lockfile`; the DS cargo install pins
`durable-streams` to `0.1.5 --locked` ([`docker/Dockerfile.ds`](/Users/bozilabs/labs/electric-circuits/docker/Dockerfile.ds),
[`ci.yml`](/Users/bozilabs/labs/electric-circuits/.github/workflows/ci.yml)).

Gaps: Docker uses mutable base tags (`node:22-slim`, `rust:1-bookworm`, `debian:bookworm-slim`), the
normal Node image runs `pnpm install` without `--frozen-lockfile`, and engine/DS/API images run as
root (only the combined fleet image changes to `USER node`)
([`Dockerfile.node`](/Users/bozilabs/labs/electric-circuits/docker/Dockerfile.node),
[`Dockerfile.engine`](/Users/bozilabs/labs/electric-circuits/docker/Dockerfile.engine),
[`Dockerfile.ds`](/Users/bozilabs/labs/electric-circuits/docker/Dockerfile.ds)). The reviewed publish
workflow has no evident dependency vulnerability scan, SBOM, image scan, signing/provenance or
dependency-update automation ([`docker.yml`](/Users/bozilabs/labs/electric-circuits/.github/workflows/docker.yml)).

Required mitigation: pin base images by digest; build reproducibly with frozen/offline locks; run as
non-root with read-only root filesystem, capability drop and seccomp; mount only required writable
volumes. Add OS/Cargo/npm vulnerability scans with exception process, SBOM (CycloneDX/SPDX), signed
provenance/attestation, registry admission policy and patch/rollback drills.

## Go-live blockers checklist

- [ ] One public TLS gateway/BFF exists; engine, DS, API and Postgres are private-only under default-deny network policy.
- [ ] Every public request maps to a server-derived tenant/subject; policy-enforced templates/projections replace arbitrary client predicates.
- [ ] DS path-level reads use proxy authorization or expiring path-bound capabilities; mobile clients have no DS write/admin credential.
- [ ] Tests prove predicates, direct paths, handles, subscriptions, guesses, subqueries and projections cannot cross tenant boundaries.
- [ ] Admin/write/introspection/debug endpoints are separately protected or absent; `ELECTRIC_CIRCUITS_TRACE=0` is set in data-plane pods.
- [ ] TLS/mTLS, secret rotation, explicit table allowlist, least-privilege Postgres design and publication-data review are complete.
- [ ] Quotas/rate limits/backpressure/response limits and alerts withstand abusive create, snapshot, long-poll and renewal load.
- [ ] Security audit logging, incident response, dependency/image scanning, SBOM and signed deploy artifacts are operational.

## Residual operational note

The catalog, epoch checks, schema-drift retirement and transaction fencing are valuable correctness
controls and should remain enabled. They reduce stale/lost synchronization; they do not reduce the
impact of an overbroad authorized feed or an unauthenticated DS read. Treat every shape stream as a
replica of the selected rows: its sensitivity, retention, encryption, revocation and access logging
must meet the standard of the underlying tenant data.
