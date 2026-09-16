# Electric Circuits production-readiness and Swift migration specification

Status: **reviewed execution spec**, updated 2026-08-23 for PostgreSQL 18 and black-box E2E/TDD.

This is the canonical output of the study. It supersedes the first synthesis draft in
[`16-production-readiness-and-swift-migration-spec.md`](16-production-readiness-and-swift-migration-spec.md).
Twelve independent GPT-5.6-sol/high differential reviews are preserved under [`reviews/`](reviews/);
the original disposition is recorded in
[`20-differential-review-disposition.md`](20-differential-review-disposition.md), with the PG18/E2E
follow-up in [`25-pg18-e2e-differential-review-disposition.md`](25-pg18-e2e-differential-review-disposition.md).
The PostgreSQL 18 decision, an unverified exploratory generated-column observation, and stable E2E scenario inventory are expanded
in [`24-postgres18-and-e2e-tdd-addendum.md`](24-postgres18-and-e2e-tdd-addendum.md).

The goal is not to make an unconstrained research system “production ready” in the abstract. The
goal is to qualify one explicit deployment and client feature profile, with every other capability
rejected by configuration and protocol until its own profile closes.

## 1. Decisions

### 1.1 Swift product decision

Use two independent migration lanes:

- **`COMPAT_V1`** — an opt-in compatibility provider in the application (or a separate additive
  ElectricSync product only if the app lacks the provider seam) talks to an authenticated template
  gateway. The gateway privately translates to Circuits `GET /v1/shape`. This is limited to eager,
  statically simple, single-owner full shapes that do not need tags/DNF ownership, `changes_only`,
  ordering, limits, subsets, SSE, or source-transaction observer atomicity.
- **`NATIVE_CORE`** — a new independent `ElectricCircuitsSwift` package exposes authenticated,
  template-driven, event-level materialized shapes. It does not depend on `electric-sync-swift`,
  tRPC, GRDB, SwiftData, or a durable-streams SDK.

Optional native profiles are selected only when the app inventory proves a use case:

- `NATIVE_AGGREGATE`
- `NATIVE_SUBSET`
- `NATIVE_TXN_ATOMIC`
- `NATIVE_REPLICA_SINK`

Do not refactor the ElectricSync core into a dual protocol backend before both lanes have stable,
independent conformance suites.

### 1.2 First production server topology

The first supported server profile is deliberately narrow:

- PostgreSQL 18.x, one known primary, one explicit-table publication, one logical slot, SCRAM, and
  verified TLS on both SQL/query-back and replication paths. Stored generated columns are supported
  only when published; virtual generated columns are rejected before readiness.
- One active engine process and one file-backed durable-streams process on a single-writer RWO
  volume; both are acknowledged single points of failure with measured restore bounds.
- Engine-process replacement is supported only after the former process is confirmed terminated.
- A PostgreSQL primary promotion/timeline change is an epoch break: fail closed, retire/reset every
  public feed generation, and fully rehydrate clients. Seamless logical-slot failover is unsupported.
- Durable-stream backups are quiesced/offline. A DS-only restore behind an advanced Postgres slot is
  never resumed in the same epoch; it forces a reset or a proven coordinated PG restore.
- Exactly one authenticated TLS gateway process is public in the first profile. Engine data/admin/probe/metrics listeners,
  durable-streams, tRPC API, and Postgres are private and separately authorized.
- Public requests name a server-owned template/version plus typed allowed parameters. They never
  expose or accept engine table names, predicates, projections, shape IDs, subscriptions, DS paths,
  or DS URLs.
- The gateway proxies every stream read in the first release. Direct signed DS capabilities and CDN
  caching are unsupported.

This can be a production system with an explicit availability envelope; it is not an HA claim.
Seamless PG failover requires a separately qualified PG18 synchronized failover-slot topology plus
downstream-enforced monotonic fencing on every DS mutation and is a separate future profile.

### 1.3 Public compatibility posture

Circuits is a separate Electric-compatible project, not a publicly announced successor to current
Electric. Current Electric remains actively released in the primary-source record. `GOV-001` makes
launch independent of upstream adoption: the team either accepts indefinite ownership of the fork or
does not ship it.

## 2. Release profiles

`docs/production/release-profiles.yaml`, created by `GOV-005`, is authoritative. A disabled feature is
not merely unfinished: configuration and protocol tests must reject it before any engine/DS/PG work.

`PLAN-001` alone has a typed bootstrap identity, not a selected release profile:

```yaml
identity_kind: bootstrap
profile_scope: uncompiled_all
canonical_spec:
  blob_sha: <pinned canonical-spec blob>
  tree_sha: <pinned canonical-spec tree>
scenario_registry_state: unavailable_until_E2E-000S
```

It pins the canonical specification and execution-protocol bytes/tree, declares the exact outputs
owned by `PLAN-001`, and may not name a future `GOV-005` release-profile manifest or an `E2E-000S`
semantic registry hash. An absent output is declared by path/owner, never supplied as an input with a
fabricated blob hash. This identity authorizes only `PLAN-001`. Before emitting any non-bootstrap
ready packet, `PLAN-001` must generate and validate the typed task-manifest and execution-scope
identities that packet requires. Early profile-independent `non_behavioral` shared producers use a
typed planning scope and `scenario_registry: not_applicable_pre_registry`; a selected release profile
still requires the `GOV-005` manifest, and a `genuine_red` behavior packet requires the populated
`E2E-000S` registry. No other task may substitute `uncompiled_all` for either identity.

The manifest has two explicit axes; free-form profile labels and prose `A or B` dependencies are
invalid task metadata:

```yaml
lane: COMPAT_V1 | NATIVE_CORE
features: [] # subset of NATIVE_AGGREGATE, NATIVE_SUBSET, NATIVE_TXN_ATOMIC, NATIVE_REPLICA_SINK
```

`COMMON_SERVER` and `COMMON_SERVER_QUALIFICATION` are inherited by either lane. Native features are
legal only with `lane: NATIVE_CORE`; each feature inherits the entire native-core closure. Every task
stores an applicability expression and every conditional edge stores a machine expression. A selected
profile is invalid when a required dependency evaluates inapplicable. Human `Profiles` text below is
explanatory until `PLAN-001` generates it from the manifest.

| Profile | Adds to `COMMON_SERVER` | Explicitly excludes initially |
| --- | --- | --- |
| `COMPAT_V1` | `CMP-*`, app ownership, compatibility shadow/cutover/rollback | DNF/tagged, on-demand, progressive, order/limit/subset, SSE, txn-atomic observers |
| `NATIVE_CORE` | native public protocol, `SWF-000`–`SWF-006`, lifecycle/security/package tasks | aggregate, subset, txn-atomic observer, generic overlapping-feed sink unless enabled |
| `NATIVE_AGGREGATE` | `SWF-008`, aggregate fixtures/capacity | subquery aggregate predicates unless explicitly supported |
| `NATIVE_REPLICA_SINK` | `SWF-007` plus app-specific ownership adapter | implicit merge of conflicting projections |
| `NATIVE_SUBSET` | `ENG-001`, `ENG-001A`, `SWF-009`, subset corpus | composite-key or deferred-subquery subset unless their fixture rows pass |
| `NATIVE_TXN_ATOMIC` | `PROTO-003B`, `ENG-002`, `SWF-005B` | any execution tier whose async output coordinator is not qualified |

The following is normative input to `PLAN-001`; the generator stores each row as a conditional edge,
not as parsed prose. Let `C = lane == COMPAT_V1`, `N = lane == NATIVE_CORE`, and `A`, `U`, `T`, `S`
mean the corresponding aggregate, subset, transaction-atomic, and replica-sink feature is selected.

| Consumer task | Condition | Required dependency or dependencies |
| --- | --- | --- |
| `CAP-005` | `C` | `TST-008C` |
| `CAP-005` | `N` | `TST-008N` |
| `SWF-013` | `T` | `SWF-005B` |
| `SWF-013` | `S` | `SWF-007` |
| `SWF-013` | `A` | `SWF-008` |
| `SWF-013` | `U` | `SWF-009` |
| `E2E-003NQ` | `N && !S` | `APP-NATIVE-CONSUMER-001` |
| `E2E-003NQ` | `N && S` | `APP-NATIVE-SINK-001` |
| `E2E-004R` | `C` | `E2E-003CQ` |
| `E2E-004R` | `N && !S` | `E2E-003NQ`, `APP-NATIVE-CONSUMER-001` |
| `E2E-004R` | `N && S` | `E2E-003NQ`, `APP-NATIVE-SINK-001` |
| `E2E-004Q` | `C` | `E2E-003CQ` |
| `E2E-004Q` | `N && !S` | `E2E-003NQ`, `APP-NATIVE-CONSUMER-001` |
| `E2E-004Q` | `N && S` | `E2E-003NQ`, `APP-NATIVE-SINK-001` |
| `E2E-005` | `C` | `E2E-003CQ` |
| `E2E-005` | `N` | `E2E-003NQ` |
| `E2E-005` | `T` | `E2E-003T` |
| `E2E-005` | `S` | `E2E-003S` |
| `E2E-005` | `A` | `E2E-003A` |
| `E2E-005` | `U` | `E2E-003U` |
| `TST-003` | `C` | `TST-002C`, `TST-006C`, `TST-008C`, `CMP-006`, `CAP-002`, `CAP-005`, `E2E-003CQ` |
| `TST-003` | `N` | `TST-002N`, `TST-006N`, `TST-008N`, `SWF-013`, `CAP-005`, `E2E-003NQ` |
| `TST-003` | `N && !S` | `APP-NATIVE-CONSUMER-001` |
| `TST-003` | `N && S` | `APP-NATIVE-SINK-001` |
| `TST-003` | `T` | `E2E-003T` |
| `TST-003` | `S` | `E2E-003S` |
| `TST-003` | `A` | `E2E-003A` |
| `TST-003` | `U` | `E2E-003U` |
| `MIG-001` | `C` | `CMP-002B` |
| `MIG-001` | `N` | `SWF-002` |
| `MIG-002` | `C` | `CMP-005` |
| `MIG-002` | `N && !S` | `SWF-006`, `APP-NATIVE-CONSUMER-001` |
| `MIG-002` | `N && S` | `SWF-006`, `SWF-007`, `APP-NATIVE-SINK-001` |
| `MIG-004` | `C` | `CMP-006` |
| `MIG-004` | `N && !S` | `SWF-013`, `APP-NATIVE-CONSUMER-001` |
| `MIG-004` | `N && S` | `SWF-013`, `APP-NATIVE-SINK-001` |

The generator verifies this table against the human task text and refuses an unlisted conditional
dependency, ambiguous `or`, selected feature without core, or a selected app integration different
from the `NATIVE-ADR-001` decision.

Future, non-launch profiles: `HOT_TABLE_RELOAD`, `SEAMLESS_PG_FAILOVER`, `DIRECT_DS_CAPABILITY`, and
`EDGE_CACHE`. They appear in the support matrix as unsupported until their task closure is added to a
release profile.

## 3. Release gates

Evidence state is `pass | fail | blocked | not_applicable_by_profile`. Only `pass` and an approved,
profile-generated `not_applicable_by_profile` can promote. Recording an environmental blocker is
valid handoff evidence but never closes a gate.

The machine record separates `task_outcome` from nested `evidence_observations`. Only the validator
may emit `not_applicable_by_profile`, and only when the applicability expression is false for the
profile-manifest hash; a required dependency can never be N/A. Skipped/filtered/zero-test, under-run,
stale/missing raw data, dirty or wrong source, wrong digest/config/profile, missing scenario ID, or a
changed contract hash maps to `fail`. Environmental unavailability maps to `blocked`. A baseline-
inventory task may pass while recording non-pass nested lanes; downstream gates inspect the nested
results, never only the meta-task outcome.

Every packet and scenario observation declares one `proof_kind`:

- `genuine_red` is a recorded failure at the intended semantic assertion and is the only proof that
  may enter `red_proved` or authorize a behavior implementation's unchanged green pair.
- `inherited_control` is a characterization of an already-passing (or otherwise explicitly
  baseline-controlled) scenario. It is never red proof; the scheduler records it as
  `characterized` and it may be consumed only as the unchanged control the scenario declares.
- `non_behavioral` covers planning, scaffolding, inventory, and other packets for which a red proof
  is inapplicable.

Gate evidence is phase-specific. Author/merge direct gates establish that a candidate may be
reviewed and integrated; inherited-baseline characterization records the current state without
laundering it into a pass; and release qualification is non-waivable promotion evidence. A named
baseline-repair packet may consume only its recorded base failure at the exact assertion it owns,
must turn that unchanged assertion green and pass every runnable direct gate, and must record an
unavailable external lane as `blocked`. That lane blocks qualification/promotion, but does not
deadlock the task that installs or repairs it. No exception weakens final qualification.

| Gate | Exact outcome |
| --- | --- |
| **G0 ownership/profile** | Fork owner, support matrix, capacity target, exact app/package revisions, version policy, and acyclic profile task closure are committed. |
| **G1 public boundary** | Only the gateway is reachable; public API is template-driven; feeds are principal-bound opaque objects; all reads are proxied. |
| **G2 tenant/security** | Authentication, query policy, lifecycle authorization, revocation, quotas, audit, TLS, secrets, data minimization, HTTP hardening, and negative tests pass. |
| **G3 correctness/durability** | Publication coverage, catalog boot, purge completion, WAL/DS restore frontier, epoch/reset, schema/TRUNCATE, retirement, segmented log, catalog compaction, and disk reserve contracts pass. |
| **G4 boundedness** | Every queue, request, snapshot, transaction, create, retained derived state, catalog, stream history, disk, socket, and scratch resource has accounting, a safe crossing action, and fixed overload evidence. |
| **G5 protocol** | Public/private schemas, stable errors, value/key/result schema, event framing, version negotiation, golden fixtures, and selected optional capability contracts pass cross-language conformance. |
| **G6 server tests** | Green inherited baseline, required real-PG18 black-box acceptance, Rust/TS/fuzz/external Electric suites, security, fault, oracle, capacity, backup/restore, and deployment checks pass for the exact digests. |
| **G7a compatibility client** | Every eligible app template passes provider, codec, cache-generation, lifecycle, shadow, app-host, cutover, and rollback gates. |
| **G7b native client** | Native core plus only profile-selected optional modules pass protocol, actor, persistence, lifecycle, security, device, and load gates. |
| **G8 operations/release** | Protected manifests, exact PG18 bootstrap/preflight/TLS and backup/PITR restore, gateway-registry ownership/restore, DS packaging, coordinated restore, same-primary replacement, PG-promotion reset, upgrades, observability, executable runbooks, signed artifacts, and rollback pass. |
| **G9 capacity** | The supported capacity table is produced from an open-loop fixed-operation harness and admission defaults stay below its conservative qualified bound. |
| **G10a lab authorization** | Common-fence acceptance, isolated clone and rollback rehearsal pass; this authorizes passive shadow only. |
| **G10b shadow authorization** | Passive shadow evidence passes; this authorizes opt-in beta only. |
| **G10c beta authorization** | Beta evidence passes; this authorizes the first canary only. |
| **G10d staged GA authorization** | Separate hash-bound 10%, 50% and 100% commands consume the prior stage's evidence and exact cohort denominator. |
| **G10e decommission** | The rollback-support window/policy is complete and the old path can be removed; this is not a prerequisite to initial GA. |

## 4. Task completion rules

Every task below is a subagent-sized work packet unless labelled **integration task**. The assignee
must prepare an injectively named execution note keyed by task ID, execution-scope/profile identity,
and attempt (for example, `notes/execution/<task-id>/<profile-or-scope-hash>/a<attempt>.md`) with
starting SHAs, scope, decisions, changed artifacts, commands/results, and remaining risk. Its
handoff state is `ready_for_review | fail | blocked`, never terminal `pass`. The controller records
terminal `pass | fail | blocked | invalidated` separately at
`notes/execution/<task-id>/<profile-or-scope-hash>/a<attempt>.resolution.json`, content-bound to the
immutable handoff and integration result; it never amends the reviewed candidate. A task-scoped Git
commit or push occurs only with explicit user or delegated authority recorded in the packet;
otherwise the content-addressed handoff patch and note remain uncommitted for the authorized
integrator. A prepared patch records canonical binary/full-index bytes, base commit/tree, SHA-256,
expected result tree, changed-file manifest and evidence hashes; review and integration verify that
exact tuple, and any mismatch starts a fresh attempt. Each implementation task owns one principal file/module
boundary; cross-runtime proof belongs in a later integration task.

No evidence command runs from the author or control checkout. Each red, green, author/merge direct,
qualification and reviewer run uses a newly created source: a clean detached worktree at the exact
candidate commit, or a newly empty export of a prepared patch's independently verified expected Git
tree. The runner records pre/post commit/tree and tracked/index/untracked/ignored cleanliness (or a
complete exported file/mode/content manifest), binds immutable dependency/tool/fixture inputs and any
source-visible read-only dependency mount through a content manifest plus canonical resolver/mount-
topology hash, and gives each command a unique newly empty external output/cache/fixture/artifact root.
The source is empty of untracked/ignored overlays before mounts attach; immediately before and after
the command, its entire overlay inventory must equal only that declared immutable mount topology. It
also binds the canonical effective environment/config hash. A dirty/reused source, undeclared or
writable overlay, mutable/stale external input, reused/nonempty run root, source mutation, identity/
config mismatch or missing attestation is `fail`, even when the behavior assertion passed. Reviewers
recreate the source, dependency mapping, empty run root and attestations independently.

All engine-touching work runs its generated author/merge direct gates from `AGENTS.md`; a named
baseline-repair exception is limited to its exact recorded base failure/assertion as defined in section
3. Release evidence additionally runs the Swift boundary script/tests, all three external Electric
lanes (`oracle`, `property`, `subqueries`), and browser demo verification for a live-path change.
An unavailable external qualification lane is recorded as a non-promotable `blocked` result, never
waived or misreported as an author-level pass.

Qualification uses a checked-in workload/cut-point manifest, not elapsed-day monitoring:

- fixed seeds and operation distributions;
- attempted/offered/admitted/committed/applied/rejected/compared counts;
- a qualifying event floor for every template, not only a global mutation count;
- virtual clocks at `t-1`, `t`, and `t+1` for leases, TTLs, retry, rotation, and retention;
- enumerated cut-point IDs and expected terminal states;
- raw samples, exact digests/config hashes, and first-divergence replay;
- zero unexpected divergence; every allowed representation difference is registered before a run;
- p999 only with at least 100,000 observations in that operation class; and
- deterministic cut-point coverage is not presented as a statistical failure-rate estimate.

Every workload also declares an exact attempt/offered budget, per-operation diagnostic deadline,
global harness deadline, minimum admitted/committed/applied counts, and deterministic stop condition.
Hitting an attempt/deadline cap without the minimum is `fail` or a named external `blocked`, never an
unbounded run. Every divergence is compared with a signed pre-run allowlist hash; changing that hash
restarts the stage. Production cohort evidence pins the eligible denominator, assignment/consent hash,
minimum per OS/device/account/template cohort, and real-versus-synthetic counts; synthetic work may
satisfy load floors but not human exposure floors.

Cut manifests have two required tiers: external release-candidate cuts (process, connection, public
request/response, external storage request, volume and readiness events) and implementation-invariant
cuts (fsync, journal/checkpoint, catalog/rotation and deferred scheduler hooks) in an instrumented build
from the same source SHA. Stable scenarios assert only external outcomes. Promotion requires both
tiers plus a disabled-hook equivalence smoke; it never calls the instrumented binary the release
artifact.

Timer tests also have two tiers. Deterministic model/process tests use injected clocks at `t-1/t/t+1`.
An exact release candidate may use a real external clock only when the timeout itself is the PG or
wire behavior under test; it polls an explicit terminal state with a diagnostic deadline and never
sleeps to create order. Acceptance-owned sources are linted against direct sleeps and ad hoc wall-
clock polling outside the central deadline module.

The default long correctness corpus is 10,000,000 committed mutations, 10,000 lifecycle cycles per
enabled client/transport, and 100 executions per enumerated failure cut point. A smaller focused
unit-test count is allowed only when the workload manifest names its distinct proof obligation; it
does not replace the long release corpus.

## 5. Planning, governance, and release tasks

### PLAN-001 — Machine-readable DAG and task/profile validator

**Depends:** none. **Profiles:** all. **Boundary:** spec tooling.

Deliver these exact artifacts: `docs/production/readiness-tasks.json`,
`docs/production/readiness-task.schema.json`, `docs/production/readiness-gates.json`,
`scripts/readiness-plan.ts`,
`scripts/readiness-plan.test.ts`, and generated
`docs/production/readiness-plan.generated.md`. The task graph records exact IDs, dependencies,
applicability, execution scope (`shared_producer` or `per_profile`), artifacts, owners, principal
read/write/semantic resources, gate phases, proof kind and required scenario IDs. Dependency edges
are typed `integrated` or `red_artifact`; the latter binds one provider/consumer/scenario/scope/base
artifact and may unlock only that implementation after independent `red_proved` review. It does not
invent semantic hashes before `E2E-000S`; instead the validator prevents a `genuine_red` task from
becoming ready until the registered ID/hash exists.

Its launch packet uses only the section-2 `uncompiled_all` bootstrap identity pinned to the canonical
specification and execution-protocol blobs/tree; the future output paths above are declarations, not
hashed inputs. It generates the typed task-manifest/execution-scope identities required to issue the
first non-bootstrap packet before that packet is reserved. Reject ranges, prose/`A or B` dependencies,
unknown IDs, cycles, duplicate authoritative task definitions, false profile inheritance, required
N/A dependencies, disabled tasks in a closure, reverse-wave scheduling, duplicate artifact ownership,
an unregistered ready behavior contract, a red artifact reused across consumers or lacking an
independent review/current base identity, and tasks without a principal write boundary. Include
mutation fixtures for each first-draft cycle and each PG18/E2E differential-review failure.

The generated gate matrix records, for every task/gate pair, gate ID, author/merge/qualification
phase, applicability expression, command/config identity, owner and any exact baseline assertion.
Packets reference its blob/canonical hash and may add only stricter gates. Reject an omitted or moved
gate, an inherited observation without its exact `TST-000` hash, and a baseline exception outside the
declared owner/assertion. The task schema and gate matrix also require the clean evidence-source
strategy; pre/post attestation fields; content-digested external-input and resolver/mount-topology
identities; a unique initially empty external run root; and the canonical effective-config hash for
each evidence row. Mutation fixtures reject author/control-checkout evidence, reused sources/run
roots, undeclared or writable overlays, mutable/mismatched dependencies, in-source writable outputs,
missing post-run checks and a tree or config mismatch. Local format/Rust/TypeScript/conformance are
direct for ordinary engine work; external Electric/browser lanes are qualification unless the matrix
declares that the task owns or modifies that lane. Missing qualification remains `blocked` and non-
promotable, not a direct-gate deadlock.

The exact versioned authoritative planning-input allowlist contains this canonical specification and
`notes/skills-research/05-parallel-agent-execution-protocol.md`, each by blob/tree ID. The six files
above are owned generated outputs and become validator inputs only after `PLAN-001` writes and hashes
them; none is an independent prose task source. Superseded note 16 and `notes/reviews/**` are immutable
historical evidence, never registry inputs; notes 23–25 may supply scenario/review traceability but are
linted to contain no authoritative task metadata.

Acceptance: the full graph topologically sorts; the bootstrap packet validates without a release
profile, task-manifest input or populated scenario registry; all six declared outputs above are
content-identified after generation; a non-behavioral shared packet can use only the typed planning
scope, while selected-profile and `genuine_red` packet generation fails until its required profile and
registered scenario identities exist; 100 randomized ready-task scheduling simulations reach a
terminal state; lease fixtures cover authenticated renewal, missed heartbeat/control-plane loss,
generation change and silent-renewal rejection; clean-source fixtures cover exact committed trees,
verified prepared-patch exports, declared read-only package-manager mounts, ignored/writable overlays,
mutated external inputs, reused/nonempty run roots, post-test source mutation and effective-config
mismatch; every seeded invalid graph fails with the named reason.

### GOV-001 — Fork ownership and upstream posture ADR

**Depends:** `PLAN-001`. **Profiles:** all. **Boundary:** governance docs.

Select indefinite self-ownership or no-ship; name release/security/support roles, upstream intake and
merge policy, and response if upstream never adopts the fork. Correct all “official successor” claims
unless backed by a dated primary source. No “wait for upstream” state is permitted.

### GOV-002 — Support and capacity target manifests

**Depends:** `GOV-001`. **Profiles:** all. **Boundary:** `docs/production` manifests.

Commit supported Postgres/DS/platform/topology/query/type/recovery behavior and nonzero demand values
for offered+accepted writes, clients, feeds, distinct/shared shapes, rows/width, transaction
distribution/max, fan-out, templates/selectivity, tenant skew, churn, long polls, renewals, dormant/
slow consumers, component CPU/RAM/IOPS/disk/FD/connections, headroom, RPO/RTO/refetch, queue/rejection
budgets, warm-up/sample cadence, estimators, variance and slope thresholds. Use explicit
`unsupported`, never blank.

### CMP-000 — Freeze the real app and ElectricSync baseline

**Depends:** `GOV-001`. **Profiles:** client profiles. **Boundary:** app dependency manifest.

Confirm the actual production app/revision. The inspected candidate
`../indexed-mighty-prod-ecs-proof` uses a materially customized vendored package at
`ios/Index/LocalPackages/ElectricSync`; the sibling `../electric-sync-swift` version/tests do not
qualify it. Record app commit, vendored-subtree content hash, any proven upstream-base/patch
provenance, Circuits, schema and semantic epoch. Treat sibling-package conformance separately unless
the exact patch/rebase relationship is proven. Any ElectricSync change is a distinct reversible app
change and regenerates inventory/tests.

Acceptance: clean-checkout CI fails if a pinned revision changes without regenerating the inventory,
codec, and compatibility corpus.

### GOV-003 — Namespace-qualified tracker and fork-delta map

**Depends:** `GOV-001`, `PLAN-001`. **Profiles:** all. **Boundary:** issue/release tooling.

Map every `electric-sql`, `pgxsinkit`, and local-fork issue/PR to fixed-with-test, a task ID, a profile
exclusion, or upstream-only. Bare `#N` is invalid. Generate fork divergence and unmapped-issue checks.

### GOV-004 — Version and compatibility policy

**Depends:** `GOV-001`, `SEC-000`, `PROTO-001A`, `PROTO-001B`. **Profiles:** all.
**Boundary:** version/compatibility docs.

Define semver and supported N/N-1 relationships for engine, gateway protocol, catalog/change-log,
qualified DS version/format, TS client, and Swift package. Define additive/breaking rules and the
minimum rollback behavior; artifact construction belongs to `RLS-001`.

### GOV-005 — Compile selected release profiles

**Depends:** `GOV-002`, `CMP-000`, `CMP-001`, `PLAN-001`. **Profiles:** all.
**Boundary:** release-profile generator.

Generate exact required/excluded task IDs and gate predicates from the canonical `lane`/`features`
axes. Store conditional dependencies as machine expressions and generate prose from them; no prose
clause is schedulable. Reject traffic/config for disabled capabilities before catalog/stream/PG work.
Test compatibility-only, native-only, every legal feature combination, feature-without-core,
required-dependency-inapplicable, and one-lane-failure fixtures.

### NATIVE-ADR-001 — Authorize the native Swift product

**Depends:** `CMP-001`, `GOV-002`. **Profiles:** native. **Boundary:** Swift product ADR.

Name at least one production template that compatibility mode cannot serve, package repository and
owner, compiler/platform floor, selected modules, version policy, maintenance/rollback commitment,
and why a new package is warranted. Select exactly one real-app integration path per template:
`APP-NATIVE-CONSUMER-001` for event/in-memory ownership or `APP-NATIVE-SINK-001` for durable local
replication, plus its production credential-provider owner. No inventory consumer means exclusion.

### RLS-001 — Immutable release artifacts and evidence attestation

**Depends:** `GOV-004`, `SEC-008A`, `SEC-008B`, `GWR-002`, `PGR-001`, `TST-003`.
**Profiles:** all.
**Boundary:** release workflow.

Assemble, sign and attest the content-addressed candidate engine, qualified DS, gateway, optional API,
deployment, DB bootstrap, migration, runbook, dashboard, protocol fixture, Swift/TS/app packages,
SBOM, provenance, security, capacity and test artifacts already exercised by `TST-003`. It may not
rebuild or mutate any tested byte. Bind source/dirty-tree status, digests, protocol, capacity, profile,
deployment hashes, toolchains, seeds and results. Missing, stale, blocked, rebuilt, unsigned or
digest-mismatched evidence rejects promotion.

## 6. Trust boundary, protocol, and security tasks

### SEC-000 — Freeze the production trust/authorization contract

**Depends:** `SEC-001`, `CMP-001`. **Profiles:** all. **Boundary:** security ADR.

Select public request `{templateID, templateVersion, typed parameters, idempotencyKey}`; normalized
principal `{issuer, subject, securityDomain, session, roles}`; gateway-owned opaque feed; private
engine definition/claim/path; proxy-only reads; bearer-header auth; no browser/cookie support; and
policy-version/revocation semantics. This decision precedes public schema implementation.

### PROTO-001A — Public template/feed/result-schema contract

**Depends:** `SEC-000`. **Profiles:** all clients. **Boundary:** public OpenAPI/JSON Schema.

Specify create/renew/release/read/query/aggregate using opaque public IDs and resume tokens. Include an
immutable `ResultSchema`: canonical alias, ordered fields, scalar kinds, nullability, field-presence
policy, ordered PKs, key codec, schema fingerprint/generation, and template version. Public schemas
contain no `table`, `where`, raw predicate/columns/subquery, engine shape/subscription, DS path/URL, or
admin operation. Add keyed subset pages `{key,value}` and opaque cursors even before the subset profile
is enabled.

### PROTO-001B — Private gateway↔engine contract

**Depends:** `SEC-000`. **Profiles:** all. **Boundary:** private OpenAPI/JSON Schema.

Version native shape/aggregate lifecycle, query, named claims, stream HEAD/read, values, opaque keys,
lease/replacement, snapshot/reset, and selected security-domain charging metadata. Keep tRPC and
debug/admin routes out of this contract.

### PROTO-001C — Implement server result-schema, scalar and opaque-key fidelity

**Depends:** `PROTO-001A`, `PROTO-001B`, `GOV-002`, `CMP-001`. **Profiles:** `COMMON_SERVER`.
**Boundary:** Rust schema/value/key codec and engine response encoding.

Implement exactly the scalar/type/key set selected by the support/app inventory; preserve field
presence versus SQL NULL, integer/decimal bounds, timestamp/time-zone, bytes, JSON/array and Unicode
semantics when selected, ordered composite identity and opaque canonical key bytes. A PostgreSQL type
without a qualified mapping is rejected before feed creation, never coerced generically to Float or
Text. Compatibility Electric keys and native keys retain distinct tagged grammars.

Acceptance: schema-directed golden/property/fuzz cases and real PG18 round trips preserve every
selected kind/key; unsupported types, duplicate canonical keys, malformed identities and lossy
numeric/text coercion fail with stable admission errors.

### PROTO-001D — Preserve result-schema and key semantics through the gateway

**Depends:** `PROTO-001C`, `SEC-003`. **Profiles:** all clients.
**Boundary:** gateway schema/value/key translation.

Proxy/translate the immutable schema and field-presence/key identity without inferring from JSON or
normalizing the compatibility and native grammars together. Bind template/version/generation and
reject a production compiler/output schema mismatch before body bytes.

Acceptance: independent SQL fixtures detect wrong tenant predicate, missing↔NULL, scalar rounding,
composite-key collision, wrong tombstone and stale schema/generation mutants across gateway encoding.

### PROTO-002 — Stable errors, idempotency, and completion semantics

**Depends:** `PROTO-001A`, `PROTO-001B`. **Profiles:** all. **Boundary:** shared error fixtures.

Define code/status/retryability/request ID/delay for auth, policy, quota, invalid input, schema/epoch,
closed/gone/replacement, protocol mismatch, and retirement pending/completed. Public idempotency keys
are scoped to principal+template. Define ambiguous create/renew/release recovery and never expose
anonymous decrement or `purge=true` to clients.

### PROTO-003A — Event-level stream framing contract

**Depends:** `PROTO-001A`, `PROTO-001B`, `PROTO-002`. **Profiles:** `COMMON_SERVER`.
**Boundary:** framing fixtures.

Specify body/header limits, long-poll timeout, next offset, caught-up, closed/gone, reset, decompression,
unknown-field/control handling, and event/response-level delivery. Backfills and no-output source
transactions have no transaction marker. Split fixtures at every byte/envelope/page boundary.

### PROTO-003B — Optional per-stream transaction capability

**Depends:** `PROTO-003A`, `ENG-002`. **Profiles:** `NATIVE_TXN_ATOMIC`.
**Boundary:** transaction fixture/capability schema.

Specify one non-empty projected source transaction on one stream, stable source token, final marker,
chunking, supported execution tiers, and no cross-stream atomicity. A stream/template whose deferred
output coordinator is unqualified does not negotiate this capability.

### PROTO-004 — Version negotiation and compatibility corpus

**Depends:** `PROTO-002`, `PROTO-003A`. **Profiles:** all clients. **Boundary:** negotiation fixtures.

Implement supported/min/max version discovery, unknown semantic field behavior, N/N-1 fixtures, and
CI schema-diff approval. Unsupported clients fail before creating a feed.

### SEC-001 — Threat model and data/route/socket inventory

**Depends:** `GOV-002`. **Profiles:** all. **Boundary:** threat/data map.

Classify every listener/route and data copy including PG/DS/catalog/change/shape WAL, spill/scratch,
gateway registry, logs/traces/metrics/backups/CI artifacts, and mobile DB/WAL/SHM. Each has identity,
authorization, sensitivity, retention, encryption, negative-test ID, and owner.

### SEC-002A — Credential validation and principal normalization

**Depends:** `SEC-000`, `E2E-002R`. **Profiles:** all clients. **Boundary:** gateway auth middleware.

Implement exact issuer/audience/authorized-party/algorithm/kid/signature/expiry/not-before/session/
security-domain checks. No mobile client secret. App Attest/DPoP is either an abuse/PoP feature or
explicitly not tenant authorization. Every rejection precedes registry/engine/DS/PG activity.

### GWR-001 — Own and qualify the gateway registry store

**Depends:** `GOV-004`, `SEC-001`, `SEC-006A`, `CAP-001A`, `E2E-002R`. **Profiles:** all clients.
**Boundary:** gateway-registry database/package.

Select and pin the registry database, exact single-gateway topology, transaction/isolation and unique-
key contracts, writer fencing, schema migrations, encryption, connection/queue/admission bounds,
physical accounting, backup interface and corruption/failure policy. The first profile has one
gateway process; multi-replica gateway/ledger HA is a future profile with a new fencing proof.

Acceptance: idempotency/principal/feed uniqueness holds across 100 commit/result/crash cuts; a second
writer, wrong store identity, failed migration, corrupt/incomplete recovery, exhausted pool/disk and
plaintext profile refuse before authorization/lifecycle mutation.

### SEC-002B — Durable public feed registry and claim reconciliation

**Depends:** `SEC-002A`, `PROTO-001A`, `PROTO-002`, `GWR-001`, `E2E-002R`. **Profiles:** all clients.
**Boundary:** gateway registry schema/service.

Transactionally bind principal security domain/subject/session/idempotency key/template/policy version
to opaque feed ID, internal shape/subscription/path identity, generation, and state. Gateway generates
internal claims. Reconcile crashes and lost responses between registry and engine create/release;
repeat creates produce one owned claim.

### GWR-002 — Gateway-registry backup, restore and authority reconciliation

**Depends:** `GWR-001`, `SEC-002B`, `E2E-002R`. **Profiles:** all clients.
**Boundary:** registry restore/reconciliation tooling.

Produce and restore checksummed registry manifests; compare registry generations/claims/revocations
with engine, catalog and DS before public readiness. Exercise ahead/behind/lost/corrupt/partial store,
schema upgrade/rollback and response-loss boundaries. Never reconstruct authority by guessing from an
engine shape or DS path; select exact reconciliation or deny/reset every affected public generation.

Acceptance: 100 empty-host and divergent-store restores yield the exact prior principal binding or a
typed full-generation reset; no foreign/forgotten/revoked claim becomes readable or renewable.

### SEC-002C — Policy/session revocation barrier

**Depends:** `SEC-002B`, `SEC-003`, `E2E-002R`. **Profiles:** all clients. **Boundary:** gateway revocation worker.

Stop new creates/renewals, cancel withheld reads before body bytes commit, stop renewal, release the
exact claim, invalidate policy generations, and record completion. State explicitly that already
delivered data cannot be recalled from a hostile offline device.

### SEC-003 — Server-owned template compiler and tenant injection

**Depends:** `SEC-000`, `CMP-001`, `E2E-002R`. **Profiles:** all clients. **Boundary:** gateway policy/compiler.

Map template+typed parameters+principal to an allowlisted table/projection/predicate/subquery/order/
page/aggregate and maximum result. Inject every tenant/access-cohort predicate server-side. Property
tests mutate every client field and cannot broaden outer or inner scope.

### SEC-003B — Sync data minimization and publication manifest

**Depends:** `SEC-003`, `ENG-017`. **Profiles:** all. **Boundary:** schema/publication review tool.

Require explicit relations/columns and classify sensitive fields. Compare templates, engine selectors,
and `pg_publication_tables/columns`; new tables/columns are not replicated or queryable until reviewed.
Never rely on RLS as the logical-replication tenant boundary without a separate proof.

### SEC-004 — Proxy all public stream reads

**Depends:** `SEC-002B`, `SEC-003`, `PROTO-003A`, `E2E-002R`. **Profiles:** all clients.
**Boundary:** gateway feed proxy.

Expose `GET/HEAD /client/v1/feeds/{opaqueFeed}` only, authenticate/authorize every request, translate
to one private DS path, canonicalize offsets/path, allowlist response headers, bound bytes, propagate
cancellation, disable redirects, and never expose DS origin. Every other verb/path/encoding and every
cross-principal substitution generates zero DS request.

### SEC-005A — Split engine data/admin/probe/scrape listeners

**Depends:** `OPS-001A`, `SEC-001`, `E2E-002R`. **Profiles:** all. **Boundary:** engine routers/config.

Generate a route manifest. Gateway listener gets only private data operations; probes get health/
ready; scrape gets read-only metrics; admin gets separately selected operations. Row/log/graph/state/
trace/memory/profile, schema/table mutation, purge, epoch and metrics reset are absent unless an
operator profile enables them. Overlapping/wildcard unsafe binds fail preflight.

### SEC-005B — Operator authentication and authorization

**Depends:** `SEC-005A`, `SEC-002A`, `E2E-002R`. **Profiles:** all. **Boundary:** admin middleware/RBAC.

Give probe, scrape, gateway, read-only operator, and destructive operator distinct identities and
method/route permissions. A compromised gateway cannot mutate tables/schema, purge, reset epoch/
metrics, or read row diagnostics. Audit every admin attempt.

### SEC-006A — Authenticated transport on every production hop

**Depends:** `OPS-001A`, `SEC-001`, `E2E-002R`. **Profiles:** all. **Boundary:** TLS/connectors.

**Amendment (2026-09-16):** Private-subnet plaintext HTTP between the engine and Durable Streams,
with security-group scoping and API-brokered client access, is the supported production mode;
DS TLS/mTLS is optional. Selected profiles must record and verify that DS network/access boundary.
Gateway HTTPS, PG query-pool and walsender verify-full TLS, and TLS/mTLS or one named verified mesh
for other internal listeners remain required. Test CA/hostname/validity/client identity/plaintext/
stripping for configured TLS independently; no fallback to `NoTls` or plaintext outside the explicit
DS HTTP mode.

### SEC-006B — Secret/key reload, rotation, and revocation

**Depends:** `SEC-002A`, `SEC-006A`, `E2E-002R`. **Profiles:** all. **Boundary:** secret providers/runbook hooks.

Inventory IdP JWKS, gateway-registry keys, DB/DS identities, TLS, audit, backup, and signing keys;
define storage/distribution/dual-version activation/revocation barriers and reload. Scan args, images,
config summaries, logs/traces/artifacts for seeded canaries.

### SEC-006C — Encryption at rest and spill protection

**Depends:** `SEC-001`, `DST-001`. **Profiles:** all. **Boundary:** volume/object/file protection.

Encrypt DS PV, DBSP/transaction spill, backups, and gateway registry; define keys, rotation, restore,
file permissions, orphan cleanup, and deletion. A known plaintext sentinel must not appear in detached
block snapshots or abandoned spill files.

### ADM-001 — Resource accounting and admission contract

**Depends:** `GOV-002`, `SEC-000`, `CAP-001A`. **Profiles:** all. **Boundary:** quota schema/ADR.

For every resource, name unit, reservation/commit/release point, owner, hierarchy, hard limit, error,
and behavior for shared shapes. First release either disables cross-security-domain shape sharing or
uses an explicit multi-owner charging ledger. Already committed WAL is never rejected for size.

### SEC-007 — Single-gateway quotas and reconciliation

**Depends:** `ADM-001`, `SEC-002B`, `GWR-001`, `ENG-007`, `ENG-008`, `ENG-009`, `ENG-009A`, `ENG-010`.
**Profiles:** all clients. **Boundary:** gateway quota ledger.

Enforce principal/security-domain/global limits atomically in the first profile's one gateway/
registry writer and reconcile reservations after cancellation/crash. Race `limit+1` concurrent
requests; exactly `limit` land and rejected work leaves no downstream state. Multi-replica quota
coordination is excluded until a separate HA/fencing profile is qualified.

### SEC-008A — Reproducible content-addressed candidate server artifacts

**Depends:** `OPS-001B`, `DST-001`. **Profiles:** all. **Boundary:** candidate build/admission policy.

Pin bases/actions, use locked builds, and produce content-addressed candidate engine/gateway/
qualified-DS images plus SBOM/provenance/signatures. Candidates cannot be rebuilt in place after a
test records the digest. Run numeric non-root/read-only/capability/seccomp policies and reject
mutable/unsigned/wrong-source artifacts. `RLS-001` later assembles and attests these exact bytes.

### SEC-008B — Dependency, vulnerability, license, and exception governance

**Depends:** `GOV-001`. **Profiles:** all. **Boundary:** scanning/update policy.

Cover Cargo/npm/SwiftPM/OS/base images, secret and license scanning, update automation, severity gates,
and owner+expiry-bound exceptions. Inject vulnerable/unlocked/unpinned/disallowed fixtures and require
independent release failure.

### SEC-009 — Public HTTP origin hardening

**Depends:** `SEC-000`, `SEC-002A`, `E2E-002R`. **Profiles:** all clients. **Boundary:** gateway HTTP edge.

Bearer headers only, no auth cookies/browser CORS in release one, `no-store, private`, trusted host/
proxy canonicalization, no authenticated redirects, and limits for request/header/body/depth/nodes/
compression/unauthenticated connections/response. Test every boundary at `limit-1/limit/limit+1`.

### SEC-010 — Security audit and privacy lifecycle

**Depends:** `SEC-002C`, `SEC-005B`, `SEC-007`. **Profiles:** all. **Boundary:** audit schema/sink.

Emit pseudonymous, integrity-verifiable events for auth/policy/feed/read/revocation/quota/admin/schema
decisions without credentials, DS locators, raw parameters, rows, or tenant IDs. Define access,
retention, export, legal hold/deletion, backup, and incident use; verify mutation/reorder/replay
detection and forbidden-canary absence.

## 7. Durable-streams, storage, and engine tasks

### CAP-001A — Bounded-resource metric and evidence schema

**Depends:** `GOV-002`. **Profiles:** all. **Boundary:** metrics/evidence types.

Define stable names/units for every queue, spool, pool, request, snapshot, catalog, stream, disk,
socket, retained-state and client buffer current/limit/peak/wait/reject value. Predeclare sampling,
warm-up, estimators, confidence/rank handling, overhead threshold, and low-cardinality labels. Engine
tasks implement these names; `OPS-006` later owns dashboards/alerts.

### DST-001 — Own and qualify durable-streams as a production database

**Depends:** `GOV-004`, `OPS-001A`, `SEC-006A`. **Profiles:** all. **Boundary:** DS fork/package/API.

Name the DS owner and pin source/version/format/image. Add authenticated readiness/preflight exposing
durability mode, store UUID, layout version, recovery/checkpoint state, free/reserved physical bytes
and telemetry capability. Add bounded inventory/list with stream class, state, tail and physical size;
atomic append reservations; WAL/fsync/queue/FD metrics; single-volume ownership refusal; and N/N-1
upgrade/refusal fixtures. File-backed mode is mandatory in production.

Acceptance: 1,000 streams/10,000 appends plus close/delete survive 100 WAL cut points; memory mode,
wrong store UUID, incomplete recovery and second writer refuse before any engine readiness/mutation.

### DSR-001 — Quiesced DS backup tool and restore manifest

**Depends:** `DST-001`. **Profiles:** all. **Boundary:** DS backup tooling.

Drain/stop the engine, checkpoint/stop DS, snapshot the complete store, then restart. Emit a checksummed
manifest with store/stack UUID, format and image digests, catalog tail/hash, stream inventory/checksums,
change position/highwater, last complete source LSN, slot binding and every local/object artifact. Do
not claim online volume-copy support.

### DSR-002 — Restore frontier verification and decision table

**Depends:** `DSR-001`, `PGR-001`, `ENG-015`. **Profiles:** all. **Boundary:** pre-readiness restore verifier.

Compare manifest frontier with Postgres cluster/timeline/slot/plugin/wal-status/confirmed-flush LSN
before applying catalog or mutating DS/PG. Enumerate whole-stack, DS-only, PG-only/PITR, ahead/behind/
equal slot, missing/lost/foreign slot, missing segment and partial object restore. Same-epoch resume is
allowed only with proof; DS rollback behind an advanced slot is never accepted.

### DSR-003 — Authorized epoch reset/full public-generation invalidation

**Depends:** `DSR-002`, `GWR-002`, `SEC-002B`, `SEC-005B`. **Profiles:** all. **Boundary:** reset workflow.

When continuity proof is absent, durably drop/retire every shape, invalidate every gateway feed
generation, bind a fresh slot/store epoch, and force one typed client full rehydrate. Reset is an
operator-authorized operation, never an automatic silent resume.

Acceptance for `DSR-001`–`003`: restore a snapshot at T020 after slot/clients reach T100 across 100
cuts at change append, shape append, catalog checkpoint, slot feedback and snapshot finalization. Each
run reconstructs T001–T100 or refuses readiness and emits exactly one whole-generation reset.

### STO-001 — Crash-safe catalog snapshot/compaction

**Depends:** `DST-001`, `GOV-004`, `PROTO-002`, `E2E-001R`. **Profiles:** all. **Boundary:** catalog storage/fold.

Add versioned folded snapshots and atomic generation switch/reclamation preserving event/highwater,
max shape ID, epoch, subscriptions+lease ages, dormant gates/positions, checkpoint+dedup highwater,
segments and unmatched `Dropped`. Bound boot pages, resident EID dedup, writer bytes and disk growth.
Reserve/stage mandatory catalog events before in-memory mutations; renewal coalescing must not cross a
later `Left`/`Dropped`/owner transition.

Acceptance: after 10,000,000 catalog/lifecycle events, disk/boot RSS/time meet the manifest; 100
crashes at every snapshot/switch/reclaim step fold identically and preserve every pending retirement/
ID/claim.

### STO-002 — Bound active shape history with a typed reset boundary

**Depends:** `DST-001`, `PROTO-002`, `PROTO-003A`, `E2E-001R`. **Profiles:** all. **Boundary:** stream generation/
retention policy.

For release one, retire an active output stream before its physical/logical quota consumes the
reserved control/catalog/change capacity; write durable `Dropped`, close/delete, then make clients
full-refetch. Do not truncate in place without consumer acknowledgement. Define slow/offline consumer,
lease and stream-generation semantics. Compaction may replace retirement only after separate proof.

### ENG-001 — Visibility-correct subset page/live fence

**Depends:** `PROTO-001A`, `PROTO-001B`. **Profiles:** `NATIVE_SUBSET`.
**Boundary:** PG query/subset protocol.

Replace bare LSN comparison with a server-owned atomic page+feed operation or opaque xid-visibility
fence equivalent to `SnapshotGate`. Apply it to initial, replacement and load-more pages and direct/
deferred events; preserve tombstones, keyset order, NULL and C-collation semantics. If an event cannot
carry/classify its causal source, that subset/template is rejected.

Acceptance: named hooks/synthetic gate tests cover xmin/xmax/xip and the WAL-recorded/not-visible
window; 100,000 randomized page/feed interleavings for insert/update-in/update-out/delete/reorder and
simple/subquery feeds equal repeatable-read SQL.

### ENG-001A — Canonically keyed subset pages and composite identity

**Depends:** `PROTO-001A`, `ENG-001`. **Profiles:** `NATIVE_SUBSET`. **Boundary:** query rows/cursors.

Return `{key,value}` using the exact shape-envelope key and an opaque cursor binding template/schema/
order/fence. Implement full ordered composite-PK tie-breaks or reject composite-PK subsets in the
profile. A client never derives identity with `String(row[pk])`.

Acceptance: 100,000 seeded one- through four-component keys (escape characters, U+001F, Unicode,
booleans, floats and Int64 extremes) match page/snapshot/live keys with no collision and exact delete.

### ENG-002 — Optional transaction-aware output coordinator

**Depends:** `PROTO-003A`, `ENG-007A`. **Profiles:** `NATIVE_TXN_ATOMIC`.
**Boundary:** sequencer/subquery/output arbitration.

Coordinate direct routing, circuit/dynamic aggregates, subquery flips, deferred query-backs and
emission lanes. Register async children before close, keep each affected stream's source transactions
ordered/contiguous, append `last` only after every emitter resolves, and hold a durable/spillable child
or source checkpoint. Retirement is the only substitute for landing. If deferred subquery tiers are
not implemented, advertise event-level capability for them and reject txn-atomic templates.

Acceptance: block nested query-backs while a later transaction touches the same keys; across 100
scheduler seeds/raw-stream crash cuts, transaction 1 is contiguous with exactly one true final marker
before transaction 2, or one typed reset.

### ENG-003 — Recoverable terminal behavior on the Electric adapter

**Depends:** `PROTO-002`, `E2E-001R`. **Profiles:** `COMMON_SERVER_QUALIFICATION`. **Boundary:** `electric.rs`.

Map backing stream closed/gone to 409 `must-refetch`, not 500; preserve false-proxy-404 reconciliation,
handle expiry/restart and 204 idle headers. Add retention/drift/purge/epoch/restart fixtures.

### TSC-001 — Recover native TypeScript readers after stream retirement

**Depends:** `PROTO-002`, `ENG-003`. **Profiles:** `COMMON_SERVER_QUALIFICATION`. **Boundary:** TS client.

Classify closed/404/410, renew with the same principal-bound claim through the gateway, accept
replacement, reset the fold/page, and prevent old cleanup from releasing the new claim. This closes
parent-fork issue #17 without coupling Swift to the TS implementation.

### ENG-004 — Fence replayed TRUNCATE retirement

**Depends:** `GOV-004`, `E2E-001R`. **Profiles:** all. **Boundary:** schema drift/catalog gate.

Persist/reseed sufficient per-dependent seed visibility so a replayed TRUNCATE already reflected in a
restored shape does not retire it; a not-reflected TRUNCATE still retires fail-closed. Under current
policy, subquery shapes are typed-retired on restore rather than falsely required to survive.

### ENG-005 — Safe selected-table discovery/re-creation

**Depends:** `ENG-017`, `OPS-008`. **Profiles:** `HOT_TABLE_RELOAD` only. **Boundary:** drift/schema
resolver.

Hot-install dynamic routing/fallback state under generation/resolve locks; compiled circuit layout
changes require controlled rebuild/restart. Cover add/drop/re-create/DML/concurrent create. Common
profiles instead validate and document restart-required behavior.

### ENG-006 — Bound PG replication setup and classify failures

**Depends:** `SEC-006A`. **Profiles:** all. **Boundary:** PG pool/walsender connection.

Bound connect and first replication frame/keepalive (not first DML), interrupt on shutdown, and
classify auth/config/TLS/no-SQLSTATE errors as permanent versus network/busy-slot retry. Test blackhole,
bad auth/CA/database, idle healthy server and shutdown at every await.

### ENG-006A — Bound internal/public HTTP transport resources

**Depends:** `PROTO-003A`, `DST-001`, `SEC-004`, `CAP-001A`. **Profiles:** all.
**Boundary:** DS reqwest/gateway HTTP clients.

Incrementally enforce compressed/decompressed/body/envelope/depth/page limits; connect/header/body/
idle deadlines; pool/in-flight/socket caps by work class; cancellation and shutdown. Test slowloris,
missing length, oversized chunks, stalled phases, distinct-offset poll flood and reconnect storm.

### ENG-007A — Spillable complete-transaction/output journal

**Depends:** `ADM-001`, `PROTO-003A`, `CAP-001A`, `E2E-001R`. **Profiles:** all. **Boundary:** sequencer transaction
staging.

Bound held input, decoded page, counts deltas, `txn_pending`, pending-create copies, activation/replay
and per-stream serialization using a checksummed disk-backed journal and chunked appends below DS hard
limits. A committed PG transaction is never rejected/retired because of size. Scratch exhaustion
stops processing/ack/checkpoint and resumes from durable input.

Acceptance: 64/128/129/512 MiB, 2× append cap, near-max envelope and scratch exhaustion at each chunk;
RSS stays below cap plus one explicit row/chunk, slot/checkpoint never passes, and all clients converge.

### ENG-007 — Bound every asynchronous queue and task set

**Depends:** `ADM-001`, `CAP-001A`, `STO-001`. **Profiles:** all. **Boundary:** channel/task inventory.

Inventory and bound by item+owned bytes: flip, emission, sequencer commands, catalog writer, retirement
dedup queue, PG waiters, HTTP requests, handle coalescers, DS requests and spawned tasks. Reserve before
state mutation for lossless work; separate critical lifecycle from rejectable diagnostics; define
drain/replay. Generate `limit-1/limit/limit+1` tests per queue.

### ENG-008 — Fair, bounded create/backfill/query-back admission

**Depends:** `ADM-001`, `ENG-007`, `CAP-001A`. **Profiles:** all. **Boundary:** lifecycle/PG pool.

Reserve PG capacity for control/drift, flips/query-backs, subsets and creates; bound waiters, create
concurrency, pending-delta bytes and total snapshot age including DS waits. Stream query-back candidates
instead of collecting. Cancellation/quota leaves no stream/catalog/share/registry/PG transaction.

### ENG-009 — Static admission and dynamic snapshot/materialization limits

**Depends:** `ADM-001`, `ENG-006A`, `CAP-001A`. **Profiles:** all. **Boundary:** backfill/query/adapter.

Separate pre-BEGIN static limits from incremental rows/encoded bytes; cap one decoded row/envelope,
use `LIMIT+1`/cursor streaming, remove arbitrary OFFSET from production templates, stream inner seeds,
isolate `/v1` full-snapshot concurrency and cancel on disconnect. Dynamic rejection cleans all state.

### ENG-009A — Bound continuously growing derived state

**Depends:** `ADM-001`, `PROTO-002`, `CAP-001A`. **Profiles:** all. **Boundary:** PK dictionaries,
membership, aggregates and circuit groups.

Generation-scope/compact the append-only PK dictionary and handle u32 exhaustion; account feed keys,
contributors/reverse indexes, Electric handle sets/coalescers, MIN/MAX multisets and count groups.
At live crossing, spill/fallback or durably retire/reset before discarding an effect; never silently
advance. Test 10,000,000 unique-PK churn at fixed live cardinality and small injected ID space.

### ENG-010 — Integrate physical disk budgets and emergency reserve

**Depends:** `ADM-001`, `DST-001`, `STO-001`, `STO-002`, `ENG-007A`, `CAP-001A`.
**Profiles:** all. **Boundary:** engine retention/storage accounting.

Reconstruct physical/logical usage across restart for catalog, current/closed input, active/dormant
outputs, WAL, pending retirement, DBSP/txn spill. Reserve independent metadata/control capacity for
`Dropped`, checkpoint, rotation and `Retired`; concurrent appends cannot overbook. Inject ENOSPC at
append/fsync/checkpoint/rotation/close/delete/spill; nothing acknowledges past data that did not land.

### ENG-011 — Storage-wide orphan stream reconciliation

**Depends:** `DST-001`, `DSR-002`, `STO-001`, `ENG-010`. **Profiles:** all.
**Boundary:** retirement/GC.

Compare bounded DS inventory with the verified catalog/store generation; run only under singleton
ownership; close then delete only true orphans and survive every GC crash phase. Future seamless HA
adds the same downstream fencing token before this task may run there.

### ENG-012 — Strict production configuration and DS attestation

**Depends:** `OPS-001A`, `DST-001`, `SEC-005A`, `SEC-006A`, `ADM-001`. **Profiles:** all.
**Boundary:** config/preflight.

Use an exhaustive key/schema; unknown/no-op/malformed values are fatal. Reject memory/wrong-UUID/
recovering DS, public/overlapping debug binds, undeclared plaintext, unlimited/broken budgets, missing spill,
invalid retention and unsupported profile flags. Implement or reject dedicated metrics port. Emit a
redacted effective config; demo defaults fail when labelled production.

**Amendment (2026-09-16):** Attestation accepts plaintext DS when the explicit private-DS HTTP mode
from `SEC-006A` is declared and its private subnet, security-group scoping, and API-brokered access
are verified. DS TLS/mTLS checks apply only when selected; store identity, durability, recovery, and
resource checks remain mandatory in either mode.

### ENG-014 — Make purge acknowledgement match retirement completion

**Depends:** `PROTO-002`, `E2E-001R`. **Profiles:** all. **Boundary:** lifecycle/retirement HTTP.

Select the stronger deterministic contract: terminal 2xx is returned only after close/delete and
durable `Retired`. The process owns the operation after request cancellation; an idempotent retry joins
the same completion. A pending response/status may be used internally, but never terminal `200 ok`
while DS still serves the stream.

Acceptance: 10,000 purges and 100 faults at Dropped/registry/close/delete/Retired/response/restart;
terminal success always means engine 404, DS gone/terminal and durable completion. This must repair the
current 284/285 baseline failure.

### ENG-015 — Transactional, fail-closed catalog application

**Depends:** `STO-001`, `PROTO-002`, `E2E-001R`. **Profiles:** all. **Boundary:** engine boot/restore.

Remove “catalog restore failed (continuing empty).” Validate/plan every record, schema, stream HEAD,
offset segment, retirement and seed without installing state, then commit atomically; any failure keeps
readiness false and storage/catalog unchanged. Unknown event/version/order is fatal at its exact offset.
Only authorized `DSR-003` may continue with a new empty epoch after durable retirement.

Acceptance: 10,000-shape catalogs, 100 failures/crashes per validation/commit step; boot restores the
identical complete registry or serves nothing, never empty/partial.

### ENG-017 — Validate publication completeness and live drift

**Depends:** `GOV-002`. **Profiles:** all. **Boundary:** PG publication inspection.

The first profile uses a bootstrap-owned publication that runtime engine/query/replication/migration
roles cannot alter. Fingerprint the full effective definition: selected table/partition expansion,
row filters, column lists, I/U/D/T flags, `publish_via_partition_root`,
`publish_generated_columns`, identity and publishable columns. Reject RLS-enabled tracked tables and
configure the walsender with `row_security=off` so a later RLS introduction halts rather than filters.
Periodic inspection is detection/diagnostics, never proof that an omitted wire change was caught in
time. Authorized publication/schema change uses `OPS-008`: fence readiness and public reads/writes
before DDL, jointly re-admit table+publication, then retire/reseed or remain unresolved.

Acceptance: every runtime identity is denied publication mutation; sanctioned changes prove readiness
unavailable before DDL and concurrent DML; missing table/operation, row filter, column list, partition
mode, RLS/policy, generated-column setting and valid fixture are checked at boot, live, reactivation,
reconciler retry and restore, with an unaffected-table control.

### LEAD-001 — First-release leadership/failover ADR

**Depends:** `GOV-002`, `DST-001`. **Profiles:** all. **Boundary:** topology ADR.

Codify one engine/one DS, RWO/recreate, operator-confirmed former-process termination, same-primary
restart, and PG18 promotion/timeline change→epoch reset. Automated seamless promotion and partition
tolerance are unsupported. The future `SEAMLESS_PG_FAILOVER` profile requires qualified PG18
synchronized failover slots and DS-enforced monotonic tokens on catalog/change/shape/close/delete/
slot feedback before `ready`.

## 8. Deployment, operations, and capacity tasks

### OPS-001A — Production service and storage scaffold

**Depends:** `SEC-000`, `SEC-001`. **Profiles:** all. **Boundary:** deployment chart/IaC.

Separate dev Compose. Scaffold one DS with RWO PVC/Recreate, one engine, exactly one gateway process,
its selected registry store, distinct private
data/admin/probe/scrape services, default-deny policies, service identities, resource/volume limits,
startup/readiness/liveness, ordered shutdown and termination grace. No public PG/DS/engine/API port;
API is absent unless an internal consumer is named.

### OPS-001B — Qualified protected production package

**Depends:** `OPS-001A`, `SEC-004`, `SEC-005A`, `SEC-005B`, `SEC-006A`, `ENG-012`, `LEAD-001`.
**Profiles:** all. **Boundary:** deployment integration.

Wire the implemented gateway, listeners, TLS, DS attestation, singleton ownership and strict config.
From the client network only the gateway resolves/connects. Clean install serves a selected template;
rollout drains without lost committed effect or overlapping engine/DS writer.

### OPS-002 — Execute offline backup, restore, disk and corruption qualification

**Depends:** `DSR-001`, `DSR-002`, `DSR-003`, `GWR-002`, `PGR-001`, `ENG-010`, `ENG-011`, `ENG-015`.
**Profiles:** all. **Boundary:** storage integration evidence.

Run empty-host restores for every supported matrix entry; flip/truncate/omit/duplicate/reorder samples
in every catalog/input/output/WAL/metadata/object class; exercise quota/reserve/full-disk recovery.
Exact resume or named fail-closed reset are the only outcomes. Retain manifest/evidence.

### PG18-000 — Freeze the PostgreSQL 18 support contract

**Depends:** `GOV-002`. **Profiles:** `COMMON_SERVER`. **Boundary:** support ADR and versioned DB
manifest.

Record the exact supported 18.x/minor policy; logical/SCRAM/verified-TLS and channel-binding
disposition; immutable publication; RLS rejection; slot/frontier/generated-column/type/identity
contract; provider backup/PITR assumptions; single-primary topology; slot-retention values relative to
planned outage; and promotion-reset semantics. For every PG image, record the OCI index digest, the
declared OS/architecture platform tuple, and the resolved platform-manifest digest; an OCI index
alone does not identify the bytes tested. Pin those exact candidates in CI, Compose, demos, examples,
docs and test discovery; never select the newest host binary implicitly.

Acceptance: every production/test entry point reports its exact PG18 minor, OCI index, platform tuple
and resolved platform-manifest digest, and the harness verifies that resolution before startup;
production-mode PG17 and an unapproved future major fail preflight; contradictory broad/PG16 claims
are removed;
just-below/above slot-retention/outage fixtures select continuation versus typed reset deterministically.
The task supplies canonical `PG18-E2E-001`–`014` definitions for `E2E-000S`; it does not claim the
later scenario registry or semantic hashes already exist.

### PG18-001A — Canonical publishable-column schema and admission

**Depends:** `PG18-000`, `E2E-000I`, `ENG-017`. **Profiles:** `COMMON_SERVER`.
**Boundary:** PG metadata/introspection and schema admission.

First register and reproduce the generated-column cases red through the isolated reference adapter.
Define one effective publishable-column set from relation metadata plus the immutable publication.
Reject virtual generated columns, unpublished stored generated columns, unpublishable identity fields,
RLS-enabled tracked tables and mismatched partition-root/leaf definitions. Run the same admission on
boot, create/join/reactivation, live drift re-introspection, reconciler retry and catalog restore.

Acceptance: table/publication mutations adding virtual or unpublished stored columns, toggling
`publish_generated_columns`, changing RLS and changing partition policy while live/down either remain
jointly admissible or retire/stay unresolved before fresh serving; an unaffected table remains live.

### PG18-001B — Use the canonical column schema on snapshot and live paths

**Depends:** `PG18-001A`. **Profiles:** `COMMON_SERVER`.
**Boundary:** table definition/fingerprint, backfill projection, tuple decode and replica identity.

Make `TableDef`, `TableSchema`, fingerprint, backfill SELECT/projection, JSON row mapping, pgoutput
tuple mapping and identity validation consume the one admitted schema. Missing wire fields are never
silently manufactured as SQL NULL. Preserve stored generated values on insert/update/delete and
predicate entry/exit.

Acceptance: focused schema/decoder/property cases prove every consumer uses identical ordered fields;
mutants that omit a consumer, conflate virtual/stored, or map missing to NULL fail before an
acknowledged feed.

### PG18-001Q — Qualify generated columns through the real engine adapter

**Depends:** `PG18-001A`, `PG18-001B`, `E2E-000I`. **Profiles:** `COMMON_SERVER`.
**Boundary:** real PG18 → engine process → file-backed DS → isolated reference materializer.

Run `PG18-E2E-003`–`005` plus live/down generated-column/publication/partition/RLS changes with
frozen source prefixes and the independent SQL oracle. Preserve the contract hash for the later public
gateway rerun.

Acceptance: stored-generated positives and every virtual/unpublished/identity/lifecycle negative pass;
snapshot and live schemas cannot diverge; boot/create/reactivation/restore never acknowledge an
inadmissible feed.

### PG18-002A — Observe and fail closed on every slot invalidation reason

**Depends:** `PG18-000`, `E2E-000I`, `ENG-006`. **Profiles:** `COMMON_SERVER`.
**Boundary:** slot observation, epoch verdict, readiness and diagnostics.

First reproduce reset-off invalidation red. Capture `invalidation_reason`; any non-null known or
future value latches a named epoch break and stops old-epoch serving. Use real primary-PG18 fixtures for
`idle_timeout` and `wal_removed`, a real restart fixture for `wal_level_insufficient` when supported,
and focused verdict fixtures for all documented strings, standby-only `rows_removed`, and unknown.

Acceptance: reset-off cases stay fail closed with stable public reason plus PG diagnostic; a
START_REPLICATION retry never reopens the old epoch; synthetic policy fixtures are not labelled real
black-box PG18 evidence.

### PG18-002C — Prove slot incarnation, frontier and timeline continuity

**Depends:** `PG18-000`, `E2E-000I`, `ENG-006`. **Profiles:** `COMMON_SERVER`.
**Boundary:** durable source frontier and epoch continuity verdict.

Persist the last completely landed source frontier and compare it with slot type/database/temporary/
failover/two-phase/plugin, `restart_lsn`, `confirmed_flush_lsn`, system identifier and timeline.
A same-name slot ahead of the durable frontier without gap proof and every first-profile timeline
change are epoch breaks; a slot behind may redeliver only through the existing durable highwater.
Never infer incarnation continuity from name/plugin alone.

Acceptance: stop at fence A, commit B, drop/recreate the same-name slot and restart: old feeds fail
closed/reset rather than serving A as current. Quiet recreation, behind/equal/ahead frontiers, and a
promoted synchronized usable same-name slot exercise the decision table; the last case still resets
because seamless failover is excluded.

### OPS-003A — Least-privilege PG bootstrap job

**Depends:** `ENG-017`, `SEC-003B`, `SEC-006A`. **Profiles:** all. **Boundary:** DB setup SQL/job.

Idempotently create the immutable explicit publication, slot, replica identity and separate bootstrap,
runtime-admin, replication and query roles. Runtime roles have no table ownership, publication
mutation/creation, slot drop or migration privilege; epoch reset and sanctioned publication change
use separate authorized credentials. Reject RLS-enabled tracked tables; set the walsender session to
`row_security=off`. Repeating bootstrap produces no DDL/grant diff.

### OPS-003B — Runtime PG preflight and provider matrix

**Depends:** `OPS-003A`, `ENG-006`, `ENG-017`, `PG18-000`, `PG18-001Q`, `PG18-002A`,
`PG18-002C`.
**Profiles:** all. **Boundary:** engine DB preflight.

Validate exact PG18 major/provider capability; connector-specific verified TLS/channel-binding policy;
logical WAL; immutable effective publication; RLS absence; slot type/database/temporary/failover/two-
phase/plugin/status/invalidation plus `restart_lsn`/`confirmed_flush_lsn`; publishable generated
columns; PK/identity/types; permissions; and effective `idle_replication_slot_timeout`,
`max_slot_wal_keep_size`, WAL/disk retention against the planned-outage envelope. Compare the slot to
the durable completely-landed source frontier: ahead without gap proof breaks the epoch, behind may
redeliver through highwater. Stable reasons cover every fixture; unapproved majors fail closed.

### PGR-001 — Qualify PostgreSQL 18 backup, PITR and provider restore

**Depends:** `PG18-000`, `OPS-003A`, `OPS-003B`. **Profiles:** `COMMON_SERVER`.
**Boundary:** PG/provider backup and restore tooling.

Pin the supported provider mechanism and artifact. Produce a checksummed source-frontier manifest and
restore to an empty environment at named commits. Record cluster/system/timeline, slot incarnation and
LSNs, publication/RLS/generated-column definition, roles/TLS identities and provider metadata. Prove
the exact-resume or whole-generation-reset decision for whole-stack, PG-only and PITR restore; a
restored same-name slot is never trusted by name.

Acceptance: 100 backup/PITR/restore cuts including missing/corrupt/ahead/behind artifacts and provider
promotion yield exact fenced continuation or fail closed/reset before public readiness. Existing
PG16/17 major import is explicitly unsupported and uses logical export/restore plus full generation
reset unless a future profile adds PostgreSQL's official slot-upgrade prerequisites.

### PG18-002B — Integrate PG18 continuity breaks with authorized reset

**Depends:** `PG18-002A`, `PG18-002C`, `DSR-003`. **Profiles:** `COMMON_SERVER`.
**Boundary:** epoch reset and public-generation invalidation integration.

For real invalidation, slot replacement and timeline cases, exercise reset-on after the fail-closed
latch: durably retire old streams/feeds/registry claims, create/bind the authorized new slot epoch,
then permit a fresh snapshot. Reset-off remains latched for operator action.

Acceptance: each selected real break has reset-off and reset-on evidence; old public handles/claims
terminally fail, exactly one new epoch binds, and a fresh materialization equals the promoted/restored
primary without mixing generations.

### PG18-003A — Package and preflight the PG18 candidate

**Depends:** `PG18-001Q`, `PG18-002A`, `PG18-002C`, `SEC-006A`, `OPS-003A`, `OPS-003B`.
**Profiles:** `COMMON_SERVER`. **Boundary:** candidate deployment/database integration.

Produce the pinned PG18 candidate profile and run engine-level `PG18-E2E-001`–`008` and `011`–`013`;
for `006`, this packet owns fail-closed observation while reset-on belongs to `PG18-002B`. Promotion
`009`/`010` belongs to `OPS-004`, and maintenance `014` belongs to `PG18-004`. For setup/admin, pool/backfill/query-
back and walsender connectors, use stable `application_name` values and `pg_stat_ssl`; target each
path independently with wrong CA/SAN, certificate rotation and reconnect after other paths are healthy.
Test URL/keyword conninfo parity, percent-escaped credentials, selected multi-host policy and channel-
binding disposition. Runtime publication mutation is denied; the sanctioned workflow fences first.

Acceptance: the immutable candidate passes baseline/fence/generated/slot/publication/RLS/plugin and
connector-specific TLS cases; every negative fails closed before stale serving. A direct engine
adapter closes only this candidate gate, never the final public contract.

### PG18-004 — Qualify PG18 minor maintenance and major-import policy

**Depends:** `PG18-003A`, `PGR-001`, `GOV-004`. **Profiles:** `COMMON_SERVER`.
**Boundary:** PG18 minor-upgrade/rollback runbook and evidence.

Run approved `18.N -> 18.N+1` maintenance on the same restored data directory: fence/drain, stop,
replace exact provider artifacts, start, verify unchanged cluster/slot/publication/frontier, resume
the existing feed, and write before/after markers. Cut restart/reconnect and exercise the declared
minor rollback or typed reset-only path. PG19 and unapproved minors fail preflight. PG16/17
`pg_upgrade` import is unsupported in the first profile and uses export/restore plus full rehydrate.

Acceptance: `PG18-E2E-014` passes for the oldest and newest supported minor pair with the exact
candidate artifacts; no maintenance resumes a slot/feed without the continuity decision.

### PG18-003Q — Qualify the complete PG18 public production profile

**Depends:** `PG18-003A`, `PG18-002B`, `PG18-004`, `PGR-001`, `OPS-001B`, `LEAD-001`,
`OPS-004`. **Profiles:** `COMMON_SERVER`. **Boundary:** public PG18 profile integration.

Rerun the hash-pinned `PG18-E2E-001`–`014` through immutable deployment images, authenticated
gateway and profile-independent public reference materializer. Include both a missing unsynchronized
slot promotion and a synchronized, usable same-name failover slot; the first profile resets in both
cases because every timeline change is an epoch break.

Acceptance: the full PG18 matrix passes against exact digests/config/provider artifacts; public
freshness never advances through failed DB identity/TLS/publication/slot proof; negative and reset
outcomes retain their stable external contracts.

### OPS-004 — Same-primary replacement and PG-promotion reset

**Depends:** `OPS-001B`, `OPS-002`, `OPS-003B`, `PG18-002C`, `LEAD-001`, `E2E-001R`.
**Profiles:** all.
**Boundary:** controller/runbook integration.

Automate engine replacement after confirmed termination on the same primary. For every PG18 promotion/
timeline change, route no reads until `DSR-003` resets and clients rehydrate—even when the promoted
standby has a synchronized, usable same-name slot. Run 100 replacement and both missing-slot and
synchronized-slot promotion cuts around commit/append/checkpoint; no overlapping writer, silent
continuation or stale success. This is not seamless failover.

### OPS-005 — Catalog/stream/DS upgrade and rollback

**Depends:** `GOV-004`, `DST-001`, `STO-001`, `ENG-004`, `ENG-015`.
**Profiles:** all. **Boundary:** storage migration tooling.

Use versioned envelopes and resumable journal. Unknown type/version/payload/order fails before mutation.
Exercise `N -> N+1 -> new writes -> N`; if N cannot read, declare rollback unsupported and run an
explicit export/retire/reseed, never call pre-mutation refusal a successful rollback.

### OPS-006 — Production metrics, dashboards, alerts and redaction

**Depends:** `CAP-001B`, `DST-001`, `SEC-010`. **Profiles:** all. **Boundary:** observability package.

Export/scrape every `CAP-001A` resource plus commit-to-client, WAL, backfill, leadership, quota,
catalog/retirement and client reset signals. Check bounded labels, authenticated listeners and
instrumentation overhead. Trigger every alert with fixtures; metrics stay responsive at accepted load
without heap walks or sensitive data.

### OPS-007 — Executable incident and maintenance scenarios

**Depends:** `OPS-002`, `OPS-004`, `OPS-005`, `OPS-006`, `SEC-006B`.
**Profiles:** all. **Boundary:** runbook scripts/manifests.

Provide noninteractive `diagnose/recover/verify` with stable JSON/exit codes for DS unavailable/full/
corrupt, catalog/retirement retry, WAL, slot/epoch, schema/TRUNCATE, flip degradation, queues/backfill,
bad release, credential rotation, revocation and resync storms. Destructive scripts require exact
target/approval and reject broad/unresolved values. Prose explains scripts; it is not acceptance.

### OPS-008 — Coordinated schema/template/circuit deployment

**Depends:** `ENG-004`, `ENG-017`, `SEC-003`, `GOV-005`. **Profiles:** all.
**Boundary:** migration preflight/workflow.

Define expand/migrate/contract for tables, PK/identity, types, projections, layout fingerprints and
template versions. Current default on any fingerprint drift is typed retirement/reseed; no implicit
additive tolerance. New compiled circuit layouts rebuild/reseed before app templates enable.

### OPS-009 — Release qualification and rollback command

**Depends:** `RLS-001`, `OPS-004`, `OPS-005`, `OPS-006`, `OPS-007`, `OPS-008`, `CAP-004`, `TST-003`,
`TST-004`, `TST-005`. **Profiles:** all. **Boundary:** promotion workflow.

Consume exact profile closure and immutable evidence; reject any fail/blocked/stale/missing artifact.
Install/rollback from the manifest under continuous oracle traffic. The profile's client and device
evidence are additional exact dependencies, not implicit waivers.

### CAP-000 — Qualification driver and raw evidence pipeline

**Depends:** `GOV-002`, `CAP-001A`. **Profiles:** all. **Boundary:** bench/loadgen harness.

Build deterministic operation-count traces and open-loop arrivals recording scheduled/offered/
admitted/committed/applied/rejected/drop times and counts; retain closed-loop realism separately.
Capture raw per-operation latency/errors, SQL oracle, all component/cgroup/PG/DS/gateway/client metrics,
fault log and pinned topology. Fail on child error, sample loss, under-run, artifact corruption or
oracle mismatch; correct coordinated omission.

### CAP-001B — Implement and validate bounded-resource instrumentation

**Depends:** `CAP-000`, `ENG-006A`, `ENG-007`, `ENG-007A`, `ENG-008`, `ENG-009`, `ENG-009A`, `ENG-010`,
`DST-001`. **Profiles:** all. **Boundary:** engine/DS/gateway emitters.

Implement every `CAP-001A` signal and validate declared CPU/p99 overhead against the same trace with
detailed instrumentation disabled. Tiny injected limits drive each resource over 80%; production
capacity need not be consumed merely to test telemetry.

### CAP-002 — Paired current-Electric/Circuits compatibility benchmarks

**Depends:** `CAP-000`, `CAP-001B`. **Profiles:** `COMPAT_V1`. **Boundary:** fleet evidence.

Run pinned current Electric and the candidate on identical isolated Linux resources, durable modes,
PG config, seed and fleet revision. Preserve raw samples/tags and process/IO metrics, fail on any error,
run scale 1 and selected target with at least three repetitions, and report quantile rank intervals/
spread. No claim compares unequal durability/platforms.

### CAP-003A — Generate the capacity matrix and discover the envelope

**Depends:** `CAP-000`, `CAP-001B`, `GOV-005`. **Profiles:** all.
**Boundary:** workload-matrix generator and envelope-search analysis.

Generate fixed-operation traces for distinct/shared shapes, active/dormant, native/v1, narrow/wide,
circuit/routing/fallback, single-thread stages, subquery selectivity, aggregate groups, churn, long
polls/slow readers/reconnect, snapshot storms and PG vacuum. Use open-loop steps to bracket saturation,
then repeat below/at/above the knee with exact attempt/stop budgets and rank/confidence analysis.

Acceptance: seeded synthetic curves recover the known knee and reject coordinated omission/under-run;
real search emits a signed candidate operating-point manifest, raw samples and conservative lower
bound without running the final release corpus.

### CAP-003Q — Qualify the conservative capacity operating point

**Depends:** `CAP-003A`. **Profiles:** all. **Boundary:** immutable candidate capacity run.

Run the exact 10,000,000-operation corpus at the selected point and adjacent control loads with
candidate digests/config. Sustainable capacity is the highest accepted rate whose latency/lag/queue/
error/resource confidence bounds pass; production admission remains below its lower bound with
failure/headroom reserve.

Acceptance: exact offered/attempt/admitted/committed/applied budgets terminate deterministically; every
template event floor and absolute resource/safe-crossing rule passes; reruns preserve the declared
statistical interval and artifact hashes.

### CAP-004 — Failure/recovery capacity qualification

**Depends:** `CAP-003Q`, `OPS-002`, `OPS-004`, `TST-010`, `TST-011`, `TST-012`.
**Profiles:** all. **Boundary:** fault/load evidence.

At selected load, inject DS latency/outage/full, PG disconnect/promotion, gateway/engine termination,
slot busy/lost, 64/128/129/512 MiB and target transactions, fan-out/flip storm, drift and client
reconnect. Execute every manifest cut 100 times; accepted transactions land or force typed reset,
resource caps act before OOM/corruption, and recovery meets declared bounds.

### CAP-005 — Swift device/long-poll load qualification

**Depends:** `CAP-000`, `CAP-001B`. The profile graph additionally requires `TST-008C` for
`COMPAT_V1` or `TST-008N` for native. **Profiles:** Swift client profiles.
**Boundary:** device/client load generator.

Reproduce real poll deadline, cache transaction cost, connection/reconnect cadence, background
transitions, supported device memory/battery/network and unique build/OS cohorts. Report client buffer/
RSS/CPU/energy, gateway sockets and end-to-end latency; TS loadgen cannot substitute.

## 9. Application inventory and compatibility-client tasks

### CMP-001 — Enumerate and classify every production sync call site

**Depends:** `CMP-000`, `GOV-002`. **Profiles:** all client profiles. **Boundary:** generated app query
inventory.

Statically find and runtime-instrument every ElectricSync subscription, tagged/ownership operation,
database mutation, sync status consumer and cache reader in the frozen app. Record model/table,
projection, predicate/DNF, parameters, mode (`eager`, `onDemand`, `progressive`), order/limit, tags,
ownership/delete semantics, overlapping projections, cache transaction boundary, launch/background
lifecycle, error/reset UX and observed cardinality/rate. Map each to `COMPAT_V1`, a native module, a
required app redesign, or `unsupported`, with a reason.

Acceptance: fixture call sites for every ElectricSync API family are discovered; clean-checkout CI
fails on an unclassified new/changed call site; every production model has one authoritative cache
owner and reset operation.

### APP-OWN-001 — Define per-model cache ownership and generation transitions

**Depends:** `CMP-001`. **Profiles:** all client profiles. **Boundary:** app persistence ADR/adapter.

For each synchronized model, identify the sole component allowed to insert/update/delete replicated
rows, the local-only columns it must preserve, relationships and observers, generation marker, atomic
replace/delete operation, and behavior when two projections overlap. Forbid two engines from writing
the same model generation. Specify old→shadow→new and rollback transitions, including cold
rebootstrap when the rollback cache is not proven current.

Acceptance: crash tests at every generation-switch statement expose exactly one complete generation;
stale tasks carrying the previous generation cannot commit rows or checkpoints.

### CMP-002 — Prove `COMPAT_V1` eligibility template by template

**Depends:** `CMP-001`, `APP-OWN-001`. **Profiles:** `COMPAT_V1`. **Boundary:** compatibility matrix.

Approve only eager, statically parameterized, full materialized shapes with one authoritative owner
and no semantic dependence on DNF/tag ownership, on-demand `offset=now`/`changes_only`, progressive
order/limit/windowing, subset paging, SSE, overlapping merge rules or source-transaction callbacks.
For each approved template, specify Electric request, private Circuits request, key/value mapping,
initial-load boundary, delete/reset behavior and counterexample tests. Rejected templates name the
native/redesign task that owns them.

Acceptance: a generated negative corpus perturbs every forbidden feature and fails eligibility; no
application traffic can select an unapproved mapping.

### CMP-002B — Electric adapter codec and behavior corpus

**Depends:** `CMP-002`, `ENG-003`, `PROTO-002`, `TST-002V`. **Profiles:** `COMPAT_V1`.
**Boundary:** `/v1/shape` golden fixtures.

Freeze snapshot headers, offsets/handles, long-poll 204, live insert/update/delete, the tagged Electric
structured-key corpus, selected scalar/field-presence values, control messages, 409 refetch,
closed/gone, timeout/cancellation and malformed responses from current Electric, the Circuits adapter
and the selected ElectricSync decoder. Register only representation differences proven irrelevant to
the app schema; never normalize tags or ownership behavior away.

Acceptance: at least 100,000 generated scalar/key rows plus every lifecycle fixture decode to the
same typed app value or the same named incompatibility; fixture hashes are pinned in CI.

### CMP-003 — Authenticated compatibility gateway adapter

**Depends:** `CMP-002`, `SEC-002B`, `SEC-003`, `SEC-004`, `ENG-003`.
**Profiles:** `COMPAT_V1`. **Boundary:** gateway Electric-compatibility routes.

Accept only an approved template/version and typed parameters, derive the private Electric shape
request and gateway-owned internal claim, and proxy snapshot/long-poll responses without exposing
table/predicate/DS identity. Bind handle/offset state to principal, policy and public feed generation;
translate private 409/closed/gone/epoch/drift into one public reset. Do not provide a general raw
`/v1/shape` pass-through.

Acceptance: all cross-principal handle/offset/template substitutions fail before engine/DS work;
10,000 ambiguous create/poll/reset/release retries leave one claim and converge to SQL.

### CMP-004 — Add the app compatibility provider

**Depends:** `CMP-002B`, `CMP-003`, `APP-OWN-001`, `E2E-003CR`. **Profiles:** `COMPAT_V1`.
**Boundary:** frozen app provider seam.

Implement an additive, feature-flagged provider at the app's existing model/cache boundary rather
than changing ElectricSync internals unless inventory proves no seam exists. Preserve the selected
package's request cancellation, typed decode, cache transaction and observer contracts. Treat reset
as generation replacement; an old request may not publish after provider/generation change.

Acceptance: each eligible template completes 10,000 create/load/live/reset/close cycles under a
deterministic scheduler; provider selection changes no ineligible model and no test needs a production
network.

### CMP-004A — Make compatibility response checkpointing crash-safe

**Depends:** `CMP-004`, `E2E-003CR`. **Profiles:** `COMPAT_V1`.
**Boundary:** vendored app ElectricSync response application/checkpoint ownership.

Characterize then fix the selected vendored client's response chunking: a persisted offset/handle/
cursor may never name response data not committed to the app cache. Test response sizes 199, 200,
201, 400 and the maximum admitted template; terminate/cancel after every committed local chunk and
replay duplicate responses containing upserts, deletes, predicate move-outs and missing-versus-NULL
updates. Attach cursor state only to a terminal control/application boundary or choose an equivalent
atomic contract. Do not infer safety from sibling `electric-sync-swift`.

Acceptance: after every cut the checkpoint is behind or equal to committed cache effects; restart
replays safely and fenced final app state equals SQL. A template whose response/checkpoint contract
cannot meet this is ineligible for `COMPAT_V1`.

### CMP-005 — Compatibility cache cutover and rollback adapter

**Depends:** `CMP-004A`, `APP-OWN-001`. **Profiles:** `COMPAT_V1`.
**Boundary:** app cache-generation controller.

Build shadow namespaces/tables where the current store permits them, atomic read-owner switch,
generation-tagged writes/checkpoints and explicit cleanup. A rollback target is usable only after a
common-fence comparator proves it current; otherwise rollback first cold-rebootstraps the old provider
with reads held. Preserve local-only fields through an explicitly tested merge, not implicit upsert.

Acceptance: 100 crash/cancellation cuts at each shadow/copy/switch/cleanup/rollback phase produce the
old complete generation, the new complete generation, or a held-for-rebootstrap state—never a mix.

### CMP-006 — Qualify compatibility mode in the real application host

**Depends:** `CMP-005`, `TST-002C`, `TST-006C`, `TST-008C`. **Profiles:** `COMPAT_V1`.
**Boundary:** app integration evidence.

Exercise every eligible template through launch, login/logout/account switch, foreground/background,
network handoff/offline/reconnect, token expiry/revocation, memory pressure, reset, migration and
rollback on the minimum/maximum supported OS and representative devices. Compare UI-visible cache
state with fenced SQL and run the existing app test suite.

Acceptance: 10,000 lifecycle cycles per eligible template across the device matrix, zero unregistered
divergence, no old-principal rows after account switch, and bounded device/gateway resources.

## 10. Native Swift package tasks

### SWF-000 — Freeze cursor, delivery and acknowledgement semantics

**Depends:** `NATIVE-ADR-001`, `PROTO-002`, `PROTO-003A`, `PROTO-004`.
**Profiles:** native. **Boundary:** Swift API/persistence ADR.

Define the difference between fetched, decoded, delivered and durably checkpointed without importing
app cache ownership. A plain observer cannot acknowledge durable application. Make an independent
`CheckpointStore` part of core; define event-level resume and permitted duplicate replay.
`NATIVE_REPLICA_SINK` separately adds one atomic sink transaction for row effects, ownership/
generation and checkpoint. Define close/cancel/reset/crash state diagrams before actors.

### SWF-001 — Scaffold `ElectricCircuitsSwift`

**Depends:** `NATIVE-ADR-001`, `GOV-004`. **Profiles:** native. **Boundary:** new Swift package.

Create independent products/targets for core protocol, transport, test support and selected optional
modules. Use strict Swift concurrency, documented Apple platform/compiler floors and no runtime
third-party dependency by default. Add library evolution/API-diff policy, DocC examples, Linux fixture
tests where Foundation permits and clean dependency-boundary enforcement against ElectricSync.

Acceptance: clean SwiftPM build/test under every supported toolchain/platform; importing the package
does not import ElectricSync, GRDB, SwiftData, Combine or an application database.

### SWF-002 — Implement schema-directed scalar, row and key codecs

**Depends:** `SWF-001`, `PROTO-001A`, `PROTO-001D`, `PROTO-004`, `TST-002V`, `E2E-003NR`.
**Profiles:** native.
**Boundary:** Swift protocol value types/codecs.

Decode only through immutable `ResultSchema`; preserve Int64/UInt64/decimal precision, JSON null versus
missing, timestamp/time-zone rules, bytes, Unicode and opaque composite keys. Unknown required fields,
type mismatches and schema fingerprints are explicit errors; additive optional fields follow negotiated
version rules. Never infer numeric types from `JSONSerialization` or derive identity from row text.

Acceptance: the shared value corpus plus the tagged native opaque-key golden/property/fuzz corpus
round-trip cross-language with exact key equality and no crash/trap for malformed or deeply nested
input; no compatibility-only task is required by a native closure.

### SWF-003A — Deterministic fixture transport

**Depends:** `SWF-001`, `PROTO-003A`, `E2E-003NR`. **Profiles:** native. **Boundary:** Swift transport protocol/test
double.

Define async request/read abstractions with injected clock, randomness and cancellation; implement a
scripted byte-stream transport able to split every header/body/envelope boundary, stall each phase,
return ambiguous outcomes and assert request sequence. It is the foundation for deterministic actor
tests and never contains production retry policy.

### SWF-003B — URLSession gateway transport

**Depends:** `SWF-003A`, `SEC-004`, `SEC-009`, `PROTO-004`, `E2E-003NR`. **Profiles:** native.
**Boundary:** Swift URLSession implementation.

Use bearer headers, ephemeral/default session as selected by the security ADR, incremental bounded
decode, connect/request/resource/idle deadlines, cancellation propagation and response/body/header
limits. Classify DNS/TLS/offline/timeout/HTTP/protocol/reset; retry only idempotent operations with
server delay and injected jitter. Do not preflight reachability, follow authenticated cross-origin
redirects, log tokens/rows or use `Task.detached` for ordinary lifecycle work.

### SWF-004 — Actor-owned subscription control plane

**Depends:** `SWF-000`, `SWF-003B`, `SEC-002B`, `E2E-003NR`. **Profiles:** native.
**Boundary:** subscription actor/state machine.

One actor owns public feed ID, generation, resume token, schema, renewal, in-flight request, retry and
terminal state. Re-check generation/cancellation after every await. Recover ambiguous create/renew/
release using idempotency keys; `close()` is one-shot to callers but retries/join are safe. Old cleanup
cannot release or publish a replacement generation.

Acceptance: model-check every state/operation pair and run 100 randomized schedules for each await/
response/crash cut; exactly one active claim exists and no event is delivered after terminal close.

### SWF-005A — Bounded pull event stream

**Depends:** `SWF-004`, `SWF-002`, `E2E-003NR`. **Profiles:** native. **Boundary:** Swift stream/backpressure.

Expose a suspending pull iterator whose bounded decoded-byte/event capacity propagates backpressure to
URLSession. Do not use a dropping `AsyncStream` buffering policy for correctness data. Define single-
consumer ownership, cancellation, duplicate replay and slow-consumer typed reset; count bytes before
allocation and cap one value.

Acceptance: `limit-1/limit/limit+1`, stalled consumer and cancellation tests prove no silent drop,
unbounded task growth or checkpoint advance beyond delivered/applied data.

### SWF-005B — Optional spillable transaction batches

**Depends:** `SWF-005A`, `PROTO-003B`, `ENG-002`. **Profiles:** `NATIVE_TXN_ATOMIC`.
**Boundary:** Swift transaction assembler.

Before implementation, register the `TXN-001` contract hash and capture its package-level red run.
Assemble only negotiated per-stream transactions, validate source token/final marker/order and spill
encrypted checksummed batches beyond memory. Publish a complete transaction or reset; remove orphan
spills on launch/close. Never claim cross-stream atomicity.

Acceptance: the server large-transaction corpus and 100 crash cuts per spill/finalize/delete step keep
RSS within the manifest and deliver exactly one complete batch or one reset.

### SWF-006 — Materialized shape fold and checkpoint contract

**Depends:** `SWF-005A`, `SWF-000`, `SWF-002`, `E2E-003NR`. **Profiles:** `NATIVE_CORE`.
**Boundary:** Swift shape subscription/fold.

Fold absolute upsert/delete by opaque key, snapshot/live/reset generations and duplicate replay.
Offer (a) event consumption with caller-controlled checkpoint and (b) an in-memory materialized view
explicitly documented as non-durable. Replacement clears the prior generation before exposing new
rows. No hidden overlapping-feed merge policy.

Acceptance: 10,000 lifecycle cycles and 10,000,000 seeded/replayed events equal the fixture oracle;
every crash between delivery and checkpoint yields documented replay, never data loss.

### SWF-007 — Optional transactional replica sink

**Depends:** `SWF-006`. **Profiles:** `NATIVE_REPLICA_SINK`.
**Boundary:** reusable Swift sink protocol.

Before implementation, register the package-level `NATIVE-001` contract hash and capture its red run.
Define begin/apply-upsert/apply-delete/set-generation/store-checkpoint/commit/rollback as one atomic
operation and require deterministic idempotency. No state exists in which the sink commit succeeded
but its checkpoint is still awaiting a separate persist. App-owned adapters are separate tasks. For
overlapping feeds, require an explicit ownership/refcount/projection merge algorithm or reject.

Acceptance: inject a crash/error before and after every sink statement over 1,000,000 events; restart
equals SQL at the stored fence and never exposes checkpoint without its row effects.

### SWF-008 — Optional aggregate subscription

**Depends:** `SWF-004`, `SWF-002`, `SWF-005A`. **Profiles:** `NATIVE_AGGREGATE`.
**Boundary:** Swift aggregate module.

Before implementation, register `AGG-001` and capture its package-level red run. Decode keyed/
ungrouped aggregate results from schema, replace absolute values by key, handle NULL/
empty group/reset and preserve numeric precision. Do not expose arbitrary client-side SQL or imply
transactional relationship with shape streams.

Acceptance: count/sum/min/max, grouped/ungrouped, NULL and overflow fixtures match PostgreSQL across
1,000,000 mutations and all replacement cases.

### SWF-009 — Optional visibility-fenced subset subscription

**Depends:** `ENG-001A`, `SWF-006`. **Profiles:** `NATIVE_SUBSET`.
**Boundary:** Swift subset module.

Before implementation, register `SUBSET-001` and capture its package-level red run. Consume the
server's atomic page/feed fence, `{key,value}`, opaque keyset cursor and tombstone/watermark
rules for initial/replacement/load-more. Serialize page merges through the subscription actor and
reject unsupported composite/deferred templates by capability before creating a feed.

Acceptance: run the `ENG-001` interleaving corpus through Swift plus 10,000 cancelled/concurrent
load-more operations; the materialized subset always equals fenced SQL with no resurrection.

### SWF-010 — Mobile lifecycle, renewal and capability refresh

**Depends:** `SWF-004`, `SWF-006`, `E2E-003NR`. **Profiles:** native. **Boundary:** Swift lifecycle coordinator.

Define foreground/background behavior without assuming suspended timers run: cancel/finish polls,
persist resume/checkpoint, renew on activation, refresh auth/capabilities/schema and accept replacement.
Handle network handoff, clock change, token expiry, account switch, memory pressure and process kill.
Background execution is optional optimization, never correctness.

Acceptance: virtual-clock `t-1/t/t+1` and app-state schedules over 10,000 cycles produce one claim,
bounded reconnects and full rehydrate whenever continuity cannot be proved.

### SWF-011 — Client observability and support diagnostics

**Depends:** `SWF-010`, `CAP-001A`. **Profiles:** native. **Boundary:** Swift metrics/log hooks.

Expose bounded low-cardinality counters/timings for lifecycle, bytes, buffer, replay, reset, retry and
checkpoint lag plus a redacted diagnostic snapshot. Correlation IDs are opaque; never emit bearer,
parameters, rows, DS locations or stable tenant identity. Observers cannot block the control actor.

### SWF-012 — Client credential and local-data protection

**Depends:** `SWF-010`, `SEC-006B`, `SEC-006C`, `E2E-003NR`. **Profiles:** native.
**Boundary:** Swift security/storage integration.

Use injected credential provider and protected checkpoint/spill storage; define keychain accessibility,
file protection, backup exclusion, account-switch deletion, device-lock/background behavior and TLS
trust. App Attest/DPoP, if selected, is separately replay/rate tested and never replaces authorization.
Seeded token/row canaries must be absent from logs, crash reports, default backups and orphan files.

### SWF-013 — Package documentation, API stability and release candidate

**Depends:** `SWF-002`, `SWF-003B`, `SWF-006`, `SWF-010`, `SWF-011`, `SWF-012`, `TST-002N`,
`TST-006N`, `TST-008N`. The generated profile adds `SWF-005B` for transaction atomicity, `SWF-007`
for sink, `SWF-008` for aggregate, and `SWF-009` for subset. **Profiles:** native.
**Boundary:** Swift candidate package workflow.

Produce content-addressed candidate artifacts only for profile-selected products with DocC
integration/reset/error examples, generated API baseline, semver/changelog, signed tag/checksum and
compatibility table. Candidate bytes cannot be rebuilt after evidence records their hash. Disabled optional
modules are absent or fail capability selection; examples use template IDs, never raw predicates.

Acceptance: two clean consumer apps build from the immutable package artifact, execute the fixture
server, and pass API-diff/upgrade/N-1 negotiation tests.

### APP-NATIVE-CONSUMER-001 — Integrate native core into the production app

**Depends:** `SWF-006`, `SWF-010`, `SWF-012`, `NATIVE-ADR-001`. **Profiles:** `NATIVE_CORE`.
**Boundary:** app event/view/credential adapter without a durable replica sink.

Before implementation, bind the selected app templates to the existing native scenario hashes and
capture the real-app consumer red run. Own template selection, credential refresh, principal/generation/task identity, event application to
the app reader/view, checkpoint-store lifetime and reset UI without importing GRDB into native core.
Account/logout uses the real auth teardown; a delayed old-principal completion cannot publish. If the
app requires durable replicated rows, this path is ineligible and the sink path is selected.

Acceptance: a real app target consumes each selected template through launch/reconnect/background/
account/reset; its normal reader equals fenced SQL, no old account state publishes, and package-core
tests remain GRDB-free.

### APP-NATIVE-SINK-001 — Implement the app-owned native transactional sink

**Depends:** `SWF-007`, `APP-OWN-001`, `NATIVE-ADR-001`. **Profiles:** `NATIVE_REPLICA_SINK`.
**Boundary:** production app database/reader/credential integration.

Before implementation, bind the selected templates to `NATIVE-001` and capture its real-app red run.
Implement the selected app DB transaction so row effects, ownership/refcounts/projection presence,
generation and checkpoint commit atomically; preserve local-only fields and dependent cleanup. Own
the production credential provider/token refresh and reject overlapping feeds without the inventory's
merge algorithm. No separate post-sink checkpoint persist exists.

Acceptance: process/cancel/error before transaction, during it, on ambiguous commit result and after
committed return over 1,000,000 events yields safe replay or exact committed state; a second cursor
owner is refused and the normal app reader equals fenced SQL.

## 11. Test and qualification tasks

### TST-000 — Pin and preserve the inherited baseline

**Depends:** `PLAN-001`. **Profiles:** all. **Boundary:** baseline evidence.

Record exact toolchains, dependencies, SHAs, environment and raw results for formatting, TypeScript,
Rust, Vitest/conformance/fuzz, Swift boundary/tests, external Electric oracle/property/subqueries and
the browser demo. Preserve failures as named blockers rather than weakening assertions. Add the
validator-owned clean evidence runner that creates a fresh detached commit worktree or verified
prepared-tree export; resolves Cargo, pnpm, SwiftPM and Mix/Hex through validator-tested, lock-bound
external-input manifests and read-only mount topologies; assigns every command a unique initially
empty external output/cache/fixture/artifact root; records pre/post source cleanliness, dependency/
topology identity and effective-config hash; and reproduces each lane. Its self-tests must reject a
tracked change, staged change, untracked file, undeclared or writable ignored overlay, changed
dependency/link target, mutable shared cache, nonempty/reused run root, source mutation during a test,
reused source, wrong commit/tree and wrong effective config. A package-manager-required source-visible
dependency mount is legal only when absent from Git, read-only and fully covered by the declared
content/topology hashes.

Current evidence is in [`17-validation-baseline.md`](17-validation-baseline.md): formatting,
typecheck, 426 engine tests and 351 Swift tests pass; Vitest is 284/285 because purge acknowledges
before retirement completion; the external Electric run is blocked because `mix` is unavailable.

### E2E-000S — Stable scenario registry and contract-hash validator

**Depends:** `PLAN-001`, `TST-000`, `PG18-000`. **Profiles:** all.
**Boundary:** generated scenario/task ownership manifest.

Create `docs/production/e2e-scenarios.json` and its validator. For every stable scenario store ID,
semantic contract hash, proof kind, test owner, implementation owner(s),
integration runner, applicability expression, source journal, independent oracle, external action,
public expected outcome, cut tier/gate/arrival/release and evidence schema. A runner may add adapters,
not alter semantics. Reject duplicate ownership, prose conditionals, changed/unregistered hashes and
release-image scenarios that require an internal hook. Seed the manifest with all 69 current IDs:
40 PG18/server/gateway/boundedness definitions in note 24 and 29 Swift/app definitions in note 23.

Acceptance: mutation fixtures catch duplicate/divergent definitions, changed oracle/exclusion after
green implementation, missing optional-profile edge, instrumented cut labelled release-image, and
runner pass with a missing scenario; adding/removing an ID without an explicit manifest/version diff
fails.

### E2E-000A — Source journal, independent oracle and causal client fence

**Depends:** `E2E-000S`. **Profiles:** all. **Boundary:** PG18 fixture, journal/oracle and receipt
protocol.

Allocate `SourceCommitID` in a harness-only marker relation and write its marker as the last statement
of the same transaction as source changes. The relation is admitted to the fixture's immutable,
explicit test publication, excluded from every public template and client-visible result, and never
stands in for a public target receipt. Its decoder may mark the transaction only after its terminal
envelope; `server.drainedThrough(id)` is emitted only after every causally preceding direct and
deferred action (including query-back work) is terminal. Define three receipts:
`source.committed(id)`; that adapter-specific server receipt; then
`client.appliedTailAfter(id)` after a public/read adapter starts a caught-up read after the server
receipt and the target principal+template+generation cache/fold transaction commits. An independent
checked-in SQL/projection/key definition—never the production template compiler—holds a
repeatable-read prefix or folds the journal through the ID. An Electric lane that cannot carry an
in-lane receipt uses quiesced writes plus an explicit per-template caught-up/apply receipt.

Acceptance: comparison blocks when the marker is unpublished, committed but not ingested, observed
before transaction end, or its receipt skips held deferred work; it also blocks when direct output
landed but query-back is held, a pre-barrier tail is reused, bytes arrive but cache commit is held, a
receipt has wrong principal/template/generation, or later SQL is accidentally included. Wrong
predicate/tenant, missing↔NULL, scalar kind/precision, composite-key collision, tombstone/delete,
source prefix and stale-generation mutants are all detected without importing the production compiler.

### E2E-000B — Stack, process, external-fault and resource primitives

**Depends:** `E2E-000S`, `TST-000`, `PG18-000`. **Profiles:** all.
**Boundary:** process/image stack controller and external cut adapters.

Start isolated PG18, file-backed DS, engine and storage from specified digests; allocate database,
publication, slot, volumes, networks and artifact directory; control process signals, TCP/storage
responses, volume replacement, public request phases and readiness; sample resources at operation
barriers. The early direct-engine adapter is confined to an isolated test network. Exact internal
fsync/checkpoint/catalog/rotation hooks remain the instrumented `TST-007` tier.

Acceptance: two stacks run concurrently without shared ports/state; every external cut announces
arrival before release/kill; teardown targets only owned resources; candidate images use recorded
OCI-index/platform/platform-manifest digests with no implicit build/pull; direct sleeps/ad hoc
wall-clock ordering fail lint.

### E2E-000I — Integrate reference adapters and prove harness mutations

**Depends:** `E2E-000A`, `E2E-000B`. **Profiles:** all. **Boundary:** acceptance adapter SDK and
baseline integration.

Expose one test language through a fast process adapter and image adapter. First use an isolated real-
server reference materializer so PG18 implementation can be driven red before the production gateway;
later gateway/Swift tasks add adapters. Run the scenario/oracle mutants and disabled-hook equivalence
smoke; keep private LSN/changelog/deferred gauges behind `server.drainedThrough` as diagnostics.

Acceptance: baseline PG18 journal→server adapter→fenced SQL passes; every seeded bad adapter fails at
its intended receipt/oracle stage; raw evidence includes exact source/config/digest/contract hashes and
first-divergence replay.

### TST-007 — Deterministic failure, scheduler and oracle harness

**Depends:** `TST-000`, `CAP-000`. **Profiles:** all. **Boundary:** shared qualification harness.

Add named hooks at durable intent/mutation/fsync/checkpoint/response/renewal/switch points, a virtual
clock, seeded task scheduler, PG/DS/gateway/network fault proxies and invariant oracle. Record the
first divergent operation, raw stream/catalog bytes and replay command. Hooks are compile/config
disabled in release artifacts and cannot alter scheduling when disabled.

Acceptance: every registered hook is hit by one test, 100 seeded schedules replay byte-for-byte, and
mutation tests prove the oracle catches dropped/duplicated/reordered/stale effects.

### TST-001 — Repository regression gates on the candidate build

**Depends:** `TST-000`, `ENG-003`, `ENG-004`, `ENG-014`, `ENG-015`, `ENG-017`, `STO-001`, `STO-002`.
**Profiles:** all. **Boundary:** CI gate manifest.

Run `cargo fmt --check`, `pnpm typecheck`, `pnpm engine:test`, full prebuilt Vitest, conformance, fixed-
seed fuzz/replay and the live demo/browser workflow under release configuration. Attribute each test
to an invariant and prevent skipped/filtered suites, zero-test success, memory DS or missing browser
write. Engine-touching tasks attach these results to their execution note.

Acceptance: all inherited tests pass, including the presently failing purge assertion; two clean runs
produce the same counts and named seed corpus.

### TST-002A — Cross-language protocol corpus generator

**Depends:** `PROTO-001A`, `PROTO-001B`, `PROTO-002`, `PROTO-003A`, `PROTO-004`.
**Profiles:** all clients. **Boundary:** canonical fixture generator.

Generate signed fixtures from one schema implementation for requests, results, scalar/key extremes,
snapshot/live/reset, long-poll, pagination, errors, unknown fields, N/N-1 and byte/page splitting.
Optional transaction, aggregate and subset fixtures are emitted only for selected profiles. Pin
canonical JSON/bytes, semantic expectations and provenance; hand-edited fixtures fail CI.

### TST-002V — Shared typed-value and tagged-key corpus

**Depends:** `PROTO-001A`, `PROTO-001C`, `PROTO-001D`, `PROTO-004`. **Profiles:** all clients.
**Boundary:** generated cross-runtime value/key fixtures.

Generate the selected scalar/field-presence corpus once, plus separate tagged key grammars: Electric
structured keys (escaped `/`, `.`, `_`, quotes, empty/non-ASCII/normalization variants and schema/
table components) and native opaque composite keys (including U+001F/backslash/delimiter-like bytes).
Include malformed/collision/unsupported fixtures. No runtime may import the other grammar accidentally.

Acceptance: Rust/gateway/TypeScript/Swift decoders agree exactly on selected values and tagged key
identity; missing and SQL NULL differ; 100,000 generated keys per grammar have no collision; a
wrong-grammar decoder and every lossy type mutant fail.

### TST-002C — Compatibility cross-implementation conformance

**Depends:** `TST-002A`, `CMP-002B`, `CMP-003`. **Profiles:** `COMPAT_V1`.
**Boundary:** Electric/Circuits/ElectricSync fixture runner.

Run the selected ElectricSync decoder/provider and current Electric/Circuits adapters over the same
eligible-template corpus. Compare typed rows, key identity, snapshot completion, live effects and
reset outcome; every permitted difference is template-scoped and pre-registered.

Acceptance: 100,000 scalar/key cases, 10,000 lifecycle sequences and every malformed fixture have the
same semantic result or a named eligibility rejection.

### TST-002N — Native Swift/server cross-language conformance

**Depends:** `TST-002A`, `SWF-002`, `SWF-003B`. **Profiles:** native.
**Boundary:** Rust/TypeScript/Swift fixture runner.

Decode/re-encode the same request, schema, value, key, event, error and capability corpus in every
runtime. Split network bodies at every boundary and fuzz valid/invalid schema-directed values. Selected
optional modules add their own corpus without changing native-core expectations.

Acceptance: at least 1,000,000 generated values/events plus every golden fixture produce exact
semantic equality; malformed input returns a typed bounded error and never traps or allocates past cap.

### TST-004 — Security and tenant-isolation qualification

**Depends:** `SEC-002A`, `SEC-002B`, `SEC-002C`, `SEC-003`, `SEC-003B`, `SEC-004`, `SEC-005A`,
`SEC-005B`, `SEC-006A`, `SEC-006B`, `SEC-006C`, `SEC-007`, `SEC-009`, `SEC-010`, `ENG-006A`.
**Profiles:** all. **Boundary:** adversarial security suite.

Generate principals/tenants/templates and attempt IDOR, parameter broadening, cross-domain shared-
shape leakage, handle/path/offset substitution, revocation races, quota races, replay, slowloris,
compression bombs, malformed JSON, SSRF/redirect, admin-route escalation, plaintext hops and secret/
row canary leakage. Run from public, gateway and compromised-private-pod network positions.

Acceptance: each negative case returns its stable public error before forbidden downstream work;
revocation has a bounded named barrier; two-replica quota races land exactly the allowed work; no
seeded secret/row appears in telemetry, images, backups or spills.

### TST-005 — External Electric compatibility suites and adapter gaps

**Depends:** `TST-001`, `ENG-003`, `TSC-001`. **Profiles:** `COMMON_SERVER_QUALIFICATION`.
**Boundary:** external-suite runner.

Pin `../electric` and its Elixir/Erlang toolchain in a hermetic image; run oracle, property and
subqueries against the exact release engine/gateway. Preserve upstream fixture patches separately and
leave the sibling checkout clean. Add cases for adapter handle restart, 204, 409, retention, drift,
purge and epoch reset that the upstream suite lacks.

Acceptance: all three lanes pass twice with unchanged counts and no copied/untracked sibling files;
the image digest and raw test output are attached to evidence.

### TST-006C — Compatibility provider concurrency model tests

**Depends:** `CMP-004`, `CMP-005`, `TST-002C`. **Profiles:** `COMPAT_V1`.
**Boundary:** app/provider deterministic tests.

Schedule provider switch, request completion, cache transaction, observer callback, reset, logout and
rollback at every await/callback boundary. Use virtual clocks and fixture transport; assert main-actor/
store rules from the frozen app rather than assuming them from the sibling library.

Acceptance: 100 schedules per cut and 10,000 lifecycle sequences have one cache owner/generation,
bounded tasks and no stale-principal publication.

### TST-006N — Native Swift actor and persistence model tests

**Depends:** `SWF-003A`, `SWF-004`, `SWF-005A`, `SWF-006`, `SWF-010`, `SWF-012`, `TST-002N`.
**Profiles:** native. **Boundary:** Swift deterministic concurrency suite.

Model-check create/renew/read/reset/release/cancel/background/account-switch/crash over every actor
state and await. Inject storage and decoder failures and validate Sendable/isolation under strict
concurrency and Thread Sanitizer where supported. Selected sink/transaction/subset modules extend the
state model.

Acceptance: 100 seeded schedules per transition/cut replay identically; no duplicate claim, lost
acknowledged effect, data race, task leak or post-close delivery.

### TST-008C — Compatibility real-device and app-host matrix

**Depends:** `TST-006C`, `CMP-005`, `SEC-006B`. **Profiles:** `COMPAT_V1`.
**Boundary:** real app device test plan.

Run the frozen production app on the minimum and maximum OS plus representative low-memory/slow-
storage devices through cold/warm launch, background suspension/kill, offline/network handoff,
credential expiry/revocation, account switch, memory pressure, reset and rollback. Verify protected
cache/checkpoint behavior while locked.

Acceptance: fixed 10,000 total lifecycle operations with every template/event class floor met; fenced
SQL equality, no stale account rows and declared client resource bounds pass.

### TST-008N — Native package device and consumer-app matrix

**Depends:** `TST-006N`, `SWF-011`, `SWF-012`. **Profiles:** native.
**Boundary:** native sample/consumer apps and devices.

Run two clean consumer apps on the supported OS/device matrix over lifecycle, protection, memory,
energy, network, auth/reset and package upgrade/N-1 negotiation cases. Run selected optional modules
independently so one module cannot mask core failure.

Acceptance: fixed 10,000 lifecycle operations and 1,000,000 delivered events satisfy SQL, bounded
buffer/RSS/task/connection constraints and canary-redaction checks.

### TST-010 — Catalog, lifecycle and retention fault matrix

**Depends:** `TST-007`, `STO-001`, `STO-002`, `ENG-007`, `ENG-010`, `ENG-011`, `ENG-014`, `ENG-015`.
**Profiles:** all. **Boundary:** catalog/lifecycle fault suite.

Cut create/join/renew/left/drop/close/delete/retired, share readiness, dormant replay/eviction,
catalog append/snapshot/switch/reclaim, queue saturation and disk reserve before/after every durable
step and response. Reboot between cuts and compare registry, subscriptions, max ID, streams and SQL.

Acceptance: 100 executions per enumerated cut, 10,000 lifecycle cycles and 10,000,000 catalog events;
the only outcomes are exact continuation, completed retirement or one typed generation reset.

### TST-011 — Replication, schema and segmented-log fault matrix

**Depends:** `TST-007`, `ENG-004`, `ENG-006`, `ENG-007A`, `ENG-009A`, `ENG-017`.
**Profiles:** all. **Boundary:** PG/engine replication fault suite.

Cut pgoutput transactions/chunks/markers/spill, slot feedback, sequencer highwater/checkpoint, segment
rotation/control/delete, SnapshotGate window, schema fingerprint/reconcile, TRUNCATE, identity change,
subquery flips/query-backs and circuit restart. Include reconnect redelivery and 64–512 MiB commits.

Acceptance: 100 executions per cut plus 10,000,000 committed mutations converge exactly or retire/
reset as specified; slot/checkpoint never passes an unlanded committed effect.

### TST-012 — Storage restore, migration and leadership fault matrix

**Depends:** `TST-007`, `DSR-001`, `DSR-002`, `DSR-003`, `ENG-012`, `GWR-002`, `PGR-001`,
`OPS-002`, `OPS-004`, `OPS-005`.
**Profiles:** all. **Boundary:** deployment/storage fault suite.

Exercise empty-host restore combinations, corrupt/missing/ahead/behind registry/PG/catalog/DS objects,
DS full/reserve, same-primary engine replacement, stale former process, PG18 promotion, catalog/DS/
registry format upgrades and rollback. Start public traffic at every preflight phase and assert
readiness fencing.

Acceptance: 100 executions per cut; no two writers, no same-epoch DS-behind-slot resume, no partial
catalog serve and no public success before the chosen exact-resume or whole-generation-reset outcome.

### E2E-001R — Freeze red server lifecycle/recovery contracts

**Depends:** `E2E-000I`, `PROTO-002`, `PROTO-003A`, `PG18-000`. **Profiles:**
`COMMON_SERVER`. **Boundary:** hash-pinned external server scenarios and stacked red patches.

Author `SRV-E2E-001`–`013` only in external terms: oversized/cross-table transactions; shared
claims; dormancy/eviction; terminal tails; externally held/lost DS requests; engine SIGKILL/SIGTERM;
sustained writes plus dormant consumer across retention/storage bounds; schema/RLS/publication
workflow; restore/system replacement; file-backed DS crash; stale former engine/slot-busy handoff.
Each final state uses the causal client receipt and independent SQL oracle. Changelog markers, segment
IDs, catalog records and internal durable steps live only in adjacent focused tests.

Acceptance: each scenario declares either `genuine_red` with the intended current product failure, or
`inherited_control` with its unchanged characterization; only the former produces red artifacts or
enters `red_proved`. Artifacts contain exact patch/hash/command/observation. The server source-log
checkpoint never passes an incomplete PG transaction, but core public offsets/checkpoints remain
event/response-level and may replay duplicates; only `NATIVE_TXN_ATOMIC` adds one per-stream
observer batch.

### E2E-001Q — Qualify server lifecycle/recovery on candidate images

**Depends:** `E2E-001R`, `PG18-003Q`, `ENG-003`, `ENG-004`, `ENG-007A`, `ENG-014`,
`ENG-015`, `STO-001`, `STO-002`, `OPS-001B`, `OPS-004`, `TST-010`, `TST-011`,
`TST-012`. **Profiles:** `COMMON_SERVER`. **Boundary:** deployable server public behavior.

Run the unchanged scenario hashes through built candidate images, file-backed DS, public gateway and
profile-independent reference materializer. External release cuts run on candidate images; exact
internal cuts run in the same-SHA instrumented suites. The runner may not repair implementation or
change oracle/exclusions.

Acceptance: 100 executions per named cut yield only exact continuation, safe duplicate replay,
completed retirement, typed refetch/reset or admission rejection; every final non-rejected
materialization equals the fenced SQL/journal state and all absolute resource/cleanup bounds hold.

### E2E-002R — Freeze red gateway security/lifecycle contracts

**Depends:** `E2E-000I`, `OPS-001A`, `SEC-000`, `PROTO-001A`, `PROTO-001B`, `PROTO-002`.
**Profiles:** all clients. **Boundary:** public-gateway scenario contracts and stacked red patches.

Author `GW-E2E-001`–`008`: authority mutation; tenant/PK/ID substitution; credential negatives;
registry↔engine response-loss/restart; revocation during held poll, create/renew and multi-page body;
public network scan; and public/internal TLS identity/rotation/plaintext/stripping. Define revocation
commit: stop admission, cancel+join principal reads, invalidate generation, then acknowledge. Bytes
whose public headers/body began are pre-barrier; responses not publicly committed emit zero bytes.
The recorder sits outside the gateway and records header/first-body-byte order.

Acceptance: each stable negative declares `genuine_red` or `inherited_control`; only a genuine red
authorizes a behavior green pair. Each proves zero forbidden downstream/cache work where required;
hashes freeze public outcomes without depending on a
registry schema or gateway implementation sequence.

### E2E-002Q — Qualify gateway security/lifecycle on candidate images

**Depends:** `E2E-002R`, `OPS-001B`, `GWR-002`, `SEC-002A`, `SEC-002B`, `SEC-002C`,
`SEC-003`, `SEC-004`, `SEC-005A`, `SEC-005B`, `SEC-006A`, `SEC-006B`, `SEC-006C`,
`SEC-007`, `SEC-009`, `SEC-010`. **Profiles:** all clients.
**Boundary:** public client network and authenticated gateway.

Run the unchanged gateway contracts from a dedicated client-network container; use a separate
management network for fault control/scraping. Only pinned candidate digests with no Compose build or
implicit pull qualify. Exercise registry loss/rollback/corruption, connector rotation and public route
isolation as well as ordinary restart reconciliation.

Acceptance: every generated negative has its stable non-enumerating result; cross-tenant identity
never crosses ownership/generation; acknowledged revocation has the defined byte boundary; registry
retry/restore leaves exactly the authorized claim outcome; only gateway routes are reachable.

### E2E-003CR — Freeze compatibility Swift/app contracts red

**Depends:** `E2E-000I`, `E2E-002Q`, `CMP-000`, `CMP-001`, `APP-OWN-001`, `CMP-002`,
`CMP-002B`. **Profiles:** `COMPAT_V1`. **Boundary:** vendored ElectricSync/app contract patches.

In the frozen vendored app package/ServicesTests and app-hosted XCUITest plan, freeze selected
`SYNC-*`, `LIFE-*`, `AUTH-*` and `CODEC-*` cases. Include response sizes 199/200/201/400/max and
cuts after every cache chunk; real auth teardown/account switch; per-template application receipts;
Electric structured keys; supported scalar/field-presence/delete semantics; OS background, kill,
protected-data and relaunch. DNF/tag/on-demand/progressive/order/limit/subset/SSE/txn-observer
call-sites remain fail-closed. Do not put app qualification in an undeclared sibling test directory.

Acceptance: every admitted-template scenario has a hash-pinned product red/control; zero eligible
templates is an explicit profile N/A/failure decision from the generated admission manifest, never a
convenient toy-model success. Core outcomes allow declared chunk-prefix observer states and safe
response replay, not one source-transaction observer batch.

### E2E-003CQ — Qualify compatibility Swift and the real app

**Depends:** `E2E-003CR`, `E2E-001Q`, `CMP-004A`, `CMP-006`, `TST-006C`, `TST-008C`.
**Profiles:** `COMPAT_V1`. **Boundary:** candidate vendored app, real cache and normal app reader.

Run every admitted template/ownership pattern with published candidate artifacts. Package-generic
sibling results are labelled separate conformance only. App ServicesTests own provider/GRDB/auth;
app-host/device jobs own suspension/kill/scene/keychain/protected-data/UI observer behavior.

Acceptance: admitted templates pass snapshot/live/reset/reconnect/account/checkpoint/cache semantics,
each exclusion has one reason, the normal app reader equals SQL at its application receipt, and one
remote generation is authoritative. Compatibility claims may persist only to their exact lease
deadline; local tasks stop promptly and the post-deadline server observation proves release.

### E2E-003NR — Freeze native-core Swift contracts red

**Depends:** `E2E-000I`, `E2E-002Q`, `SWF-000`, `SWF-001`, `TST-002V`.
**Profiles:** `NATIVE_CORE`. **Boundary:** package-core scenario patches and minimal consumers.

Freeze core `SYNC-*`, `LIFE-*`, `AUTH-*` and native `CODEC-*` against a minimal in-memory view
plus independent temporary/in-memory `CheckpointStore`; no GRDB/app migration dependency. Tests cover
event-level duplicate replay, caught-up/reset, cancellation at external I/O/commit boundaries,
account/generation and native opaque keys. Internal awaits are diagnostic unit cuts, not stable E2E
actions.

Acceptance: two clean minimal consumers reproduce the intended current red/control; observer callback
count/order is not a core oracle; the final fenced view, checkpoint safety, task/claim lifecycle and
generation/security outcomes are frozen.

### E2E-003NQ — Qualify native core and the selected app integration

**Depends:** `E2E-003NR`, `E2E-001Q`, `SWF-013`, `TST-006N`, `TST-008N`. The generated
profile adds exactly one selected `APP-NATIVE-CONSUMER-001` or `APP-NATIVE-SINK-001` edge.
**Profiles:** `NATIVE_CORE`. **Boundary:** candidate package, two clean consumers and selected app.

Run the unchanged core contracts through URLSession, strict concurrency, independent checkpoint
storage, lifecycle/account transitions and N/N-1 package upgrade. Qualify the selected real-app
consumer path separately; no optional module is implied by core.

Acceptance: two independent consumers and the selected app integration pass; cancellation at every
external boundary leaks no task/claim/checkpoint, safe duplicate replay converges, and package core
imports no GRDB/ElectricSync/app database.

### E2E-003T — Qualify optional native per-stream transaction atomicity

**Depends:** `E2E-003NQ`, `PROTO-003B`, `ENG-002`, `SWF-005B`. **Profiles:**
`NATIVE_TXN_ATOMIC`. **Boundary:** negotiated transaction scenario runner.

Consume the unchanged `TXN-001` hash and its implementation packet's red/green evidence. Run eligible direct/deferred tiers through
large/chunked/crash cases: one complete per-stream observer batch and transaction checkpoint after its
final marker, safe reset otherwise, and explicitly no cross-stream atomicity.

Acceptance: the immutable candidate passes every registered `TXN-001` journal/cut on each admitted
execution tier; an unqualified tier does not negotiate the capability and no result claims
cross-stream atomicity.

### E2E-003S — Qualify optional native transactional app sink

**Depends:** `E2E-003NQ`, `SWF-007`, `APP-NATIVE-SINK-001`. **Profiles:**
`NATIVE_REPLICA_SINK`. **Boundary:** package sink plus production app adapter.

Consume the unchanged `NATIVE-001` hash and package/app implementation packets' red/green evidence.
Cut before/during the one app transaction,
at ambiguous commit result and after committed return; there is no post-commit checkpoint gap. Restart
uses safe replay and the normal app reader equals SQL.

Acceptance: every cut yields pre-transaction state or the complete row/ownership/generation/checkpoint
transaction; restart replay converges and a second checkpoint owner is refused.

### E2E-003A — Qualify optional native aggregates

**Depends:** `E2E-003NQ`, `SWF-008`. **Profiles:** `NATIVE_AGGREGATE`.
**Boundary:** aggregate package/profile runner.

Consume the unchanged `AGG-001` hash and its implementation packet's red/green evidence. Run
empty/nonempty/NULL/bigint count/sum/avg/
min/max insert/update/delete/exit/reset/restart cases and compare schema-directed values to SQL.

Acceptance: every selected aggregate/value kind matches PostgreSQL at each application receipt;
unsupported precision/type/template combinations fail admission before a subscription.

### E2E-003U — Qualify optional native subsets

**Depends:** `E2E-003NQ`, `ENG-001`, `ENG-001A`, `SWF-009`. **Profiles:**
`NATIVE_SUBSET`. **Boundary:** subset package/profile runner.

Consume the unchanged `SUBSET-001` hash and its implementation packet's red/green evidence. Interleave page/live update/delete/reorder,
tie/NULL/Unicode cursors, lapse/crash and unsupported admission; visible state equals fenced SQL with
no resurrection.

Acceptance: every supported page/feed ordering converges to the fenced SQL page with no duplicate or
resurrection; an unavailable visibility fence or unsupported template is rejected before feed work.

### E2E-004R — Freeze migration/cutover/rollback contracts red

**Depends:** `E2E-000I`, `MIG-000`, `MIG-001`, `APP-OWN-001`. The generated profile adds
`E2E-003CQ` for compatibility or `E2E-003NQ` plus the selected app-integration edge for native.
**Profiles:** all migration lanes. **Boundary:** hash-pinned real-app migration scenario patches.

Freeze `OWN-001`–`003`, `CUT-001`, `ROLL-001` and `ROLL-002` using per-template application
receipts, delayed old/new cache commits, overlapping PKs, local-only fields, every externally
observable cache-transaction result, kill/relaunch and chosen warm/cold rollback. Demonstrate current
red/control before cutover implementation.

Acceptance: every declared `genuine_red` proves its intended failure and every declared
`inherited_control` remains characterization only; comparison remains blocked for unequal
source/application prefixes; hashes define one complete visible generation, ownership-safe deletion
and fenced rollback without asserting private DB statement order.

### E2E-004Q — Qualify migration/cutover/rollback before rehearsal

**Depends:** `E2E-004R`, `MIG-002`, `MIG-002B`, `MIG-003`. The generated profile adds
`E2E-003CQ` for compatibility or `E2E-003NQ` plus the selected app integration for native.
**Profiles:** all migration lanes. **Boundary:** real app reader/cache-generation integration.

Run unchanged migration hashes through candidate stacks. Every cut exposes one complete authoritative
generation, refuses unequal prefixes, preserves ownership/local fields, and exposes rollback only
after a fresh incumbent application receipt. No cursor/handle/tag becomes a cross-backend token.

### E2E-005 — Fixed-operation candidate-profile qualification

**Depends:** `E2E-001Q`, `E2E-002Q`, `CAP-004`, `TST-010`, `TST-011`, `TST-012`.
The generated profile adds `E2E-003CQ` for compatibility or `E2E-003NQ` for native plus every
selected optional `E2E-003T/S/A/U` runner. **Profiles:** all.
**Boundary:** immutable content-addressed candidate artifact set.

Run `BND-E2E-001`–`005`, every selected stable scenario, inherited/external suites and exact client/
device lane. At limit-1/limit/limit+1 stall readers, cancel create/snapshot, churn reconnect, and hold
one downstream unavailable; assert typed admission/backpressure/reset, source/client checkpoint
safety and cleanup at named lifecycle/clock events.

Acceptance: the signed workload manifest's exact attempt/offered budgets terminate after at least
10,000,000 committed operations, 10,000 lifecycle cycles per enabled transport, every template event
floor and 100 runs per cut; all minimums and absolute resource/cleanup bounds pass. Blocked/skipped/
under-run/wrong digest/profile/hash or a divergence outside the pre-run allowlist fails promotion.

### TST-003 — Profile qualification coordinator

**Depends:** `TST-001`, `TST-004`, `TST-005`, `TST-010`, `TST-011`, `TST-012`, `CAP-004`,
`E2E-001Q`, `E2E-002Q`, `E2E-005`.
The generated profile graph additionally depends on `TST-002C`, `TST-006C`, `TST-008C`, `CMP-006`,
`CAP-002`, `CAP-005`, and `E2E-003CQ` for `COMPAT_V1`, or `TST-002N`, `TST-006N`, `TST-008N`,
`SWF-013`, `CAP-005`, `E2E-003NQ`, the selected app-integration task, and exact selected optional
`E2E-003T/S/A/U` runners for native. These are generated conditional edges, not prose metadata.
**Profiles:** all.
**Boundary:** release evidence coordinator.

Run the content-addressed candidate profile on clean infrastructure, verify every required task/
scenario/evidence hash and reject
blocked/skipped/stale/extra-capability results. Produce the G0–G9 decision, exact commands, raw links
and first failing task; it may report failure but cannot waive it.

Acceptance: success and each seeded missing/stale/failing/wrong-profile artifact produce the expected
machine-readable decision; compatibility-only and native-only can qualify independently.

## 12. Migration, exposure and decommission tasks

### MIG-000 — Establish a common PostgreSQL comparison fence

**Depends:** `CMP-001`, `OPS-003A`, `OPS-008`, `E2E-000A`. **Profiles:** all migration lanes.
**Boundary:** migration sentinel/control relation.

Reuse the `E2E-000A` three-stage source/server/target-application receipt for both incumbent and
Circuits. A comparison receipt is keyed by principal+template+generation+backend and commits only
after all prior target-cache effects. If `COMPAT_V1` cannot carry an in-lane receipt, quiesce writes
and obtain an explicit per-template caught-up/cache-commit receipt; a separate sentinel feed is
insufficient. Electric and Circuits offsets/LSNs remain opaque and are never compared. Markers contain
no tenant data and clients cannot forge them.

Acceptance: 10,000 fences interleaved with writes, reconnects, long polls and resets never compare
unequal source or application prefixes or suppress a real divergence; holding one target cache
transaction after server drain keeps comparison blocked.

### MIG-001 — Build a semantic shadow comparator

**Depends:** `MIG-000`, `APP-OWN-001`, `TST-002A`. The profile graph additionally requires
`CMP-002B` for `COMPAT_V1` or `SWF-002` for native. **Profiles:** all migration lanes.
**Boundary:** offline/app shadow comparison service.

At a common fence, canonicalize schema-directed typed rows and opaque keys, then compare membership,
values, deletes, aggregate/subset state when selected, generation and terminal/reset outcome. Ignore
only pre-registered representation fields with a proof. Emit first differing template/key/field,
source operations and replay bundle; redact row contents outside the protected evidence store.

Acceptance: mutation fixtures for missing/extra/stale row, wrong key/type/delete, premature compare,
tag/ownership loss and reset mismatch are all detected; 10,000 identical fenced states yield zero
false divergence.

### MIG-002 — Implement shadow writes and single-owner cutover

**Depends:** `MIG-001`, `APP-OWN-001`, `MIG-003`, `E2E-004R`. The profile graph additionally requires `CMP-005`
for `COMPAT_V1` or `SWF-006` plus `SWF-007` when a durable native replica is selected.
**Profiles:** all migration lanes. **Boundary:** app migration coordinator.

Keep incumbent reads authoritative while the candidate writes only its isolated generation. At a
common fence and successful comparator result, atomically switch one template/model read owner; stop
or fence old writer tasks before candidate writes the authoritative generation. Reversal uses the
same operation with `MIG-002B` freshness proof.

Acceptance: 100 faults at every start/fence/compare/stop/switch/resume step expose one complete
authoritative owner and preserve local-only fields.

### MIG-002B — Guarantee rollback-cache freshness

**Depends:** `MIG-000`, `MIG-001`, `APP-OWN-001`, `E2E-004R`. **Profiles:** all migration lanes.
**Boundary:** rollback controller.

Choose and implement one per model: keep the incumbent cache consuming and fenced while not serving,
rebuild it in a shadow namespace to the current common fence, or hold reads and cold rebootstrap it at
rollback. A cached generation without an unbroken fence proof is never exposed.

Acceptance: revoke candidate access or corrupt its cache after 1,000/10,000/100,000 intervening writes;
rollback returns a fenced incumbent generation or a typed held/rebootstrap state, never stale success.

### MIG-003 — Template flags, circuit breakers and reset controls

**Depends:** `GOV-005`, `APP-OWN-001`, `SEC-002C`, `E2E-004R`. **Profiles:** all migration lanes.
**Boundary:** app/gateway release-control schema.

Provide independently authorized flags by semantic template version for candidate create, shadow
write, compare, read ownership and forced reset. Changes are audited, generation-safe and default to
incumbent/disabled when configuration is missing or stale. A global candidate circuit breaker stops
new/renewal traffic without deleting rollback state.

Acceptance: 100 concurrent flag/account/reset races affect only selected templates/principals and
leave one cache owner; unauthorized/stale flag writes do nothing.

### MIG-004 — Qualify the exact release in an isolated production clone

**Depends:** `MIG-001`, `MIG-002`, `MIG-002B`, `E2E-004Q`, `OPS-009`, `TST-003`.
The profile graph additionally requires `CMP-006` for compatibility or `SWF-013` for native.
**Profiles:** all migration lanes. **Boundary:** laboratory migration evidence.

Restore scrubbed representative data/config to isolated infrastructure, replay the pinned trace and
execute install, common-fence shadow, cutover, every runbook fault, rollback and upgrade using the
immutable artifacts. No production credentials, endpoints or unsanitized evidence are allowed.

Acceptance: 10,000,000 mutations, 10,000 client lifecycle cycles, every enabled template's event
floor and 100 executions per failure cut pass with zero unexpected divergence and declared bounds.

### MIG-005 — Rehearse production-shaped rollback

**Depends:** `MIG-004`, `MIG-002B`. **Profiles:** all migration lanes.
**Boundary:** rollback exercise/evidence.

At conservative qualified load, cut back before/after feed create, common fence, cache switch,
schema/template deployment, server upgrade, DS restore/reset and app upgrade. Verify incumbent cache
freshness or cold rebootstrap, old/new server compatibility and no dual owner. Exercise both operator
and automatic circuit-breaker entry, but require authorization for destructive reset.

Acceptance: 100 runs per rollback cut meet the declared read-hold/RTO and exact SQL result; a stale
rollback cache is detected and withheld every time.

### MIG-006 — Passive production shadow

**Depends:** `MIG-005`, `MIG-003`, `E2E-004Q`. **Profiles:** all migration lanes.
**Boundary:** production shadow evidence.

Enable candidate server feeds and isolated client/app shadow state, but keep every incumbent read and
write owner authoritative. Compare only common-fenced states and exercise token refresh, lifecycle,
schema deployment, reset and the global circuit breaker.

Acceptance: at least 10,000,000 compared committed mutations, 10,000 lifecycle cycles, 1,000 resets/
reconnects and 100,000 events per enabled template; zero unexplained divergence, forbidden access,
resource-cap breach or unbounded lag. Counts, not elapsed time, control advancement.

### MIG-007 — Opt-in beta cutover

**Depends:** `MIG-006`. **Profiles:** all migration lanes. **Boundary:** beta exposure evidence.

Select 50 consenting installations spanning supported OS/device/account/template cohorts. Switch
eligible templates independently, retain fenced incumbent rollback state, and automatically stop a
template on correctness, auth/isolation, crash-loop, reset-loop or resource-bound failure.

Acceptance: at least 10,000 sessions, 1,000,000 committed mutations, 1,000 lifecycle/reset operations
and 50,000 events per enabled template with zero correctness/security divergence; reliability and
latency stay within `GOV-002` bounds.

### MIG-008 — Staged canary and general availability

**Depends:** `MIG-007`. **Profiles:** all migration lanes. **Boundary:** production promotion evidence.

Promote one semantic template version at a time through 10%, 50% and 100% eligible installations.
Each stage retains the global/template circuit breakers and fenced rollback cache; account/template
assignment is sticky and auditable. No server capability is enabled merely because a client ships.

Acceptance at each stage: at least 5,000,000 committed mutations, 5,000 lifecycle/reset operations and
100,000 events per enabled template, with zero unexplained correctness/security divergence and all
declared SLO/resource bounds passing. If the population cannot supply a floor, generate equivalent
qualified synthetic traffic without counting it as distinct-user coverage.

### MIG-009 — Remove the incumbent Electric path

**Depends:** `MIG-008`. **Profiles:** all migration lanes. **Boundary:** decommission change set.

Before removal, prove no supported client/app/template/server/runbook/backup/rollback manifest refers
to the old path, all minimum-supported app versions can reset to the candidate, and the candidate has
completed another 10,000,000 mutations plus 10,000 lifecycle cycles at 100% ownership. Export required
audit/evidence, revoke old credentials/routes/publication access, remove flags/providers/dependencies,
delete old cache generations through the privacy policy and update disaster recovery.

Acceptance: clean code/config/dependency/network scans find no incumbent endpoint or credential;
fresh install, upgrade from the minimum supported version, full reset, backup/restore and incident
recovery pass using only the selected Circuits profile.

## 13. Subagent execution and merge protocol

`PLAN-001` is the scheduling authority. It emits the ready set after each merged task and computes
profile-specific conditional dependencies; section ordering is explanatory, not permission to start
a task whose prerequisites are open.

Use these assignment rules:

1. Give one subagent one task ID and its declared principal boundary. Parallel agents may read shared
   code but must not own the same implementation artifact.
2. Design/ADR/schema tasks merge before their implementation consumers. Implementation merges before
   adversarial qualification; qualification never patches the behavior it is judging.
3. Each agent prepares an immutable execution handoff whose task/execution-scope/profile/attempt
   identity is injective, plus task-specific tests and machine-readable evidence. It may hand off
   `ready_for_review`, `fail`, or `blocked`; the controller's distinct resolution may record
   `pass`, `fail`, `blocked`, or `invalidated`, and only accepted integration writes `pass`. A commit
   or push remains explicitly user/delegated-authority gated; absent that authority, the integrator
   verifies and commits the reviewed content-addressed handoff patch.
4. A separate reviewer checks the diff against the task contract and invariants. Integration tasks
   (`E2E-000I`, `PG18-003A`, `PG18-003Q`, `E2E-001Q`, `E2E-002Q`, `E2E-003CQ`,
   `E2E-003NQ`, `E2E-004Q`, `E2E-005`, `CAP-003Q`, `OPS-001B`, `OPS-002`, `OPS-004`,
   `OPS-009`, `TST-003`, `MIG-004`–`MIG-009`) get an additional cross-boundary reviewer.
   The reviewer never runs evidence from the author's directory: it recreates the exact commit or
   verified prepared tree in a new source, resolves the exact immutable external-input/mount manifest
   into its own new empty run root, and verifies the required pre/post clean-source, dependency and
   effective-config attestations before accepting results.
5. Merge one logical task at a time. For an unrelated ancestor change, preserve the reviewed
   candidate SHA and red evidence, create a fresh merge-preview against current integration HEAD,
   rerun direct gates there, and refresh review only if the preview preserves contract and observed
   results. A changed declared predecessor, allowed-path/read-set or semantic-resource intersection,
   contract/schema, profile/config, image, or toolchain is a hard invalidation requiring a fresh
   packet/attempt; integration never rebases a stale candidate to reuse evidence. This refresh applies
   only to committed immutable task SHAs; an uncommitted patch whose base advances receives a fresh
   packet and review.

For behavior declared `genuine_red`, the scheduler first issues a `red_artifact` packet bound to one
provider/consumer/scenario/scope/base. Its independently reviewed failing commit enters `red_proved`
but is not merged alone; only then may the scheduler issue the implementation packet that consumes
that exact SHA and closes the unchanged contract green. Even one author receives two packets with the
red review between them. A provider serving several implementations emits separate artifacts so one
consumer never inherits unrelated failing tests. An `inherited_control` records only its unchanged
characterization, and a `non_behavioral` packet has no red-proof requirement. Only the green stack
merges. A skipped, inverted or permanent expected-failure test does not count as TDD or gate evidence.

The initial merge-ready set is only `PLAN-001`; inventory collection for later tasks may proceed in
parallel, but it cannot be marked complete or merged before the validator exists. After `PLAN-001`,
the generated graph exposes `GOV-001` and `TST-000`. After `GOV-001`, agents may take `CMP-000`,
`GOV-002`, `GOV-003`, and `SEC-008B`; `CMP-001` becomes ready only after both `CMP-000` and
`GOV-002`, and `PG18-000` becomes ready after `GOV-002`.
Thereafter the generated graph—not a manually maintained wave number—selects work, which prevents the
dependency cycles found in the first draft.

## 14. Current readiness decision

**Decision: no-go for production traffic today.** The fork has unusually substantial correctness
work and passing Rust/Swift baselines, but G0–G9 and the applicable G10 authorization stage are not
closed. The most immediate blockers are:

- no accepted ownership/support/profile/capacity contract or stable release line;
- public engine/API/DS/control surfaces lack the required authenticated template gateway and tenant
  boundary;
- durable-streams lacks a qualified production package, physical accounting/inventory and proven
  restore frontier;
- catalog boot/compaction, active stream growth, asynchronous queues, transaction/output staging and
  derived state do not yet have complete fail-closed boundedness;
- publication coverage, purge completion and subset visibility have concrete correctness gaps;
- the first supported single-writer/PG18 reset topology is not packaged or fault-qualified; PG18
  virtual generated columns currently diverge on live replication, and slot-invalidation/TLS and
  PG/provider restore-frontier blockers remain open;
- the public gateway registry has no qualified owner/backup/restore contract;
- the compatibility inventory is not frozen against the real production app revision, and its
  200-message response-chunk/checkpoint ordering is not yet proven safe; and
- neither the restricted compatibility provider nor the native Swift package exists and passes its
  profile gates.

Production readiness for initial 100% GA means one selected profile's machine-generated closure
reaches G0–G9 and G10a–G10d through the explicit 100% authorization. G10e is the later incumbent
decommission gate, not a prerequisite to start GA. Readiness does not require implementing
`NATIVE_SUBSET`, `NATIVE_TXN_ATOMIC`, hot table reload, seamless failover, direct DS capabilities or
edge caching when those capabilities are rejected by the selected profile.

## 15. Open-issue disposition

`GOV-003` must preserve per-issue detail and current tracker state. This summary prevents a task agent
from treating an open number as either automatically unfixed or automatically out of scope.

| Tracker work | Reviewed disposition |
| --- | --- |
| Public upstream [#3](https://github.com/electric-sql/electric-circuits/issues/3), [#4](https://github.com/electric-sql/electric-circuits/issues/4) | Historical ID/restart mechanisms exist locally; re-prove through `STO-001`, `ENG-015`, `TST-010`; terminal client recovery is `TSC-001`. |
| Public upstream [#5](https://github.com/electric-sql/electric-circuits/issues/5) | Slot-loss protection exists, but supported restore/promotion behavior is narrowed and qualified by `DSR-001`–`DSR-003`, `LEAD-001`, `OPS-004`, `TST-012`. |
| Public upstream [#6](https://github.com/electric-sql/electric-circuits/issues/6) | Ingest spill exists; end-to-end transaction/output boundedness remains `ENG-007A`, `ENG-002` when selected, `TST-011`. |
| Public upstream [#7](https://github.com/electric-sql/electric-circuits/issues/7) | Drift/TRUNCATE retirement exists; replay/publication completeness remains `ENG-004`, `ENG-017`, `SEC-003B`, `TST-011`. |
| Public upstream [#8](https://github.com/electric-sql/electric-circuits/issues/8) | Durable catalog exists; compaction, atomic fail-closed application and storage-wide orphan reconciliation are `STO-001`, `ENG-015`, `ENG-011`. |
| Public upstream [#10](https://github.com/electric-sql/electric-circuits/issues/10), [#11](https://github.com/electric-sql/electric-circuits/issues/11) | HTTP cache/CDN/direct capability work is explicitly unsupported for the first release; proxy reads use `no-store`. A future profile needs new tasks and proof. |
| Public upstream [#12](https://github.com/electric-sql/electric-circuits/issues/12) | Segmented input retention exists; full physical disk, active output and reserve contracts remain `DST-001`, `STO-002`, `ENG-010`. |
| Public upstream [#13](https://github.com/electric-sql/electric-circuits/issues/13) | Pool/streamed backfill exists; connection/admission work is `ENG-006`, `ENG-008`. The proposed advisory lock is not claimed as stale-writer fencing; first-release ownership is `LEAD-001`. |
| Public upstream [#14](https://github.com/electric-sql/electric-circuits/issues/14), [#15](https://github.com/electric-sql/electric-circuits/issues/15) | Launch blockers owned by `SEC-002A`–`SEC-010`, `OPS-001A`/`OPS-001B`, `OPS-003A`/`OPS-003B`, `TST-004`. |
| Public upstream [#16](https://github.com/electric-sql/electric-circuits/issues/16) | Shutdown/readiness basics exist; listener split, objective telemetry and executable operations are `SEC-005A`, `CAP-001B`, `OPS-006`, `OPS-007`. |
| Public upstream [#17](https://github.com/electric-sql/electric-circuits/issues/17) | pgoutput v2 is not a launch requirement: current commit-spill semantics must pass `ENG-007A`/`TST-011`; v2 remains a future optimization unless a separate safety requirement is proven. |
| Public upstream [PR #47](https://github.com/electric-sql/electric-circuits/pull/47) | A badge is not license governance; `SEC-008B` owns actual dependency/license policy and release gates. |
| Parent fork [#11](https://github.com/pgxsinkit/electric-circuits/issues/11) | Hot discovery/re-created-table support is excluded from common profiles and owned by future-profile `ENG-005`; restart-required deployment remains `OPS-008`. |
| Parent fork [#12](https://github.com/pgxsinkit/electric-circuits/issues/12), [#13](https://github.com/pgxsinkit/electric-circuits/issues/13) | Replayed TRUNCATE is `ENG-004`; missing drift/publication/WAL evidence is `ENG-017`, `OPS-003B`, `TST-011`. |
| Parent fork [#14](https://github.com/pgxsinkit/electric-circuits/issues/14), [#15](https://github.com/pgxsinkit/electric-circuits/issues/15), [#16](https://github.com/pgxsinkit/electric-circuits/issues/16) | Walsender timeout/classification is `ENG-006`; DS/config/Prometheus behavior is `ENG-012`/`SEC-005A`; boot/provider checks are `OPS-003B`. |
| Parent fork [#17](https://github.com/pgxsinkit/electric-circuits/issues/17), [#18](https://github.com/pgxsinkit/electric-circuits/issues/18) | Native TS terminal recovery is `TSC-001`; Electric adapter gone→409 behavior is `ENG-003`. |
| Parent fork stale red-test PRs and locally fixed subset issues | `GOV-003` records them as superseded only after inherited regressions pass `TST-001`; they are not merged as-is. |
