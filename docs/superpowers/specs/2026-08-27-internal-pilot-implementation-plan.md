# Electric Circuits internal pilot implementation plan

**Date:** 2026-08-27  
**Status:** implementation plan for review  
**Architecture:**
[`2026-08-27-aws-durable-streams-and-engine-deployment-design.md`](./2026-08-27-aws-durable-streams-and-engine-deployment-design.md)

## 1. Outcome

Deploy Electric Circuits and a shared self-hosted Durable Streams service to the Indexed development
and production AWS accounts, then enable two independent employee-only product experiments:

1. committed `Conversation` and `Message` synchronization through Electric Circuits; and
2. provisional in-progress agent output through Durable Streams.

The incumbent Electric path remains the default and rollback path. This plan does not claim external
production readiness, zero-downtime source freshness, multi-AZ storage, or an independently deployed
ingestor.

`INTERNAL_PILOT_V1` is an operational cohort mode, not a release-qualification profile. Evidence from
the pilot can qualify later profiles, but the profile name itself must never be used as evidence that
the system is ready for general customer traffic.

Deploying the services with both product flags off is permitted before cohort enablement. Enabling
employee traffic remains blocked until `INTERNAL_PILOT_V1` is emitted ready by the generated
production-readiness manifest governed by `notes/18-production-readiness-spec-reviewed.md`; this task
list does not independently authorize the pilot.

## 2. Decisions closed for implementation

### 2.1 Engine and ingestor boundary

Do not deploy a separate ingestor now. The pilot runs the current coupled engine and uses the
stop-confirm-start handoff from the architecture specification. Durable Streams stays available, so
existing stream reads continue while new control operations and PostgreSQL-to-stream freshness pause.

After path and storage lineage are in place, extract a typed **in-process** transaction-source seam.
The seam must not add a process, task, slot owner, network hop, durable consumer registry, deployment
mode, or configuration switch in the pilot. Its purpose is to make a later service split mechanical
without paying the continuous-generation correctness cost now.

The first envelope contract is:

```text
InputTransactionV1 {
  version: 1,
  source_commit_id,
  txid,
  commit_lsn,
  envelopes: [EnvelopeV1 { seq, last, ... }]
}
```

Only complete transactions cross the seam; exactly one final envelope has `last=true`. N/N-1 codecs,
durable consumer leases, snapshot/change-log fences, and an independently deployed writer remain
blocked work for `CONTINUOUS_ENGINE_V1`.

### 2.2 Stack namespace and paths

The required engine scope is:

- `ELECTRIC_CIRCUITS_DS_NAMESPACE`: immutable stack name;
- `ELECTRIC_CIRCUITS_DS_STORE_ID`: expected storage identity;
- `ELECTRIC_CIRCUITS_DS_STORE_GENERATION`: expected storage generation; and
- `ELECTRIC_CIRCUITS_QUERY_GENERATION`: immutable query-engine generation.

The engine also requires `ELECTRIC_CIRCUITS_DS_PROTOCOL_VERSION`,
`ELECTRIC_CIRCUITS_DS_LAYOUT_VERSION`, `ELECTRIC_CIRCUITS_DS_DURABILITY_MODE`,
`ELECTRIC_CIRCUITS_DS_WAL_SHARDS`, `ELECTRIC_CIRCUITS_DS_STREAM_LANES`, and
`ELECTRIC_CIRCUITS_DS_FILESYSTEM_UUID`. Together these expected values form `StoreIdentityV1`. No
observed storage response can supply a missing expected value.

The namespace and query generation use lowercase ASCII identifiers matching
`^[a-z][a-z0-9-]{1,46}[a-z0-9]$` (3–48 characters). Dots, slashes, underscores, percent escapes,
whitespace, uppercase characters, and empty values are rejected. `store_generation` is a canonical
lowercase UUID. Development and production have no unqualified compatibility fallback. Unit tests may
construct an explicit test scope; no environment-sensitive `unscoped` mode is shipped.

All logical Durable Streams paths are qualified exactly once inside `DsClient`. Callers continue to
use `meta/catalog`, `changes/<segment>`, and `shape/<id>`. A path already beginning with `/`, containing
an empty component, `.` or `..`, a backslash, a query/fragment marker, or a percent escape is rejected
before HTTP I/O.

The agent product/API contract similarly retains the logical path `agent-runs/v1/{run_ref}`. The
authenticated gateway maps that logical identity exactly once to the normalized physical request path
`/agent-runs/v1/<stack>/stores/<store-generation>/runs/<run-ref>`; neither producers nor clients choose
the stack, generation, or physical prefix.

### 2.3 Storage identity authority

Pulumi creates `store_id` and `store_generation` as explicit per-environment values and stores them in
protected SSM parameters. Pulumi also records the expected volume ID, KMS key, selected AZ, filesystem
UUID, protocol/layout version, durability mode, WAL shards, and stream lanes. Those values are the
independent expected configuration; the on-volume manifest is the observed state.

A one-shot host operation initializes a new volume using the explicit expected values. Its Pulumi
authorization defaults off in every stack, is enabled only for the exact volume/store/generation
tuple being initialized, and is returned to off before ECS registration and ordinary service
admission. Ordinary server startup only validates. A reset allocates a new `store_generation` and
uses a newly empty or explicitly replaced filesystem; it never rewrites the identity of a live store
in place. Neither the ECS task entrypoint nor an empty stream inventory may auto-initialize a store.

The first manifest format is JSON at `/data/.durable-streams-store-v1.json`, written via temporary
file, file `fsync`, atomic rename, and parent-directory `fsync`. It contains every field named in the
architecture specification. Unknown fields are tolerated; missing required fields, duplicate keys,
type mismatches, or expected/observed mismatches are fatal.

`artifact_digest` is not a bootstrap argument or manifest field. It is supplied only to ordinary
server startup and echoed by readiness so operators and consumers can attest the running artifact
without coupling a deploy to persistent storage lineage.

### 2.3.1 `StoreReadinessV1` contract

`GET /_admin/ready` returns the following versioned shape. This document is the normative contract
for the first-wave implementations; a semantic change requires a new `contract_version`.

```json
{
  "contract_version": "durable-streams-store-ready-v1",
  "status": "ready",
  "artifact_digest": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "manifest": {
    "store_id": "2bc96d0b-9740-4f50-97c6-754b2b27d6b0",
    "store_generation": "ff8b5fa6-e786-4994-8da0-f14e9e79f318",
    "protocol_version": 1,
    "layout_version": 1,
    "durability_mode": "wal",
    "wal_shard_count": 2,
    "stream_lane_count": 1,
    "filesystem_uuid": "253f14d5-cbee-4df8-9e3c-e44c6e41501b",
    "creation_time": "2026-08-27T19:00:00Z"
  },
  "recovery": {
    "completed": true,
    "wal_shards": [
      { "shard": 0, "durable_lsn": 0, "checkpoint_lsn": 0 },
      { "shard": 1, "durable_lsn": 0, "checkpoint_lsn": 0 }
    ]
  },
  "reserve": {
    "free_bytes": 85899345920,
    "free_inodes": 1000000,
    "minimum_free_bytes": 21474836480,
    "minimum_free_inodes": 10000,
    "satisfied": true
  }
}
```

Required fields have exactly the displayed JSON types. Integer fields are non-negative and fit in
unsigned 64 bits; shard indices and the manifest counts fit in unsigned 32 bits. UUIDs are canonical
lowercase hyphenated values. `creation_time` uses the whole-second canonical RFC 3339 UTC form
`YYYY-MM-DDTHH:MM:SSZ`; fractional seconds and numeric offsets are rejected so it has one spelling.
`artifact_digest` is lowercase `sha256:` plus 64 hexadecimal digits. Duplicate keys at any level are
rejected; unknown fields are tolerated. The manifest object is byte-for-field identical to the
on-volume manifest.

`status` is one of `starting`, `recovering`, `ready`, or `stopping`. It is `ready` if and only if
`recovery.completed` and `reserve.satisfied` are both true. The endpoint returns HTTP 200 only for
`ready` and HTTP 503 for the other well-formed states. It is read-only and bounded, contains no
credentials or stream inventory, and never initializes or repairs state. The per-shard LSN values are
operational WAL frontiers, not PostgreSQL lineage or a global ordering across shards.

Both first-wave repositories keep positive and negative fixtures for this exact shape, including
duplicate-key, missing-field, wrong-type, non-canonical UUID/time/digest, non-ready, and reserve-failed
cases. The engine owns the strict reader and the server owns the writer; neither may weaken the
contract independently.

### 2.4 Access boundary

**Amendment (2026-09-16):** The supported production boundary is private-subnet HTTP to Durable
Streams, scoped by security groups to the engine and API/broker services. Public clients and agent
producers have no direct DS access. The authenticated API/gateway enforces service roles, method/
prefix permissions, per-run authorization, and request budgets. Infrastructure evidence verifies
the private listener and allowed callers. TLS/mTLS and the `DS-02` proxy are optional.

When TLS is selected, a small Rust reverse-proxy binary, `durable-streams-access`, is built and
pinned alongside the server. Durable Streams then remains HTTP on `127.0.0.1:4437`; the proxy alone
listens on private port 8443. The proxy:

- terminates TLS 1.3; client certificates are required only when mTLS is selected;
- when mTLS is selected, validates the environment CA and maps certificate URI SANs to service
  identities, enforcing method/prefix policy before forwarding;
- for server-auth TLS, retains security-group scoping and API-brokered authorization as in HTTP mode;
- in mTLS mode, exposes `GET /_admin/ready` only to storage-administrator/engine identities and
  `/_admin/inventory` only to storage-administrator/retention identities; server-auth TLS retains
  the network/API restrictions on these routes;
- forwards streaming reads without buffering the full body and applies explicit body/time limits;
- emits structured authorization, queue, upstream, and trace logs to stdout/CloudWatch; and
- enforces per-identity concurrency and append-rate limits in mTLS mode; the API enforces those
  limits for brokered requests in HTTP and server-auth TLS modes.

When configured, certificates and private keys are delivered through Secrets Manager to the task and
rotated by a controlled task replacement. A versioned, read-only policy file is rendered by IaC; its SHA-256 is
logged at startup and included in deployment evidence. Policy parse failure, unknown identities,
ambiguous prefix matches, unnormalized paths, missing client certificates in mTLS mode, and inability to obtain
storage reserve state all fail closed.

Agent producers do not connect directly. The authenticated Indexed API/gateway owns run assignment
and writes on the producer's behalf using the `agent-writer` service identity. The API/gateway
(and the certificate-aware proxy when mTLS is selected) enforces
`agent-writer -> /agent-runs/v1/<stack>/stores/<store-generation>/runs/` and the gateway enforces the
exact assigned opaque run ID. Similarly, the client gateway resolves a public handle before using
its read-only storage identity. This avoids inventing per-run storage credentials while preserving
both application authorization and storage prefix isolation.

### 2.5 Pilot capacity reservations

`PILOT_AWS_V1` fixes the following conservative admission values for its 100 GiB volume. Changing
them creates `PILOT_AWS_V2` and requires repeat load qualification.

| Resource | Circuits/control | Agent writes | Gateway reads | Global |
| --- | ---: | ---: | ---: | ---: |
| Concurrent upstream requests | 64 | 32 | 128 | 232 |
| Append request rate | unthrottled inside concurrency bound | 100/s | n/a | n/a |
| Append bytes | protected by global reserve | 8 MiB/s | n/a | n/a |
| Stored-byte budget | 60 GiB operational budget | 20 GiB hard budget | n/a | 20 GiB free reserve |

Eight additional admin/readiness request slots are reserved outside the 232 data-request slots.
Agent admission stops when its registry reaches 20 GiB, when filesystem free space reaches 40 GiB,
or when free inodes reach twice the global hard reserve. Circuits mutations stop only at the global
20 GiB/free-inode reserve. Reads remain admitted under storage pressure. The qualification load test
must prove an agent flood cannot consume admin slots or prevent catalog/checkpoint traffic.

These are admission limits, not throughput claims. The performance task measures whether they fit
the selected instance/EBS profile and lowers them if necessary; it may not silently raise them.

### 2.6 Named source fence

The deployment controller creates a fence by inserting a UUID `source_commit_id` into a small
PostgreSQL `circuits_source_fence` table included in the Circuits publication. The row has no business
effect. The engine records the observed fence only after the complete source transaction has passed
the sequencer and every resulting catalog/output append is durable.

The durable catalog event is `SourceDrained { source_commit_id, commit_lsn }`. Private engine
endpoints close/open control admission and return the last durable receipt plus whether a requested
ID is drained. They require a controller-only bearer token from
`ELECTRIC_CIRCUITS_CONTROL_SECRET`; the client-facing `ELECTRIC_SECRET` must not authorize them, and
an unset control secret disables them. The controller, not ECS, calls these endpoints. A handoff
timeout aborts new control admission and leaves the incumbent Electric provider selected; it never
treats task exit or slot release as a drain receipt.

## 3. Delivery graph

```text
EC-01 namespace + StoreBound ---------+----> EC-02 shutdown + source fence ----+
                                      |                                       |
DS-01 manifest + lock + readiness ----+----> DS-02 (TLS only) -----------------+--> INFRA-01
                                                                              |
EC-03 in-process transaction seam --------------------------------------------+
                                                                              v
IDX-01 gateway/control contracts ---------> IDX-02 committed chat ------------> IDX-03 agent streams
                                  INFRA-01 ---------^               EC-02 -----^
                                                                              |
                                                                              v
                                                                     QUAL-01 --> ROLL-01
```

Only `EC-01` and `DS-01` begin immediately. They operate in different repositories and freeze the
lineage contract consumed by every later task.

**Amendment (2026-09-16):** The `DS-02` node and its downstream edges apply only when TLS/mTLS is
selected. Private HTTP proceeds from `DS-01`, `EC-01`, and `EC-02` to `INFRA-01` without that node;
`IDX-01` requires `DS-01` and `EC-01`, adding `DS-02` only for TLS/mTLS.

## 4. Concrete tasks

### EC-01 — Qualify Durable Streams paths and bind the catalog to storage

**Repository:** `electric-circuits`  
**Owns:** `apps/engine/src/config.rs`, `apps/engine/src/ds.rs`, a new path-scope module,
`apps/engine/src/engine/catalog.rs`, engine boot wiring, and focused tests.

Implement:

- required `stack_namespace`, `query_generation`, and explicit expected `StoreIdentityV1`
  configuration: `store_id`, `store_generation`, `protocol_version`, `layout_version`,
  `durability_mode`, `wal_shard_count`, `stream_lane_count`, and `filesystem_uuid`, all strictly
  parsed from the independent per-environment deployment configuration;
- a `StreamScope` value that maps validated logical paths to the physical pilot prefix exactly once;
- treat the engine's existing `stream_url`/`streamUrl` response as an internal compatibility surface,
  not a pilot client contract: it may identify the private DS endpoint during EC-01, but no iOS or
  web client may receive or use it; `IDX-01` replaces it at the product boundary with an authenticated
  opaque gateway handle before either product flag can be enabled;
- **Amendment (2026-09-16):** Private-subnet plaintext HTTP to Durable Streams, with security-group
  scoping and API-brokered client access, is the supported production mode. TLS/mTLS is optional:
  HTTPS verifies the server using an optional CA bundle (system roots when absent), with an optional
  client certificate/key pair configured together. HTTP ignores TLS paths; the explicit in-process
  test-store mode remains separate;
- `StoreReadinessV1` decoding and full expected/observed identity comparison: perform
  `GET /_admin/ready` as the first network operation after configuration validation and before
  constructing or starting PostgreSQL setup; a non-`ready`, malformed, unauthorized, or mismatched
  response is boot-fatal and performs no normal DS operation (`ensure`, append, read, close, or
  delete) and no PostgreSQL connection/setup;
- `StoreBound { store: StoreIdentityV1, stack_namespace, ingest_epoch: "coupled-v1",
  query_generation }`; it is catalog event zero, occurs exactly once, and no ordinary catalog event
  may precede it; missing, duplicate, conflicting, malformed, or later/reordered bindings are fatal;
- explicit first-namespace initialization: an empty physical catalog may receive `StoreBound` only
  when an operator-provided `ELECTRIC_CIRCUITS_INITIALIZE_NAMESPACE=1` is present; ordinary boot may
  not adopt an empty catalog; and
- mismatch errors that are typed, name expected versus observed identity, and leave both PostgreSQL
  and Durable Streams untouched.

Acceptance:

- two clients with different namespaces generate disjoint physical paths for identical logical
  operations;
- malformed scope/path input fails before the fake store records an operation;
- readiness/manifest mismatch fails before catalog ensure, catalog append, or PostgreSQL setup;
- first initialization writes `StoreBound` durably before slot creation/adoption;
- subsequent boot requires exactly one matching binding; missing, conflicting, or reordered binding
  histories fail closed; and
- existing logical-path tests continue to run using an explicit test scope.

Verify with `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`, focused engine
tests, then the repository's required test command.

### DS-01 — Add store bootstrap, exclusive ownership, attested readiness, and inventory

**Repository:** `durable-streams-rust`  
**Owns:** `src/main.rs`, new `src/store_manifest.rs` and `src/data_dir_lock.rs` modules, loopback admin
handlers, `tests/cli_durability_guards.rs`, and new focused integration tests.

Implement:

- an explicit `bootstrap-store` CLI operation accepting every expected manifest field but no
  artifact digest;
- atomic/fsynced creation that refuses any existing manifest, WAL, stream, segment, or cold-tier
  state and never starts a listener;
- a non-blocking exclusive advisory lock at `<data-dir>/.durable-streams.lock`
  (`/data/.durable-streams.lock` in deployment), acquired before bootstrap, manifest validation,
  store construction, WAL recovery, mutation, or listener bind and held for the process lifetime;
- required expected-identity startup arguments in WAL mode and strict comparison with the manifest:
  `--store-id`, `--store-generation`, `--protocol-version`, `--layout-version`, and
  `--filesystem-uuid`; ordinary server startup also requires `--artifact-digest`, which is reported
  through readiness but is never written into the manifest;
- loopback-only `/_admin/ready` reporting `starting`, `recovering`, `ready`, or `stopping`, plus the
  manifest, canonical running `artifact_digest`, recovery result, durable frontier, free bytes/inodes,
  and reserve state; the digest is a required immutable startup value injected from the built image
  digest, not the Cargo package version;
- bounded/paginated `/_admin/inventory` with stream path, closed/deleted state, and durable byte count;
- a shutdown transition that becomes unready before draining committers and releasing the lock; and
- a reserved `/_admin/` route namespace that can never be created, read, appended, closed, or deleted
  as a user stream.

Acceptance:

- a second process against the same data directory exits non-zero before changing a byte or binding a
  port;
- killing a WAL process and restarting on the same matching manifest recovers acknowledged appends;
- wrong store ID/generation, filesystem UUID, layout, durability, shard, or lane refuses before
  recovery mutation;
- ordinary startup against a blank directory refuses; the lock inode is the sole allowed control
  artifact, and refusal creates no manifest, WAL, stream, segment, or cold-tier state;
- the lock path is derived from the supplied `--data-dir`; a second process fails before Store
  construction, WAL recovery, or public-listener bind while the first remains unchanged;
- bootstrap refuses a non-empty or previously initialized directory and survives crash-boundary
  tests around temporary write, rename, and directory fsync;
- readiness never reports ready before recovery and reserve checks complete; and
- inventory is bounded, stable under pagination, and excludes no stream the backup contract needs.

Verify with `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test`, and the
existing WAL simulation/recovery suites.

### DS-02 — Build and qualify the optional TLS/mTLS access boundary

**Depends on:** `DS-01`  
**Applicability (2026-09-16):** Only deployments selecting TLS/mTLS; not required for private HTTP.

**Repository:** `durable-streams-rust`  
**Owns:** a new `durable-streams-access` binary/library, policy schema, container packaging, and proxy
integration tests. It does not change WAL/storage semantics.

Implement the selected TLS contract in section 2.4, including streaming proxy behavior, trace
propagation, timeouts, admin isolation, and the fixed capacity profile. For mTLS, also implement
certificate identity, method/prefix policy, and URI normalization; test each identity against every
verb and own/foreign prefix. Test overload, slow readers, lost upstream responses, rotation of
configured certificates, and agent-budget isolation. Private HTTP qualifies its network and API
access controls through `INFRA-01` and `IDX-01` instead.

### EC-02 — Make pilot handoff observable and fail closed

**Depends on:** `EC-01`, `DS-01`  
**Repository:** `electric-circuits`  
**Owns:** engine admission/readiness, catalog receipt, shutdown result, source-fence observation, and
private admin endpoint.

Implement:

- a control-admission gate distinct from read health;
- `SourceDrained` only after the named complete transaction and all derived writes are durable;
- authenticated private `drainedThrough(source_commit_id)` state;
- an explicit non-zero/incomplete shutdown result when catalog drain does not complete; and
- tests that prove task exit, slot release, and checkpoint attempts are not mistaken for the durable
  source receipt.

The stop-confirm-start controller is delivered in `INFRA-01`; no pre-started candidate is added here.

### EC-03 — Extract the in-process transaction seam

**Depends on:** `EC-01`  
**Repository:** `electric-circuits`  
**Owns:** replication-to-sequencer transaction interface and compatibility tests.

Introduce `InputTransactionV1` between pgoutput decoding and the existing sequencer while preserving
the current single process, task, slot, ordering, backpressure, and acknowledgement behavior. Add
round-trip/golden tests for transaction boundaries and envelope version 1. Do not add HTTP, a service
binary, a durable input consumer, N/N-1 negotiation, or an independent deployment flag.

### INFRA-01 — Provision persistent storage and controlled deployments

**Depends on:** `DS-01`, `EC-01`, `EC-02`; additionally `DS-02` only when TLS/mTLS is selected (2026-09-16).

**Repository:** `indexed`  
**Owns:** Pulumi components, AMI/user-data mount gate, ECS task/service definitions, service discovery,
IAM/KMS/Private CA/Secrets Manager/SSM policy, alarms, and the deployment controller.

Deliver:

- one environment-specific dedicated ECS EC2 capacity provider in the selected AZ;
- a separately owned encrypted gp3 volume with delete-on-termination disabled and no Multi-Attach;
- fail-closed host mount gate that mounts the filesystem at `/mnt/durable-streams` and bind-mounts
  only `/mnt/durable-streams/data` to container `/data`, leaving ext4 `lost+found` outside the server
  data directory;
- explicit bootstrap runbook/automation guarded by a per-stack one-shot authorization that defaults
  off, is scoped to the exact volume/store/generation tuple, and is off before ECS registration;
- singleton Durable Streams service with the server; add the access-boundary container only for TLS/mTLS;
- a server task command that supplies `--store-id`, `--store-generation`, `--protocol-version`,
  `--layout-version`, `--filesystem-uuid`, and `--artifact-digest` on every ordinary start;
- engine service using stop-confirm-start, source-fence polling, and rollback to the incumbent provider;
- security groups allowing only the engine and API/broker services to reach the private DS endpoint,
  with service roles and prefix policy enforced by the API (and by the proxy when mTLS is selected);
- the private HTTP DS origin rendered into the engine task, with no TLS mounts required. When HTTPS
  is selected, render the proxy origin and optional CA bundle (system roots when absent); mount an
  optional client certificate/key pair together for mTLS. Refuse half pairs or unreadable configured
  material; HTTP ignores TLS paths (2026-09-16);
- alarms for free bytes/inodes, lock/readiness, WAL recovery, request budgets, latency, and task/host
  replacement; and
- dev first, then production-account deployment with both product flags default-off.

Infrastructure tests must prove a task revision retains the volume, a second task cannot become ready,
a root-volume fallback cannot register, and host replacement reattaches the exact volume only after
former-host fencing. Access tests prove that public clients cannot reach DS and only the declared
engine/API security groups can reach its selected private listener; API authorization remains required.

### IDX-01 — Add gateway, handle, and agent-run control contracts

**Depends on:** `DS-01`, `EC-01`; additionally `DS-02` only when TLS/mTLS is selected (2026-09-16).

**Repository:** `indexed`  
**Owns:** authenticated server-side Circuits handles, agent-run registry/leases, provider feature flags,
and DS client configuration.

Implement separate flags for committed-chat sync and provisional-agent delivery. Public clients never
receive physical DS credentials or choose prefixes. The gateway binds every handle to principal,
template, stack, store generation, query generation, and physical stream; returns typed
`generation_changed`/`stream_expired`; owns agent stream assignment, producer epochs, terminal close,
24-hour retention eligibility, and server-authoritative leases; and enforces the 20 GiB agent budget.
It also owns the authenticated handle-stream metadata contract that carries ordered `SourceCommitID`
markers required for client materialization receipts. This is a client data contract, not an exported
engine lifecycle/control API.

### IDX-02 — Synchronize committed chat through Circuits into GRDB

**Depends on:** `IDX-01`, `INFRA-01` dev deployment  
**Repository:** `indexed` iOS  
**Owns:** reusable Circuits transport/provider wiring plus `Conversation` and `Message` scopes.

Factor the calendar prototype's Circuits configuration/session into a reusable service. Register
server-owned `Conversation` and `Message` scopes and write received committed changes into GRDB.
SwiftUI continues to observe GRDB; it never renders subscription state directly. Preserve one
authoritative base provider at a time and the existing optimistic-message ownership rules. The flag is
employee-only and can switch back to Electric without changing local query APIs.

Persist
`appliedTailAfter(source_commit_id, stack_namespace, store_generation, query_generation,
materialization_generation)` only in the same GRDB transaction that applies the corresponding
committed Circuits changes.

Planning guidance loaded for this task: repo router `ios-electric-grdb-sync`, Axiom
`axiom-networking`, and `docs/ios/agent-workflow.md`, `docs/ios/electric-sync-model.md`, and
`docs/ios/electric-grdb-coordination.md`. Before implementation, re-load those instructions and use
the repo's required build/test/simulator verification workflow.

### IDX-03 — Materialize provisional agent output into a disposable GRDB projection

**Depends on:** `IDX-01`, `IDX-02`, `EC-02`, and `INFRA-01` dev deployment  
**Repository:** `indexed` API and iOS  
**Owns:** authenticated agent-run stream endpoint/client, disposable local projection, and final-row
reconciliation.

The iOS client follows the gateway stream with `URLSession` using opaque offsets. In one GRDB
transaction it applies each effect, records `(run_id, producer_epoch, sequence)`, and advances the
opaque offset. `ConversationView` renders that GRDB projection, not the network stream. A final
committed `Message` replaces provisional output exactly once after its source receipt; `404`, typed
`410`, restart, duplicate delivery, late prior-epoch events, and an already-finalized row all converge
to the durable database state. The committed-chat and agent-delivery flags remain independent.

The terminal replacement transaction requires the matching persisted `appliedTailAfter` receipt;
otherwise it retains or reloads the disposable projection. Relaunch, duplicate delivery, stale
producer epoch, `404`, `410`, and final-before/after-commit cases converge through GRDB transactions,
never view state or in-memory sequence bookkeeping.

### QUAL-01 — Produce deployment and failure evidence

**Depends on:** all implementation tasks  
**Repositories:** all three  

Automate the architecture specification's acceptance matrix in development, including lock refusal,
mount identity, lineage mismatch, prefix authorization, acknowledgement-crash recovery, agent/control
capacity isolation, old-handle generation behavior, agent terminal/lease races, HTTP latency envelope,
DS task recreation, engine stop-confirm-start, and cohort rollback. Repeat the storage/performance
subset twice on production-shaped resources before enabling the production employee cohort.

The provisional same-AZ HTTP objective remains a measurement target, not a promise: matched-trace
transport overhead below both 1 ms p99 and 25% of append p99. Preserve raw samples and configuration;
do not subtract independently aggregated percentiles.

### ROLL-01 — Enable the internal cohort

**Depends on:** `QUAL-01`  
**Repository:** `indexed`

Enable dev staff first only after the generated production-readiness manifest emits
`INTERNAL_PILOT_V1` ready. Then deploy the same pinned images and versioned profile to the production
account with flags off, run smoke/recovery checks, and enable a named employee cohort only while that
generated gate remains satisfied. Roll back the two flags independently. A rollback never deletes
the DS volume, resets a store generation, drops the incumbent Electric slot, or rebinds an old client
offset to a new physical stream.

## 5. First execution wave

Start only:

1. `EC-01` in `electric-circuits`; and
2. `DS-01` in `durable-streams-rust`.

Review their shared JSON identity contract before either merges. Then start `EC-02` and `EC-03`,
adding `DS-02` only when TLS/mTLS is selected (2026-09-16). Infrastructure and product work follow
their dependencies above; private HTTP has no `DS-02` gate. Storage and handoff contracts must be
executable, and `INFRA-01`/`IDX-01` qualify network/API access controls. `EC-03` gates only code consuming the
in-process transaction seam. This prevents Pulumi, gateway, and iOS code from hard-coding guessed
storage or path behavior.

## 6. Explicit non-goals for the pilot

- no separate ingestor ECS service;
- no active/candidate query engine overlap;
- no second Durable Streams writer on the volume;
- no ECS service-managed EBS volume;
- no EFS, S3 hot WAL, hosted Durable Streams, Multi-Attach, or multi-AZ failover claim;
- no client-to-storage credentials or direct client DS endpoint;
- no replacement of the incumbent Electric path for non-employees; and
- no scheduler, exported lifecycle-control API, temporary CLI fixture, or generated trust-input work
  as part of this delivery plan.
