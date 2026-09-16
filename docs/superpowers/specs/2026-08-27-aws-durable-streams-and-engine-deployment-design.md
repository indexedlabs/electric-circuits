# AWS Durable Streams, Circuits handoff, and continuous-engine deployment

**Date:** 2026-08-27  
**Status:** architecture contract revised after independent GPT-5.6 Sol xhigh review; not production-qualified  
**Authority:** subordinate to [`notes/18-production-readiness-spec-reviewed.md`](../../../notes/18-production-readiness-spec-reviewed.md)

## Transport amendment — 2026-09-16 user decision

TLS between the engine and Durable Streams is optional. **Private-subnet plaintext is the supported
production mode**, with the API brokering client access. This supersedes mandatory engine-to-DS
mTLS statements in decision 5, the topology, and the access-boundary sections below; mTLS remains
available when selected. The remaining identity, namespace, readiness, durability and recovery
contracts still apply, and this amendment is not release qualification.

`ELECTRIC_CIRCUITS_DS_URL=http://…` uses HTTP/1.1 and ignores DS certificate settings.
For `https://…`, `ELECTRIC_CIRCUITS_DS_CA_BUNDLE` is optional (system roots when unset);
`ELECTRIC_CIRCUITS_DS_CLIENT_CERT` plus `ELECTRIC_CIRCUITS_DS_CLIENT_KEY` are optional together
(present: mTLS; absent: server-auth TLS; half pair: startup error). See the
[exact environment contract](../../../docker/README.md#durable-streams-transport-amendment-2026-09-16).
PostgreSQL TLS and the explicit in-process test mode are unchanged.

## 1. Goal and profiles

Define the AWS topology and lifecycle for:

- deploying one self-hosted Durable Streams service in development and the production account;
- running Circuits for an employee-only production cohort without replacing the incumbent sync path;
- storing Circuits streams and in-progress agent messages on the same physical service;
- replacing the current singleton Circuits engine without losing a committed effect; and
- evolving toward query-engine deployments that do not pause PostgreSQL ingest.

This specification deliberately separates what can be deployed with the current engine from the
architecture required for continuous engine replacement:

| Profile | Purpose | Status |
| --- | --- | --- |
| `INTERNAL_PILOT_V1` | One Durable Streams process, one Circuits engine, stop-completely/start-new engine replacement, employee-only traffic | implementation target after the pilot blockers in this document close |
| `CONTINUOUS_ENGINE_V1` | Stable ingestor, active/candidate query generations, and fenced promotion | blocked future profile |

The pilot is internal testing, not an external-customer cutover. Because it runs in the production
account, it still follows the production transport, identity, storage, and recovery boundaries from
the canonical production-readiness note.

Provisioning resources with product flags off does not authorize employee traffic. Employee pilot
enablement remains gated on `INTERNAL_PILOT_V1` being emitted ready by the generated
production-readiness manifest governed by the canonical note; this prose contract and a successful
infrastructure deployment are not substitutes for that generated closure.

## 2. Locked decisions

1. PostgreSQL is authoritative for committed application rows. Durable Streams is authoritative for
   acknowledged stream bytes, stream metadata, and opaque tail validity. The gateway registry is
   authoritative for public-handle-to-physical-stream bindings. Each client cache owns its last
   applied opaque offset atomically with the materialized effects of that offset. Durable Streams is
   the only recovery source for provisional agent output not yet represented in PostgreSQL.
2. Self-host [`pgxsinkit/durable-streams-rust`](https://github.com/pgxsinkit/durable-streams-rust)
   in WAL mode. Hosted Durable Streams is not a deployment dependency.
3. `durable-streams` owns one persistent gp3 EBS volume. Circuits, agent producers, and the gateway
   reach it over a private authenticated HTTP boundary; none mounts its filesystem.
4. Run Durable Streams as a singleton ECS service on a dedicated ECS EC2 capacity provider in one
   Availability Zone. The EBS volume belongs to the storage-host lifecycle, not to an ECS
   service-managed task volume.
5. Durable Streams listens only on loopback. A co-located access-boundary container exposes mTLS,
   identity, verb authorization, and prefix authorization. Direct same-AZ traffic goes to this
   boundary through private service discovery; there is no ALB, API Gateway, or public listener.
6. Clients never reach the storage listener. An authenticated gateway owns client authorization,
   public read handles, generation binding, and typed invalidation.
7. Every Circuits stack has an immutable namespace. Namespaces prevent path collisions; they are not
   writer fencing. One active engine writer is permitted per pilot namespace.
8. `INTERNAL_PILOT_V1` stops and confirms termination of the old engine before starting the new
   engine. A candidate that merely retries the logical slot is not passive and must not be pre-started.
9. Losing or rolling back the Durable Streams volume is a whole-store-generation recovery event. It
   never silently becomes a first boot against an already-advanced PostgreSQL slot.
10. S3 tiering is excluded from the pilot. It may later hold sealed cold segments, but it never
    replaces the locally durable WAL, metadata, and hot tail.

## 3. Pilot topology and ownership

```text
                              production VPC, one selected AZ

 PostgreSQL primary
      |       ^
      | WAL   | snapshot/query-back
      v       |
 Circuits engine -------------------- mTLS ------------------+
                                                            |
 Indexed API agent writer ----------- mTLS ------------------+--> DS access boundary
                                                            |      (prefix + verb auth)
 authenticated gateway -------------- mTLS, read only -------+             |
      ^                                                                    | loopback HTTP
      | authenticated client API                                           v
 internal iOS/web clients                                          durable-streams
                                                                          |
                                                                          v
                                                               host-mounted gp3 EBS
```

The incumbent Electric deployment remains installed and is the default/rollback path. Product flags
select Circuits synchronization and Durable Streams agent delivery independently for the employee
cohort.

| Component | Canonical state | Local storage |
| --- | --- | --- |
| PostgreSQL | Application rows; Circuits deployment registry; durable producer-epoch allocation | Provider-managed database storage |
| Durable Streams | Stream bytes and metadata, WAL, on-volume store manifest | Container `/data`, backed by host `/mnt/durable-streams/data` on persistent gp3 EBS |
| Circuits engine | No irreplaceable state beyond PostgreSQL and Durable Streams | Ephemeral transaction spill and rebuildable DBSP state |
| Future ingestor | No irreplaceable state beyond its durable registry/catalog, slot, and acknowledged input streams | Ephemeral bounded transaction spill |
| Gateway | Public-handle registry and generation routing | Separately qualified durable registry |
| Client | Materialized effects, semantic event IDs, and opaque offset in one transaction | Client database |

The engine and future ingestor never receive the Durable Streams host path. Filesystem ownership
stays entirely inside the storage task.

## 4. Identity, namespace, and physical paths

### 4.1 Lineage terms

Every recovery decision compares the complete lineage relevant to that component:

- PostgreSQL: cluster/system identifier, timeline or promotion epoch, publication identity, slot
  incarnation, and source frontier;
- storage: `store_id`, `store_generation`, `protocol_version`, `layout_version`, WAL shard count,
  stream lane count, and filesystem UUID;
- Circuits: immutable `stack_namespace`, `ingest_epoch`, and `query_generation`;
- producer: durable producer identity and `producer_epoch`; and
- client: principal, template, store generation, query generation, materialization generation, and
  opaque offset.

These identities are not interchangeable. Persisted records and operational APIs must never use the
unqualified field name `generation`.

### 4.2 Circuits paths

`ELECTRIC_CIRCUITS_DS_NAMESPACE` supplies the immutable `stack_namespace`. It is an operator-owned
identifier, not an instance ID, hostname, endpoint URL, metric tag, or replication-slot name.
Production rejects an empty, malformed, traversal-like, or unexpected namespace before PostgreSQL or
Durable Streams mutation.

For the coupled pilot engine, logical paths map once at the Durable Streams adapter:

```text
meta/catalog
  -> circuits/v1/<stack>/stores/<store-generation>/queries/<query-generation>/meta/catalog

changes/<segment>
  -> circuits/v1/<stack>/stores/<store-generation>/queries/<query-generation>/changes/<segment>

shape/<id>
  -> circuits/v1/<stack>/stores/<store-generation>/queries/<query-generation>/shape/<id>
```

Putting both generations in the physical path prevents an old handle from ever resolving to reused
bytes after a storage reset or query rehydrate. Namespace qualification remains below the logical
`DsClient` semantics so catalogs, sequencers, and lifecycle code continue to use logical paths.

The extracted-ingestor profile instead uses:

```text
circuits/v1/<stack>/stores/<store-generation>/ingest/<ingest-epoch>/meta/catalog
circuits/v1/<stack>/stores/<store-generation>/ingest/<ingest-epoch>/changes/<segment>
circuits/v1/<stack>/stores/<store-generation>/queries/<query-generation>/meta/catalog
circuits/v1/<stack>/stores/<store-generation>/queries/<query-generation>/shape/<id>
circuits/v1/<stack>/stores/<store-generation>/queries/<query-generation>/consumer/checkpoint
```

The ingest catalog contains the PostgreSQL/slot binding, envelope version, segment-rotation state,
and last complete transaction acknowledged by Durable Streams. Query catalogs bind to exactly one
ingest epoch.

Agent streams live outside the Circuits prefix. The product/API contract keeps its logical name while
the gateway qualifies it exactly once with the stack and storage generation:

```text
agent-runs/v1/{run_ref}
  -> /agent-runs/v1/<stack>/stores/<store-generation>/runs/<run-ref>
```

The leading slash denotes the normalized physical HTTP request path. Prefixes prevent accidental
naming collision. Only the access boundary makes them an authorization boundary.

### 4.3 Store and handle binding

Before listener bind, the storage process reads an fsynced on-volume manifest outside the user stream
namespace containing:

```text
store_id
store_generation
protocol_version
layout_version
durability_mode
wal_shard_count
stream_lane_count
filesystem_uuid
creation_time
```

Creating this manifest is permitted only through an explicit first-store or reset operation.
Ordinary startup never invents one. The expected identity comes from independent deployment
configuration; reading an empty catalog is not evidence that an empty store is intended.

A new-volume bootstrap is a separate, explicitly authorized one-shot host operation executed before
ECS registration. Its IaC authorization defaults off in every stack, is enabled only for the exact
volume/store/generation tuple being initialized, and is returned to off before ordinary service
admission. Bootstrap verifies that the selected ext4 data directory contains no prior store,
atomically writes and fsyncs the manifest and its parent directory, and exits without starting a
listener. A reset creates a new `store_generation`; it never overwrites an existing store identity in
place. Ordinary task startup cannot perform either operation.

Each Circuits catalog contains a `StoreBound` event with the storage identity plus
`stack_namespace`, `ingest_epoch`, and `query_generation`. Every public handle is immutable to:

```text
principal
authorized template
stack namespace
store generation
query generation
physical stream id
```

A missing, mismatched, unexpectedly empty, or stale binding refuses readiness. Initializing an empty
store/namespace is an explicit authorized operation completed before slot creation or adoption.

## 5. Durable Streams deployment contract

The inspected source baseline is
[`pgxsinkit/durable-streams-rust@2c1382a`](https://github.com/pgxsinkit/durable-streams-rust/commit/2c1382a32d6962f6fefc513c7c625746e03fc526).
The deployed artifact is built from an exact reviewed commit and pinned by image digest. A branch
name is not a release identity.

The baseline does not currently provide an exclusive data-directory lock, authenticated readiness,
TLS, authentication, or path ACLs. `INTERNAL_PILOT_V1` is blocked until the lock/readiness behavior is
implemented in the server fork and the access boundary is qualified.

### 5.1 Versioned pilot resource profile

The first qualification profile is deliberately small and fixed:

| Setting | `PILOT_AWS_V1` value |
| --- | --- |
| ECS task CPU / memory | 2 vCPU / 4 GiB |
| Dedicated capacity instance | `m7i.xlarge`, one selected AZ/subnet |
| EBS | 100 GiB gp3, 3,000 IOPS, 125 MiB/s, encrypted with the environment KMS key |
| Filesystem | ext4 |
| Runtime worker threads | 2 |
| WAL shards | 2 |
| Stream lanes | 1 |
| WAL segment size | 128 MiB per shard |
| Hard free-space reserve | greater of 10 GiB or 20% of volume |

The AZ, subnet, volume ID, filesystem UUID, and KMS key are exact per-environment IaC inputs. A profile
change to filesystem, WAL shards, lanes, durability mode, protocol/layout version, or binding fields
is a migration or a new store generation, not an ordinary task revision. CPU, memory, IOPS,
throughput, volume size, and reserve may change only in a new versioned resource profile after load
qualification.

The storage command is:

```text
durable-streams-server
  --host 127.0.0.1
  --port 4437
  --durability wal
  --data-dir /data
  --store-id <store-id>
  --store-generation <store-generation>
  --protocol-version 1
  --layout-version 1
  --filesystem-uuid <filesystem-uuid>
  --artifact-digest sha256:<64-lowercase-hex>
  --worker-threads 2
  --wal-shards 2
  --stream-lanes 1
```

The image builds the `telemetry` feature or supplies equivalent bounded metrics from the access
boundary. The `tier` feature remains off.

### 5.2 EBS, mount, singleton, and host lifecycle

- One encrypted gp3 EBS filesystem is mounted on the dedicated EC2 host at
  `/mnt/durable-streams`. The storage data directory is the dedicated child
  `/mnt/durable-streams/data`, which the ECS task bind-mounts to container `/data`. The ext4
  `lost+found` directory therefore remains at the filesystem root, outside the server data directory
  and its strict blank-store/bootstrap checks.
- Delete-on-termination is disabled for the volume. EBS Multi-Attach is disabled.
- The capacity provider is constrained to the volume's AZ.
- Before store construction or WAL recovery, the server acquires a non-blocking exclusive lock on
  `/data/.durable-streams.lock` and holds the file descriptor for the process lifetime. Refusal exits
  non-zero before mutation or listener bind.
- The desired-count-one ECS service uses recreate semantics with
  `minimumHealthyPercent=0` and `maximumHealthyPercent=100`. A controller may not temporarily raise
  desired count. The container entrypoint directly `exec`s the server.
- Scheduler exclusion and the filesystem lock are independent requirements. A qualification test
  starts a second process on the same mount and proves deterministic refusal while the first remains
  unchanged.

The host does not register with ECS, and the task cannot start, until a mount gate verifies:

- expected EBS volume ID and filesystem UUID;
- ext4 and the pinned mount options;
- `/mnt/durable-streams` is a mount point rather than an ordinary root-volume directory;
- `/mnt/durable-streams/data` exists only beneath that verified mount and has the expected ownership
  and permissions;
- the immutable on-volume store manifest; and
- minimum free-byte and inode reserves.

Host replacement is controlled: terminate or fence the former host, perform a normal detach, attach
and mount the exact volume on the replacement in the same AZ, pass the mount gate, then enable task
placement. Force-attach is forbidden in an ordinary replacement. Cross-AZ recovery creates a volume
from a qualified snapshot and executes the restore-frontier decision.

ECS service-managed EBS is excluded because a service task's managed volume is deleted when that
task terminates. A disposable task volume would cause a store-generation reset, invalidate client
offsets, risk a PostgreSQL rehydrate stampede, and lose provisional agent output; it is not an
ordinary restart model.

### 5.3 Access boundary and readiness

The essential access-boundary container listens on private port 8443 with mTLS and reaches the
storage server over loopback. The storage server's port is not present in a security group or service
discovery. Prefix/verb policy is specified in section 11.

After WAL recovery, an authenticated admin-readiness endpoint returns the on-volume manifest plus
recovery state, durable frontier, free bytes/inodes, reserve state, and running artifact digest.
The artifact digest is an ordinary server-start argument and readiness attestation only. Bootstrap
does not accept it and never persists it in the store manifest, so updating server bytes does not
rewrite storage lineage.
`GET /_admin/ready` is available only to the storage-administrator and configured pilot
Circuits-engine identities; it is read-only, bounded, contains no credentials or inventory, and
grants no other admin verb. Inventory remains restricted to storage-administrator/retention
identities. `/health` alone is only liveness. The engine compares the attested identity with its
catalog and PostgreSQL/slot lineage before any mutation. Missing, mismatched, recovering,
unexpectedly empty, or below-reserve storage refuses readiness.

### 5.4 Storage updates and reconnects

A Durable Streams update is a singleton recreate:

1. Deregister the access boundary from new discovery and quiesce administrative mutations.
2. Allow producers and readers to enter bounded retry/reconnect paths.
3. SIGTERM the storage task and drain in-flight requests and WAL committers.
4. Confirm the task is stopped and the data-directory lock is released.
5. Start the new digest on the same mounted volume with the identical layout configuration.
6. Complete WAL recovery, mount/store attestation, and reserve checks.
7. Register private discovery and resume admission.

Storage reads and appends are unavailable during steps 3–6. Clients reconnect with opaque offsets.
Each writer path declares its lost-response strategy: crash-safe DS producer sequencing or semantic
deduplication such as catalog event ID and source `(lsn, seq)`. The current Circuits adapter does not
yet provide generic producer headers, so the deployment must not claim blanket idempotence.

Discovery deregisters before SIGTERM. Clients use bounded exponential retry with jitter, refresh DNS
after connection refusal/reset, and cap connection age so a pooled socket cannot pin a dead task.

### 5.5 Backup and restore

Pilot backup quiesces new handle/control mutations and DS-only producers, drains the engine through a
named `SourceCommitID`, stops Durable Streams, verifies or freezes/unmounts the filesystem, snapshots
the volume, and snapshots the gateway registry or records that all handles will be invalidated.

The backup manifest includes EBS snapshot/volume/KMS/filesystem identities, store binding, namespace
inventory hash, catalog/change/agent tails, PostgreSQL cluster/timeline/publication/slot incarnation,
slot confirmed frontier, source fence, gateway-registry version, and artifact digests. The namespace
inventory hash comes from an authenticated, bounded storage inventory reconciled with Circuits
catalogs and the gateway agent-run registry; backup fails if any source is incomplete or inconsistent.
Snapshot completion is verified before admission resumes.

Restore compares that manifest with current PostgreSQL and slot state before mutation. A DS snapshot
behind the live slot cannot resume the same generation unless missing WAL is independently
recoverable and qualified. Otherwise restore performs an authorized whole-store-generation reset
and invalidates old handles.

For the employee-only pilot, the declared DS-only-output objective is 24-hour RPO and four-hour RTO;
this is not an external SLA. Tighter objectives require more frequent qualified quiesced snapshots or
a separately designed replicated storage profile.

## 6. Private HTTP performance contract

Extracting the ingestor does not add a Durable Streams operation:

```text
embedded today:  ingestor -> HTTP append -> DS -> HTTP read -> engine -> HTTP append -> DS
extracted later: ingestor -> HTTP append -> DS -> HTTP read -> engine -> HTTP append -> DS
```

The extra process boundary changes who performs the existing requests. In the same AZ, persistent
private connections should make transport small relative to WAL group commit and EBS durability, but
that is a hypothesis, not a correctness claim. The access boundary's loopback hop and TLS work are
included in qualification.

The pilot requires direct private discovery, pooled keep-alive connections, bounded body sizes,
explicit pool limits, and distinct connect, ordinary-request, body, long-poll, retry-attempt, and
total-retry deadlines. The versioned deployment manifest also pins maximum connection age, idle
lifetime, and DNS refresh behavior.

Each request carries a trace ID. The client records one monotonic interval from request start through
the relevant response body. The access boundary and server export, for the same trace ID, one
non-overlapping server interval from accepted request body through response-ready plus disjoint proxy
queue, server queue, appender-lock, WAL-stage, and durability-wait spans.

For each matched request:

```text
transport_and_connection_overhead = client_total_duration - server_request_duration
```

Percentiles are calculated from these per-request samples; independently aggregated percentiles are
never subtracted. Results separate reused/new connections and record DNS, connect, TLS, retry, body
size, concurrency, fan-out, task resources, EBS resources, sample count, and histogram precision.

The provisional qualification objective is transport overhead below both 1 ms p99 and 25% of append
p99 at the conservative pilot envelope. It becomes a locked SLO only after two reproducible
production-like runs establish the baseline. The important scale case is many small per-output-stream
appends; testing includes high-fan-out source transactions and quiet, low-concurrency agent streams
where group commit has less opportunity to amortize the EBS barrier.

## 7. Engine replacement in `INTERNAL_PILOT_V1`

The current candidate is not inert: it retries full PostgreSQL setup and can acquire the slot after
the old walsender releases it while the old sequencer/catalog writer is still draining. Slot
ownership therefore does not make a pre-started candidate safe.

The pilot uses stop-completely/start-new replacement:

1. Stop new engine control admission. Commit a named source-fence transaction `F` and record its
   `SourceCommitID`.
2. SIGTERM the old engine. It becomes unready and completes or abandons its current replication unit
   according to the qualified shutdown contract. A final catalog checkpoint is an optimization; if
   absent, the successor replays from the last durable checkpoint.
3. Wait for ECS to report the old task `STOPPED`, then independently confirm its container/PID/cgroup
   no longer exists and cannot execute. Loss of readiness, network contact, walsender, or slot
   ownership is not termination evidence.
4. Only after that evidence exists, start the new task.
5. The new engine verifies the storage and PostgreSQL lineage, acquires the slot, folds the catalog,
   restores state, and catches up.
6. It remains unready until it emits a durable `drainedThrough(F)` receipt covering the named source
   transaction and all deferred output work.
7. Route control traffic only after that receipt.

The current shutdown path logs catalog-drain timeout and can still return success. Before the pilot,
the engine must either make that failure non-zero or expose an explicit incomplete-drain result. The
controller never treats process exit alone as proof of a final checkpoint; replay is the correctness
path.

Durable Streams remains up, so already-issued shape and agent-stream reads continue against durable
data. What pauses is new control admission and PostgreSQL-to-shape freshness. This is not zero
downtime in the freshness/control sense, but it avoids a storage-read outage and prevents overlapping
writers.

A pre-started candidate is permitted only after a fail-closed activation gate exists. Before a
controller-authorized release, the gate prevents `setup_postgres`, slot acquisition, catalog restore,
and every PostgreSQL read or Durable Streams read/mutation. Merely retrying a busy slot is not an
activation gate. Release occurs only after confirmed old-process termination.

## 8. `CONTINUOUS_ENGINE_V1`: extracted ingestor and warm generations

This profile is blocked—not merely unselected—until all of these independently qualified primitives
exist:

1. A storage mutation token enforced for every input, catalog, and output ensure/create, append,
   close, delete, and retention-GC operation. PostgreSQL slot feedback remains governed by the
   ingestor singleton contract: the successor is not started until former-process termination is
   confirmed unless a separately qualified PostgreSQL-side activation/fencing mechanism exists.
2. A durable consumer registry with authoritative lease time, complete-transaction checkpoints, and
   segment-safe reclamation.
3. A PostgreSQL snapshot/change-log fence based on transaction visibility, not LSN comparison alone.
4. A durable demand snapshot and ordered demand-change journal.
5. A generation-router compare-and-swap and explicit public-handle/client-reset protocol.
6. Independent `caughtUpThrough(SourceCommitID)` and `drainedThrough(SourceCommitID)` receipts.

Producer headers, logical-slot ownership, advisory locks, process memory, and gateway routing state do
not substitute for the mutation token.

### 8.1 Stable ingestor

`circuits-ingestor` owns the only walsender and:

- verifies PostgreSQL cluster, timeline, publication, slot, plugin, and store frontier;
- decodes `pgoutput` into versioned canonical envelopes that preserve source transaction identity and
  boundaries;
- spills oversized transactions within a bound;
- appends only complete transactions to the shared input log; and
- advances slot feedback only after the complete transaction is durable.

It does not own queries, shapes, subscriptions, backfills, arrangements, client handles, or output
streams. Its durable ingest catalog records slot binding, ingest epoch, envelope version, segment
rotation, and last complete acknowledged transaction.

```text
PostgreSQL slot
      |
      v
circuits-ingestor
      |
      v
stores/<store-generation>/ingest/<ingest-epoch>/changes/<segment>
      |
      +--------------------------+
      |                          |
      v                          v
query generation A          query generation B
active catalog/outputs      candidate catalog/outputs
      |                          |
      +-------------+------------+
                    v
       authenticated generation router
```

The ingestor is updated less often than query engines. Its own replacement remains singleton and may
briefly pause new input appends while PostgreSQL retains WAL. Envelope changes use N/N-1
expand/contract: readers first, then writer, then old-version retirement after the retention frontier.
Losing the walsender is not ingestor-termination evidence. Ingestor replacement uses the pilot's same
stop-confirm/start rule; no waiting candidate may append input or send slot feedback while the former
ingestor can still execute.

### 8.2 Durable control registry and candidate seeding

A Circuits deployment schema in PostgreSQL, excluded from the source publication, owns query
generation records, authoritative-time consumer leases, the demand snapshot/journal, producer/writer
epoch allocation, and router compare-and-swap. The downstream storage boundary persists and enforces
the selected writer token so delayed requests cannot bypass promotion.

Candidate seeding uses this ordered protocol:

1. Allocate the query generation and durable consumer lease before snapshot work.
2. Persist a checkpoint at a complete-transaction input-log boundary that is still retained.
3. Open the PostgreSQL snapshot and record its xid-visibility `SnapshotGate`.
4. Seed the selected demand from that snapshot while applying the durable demand snapshot and every
   ordered demand-journal change concurrent with seeding.
5. Replay complete input transactions from the retained boundary. Use snapshot xid visibility to
   suppress effects already visible in the seed and apply effects not visible there. LSN comparison
   alone is not a snapshot-membership test.
6. Never checkpoint beyond an incomplete transaction, undrained deferred work, or unacknowledged
   catalog mutation.
7. Declare catch-up only with a durable receipt for a named `SourceCommitID`.

Reclamation considers only non-expired durable consumers and deletes whole segments strictly before
their minimum complete-transaction checkpoint. A candidate that discovers its required boundary is
gone first registers a new retained boundary and only then takes a fresh snapshot. An expired
consumer cannot resurrect behind reclaimed history.

### 8.3 Promotion, handles, and fencing

```text
created -> seeding -> catching_up -> caught_up -> promoted -> draining -> retired
```

The storage boundary maintains one durable generation-authorization record per
`(store_id, stack_namespace, ingest_epoch)`. It contains a shared fence epoch and each query
generation's state and mutation grant. While seeding, A may be `active-writable` and B
`seeding-writable`; each grant is restricted to its own physical paths.

Promotion is a recoverable two-phase transition:

1. PostgreSQL records `promotion_prepared(B, fence_epoch=N+1)`.
2. The storage boundary compare-and-swaps A to `draining-read-only`, B to `active-writable`, and the
   shared fence epoch to `N+1`. This single storage mutation rejects every later A mutation, including
   requests against A's own paths.
3. After that acknowledgement, the router compare-and-swaps new-handle selection to B and PostgreSQL
   records `promoted`.

A crash before step 2 leaves A active. A crash between steps 2 and 3 leaves existing reads available
but new control unavailable until the controller completes promotion forward. Rollback uses another
monotonically higher fence epoch and is permitted only if the selected generation is current at the
named source fence.

A public handle is immutable to its physical stream and generations. Promotion directs only newly
issued handles to the new query generation. Existing handles remain on the old generation for a
bounded lease drain, then return typed `410 generation_expired`. A handle is never rebound while
retaining its old opaque offset.

Stable logical remapping is a later client protocol. It must return `generation_changed`, perform a
complete bootstrap into a new local materialization generation, and atomically replace the old cache
only after bootstrap completion.

Rollback selects only a still-consuming generation proven current at the same named source fence.
Otherwise rollback creates and rehydrates a new query generation.

## 9. In-progress agent streams

Each agent run has an immutable opaque run ID. Each event carries a semantic ID
`(run_id, producer_epoch, sequence)`. Producer epochs are durably and monotonically allocated outside
process memory; a restarted producer receives a strictly higher epoch and starts at sequence zero.
Clients deduplicate semantic IDs and persist the event effect, semantic ID, and opaque DS offset in
one local transaction. Durable Streams producer headers can reduce retries but do not replace
semantic deduplication.

The terminal stream event references the final PostgreSQL record and its `SourceCommitID`. A client
may display Durable Streams output provisionally, but commits the final run state only after its
Circuits materialization has an `appliedTailAfter` receipt for that source commit. The final database
row includes run ID, attempt/producer epoch, and output generation so late events from an older
attempt cannot supersede it.

The producer closes the run stream only after the terminal append is acknowledged. The gateway holds
agent-stream leases on behalf of authenticated clients in its qualified registry using
server-authoritative expiry time. The retention identity may delete a closed stream only after both
`closed_at + 24 hours` and every authenticated lease expiry. A lease cannot be created or renewed
after deletion begins. An expired client receives typed `410 stream_expired` and performs the
declared final-row/bootstrap recovery path. Deletion is never an agent-producer operation.

The first shared-service pilot reserves storage and request capacity for Circuits catalog, change,
and checkpoint traffic before admitting agent writes. Admission enforces per-class bytes, requests,
connections, and append-rate budgets and backpressures agent work before the global control/free-inode
reserve is crossed. If the access boundary cannot enforce these controls, agent and Circuits streams
must use separate Durable Streams services.

Volume loss before final database materialization can lose provisional agent output. This is a second
reason the volume is not a disposable cache.

## 10. Failure and recovery decisions

| Failure | Required outcome |
| --- | --- |
| Circuits engine task | Stop-confirm/start replacement on the same lineage; DS reads remain available but freshness pauses |
| Future ingestor task | Singleton handoff and slot-retained replay; active query generations keep serving durable history |
| Durable Streams task | Recreate on the same verified EBS volume; reconnect after WAL recovery |
| Second DS process on `/data` | Non-blocking filesystem lock refuses before recovery, mutation, or listen |
| EC2 host | Fence former host, normally detach/reattach the exact volume in the same AZ, pass mount gate, recover |
| EBS unavailable/corrupt | Refuse readiness; use only the qualified snapshot/frontier decision |
| AZ loss | Snapshot restore plus explicit whole-stack recovery/reset; no automatic HA claim |
| Empty DS with advanced slot | Refuse silent first boot; require authorized store generation and full client rehydrate |
| Storage pressure | Preserve control/WAL/inode reserve; reject or backpressure new work; never acknowledge an unlanded effect |
| Namespace or lineage mismatch | Refuse before DS or PostgreSQL mutation |
| Old query engine after promotion | Downstream token rejects ensure/create/append/close/delete/GC mutation |
| Input-log consumer exceeds lease | Expire it; never resurrect behind reclaimed segments; require new retained boundary and snapshot |

Mass rehydrate is admitted and jittered per principal/template so recovery does not create an
unbounded PostgreSQL backfill storm. Admission control mitigates load; it is not the correctness
mechanism for a lost generation.

## 11. Security and authorization

Durable Streams binds only to loopback. The access boundary is the sole network-visible storage
endpoint and uses TLS/mTLS service identities. It enforces both verb and prefix:

- a pilot Circuits engine identity can read/mutate only its configured
  `circuits/v1/<stack>/stores/<store-generation>/...` paths; the future multi-generation profile also
  requires its current downstream-enforced writer token;
- a future ingestor identity can mutate only its ingest-epoch paths;
- the authenticated Indexed API/gateway owns exact agent-run assignment and writes on a producer's
  behalf; its `agent-writer` storage identity can ensure, append, and close only
  `/agent-runs/v1/<stack>/stores/<store-generation>/runs/...` paths;
- the gateway has read-only access only to physical streams selected by its authorized handle
  registry; and
- only a storage-administrator/retention identity can enumerate across prefixes or perform
  unrestricted deletion.

Stream paths contain no credentials. Namespace/generation selection is server configuration, not a
client-controlled prefix. Circuits requests use server-owned authorized templates rather than
arbitrary client ASTs.

Security groups provide network admission but are not prefix isolation. If a reviewed DS extension or
access boundary cannot enforce the policy and capacity reserves above, Circuits and agent streams use
separate services.

## 12. Acceptance evidence

Every scenario declares exact artifact digests/configuration, setup lineage, deterministic injection
gate, allowed outcomes, deadline, independent oracle, and retained evidence. Correctness uses this
causal chain:

```text
source transaction + SourceCommitID
  -> server drainedThrough receipt including deferred work
  -> public read/event
  -> client appliedTailAfter receipt scoped to principal/template/store/query/materialization generation
  -> independent SQL/reference result over the same prefix
```

Sleeps, eventual polling, readiness alone, task count, or absence of an error log are not correctness
evidence. Crash-sensitive named cuts run at least 100 deterministic seeded repetitions where
`TST-012` requires it.

### 12.1 `INTERNAL_PILOT_V1`

1. `PILOT-HANDOFF-WALSENDER-RELEASE`: pause the old process after walsender release but before its
   final sequencer/catalog mutation; prove no candidate process exists or can mutate until confirmed
   old-process termination, then prove the successor drains through the named fence.
2. `DS-DATA-DIR-LOCK`: a second process on the same mount refuses before recovery/listen while the
   first keeps serving unchanged.
3. `EBS-MOUNT-GATE`: absent mount, ordinary directory, wrong device, wrong filesystem, and wrong UUID
   all refuse.
4. `STORE-LINEAGE`: empty, old snapshot, wrong ID/generation/layout, PostgreSQL-ahead, and DS-ahead
   cases refuse or execute the one authorized reset.
5. `PREFIX-AUTH`: each service identity attempts every verb against its own and foreign prefixes.
6. `DS-ACK-CRASH`: cuts before WAL reservation, after WAL write, after fsync, and after response
   observation distinguish acknowledged, ambiguous, and unacknowledged appends.
7. `CONTROL-RESERVE`: agent load reaches its quota while catalog/checkpoint traffic remains within
   budget and the emergency reserve remains intact.
8. `HANDLE-GENERATION`: an old offset after reset cannot address a different physical stream.
9. `AGENT-TERMINAL-RACE`: the terminal event arrives before/after the final database row and across a
   producer crash/restart without final-state regression.
10. `AGENT-LEASE-BOUNDARIES`: expiry at `t-1`, `t`, and `t+1`, including renewal racing deletion,
    permits deletion only after the closed-retention boundary and all leases expire.
11. `HTTP-ENVELOPE`: reused/new connections, high fan-out, and quiet streams measure the matched-trace
    transport envelope from section 6.
12. `DS-RECREATE`: the storage service drains, restarts the same volume/digest profile, attests its
    lineage, and resumes Circuits plus agent streams without offset corruption.
13. `COHORT-ROLLBACK`: the employee cohort independently switches sync and agent-delivery providers
    without two providers writing one local materialization generation.

### 12.2 Additional `CONTINUOUS_ENGINE_V1` evidence

1. `SNAPSHOT-XID-BOUNDARIES`: transactions visible, in progress, and begun after the snapshot appear
   exactly once after seed plus replay.
2. `RETENTION-LEASE-BOUNDARIES`: lease at `t-1`, `t`, and `t+1`, a slow valid consumer, an expired
   consumer, and resurrection after reclamation produce only the declared outcomes.
3. `DEMAND-JOURNAL-RACE`: create/release operations concurrent with seeding appear exactly once in
   candidate demand.
4. `PROMOTION-FENCING`: delayed former-engine ensure/append/close/delete/GC operations are rejected by
   the downstream token.
5. `HANDLE-PROMOTION`: old handles drain on old streams and expire with typed 410; no old offset is
   interpreted against the candidate stream.
6. `INGESTOR-COMPATIBILITY`: N/N-1 expand/contract and singleton replacement preserve complete source
   transactions and never advance slot feedback past unlanded input.
7. `PROMOTION-ROLLBACK-CUTS`: every state-machine boundary exposes one complete generation or a typed
   unavailable/reset result, never mixed generations.

## 13. Out of scope

- External-customer cutover in the pilot.
- Active/active Durable Streams, multi-AZ automatic failover, or EBS Multi-Attach.
- Treating S3 cold tiering as replication or WAL durability.
- Ordinary rolling deployment of the singleton engine or Durable Streams task.
- Direct public access to the engine or storage listener.
- Sharing one Circuits namespace between independent stacks.
- Claiming zero freshness/control downtime before an inert engine activation gate exists.
- Implementing `CONTINUOUS_ENGINE_V1` before all six blocking primitives and its acceptance evidence
  close under the canonical production-readiness task graph.

## 14. Relationship to production-readiness work

This design refines rather than replaces these canonical boundaries:

- `DST-001`, `DSR-001`–`003`: owned Durable Streams database, singleton lock, backup, restore frontier,
  and store-generation reset;
- `ENG-010`–`012`, `ENG-015`: disk reserve, namespace-scoped reconciliation, strict configuration,
  and fail-closed catalog boot;
- `LEAD-001`: pilot singleton ownership and future downstream fencing;
- `OPS-001A/B`, `OPS-002`: deployment/storage scaffold and recovery qualification;
- `TST-012`: storage, replacement, former-process, and leadership fault matrix; and
- `MIG-000` onward: common-fence comparison, cohort cutover, rollback, and staged migration.

No implementation or deployment may cite this document alone as evidence that a canonical task or
profile is complete.
