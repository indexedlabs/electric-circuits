# Deploying electric-circuits with Postgres

electric-circuits can run **Postgres as the system of record**: your application writes to Postgres
normally, and electric-circuits keeps every declared shape (a filtered view of a table) live by ingesting
changes from Postgres **logical replication** and reading rows back from Postgres for backfill. There
is no separate write API and no in-memory table copy to keep in sync — Postgres is the source of truth.

```
  app ──writes──▶  Postgres  ──logical replication──▶  engine  ──append──▶  durable-streams
                     ▲                                   │                    (shape/<id>)
                     └──────────── backfill SELECT ──────┘                         │
                                                                                   ▼
                                                                          client (live rows)
```

## What you need

- **PostgreSQL 18.x** with logical decoding (the built-in `pgoutput` plugin — no extensions to
  install). Managed Postgres works if it allows `wal_level = logical` and a logical replication slot
  (RDS, Cloud SQL, Supabase, Neon, etc. all do).
- **A durable-streams server** (the transport/persistence layer). Set its base URL in
  `ELECTRIC_CIRCUITS_DS_URL`.
- **The engine binary** (`apps/engine`, Rust): `cargo build -p electric-circuits-engine --release` →
  `target/release/electric-circuits-engine`.

## Step 1 — Configure Postgres

Logical replication must be on. In `postgresql.conf` (or your provider's parameter group):

```conf
wal_level = logical
max_replication_slots = 10     # ≥ number of engine instances
max_wal_senders = 10
```

Then restart Postgres (the `wal_level` change requires a restart).

The engine sets everything else up for you on startup, per configured table:

- `ALTER TABLE <t> REPLICA IDENTITY FULL` — so an UPDATE/DELETE carries the **full old row** (needed to
  compute the exact delta). The role you connect with must own the tables (or be superuser) for this.
- `pg_create_logical_replication_slot('<slot>', 'pgoutput')` + `CREATE PUBLICATION <slot>_pub FOR
  ALL TABLES` (superuser) — the replication slot and its publication, created once
  and reused.

> **Each table needs a single-column primary key.** The engine introspects columns, types, and the pk
> from the catalog; composite primary keys are not supported.

> **One slot per engine instance.** Replication-slot names are unique across the whole Postgres
> instance. If you run more than one engine against the same database, give each a distinct
> `ELECTRIC_CIRCUITS_PG_SLOT`.

## Step 2 — Run the engine

Point it at Postgres, list the tables to watch, and give it the durable-streams URL:

```sh
export ELECTRIC_CIRCUITS_DS_URL="https://streams.internal:8080"
export ELECTRIC_CIRCUITS_PG_URL="postgres://user:pass@db.internal:5432/appdb?sslmode=verify-full"
export ELECTRIC_CIRCUITS_PG_TLS_CA_BUNDLE="/run/secrets/postgres/ca.pem"
export ELECTRIC_CIRCUITS_PG_TABLES="users,projects,reporting.tasks"
export ELECTRIC_CIRCUITS_BIND="0.0.0.0:9000"

./electric-circuits-engine
```

On startup the engine sets `REPLICA IDENTITY FULL` on each table, introspects it, ensures the slot, starts
the replication ingestor, and begins serving the control API on `ELECTRIC_CIRCUITS_BIND`. It prints
`ENGINE_LISTENING <addr>` once ready.

### Configuration reference

| Variable                  | Required | Default          | Meaning |
|---------------------------|:--------:|------------------|---------|
| `ELECTRIC_CIRCUITS_DS_URL`    | yes      | —                | durable-streams base URL. |
| `ELECTRIC_CIRCUITS_PG_URL`    | yes¹     | —                | Postgres connection string. Setting it enables Postgres mode. Nonlocal hosts require `sslmode=verify-full`; plaintext is accepted only for local development hosts. |
| `ELECTRIC_CIRCUITS_PG_TLS_CA_BUNDLE` | for nonlocal PG | — | Absolute path to a PEM CA bundle used by both ordinary queries and logical replication. The engine image includes the AWS RDS us-east-1 roots at `/etc/ssl/certs/aws-rds-us-east-1-bundle.pem`. |
| `ELECTRIC_CIRCUITS_PG_TLS_SERVER_NAME` | no | URL host | Optional DNS name used for certificate verification and SNI when the URL host is an IP address. |
| `ELECTRIC_CIRCUITS_PG_TABLES` | yes¹     | (empty)          | Comma-separated tables to watch: `schema.name`, a bare `name` (= `public.<name>`), or `schema.*` / `*` for every table with a primary key in that schema. `*` and an empty setting both mean `public.*` — never every schema. |
| `ELECTRIC_CIRCUITS_PG_SLOT`   | no       | `electric_circuits`  | Logical replication slot name (unique per engine). |
| `ELECTRIC_CIRCUITS_PG_POLL_MS`| no       | —                | Legacy; accepted but unused (the ingestor streams pgoutput, push delivery). |
| `ELECTRIC_CIRCUITS_BIND`      | no       | `127.0.0.1:0`    | Address for the control/HTTP API. |
| `ELECTRIC_CIRCUITS_LOG`       | no       | `info`           | Log filter (`error`, `warn`, `info`, `debug`). |
| `ELECTRIC_CIRCUITS_LOG_JSON`  | no       | `1`              | Emit structured JSON logs so CloudWatch subscription consumers preserve memory fields. Set `0` for text. |
| `ELECTRIC_CIRCUITS_MEMORY_LOG_PERIOD_SECS` | no | `5` | Cheap process/cgroup/cardinality memory snapshot interval; `0` disables. |
| `ELECTRIC_CIRCUITS_MEMORY_BYTES_LOG_PERIOD_SECS` | no | `30` | Expensive owned-heap breakdown interval; `0` disables byte walks. |
| `OTEL_SERVICE_NAME` | no | unset | OTel resource service name used by the OTLP metrics exporter. |
| `OTEL_RESOURCE_ATTRIBUTES` | no | unset | Comma-separated OTel resource attributes, such as `deployment.environment=prod`. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | no       | unset            | Enables the engine's optional OTLP HTTP/protobuf metrics reader alongside its local Prometheus reader. |
| `OTEL_EXPORTER_OTLP_HEADERS` | no       | unset            | Comma-separated OTLP HTTP headers; provide via a secret, never in a checked-in manifest. |
| `ELECTRIC_CIRCUITS_RESET_ON_SLOT_LOSS` | no | `true`      | Policy when the replication slot can no longer be trusted: `true` retires every shape and starts a new epoch; `false` refuses (fail-closed) until `POST /epoch/reset`. See "Losing the replication slot" below. |
| `ELECTRIC_CIRCUITS_CHANGES_SEGMENT_BYTES` | no | `1073741824` | Change-log segment size before rotation (`0` disables the size criterion). See "Change-log disk" below. |
| `ELECTRIC_CIRCUITS_CHANGES_SEGMENT_SECS` | no | `86400` | Change-log segment age before rotation (`0` disables the age criterion). |
| `ELECTRIC_CIRCUITS_CHANGES_RETAIN_SECS` | no | `604800` | How long a rotated-out segment may stay pinned by a dormant shape before that shape is evicted and the segment deleted (`0` = pin forever). |
| `ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS` | no | `21600` | Dormancy idle threshold (6 hours; `0` disables dormancy). |
| `ELECTRIC_CIRCUITS_SUBSCRIPTION_LEASE_SECS` | no | `1800` | Native subscription lease window (30 minutes; `0` disables lease expiry). |
| `ELECTRIC_CIRCUITS_DS_READ_MAX_BYTES` | no | `16777216` / `67108864`, floored at `ELECTRIC_CIRCUITS_CHANGES_APPEND_BYTES` | Client-side Durable Streams response cap. 16 MiB (four times the 4 MiB server page) when readiness advertises a page within it; 64 MiB when it advertises none, because such a store returns the whole remainder in one response and a smaller cap stalls the live loop instead of bounding it. Neither derived value is ever below the append budget (64 MiB by default): the store frames one append whole on read, so the engine must be able to read back what it writes. A value larger than the cap against a store whose `max_value_bytes` exceeds it raises the cap toward `ELECTRIC_CIRCUITS_DS_READ_MAX_CEILING_BYTES` and retries; it is never truncated. An explicit value always wins, and one below the append budget is refused at boot. |
| `ELECTRIC_CIRCUITS_DS_CONNECT_TIMEOUT_SECS` | no | `10` | Durable Streams connect deadline. |
| `ELECTRIC_CIRCUITS_DS_READ_TIMEOUT_SECS` | no | `30` | Deadline for a bounded Durable Streams request (replay page, HEAD, append), applied both at the transport and around the replay read. |
| `ELECTRIC_CIRCUITS_DS_LIVE_READ_TIMEOUT_SECS` | no | `45` | Deadline for a long-poll read. Keep it above the store's `long_poll_timeout_ms` (35 s deployed) or every idle read errors before the store's own empty answer. |
| `ELECTRIC_CIRCUITS_DS_REQUEST_TIMEOUT_SECS` | no | `60` | Whole-request deadline for bounded requests; a long-poll gets `max(this, live read + 15s)`. |
| `ELECTRIC_CIRCUITS_DS_READ_MAX_CEILING_BYTES` | no | `536870912` | Hard ceiling the read cap may be raised to. An uncapped store's oversized read doubles the cap and retries up to here; an advertised-page store that breaks its promise, an explicit cap, or a breach past this ceiling latches the engine degraded. |
| `ELECTRIC_CIRCUITS_REQUIRE_DS_CHUNK_CAP` | no | unset | `1` refuses the boot when readiness does not advertise a `max_chunk_bytes` within the client cap. Unset warns once and boots, because the store advertises no such field today. |
| `ELECTRIC_CIRCUITS_REACTIVATION_CONCURRENCY` | no | `2` | Maximum concurrent dormant replay scans. |
| `ELECTRIC_CIRCUITS_REACTIVATION_JOIN_TIMEOUT_SECS` | no | `20` | Bound on how long one request waits for an in-flight reactivation. Keep it under the gateway's read timeout (30s today); past it the request returns the typed recreate outcome while the replay continues. `0` = wait forever. |
| `ELECTRIC_CIRCUITS_REPLAY_MIN_BYTES` | no | `16777216` | Minimum replay span budget (16 MiB). |
| `ELECTRIC_CIRCUITS_PENDING_BUFFER_MAX_BYTES` | no | `67108864` | Per-shape ceiling for the deltas buffered while a shape is being created or woken. Past it the shape is retired and the client recreates it (`pending_buffer_overflows`). `0` = no ceiling. |
| `ELECTRIC_CIRCUITS_REPLAY_MULTIPLIER` | no | `4` | Multiplier for replay admission against recorded backfill bytes. |
| `ELECTRIC_CIRCUITS_TXN_MEMORY_BYTES` | no | `134217728` | In-memory bytes of ONE transaction (the changes actually held: inline size plus owned heap, not the size they would serialize to) before the ingestor spills the rest to disk (`0` = never spill). See "Large transactions" below. |
| `ELECTRIC_CIRCUITS_CHANGES_APPEND_BYTES` | no | `67108864` | Byte budget for one append when a large commit is appended in chunks. Must be > 0 and ≤ the durable-streams 1 GiB body cap — outside that, the engine refuses to boot. |
| `ELECTRIC_CIRCUITS_TXN_SPILL_DIR` | no | `<temp dir>/circuits-txn-spill-<uid>` | Where a spilled transaction's temporary file goes (created 0700, files 0600). Must have room for your largest transaction, must be writable at boot, and must not be shared between engines. |
| `ELECTRIC_CIRCUITS_BACKFILL_APPEND_BYTES` | no | `16777216` | Byte budget for one backfill append. A backfill is streamed and appended chunk by chunk, so engine memory per backfill is one chunk. Must be > 0 and ≤ the durable-streams 1 GiB body cap. See "Backfills" below. |
| `ELECTRIC_CIRCUITS_BACKFILL_STATEMENT_TIMEOUT_MS` | no | `0` (off) | `SET LOCAL statement_timeout` inside the backfill transaction. A timeout fails **that** shape creation with a clear error; nothing else is affected. |
| `ELECTRIC_CIRCUITS_SHUTDOWN_GRACE_SECS` | no | `25` | How long a graceful shutdown may take before it is forced (exit `70`). Keep it below your `terminationGracePeriodSeconds`. A catalog append still being retried through a storage outage counts as work in flight (party `catalog writer`). |
| `ELECTRIC_CIRCUITS_SHUTDOWN_DRAIN_SECS` | no | `2` | How long the port stays open after `SIGTERM` answering `/ready` with 503, so a load balancer drains first. Comes out of the grace, must be less than it; `0` = stop accepting at once. |

¹ Omit `ELECTRIC_CIRCUITS_PG_URL` to run in library/no-source mode (shapes start empty; used by tests).

## Step 3 — Connect the client

The client subscribes to shapes over the engine's API and materializes them with TanStack DB.
Writes go to **Postgres**, not the client:

```ts
import { createClient } from '@electric-circuits/client'

const client = createClient({ apiUrl: 'http://engine.internal:9000', schema })

// Declare a shape; rows stay live as Postgres changes.
const activeUsers = await client.shape({
  table: 'users',
  where: { col: 'active', op: 'eq', value: true },
})

activeUsers.subscribe((rows) => render(rows))

// To change data, write to Postgres however you already do (psql, your ORM, etc.):
//   UPDATE users SET active = false WHERE id = 42;
// electric-circuits picks it up via logical replication and updates the shape.
```

## Step 4 — Run it under Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: electric-circuits-engine
spec:
  # ONE replica. The engine binds a logical replication slot, and Postgres allows exactly one
  # walsender per slot — a second pod does not share the work, it waits. It is not a cold standby
  # either: it holds no state until it gets the slot. During a rolling update the new pod boots,
  # finds the slot `busy` (its predecessor's walsender is still attached), and parks in its
  # reconnect backoff answering `GET /ready` with 503 until the old pod's slot is released; then it
  # restores the durable catalog and takes over. Give each engine its OWN slot
  # (`ELECTRIC_CIRCUITS_PG_SLOT`) if you genuinely want more than one — they are independent
  # engines, not replicas of each other.
  replicas: 1
  strategy:
    type: Recreate # or RollingUpdate with maxSurge: 1 — the new pod waits on `busy` either way
  selector:
    matchLabels: { app: electric-circuits-engine }
  template:
    metadata:
      labels: { app: electric-circuits-engine }
    spec:
      # 25s of engine grace (the default) + headroom for the kubelet.
      terminationGracePeriodSeconds: 30
      containers:
        - name: engine
          image: electric-circuits-engine:latest
          ports:
            - { name: http, containerPort: 3000 }
          env:
            - { name: ELECTRIC_CIRCUITS_DS_URL, value: "http://durable-streams:8080" }
            - { name: ELECTRIC_CIRCUITS_PG_TABLES, value: "*" }
            - { name: ELECTRIC_CIRCUITS_BIND, value: "0.0.0.0:3000" }
            - name: ELECTRIC_CIRCUITS_PG_URL
              valueFrom: { secretKeyRef: { name: engine-db, key: url } }
          # LIVENESS: "is the process alive". Never fails for a condition a restart does not fix —
          # not for a database that is still coming up, and not for a broken epoch (which needs an
          # operator's POST /epoch/reset, and which a restart would only make harder to recover).
          livenessProbe:
            httpGet: { path: /health, port: http }
            periodSeconds: 10
            failureThreshold: 3
          # READINESS: "should this pod get traffic". 200 only when Postgres is connected, the slot
          # is verified, the catalog is restored and the ingestor is running — and 503 the instant a
          # SIGTERM lands, which is what drains the Service endpoint before the port closes.
          #
          # periodSeconds x failureThreshold is the WORST-CASE time to go unready, and it must fit
          # inside ELECTRIC_CIRCUITS_SHUTDOWN_DRAIN_SECS (default 2s) or the port closes while the
          # probe still believes the pod is ready. 1x1 = 1s fits; the default 2x2 = 4s would not.
          readinessProbe:
            httpGet: { path: /ready, port: http }
            periodSeconds: 1
            failureThreshold: 1
          # NO startupProbe, deliberately. One on /ready would restart a pod whose only problem is
          # that its database has not come up yet — which is the exact case the engine is built to
          # wait through ("retry forever; a restart buys nothing"). Nothing needs one either:
          # liveness is /health, which answers 200 from the moment the process is up, so a slow boot
          # is never cut short. If you want a bounded "give up and page someone", do it with an
          # alert on `engine_replication_slot_active == 0` or on the pod's readiness age — not with
          # a probe that restarts.
```

**Drain window vs probe period.** The engine keeps its port open for
`ELECTRIC_CIRCUITS_SHUTDOWN_DRAIN_SECS` (2 s) after the signal, answering `/ready` with 503, which is
what a `preStop: sleep` is usually there to buy — so no `preStop` hook is needed **provided the
window is at least your readiness `periodSeconds` × `failureThreshold`**. Raise it if your probe is
slower (the snippet above instead makes the probe fast, 1 s × 1).

Two caveats worth knowing rather than discovering:

- For a **Service**, Kubernetes removes a `Terminating` pod's endpoint independently of its readiness
  probe, so a Service's traffic drains even if the probe never gets a chance to fail. That masks a
  mismatched drain window — it does not fix it.
- For anything polling `/ready` **itself** — an external load balancer, a gateway with its own health
  checks, a service mesh with passive health checking — there is no such masking, and the window is
  the only thing keeping it from sending a request into a closing socket. Size it against that
  poller, not against the Service.

## Operating notes

### Opt-in managed writer ownership schema

Managed blue/green ownership is a pilot-only, opt-in engine capability. The Indexed migration, not
the engine, provisions this relation before any capable revision starts; normal engine boot must
never create, alter, or grant it.

```sql
CREATE SCHEMA IF NOT EXISTS electric_circuits;
CREATE TABLE electric_circuits.writer_ownership (
  coordination_key char(64) PRIMARY KEY,
  generation bigint NOT NULL CHECK (generation >= 1),
  owner_revision text NOT NULL CHECK (length(owner_revision) BETWEEN 1 AND 255),
  phase text NOT NULL CHECK (phase IN ('active', 'quiesced')),
  handoff_id uuid,
  source_commit_id uuid,
  updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  CHECK (phase = 'active' OR (handoff_id IS NOT NULL AND source_commit_id IS NOT NULL))
);
GRANT USAGE ON SCHEMA electric_circuits TO electric_circuits_engine;
GRANT SELECT, INSERT, UPDATE ON electric_circuits.writer_ownership TO electric_circuits_engine;
```

The migration must exclude this table from the engine's logical publication. A managed engine
starts with control admission closed, including a restart of the persisted active revision. The
controller may explicitly reopen it only after `/ready` is `active`; this is deliberate
fail-closed recovery. A running second process with the same revision is fenced before readiness by
the logical slot claim, while all public traffic and data/control mutations require full readiness.

### Managed blue/green pilot

The managed writer-ownership capability is opt-in. Its PostgreSQL schema and grants are provisioned
by the deployment migration, never by engine boot. Install the first managed-capable revision through
the existing stop-confirm-start procedure with an explicit immutable revision and
`ELECTRIC_CIRCUITS_MANAGED_DEPLOYMENT_INITIAL_ACTIVE=1`; every subsequent revision starts standby
until the authenticated controller proves the source receipt, quiesces the current owner, confirms
slot release, and promotes the successor generation. `/health` remains liveness-only so ECS can
reach its scale-up lifecycle hook; `/ready` and public data admission stay closed until the exact
active revision has restored and started ingest. This document does not authorize production use:
the lifecycle controller, ECS/ALB topology, and failure-matrix qualification remain separate gates.

The quiesce operation is deliberately two-step when admission is open: its first call closes and
drains admission and returns retryable 503. The controller then obtains a fresh source receipt and
repeats quiesce with the same expected identities. A durable receipt that predates the closure is
also rejected with retryable 503: it cannot fence a mutation already admitted before the closure.
The engine snapshots an in-process receipt sequence when close-and-drain completes, and accepts
only a receipt recorded strictly after that snapshot. Repeating close is idempotent and preserves
the snapshot; after restart, managed admission is fail-closed and the controller must close then
record a fresh fence again. A 409 remains reserved for an ownership CAS conflict and makes no
ownership-row mutation.

Promotion requests additionally include `successorRevision`; it is required to exactly match the
immutable revision configured on the receiving process. A quiescing or shutting-down incumbent
therefore cannot replay a promote request and reclaim the row for itself. The successor alone may
perform the idempotent exact-generation promotion.

- **Adding a table:** add it to `ELECTRIC_CIRCUITS_PG_TABLES` and restart the engine. It will set
  replica identity on the new table and introspect it at startup.
- **Change-log disk is bounded by segment size/age and the retain window.** Every committed change
  rides one ordered log on the durable-streams server, which the engine rotates into segments
  (`changes/0`, `changes/1`, …) by `ELECTRIC_CIRCUITS_CHANGES_SEGMENT_BYTES` (1 GiB) or
  `ELECTRIC_CIRCUITS_CHANGES_SEGMENT_SECS` (1 day), whichever comes first, and deletes once the
  engine has consumed past them. Steady-state usage is therefore roughly **one segment budget plus
  whatever a dormant shape still pins**: a shape that went dormant a while ago holds every segment
  from its resume point on, which is capped by `ELECTRIC_CIRCUITS_CHANGES_RETAIN_SECS` (7 days) —
  past that the shape is evicted (clients re-subscribe and backfill) and the segments go. Size the
  durable-streams volume for `segment budget × (retain window ÷ rotation interval)` in the worst
  case, plus your shape streams. Setting both rotation criteria to `0` disables rotation entirely and
  the log grows without bound. `GET /metrics` reports `changes_rotations_total`,
  `changes_segments_deleted_total` and the `changes_segments_retained` gauge.
- **Large transactions spill to disk, they do not blow up the ingestor's memory.** A transaction is
  only appendable once its commit arrives, so the ingestor has to hold it — and a bulk
  `UPDATE`/`DELETE` under `REPLICA IDENTITY FULL` carries the old *and* new row for every change. Past
  `ELECTRIC_CIRCUITS_TXN_MEMORY_BYTES` (128 MiB of held changes) the buffer is written to one
  temporary file in `ELECTRIC_CIRCUITS_TXN_SPILL_DIR` and memory is released, so peak **ingestor**
  memory is that cap plus one append chunk whatever the transaction's size. That bound is the
  ingestor's alone: downstream, the sequencer reads and applies the transaction as one unit, so its
  read page and pending appends still scale with the transaction — size the pod for the largest
  transaction you expect to sync, not just for this knob. **Size the spill directory for the largest
  single transaction your database can produce** — the file holds that transaction's changes as JSON,
  roughly the size of the rows involved (doubled for updates and deletes, which carry the prior row
  too), and it exists only between that transaction's `BEGIN` and its commit. The directory defaults
  to a private `<temp dir>/circuits-txn-spill-<uid>` (0700, files 0600), is **probed at boot** — an
  unwritable one refuses the boot rather than failing every large commit — and must not be shared
  between engines (leftovers are identified by pid, which means nothing across containers): give each
  engine its own. The commit is then appended in chunks of at most
  `ELECTRIC_CIRCUITS_CHANGES_APPEND_BYTES`, and the replication slot is acknowledged only after the
  last chunk lands, so an interruption re-delivers the whole transaction rather than losing part of
  it; subscribers still see the transaction as one unit, never chunk by chunk. Nothing is invalidated
  or dropped for being large. If the engine dies mid-transaction the file is left behind; the next
  boot sweeps it. `GET /metrics` reports `txn_spills_total`, `txn_spill_bytes` and
  `txn_chunked_appends_total`.
- **`SIGTERM` drains, it does not kill.** On the signal the engine turns `GET /ready` into
  `503 {"status":"shutting_down"}` and keeps the port open for
  `ELECTRIC_CIRCUITS_SHUTDOWN_DRAIN_SECS` (2 s) so your load balancer takes the pod out of rotation;
  then it stops accepting, releases every parked `/v1/shape` long-poll at once (rather than holding
  the termination grace for their full ~20 s window), lets the ingestor finish the transaction it is
  *appending*, lets the sequencer finish its batch and write a final checkpoint, drains that
  checkpoint to durable-streams, and exits `0`. The ingestor's position is recorded **locally**: the
  acknowledgement Postgres sees rides the replication client's 1 s status interval and is not forced
  on the way out, so a shutdown never advances the slot and the last second's commits are
  re-delivered on the next boot and de-duplicated there. New `live=true` requests arriving during
  the drain are answered `503` + `Retry-After: 1` so clients back off to the successor instead of
  spinning. Shape streams are **never** closed or deleted on
  shutdown — the restored shape continues its stream, so a restart costs clients nothing. The whole
  sequence is bounded by `ELECTRIC_CIRCUITS_SHUTDOWN_GRACE_SECS` (25 s); a second signal, or the
  grace running out, exits `70` immediately (nothing is corrupted: an unacknowledged commit is
  re-delivered and the previous checkpoint stands). Keep `terminationGracePeriodSeconds` above the
  grace.
- **Liveness and readiness are different probes.** `GET /health` is liveness — `200 ok` while the
  process runs, and it must never be what restarts a pod. `GET /ready` is readiness — 200 only when
  Postgres is connected, the slot is verified, the catalog is restored and the ingestor is running;
  otherwise 503 with the reason word (`waiting`, `starting`, `degraded`, `shutting_down`). Point
  Kubernetes at those two, not at `/v1/health` (which exists for Electric-fleet parity and answers
  202 while booting).
- **A boot failure is either fatal or retryable, and the engine says which.** Bad credentials, a
  missing privilege, an unknown database, `wal_level` ≠ `logical` (checked explicitly at connect —
  it needs a Postgres *restart* to fix), a `ELECTRIC_CIRCUITS_PG_URL` the driver cannot parse (caught
  while resolving the configuration, before the port is bound), an unusable
  `ELECTRIC_CIRCUITS_PG_TABLES`, a publication with a column list, or a durable catalog it could not
  read: the engine names the problem and exits **`78`** immediately, because retrying would only
  repeat it. Every refused setting exits `78`, whether it was caught before or after the log
  subscriber existed — the reason is always on stderr. **Durable-streams gets the same treatment as
  Postgres:** a refused connection, a timeout or a 5xx from the storage server is retryable
  (`durable-streams is unreachable` in the log, `/ready` = `waiting`), because storage coming up
  after its engine is ordinary; a malformed catalog or an unusable `ELECTRIC_CIRCUITS_DS_URL` is
  fatal. A connection attempt that *hangs* — a firewalled host, a stale Service IP — is cut off after
  10 s and retried rather than silently wedging the boot. A connection refused, a DNS failure, a
  timeout, or "the database system is starting up": it backs off 1 s → 30 s with jitter and keeps
  trying **forever**, answering `/ready` with `503 waiting` and logging every attempt. Its HTTP port
  is open the whole time, on purpose — a readiness probe you cannot reach is no probe at all — so a
  database that comes up after its engine is a non-event. Alert on the pod restarting with `78`;
  ignore `waiting`.
- **Backfills are streamed; a wide table does not spike engine memory.** Creating a shape reads its
  rows off a `REPEATABLE READ` cursor and appends them to the (not yet live) shape stream in chunks
  of at most `ELECTRIC_CIRCUITS_BACKFILL_APPEND_BYTES` (16 MiB), so the engine holds one chunk
  regardless of the table's size — where before it read every matching row into memory first.
  Aggregates fold each chunk and drop the rows; a subquery inner-set node's seed is the one thing
  still collected whole, because that set is the state the node maintains. If a backfill can pin a
  Postgres snapshot for longer than you are willing to tolerate (a `REPEATABLE READ` transaction
  holds back vacuum), set `ELECTRIC_CIRCUITS_BACKFILL_STATEMENT_TIMEOUT_MS`: a timeout fails **that**
  shape creation with `canceling statement due to statement timeout` and nothing else — no shape is
  retired, nothing is purged, and the client may retry. It is off by default. `GET /metrics` reports
  `backfill_chunked_appends_total`.
- **Replication slot lag:** an engine that is stopped for a long time holds its slot, and Postgres
  retains WAL for it. If you decommission an engine, drop its slot:
  `SELECT pg_drop_replication_slot('<slot>');` The engine measures both numbers for you every ~10 s
  on a pooled connection and publishes them on `GET /metrics` and `GET /metrics/prometheus` (with or
  without StatsD): `replication_slot_retained_wal_bytes` (`pg_current_wal_lsn() - restart_lsn` — the
  WAL the source database is holding on disk for this engine, i.e. what fills its volume) and
  `replication_confirmed_flush_lag_bytes` (`pg_current_wal_lsn() - confirmed_flush_lsn` — ingest
  lag). `replication_slot_active` is `1` while a walsender holds the slot. Alert on the first one
  against `max_slot_wal_keep_size`.
- **Losing the replication slot costs a full resync — and the engine says so.** The engine records
  which slot, in which cluster (`pg_control_system().system_identifier`), it is bound to, and checks
  that binding before *every* connection. Things that break it in practice:
  `max_slot_wal_keep_size` reclaiming the WAL the slot needed (`pg_replication_slots.wal_status`
  goes to `lost`); restoring the database from a backup or a snapshot (new `system_identifier`,
  and usually no slot — slots are not part of a `pg_dump` and do not survive a PITR); a **major
  version upgrade** (`pg_upgrade` does not carry logical slots across); and an operator or cleanup
  script dropping it. There is no way to recover the changes that happened in the gap, so every
  shape built on that slot is wrong.

  With the default `ELECTRIC_CIRCUITS_RESET_ON_SLOT_LOSS=true` the engine **resets**: it retires
  every shape (each stream closed, then deleted — clients see the same `stream-closed`/404 they see
  for an eviction and re-subscribe), creates a fresh slot under the same name, records the new epoch,
  and resumes. Expect a backfill storm proportional to your live shape count, and note that a table
  with a counts pipeline additionally restarts the process (exit `75`). With
  `ELECTRIC_CIRCUITS_RESET_ON_SLOT_LOSS=false` it **refuses**: ingest stops, `/v1/health` reports
  `degraded` (503) and every shape route answers 503, `GET /replication/lsn` names the reason
  (`epoch.state = "broken"`, `epoch.reason` = `slot_lost` | `slot_wal_lost` |
  `system_identifier_mismatch`), and nothing is torn down until you post `/epoch/reset` — which then
  performs exactly the reset above. Pick `false` when an unscheduled resync is worse than an outage
  (and alert on `epoch_breaks_total`); pick the default when an unattended deployment must heal
  itself. Either way, budget `max_slot_wal_keep_size` for your worst expected engine downtime.
- **A durable-streams outage at boot stops the engine, deliberately.** The epoch is decided from the
  durable catalog, so a catalog the engine cannot read is refused rather than guessed at — booting on
  would look like "no epoch has ever been claimed", create a slot at the current WAL head, and leave
  every shape already in the log undropped and short of the gap. Restore durable-streams and restart.
- **A durable-streams outage while running makes creates WAIT; it never makes them lie.** A shape is
  acknowledged only once its `Created` record has reached the catalog (the same for a join), and a
  catalog append that fails transiently — a refused connection, a timeout, a 5xx — is retried in
  place, forever, at 100 ms → 5 s. So `POST /shapes` hangs for the duration of the outage instead of
  returning a handle a restart would forget; use your client's own timeout, and expect
  `catalog_append_retries_total` to climb with `shape_appends` flat. If the client gives up and the
  record lands anyway, the shape simply has no subscriber and the retention sweeper evicts it.
  **Deletes are not affected**: `DELETE /shapes/{id}` (and `?purge=true`) answers as soon as the
  engine state is updated, and its record lands behind it — a delete that waited would time out and
  be retried, and a repeated delete decrements a shared refcount twice. Two outcomes are NOT this: a
  definite refusal from storage (a 4xx, once the catalog stream exists) exits the process `74`,
  because the engine's memory and its durable record have diverged and only a re-fold at boot
  reconciles them; and a `SIGTERM` during an outage names the `catalog writer` party and exits `70`
  when the grace runs out.
- **A shape stream never outlives its shape, even across a crash.** Removing a shape writes its
  `Dropped` record BEFORE it retires the stream and a `Retired` record only after storage accepted
  the delete, so a retirement storage refused is retried in the background (500 ms → 5 s) and, if the
  process dies first, re-queued by the next boot straight from the catalog. `GET /shapes/{id}` is 404
  from the moment the record goes, whether or not its stream has finished going. Watch the
  `retirements_pending` gauge: non-zero means public stream URLs are outliving their shapes right
  now, and it should return to 0 on its own.
- **Consistency:** on shape registration the engine takes a `REPEATABLE READ` snapshot of the
  matching rows (the backfill) and, atomically with it, captures the snapshot's
  `pg_current_snapshot()` — the **snapshot gate**. Each replicated change is stamped with its
  transaction's **commit LSN, xid, and in-transaction position**, and the engine skips a change iff
  its xid was **visible to the backfill snapshot** (already in the seed); everything else is taken
  from the live stream. Visibility — not WAL position — is the fence because a commit's WAL record
  exists before the transaction becomes snapshot-visible; an LSN-only comparison would drop rows in
  that window. Ingest delivery is at-least-once (append, then advance the slot), and the engine
  de-duplicates by `(commit LSN, position)`, so each change takes effect exactly once. This assumes a
  single ingestor per database (the model above). Running multiple ingestors over the same tables is
  not supported.
- **Migrations cost one resync of that table's shapes.** The engine compares every table's compiled
  schema with what Postgres reports — on each pgoutput `Relation` message (which Postgres re-sends
  after DDL) and on a background reconciler tick (`ELECTRIC_CIRCUITS_SCHEMA_RECONCILE_SECS`, default
  60 s, for DDL that no write follows, and for primary-key changes, which the replication stream
  cannot describe). On any difference — a column added, dropped, retyped or reordered, a changed
  primary key, or a `REPLICA IDENTITY` reset from `FULL` (which the engine re-asserts) — it
  re-introspects the table and **retires every shape that depends on it**: the streams are closed and
  deleted, and clients re-subscribe and backfill through the new schema. Rows already on a stream can
  never gain a new column, so this is enforced rather than hoped for. `TRUNCATE` retires the same
  shapes (the engine holds no row copy from which to synthesise the deletes). Other tables' shapes
  are untouched. A shape creation that overlapped the migration is refused with
  `schema of <t> changed during creation; retry` rather than being served over the old schema.
  One exception: a table with a counts pipeline (`ELECTRIC_CIRCUITS_DBSP_COUNTS`) has no runtime
  circuit rebuild, so the engine additionally exits (code `75`) to be restarted once the retirements
  have landed — plan migrations *and truncates* on those tables like a rolling restart.
- **Migrating while the engine is stopped works too.** Every shape record stores the fingerprint its
  table had when the shape was created; at boot the engine re-introspects and retires any shape whose
  table moved while it was down, so a restart after a migration is a resync of the affected tables,
  never a silent resume over the old schema.
- **When a migration cannot be settled, the table is parked, not guessed at.** If the engine cannot
  re-introspect (Postgres unreachable) or cannot get the `ACCESS EXCLUSIVE` lock to restore
  `REPLICA IDENTITY FULL` within 5 s, the table becomes **unresolved**: its shapes are retired, its
  changes are dropped, and creates on it are refused with
  `schema of <t> is unresolved after a change; retry later`. A per-table retry task keeps working on
  it (2 s → 30 s backoff) and un-parks it on the first successful re-introspection. Watch
  `GET /tables` (each table's `unresolved` flag) and the `schema_unresolved_total` counter.
- **The publication must deliver whole rows.** A `Relation` message that describes fewer columns than
  the table has can never be reconciled with its schema, so the engine **refuses to boot** if its
  publication has a per-table column list, naming the tables. Stored generated columns follow
  `pg_publication.pubgencols` (PG18+): the engine includes them in a table's schema fingerprint
  exactly when the publication publishes them. The engine's own `<slot>_pub` is `FOR ALL TABLES` and
  satisfies both; point it at a hand-made publication only if it does too.
- **Dropping a table is one-way until a restart.** A table dropped from Postgres has its shapes
  retired and is untracked; re-creating it under the same name does not resume syncing — restart the
  engine, exactly as when adding a table.
- **Permissions:** the engine's Postgres role needs `SELECT` on the watched tables, ownership (for
  `ALTER TABLE … REPLICA IDENTITY`), and the `REPLICATION` attribute (to create/read the slot).
- **Introspection surface:** the engine's control port also serves the pipeline-visualizer backend —
  `/trace` (per-envelope SSE), `/graph`, `/state`, and the `/state/node` deep dumps, which expose row
  data. These endpoints are **unauthenticated**; anyone who can reach the control port can read them.
  The runtime cost is near zero while nobody subscribes (instrumentation is gated on subscriber
  count), so leaving them on is fine when the port is private. If the control port is reachable
  beyond your trust boundary, either front it with network policy or disable the surface outright
  with `ELECTRIC_CIRCUITS_TRACE=0` (the routes are then never registered).
