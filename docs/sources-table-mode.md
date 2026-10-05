# Sources-table mode

Sources-table mode lets one engine host serve multiple PostgreSQL sources. The host owns no
source registry: the rows in the configured control relation are the complete desired set. Each
source has its own engine, replication slot, worker thread, and storage directory.

## Configuration

`ELECTRIC_CIRCUITS_SOURCES_MODE` is opt-in. Without it, the existing single-source boot path and
its `ELECTRIC_CIRCUITS_PG_*` settings are unchanged. Set it to `table` for PostgreSQL control
tables or `file` for local development.

| Setting | Default | Meaning |
| --- | --- | --- |
| `ELECTRIC_CIRCUITS_SOURCES_MODE` | — | `table` or `file`. |
| `ELECTRIC_CIRCUITS_SOURCES_PG_URL` | — | Required in `table` mode; the control database URL. |
| `ELECTRIC_CIRCUITS_SOURCES_TABLE` | `circuits_sources` | Control row table; a simple or schema-qualified identifier. |
| `ELECTRIC_CIRCUITS_SOURCES_VERSION_TABLE` | `circuits_sources_version` | Single-row revision table. |
| `ELECTRIC_CIRCUITS_SOURCES_POLL_SECS` | `30` | Version-row poll interval. Must be positive. |
| `ELECTRIC_CIRCUITS_SOURCES_FILE` | — | Required in `file` mode; a JSON array containing source rows. |
| `ELECTRIC_CIRCUITS_SOURCES_STORAGE_DIR` | `./data/sources` | Root for `<source_id>/` storage, DBSP state, and transaction spill. |

In `table` mode, `ELECTRIC_CIRCUITS_PG_URL`, `ELECTRIC_CIRCUITS_PG_SLOT`, and
`ELECTRIC_CIRCUITS_PG_TABLES` are rejected. `ELECTRIC_CIRCUITS_BIND`, secrets, Durable Streams, DBSP, transaction,
backfill, and shutdown settings apply to every source.

## Control rows

The engine reads, but never creates or alters, these relations:

```sql
CREATE TABLE circuits_sources (
  source_id       TEXT PRIMARY KEY,
  plugin          TEXT NOT NULL,
  database_secret TEXT NOT NULL,
  slot            TEXT NOT NULL,
  publication     TEXT NOT NULL,
  tables          TEXT[] NOT NULL,
  revision        BIGINT NOT NULL,
  updated_at      TIMESTAMPTZ NOT NULL
);

CREATE TABLE circuits_sources_version (revision BIGINT NOT NULL);
```

`tables` entries are schema-qualified, for example `public.thread_messages`. A file-mode row uses
the same field names and JSON types. `plugin` names the consumer's package that owns the source (recorded in status; decoding always uses `pgoutput`); `publication` must be the
slot's `<slot>_pub` publication. A row contains a secret reference, never a connection URL or
password. `source_id` must be a single safe filesystem path component: not empty, not `.` or `..`,
and without `/`, `\`, control characters, or other path separators. An unsafe id makes only that
source not ready.

## Database-secret resolution

The `database_secret` prefix selects the resolver:

- `env:NAME` reads and validates environment variable `NAME`.
- `file:/absolute/path` reads, trims, and validates the file contents.
- `aws-sm:NAME` reads the current `SecretString` from AWS Secrets Manager using the ambient
  credential chain and environment-selected region.

Every source start and restart resolves its secret again. Unknown prefixes, missing values,
malformed URLs, URL-shaped secret fields, and failed AWS lookups make only that source not ready.
The public `error` field is a fixed classification plus the resolver prefix only, for example
`resolve failed: env variable missing` or `resolve failed: aws-sm lookup error`. It never includes
the secret name, the resolved value, or an underlying error string.

## HTTP routes

The host exposes:

- `GET /health` for liveness.
- `GET /ready`, which becomes `200` after the first successful discovery fetch, even if one or
  more source rows are not ready.
- `GET /sources`, returning `{source_id, revision, ready, error}` summaries.
- `GET /sources/{source_id}/status`, returning the summary plus the source's changes route,
  epoch, position, segments, consumers, and readiness fields.
- Every engine route at `/sources/{source_id}/...`, forwarded to that source after rewriting the
  URI back to the engine root. The engine router is intentionally not nested. Operator/admin
  paths are not forwarded: `/sources/{source_id}/_admin/...` and
  `/sources/{source_id}/epoch/reset` return 404. There is no source-scoped admin surface.
- `POST /admin/refresh`, protected by the private control secret. It accepts no body, fetches rows
  unconditionally, reconciles them, and returns `{ "revision": ... }`.

There is no write API for sources and no source-specific admin route. Host-level `/admin/refresh`
is the only admin route in this mode. The existing `/_admin/*` deployment routes and
`POST /epoch/reset` remain single-source-only and return 404 under a source prefix.

## Discovery and reconciliation

The host fetches the full row set before binding its listener. If the control database is
unreachable, it retries with backoff. A table-mode poll reads only the one version row; unchanged
revision means no other control-table query. A changed revision or explicit refresh fetches all
rows. File mode rereads its file on each poll and refresh.

Reconciliation is idempotent:

- a new row starts one source;
- a missing row stops it;
- a changed row revision stops and restarts it;
- an unchanged healthy row does nothing.

A failed start is retained as a not-ready source with its classified error and retried on every poll
tick in both modes. The poll interval is the retry interval. When the table version is unchanged,
retries use the desired row already held in memory without fetching the sources table again. A
changed discovery snapshot is reconciled before retries, so deleted rows are not restarted. Manual
refresh still retries immediately. Unchanged healthy rows remain no-ops; one failure does not stop
other sources.
Stopping leaves that source's storage directory in place, so a restart can restore its shape catalog.

Each source runs on its own operating-system thread with a current-thread Tokio runtime. The
control plane uses one serialized reconcile lock, so concurrent refresh calls cannot interleave
plans. The poll task is owned and joined on shutdown. Once the host shutdown token is set, control
I/O and reconciliation short-circuit and no new source is started.

## Engine ownership and stop

Both hosting modes use `Engine::close`: begin shutdown, wait for registered parties, release
Electric handle subscriptions and drain the catalog, stop and join the membership/counts circuits,
close the Postgres pool, end and join remaining Engine tasks, fail unfinished retirement completions,
and clear the DS reconciler callback. Source workers also close an Engine whose boot fails or is
cancelled before dropping their runtime. The standalone binary uses the same lifecycle on shutdown.

Each Engine owns its Postgres pool, settle record and poller, publication-generated-column setting,
DS read-cap state, Electric handle registry and evictor, and background tasks. This includes work
started by an HTTP handler on the host runtime. Pools are distinct even when source URLs match.
The existing process-wide metrics and settle statistics remain aggregated; per-source observability
is separate work. The statistics collector keeps only weak references to pools.

Electric handles belong to the Engine that minted them. A foreign or pre-restart handle receives
`409 must-refetch`. Idle eviction releases the handle's own subscription on its own Engine. Plain
`offset=-1` recovery snapshots also create handles; clients that never resume them are cleaned up by
the same TTL. `ttl_registry_heap_bytes` measures only the serving Engine's registry.

Membership spill settings are resolved through `Config`. Sources use `<source_root>/subq`, overriding
any host-wide explicit membership directory; the circuit cache is removed when its joined thread
exits. The configured standalone membership directory is likewise an Engine-owned disposable cache.
The source storage root, including retained storage, stays on disk across stops and row deletion;
removing an obsolete root remains an operator action.

Catalog refusal (standalone exit 74) and counts-circuit rebuild (standalone exit 75) notify the
supervisor to stop and restart only the affected source, without a source-row revision change.
The supervisor serializes this with discovery reconciliation. If that start fails, the same
failed-source poll retry policy applies. The standalone binary retains its process exit codes.

Authentication secrets, environment-derived TTL/deadline knobs, pool capacity policy, backfill
settings, and the StatsD transport remain host-wide. Source database URLs, slots, publications,
tables, and storage paths remain source-specific. `PostgresSetup::ExternallyManaged` means the
consumer's migration/bootstrap step owns publications, slots, replica identity, and grants; the
Engine verifies them.

## Shape gateway authentication

The gateway sends `ELECTRIC_SECRET` as `Authorization: Bearer <gateway-secret>`
on `/sources/{source_id}/v1/shape`. The Electric-compatible `secret` and
`api_secret` query parameters remain supported. Missing or incorrect credentials
return 401 before table lookup. The distinct controller secret cannot authorize
shape reads; browser session credentials stay at the gateway.
