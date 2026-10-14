# ADR-0012: Static per-table ingest columns and durable restart provenance

Status: accepted (owner decision, 2026-10-13).

## Context

Unused multi-megabyte columns multiply new/old envelope, transaction-buffer, spill and replay costs.
A static projection reduces that work, but widening it without DDL leaves dormant streams missing
values for unchanged rows. Physical schema fingerprints cannot distinguish that deployment change.

## Decision

`ELECTRIC_CIRCUITS_PG_COLUMNS` lists qualified tables and column names:
`schema.table:col,col;schema.table:col` (a trailing semicolon is accepted). Library mode logs a
warning that the setting is ignored. A missing table entry means all compiled columns, including
stored generated columns absent from pgoutput. Boot rejects untracked tables, unknown columns
never recorded in a witness, and lists missing any primary-key component before
slot/publication/replica-identity mutations. Each supervised source receives entries for its own
tracked tables; entries tracked by no source refuse startup. After startup, such entries are ignored
with one warning per table so source removals and other reconciliation actions still apply. Schema preflight reads take a
backfill-sized budget permit per configured table. The compiled
engine schema is projected while its physical `SchemaFingerprint` remains intact. Drift refreshes
reapply the setting. Replication removes unlisted new/old cells before buffering; snapshot and
query-back SQL select the same columns. Shape and subset output/predicate references outside that schema return
400 naming the table and column, including subquery inner references.

The additive catalog event is:

```json
{"t":"ingestColumns","eid":"<writer event id>","table":"public.items","columns":["id","name"],"hash":"<sha256>"}
```

Names are sorted and unique. The hash is lowercase hexadecimal SHA-256 of the compact UTF-8 JSON
array of names (`["id","name"]`), without a trailing newline. Malformed records refuse Postgres boot;
the normal catalog `eid` deduplication and last-event semantics apply. No record means all columns,
so legacy catalogs need no migration. Existing fields and events keep their meanings, including
`ShapeRecord.columns` (caller output projection) and audit-only `SchemaChanged`.

Before installing dormant shapes or starting ingest, boot compares each tracked table's effective
set with its last recorded one. Any difference (widening, narrowing or removing a restriction)
retires every dependent shape, including shapes whose subquery reads the table. The existing
retirement path writes durable `Dropped`, closes/deletes the stream, then writes durable `Retired`.
Only after completion does boot write the new `ingestColumns` event durably. Outstanding retirement
intents recovered from the catalog must complete before this witness too. Unchanged recorded sets do nothing.
An explicit list without a witness always records one, even if it names every current column;
that case preserves shapes and prevents a later unlisted ADD COLUMN from appearing to change projection.
After an epoch reset, boot still records changed sets before starting the replacement ingestor.
Legacy comparison uses the unprojected compiled schema, including unpublished stored generated
columns. Catalog validation precedes projection preflight: a missing configured column that appears
in a prior witness was dropped by DDL, so compilation omits it and warns with its table and name.
A missing name never witnessed remains a fatal typo. Historical names are derived from existing
events; retained names are bounded by configuration, with one entry per witnessed table even after
its setting is removed. This distinction survives the next witness narrowing the effective set.
Runtime drift writes a changed effective set only for a configured or previously witnessed table,
after dependent retirements and before schema installation; post-DDL shapes then retain their streams
at restart. Other drift, including ADD/DROP COLUMN with neither a setting nor a witness, queues failed
stream retirements without waiting or writing a witness, preserving ingest progress on other tables
(ADR-0007).
No event format changes are needed.

## Consequences

- Clients observe deleted streams / Electric 409 and recreate from a fresh snapshot; widening cannot
  replay an incomplete old projection into a retained dormant stream. Ordinary unchanged restart
  semantics remain, including the existing retirement of subquery shapes whose inner state is absent.
- A crash before `Dropped` repeats the retirement; after `Dropped`, boot retries unfinished deletion;
  after `Retired` but before the column event, boot records the new set without resurrecting shapes.
  A persisted column event proves retirement completed. Storage outages delay boot under the existing
  writer/retirement policies. No new task, queue, change-log/envelope or spill format is introduced.
- Existing budgeted catalog pages and backfill frames retain their permits. Reconciliation holds no
  body permit or engine-state lock during durability waits. Retained schema/provenance metadata,
  arrangements and result state remain outside the transient byte ledger.
- PostgreSQL still publishes full rows and supplies full wire tuples. This is engine-side projection,
  not publication column-list support. Unlisted data may remain in old log segments until retention.
- External change-log consumers (ADR-0010) receive the projected new **and** old rows too. A column-set
  change at restart is not signalled to pinned consumers, and their pins are not evicted. Consumers
  can encounter mixed-width envelopes across that boundary and must coordinate column settings
  with the deployment; shape retirement does not provide a consumer reset signal.
- A binary predating this event cannot enforce projection provenance. Rolling back to such a binary
  against a projected catalog is unsupported; use an isolated fresh namespace and fresh shapes.
