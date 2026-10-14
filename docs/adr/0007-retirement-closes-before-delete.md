# Engine-initiated retirement of a shape stream closes it before deleting it

Status: accepted (2026-08-21)

When the engine retires a `shape/*` stream — eviction, purge, schema drift, epoch reset — it first
appends with `Stream-Closed: true`, so a tailing long-poll is released immediately with
`stream-closed` and "closed" unambiguously means "the engine retired this shape; re-subscribe", and
only then deletes the stream. Closing is terminal, so it is never applied to a dormant shape (its
retained stream must stay appendable for reactivation) or on engine shutdown (restored shapes continue
their streams). Clients **must** treat `stream-closed`, 404 and 410 alike: re-subscribe. A final control
envelope naming the reason was considered and rejected — the client would make no different decision.

## Consequences

Retirement is **mandatory, not best-effort**, and the engine has already forgotten the shape by the
time it runs — so a storage failure there used to leave the public stream URL open forever, serving
rows Postgres no longer contains, with nothing left anywhere that remembered it should not exist.
Retirement is therefore written down in two halves and retried to completion:

- `Dropped { id }` is the durable **intent**, written before the retirement is attempted, at every
  site (purge, eviction, drift/`TRUNCATE`, the epoch reset, the catalog restore's drops, a rolled-back
  create); `Retired { id }` is the durable **completion**, written only once storage accepted the
  delete (404/410 count — deletion is idempotent).
- A failed retirement goes to a background **retirement queue** (`engine/retirement.rs`) that retries
  it with backoff (500 ms → 5 s) until it lands. Ordinary drift does not wait on the queue, including
  ADD/DROP COLUMN on a table with no ingest-column setting or witness, so other tables keep ingesting.
  The shape is gone from the engine either way, and `GET /shapes/{id}` stays 404 while its stream is
  still being retired. A changed effective column set on a configured or previously witnessed table
  waits for completion before its durable witness ([ADR-0012](0012-static-ingest-columns.md)).
- Every boot enqueues each `Dropped` the catalog fold could not match with a `Retired`. That closes
  the crash window (the in-memory queue is lost at exit and costs nothing, because the intent is
  durable) and doubles as the orphan-`shape/*` GC — bounded by the catalog rather than by a storage
  listing, which durable-streams does not offer.
- `retirements_pending` (gauge) and `retirement_retries_total` are the operator-visible form: a
  non-zero gauge means stream URLs are outliving their shapes right now, and it returns to 0 on its
  own.

The exception stays an exception: a rolled-back create still uses a plain `delete_stream` (no
subscriber ever saw that stream, so there is nothing to signal). Only if that delete FAILS does the
queue take over, which closes before deleting — a harmless extra round trip on a stream nobody read.
