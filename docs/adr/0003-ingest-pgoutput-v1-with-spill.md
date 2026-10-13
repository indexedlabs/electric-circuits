# Ingest stays on pgoutput protocol v1; large transactions spill receiver-side; protocol v2 is deferred

Status: accepted (2026-08-21)

Upstream issue #17 proposes pgoutput protocol v2+ (streaming of in-progress transactions) as the real
fix for unbounded memory on large transactions. We decline for now. The engine's invariant is that one
commit reaches the change log as one atomic unit in commit order, and a streamed transaction still
cannot be appended until its commit frame arrives (commit LSN unknown, aborts possible) — so the
receiver buffers or spills under v2 exactly as under v1, while v2 additionally needs a fork of the
replication client (`pgwire-replication` hardcodes `proto_version '1'` and has no streaming option),
decoding of the stream frames and xid-prefixed DML, and interleaved in-flight transactions. Postgres
already spills its reorder buffer server-side past `logical_decoding_work_mem`; only the receiver was
ever at risk.

Instead: a per-transaction in-memory byte cap; past it the transaction's envelopes spill to a local
temporary file and are streamed back at `Commit`; the single append becomes chunked appends, each well
under the durable-streams body cap; the slot is acknowledged only after the last chunk; and the
sequencer's `(lsn, seq)` de-duplication makes re-delivery after a mid-chunk crash safe. Transaction
size never invalidates a shape.

Revisit v2 when large transactions measurably delay small ones — a latency reason, not a memory one.

## Consequences

Peak **ingestor** memory is bounded by `ELECTRIC_CIRCUITS_TXN_MEMORY_BYTES` (default 134217728,
128 MiB; `0` never spills) plus one chunk — the chunk held parsed, plus its serialized body while
the POST is in flight. That is a bound on the ingestor, not on the engine: the sequencer's read
page, the run it holds (see below) and its per-transaction `txn_pending` are bounded by a
transaction's size, not by this knob. The cap is measured on what is actually held (inline size plus
owned heap, the engine's `HeapSize` estimate), so nothing is serialized on the way in and an
ordinary commit costs exactly what it did before this ADR. Past the cap the buffer is serialized out
to one newline-delimited-JSON file under `ELECTRIC_CIRCUITS_TXN_SPILL_DIR` (default a private 0700
`<temp dir>/circuits-txn-spill-<uid>`; files are `O_EXCL`, mode 0600), memory is released, and every
further change of that transaction is written straight to the file; that directory therefore needs
room for the largest transaction the database can produce, and is probed for writability at boot —
an unusable one refuses the boot rather than failing every large commit. The file is scratch, never
state: it is removed at commit, at abort, and when the replication connection is torn down, and a
leftover from a process that died mid-transaction is swept at the next boot by pid liveness. Pids
are only meaningful **within one pid namespace**, so a spill directory must belong to exactly one
engine — two containers sharing one (a `hostPath` temp mount) is unsupported.

At `Commit` the transaction is streamed back out — in order, stamped with `(lsn, txid, seq)`, `seq`
running contiguously `0..n` across chunk boundaries — and appended in chunks of at most
`ELECTRIC_CIRCUITS_CHANGES_APPEND_BYTES` (default 67108864, 64 MiB; refused at boot if above the
durable-streams 1 GiB body cap). **The slot is acknowledged, `last_lsn` published and the drain
barrier's sentinel released only after the LAST chunk has landed**: a failure on any chunk tears the
connection down unacknowledged, Postgres re-delivers the whole transaction, and the sequencer's
`(lsn, seq)` de-duplication discards the chunks that already landed. Rotation stays a
transaction-boundary decision, so a segment never splits a commit by policy.

Runtime and source/deployment drain receipts also wait for the complete transaction and its direct
appends, then for pending subquery propagation and emission batches whose originating commit LSN is
at or before the marker's LSN. A later commit's pending work does not delay an earlier receipt.
Propagation passes its origin to nested work, create replay and ordered stream appends; an unknown
origin conservatively holds every marker. This changes neither the ingest acknowledgement boundary
nor the aggregate `pendingFlips` convergence metric. Shutdown and degradation still refuse receipts.

**Chunking must not become visible as several transactions.** Durable-streams exposes each append
atomically, so a reader long-polling the segment tail sees chunk 1 on its own; splitting a page into
transactions by `(txid, lsn)` alone would fan that chunk out and flush it to the shape streams as if
it were a whole commit. The ingestor therefore marks the **last envelope of every transaction**
(`headers.last`, on single-chunk commits too, so "no marker" always means "incomplete"), library-mode
writers mark their one-envelope writes, and the sequencer **holds** a trailing run that no marker
terminates: the held envelopes are carried into the next read and the transaction is processed and
flushed only once the marker arrives.

Re-delivery folds into the hold, carefully. An interrupted chunked commit is acknowledged nowhere, so
Postgres re-sends it from its start; and because acknowledgements are flushed on an interval, a
reconnect can re-send earlier **complete** commits ahead of it. The "already held, skip it" filter
(`seq` greater than the last one held) therefore applies to the **leading run of the page, and only
while that run is the held transaction** — `seq` is the running index over one transaction, so a
page-wide filter would silently and permanently drop acknowledged transactions whose seqs are lower.
When the held transaction is not what came next at all (that reconnect, or an epoch reset abandoning
it), the fragment is discarded: it arrives again in full, and any commit already applied is skipped by
the highwater.

While a run is held, nothing is published past the page it began in — not `processed`, not the
checkpoint, not the segment-deletion floor, and not the resume position a shape going dormant is
parked at (which would otherwise leave it permanently missing that transaction). A page that
completes one held run and starts another re-pins to **its own** page, so a catch-up over consecutive
chunked commits does not freeze the checkpoint at the first of them. The dormant-shape replay
(`replay_changes_for_shape`) does not hold: it filters by table and appends absolute per-pk rows, so a
partly-appended commit is a prefix of the same absolute rows.

The checkpoint carries the de-duplication highwater with the position (`Offset { pos, highwater }`),
and is written whenever **either** moves — the highwater advances on transactions completed before a
hold, while the position is pinned. It has to: a crash can leave a prefix of a chunked commit applied
and checkpointed while the rest is re-delivered, and aggregate and subquery contributor weights are
not idempotent under duplicates.

`GET /metrics` reports `txn_spills_total`, `txn_spill_bytes` (cumulative bytes ever spilled) and
`txn_chunked_appends_total` (the chunk appends made by commits too large for one append; an ordinary
commit contributes 0).
