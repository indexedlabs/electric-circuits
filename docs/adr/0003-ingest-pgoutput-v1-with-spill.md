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

Process byte admission adds a shared reservation ledger across ingest, sequencer input, boot catalog
pages, backfills and Electric folds. `ELECTRIC_CIRCUITS_MEMORY_BUDGET_BYTES` enables admission only
when set to a positive byte count; unset or `0` disables it with one boot log, regardless of cgroup
limits. It accounts only transient input, so the default stays off until a profiled boot peak shows
which stage needs bounding. Cgroup counters remain in memory snapshot logs. Held spill (its own
default 64 MiB cap) and ingest spill apply independently of admission. Oversized requests wait
for an empty ledger, and permit/waiter owners release on cancellation. Byte reservations are not an
RSS ceiling: retained state, output batches, protocol buffers and allocator slack are additional.

An enabled ledger makes held input spill before another page is read, even below the held cap. A
page that carries a receipt or continues a held transaction spills all remaining input before
processing; other complete pages consume their original reservation directly. Page permits follow
input across blocking jobs and are released only after consumption/spill. Reader workspace permits
end before receipt waits. Scratch writers are flushed/closed before those waits. All files retain
the existing NDJSON format, replay pins, highwater rules and cleanup ownership.

An ingestor initially reserves the smaller of its cap plus append workspace and one quarter of the
process budget (at least one byte). Drain workspace is bounded by actual buffered bytes and the append
cap, plus one envelope. Admission after a schema message restores the original reservation floor,
not the last wire frame's size. A larger workspace grows the existing permit only when capacity
fits immediately and nobody is queued; it never waits while retaining credit. Otherwise a capped
ingestor spills before waiting for a larger reservation or pool-dependent schema work. The initial
share is not a spill threshold: an under-cap commit with available capacity stays in memory.
This prevents a backfill waiting for memory while holding a pool slot from blocking the ingestor
that owns the needed memory. The documented `0 = never spill` mode retains rows outside the ledger;
its input/drain workspace uses ordinary FIFO requests, without forcing exclusivity.

Backfill readers and Electric materialization/key folds reserve per frame/page, sized to the append
budget or DS read cap. They release credit after decoding/consumption and before requesting more.
A larger indivisible backfill frame drops the smaller permit before admitting its actual wire size.
Only a single request larger than the process budget requires exclusivity; fitting requests coexist.
Retained fold maps, decoded backfill rows, collected results and response conversion stay unaccounted,
like arrangement state. Snapshot SQL, xid fences, pool ownership and results are unchanged. Every fold page checks
lifecycle/degradation before admission, after releasing the previous permit. Waiting ingest never advances applied LSN, and acknowledgement still
follows the last durable append. Non-live DS reads acquire before HTTP send; long-polls wait for
data without credits and consume that response if admission is immediate. Only a real admission
wait discards it and re-reads the same offset with a fresh HTTP deadline. The sequencer preserves
its pending read/admission across ordinary commands; explicit pause and shutdown cancel it.

The process gauges are `engine_memory_budget_bytes` and `engine_memory_permits_held_bytes`; the
counter is `engine_memory_permit_waits_total`. Source endpoints observe the same process totals.
This is a transient admission policy, not a new catalog/change-log format or recovery authority.

Peak **ingestor** memory is bounded by `ELECTRIC_CIRCUITS_TXN_MEMORY_BYTES` (default 134217728,
128 MiB; `0` never spills) plus one chunk — the chunk held parsed, plus its serialized body while
the POST is in flight. That is a bound on the ingestor, not on the engine: the sequencer's read
page and per-transaction `txn_pending` are not bounded by this knob; held input has its own cap below. The cap is measured on what is actually held (inline size plus
owned heap, the engine's `HeapSize` estimate), so nothing is serialized on the way in and an
ordinary commit costs exactly what it did before this ADR. Past the cap the buffer is serialized out
to one newline-delimited-JSON file under `ELECTRIC_CIRCUITS_TXN_SPILL_DIR` (default a private 0700
`<temp dir>/circuits-txn-spill-<uid>`; files are `O_EXCL`, mode 0600), memory is released, and every
further change of that transaction is written straight to the file; that directory therefore needs
room for concurrent ingest and sequencer held spills, and is probed for writability at boot —
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

When process byte admission is disabled, the held run has a separate envelope-memory cap, `ELECTRIC_CIRCUITS_HELD_MEMORY_BYTES` (67108864,
64 MiB; `0` spills immediately). Strictly past it, input moves to a unique
`circuits-held-<uuid>-<pid>.ndjson` file in the same spill directory, using the ingestor's NDJSON
format **without restamping**. Further held input goes straight to that file. After the end marker,
the sequencer reads bounded chunks rather than reconstructing a transaction vector; chunk boundaries
never flush partial output. Per-transaction output and arrangement batches remain proportional to the
transaction; the current read page, one oversized envelope, decode storage and 1 MiB I/O buffers are
additional to the held-input cap. This is an input bound, not a total engine memory budget.

One awaited blocking operation per sequencer owns spill I/O. The scratch owner removes the file after
processing/receipt completion, on orphan or epoch-fragment discard, and on shutdown. A cancelled
blocking operation drops its owner when it finishes. Read/write/parse/truncation failures stop
sequencing at the replay boundary without publishing the failed transaction's highwater. Stale held
files join the ingest boot sweep; active held owners are registered so a later ingestor/source boot
cannot remove them. The directory is required and probed even when ingest spilling is disabled.
No durable format or recovery authority changes: a restart replays the change log, not scratch.

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
commit contributes 0). It also reports `sequencer_held_bytes` (estimated in-memory input, excluding
disk/I/O buffers) and `sequencer_held_spills_total` (transactions spilled). Prometheus exports
`engine_sequencer_held_bytes` and `engine_sequencer_held_spills_total`; counter reset preserves the gauge.

### Static ingest projection (ADR-0012)

A deployment may restrict each tracked table with `ELECTRIC_CIRCUITS_PG_COLUMNS`. Both new and old
images lose unlisted columns before transaction buffering, spilling and append sizing; all primary
keys remain. This reduces retained/log bytes but does not reduce PostgreSQL's full-row wire frames.
The NDJSON scratch format, change envelopes, transaction markers, acknowledgement and held-run rules
remain unchanged. Backfills/query-backs use the same projected schema. A changed effective set retires
dependent streams at boot, durably before its additive catalog witness and before ingest starts;
clients recreate from fresh snapshots. Physical schema fingerprints remain full-width. See
[ADR-0012](0012-static-ingest-columns.md).
