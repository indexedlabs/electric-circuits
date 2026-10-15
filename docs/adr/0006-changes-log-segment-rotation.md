# The change log is rotated into segments by the engine; a segment is deleted once nothing can resume inside it

Status: accepted (2026-08-21)

Since the sequencer architecture, every committed change to every tracked table is appended to one
global `changes` stream forever; the durable-streams server offers whole-stream TTL but no prefix
trimming, so a long-running deployment fills its disk. We rotate engine-side. At a transaction
boundary, by bytes or age, the ingestor appends a final `{control: "rotated", next: "changes/<n+1>"}`
envelope to segment *n*, **closes** it, and continues in *n+1*; the sequencer's live read is released
with `stream-closed` and follows the pointer, and a dormant shape's resume offset becomes
`(segment, offset)` and replays across the pointer the same way. A segment is deleted when the
sequencer's checkpoint is past it and no dormant shape resumes inside it; a dormant shape that would
pin a segment beyond the retention window is evicted first. The catalog records `ChangesRotated`.

## Considered options

- Prefix trimming as a durable-streams primitive (upstream issue #12's plan): cleaner offsets, but a
  change to another upstream. The low-watermark computation is the same, so it can replace rotation
  later without touching the policy.

## Consequences

Disk is bounded by the segment budget times the number of segments anything still needs:
`ELECTRIC_CIRCUITS_CHANGES_SEGMENT_BYTES` (default 1 GiB) and
`ELECTRIC_CIRCUITS_CHANGES_SEGMENT_SECS` (default 86400) decide when to rotate (`0` disables that
criterion), and `ELECTRIC_CIRCUITS_CHANGES_RETAIN_SECS` (default 604800, the dormancy TTL's default)
is how long a rotated-out segment may stay pinned by a dormant shape. The rotation pointer is a
control envelope — `type: "__circuits.control"`; the `__circuits` schema is reserved, so no tracked
table can produce that spelling — and every reader drops control envelopes **by type,
unconditionally**, never by position. Position would not be safe: if the close after the pointer
fails, the rotation is retried at the next commit, so a segment can carry commits *after* a pointer
and end up with two. Readers cross only on `closed` **and** drained, which makes an abandoned
pointer inert. Deletion and eviction interlock in one direction only: a dormant shape whose resume segment
was rotated out longer ago than the retain window is **evicted first** (the ordinary close-then-delete
path, ADR-0007), and only the segments that eviction unpinned are then deleted — an eviction that
fails defers its segment to the next sweep rather than deleting a segment something could still
resume inside. Every change-log position becomes `(segment, offset)`; a catalog holding a bare offset
predates this ADR and is refused at boot rather than coerced. The deletion floor is the **durable**
checkpoint (the last `Offset` that reached storage), not the sequencer's in-memory position, so a
crash can never leave a boot resuming inside a segment a sweep has deleted — and a boot whose
restored position names a segment storage does not have refuses to start rather than spin on a 404.
A reader leaving a closed segment steps to exactly the next one, never to the first open one: the
segments in between are unread changes.

The writer rejects any `Offset` below its restored/last durable floor, logging both positions.
It checks at dequeue, and serializes boot floor seeding with checkpoint appends; queued periodic
or shutdown checkpoints cannot move the floor backwards. Equal positions may update their paired
de-duplication highwater. Folding an old catalog logs an error for a regressing offset but preserves
the last position/highwater pair: choosing an earlier record would be an unproven repair. A missing
resume segment therefore still refuses boot.

The same rules apply to a supported external reader: it drops `__circuits.control` by type, holds
until `last`, deduplicates by `(lsn, seq)`, and crosses only after the closed segment is drained.
Its checkpoint also includes the immutable query generation returned by `GET /changes/position`; it
presents that value as `generation=` on reads. A mismatch is a named `410 Gone`
(`stale-generation`), not a 404: the reader must rebuild from state instead of treating an old
store/query generation as an absent segment in the current one.
