# ADR-0011: Dormant reactivation is bounded

## Decision

Dormant plain shapes may be reactivated by replaying the change log, but every Durable Streams
read is page-bounded and the engine caps concurrent replays with
`ELECTRIC_CIRCUITS_REACTIVATION_CONCURRENCY` (default `2`). Replay decoding filters by envelope
type before materializing row bodies. `changes_only` feeds are exempt from dormancy: recreating one
from a Postgres snapshot would silently discard the changes-only history promised to subscribers.

The replay span budget is configurable with `ELECTRIC_CIRCUITS_REPLAY_MIN_BYTES` (default 16 MiB)
and `ELECTRIC_CIRCUITS_REPLAY_MULTIPLIER` (default 4). Plain-shape catalog records persist the
last streamed backfill's row and byte estimates (missing values from older catalogs mean unknown).
At wake, the engine computes the byte span across retained segment HEADs and replays only when
`span <= max(min_bytes, multiplier * backfill_bytes)`; unknown sizing uses `min_bytes` alone.
Over-budget or unresumable shapes are retired through the closed-stream path so subscribers
recreate them from a fresh snapshot. The retention sweeper applies the same admission check before
pressure/TTL eviction.

The retirement is typed (`ReactivationRecreate`), and each surface answers it in its own recreate
vocabulary rather than as a server fault:

* **Native create/join** (`POST /v1/shapes`, `POST /v1/subset-feeds`): the request falls through to
  a fresh create in the same round trip and returns `2xx` carrying a NEW shape id. This is the only
  outcome today's iOS SDK recreates from on its own — it treats a create/join answer whose shape id
  differs from the one it asked for as a replacement and reseeds.
* **Native read routes** (`/shapes/{id}/rows`, `/shapes/{id}/log`, and a create whose fall-through
  is exhausted): `410 Gone`. `410` is what that SDK's stream reader already maps to "gone, get a
  fresh one"; `409` it classifies as a terminal failure with no reseed, and `404` is ambiguous with
  an unknown shape id.
* **Electric `/v1/shape`**: the protocol's own `must-refetch` (`409` plus the control message), the
  same answer a genuinely gone stream produces.

A wake that is admitted can still outlast the request that triggered it: a large-span replay was
measured at ~40s in production against an API gateway whose read timeout is 30s, so the client got
a 503 carrying nothing to act on. A touch therefore waits at most
`ELECTRIC_CIRCUITS_REACTIVATION_JOIN_TIMEOUT_SECS` (default 20s, deliberately under that gateway
timeout; `0` waits forever) and then retires the old identity, returning the same typed recreate
outcome with reason `JoinTimedOut`. Create callers fall through to a fresh shape id; read/join
callers receive the surface-specific recreate signal. The detached replay is discarded when it
settles.

## Alternatives considered

* **Unbounded replay:** preserves stream identity but makes cost proportional to global log growth
  and multiplies memory by the number of waking shapes. Rejected after the multi-GiB OOM incident.
* **Eviction-only:** always close/delete dormant streams and force a fresh backfill. Safe for plain
  materialized shapes, but incorrect for `changes_only` feeds and needlessly loses stream identity.
* **Reconcile in place:** rerun a backfill and reconcile against the retained stream. This can
  preserve identity and bound work, but requires a durable per-shape reconciliation protocol and is
  deferred until its causal and failure semantics are specified.

## Consequences

Replay memory is bounded by the response cap, parsed page, and scheduler permits rather than log
size or shape count. The response cap is sized from the store's own readiness, not from a fixed
number: a store that advertises a page gets 16 MiB (four of its pages, so a page never reaches it),
and a store that advertises none gets 64 MiB, because it answers a read with the whole remainder of
the stream and a cap below the backlog does not bound memory — the sequencer retries the identical
read and it fails identically forever, so no data flows at all. Neither derived cap is ever below
the engine's own append budget (`ELECTRIC_CIRCUITS_CHANGES_APPEND_BYTES`, 64 MiB): a JSON page cuts
only on a value boundary, so the store frames one append whole on read, and an engine whose cap
could not hold its own largest append would latch on its own change log and cycle under the health
check on every restart. An explicit cap below the budget is refused at boot for the same reason. A deployment that wants the tighter
bound guaranteed sets `ELECTRIC_CIRCUITS_REQUIRE_DS_CHUNK_CAP=1` and runs a store that pages. The
verdict is not boot-only state: the sequencer re-attests readiness on the first failed read of a
streak — the only reconnect signal the HTTP client offers — so a store upgraded to page, or rolled
back to one that does not, changes the cap in force without a restart. If a
live read nevertheless exceeds its cap, the sequencer records a typed cap failure and increments
`sequencer_read_cap_failures_total`; what happens next depends on what the store advertised, because
the same symptom means two different things.

A page is a target, not a bound. The store cuts a JSON page only on a value boundary, so a single
value larger than the page is framed whole, and readiness advertises the largest value the store
accepts (`max_value_bytes`) beside the page for exactly that reason. Against a store that advertised
a page AND a value bound the cap already covered, and then answered with more than either, the store
is broken in a way a bigger buffer does not fix: the engine logs an error, latches the
`degraded`/not-ready status and halts further reads until a restart, rather than retrying a page
that can never fit or hiding the regression behind a larger cap. An operator-named
`ELECTRIC_CIRCUITS_DS_READ_MAX_BYTES` latches for the same reason — that number is a decision, not a
guess the engine may overrule.

Against a store that advertises no page — or a page beside a value bound the cap does not cover,
which is the deployed durable-streams build (a 4 MiB page target beside a 1 GiB value bound) — an
oversized read is not a fault at all. The uncapped store answers a read with the whole remainder of
the stream, and the paged one is entitled to one value larger than its page, so a read larger than
the client's guess is an ordinary operational state, and halting there costs more than the memory
it saves: the fleet health check replaces the task, the restart
reads from the same checkpoint, fails identically, and the task cycles until someone intervenes,
where the pre-cap engine read the page whole and made progress. So the cap is doubled (WARN,
`sequencer_read_cap_raised`) and the read retried, up to
`ELECTRIC_CIRCUITS_DS_READ_MAX_CEILING_BYTES` (default 512 MiB); only past that hard ceiling does the
engine latch. Deploying a store that pages, and then setting
`ELECTRIC_CIRCUITS_REQUIRE_DS_CHUNK_CAP=1`, is what turns the whole class back into a refused boot. The other side of a permit wait is the shape's pending buffer: from the
`BeginShape` ack until activation, every delta of its table is cloned into it, so the queue depth
that bounds scan concurrency also lengthens the window that buffer grows over. It is capped per
shape by `ELECTRIC_CIRCUITS_PENDING_BUFFER_MAX_BYTES` (default 64 MiB), counted in the executor heap
walk so `GET /memory` shows it, and dropped whole past the cap — a prefix of a shape's missing
deltas is a silently wrong stream, so activation refuses and the client recreates from a fresh
backfill. A scan whose shape is retired underneath it — a
join timeout that purged the identity, a sweeper eviction — drops that target at the next page
boundary and returns its permit there, rather than at its next append, which for a predicate that
matches nothing in the remaining span never comes. Reactivation latency can queue behind the
semaphore. `changes_only` shapes
remain active and therefore consume their normal routing state because recreation would lose their
dormant-period history. Durable Streams server paging (PR #4) improves the normal page size, but
the engine-side cap remains mandatory defense in depth. Cross-segment span calculation HEADs each
segment, subtracts the parked byte offset on the first, sums complete intermediate segments, and
stops at the ingestor's current tail (`Engine::changes_position`), which over-estimates the span by
the sequencer's lag — deliberately, since an admission budget must never under-count.

The replay itself is fenced at a different, exact point: the change-log position the sequencer
carries back in the `BeginShape` ack, which is where that shape's pending buffer starts collecting.
Everything before the fence is the replay's to deliver, everything after it is the buffer's, and the
two meet with neither a gap nor a dependency on how far apart the ingestor's tail and the
sequencer's cursor are. A fence captured earlier — the ingestor tail at touch time, before the
admission HEADs and the sequencer's state lock — would leave the envelopes processed in between to
neither path: the replay stops at the fence (and against a store that pages, its last page ends
short of the tail), and the buffer did not exist yet. Overlap is harmless because the replay appends
absolute per-pk rows; a gap is permanent.
