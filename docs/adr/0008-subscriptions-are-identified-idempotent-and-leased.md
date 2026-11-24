# Subscriptions are identified, idempotent, and leased

Status: accepted (2026-08-22)

A shape's subscribers were an anonymous refcount: `POST /shapes` incremented, `DELETE /shapes/{id}`
decremented, and the catalog recorded `Joined`/`Left` as counter arithmetic. That cannot survive an
ambiguous outcome — a storage response lost after the append committed made the writer's retry count
a join twice; an HTTP response lost after the engine committed made the client's retry release twice
(deleting the shape under another live subscriber); a create whose response was lost left a
subscriber nobody could ever release; and because releases were not idempotent they could not safely
be made durable-before-ack, so a crash could resurrect an acknowledged purge or forget an acknowledged
release. We decided that a **subscription is a first-class identity**: the caller names it (a
client-chosen `subscription` id on `POST /shapes`, echoed in the response and carried by
`Created`/`Joined`/`Left`), repeating a create or a release with the same id is a no-op rather than a
second count, every catalog event carries an event id the fold de-duplicates, and the records that
**create** something a client is promised — `Created`, and the `Joined` of a new claim — are durable
before they are acknowledged. So are the client-facing **removals**: a native `DELETE` does not answer
until its `Left`/`Dropped` is in the restart contract, and a retry of one — idempotent by
construction — waits on the same barrier instead of answering from memory. Engine-internal removals
(schema drift, `TRUNCATE`, the epoch reset, retention) stay queued; they carry their own completion
barriers and no client is being told anything. The earlier reasoning for answering client removals at
once was wrong twice over: it argued that a purge waiting on storage would deadlock a create parked
on its own durability wait, which it cannot — a purge does not unpark a create, and both are simply
waiting for the same storage to come back — and it leaned entirely on the lease to reconverge a
removal that never landed, when `ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS=0` is a supported production
setting that disables lease expiry outright, leaving durable-before-ack the only restart-safe
contract there is. A renewal's `Joined` does stay queued (see below). A subscription is also a
**lease**: it counts as live only if it was created or renewed (the same `POST` with the same id)
within the shape idle window (`ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS`), because on the native path reads
go straight to durable-streams and the engine cannot see them — the refcount is the only liveness
signal it has, and an unrenewed one is not a signal. Server-minted ids (no client idempotency on
create) and refcounts without leases were considered and rejected: the first leaves the lost-create
phantom unrecoverable, the second lets any dead client pin a shape forever.

## Consequences

- The native API gains one optional field and one query parameter: `POST /shapes { …, subscription }`
  (omitted → the engine mints one and returns it; the caller then has no idempotency on that create)
  and `DELETE /shapes/{id}?subscription=…` (omitted → the legacy anonymous decrement, kept only for
  callers that never learned their id; it is not retry-safe and the docs say so). The response carries
  `subscription`. Consumers (pgxsinkit's `engine-client.ts`, the published client) send the id on
  every create/release and renew on their own cadence — pgxsinkit on its 5-minute token re-mint.
- A subscription that lapses (not renewed within the idle window) is released by the retention
  sweeper exactly as an explicit `Left` would be; a shape whose last live subscription lapsed goes
  dormant and follows the ordinary lifecycle. A still-interested client that renews late simply
  re-subscribes (it may find the shape dormant or evicted and get a fresh one — the ADR-0007
  contract).
- Catalog events carry an `eid`; the fold ignores an `eid` it has already applied, so the writer's
  retry-in-place (no event is ever dropped) can never double-apply anything — joins, leaves, drops,
  rotations, segment deletions alike. A catalog written before this ADR is boot-fatal, like the
  pre-ADR-0002 and pre-ADR-0006 formats (greenfield; nothing shipped).
- Store admission now decodes only event zero. The full boot fold still checks the immutable
  `storeBound` binding before `eid` de-duplication: a second binding, even with the same `eid`, is
  fatal. The fold logs and exports its response-body bytes as `engine_catalog_bytes` (JSON
  `catalog_bytes`), including repeated events and page framing; admission reads are excluded.
  This measures append-only history without changing the catalog format or compacting it.
- A join that is abandoned mid-wait (client disconnect while storage is unavailable) compensates with
  a `Left` for its own subscription id — idempotent, so it cannot steal anyone else's claim.
- What this does not do: it does not observe reads. Liveness is the caller's renewal, full stop; a
  client that holds a handle and never renews is, after the idle window, a client that left.
- **Client-facing removals are durable before they are acknowledged** (`Left` and `Dropped` from the
  native `DELETE`). The success response is a promise a restart keeps: a purged shape does not come
  back, a released claim is not restored. A concurrent retry finds its in-memory mutation already
  applied, enqueues nothing, and waits on the same durability barrier before answering, so the repeat
  is exactly as strong as the first request rather than a weaker one. What is given up is
  availability: a native `DELETE` blocks for as long as storage is down. A client that times out
  learns nothing about the outcome and may safely ask again — and abandoning the request does not
  abandon the work, because the record still lands (the writer owns it) and the teardown it promised
  is completed by the engine rather than by the dropped request future.
- **Engine-internal removals stay queued** — drift, `TRUNCATE`, the epoch reset, retention eviction —
  as does the legacy anonymous `DELETE`, which cannot name what it released. For those the lease is
  still the repair: the restore brings a subscription back with its **lease age**, so a `Left` that
  never landed is re-applied by the sweeper within one idle window, and a `Dropped` that never landed
  leaves a shape whose (stale) subscriptions lapse the same way — with a repeat purge a no-op. That
  repair does not exist under `ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS=0`, which is precisely why the
  client-facing path no longer relies on it.
- **A lease RENEWAL's `Joined` is queued, not awaited.** Durable-before-ack exists for a record whose
  loss would make an acknowledged subscription vanish; a renewal's claim is already in the log, and
  making every renewal a storage round trip would put a client's heartbeat on the critical path of
  an outage it has no part in. (A `/v1/shape` handle's renewal is not even written: the poll renews
  it in memory, because an Electric handle does not survive a restart.)
- **The engine mints an id for a create that names none** (as decided) — `~<nonce>-<n>`, where the
  `~` is a **marker, not a reserved namespace**. Its whole job is the legacy anonymous `DELETE`,
  which releases a `~` claim before a named one so a caller that never learned the protocol cannot
  take the claim of one that did. The engine does not check whether it minted a given `~` id: a
  create may name any well-formed id, marked or not, so a returned minted id keeps working after it
  lapses, after it is released, and after a restart — with no state to remember it by.
- **Refusing an un-minted `~` id was tried and dropped.** Validating a create's `~` id against a
  set of every id this history had minted only stopped a caller from making its OWN claim the
  expendable one — which harms nobody else — and the ids were never unguessable anyway (a
  time/address nonce plus a counter), so it bought no security boundary. The price was a
  history-sized in-memory set, one entry per anonymous create for the catalog's lifetime, re-derived
  by the fold at every boot and compacted by nothing. Not worth it.
- **The lease boundary belongs to the client**: the clock is wall-clock seconds, so a claim lapses
  once its age is strictly greater than the window. Lapsing at `>=` would end a one-second window
  after a millisecond.

## A claim has an identity

Added when a group's sessions began to share one subscription: the subscription id alone stopped
being enough to say whose claim a release means.

A subscription id is the caller's to choose, so two sessions can use one, one after the other. A
group's sessions always do: the next app launch loads the group under the subscription the last
launch loaded it with. A release names the subscription and the shape (or a group's member), so a
release an earlier session sent, arriving late or retried after a later session took the claim
again, dropped the later session's claim.

So each claim has an identity, minted by the engine: 16 hex digits, opaque, compared only for
equality.

- **Every grant answers it.** `POST /v1/shapes`, `/v1/subset-feeds`, `/v1/aggregates` and
  `/v1/subsets/load` answer `claim`: the identity of the claim the subscription holds once the
  request has taken or renewed it.
- **A renewal names it to keep it.** Those requests take `?claim=…`, the identity the caller holds.
  When it is the held claim's, the grant is a renewal by the claim's holder and the identity is
  kept, so the release that holder sends later still names its claim however many renewals came
  between, and whether or not their answers arrived. Any other grant gives the claim a new
  identity: a new claim, and a claim that is held whose identity the request did not name. The
  second is a later session taking over, and it is why the rule is not "a renewal always keeps the
  identity": a session that loads while an earlier one is still held (it was killed, or its
  release is still on the way) must not share that session's identity.
- **A release names it.** `DELETE /shapes/{id}?subscription=…&claim=…` releases the claim only
  while it is the one with that identity. When the subscription holds another by now, or none, the
  release answers `200` and changes nothing, exactly as a repeat of an applied release does.
  `claim` needs `subscription` (`400` without it).
- **A release that names no claim is unchanged.** It releases whatever claim the subscription
  holds. That is the release an older client or gateway sends, and it keeps the exposure above.
- **It applies to a single shape as to a group.** Wherever two sessions share a subscription id
  they share a claim; the engine cannot tell, so every native grant and release follows the rule.
  A group's claim is one claim with one identity, held on its public subscription.
- **It is in the catalog.** `Created` and `Joined` carry `claim` when they gave the claim its
  identity, and the fold restores it. A new claim's record is durable before the answer, as it was.
  When only the identity is new (a takeover of a claim that was still held), the record is the
  renewal's and is queued like any renewal's: an engine that stops before it lands restores the
  claim under its earlier identity, and the holder's next renewal, which names an identity the
  claim no longer has, replaces it again. A renewal that kept the identity records none. Older
  engines ignore the field.
- **The engine's own compensations name it too.** A join that is abandoned mid-wait gives back
  the claim it took by its identity, so it cannot release one a later request took under the same
  subscription.

The identity travels as a query parameter on requests and a body member on answers. Not a request
body member: a client renews by repeating the request it created with, and a gateway that validates
bodies strictly would refuse a member it does not know. Not a counter: nothing needs an order, and
an identity that says how many claims an engine has granted is a number a gateway would have to
hide.
