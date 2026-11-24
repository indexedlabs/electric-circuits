# Scratch measurement changes — never merge

The production-source baseline is the immutable release artifact recorded in
RESULTS.md. Observational builds are separate artifacts with separate hashes.

The `stall_profile` module adds bounded atomic counters and RAII wall timers:
marker waits, membership calls, registry acquisition/holding, and active flip
batches/walks. It changes no receipt condition, replay position, append ordering,
snapshot gate, retry policy or task ownership. Existing shutdown-owned tasks own
the timers; cancellation drops them. Mutex guards retain the original lifetime.
No new task, queue, network operation, durable record or dependency is introduced.
The counters have eight fixed categories and no per-row retained state.

Wall categories overlap and may sum beyond elapsed time because eight flip tasks
can be waiting concurrently. They are not a partition of CPU. Perf supplies CPU
samples independently. A timer around a registry hold includes CPU and awaited
circuit work while that guard is actually retained. Flip registry counters cover
the production propagation/requery functions, not catalog creation or tests.

Focused guard tests check mutation, release and completed-wait counting. Release
builds and those tests run only in Actions. This is characterization, not a
production-readiness regression or permission to merge. Full product/client,
restart, fault and authorization qualification gates are intentionally not
claimed for these scratch observations.

## Audience-key prototypes

`B` is the user's all-plain idealization, not a claim that every proposed access
path can be replaced by a Space filter. Every tracked non-fence table gets nullable
`audience_key uuid` and `reader_class text`. The synthetic writer derives a row's
Space from its synthetic owning account, or its Calendar for `calendar_event`,
and writes both columns on insert. This workload contains only live, home-owned
rows; updates do not move parents and deletions remove the keyed rows normally.
No additional parent lookup is charged where the importing writer already knows
its Space/Calendar. General parent moves, hidden/deleted classes, share changes,
constraint repair and migration costs are not measured.

Historical windowed requests collapse to one equality feed per table/key/class;
we retain the original gateway projection plus the two measured columns. The
gateway token/access-list/relay implementation is not prototyped, and the original
marker probe intentionally remains to compare engine work on the same basis.
Only owner-key access is exercised. Equal payloads share a canonical shape.

`B-retained` is an additional cautionary control: chat, graph edges, Resources,
Files and User audiences retain their original gateway predicates and historical
windows. Other scopes use the prototype keys. It does not claim complete proposal
parity either; in particular it does not implement live share/revocation enforcement.
Comparing B with B-retained prevents an all-plain best case being mislabeled as
proof that the proposal's explicitly retained subqueries are free.

Both use the same source accounts, row counts, transaction boundaries, synthetic
payloads, disabled FK/cascade triggers, and other-account probe as A. The extra
columns add real stored/replicated bytes. The prototype is not an authorization
qualification and must never be deployed.
