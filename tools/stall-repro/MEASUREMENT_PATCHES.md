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
