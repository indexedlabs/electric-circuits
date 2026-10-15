# Local hardening lab report

## Milestone 1 — scope and preflight (2026-10-15)

- Authorized base: `224cd1c`, branch `mh/hardening-lab`; initial tracked checkout clean. Local commits only; no pushes.
- Read `.agent/BRIEF.md`, including the disk update. All generated data stays in this worktree or owned `/tmp` directories; loopback only. No cloud tools, external endpoints, credential files, other worktrees, or unrelated cleanup.
- Initial `df -k /`: 23,731,780 KiB available (22.63 GiB). Updated limits: **8 GB total generated files**, stop/clean owned data below **12 GB free**. Build/dependency artifacts count toward the cap too. Scenarios run serially and remove their own data afterward.
- Tools found: Homebrew PostgreSQL **18.6**, Rust **1.96.0** installed, Node **22.23.2**, cached Corepack pnpm **10.15.1** (`COREPACK_ENABLE_NETWORK=0 corepack pnpm --version`), installed durable-streams server. No local `target/` or `node_modules/` initially.
- Applied `electric-circuits-testing` and `electric-circuits-rust-code`. This is an explicitly authorized local probe/prototype, not a production-readiness task packet or release qualification. Preserve genuine red before any engine behavior fix. No production capability claims from these measurements.
- Initial source finding: managed standby returns from `claim_managed_ownership` before catalog restore. Promotion wakes the ordinary boot retry loop; current standby does **not** pre-fold the catalog. Ownership is persisted CAS, not an automatic lease/failover system.
- Planned contract: N public plain shapes, steady committed writes with a marker last in each transaction, server drain receipt followed by client materialization, then comparison with an independently specified SQL state after writers quiesce. Measure source-commit-to-receipt and source-commit-to-client separately. Fault cuts: stopped engine/WAL backlog, committed 10/100/500 MB transactions, SIGTERM/SIGKILL, managed handover. This is the direct loopback development topology with real file-backed streams.
- No persistent lab processes started yet. Commands and child PIDs will be retained in run artifacts.

## Milestone 2 — isolated build and first real-stack smoke

- Built the exact base exported with `git archive HEAD`, without touching another worktree. Rust command: `cargo build --locked --offline -p electric-circuits-engine --features test-support`, Rust 1.96.0, six jobs, dev/test debug info 0, incremental off. Finished in 6m27s; two existing dead-code warnings. Copied only the lockfile-selected cached crate archives and index into `.agent/lab-build/cargo-home`; no crate network access.
- Installed frozen npm dependencies through the cached pnpm 10.15.1, with isolated HOME/store and lifecycle scripts disabled. Only lockfile-pinned package downloads. Baseline TypeScript check passed (raw `.agent/results/typecheck.log`).
- New `scripts/lab/hardening.py` is a stdlib-only loopback runner modeled on conformance. It uses the existing explicit `test-support` DS transport, so no credential file is read. It records all service/psql PIDs, monitors total created bytes, and tears down each fixture.
- Runner smoke failures retained: HTTP helper shadowed the `http` module; DS root returns text rather than JSON. Corrected the runner before measurements. Both failed setups reaped all children. Build monitor encountered a transient `du` ENOENT during compilation; the build was immediately adopted by a replacement monitor and completed. Disk accounting now tolerates disappearing temporary files while retaining the reported total. These are harness errors, not engine-red evidence.
- First successful 10 MB tracked transaction: 9,961,472 payload bytes, 10,701,840 WAL bytes, SQL transaction 86.6 ms, commit-to-drain receipt 1043.9 ms, following marker 1047.7 ms. 121 tick commits: client p50 12.5 ms / p99 923.8 ms. Sampled engine RSS peak 75,956,224 bytes. All 152 bulk rows and 121 tick rows matched SQL; zero feed errors. Peak total artifacts 2,799,063,040 bytes, all children reaped, fixture deleted. Raw run: `.agent/results/transaction-20261015-010639/`.
- These measurements use an unoptimized binary with debug info disabled; they are characterization only. Native feeds read DS directly. Ordinary restart error behavior for the Electric adapter needs a separate probe.

## Milestone 3 — transaction boundary and baseline

- **Tracked 100 MB:** 99,942,400 payload / 107,325,552 WAL bytes; SQL commit 1.681 s; receipt 29.382 s after commit; following marker 29.394 s. Tick p50/p99 23.332/29.234 s, sampled engine peak RSS 240,959,488 bytes. Exact SQL equality for 1,525 bulk rows + 121 ticks; no feed errors.
- **Tracked 500 MB: FAIL**, not a pass or genuine-red proof by timeout. The 240 s drain deadline expired; engine logged `POST shape/s9 -> 500 (durable-streams unavailable)` and retried. Earlier, the adaptive change-log read cap rose from 128 MiB to 256 MiB. Sampled engine RSS reached 798,294,016 bytes. Peak owned artifacts 5,937,639,424 bytes; minimum free disk 15,087,423,488 bytes. All children reaped and scenario data removed. Raw `.agent/results/transaction-20261015-010925/`. A separate real-storage semantic assertion is being prepared to isolate the 500 response; do not infer its cause yet.
- **Steady baseline:** 121 commits, p50/p99 14.0/55.9 ms, final server receipt 19.6 ms, exact SQL equality, no feed errors; peak RSS 56,934,400 bytes.
- **Untracked 10 / 100 MB:** following-marker latency 90.1 / 736.2 ms; tick p99 63.1 / 571.7 ms; RSS 62,783,488 / 65,404,928 bytes. Both exact SQL equality, no feed errors. Larger and restart/ownership/corpus scenarios continue serially.
- Installed DS metadata identifies **durable-streams 0.1.5**. Binary SHA-256 `1cd958a396afecca7a80e65ece5b8cb43546bc9a1684e9e73fe6159ce5594f3b`; engine SHA-256 `36d58a02623c3c67130fee7e520b2cd015a2dae7449b3e6cfcfbbf710e2ba197`. `--version` is not supported by this DS binary.
- Added three focused runner regressions (literal loopback restriction, disk thresholds, plain-text readiness/case-insensitive headers); all pass. Upcoming restart/rolling runs also record Electric adapter status/reset observations.
- Prototype hypothesis: the existing controller protocol already supports fenced handover; the fixed two-second readiness-drain wait may be avoidable in a controller-owned local handover using the existing shutdown-drain setting. No new engine flag, durable protocol, or boot path is proposed. Freeze and run the <=2 s client-progress contract before changing the lab controller. Large catalog pre-folding is a separate design issue.

## Milestone 4 — backlog, restart, and isolated storage failure

- **Untracked 500 MB:** 499,974,144 payload / 536,827,656 WAL bytes. Commit 11.915 s; following marker 1.950 s; tick p50/p99 313/1850 ms; engine RSS 57,442,304 bytes. Exact result, no feed errors.
- **WAL backlog:** engine stopped; generated **2,014,113,984 WAL bytes**, mostly 37 rewrites of the untracked table plus tracked tick commits. Restart readiness 1.496 s; source receipt catch-up 17.392 s. All 49 ticks matched SQL without re-registering native feeds. Engine RSS 60,571,648 bytes. Peak artifacts 4.869 GB; minimum free 15.459 GB; fixture removed. Raw `backlog-20261015-011614`.
- **Seed 90210 restart cuts:** SIGTERM at operation 8, SIGKILL at 53, SIGTERM at 83, SIGKILL at 125. Readiness boot times 915/708/562/676 ms; cut-to-receipt 3575/1615/3181/1378 ms. All 161 ticks matched SQL; native streams had zero HTTP errors and max progress gap 3020 ms. Electric observer: 252 HTTP 200, 52 transport failures, **four 409 handle resets**; recovered outages 1004/786/673/742 ms. It follows the controller-selected endpoint, not a real load balancer. Raw `restart-20261015-011705`.
- Initial rolling setup returned expected `503 control admission is closed for deployment handoff` on create. Updated the harness to explicitly open admission after managed bootstrap and promotion; this is required controller behavior, not an engine bug.
- **Isolated reproducible failure:** the real installed DS 0.1.5 rejected a valid **500,614,981-byte JSON append** with HTTP **500**, body **`wal stage failed`**, after 2.499 s. `scripts/lab/append_limit.py` asserts 2xx and fails at that semantic assertion. This identifies a storage boundary behind the large tracked transaction stall; the full transaction timeout alone is not genuine-red evidence. Engine retries this response indefinitely. Splitting a shape transaction into separately visible appends would weaken atomic visibility, so no such workaround is being introduced. Keep the known-red repro and resolve the storage/atomicity design explicitly. Raw `append-limit-20261015-011830`.

## Milestone 5 — managed handover genuine red and local prototype

- Frozen red source: commit `19ede9f`, exported to `.agent/evidence/red`. Ran `python3 .agent/evidence/red/scripts/lab/rolling_contract.py --engine .agent/lab-build/target/debug/electric-circuits-engine`. Contract SHA-256: `fba37603e0bca5a384e21080c16be8a41c820c829f81074fd67d7205af83b8fb`.
- The real-stack handover completed with all 151 source writes correct on the original native feeds, then **failed the intended semantic latency assertion**: maximum client progress gap **2523.8 ms > 2000 ms**. Total handover 2928.5 ms, old drain/exit 2386.5 ms, promotion-to-ready 93.5 ms. Electric observer outage 2487.7 ms: 35 HTTP 503, six transport errors, one expected handle-reset 409. Standby refused public create with 503 + Retry-After: 1 and ownership transferred generation 1 → 2. Raw `rolling-contract-20261015-012007`.
- Minimal prototype decision: retain the existing controller sequence and single engine boot path; set the **existing** `ELECTRIC_CIRCUITS_SHUTDOWN_DRAIN_SECS=0` only for the local controller experiment. No new engine flag, Rust behavior, persisted state, lease, or alternate boot path. The lab has no load balancer waiting for the readiness advertisement. It still waits for old process exit and inactive slot before exact-generation promotion and explicitly opens admission afterward.
- This cannot be recommended unchanged for a real load balancer: route withdrawal/draining must be proven there. It also does not pre-fold a large catalog or implement automatic takeover after an unplanned owner crash. Those remain separate design/qualification work. The contract and SQL oracle are unchanged for the green run.

## Milestone 6 — prototype green and catalog boot refusal

- **Unchanged rolling contract GREEN** from clean export of `8ac261e`: native maximum progress gap **542.4 ms** (red 2523.8 ms); Electric observer outage **529.8 ms** (red 2487.7 ms). Total handover 797.0 ms; old drain/exit 554.3 ms; promotion-to-ready 99.3 ms. All 151 ticks exactly match SQL; zero native-feed errors. Electric: one 503, seven transport errors, one 409 reset; all recovered. Native p50/p99 14.0/405.7 ms. Raw `rolling-contract-20261015-012141`. This proves only this small-catalog local controller experiment.
- **200 MB catalog FAIL:** after 25 real API join/release pairs, appended 1,003,000 synthetic valid join/left pairs (200,167,560 bytes). The successor **exited 78 before binding HTTP**: its initial StoreBound admission read observed at least 67,141,633 response bytes against a 67,108,864-byte cap. The installed DS 0.1.5 responds without the bounded page capability used by newer deployment storage. Thus no 200 MB fold time exists for this provider; recording a timeout or smaller-fixture boot as that measurement would be false. Raw `catalog-20261015-012255`. Fixture and all processes removed.
- Added opt-in Rust integration regressions in `apps/engine/tests/hardening_lab.rs` for the 500 MB append and the 200 MB catalog restart. Their assertions remain success contracts, so the provider limitations stay visibly red when explicitly run. They are intentionally outside the ordinary fast lane because they allocate GB-scale local fixtures. No engine behavior workaround weakens atomic output or raises the read bound.
- Catalog growth is explicitly a replay-corpus experiment: 25 real API lifecycle pairs followed by synthetic amplification, not a claim of measured HTTP churn throughput. Smaller-catalog boot measurement and final gates follow.

## Milestone 7 — smaller catalog and validation under disk pressure

- **50 MB catalog:** 253,000 amplified join/left pairs, 50,155,560 added bytes. Restart readiness **8.817 s**, tick p50/p99 4.424/8.765 s, sampled engine peak RSS **479,428,608 bytes**. All 101 source writes matched SQL; zero feed errors. Raw `catalog-20261015-012801`. The reported inter-arrival progress-gap metric alone is misleading here because the workload began while the engine was already down; use commit latency/readiness for this scenario.
- Free filesystem space fell to about 13 GB while this task still owned only ~2.6 GB. Removed the task-created build/dependency directory (~2.5 GB), including copied Cargo cache, npm store/modules, and target files. Kept the exact measured engine and a small regression-test executable in `.agent/bin/`, plus source exports and raw results. No pre-existing or other-worktree data was removed.
- `cargo fmt --check` passed on the clean `bb3ae9a` export; baseline `pnpm typecheck` equivalent (`node node_modules/typescript/bin/tsc --noEmit -p tsconfig.json`) passed earlier. Three Python boundary regressions pass.
- To avoid rebuilding the engine during the disk squeeze, compiled the new Rust integration-test file with pinned **rustc 1.96.0 --test**, linking the already-built lockfile-pinned serde_json rlib and binding the exact measured engine via Cargo's usual compile-time binary-path variable. Exact command/env are in `.agent/results/rust-regression-build-command.json`. The two opt-in real-storage tests are now executing serially from the clean `bb3ae9a` source export; this is not a claim that the full Cargo test gate ran.
- Full `pnpm engine:test` / `pnpm test` are **blocked under this brief's hard limits**, not passed: the Rust StatsD lane binds `0.0.0.0:0` (`apps/engine/src/statsd.rs:106`), the secret-resolution lane reads credential files, and conformance defaults to reading TLS private-key fixtures. The external Electric oracle additionally requires a sibling checkout, which this brief forbids accessing. No browser/demo or production gateway/ALB qualification was performed. No production Rust behavior was modified.
- Hardened runner teardown against already-exited process groups and monitor failure, bounded psql connection/statement waits, and made the free-space floor conservatively **12 GiB** (matching `df -h`) with 300 MB headroom. These are runner safeguards, not new engine flags.

## Milestone 8 — final results, retained failures, cleanup

The local lab and controller prototype are committed on `mh/hardening-lab`. No pushes. Production Rust behavior is unchanged. The two provider failures are **unfixed known-red regressions**, not qualified behavior.

| Scenario | Measured result | Correctness / outcome |
|---|---|---|
| Steady, 8 tick shapes + 1 bulk shape, 10 writes/s | client p50 **14.0 ms**, p99 **55.9 ms** | 121 commits match SQL |
| Tracked 10 MB | following marker **1.048 s**, RSS **76.0 MB** | 152 bulk rows + ticks match SQL |
| Tracked 100 MB | following marker **29.394 s**, RSS **241.0 MB** | 1,525 bulk rows + ticks match SQL |
| Tracked 500 MB | drain **>240 s**, RSS **798.3 MB** | FAIL: shape append receives storage HTTP 500 |
| Untracked 10 / 100 / 500 MB | following marker **0.090 / 0.736 / 1.950 s** | all tracked materializations match SQL |
| 2.014 GB WAL backlog | readiness **1.496 s**, catch-up **17.392 s**, RSS **60.6 MB** | 49 ticks match SQL |
| Four seeded restart cuts | cut-to-receipt **1.378–3.575 s** | 161 ticks match SQL, four Electric handle resets |
| Managed handover baseline → prototype | native progress gap **2.524 → 0.542 s**, Electric outage **2.488 → 0.530 s** | unchanged contract red → green, 151 ticks correct |
| 50.156 MB catalog / 506,000 amplified events | boot **8.817 s**, RSS **479.4 MB** | 101 ticks match SQL |
| 200.168 MB catalog / 2,006,000 amplified events | exits **78**, initial read exceeds **64 MiB** cap | FAIL on installed unpaged DS 0.1.5 |

All MB in the table are decimal except explicitly marked MiB. The engine is the pinned unoptimized development build, not a release build. Peak RSS is sampled and is not a strict memory bound. Catalog history was amplified from the real event schema after 25 actual API churn pairs; it is not a lifecycle-throughput benchmark.

Validation and evidence:

- The Rust regression executable ran **2 tests: 0 passed, 2 failed, 0 ignored, 0 filtered**, exit 101, in 24.29 s. Both failures executed their intended semantic assertion: HTTP 500 (`wal stage failed`) versus 204 for the 500,614,981-byte append; failed restoration versus success for the 200 MB catalog. The repeat isolated append failed in 1.833 s. All fixture processes were reaped. Raw `.agent/results/rust-regressions.log`.
- Three Python runner regressions pass; `cargo fmt --check` and the TypeScript no-emit check pass. Full Rust/TS, external oracle, and browser/gateway lanes are not claimed green; limitations are listed in Milestone 7 and `.agent/results/validation-summary.json`.
- `.agent/results/evidence-index.json` hashes raw artifacts and pins engine/DS/Postgres identities. It also verifies that **both `rolling_contract.py` and `hardening.py` were byte-identical between the red and green exports**. Only the local controller prototype changed the existing drain setting.
- Older result JSON calls the current controller branch head `git_head`; for exported sources that is **not necessarily the tested source commit**. In particular, the Rust tests both used the immutable `bb3ae9a` export while the controller advanced during execution. Use the export/build identity recorded here and the runner hashes. The runner now calls that diagnostic `control_head` and records hashes of all lab Python sources to avoid this ambiguity.
- Cleanup audit: **106 directly spawned fixture children recorded, 106 reaped, zero live recorded PIDs**. Build/install/test PIDs 53381 / 77792 / 12801 / 37805 also no longer exist. Every scenario data directory is gone. Large build/dependency artifacts were removed; roughly 131 MB of measured binaries, clean source exports and evidence remain for replay. Peak recorded task artifacts were **5.938 GB**, below the 8 GB limit. No observed run crossed the 12 GB free-space floor.

Ranked next steps (simplest first):

1. Keep the existing managed handover sequence: explicitly open initial admission; start B as standby; close A admission; commit a fresh source fence; quiesce; confirm old exit and slot release; promote the exact generation/revisions; await readiness; open admission. The local zero-drain experiment removes about two seconds. A real load balancer must prove route withdrawal before adopting that setting.
2. Where consumers do not need large payload columns, use the already-supported static ingest projection and an appropriately explicit publication. Retest the real query set; this lab did not measure projected 500 MB transactions, and source WAL still has to be scanned.
3. Resolve the DS provider boundaries before claiming large-transaction or large-catalog readiness: atomic large appends currently return `wal stage failed`, and unpaged reads prevent catalog admission. Keep the two Rust repros red until the storage capability is fixed and pinned. Do not split one transaction into separately visible shape appends or just remove memory bounds.
4. Once bounded paging works, reduce catalog replay cost with a reviewed durable snapshot/checkpoint or read-only incremental warm fold that catches up to an exact final tail. The 50 MB / 8.8 s result already shows that eliminating a two-second drain cannot make large-catalog handover seamless. Preserve identity, epoch, highwater, subscription and retirement witnesses; this requires design, not a small local patch.
5. Treat unplanned owner death separately from rolling deployment. The persisted revision/generation CAS is not an expiring lease or automatic HA takeover. A killed owner can restart with its own revision; a different standby requires a fenced takeover design and failure-matrix qualification.

Replay now (no rebuild needed on this machine):

```sh
python3 -B scripts/lab/rolling_contract.py --engine .agent/bin/electric-circuits-engine-base
.agent/bin/hardening-lab-tests --ignored --test-threads=1
```

The first command exercises the local prototype; the second deliberately reproduces the two known-red provider contracts. Both may refuse to start if the conservative disk headroom is unavailable. No production rollout is authorized or implied.
