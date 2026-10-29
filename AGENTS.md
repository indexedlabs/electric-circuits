# AGENTS.md

Guidance for AI agents working in **electric-circuits** — an Electric-style reactive sync engine. App
writes go to **Postgres**; a Rust engine turns logical-replication changes into **live shapes**
(incrementally maintained, fully de-duplicated); **durable streams** is the log between them. Two
client surfaces: the Electric-compatible `GET /v1/shape` (works with the ElectricSQL TS client) and
the extended `@electric-circuits/client` API (shapes + subset queries + live aggregations — the surface
the project is growing toward).

## Current state, target state and authority

This repository is an active implementation, not yet a production-ready successor deployment. Keep
these statement types separate:

- Architecture, runbook and invariant statements describe the **as-built repository**. Sections
  explicitly introduced as policy or target define how agents must work or what must be built and
  qualified; they are requirements, not claims that the capability exists today.
- [`notes/18-production-readiness-spec-reviewed.md`](notes/18-production-readiness-spec-reviewed.md)
  is the canonical production-readiness task/dependency authority. Notes 23 and 24 supply stable
  Swift/app and PG18/E2E scenarios; they do not define a second task graph.
- The production target is PostgreSQL 18 behind one authenticated gateway with an explicit
  publication and durable storage; the development baseline is also PostgreSQL 18. Compose uses the
  pinned `postgres:18.6` image, ephemeral demo/test launchers require PostgreSQL 18 binaries, and
  direct engine/API routes remain available. A green current suite is inherited regression evidence,
  not gateway or release qualification.
- Until `PLAN-001` generates and validates the checked-in task manifest, it is the only initially
  merge-ready production-readiness packet. Its one typed bootstrap packet pins this specification's
  blob/tree with `profile_scope: uncompiled_all`; it cannot claim a future release-profile or scenario
  registry hash. Every later packet requires generated identities and the ordinary no-placeholder
  rules. Section order, disjoint files or an agent's local green run do not make another task ready.

No agent may describe a target contract as implemented, production-ready or qualified without the
selected profile's generated dependency closure and exact candidate evidence.

## Mandatory skill routing

Repository-local skills live under `.agents/skills/`. Read the matching `SKILL.md` completely before
acting, then load only the references it routes to:

- Any implementation, debugging, refactor or review of Rust under `apps/engine`, including Tokio,
  unsafe/FFI, failure semantics, observability or resource bounds: read
  [electric-circuits-rust-code](.agents/skills/electric-circuits-rust-code/SKILL.md).
- Any crate/module/workspace boundary, `Cargo.toml`, feature, visibility/public API, toolchain/MSRV,
  dependency graph, platform seam, build script or proc-macro decision: also read
  [electric-circuits-rust-structure](.agents/skills/electric-circuits-rust-structure/SKILL.md).
- Any behavior change, bug fix, test authoring/review, acceptance harness, fault injection,
  qualification or flake investigation: read
  [electric-circuits-testing](.agents/skills/electric-circuits-testing/SKILL.md) **before** writing
  implementation code.

For behavior-changing Rust structure work, the order is testing contract → structural boundary →
Rust implementation. These skills operationalize this file; they never override the invariants below.
When maintaining a skill, run the installed skill-creator validator through an interpreter that
provides PyYAML and verify local links. In this environment the runnable form is
`uv run --with pyyaml python /Users/bozilabs/.codex/skills/.system/skill-creator/scripts/quick_validate.py
<skill-dir>`; do not rely on the script's executable bit or host Python dependencies. Validate
`agents/openai.yaml` with the Codex-distributed schema/tool when one is available; until then require
valid YAML with string `interface.display_name` and `interface.short_description` fields.

## Layout

| Path | What |
|---|---|
| `apps/engine` | Rust engine. Key files: `engine/` (the engine module — `sequencer.rs` the LSN-ordered sequencer, `lifecycle.rs` shape creation/sharing/retention, `circuit_serving.rs` circuit-tier serving, `executors.rs` routers/filters/folds, `planning.rs` circuit placement, `catalog.rs` durable catalog, `drift.rs` schema-drift retirement + the reconciler, `epoch.rs` slot binding + epoch reset, `introspection.rs` graph/state, `membership.rs` the shared membership kernel (flips, query-backs), `output.rs` envelope codec, `mod.rs` the `Engine` handle), `arrangements.rs` (the circuit: in-memory counts pipelines, group-aggregated boot seeding), `subquery.rs` (cross-table registry: shared inner-set nodes, flips, absolute emission), `replication.rs` (streaming pgoutput ingestor) + `pgoutput.rs` (message decoder), `pg.rs` (backfill + `SnapshotGate`), `electric.rs` (`/v1/shape`), `where_sql.rs`/`sql.rs` (SQL⇄predicate), `ds.rs` (streams client incl. `append_reliable`). |
| `apps/api` | TypeScript tRPC compatibility adapter (`router.ts` + historical `core.ts`); lifecycle/query calls forward to the Rust engine's native `/v1` API. |
| `packages/protocol` | Shared types + the change-event envelope (`types.ts`, `envelope.ts`). |
| `packages/client` | Browser client: `shape()`, `subset()` (see `subset.ts` — LSN watermarks + tombstones), `aggregate()`. All lifecycles tracked; `close()` is one-shot and deletes server-side with retry. |
| `packages/conformance` | The real test suite — engine vs oracle, incl. live replication, fuzz, NULLs, concurrency, shape sharing. |
| `packages/oracle` | Reference implementation shapes are checked against. |
| `packages/bench` | Benchmarks incl. the **benchmarking-fleet runner** (`electric-bench-runner.ts`, `pnpm bench:fleet` — auto-clones electric-sql/benchmarking-fleet). |
| `packages/loadgen` | Headless load generator (state-machine users; memory/CPU/disk sampling; Docker-scalable clients). |
| `electric-conformance/` | Electric's own oracle/property/integration tests pointed at our `/v1/shape`. Optional; not a gate. |
| `docker/` | Containerized stack: `compose.yaml` (postgres + ds + engine + api), `Dockerfile.engine`, `Dockerfile.node`. `pnpm docker:up`. |
| `apps/pipeline-viz` | Live pipeline explorer (shapes, shared families/nodes, reactive per-node state + index dumps) over `GET /graph` + `/state` + `/trace`. |
| `examples/linearlite` | The flagship demo. `scripts/linearlite.sh start <size>` boots everything. |

## Docs (read these before designing)

- `README.md` — the system in one page + the consistency model summary.
- `docs/ARCHITECTURE.md` — the as-built architecture: ingest, `SnapshotGate` fencing, sharing,
  subquery registry, reliability model, Electric adapter, client layer.
- `docs/ivm-engine-internals.md` — engine execution strategies + the analytical cost model,
  including the three-tier serving model (circuit/routing/fallback): see
  [`docs/ivm-engine-internals.md#serving-tiers-compiled-routed-fallback`](docs/ivm-engine-internals.md#serving-tiers-compiled-routed-fallback).
- `docs/live-queries-guide.md` — user/integrator guide.
- `docs/deployment-postgres.md` — Postgres-as-source-of-record setup.
- Each package has its own `README.md` (surface, commands, env knobs).

## Contract-first TDD

**Development policy and target qualification rule:** the red/green loop applies now. The PG18,
gateway, causal-receipt and immutable-candidate topology below is target infrastructure until its
named production-readiness tasks land.

The default development loop is **public contract → genuine red → minimal green → refactor**. We
prefer a small set of stable high-level E2E contracts because they let the engine, gateway and client
internals change without rewriting the product specification. Focused tests explain failures and
exhaust small state spaces; they do not replace the public contract.

For every behavior-changing task:

1. Name the supported profile/capability, stable scenario, public inputs/outcomes, independent oracle,
   failure cuts and resource limits before implementation. After `E2E-000S`, production-readiness
   behavior packets use its registered scenario ID and semantic hash. Before then, only the typed
   bootstrap, `non_behavioral` work and explicitly declared inherited controls are legal; no
   implementation packet may invent an unregistered product contract.
2. Add the highest stable boundary regression that can falsify the promised behavior. For a local-only
   parser, codec, algebra or state-machine law, that boundary is a focused unit/property/model test;
   do not manufacture E2E. For a product behavior, run the stable black-box test on the exact frozen
   red-patch tree descended from the pinned base and record a failure at the intended semantic
   assertion. A compile/setup error,
   timeout, skip, broad expected failure, disabled assertion or mock returning its own expected value
   is not red evidence.
3. Add the smallest focused test that localizes the mechanism when useful: unit, golden corpus,
   property/model state machine, deterministic concurrency schedule, fault cut or fuzz regression.
4. Make the smallest implementation change that turns the **unchanged** contract green. Do not edit
   the oracle, exclusions, scenario hash or qualification runner to accommodate the implementation.
5. Refactor only while the contract and focused tests remain green. A discovered seed/operation trace
   is minimized, retained in the task patch/evidence and replayed before novel generation.
6. Run the adjacent scenario matrix and the applicable repository gates. Record exact commands,
   candidate SHA/config/profile/digests, test counts and raw failures. `blocked`, flaky, filtered,
   zero-test, under-run or wrong-input evidence is never a pass.

Use these identities consistently:

| Term | Meaning and admissible evidence |
|---|---|
| **base** | Clean pinned starting commit/tree before the new contract test. |
| **red patch** | Test-only descendant of the base whose intended semantic assertion demonstrably fails before implementation. |
| **green candidate** | Task-scoped descendant of the red patch (or base for typed non-behavioral work) where the unchanged contract passes. |
| **qualification candidate** | Exact integrated, immutable artifact/config/profile/platform tuple on which qualification is rerun; author-worktree evidence does not transfer automatically. |

`genuine_red`, `inherited_control` and `non_behavioral` are distinct proof kinds. A passing inherited
control is characterization, never `red_proved` and never authority for a behavior implementation.

### What the high-level E2E contract observes

Production acceptance crosses real PostgreSQL 18, replication, the engine, file-backed durable
streams, the public gateway/client and the real materializer/cache selected by the profile. It asserts
only public effects and an independent result—not circuit nodes, Rust tasks, private offsets, retry
counts or log text.

The reusable causal fence is:

```text
source transaction + SourceCommitID
  -> server drainedThrough(SourceCommitID), including deferred work
  -> public read/event begins after that receipt
  -> target materializer/cache commits appliedTailAfter(SourceCommitID)
  -> compare with independently authored SQL/reference state at the same source prefix
```

The target `E2E-000A` protocol writes `SourceCommitID` as the final change in the **same PostgreSQL
transaction** as the mutations. The marker lives in a harness-only relation included in the immutable
explicit test publication and excluded from public templates. The server observes it only after the
transaction-end envelope and emits `drainedThrough` only after all causally prior direct and deferred
work completes. The target receipt is keyed by principal, template and generation. An unpublished or
early marker, a receipt that skips deferred work, a later SQL query, a separate sentinel feed, byte
arrival, a private LSN/offset or a server-only drain is not proof that the client applied the same
prefix. Until `E2E-000A` supplies and mutation-tests this infrastructure, the current conformance drain
helper is valuable characterization/regression evidence but not final release qualification.

Use named gates/events to create order: arrived → held → released → terminal. Every wait has a
diagnostic deadline, but sleeps and ad-hoc polling never establish correctness. Qualification uses
real collaborators and externally controlled process/network/storage cuts; test doubles are for
focused deterministic failure paths, not proof of the deployed topology. Qualification has zero
retry tolerance: a retry-pass is a flaky failure whose attempts remain evidence.

High-level E2E is not the primary tool for pure parsers, codecs, predicate algebra, fold laws or all
thread interleavings. Test those exhaustively with golden/property/model/fuzz tests and Loom/Miri where
supported, plus one real-stack contract when the local law participates in a product promise. Every
bug fix keeps a regression at the highest stable boundary that would have caught it.

## Parallel implementation protocol

**Target execution policy:** this governs production-readiness subagents once the foundation and
`PLAN-001` control plane are present in a clean, authorized integration history. It does not make the
current dirty checkout or a prose task list launchable.

Parallelism is dependency- and ownership-bounded. Follow
[`notes/skills-research/05-parallel-agent-execution-protocol.md`](notes/skills-research/05-parallel-agent-execution-protocol.md);
the generated `PLAN-001` manifest remains the scheduler and source of truth.

- The sole pre-validator exception is the typed `PLAN-001` bootstrap packet described above. Every
  other launch is a task/execution-scope/profile pair emitted `ready` by the validator. A packet pins
  its injective identity and attempt, complete evaluated predecessors, base/tree and declared read/
  write/semantic resources, proof kind and contract, exact gates, artifact/config/toolchain/platform
  identities, the validator-generated gate-matrix hash, scheduler generation, reservation lease,
  reviewers and integration destination. Packets may add stricter gates but cannot rephase or omit a
  generated one. A `TBD`, mutable tag or prose alternative is not launchable.
- One author owns one principal write boundary in one clean linked worktree/branch based on the pinned
  integration SHA. The shared checkout is control/integration state, not a parallel author worktree.
  Agents do not opportunistically fix adjacent files, edit `AGENTS.md`, or claim another packet's
  contract/fixture/schema/lockfile. A packet records whether the user/delegated operator authorized
  task commits; without it, the agent prepares a patch and note but does not commit or push. Push and
  integration require separate authority. A prepared patch is canonical, content-addressed and bound
  to its base/tree, expected result tree, changed-file manifest and evidence hashes; review and apply
  verify those exact bytes.
- Editing and evidence use different directories. Every red, green, direct, merge-preview,
  qualification and reviewer command runs from a newly created evidence source: a clean detached
  worktree at the exact commit, or a newly empty export of a prepared patch's verified expected Git
  tree. Immediately before each command, bind the exact commit/tree, empty tracked/index and pre-mount
  untracked/ignored state (or the exported tree's complete file/mode/content manifest), and canonical
  effective config to the evidence. Immutable dependencies/tools/fixture inputs use a content-digested
  read-only manifest plus resolver/mount-topology hash; a source-visible dependency mount is allowed
  only when that exact absent-from-Git read-only mapping is declared, is the entire post-mount overlay
  inventory, and remains unchanged in the post-command attestation. Every command gets
  a unique, newly empty external output/cache/fixture/artifact root keyed by its packet/candidate/gate.
  A run from the author/control checkout, an undeclared or writable overlay, stale/mutable dependency
  input, reused/nonempty output root, source mutation or missing attestation is `fail`; the reviewer
  independently recreates the source, resolver mapping and empty run root.
- Behavior work is a stacked red/green pair with two packets. A scenario/scope/consumer-bound
  `red_artifact` packet creates the failing commit; an independent reviewer reruns it before the
  implementation packet may consume that exact SHA. Even one author crosses this packet/review
  boundary. Neither implementation nor qualification may weaken the scenario, oracle, exclusions or
  hash, and only the green stack merges.
- Every attempt writes an immutable handoff at
  `notes/execution/<task-id>/<execution-scope-or-profile-hash>/a<attempt>.md` with packet/lease hashes,
  starting and candidate/patch identities, paths, decisions, commands/results/counts, raw artifact
  hashes, remaining risks and `ready_for_review|fail|blocked`. The controller alone writes the
  distinct content-addressed `a<attempt>.resolution.json` with terminal
  `pass|fail|blocked|invalidated`; it never amends the reviewed candidate. A task declared once as a
  shared producer cannot also run concurrently per profile.
- Agents check the current scheduler generation and reservation lease at each phase boundary. An
  input/resource collision, changed predecessor/contract/profile/config/image/toolchain, or revoked
  lease hard-invalidates the attempt. Leases have a packet-bounded TTL (at most 300 seconds) and an
  authenticated agent heartbeat at most one-third of that TTL; the controller rechecks inputs before
  renewal, the agent must observe the acknowledgement, and control-plane loss expires rather than
  silently extending ownership. A green stale candidate is never reviewable or mergeable.
- The integration operator accepts one reviewed logical task at a time. For a candidate based before
  an unrelated merge, it may preserve the reviewed task commits through a machine-checked merge
  preview only when intervening paths/read sets/semantic resources and all declared inputs are
  disjoint; direct and affected gates rerun on that preview and an independent reviewer accepts the
  refreshed evidence. Never rewrite/rebase the reviewed commits or reuse qualification evidence.
  Otherwise issue a new packet. After integration, regenerate the DAG/profile/ownership report and
  atomically refresh or revoke outstanding leases before emitting the next ready set.
- Contract authors, implementers, qualification authors and integration reviewers are distinct for
  high-risk/cross-boundary tasks. A qualification agent cannot repair the behavior it judges.

Author/merge gates and release qualification are separate. Direct task gates must pass. A manifest-
declared baseline-repair packet may consume only its exact recorded base failure and must turn that
assertion green; unrelated inherited failures must remain identical and visible. An unavailable lane
may block release qualification without deadlocking the task that installs or repairs that lane. No
profile promotes until every generated qualification gate is green on the exact qualification
candidate. `PLAN-001` owns the per-task gate phases, commands, applicability and baseline assertions;
a packet author cannot classify its own inconvenient gate as inherited or qualification-only.

Stop a task rather than broadening it when it needs an unowned shared surface, a required genuine red
cannot be demonstrated, a pinned input changed, a direct gate fails, its lease is revoked, or an
external dependency is absent.
No calendar-duration monitoring requirement substitutes for fixed operations, event floors, named
cuts, deterministic terminal conditions and explicit resource bounds.

## Designing dbsp circuits: pipelines vs shapes

The load-bearing mental model: **pipelines are few and fixed; shapes are many and dynamic —
and the fan-out between them lives outside the circuit.** A pipeline's output is keyed by
*cohort groups* (project, (project, status), aggregate group, …). A shape is a selection or
union over those groups, materialized as a per-shape stream at the delivery edge. Shape
cardinality can vastly exceed pipeline cardinality: a subquery shape filtering issues exists
per *combination* of projects a client asks for, yet every combination is fed from the same
`issues_by_project` pipeline — the circuit never grows with shape count, only the routing
table does. If a design makes the circuit's structure scale with shapes, users, or parameter
combinations, it is wrong (the circuit-per-shape trap: structure must never scale with
subscriptions).

The recipe for capturing an app's query set in one circuit:

1. **Enumerate call sites → collapse to templates.** Parameters become *data* (keys in the
   output index, rows in an input relation) — never circuit structure.
2. **Find the access cohort** (LinearLite: the project) and key every pipeline output by it,
   never by user or shape. Per-shape work happens only at the fan-out edge: a shape = the set
   of cohort groups its parameters select, unioned by delivery. The union is correct only when
   the cohort key **partitions** the table (a row lives in exactly one group) — overlapping
   groups would double-emit and need dedup at the edge. Genuinely per-user predicates
   (`username = $me`) get their own keyed feed — same pattern, cohort of size one.
3. **Visibility relations drive membership through the registry**: a membership table's
   deltas flip shared inner-set nodes, and move-in/move-out are parallel pooled Postgres
   query-backs with absolute per-pk emission through ordered lanes (row data lives in
   Postgres — there are no local row snapshots to read).
4. **Linear operators are free** (filter / project / `map_index`); **aggregates knowingly** —
   counts pipelines hold O(distinct groups) in memory (`ARCHITECTURE.md` §6b). Aggregate at the
   finest useful group grain and let the reader sum groups, so one pipeline serves every
   filter combination.
5. **Structure ships with deploys.** New templates = circuit rebuild + reseed (layout
   fingerprint) or `Mode::Persistent` bootstrap. Ad-hoc predicates that match no template fall
   back to the dynamic-shape path (standalone evaluator / KeyRouter / registry) — the circuit
   is an optimization tier, never a correctness dependency.

## PostgreSQL 18 production target

**Target contract, not current connector support:** current Compose and CI facts remain those stated
above. Production traffic is forbidden until the generated PG18 tasks and qualification close.

Production-readiness work targets the content-addressed PostgreSQL 18 image and configuration pinned
by the selected release profile. Image evidence includes the OCI index digest, OS/architecture/
variant and resolved platform-manifest digest; an index alone does not identify tested bytes. Do not
silently substitute PostgreSQL 16, a host-default major, another platform image or a mutable
`postgres:18` tag and reuse the evidence. The initial topology is one writable primary, one explicit
immutable table publication, one `pgoutput` slot, one active engine and file-backed durable streams;
failover is a separate qualified profile, not implied by a same-named slot.

The production bootstrap/admin role owns publication/slot DDL. The runtime role is non-superuser and
must not create `FOR ALL TABLES` publications. Admission, snapshot SQL, pgoutput decode, schema drift,
reactivation and restore must use one canonical publishable-column schema. Until their named PG18
tasks prove otherwise, reject virtual generated columns, tracked-table RLS, missing/unpublished stored
generated or identity columns, inadequate replica identity, and publication drift. Production-mode
preflight rejects every connector until bootstrap/admin, pooled query/backfill/query-back and walsender
paths each have a documented TLS backend, CA/SAN/hostname verification, channel-binding disposition
and `pg_stat_ssl` proof for SCRAM plus `verify-full`; accepting an `sslmode` token is not evidence that
the connector enforces it. `sslmode=disable` is development-only. Slot invalidation or unproven
cluster/frontier continuity fails closed or uses the one authorized whole-generation reset path; it
never silently recreates the slot while serving shapes.

See [`notes/24-postgres18-and-e2e-tdd-addendum.md`](notes/24-postgres18-and-e2e-tdd-addendum.md)
for the stable `PG18-E2E-*` contract matrix. An earlier exploratory local run reported ordinary and
stored generated snapshot/live success but virtual-generated snapshot/live divergence. Treat that as
an unverified blocker hypothesis—not inherited evidence—until `PG18-000`/the harness retain the exact
platform digest, fixture, source/engine/DS SHAs, replay command and raw result. Virtual generated
columns remain rejected in the first profile regardless; representation drift is never tolerated.

## Build & test

```bash
pnpm engine:build          # cargo build -p electric-circuits-engine
cargo fmt --check          # rustfmt.toml at the root (120 cols, Max heuristics); CI enforces it
pnpm engine:test           # cargo test  -p electric-circuits-engine   (fast)
pnpm typecheck             # tsc --noEmit over the whole TS workspace (seconds; no PG, no engine)
pnpm test                  # vitest run — full suite incl. conformance (~60s; boots its own PG)
pnpm test:conformance      # just the conformance package
pnpm test:fuzz             # random-predicate fuzz vs oracle
pnpm loop [N]              # fuzz until failure; replay with SEED=<n>
pnpm demo:linearlite       # LinearLite demo (ephemeral PG + engine + ds + api + vite + caddy)
pnpm bench:fleet           # ElectricSQL benchmarking-fleet vs our /v1/shape (auto-clones)
pnpm docker:up             # containerized stack
```

**Benchmarking against other Electric versions.** The fleet runner also drives any
Electric-compatible server instead of our stack — use this to baseline stock Electric releases
against the same workloads:

```bash
# 1. Boot the target, e.g. stock Electric:
docker run -d --name electric-baseline -p 3000:3000 \
  -e DATABASE_URL=postgresql://postgres:password@host.docker.internal:54321/electric \
  -e ELECTRIC_INSECURE=true electricsql/electric:latest

# 2. Point the fleet at it (both vars required together; tables are dropped/recreated in that DB):
EXTERNAL_ELECTRIC_URL=http://localhost:3000 \
EXTERNAL_DATABASE_URL=postgresql://postgres:password@localhost:54321/electric \
BENCH_OUT=docs/bench/electric-fleet-results-baseline.md pnpm bench:fleet
```

Use a distinct `BENCH_OUT` per target and diff the reports; `BENCH_ONLY`/`BENCH_SCALE` apply the
same way. (Our own image can also be the target — `pnpm docker:up`, then point at port 7010.)

**vitest does not typecheck** — it runs through esbuild, which strips types without reading them.
`pnpm typecheck` is the gate (one root `tsconfig.json` over every server/node TS package + the test
files; CI runs it right after install, before the suite). The browser/Vite apps — `apps/pipeline-viz`
and `examples/**` — are excluded: they are React 18 / TS 5 trees with their own configs. Always run
`pnpm typecheck` + `pnpm engine:test` + `pnpm test` before claiming done.

**Strict Clippy is a known non-green baseline, not a current inherited gate.** `TST-000` must retain
the exact source/toolchain command and raw result before a dedicated baseline/CI task makes it
mandatory. Do not hide the gap with blanket `allow`s or make unrelated feature packets fix the whole
tree. Changed Rust should still avoid new compiler/Clippy debt, and every task must report the strict
command honestly if its packet asks for it.

## Running the stack (sizes, explorer, load testing)

`scripts/linearlite.sh start <size>` — size = `small|medium|large|xlarge|<issue count>`; users and
projects scale with it (users ~√issues). Boots PG + ds + engine + API + web UI + the pipeline
explorer; `stop` tears down cleanly; `status` reports. One instance at a time (teardown is
pattern-based). Ports: `DEMO_HTTPS_PORT` (8443), `DEMO_VIZ_PORT` (5180), `DEMO_VIZ=0` to skip.

`packages/loadgen` — `USERS=100 SEED_ISSUES=20000 DURATION_S=90 pnpm --filter @electric-circuits/loadgen
loadgen`; `SWEEP_USERS=…` for comparison tables; Docker client scaling in `packages/loadgen/docker/`.
The streams layer is the Rust durable-streams server (group-commit WAL — appends batch under
concurrency); `DS_MEMORY=1` still removes durability entirely for max-throughput runs
(`--durability memory`, Linux-only).

## Demo + visualizer: start, drive, verify (agent runbook)

**Start everything with one command** (rebuilds the engine, boots an ephemeral throwaway Postgres
with `wal_level=logical`, durable-streams, the API, LinearLite, and the pipeline visualizer wired
to the engine):

```bash
pnpm demo:linearlite > /tmp/demo.log 2>&1 &     # agents: run in background, tail the log
# ready when the log prints "👉 Open a URL above"
```

Fixed URLs: LinearLite `http://localhost:5174` (HTTPS/HTTP-2 `https://localhost:8443`), visualizer
`http://localhost:5180` (`https://localhost:5443`). Ephemeral ports for the rest — grep the log:
`postgres →`, `engine →`, `api →`. `DEMO_SEED_COUNT=<n>` scales the faker seed (default 512 issues).
Data resets every run. **Restarting:** kill the previous run first or Vite silently binds 5175 —
`pkill -f electric-circuits-engine; pkill -f caddy; pkill -f linearlite/start.ts`, then relaunch (if a
port lingers: `lsof -ti :5174 -ti :5180 | xargs kill`).

The **visualizer** can also attach to any running engine on its own:
`ELECTRIC_CIRCUITS_ENGINE_URL=http://127.0.0.1:<port> pnpm --filter @electric-circuits/pipeline-viz dev`.
Its dev server proxies `/engine/*` → the engine control plane, so browser-side `fetch('/engine/graph')`
etc. work from the page — the backbone of the verification workflow below. A third way is the
containerized visualizer (`docker/Dockerfile.viz`): `docker build -f docker/Dockerfile.viz -t
electric-circuits-viz . && docker run -p 5180:5180 -p 5443:5443 electric-circuits-viz` serves
`http://localhost:5180` with Caddy proxying `/engine/*` to the engine; set `ENGINE_UPSTREAM` to
point it at another engine.

### Typical verification workflow (Playwright MCP)

Use the Playwright MCP browser to drive both apps; keep LinearLite and the visualizer in two tabs
(`browser_tabs` to create/select, `browser_navigate` to each URL).

1. **Make the pipeline do something.** Drive LinearLite: switch the "Viewing as" user
   (`browser_select_option` on the sidebar `<select>`) to create/join that user's shapes; open the
   Board view or an issue detail for more; drag cards / edit issues for live writes. For surgical
   writes, `psql` straight at the demo Postgres (URL from the log) — replication picks it up.
2. **Verify engine state from the viz page** with `browser_evaluate` — no CORS friction thanks to
   the proxy: `await (await fetch('/engine/graph')).json()` (shapes/nodes/edges),
   `/engine/shapes/{id}` (incl. retention `state`), `/engine/metrics`, `/engine/state`.
3. **Verify the canvas against the engine.** Count DOM vs graph:
   `document.querySelectorAll('.react-flow__node').length` / `'.react-flow__edge'` vs
   `graph.shapes` — nodes render immediately, edges must too (regression test: clear shapes via the
   trash button, switch user, edges must appear without a reload).
4. **Verify animations deterministically** via the sidebar **Activity** log (last 50 replicated
   changes): click an entry with `browser_evaluate` and sample in the same script — dot positions
   over time (`.react-flow__edge g circle` → `getBoundingClientRect()`), staged flash delays
   (`.flash` → `--flash-delay`), pulse stagger. Replay beats racing a live write's timing.
5. **Eyeball it.** `browser_take_screenshot` after driving — a blank or stale frame is a failure
   even when the DOM probes pass. Check the browser console output for React/engine errors.

Retention interplay while testing: an open LinearLite tab holds subscriptions (refcount ≥ 1), which
blocks dormancy for its shapes; `GET /shapes/{id}` is deliberately NOT a retention touch, so
polling it never keeps a shape alive. To exercise dormancy/eviction fast, boot with second-scale
knobs (`ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS=1 ELECTRIC_CIRCUITS_RETENTION_SWEEP_SECS=1 …`) — see
`packages/conformance/src/conformance-retention.test.ts` for the canonical sequence.

### Testing checklist before handoff or qualification

**This is a task-completion requirement, not a suggestion.** Every engine-touching author runs and
reports each lane the packet classifies as direct or inherited. Direct/affected task gates must be
green; missing tooling is never a silent waiver.

```bash
cargo fmt --check                         # root rustfmt.toml; CI enforces it
pnpm typecheck                            # tsc --noEmit over the TS workspace (vitest cannot see type errors)
pnpm engine:test                          # Rust unit + integration (fast)
ELECTRIC_CIRCUITS_ENGINE_PREBUILT=1 pnpm test  # full vitest suite incl. oracle conformance (set the var iff you already built)
```

The vitest suite includes `packages/conformance` — the engine-vs-oracle harness — and runs
against the always-on circuit on every run (there is no off mode). The
`ELECTRIC_CIRCUITS_DBSP_INDEXES`/`_COUNTS` tunables decide which shapes the circuit actually serves
versus which fall through to the routing/fallback tiers.

Electric's *own* oracle suite pointed at our `/v1/shape` is an optional tool, not a gate: no task,
merge or qualification has to run it or report it. Run it when a change to `/v1/shape` warrants a
check against Electric's client (the ASDF pins matter: `../electric` asks for an Elixir that may not
be installed locally):

```bash
ASDF_ELIXIR_VERSION=1.18.4-otp-28 ASDF_ERLANG_VERSION=28.1 \
  ./electric-conformance/run.sh oracle    # Electric's own oracle vs /v1/shape (needs elixir + ../electric)
```

Gate phases prevent a red inherited baseline from deadlocking the task that repairs or hermeticizes
it. `TST-000` freezes every existing failure/blocker. A validator-declared baseline-repair task must
show genuine red on the exact owned assertion and green on the candidate; unrelated inherited results
must remain identical. A task that installs an unavailable external/browser lane may integrate after
its runnable direct gates pass and its named blocker is preserved, but that lane remains `blocked`.
These are sequencing rules, not release waivers: no profile is qualified or production-ready until
the full generated qualification matrix is green on the exact qualification candidate.

Evidence provenance is part of every lane above. Run it from the packet's fresh clean evidence source,
not the editing checkout, and retain the pre/post source, external-input/resolver-topology, empty-run-
root and effective-config hashes with the raw result. A passing assertion on dirty, undeclared,
mutable, reused or unidentified bytes is a failed gate.

**E2E (browser) tier:** for anything touching the engine's live path, shapes, or the visualizer,
finish by **driving the demo as above** (Playwright MCP runbook, §"Demo + visualizer") — the
suites don't render a canvas or exercise the browser, so a green run does not prove the live
UI path. A quick pass = boot `pnpm demo:linearlite`, drive a write, verify the shape stream and
canvas update, screenshot.

## Invariants (violate these and conformance will catch you — eventually)

- **Postgres is the system of record; the engine's hot path holds no table copy.** Backfills read
  matching rows in a `REPEATABLE READ` snapshot. (The always-on circuit tier
  holds disk-spillable *derived* state — table arrangements + counts pipelines — rebuildable,
  with Postgres fallback for lookups, never the record of truth; see `ARCHITECTURE.md` §6b.)
- **Backfill↔live is fenced by xid visibility, NOT by LSN.** Every seeded structure carries a
  `pg::SnapshotGate` (from `pg_current_snapshot()`); a replicated change is skipped iff its xid was
  visible to that snapshot. `commit_lsn < seed_lsn` is only the fallback for changes without an xid.
  If you add a read path, use the gate. (Why: a commit's WAL record exists before it becomes
  snapshot-visible; LSN comparison drops rows in that window and duplicates at the boundary.)
- **Ingest is at-least-once; consumers restore exactly-once effect.** The ingestor stamps
  `(commit lsn, xid, seq)`; the sequencer de-duplicates by `(lsn, seq)`. Aggregates and subquery contributor
  weights are NOT idempotent under duplicates — never bypass the highwater.
- **Live shape appends must not drop, and a registered shape's batch is never advanced past without
  either LANDING it or RETIRING the shape.** Use `ds.append_reliable` (retry/backoff). A terminal
  answer — 404/410/`stream-closed` — is *reconciled*, never taken on trust: `append_reliable` asks
  the engine (`Engine::reconcile_gone_shape_stream`, installed on the `DsClient` at construction),
  which retries when the shape is still registered AND `HEAD` finds its stream (the 404 was a proxy's,
  not storage's), and otherwise retires the shape (`Dropped` + close-then-delete + deregister) before
  discarding. Discarding while the shape stays registered is how a committed Postgres change goes
  missing forever with nothing left that remembers it. The sequencer's processed offset is published
  only after the whole batch landed, and each source transaction's appends are flushed before the next
  transaction is processed (per-transaction atomic emission).
- **An append whose FAILURE would retire an acknowledged shape retries first** (`ds.append_retrying`;
  activation's aggregate re-seed, the circuit-aggregate seed, a dormant shape's replay). Those errors
  reach `apply_catalog`/`ensure_active`, which drop and retire the record — so a plain `ds.append`
  there turns one transient 503 during a boot into the permanent loss of a live subscription. Retry
  transient (`ds::is_unavailable`) with backoff, bounded by a budget and the shutdown token; retire
  only on a definite refusal, a `head` that confirms the stream is gone, or an exhausted budget.
- **A transaction is one unit of visibility even when it is several appends** (ADR-0003). A commit
  larger than `ELECTRIC_CIRCUITS_CHANGES_APPEND_BYTES` reaches the change log as several appends, and
  durable-streams exposes each atomically — so splitting a read page into transactions by
  `(txid, lsn)` alone would flush a fraction of a commit to the shape streams. The rule on the wire is
  the **transaction-end marker**: `headers.last = true` on the LAST envelope of every transaction, and
  only there. Every producer sets it (the ingestor on the final envelope of the final chunk, including
  single-chunk commits; `toTableEnvelope` on each library-mode write, which is a one-envelope
  transaction), and the sequencer HOLDS a trailing run that no marker terminates — carrying it into
  the next read and publishing nothing (not `processed`, not the checkpoint, not the deletion floor,
  not a dormant shape's resume position) past the page that run began in. Two sharp edges: the
  "already held, skip it" `seq` filter applies to the **leading run of a page and only while it is
  the held transaction** (a page-wide filter silently drops acknowledged transactions whose seqs are
  lower — a reconnect re-delivers complete commits ahead of the interrupted one), and a page that
  completes one held run and starts another must **re-pin to its own page**, or the checkpoint
  freezes for the whole catch-up. If you add a producer, stamp the marker; if you add a change-log
  reader that cares about transactions, hold like the sequencer does. A transaction's ingest-side
  ack (`update_applied_lsn`), `last_lsn` and the drain-barrier sentinel come **only after the last
  chunk landed** — never from inside the chunk loop. Between decoded transactions, a server
  keepalive may advance only `update_applied_lsn`; it never publishes `last_lsn` or the sentinel.
- **The sequencer's de-dup highwater is checkpointed with its position** (`Offset { pos, highwater }`),
  and written whenever **either** moves — while a hold pins the position, the highwater still advances
  on the transactions completed before it. A crash can leave a prefix of a chunked commit applied and
  checkpointed while the rest is re-delivered; aggregate and subquery weights are not idempotent, so
  the position alone is not a restart point.
- **The INGESTOR's memory is bounded, and transaction size never invalidates anything** (ADR-0003).
  Its per-transaction buffer holds `Envelope` structs (nothing is serialized on the way in) and spills
  to a temp file past `ELECTRIC_CIRCUITS_TXN_MEMORY_BYTES` (`txn_buffer.rs`), so its peak is that cap
  plus one chunk. That bound is the ingestor's alone — the sequencer's read page, held run and
  `txn_pending` are bounded by a transaction's size. There is no "transaction too large" branch: no
  shape is retired, nothing is purged, nothing is refused for being big. If you add work between
  `Begin` and `Commit`, it must not accumulate unbounded state of its own.
- **Schema drift, `TRUNCATE` and a replica-identity regression retire that table's dependents**
  (ADR 0005; `engine/drift.rs`). The compiled schema carries a fingerprint (`attnum`-ordered
  `(name, type oid, typmod)` + `relreplident`); a pgoutput `Relation` that disagrees with it, a
  `TRUNCATE`, or the `ELECTRIC_CIRCUITS_SCHEMA_RECONCILE_SECS` reconciler retires every shape on the
  table and every subquery shape referencing it. Drift also re-introspects and swaps the schema in
  all holders (TRUNCATE does not — nothing changed); the ingestor awaits the handling inline, so
  post-DDL DML decodes against the new schema. A create that overlapped a retirement is refused by
  the per-table **schema generation** (bumped in the retirement's own enumeration critical section,
  captured + re-checked by every create/join/reactivation) plus the per-table **resolve lock** (a
  create registering mid-resolution is refused outright). An unsettleable drift parks the table
  **unresolved** (creates refused, changes dropped, retry task running until a re-introspection
  succeeds); the catalog restore retires any shape whose table moved while the engine was down; and
  the circuit-tier restart is gated on the trigger's xid against the boot seed, so an at-least-once
  re-delivery cannot become an exit loop. Never serve stale: no additive tolerance, no whole-engine
  reset.
- **The replication slot is bound to a catalog epoch** (ADR 0004; `engine/epoch.rs`). A `SlotBound
  { system_identifier, timeline_id, slot }` record in the durable catalog names the epoch every shape
  belongs to, and the slot is verified against it before **every** connection — boot and each ingestor
  reconnect. Slot gone, `wal_status = 'lost'`, foreign output plugin, or a different cluster
  `system_identifier` = **epoch break**: the gap cannot be filled, so either every shape is retired
  and a new epoch bound (`ELECTRIC_CIRCUITS_RESET_ON_SLOT_LOSS=true`, the default), or the engine
  fails closed with a named reason until `POST /epoch/reset` (`=false`). A slot held by another
  walsender is NOT a break (wait for it) and a changed timeline is recorded, not acted on. Never
  recreate the slot silently: a fresh slot at the WAL head with shapes still being served is the
  exact failure the ADR exists to prevent — which is also why a durable catalog the engine cannot
  READ at boot is fatal (an unreadable log is not an epoch-less one), why the reset drains its
  `Dropped` records to storage BEFORE creating the new slot, and why the break stays latched until
  the new `SlotBound` is written. A create that overlapped a reset is refused (`resetting`) or rolled
  back by the epoch component of `SchemaGens` — the whole-engine twin of the per-table schema
  generation.
- **The change log is segmented; never append to a closed segment** (ADR 0006; `engine/src/changelog.rs`).
  The log is `changes/0`, `changes/1`, … — there is no bare `changes` stream. The ingestor rotates at
  a transaction boundary by bytes or age: create the successor, append ONE control envelope naming
  it, close the predecessor, record `ChangesRotated`. Control envelopes carry
  `type: "__circuits.control"` and are skipped **by type, unconditionally** by every reader (never by
  position — an abandoned rotation leaves a pointer mid-segment; `__circuits` is a reserved schema so
  no tracked table can spell it), so they can never reach `exec_for`, an arrangement or a shape
  stream. Every change-log position is a `LogPosition { segment, offset }` — the sequencer's
  checkpoint, a dormant shape's resume state, `GET /tables/{name}/offset` — and comparing bare
  offsets across segments is wrong. Readers cross a boundary only on **closed AND drained**, and step
  to **exactly** the next segment (the writer is the only thing that walks to the first open one). A
  rotated-out segment is deleted only once the **durable** checkpoint (the last `Offset` that reached
  the catalog, not the in-memory position) is past it and no shape pins it — a dormant shape pinning
  past `ELECTRIC_CIRCUITS_CHANGES_RETAIN_SECS` is evicted first, a reactivating one is never evicted
  mid-replay, and the plan is recomputed after the evictions so a skipped one cannot license a
  delete. The current segment is never deleted, and a boot refuses to start (exit 78) when any segment
  from its restored position through the current one is missing; a live read that later finds its
  segment gone requests a restart (exit 75), and that boot re-checks the chain.
- **Engine-initiated retirement closes the stream, then deletes it** (`ds.retire_stream`; ADR 0007):
  purge, eviction, drop-at-restore, schema drift, the degraded subquery reap. The close releases a tailing
  long-poll at once with `stream-closed`. Closing is terminal, so the non-retirement paths never
  close — a parked dormant shape's stream must stay appendable, and a rolled-back create's stream
  had no subscriber (plain `delete_stream`).
- **The catalog writer never drops a lifecycle event, and the records a client is *promised* are durable
  before it is answered** (`engine/catalog.rs`). A failed append is classified with
  `ds::is_unavailable`: transport/timeout/5xx retries THAT event in place, forever, with backoff
  (100 ms → 5 s) — the queue is ordered and single-consumer, so everything behind it waits, which is
  the point. A definite refusal (a 4xx, an event that will not serialize) exits `74` (`EX_IOERR`),
  because an engine whose memory and durable record disagree has no honest way to continue and only a
  re-fold at boot can reconcile them — but only **once the catalog stream exists in this process**:
  until the creating PUT has succeeded, an append failure can just as well be "the stream is not
  there yet", so every failure is retried (a permanently-4xx PUT is retried forever, and says so).
  In Postgres mode, an `Offset` below the durable position/highwater floor is not appended; its
  durability barrier is acknowledged because the newer floor already covers it. An `Offset` before
  the floor is known requests a restart (standalone exit `75`) without an append. Library mode does
  not adopt or enforce this floor.
  The rule for which records wait: **durable-before-ack = every record a CLIENT is told about** —
  `Created` and the `Joined` of a NEW claim (`send_durable`, awaited before the HTTP answer, no
  timeout — a create while storage is down waits rather than handing back a shape a restart would
  forget), and the `Left`/`Dropped` of a native `DELETE`, whose success response has to mean the
  release or the purge survives a restart even under `ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS=0`, where no
  lease will ever repair it. A retry of an idempotent removal finds its mutation already applied,
  enqueues nothing and waits on the same barrier (`CatalogWriter::wait_durable`) rather than
  answering from memory. **Queued-never-dropped = lifecycle work the engine does to itself**: a lease
  RENEWAL's `Joined` (that claim is already in the log), and the removals of drift, `TRUNCATE`, the
  epoch reset, retention and the `/v1/shape` adapter — those have their own completion barriers, and
  the lease still reconverges them (the restore brings the shape back with its subscriptions' **lease
  ages**, so an unrenewed claim is released within one sweep — ADR-0008). If you add a mutation whose
  acknowledgement PROMISES something — that a shape exists, or that it is gone — use `send_durable`,
  and do the work that follows the wait in a spawned task: a client that times out drops the request
  future, and the promise must not be dropped with it (`purge_shape_inner`).
- **A durability wait is an interval: re-check the closing conditions AFTER it, before answering**
  (`Engine::recheck_after_durability`). `send_durable` is unbounded external I/O, so anyone who can
  make storage slow can hold a create/join open while a `TRUNCATE`, a schema drift, an epoch reset or
  a purge retires the very shape it is about to acknowledge — the pre-wait checks cannot see any of
  it. Every `Created`/`Joined` await is therefore followed by the same closing check (degradation
  latch, captured schema generations, epoch generation) **plus** "is the record still registered, on
  the same stream?" — a purge moves no generation, so registration is checked directly. One helper,
  six callers; a mismatch unwinds through the ordinary rollback (`CreateGuard`, or giving a join's
  provisional refcount back) and is typed `CreateRaced`, which the create redoes up to
  `CREATE_RACE_ATTEMPTS` times before answering 503. Never acknowledge a handle whose shape was
  retired during the wait. The same rule governs a JOIN's target stream: one `HEAD` per join, and a
  stale registry entry (storage lost the stream) is retired so the caller gets a fresh shape.
- **A retirement completes, eventually — durable intent, durable completion** (ADR 0007;
  `engine/retirement.rs`). `Dropped { id }` is written BEFORE the retirement at every site and
  `Retired { id }` only after storage accepted the delete, so a `Dropped` with no `Retired` is exactly
  "this stream must still go". Every `retire_stream` failure enqueues on the background retirement
  queue (retries 500 ms → 5 s until it lands), and every boot re-queues the fold's unmatched intents —
  which is also the orphan-`shape/*` GC, bounded by the catalog because durable-streams has no list
  API. `GET /shapes/{id}` is 404 from the moment the record goes, pending retirement or not. If you
  add a retirement site, write `Dropped` first and go through `Engine::retire_shape_stream`.
- **A shape id is never re-minted, and the surviving records are not the high-water mark.** The boot
  resumes `next_shape_id` past the maximum id of every `Created` the catalog fold saw, dropped ones
  included (`CatalogFold::max_shape_id`), before the `is_empty()` early return and in `Park` alike. A
  dropped shape's id stays spoken for while its `shape/sN` stream is still being retired: re-minting
  it would PUT the new shape onto the dead one's stream (`ensure_stream` is idempotent, so it
  succeeds), backfill alongside rows the new predicate never matched, and then let the pending
  retirement close and delete the LIVE shape's stream — leaving a registered shape whose every append
  is `Gone`.
- **Subqueries: emit outer membership *absolutely*** — per touched pk, `upsert` if the row matches
  *now* else idempotent `delete`. Flip-driven query-backs run deferred on the flip-propagator task
  (out of commit order relative to the sequencer), so delta-based emission would miss move-outs.
  Symptom when wrong: op-by-op converges, *batched* mutations diverge.
- **NULL flips re-derive any dependent whose `IN` leaf is negated OR under a `Not{…}`** (edge
  `null_sensitive`). Plain-`IN` dependents can't change (monotonicity over FALSE<UNKNOWN<TRUE).
- **Shape creation is atomic.** On any failure, everything (record, share entries, registry
  refcounts/edges, stream) rolls back and the error propagates — including to joiners waiting on the
  share's ready-watch. Never leave a signature pointing at a dead stream.
- **Sharing lifecycle:** equal shapes share one id+stream, held by a SET of named subscriptions; N
  joiners each release exactly once. The final release does NOT delete anything: the shape stays
  active/warm and is retired by the retention lifecycle (idle → dormant → evicted;
  `engine/src/retention.rs`). Client `close()` is still one-shot, but a repeat is now harmless (see
  the subscription invariant below).
- **A subscription is an identity, and it is a lease** (ADR-0008; `POST /shapes { subscription }`,
  `DELETE /shapes/{id}?subscription=…`). The caller names its claim, so **repeating a create is a
  renewal** (same handle, nothing counted) and **repeating a release is a no-op** — the two
  ambiguous outcomes of an HTTP mutation (a response lost after the server committed) stop being
  corruption. An id held by a different shape is refused 409; an omitted one is minted by the engine
  and returned, with no idempotency on that first create; a `DELETE` with no id is the legacy
  anonymous decrement (not retry-safe, and it takes a minted claim before a named one). The `~` on a
  minted id is a **marker, not a reserved namespace**: it exists only so that anonymous `DELETE`
  tie-break can prefer it, and the engine never checks whether it minted a given `~` id — any
  well-formed id is accepted from any caller, so a returned minted id stays usable after its claim
  lapses, after it is released and across a restart, with nothing remembered to make that true. (A
  caller that names a `~` id of its own only makes its own claim the expendable one.) And because
  native reads go straight to durable-streams where the engine cannot see them, the renewal IS the
  liveness signal: a claim not renewed within `ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS` is released by the
  retention sweeper exactly as an explicit delete would release it (`Left { lapsed: true }`), and
  the shape then follows the ordinary lifecycle. If you add a create path, thread the subscription
  through it; if you add a release path, name the claim. A provisional claim taken before an await
  needs a guard (`JoinGuard`/`CreateGuard`): a client that disconnects mid-wait must not leave one
  behind, and the compensation is safe precisely because it names its own id.
- **Every catalog event carries an `eid`, and the fold applies each one at most once** (ADR-0008).
  The id is assigned at ENQUEUE (`CatalogWriter::send*`), so every append attempt of one event —
  the first and every retry after a lost response — carries the same one; the boot fold keeps the
  set it has seen and skips repeats. This is what makes "the writer never drops a lifecycle event" safe for
  records whose effect is not naturally idempotent. A catalog event without an `eid` (or a
  shape-lifecycle event without a `subscription`) is boot-fatal, like the pre-ADR-0002 and
  pre-ADR-0006 formats — greenfield, no compat.
- **A backfill is never materialised.** Every Postgres backfill streams off a `query_raw` cursor and
  is consumed **chunk by chunk** (`pg::BackfillReader`, bounded by
  `ELECTRIC_CIRCUITS_BACKFILL_APPEND_BYTES`), so engine memory per backfill is one chunk however wide
  the table. A plain shape appends each chunk to its still-pending stream; an aggregate folds each
  chunk into an `AggSeed` (via the same `fold_agg_row` the live path uses) and drops the rows. The
  ONLY legitimate `BackfillReader::collect` callers are the ones whose *result* is an in-memory set
  with nothing to stream it to — a subquery inner-set node's seed, a membership query-back's
  candidate rows. If you add a backfill site, take chunks; if you find yourself building a
  `Vec<Row>` of a whole table, that is the bug. The same rule governs stream folds: `/v1/shape`'s
  snapshot must materialise (the body *is* every row), the key set it rebuilds for a catch-up must
  not (`StreamFold::up_to` is keys-only by construction).
- **`SIGTERM` drains; it never retires anything** (`src/shutdown.rs`). Order: `/ready` → 503
  `shutting_down` FIRST (so a load balancer drains) and the port stays open for the drain window;
  stop accepting; the ingestor completes a commit it is *appending* and records its position
  LOCALLY — the wire ack rides the replication client's status interval (1 s) and is not forced on
  the way out, so the last second's commits are re-delivered and de-duplicated by the sequencer
  highwater; mid-transaction it just stops, having appended nothing. **Shutdown never advances the
  slot.** The sequencer then completes its batch and flushes. It emits a final `Offset` checkpoint
  only if it completed a read and replay is not paused; the catalog writer applies the Postgres-mode
  floor check above and drains accepted writes; exit 0. **Shape streams are left untouched**
  — closing a stream means "this shape is gone, re-subscribe", and a restart is not that. Every
  select that could block past the grace must join the shutdown token (the sequencer's change-log
  long-poll, the ingestor's `recv` and its backoff, `poll_live_until`); if you add a long-poll, join
  it, and if you add a task that must reach a safe point, register a `party`.
- **Shapes vs subset queries stay distinct.** Ranges/`orderBy`/`limit` live ONLY in subset queries
  (never live-tailed); a `changes_only` feed uses a passthrough gate and the client reads from the
  offset captured *before* the page snapshot.
- **Aggregations follow SQL NULL semantics** (ignore NULLs; `COUNT(col)` = non-NULLs; empty
  SUM/AVG/MIN/MAX = NULL). Extended API only — the Electric surface doesn't cover them.
- Do not commit or push unless the Git Policy below and the user explicitly authorize it. If
  authorized for a parallel task, use its pinned task branch/worktree; never author on the shared
  integration checkout.

## Gotchas (know these before touching the respective areas)

- **`pg_current_wal_lsn()` is not a visibility fence.** A commit's WAL record exists (and the LSN
  moves past it) before the transaction becomes visible to snapshots — the gap includes a WAL fsync.
  The xid gate (`pg_current_snapshot()`) decides both the dropped-row and boundary-duplicate cases
  exactly. See `pg.rs::SnapshotGate`.
- **The client should still release what it creates** (`track()` in `packages/client/src/index.ts`
  is the pattern), but the engine now has a server-side reaper: an unpaired create pins the shape
  active only until the retention sweeper's idle/dormancy/eviction layers retire it
  (`engine/src/retention.rs`). A leaked refcount (double create, missed release) DOES still pin a
  shape active forever — releases must stay paired one-to-one with creates.
- **Backfill and replication must produce byte-identical text values.** `to_jsonb(t)` renders
  timestamps ISO-`T`-style; pgoutput text-mode tuples use Postgres text output. Same cell, different string →
  broken retractions/routing/MIN-MAX. Backfill casts text-mapped columns with `::text`
  (`pg.rs::row_json_expr`). If you add a read path, match it.
- **Ingest is streaming pgoutput** (walsender protocol via `pgwire-replication` + our own
  `pgoutput.rs` decoder, text-mode tuples, never the `binary` option — binary values would break
  the byte-identity above). The slot is `pgoutput`-plugin. **Current development setup** creates a
  `<slot>_pub` `FOR ALL TABLES` publication and therefore needs superuser; this is not the production
  PG18 contract. Production bootstrap owns an explicit table publication and runtime DDL is refused.
- **Read raw stream envelopes, not stream-db's reconciled view, when you need every delta.** A
  subset's live feed must apply *move-outs*; stream-db no-ops a delete for a key it never inserted.
  (`packages/client/subset.ts` reads raw `StreamEnvelope`s.)
- **Deletes must leave tombstones across the page/live seam.** An in-flight `loadMore` whose
  snapshot predates a delete would resurrect the row (or insert a ghost for a never-seen pk) unless
  the per-pk watermark survives the delete. (`subset.ts` keeps LSN tombstones, pruned when no page
  is in flight.)
- **Shape rows stringify the primary key** (TanStack DB keys are strings); non-pk ints stay numbers
  *unless they exceed 2^53* (see below). Normalize ids when cross-referencing shape rows against
  query-back rows.
- **The envelope `key` must be an injective encoding of the primary-key tuple.** Single-column keys
  are the bare value string; composite keys escape each component (`\` → `\\`, U+001F → `\x1f`) before
  joining with U+001F (`schema::key_string` / `join_key_components`, mirrored in `replication.rs`).
  Raw joining made `('x','y␟z')` and `('x␟y','z')` the same key, and `translate_output` de-duplicates
  by it — one legal row silently disappeared. If you add a key-building site, use the shared helper.
- **An `int` beyond 2^53 is a decimal STRING on the wire, not a rounded number** (`Value::to_json`;
  the rule aggregates' `SUM` already used). Postgres `bigint` outruns a JSON number in every
  JavaScript parser. It applies to every serialised row value — envelopes, `/query`, subset pages,
  MIN/MAX of an int column — so TypeScript sees `number | string` for `int` and `Value::from_json`
  takes the string back. Consumers of the envelope JSON (pgxsinkit) must widen.
- **Text ordering in subsets is code-point order, not the database collation.** The subset page's
  `ORDER BY`, the keyset cursor's range comparisons and the client's window comparator must all
  agree, and the client can only compare the strings it got — so collatable text columns carry an
  explicit `COLLATE "C"` in SQL (`pg::order_term`, `sql::build`) and the client compares code points,
  not UTF-16 code units (`cmpCodePoints`). Equality is never collated (byte equality under any
  deterministic collation), which keeps it index-eligible. Two limits: the collation is emitted only
  for a KNOWN collatable Postgres type (`TableSchema::is_collatable_text`) — a JSON-declared schema
  has no type to check and `COLLATE "C"` on a uuid is an error, so the guarantee needs introspection
  — and `COLLATE "C"` on `<`/`>` cannot use a default-collation btree index, so on a non-`C` database
  an ordered subset over a large table wants an expression index `((col COLLATE "C"))`.
- **A subset whose predicate folds in live UI filters re-creates the engine feed on every click.**
  Prefer per-facet feeds reused across filter changes + a client merge (identical predicates across
  users ⇒ shared engine families). LinearLite's browse list does this.
- **The demo boots an _ephemeral_ Postgres each run** (`mkdtemp`); data does not persist. **Kill
  stale demos before restarting** — a leftover `tsx start.ts`/`caddy` keeps the ports and serves
  stale code, which reads as a mysterious schema mismatch. `scripts/linearlite.sh stop`, or
  `pkill -f electric-circuits-engine`, `pkill -f "tsx start.ts"`, `pkill -f caddy`. If two demos run,
  scope kills by port (`ps -o ppid= -p $(lsof -ti :<httpsPort>)`) — a SIGKILL mid-shutdown leaks
  the ephemeral Postgres.
- **Vite binds IPv6 `[::1]` only** — prefer the `https://localhost:8443` Caddy proxy (HTTP/2 also
  dodges the browser's ~6-connection HTTP/1.1 cap that freezes multi-stream apps). **The pipeline
  visualizer needs its Caddy front too** (`https://localhost:5443`, auto-started by the demo): its
  `/trace` SSE + engine polling compete for the same connection budget, so open it over HTTPS in a
  browser — plain `http://localhost:5180` is for `curl` only. See `.claude/skills/run-linearlite`.
- **The durable-streams server is the Rust binary** (crates.io `durable-streams`, spawned by the
  drop-in wrapper `packages/ds-rust`; self-provisions via `cargo install`, override with
  `DS_RUST_BIN`). Appends are group-commit WAL `fdatasync` — no per-append fsync ceiling like the
  old Node test server. `DS_MEMORY=1` still works (ephemeral data dir; `--durability memory` is
  Linux-only, macOS falls back to `wal`).
- **Docker + pnpm:** scripts that import workspace deps must live in a workspace package
  (`docker/package.json`) — running `tsx docker/x.ts` from the repo root can't resolve them.
- **Verify against the live stack, not just types.** `pnpm typecheck` proves the types line up; a
  headless `tsx` script driving the real client against a running demo catches what it can't.
  **Changing code means realigning docs in the same pass.**

## Git Policy

Do not commit or push unless explicitly asked. At handoff, report changed files, validation run,
and suggested next commands.
