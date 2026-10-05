# PostgreSQL 18 differential review

Review date: 2026-08-23.

Scope: independent differential review of
[`24-postgres18-and-e2e-tdd-addendum.md`](../24-postgres18-and-e2e-tdd-addendum.md) and its
integration into
[`18-production-readiness-spec-reviewed.md`](../18-production-readiness-spec-reviewed.md), using
[`21-postgres18-support.md`](../21-postgres18-support.md), the current engine implementation, and
only official PostgreSQL 18 documentation for PostgreSQL facts that can change. No product code or
execution-spec file was edited.

## Verdict

**The PostgreSQL 18 choice is technically defensible, but the current packets cannot yet certify the
stated launch contract.** The research correctly identifies the generated-column, slot-invalidation,
and TLS defects and correctly keeps seamless failover out of the first release. The blocker list is
nevertheless incomplete:

1. the launch contract says every promotion/timeline change resets the epoch, while the current code
   deliberately accepts a timeline change and the proposed promotion test can pass merely because a
   non-synchronized slot is absent;
2. a same-name `pgoutput` slot dropped and recreated ahead of the durable input frontier can be
   mistaken for the original slot;
3. the promise that live publication mutation fails before stale serving needs a DDL/ownership fence,
   because an excluded operation produces no wire event for a poller to notice first;
4. neither publication admission nor the PG18 role profile handles row-level security, which can make
   snapshot/query and walsender visibility disagree;
5. generated-column admission is specified only strongly enough for boot, not for live or down-time
   schema/publication changes;
6. the TLS test can fail on the first SQL connector without ever exercising hostname verification in
   the walsender or query-back connector; and
7. `18.x` is promised without an executable minor-upgrade/rollback qualification.

These are correctness and qualification gaps, not reasons to fall back to PostgreSQL 16. The narrow
PG18 single-primary profile remains the right first profile after the task corrections below.

## Severity-ranked findings

### P0-1 — The promotion test does not prove the launch rule, and the qualifying task does not depend on its implementation

**Evidence.** The launch contract says that a primary promotion or timeline change is an epoch break
([production spec lines 45–53](../18-production-readiness-spec-reviewed.md)). The implementation does
the opposite: after checking system identifier, slot presence, plugin, and `wal_status`,
`epoch::verdict` returns `Ok { timeline_changed: true }`
([`epoch.rs` lines 181–206](../../apps/engine/src/engine/epoch.rs)); boot logs that the epoch stands and
that failover handling is deferred ([`epoch.rs` lines 384–405](../../apps/engine/src/engine/epoch.rs)).

`PG18-E2E-009` promotes a standby **without** synchronized logical-slot support
([addendum lines 168–171](../24-postgres18-and-e2e-tdd-addendum.md)). In that fixture the promoted
server will ordinarily have no logical slot, so the existing `SlotLost` branch can reset the epoch
without the timeline change having any effect. That is a false-positive test for the stated rule.
PostgreSQL's official failover procedure says a standby slot is ready only when the required slot is
present and `(synced AND NOT temporary AND invalidation_reason IS NULL)`; slot synchronization is
asynchronous ([PostgreSQL 18 logical-replication failover](https://www.postgresql.org/docs/18/logical-replication-failover.html)).

The DAG compounds the problem. `LEAD-001` and `OPS-004` own promotion/timeline reset
([production spec lines 726–734 and 835–843](../18-production-readiness-spec-reviewed.md)), but
`PG18-003`, which claims the complete `PG18-E2E-001`–`009` matrix and a promotion-reset workflow,
depends on neither task ([production spec lines 822–833](../18-production-readiness-spec-reviewed.md)).

**Required correction.** Change the first-release verdict so `bound.timeline_id !=
observed.timeline_id` is a named epoch break regardless of whether a usable same-name slot exists.
Add a second promotion case with a synchronized, usable same-name `pgoutput` slot on the promoted
standby and require reset anyway; retain the unsynchronized/missing-slot case separately. Make
`PG18-003` depend on `LEAD-001`, `DSR-003`, and `OPS-004`, or split it into an engine/DB qualification
packet and a later production-topology qualification packet that owns the public promotion test.

### P0-2 — “Slot provenance” is not a continuity rule; drop/recreate can skip commits on the same cluster

**Evidence.** `OPS-003B` names slot “provenance” but gives no observable fields, comparison, or test
([production spec lines 812–820](../18-production-readiness-spec-reviewed.md)). Current observation
captures `active`, `active_pid`, `wal_status`, `confirmed_flush_lsn`, and `plugin`
([`pg.rs` lines 665–706](../../apps/engine/src/pg.rs)). The verdict does not compare
`confirmed_flush_lsn` or `restart_lsn`; a slot dropped while the engine is down and recreated under
the same name with `pgoutput` on the same system identifier and timeline is accepted
([`epoch.rs` lines 181–206](../../apps/engine/src/engine/epoch.rs)). If source commits occurred before
recreation, the new slot can begin after them while old shapes remain live-looking.

PostgreSQL exposes slot WAL state and invalidation, but it does not expose a durable user-defined
slot-incarnation nonce in `pg_replication_slots`; continuity must therefore be proved from the
consumer's durable source frontier and slot LSN state, not from name/plugin alone
([PostgreSQL 18 `pg_replication_slots`](https://www.postgresql.org/docs/18/view-pg-replication-slots.html)).

**Required correction.** Replace the word “provenance” in `OPS-003B` with an explicit decision table:
persist the last completely landed input/source frontier, observe at least slot type, database,
temporary/failover/two-phase properties, `restart_lsn`, `confirmed_flush_lsn`, plugin and invalidation,
and accept only a relation proved not to skip that durable frontier. A slot ahead of the durable
frontier is an epoch break; a slot behind may redeliver through the existing highwater; exact-equivalent
recreation may be accepted only if the proof says why it is gap-free. Add a black-box case: stop the
engine at fence A, commit fence B, drop/recreate the same-name slot, restart, and require reset/fail
closed rather than healthy state at A. Add quiet drop/recreate as a separate equivalence/refusal case.

### P0-3 — Live publication mutation cannot be guaranteed safe by inspection after the fact

**Evidence.** `ENG-017` requires a runtime publication change to fail closed and retire affected
shapes, while `PG18-E2E-008` requires the failure before stale serving
([production spec lines 717–724](../18-production-readiness-spec-reviewed.md);
[addendum lines 168–171](../24-postgres18-and-e2e-tdd-addendum.md)). Current inspection runs at boot,
and only rejects a detected column list ([`pg.rs` lines 709–781](../../apps/engine/src/pg.rs)). More
fundamentally, eventual polling cannot uphold the promised ordering: PostgreSQL publications may omit
any of INSERT/UPDATE/DELETE/TRUNCATE, tables may be removed transactionally, and row filters may
suppress rows. An excluded change yields no `pgoutput` message that can retire a shape before it is
already stale ([PostgreSQL 18 publications](https://www.postgresql.org/docs/18/logical-replication-publication.html)).
`ALTER PUBLICATION` is owner-controlled and can change the table set and all publication parameters
([PostgreSQL 18 `ALTER PUBLICATION`](https://www.postgresql.org/docs/18/sql-alterpublication.html)).

**Required correction.** Choose and specify one enforceable production model:

- make the publication immutable while the stack is ready, owned by a bootstrap role unavailable to
  runtime identities, and require the sanctioned publication-change workflow to fence readiness and
  writes/reads before `ALTER PUBLICATION`; or
- install a qualified DDL guard/event mechanism that prevents or synchronously fences publication
  mutation while live.

Keep periodic inspection as detection/diagnostics, not as proof that no stale read preceded detection.
Rewrite `PG18-E2E-008` to attempt mutation with every runtime identity and prove denial; then exercise
the authorized change workflow with readiness already unavailable before DDL and DML. State explicitly
that an unrestricted superuser is outside the runtime threat boundary. Fingerprint the full effective
publication definition: table/partition expansion, row filters, column lists, I/U/D/T flags,
`publish_via_partition_root`, and `publish_generated_columns`.

### P0-4 — The profile omits row-level-security equivalence between snapshot/query-back and pgoutput

**Evidence.** The publication tasks reject publication row filters but never mention table RLS.
`OPS-003A` creates separate least-privilege query and replication roles
([production spec lines 805–810](../18-production-readiness-spec-reviewed.md)), so their visibility can
differ. PostgreSQL documents that when a replication role lacks `SUPERUSER` and `BYPASSRLS`, publisher
row-security policies can execute; if table owners are not trusted, the replication connection should
set `options=-crow_security=off` so a new policy halts replication rather than filtering it
([PostgreSQL 18 logical-replication security](https://www.postgresql.org/docs/18/logical-replication-security.html)).
Normal SQL roles are subject to enabled RLS unless a policy permits them
([PostgreSQL 18 row security](https://www.postgresql.org/docs/18/ddl-rowsecurity.html)). A boot snapshot
can therefore contain fewer rows than live decoding, or live decoding can silently omit rows the
query role returns.

**Required correction.** Add an explicit RLS policy to `PG18-000` and `OPS-003A/B`. The conservative
first-release choice is to reject RLS-enabled tracked tables and set `row_security=off` on the
walsender so later RLS introduction stops rather than filters. If RLS is supported, prove the query,
snapshot/query-back and replication roles have identical intended visibility (for example, a narrowly
controlled `BYPASSRLS` design) and state how gateway tenant policy remains authoritative. Add boot and
live `ENABLE ROW LEVEL SECURITY`/policy-change cases to `PG18-E2E-008`, with both roles and an
unaffected table as control.

### P0-5 — Generated-column admission is safe at boot only; live/down-time changes remain unspecified

**Evidence.** The base facts are correct: PG18 publishes stored generated columns only when
`publish_generated_columns=stored` or an explicit column list nominates them, a column list takes
precedence, and virtual generated columns are not supported
([PostgreSQL 18 generated-column replication](https://www.postgresql.org/docs/18/logical-replication-gencols.html);
[PostgreSQL 18 column lists](https://www.postgresql.org/docs/18/logical-replication-col-lists.html)).
The chosen no-column-list policy plus global `stored` option is therefore conservative and safe.

The task, however, tests only initial tracking and says merely that boot does not acknowledge before
admission ([production spec lines 778–789](../18-production-readiness-spec-reviewed.md)). The current
fingerprint uses one process-global `publish_generated` flag and the predicate
`attgenerated = '' OR publish_generated`, which admits both stored and virtual columns when enabled
([`pg.rs` lines 415–440](../../apps/engine/src/pg.rs)). The schema reconciler can encounter a virtual
column added after readiness, a stored column added while `pubgencols=none`, or a global
`publish_generated_columns` change while the engine is down. A boot-only fix or fingerprint-only fix
can still reinstall a half-schema during drift recovery.

**OTTO-6003 ownership update.** Publication-generated-column state is now held by each Engine and
passed explicitly into fingerprinting and re-introspection. This removes the first-source-wins
problem described above; it does not complete the broader PG18 column-admission requirements below.

**Required correction.** Amend `PG18-001` so the single column-admission function is mandatory on
every boot, create/join/reactivation, live drift re-introspection, reconciler retry, and catalog restore;
make the effective publication manifest an input rather than process-global mutable state. Add cases
that add a virtual column, add an unpublished stored column, and toggle
`publish_generated_columns=stored/none` while live and while the engine is down. The affected table
must retire/stay unresolved and refuse fresh feeds until table plus publication are jointly admissible;
the unaffected-table control must remain live. Include partition root/leaf variants selected by the
production `publish_via_partition_root` policy. PostgreSQL also requires generated replica-identity
columns to be explicitly published for UPDATE/DELETE, so keep the identity negative case
([PostgreSQL 18 `CREATE PUBLICATION`](https://www.postgresql.org/docs/18/sql-createpublication.html)).

### P1-1 — `PG18-E2E-006` conflates real black-box invalidation with synthetic policy coverage

**Evidence.** PostgreSQL 18 documents four current non-null reasons: `wal_removed`, `rows_removed`,
`wal_level_insufficient`, and `idle_timeout`
([PostgreSQL 18 `pg_replication_slots`](https://www.postgresql.org/docs/18/view-pg-replication-slots.html)).
The addendum demands that a real PG18 E2E invalidate the slot for every reason and also that an unknown
future reason fail closed ([addendum lines 161–170 and 297–308](../24-postgres18-and-e2e-tdd-addendum.md)).
An unknown value cannot be produced by PG18, and `rows_removed` is a recovery-conflict case associated
with logical slots on standby, outside the first-release single-primary profile. In contrast,
`idle_timeout` has a clean real fixture, but it triggers only at checkpoint and does not apply to a
slot that does not reserve WAL or to a synchronized standby slot
([PostgreSQL 18 replication settings](https://www.postgresql.org/docs/18/runtime-config-replication.html)).

The dependency also defeats the advertised delivery order: `PG18-002` depends on the full `DSR-003`
reset workflow, while the recommended execution fronts schedule it as an immediate PG18 fix
([addendum lines 297–308 and 403–415](../24-postgres18-and-e2e-tdd-addendum.md)).

**Required correction.** Split `PG18-002A` (parse any non-null reason, latch fail-closed readiness,
stop old-epoch serving; depends on `ENG-006`) from `PG18-002B` (durable auto-reset/rehydration; depends
on `PG18-002A` and `DSR-003`). Use real primary PG18 fixtures for `idle_timeout` and `wal_removed`;
cover `wal_level_insufficient` with a real restart fixture if the package supports it; put
standby-only `rows_removed` in the standby/future-profile lane. Use focused decoder/verdict tests for
all four strings and an unknown string. Keep at least one end-to-end real invalidation for reset-off
and reset-on behavior; do not label synthetic reason injection as black-box PG18 coverage.

### P1-2 — The TLS lane proves the first connector, not each independent connector's verification policy

**Evidence.** The implementation gap is real: ordinary SQL uses `NoTls`, while the walsender URL is
manually reduced and receives `TlsConfig::default()`
([`replication.rs` lines 322–345](../../apps/engine/src/replication.rs)). PostgreSQL documents that
`verify-full` verifies both the CA chain and the requested host name and that weaker/default modes do
not provide the same MITM guarantee
([PostgreSQL 18 SSL support](https://www.postgresql.org/docs/18/libpq-ssl.html)).

The proposed wrong-CA/name boot test can nevertheless pass when only the setup SQL connector enforces
verification: setup fails before a walsender or deferred query-back is attempted. The valid happy path
through `hostssl` proves encryption, but not that each client parsed and enforced the requested host
identity. The reconnect case names only the replication connection and does not say what happens to
readiness and old materialization while verification fails ([addendum lines 168–170](../24-postgres18-and-e2e-tdd-addendum.md);
[support note lines 190–199](../21-postgres18-support.md)).

**Required correction.** Give setup/admin, pool/backfill/query-back, and walsender connections stable
test `application_name`s. While each path is active, assert externally through `pg_stat_ssl` that its
backend or WAL sender has `ssl=true`; PostgreSQL exposes one row per backend/WAL sender for this purpose
([PostgreSQL 18 monitoring](https://www.postgresql.org/docs/18/monitoring.html)). Then target each path
independently with a wrong-CA/wrong-SAN proxy or certificate rotation after the other paths are already
healthy. Require readiness to fence and no public freshness/advancement while the failed connector
cannot authenticate; restoring the correct identity must converge. Add URL/keyword-conninfo parity
tests for CA, SNI/host, client cert/key if supported, percent-escaped credentials, multiple hosts, and
unknown TLS parameters. Either require SCRAM channel binding or record its explicit disposition;
PostgreSQL recommends `channel_binding=require` in addition to verified TLS against server spoofing
([PostgreSQL 18 preventing spoofing](https://www.postgresql.org/docs/18/preventing-server-spoofing.html)).

### P1-3 — `18.x` has no executable minor-upgrade or rollback contract

**Evidence.** `PG18-000` says to record a minor-version process, but its acceptance checks only a PG18
version, PG17 refusal, and future-major refusal ([addendum lines 255–265](../24-postgres18-and-e2e-tdd-addendum.md)).
No PG18 scenario upgrades a running production dataset, slot, publication, TLS role, durable streams,
or clients. `OPS-005` is explicitly catalog/stream/DS upgrade, not PostgreSQL upgrade
([production spec lines 845–850](../18-production-readiness-spec-reviewed.md)).

PostgreSQL documents that minor releases within a major retain the data format and are updated by
replacing executables while the server is down, whereas major upgrades require dump/restore,
`pg_upgrade`, or replication ([PostgreSQL 18 upgrading a cluster](https://www.postgresql.org/docs/18/upgrading.html)).
For logical publisher major upgrades, `pg_upgrade` migrates logical slots only from an old cluster
version 17 or later and imposes WAL-level, slot-count, plugin-allowlist, caught-up and slot-usability
prerequisites ([PostgreSQL 18 logical-replication upgrade](https://www.postgresql.org/docs/18/logical-replication-upgrade.html)).

**Required correction.** Add `PG18-E2E-010` and an operations packet for approved `18.N -> 18.N+1`
updates: drain/fence, stop/start exact provider artifacts on the same data directory, verify unchanged
system/slot/publication/frontier, resume the existing feed exactly once, write before/after markers,
and exercise the documented rollback or declare rollback reset-only. Run both a clean update and a
cut during restart/reconnect. State explicitly whether importing an existing PG16/17 deployment via
`pg_upgrade` is unsupported (then require dump/restore plus whole-generation reset) or supported (then
implement the official slot prerequisites and a separate E2E). Keep unapproved PG19 fail-closed; do
not let that negative test substitute for an 18.x maintenance test.

### P1-4 — The `SourceCommitID` barrier has no public causal path to each materializer

**Evidence.** The addendum requires every compared path to prove it consumed a sentinel row written in
the same source transaction ([addendum lines 94–103](../24-postgres18-and-e2e-tdd-addendum.md)), but it
does not define how a client subscribed to another table/template observes that journal row. Seeing a
sentinel on an independent feed does not prove that the target feed has folded its changes, and a
client's normal row map contains no `SourceCommitID`. The proposed `ReplicationBarrier` therefore
risks either inspecting a private engine offset (forbidden by the black-box rule) or merely waiting for
eventual SQL equality, which is not proof that the named fence was consumed.

`PG18-E2E-002` has a related control gap: “hold creation after its snapshot is fixed” is an internal
phase, but `FaultGate` only names external network/storage/process/cache cuts
([addendum lines 118–125 and 161–165](../24-postgres18-and-e2e-tdd-addendum.md)). No external mechanism
is assigned to establish and announce that exact snapshot point.

**Required correction.** Define one refactor-safe test protocol. For example, add a test-only adapter
that exposes a source-transaction completion watermark only after all target-stream appends and named
deferred phases for that commit have landed, and require each client adapter to acknowledge folding
the target stream through that watermark. Alternatively, constrain the early PG tests to a frozen
source (no later writes) and define a deterministic quiescence/oracle condition, reserving common-fence
cross-client comparisons for a later public barrier protocol. For `PG18-E2E-002`, specify an external
PG/proxy mechanism (blocked backfill statement after transaction snapshot establishment) or explicitly
permit one compile-disabled named test hook; acceptance must prove the gate was hit, not sleep.

### P1-5 — The early adapter and the claimed “public client” acceptance are inconsistent

**Evidence.** `E2E-000` now explicitly permits an isolated direct-engine adapter before the gateway
exists ([production spec lines 1266–1281](../18-production-readiness-spec-reviewed.md)), which is a
reasonable way to drive the PG18 engine fixes red. But `PG18-001` still requires its cases to pass
through “the public client”, and `PG18-003` claims the complete matrix without depending on the
protected public package or `OPS-004` ([production spec lines 778–789 and 822–843](../18-production-readiness-spec-reviewed.md)).
The support note likewise defines the mandatory boundary as PG18 → engine → DS → public client/control
API ([support note lines 169–179](../21-postgres18-support.md)). Agents cannot tell whether a green
direct-engine case closes the public contract.

**Required correction.** Name two gates explicitly:

1. `PG18-ENGINE-*`: real PG18 → engine process → file-backed DS → isolated reference materializer,
   used by `PG18-001/002A` during implementation; and
2. `PG18-PUBLIC-*`: immutable deployment → authenticated gateway → selected real client/cache,
   rerunning every section 4.1 case before release.

Either make `PG18-003` the second gate and depend on `OPS-001B` and `OPS-004`, or add `PG18-004` for
it. Change `PG18-001` acceptance from “public client” to the named engine adapter, while retaining the
unchanged scenario/oracle for the later public rerun.

### P2-1 — Slot-retention policy is observed but not selected

**Evidence.** The profile handles `idle_timeout` after invalidation and says preflight validates WAL
retention, but does not select an admissible `idle_replication_slot_timeout` or
`max_slot_wal_keep_size` relative to the supported engine outage/recovery objective. PostgreSQL notes
that idle invalidation occurs at checkpoint and that a bounded slot WAL allowance can make required
WAL unavailable ([PostgreSQL 18 replication settings](https://www.postgresql.org/docs/18/runtime-config-replication.html)).
A routine deployment longer than an aggressive provider timeout can therefore force a full client
rehydration even though the implementation is correct.

**Required correction.** In `PG18-000` choose either `idle_replication_slot_timeout=0` for the
dedicated production slot or a provider-specific minimum greater than the maximum supported planned
outage plus checkpoint lag; choose a corresponding WAL-retention/disk budget and alert threshold.
Make `OPS-003B` report the effective values and reject a profile that cannot meet the declared outage
envelope. Test just-below/just-above bounds and document the reset outcome above the bound.

## Integration and task-DAG corrections summary

The following changes make the addendum schedulable without weakening its public release gate:

1. Split `PG18-002` into fail-closed detection (`002A`) and durable reset integration (`002B`) so the
   immediate invalidation fix does not wait on the full storage-reset program.
2. Split or retarget `PG18-003`: the engine/DB lane may run early, but the full production lane must
   depend on `OPS-001B`, `LEAD-001`, `DSR-003`, and `OPS-004` and rerun through the gateway/client.
3. Add explicit scenario IDs for same-name slot replacement, synchronized-slot promotion that still
   resets in the first profile, RLS mutation, live/down generated-column mutation, connector-specific
   TLS failure, and PG18 minor upgrade.
4. Amend `ENG-017` from “inspect and detect” to an enforceable publication-immutability/change
   workflow. Keep polling as diagnostics.
5. Replace undefined “slot provenance” with a durable-frontier comparison and decision table.
6. Define a causal, adapter-visible `SourceCommitID` barrier or narrow the early tests to frozen-source
   quiescence.

## Lenses with no adverse finding

- **pgoutput protocol:** no finding. PostgreSQL 18 supports pgoutput protocol versions 1–4, so the
  engine's deliberate v1/spill design does not need a protocol upgrade
  ([PostgreSQL 18 logical streaming replication protocol](https://www.postgresql.org/docs/18/protocol-logical-replication.html)).
- **Base generated-column facts:** no factual finding. Virtual is the PG18 default, only stored
  generated columns are logically publishable, and the two supported publication mechanisms are
  stated correctly. The finding is incomplete lifecycle admission, not an incorrect PostgreSQL fact.
- **Slot invalidation facts:** no factual finding. The four documented reasons and the rule that any
  non-null reason must fail closed are correct. The finding is test scoping/actionability.
- **`output_plugin_libraries`:** no finding. PG18 defaults to `pgoutput, test_decoding`, refuses plugins
  not in the allowlist, and the proposed negative boot test is appropriate
  ([PostgreSQL 18 replication settings](https://www.postgresql.org/docs/18/runtime-config-replication.html)).
- **Future synchronized failover-slot recipe:** no factual finding. `failover=true`, standby
  `sync_replication_slots=true`, primary `synchronized_standby_slots`, and the documented standby
  readiness predicate are correctly identified. The first-release promotion test still needs the
  synchronized-slot negative/control described in P0-1, and seamless continuation remains properly
  excluded.
- **SCRAM direction:** no finding. PG18 deprecates MD5 password authentication and SCRAM is the
  appropriate first-production password profile
  ([PostgreSQL 18 authentication settings](https://www.postgresql.org/docs/18/runtime-config-connection.html)).

## Final disposition

Retain PostgreSQL 18 as the only first-production database profile, but do not mark the PG18 addendum
fully integrated or delegate `PG18-003` as currently written. Close P0-1 through P0-5 in the task
definitions first, then make the P1 acceptance and dependency corrections before implementation work
is used as release evidence.
