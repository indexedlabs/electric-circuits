// The Swift SDK's real collection stack against the real engine, Postgres and durable-streams:
// one test for each root cause behind the app's sync failures, asserting the fixed behaviour.
//
// Each test spawns the SDK's probe (see `swift-probe.ts`), changes rows in Postgres and compares
// what the SDK's store holds with a SQL query for the same scope. The probe reports every network
// request it makes, so a test can count feed creates, snapshot queries, group loads and group
// re-joins. The scopes are shaped like the app's: a week window on a `timestamptz`, a soft-delete
// column and an access check that is a subquery (`conformance-subset-feed-scope.test.ts`).
//
//   1. A live change on a subquery scope is applied in place.
//   2. A relaunch resumes a single scope and a group with no snapshot and no member query.
//   3. A graceful engine restart keeps the same streams; a crash costs one snapshot.
//   4. A row the client holds only from its snapshot is removed when it leaves the scope.
//   5. An edit on the window's first UTC day arrives, whatever the database's time zone.
//   6. A group stream's nested-subquery ordering case ends on what Postgres holds.
//   7. An idle client's streams are retired and its next launch snapshots once; a create refused
//      at the shape cap is retried and succeeds after a release.
//
// Every numbered test here fails against the engine at `1be10fb` (production's commit), except
// the crash control in 3. What the same SDK did there is recorded by this file's earlier version,
// kept in the checkout of that commit. The last test writes the SDK's
// `Tests/Fixtures/engine-live-envelopes.json` when asked to (`ECS_SWIFT_PROBE_FIXTURES=<path>`).
//
// Build the probe first: `swift build --product ElectricCircuitsSwiftProbe` in the SDK checkout.
// Run: CARGO_PROFILE_DEV_DEBUG=0 COREPACK_ENABLE_DOWNLOAD_PROMPT=0 corepack pnpm exec vitest run \
//   packages/conformance/src/conformance-swift-client.test.ts
// `ECS_SWIFT_PROBE_EVIDENCE=<dir>` keeps every probe's event lines.

import { execFileSync } from 'node:child_process'
import { writeFileSync } from 'node:fs'
import { createServer, type AddressInfo } from 'node:net'

import type { Predicate, Row, Schema } from '@electric-circuits/protocol'
import pgpkg from 'pg'
import { afterEach, describe, expect, it } from 'vitest'

import { lockTable, pgQuery, tableLockWaiters, waitFor } from './engine-native.js'
import { bootHarness, changesTail, drainEngine, engineChangesOffset, positionReached, type Harness } from './harness.js'
import { probeStoreDir, SwiftProbe, type ProbeCounters, type ProbeEvent, type ProbeScope } from './swift-probe.js'

const schema: Schema = {
  tables: {
    records: { columns: { id: { type: 'int' }, visible: { type: 'bool' } }, primaryKey: 'id' },
    events: {
      columns: {
        id: { type: 'int' },
        record_id: { type: 'int' },
        starts: { type: 'text' },
        deleted_at: { type: 'text' },
        name: { type: 'text' },
      },
      primaryKey: 'id',
    },
  },
}

const ddl = `
  CREATE TABLE records (id int PRIMARY KEY, visible boolean NOT NULL);
  ALTER TABLE records REPLICA IDENTITY FULL;
  CREATE TABLE events (
    id int PRIMARY KEY,
    record_id int NOT NULL,
    starts timestamptz NOT NULL,
    deleted_at timestamptz,
    name text NOT NULL
  );
  ALTER TABLE events REPLICA IDENTITY FULL;
`

// One week, Monday to Monday at midnight Pacific, with its bounds spelled as the app spells them.
const window: Predicate[] = [
  { col: 'starts', op: 'gte', value: '2026-10-19T07:00:00Z' },
  { col: 'starts', op: 'lt', value: '2026-10-26T07:00:00Z' },
  { col: 'deleted_at', isNull: true },
]
const visibleRecord: Predicate = { col: 'record_id', in: { table: 'records', project: 'id', where: { col: 'visible', op: 'eq', value: true } } }
/** The app's scope: the window, the soft-delete column and access as a subquery. */
const week: Predicate = { and: [...window, visibleRecord] }
/** The same window without the access check: a predicate with no subquery. */
const plainWeek: Predicate = { and: window }

const weekScope: ProbeScope = { name: 'week', table: 'events', key: 'id', where: week }
const plainScope: ProbeScope = { name: 'plain', table: 'events', key: 'id', where: plainWeek }
const groupScope: ProbeScope = {
  name: 'group',
  group: [
    { table: 'events', key: 'id', where: week },
    { table: 'records', key: 'id', where: { col: 'visible', op: 'eq', value: true } },
  ],
}

// The same scopes in SQL: what Postgres says is in them, independently of the engine's predicate.
const windowSql = `starts >= '2026-10-19T07:00:00Z' AND starts < '2026-10-26T07:00:00Z' AND deleted_at IS NULL`
const sql = {
  week: `SELECT id, name FROM events WHERE ${windowSql} AND record_id IN (SELECT id FROM records WHERE visible) ORDER BY id`,
  plain: `SELECT id, name FROM events WHERE ${windowSql} ORDER BY id`,
  records: 'SELECT id FROM records WHERE visible ORDER BY id',
}

let h: Harness | undefined
let stores: ReturnType<typeof probeStoreDir> | undefined
const probes: SwiftProbe[] = []

afterEach(async () => {
  for (const probe of probes.splice(0)) await probe.kill()
  await h?.shutdown()
  h = undefined
  stores?.cleanup()
  stores = undefined
})

/** A port nothing is listening on, for an engine that must come back at the same address. */
async function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer()
    server.on('error', reject)
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address() as AddressInfo
      server.close(() => resolve(port))
    })
  })
}

interface BootOptions {
  engineEnv?: Record<string, string>
  /** The database's default time zone (`ALTER DATABASE … SET timezone`). */
  zone?: string
  /** Bind the engine to one port, so a probe that outlives an engine restart finds it again. */
  fixedAddress?: boolean
}

async function boot(opts: BootOptions = {}): Promise<Harness> {
  const engineEnv = { ...(opts.engineEnv ?? {}) }
  if (opts.fixedAddress) engineEnv.ELECTRIC_CIRCUITS_BIND = `127.0.0.1:${await freePort()}`
  const zone = opts.zone
  // Plain HTTP to durable-streams: the engine hands the probe a stream URL it can read directly.
  h = await bootHarness(schema, {
    ddl,
    dsTransport: 'http',
    engineEnv,
    ...(zone
      ? {
          beforeEngine: async ({ pgUrl }: { pgUrl: string }) => {
            const client = new pgpkg.Client({ connectionString: pgUrl })
            await client.connect()
            try {
              await client.query(`DO $$ BEGIN EXECUTE format('ALTER DATABASE %I SET timezone TO ''${zone}''', current_database()); END $$`)
            } finally {
              await client.end()
            }
          },
        }
      : {}),
  })
  stores = probeStoreDir()
  await pgQuery(h, 'INSERT INTO records (id, visible) VALUES (1, true), (2, true), (3, false), (4, true)')
  // Every instant is written with its offset, so the session zone of this INSERT is irrelevant.
  await pgQuery(
    h,
    `INSERT INTO events (id, record_id, starts, name) VALUES
       (10, 1, '2026-10-19 16:00:00+00', 'first day'),
       (11, 1, '2026-10-21 16:00:00+00', 'midweek'),
       (12, 2, '2026-10-22 16:00:00+00', 'on record two'),
       (13, 3, '2026-10-23 16:00:00+00', 'on a hidden record'),
       (14, 1, '2026-10-24 16:00:00+00', 'to soft-delete'),
       (15, 1, '2026-10-25 16:00:00+00', 'to move'),
       (16, 1, '2026-10-20 16:00:00+00', 'to delete'),
       (18, 4, '2026-10-22 18:00:00+00', 'on record four'),
       (20, 1, '2026-11-05 16:00:00+00', 'another week')`,
  )
  await drainEngine(h)
  return h
}

async function startProbe(scopes: ProbeScope[], label: string): Promise<SwiftProbe> {
  const probe = await SwiftProbe.start({ engineUrl: h!.engineUrl, store: `${stores!.dir}/store.json`, scopes, label })
  probes.push(probe)
  return probe
}

function note(title: string, value: unknown): void {
  console.log(`[swift-client] ${title}: ${JSON.stringify(value)}`)
}

/** A row as Postgres returns it or as the probe's store holds it. */
type StoredRow = Record<string, unknown>
const eventLabels = (rows: StoredRow[]) => rows.map((row) => `${Number(row.id)}:${String(row.name)}`)
const recordLabels = (rows: StoredRow[]) => rows.map((row) => `${Number(row.id)}`)

type Held = Record<string, string[]>

/** What Postgres says each named scope holds. */
async function inPostgres(scopes: string[]): Promise<Held> {
  const held: Held = {}
  for (const scope of scopes) {
    if (scope === 'group') {
      held['group.events'] = eventLabels(await pgQuery(h!, sql.week))
      held['group.records'] = recordLabels(await pgQuery(h!, sql.records))
    } else {
      held[scope] = eventLabels(await pgQuery(h!, sql[scope as 'week' | 'plain']))
    }
  }
  return held
}

/** What the SDK's store holds for each named scope. It asks the probe, never Postgres. */
async function inStore(probe: SwiftProbe, scopes: string[]): Promise<Held> {
  const held: Held = {}
  for (const scope of scopes) {
    if (scope === 'group') {
      const tables = await probe.groupRows('group')
      held['group.events'] = eventLabels(tables.events ?? [])
      held['group.records'] = recordLabels(tables.records ?? [])
    } else {
      held[scope] = eventLabels(await probe.rows(scope))
    }
  }
  return held
}

/** Every failure the probe reported from event index `from` on. */
function failures(probe: SwiftProbe, from = 0): Array<Record<string, unknown>> {
  return probe.of('failure', undefined, from).map((e) => ({ scope: e.scope, phase: e.phase, error: e.error, cause: e.cause }))
}

/**
 * The store comes to hold `expected` without any scope failing on the way. It stops waiting at
 * the first failure, so a scope the SDK gave up on is reported as that, not as a timeout.
 */
async function expectStore(probe: SwiftProbe, expected: Held, what: string, from = 0): Promise<void> {
  const scopes = [...new Set(Object.keys(expected).map((name) => name.split('.')[0]!))]
  try {
    await waitFor(async () => failures(probe, from).length > 0 || JSON.stringify(await inStore(probe, scopes)) === JSON.stringify(expected), what, 15000)
  } catch {
    // Fall through: the assertions below say what is wrong.
  }
  expect(failures(probe, from), `${what}: a scope failed`).toEqual([])
  expect(await inStore(probe, scopes), what).toEqual(expected)
}

/** After `change`, the SDK's store must come to equal what Postgres says is in each scope. */
async function expectScopes(probe: SwiftProbe, scopes: string[], what: string, change?: () => Promise<unknown>): Promise<void> {
  const from = probe.mark()
  await change?.()
  // The barrier: replication has caught up and every flip the change caused has been propagated,
  // so whatever the streams are going to say about it is already on them.
  await drainEngine(h!)
  await expectStore(probe, await inPostgres(scopes), what, from)
}

async function live(probe: SwiftProbe, scopes: string[]): Promise<void> {
  for (const scope of scopes) {
    await probe.waitFor((e) => e.scope === scope && (e.event === 'failure' || (e.event === 'state' && e.state === 'live')), `scope ${scope} to be live`)
  }
  expect(failures(probe), 'a scope failed before it was live').toEqual([])
}

const paths = (probe: SwiftProbe, scope: string, from = 0) => probe.of('path', scope, from).map((e) => e.path as string)
const requests = (probe: SwiftProbe, scope: string, kind: string, from = 0) => probe.of('request', scope, from).filter((e) => e.kind === kind)
const changes = (probe: SwiftProbe, scope: string, from = 0) =>
  probe
    .of('change', scope, from)
    .map((e) => `${e.op as string} ${e.table ? `${e.table as string} ` : ''}${e.key as string}`)
    .sort()

/** Backends waiting on a lock on either table a member query reads. */
async function memberQueries(): Promise<number[]> {
  return [...(await tableLockWaiters(h!, 'events')), ...(await tableLockWaiters(h!, 'records'))]
}

async function shapeStatus(id: string): Promise<number> {
  const res = await fetch(`${h!.engineUrl}/shapes/${encodeURIComponent(id)}`)
  await res.text()
  return res.status
}

/** Stop the engine with `SIGTERM`, run `whileDown`, and start it again at the same address. */
async function restartGracefully(whileDown: () => Promise<void>): Promise<void> {
  h!.signalEngine('SIGTERM')
  const exit = await h!.waitForEngineExit(30000)
  expect(exit.code, `the engine must stop cleanly (got ${JSON.stringify(exit)})`).toBe(0)
  await whileDown()
  await h!.startEngine()
  await drainEngine(h!)
}

describe('1. a live change on a subquery scope', () => {
  it('is applied in place by the real client: no feed failure and no new snapshot', async () => {
    const h = await boot()
    const probe = await startProbe([weekScope, groupScope], 'cause1')
    await live(probe, ['week', 'group'])
    await expectScopes(probe, ['week', 'group'], 'the snapshots to load the week')
    const before = await probe.counters()
    expect(before.week).toMatchObject({ feedCreate: 1, snapshotQuery: 1 })
    expect(before.group).toMatchObject({ groupLoad: 1, snapshotQuery: 0 })

    const beforeChange = probe.mark()
    await expectScopes(probe, ['week', 'group'], 'an edit to a row in the scope to be applied', () => pgQuery(h, `UPDATE events SET name = 'midweek (edited)' WHERE id = 11`))

    // The SDK applied the engine's envelope, at the position the engine gave it.
    const applied = probe.of('change', 'week', beforeChange)
    expect(applied.map((e) => ({ op: e.op, key: e.key, name: (e.row as Row | undefined)?.name }))).toEqual([{ op: 'upsert', key: '11', name: 'midweek (edited)' }])
    expect(applied[0]!.lsn).toMatch(/^[0-9A-F]+\/[0-9A-F]+$/)
    const envelope = requests(probe, 'week', 'streamRead', beforeChange).flatMap((e) => (e.envelopes ?? []) as Array<{ headers: Row }>)[0]!
    expect(envelope.headers.lsn, 'the subquery feed positions its envelope').toBe(applied[0]!.lsn)
    expect(changes(probe, 'group', beforeChange)).toEqual(['upsert events 11'])

    // In place: the scopes stayed live and asked the engine for nothing but the stream.
    expect(probe.state('week')).toBe('live')
    expect(probe.state('group')).toBe('live')
    const after = await probe.counters()
    note('counters after a live change on a subquery scope', after)
    expect(after.week).toMatchObject({ feedCreate: 1, snapshotQuery: 1, release: 0 })
    expect(after.group).toMatchObject({ groupLoad: 1, groupRejoin: 0, snapshotQuery: 0, release: 0 })
    expect(paths(probe, 'week')).toEqual(['snapshot'])
    expect(paths(probe, 'group')).toEqual(['snapshot'])
  }, 120000)
})

describe('2. a new process on the same store', () => {
  it.each(['released', 'killed'] as const)('resumes a single scope and a group with no snapshot and no member query on the engine (first process %s)', async (ending) => {
    const h = await boot()
    const first = await startProbe([weekScope, groupScope], `relaunch-${ending}-first`)
    await live(first, ['week', 'group'])
    await expectScopes(first, ['week', 'group'], 'the snapshots to load the week')
    if (ending === 'released') {
      // A clean stop: the SDK releases its claims, the group's by the group's own id.
      const released = await first.stop({ release: true })
      expect(released.week!.release).toBe(1)
      expect(released.group!.release).toBe(1)
    } else {
      // A killed app exits holding its claims.
      await first.kill()
    }

    // While no process is running: an edit, an arrival, a removal and both kinds of access change.
    await pgQuery(h, `UPDATE events SET name = 'midweek (edited)' WHERE id = 11`)
    await pgQuery(h, `INSERT INTO events (id, record_id, starts, name) VALUES (30, 1, '2026-10-22 09:00:00+00', 'arrived')`)
    await pgQuery(h, `UPDATE events SET deleted_at = now() WHERE id = 14`)
    await pgQuery(h, `UPDATE records SET visible = false WHERE id = 2`)
    await pgQuery(h, `UPDATE records SET visible = true WHERE id = 3`)
    await drainEngine(h)
    const expected = await inPostgres(['week', 'group'])
    expect(expected.week).toEqual(['10:first day', '11:midweek (edited)', '13:on a hidden record', '15:to move', '16:to delete', '18:on record four', '30:arrived'])

    // Every table a member's query reads is locked for the whole relaunch. A snapshot query, a
    // group load or any other read of the scope's rows on the engine would park on these locks.
    const locks = [await lockTable(h, 'events'), await lockTable(h, 'records')]
    let second: SwiftProbe
    try {
      second = await startProbe([weekScope, groupScope], `relaunch-${ending}-second`)
      const isLive = (scope: string) => second.state(scope) === 'live'
      await waitFor(
        async () => (isLive('week') && isLive('group')) || failures(second).length > 0 || (await memberQueries()).length > 0,
        'the relaunched scopes to be live, or a member query to reach Postgres',
        30000,
      )
      expect(await memberQueries(), 'the engine ran a query on a member table for the relaunch').toEqual([])
      // The store holds what Postgres held before the locks, read from the streams alone.
      await expectStore(second, expected, 'the relaunched store to hold what happened while it was down')
      expect(await memberQueries(), 'the engine ran a query on a member table for the relaunch').toEqual([])
    } finally {
      for (const lock of locks) await lock.release()
    }

    // Both continued from the stored cursor: no snapshot query, no group load, no frontier read.
    expect(paths(second, 'week')).toEqual(['resume'])
    expect(paths(second, 'group')).toEqual(['resume'])
    expect(second.of('snapshot')).toEqual([])
    const counters = await second.counters()
    note(`second process counters (first ${ending})`, counters)
    expect(counters.week).toMatchObject({ feedCreate: 1, snapshotQuery: 0, streamHead: 0 })
    expect(counters.group).toMatchObject({ groupRejoin: 1, groupLoad: 0, snapshotQuery: 0, streamHead: 0 })
    const stored = second.events[0]!.stored as Record<string, Row>
    for (const scope of ['week', 'group']) {
      expect(requests(second, scope, 'streamRead')[0]!.offset, `${scope} read on from its stored cursor`).toBe(stored[scope]!.cursor)
    }
    // What it applied is what happened while it was down, each from the stream.
    expect(changes(second, 'week')).toEqual(['delete 12', 'delete 14', 'upsert 11', 'upsert 13', 'upsert 30'])
    expect(changes(second, 'group')).toEqual(['delete events 12', 'delete events 14', 'delete records 2', 'upsert events 11', 'upsert events 13', 'upsert events 30', 'upsert records 3'])

    // And it is live on the same streams from here on.
    await expectScopes(second, ['week', 'group'], 'a change after the relaunch to be applied', () => pgQuery(h, `UPDATE events SET name = 'first day (edited)' WHERE id = 10`))
    expect((await second.counters()).week).toMatchObject({ snapshotQuery: 0 })
  }, 120000)
})

describe('3. an engine restart', () => {
  it('graceful, during a session and between sessions: the same streams continue with no snapshot', async () => {
    const h = await boot({ fixedAddress: true })
    const first = await startProbe([weekScope, groupScope], 'restart-graceful-first')
    await live(first, ['week', 'group'])
    await expectScopes(first, ['week', 'group'], 'the snapshots to load the week')

    // During the session: the client keeps tailing its streams while the engine is gone.
    const duringSession = first.mark()
    await restartGracefully(async () => {
      await pgQuery(h, `UPDATE events SET name = 'midweek (edited)' WHERE id = 11`)
      await pgQuery(h, `INSERT INTO events (id, record_id, starts, name) VALUES (30, 1, '2026-10-22 09:00:00+00', 'arrived')`)
      await pgQuery(h, `UPDATE events SET deleted_at = now() WHERE id = 14`)
      await pgQuery(h, `UPDATE records SET visible = false WHERE id = 2`)
    })
    await expectStore(first, await inPostgres(['week', 'group']), 'the running client to hold what happened while the engine was down', duringSession)
    expect(first.state('week')).toBe('live')
    expect(first.state('group')).toBe('live')
    const running = await first.counters()
    note('running client counters across a graceful restart', running)
    expect(running.week).toMatchObject({ feedCreate: 1, snapshotQuery: 1, release: 0 })
    expect(running.group).toMatchObject({ groupLoad: 1, snapshotQuery: 0, release: 0 })
    expect(paths(first, 'week')).toEqual(['snapshot'])
    expect(paths(first, 'group')).toEqual(['snapshot'])
    expect(h.engineStderr(), 'the restore dropped a subquery shape').not.toContain('dropping subquery shape')

    // Between sessions: the process is gone when the engine restarts, and comes back after.
    await first.kill()
    await restartGracefully(async () => {
      await pgQuery(h, `UPDATE events SET name = 'first day (edited)' WHERE id = 10`)
      await pgQuery(h, `DELETE FROM events WHERE id = 16`)
      await pgQuery(h, `UPDATE records SET visible = true WHERE id = 3`)
    })
    const second = await startProbe([weekScope, groupScope], 'restart-graceful-second')
    await live(second, ['week', 'group'])
    await expectStore(second, await inPostgres(['week', 'group']), 'the relaunched client to hold what happened while the engine was down')
    expect(paths(second, 'week')).toEqual(['resume'])
    expect(paths(second, 'group')).toEqual(['resume'])
    const relaunched = await second.counters()
    note('relaunched client counters after a graceful restart', relaunched)
    expect(relaunched.week).toMatchObject({ feedCreate: 1, snapshotQuery: 0, streamHead: 0 })
    expect(relaunched.group).toMatchObject({ groupRejoin: 1, groupLoad: 0, snapshotQuery: 0 })
    expect(second.of('snapshot')).toEqual([])
  }, 180000)

  // The control: a crash writes nothing on the way out, so the engine drops its subquery shapes
  // and the client is told so. It passes at `1be10fb` too.
  it('after a kill -9: the client is told its streams are gone, takes one snapshot and ends equal to Postgres', async () => {
    const h = await boot({ fixedAddress: true })
    const probe = await startProbe([weekScope, groupScope], 'restart-crash')
    await live(probe, ['week', 'group'])
    await expectScopes(probe, ['week', 'group'], 'the snapshots to load the week')
    const before = await probe.counters()

    const crashed = probe.mark()
    await h.restartEngine(async () => {
      await pgQuery(h, `UPDATE events SET name = 'midweek (edited)' WHERE id = 11`)
      await pgQuery(h, `UPDATE records SET visible = false WHERE id = 2`)
    })
    await drainEngine(h)
    // Each scope's stream was retired by the restore, and its session ends there.
    for (const scope of ['week', 'group']) {
      await probe.waitFor((e) => e.event === 'failure' && e.scope === scope, `the ${scope} scope to learn its stream is gone`, { from: crashed })
    }
    await probe.waitForState('week', 'failed(sourceUnavailable)', crashed)
    await probe.waitForState('group', 'failed', crashed)

    // The app's next attempt: one snapshot each, and the store is Postgres again.
    const retry = probe.mark()
    probe.send({ cmd: 'acquire', scope: 'week' })
    probe.send({ cmd: 'acquire', scope: 'group' })
    await probe.waitForState('week', 'live', retry)
    await probe.waitForState('group', 'live', retry)
    await expectStore(probe, await inPostgres(['week', 'group']), 'the store to equal Postgres after the snapshot', retry)
    expect(paths(probe, 'week', retry)).toEqual(['resume-declined', 'snapshot'])
    expect(paths(probe, 'group', retry)).toEqual(['resume-declined', 'snapshot'])
    const after = await probe.counters()
    note('counters after a crash and one retry', after)
    expect(after.week!.snapshotQuery! - before.week!.snapshotQuery!).toBe(1)
    expect(after.group!.groupLoad! - before.group!.groupLoad!).toBe(1)
    expect(probe.of('snapshot', 'week', retry)).toHaveLength(1)
    expect(probe.of('snapshot', 'group', retry)).toHaveLength(1)
  }, 180000)
})

describe('4. a row the client holds only from its snapshot', () => {
  it('is removed when it is soft-deleted, moved out of the window, deleted or loses access', async () => {
    const h = await boot()
    // The plain scope first: at `1be10fb` it is the one whose envelopes the client can apply.
    const probe = await startProbe([plainScope, weekScope, groupScope], 'cause4')
    const all = ['plain', 'week', 'group']
    await live(probe, all)
    await expectScopes(probe, all, 'the snapshots to load the week')
    const loaded = probe.mark()

    // The feeds have delivered none of these rows: they opened empty, and nothing has changed.
    await expectScopes(probe, all, 'a soft-deleted snapshot row to leave', () => pgQuery(h, `UPDATE events SET deleted_at = now() WHERE id = 14`))
    await expectScopes(probe, all, 'a snapshot row moved to another week to leave', () => pgQuery(h, `UPDATE events SET starts = starts + interval '14 days' WHERE id = 15`))
    await expectScopes(probe, all, 'a deleted snapshot row to leave', () => pgQuery(h, `DELETE FROM events WHERE id = 16`))
    // The row is untouched: its Record stops being visible, which only the subquery scopes see.
    await expectScopes(probe, all, 'a snapshot row whose Record is hidden to leave', () => pgQuery(h, `UPDATE records SET visible = false WHERE id = 2`))

    expect(changes(probe, 'plain', loaded)).toEqual(['delete 14', 'delete 15', 'delete 16'])
    expect(changes(probe, 'week', loaded)).toEqual(['delete 12', 'delete 14', 'delete 15', 'delete 16'])
    expect(changes(probe, 'group', loaded)).toEqual(['delete events 12', 'delete events 14', 'delete events 15', 'delete events 16', 'delete records 2'])
    expect((await inStore(probe, ['week'])).week).toEqual(['10:first day', '11:midweek', '18:on record four'])
    // Each removal came down the feed: no scope took another snapshot to learn it.
    const counters = await probe.counters()
    note('counters after four removals of snapshot-only rows', counters)
    expect(counters.plain).toMatchObject({ feedCreate: 1, snapshotQuery: 1 })
    expect(counters.week).toMatchObject({ feedCreate: 1, snapshotQuery: 1 })
    expect(counters.group).toMatchObject({ groupLoad: 1, snapshotQuery: 0 })
  }, 180000)
})

describe('5. an edit on the window’s first UTC day', () => {
  it('reaches the client on a database whose default time zone is not UTC', async () => {
    const h = await boot({ zone: 'America/Los_Angeles' })
    expect((await pgQuery(h, 'SHOW timezone'))[0]!.TimeZone, 'the database default is in force').toBe('America/Los_Angeles')
    // The plain scope first: at `1be10fb` it is the one whose envelopes the client can apply.
    const probe = await startProbe([plainScope, weekScope], 'cause5')
    const both = ['plain', 'week']
    await live(probe, both)
    await expectScopes(probe, both, 'the snapshots to load the week')
    expect((await inStore(probe, ['plain'])).plain).toContain('10:first day')
    const loaded = probe.mark()

    // Event 10 starts at 16:00Z on 2026-10-19, the date of the window's lower bound.
    await expectScopes(probe, both, 'an edit on the window’s first UTC date to reach the client', () => pgQuery(h, `UPDATE events SET name = 'first day (edited)' WHERE id = 10`))
    // A row entering on that date is an arrival the feed alone can report.
    await expectScopes(probe, both, 'a row created on the window’s first UTC date to reach the client', () =>
      pgQuery(h, `INSERT INTO events (id, record_id, starts, name) VALUES (30, 1, '2026-10-19 08:00:00+00', 'early')`),
    )
    // And one just outside the lower bound stays out: 06:59Z is before 07:00Z.
    await expectScopes(probe, both, 'a row before the lower bound to stay out', () =>
      pgQuery(h, `INSERT INTO events (id, record_id, starts, name) VALUES (31, 1, '2026-10-19 06:59:00+00', 'too early')`),
    )

    expect(changes(probe, 'plain', loaded)).toEqual(['upsert 10', 'upsert 30'])
    expect(changes(probe, 'week', loaded)).toEqual(['upsert 10', 'upsert 30'])
    // The row the client holds is spelled as the snapshot spelled it, in UTC.
    const starts = (scope: string) => probe.of('change', scope, loaded).map((e) => (e.row as Row).starts)
    expect(starts('plain')).toEqual(['2026-10-19 16:00:00+00', '2026-10-19 08:00:00+00'])
    expect(starts('week')).toEqual(starts('plain'))
    const counters = await probe.counters()
    expect(counters.plain).toMatchObject({ feedCreate: 1, snapshotQuery: 1 })
    expect(counters.week).toMatchObject({ feedCreate: 1, snapshotQuery: 1 })
  }, 120000)
})

describe('7. retention', () => {
  it('retires an idle client’s streams after the live-only idle period, and its next launch takes exactly one snapshot', async () => {
    const h = await boot({
      engineEnv: {
        ELECTRIC_CIRCUITS_RETENTION_SWEEP_SECS: '1',
        ELECTRIC_CIRCUITS_SUBSCRIPTION_LEASE_SECS: '3',
        ELECTRIC_CIRCUITS_LIVE_ONLY_IDLE_SECS: '6',
      },
    })
    const first = await startProbe([weekScope, groupScope], 'idle-first')
    await live(first, ['week', 'group'])
    await expectScopes(first, ['week', 'group'], 'the snapshots to load the week')
    const feedId = requests(first, 'week', 'feedCreate')[0]!.shapeId as string
    const memberIds = (requests(first, 'group', 'groupLoad')[0]!.members as Array<{ shapeId: string }>).map((member) => member.shapeId)
    expect(memberIds).toHaveLength(2)

    // While it runs the SDK renews inside the three-second lease: nothing is retired under a
    // client that is there.
    await waitFor(async () => ((await first.counters()).group!.groupRejoin ?? 0) >= 2, 'the group’s lease to be renewed twice', 20000)
    const renewing = await first.counters()
    note('a running client’s counters over two lease renewals', renewing)
    expect(await shapeStatus(feedId)).toBe(200)
    for (const id of memberIds) expect(await shapeStatus(id)).toBe(200)

    // The app is killed: no release, no renewal. The lease lapses, then the idle period passes.
    await first.kill()
    await pgQuery(h, `UPDATE events SET name = 'midweek (edited)' WHERE id = 11`)
    await pgQuery(h, `UPDATE events SET deleted_at = now() WHERE id = 14`)
    await pgQuery(h, `UPDATE records SET visible = false WHERE id = 2`)
    await drainEngine(h)
    await waitFor(async () => (await shapeStatus(feedId)) === 404, 'the idle feed to be retired', 30000)
    await waitFor(async () => (await Promise.all(memberIds.map(shapeStatus))).every((status) => status === 404), 'the idle group to be retired', 30000)

    // The next launch finds neither stream, and each scope costs exactly one snapshot.
    const second = await startProbe([weekScope, groupScope], 'idle-second')
    await live(second, ['week', 'group'])
    await expectStore(second, await inPostgres(['week', 'group']), 'the relaunched store to equal Postgres')
    expect(paths(second, 'week')).toEqual(['resume-declined', 'snapshot'])
    expect(paths(second, 'group')).toEqual(['resume-declined', 'snapshot'])
    expect(second.of('snapshot', 'week')).toHaveLength(1)
    expect(second.of('snapshot', 'group')).toHaveLength(1)
    // The re-join found no group and created none; one load then read the group.
    expect(requests(second, 'group', 'groupRejoin')[0]!.status).toBe(412)
    const counters = await second.counters()
    note('second process counters after its streams were retired', counters)
    expect(counters.week).toMatchObject({ feedCreate: 1, snapshotQuery: 1 })
    expect(counters.group).toMatchObject({ groupLoad: 1, snapshotQuery: 0 })
    // And the first process's renewals were re-joins: its setup load stayed the only one.
    expect(renewing.group).toMatchObject({ groupLoad: 1 })
    expect(requests(first, 'group', 'groupRejoin').map((e) => e.status)).not.toContain(412)
  }, 180000)

  it('at the shape cap a create is refused 503 with Retry-After, and the client retries and loads after a release', async () => {
    const h = await boot({ engineEnv: { ELECTRIC_CIRCUITS_MAX_SHAPES: '1', ELECTRIC_CIRCUITS_RETENTION_SWEEP_SECS: '1' } })
    const feed = (subscription: string) =>
      fetch(`${h.engineUrl}/v1/subset-feeds`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ table: 'records', where: { col: 'visible', op: 'eq', value: true }, subscription }),
      })
    // Another client holds the engine's one shape.
    const holder = await feed('holder')
    expect(holder.status).toBe(200)
    const held = (await holder.json()) as { shapeId: string }

    // The scope's feed create is refused: the request is fine, and the engine says when to retry.
    const probe = await startProbe([plainScope], 'cap')
    const refused = await probe.waitFor((e) => e.event === 'request' && e.scope === 'plain' && e.kind === 'feedCreate', 'the scope’s first feed create to be answered')
    expect(refused.status, 'the engine refuses a create at its cap').toBe(503)
    const direct = await fetch(`${h.engineUrl}/v1/subset-feeds`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ table: 'events', where: plainWeek, subscription: 'someone-else' }),
    })
    expect(direct.status).toBe(503)
    expect(direct.headers.get('retry-after')).toMatch(/^[1-9][0-9]*$/)
    await direct.text()

    // The holder goes away. The client's next retry, which nothing here asks for, takes the room.
    const release = await fetch(`${h.engineUrl}/v1/shapes/${encodeURIComponent(held.shapeId)}?subscription=holder`, { method: 'DELETE' })
    expect(release.status).toBe(200)
    await live(probe, ['plain'])
    await expectStore(probe, await inPostgres(['plain']), 'the scope to load once the engine has room')
    const statuses = requests(probe, 'plain', 'feedCreate').map((e) => e.status)
    note('feed create answers at the shape cap', statuses)
    expect(statuses[0]).toBe(503)
    expect(statuses.at(-1)).toBe(200)
    expect(statuses.slice(0, -1).every((status) => status === 503), 'every answer before the last was a refusal').toBe(true)
    expect(paths(probe, 'plain')).toEqual(['snapshot'])
    expect((await probe.counters()).plain).toMatchObject({ snapshotQuery: 1 })
  }, 120000)
})

// A group stream and nested subqueries: `conformance-group-evaluation-order.test.ts` has the case
// against raw stream reads. This is the same interleaving with the real client reading the stream.
describe('6. a group stream whose nested flip’s verdict follows a newer change to the same key', () => {
  const nestedSchema: Schema = {
    tables: {
      orgs: { columns: { id: { type: 'int' }, active: { type: 'bool' } }, primaryKey: 'id' },
      grants: { columns: { id: { type: 'int' }, org_id: { type: 'int' }, ws: { type: 'int' } }, primaryKey: 'id' },
      items: { columns: { id: { type: 'int' }, ws: { type: 'int' }, title: { type: 'text' } }, primaryKey: 'id' },
    },
  }
  /** `items.ws IN (SELECT ws FROM grants WHERE org_id IN (SELECT id FROM orgs WHERE active))`. */
  const inActiveGrants: Predicate = {
    col: 'ws',
    in: {
      table: 'grants',
      project: 'ws',
      where: { col: 'org_id', in: { table: 'orgs', project: 'id', where: { col: 'active', op: 'eq', value: true } } },
    },
  }
  const inActiveGrantsSql = `
    SELECT id, title FROM items
     WHERE ws IN (SELECT ws FROM grants WHERE org_id IN (SELECT id FROM orgs WHERE active))
     ORDER BY id`
  const nested: ProbeScope = { name: 'nested', group: [{ table: 'items', key: 'id', where: inActiveGrants }] }
  const titles = (rows: StoredRow[]) => rows.map((row) => `${Number(row.id)}:${String(row.title)}`)

  /** The sequencer has processed every commit so far; deferred flips may still be parked. */
  async function sequenced(h: Harness): Promise<void> {
    const [sentinel] = await pgQuery(h, 'UPDATE __el_sync SET n = n + 1 WHERE id = 1 RETURNING n')
    await waitFor(async () => {
      const res = await fetch(`${h.engineUrl}/replication/lsn`)
      return res.ok && Number(((await res.json()) as { sync: number }).sync) >= Number(sentinel!.n)
    }, 'replication to reach the sentinel')
    const tail = await changesTail(h.dsUrl, h.engineUrl)
    if (!tail) return
    await waitFor(async () => {
      const at = await engineChangesOffset(h.engineUrl)
      return at === null || positionReached(at, tail)
    }, 'the sequencer to reach the change-log tail')
  }

  it('ends on what Postgres holds in the real client', async () => {
    h = await bootHarness(nestedSchema, { dsTransport: 'http' })
    stores = probeStoreDir()
    const harness = h
    await pgQuery(harness, 'INSERT INTO orgs (id, active) VALUES (1, true), (2, true)')
    await pgQuery(harness, 'INSERT INTO grants (id, org_id, ws) VALUES (1, 1, 10), (2, 2, 20)')
    await pgQuery(harness, `INSERT INTO items (id, ws, title) VALUES (1, 10, 'a'), (2, 10, 'b'), (3, 20, 'c')`)
    await drainEngine(harness)
    const probe = await startProbe([nested], 'cause6')
    await live(probe, ['nested'])
    expect(titles((await probe.groupRows('nested')).items ?? [])).toEqual(['1:a', '2:b', '3:c'])
    const loaded = probe.mark()

    // Deactivate org 1 with its re-derivation of the grants set parked on a lock, commit an edit
    // to item 1 and let the engine evaluate it (against the grants set the first commit made
    // stale), then release the lock so the OLDER commit's verdict is evaluated last.
    const lock = await lockTable(harness, 'grants')
    try {
      await pgQuery(harness, 'UPDATE orgs SET active = false WHERE id = 1')
      await waitFor(async () => (await tableLockWaiters(harness, 'grants')).length > 0, 'the re-derivation of the grants set to park on the lock')
      await pgQuery(harness, `UPDATE items SET title = 'a2' WHERE id = 1`)
      await sequenced(harness)
    } finally {
      await lock.release()
    }
    await drainEngine(harness)

    const expected = titles(await pgQuery(harness, inActiveGrantsSql))
    expect(expected).toEqual(['3:c'])
    try {
      await waitFor(async () => failures(probe, loaded).length > 0 || JSON.stringify(titles((await probe.groupRows('nested')).items ?? [])) === JSON.stringify(expected), 'the group to end on what Postgres holds', 15000)
    } catch {
      // Fall through: the assertions below say what is wrong.
    }
    expect(failures(probe, loaded), 'the group failed').toEqual([])
    expect(titles((await probe.groupRows('nested')).items ?? []), 'the client kept a row Postgres does not hold').toEqual(expected)
    // The stale live verdict was applied, and the verdict that corrects it came after it.
    const applied = probe.of('change', 'nested', loaded).map((e: ProbeEvent) => `${e.op as string} ${e.key as string}`)
    note('nested group changes in the order the client applied them', applied)
    expect(applied.indexOf('upsert 1')).toBeGreaterThanOrEqual(0)
    expect(applied.lastIndexOf('delete 1')).toBeGreaterThan(applied.indexOf('upsert 1'))
    expect(probe.state('nested')).toBe('live')
    const counters: ProbeCounters = await probe.counters()
    expect(counters.nested).toMatchObject({ groupLoad: 1, snapshotQuery: 0 })
  }, 120000)
})

/** Every envelope on a stream from its start, read straight from durable-streams. */
async function readStream(streamUrl: string): Promise<{ envelopes: unknown[]; offsets: string[] }> {
  const envelopes: unknown[] = []
  const offsets: string[] = []
  let offset = '-1'
  for (;;) {
    const res = await fetch(`${streamUrl}?offset=${encodeURIComponent(offset)}`)
    if (res.status === 204) break
    if (!res.ok) throw new Error(`GET ${streamUrl} -> ${res.status}`)
    const body = (await res.text()).trim()
    const batch = body ? (JSON.parse(body) as unknown[]) : []
    const next = res.headers.get('stream-next-offset')
    if (batch.length === 0 || !next || next === offset) break
    envelopes.push(...batch)
    offsets.push(next)
    offset = next
  }
  return { envelopes, offsets }
}

describe('the live envelopes the SDK’s fixtures pin', () => {
  // The SDK's `Tests/Fixtures/engine-live-envelopes.json` is this test's output at the engine
  // commit it names: run it with ECS_SWIFT_PROBE_FIXTURES=<path> to write the file again.
  it('records what a plain feed, a subquery feed and a group stream carry for the same changes: every envelope is positioned', async () => {
    const listsSchema: Schema = {
      tables: {
        lists: { columns: { id: { type: 'int' }, owner: { type: 'text' } }, primaryKey: 'id' },
        items: { columns: { id: { type: 'int' }, list_id: { type: 'int' }, title: { type: 'text' } }, primaryKey: 'id' },
      },
    }
    const inMyLists: Predicate = { col: 'list_id', in: { table: 'lists', project: 'id', where: { col: 'owner', op: 'eq', value: 'u1' } } }
    const plain: ProbeScope = { name: 'plain', table: 'items', key: 'id', where: { col: 'list_id', op: 'eq', value: 1 } }
    const subquery: ProbeScope = { name: 'subquery', table: 'items', key: 'id', where: inMyLists }
    const mixedGroup: ProbeScope = {
      name: 'mixedGroup',
      group: [
        { table: 'lists', key: 'id', where: { col: 'owner', op: 'eq', value: 'u1' } },
        { table: 'items', key: 'id', where: inMyLists },
      ],
    }
    h = await bootHarness(listsSchema, { dsTransport: 'http' })
    stores = probeStoreDir()
    const harness = h
    await pgQuery(harness, "INSERT INTO lists (id, owner) VALUES (1, 'u1'), (2, 'u2')")
    await pgQuery(harness, "INSERT INTO items (id, list_id, title) VALUES (1, 1, 'one'), (2, 1, 'two'), (3, 2, 'three')")
    await drainEngine(harness)
    const probe = await startProbe([plain, subquery, mixedGroup], 'fixtures')
    await live(probe, ['plain', 'subquery', 'mixedGroup'])
    const streamUrl = (scope: string) => probe.of('request', scope).find((e) => e.streamUrl)!.streamUrl as string

    const steps: Array<[string, string]> = [
      ['update a row in the scope', "UPDATE items SET title = 'one-b' WHERE id = 1"],
      ['insert a row into the scope', "INSERT INTO items (id, list_id, title) VALUES (4, 1, 'four')"],
      ['delete a row the stream delivered', 'DELETE FROM items WHERE id = 4'],
      ['insert a row into the group member that has a plain predicate', "INSERT INTO lists (id, owner) VALUES (3, 'u1')"],
    ]
    for (const [, statement] of steps) {
      await pgQuery(harness, statement)
      await drainEngine(harness)
    }
    // A group stream lands through its own lane, which the drain barrier does not wait for.
    await waitFor(async () => (await readStream(streamUrl('mixedGroup'))).envelopes.length >= 4, 'the group stream to catch up')
    const fixture = {
      engine: execFileSync('git', ['rev-parse', '--short', 'HEAD'], { encoding: 'utf8' }).trim(),
      schema: 'lists(id int, owner text), items(id int, list_id int, title text)',
      steps: steps.map(([what, statement]) => ({ what, statement })),
      plain: { where: (plain as { where: unknown }).where, ...(await readStream(streamUrl('plain'))) },
      subquery: { where: (subquery as { where: unknown }).where, ...(await readStream(streamUrl('subquery'))) },
      group: { members: (mixedGroup as { group: unknown }).group, ...(await readStream(streamUrl('mixedGroup'))) },
    }
    if (process.env.ECS_SWIFT_PROBE_FIXTURES) writeFileSync(process.env.ECS_SWIFT_PROBE_FIXTURES, `${JSON.stringify(fixture, null, 2)}\n`)

    // Every envelope on every stream carries a position, and positions never fall along a stream.
    const position = (lsn: string) => {
      const [hi, lo] = lsn.split('/')
      return (BigInt(`0x${hi}`) << 32n) | BigInt(`0x${lo}`)
    }
    for (const [name, stream] of Object.entries({ plain: fixture.plain, subquery: fixture.subquery, group: fixture.group })) {
      const lsns = stream.envelopes.map((e) => (e as { headers: Row }).headers.lsn)
      expect(stream.envelopes.length, `${name} stream`).toBeGreaterThanOrEqual(3)
      for (const lsn of lsns) expect(lsn, `an envelope on the ${name} stream carries no position`).toMatch(/^[0-9A-F]+\/[0-9A-F]+$/)
      for (let i = 1; i < lsns.length; i++) {
        expect(position(lsns[i] as string) >= position(lsns[i - 1] as string), `${name}: position fell at envelope ${i}`).toBe(true)
      }
    }
    // The SDK applied all of it: the three scopes are live and hold what Postgres holds.
    expect(failures(probe)).toEqual([])
    const titles = (rows: StoredRow[]) => rows.map((row) => `${Number(row.id)}:${String(row.title)}`)
    const inList1 = titles(await pgQuery(harness, 'SELECT id, title FROM items WHERE list_id = 1 ORDER BY id'))
    await waitFor(async () => JSON.stringify(titles(await probe.rows('subquery'))) === JSON.stringify(inList1), 'the subquery scope to hold list 1', 15000)
    expect(titles(await probe.rows('plain'))).toEqual(inList1)
    expect(titles((await probe.groupRows('mixedGroup')).items ?? [])).toEqual(inList1)
  }, 120000)
})
