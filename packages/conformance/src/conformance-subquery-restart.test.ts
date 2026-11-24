// A graceful restart keeps subquery shapes: the SAME stream, read from the offset the client
// saved, ends equal to Postgres — with no new snapshot.
//
// Until this change the catalog restore dropped every subquery shape (and every group with a
// subquery member): the inner-set nodes that decide membership lived only in memory, and a node
// re-seeded from Postgres cannot tell which values entered or left while the engine was down. So
// each stream was closed and deleted, and every client re-created its scope and re-read all of it
// — on every deploy, for every client at once.
//
// The required behavior writes the subquery registry (inner sets with their contributors, each shape's
// key set and stream position) to durable storage at a graceful stop, names it from the final
// checkpoint, and restores it before it replays the change log from that checkpoint. The replay is
// the ordinary live path, so what happened while the engine was down is derived exactly as it would
// have been live — including the case a re-seed gets wrong: a row moved to another scope, whose old
// scope then loses access.
//
// A crash is unchanged: nothing was written, so the shapes are dropped and the read reports the
// stream gone. The last test is that control.

import type { Predicate, Row, Schema, StreamEnvelope } from '@electric-circuits/protocol'
import { afterEach, describe, expect, it } from 'vitest'

import { createShape, lockTable, pgQuery, tableLockWaiters, waitFor } from './engine-native.js'
import { bootHarness, drainEngine, type Harness } from './harness.js'

const schema: Schema = {
  tables: {
    records: { columns: { id: { type: 'int' }, visible: { type: 'bool' } }, primaryKey: 'id' },
    events: {
      columns: {
        id: { type: 'int' },
        record_id: { type: 'int' },
        deleted_at: { type: 'text' },
        name: { type: 'text' },
      },
      primaryKey: 'id',
    },
    notes: {
      columns: { id: { type: 'int' }, record_id: { type: 'int' }, body: { type: 'text' } },
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
    deleted_at timestamptz,
    name text NOT NULL
  );
  ALTER TABLE events REPLICA IDENTITY FULL;
  CREATE TABLE notes (id int PRIMARY KEY, record_id int NOT NULL, body text NOT NULL);
  ALTER TABLE notes REPLICA IDENTITY FULL;
`

/** Access is the inner table: a Record is in scope while it is visible. */
const onVisibleRecord: Predicate = {
  col: 'record_id',
  in: { table: 'records', project: 'id', where: { col: 'visible', op: 'eq', value: true } },
}
const liveEvents: Predicate = { and: [{ col: 'deleted_at', isNull: true }, onVisibleRecord] }

// The same scopes in SQL: what Postgres says is in them, independently of the engine's predicate.
const eventsSql = `
  SELECT id, name FROM events
   WHERE deleted_at IS NULL AND record_id IN (SELECT id FROM records WHERE visible)
   ORDER BY id`
const notesSql = `
  SELECT id, body FROM notes
   WHERE record_id IN (SELECT id FROM records WHERE visible)
   ORDER BY id`

let h: Harness | undefined
afterEach(async () => {
  await h?.shutdown()
  h = undefined
})

async function boot(engineEnv: Record<string, string> = {}): Promise<Harness> {
  const harness = await bootHarness(schema, {
    ddl,
    durableStreamsDurability: 'wal',
    engineEnv: { ELECTRIC_CIRCUITS_SUBSCRIPTION_LEASE_SECS: '3600', ...engineEnv },
  })
  await pgQuery(
    harness,
    `INSERT INTO records (id, visible) VALUES (1, true), (2, true), (3, true), (4, false), (5, false), (6, true)`,
  )
  await pgQuery(
    harness,
    `INSERT INTO events (id, record_id, name) VALUES
       (10, 1, 'to edit'),
       (11, 1, 'to delete'),
       (12, 1, 'to soft-delete'),
       (13, 2, 'on the record that is revoked'),
       (14, 4, 'on the record that is granted'),
       (15, 3, 'moved to a hidden record, then its record is revoked'),
       (16, 3, 'left on the record that is revoked last'),
       (17, 6, 'untouched'),
       (19, 2, 'moved to a visible record before its record is revoked')`,
  )
  await pgQuery(
    harness,
    `INSERT INTO notes (id, record_id, body) VALUES
       (100, 1, 'kept'), (101, 2, 'revoked'), (102, 4, 'granted'), (103, 6, 'untouched')`,
  )
  await drainEngine(harness)
  return harness
}

/**
 * A client of one stream: the rows it holds and the offset it has read to. It never asks for a
 * snapshot again — everything after the load comes from the stream at `offset`.
 */
interface Reader {
  name: string
  url: string
  offset: string
  /** A group stream names a row by table and key; a single shape's stream by key alone. */
  keyOf: (env: StreamEnvelope) => string
  rows: Map<string, Row>
  /** Every envelope applied since the load, in order. */
  applied: StreamEnvelope[]
  /** The stream was closed or deleted: the engine retired the shape. */
  gone: boolean
  /** A must-refetch marker arrived: the group dropped output and wants a reload. */
  refetch: boolean
}

function reader(name: string, url: string, offset: string, keyOf: Reader['keyOf'], rows: [string, Row][]): Reader {
  return { name, url, offset, keyOf, rows: new Map(rows), applied: [], gone: false, refetch: false }
}

/** Read the stream from the saved offset to its tail and apply what it says. */
async function catchUp(r: Reader, signal?: AbortSignal, live = false): Promise<void> {
  for (let i = 0; i < 200; i++) {
    const query = new URLSearchParams({ offset: r.offset })
    if (live) query.set('live', 'long-poll')
    const res = await fetch(`${r.url}?${query}`, { signal })
    if (res.status === 404 || res.status === 410) {
      r.gone = true
      return
    }
    if (!res.ok) throw new Error(`GET ${r.url} -> ${res.status}`)
    const body = res.status === 204 ? '' : (await res.text()).trim()
    for (const env of body ? (JSON.parse(body) as StreamEnvelope[]) : []) {
      r.applied.push(env)
      if (env.type === '__circuits.control') {
        r.refetch = true
        continue
      }
      if (env.headers.operation === 'delete') r.rows.delete(r.keyOf(env))
      else if (env.value) r.rows.set(r.keyOf(env), env.value as Row)
    }
    if (res.headers.get('stream-closed') !== null) {
      r.gone = true
      return
    }
    const next = res.headers.get('stream-next-offset')
    const upToDate = res.headers.get('stream-up-to-date') !== null
    if (!next || next === r.offset) return
    r.offset = next
    if (upToDate) return
  }
  throw new Error(`${r.name}: the stream never reported its tail`)
}

/** The offset a client holds for a stream it has read nothing from yet: its current tail. */
async function tailOffset(url: string): Promise<string> {
  const res = await fetch(url, { method: 'HEAD' })
  if (!res.ok) throw new Error(`HEAD ${url} -> ${res.status}`)
  return res.headers.get('stream-next-offset') ?? '-1'
}

const byKey = (env: StreamEnvelope) => env.key
const byTableAndKey = (env: StreamEnvelope) => `${env.type} ${env.key}`

interface Clients {
  /** A materialized subquery shape: the stream carries its backfill. */
  shape: Reader
  shapeId: string
  /** A changes-only subquery feed: the rows came from a snapshot query, the stream began empty. */
  feed: Reader
  feedId: string
  /** A group with two subquery members on one stream. */
  group: Reader
}

async function openClients(harness: Harness): Promise<Clients> {
  const created = await createShape(harness, { table: 'events', where: liveEvents })
  const shape = reader('materialized subquery shape', created.streamUrl, '-1', byKey, [])
  await catchUp(shape)

  // The feed first, its offset before the snapshot, then the snapshot: the order a subset load uses.
  const fed = await createShape(harness, { table: 'events', where: liveEvents, changesOnly: true })
  const feedOffset = await tailOffset(fed.streamUrl)
  const page = await fetch(`${harness.engineUrl}/v1/subsets/query`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ table: 'events', where: liveEvents }),
  })
  expect(page.status, await page.clone().text()).toBe(200)
  const snapshot = ((await page.json()) as { rows: Row[] }).rows
  const feed = reader(
    'changes-only subquery feed',
    fed.streamUrl,
    feedOffset,
    byKey,
    snapshot.map((row) => [String(row.id), row]),
  )

  const load = await fetch(`${harness.engineUrl}/v1/subsets/load`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      members: [
        { table: 'events', where: liveEvents },
        { table: 'notes', where: onVisibleRecord },
      ],
    }),
  })
  expect(load.status, await load.clone().text()).toBe(200)
  const loaded = (await load.json()) as { streamUrl: string; offset: string; rows: { table: string; value: Row }[] }
  const group = reader(
    'group with subquery members',
    loaded.streamUrl,
    loaded.offset,
    byTableAndKey,
    loaded.rows.map((row) => [`${row.table} ${String(row.value.id)}`, row.value]),
  )
  return { shape, shapeId: created.shapeId, feed, feedId: fed.shapeId, group }
}

const eventLabels = (rows: Iterable<Row>) =>
  [...rows].map((row) => `${Number(row.id)}:${String(row.name)}`).sort((a, b) => Number.parseInt(a, 10) - Number.parseInt(b, 10))

/** What a group reader holds, spelled like the two SQL oracles spell it. */
function groupLabels(r: Reader): string[] {
  return [...r.rows.entries()]
    .map(([key, row]) => `${key.split(' ')[0]} ${Number(row.id)}:${String(row.name ?? row.body)}`)
    .sort()
}

async function groupInPostgres(harness: Harness): Promise<string[]> {
  const events = (await pgQuery(harness, eventsSql)).map((row) => `public.events ${Number(row.id)}:${String(row.name)}`)
  const notes = (await pgQuery(harness, notesSql)).map((row) => `public.notes ${Number(row.id)}:${String(row.body)}`)
  return [...events, ...notes].sort()
}

/** Every client reads on from its saved offset and must hold exactly what Postgres holds. */
async function expectConverged(harness: Harness, c: Clients, when: string): Promise<void> {
  // The barrier: replication has caught up, the sequencer has processed the change log to its
  // tail and every deferred flip has landed, so whatever a single shape's stream will say is on
  // it. A group stream lands through its own lane, which that barrier does not wait for, so each
  // reader reads on until it holds what Postgres holds (or the deadline says it never will).
  await drainEngine(harness)
  const events = eventLabels(await pgQuery(harness, eventsSql))
  const group = await groupInPostgres(harness)
  const expected: [Reader, () => string[], string[]][] = [
    [c.shape, () => eventLabels(c.shape.rows.values()), events],
    [c.feed, () => eventLabels(c.feed.rows.values()), events],
    [c.group, () => groupLabels(c.group), group],
  ]
  for (const [r, held, want] of expected) {
    try {
      await waitFor(
        async () => {
          await catchUp(r)
          return r.gone || r.refetch || JSON.stringify(held()) === JSON.stringify(want)
        },
        `${when}: the ${r.name} to converge`,
        8000,
      )
    } catch {
      // Fall through: the assertions below say what is wrong.
    }
    expect(r.gone, `${when}: the ${r.name}'s stream was retired — its client would have to reload`).toBe(false)
    expect(r.refetch, `${when}: the ${r.name} was told to refetch`).toBe(false)
    expect(held(), `${when}: the ${r.name}`).toEqual(want)
  }
}

/** Stop the engine with `SIGTERM` and wait for a clean exit. */
async function stopGracefully(harness: Harness): Promise<void> {
  harness.signalEngine('SIGTERM')
  const exit = await harness.waitForEngineExit(30000)
  expect(exit.code, `the engine must stop cleanly (got ${JSON.stringify(exit)})`).toBe(0)
}

/** A `HEX/HEX` position as one comparable number, the way `pg::lsn_to_u64` reads it. */
function position(lsn: string): bigint {
  const [hi, lo] = lsn.split('/')
  return (BigInt(`0x${hi}`) << 32n) | BigInt(`0x${lo}`)
}

describe('subquery shapes across a graceful restart', () => {
  it('a client reading the same streams from its saved offsets ends equal to Postgres, with no new snapshot', async () => {
    h = await boot()
    const c = await openClients(h)
    await expectConverged(h, c, 'after the load')

    // One live change before the stop: the live path works, and the changes-only feed has now
    // DELIVERED a row (20) as well as holding rows it never delivered (the snapshot's).
    await pgQuery(h, `INSERT INTO events (id, record_id, name) VALUES (20, 1, 'delivered live, soft-deleted while down')`)
    await expectConverged(h, c, 'after a live change')
    const before = { shape: c.shape.applied.length, feed: c.feed.applied.length, group: c.group.applied.length }

    await stopGracefully(h)

    // While the engine is down. Every statement is its own commit, in this order.
    await pgQuery(h, `UPDATE events SET name = 'edited while down' WHERE id = 10`)
    await pgQuery(h, `INSERT INTO events (id, record_id, name) VALUES (30, 1, 'inserted while down'), (31, 5, 'inserted on a hidden record')`)
    await pgQuery(h, `DELETE FROM events WHERE id = 11`)
    await pgQuery(h, `UPDATE events SET deleted_at = now() WHERE id = 12`)
    await pgQuery(h, `UPDATE events SET deleted_at = now() WHERE id = 20`)
    await pgQuery(h, `UPDATE events SET record_id = 1 WHERE id = 19`)
    // Revoke: record 2 leaves the inner set. 13 and note 101 leave with it; 19 already moved away.
    await pgQuery(h, `UPDATE records SET visible = false WHERE id = 2`)
    // Grant: record 4 enters. 14 and note 102 arrive, though neither row changed.
    await pgQuery(h, `UPDATE records SET visible = true WHERE id = 4`)
    // The row moves to another scope (a hidden record), THEN the first scope's access is removed.
    // By the time record 3 leaves the inner set, 15 no longer names it: only the engine's own
    // record of what each stream holds can remove it.
    await pgQuery(h, `UPDATE events SET record_id = 5 WHERE id = 15`)
    await pgQuery(h, `UPDATE records SET visible = false WHERE id = 3`)

    await h.startEngine()
    await expectConverged(h, c, 'after a graceful restart')

    expect(eventLabels(c.shape.rows.values())).toEqual([
      '10:edited while down',
      '14:on the record that is granted',
      '17:untouched',
      '19:moved to a visible record before its record is revoked',
      '30:inserted while down',
    ])

    // No snapshot was needed, and none was sent: the shapes are the same ones, and the streams
    // carried only what changed. A row nothing touched (17, note 103) is not said again.
    for (const id of [c.shapeId, c.feedId]) {
      expect((await fetch(`${h.engineUrl}/shapes/${id}`)).status, `shape ${id} is still registered`).toBe(200)
    }
    const since = {
      shape: c.shape.applied.slice(before.shape),
      feed: c.feed.applied.slice(before.feed),
      group: c.group.applied.slice(before.group),
    }
    expect(since.shape.map(byKey), 'the materialized shape re-sent an untouched row').not.toContain('17')
    expect(since.feed.map(byKey), 'the changes-only feed re-sent an untouched row').not.toContain('17')
    expect(since.group.map(byTableAndKey), 'the group re-sent an untouched row').not.toContain('public.events 17')
    expect(since.group.map(byTableAndKey), 'the group re-sent an untouched row').not.toContain('public.notes 103')
    expect(h.engineStderr(), 'the restore dropped a subquery shape').not.toContain('dropping subquery shape')

    // Positions never fall along a stream, across the restart included.
    for (const r of [c.shape, c.feed, c.group]) {
      const placed = r.applied.map((env) => env.headers.lsn).filter((lsn): lsn is string => typeof lsn === 'string')
      for (let i = 1; i < placed.length; i++) {
        expect(
          position(placed[i]!) >= position(placed[i - 1]!),
          `${r.name}: position fell from ${placed[i - 1]} to ${placed[i]}`,
        ).toBe(true)
      }
    }

    // The restored shapes are maintained like any other from here on.
    await pgQuery(h, `UPDATE records SET visible = true WHERE id = 3`)
    await pgQuery(h, `INSERT INTO notes (id, record_id, body) VALUES (104, 3, 'after the restart')`)
    await expectConverged(h, c, 'after a change that follows the restart')
    expect(eventLabels(c.feed.rows.values())).toContain('16:left on the record that is revoked last')
  }, 180000)

  it('keeps them across a second stop, and across a stop that follows a boot with no new change', async () => {
    h = await boot()
    const c = await openClients(h)
    await expectConverged(h, c, 'after the load')

    await stopGracefully(h)
    await pgQuery(h, `UPDATE records SET visible = false WHERE id = 2`)
    await h.startEngine()
    await expectConverged(h, c, 'after the first restart')

    await stopGracefully(h)
    await pgQuery(h, `UPDATE records SET visible = true WHERE id = 2`)
    await h.startEngine()
    await expectConverged(h, c, 'after the second restart')

    // Nothing changes in Postgres between this boot and its stop: the registry it writes on the
    // way out is the one it restored.
    await stopGracefully(h)
    await h.startEngine()
    await stopGracefully(h)
    await pgQuery(h, `UPDATE events SET name = 'edited after an idle restart' WHERE id = 17`)
    await h.startEngine()
    await expectConverged(h, c, 'after a restart that followed an idle one')
    expect(eventLabels(c.shape.rows.values())).toContain('17:edited after an idle restart')
  }, 240000)
})

// November 19 reported 13 lost groups with the app open, but not idle. Keep the same 13
// acknowledged groups in both cases. The connected case consumes actual long-polls throughout
// shutdown and restart, and parks one already-admitted group renewal on a PostgreSQL table lock.
// That lock, observed through pg_locks, establishes the overlap without a timing sleep. No
// /subsets/rejoin or claim identity is used: those belong to OTTO-13834.
describe('groups with connected clients across a graceful restart', () => {
  it.each(['idle', 'connected'] as const)(
    'keeps all 13 groups with %s clients, without loading another snapshot',
    async (activity) => {
      h = await boot()
      const harness = h
      const members = [
        { table: 'events', where: liveEvents },
        { table: 'notes', where: onVisibleRecord },
      ]
      const groups: Reader[] = []
      for (let i = 0; i < 13; i++) {
        const res = await fetch(`${harness.engineUrl}/v1/subsets/load`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ subscription: `restart-group-${i}`, members }),
        })
        expect(res.status, await res.clone().text()).toBe(200)
        const loaded = (await res.json()) as {
          streamUrl: string
          offset: string
          rows: { table: string; value: Row }[]
        }
        groups.push(reader(
          `group ${i}`,
          loaded.streamUrl,
          loaded.offset,
          byTableAndKey,
          loaded.rows.map((row) => [`${row.table} ${String(row.value.id)}`, row.value]),
        ))
      }
      expect(new Set(groups.map((r) => r.url)).size, 'each acknowledged group has its own stream').toBe(13)
      const initial = await groupInPostgres(harness)
      for (const r of groups) expect(groupLabels(r), r.name).toEqual(initial)

      const abort = new AbortController()
      const failures: string[] = []
      const followers = activity === 'connected' ? groups.map(async (r) => {
        try {
          while (!abort.signal.aborted && !r.gone && !r.refetch) {
            await catchUp(r, abort.signal, true)
          }
        } catch (error) {
          if (!abort.signal.aborted) failures.push(`${r.name}: ${String(error)}`)
        }
      }) : []
      let lock: Awaited<ReturnType<typeof lockTable>> | undefined
      let renewal: Promise<{ status: number; body: string }> | undefined
      try {
        // Positive evidence that all connected readers are consuming real live output before
        // the stop. Idle clients read it once and then stop issuing requests.
        await pgQuery(harness, `INSERT INTO notes (id, record_id, body) VALUES (104, 1, 'read before stop')`)
        await drainEngine(harness)
        await waitFor(async () => {
          if (activity === 'idle') await Promise.all(groups.map((r) => catchUp(r)))
          return failures.length > 0 || groups.some((r) => r.gone || r.refetch)
            || groups.every((r) => r.rows.has('public.notes 104'))
        }, 'all 13 clients to apply the live insert')
        expect(failures).toEqual([])
        for (const r of groups) {
          expect(r.gone || r.refetch, `${r.name} failed before shutdown`).toBe(false)
          expect(r.rows.has('public.notes 104'), r.name).toBe(true)
        }

        if (activity === 'connected') {
          lock = await lockTable(harness, 'events')
          renewal = fetch(`${harness.engineUrl}/v1/subsets/load`, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ subscription: 'restart-group-0', members }),
            signal: abort.signal,
          }).then(async (res) => ({ status: res.status, body: await res.text() }))
          // Attach a handler now so cleanup can join a rejected request without an unhandled rejection.
          void renewal.catch(() => {})
          await waitFor(
            async () => (await tableLockWaiters(harness, 'events')).length > 0,
            'the connected client renewal to reach its snapshot read',
          )
          harness.signalEngine('SIGTERM')
          await waitFor(async () => {
            const res = await fetch(`${harness.engineUrl}/ready`)
            return ((await res.json()) as { status: string }).status === 'shutting_down'
          }, 'the stop to begin while the renewal is in flight')
          await lock.release()
          const renewed = await renewal
          expect(renewed.status, renewed.body).toBe(200)
          expect((JSON.parse(renewed.body) as { streamUrl: string }).streamUrl).toBe(groups[0]!.url)
          // This was a renewal admitted before SIGTERM. It is the last snapshot request this
          // test ever issues; the client keeps its existing rows and saved stream offset.
          expect((await harness.waitForEngineExit(30000)).code).toBe(0)
        } else {
          await stopGracefully(harness)
        }

        await pgQuery(harness, `UPDATE records SET visible = false WHERE id = 2`)
        await pgQuery(harness, `UPDATE events SET name = 'edited during replacement' WHERE id = 10`)
        await harness.startEngine()
        await drainEngine(harness)
        const expected = await groupInPostgres(harness)
        await waitFor(async () => {
          if (activity === 'idle') await Promise.all(groups.map((r) => catchUp(r)))
          return failures.length > 0 || groups.some((r) => r.gone || r.refetch)
            || groups.every((r) => JSON.stringify(groupLabels(r)) === JSON.stringify(expected))
        }, 'the same 13 group streams to deliver the changes made during replacement')
        expect(failures, 'no connected feed failed').toEqual([])
        for (const r of groups) {
          expect(r.gone, `${r.name} lost its stream and would have to load a snapshot`).toBe(false)
          expect(r.refetch, `${r.name} requested a snapshot reload`).toBe(false)
          expect(groupLabels(r), r.name).toEqual(expected)
        }
      } finally {
        await lock?.release()
        abort.abort()
        await Promise.all(followers)
        await renewal?.catch(() => {})
      }
    },
    180000,
  )
})

// A stop does not wait for the engine to be idle. A commit it has already read can still owe its
// subquery effects — here a revoke whose query-back (the read that finds the rows to remove) is
// parked on a table lock when the signal lands. The engine gives such work a drain window
// (`ELECTRIC_CIRCUITS_SHUTDOWN_SUBQUERY_DRAIN_SECS`) to land instead of cancelling it at the signal;
// once it has, the registry is at rest and is written. Without the window the effect is abandoned,
// nothing is written, and the shapes are dropped — the second test, and what every stop under
// load did before.
describe('a stop with a flip still in flight', () => {
  /** Revoke record 2 with its query-back parked on a lock, signal the engine, then let it run. */
  async function stopWithAFlipInFlight(harness: Harness): Promise<void> {
    const lock = await lockTable(harness, 'events')
    try {
      await pgQuery(harness, `UPDATE records SET visible = false WHERE id = 2`)
      await waitFor(
        async () => (await tableLockWaiters(harness, 'events')).length > 0,
        'the revoke’s query-back to park on the lock',
      )
      harness.signalEngine('SIGTERM')
      // The stop is under way (the port stays open for the readiness drain) and the flip is not done.
      await waitFor(async () => {
        const res = await fetch(`${harness.engineUrl}/ready`)
        return ((await res.json()) as { status: string }).status === 'shutting_down'
      }, 'the engine to begin shutting down')
    } finally {
      await lock.release()
    }
    const exit = await harness.waitForEngineExit(30000)
    expect(exit.code, `the engine must stop cleanly (got ${JSON.stringify(exit)})`).toBe(0)
  }

  it('lets the flip land inside the drain window, and the shapes survive the restart', async () => {
    h = await boot()
    const c = await openClients(h)
    await expectConverged(h, c, 'after the load')

    await stopWithAFlipInFlight(h)
    await h.startEngine()

    await expectConverged(h, c, 'after a restart whose stop had a flip in flight')
    expect(eventLabels(c.shape.rows.values()), 'the revoke took effect').not.toContain(
      '13:on the record that is revoked',
    )
    expect(h.engineStderr()).not.toContain('dropping subquery shape')
  }, 120000)

  it('with no drain window the flip is abandoned and the shapes are dropped, loudly', async () => {
    h = await boot({ ELECTRIC_CIRCUITS_SHUTDOWN_SUBQUERY_DRAIN_SECS: '0' })
    const c = await openClients(h)
    await expectConverged(h, c, 'after the load')

    await stopWithAFlipInFlight(h)
    await h.startEngine()

    for (const r of [c.shape, c.feed, c.group]) {
      await waitFor(async () => {
        await catchUp(r)
        return r.gone
      }, `the ${r.name}'s stream to be retired`)
    }
    expect(h.engineStderr()).toContain('dropping subquery shape')
  }, 120000)
})

describe('subquery shapes across a crash', () => {
  it('are dropped as before: the read from the saved offset reports the stream gone', async () => {
    h = await boot()
    const c = await openClients(h)
    await expectConverged(h, c, 'after the load')

    // SIGKILL: nothing is written on the way out.
    await h.restartEngine(async () => {
      await pgQuery(h!, `UPDATE records SET visible = false WHERE id = 2`)
    })

    for (const r of [c.shape, c.feed, c.group]) {
      await waitFor(async () => {
        await catchUp(r)
        return r.gone
      }, `the ${r.name}'s stream to be retired after a crash`)
    }
    for (const id of [c.shapeId, c.feedId]) {
      expect((await fetch(`${h.engineUrl}/shapes/${id}`)).status, `shape ${id} is no longer registered`).toBe(404)
    }
    expect(h.engineStderr()).toContain('dropping subquery shape')
  }, 120000)
})
