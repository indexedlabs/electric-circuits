// One client's whole resume, end to end, through every fix it depends on at once:
//
//   * a group load (`POST /v1/subsets/load`) with a subquery member, whose answer carries the
//     claim's identity;
//   * inserts, updates, deletes, a row moving out of the subquery scope and back (by its own
//     column, by the inner table's access flip, and by the outer predicate), live and while the
//     engine is down;
//   * a graceful engine restart, which writes the subquery registry on the way out and restores it
//     at boot instead of dropping the group;
//   * the re-join (`POST /v1/subsets/rejoin?claim=…`), which takes the same group's claim again
//     without a snapshot;
//   * the client reading on from the offset it saved, never reloading.
//
// The resumed rows must equal a fresh snapshot: the engine's own subset query of each member, and
// the same scope written independently in SQL.

import type { Predicate, Row, Schema, StreamEnvelope } from '@electric-circuits/protocol'
import { afterEach, describe, expect, it } from 'vitest'

import { pgQuery, waitFor } from './engine-native.js'
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
  },
}

const ddl = `
  CREATE TABLE records (id int PRIMARY KEY, visible boolean NOT NULL);
  ALTER TABLE records REPLICA IDENTITY FULL;
  CREATE TABLE events (id int PRIMARY KEY, record_id int NOT NULL, deleted_at timestamptz, name text NOT NULL);
  ALTER TABLE events REPLICA IDENTITY FULL;
`

const liveEvents: Predicate = {
  and: [
    { col: 'deleted_at', isNull: true },
    { col: 'record_id', in: { table: 'records', project: 'id', where: { col: 'visible', op: 'eq', value: true } } },
  ],
}
const visibleRecords: Predicate = { col: 'visible', op: 'eq', value: true }
const members = [
  { table: 'events', where: liveEvents },
  { table: 'records', where: visibleRecords },
]
const SUBSCRIPTION = 'phone-1'

// The same scope in SQL, written without the engine's predicate.
const scopeSql = `
  SELECT 'public.events ' || id || ':' || name AS label FROM events
   WHERE deleted_at IS NULL AND record_id IN (SELECT id FROM records WHERE visible)
  UNION ALL
  SELECT 'public.records ' || id || ':' || visible FROM records WHERE visible`

let h: Harness | undefined
afterEach(async () => {
  await h?.shutdown()
  h = undefined
})

async function post(path: string, body: unknown): Promise<{ status: number; body: Record<string, unknown> }> {
  const res = await fetch(`${h!.engineUrl}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  })
  const text = await res.text()
  return { status: res.status, body: text ? (JSON.parse(text) as Record<string, unknown>) : {} }
}

interface Client {
  url: string
  offset: string
  rows: Map<string, Row>
  applied: StreamEnvelope[]
  gone: boolean
  refetch: boolean
}

/** Read the group stream from the client's saved offset to its tail and apply what it says. */
async function catchUp(c: Client): Promise<void> {
  for (let i = 0; i < 200; i++) {
    const res = await fetch(`${c.url}?offset=${encodeURIComponent(c.offset)}`)
    if (res.status === 404 || res.status === 410) {
      c.gone = true
      return
    }
    if (!res.ok) throw new Error(`GET ${c.url} -> ${res.status}`)
    const body = res.status === 204 ? '' : (await res.text()).trim()
    for (const env of body ? (JSON.parse(body) as StreamEnvelope[]) : []) {
      c.applied.push(env)
      if (env.type === '__circuits.control') {
        c.refetch = true
        continue
      }
      const key = `${env.type} ${env.key}`
      if (env.headers.operation === 'delete') c.rows.delete(key)
      else if (env.value) c.rows.set(key, env.value as Row)
    }
    if (res.headers.get('stream-closed') !== null) {
      c.gone = true
      return
    }
    const next = res.headers.get('stream-next-offset')
    if (!next || next === c.offset) return
    c.offset = next
    if (res.headers.get('stream-up-to-date') !== null) return
  }
  throw new Error('the group stream never reported its tail')
}

const label = (key: string, row: Row) =>
  `${key.split(' ')[0]} ${Number(row.id)}:${row.name !== undefined ? String(row.name) : String(row.visible)}`
const held = (c: Client) => [...c.rows.entries()].map(([key, row]) => label(key, row)).sort()

/** A fresh snapshot of the scope: each member read by the engine's subset query, now. */
async function freshSnapshot(): Promise<string[]> {
  const labels: string[] = []
  for (const m of members) {
    const page = await post('/v1/subsets/query', { table: m.table, where: m.where })
    expect(page.status, JSON.stringify(page.body)).toBe(200)
    for (const row of page.body.rows as Row[]) labels.push(label(`public.${m.table} ${String(row.id)}`, row))
  }
  return labels.sort()
}

async function inSql(): Promise<string[]> {
  return (await pgQuery(h!, scopeSql)).map((row) => String(row.label)).sort()
}

/** Read on until the client holds the fresh snapshot, then check it against SQL too. */
async function expectResumed(c: Client, when: string): Promise<void> {
  await drainEngine(h!)
  const fresh = await freshSnapshot()
  expect(fresh, `${when}: the engine's fresh snapshot and SQL agree`).toEqual(await inSql())
  try {
    await waitFor(
      async () => {
        await catchUp(c)
        return c.gone || c.refetch || JSON.stringify(held(c)) === JSON.stringify(fresh)
      },
      `${when}: the resumed client to converge`,
      10000,
    )
  } catch {
    // The assertions below say what is wrong.
  }
  expect(c.gone, `${when}: the group's stream was retired — the client would have to reload`).toBe(false)
  expect(c.refetch, `${when}: the client was told to refetch`).toBe(false)
  expect(held(c), `${when}: the resumed rows`).toEqual(fresh)
}

async function stopGracefully(): Promise<void> {
  h!.signalEngine('SIGTERM')
  const exit = await h!.waitForEngineExit(30000)
  expect(exit.code, `the engine must stop cleanly (got ${JSON.stringify(exit)})`).toBe(0)
}

describe('a client resumes one group across changes and a graceful restart', () => {
  it('re-joins the same group by its claim and reads on to exactly a fresh snapshot', async () => {
    h = await bootHarness(schema, { ddl })
    await pgQuery(h, `INSERT INTO records (id, visible) VALUES (1, true), (2, true), (3, true), (4, false), (5, false)`)
    await pgQuery(
      h,
      `INSERT INTO events (id, record_id, name) VALUES
         (10, 1, 'to edit'), (11, 1, 'to delete'), (12, 1, 'to soft-delete and restore'),
         (13, 2, 'on a record hidden and shown again'), (14, 4, 'on a record shown while down'),
         (15, 3, 'moved out and back live'), (16, 3, 'moved out live, back while down'),
         (17, 1, 'untouched'), (18, 3, 'moved out and back while down')`,
    )
    await drainEngine(h)

    // The load: the group's rows, the stream offset they are good to, and the claim's identity.
    const loaded = await post('/v1/subsets/load', { subscription: SUBSCRIPTION, members })
    expect(loaded.status, JSON.stringify(loaded.body)).toBe(200)
    const group = loaded.body as { groupId: string; streamUrl: string; offset: string; claim: string; rows: { table: string; value: Row }[] }
    expect(typeof group.claim, 'the load answers the claim it granted').toBe('string')
    const c: Client = {
      url: group.streamUrl,
      offset: group.offset,
      rows: new Map(group.rows.map((row) => [`${row.table} ${String(row.value.id)}`, row.value])),
      applied: [],
      gone: false,
      refetch: false,
    }
    await expectResumed(c, 'after the load')

    // Live, with the client reading.
    await pgQuery(h, `INSERT INTO events (id, record_id, name) VALUES (40, 1, 'inserted live')`)
    await pgQuery(h, `UPDATE events SET name = 'edited live' WHERE id = 10`)
    await pgQuery(h, `DELETE FROM events WHERE id = 11`)
    await pgQuery(h, `UPDATE events SET record_id = 5 WHERE id = 15`) // out, by its own column
    await pgQuery(h, `UPDATE events SET record_id = 3 WHERE id = 15`) // and back
    await pgQuery(h, `UPDATE records SET visible = false WHERE id = 2`) // out, by the inner set
    await pgQuery(h, `UPDATE records SET visible = true WHERE id = 2`) // and back
    await pgQuery(h, `UPDATE events SET deleted_at = now() WHERE id = 12`) // out, by the outer predicate
    await pgQuery(h, `UPDATE events SET deleted_at = NULL WHERE id = 12`) // and back
    await expectResumed(c, 'after live changes')

    // Live, with the client away: it holds its offset and reads nothing.
    const away = c.applied.length
    await pgQuery(h, `INSERT INTO events (id, record_id, name) VALUES (41, 2, 'inserted while the client was away')`)
    await pgQuery(h, `DELETE FROM events WHERE id = 40`)
    await pgQuery(h, `UPDATE events SET record_id = 4 WHERE id = 16`)
    await drainEngine(h)

    await stopGracefully()

    // While the engine is down: each its own commit.
    await pgQuery(h, `UPDATE events SET name = 'edited while down' WHERE id = 10`)
    await pgQuery(h, `UPDATE events SET record_id = 3 WHERE id = 16`) // back while down
    await pgQuery(h, `UPDATE events SET record_id = 5 WHERE id = 18`) // out while down…
    await pgQuery(h, `UPDATE events SET record_id = 3 WHERE id = 18`) // …and back while down
    await pgQuery(h, `UPDATE records SET visible = true WHERE id = 4`) // 14 enters by the inner set
    await pgQuery(h, `UPDATE records SET visible = false WHERE id = 2`) // 13 and 41 leave by it
    await pgQuery(h, `INSERT INTO events (id, record_id, name) VALUES (42, 1, 'inserted while down')`)

    await h.startEngine()
    expect(h.engineStderr(), 'the restore dropped a subquery shape').not.toContain('dropping subquery shape')

    // The re-join names the claim: the same group, the same stream, the same claim, no rows.
    const rejoined = await post(`/v1/subsets/rejoin?claim=${encodeURIComponent(group.claim)}`, {
      subscription: SUBSCRIPTION,
      members,
    })
    expect(rejoined.status, JSON.stringify(rejoined.body)).toBe(200)
    expect(rejoined.body.groupId, 'the re-join found the group the load created').toBe(group.groupId)
    expect(rejoined.body.streamUrl, 'on the same stream').toBe(group.streamUrl)
    expect(rejoined.body.claim, 'a renewal that names its claim keeps it').toBe(group.claim)
    expect(rejoined.body.rows, 'a re-join reads no rows').toBeUndefined()

    // After the re-join, live again.
    await pgQuery(h, `INSERT INTO events (id, record_id, name) VALUES (43, 3, 'inserted after the restart')`)
    await pgQuery(h, `UPDATE records SET visible = true WHERE id = 2`) // 13 and 41 come back
    await pgQuery(h, `UPDATE events SET name = 'edited after the restart' WHERE id = 17`)

    await expectResumed(c, 'after a graceful restart and a re-join')
    expect(held(c)).toEqual(
      [
        'public.events 10:edited while down',
        'public.events 12:to soft-delete and restore',
        'public.events 13:on a record hidden and shown again',
        'public.events 14:on a record shown while down',
        'public.events 15:moved out and back live',
        'public.events 16:moved out live, back while down',
        'public.events 17:edited after the restart',
        'public.events 18:moved out and back while down',
        'public.events 41:inserted while the client was away',
        'public.events 42:inserted while down',
        'public.events 43:inserted after the restart',
        'public.records 1:true',
        'public.records 2:true',
        'public.records 3:true',
        'public.records 4:true',
      ].sort(),
    )
    // What the client read after it came back was only what changed: no reload of the scope.
    const since = c.applied.slice(away).map((env) => `${env.type} ${env.key}`)
    expect(since, 'an untouched row was sent again').not.toContain('public.events 15')
  }, 180000)
})
