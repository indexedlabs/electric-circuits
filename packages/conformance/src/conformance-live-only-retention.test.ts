// Live-only shapes stay bounded while the engine runs.
//
// A shape that cannot park (a subquery shape, an aggregate or a changes-only feed)
// is evaluated on every change to its tables for as long as it is registered, subscribed or not.
// Three rules keep that bounded between restarts, each checked here against the real engine,
// Postgres and durable-streams through public signals only:
//
//   1. An unsubscribed live-only shape is retired after the live-only idle period
//      (`ELECTRIC_CIRCUITS_LIVE_ONLY_IDLE_SECS`), counted from its last touch. A lease lapse is not
//      a touch. Its reader learns it as every retirement is learned: the tail is released with
//      `stream-closed`, and a later read finds the stream gone. After that a change to its table or
//      to its subquery's table costs the engine nothing for it: it keeps no subquery node, and it
//      sends Postgres no query (a lock on the feed's table would park one, as it does while live).
// Second-scale knobs as in `conformance-retention.test.ts`. The production defaults are a 30 minute
// lease, a 24 hour live-only period and a 60 second sweep.

import type { Schema } from '@electric-circuits/protocol'
import { afterEach, describe, expect, it } from 'vitest'

import { lockTable, pgQuery, readEnvelopes, tableLockWaiters, waitFor } from './engine-native.js'
import { bootHarness, drainEngine, type Harness } from './harness.js'

const schema: Schema = {
  tables: {
    lists: { columns: { id: { type: 'int' }, owner: { type: 'text' } }, primaryKey: 'id' },
    items: {
      columns: { id: { type: 'int' }, list_id: { type: 'int' }, title: { type: 'text' } },
      primaryKey: 'id',
    },
  },
}

/** `items.list_id IN (SELECT id FROM lists WHERE owner = 'u1')`. */
const inMyLists = {
  col: 'list_id',
  in: { table: 'lists', project: 'id', where: { col: 'owner', op: 'eq', value: 'u1' } },
}
const inList = (id: number) => ({ col: 'list_id', op: 'eq', value: id })

let h: Harness | undefined
afterEach(async () => {
  await h?.shutdown()
  h = undefined
})

async function boot(engineEnv: Record<string, string>): Promise<Harness> {
  h = await bootHarness(schema, { engineEnv })
  await pgQuery(h, "INSERT INTO lists (id, owner) VALUES (1, 'u1'), (2, 'u2')")
  await pgQuery(h, "INSERT INTO items (id, list_id, title) VALUES (1, 1, 'one'), (2, 1, 'two'), (3, 2, 'three')")
  await drainEngine(h)
  return h
}

interface Answer {
  status: number
  retryAfter: string | null
  body: Record<string, unknown>
}

async function engine(method: string, path: string, body?: unknown): Promise<Answer> {
  const res = await fetch(`${h!.engineUrl}${path}`, {
    method,
    headers: body === undefined ? {} : { 'content-type': 'application/json' },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  })
  const text = await res.text()
  return {
    status: res.status,
    retryAfter: res.headers.get('retry-after'),
    body: text ? (JSON.parse(text) as Record<string, unknown>) : {},
  }
}

interface Feed {
  shapeId: string
  streamUrl: string
  subscription: string
}

const feed = (where: unknown, subscription: string) =>
  engine('POST', '/v1/subset-feeds', { table: 'items', where, subscription })

const shapeStatus = async (id: string) => (await engine('GET', `/shapes/${encodeURIComponent(id)}`)).status

async function counter(name: string): Promise<number> {
  const { body } = await engine('GET', '/metrics')
  const value = (body.counters as Record<string, number>)[name]
  expect(value, `GET /metrics has no counter "${name}"`).toBeTypeOf('number')
  return value!
}

async function cardinality(name: string): Promise<number> {
  const { body } = await engine('GET', '/memory')
  return (body.cardinalities as Record<string, number>)[name]!
}

async function subqueryNodes(): Promise<unknown[]> {
  return (await engine('GET', '/subqueries')).body.nodes as unknown[]
}

/** The offset a reader that has read everything parks its long-poll on. */
async function tailOffset(streamUrl: string): Promise<string> {
  let offset = '-1'
  for (let i = 0; i < 100; i++) {
    const res = await fetch(`${streamUrl}?offset=${encodeURIComponent(offset)}`)
    if (res.status !== 200 && res.status !== 204) throw new Error(`GET ${streamUrl} -> ${res.status}`)
    await res.text()
    const next = res.headers.get('stream-next-offset')
    if (next && next !== offset) offset = next
    if (res.headers.get('stream-up-to-date') !== null || !next) break
  }
  return offset
}

/** A reader tailing the stream. A raw durable-streams read is not seen by the engine: no touch. */
function tail(streamUrl: string, offset: string): { answer: Promise<Response>; stop: () => void } {
  const abort = new AbortController()
  const answer = fetch(`${streamUrl}?offset=${encodeURIComponent(offset)}&live=long-poll`, { signal: abort.signal })
  answer.catch(() => {})
  return { answer, stop: () => abort.abort() }
}

/** What a reader sees of a retired stream once the retirement has finished. */
async function streamIsGone(streamUrl: string): Promise<boolean> {
  const res = await fetch(`${streamUrl}?offset=-1`)
  await res.text()
  return res.status === 404 || res.status === 410
}

describe('an unsubscribed live-only feed is retired after the live-only idle period', () => {
  it('closes the stream under its reader, then does no work for the feed when its tables change', async () => {
    const h = await boot({
      ELECTRIC_CIRCUITS_RETENTION_SWEEP_SECS: '1',
      ELECTRIC_CIRCUITS_SUBSCRIPTION_LEASE_SECS: '1',
      ELECTRIC_CIRCUITS_LIVE_ONLY_IDLE_SECS: '8',
      // The explicit period keeps this feed around long enough to observe its live behavior before
      // the one-second subscription lease floor permits retirement.
      ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS: '1',
      ELECTRIC_CIRCUITS_SHAPE_DORMANT_TTL_SECS: '3600',
    })
    const created = await feed(inMyLists, 'gone')
    expect(created.status, JSON.stringify(created.body)).toBe(200)
    const live = created.body as unknown as Feed
    expect((await subqueryNodes()).length, 'the feed keeps a subquery node maintained').toBe(1)

    // While it is live, a change to its subquery's table is work: list 2 becomes one of `u1`'s, so
    // the engine asks Postgres for that list's items. A lock on `items` parks that query, which is
    // how this test sees it.
    const whileLive = await lockTable(h, 'items')
    try {
      await pgQuery(h, "UPDATE lists SET owner = 'u1' WHERE id = 2")
      await waitFor(
        async () => (await tableLockWaiters(h, 'items')).length > 0,
        'the live feed’s query for the rows that moved in to reach Postgres',
      )
    } finally {
      await whileLive.release()
    }
    await drainEngine(h)
    await waitFor(async () => (await readEnvelopes(live.streamUrl)).length === 1, 'the live feed to deliver the move-in')

    // The client goes away without releasing: no renewal, so the lease lapses, and nothing touches
    // the feed again. Its reader is still parked on the stream.
    const reader = tail(live.streamUrl, await tailOffset(live.streamUrl))
    const parkedAt = Date.now()
    try {
      await waitFor(async () => (await shapeStatus(live.shapeId)) === 404, 'the idle live-only feed to be retired', 15000)

      // The reader learns it at once and unambiguously: `stream-closed`, not a long-poll timeout.
      const released = await reader.answer
      await released.text()
      expect(Date.now() - parkedAt, 'released by the close, not by the 30 s long-poll').toBeLessThan(20000)
      expect([200, 204]).toContain(released.status)
      expect(released.headers.get('stream-closed')).toBe('true')
    } finally {
      reader.stop()
    }
    await waitFor(() => streamIsGone(live.streamUrl), 'the retired stream to be deleted')
    expect(await counter('shapes_evicted_live_only_idle_total')).toBe(1)
    expect(await counter('subscriptions_lapsed_total')).toBe(1)
    expect(await cardinality('shapes')).toBe(0)
    expect(await cardinality('subquery_shapes')).toBe(0)
    expect(await subqueryNodes(), 'no subquery node is maintained for the retired feed').toEqual([])

    // The same kind of change to the subquery's table, with `items` locked again: move list 2 out
    // and back in. The engine sends Postgres nothing for the retired feed, so nothing parks on the
    // lock and the drain barrier (which waits for every deferred flip) completes while it is held.
    const afterRetirement = await lockTable(h, 'items')
    try {
      await pgQuery(h, "UPDATE lists SET owner = 'u2' WHERE id = 2")
      await pgQuery(h, "UPDATE lists SET owner = 'u1' WHERE id = 2")
      await drainEngine(h)
      expect(await tableLockWaiters(h, 'items'), 'no query for a retired feed').toEqual([])
    } finally {
      await afterRetirement.release()
    }
    // And a change to the feed's own table finds nothing to evaluate it against.
    await pgQuery(h, "UPDATE items SET title = 'two-b' WHERE id = 2")
    await pgQuery(h, "INSERT INTO items (id, list_id, title) VALUES (4, 1, 'four')")
    await drainEngine(h)
    expect(await subqueryNodes()).toEqual([])
    expect(await cardinality('subquery_shapes')).toBe(0)
    expect(await streamIsGone(live.streamUrl)).toBe(true)
    expect(await shapeStatus(live.shapeId)).toBe(404)

    // A returning client creates the feed again and gets a fresh one.
    const again = await feed(inMyLists, 'gone')
    expect(again.status, JSON.stringify(again.body)).toBe(200)
    expect((again.body as unknown as Feed).shapeId).not.toBe(live.shapeId)
  }, 90000)
})

describe('live-shape gauges are refreshed by ordinary retention sweeps', () => {
  it('reports subscribed and unsubscribed live-only feeds below the cap', async () => {
    await boot({
      ELECTRIC_CIRCUITS_MAX_SHAPES: '10',
      ELECTRIC_CIRCUITS_RETENTION_SWEEP_SECS: '1',
      ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS: '1',
      ELECTRIC_CIRCUITS_SUBSCRIPTION_LEASE_SECS: '0',
      ELECTRIC_CIRCUITS_LIVE_ONLY_IDLE_SECS: '3',
    })
    const held = await feed(inList(1), 'gauge-held')
    const released = await feed(inList(2), 'gauge-released')
    expect([held.status, released.status]).toEqual([200, 200])
    const releasedFeed = released.body as unknown as Feed
    expect((await engine('DELETE', `/v1/shapes/${releasedFeed.shapeId}?subscription=gauge-released`)).status).toBe(200)

    const gaugesAfterFirstSweep = async () => {
      const { body } = await engine('GET', '/metrics')
      const gauges = body.gauges as Record<string, number>
      return gauges.shapes_live_changes_only_subscribed === 1 && gauges.shapes_live_changes_only_unsubscribed === 1
    }
    await waitFor(gaugesAfterFirstSweep, 'the ordinary sweep to refresh live-only gauges')
    const releasedId = releasedFeed.shapeId
    await waitFor(async () => (await shapeStatus(releasedId)) === 404, 'the sweep to evict the released live-only feed')
    await waitFor(async () => {
      const { body } = await engine('GET', '/metrics')
      const gauges = body.gauges as Record<string, number>
      return gauges.shapes_live_changes_only_subscribed === 1 && gauges.shapes_live_changes_only_unsubscribed === 0
    }, 'the sweep to refresh gauges after eviction')
  }, 90000)
})

