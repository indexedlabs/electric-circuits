// Live-only shapes stay bounded while the engine runs.
//
// A shape that cannot park (a subquery shape, an aggregate, a changes-only feed, a group of feeds)
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
//   2. `ELECTRIC_CIRCUITS_MAX_SHAPES` counts every registered shape. At the cap a create that would
//      mint a shape first evicts what nobody holds; with nothing to evict it is refused with `503`
//      and `Retry-After`, on every create route. Joining a shape that exists is never refused, and
//      a subscribed shape is never evicted.
//   3. `DELETE /v1/shapes/{groupId}` releases the group's lease, as naming one of its members does.
//      A released or lapsed group is kept like any other live-only shape nobody holds: its members
//      stay live and its stream stays open, so the same subscription re-joins the same group and
//      reads what it missed from its earlier offset. It is retired as one unit, by rule 1 or 2.
//
// Second-scale knobs as in `conformance-retention.test.ts`. The production defaults are a 30 minute
// lease, a 24 hour live-only period and a 60 second sweep.

import type { Row, Schema } from '@electric-circuits/protocol'
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
      // Without the live-only period the feed would be kept for idle + dormancy: over an hour here,
      // 7 days 6 hours by default.
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

describe('the shape cap evicts what nobody holds, then refuses new shapes', () => {
  it('answers 503 with Retry-After on every create route at the cap, and succeeds after a release', async () => {
    await boot({ ELECTRIC_CIRCUITS_MAX_SHAPES: '2', ELECTRIC_CIRCUITS_RETENTION_SWEEP_SECS: '1' })
    const a = await feed(inList(1), 'sub-a')
    const b = await feed(inList(2), 'sub-b')
    expect([a.status, b.status]).toEqual([200, 200])
    const feedA = a.body as unknown as Feed
    const feedB = b.body as unknown as Feed

    // Both shapes are subscribed: nothing may be evicted, so one more shape is refused, whichever
    // route asks for it.
    const refusals: Record<string, Answer> = {
      'a changes-only feed': await feed(inList(3), 'sub-c'),
      'a subquery feed': await feed(inMyLists, 'sub-c'),
      'a row shape': await engine('POST', '/v1/shapes', { table: 'items', where: inList(3), subscription: 'sub-c' }),
      'an aggregate': await engine('POST', '/v1/aggregates', { table: 'items', fn: 'count', subscription: 'sub-c' }),
      'a group': await engine('POST', '/v1/subsets/load', { subscription: 'sub-c', members: [{ table: 'lists' }] }),
    }
    for (const [what, refused] of Object.entries(refusals)) {
      expect(refused.status, `${what}: ${JSON.stringify(refused.body)}`).toBe(503)
      expect(refused.retryAfter, `${what} carries Retry-After`).toMatch(/^[1-9][0-9]*$/)
      expect(String(refused.body.error), what).toContain('ELECTRIC_CIRCUITS_MAX_SHAPES')
    }
    expect(await counter('shape_creates_refused_total')).toBe(Object.keys(refusals).length)
    expect(await counter('shapes_evicted_max_shapes_total'), 'a subscribed shape is never evicted').toBe(0)
    expect([await shapeStatus(feedA.shapeId), await shapeStatus(feedB.shapeId)]).toEqual([200, 200])
    expect(await cardinality('shapes'), 'a refused create leaves nothing behind').toBe(2)

    // Joining a shape that exists mints nothing, so the cap does not apply to it.
    const joined = await feed(inList(1), 'sub-a2')
    expect(joined.status, JSON.stringify(joined.body)).toBe(200)
    expect((joined.body as unknown as Feed).shapeId).toBe(feedA.shapeId)

    // One release leaves an unsubscribed live-only shape. The next create evicts it and takes its
    // place, without waiting for a sweep or for its idle period.
    const release = await engine('DELETE', `/v1/shapes/${feedB.shapeId}?subscription=sub-b`)
    expect(release.status).toBe(200)
    const c = await feed(inList(3), 'sub-c')
    expect(c.status, JSON.stringify(c.body)).toBe(200)
    expect(await shapeStatus(feedB.shapeId), 'the released feed made the room').toBe(404)
    expect(await shapeStatus(feedA.shapeId), 'the feed with two subscriptions is untouched').toBe(200)
    await waitFor(() => streamIsGone(feedB.streamUrl), 'the evicted feed’s stream to be deleted')
    expect(await counter('shapes_evicted_max_shapes_total')).toBe(1)
    expect(await cardinality('shapes')).toBe(2)

    // Full again, with both shapes held.
    const d = await feed(inList(4), 'sub-d')
    expect(d.status).toBe(503)
    expect(d.retryAfter).not.toBeNull()
  }, 90000)
})

describe('DELETE /v1/shapes/{groupId} releases the group’s lease, and the group is kept', () => {
  interface Group {
    groupId: string
    shapeId: string
    streamUrl: string
    streamPath: string
    leaseSeconds: number
    rows: { table: string; value: Row }[]
    results: { shapeId: string }[]
  }
  const members = [
    { table: 'lists', where: { col: 'owner', op: 'eq', value: 'u1' } },
    { table: 'items', where: inList(1) },
  ]
  const load = (loadMembers: unknown[] = members) =>
    engine('POST', '/v1/subsets/load', { subscription: 'scope-1', members: loadMembers })
  const groupPath = (group: Group) => `/v1/shapes/${encodeURIComponent(group.groupId)}`

  async function loaded(): Promise<Group> {
    const answer = await load()
    expect(answer.status, JSON.stringify(answer.body)).toBe(200)
    return answer.body as unknown as Group
  }

  async function subscriptions(id: string): Promise<number | undefined> {
    const shape = await engine('GET', `/shapes/${encodeURIComponent(id)}`)
    return shape.status === 200 ? (shape.body.subscriptions as number) : undefined
  }

  /** What a reader that stopped at `offset` reads when it comes back: `type key title`. */
  async function readFrom(streamUrl: string, offset: string): Promise<string[]> {
    const res = await fetch(`${streamUrl}?offset=${encodeURIComponent(offset)}`)
    if (res.status === 204) return []
    if (!res.ok) throw new Error(`GET ${streamUrl} -> ${res.status}`)
    const body = (await res.text()).trim()
    const envelopes = body ? (JSON.parse(body) as { type: string; key: string; value?: Row }[]) : []
    return envelopes.map((env) => `${env.type} ${env.key} ${String(env.value?.title)}`)
  }

  it('lets the same subscription re-join the same group and read what it missed from its earlier offset', async () => {
    const h = await boot({})
    const group = await loaded()
    // The lease is the default 30 minutes: nothing below is the work of a lapse.
    expect(group.leaseSeconds).toBe(1800)
    expect(group.shapeId).toBe(group.groupId)
    const memberIds = group.results.map((member) => member.shapeId)
    expect(memberIds).toHaveLength(2)
    for (const id of memberIds) expect(await subscriptions(id)).toBe(1)
    // Where the client's reader stopped, as a clean app stop stores it.
    const cursor = await tailOffset(group.streamUrl)

    const released = await engine('DELETE', `${groupPath(group)}?subscription=scope-1`)
    expect(released.status, JSON.stringify(released.body)).toBe(200)
    // The lease is gone by the time the release is acknowledged; the group is not.
    for (const id of memberIds) expect(await subscriptions(id), `member ${id} after the release`).toBe(0)
    expect(await cardinality('shapes')).toBe(2)
    // Repeating the release is harmless.
    expect((await engine('DELETE', `${groupPath(group)}?subscription=scope-1`)).status).toBe(200)

    // A member's row changes while nobody holds the group.
    await pgQuery(h, "UPDATE items SET title = 'one-b' WHERE id = 1")
    await drainEngine(h)

    // The same subscription gets the same group and the same stream back.
    const rejoined = await loaded()
    expect(rejoined.groupId).toBe(group.groupId)
    expect(rejoined.streamUrl).toBe(group.streamUrl)
    expect(rejoined.results.map((member) => member.shapeId)).toEqual(memberIds)
    for (const id of memberIds) expect(await subscriptions(id), `member ${id} after the re-join`).toBe(1)
    // And the change made while it was away is on that stream, after the offset it stopped at.
    await waitFor(
      async () => (await readFrom(group.streamUrl, cursor)).includes('public.items 1 one-b'),
      'the change made while the group was released to be readable from the earlier offset',
    )
    expect(await counter('shapes_evicted_live_only_idle_total')).toBe(0)
    expect(await counter('shapes_evicted_max_shapes_total')).toBe(0)
  }, 90000)

  it('keeps a released group across an engine restart', async () => {
    const h = await boot({})
    const group = await loaded()
    const cursor = await tailOffset(group.streamUrl)
    expect((await engine('DELETE', `${groupPath(group)}?subscription=scope-1`)).status).toBe(200)

    await h.restartEngine(async () => {
      await pgQuery(h, "UPDATE items SET title = 'one-c' WHERE id = 1")
    })
    await drainEngine(h)

    const rejoined = await loaded()
    expect(rejoined.groupId).toBe(group.groupId)
    expect(rejoined.streamUrl).toBe(group.streamUrl)
    await waitFor(
      async () => (await readFrom(group.streamUrl, cursor)).includes('public.items 1 one-c'),
      'the change made while the engine was down to be readable from the earlier offset',
    )
  }, 90000)

  it('retires a released group as one unit after the live-only idle period, and the next load is a new group', async () => {
    await boot({
      ELECTRIC_CIRCUITS_RETENTION_SWEEP_SECS: '1',
      ELECTRIC_CIRCUITS_SUBSCRIPTION_LEASE_SECS: '1',
      ELECTRIC_CIRCUITS_LIVE_ONLY_IDLE_SECS: '3',
    })
    const group = await loaded()
    const memberIds = group.results.map((member) => member.shapeId)
    // Released at once: the release is the last touch, and the period runs from it.
    expect((await engine('DELETE', `${groupPath(group)}?subscription=scope-1`)).status).toBe(200)
    for (const id of memberIds) expect(await shapeStatus(id)).toBe(200)

    const reader = tail(group.streamUrl, await tailOffset(group.streamUrl))
    try {
      await waitFor(async () => (await shapeStatus(memberIds[0]!)) === 404, 'the released group to be retired', 15000)
      const closed = await reader.answer
      await closed.text()
      expect(closed.headers.get('stream-closed')).toBe('true')
    } finally {
      reader.stop()
    }
    await waitFor(() => streamIsGone(group.streamUrl), 'the retired group’s stream to be deleted')
    for (const id of memberIds) expect(await shapeStatus(id), `member ${id} goes with its group`).toBe(404)
    expect(await counter('shapes_evicted_live_only_idle_total')).toBe(memberIds.length)
    expect(await cardinality('shapes')).toBe(0)

    const next = await loaded()
    expect(next.groupId).not.toBe(group.groupId)
    expect(next.streamPath).not.toBe(group.streamPath)
    const rows = next.rows.map((row) => `${row.table} ${row.value.id}`)
    expect(rows.sort()).toEqual(['public.items 1', 'public.items 2', 'public.lists 1'])
  }, 90000)

  it('keeps a member release’s rules, and replaces a group only once nobody holds it', async () => {
    await boot({})
    const group = await loaded()
    const memberIds = group.results.map((member) => member.shapeId)

    // A release that names no claim is refused for a group id, as it is for a member's id.
    const anonymous = await engine('DELETE', groupPath(group))
    expect(anonymous.status, JSON.stringify(anonymous.body)).toBe(409)
    expect((await engine('DELETE', `/v1/shapes/${memberIds[0]}`)).status).toBe(409)
    // Releasing a claim the group does not hold is the idempotent no-op it is for any shape.
    expect((await engine('DELETE', `${groupPath(group)}?subscription=someone-else`)).status).toBe(200)
    for (const id of memberIds) expect(await subscriptions(id)).toBe(1)
    expect((await loaded()).groupId, 'the group is still the one loaded').toBe(group.groupId)

    // Other members under a subscription that holds a group: a conflict while it is held.
    const others = [{ table: 'items', where: inList(2) }]
    expect((await load(others)).status).toBe(409)
    // Released by a member's id (the same release), the kept group gives way to the new one.
    expect((await engine('DELETE', `/v1/shapes/${memberIds[0]}?subscription=scope-1`)).status).toBe(200)
    for (const id of memberIds) expect(await subscriptions(id)).toBe(0)
    const replaced = await load(others)
    expect(replaced.status, JSON.stringify(replaced.body)).toBe(200)
    expect((replaced.body as unknown as Group).groupId).not.toBe(group.groupId)
    for (const id of memberIds) expect(await shapeStatus(id), `member ${id} of the replaced group`).toBe(404)
  }, 90000)
})
