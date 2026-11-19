// `POST /v1/subsets/rejoin`: a client that kept its rows and its stream offset takes its group's
// claim again, or renews it, without the engine reading a row.
//
// A group is created by a load (`POST /v1/subsets/load`), which reads every member in one snapshot.
// Until this route existed the load was also the only way to renew a group's lease or to come back
// to a released one, so every app launch and every renewal ran the members' queries on Postgres for
// rows the client threw away. The re-join sends the load's body and gets the load's answer less the
// snapshot. Checked here against the real engine, Postgres and durable-streams, through public
// signals only:
//
//   1. A re-join answers while an `ACCESS EXCLUSIVE` lock is held on every table a member query
//      would read, and nothing waits on those locks. The same request as a load parks on them.
//   2. A subscription that holds no group for exactly those members is answered `412`: nothing is
//      created, and nothing about a group that is there changes.
//   3. A re-join renews the lease: a group re-joined inside each lease period never lapses, and
//      lapses once the re-joins stop.
//   4. A group's sessions share its subscription, so a claim has an identity: every load and
//      re-join answers it (`claim`), a renewal names it (`?claim=…`) to keep it, and a release
//      names it. A release that arrives after a later session re-joined names an earlier claim
//      than the one held, and changes nothing: the later session's claim stays, and the cap
//      cannot evict the group under it. The same holds for a single shape whose subscription id
//      two sessions share.
//   5. A release that names no claim is the one an older client sends, and still drops whatever
//      claim the subscription holds. After a later session re-joined that is the later session's:
//      the group and its stream are kept and its next re-join takes the claim again, but until
//      then nothing holds the group, so the shape cap may evict it under its reader.
//
// The scope is shaped like the app's: a week window on a `timestamptz`, a soft-delete column and
// an access check that is a subquery (see `conformance-subset-feed-scope.test.ts`).

import type { Predicate, Row, Schema } from '@electric-circuits/protocol'
import { afterEach, describe, expect, it } from 'vitest'

import { lockTable, pgQuery, sleep, tableLockWaiters, waitFor } from './engine-native.js'
import { bootHarness, drainEngine, type Harness } from './harness.js'

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
const week: Predicate = {
  and: [
    { col: 'starts', op: 'gte', value: '2026-10-19T07:00:00Z' },
    { col: 'starts', op: 'lt', value: '2026-10-26T07:00:00Z' },
    { col: 'deleted_at', isNull: true },
    { col: 'record_id', in: { table: 'records', project: 'id', where: { col: 'visible', op: 'eq', value: true } } },
  ],
}
const visible: Predicate = { col: 'visible', op: 'eq', value: true }
const members = [
  { table: 'events', where: week },
  { table: 'records', where: visible },
]

let h: Harness | undefined
afterEach(async () => {
  await h?.shutdown()
  h = undefined
})

async function boot(engineEnv: Record<string, string> = {}): Promise<Harness> {
  h = await bootHarness(schema, { ddl, engineEnv })
  await pgQuery(h, 'INSERT INTO records (id, visible) VALUES (1, true), (2, true), (3, false)')
  await pgQuery(
    h,
    `INSERT INTO events (id, record_id, starts, name) VALUES
       (10, 1, '2026-10-19 16:00:00+00', 'first day'),
       (11, 1, '2026-10-21 16:00:00+00', 'midweek'),
       (12, 2, '2026-10-22 16:00:00+00', 'on record two'),
       (13, 3, '2026-10-23 16:00:00+00', 'on a hidden record'),
       (20, 1, '2026-11-05 16:00:00+00', 'another week')`,
  )
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

interface Group {
  groupId: string
  shapeId: string
  subscription: string
  /** The identity of the claim the answer granted. */
  claim: string
  leaseSeconds: number
  streamUrl: string
  streamPath: string
  offset: string
  rows?: { table: string; value: Row }[]
  results: { key: string; table: string; shapeId: string }[]
}

const load = (subscription = 'scope-1', loadMembers: unknown[] = members) =>
  engine('POST', '/v1/subsets/load', { subscription, members: loadMembers })
const rejoin = (subscription = 'scope-1', rejoinMembers: unknown[] = members) =>
  engine('POST', '/v1/subsets/rejoin', { subscription, members: rejoinMembers })
/** A release that names no claim: what a client from before claims had an identity sends. */
const release = (group: Group, subscription = 'scope-1') =>
  engine('DELETE', `/v1/shapes/${encodeURIComponent(group.groupId)}?subscription=${encodeURIComponent(subscription)}`)
/** A release that names the claim it was issued for. */
const releaseClaim = (group: Group, claim: string) =>
  engine('DELETE', `/v1/shapes/${encodeURIComponent(group.groupId)}?subscription=scope-1&claim=${encodeURIComponent(claim)}`)
/** A renewal: a re-join (or a load) by the session that holds `claim`. */
const renew = (route: 'rejoin' | 'load', claim: string) =>
  engine('POST', `/v1/subsets/${route}?claim=${encodeURIComponent(claim)}`, { subscription: 'scope-1', members })
/** The identity of the claim an answer granted; a marker when the engine answered none. */
const claimOf = (answer: Answer): string => {
  expect(answer.status, JSON.stringify(answer.body)).toBe(200)
  return typeof answer.body.claim === 'string' ? answer.body.claim : 'no-claim-answered'
}

async function loaded(subscription = 'scope-1'): Promise<Group> {
  const answer = await load(subscription)
  expect(answer.status, JSON.stringify(answer.body)).toBe(200)
  return answer.body as unknown as Group
}

async function subscriptions(id: string): Promise<number | undefined> {
  const shape = await engine('GET', `/shapes/${encodeURIComponent(id)}`)
  return shape.status === 200 ? (shape.body.subscriptions as number) : undefined
}

async function held(group: Group): Promise<Array<number | undefined>> {
  return Promise.all(group.results.map((member) => subscriptions(member.shapeId)))
}

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

/** The offset a reader that has read everything holds. */
async function tailOffset(streamUrl: string): Promise<string> {
  const res = await fetch(streamUrl, { method: 'HEAD' })
  if (!res.ok) throw new Error(`HEAD ${streamUrl} -> ${res.status}`)
  return res.headers.get('stream-next-offset') ?? '-1'
}

/** What a reader that stopped at `offset` reads when it comes back: `operation type key name`. */
async function readFrom(streamUrl: string, offset: string): Promise<string[]> {
  const res = await fetch(`${streamUrl}?offset=${encodeURIComponent(offset)}`)
  if (res.status === 204) return []
  if (!res.ok) throw new Error(`GET ${streamUrl} -> ${res.status}`)
  const body = (await res.text()).trim()
  const envelopes = body ? (JSON.parse(body) as { type: string; key: string; value?: Row; headers: { operation: string } }[]) : []
  return envelopes.map((env) => `${env.headers.operation} ${env.type} ${env.key}${env.value?.name ? ` ${String(env.value.name)}` : ''}`)
}

/** A reader parked on the stream's tail. A raw durable-streams read is not seen by the engine. */
function tail(streamUrl: string, offset: string): { answer: Promise<Response>; stop: () => void } {
  const abort = new AbortController()
  const answer = fetch(`${streamUrl}?offset=${encodeURIComponent(offset)}&live=long-poll`, { signal: abort.signal })
  answer.catch(() => {})
  return { answer, stop: () => abort.abort() }
}

describe('POST /v1/subsets/rejoin takes a group’s claim again without reading its rows', () => {
  it('answers under a lock that parks every member query, with the load’s answer less the snapshot', async () => {
    const h = await boot()
    const group = await loaded()
    expect(group.rows!.map((row) => `${row.table} ${row.value.id}`).sort()).toEqual([
      'public.events 10',
      'public.events 11',
      'public.events 12',
      'public.records 1',
      'public.records 2',
    ])
    // Where the client's reader stopped, and a clean stop: the SDK releases by the group's id.
    const cursor = await tailOffset(group.streamUrl)
    expect((await release(group)).status).toBe(200)
    expect(await held(group)).toEqual([0, 0])

    // While the client is away: an edit, a soft delete, and access withdrawn from a Record.
    await pgQuery(h, `UPDATE events SET name = 'midweek (edited)' WHERE id = 11`)
    await pgQuery(h, `UPDATE events SET deleted_at = now() WHERE id = 10`)
    await pgQuery(h, `UPDATE records SET visible = false WHERE id = 2`)
    await drainEngine(h)

    // Every table a member's query reads is locked: the two members', one of which is also the
    // access subquery's. A statement that reads either one parks until the lock is released.
    const locks = [await lockTable(h, 'events'), await lockTable(h, 'records')]
    let rejoined: Answer
    let loading: Promise<Answer>
    try {
      rejoined = await rejoin()
      expect(rejoined.status, JSON.stringify(rejoined.body)).toBe(200)
      expect(await tableLockWaiters(h, 'events'), 'the re-join sent Postgres no query on events').toEqual([])
      expect(await tableLockWaiters(h, 'records'), 'the re-join sent Postgres no query on records').toEqual([])

      // The control: the same request as a load reads the members, so it parks on the lock.
      loading = load()
      await waitFor(
        async () => (await tableLockWaiters(h, 'events')).length + (await tableLockWaiters(h, 'records')).length > 0,
        'the load’s member query to reach Postgres and park on the lock',
      )
    } finally {
      for (const lock of locks) await lock.release()
    }

    // A load's answer less the snapshot: the same group, the same stream, no rows.
    expect(Object.keys(rejoined.body).sort()).toEqual([
      'claim',
      'groupId',
      'leaseSeconds',
      'offset',
      'results',
      'shapeId',
      'streamPath',
      'streamUrl',
      'subscription',
    ])
    const again = rejoined.body as unknown as Group
    expect(again.groupId).toBe(group.groupId)
    expect(again.shapeId).toBe(group.groupId)
    expect(again.subscription).toBe('scope-1')
    expect(again.leaseSeconds).toBe(group.leaseSeconds)
    expect(again.streamUrl).toBe(group.streamUrl)
    expect(again.streamPath).toBe(group.streamPath)
    expect(again.results).toEqual(group.results)
    expect(await held(group), 'every member is claimed again').toEqual([1, 1])
    expect(await counter('group_rejoins_total')).toBe(1)

    // The load, once the lock is gone, is the same group with its rows read again.
    const reloaded = await loading
    expect(reloaded.status, JSON.stringify(reloaded.body)).toBe(200)
    expect((reloaded.body as unknown as Group).groupId).toBe(group.groupId)
    expect((reloaded.body as unknown as Group).rows!.length).toBeGreaterThan(0)

    // What happened while the client was away is on the same stream, after the offset it kept.
    await waitFor(async () => (await readFrom(group.streamUrl, cursor)).length >= 4, 'the changes made while away to be readable from the kept offset')
    expect((await readFrom(group.streamUrl, cursor)).sort()).toEqual([
      'delete public.events 10',
      'delete public.events 12',
      'delete public.records 2',
      'upsert public.events 11 midweek (edited)',
    ])
  }, 90000)

  it('answers 412 for a subscription that holds no group with those members, and creates nothing', async () => {
    await boot()
    expect(await cardinality('shapes')).toBe(0)

    // Never loaded.
    const never = await rejoin('never-loaded')
    expect(never.status, JSON.stringify(never.body)).toBe(412)
    expect(never.retryAfter).toBeNull()
    expect(String(never.body.error)).toContain('never-loaded')
    expect(await cardinality('shapes'), 'a re-join mints no shape').toBe(0)
    // The same subscription can still be loaded: the re-join reserved nothing under it.
    const group = await loaded('never-loaded')
    expect(group.results).toHaveLength(2)
    expect(await cardinality('shapes')).toBe(2)

    // Other members under a subscription that holds a group, held or released.
    const others = [{ table: 'events', where: { col: 'record_id', op: 'eq', value: 1 } }]
    expect((await rejoin('never-loaded', others)).status).toBe(412)
    expect((await release(group, 'never-loaded')).status).toBe(200)
    expect((await rejoin('never-loaded', others)).status).toBe(412)
    expect((await rejoin('never-loaded', [members[1], members[0]])).status, 'the members in another order').toBe(412)
    // Unlike a load asked for other members, the re-join left the released group as it was.
    expect(await held(group)).toEqual([0, 0])
    expect(await cardinality('shapes')).toBe(2)

    // A subscription that holds a single feed, not a group: no such group, and not a conflict.
    const feed = await engine('POST', '/v1/subset-feeds', { table: 'events', where: week, subscription: 'a-feed' })
    expect(feed.status, JSON.stringify(feed.body)).toBe(200)
    expect((await rejoin('a-feed')).status).toBe(412)
    expect(await subscriptions(feed.body.shapeId as string), 'the feed keeps its claim').toBe(1)

    // A re-join names its claim; without one there is nothing to look for.
    expect((await engine('POST', '/v1/subsets/rejoin', { members })).status).toBe(400)

    expect(await counter('group_rejoins_absent_total')).toBe(5)
    expect(await counter('group_rejoins_total')).toBe(0)
    // And the group it would not replace is still its subscription's to re-join.
    const back = await rejoin('never-loaded')
    expect(back.status, JSON.stringify(back.body)).toBe(200)
    expect((back.body as unknown as Group).groupId).toBe(group.groupId)
  }, 90000)

  it('renews the lease: a group re-joined inside each lease period never lapses, and lapses once the re-joins stop', async () => {
    const leaseSeconds = 4
    await boot({
      ELECTRIC_CIRCUITS_RETENTION_SWEEP_SECS: '1',
      ELECTRIC_CIRCUITS_SUBSCRIPTION_LEASE_SECS: String(leaseSeconds),
    })
    const group = await loaded()
    expect(group.leaseSeconds).toBe(leaseSeconds)

    // Two and a half lease periods, renewed four times per period.
    const until = Date.now() + leaseSeconds * 2500
    while (Date.now() < until) {
      const renewed = await rejoin()
      expect(renewed.status, JSON.stringify(renewed.body)).toBe(200)
      expect((renewed.body as unknown as Group).leaseSeconds).toBe(leaseSeconds)
      expect(await held(group)).toEqual([1, 1])
      expect(await counter('subscriptions_lapsed_total'), 'a renewed lease does not lapse').toBe(0)
      await sleep(leaseSeconds * 250)
    }

    // The control: left alone, the same lease lapses, for every member together.
    await waitFor(async () => (await counter('subscriptions_lapsed_total')) >= 2, 'the lease to lapse once the re-joins stop', 20000)
    expect(await held(group)).toEqual([0, 0])
    // A lapsed group is kept, and its subscription re-joins it.
    const back = await rejoin()
    expect(back.status, JSON.stringify(back.body)).toBe(200)
    expect((back.body as unknown as Group).streamUrl).toBe(group.streamUrl)
    expect(await held(group)).toEqual([1, 1])
  }, 90000)
})

describe('a release names the claim it was issued for', () => {
  it('leaves the claim a later session holds when an earlier session’s release is retried or arrives late', async () => {
    // The cap is the group's two members, so one more shape needs the group's room.
    const h = await boot({ ELECTRIC_CIRCUITS_MAX_SHAPES: '2', ELECTRIC_CIRCUITS_RETENTION_SWEEP_SECS: '1' })
    const oneMore = () => engine('POST', '/v1/subset-feeds', { table: 'events', where: { col: 'record_id', op: 'eq', value: 1 }, subscription: 'one-more' })

    // Session A loads the group, gets its claim and stops cleanly, naming it.
    const group = await loaded()
    const claimA = group.claim ?? 'no-claim-answered'
    const cursor = await tailOffset(group.streamUrl)
    expect((await releaseClaim(group, claimA)).status).toBe(200)
    expect(await held(group), 'A’s release dropped A’s claim').toEqual([0, 0])
    // Session B, the next launch, re-joins under the same subscription and tails the stream.
    const claimB = claimOf(await rejoin())
    expect(await held(group)).toEqual([1, 1])

    // A's release is retried: the answer to the first one was lost.
    expect((await releaseClaim(group, claimA)).status, 'a stale release is still a success').toBe(200)

    expect(await held(group), 'B’s claim is still held').toEqual([1, 1])
    expect(claimB, 'each session’s claim has its own identity').not.toBe(claimA)
    // Held, so the cap cannot evict it: one more shape is refused, and B's reader stays open.
    const refused = await oneMore()
    expect(refused.status, JSON.stringify(refused.body)).toBe(503)
    expect(await held(group)).toEqual([1, 1])
    await pgQuery(h, `UPDATE events SET name = 'first day (edited)' WHERE id = 10`)
    await drainEngine(h)
    await waitFor(async () => (await readFrom(group.streamUrl, cursor)).length >= 1, 'the change to reach the stream B is reading')
    expect(await readFrom(group.streamUrl, cursor)).toEqual(['upsert public.events 10 first day (edited)'])

    // B renews by re-join and by load, naming its claim: the identity is kept each time, so
    // the release B sends after any number of renewals still names the claim B holds.
    expect(claimOf(await renew('rejoin', claimB))).toBe(claimB)
    expect(claimOf(await renew('load', claimB))).toBe(claimB)
    expect(await held(group)).toEqual([1, 1])

    // Session C takes over while B's claim is still held (B was killed and never released):
    // C names no claim, so the claim is C's from then on, and B's release arrives late.
    const claimC = claimOf(await rejoin())
    expect(claimC).not.toBe(claimB)
    expect((await releaseClaim(group, claimB)).status).toBe(200)
    expect(await held(group), 'C’s claim is still held').toEqual([1, 1])
    expect((await oneMore()).status, 'held, so not evictable').toBe(503)

    // The release that names the held claim drops it, and only then may the cap evict the group.
    expect((await releaseClaim(group, claimC)).status).toBe(200)
    expect(await held(group), 'the release that names the held claim drops it').toEqual([0, 0])
    const made = await oneMore()
    expect(made.status, JSON.stringify(made.body)).toBe(200)
    expect(await counter('shapes_evicted_max_shapes_total')).toBe(group.results.length)
    expect(await counter('subscriptions_lapsed_total'), 'no claim was dropped by a lapse').toBe(0)
  }, 90000)

  it('holds for a single feed and an aggregate whose subscription id two sessions share', async () => {
    await boot()
    const create = {
      feed: (claim?: string) =>
        engine('POST', `/v1/subset-feeds${claim ? `?claim=${claim}` : ''}`, { table: 'events', where: week, subscription: 'device' }),
      aggregate: (claim?: string) =>
        engine('POST', `/v1/aggregates${claim ? `?claim=${claim}` : ''}`, { table: 'records', fn: 'count', subscription: 'device-count' }),
    }
    for (const [kind, subscription] of [['feed', 'device'], ['aggregate', 'device-count']] as const) {
      const releaseShape = (id: string, claim?: string) =>
        engine('DELETE', `/v1/shapes/${id}?subscription=${subscription}${claim ? `&claim=${claim}` : ''}`)
      // Session A creates under a subscription id it will use again, and stops cleanly.
      const first = await create[kind]()
      const id = first.body.shapeId as string
      const claimA = claimOf(first)
      expect((await releaseShape(id, claimA)).status).toBe(200)
      expect(await subscriptions(id), kind).toBe(0)
      // Session B repeats the create under the same id: the same shape, claimed again.
      const second = await create[kind]()
      expect(second.body.shapeId, kind).toBe(id)
      const claimB = claimOf(second)

      expect((await releaseShape(id, claimA)).status, kind).toBe(200)

      expect(await subscriptions(id), `${kind}: B’s claim is still held`).toBe(1)
      expect(claimB, kind).not.toBe(claimA)
      // A renewal by B names B's claim and keeps it; one that names none is answered a new one.
      expect(claimOf(await create[kind](claimB)), kind).toBe(claimB)
      const claimC = claimOf(await create[kind]())
      expect(claimC, kind).not.toBe(claimB)
      expect((await releaseShape(id, claimB)).status, kind).toBe(200)
      expect(await subscriptions(id), `${kind}: the claim was taken over, so B’s release names none held`).toBe(1)
      expect((await releaseShape(id, claimC)).status, kind).toBe(200)
      expect(await subscriptions(id), kind).toBe(0)
      // A release that names no claim releases whatever claim the subscription holds, as before.
      await create[kind]()
      expect((await releaseShape(id)).status, kind).toBe(200)
      expect(await subscriptions(id), kind).toBe(0)
    }
    // A claim is named with its subscription, and is not empty.
    const feed = await create.feed()
    expect((await engine('DELETE', `/v1/shapes/${feed.body.shapeId as string}?claim=${claimOf(feed)}`)).status).toBe(400)
    expect((await engine('DELETE', `/v1/shapes/${feed.body.shapeId as string}?subscription=device&claim=`)).status).toBe(400)
    expect(await subscriptions(feed.body.shapeId as string)).toBe(1)
  }, 90000)
})

describe('a release that names no claim, arriving after a later session re-joined the group', () => {
  it('drops that session’s claim; its next re-join takes it again, and until then the cap may evict the group under its reader', async () => {
    // The cap is the group's two members, so one more shape needs the group's room.
    const h = await boot({ ELECTRIC_CIRCUITS_MAX_SHAPES: '2', ELECTRIC_CIRCUITS_RETENTION_SWEEP_SECS: '1' })
    const oneMore = () => engine('POST', '/v1/subset-feeds', { table: 'events', where: { col: 'record_id', op: 'eq', value: 1 }, subscription: 'one-more' })

    // Session A loads the group and stops cleanly; session B (the next launch) re-joins it under
    // the subscription its stored fence names, which is the same one, and tails the stream.
    const group = await loaded()
    const cursor = await tailOffset(group.streamUrl)
    expect((await release(group)).status).toBe(200)
    expect((await rejoin()).status).toBe(200)
    expect(await held(group)).toEqual([1, 1])
    // While B holds the group nothing may evict it: one more shape is refused at the cap.
    const refused = await oneMore()
    expect(refused.status, JSON.stringify(refused.body)).toBe(503)
    expect(refused.retryAfter).not.toBeNull()

    // A's release arrives again, late (a retry of a request whose answer was lost, or one a
    // gateway sent detached). It names no claim, as a client from before claims had an identity
    // does, so the subscription and the group are all it names, and it releases B's claim.
    expect((await release(group)).status).toBe(200)
    expect(await held(group), 'the late release dropped the claim session B holds').toEqual([0, 0])

    // Nothing B reads is affected: the group is kept, and its stream still carries every change.
    await pgQuery(h, `UPDATE events SET name = 'first day (edited)' WHERE id = 10`)
    await drainEngine(h)
    await waitFor(async () => (await readFrom(group.streamUrl, cursor)).length >= 1, 'the change to reach the stream B is reading')
    expect(await readFrom(group.streamUrl, cursor)).toEqual(['upsert public.events 10 first day (edited)'])

    // B's next renewal is a re-join, and it takes the claim again on the same group and stream.
    const renewed = await rejoin()
    expect(renewed.status, JSON.stringify(renewed.body)).toBe(200)
    expect((renewed.body as unknown as Group).streamUrl).toBe(group.streamUrl)
    expect(await held(group)).toEqual([1, 1])
    expect((await oneMore()).status, 'held again, so not evictable').toBe(503)

    // The window between the late release and that renewal is the cost. Nothing holds the group
    // then, so a create at the cap evicts it as a unit, under B's reader.
    const reader = tail(group.streamUrl, await tailOffset(group.streamUrl))
    try {
      expect((await release(group)).status).toBe(200)
      const made = await oneMore()
      expect(made.status, JSON.stringify(made.body)).toBe(200)
      // B learns it at once and unambiguously, as it learns any retirement: the stream closes.
      const closed = await reader.answer
      await closed.text()
      expect(closed.headers.get('stream-closed')).toBe('true')
    } finally {
      reader.stop()
    }
    expect(await counter('shapes_evicted_max_shapes_total')).toBe(group.results.length)
    // B's renewal now finds no group, and B loads a snapshot: correct rows, one extra load.
    expect((await rejoin()).status).toBe(412)
  }, 90000)
})
