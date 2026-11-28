// A group member's snapshot and its feed can take different predicates.
//
// A group load (`POST /v1/subsets/load`) reads each member's page once and then keeps a changes-only
// feed for it. A member that names a `feedWhere` reads its page with `where` and files its feed with
// `feedWhere`, so a page may carry a membership subquery while the feed stays a plain shape that the
// engine files by its own row: no subquery shape, no membership node, no flip. A member that names
// no `feedWhere` keeps one predicate for both, as before.
//
// The caller chooses a feed that admits every row the page can return, as the gateway does: the page
// is a window, the feed is every row in the reader's workspaces. A page row the feed did not admit
// would never be updated or removed on the stream, so the fixture's feed covers its page.
//
// The predicates are part of the group's identity: a load that repeats both renews the group, and
// one that changes either is another group. A split member is a plain shape, so its group survives
// an engine restart, which a group with a subquery member does not. A table only the page reads is
// still one the group reads: a change to it releases the group, live or while the engine was down.

import type { Predicate, Schema, StreamEnvelope } from '@electric-circuits/protocol'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { pgQuery, readEnvelopes, waitFor } from './engine-native.js'
import { bootHarness, drainEngine, type Harness } from './harness.js'

const schema: Schema = {
  tables: {
    grants: { columns: { id: { type: 'int' }, ws: { type: 'int' } }, primaryKey: 'id' },
    items: {
      columns: { id: { type: 'int' }, ws: { type: 'int' }, title: { type: 'text' } },
      primaryKey: 'id',
    },
  },
}

/** The page: `items.ws IN (SELECT ws FROM grants)`, a membership into another table. */
const inGrants: Predicate = { col: 'ws', in: { table: 'grants', project: 'ws' } }
/** The feed: the reader's workspaces 10, 20 and 30 on the row's own column, no lookup. */
const inMyWorkspaces: Predicate = {
  or: [10, 20, 30].map((value) => ({ col: 'ws', op: 'eq', value }) as Predicate),
}

const split = { table: 'items', where: inGrants, feedWhere: inMyWorkspaces }

interface GroupLoad {
  groupId: string
  streamPath: string
  streamUrl: string
  rows: { table: string; value: { id: number } }[]
  results: { key: string; table: string; shapeId: string }[]
}

interface GraphShape {
  id: string
  table: string
  where: Predicate | null
  isSubquery: boolean
}

let h: Harness
beforeEach(async () => {
  h = await bootHarness(schema)
  // The page's grants cover workspaces 10 and 30; item 3 sits in workspace 20, which the reader's
  // feed covers and nothing grants.
  await pgQuery(h, 'INSERT INTO grants (id, ws) VALUES (1, 10), (2, 30)')
  await pgQuery(h, `INSERT INTO items (id, ws, title) VALUES (1, 10, 'a'), (2, 30, 'b'), (3, 20, 'c')`)
  await drainEngine(h)
}, 60000)
afterEach(async () => {
  await h?.shutdown()
})

async function load(body: { subscription?: string; members: unknown[] }): Promise<{ status: number; body: GroupLoad }> {
  const res = await fetch(`${h.engineUrl}/v1/subsets/load`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  })
  return { status: res.status, body: (await res.json()) as GroupLoad }
}

async function loaded(body: { subscription?: string; members: unknown[] }): Promise<GroupLoad> {
  const res = await load(body)
  expect(res.status, JSON.stringify(res.body)).toBe(200)
  return res.body
}

/**
 * A load once the engine has settled a change to a table the page reads. While that table is
 * unresolved or being resolved a load gets the refusal a create on it gets, a `500` that says to
 * retry; that is the only answer this waits out, and any other one fails the test at once.
 */
async function loadedAfterResolution(body: { subscription?: string; members: unknown[] }): Promise<GroupLoad> {
  let loaded: GroupLoad | undefined
  await waitFor(async () => {
    const res = await load(body)
    if (res.status === 200) {
      loaded = res.body
      return true
    }
    const error = (res.body as unknown as { error?: string }).error ?? ''
    expect(res.status, JSON.stringify(res.body)).toBe(500)
    expect(error, 'only a resolution refusal is waited out').toMatch(/is (being resolved|unresolved) after a change/)
    return false
  }, 'a load after the resolution settles')
  return loaded!
}

async function engineGet<T>(path: string): Promise<T> {
  const res = await fetch(`${h.engineUrl}${path}`)
  expect(res.ok, `GET ${path} -> ${res.status}`).toBe(true)
  return (await res.json()) as T
}

const graphShapes = async () => (await engineGet<{ shapes: GraphShape[] }>('/graph')).shapes
const subqueryShapes = async () =>
  (await engineGet<{ cardinalities: Record<string, number> }>('/memory')).cardinalities.subquery_shapes
const subqueryNodes = async () => (await engineGet<{ nodes: unknown[] }>('/subqueries')).nodes

const snapshot = (group: GroupLoad) => group.rows.map((row) => `${row.table} ${row.value.id}`).sort()
const said = (envs: StreamEnvelope[]) => envs.map((env) => `${env.headers.operation} ${env.type} ${env.key}`)

async function groupStream(group: GroupLoad, count: number): Promise<StreamEnvelope[]> {
  // A group stream lands through its own lane, which the drain barrier does not wait for.
  await waitFor(async () => (await readEnvelopes(group.streamUrl)).length >= count, 'the group stream to catch up')
  return readEnvelopes(group.streamUrl)
}

describe('a group member reads its page and files its feed with separate predicates', () => {
  it('files the member as a plain shape on its feed predicate, and reads its page with its snapshot predicate', async () => {
    const group = await loaded({ members: [split] })

    // The page is the snapshot predicate's: workspaces 10 and 30, read through `grants`.
    expect(snapshot(group)).toEqual(['public.items 1', 'public.items 2'])

    // The member's shape is the feed predicate's, and nothing in the engine looks into `grants`.
    const member = group.results[0]!.shapeId
    const shape = (await graphShapes()).find((s) => s.id === member)
    expect(shape, `member ${member} on /graph`).toBeDefined()
    expect(shape!.isSubquery).toBe(false)
    expect(shape!.where).toEqual(inMyWorkspaces)
    expect(await subqueryShapes()).toBe(0)
    expect(await subqueryNodes()).toEqual([])

    // Live, the feed predicate decides. Item 4 is outside the reader's workspaces. Granting workspace
    // 20 would move item 3 into the page's predicate, and a plain feed has no membership to flip.
    // The page's own rows change inside the feed, so the stream carries them, and a new row in the
    // feed arrives. Commits land on the group stream in order, so once the last one is there
    // anything the earlier ones produced would be too.
    await pgQuery(h, `INSERT INTO items (id, ws, title) VALUES (4, 40, 'outside the feed')`)
    await pgQuery(h, 'INSERT INTO grants (id, ws) VALUES (3, 20)')
    await pgQuery(h, `UPDATE items SET title = 'b2' WHERE id = 2`)
    await pgQuery(h, 'DELETE FROM items WHERE id = 1')
    await pgQuery(h, `INSERT INTO items (id, ws, title) VALUES (5, 10, 'in the feed')`)
    await drainEngine(h)

    expect(said(await groupStream(group, 3))).toEqual([
      'upsert public.items 2',
      'delete public.items 1',
      'upsert public.items 5',
    ])
    expect(await subqueryNodes()).toEqual([])
  })

  it('renews a group that repeats both predicates, and refuses one that changes either', async () => {
    const first = await loaded({ subscription: 'split-renew', members: [split] })

    const again = await loaded({ subscription: 'split-renew', members: [split] })
    expect(again.groupId).toBe(first.groupId)
    expect(again.streamPath).toBe(first.streamPath)
    expect(snapshot(again)).toEqual(['public.items 1', 'public.items 2'])

    const otherFeed = await load({
      subscription: 'split-renew',
      members: [{ ...split, feedWhere: { col: 'ws', op: 'eq', value: 10 } }],
    })
    expect(otherFeed.status, JSON.stringify(otherFeed.body)).toBe(409)

    const otherPage = await load({
      subscription: 'split-renew',
      members: [{ ...split, where: { col: 'ws', op: 'eq', value: 10 } }],
    })
    expect(otherPage.status, JSON.stringify(otherPage.body)).toBe(409)

    // Naming no feed predicate files the feed on the snapshot predicate: another group.
    const oneFilter = await load({ subscription: 'split-renew', members: [{ table: 'items', where: inGrants }] })
    expect(oneFilter.status, JSON.stringify(oneFilter.body)).toBe(409)

    // And the refusals left the group as it was.
    const still = await loaded({ subscription: 'split-renew', members: [split] })
    expect(still.groupId).toBe(first.groupId)
  })

  it('keeps a split group across an engine restart', async () => {
    const first = await loaded({ subscription: 'split-restart', members: [split] })

    await h.restartEngine()

    // A group with a subquery member is dropped at boot; a split member is a plain shape, so the
    // group is restored and the same load renews it.
    const again = await loaded({ subscription: 'split-restart', members: [split] })
    expect(again.groupId).toBe(first.groupId)
    expect(again.streamPath).toBe(first.streamPath)
    expect(snapshot(again)).toEqual(['public.items 1', 'public.items 2'])

    await pgQuery(h, `INSERT INTO items (id, ws, title) VALUES (6, 10, 'after the restart')`)
    await drainEngine(h)
    await waitFor(
      async () => said(await readEnvelopes(again.streamUrl)).includes('upsert public.items 6'),
      'the restored group to deliver a change made after the restart',
    )
  })

  it('releases a split group when a table only its page reads is truncated', async () => {
    const first = await loaded({ subscription: 'split-drift', members: [split] })
    const member = first.results[0]!.shapeId

    // No shape reads `grants`: the member's feed reads only `items`. The group's page does, so the
    // group goes with it, as it did when the member was a subquery shape.
    await pgQuery(h, 'TRUNCATE grants')
    await drainEngine(h)
    await waitFor(
      async () => (await fetch(`${h.engineUrl}/shapes/${member}`)).status === 404,
      'the group whose page reads the truncated table to be released',
    )

    const again = await loadedAfterResolution({ subscription: 'split-drift', members: [split] })
    expect(again.groupId).not.toBe(first.groupId)
    expect(again.streamPath).not.toBe(first.streamPath)
    expect(snapshot(again), 'the reload reads the page after the truncate').toEqual([])
  })

  it('releases a split group when a table only its page reads changes schema', async () => {
    const first = await loaded({ subscription: 'split-alter', members: [split] })
    const member = first.results[0]!.shapeId

    // The engine sees the new schema with the next change to the table, in its `Relation` message.
    await pgQuery(h, 'ALTER TABLE grants ADD COLUMN note text')
    await pgQuery(h, 'INSERT INTO grants (id, ws) VALUES (3, 40)')
    await drainEngine(h)
    await waitFor(
      async () => (await fetch(`${h.engineUrl}/shapes/${member}`)).status === 404,
      'the group whose page reads the changed table to be released',
    )

    const again = await loadedAfterResolution({ subscription: 'split-alter', members: [split] })
    expect(again.groupId).not.toBe(first.groupId)
    expect(snapshot(again)).toEqual(['public.items 1', 'public.items 2'])
  })

  it('drops a split group at restart when a table only its page reads changed while the engine was down', async () => {
    const first = await loaded({ subscription: 'split-ddl-down', members: [split] })

    await h.restartEngine(async () => {
      await pgQuery(h, 'ALTER TABLE grants ADD COLUMN note text')
    })

    // Nothing on the live path saw the change, so the group's record is the only witness of the
    // schema its page was read with; it no longer matches, and the group is not restored. The new
    // engine resolves the changed table as it starts, and a load waits that out.
    const again = await loadedAfterResolution({ subscription: 'split-ddl-down', members: [split] })
    expect(again.groupId).not.toBe(first.groupId)
    expect(again.streamPath).not.toBe(first.streamPath)
    expect(snapshot(again)).toEqual(['public.items 1', 'public.items 2'])
  })
})
