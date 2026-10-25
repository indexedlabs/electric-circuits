// Prototype: the one-table subset load (`POST /v1/subset-loads`), the scope load (`POST /v1/scopes/load`)
// and the batch query (`POST /v1/subsets/query-batch`).
// One request creates every table's changes-only feed, reads each feed's tail offset, then reads
// every table's rows from ONE settled snapshot. A row written after the load must arrive on its
// feed after the returned offset and must not be in the snapshot.
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import type { Row, Schema, StreamEnvelope } from '@electric-circuits/protocol'

import { bootHarness, drainEngine, type Harness } from './harness.js'
import { pgQuery } from './engine-native.js'

const schema: Schema = {
  tables: {
    parent: { columns: { id: { type: 'int' }, active: { type: 'bool' } }, primaryKey: 'id' },
    child: { columns: { id: { type: 'int' }, parent_id: { type: 'int' } }, primaryKey: 'id' },
  },
}
const activeParents = { col: 'active', op: 'eq', value: true }
const childrenOfActive = { col: 'parent_id', in: { table: 'parent', project: 'id', where: activeParents } }

interface ScopeResult {
  key: string; shapeId: string; streamUrl: string; streamPath: string; subscription: string
  leaseSeconds: number; offset: string; rows: Row[]
}
interface ScopeResp { lsn: string; snapshot: string; horizon: string; results: ScopeResult[] }

async function post<T>(h: Harness, path: string, body: unknown): Promise<T> {
  const res = await fetch(`${h.engineUrl}${path}`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body),
  })
  const text = await res.text()
  if (res.status !== 200) throw new Error(`${path} -> ${res.status}: ${text}`)
  return JSON.parse(text) as T
}

/** Keys of every envelope on a stream after `offset` (raw reads, no fold). */
async function keysAfter(streamUrl: string, offset: string): Promise<string[]> {
  const keys: string[] = []
  for (let i = 0; i < 100; i++) {
    const res = await fetch(`${streamUrl}?offset=${encodeURIComponent(offset)}`)
    if (res.status === 204) break
    if (!res.ok) throw new Error(`GET ${streamUrl} -> ${res.status}`)
    const body = (await res.text()).trim()
    for (const env of (body ? JSON.parse(body) : []) as StreamEnvelope[]) if (env.key) keys.push(env.key)
    const next = res.headers.get('stream-next-offset')
    if (!next || next === offset || res.headers.get('stream-up-to-date') !== null) break
    offset = next
  }
  return keys
}

const ids = (rows: Row[]) => rows.map((r) => Number(r.id)).sort((a, b) => a - b)

describe('prototype: scope load', () => {
  let h: Harness
  beforeAll(async () => { h = await bootHarness(schema) }, 120000)
  afterAll(async () => await h?.shutdown())

  it('loads parent and child in one snapshot, then tails both feeds from their offsets', async () => {
    await pgQuery(h, 'INSERT INTO parent (id, active) VALUES (1, true), (2, false)')
    await pgQuery(h, 'INSERT INTO child (id, parent_id) VALUES (10, 1), (11, 2), (12, 1)')
    await drainEngine(h)

    const body = {
      subscription: 'scope-test',
      tables: [
        { key: 'parent', table: 'parent', where: activeParents },
        { key: 'child', table: 'child', where: childrenOfActive },
      ],
    }
    const load = await post<ScopeResp>(h, '/v1/scopes/load', body)
    console.log('scope load response', JSON.stringify(load))
    const [parent, child] = load.results
    expect(load.results.map((r) => r.key)).toEqual(['parent', 'child'])
    expect(ids(parent!.rows)).toEqual([1])
    expect(ids(child!.rows)).toEqual([10, 12])
    expect(load.snapshot).toMatch(/^\d+:\d+:/)
    expect(parent!.subscription).toBe('scope-test:parent')
    expect(child!.subscription).toBe('scope-test:child')
    expect(parent!.shapeId).not.toBe(child!.shapeId)
    for (const r of load.results) expect(r.offset).toBeTruthy()

    // Written after the load: on the feeds after their offsets, not in the snapshot.
    await pgQuery(h, 'INSERT INTO child (id, parent_id) VALUES (13, 1), (14, 2)')
    await pgQuery(h, 'INSERT INTO parent (id, active) VALUES (3, true)')
    await drainEngine(h)
    expect(await keysAfter(child!.streamUrl, child!.offset)).toEqual(['13'])
    expect(await keysAfter(parent!.streamUrl, parent!.offset)).toEqual(['3'])

    // Repeating the load renews the same subscriptions and returns the same feeds.
    const again = await post<ScopeResp>(h, '/v1/scopes/load', body)
    expect(again.results.map((r) => r.shapeId)).toEqual([parent!.shapeId, child!.shapeId])
    expect(ids(again.results[0]!.rows)).toEqual([1, 3])
    expect(ids(again.results[1]!.rows)).toEqual([10, 12, 13])
  }, 60000)

  it('subset load: one table in one round trip, feed then offset then snapshot', async () => {
    interface LoadResp {
      shapeId: string; table: string; streamPath: string; streamUrl: string; subscription: string
      leaseSeconds: number; offset: string; rows: Row[]; lsn: string; snapshot: string; horizon: string
    }
    const body = { table: 'child', where: childrenOfActive, subscription: 'load-test:child' }
    const load = await post<LoadResp>(h, '/v1/subset-loads', body)
    console.log('subset load response', JSON.stringify(load))
    expect(ids(load.rows)).toEqual([10, 12, 13])
    expect(load.subscription).toBe('load-test:child')
    expect(load.leaseSeconds).toBeGreaterThan(0)
    expect(load.offset).toBeTruthy()
    expect(load.snapshot).toMatch(/^\d+:\d+:/)
    expect(load.horizon).toBeTruthy()

    // Written after the load: on the feed after its offset, not in the snapshot.
    await pgQuery(h, 'INSERT INTO child (id, parent_id) VALUES (15, 1), (16, 2)')
    await drainEngine(h)
    expect(await keysAfter(load.streamUrl, load.offset)).toEqual(['15'])

    // A retry with the same subscription renews the same feed and reads a fresh page.
    const again = await post<LoadResp>(h, '/v1/subset-loads', body)
    expect(again.shapeId).toBe(load.shapeId)
    expect(again.subscription).toBe(load.subscription)
    expect(ids(again.rows)).toEqual([10, 12, 13, 15])

    // `query` bounds the page; the feed keeps its own predicate.
    const page = await post<LoadResp>(h, '/v1/subset-loads', {
      ...body,
      query: { orderBy: { col: 'id', desc: true }, limit: 2 },
    })
    expect(page.shapeId).toBe(load.shapeId)
    expect(page.rows.map((r) => Number(r.id))).toEqual([15, 13])
  }, 60000)

  it('batch query reads several tables in one snapshot with page bounds', async () => {
    const res = await post<{ snapshot: string; horizon: string; results: { key: string; rows: Row[] }[] }>(
      h, '/v1/subsets/query-batch', {
        tables: [
          { key: 'p', table: 'parent', where: activeParents, orderBy: { col: 'id', desc: true }, limit: 1 },
          { key: 'c', table: 'child', where: childrenOfActive, columns: ['id'] },
        ],
      })
    expect(res.snapshot).toMatch(/^\d+:\d+:/)
    expect(res.results.map((r) => r.key)).toEqual(['p', 'c'])
    expect(ids(res.results[0]!.rows)).toEqual([3])
    expect(ids(res.results[1]!.rows)).toEqual([10, 12, 13, 15])
    expect(Object.keys(res.results[1]!.rows[0]!)).toEqual(['id'])
  }, 60000)
})
