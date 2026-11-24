// A group's claim has an identity (ADR-0008, "A claim has an identity"; OTTO-13911).
//
// A group (`POST /v1/subsets/load`) is one claim on one stream, and every launch of a phone loads
// it under the same subscription. A release names the subscription, so a release an earlier
// launch sent, arriving late, could drop the claim a later launch had just taken. Each load now
// answers `claim`; a load that names it (`?claim=…`) renews it under the same identity, any other
// load is answered a new one, and a release that names a `claim` drops the group only while the
// group's claim still has that identity.

import type { Schema } from '@electric-circuits/protocol'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { pgQuery } from './engine-native.js'
import { bootHarness, drainEngine, type Harness } from './harness.js'

const schema: Schema = {
  tables: {
    items: { columns: { id: { type: 'int' }, n: { type: 'int' } }, primaryKey: 'id' },
    notes: { columns: { id: { type: 'int' }, item_id: { type: 'int' } }, primaryKey: 'id' },
  },
}

const SUBSCRIPTION = 'device-group'

let h: Harness
beforeEach(async () => {
  h = await bootHarness(schema)
  await pgQuery(h, 'INSERT INTO items (id, n) VALUES (1, 10), (2, 20)')
  await pgQuery(h, 'INSERT INTO notes (id, item_id) VALUES (1, 1)')
  await drainEngine(h)
}, 60000)
afterEach(async () => {
  await h?.shutdown()
})

interface GroupLoad {
  groupId: string
  claim: string
  rows: { table: string; value: { id: number } }[]
  results: { shapeId: string }[]
}

/** A load of the fixture's group, by a caller that holds `claim` (or none). */
async function load(claim?: string): Promise<GroupLoad> {
  const query = claim === undefined ? '' : `?claim=${encodeURIComponent(claim)}`
  const res = await fetch(`${h.engineUrl}/v1/subsets/load${query}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      subscription: SUBSCRIPTION,
      members: [{ table: 'items', where: { col: 'n', op: 'gte', value: 10 } }, { table: 'notes' }],
    }),
  })
  expect(res.status, await res.clone().text()).toBe(200)
  return (await res.json()) as GroupLoad
}

/** The group's release, by its public member's id, naming `claim`. */
async function release(shapeId: string, claim: string): Promise<number> {
  const path = `/v1/shapes/${encodeURIComponent(shapeId)}?subscription=${SUBSCRIPTION}&claim=${encodeURIComponent(claim)}`
  const res = await fetch(`${h.engineUrl}${path}`, { method: 'DELETE' })
  return res.status
}

/** How many claims a member shape holds, as `GET /v1/shapes/{id}` reports it; `null` once it is gone. */
async function held(shapeId: string): Promise<number | null> {
  const res = await fetch(`${h.engineUrl}/v1/shapes/${encodeURIComponent(shapeId)}`)
  if (res.status === 404) return null
  expect(res.status, await res.clone().text()).toBe(200)
  return ((await res.json()) as { subscriptions: number }).subscriptions
}

describe('a group load answers and accepts its claim', () => {
  it('a renewal that names the claim keeps it, and a late release of an earlier claim leaves the group', async () => {
    const a = await load()
    expect(typeof a.claim).toBe('string')
    expect(a.claim.length).toBeGreaterThan(0)
    expect(a.rows.map((row) => `${row.table}:${row.value.id}`).sort()).toEqual(['items:1', 'items:2', 'notes:1'])
    const publicMember = a.results[0]!.shapeId

    const renewed = await load(a.claim)
    expect(renewed.groupId).toBe(a.groupId)
    expect(renewed.claim, 'a load by the claim holder keeps its identity').toBe(a.claim)

    // A later launch loads the same group under the same subscription, naming no claim.
    const b = await load()
    expect(b.groupId).toBe(a.groupId)
    expect(b.claim, 'a load that names no claim is a new session').not.toBe(a.claim)

    // The earlier launch's release arrives late: it names a claim nobody holds any more.
    expect(await release(publicMember, a.claim)).toBe(200)
    expect(await held(publicMember), "B's claim is still held").toBe(1)
    const again = await load(b.claim)
    expect(again.groupId, 'the group B holds is the one it renews').toBe(a.groupId)
    expect(again.claim).toBe(b.claim)

    // B's own release names B's claim, and releases the group.
    expect(await release(publicMember, b.claim)).toBe(200)
    expect(await held(publicMember), 'the released group is gone').toBeNull()
  })
})
