// Every live envelope on a native stream carries a commit position (`headers.lsn`, and the `txid`
// of the transaction that caused it), and positions never decrease along a stream.
//
// A plain shape's envelopes always did: the ingestor stamps them. A subquery shape's did not — its
// envelopes are built by the subquery registry, which emitted them with `operation`, `txid` and
// `commit_ts` only — and neither did a group stream's subquery members. A consumer that orders by
// position cannot place such an envelope: the Swift client the iOS app uses treats one as a fatal
// feed error and reloads the whole scope.
//
// The position is not only presence. A membership verdict produced by an inner-set flip is taken
// later than the commit that caused it, out of commit order, so it can follow a NEWER live change
// to the same key. Stamped with its own commit alone it would sit below that change, and a client
// that applies a delta only at or above the key's applied position would throw the verdict away.
// The last test here is that interleaving, against the real client.
//
// See docs/ARCHITECTURE.md §7, "Native envelope positions".

import type { Predicate, Row, Schema, StreamEnvelope } from '@electric-circuits/protocol'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { createShape, lockTable, pgQuery, readEnvelopes, tableLockWaiters, waitFor } from './engine-native.js'
import { bootHarness, drainEngine, type Harness } from './harness.js'

const schema: Schema = {
  tables: {
    orgs: { columns: { id: { type: 'int' }, active: { type: 'bool' } }, primaryKey: 'id' },
    grants: {
      columns: { id: { type: 'int' }, org_id: { type: 'int' }, ws: { type: 'int' } },
      primaryKey: 'id',
    },
    items: {
      columns: { id: { type: 'int' }, ws: { type: 'int' }, title: { type: 'text' } },
      primaryKey: 'id',
    },
  },
}

/** `items.ws IN (SELECT ws FROM grants)`: membership is granted and revoked by the inner table. */
const inGranted: Predicate = { col: 'ws', in: { table: 'grants', project: 'ws' } }

/** The same through one more table: a grant counts only while its org is active. */
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

let h: Harness
beforeEach(async () => {
  h = await bootHarness(schema)
}, 60000)
afterEach(async () => {
  await h?.shutdown()
})

/** Run one writing statement and return its transaction id as an envelope spells it. */
async function write(sql: string): Promise<string> {
  const rows = await pgQuery(h, `${sql} RETURNING (pg_current_xact_id()::text::numeric % 4294967296)::text AS xid`)
  expect(rows.length, `"${sql}" must change a row`).toBeGreaterThan(0)
  return String(rows[0]!.xid)
}

const POSITION = /^[0-9A-F]+\/[0-9A-F]+$/

/** A `HEX/HEX` position as one comparable number, the way `pg::lsn_to_u64` reads it. */
function position(env: StreamEnvelope, at: number): bigint {
  const lsn = env.headers.lsn
  expect(lsn, `envelope ${at} (${env.headers.operation} ${env.type} ${env.key}) carries no lsn`).toBeDefined()
  expect(lsn, `envelope ${at} lsn`).toMatch(POSITION)
  const [hi, lo] = lsn!.split('/')
  return (BigInt(`0x${hi}`) << 32n) | BigInt(`0x${lo}`)
}

/** Every envelope carries a parseable position and a transaction id, and positions never decrease. */
function expectPositions(envs: StreamEnvelope[]): bigint[] {
  const positions = envs.map((env, at) => position(env, at))
  envs.forEach((env, at) => {
    expect(env.headers.txid, `envelope ${at} (${env.headers.operation} ${env.key}) carries no txid`).toMatch(/^\d+$/)
  })
  positions.forEach((lsn, at) => {
    if (at > 0) expect(lsn >= positions[at - 1]!, `position falls at envelope ${at}`).toBe(true)
  })
  return positions
}

const said = (envs: StreamEnvelope[]) => envs.map((env) => `${env.headers.operation} ${env.key}`)

describe('native envelope positions', () => {
  it('stamps every emission of a changes-only subquery feed with its transaction’s position', async () => {
    await pgQuery(h, 'INSERT INTO grants (id, org_id, ws) VALUES (1, 1, 10)')
    await pgQuery(
      h,
      `INSERT INTO items (id, ws, title) VALUES (1, 10, 'held'), (2, 10, 'to leave'), (3, 30, 'not granted yet')`,
    )
    await drainEngine(h)

    // A change committed while the feed is being created is buffered and replayed when the feed is
    // installed. The create is parked on its inner-set seed (a read of `grants`) to make that window.
    const lock = await lockTable(h, 'grants')
    let replayed: string
    let feed: Awaited<ReturnType<typeof createShape>>
    try {
      const creating = createShape(h, { table: 'items', where: inGranted, changesOnly: true })
      await waitFor(async () => (await tableLockWaiters(h, 'grants')).length > 0, 'the create to reach its inner seed')
      replayed = await write(`INSERT INTO items (id, ws, title) VALUES (4, 10, 'during the create')`)
      await drainEngine(h)
      await lock.release()
      feed = await creating
    } finally {
      await lock.release()
    }

    // A live arrival, then its removal: the feed delivered the row, so its own key set removes it.
    const arrived = await write(`INSERT INTO items (id, ws, title) VALUES (5, 10, 'live')`)
    await drainEngine(h)
    const removed = await write('DELETE FROM items WHERE id = 5')
    await drainEngine(h)
    // A row the feed never delivered leaves by its own change: removed on proof it was a member.
    const proven = await write('UPDATE items SET ws = 99 WHERE id = 2')
    await drainEngine(h)
    // Inner-set flips, each resolved by a query-back: a grant moves item 3 in, a revocation moves
    // items 1 (never delivered) and 4 (delivered by the replay) out.
    const granted = await write('INSERT INTO grants (id, org_id, ws) VALUES (2, 1, 30)')
    await drainEngine(h)
    const revoked = await write('DELETE FROM grants WHERE id = 1')
    await drainEngine(h)

    const envs = await readEnvelopes(feed.streamUrl)
    expect(said(envs).slice(0, 5)).toEqual(['upsert 4', 'upsert 5', 'delete 5', 'delete 2', 'upsert 3'])
    expect(said(envs).slice(5).sort()).toEqual(['delete 1', 'delete 4'])

    const positions = expectPositions(envs)
    expect(
      envs.map((env) => env.headers.txid),
      'each envelope names the transaction that caused it',
    ).toEqual([replayed, arrived, removed, proven, granted, revoked, revoked])
    // Six transactions in commit order: each later one sits strictly above the one before, and the
    // two removals of one revocation share its position.
    for (let at = 1; at < 6; at++) expect(positions[at]! > positions[at - 1]!, `envelope ${at} is a later commit`).toBe(true)
    expect(positions[6]).toBe(positions[5])
  }, 120000)

  it('stamps a materialized subquery shape’s live envelopes; its backfill rows are the snapshot', async () => {
    await pgQuery(h, 'INSERT INTO grants (id, org_id, ws) VALUES (1, 1, 10)')
    await pgQuery(h, `INSERT INTO items (id, ws, title) VALUES (1, 10, 'a'), (2, 10, 'b'), (3, 30, 'c')`)
    await drainEngine(h)
    const shape = await createShape(h, { table: 'items', where: inGranted })
    await drainEngine(h)

    const edited = await write(`UPDATE items SET title = 'a2' WHERE id = 1`)
    await drainEngine(h)
    const granted = await write('INSERT INTO grants (id, org_id, ws) VALUES (2, 1, 30)')
    await drainEngine(h)
    const revoked = await write('DELETE FROM grants WHERE id = 1')
    await drainEngine(h)

    const envs = await readEnvelopes(shape.streamUrl)
    const backfill = envs.slice(0, 2)
    expect(said(backfill).sort()).toEqual(['upsert 1', 'upsert 2'])
    for (const env of backfill) expect(env.headers.lsn, 'a backfill row has no commit of its own').toBeUndefined()

    const live = envs.slice(2)
    expect(said(live).slice(0, 2)).toEqual(['upsert 1', 'upsert 3'])
    expect(said(live).slice(2).sort()).toEqual(['delete 1', 'delete 2'])
    expectPositions(live)
    expect(live.map((env) => env.headers.txid)).toEqual([edited, granted, revoked, revoked])
  }, 120000)

  it('stamps every envelope of a group stream, plain and subquery members alike', async () => {
    await pgQuery(h, 'INSERT INTO grants (id, org_id, ws) VALUES (1, 1, 10)')
    await pgQuery(h, `INSERT INTO items (id, ws, title) VALUES (1, 10, 'a'), (2, 20, 'b')`)
    await drainEngine(h)

    const res = await fetch(`${h.engineUrl}/v1/subsets/load`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ members: [{ table: 'items', where: inGranted }, { table: 'grants' }] }),
    })
    expect(res.status, await res.clone().text()).toBe(200)
    const group = (await res.json()) as { streamUrl: string; rows: unknown[] }
    expect(group.rows.length, 'the snapshot holds item 1 and grant 1').toBe(2)

    // Each write is drained before the next, so every query-back has been evaluated before the
    // following commit is sequenced and the set of envelopes below is exact.
    const arrived = await write(`INSERT INTO items (id, ws, title) VALUES (3, 10, 'live')`)
    await drainEngine(h)
    // One transaction, two members: the grant row itself (the plain member) and the item it moves
    // in (the subquery member, by a query-back that runs after the commit).
    const granted = await write('INSERT INTO grants (id, org_id, ws) VALUES (2, 1, 20)')
    await drainEngine(h)
    const edited = await write(`UPDATE items SET title = 'b2' WHERE id = 2`)
    await drainEngine(h)
    // And out again: the grant's removal, and the two items that depended on it.
    const revoked = await write('DELETE FROM grants WHERE id = 1')
    await drainEngine(h)

    // A group stream lands through its own lane, which the drain barrier does not wait for.
    const expected = 7
    await waitFor(async () => (await readEnvelopes(group.streamUrl)).length >= expected, 'the group stream to catch up')
    const envs = await readEnvelopes(group.streamUrl)
    const names = envs.map((env) => `${env.headers.operation} ${env.type} ${env.key}`)
    expect([...names].sort()).toEqual(
      [
        'upsert public.items 3',
        'upsert public.grants 2',
        'upsert public.items 2',
        'upsert public.items 2',
        'delete public.grants 1',
        'delete public.items 1',
        'delete public.items 3',
      ].sort(),
    )

    expectPositions(envs)
    const of = (name: string) => envs.filter((_, at) => names[at] === name)
    const txids = (name: string) => of(name).map((env) => env.headers.txid)
    expect(txids('upsert public.items 3')).toEqual([arrived])
    expect(txids('upsert public.grants 2')).toEqual([granted])
    expect(txids('upsert public.items 2').sort()).toEqual([granted, edited].sort())
    expect(txids('delete public.grants 1')).toEqual([revoked])
    expect(txids('delete public.items 1')).toEqual([revoked])
    expect(txids('delete public.items 3')).toEqual([revoked])
    // The lane keeps a group in commit order, so a flip's verdict sits at its own commit's position:
    // exactly where the plain member's envelope of that same transaction sits.
    const moved = of('upsert public.items 2').find((env) => env.headers.txid === granted)!
    expect(moved.headers.lsn).toBe(of('upsert public.grants 2')[0]!.headers.lsn)
    for (const name of ['delete public.items 1', 'delete public.items 3']) {
      expect(of(name)[0]!.headers.lsn).toBe(of('delete public.grants 1')[0]!.headers.lsn)
    }
  }, 120000)

  // The interleaving that decides the stamping rule. A grant counts only while its org is active,
  // so deactivating the org is resolved in two deferred steps: re-derive the grants' inner set,
  // then query back the items that depended on it. Until the first step has run, the engine still
  // believes the grant stands.
  //
  //   T1  deactivate the org            (its deferred work is parked on a lock)
  //   T2  edit item 1                   -> live upsert at T2: the engine still thinks it is a member
  //       release the lock              -> the verdict T1 caused: item 1 is not a member
  //
  // T1's verdict is correct and is the last thing the stream says about item 1, and it was caused
  // by a commit OLDER than the upsert before it. At T1's own position a client that had applied
  // the upsert would discard it and keep the row for good.
  it('a flip’s verdict that follows a newer live change to the same key still reaches the client', async () => {
    await pgQuery(h, 'INSERT INTO orgs (id, active) VALUES (1, true), (2, true)')
    await pgQuery(h, 'INSERT INTO grants (id, org_id, ws) VALUES (1, 1, 10), (2, 2, 20)')
    await pgQuery(h, `INSERT INTO items (id, ws, title) VALUES (1, 10, 'a'), (2, 10, 'b'), (3, 20, 'c')`)
    await drainEngine(h)

    const sub = await h.client.subset({ table: 'items', where: inActiveGrants })
    const held = () => (sub.collection.toArray as unknown as Row[]).map((row) => `${Number(row.id)}:${String(row.title)}`).sort()
    // A second claim on the same feed, only to learn the stream the client is reading.
    const feed = await createShape(h, { table: 'items', where: inActiveGrants, changesOnly: true })
    try {
      await waitFor(() => held().length === 3, 'the snapshot to load')

      const lock = await lockTable(h, 'grants')
      let deactivated: string
      let edited: string
      try {
        deactivated = await write('UPDATE orgs SET active = false WHERE id = 1')
        await waitFor(
          async () => (await tableLockWaiters(h, 'grants')).length > 0,
          'the re-derivation of the grants set to park on the lock',
        )
        edited = await write(`UPDATE items SET title = 'a2' WHERE id = 1`)
        await waitFor(() => held().includes('1:a2'), 'the client to apply the newer live change')
      } finally {
        await lock.release()
      }
      await drainEngine(h)

      // The stream said, in this order: the live change, then the verdict the older commit caused.
      const envs = await readEnvelopes(feed.streamUrl)
      expect(said(envs)[0]).toBe('upsert 1')
      expect(said(envs).slice(1).sort()).toEqual(['delete 1', 'delete 2'])

      // What matters: the client ends on what Postgres holds.
      const expected = (await pgQuery(h, inActiveGrantsSql)).map((row) => `${Number(row.id)}:${String(row.title)}`)
      expect(expected).toEqual(['3:c'])
      try {
        await waitFor(() => JSON.stringify(held()) === JSON.stringify(expected), 'the client to equal Postgres', 8000)
      } catch {
        expect(held(), 'the client must end on what Postgres holds').toEqual(expected)
      }

      // And why it does: every envelope is placed, the verdict names the commit that caused it, and
      // it is not placed below the change it follows.
      const positions = expectPositions(envs)
      expect(envs.map((env) => env.headers.txid)).toEqual([edited, deactivated, deactivated])
      const removal = said(envs).indexOf('delete 1')
      expect(positions[removal]! >= positions[0]!, 'the verdict is not placed below the change it follows').toBe(true)
    } finally {
      await sub.close()
    }
  }, 120000)
})
