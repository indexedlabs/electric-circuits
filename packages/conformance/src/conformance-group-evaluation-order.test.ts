// A group stream says, for every key, what the engine decided LAST — as a single shape stream does.
//
// A group (`POST /v1/subsets/load`) delivers several changes-only feeds on one stream through its
// own lane, which keeps the stream in commit order: a commit's deferred flip verdict lands before
// the next commit's output. That order was applied to every batch, and it is wrong for one key
// whose two verdicts were not evaluated in commit order. With a NESTED subquery a commit T reaches
// the outer rows in two deferred steps (re-derive the middle set, then query back the rows), and
// until the first has run the engine judges those rows against a middle set T already made stale:
//
//   T    deactivate the org            (its re-derivation of the grants set is parked on a lock)
//   T+1  edit item 1                   -> live verdict at T+1: "member", from the stale grants set
//        release the lock              -> T's verdict, evaluated last: item 1 is not a member
//
// T's verdict is the correct one and the last one evaluated. Queued by commit, it was appended
// AHEAD of the T+1 upsert, so the stream ended `delete 1, upsert 1` and a reader kept a row
// Postgres does not hold — by offset and by position alike, and until the row changed again.
//
// The rule (`apps/engine/src/engine/emission.rs`): a group stream carries each key's envelopes in
// the order they were evaluated; everything else stays in commit order. The second test is that
// second half: the same interleaving with the T+1 change on ANOTHER key keeps T's verdict ahead of
// it, at T's own position.
//
// See docs/ARCHITECTURE.md §7, "Native envelope positions".

import type { Predicate, Schema, StreamEnvelope } from '@electric-circuits/protocol'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { lockTable, pgQuery, readEnvelopes, tableLockWaiters, waitFor } from './engine-native.js'
import {
  bootHarness,
  changesTail,
  drainEngine,
  engineChangesOffset,
  positionReached,
  type Harness,
} from './harness.js'

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

/** `items.ws IN (SELECT ws FROM grants WHERE org_id IN (SELECT id FROM orgs WHERE active))`. */
const inActiveGrants: Predicate = {
  col: 'ws',
  in: {
    table: 'grants',
    project: 'ws',
    where: { col: 'org_id', in: { table: 'orgs', project: 'id', where: { col: 'active', op: 'eq', value: true } } },
  },
}
// The same scope in SQL: what Postgres says is in it, independently of the engine's predicate.
const inActiveGrantsSql = `
  SELECT id, title FROM items
   WHERE ws IN (SELECT ws FROM grants WHERE org_id IN (SELECT id FROM orgs WHERE active))
   ORDER BY id`

let h: Harness
beforeEach(async () => {
  h = await bootHarness(schema)
  await pgQuery(h, 'INSERT INTO orgs (id, active) VALUES (1, true), (2, true)')
  await pgQuery(h, 'INSERT INTO grants (id, org_id, ws) VALUES (1, 1, 10), (2, 2, 20)')
  await pgQuery(h, `INSERT INTO items (id, ws, title) VALUES (1, 10, 'a'), (2, 10, 'b'), (3, 20, 'c')`)
  await drainEngine(h)
}, 60000)
afterEach(async () => {
  await h?.shutdown()
})

interface GroupLoad {
  streamUrl: string
  rows: { table: string; value: { id: number; title?: string } }[]
}

async function loadGroup(members: { table: string; where?: Predicate; feedWhere?: Predicate }[]): Promise<GroupLoad> {
  const res = await fetch(`${h.engineUrl}/v1/subsets/load`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ members }),
  })
  expect(res.status, await res.clone().text()).toBe(200)
  return (await res.json()) as GroupLoad
}

/** Run one writing statement and return its transaction id as an envelope spells it. */
async function write(sql: string): Promise<string> {
  const rows = await pgQuery(h, `${sql} RETURNING (pg_current_xact_id()::text::numeric % 4294967296)::text AS xid`)
  expect(rows.length, `"${sql}" must change a row`).toBeGreaterThan(0)
  return String(rows[0]!.xid)
}

async function pendingFlips(): Promise<number> {
  const res = await fetch(`${h.engineUrl}/replication/lsn`)
  expect(res.ok, `engine replication status -> ${res.status}`).toBe(true)
  return Number(((await res.json()) as { pendingFlips?: number }).pendingFlips ?? 0)
}

/**
 * The sequencer has processed every change committed so far: the first two stages of
 * `drainEngine`, without its third. Deferred flips may still be parked, which is the point — the
 * tests below release them only once a newer commit has been evaluated ahead of them.
 */
async function sequenced(): Promise<void> {
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

/**
 * Deactivate org 1 with its re-derivation of the grants set parked on a lock, commit `later` and
 * let the engine evaluate it, then release the lock so the older commit's verdict is evaluated
 * last. Returns both transaction ids.
 */
async function deactivateThen(later: string): Promise<{ deactivated: string; edited: string }> {
  const lock = await lockTable(h, 'grants')
  try {
    const deactivated = await write('UPDATE orgs SET active = false WHERE id = 1')
    await waitFor(
      async () => (await tableLockWaiters(h, 'grants')).length > 0,
      'the re-derivation of the grants set to park on the lock',
    )
    const edited = await write(later)
    await sequenced()
    expect(await pendingFlips(), 'the older commit’s flip is still parked').toBeGreaterThan(0)
    return { deactivated, edited }
  } finally {
    await lock.release()
  }
}

/** A `HEX/HEX` position as one comparable number, the way `pg::lsn_to_u64` reads it. */
function position(env: StreamEnvelope): bigint {
  const lsn = env.headers.lsn
  expect(lsn, `${env.headers.operation} ${env.type} ${env.key} carries no lsn`).toMatch(/^[0-9A-F]+\/[0-9A-F]+$/)
  const [hi, lo] = lsn!.split('/')
  return (BigInt(`0x${hi}`) << 32n) | BigInt(`0x${lo}`)
}

// A group stream names a row by its table and key (`public.items 1`): keys of different members
// never collide, and the load's snapshot rows spell the table the same way.
const said = (envs: StreamEnvelope[]) => envs.map((env) => `${env.headers.operation} ${env.type} ${env.key}`)
const label = (keys: Iterable<string>) => [...keys].sort()
const snapshot = (group: GroupLoad) => group.rows.map((row) => `${row.table} ${row.value.id}`)
/** The scope as Postgres holds it now, spelled the same way. */
const inPostgres = async () => (await pgQuery(h, inActiveGrantsSql)).map((row) => `public.items ${Number(row.id)}`)

/** What a reader that folds the stream by offset holds: the snapshot, then every envelope in order. */
function foldByOffset(group: GroupLoad, envs: StreamEnvelope[]): string[] {
  const held = new Set(snapshot(group))
  for (const env of envs) {
    const key = `${env.type} ${env.key}`
    if (env.headers.operation === 'delete') held.delete(key)
    else held.add(key)
  }
  return label(held)
}

/**
 * What a reader that orders by position holds (docs/ARCHITECTURE.md §7, rule 1): it applies an
 * envelope iff its position is at or above the last one it applied to that key.
 */
function foldByPosition(group: GroupLoad, envs: StreamEnvelope[]): string[] {
  const held = new Set(snapshot(group))
  const applied = new Map<string, bigint>()
  for (const env of envs) {
    const key = `${env.type} ${env.key}`
    const at = position(env)
    if (at < (applied.get(key) ?? 0n)) continue
    applied.set(key, at)
    if (env.headers.operation === 'delete') held.delete(key)
    else held.add(key)
  }
  return label(held)
}

async function groupStream(group: GroupLoad, count: number): Promise<StreamEnvelope[]> {
  // A group stream lands through its own lane, which the drain barrier does not wait for.
  await waitFor(async () => (await readEnvelopes(group.streamUrl)).length >= count, 'the group stream to catch up')
  return readEnvelopes(group.streamUrl)
}

describe('a group stream keeps each key’s envelopes in the order they were evaluated', () => {
  it('uses a member’s feed predicate for live changes and its snapshot predicate for the page', async () => {
    const group = await loadGroup([
      {
        table: 'items',
        where: inActiveGrants,
        feedWhere: { col: 'ws', op: 'eq', value: 10 },
      },
    ])
    expect(label(snapshot(group))).toEqual(['public.items 1', 'public.items 2', 'public.items 3'])

    await write(`INSERT INTO items (id, ws, title) VALUES (4, 20, 'feed-excluded'), (5, 10, 'feed-included')`)
    await drainEngine(h)
    const envs = await groupStream(group, 1)

    expect(said(envs)).toEqual(['upsert public.items 5'])
  })

  it('ends on what Postgres holds when a nested flip’s verdict follows a newer live change to the same key', async () => {
    const group = await loadGroup([{ table: 'items', where: inActiveGrants }])
    expect(label(snapshot(group))).toEqual(['public.items 1', 'public.items 2', 'public.items 3'])

    const { deactivated, edited } = await deactivateThen(`UPDATE items SET title = 'a2' WHERE id = 1`)
    await drainEngine(h)
    const envs = await groupStream(group, 3)

    // What matters: a reader ends on what Postgres holds, whichever way it orders the stream.
    const expected = await inPostgres()
    expect(expected).toEqual(['public.items 3'])
    expect(foldByOffset(group, envs), `folded by offset: ${JSON.stringify(said(envs))}`).toEqual(expected)
    expect(foldByPosition(group, envs), `folded by position: ${JSON.stringify(said(envs))}`).toEqual(expected)

    // And why: the stale live verdict is still said, and the verdict that corrects it — caused by
    // the OLDER commit, evaluated last — comes after it, placed no lower.
    expect(said(envs)[0]).toBe('upsert public.items 1')
    expect(said(envs).slice(1).sort()).toEqual(['delete public.items 1', 'delete public.items 2'])
    expect(envs.map((env) => env.headers.txid)).toEqual([edited, deactivated, deactivated])
    const positions = envs.map(position)
    positions.forEach((at, i) => {
      if (i > 0) expect(at >= positions[i - 1]!, `position falls at envelope ${i}`).toBe(true)
    })
  }, 120000)

  it('still lands a flip’s verdict ahead of a later commit’s change to another key, at its own commit', async () => {
    // `orgs` is a plain member: its envelope for the deactivation marks that commit's position.
    const group = await loadGroup([{ table: 'items', where: inActiveGrants }, { table: 'orgs' }])

    const { deactivated, edited } = await deactivateThen(`UPDATE items SET title = 'c2' WHERE id = 3`)
    await drainEngine(h)
    const envs = await groupStream(group, 4)

    // Commit order: the deactivation's own row, the two items it removed, then the later edit.
    expect(said(envs)[0]).toBe('upsert public.orgs 1')
    expect(said(envs).slice(1, 3).sort()).toEqual(['delete public.items 1', 'delete public.items 2'])
    expect(said(envs)[3]).toBe('upsert public.items 3')
    expect(envs.map((env) => env.headers.txid)).toEqual([deactivated, deactivated, deactivated, edited])
    const positions = envs.map(position)
    expect(positions[1], 'the verdict sits at its own commit').toBe(positions[0])
    expect(positions[2]).toBe(positions[0])
    expect(positions[3]! > positions[0]!, 'the later commit sits above it').toBe(true)

    const items = (keys: string[]) => keys.filter((key) => key.startsWith('public.items '))
    const expected = await inPostgres()
    expect(expected).toEqual(['public.items 3'])
    expect(items(foldByOffset(group, envs))).toEqual(expected)
    expect(items(foldByPosition(group, envs))).toEqual(expected)
  }, 120000)
})
