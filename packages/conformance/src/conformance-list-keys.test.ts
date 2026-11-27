// The list rule (`ELECTRIC_CIRCUITS_LIST_KEYS=1`) against the oracle, through real Postgres
// replication. With the rule on, a filter whose OR has an equality in every branch is filed under
// every one of those keys instead of on the engine's fallback scan list, so a change is offered
// only to the shapes filed under its values. A wrong rule drops rows silently, and the change it
// is most likely to drop is a removal: a row whose key moved away no longer carries the key its
// old reader is filed under. So every reader here is compared with Postgres after rows move
// between readers, between a link's two ends, to NULL, and are deleted.
//
// `GET /tables/:name/families` reports how many standalone shapes are still on the scan list
// (`scan`), which is how the test knows the rule is on and which shapes it filed.

import type { Row, Schema, ShapeDef } from '@electric-circuits/protocol'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { compareShapeSets, formatCompare } from './compare.js'
import { testPhysicalPath } from './ds-mtls-access.js'
import { applyOp, bootHarness, drainEngine, type Harness, waitForConvergence } from './harness.js'
import { createSimulator, randomShapeDefs } from './simulator.js'

const schema: Schema = {
  tables: {
    links: {
      columns: {
        id: { type: 'int' },
        from_space: { type: 'text' },
        to_space: { type: 'text' },
        kind: { type: 'text' },
        n: { type: 'int' },
      },
      primaryKey: 'id',
    },
  },
}
const COLUMNS = ['id', 'from_space', 'to_space', 'kind', 'n']
const LIST_KEYS = { ELECTRIC_CIRCUITS_LIST_KEYS: '1' }
const READERS = 12

type Where = NonNullable<ShapeDef['where']>
const eq = (col: string, value: string): Where => ({ col, op: 'eq', value })
const home = (r: number) => `home-${r}`
const shared = (r: number) => `shared-${r}`

/** A reader of two Spaces, by either end of a link: a list at the top. */
const byEitherEnd = (r: number): Where => ({
  or: [
    { or: [eq('from_space', home(r)), eq('from_space', shared(r))] },
    { or: [eq('to_space', home(r)), eq('to_space', shared(r))] },
  ],
})
/** Branches by link kind, each testing an end, under a row condition: a list in the AND chain. */
const byKind = (r: number): Where => ({
  and: [
    { col: 'n', isNull: false },
    {
      or: [
        { and: [eq('kind', 'a'), { or: [eq('from_space', home(r))] }] },
        { and: [{ or: [eq('kind', 'b'), eq('kind', 'c')] }, { or: [eq('to_space', home(r)), eq('to_space', shared(r))] }] },
      ],
    },
  ],
})
/** A list with a branch no key can be made of: it has to stay on the scan list. */
const withARange = (r: number): Where => ({ or: [eq('from_space', home(r)), { col: 'n', op: 'gt', value: 900 }] })

interface Families {
  standalone: number
  scan: number
}
async function families(h: Harness): Promise<Families> {
  const res = await fetch(`${h.engineUrl}/tables/links/families`)
  if (!res.ok) throw new Error(`families endpoint -> ${res.status}`)
  return (await res.json()) as Families
}

type Shape = Awaited<ReturnType<Harness['client']['shape']>>

describe('conformance: list filters filed under every key (ELECTRIC_CIRCUITS_LIST_KEYS=1)', () => {
  let h: Harness
  const shapes: { def: ShapeDef; shape: Shape }[] = []

  beforeAll(async () => {
    h = await bootHarness(schema, { engineEnv: LIST_KEYS })
    for (let r = 0; r < READERS; r++) {
      for (const where of [byEitherEnd(r), byKind(r), withARange(r)]) {
        const def: ShapeDef = { table: 'links', where }
        shapes.push({ def, shape: await h.client.shape(def) })
      }
    }
  }, 120000)
  afterAll(async () => {
    await h?.shutdown()
  })

  async function expectEveryReaderMatchesPostgres(when: string): Promise<void> {
    await drainEngine(h)
    for (const { def, shape } of shapes) {
      const res = await waitForConvergence(h, { shape, def, columns: COLUMNS, pk: 'id' })
      expect(res.equal, `${when}: ${JSON.stringify(def.where)}\n${formatCompare(res)}`).toBe(true)
    }
  }

  it('files every list filter and leaves only the mixed ones on the scan list', async () => {
    const stats = await families(h)
    expect(stats.standalone).toBe(READERS * 3)
    expect(stats.scan).toBe(READERS) // one `withARange` per reader; nothing else
  })

  it('delivers inserts, moves between readers and ends, NULL keys and deletes exactly as Postgres filters them', async () => {
    const rows = new Map<number, Row>()
    const write = async (op: 'insert' | 'update', row: Row) => {
      rows.set(row.id as number, row)
      await applyOp(h, 'links', { op, pk: row.id as number, row })
    }
    for (let pk = 1; pk <= 72; pk++) {
      await write('insert', {
        id: pk,
        from_space: pk % 3 === 0 ? shared(pk % READERS) : home(pk % READERS),
        to_space: pk % 5 === 0 ? null : shared((pk * 7) % READERS),
        kind: ['a', 'b', 'c'][pk % 3]!,
        n: pk % 11 === 0 ? null : pk * 20,
      })
    }
    await expectEveryReaderMatchesPostgres('after the inserts')

    // A key moves to another reader's value: the reader it left has to be told.
    for (let pk = 1; pk <= 20; pk++) await write('update', { ...rows.get(pk)!, from_space: home((pk + 5) % READERS) })
    // A value moves from one end's column to the other's.
    for (let pk = 21; pk <= 32; pk++) {
      const row = rows.get(pk)!
      await write('update', { ...row, from_space: row.to_space ?? null, to_space: row.from_space ?? null })
    }
    // Both keys become NULL: a NULL key equals nothing, so the row leaves every keyed reader.
    for (let pk = 33; pk <= 40; pk++) await write('update', { ...rows.get(pk)!, from_space: null, to_space: null })
    // A column no reader key is on changes: the row leaves or enters by the rest of the filter.
    for (let pk = 41; pk <= 50; pk++) await write('update', { ...rows.get(pk)!, kind: 'c', n: pk % 2 === 0 ? null : 950 })
    await expectEveryReaderMatchesPostgres('after the moves')

    // ... and back out of NULL, into a reader's Space.
    for (let pk = 33; pk <= 36; pk++) await write('update', { ...rows.get(pk)!, to_space: shared(pk % READERS) })
    for (let pk = 51; pk <= 62; pk++) await applyOp(h, 'links', { op: 'delete', pk })
    await expectEveryReaderMatchesPostgres('after the deletes')
  }, 180000)
})

/** Fold a shape stream's raw envelopes into the current row set (upsert/delete by key). */
async function streamRows(dsUrl: string, streamPath: string): Promise<Row[]> {
  const rows = new Map<string, Row>()
  let offset = '-1'
  for (;;) {
    const res = await fetch(`${dsUrl}/${testPhysicalPath(streamPath)}?offset=${offset}`)
    if (!res.ok) throw new Error(`read ${streamPath} -> ${res.status}`)
    const next = res.headers.get('stream-next-offset')
    const body = res.status === 204 ? [] : ((await res.json()) as Array<{ key: string; value?: Row; headers: { operation: string } }>)
    for (const env of body) {
      if (env.headers.operation === 'delete') rows.delete(env.key)
      else if (env.value) rows.set(env.key, env.value)
    }
    if (!next || next === offset || res.headers.get('stream-up-to-date')) break
    offset = next
  }
  return [...rows.values()]
}

// A restart restores every shape from the catalog and files it again. The harness's API keeps
// pointing at the engine that was killed, so this talks to the engine and its streams directly,
// as the other restart tests do, and holds no client shape for teardown to release.
describe('conformance: list filters are filed again after a restart', () => {
  let h: Harness
  beforeAll(async () => {
    h = await bootHarness(schema, { engineEnv: LIST_KEYS })
  }, 60000)
  afterAll(async () => {
    await h?.shutdown()
  })

  it('restores each shape under its keys and keeps delivering moves and removals', async () => {
    const created: { def: ShapeDef; streamPath: string }[] = []
    for (let r = 0; r < READERS; r++) {
      for (const where of [byEitherEnd(r), byKind(r), withARange(r)]) {
        const def: ShapeDef = { table: 'links', where }
        const res = await fetch(`${h.engineUrl}/shapes`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify(def),
        })
        expect(res.ok, `create ${JSON.stringify(where)} -> ${res.status}`).toBe(true)
        created.push({ def, streamPath: ((await res.json()) as { streamPath: string }).streamPath })
      }
    }
    const row = (pk: number, shift: number): Row => ({
      id: pk,
      from_space: pk % 4 === 0 ? null : home((pk + shift) % READERS),
      to_space: shared((pk * 5 + shift) % READERS),
      kind: ['a', 'b', 'c'][(pk + shift) % 3]!,
      n: pk + shift,
    })
    for (let pk = 1; pk <= 36; pk++) await applyOp(h, 'links', { op: 'insert', pk, row: row(pk, 0) })
    await drainEngine(h)

    await h.restartEngine()
    expect(await families(h)).toMatchObject({ standalone: READERS * 3, scan: READERS })

    // After the restart every row moves to other readers' keys, and some rows are deleted.
    for (let pk = 1; pk <= 30; pk++) await applyOp(h, 'links', { op: 'update', pk, row: row(pk, 3) })
    for (let pk = 31; pk <= 36; pk++) await applyOp(h, 'links', { op: 'delete', pk })
    await drainEngine(h)
    for (const { def, streamPath } of created) {
      const res = compareShapeSets(COLUMNS, 'id', await h.oracle.queryShape(def), await streamRows(h.dsUrl, streamPath))
      expect(res.equal, `after the restart: ${JSON.stringify(def.where)}\n${formatCompare(res)}`).toBe(true)
    }
  }, 180000)
})

// The oracle fuzz with the rule on: random predicates (ORs of every shape among them) over an op
// stream with NULLs in a third of the cells, compared row for row with Postgres.
const SEEDS = Number(process.env.LIST_KEYS_FUZZ_SEEDS ?? 3)
const SHAPES = Number(process.env.LIST_KEYS_FUZZ_SHAPES ?? 16)
const OPS = Number(process.env.LIST_KEYS_FUZZ_OPS ?? 300)

describe('conformance: list rule fuzz vs oracle', () => {
  it(
    `holds the oracle invariant with the rule on across ${SEEDS} scenarios`,
    async () => {
      const base = process.env.SEED ? Number(process.env.SEED) : 14382
      for (let i = 0; i < SEEDS; i++) {
        const seed = base + i * 7919
        const h = await bootHarness(schema, { engineEnv: LIST_KEYS })
        try {
          const defs = randomShapeDefs(schema, seed, SHAPES, { maxDepth: 3, edgeLiterals: true })
          const shapes = await Promise.all(defs.map((d) => h.client.shape(d)))
          for (const { table, ev } of createSimulator(schema, { seed, nullProb: 0.35 }).take(OPS)) {
            await applyOp(h, table, ev)
          }
          await drainEngine(h)
          for (let s = 0; s < defs.length; s++) {
            const def = defs[s]!
            const res = await waitForConvergence(h, { shape: shapes[s]!, def, columns: COLUMNS, pk: 'id' })
            expect(
              res.equal,
              `FAILED seed=${seed} shape#${s}=${JSON.stringify(def.where ?? 'ALL')}\n${formatCompare(res)}`,
            ).toBe(true)
          }
        } finally {
          await h.shutdown()
        }
      }
    },
    300000,
  )
})
