// The engine's SQL writer rewrites an `OR` of `IN (subquery)` tests before it prints it
// (`apps/engine/src/sql.rs`, `JsonSql::or`):
//
//   1. `c IN (s1) OR c IN (s2)` becomes `c IN (s1 UNION ALL s2)` when both subqueries project one
//      SQL type;
//   2. an `OR` with `k IN (…)` on the table's single-column primary key gathers every other branch
//      B into that union as `SELECT k FROM <table> WHERE B` — only where the `OR` is a filter, not
//      under a `NOT`.
//
// Both exist so Postgres can drive the table from a set of keys instead of scanning it; neither may
// change a row. This suite runs generated predicates — nested `OR`s, subqueries on other tables
// (and on the same table), `NOT IN`, `NOT`, NULL cells, NULL projections, NULL literals and
// projections of a different type that must be left alone — through the engine's subset query,
// which prints the NEW SQL, and through the protocol's `predicateToSql`, which prints the old form
// (the rewrite-free mirror of the engine's writer), against the same Postgres, and asserts the same
// rows. A handful are also created as subquery shapes, whose backfill uses the same writer.

import { type Predicate, predicateToSql, type Row, type Schema } from '@electric-circuits/protocol'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import { createShape, foldStream, pgQuery } from './engine-native.js'
import { bootHarness, drainEngine, type Harness } from './harness.js'

const schema: Schema = {
  tables: {
    parents: { columns: { id: { type: 'int' }, flag: { type: 'bool' }, tag: { type: 'text' } }, primaryKey: 'id' },
    others: { columns: { id: { type: 'int' }, parent_id: { type: 'int' }, n: { type: 'int' } }, primaryKey: 'id' },
    wide: { columns: { id: { type: 'int' }, n: { type: 'int' } }, primaryKey: 'id' },
    items: {
      columns: {
        id: { type: 'int' },
        parent_id: { type: 'int' },
        other_id: { type: 'int' },
        n: { type: 'int' },
        label: { type: 'text' },
      },
      primaryKey: 'id',
    },
  },
}

// `wide.id` is a `bigint`: a subquery projecting it is of another type than the `int` columns, so
// an `OR` that mixes it with them is one the rewrites must leave alone.
const ddl = `
  CREATE TABLE parents (id int PRIMARY KEY, flag boolean, tag text);
  ALTER TABLE parents REPLICA IDENTITY FULL;
  CREATE TABLE others (id int PRIMARY KEY, parent_id int, n int);
  ALTER TABLE others REPLICA IDENTITY FULL;
  CREATE TABLE wide (id bigint PRIMARY KEY, n int);
  ALTER TABLE wide REPLICA IDENTITY FULL;
  CREATE TABLE items (id int PRIMARY KEY, parent_id int, other_id int, n int, label text);
  ALTER TABLE items REPLICA IDENTITY FULL;
`

/** mulberry32: a small seeded PRNG, so a failure names the seed that replays it. */
function rng(seed: number): () => number {
  let a = seed >>> 0
  return () => {
    a = (a + 0x6d2b79f5) >>> 0
    let t = a
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

const SEED = Number(process.env.SQL_UNION_SEED ?? 20261119)
const PREDICATES = Number(process.env.SQL_UNION_PREDICATES ?? 240)

type Table = 'items' | 'parents' | 'others'

class Gen {
  constructor(private readonly r: () => number) {}
  pick<T>(xs: readonly T[]): T {
    return xs[Math.floor(this.r() * xs.length)]!
  }
  chance(p: number): boolean {
    return this.r() < p
  }

  /** A leaf on `table`: comparisons (NULL literal included), equality on text, null tests. */
  leaf(table: Table): Predicate {
    const k = Math.floor(this.r() * 8)
    switch (table) {
      case 'items':
        return this.pick<Predicate>([
          { col: 'n', op: this.pick(['eq', 'neq', 'lt', 'gte'] as const), value: k },
          { col: 'n', op: 'eq', value: null },
          { col: 'label', op: this.pick(['eq', 'neq'] as const), value: this.pick(['x', 'y']) },
          { col: 'n', isNull: this.chance(0.5) },
          { col: 'label', isNull: this.chance(0.5) },
          { col: 'id', op: this.pick(['lt', 'gt'] as const), value: 5 * k },
        ])
      case 'parents':
        return this.pick<Predicate>([
          { col: 'flag', op: 'eq', value: this.chance(0.5) },
          { col: 'tag', op: 'eq', value: this.pick(['a', 'b']) },
          { col: 'tag', isNull: this.chance(0.5) },
          { col: 'id', op: 'gt', value: k },
        ])
      case 'others':
        return this.pick<Predicate>([
          { col: 'n', op: this.pick(['eq', 'lt', 'gt'] as const), value: k % 6 },
          { col: 'parent_id', isNull: this.chance(0.5) },
          { col: 'id', op: 'lte', value: 2 * k },
        ])
    }
  }

  /** An inner `WHERE`, or none. Inner predicates nest `OR`s and subqueries of their own. */
  inner(table: Table, depth: number): Predicate | undefined {
    if (this.chance(0.25)) return undefined
    return this.pred(table, depth)
  }

  /** `IN (subquery)` tests on `table`, by column; `key` marks the ones on the primary key. */
  subqueries(table: Table, depth: number): { col: string; key: boolean; make: () => Predicate }[] {
    const sq = (col: string, inner: string, project: string, innerTable: Table | 'wide') => () => ({
      col,
      in: {
        table: inner,
        project,
        ...(innerTable === 'wide' ? {} : { where: this.inner(innerTable, depth + 1) }),
      },
      ...(this.chance(0.15) ? { negated: true } : {}),
    })
    switch (table) {
      case 'items':
        return [
          { col: 'parent_id', key: false, make: sq('parent_id', 'parents', 'id', 'parents') },
          { col: 'parent_id', key: false, make: sq('parent_id', 'others', 'parent_id', 'others') },
          { col: 'parent_id', key: false, make: sq('parent_id', 'wide', 'id', 'wide') },
          { col: 'other_id', key: false, make: sq('other_id', 'others', 'id', 'others') },
          { col: 'id', key: true, make: sq('id', 'others', 'n', 'others') },
          { col: 'id', key: true, make: sq('id', 'parents', 'id', 'parents') },
          { col: 'id', key: true, make: sq('id', 'items', 'other_id', 'items') },
          { col: 'id', key: true, make: sq('id', 'wide', 'id', 'wide') },
        ]
      case 'parents':
        return [
          { col: 'id', key: true, make: sq('id', 'others', 'parent_id', 'others') },
          { col: 'id', key: true, make: sq('id', 'items', 'parent_id', 'items') },
        ]
      case 'others':
        return [
          { col: 'parent_id', key: false, make: sq('parent_id', 'parents', 'id', 'parents') },
          { col: 'id', key: true, make: sq('id', 'items', 'other_id', 'items') },
        ]
    }
  }

  /**
   * A predicate on `table`. An `OR` is the node under test, so it is the likeliest one, and half
   * the time its branches are built around one column's `IN` tests (the key's or another's).
   */
  pred(table: Table, depth: number): Predicate {
    if (depth >= 3) return this.leaf(table)
    const roll = this.r()
    if (roll < 0.45) {
      const branches: Predicate[] = []
      const subs = this.subqueries(table, depth)
      if (this.chance(0.6)) {
        const col = this.pick(subs).col
        const same = subs.filter((s) => s.col === col)
        for (let i = 0; i < 2 + Math.floor(this.r() * 2); i++) branches.push(this.pick(same).make())
      }
      const extra = 1 + Math.floor(this.r() * 2)
      for (let i = 0; i < extra; i++) {
        branches.push(this.chance(0.4) ? this.pick(subs).make() : this.chance(0.3) ? this.pred(table, depth + 1) : this.leaf(table))
      }
      // Shuffle, so a union's members are not always adjacent.
      for (let i = branches.length - 1; i > 0; i--) {
        const j = Math.floor(this.r() * (i + 1))
        ;[branches[i], branches[j]] = [branches[j]!, branches[i]!]
      }
      return { or: branches }
    }
    if (roll < 0.65) return { and: [this.pred(table, depth + 1), this.pred(table, depth + 1)] }
    if (roll < 0.8) return { not: this.pred(table, depth + 1) }
    if (roll < 0.9) return this.pick(this.subqueries(table, depth)).make()
    return this.leaf(table)
  }
}

/** The `OR`s in `p` that a rewrite can apply to, by the predicate's shape alone (types aside). */
function rewritable(p: Predicate, table: string, filter = true, counts = { union: 0, key: 0 }): typeof counts {
  const node = p as unknown as Record<string, unknown>
  if (Array.isArray(node.or)) {
    const ins = (node.or as Record<string, unknown>[]).filter((b) => b.in && !b.negated)
    const cols = ins.map((b) => b.col as string)
    if (cols.some((c, i) => cols.indexOf(c) !== i)) counts.union++
    if (filter && cols.includes('id') && (node.or as unknown[]).length > 1) counts.key++
    for (const b of node.or as Predicate[]) rewritable(b, table, filter, counts)
  } else if (Array.isArray(node.and)) {
    for (const b of node.and as Predicate[]) rewritable(b, table, filter, counts)
  } else if (node.not) {
    rewritable(node.not as Predicate, table, false, counts)
  }
  if (node.in) {
    const sq = node.in as { table: string; where?: Predicate }
    if (sq.where) rewritable(sq.where, sq.table, true, counts)
  }
  return counts
}

let h: Harness

beforeAll(async () => {
  h = await bootHarness(schema, { ddl })
  const r = rng(SEED)
  const maybe = <T>(value: T, pNull: number): T | null => (r() < pNull ? null : value)
  const int = (n: number) => Math.floor(r() * n)
  for (let id = 1; id <= 12; id++) {
    await pgQuery(h, 'INSERT INTO parents (id, flag, tag) VALUES ($1, $2, $3)', [
      id,
      maybe(r() < 0.5, 0.2),
      maybe(['a', 'b', 'c'][int(3)], 0.25),
    ])
  }
  for (let id = 1; id <= 16; id++) {
    await pgQuery(h, 'INSERT INTO others (id, parent_id, n) VALUES ($1, $2, $3)', [id, maybe(1 + int(14), 0.2), maybe(int(70), 0.2)])
  }
  for (let id = 1; id <= 10; id++) await pgQuery(h, 'INSERT INTO wide (id, n) VALUES ($1, $2)', [id * 3, int(5)])
  for (let id = 1; id <= 60; id++) {
    await pgQuery(h, 'INSERT INTO items (id, parent_id, other_id, n, label) VALUES ($1, $2, $3, $4, $5)', [
      id,
      maybe(1 + int(14), 0.15),
      maybe(1 + int(18), 0.15),
      maybe(int(8), 0.2),
      maybe(['x', 'y', 'z'][int(3)], 0.2),
    ])
  }
  await drainEngine(h)
}, 120000)

afterAll(async () => {
  await h?.shutdown()
})

/** The ids Postgres selects for `where`, through the protocol's rewrite-free SQL. */
async function oldSqlIds(table: string, where: Predicate): Promise<number[]> {
  const { text, params } = predicateToSql(where)
  const rows = await pgQuery(h, `SELECT id FROM "public"."${table}" WHERE ${text}`, params)
  return rows.map((row) => Number(row.id)).sort((a, b) => a - b)
}

/** The ids the engine's subset query selects for `where`, through the engine's (rewritten) SQL. */
async function engineIds(table: string, where: Predicate): Promise<number[]> {
  const res = await fetch(`${h.engineUrl}/v1/subsets/query`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ table, where, orderBy: { col: 'id' }, limit: 10000 }),
  })
  const text = await res.text()
  if (!res.ok) throw new Error(`subset query on ${table} answered ${res.status}: ${text}\n${JSON.stringify(where)}`)
  return ((JSON.parse(text) as { rows: Row[] }).rows ?? []).map((row) => Number(row.id)).sort((a, b) => a - b)
}

describe('SQL writer: OR-of-IN rewrites select exactly the rows of the SQL they replace', () => {
  it(`generated predicates (seed ${SEED}) select the same rows through the engine as through the old SQL`, async () => {
    const gen = new Gen(rng(SEED + 1))
    const tables: Table[] = ['items', 'items', 'items', 'parents', 'others']
    const counts = { union: 0, key: 0 }
    let nonEmpty = 0
    for (let i = 0; i < PREDICATES; i++) {
      const table = gen.pick(tables)
      const where = gen.pred(table, 0)
      rewritable(where, table, true, counts)
      const expected = await oldSqlIds(table, where)
      if (expected.length > 0) nonEmpty++
      expect(await engineIds(table, where), `predicate #${i} on ${table} (seed ${SEED}): ${JSON.stringify(where)}`).toEqual(
        expected,
      )
    }
    // The generator has to have reached both rewrites often, and selected rows, or the equality
    // above proves nothing about them.
    expect(counts.union, 'ORs with same-column IN siblings').toBeGreaterThan(PREDICATES / 4)
    expect(counts.key, 'ORs with an IN on the key and another branch, outside a NOT').toBeGreaterThan(PREDICATES / 4)
    expect(nonEmpty, 'predicates that select at least one row').toBeGreaterThan(PREDICATES / 3)
  }, 300000)

  // Hand-written cases for each rule and each thing it leaves alone, on rows chosen so that the
  // three-valued answers differ: a NULL cell, a NULL in a projection, an empty subquery.
  const cases: { name: string; table: Table; where: Predicate }[] = [
    {
      name: 'rule 1: same-column IN siblings, one of them projecting a NULL',
      table: 'items',
      where: {
        or: [
          { col: 'parent_id', in: { table: 'parents', project: 'id', where: { col: 'flag', op: 'eq', value: true } } },
          { col: 'parent_id', in: { table: 'others', project: 'parent_id' } },
        ],
      },
    },
    {
      name: 'rule 1 under NOT: NOT (c IN s1 OR c IN s2) over a NULL projection',
      table: 'items',
      where: {
        not: {
          or: [
            { col: 'parent_id', in: { table: 'parents', project: 'id', where: { col: 'tag', op: 'eq', value: 'a' } } },
            { col: 'parent_id', in: { table: 'others', project: 'parent_id', where: { col: 'n', op: 'lt', value: 30 } } },
          ],
        },
      },
    },
    {
      name: 'rule 2: the key IN, a nullable comparison and an IN on another column',
      table: 'items',
      where: {
        or: [
          { col: 'id', in: { table: 'others', project: 'n', where: { col: 'n', op: 'lt', value: 40 } } },
          { col: 'n', op: 'gt', value: 5 },
          { col: 'other_id', in: { table: 'others', project: 'id', where: { col: 'parent_id', isNull: true } } },
        ],
      },
    },
    {
      name: 'rule 2 is not made under NOT, where UNKNOWN and FALSE differ',
      table: 'items',
      where: {
        not: { or: [{ col: 'id', in: { table: 'parents', project: 'id' } }, { col: 'n', op: 'gt', value: 3 }] },
      },
    },
    {
      name: 'left alone: NOT IN beside IN on one column',
      table: 'items',
      where: {
        or: [
          { col: 'parent_id', negated: true, in: { table: 'others', project: 'parent_id' } },
          { col: 'parent_id', in: { table: 'parents', project: 'id', where: { col: 'flag', op: 'eq', value: false } } },
        ],
      },
    },
    {
      name: 'left alone: a bigint projection beside an int one',
      table: 'items',
      where: {
        or: [
          { col: 'parent_id', in: { table: 'wide', project: 'id' } },
          { col: 'parent_id', in: { table: 'parents', project: 'id', where: { col: 'tag', isNull: true } } },
          { col: 'id', in: { table: 'wide', project: 'id' } },
          { col: 'label', op: 'eq', value: 'x' },
        ],
      },
    },
    {
      name: 'every depth: rule 2 inside a subquery, rule 1 inside an AND',
      table: 'items',
      where: {
        and: [
          { col: 'n', isNull: false },
          {
            or: [
              {
                col: 'parent_id',
                in: {
                  table: 'parents',
                  project: 'id',
                  where: {
                    or: [
                      { col: 'id', in: { table: 'others', project: 'parent_id', where: { col: 'n', op: 'gt', value: 20 } } },
                      { col: 'tag', op: 'eq', value: 'b' },
                    ],
                  },
                },
              },
              { col: 'parent_id', in: { table: 'others', project: 'parent_id', where: { col: 'n', isNull: true } } },
            ],
          },
        ],
      },
    },
  ]
  for (const c of cases) {
    it(c.name, async () => {
      expect(await engineIds(c.table, c.where), JSON.stringify(c.where)).toEqual(await oldSqlIds(c.table, c.where))
    })
  }

  // A shape's backfill prints its predicate with the same writer, so a few of the cases above are
  // also created as subquery shapes and their streams compared with the old SQL's rows.
  it('a subquery shape backfills the same rows as the old SQL', async () => {
    for (const [i, c] of cases.entries()) {
      const shape = await createShape(h, { table: c.table, where: c.where, subscription: `or-union-${i}` })
      await drainEngine(h)
      const held = [...(await foldStream(shape.streamUrl)).keys()].map(Number).sort((a, b) => a - b)
      expect(held, `${c.name}: ${JSON.stringify(c.where)}`).toEqual(await oldSqlIds(c.table, c.where))
    }
  }, 120000)
})
