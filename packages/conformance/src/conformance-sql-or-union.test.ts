// The engine's SQL writer rewrites an `OR` of `IN (subquery)` tests before it prints it
// (`apps/engine/src/sql.rs`, `JsonSql::or`): `c IN (s1) OR c IN (s2)` becomes
// `c IN (s1 UNION ALL s2)` when both subqueries project one known non-collatable SQL type (column
// collations are not available to the printer). An `OR` across columns, the table's key included,
// is printed as the client wrote it.
//
// The rewrite must not change a row. This suite runs generated predicates — nested `OR`s, subqueries on other tables
// (and on the same table), `NOT IN`, `NOT`, NULL cells, NULL projections, NULL literals and
// projections of a different type that must be left alone — through the engine's subset query,
// which prints the NEW SQL, and through the protocol's `predicateToSql`, which prints the old form
// (the rewrite-free mirror of the engine's writer), against the same Postgres, and asserts the same
// rows. A handful are also created as subquery shapes, whose backfill uses the same writer.

import { type Predicate, predicateToSql, type Row, type Schema } from '@electric-circuits/protocol'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import { createShape, foldStream, lockTable, pgQuery, waitFor } from './engine-native.js'
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
// an `OR` that mixes it with them is one the rewrite must leave alone.
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

/**
 * Query the production subset endpoint; text keys are kept as text for the collation case. With no
 * `limit` the query is unpaged.
 */
async function subsetRows(harness: Harness, table: string, where: Predicate, limit?: number, offset?: number): Promise<Row[]> {
  const res = await fetch(`${harness.engineUrl}/v1/subsets/query`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ table, where, orderBy: { col: 'id' }, limit, offset }),
    signal: AbortSignal.timeout(20000),
  })
  const text = await res.text()
  if (!res.ok) throw new Error(`subset query on ${table} answered ${res.status}: ${text}\n${JSON.stringify(where)}`)
  return (JSON.parse(text) as { rows: Row[] }).rows ?? []
}

async function engineIds(table: string, where: Predicate, limit?: number): Promise<number[]> {
  return (await subsetRows(h, table, where, limit)).map((row) => Number(row.id)).sort((a, b) => a - b)
}

/**
 * Park the real subset SELECT at a PostgreSQL relation lock, inspect the statement PostgreSQL
 * received, then release it and collect its rows. The lock is the causal gate: no log timing,
 * extension, engine instrumentation, or copy of the SQL printer is needed.
 */
async function subsetWithSql(
  harness: Harness,
  table: string,
  where: Predicate,
  limit?: number,
  offset?: number,
): Promise<{ sql: string; rows: Row[] }> {
  const lock = await lockTable(harness, `"public"."${table}"`)
  // Observe rejection immediately even while waiting for the relation-lock gate.
  let failure: unknown
  const pending = subsetRows(harness, table, where, limit, offset).catch((error: unknown) => {
    failure = error
    return []
  })
  let sql = ''
  try {
    await waitFor(async () => {
      if (failure) throw failure
      const blocked = await pgQuery(harness, `
        SELECT DISTINCT a.query
        FROM pg_stat_activity a
        JOIN pg_locks l ON l.pid = a.pid AND NOT l.granted
        WHERE a.datname = current_database() AND a.pid <> pg_backend_pid()
          AND a.wait_event_type = 'Lock' AND l.relation = $1::regclass
          AND a.query LIKE $2
      `, [`public.${table}`, `% from "public"."${table}" t where %`])
      if (blocked.length !== 1) return false
      sql = String(blocked[0]!.query)
      return true
    }, 'the subset SELECT waiting on its table lock', 15000)
  } finally {
    await lock.release()
    // Always settle the request before leaving this test's gate, including assertion/setup failure.
    await pending
  }
  if (failure) throw failure
  return { sql, rows: await pending }
}

describe('SQL writer: the OR-of-IN rewrite selects exactly the rows of the SQL it replaces', () => {
  it(`generated predicates (seed ${SEED}) select the same rows through the engine as through the old SQL`, async () => {
    const gen = new Gen(rng(SEED + 1))
    const tables: Table[] = ['items', 'items', 'items', 'parents', 'others']
    let nonEmpty = 0
    for (let i = 0; i < PREDICATES; i++) {
      const table = gen.pick(tables)
      const where = gen.pred(table, 0)
      const expected = await oldSqlIds(table, where)
      if (expected.length > 0) nonEmpty++
      const named = `predicate #${i} on ${table} (seed ${SEED}): ${JSON.stringify(where)}`
      expect(await engineIds(table, where), named).toEqual(expected)
      // Paged, with a limit no table reaches.
      expect(await engineIds(table, where, 10000), `paged ${named}`).toEqual(expected)
    }
    // Actual execution of the rewrite is checked against pg_stat_activity below, independently
    // of these row comparisons; counting predicate shapes would not prove SQL was rewritten.
    expect(nonEmpty, 'predicates that select at least one row').toBeGreaterThan(PREDICATES / 3)
  }, 300000)

  // Hand-written cases for each rule and each thing it leaves alone, on rows chosen so that the
  // three-valued answers differ: a NULL cell, a NULL in a projection, an empty subquery.
  const cases: { name: string; table: Table; where: Predicate }[] = [
    {
      name: 'same-column IN siblings, one of them projecting a NULL',
      table: 'items',
      where: {
        or: [
          { col: 'parent_id', in: { table: 'parents', project: 'id', where: { col: 'flag', op: 'eq', value: true } } },
          { col: 'parent_id', in: { table: 'others', project: 'parent_id' } },
        ],
      },
    },
    {
      name: 'the union under NOT: NOT (c IN s1 OR c IN s2) over a NULL projection',
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
      name: 'a key IN beside a conjunction containing an IN on another column',
      table: 'items',
      where: {
        or: [
          { col: 'id', in: { table: 'others', project: 'n', where: { col: 'n', op: 'lt', value: 40 } } },
          { and: [
            { col: 'n', op: 'gt', value: 5 },
            { col: 'other_id', in: { table: 'others', project: 'id', where: { col: 'parent_id', isNull: true } } },
          ] },
        ],
      },
    },
    {
      name: 'a key OR under NOT, where UNKNOWN and FALSE differ',
      table: 'items',
      where: {
        not: { or: [{ col: 'id', in: { table: 'parents', project: 'id' } }, { and: [
          { col: 'parent_id', in: { table: 'others', project: 'parent_id' } }, { col: 'n', op: 'gt', value: 3 },
        ] }] },
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
      name: 'every depth: a key OR inside a subquery, the union inside an AND',
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
                      { and: [
                        { col: 'id', in: { table: 'items', project: 'parent_id' } },
                        { col: 'tag', op: 'eq', value: 'b' },
                      ] },
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

  it('PostgreSQL receives the same-column union, a key OR as written, and the same rows as the old SQL', async () => {
    const probes: { where: Predicate; fragment: string; union: boolean }[] = [
      {
        where: { or: [
          { col: 'parent_id', in: { table: 'parents', project: 'id' } },
          { col: 'parent_id', in: { table: 'others', project: 'parent_id' } },
        ] },
        fragment: 'where ("parent_id" IN (SELECT "id" FROM "public"."parents" UNION ALL SELECT "parent_id" FROM "public"."others"))',
        union: true,
      },
      {
        where: { or: [
          { col: 'id', in: { table: 'parents', project: 'id' } },
          { and: [
            { col: 'parent_id', in: { table: 'others', project: 'parent_id' } },
            { col: 'n', op: 'gt', value: 3 },
          ] },
        ] },
        fragment: 'where ("id" IN (SELECT "id" FROM "public"."parents") OR ("parent_id" IN (SELECT "parent_id" FROM "public"."others") AND "n" > 3))',
        union: false,
      },
    ]
    for (const { where, fragment, union } of probes) {
      const expected = await oldSqlIds('items', where)
      expect(expected.length).toBeGreaterThan(0)
      const { sql, rows } = await subsetWithSql(h, 'items', where)
      expect(sql).toContain(fragment)
      expect(sql.includes('UNION ALL')).toBe(union)
      expect(rows.map((r) => Number(r.id)).sort((a, b) => a - b)).toEqual(expected)
    }
  })

  it('a paged key OR with a broad branch receives only the same-column union', async () => {
    const where: Predicate = { or: [
      { col: 'id', in: { table: 'parents', project: 'id' } },
      { col: 'id', in: { table: 'others', project: 'id' } },
      { col: 'label', isNull: true },
    ] }
    const { sql, rows } = await subsetWithSql(h, 'items', where, 1)
    expect(sql).toContain('where ("id" IN (SELECT "id" FROM "public"."parents" UNION ALL SELECT "id" FROM "public"."others") OR "label" IS NULL)')
    expect(sql).toContain('limit 1')
    expect(rows.map((r) => Number(r.id))).toEqual((await oldSqlIds('items', where)).slice(0, 1))
  })

  it('a key OR across columns reaches PostgreSQL as written, paged and unpaged, with the same ordered pages', async () => {
    const where: Predicate = { or: [
      { col: 'id', in: { table: 'parents', project: 'id' } },
      { col: 'parent_id', in: { table: 'others', project: 'parent_id' } },
    ] }
    const written = 'where ("id" IN (SELECT "id" FROM "public"."parents") OR "parent_id" IN (SELECT "parent_id" FROM "public"."others"))'
    const old = predicateToSql(where)
    const orderedSql = `SELECT * FROM "public"."items" WHERE ${old.text} ORDER BY "id" ASC`
    const expected = await pgQuery(h, orderedSql, old.params)
    const pageSize = 7
    expect(expected.length).toBeGreaterThan(2 * pageSize)
    const pages: Row[] = []
    // Include the empty final page. Compare each SQL page and the combined result without sorting
    // either response in JS, so an ordering or page-boundary regression cannot hide in set equality.
    for (let offset = 0; offset < expected.length + pageSize; offset += pageSize) {
      const expectedPage = await pgQuery(h, `${orderedSql} LIMIT ${pageSize} OFFSET ${offset}`, old.params)
      const { sql, rows } = await subsetWithSql(h, 'items', where, pageSize, offset)
      expect(sql).toContain(written)
      expect(sql).not.toContain('UNION ALL')
      expect(sql).toContain(`order by "id" asc, "id" asc limit ${pageSize}`)
      expect(rows, `page at offset ${offset}`).toEqual(expectedPage)
      pages.push(...rows)
    }
    expect(pages).toEqual(expected)

    const { sql, rows } = await subsetWithSql(h, 'items', where)
    expect(sql).toContain(written)
    expect(sql).not.toContain('UNION ALL')
    expect(sql).not.toContain(' limit ')
    expect(rows).toEqual(expected)
  })

  it('a paged query still receives the same-column union', async () => {
    // A key OR: the two key INs share a union, and the parent_id branch stays a branch of its own.
    const where: Predicate = { or: [
      { col: 'id', in: { table: 'parents', project: 'id', where: { col: 'flag', op: 'eq', value: true } } },
      { col: 'parent_id', in: { table: 'others', project: 'parent_id', where: { col: 'n', op: 'lt', value: 30 } } },
      { col: 'id', in: { table: 'others', project: 'n' } },
    ] }
    const expected = await oldSqlIds('items', where)
    expect(expected.length).toBeGreaterThan(5)
    const { sql, rows } = await subsetWithSql(h, 'items', where, 5)
    expect(sql).toContain('where ("id" IN (SELECT "id" FROM "public"."parents" WHERE "flag" = true UNION ALL SELECT "n" FROM "public"."others") OR "parent_id" IN (SELECT "parent_id" FROM "public"."others" WHERE "n" < 30))')
    expect(sql).toContain('limit 5')
    expect(rows.map((r) => Number(r.id))).toEqual(expected.slice(0, 5))
  })

  it('different projection collations keep the original SQL and rows for text keys and columns', async () => {
    const textSchema: Schema = { tables: {
      words: { columns: { id: { type: 'text' }, tag: { type: 'text' } }, primaryKey: 'id' },
      exact: { columns: { id: { type: 'text' } }, primaryKey: 'id' },
      folded: { columns: { id: { type: 'text' } }, primaryKey: 'id' },
    } }
    const textHarness = await bootHarness(textSchema, { ddl: `
      CREATE COLLATION case_insensitive (provider = icu, locale = 'und-u-ks-level2', deterministic = false);
      CREATE TABLE words (id text PRIMARY KEY, tag text);
      CREATE TABLE exact (id text COLLATE "C" PRIMARY KEY);
      CREATE TABLE folded (id text COLLATE case_insensitive PRIMARY KEY);
      ALTER TABLE words REPLICA IDENTITY FULL;
      ALTER TABLE exact REPLICA IDENTITY FULL;
      ALTER TABLE folded REPLICA IDENTITY FULL;
    ` })
    try {
      await pgQuery(textHarness, `INSERT INTO words VALUES ('X', 'X'), ('x', 'x'), ('Y', 'Y'), ('y', 'y')`)
      await pgQuery(textHarness, `INSERT INTO exact VALUES ('X')`)
      await pgQuery(textHarness, `INSERT INTO folded VALUES ('Y')`)
      await drainEngine(textHarness)
      const collations = await pgQuery(textHarness, `
        SELECT attcollation::text AS collation FROM pg_attribute
        WHERE attrelid IN ('exact'::regclass, 'folded'::regclass) AND attname = 'id'
      `)
      expect(new Set(collations.map((r) => r.collation)).size).toBe(2)
      for (const col of ['id', 'tag']) {
        const where: Predicate = { or: [
          { col, in: { table: 'exact', project: 'id' } },
          { col, in: { table: 'folded', project: 'id' } },
        ] }
        const old = predicateToSql(where)
        const expected = await pgQuery(textHarness, `SELECT id FROM words WHERE ${old.text}`, old.params)
        expect(expected.map((r) => String(r.id)).sort()).toEqual(['X', 'Y', 'y'])
        const { sql, rows } = await subsetWithSql(textHarness, 'words', where)
        expect(sql).not.toContain('UNION ALL')
        expect(sql).toContain(`where ("${col}" IN (SELECT "id" FROM "public"."exact") OR "${col}" IN (SELECT "id" FROM "public"."folded"))`)
        expect(rows.map((r) => String(r.id)).sort()).toEqual(expected.map((r) => String(r.id)).sort())
      }
    } finally {
      await textHarness.shutdown()
    }
  }, 120000)

  // Evidence, not an assertion on timings: PostgreSQL's plans for the statements the engine sends,
  // on a copy of the tables large enough to show their shape, printed into the test log. The copy
  // is UNLOGGED in its own schema, so logical decoding never sees it and the engine's tables keep
  // the rows every other case expects. Each statement is the one the engine sent for the small
  // tables (read from pg_stat_activity), pointed at the copy; "before" swaps in main's WHERE.
  it('prints PostgreSQL plans for the same-column union, unpaged and paged', async () => {
    /** The copy: 40,000 items, 10 per parent_id; `keys` parents and `others` rows whose parent_id
     * is `id * 37`, so the union selects few rows when both are small and many when they are not. */
    const copy = (keys: number, others: number) => pgQuery(h, `
      DROP SCHEMA IF EXISTS plans CASCADE;
      CREATE SCHEMA plans;
      CREATE UNLOGGED TABLE plans.parents (id int PRIMARY KEY, flag boolean, tag text);
      CREATE UNLOGGED TABLE plans.others (id int PRIMARY KEY, parent_id int, n int);
      CREATE UNLOGGED TABLE plans.items (id int PRIMARY KEY, parent_id int, other_id int, n int, label text);
      CREATE INDEX ON plans.items (parent_id);
      INSERT INTO plans.parents SELECT g, g % 2 = 0, 'a' FROM generate_series(1, ${keys}) g;
      INSERT INTO plans.others SELECT g, g * 37, g % 70 FROM generate_series(1, ${others}) g;
      INSERT INTO plans.items SELECT g, g % 4000 + 1, g % 100 + 1, g % 7, 'x' FROM generate_series(1, 40000) g;
      ANALYZE plans.parents, plans.others, plans.items;
    `)
    const plan = async (label: string, statement: string, params: unknown[] = []): Promise<number> => {
      const rows = await pgQuery(h, `EXPLAIN (ANALYZE, BUFFERS) ${statement}`, params)
      const bound = params.length > 0 ? `\nparams: ${JSON.stringify(params)}` : ''
      console.log(`\n--- ${label}\n${statement}${bound}\n${rows.map((r) => String(r['QUERY PLAN'])).join('\n')}`)
      return (await pgQuery(h, statement, params)).length
    }
    const onCopy = (sql: string) => sql.replaceAll('"public".', '"plans".')
    /** `sql` with its WHERE replaced by main's printing of `where`, and that printing's params. */
    const withMainWhere = (sql: string, where: Predicate): [string, unknown[]] => {
      const [head, rest] = sql.split(' t where ') as [string, string]
      const old = predicateToSql(where)
      return [`${head} t where ${old.text}${rest.slice(rest.indexOf(' order by '))}`, old.params]
    }
    const where: Predicate = { or: [
      { col: 'parent_id', in: { table: 'parents', project: 'id' } },
      { col: 'parent_id', in: { table: 'others', project: 'parent_id' } },
    ] }
    const statements: [string, string][] = []
    for (const [page, limit] of [['unpaged', undefined], ['paged (limit 10)', 10]] as const) {
      const { sql } = await subsetWithSql(h, 'items', where, limit)
      expect(sql).toContain('"parent_id" IN (SELECT "id" FROM "public"."parents" UNION ALL SELECT "parent_id" FROM "public"."others")')
      statements.push([page, sql])
    }
    try {
      const version = String((await pgQuery(h, 'SELECT version()'))[0]!.version)
      console.log(`\nPlans for conformance-sql-or-union on ${version}`)
      for (const [keys, others] of [[20, 10], [2000, 100]] as const) {
        await copy(keys, others)
        const data = `40,000 items, ${keys} parents, ${others} others`
        for (const [page, sql] of statements) {
          const [mainSql, mainParams] = withMainWhere(sql, where)
          // Twice each, so the second plan of a pair reads a warm cache.
          let before = 0
          let after = 0
          for (let run = 1; run <= 2; run++) {
            before = await plan(`${page}, before (main), ${data}, run ${run}`, onCopy(mainSql), mainParams)
            after = await plan(`${page}, after (union), ${data}, run ${run}`, onCopy(sql))
          }
          expect(after).toBe(before)
          expect(after).toBeGreaterThan(0)
        }
      }
    } finally {
      await pgQuery(h, 'DROP SCHEMA IF EXISTS plans CASCADE')
    }
  }, 120000)

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
