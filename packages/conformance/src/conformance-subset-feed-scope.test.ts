// A native subset load is two requests: a changes-only feed and a snapshot query. The rows the
// client holds came from the snapshot, so from then on the feed owes it every change to a row the
// snapshot's SQL put in the scope, judged the way that SQL judged it.
//
// That failed against the Indexed gateway on 2026-11-16, on app calendar scopes shaped like the
// one below (a week window on a `timestamptz`, a soft-delete column, an access check that is a
// subquery): 17 of 40 edits never reached the feed — exactly the events on the window's first UTC
// date. The app writes the bound as `2026-10-19T07:00:00Z`; the engine compared it as text against
// cells Postgres prints as `2026-10-19 16:00:00+00`.
//
// Postgres reads more spellings of a bound than that one, and the snapshot query is handed the
// literal as written. So the feed reads each spelling it can as SQL does, and a create whose
// literal it cannot read is refused with the reason: it is never compared as text.

import type { Predicate, Row, Schema } from '@electric-circuits/protocol'
import { afterEach, describe, expect, it } from 'vitest'

import { pgQuery, waitFor } from './engine-native.js'
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

/** One window on `starts`, with its bounds spelled as given, as a predicate and as SQL. */
function scope(from: string, to: string): { where: Predicate; sql: string } {
  return {
    where: {
      and: [
        { col: 'starts', op: 'gte', value: from },
        { col: 'starts', op: 'lt', value: to },
        { col: 'deleted_at', isNull: true },
        { col: 'record_id', in: { table: 'records', project: 'id', where: { col: 'visible', op: 'eq', value: true } } },
      ],
    },
    // The same scope in SQL: what Postgres says is in it, independently of the engine's predicate.
    sql: `
      SELECT id, name FROM events
       WHERE starts >= '${from}' AND starts < '${to}'
         AND deleted_at IS NULL
         AND record_id IN (SELECT id FROM records WHERE visible)
       ORDER BY id`,
  }
}

// One week, Monday to Monday at midnight Pacific, with its bounds spelled as the app spells them.
const week = scope('2026-10-19T07:00:00Z', '2026-10-26T07:00:00Z')

const label = (rows: Row[]) =>
  rows
    .map((row) => `${Number(row.id)}:${String(row.name)}`)
    .sort((a, b) => Number.parseInt(a, 10) - Number.parseInt(b, 10))

let h: Harness | undefined
afterEach(async () => {
  await h?.shutdown()
  h = undefined
})

async function boot(): Promise<Harness> {
  // No time zone is set here: `initdb` gives a test cluster the HOST's zone, and the scope must be
  // judged the same under any of them (`conformance-timestamp-zones.test.ts` forces several).
  const harness = await bootHarness(schema, { ddl })
  await pgQuery(harness, 'INSERT INTO records (id, visible) VALUES (1, true)')
  await pgQuery(
    harness,
    `INSERT INTO events (id, record_id, starts, name) VALUES
       (10, 1, '2026-10-19 16:00:00+00', 'first day'),
       (11, 1, '2026-10-21 16:00:00+00', 'midweek'),
       (20, 1, '2026-11-05 16:00:00+00', 'another week')`,
  )
  await drainEngine(harness)
  return harness
}

/** After `change`, the client's rows must come to equal what Postgres says is in the scope. */
async function expectScope(
  harness: Harness,
  held: () => Row[],
  what: string,
  change?: () => Promise<unknown>,
  sql: string = week.sql,
): Promise<void> {
  await change?.()
  // The barrier: replication has caught up and every flip the change caused has been propagated,
  // so whatever the feed is going to say about it is already on the stream.
  await drainEngine(harness)
  const expected = label(await pgQuery(harness, sql))
  try {
    await waitFor(() => JSON.stringify(label(held())) === JSON.stringify(expected), what, 8000)
  } catch {
    expect(label(held()), what).toEqual(expected)
  }
}

describe('a subset feed keeps a client on the scope its snapshot loaded', () => {
  it('reports a change to a row on the first UTC date of a timestamptz window', async () => {
    h = await boot()
    const sub = await h.client.subset({ table: 'events', where: week.where })
    const held = () => sub.collection.toArray as unknown as Row[]
    try {
      await expectScope(h, held, 'the snapshot to load the week')
      expect(label(held())).toContain('10:first day')

      await expectScope(h, held, 'an edit on the window’s first UTC date to reach the client', () =>
        pgQuery(h!, `UPDATE events SET name = 'first day (edited)' WHERE id = 10`),
      )
      // A row entering on that date is an arrival the feed alone can report.
      await expectScope(h, held, 'a row created on the window’s first UTC date to reach the client', () =>
        pgQuery(h!, `INSERT INTO events (id, record_id, starts, name) VALUES (30, 1, '2026-10-19 08:00:00+00', 'early')`),
      )
      // And one just outside the lower bound must stay out: 06:59Z is before 07:00Z.
      await expectScope(h, held, 'a row before the lower bound to stay out', () =>
        pgQuery(h!, `INSERT INTO events (id, record_id, starts, name) VALUES (31, 1, '2026-10-19 06:59:00+00', 'too early')`),
      )
      expect(label(held())).not.toContain('31:too early')
    } finally {
      await sub.close()
    }
  }, 120000)

  // The same week, its bounds in other spellings Postgres reads as the same two instants: a name
  // for UTC, an offset with a one-digit hour, an offset with no colon, and a zone straight after
  // the date (midnight in that zone).
  it.each([
    ['2026-10-19 07:00:00 UTC', '2026-10-26 07:00:00 UTC'],
    ['2026-10-19 12:00:00+5', '2026-10-26 12:00:00+5'],
    ['2026-10-19T00:00:00-0700', '2026-10-26T00:00:00-0700'],
    ['2026-10-19 -07', '2026-10-26 -07'],
  ])('judges a window whose bounds are spelled %s and %s as Postgres does', async (from, to) => {
    h = await boot()
    const spelled = scope(from, to)
    const sub = await h.client.subset({ table: 'events', where: spelled.where })
    const held = () => sub.collection.toArray as unknown as Row[]
    try {
      await expectScope(h, held, 'the snapshot to load the week', undefined, spelled.sql)
      expect(label(held())).toEqual(['10:first day', '11:midweek'])

      await expectScope(
        h,
        held,
        'an edit on the window’s first UTC date to reach the client',
        () => pgQuery(h!, `UPDATE events SET name = 'first day (edited)' WHERE id = 10`),
        spelled.sql,
      )
      await expectScope(
        h,
        held,
        'rows created on each side of the lower bound to be placed as Postgres places them',
        () =>
          pgQuery(
            h!,
            `INSERT INTO events (id, record_id, starts, name) VALUES
               (30, 1, '2026-10-19 07:00:00+00', 'on the bound'),
               (31, 1, '2026-10-19 06:59:59+00', 'too early')`,
          ),
        spelled.sql,
      )
      expect(label(held())).toEqual(['10:first day (edited)', '11:midweek', '30:on the bound'])
    } finally {
      await sub.close()
    }
  }, 120000)

  // Postgres reads these too, from its zone tables and its settings; the engine has neither. Kept
  // as text they would be compared in code-point order while the snapshot's SQL read an instant,
  // so both halves of the subset say no, and say why.
  it('refuses a bound it cannot read, on the feed and on the snapshot query, and says why', async () => {
    h = await boot()
    const post = (path: string, body: unknown) =>
      fetch(`${h!.engineUrl}${path}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body),
      })
    for (const literal of [
      '2026-10-19 07:00:00 PST',
      '2026-10-19 07:00:00 America/Los_Angeles',
      'Oct 19 2026',
      'tomorrow',
    ]) {
      const where: Predicate = { col: 'starts', op: 'gte', value: literal }
      for (const path of ['/v1/subset-feeds', '/v1/subsets/query', '/v1/shapes']) {
        const res = await post(path, { table: 'events', where })
        const body = await res.text()
        expect(res.status, `${path} with ${literal}: ${body}`).toBe(400)
        expect(body, `${path} names the literal`).toContain(literal)
        expect(body, `${path} names the column's type`).toContain('timestamptz')
        expect(body, `${path} says what it reads`).toContain('ISO 8601')
      }
    }
  }, 120000)
})
