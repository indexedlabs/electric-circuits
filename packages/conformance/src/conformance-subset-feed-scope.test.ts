// A native subset load is two requests: a changes-only feed and a snapshot query. The rows the
// client holds came from the snapshot, so from then on the feed owes it every change to a row the
// snapshot's SQL put in the scope, judged the way that SQL judged it.
//
// That failed against the Indexed gateway on 2026-11-16, on app calendar scopes shaped like the
// one below (a week window on a `timestamptz`, a soft-delete column, an access check that is a
// subquery): 17 of 40 edits never reached the feed — exactly the events on the window's first UTC
// date. The app writes the bound as `2026-10-19T07:00:00Z`; the engine compared it as text against
// cells Postgres prints as `2026-10-19 16:00:00+00`.

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

// One week, Monday to Monday at midnight Pacific, with its bounds spelled as the app spells them.
const week: Predicate = {
  and: [
    { col: 'starts', op: 'gte', value: '2026-10-19T07:00:00Z' },
    { col: 'starts', op: 'lt', value: '2026-10-26T07:00:00Z' },
    { col: 'deleted_at', isNull: true },
    { col: 'record_id', in: { table: 'records', project: 'id', where: { col: 'visible', op: 'eq', value: true } } },
  ],
}

// The same scope in SQL: what Postgres says is in it, independently of the engine's predicate.
const weekSql = `
  SELECT id, name FROM events
   WHERE starts >= '2026-10-19T07:00:00Z' AND starts < '2026-10-26T07:00:00Z'
     AND deleted_at IS NULL
     AND record_id IN (SELECT id FROM records WHERE visible)
   ORDER BY id`

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
): Promise<void> {
  await change?.()
  // The barrier: replication has caught up and every flip the change caused has been propagated,
  // so whatever the feed is going to say about it is already on the stream.
  await drainEngine(harness)
  const expected = label(await pgQuery(harness, weekSql))
  try {
    await waitFor(() => JSON.stringify(label(held())) === JSON.stringify(expected), what, 8000)
  } catch {
    expect(label(held()), what).toEqual(expected)
  }
}

describe('a subset feed keeps a client on the scope its snapshot loaded', () => {
  it('reports a change to a row on the first UTC date of a timestamptz window', async () => {
    h = await boot()
    const sub = await h.client.subset({ table: 'events', where: week })
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
})
