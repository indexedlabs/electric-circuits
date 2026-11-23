// A native subset load is two requests: a changes-only feed and a snapshot query. The rows the
// client holds came from the snapshot, so the feed owes it two things from then on: every change
// to a row the snapshot's SQL put in the scope, judged the way that SQL judged it, and the removal
// of every row that later leaves the scope — including the rows the feed itself never delivered.
//
// Both failed against the Indexed gateway on 2026-11-16, on app calendar scopes shaped like the
// one below (a week window on a `timestamptz`, a soft-delete column, an access check that is a
// subquery):
//   * 17 of 40 edits never reached the feed — exactly the events on the window's first UTC date.
//     The app writes the bound as `2026-10-19T07:00:00Z`; the engine compared it as text against
//     cells Postgres prints as `2026-10-19 16:00:00+00`.
//   * 10 soft-deleted events and 12 re-dated Records stayed on the client: the feeds carried
//     upserts only, because a delete was sent only for a row the feed had delivered.

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
  await pgQuery(harness, 'INSERT INTO records (id, visible) VALUES (1, true), (2, true), (3, true), (4, true)')
  await pgQuery(
    harness,
    `INSERT INTO events (id, record_id, starts, name) VALUES
       (10, 1, '2026-10-19 16:00:00+00', 'first day'),
       (11, 1, '2026-10-21 16:00:00+00', 'midweek'),
       (12, 2, '2026-10-22 16:00:00+00', 'on record two'),
       (13, 3, '2026-10-23 16:00:00+00', 'on record three'),
       (14, 1, '2026-10-24 16:00:00+00', 'to soft-delete'),
       (15, 1, '2026-10-25 16:00:00+00', 'to move'),
       (16, 1, '2026-10-20 16:00:00+00', 'to delete'),
       (18, 4, '2026-10-22 18:00:00+00', 'on record four'),
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

  it('removes a row that leaves the scope when the client holds it only from the snapshot', async () => {
    h = await boot()
    const sub = await h.client.subset({ table: 'events', where: week })
    const held = () => sub.collection.toArray as unknown as Row[]
    try {
      await expectScope(h, held, 'the snapshot to load the week')

      // The feed has delivered none of these rows: it opened empty, and nothing has changed since.
      await expectScope(h, held, 'a soft-deleted snapshot row to leave', () =>
        pgQuery(h!, `UPDATE events SET deleted_at = now() WHERE id = 14`),
      )
      await expectScope(h, held, 'a snapshot row moved to another week to leave', () =>
        pgQuery(h!, `UPDATE events SET starts = starts + interval '14 days' WHERE id = 15`),
      )
      await expectScope(h, held, 'a deleted snapshot row to leave', () => pgQuery(h!, `DELETE FROM events WHERE id = 16`))
      // The row is untouched: its Record stops being visible, which the engine learns as an
      // inner-set flip and resolves with a query-back.
      await expectScope(h, held, 'a snapshot row whose Record is hidden to leave', () =>
        pgQuery(h!, `UPDATE records SET visible = false WHERE id = 2`),
      )
      // One transaction hides the Record and then edits its event.
      await expectScope(h, held, 'a snapshot row hidden and edited in one transaction to leave', () =>
        pgQuery(
          h!,
          `DO $$ BEGIN
             UPDATE records SET visible = false WHERE id = 3;
             UPDATE events SET name = 'edited while hidden' WHERE id = 13;
           END $$`,
        ),
      )

      expect(label(held()), 'what is left is the three untouched rows').toEqual([
        '10:first day',
        '11:midweek',
        '18:on record four',
      ])
    } finally {
      await sub.close()
    }
  }, 180000)

  // The control: a row the feed itself delivered was always removed. It is here so a failure
  // above cannot be mistaken for removals not working at all.
  it('removes a row that leaves the scope after the feed delivered it', async () => {
    h = await boot()
    const sub = await h.client.subset({ table: 'events', where: week })
    const held = () => sub.collection.toArray as unknown as Row[]
    try {
      await expectScope(h, held, 'the snapshot to load the week')

      await expectScope(h, held, 'a new row to arrive', () =>
        pgQuery(h!, `INSERT INTO events (id, record_id, starts, name) VALUES (17, 1, '2026-10-22 09:00:00+00', 'arrived live')`),
      )
      expect(label(held())).toContain('17:arrived live')
      await expectScope(h, held, 'a delivered row that is soft-deleted to leave', () =>
        pgQuery(h!, `UPDATE events SET deleted_at = now() WHERE id = 17`),
      )

      await expectScope(h, held, 'an edit to reach the client', () =>
        pgQuery(h!, `UPDATE events SET name = 'on record four (edited)' WHERE id = 18`),
      )
      expect(label(held())).toContain('18:on record four (edited)')
      await expectScope(h, held, 'a delivered row whose Record is hidden to leave', () =>
        pgQuery(h!, `UPDATE records SET visible = false WHERE id = 4`),
      )
      expect(label(held()).some((row) => row.startsWith('18:'))).toBe(false)
    } finally {
      await sub.close()
    }
  }, 180000)
})
