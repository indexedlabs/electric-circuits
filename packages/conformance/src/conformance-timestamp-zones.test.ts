// A timestamp must mean the same instant to the engine whatever time zone the database, the role
// or the server defaults to.
//
// The engine keeps a `timestamptz` cell as the text Postgres prints and compares that text. Text
// order is instant order only when every cell is printed in ONE zone, and Postgres prints in the
// session's: a database whose default is `America/Los_Angeles` prints 08:00 UTC as
// `2026-10-19 01:00:00-07`, which sorts below a `07:00:00+00` bound it is actually after. Nothing
// set the engine's sessions to UTC, so on such a database the live path judged a window
// differently from the snapshot's SQL, and the rows a client received were spelled in local time.
// A test cluster made by `initdb` takes the HOST's zone, which is how this was first seen.
//
// Each zone below is forced explicitly (`ALTER DATABASE … SET timezone`), so the result does not
// depend on where the suite runs.

import type { Predicate, Row, Schema } from '@electric-circuits/protocol'
import pgpkg from 'pg'
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest'

import { createShape, pgQuery, readEnvelopes, sleep, waitFor } from './engine-native.js'
import { bootHarness, drainEngine, type Harness } from './harness.js'

const schema: Schema = {
  tables: {
    records: { columns: { id: { type: 'int' }, visible: { type: 'bool' } }, primaryKey: 'id' },
    events: {
      columns: {
        id: { type: 'int' },
        record_id: { type: 'int' },
        starts: { type: 'text' },
        name: { type: 'text' },
        marks: { type: 'text' },
        span: { type: 'text' },
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
    name text NOT NULL,
    marks timestamptz[],
    span tstzrange
  );
  ALTER TABLE events REPLICA IDENTITY FULL;
`

const visibleRecords = {
  col: 'record_id',
  in: { table: 'records', project: 'id', where: { col: 'visible', op: 'eq', value: true } },
} as const

/** A scope as the app writes one: a window on a `timestamptz` with ISO 8601 bounds, and access. */
function scope(from: string, to: string): { where: Predicate; sql: string } {
  return {
    where: { and: [{ col: 'starts', op: 'gte', value: from }, { col: 'starts', op: 'lt', value: to }, visibleRecords] },
    // The same scope in SQL: what Postgres says is in it, independently of the engine's predicate.
    sql: `SELECT id, name FROM events
           WHERE starts >= '${from}' AND starts < '${to}'
             AND record_id IN (SELECT id FROM records WHERE visible)
           ORDER BY id`,
  }
}

// One week, Monday to Monday at midnight Pacific.
const week = scope('2026-10-19T07:00:00Z', '2026-10-26T07:00:00Z')
// The hour America/Los_Angeles repeats when daylight saving ends: 01:00–02:00 PST on 2026-11-01.
// 08:45 UTC is 01:45 PDT and 09:15 UTC is 01:15 PST, so local wall-clock order is the reverse of
// the instants'.
const repeatHour = scope('2026-11-01T09:00:00Z', '2026-11-01T10:00:00Z')

const label = (rows: Row[]) =>
  rows
    .map((row) => `${Number(row.id)}:${String(row.name)}`)
    .sort((a, b) => Number.parseInt(a, 10) - Number.parseInt(b, 10))

function setDatabaseDefault(setting: string, value: string) {
  return async ({ pgUrl }: { pgUrl: string }) => {
    const client = new pgpkg.Client({ connectionString: pgUrl })
    await client.connect()
    try {
      await client.query(
        `DO $$ BEGIN EXECUTE format('ALTER DATABASE %I SET ${setting} TO ''${value}''', current_database()); END $$`,
      )
    } finally {
      await client.end()
    }
  }
}

/** After `change`, the client's rows must come to equal what Postgres says is in the scope. */
async function expectScope(
  h: Harness,
  sql: string,
  held: () => Row[],
  what: string,
  change?: () => Promise<unknown>,
): Promise<void> {
  await change?.()
  // The barrier: replication has caught up and every flip the change caused has been propagated,
  // so whatever the feed is going to say about it is already on the stream.
  await drainEngine(h)
  const expected = label(await pgQuery(h, sql))
  try {
    await waitFor(() => JSON.stringify(label(held())) === JSON.stringify(expected), what, 8000)
  } catch {
    expect(label(held()), what).toEqual(expected)
  }
}

describe.each([
  ['America/Los_Angeles', 'behind UTC, with a daylight-saving boundary'],
  ['Asia/Kolkata', 'ahead of UTC by a half-hour offset'],
])('timestamps on a database whose default time zone is %s (%s)', (zone) => {
  let h: Harness
  beforeAll(async () => {
    h = await bootHarness(schema, { ddl, beforeEngine: setDatabaseDefault('timezone', zone) })
    expect((await pgQuery(h, 'SHOW timezone'))[0]!.TimeZone, 'the database default is in force').toBe(zone)
    await pgQuery(h, 'INSERT INTO records (id, visible) VALUES (1, true)')
    // Every instant is written with its offset, so the session zone of this INSERT is irrelevant.
    await pgQuery(
      h,
      `INSERT INTO events (id, record_id, starts, name, marks, span) VALUES
         (10, 1, '2026-10-19 08:00:00+00', 'in the first hours', NULL, NULL),
         (11, 1, '2026-10-19 06:00:00+00', 'before the week', NULL, NULL),
         (12, 1, '2026-10-26 06:30:00+00', 'in the last minutes', NULL, NULL),
         (13, 1, '2026-10-26 08:00:00+00', 'after the week', NULL, NULL),
         (14, 1, '2026-10-21 12:34:56.789+00', 'midweek',
              ARRAY['2026-10-21 12:00:00+00', '2026-10-22 00:30:00+00']::timestamptz[],
              tstzrange('2026-10-21 12:00:00+00', '2026-10-21 13:30:00+00')),
         (20, 1, '2026-11-01 08:45:00+00', 'before the repeated hour', NULL, NULL),
         (21, 1, '2026-11-01 09:15:00+00', 'in the repeated hour', NULL, NULL)`,
    )
    await drainEngine(h)
  }, 90000)
  afterAll(async () => {
    await h?.shutdown()
  })

  it('judges a timestamptz window on the live path as Postgres does', async () => {
    const sub = await h.client.subset({ table: 'events', where: week.where })
    const held = () => sub.collection.toArray as unknown as Row[]
    const startsOf = (id: number) => String(held().find((row) => Number(row.id) === id)?.starts)
    try {
      await expectScope(h, week.sql, held, 'the snapshot to load the week')
      expect(label(held())).toEqual(['10:in the first hours', '12:in the last minutes', '14:midweek'])
      expect(startsOf(14), 'a snapshot row is spelled in UTC').toBe('2026-10-21 12:34:56.789+00')

      // An edit to each row, inside the week and out of it: the feed must report the three inside
      // and say nothing about the two outside.
      await expectScope(h, week.sql, held, 'edits inside the week to reach the client, and only those', () =>
        pgQuery(h, `UPDATE events SET name = name || ' (edited)' WHERE id BETWEEN 10 AND 14`),
      )
      expect(label(held())).toEqual([
        '10:in the first hours (edited)',
        '12:in the last minutes (edited)',
        '14:midweek (edited)',
      ])
      expect(startsOf(14), 'the same row from the live path is spelled the same').toBe('2026-10-21 12:34:56.789+00')

      // Arrivals on each side of each bound.
      await expectScope(h, week.sql, held, 'rows created on each side of each bound to be placed as Postgres places them', () =>
        pgQuery(
          h,
          `INSERT INTO events (id, record_id, starts, name) VALUES
             (15, 1, '2026-10-19 07:30:00+00', 'just inside the start'),
             (16, 1, '2026-10-19 06:59:59+00', 'just before the start'),
             (17, 1, '2026-10-26 06:59:59+00', 'just inside the end'),
             (18, 1, '2026-10-26 07:00:00+00', 'at the end')`,
        ),
      )
      expect(label(held()).map((row) => Number.parseInt(row, 10))).toEqual([10, 12, 14, 15, 17])

      // A row re-dated out of the week leaves; one re-dated onto the lower bound enters.
      await expectScope(h, week.sql, held, 're-dated rows to move as Postgres moves them', () =>
        pgQuery(
          h,
          `DO $$ BEGIN
             UPDATE events SET starts = '2026-10-26 07:30:00+00' WHERE id = 10;
             UPDATE events SET starts = '2026-10-19 07:00:00+00' WHERE id = 11;
           END $$`,
        ),
      )
      expect(label(held()).map((row) => Number.parseInt(row, 10))).toEqual([11, 12, 14, 15, 17])
    } finally {
      await sub.close()
    }
  }, 180000)

  it('judges a window on the hour a daylight-saving change repeats as Postgres does', async () => {
    const sub = await h.client.subset({ table: 'events', where: repeatHour.where })
    const held = () => sub.collection.toArray as unknown as Row[]
    try {
      await expectScope(h, repeatHour.sql, held, 'the snapshot to load the hour')
      expect(label(held())).toEqual(['21:in the repeated hour'])

      await expectScope(h, repeatHour.sql, held, 'an edit inside the hour to reach the client, and only that', () =>
        pgQuery(h, `UPDATE events SET name = name || ' (edited)' WHERE id IN (20, 21)`),
      )
      expect(label(held())).toEqual(['21:in the repeated hour (edited)'])

      await expectScope(h, repeatHour.sql, held, 'rows created on each side of each bound to be placed as Postgres places them', () =>
        pgQuery(
          h,
          `INSERT INTO events (id, record_id, starts, name) VALUES
             (22, 1, '2026-11-01 08:59:59+00', 'the second before'),
             (23, 1, '2026-11-01 09:00:00+00', 'the first instant'),
             (24, 1, '2026-11-01 09:59:59+00', 'the last second'),
             (25, 1, '2026-11-01 10:00:00+00', 'the instant after')`,
        ),
      )
      expect(label(held()).map((row) => Number.parseInt(row, 10))).toEqual([21, 23, 24])
    } finally {
      await sub.close()
    }
  }, 180000)

  // A client compares what it holds with what arrives. If a backfill row and the same row's next
  // live change spell one instant two ways, every such row looks changed when nothing was.
  it('spells one instant the same in a backfill row and in a live change, in UTC', async () => {
    const shape = await createShape(h, { table: 'events', where: { col: 'id', op: 'eq', value: 14 } })
    await drainEngine(h)
    await pgQuery(h, `UPDATE events SET name = 'midweek (respelled?)' WHERE id = 14`)
    await drainEngine(h)

    const envs = (await readEnvelopes(shape.streamUrl)).filter((env) => env.key === '14')
    expect(envs.map((env) => env.headers.lsn !== undefined), 'one backfill row, then one live change').toEqual([false, true])
    const [backfill, live] = envs.map((env) => env.value as Row)
    const utc = {
      starts: '2026-10-21 12:34:56.789+00',
      marks: '{"2026-10-21 12:00:00+00","2026-10-22 00:30:00+00"}',
      span: '["2026-10-21 12:00:00+00","2026-10-21 13:30:00+00")',
    }
    for (const [column, spelling] of Object.entries(utc)) {
      expect(backfill![column], `backfill ${column}`).toBe(spelling)
      expect(live![column], `live ${column}`).toBe(spelling)
    }
  }, 120000)
})

// The boot's answer is about the defaults the boot saw. A default set later reaches the NEXT
// replication connection, which starts with whatever the database says then. So the engine asks
// again before every such connection, and one that would print another style is not opened:
// nothing is decoded from it, and ingest waits until the style is ISO again.
describe('a date style changed under a running engine', () => {
  let h: Harness | undefined
  afterEach(async () => {
    await h?.shutdown()
    h = undefined
  })

  it('is never decoded: the next replication connection waits until the style is ISO again', async () => {
    h = await bootHarness(schema, { ddl })
    await pgQuery(h, 'INSERT INTO records (id, visible) VALUES (1, true)')
    await pgQuery(
      h,
      `INSERT INTO events (id, record_id, starts, name) VALUES (14, 1, '2026-10-21 12:34:56.789+00', 'midweek')`,
    )
    await drainEngine(h)
    const shape = await createShape(h, { table: 'events', where: { col: 'id', op: 'eq', value: 14 } })
    await drainEngine(h)
    const live = async () => (await readEnvelopes(shape.streamUrl)).filter((env) => env.headers.lsn !== undefined)
    expect(await live(), 'nothing has changed since the backfill').toHaveLength(0)

    // The default changes — for new sessions only — and the replication connection drops.
    await setDatabaseDefault('datestyle', 'German')({ pgUrl: h.pgUrl })
    const dropped = await pgQuery(
      h,
      'SELECT pg_terminate_backend(active_pid) AS done FROM pg_replication_slots WHERE slot_name = $1 AND active',
      [h.slot],
    )
    expect(dropped, 'the walsender was connected').toHaveLength(1)
    await pgQuery(h, `UPDATE events SET name = 'edited under German' WHERE id = 14`)

    // A walsender session opened now prints `21.10.2026 …`. No cell in that style may reach the
    // stream — the engine reconnects within about a second, so several seconds is long enough to
    // see one arrive.
    const utc = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?\+00$/
    const until = Date.now() + 6000
    while (Date.now() < until) {
      for (const env of await live()) {
        expect(String((env.value as Row).starts), 'a live timestamptz cell is spelled in ISO style, in UTC').toMatch(utc)
      }
      await sleep(250)
    }
    expect(await live(), 'nothing is ingested while the replication session would print German dates').toHaveLength(0)
    expect(h.engineStderr(), 'the refusal names the setting').toContain('DateStyle=German')

    // Put back, the next attempt connects, and the change arrives spelled as every other one is.
    await setDatabaseDefault('datestyle', 'ISO, MDY')({ pgUrl: h.pgUrl })
    await waitFor(async () => (await live()).length === 1, 'the edit to arrive once the date style is ISO again', 60000)
    const edited = (await live())[0]!.value as Row
    expect(edited.name).toBe('edited under German')
    expect(edited.starts).toBe('2026-10-21 12:34:56.789+00')
  }, 150000)
})

// The live path's session cannot be given a date style (the replication connection takes no
// startup options), and the engine can only read the ISO one. A database that prints dates another
// way is refused, by name, before anything is served from it.
describe('a database whose sessions print dates in a non-ISO style', () => {
  let booted: Harness | undefined
  afterEach(async () => {
    await booted?.shutdown()
    booted = undefined
  })

  it('is refused at boot as a configuration error', async () => {
    const boot = bootHarness(schema, { ddl, beforeEngine: setDatabaseDefault('datestyle', 'German') }).then((harness) => {
      booted = harness
      return harness
    })
    await expect(boot).rejects.toThrow(/engine exited \(code 78\)/)
  }, 60000)
})
