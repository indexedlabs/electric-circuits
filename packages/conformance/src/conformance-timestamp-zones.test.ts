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
        stamps: { type: 'text' },
        later: { type: 'text' },
        held: { type: 'text' },
        slot: { type: 'text' },
      },
      primaryKey: 'id',
    },
  },
}

const ddl = `
  CREATE DOMAIN instant AS timestamptz;
  CREATE DOMAIN later_instant AS instant CHECK (VALUE > '2000-01-01 00:00:00+00');
  CREATE TYPE hold AS RANGE (subtype = timestamptz);
  CREATE TYPE booking AS (starts timestamptz, label text);
  CREATE TABLE records (id int PRIMARY KEY, visible boolean NOT NULL);
  ALTER TABLE records REPLICA IDENTITY FULL;
  CREATE TABLE events (
    id int PRIMARY KEY,
    record_id int NOT NULL,
    starts timestamptz NOT NULL,
    name text NOT NULL,
    marks timestamptz[],
    span tstzrange,
    stamps instant[],
    later later_instant[],
    held hold,
    slot booking
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

  // An array whose ELEMENT type is a domain is named after the domain (`_instant`), not
  // `_timestamptz`, and a domain may itself be over a domain. Its elements are timestamps printed
  // in the session zone all the same.
  it('spells an array of a domain over timestamptz the same in a backfill row and in a live change', async () => {
    await pgQuery(
      h,
      `INSERT INTO events (id, record_id, starts, name, stamps, later) VALUES
         (60, 1, '2026-10-21 12:00:00+00', 'domains',
              ARRAY['2026-10-21 12:00:00+00', '2026-10-22 00:30:00+00']::instant[],
              ARRAY['2026-10-21 12:00:00+00']::later_instant[])`,
    )
    await drainEngine(h)
    const shape = await createShape(h, { table: 'events', where: { col: 'id', op: 'eq', value: 60 } })
    await drainEngine(h)
    await pgQuery(h, `UPDATE events SET name = 'domains (respelled?)' WHERE id = 60`)
    await drainEngine(h)

    const envs = (await readEnvelopes(shape.streamUrl)).filter((env) => env.key === '60')
    expect(envs.map((env) => env.headers.lsn !== undefined), 'one backfill row, then one live change').toEqual([false, true])
    const [backfill, live] = envs.map((env) => env.value as Row)
    const utc = { stamps: '{"2026-10-21 12:00:00+00","2026-10-22 00:30:00+00"}', later: '{"2026-10-21 12:00:00+00"}' }
    for (const [column, spelling] of Object.entries(utc)) {
      expect(backfill![column], `backfill ${column}`).toBe(spelling)
      expect(live![column], `live ${column}`).toBe(spelling)
    }
  }, 120000)

  // A range type of the application's own (`CREATE TYPE … AS RANGE (subtype = timestamptz)`) is
  // not named `tstzrange`, and prints its bounds in the session zone exactly as one does.
  it('spells a range type of the application\'s own over timestamptz the same in a backfill row and in a live change', async () => {
    await pgQuery(
      h,
      `INSERT INTO events (id, record_id, starts, name, held) VALUES
         (61, 1, '2026-10-21 12:00:00+00', 'a hold', hold('2026-10-21 12:00:00+00', '2026-10-21 13:30:00+00'))`,
    )
    await drainEngine(h)
    const shape = await createShape(h, { table: 'events', where: { col: 'id', op: 'eq', value: 61 } })
    await drainEngine(h)
    await pgQuery(h, `UPDATE events SET name = 'a hold (respelled?)' WHERE id = 61`)
    await drainEngine(h)

    const envs = (await readEnvelopes(shape.streamUrl)).filter((env) => env.key === '61')
    expect(envs.map((env) => env.headers.lsn !== undefined), 'one backfill row, then one live change').toEqual([false, true])
    const [backfill, live] = envs.map((env) => env.value as Row)
    expect(backfill!.held, 'backfill').toBe('["2026-10-21 12:00:00+00","2026-10-21 13:30:00+00")')
    expect(live!.held, 'live').toBe('["2026-10-21 12:00:00+00","2026-10-21 13:30:00+00")')
  }, 120000)

  // A literal for an array or a range of timestamps is refused rather than compared as text: the
  // rows are respelled element by element, and the snapshot's SQL reads the literal as instants.
  it('refuses a predicate that compares an array or a range of timestamps to a literal', async () => {
    for (const [col, type, literal] of [
      ['marks', '_timestamptz', '{"2026-10-21 05:00:00-07","2026-10-21 17:30:00-07"}'],
      ['span', 'tstzrange', '["2026-10-21 12:00:00+00","2026-10-21 13:30:00+00")'],
      ['stamps', '_instant', '{"2026-10-21 12:00:00+00"}'],
      ['held', 'hold', '["2026-10-21 12:00:00+00","2026-10-21 13:30:00+00")'],
    ] as const) {
      for (const path of ['/v1/subset-feeds', '/v1/subsets/query', '/v1/shapes']) {
        const res = await fetch(`${h.engineUrl}${path}`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ table: 'events', where: { col, op: 'eq', value: literal } }),
        })
        const body = await res.text()
        expect(res.status, `${path} on ${col}: ${body}`).toBe(400)
        expect(body, `${path} names the column`).toContain(`'${col}'`)
        expect(body, `${path} names the column's type`).toContain(type)
        expect(body, `${path} says what is not read`).toContain('array, range')
      }
    }
  }, 120000)

  // The same at the edges of Postgres's range. A year before year one is printed with ` BC` after
  // the zone, a year past 9999 with more digits, and in a non-UTC zone the walsender prints both
  // with the zone's old local-mean-time offset or across the boundary.
  it('spells an instant before year one, and one past 9999, the same in a backfill row and in a live change', async () => {
    const utc: Record<number, { starts: string; marks: string }> = {
      50: {
        starts: '0044-03-15 12:00:00+00 BC',
        marks: '{"0044-03-15 12:00:00+00 BC","2026-10-19 08:00:00+00"}',
      },
      51: { starts: '0001-12-31 10:00:00+00 BC', marks: '{"0001-01-01 00:00:00+00"}' },
      52: { starts: '10000-01-01 08:00:00+00', marks: '{"9999-12-31 23:00:00+00","10000-01-01 08:00:00+00"}' },
    }
    for (const [id, row] of Object.entries(utc)) {
      await pgQuery(
        h,
        `INSERT INTO events (id, record_id, starts, name, marks) VALUES ($1, 1, $2::timestamptz, 'edge', $3::timestamptz[])`,
        [Number(id), row.starts, row.marks],
      )
    }
    await drainEngine(h)
    const shape = await createShape(h, { table: 'events', where: { col: 'id', op: 'gte', value: 50 } })
    await drainEngine(h)
    await pgQuery(h, `UPDATE events SET name = 'edge (respelled?)' WHERE id >= 50`)
    await drainEngine(h)

    const envs = await readEnvelopes(shape.streamUrl)
    for (const [id, row] of Object.entries(utc)) {
      const mine = envs.filter((env) => env.key === id)
      expect(mine.map((env) => env.headers.lsn !== undefined), `row ${id}: one backfill row, then one live change`).toEqual([
        false,
        true,
      ])
      const [backfill, live] = mine.map((env) => env.value as Row)
      for (const column of ['starts', 'marks'] as const) {
        expect(backfill![column], `row ${id} backfill ${column}`).toBe(row[column])
        expect(live![column], `row ${id} live ${column}`).toBe(row[column])
      }
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

  // A SERVER-wide style (postgresql.conf, `ALTER SYSTEM`) does not wait for a reconnect: a reload
  // changes it inside the walsender that is already streaming. The check made when that connection
  // was opened says nothing about it, so the decoder itself must refuse a cell that is not ISO.
  it('is never decoded when a reload brings it to the replication connection that is already open', async () => {
    const serverWide = async (sql: string) => {
      await pgQuery(h!, sql)
      await pgQuery(h!, 'SELECT pg_reload_conf()')
    }
    h = await bootHarness(schema, { ddl })
    try {
      await pgQuery(h, 'INSERT INTO records (id, visible) VALUES (1, true)')
      await pgQuery(
        h,
        `INSERT INTO events (id, record_id, starts, name) VALUES (14, 1, '2026-10-21 12:34:56.789+00', 'midweek')`,
      )
      await drainEngine(h)
      const shape = await createShape(h, { table: 'events', where: { col: 'id', op: 'eq', value: 14 } })
      await drainEngine(h)
      const live = async () => (await readEnvelopes(shape.streamUrl)).filter((env) => env.headers.lsn !== undefined)
      const walsender = async () =>
        (await pgQuery(h!, 'SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1', [h!.slot]))[0]?.active_pid
      const connected = await walsender()
      expect(connected, 'the walsender is connected').toBeTruthy()

      // No reconnect: the same walsender now prints `21.10.2026 …`.
      await serverWide(`ALTER SYSTEM SET datestyle TO 'German, DMY'`)
      await pgQuery(h, `UPDATE events SET name = 'edited under German' WHERE id = 14`)

      const utc = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?\+00$/
      const until = Date.now() + 6000
      while (Date.now() < until) {
        for (const env of await live()) {
          expect(String((env.value as Row).starts), 'a live timestamptz cell is spelled in ISO style, in UTC').toMatch(utc)
        }
        await sleep(250)
      }
      expect(await live(), 'nothing is ingested from a session that prints German dates').toHaveLength(0)
      expect(h.engineStderr(), 'the decode names the cell').toContain('not in the ISO date style')
      expect(h.engineStderr(), 'and the check before the next connection names the setting').toContain('DateStyle=German')

      await serverWide('ALTER SYSTEM RESET datestyle')
      await waitFor(async () => (await live()).length === 1, 'the edit to arrive once the date style is ISO again', 60000)
      const edited = (await live())[0]!.value as Row
      expect(edited.name).toBe('edited under German')
      expect(edited.starts).toBe('2026-10-21 12:34:56.789+00')
    } finally {
      // The setting is the whole test cluster's: never leave it behind for the files that follow.
      await serverWide('ALTER SYSTEM RESET datestyle')
    }
  }, 150000)
})

// The live path's session cannot be given a date style (the replication connection takes no
// startup options), and the engine can only read the ISO one. A database that prints dates another
// way is refused, by name, before anything is served from it.
// A composite type with a `timestamptz` attribute prints it in the session zone, inside text the
// engine cannot respell exactly: a `text` attribute beside it may hold anything, a timestamp's
// spelling included. Query sessions print it in UTC. So on a replication session whose zone is not
// UTC the two roads would spell one row two ways, and the engine ingests nothing from such a
// column until the zone is UTC, saying which column and how to fix it.
describe('a composite column that holds a timestamptz, on a database whose default time zone is not UTC', () => {
  let h: Harness | undefined
  afterEach(async () => {
    await h?.shutdown()
    h = undefined
  })

  it('is never ingested in the local spelling: the engine waits, names the column, and resumes once the zone is UTC', async () => {
    // The row is written before the engine's slot exists, so the engine first meets it in a
    // backfill: every change to the table is decoded, whether or not a shape wants it.
    const seeded = `${ddl}
      INSERT INTO records (id, visible) VALUES (1, true);
      INSERT INTO events (id, record_id, starts, name, slot) VALUES
        (70, 1, '2026-10-21 12:00:00+00', 'booked', ROW('2026-10-21 12:00:00+00', 'room 4')::booking);`
    h = await bootHarness(schema, { ddl: seeded, beforeEngine: setDatabaseDefault('timezone', 'America/Los_Angeles') })
    const shape = await createShape(h, { table: 'events', where: { col: 'id', op: 'eq', value: 70 } })
    await drainEngine(h)
    const all = async () => (await readEnvelopes(shape.streamUrl)).filter((env) => env.key === '70')
    const utc = '("2026-10-21 12:00:00+00","room 4")'
    expect(((await all())[0]!.value as Row).slot, 'the backfill row, from a UTC query session').toBe(utc)

    // The walsender prints ("2026-10-21 05:00:00-07","room 4").
    await pgQuery(h, `UPDATE events SET name = 'booked (edited)' WHERE id = 70`)
    const until = Date.now() + 5000
    while (Date.now() < until) {
      for (const env of await all()) expect((env.value as Row).slot, 'every image of the row spells the instant in UTC').toBe(utc)
      await sleep(250)
    }
    expect(await all(), 'nothing is ingested from the local-zone session').toHaveLength(1)
    expect(h.engineStderr(), 'the refusal names the column').toContain('events.slot')
    expect(h.engineStderr(), 'and the zone').toContain('America/Los_Angeles')

    // Fixed as the message says: the engine's sessions default to UTC, and the next replication
    // connection prints the composite as the query sessions do.
    await pgQuery(h, `DO $$ BEGIN EXECUTE format('ALTER DATABASE %I SET timezone TO ''UTC''', current_database()); END $$`)
    await pgQuery(h, 'SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1', [h.slot])
    await waitFor(async () => (await all()).length === 2, 'the edit to arrive once the zone is UTC', 60000)
    const live = (await all())[1]!.value as Row
    expect(live.name).toBe('booked (edited)')
    expect(live.slot).toBe(utc)
  }, 150000)
})

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
