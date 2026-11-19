// A timestamp must mean the same instant to the engine whatever time zone and date style the
// database, the role or the server defaults to.
//
// The engine keeps a `timestamptz` cell as the text Postgres prints and compares that text. Text
// order is instant order only when every cell is printed in ONE zone and ONE style, and Postgres
// prints in the session's: a database whose default is `America/Los_Angeles` prints 08:00 UTC as
// `2026-10-19 01:00:00-07`, which sorts below a `07:00:00+00` bound it is actually after, and a role
// whose `DateStyle` is `SQL, DMY` prints it as `19/10/2026 01:00:00 PDT`. Nothing set the engine's
// sessions to UTC and ISO, so on such a database the live path judged a window differently from
// the snapshot's SQL, and the rows a client received were spelled in local time. A test cluster
// made by `initdb` takes the HOST's zone, which is how this was first seen.
//
// Each setting below is forced explicitly (`ALTER DATABASE … SET timezone`, `ALTER ROLE … IN
// DATABASE … SET datestyle`, `ALTER SYSTEM`), so the result does not depend on where the suite runs.

import type { Predicate, Row, Schema } from '@electric-circuits/protocol'
import pgpkg from 'pg'
import { afterAll, describe, expect, it, beforeAll } from 'vitest'

import { createShape, foldStream, pgQuery, readEnvelopes, waitFor } from './engine-native.js'
import { bootHarness, drainEngine, type Harness } from './harness.js'

const schema: Schema = {
  tables: {
    records: { columns: { id: { type: 'int' }, visible: { type: 'bool' } }, primaryKey: 'id' },
    events: {
      columns: {
        id: { type: 'int' },
        record_id: { type: 'int' },
        starts: { type: 'text' },
        day: { type: 'text' },
        name: { type: 'text' },
        marks: { type: 'text' },
        span: { type: 'text' },
        slot: { type: 'text' },
      },
      primaryKey: 'id',
    },
  },
}

const ddl = `
  CREATE TYPE booking AS (starts timestamptz, label text);
  CREATE TABLE records (id int PRIMARY KEY, visible boolean NOT NULL);
  ALTER TABLE records REPLICA IDENTITY FULL;
  CREATE TABLE events (
    id int PRIMARY KEY,
    record_id int NOT NULL,
    starts timestamptz NOT NULL,
    day date,
    name text NOT NULL,
    marks timestamptz[],
    span tstzrange,
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

/** Run statements as one session of this database, with no settings of its own. */
async function asDefaultSession(pgUrl: string, sql: string): Promise<void> {
  const client = new pgpkg.Client({ connectionString: pgUrl })
  await client.connect()
  try {
    await client.query(sql)
  } finally {
    await client.end()
  }
}

/** Defaults for every session of this database that does not ask for its own: a zone for the
 *  database, and a date style for the role the engine (and this suite) connects as. */
function setDefaults(timeZone: string, dateStyle: string) {
  return ({ pgUrl }: { pgUrl: string }) =>
    asDefaultSession(
      pgUrl,
      `DO $$ BEGIN
         EXECUTE format('ALTER DATABASE %I SET timezone TO ''${timeZone}''', current_database());
         EXECUTE format('ALTER ROLE %I IN DATABASE %I SET datestyle TO ''${dateStyle}''', current_user, current_database());
       END $$`,
    )
}

/** Each column of event 14 as a UTC session in ISO style prints it. */
const row14 = {
  starts: '2026-10-21 12:34:56.789+00',
  day: '2026-10-21',
  marks: '{"2026-10-21 12:00:00+00","2026-10-22 00:30:00+00"}',
  span: '["2026-10-21 12:00:00+00","2026-10-21 13:30:00+00")',
  slot: '("2026-10-21 12:00:00+00","room 4")',
}
const columns = Object.keys(row14) as (keyof typeof row14)[]
/** Event 14's timestamp-holding columns as an INSERT writes them: every instant with its offset,
 *  so the zone of the session that writes them does not matter. */
const row14Sql = `'2026-10-21 12:34:56.789+00', '2026-10-21',
  ARRAY['2026-10-21 12:00:00+00', '2026-10-22 00:30:00+00']::timestamptz[],
  tstzrange('2026-10-21 12:00:00+00', '2026-10-21 13:30:00+00'),
  ROW('2026-10-21 12:00:00+00', 'room 4')::booking`

/** The timestamp-holding columns of `row`, to compare one image of a row with another. */
const temporal = (row: Row | undefined) => Object.fromEntries(columns.map((column) => [column, row?.[column]]))

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
])('timestamps on a database whose default time zone is %s (%s) and whose role prints `SQL, DMY` dates', (zone) => {
  let h: Harness
  beforeAll(async () => {
    h = await bootHarness(schema, { ddl, beforeEngine: setDefaults(zone, 'SQL, DMY') })
    expect((await pgQuery(h, 'SHOW timezone'))[0]!.TimeZone, 'the database default is in force').toBe(zone)
    expect((await pgQuery(h, 'SHOW datestyle'))[0]!.DateStyle, 'the role default is in force').toBe('SQL, DMY')
    await pgQuery(h, 'INSERT INTO records (id, visible) VALUES (1, true)')
    // Every instant is written with its offset, so the session zone of this INSERT is irrelevant.
    await pgQuery(
      h,
      `INSERT INTO events (id, record_id, starts, name) VALUES
         (10, 1, '2026-10-19 08:00:00+00', 'in the first hours'),
         (11, 1, '2026-10-19 06:00:00+00', 'before the week'),
         (12, 1, '2026-10-26 06:30:00+00', 'in the last minutes'),
         (13, 1, '2026-10-26 08:00:00+00', 'after the week'),
         (20, 1, '2026-11-01 08:45:00+00', 'before the repeated hour'),
         (21, 1, '2026-11-01 09:15:00+00', 'in the repeated hour')`,
    )
    await pgQuery(h, `INSERT INTO events (id, record_id, starts, day, marks, span, slot, name) VALUES (14, 1, ${row14Sql}, 'midweek')`)
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
  // live change spell one instant two ways, every such row looks changed when nothing was — and an
  // old image the engine routes by is judged in a spelling its predicate was not written in. Every
  // timestamp a row holds, inside an array, a range or a composite too, must reach the client as a
  // UTC session in ISO style prints it, by either road.
  it('spells every timestamp and date the same in backfill rows and in live inserts, updates and deletes, in UTC ISO', async () => {
    // A window on the `timestamptz` column that holds event 14 (12:34:56.789Z) and nothing else yet.
    const window = {
      and: [
        { col: 'starts', op: 'gte', value: '2026-10-21T12:30:00Z' },
        { col: 'starts', op: 'lt', value: '2026-10-21T13:00:00+00:00' },
      ],
    }
    const shape = await createShape(h, { table: 'events', where: window })
    await drainEngine(h)
    const mine = async (key: string) => (await readEnvelopes(shape.streamUrl)).filter((env) => env.key === key)

    const [backfill] = await mine('14')
    expect(backfill?.headers.lsn, 'event 14 is in the backfill').toBeUndefined()
    expect(temporal(backfill?.value), 'a backfill row, from a query session').toEqual(row14)

    // An update: the walsender's new image, routed by its old one.
    await pgQuery(h, `UPDATE events SET name = 'midweek (updated live)' WHERE id = 14`)
    await drainEngine(h)
    const updated = (await mine('14')).filter((env) => env.headers.lsn !== undefined)
    // A live change is written as the row's whole new value (`upsert`) or its removal.
    expect(updated.map((env) => env.headers.operation), 'one live change to event 14').toEqual(['upsert'])
    expect(temporal(updated[0]!.value), 'the live update spells every value as the backfill did').toEqual(row14)

    // An insert inside the window and one just past it.
    await pgQuery(
      h,
      `INSERT INTO events (id, record_id, starts, day, marks, span, slot, name) VALUES
         (31, 1, ${row14Sql.replace('12:34:56.789+00', '12:45:00+00')}, 'inserted live'),
         (32, 1, ${row14Sql.replace('12:34:56.789+00', '13:00:00+00')}, 'at the upper bound')`,
    )
    await drainEngine(h)
    const inserted = await mine('31')
    expect(inserted.map((env) => env.headers.operation), 'the insert inside the window, once').toEqual(['upsert'])
    const row31 = { ...row14, starts: '2026-10-21 12:45:00+00' }
    expect(temporal(inserted[0]!.value), 'the live insert').toEqual(row31)
    expect(await mine('32'), 'a row at the exclusive upper bound is not in the window').toHaveLength(0)

    // The same row read by a backfill now, from a second shape over the same window.
    const second = await createShape(h, { table: 'events', where: { ...window, and: [...window.and, { col: 'id', op: 'gte', value: 14 }] } })
    await drainEngine(h)
    const rows = await foldStream(second.streamUrl)
    expect([...rows.keys()].sort(), 'Postgres puts events 14 and 31 in the window').toEqual(['14', '31'])
    expect(temporal(rows.get('31')), 'a backfill of the inserted row equals its live insert').toEqual(row31)

    // A delete is routed by its old image alone: it reaches the shape only if that image's
    // `starts` is judged inside the window.
    await pgQuery(h, 'DELETE FROM events WHERE id = 31')
    await drainEngine(h)
    expect((await mine('31')).map((env) => env.headers.operation), 'the delete reaches the shape').toEqual(['upsert', 'delete'])
    expect([...(await foldStream(shape.streamUrl)).keys()]).toEqual(['14'])
  }, 120000)

  // A literal for an array or a range of timestamps is refused rather than compared as text: the
  // snapshot's SQL reads the literal's elements as instants, and the engine does not.
  it('refuses a predicate that compares an array or a range of timestamps to a literal', async () => {
    for (const [col, type, literal] of [
      ['marks', '_timestamptz', '{"2026-10-21 05:00:00-07","2026-10-21 17:30:00-07"}'],
      ['span', 'tstzrange', '["2026-10-21 12:00:00+00","2026-10-21 13:30:00+00")'],
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
})

// A SERVER-wide setting (postgresql.conf, `ALTER SYSTEM`) does not wait for a reconnect: a reload
// changes it inside every session that did not ask for its own, the walsender that is already
// streaming included. The engine's sessions asked for theirs at startup, which a reload does not
// override.
describe('a server-wide time zone and date style reloaded under a running engine', () => {
  let h: Harness | undefined
  const serverWide = async (sql: string) => {
    await pgQuery(h!, sql)
    await pgQuery(h!, 'SELECT pg_reload_conf()')
  }
  afterAll(async () => {
    await h?.shutdown()
  })

  it('does not reach the replication connection that is already open', async () => {
    h = await bootHarness(schema, { ddl })
    try {
      await pgQuery(h, `INSERT INTO events (id, record_id, starts, day, marks, span, slot, name) VALUES (14, 1, ${row14Sql}, 'midweek')`)
      await drainEngine(h)
      const shape = await createShape(h, { table: 'events', where: { col: 'id', op: 'eq', value: 14 } })
      await drainEngine(h)
      const walsender = async () =>
        (await pgQuery(h!, 'SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1', [h!.slot]))[0]?.active_pid
      const connected = await walsender()
      expect(connected, 'the walsender is connected').toBeTruthy()

      // No reconnect: every session without settings of its own now prints `21.10.2026 18:04:56.789 IST`.
      await serverWide(`ALTER SYSTEM SET timezone TO 'Asia/Kolkata'`)
      await serverWide(`ALTER SYSTEM SET datestyle TO 'German, DMY'`)
      expect((await pgQuery(h, 'SHOW datestyle'))[0]!.DateStyle, 'the reload is in force').toBe('German, DMY')
      await pgQuery(h, `UPDATE events SET name = 'edited after the reload' WHERE id = 14`)
      await drainEngine(h)

      expect(await walsender(), 'the same walsender delivered the change').toBe(connected)
      const envs = await readEnvelopes(shape.streamUrl)
      expect(envs.map((env) => env.headers.lsn !== undefined), 'one backfill row, then one live change').toEqual([false, true])
      const [backfill, live] = envs.map((env) => env.value as Row)
      expect(live!.name).toBe('edited after the reload')
      expect(temporal(backfill), 'backfill').toEqual(row14)
      expect(temporal(live), 'live').toEqual(row14)
    } finally {
      // The settings are the whole test cluster's: never leave them behind for the files that follow.
      await serverWide('ALTER SYSTEM RESET timezone')
      await serverWide('ALTER SYSTEM RESET datestyle')
    }
  }, 150000)
})
