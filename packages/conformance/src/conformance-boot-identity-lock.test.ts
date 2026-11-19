// A boot must never queue an application table behind the engine's replica-identity setup.
//
// `ALTER TABLE … REPLICA IDENTITY FULL` takes `ACCESS EXCLUSIVE` even when the table already has
// full identity. While that request waits behind ANY open transaction that touched the table, every
// later statement on the table — a plain `SELECT` included — queues behind the request. A boot that
// issues the `ALTER` unconditionally and waits without bound therefore turns one idle application
// transaction into an outage of that table for as long as the transaction stays open, and the engine
// itself never becomes ready.
//
// Two promises, both observed from outside the engine (Postgres' own lock table, a third session's
// `SELECT` latency, `GET /ready`, the catalog):
//
//   1. A table that already has full identity is not locked at all: the boot resolves while the
//      other session's transaction is still open, and nothing ever waits on the table.
//   2. A table that needs the `ALTER` is asked for with a bounded lock wait. When the wait expires
//      the request is withdrawn, the queued readers proceed, and the boot retries after a pause;
//      once the transaction ends the identity is set, the boot resolves, and the schema fingerprint
//      it recorded is the post-`ALTER` one.
//
// Both run as a RESTART (the slot and the publication already exist), which is the case that
// matters in production: the tables have had full identity since the first boot.

import type { Schema } from '@electric-circuits/protocol'
import pgpkg from 'pg'
import { afterEach, describe, expect, it } from 'vitest'
import { testPhysicalPath } from './ds-mtls-access.js'
import { createShape, pgQuery, sleep, waitFor } from './engine-native.js'
import { bootHarness, drainEngine, type Harness } from './harness.js'

const schema: Schema = {
  tables: { items: { columns: { id: { type: 'int' }, label: { type: 'text' } }, primaryKey: 'id' } },
}

/** The engine's bound on one boot-time lock wait (`BOOT_IDENTITY_LOCK_TIMEOUT` in `engine/mod.rs`). */
const BOOT_LOCK_TIMEOUT_MS = 2000
/** Scheduling slack on top of that bound: the lock manager's timer, the probe's own round trip. */
const SLACK_MS = 1500
/** A `SELECT count(*)` on a one-row table that nothing blocks returns in milliseconds; anything near
 * a second waited on a lock. Kept under the boot's lock timeout so even ONE bounded wait would show. */
const NEVER_BLOCKED_MS = 1000

let h: Harness | undefined
afterEach(async () => {
  await h?.shutdown()
  h = undefined
})

const pg = (sql: string, params: unknown[] = []) => pgQuery(h!, sql, params)

async function replicaIdentity(): Promise<string> {
  const rows = await pg(`SELECT relreplident::text AS r FROM pg_class WHERE oid = 'public.items'::regclass`)
  return String(rows[0]?.r)
}

async function readyStatus(engineUrl: string): Promise<string> {
  const res = await fetch(`${engineUrl}/ready`).catch(() => undefined)
  if (!res) return 'unreachable'
  return ((await res.json()) as { status: string }).status
}

/** Boot once (creating the slot and the publication), seed a row, then stop the engine. */
async function bootThenStop(): Promise<Harness> {
  h = await bootHarness(schema)
  await pg(`INSERT INTO items (id, label) VALUES (1, 'a')`)
  await drainEngine(h)
  h.signalEngine('SIGKILL')
  await h.waitForEngineExit()
  return h
}

/**
 * Another session's open transaction that has READ the table: it holds `ACCESS SHARE` on `items`
 * until it ends, which is all it takes to make an `ACCESS EXCLUSIVE` request wait.
 */
async function openReadingTransaction(pgUrl: string) {
  const c = new pgpkg.Client({ connectionString: pgUrl })
  await c.connect()
  await c.query('BEGIN')
  await c.query('SELECT * FROM items')
  const pid = Number((await c.query('SELECT pg_backend_pid() AS pid')).rows[0].pid)
  let ended = false
  return {
    /** `idle in transaction` for as long as the transaction is still open. */
    state: async () => String((await pg('SELECT state FROM pg_stat_activity WHERE pid = $1', [pid]))[0]?.state),
    commit: async () => {
      ended = true
      await c.query('COMMIT')
    },
    close: async () => {
      if (!ended) await c.query('ROLLBACK').catch(() => {})
      await c.end().catch(() => {})
    },
  }
}

interface LockRequest {
  pid: number
  query: string
}

/**
 * Two observers of `items`, each on its own connection, running from before the engine starts:
 *
 * - a **sampler** of Postgres' lock table, recording every `ACCESS EXCLUSIVE` request seen waiting
 *   on the table and whether the reader below was seen queued;
 * - a **reader**, the third session: `SELECT count(*) FROM items` in a loop, timing each one. Its
 *   longest wait INCLUDES a `SELECT` that is still in flight, so a reader stuck behind a lock
 *   request counts for as long as it has been stuck.
 */
async function watchItems(pgUrl: string) {
  const sampler = new pgpkg.Client({ connectionString: pgUrl })
  const reader = new pgpkg.Client({ connectionString: pgUrl })
  await sampler.connect()
  await reader.connect()
  const readerPid = Number((await reader.query('SELECT pg_backend_pid() AS pid')).rows[0].pid)

  let stopped = false
  const exclusive = new Map<number, string>()
  let readerQueued = false
  let completed = 0
  let slowestMs = 0
  let inFlightSince: number | undefined

  // A connection cut at teardown is expected; one cut mid-test is a broken observation, and `stop`
  // reports it rather than letting the assertions pass on a watcher that had stopped watching.
  let broken: Error | undefined
  const guarded = (loop: () => Promise<void>) =>
    loop().catch((e: Error) => {
      if (!stopped) broken = e
    })

  const sampling = guarded(async () => {
    while (!stopped) {
      const { rows } = await sampler.query(
        `SELECT l.pid, l.mode, a.query
           FROM pg_locks AS l
           JOIN pg_stat_activity AS a ON a.pid = l.pid
          WHERE l.locktype = 'relation' AND l.relation = 'public.items'::regclass AND NOT l.granted`,
      )
      for (const row of rows as Array<{ pid: number; mode: string; query: string }>) {
        if (row.mode === 'AccessExclusiveLock') exclusive.set(Number(row.pid), row.query)
        if (Number(row.pid) === readerPid) readerQueued = true
      }
      await sleep(20)
    }
  })

  const reading = guarded(async () => {
    while (!stopped) {
      inFlightSince = Date.now()
      await reader.query('SELECT count(*) FROM items')
      slowestMs = Math.max(slowestMs, Date.now() - inFlightSince)
      inFlightSince = undefined
      completed += 1
      await sleep(25)
    }
  })

  return {
    /** Every `ACCESS EXCLUSIVE` request seen waiting on `items`, with the statement that made it. */
    exclusiveRequests: (): LockRequest[] => [...exclusive].map(([pid, query]) => ({ pid, query })),
    /** Was the reader seen queued on the table? */
    readerQueued: () => readerQueued,
    selects: () => completed,
    longestSelectMs: () => Math.max(slowestMs, inFlightSince === undefined ? 0 : Date.now() - inFlightSince),
    /** Call only once nothing can still block the reader, or this waits with it. */
    stop: async () => {
      stopped = true
      await Promise.all([sampling, reading])
      await sampler.end().catch(() => {})
      await reader.end().catch(() => {})
      if (broken) throw new Error(`the watcher of items stopped observing mid-test: ${broken.message}`)
    },
  }
}

/** The `created` record the durable catalog holds for one shape. */
async function createdRecord(shapeId: string): Promise<{ fingerprint?: { replident?: string } } | undefined> {
  let offset = '-1'
  for (let page = 0; page < 100; page++) {
    const res = await fetch(`${h!.dsUrl}/${testPhysicalPath('meta/catalog')}?offset=${encodeURIComponent(offset)}`)
    if (res.status === 204) break
    if (!res.ok) throw new Error(`GET meta/catalog -> ${res.status}`)
    const body = (await res.text()).trim()
    const events = body
      ? (JSON.parse(body) as Array<{ t?: string; rec?: { id: string; fingerprint?: { replident?: string } } }>)
      : []
    const created = events.find((e) => e.t === 'created' && e.rec?.id === shapeId)
    if (created) return created.rec
    const next = res.headers.get('stream-next-offset')
    if (!next || next === offset || res.headers.get('stream-up-to-date') !== null) break
    offset = next
  }
  return undefined
}

describe('boot-time replica identity never queues a table behind an open transaction', () => {
  it('a table that already has full identity is not locked: the boot resolves past an open transaction', async () => {
    const harness = await bootThenStop()
    expect(await replicaIdentity()).toBe('f')

    const blocker = await openReadingTransaction(harness.pgUrl)
    const watch = await watchItems(harness.pgUrl)
    try {
      await harness.startEngine(async (url) => {
        // Either the boot resolves, or the engine is seen asking for the table lock. With the other
        // session's transaction open that request can only wait, so it cannot be missed.
        await waitFor(
          async () => watch.exclusiveRequests().length > 0 || (await readyStatus(url)) === 'active',
          'the engine to become ready, or to be seen requesting ACCESS EXCLUSIVE on items',
        )
        expect(
          watch.exclusiveRequests(),
          'items already has REPLICA IDENTITY FULL, so the boot must not request ACCESS EXCLUSIVE on it',
        ).toEqual([])
      })

      // Ready WHILE the other session's transaction is still open — not after it ended.
      expect(await blocker.state()).toBe('idle in transaction')
      expect(await readyStatus(harness.engineUrl)).toBe('active')
      expect(watch.exclusiveRequests()).toEqual([])
      // The third session read the table throughout the boot and never waited.
      expect(watch.selects()).toBeGreaterThan(0)
      expect(watch.readerQueued(), 'a SELECT on items queued behind a lock request during the boot').toBe(false)
      expect(watch.longestSelectMs()).toBeLessThan(NEVER_BLOCKED_MS)
      // ...and the engine it booted into is ingesting.
      await drainEngine(harness)
    } finally {
      await blocker.close()
      await watch.stop()
    }
  }, 90_000)

  it('a table that needs the ALTER waits for its lock in bounded attempts, and gets it once the transaction ends', async () => {
    const harness = await bootThenStop()
    await pg('ALTER TABLE items REPLICA IDENTITY DEFAULT')
    expect(await replicaIdentity()).toBe('d')

    const blocker = await openReadingTransaction(harness.pgUrl)
    const watch = await watchItems(harness.pgUrl)
    const bound = BOOT_LOCK_TIMEOUT_MS + SLACK_MS
    try {
      await harness.startEngine(async (url) => {
        // Arrived: the engine's ALTER is waiting behind the open transaction.
        await waitFor(
          () => watch.exclusiveRequests().length > 0,
          'the engine to request ACCESS EXCLUSIVE on items for REPLICA IDENTITY FULL',
        )
        // Held: the third session's SELECT is queued behind that request.
        await waitFor(() => watch.readerQueued(), 'a SELECT on items to queue behind the engine request', 10_000)
        const queuedWith = watch.selects()
        // Released: the engine withdraws the request within its bound — the transaction that made
        // it wait is still open, so nothing else can have released the reader.
        while (watch.selects() === queuedWith && watch.longestSelectMs() < bound) await sleep(20)
        expect(
          watch.longestSelectMs(),
          `a SELECT on items was still queued behind the engine's ALTER after ${bound} ms`,
        ).toBeLessThan(bound)
        expect(await blocker.state()).toBe('idle in transaction')

        // The boot has not resolved and has not given up either: no identity yet, not ready.
        expect(await replicaIdentity()).toBe('d')
        expect(await readyStatus(url)).toBe('starting')

        await blocker.commit()
      })

      // Terminal: with the transaction gone, a later attempt set the identity and the boot resolved.
      expect(await readyStatus(harness.engineUrl)).toBe('active')
      expect(await replicaIdentity()).toBe('f')
      // No reader ever waited longer than one bounded attempt, across the whole boot.
      expect(watch.longestSelectMs()).toBeLessThan(bound)

      // Each failed attempt is a WARN naming the table and the attempt.
      const attempt = harness
        .engineStderr()
        .split('\n')
        .find((line) => line.includes('public.items') && line.includes('attempt 1'))
      expect(attempt, 'a log line for the first timed-out attempt on public.items').toBeDefined()
      expect(attempt).toContain('WARN')
      expect(attempt).toContain('REPLICA IDENTITY FULL')

      // The schema the boot compiled carries the post-ALTER identity: a shape created now records it.
      const shape = await createShape(harness, { table: 'items', where: { col: 'id', op: 'gte', value: 0 } })
      expect((await createdRecord(shape.shapeId))?.fingerprint?.replident).toBe('f')
      await drainEngine(harness)
    } finally {
      await blocker.close()
      await watch.stop()
    }
  }, 90_000)
})
