// Settling snapshots against what the engine has already fanned out — the failure behaviour.
//
// `conformance-subset-seam.test.ts` proves the settle makes a committed-but-invisible transaction T
// reach the subset and the full shape. This file proves the settle cannot turn into an outage:
//
//  * a shape create whose snapshot cannot settle answers a retryable 503 with `Retry-After` (never a
//    500), and a query on the held table fails fast with the same, within the settle budget;
//  * a held transaction on one table never delays reads of another table;
//  * past the waiter cap, excess requests are refused at once, and the admitted one still succeeds
//    once T becomes visible;
//  * a long-open transaction the engine never sequenced (pinning `xmin`) never blocks settling, even
//    when the settle record has overflowed its bound;
//  * at its bound the record gives up its oldest transactions instead of failing snapshots.
//
// T is held exactly as in the seam test: the cluster names a synchronous standby that never connects
// (`phantom_standby`), T commits with `synchronous_commit = on` and parks in `SyncRepWaitForLSN` after
// its commit record is flushed (committed, decodable, invisible to new snapshots), and
// `pg_cancel_backend` ends the wait. The cluster is owned by this file.
import { execFileSync } from 'node:child_process'
import { appendFileSync, mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest'
import pg from 'pg'
import type { Row, Schema } from '@electric-circuits/protocol'
import { postgres18Tools } from '../../../scripts/postgres18.js'
import { bootHarness, drainEngine, type Harness } from './harness.js'

const schema: Schema = {
  tables: {
    items: { columns: { id: { type: 'int' }, n: { type: 'int' } }, primaryKey: 'id' },
    other: { columns: { id: { type: 'int' }, n: { type: 'int' } }, primaryKey: 'id' },
  },
}
const tools = postgres18Tools()
let dir: string
let previousUrl: string | undefined
let h: Harness | undefined
let held: { client: pg.Client; commit: Promise<unknown>; pid: number; xid: string } | undefined
let pin: pg.Client | undefined

beforeAll(() => {
  previousUrl = process.env.ELECTRIC_CIRCUITS_TEST_PG_URL
  dir = mkdtempSync(join(tmpdir(), 'settle-admission-pg-'))
  execFileSync(tools.initdb, ['-D', join(dir, 'data'), '-U', 'postgres', '--auth=trust', '--no-sync'], { stdio: 'ignore' })
  appendFileSync(
    join(dir, 'data/postgresql.conf'),
    `\nwal_level=logical\nmax_replication_slots=10\nmax_wal_senders=10\nsynchronous_commit=local\n` +
      `synchronous_standby_names='phantom_standby'\nlisten_addresses='127.0.0.1'\nunix_socket_directories='${dir}'\n`,
  )
  for (let attempt = 0; attempt < 8; attempt++) {
    const port = 59000 + Math.floor(Math.random() * 5000)
    try {
      execFileSync(tools.pgCtl, ['-D', join(dir, 'data'), '-l', join(dir, 'postgres.log'), '-o', `-p ${port}`, '-w', 'start'], { stdio: 'ignore' })
      process.env.ELECTRIC_CIRCUITS_TEST_PG_URL = `postgres://postgres@127.0.0.1:${port}/postgres`
      return
    } catch (error) {
      if (attempt === 7) throw error
    }
  }
})
afterAll(() => {
  process.env.ELECTRIC_CIRCUITS_TEST_PG_URL = previousUrl
  if (dir) {
    try {
      execFileSync(tools.pgCtl, ['-D', join(dir, 'data'), '-m', 'immediate', '-w', 'stop'], { stdio: 'ignore' })
    } finally {
      rmSync(dir, { recursive: true, force: true })
    }
  }
})
afterEach(async () => {
  await release()
  if (pin) {
    await pin.query('ROLLBACK').catch(() => {})
    await pin.end().catch(() => {})
    pin = undefined
  }
  await h?.shutdown()
  h = undefined
})

async function boot(engineEnv: Record<string, string>): Promise<void> {
  h = await bootHarness(schema, { engineEnv })
  await drainEngine(h)
}

async function sql(text: string, params: unknown[] = []): Promise<Row[]> {
  const c = new pg.Client({ connectionString: h!.pgUrl })
  await c.connect()
  try {
    return (await c.query(text, params)).rows as Row[]
  } finally {
    await c.end()
  }
}

/** Commit `statement` as T and return once T is parked in SyncRep: committed, invisible. */
async function holdCommit(statement: string): Promise<string> {
  const client = new pg.Client({ connectionString: h!.pgUrl })
  await client.connect()
  await client.query('BEGIN; SET LOCAL synchronous_commit = on')
  const info = (await client.query('SELECT pg_backend_pid() AS pid, pg_current_xact_id()::text AS xid')).rows[0]
  await client.query(statement)
  held = { client, commit: client.query('COMMIT').catch((e: Error) => e), pid: info.pid, xid: info.xid }
  const deadline = Date.now() + 10000
  for (;;) {
    const [state] = await sql('SELECT wait_event FROM pg_stat_activity WHERE pid = $1', [info.pid])
    if (state?.wait_event === 'SyncRep') break
    if (Date.now() > deadline) throw new Error(`T ${info.xid} did not reach SyncRep`)
    await new Promise((r) => setTimeout(r, 10))
  }
  await sql('SELECT pg_current_xact_id()') // a later completed xid: xmax moves past T, T is in xip
  return info.xid
}

async function release(): Promise<void> {
  if (!held) return
  const { client, commit, pid } = held
  held = undefined
  try {
    await sql('SELECT pg_cancel_backend($1)', [pid])
    await commit
  } finally {
    await client.end()
  }
}

type Answer = { status: number; ms: number; retryAfter: string | null; body: any }
async function post(path: string, body: unknown): Promise<Answer> {
  const t0 = performance.now()
  const res = await fetch(`${h!.engineUrl}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  })
  const text = await res.text()
  let json: unknown = text
  try {
    json = JSON.parse(text)
  } catch {
    /* not JSON */
  }
  return { status: res.status, ms: performance.now() - t0, retryAfter: res.headers.get('retry-after'), body: json }
}
const query = (table: string) => post('/v1/subsets/query', { table, orderBy: { col: 'id' }, limit: 100 })
async function replicationStatus(): Promise<{ visibilityWaits?: number; settle?: Record<string, any> }> {
  return (await (await fetch(`${h!.engineUrl}/replication/lsn`)).json()) as { visibilityWaits?: number; settle?: Record<string, any> }
}
async function settleStats(): Promise<Record<string, any>> {
  return (await replicationStatus()).settle ?? {}
}

describe('settling snapshots never turns into an outage', () => {
  it('a shape create whose snapshot cannot settle answers 503 with Retry-After, and a held-table query fails fast', async () => {
    await boot({ ELECTRIC_CIRCUITS_SNAPSHOT_SETTLE_TIMEOUT_MS: '600' })
    await holdCommit('INSERT INTO items VALUES (1, 10)')
    await drainEngine(h!) // T is sequenced (on the change log) while still invisible

    const create = await post('/v1/shapes', { table: 'items' })
    console.log('create on the held table', create)
    expect(create.status, JSON.stringify(create.body)).toBe(503)
    expect(create.retryAfter).toBe('1')
    expect(String((create.body as { error?: string }).error)).toContain('not settled')

    const q = await query('items')
    expect(q.status).toBe(503)
    expect(q.retryAfter).toBe('1')
    expect(q.ms, 'bounded by the settle budget, not a gateway timeout').toBeLessThan(5000)

    // Once T is visible the same requests succeed, and nothing was retired meanwhile.
    await release()
    const again = await post('/v1/shapes', { table: 'items' })
    expect(again.status).toBe(200)
    expect((await query('items')).status).toBe(200)
    expect((await settleStats()).timeouts).toBeGreaterThanOrEqual(2)
  })

  it('a transaction held on one table never delays reads, or shape creates, on another', async () => {
    await boot({ ELECTRIC_CIRCUITS_SNAPSHOT_SETTLE_TIMEOUT_MS: '3000' })
    await sql('INSERT INTO other VALUES (1, 1)')
    await holdCommit('INSERT INTO items VALUES (1, 10)')
    await drainEngine(h!)

    const held = query('items') // waits the full budget, holding no pooled connection
    const others = await Promise.all(Array.from({ length: 10 }, () => query('other')))
    const create = await post('/v1/shapes', { table: 'other' })
    expect(others.map((a) => a.status)).toEqual(Array(10).fill(200))
    expect(Math.max(...others.map((a) => a.ms)), 'unrelated reads are not queued behind the held one').toBeLessThan(1500)
    expect(create.status).toBe(200)
    expect((await held).status).toBe(503)
  })

  it('past the waiter cap a request is refused at once; the admitted one succeeds when T becomes visible', async () => {
    await boot({ ELECTRIC_CIRCUITS_SNAPSHOT_SETTLE_TIMEOUT_MS: '8000', ELECTRIC_CIRCUITS_SNAPSHOT_SETTLE_MAX_WAITERS: '1' })
    await holdCommit('INSERT INTO items VALUES (1, 10)')
    await drainEngine(h!)
    const rejectedBefore = (await settleStats()).rejections ?? 0

    const first = query('items')
    // Wait until it is admitted and waiting (a named engine state, not elapsed time).
    const deadline = Date.now() + 5000
    while (((await replicationStatus()).visibilityWaits ?? 0) < 1) {
      if (Date.now() > deadline) throw new Error('the first query never started waiting')
      await new Promise((r) => setTimeout(r, 10))
    }
    const excess = await Promise.all(Array.from({ length: 5 }, () => query('items')))
    expect(excess.map((a) => a.status)).toEqual(Array(5).fill(503))
    expect(excess.every((a) => a.retryAfter === '1')).toBe(true)
    expect(Math.max(...excess.map((a) => a.ms)), 'refused at once, not after the budget').toBeLessThan(1000)
    expect((await settleStats()).rejections - rejectedBefore).toBe(5)

    await release()
    const admitted = await first
    expect(admitted.status).toBe(200)
    expect((admitted.body as { rows: Row[] }).rows.map((r) => Number(r.id))).toEqual([1])
  })

  it('a long-open unsequenced writer never blocks settling, and at its bound the record degrades without 503s', async () => {
    // A poller that effectively never ticks on its own and the smallest bound: the record only grows,
    // so it must hit the bound — the exact condition under which waiting on xmin failed everything.
    await boot({ ELECTRIC_CIRCUITS_SNAPSHOT_SETTLE_MAX_XIDS: '1024', ELECTRIC_CIRCUITS_SNAPSHOT_SETTLE_POLL_MS: '600000' })
    pin = new pg.Client({ connectionString: h!.pgUrl })
    await pin.connect()
    await pin.query('BEGIN')
    await pin.query('INSERT INTO other VALUES (1000, 0)') // holds an xid (and xmin) until the test ends
    const [{ x: pinXid }] = await pin.query('SELECT pg_current_xact_id()::text AS x').then((r) => r.rows)

    // T, held and sequenced, is the OLDEST entry; then enough commits to push the record past 1024.
    await holdCommit('INSERT INTO items VALUES (1, 10)')
    const writer = new pg.Client({ connectionString: h!.pgUrl })
    await writer.connect()
    for (let i = 0; i < 2100; i++) await writer.query('INSERT INTO items VALUES ($1, 0)', [100 + i])
    await writer.end()
    await drainEngine(h!, 60000)

    const stats = await settleStats()
    console.log('settle after overflow', { pinXid, ...stats, waitMs: undefined })
    expect(stats.xidsDropped, 'the bound was hit and counted').toBeGreaterThan(0)
    expect(stats.sequencedXids).toBeLessThanOrEqual(2048)
    const [snap] = await sql('SELECT pg_current_snapshot()::text AS s')
    expect(String(snap!.s).startsWith(`${pinXid}:`), 'xmin is pinned by the open writer').toBe(true)

    // Every query succeeds promptly: the pin was never sequenced, and T — dropped at the bound — is
    // no longer waited for.
    const answers = await Promise.all(Array.from({ length: 20 }, () => query('items')))
    expect(answers.map((a) => a.status)).toEqual(Array(20).fill(200))
    expect(Math.max(...answers.map((a) => a.ms))).toBeLessThan(2000)
    const create = await post('/v1/shapes', { table: 'items', where: { col: 'n', op: 'eq', value: 0 } })
    expect(create.status).toBe(200)
  })
})
