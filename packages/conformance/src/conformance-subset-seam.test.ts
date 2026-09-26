// Stock PostgreSQL fault cut: COMMIT has flushed its WAL but is held in SyncRep.
// This file owns its cluster; never ALTER SYSTEM on the caller's/shared database.
import { execFileSync } from 'node:child_process'
import { appendFileSync, mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import pg from 'pg'
import { lsnToU64, mergeFeedDelta, type SubsetView } from '@electric-circuits/client'
import type { Row, Schema, StreamEnvelope } from '@electric-circuits/protocol'
import { postgres18Tools } from '../../../scripts/postgres18.js'
import { bootHarness, drainEngine, type Harness } from './harness.js'

const schema: Schema = { tables: { items: { columns: { id: { type: 'int' }, n: { type: 'int' } }, primaryKey: 'id' } } }
const tools = postgres18Tools()
let dir: string
let previousUrl: string | undefined
let h: Harness | undefined
let writer: pg.Client | undefined
let commit: Promise<unknown> | undefined
let pid: number | undefined

beforeAll(() => {
  previousUrl = process.env.ELECTRIC_CIRCUITS_TEST_PG_URL
  dir = mkdtempSync(join(tmpdir(), 'subset-seam-pg-'))
  execFileSync(tools.initdb, ['-D', join(dir, 'data'), '-U', 'postgres', '--auth=trust', '--no-sync'], { stdio: 'ignore' })
  // An exclusively owned socket directory and port avoid both shared and Docker clusters.
  appendFileSync(join(dir, 'data/postgresql.conf'), `\nwal_level=logical\nmax_replication_slots=10\nmax_wal_senders=10\nsynchronous_commit=local\nsynchronous_standby_names='phantom_standby'\nlisten_addresses='127.0.0.1'\nunix_socket_directories='${dir}'\n`)
  for (let attempt = 0; attempt < 8; attempt++) {
    const port = 59000 + Math.floor(Math.random() * 5000)
    try {
      execFileSync(tools.pgCtl, ['-D', join(dir, 'data'), '-l', join(dir, 'postgres.log'), '-o', `-p ${port}`, '-w', 'start'], { stdio: 'ignore' })
      process.env.ELECTRIC_CIRCUITS_TEST_PG_URL = `postgres://postgres@127.0.0.1:${port}/postgres`
      return
    } catch (error) { if (attempt === 7) throw error }
  }
})
afterAll(() => {
  process.env.ELECTRIC_CIRCUITS_TEST_PG_URL = previousUrl
  if (dir) {
    try { execFileSync(tools.pgCtl, ['-D', join(dir, 'data'), '-m', 'immediate', '-w', 'stop'], { stdio: 'ignore' }) }
    finally { rmSync(dir, { recursive: true, force: true }) }
  }
})
beforeEach(async () => { h = await bootHarness(schema); await drainEngine(h) })
afterEach(async () => {
  await release()
  await h?.shutdown()
  h = undefined
})

async function sql(text: string, params: unknown[] = []) {
  const c = new pg.Client({ connectionString: h!.pgUrl })
  await c.connect()
  try { return (await c.query(text, params)).rows }
  finally { await c.end() }
}
async function post<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${h!.engineUrl}${path}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) })
  if (!res.ok) throw new Error(`${path}: ${res.status} ${await res.text()}`)
  return await res.json() as T
}
type Feed = { shapeId: string; streamUrl: string }
type Page = { rows: Row[]; lsn: string }
const feed = () => post<Feed>('/v1/subset-feeds', { table: 'items' })
const page = () => post<Page>('/v1/subsets/query', { table: 'items', orderBy: { col: 'id' }, limit: 100 })
async function head(url: string) {
  const res = await fetch(url, { method: 'HEAD' })
  expect(res.ok).toBe(true)
  return res.headers.get('stream-next-offset')!
}
async function read(url: string, offset = '-1'): Promise<StreamEnvelope[]> {
  const out: StreamEnvelope[] = []
  for (let i = 0; i < 100; i++) {
    const res = await fetch(`${url}?offset=${encodeURIComponent(offset)}`)
    if (res.status === 204) return out
    expect(res.ok).toBe(true)
    const text = (await res.text()).trim()
    if (text) out.push(...JSON.parse(text) as StreamEnvelope[])
    offset = res.headers.get('stream-next-offset')!
    if (res.headers.has('stream-up-to-date') || !text) return out
  }
  throw new Error('feed exceeded 100 catch-up pages')
}
async function hold(statement: string): Promise<string> {
  writer = new pg.Client({ connectionString: h!.pgUrl })
  await writer.connect()
  await writer.query('BEGIN; SET LOCAL synchronous_commit = on')
  const info = (await writer.query('SELECT pg_backend_pid() AS pid, pg_current_xact_id()::text AS xid')).rows[0]
  pid = info.pid
  await writer.query(statement)
  commit = writer.query('COMMIT')
  const deadline = Date.now() + 10000
  // Poll a named PG wait event, never use elapsed time to establish ordering.
  for (;;) {
    const state = await sql('SELECT wait_event FROM pg_stat_activity WHERE pid=$1', [pid])
    if (state[0]?.wait_event === 'SyncRep') break
    if (Date.now() > deadline) throw new Error(`T ${info.xid} did not enter SyncRep`)
    await new Promise(r => setTimeout(r, 10))
  }
  await drainEngine(h!) // later local commit moves xmax past T and proves sequencer delivery
  return info.xid
}
async function release() {
  if (!writer) return
  try {
    await sql('SELECT pg_cancel_backend($1)', [pid])
    await commit
  } finally { await writer.end(); writer = undefined; commit = undefined; pid = undefined }
}
async function mechanism(xid: string, f: Feed, p: Page) {
  const [probe] = await sql('SELECT pg_current_snapshot()::text AS snapshot, pg_xact_status($1::xid8) AS status', [xid])
  expect(probe.snapshot.split(':')[2].split(',')).toContain(xid)
  const env = (await read(f.streamUrl)).find(e => e.headers.txid === xid)!
  expect(env, 'T must already be decoded and durably delivered while invisible').toBeDefined()
  expect(lsnToU64(env.headers.lsn)! < lsnToU64(p.lsn)!).toBe(true)
  console.log('SEAM', { xid, ...probe, commitLsn: env.headers.lsn, snapshotLsn: p.lsn })
  return env
}
function fold(p: Page, envs: StreamEnvelope[], bypassFloor = false): Row[] {
  const rows = new Map(p.rows.map(r => [String(r.id), r]))
  const S = lsnToU64(p.lsn)!
  const view: SubsetView = { snapshotLsn: bypassFloor ? 0n : S, present: new Set(rows.keys()), applied: new Map([...rows.keys()].map(k => [k, S])), inView: () => true }
  for (const env of envs) {
    const action = mergeFeedDelta(view, env)
    if (action?.type === 'delete') rows.delete(env.key)
    else if (action) rows.set(env.key, action.value)
  }
  return [...rows.values()]
}

describe('subset snapshot/feed invisible committed transaction', () => {
  it('A: applies an invisible commit delivered after HEAD through the real client merge', async () => {
    const f = await feed()
    const offset = await head(f.streamUrl)
    const xid = await hold('INSERT INTO items VALUES (1, 10)')
    const p = await page()
    expect(p.rows).toEqual([])
    const env = await mechanism(xid, f, p)
    const tail = await read(f.streamUrl, offset)
    expect(tail).toContainEqual(env)
    console.log('POSITION A', { offset, after: await head(f.streamUrl) })
    await release()
    await drainEngine(h!)
    expect(fold(p, await read(f.streamUrl, offset))).toEqual(await sql('SELECT id,n FROM items ORDER BY id'))
  })
  it('B: covers an invisible commit delivered before HEAD on a shared subset feed', async () => {
    const f = await feed()
    const xid = await hold('INSERT INTO items VALUES (1, 10)')
    const offset = await head(f.streamUrl)
    const p = await page()
    expect(p.rows).toEqual([])
    await mechanism(xid, f, p)
    expect(await read(f.streamUrl, offset)).toEqual([])
    console.log('POSITION B', { offset })
    await release()
    await drainEngine(h!)
    expect(fold(p, await read(f.streamUrl, offset))).toEqual(await sql('SELECT id,n FROM items ORDER BY id'))
  })
  it('B full shape: covers a commit sequenced before BeginShape but excluded by backfill', async () => {
    const diagnostic = await feed()
    const xid = await hold('INSERT INTO items VALUES (1, 10)')
    const p = await page()
    await mechanism(xid, diagnostic, p)
    const shape = await post<Feed>('/v1/shapes', { table: 'items' })
    expect(await read(shape.streamUrl)).toEqual([])
    await release()
    await drainEngine(h!)
    expect((await read(shape.streamUrl)).filter(e => e.value).map(e => e.value)).toEqual(await sql('SELECT id,n FROM items ORDER BY id'))
  })
  it('watermark: accepts an invisible update even after the global LSN filter is bypassed', async () => {
    await sql('INSERT INTO items VALUES (1, 0)')
    await drainEngine(h!)
    const f = await feed()
    const offset = await head(f.streamUrl)
    const xid = await hold('UPDATE items SET n=10 WHERE id=1')
    const p = await page()
    expect(p.rows).toEqual([{ id: 1, n: 0 }])
    await mechanism(xid, f, p)
    await release()
    await drainEngine(h!)
    expect(fold(p, await read(f.streamUrl, offset), true)).toEqual(await sql('SELECT id,n FROM items ORDER BY id'))
  })
})
