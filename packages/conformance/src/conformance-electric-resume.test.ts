// OTTO-11096: an Electric handle survives an engine restart. A client that reconnects with its old
// handle and offset resumes in the same stream instead of being told to re-download the whole shape,
// whenever that is safe; anything else is the protocol's own `409 must-refetch`.
//
// Real PostgreSQL 18, replication, durable-streams and engine process (SIGKILL + reboot). Rows are
// compared with SQL only after `drainEngine`, and every client here rejects what Electric's client
// rejects, so a wrong insert/update/delete classification fails where it happens.

import { ShapeStream, isChangeMessage, type Row as ElectricRow } from '@electric-sql/client'
import type { Schema } from '@electric-circuits/protocol'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { testPhysicalPath } from './ds-mtls-access.js'
import { pgQuery, waitFor } from './engine-native.js'
import { bootHarness, drainEngine, type Harness } from './harness.js'

const table = (extra: Record<string, { type: 'int' | 'text' }> = {}) => ({
  columns: { id: { type: 'int' as const }, n: { type: 'int' as const }, label: { type: 'text' as const }, ...extra },
  primaryKey: 'id',
})

const tables = ['resumed', 'retired', 'filtered', 'raced', 'legacy', 'claimed', 'minted', 'positions', 'official'] as const
const schema: Schema = { tables: Object.fromEntries(tables.map((name) => [name, table()])) }
const ddl = tables
  .map((name) => `CREATE TABLE ${name} (id int PRIMARY KEY, n int, label text); ALTER TABLE ${name} REPLICA IDENTITY FULL;`)
  .join('\n')

type Message = {
  key?: string
  value?: Record<string, unknown>
  headers: { operation?: string; control?: string }
}

/** An Electric client reduced to its protocol: handle + offset + the rows it holds. */
class Client {
  rows = new Map<string, Record<string, unknown>>()
  handle = ''
  offset = '-1'

  constructor(
    private readonly h: Harness,
    private readonly table: string,
    private readonly where?: string,
  ) {}

  url(params: { handle?: string; offset?: string; where?: string | null; live?: boolean } = {}): string {
    const query = new URLSearchParams({ table: this.table, offset: params.offset ?? this.offset })
    const handle = params.handle ?? this.handle
    if (handle) query.set('handle', handle)
    const where = params.where === undefined ? this.where : params.where
    if (where) query.set('where', where)
    if (params.live) query.set('live', 'true')
    return `${this.h.engineUrl}/v1/shape?${query}`
  }

  apply(messages: Message[]): void {
    for (const message of messages) {
      const key = message.key
      if (key === undefined) continue
      const operation = message.headers.operation
      if (operation === 'insert') {
        expect(this.rows.has(key), `insert of a row the client already holds: ${key}`).toBe(false)
        this.rows.set(key, message.value!)
      } else if (operation === 'update') {
        expect(this.rows.has(key), `update of a row the client does not hold: ${key}`).toBe(true)
        this.rows.set(key, { ...this.rows.get(key), ...message.value })
      } else if (operation === 'delete') {
        expect(this.rows.has(key), `delete of a row the client does not hold: ${key}`).toBe(true)
        this.rows.delete(key)
      }
    }
  }

  /** One request from the client's position. Applies a 200 and returns the response either way. */
  async poll(): Promise<Response> {
    const response = await fetch(this.url())
    if (response.status !== 200) return response
    const messages = (await response.clone().json()) as Message[]
    this.apply(messages)
    this.handle = response.headers.get('electric-handle') ?? this.handle
    this.offset = response.headers.get('electric-offset') ?? this.offset
    return response
  }

  async snapshot(): Promise<void> {
    expect((await this.poll()).status).toBe(200)
  }

  async catchUp(): Promise<void> {
    for (let page = 0; page < 100; page++) {
      const response = await this.poll()
      expect(response.status, 'catching up must not ask for a refetch').toBe(200)
      if (response.headers.has('electric-up-to-date')) return
    }
    throw new Error('never reached up-to-date')
  }

  shapeId(): string {
    return this.handle.split('h')[0]!
  }
}

async function expectMustRefetch(response: Response): Promise<void> {
  expect(response.status).toBe(409)
  expect(response.headers.get('electric-offset')).toBe('-1')
  expect(response.headers.has('electric-handle')).toBe(false)
  expect((await response.json()) as Message[]).toEqual([{ headers: { control: 'must-refetch' } }])
}

describe('conformance: Electric handles survive an engine restart', () => {
  let h: Harness
  const pg = (sql: string, params: unknown[] = []) => pgQuery(h, sql, params)

  /** The rows of `name` according to Postgres, in the wire's text form. */
  async function sqlRows(name: string): Promise<Map<string, Record<string, unknown>>> {
    const rows = await pg(`SELECT id::text AS id, n::text AS n, label FROM ${name} ORDER BY id`)
    return new Map(rows.map((row) => [String(row.id), row as Record<string, unknown>]))
  }

  async function subscriptions(shapeId: string): Promise<number> {
    const response = await fetch(`${h.engineUrl}/shapes/${encodeURIComponent(shapeId)}`)
    expect(response.status).toBe(200)
    return ((await response.json()) as { subscriptions: number }).subscriptions
  }

  /** Every claim the durable catalog has recorded on `shapeId`, in order, renewals included. */
  async function claimsInCatalog(shapeId: string): Promise<string[]> {
    const claims: string[] = []
    let offset = '-1'
    for (let page = 0; page < 100; page++) {
      const response = await fetch(`${h.dsUrl}/${testPhysicalPath('meta/catalog')}?offset=${offset}`)
      expect(response.ok).toBe(true)
      const events = (await response.json()) as Array<{
        t: string
        id?: string
        rec?: { id: string }
        subscription?: string
      }>
      for (const event of events) {
        const claimed = event.t === 'created' ? event.rec?.id : event.t === 'joined' ? event.id : undefined
        if (claimed === shapeId) claims.push(event.subscription!)
      }
      const next = response.headers.get('stream-next-offset')
      if (!next || next === offset || response.headers.has('stream-up-to-date')) break
      offset = next
    }
    return claims
  }

  beforeAll(async () => {
    h = await bootHarness(schema, { ddl })
  }, 60_000)

  afterAll(async () => {
    await h?.shutdown()
  })

  it('a client resumes with no refetch and ends with the rows Postgres has', async () => {
    await pg(`INSERT INTO resumed VALUES (1, 10, 'one'), (2, 20, 'two'), (3, 30, 'three')`)
    await drainEngine(h)
    const client = new Client(h, 'resumed')
    await client.snapshot()

    // Move past the snapshot's tail, so the client's position is not the first thing in the stream.
    await pg(`INSERT INTO resumed VALUES (4, 40, 'four')`)
    await pg(`UPDATE resumed SET n = 11 WHERE id = 1`)
    await drainEngine(h)
    await client.catchUp()
    const { handle } = client

    await h.restartEngine()

    // All of this lands BEFORE the client returns, past its offset: the replayed window. A key set
    // rebuilt at the stream's tail instead of the client's offset loses the delete of 2 for ever.
    await pg(`DELETE FROM resumed WHERE id = 2`)
    await pg(`UPDATE resumed SET n = 41 WHERE id = 4`)
    await pg(`INSERT INTO resumed VALUES (5, 50, 'five')`)
    await pg(`DELETE FROM resumed WHERE id = 3`)
    await pg(`INSERT INTO resumed VALUES (3, 31, 'three again')`)
    await drainEngine(h)

    const resumed = await client.poll()
    expect(resumed.status, 'the old handle must resume, not refetch').toBe(200)
    expect(resumed.headers.get('electric-handle')).toBe(handle)
    await client.catchUp()
    expect(client.rows).toEqual(await sqlRows('resumed'))

    // ...and it is a live handle again: a later change reaches it.
    await pg(`INSERT INTO resumed VALUES (6, 60, 'six')`)
    await pg(`DELETE FROM resumed WHERE id = 1`)
    await drainEngine(h)
    await client.catchUp()
    expect(client.rows).toEqual(await sqlRows('resumed'))
  }, 60_000)

  it('the official Electric client crosses a restart with no 409 and no second snapshot', async () => {
    await pg(`INSERT INTO official VALUES (1, 10, 'one'), (2, 20, 'two')`)
    await drainEngine(h)

    const requests: Array<{ status: number | 'failed'; offset: string | null }> = []
    const abort = new AbortController()
    const firstUrl = h.engineUrl
    const held = new Map<string, unknown>()
    const stream = new ShapeStream<ElectricRow>({
      url: `${firstUrl}/v1/shape`,
      params: { table: 'official' },
      signal: abort.signal,
      backoffOptions: { initialDelay: 50, maxDelay: 200, multiplier: 1.3 },
      // The engine comes back on another port; a load balancer would hide that from the client.
      fetchClient: async (input, init) => {
        const url = String(input instanceof Request ? input.url : input).replace(firstUrl, h.engineUrl)
        const offset = new URL(url).searchParams.get('offset')
        try {
          const response = await fetch(url, init)
          requests.push({ status: response.status, offset })
          return response
        } catch (error) {
          requests.push({ status: 'failed', offset })
          throw error
        }
      },
      onError: () => {},
    })
    const unsubscribe = stream.subscribe((messages) => {
      for (const message of messages) {
        if (!isChangeMessage(message)) continue
        if (message.headers.operation === 'delete') held.delete(message.key)
        else held.set(message.key, { ...(held.get(message.key) as object | undefined), ...message.value })
      }
    })
    try {
      await waitFor(async () => held.size === 2, 'the first snapshot')
      await h.restartEngine()
      await pg(`DELETE FROM official WHERE id = 2`)
      await pg(`INSERT INTO official VALUES (3, 30, 'three')`)
      await drainEngine(h)
      await waitFor(async () => held.has('3') && !held.has('2'), 'the resumed stream to deliver the later changes')

      expect(requests.filter((request) => request.status === 409)).toEqual([])
      expect(requests.filter((request) => request.offset === '-1')).toHaveLength(1)
      expect([...held.keys()].sort()).toEqual([...(await sqlRows('official')).keys()].sort())
    } finally {
      unsubscribe()
      abort.abort()
    }
  }, 60_000)

  it('a shape retired while the engine was down answers 409, and the refetch works', async () => {
    await pg(`INSERT INTO retired VALUES (1, 10, 'one')`)
    await drainEngine(h)
    const client = new Client(h, 'retired')
    await client.snapshot()

    await h.restartEngine(async () => {
      await pg(`ALTER TABLE retired ADD COLUMN extra text`)
    })

    await expectMustRefetch(await fetch(client.url()))
    const fresh = new Client(h, 'retired')
    await fresh.snapshot()
    expect(fresh.shapeId()).not.toBe(client.shapeId())
    expect([...fresh.rows.keys()]).toEqual(['1'])
    // The refused resume resolved the definition and gave its claim back: only the refetch holds it.
    expect(await subscriptions(fresh.shapeId())).toBe(1)
  }, 60_000)

  it('the same handle with a different where answers 409 and costs the real handle nothing', async () => {
    await pg(`INSERT INTO filtered VALUES (1, 10, 'kept'), (2, 20, 'other')`)
    await drainEngine(h)
    const client = new Client(h, 'filtered', `label = 'kept'`)
    await client.snapshot()
    await h.restartEngine()

    await expectMustRefetch(await fetch(client.url({ where: `label = 'other'` })))
    await expectMustRefetch(await fetch(client.url({ where: null })))
    expect(await subscriptions(client.shapeId())).toBe(1)

    await pg(`INSERT INTO filtered VALUES (3, 30, 'kept'), (4, 40, 'other')`)
    await drainEngine(h)
    await client.catchUp()
    expect([...client.rows.keys()].sort()).toEqual(['1', '3'])
  }, 60_000)

  it('two concurrent resumes of one handle end with one handle and one claim', async () => {
    await pg(`INSERT INTO raced VALUES (1, 10, 'one')`)
    await drainEngine(h)
    const client = new Client(h, 'raced')
    await client.snapshot()
    await h.restartEngine()
    await pg(`INSERT INTO raced VALUES (2, 20, 'two')`)
    await pg(`DELETE FROM raced WHERE id = 1`)
    await drainEngine(h)

    const [first, second] = await Promise.all([fetch(client.url()), fetch(client.url())])
    expect([first.status, second.status]).toEqual([200, 200])
    expect(first.headers.get('electric-handle')).toBe(client.handle)
    expect(second.headers.get('electric-handle')).toBe(client.handle)
    // Identical requests get identical changes: neither sees the other's key-set state.
    const [a, b] = (await Promise.all([first.json(), second.json()])) as [Message[], Message[]]
    expect(a).toEqual(b)
    client.apply(a)
    client.offset = first.headers.get('electric-offset')!
    await client.catchUp()
    expect(client.rows).toEqual(await sqlRows('raced'))

    expect(await subscriptions(client.shapeId())).toBe(1)
  }, 60_000)

  it('a handle in the old format never serves rows from a stream it cannot prove', async () => {
    await pg(`INSERT INTO legacy VALUES (1, 10, 'one')`)
    await drainEngine(h)
    const client = new Client(h, 'legacy')
    await client.snapshot()
    const legacy = `${client.shapeId()}h1`
    await h.restartEngine()
    await expectMustRefetch(await fetch(client.url({ handle: legacy })))
    expect(await subscriptions(client.shapeId())).toBe(1)
  }, 60_000)

  it('the claim carried in the handle is renewed, never duplicated', async () => {
    await pg(`INSERT INTO claimed VALUES (1, 10, 'one')`)
    await drainEngine(h)
    const client = new Client(h, 'claimed')
    await client.snapshot()
    const before = await claimsInCatalog(client.shapeId())
    expect(before).toHaveLength(1)

    await h.restartEngine()
    const resumed = await client.poll()
    expect(resumed.status, 'the old handle must resume, not refetch').toBe(200)

    // The resume's `Joined` is queued, not awaited: wait for it, then read what it names.
    await waitFor(async () => (await claimsInCatalog(client.shapeId())).length > before.length, 'the renewal record')
    expect(new Set(await claimsInCatalog(client.shapeId()))).toEqual(new Set(before))
    expect(await subscriptions(client.shapeId())).toBe(1)
  }, 60_000)

  it('a handle minted after a restart never equals one from before it', async () => {
    // A fresh process's first snapshot on each side of the restart: the old counter gave both `h1`.
    await h.restartEngine()
    const before = new Client(h, 'minted')
    await before.snapshot()
    await h.restartEngine()
    const after = new Client(h, 'minted')
    await after.snapshot()
    expect(after.shapeId()).toBe(before.shapeId())
    expect(after.handle).not.toBe(before.handle)
  }, 60_000)

  it('a position the stream does not have answers 409, and the same handle then resumes', async () => {
    await pg(`INSERT INTO positions VALUES (1, 10, 'one'), (2, 20, 'two')`)
    await drainEngine(h)
    const client = new Client(h, 'positions')
    await client.snapshot()
    const tail = client.offset
    await h.restartEngine()

    const [seq, bytes] = tail.split('_') as [string, string]
    const inside = `${seq}_${String(Number(bytes) - 1).padStart(bytes.length, '0')}`
    const past = `${seq}_${String(Number(bytes) + 1).padStart(bytes.length, '0')}`
    for (const offset of [past, inside, 'not-an-offset']) {
      await expectMustRefetch(await fetch(client.url({ offset })))
    }

    // Refusing a bad position gave the claim back; the client's own position still resumes.
    const resumed = await client.poll()
    expect(resumed.status, 'the old handle must resume, not refetch').toBe(200)
    expect(await subscriptions(client.shapeId())).toBe(1)
    await pg(`INSERT INTO positions VALUES (3, 30, 'three')`)
    await drainEngine(h)
    await client.catchUp()
    expect(client.rows).toEqual(await sqlRows('positions'))
  }, 60_000)
})
