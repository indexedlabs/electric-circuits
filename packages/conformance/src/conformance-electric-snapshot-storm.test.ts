// A reconnect storm on `GET /v1/shape?offset=-1` (OTTO-11109, #v-shared-first-response).
//
// After an engine restart every Electric client's handle is gone, so all of them re-request
// `offset=-1` within seconds; the shapes themselves were restored. Each shape's stream must be
// folded once for the whole storm, the clients that disconnect must not fail the others — including
// the client whose request started the shared build — and every remaining client must end with
// exactly PostgreSQL's rows, be sent the same bytes as every other client of its shape, and then
// catch up on later writes through its own handle and key set.
//
// Order comes from a proxy in front of durable-streams (engine side only) that holds every
// catch-up read of a shape stream from its beginning — one per fold — until it is released, and
// counts them. No sleeps: each phase waits on that gate or on the engine's public state.
import { createServer, request } from 'node:http'
import type { Schema } from '@electric-circuits/protocol'
import pgpkg from 'pg'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { testPhysicalPath } from './ds-mtls-access.js'
import { bootHarness, drainEngine, type Harness } from './harness.js'

const SHAPES = 60
// Per shape: the clients sent first, which start every build and then disconnect, and the clients
// sent while those builds are held, which must be served by them.
const LEAVING_PER_SHAPE = 7
const STAYING_PER_SHAPE = 13
const ROWS = 1200

const schema: Schema = {
  tables: {
    storm_items: {
      columns: {
        id: { type: 'int' },
        grp: { type: 'int' },
        label: { type: 'text' },
        flag: { type: 'bool' },
      },
      primaryKey: 'id',
    },
  },
}

type Row = Record<string, string | null>
type Message = { headers: { operation?: string; control?: string }; key?: string; value?: Row }

// Independently authored: what PostgreSQL holds for one shape, as Electric text values.
async function sourceRows(pg: pgpkg.Client, grp: number): Promise<Map<string, Row>> {
  const res = await pg.query<{ id: string; grp: string; label: string | null; flag: string | null }>(
    'SELECT id::text AS id, grp::text AS grp, label, flag::text AS flag FROM storm_items WHERE grp = $1',
    [grp],
  )
  return new Map(res.rows.map((r) => [r.id, { id: r.id, grp: r.grp, label: r.label, flag: r.flag }]))
}

function shapeUrl(h: Harness, grp: number, extra = ''): string {
  return `${h.engineUrl}/v1/shape?table=storm_items&where=${encodeURIComponent(`grp = ${grp}`)}${extra}`
}

type SnapshotCounters = { builds: number; joins: number; reuses: number }

async function snapshotCounters(h: Harness): Promise<SnapshotCounters> {
  const res = await fetch(`${h.engineUrl}/metrics`)
  if (!res.ok) throw new Error(`GET /metrics -> ${res.status}`)
  const counters = ((await res.json()) as { counters: Record<string, number> }).counters
  return {
    builds: counters.electric_snapshot_builds_total ?? 0,
    joins: counters.electric_snapshot_joins_total ?? 0,
    reuses: counters.electric_snapshot_reuses_total ?? 0,
  }
}

async function within<T>(label: string, wait: () => Promise<T | undefined>): Promise<T> {
  const deadline = Date.now() + 60_000
  for (;;) {
    const value = await wait()
    if (value !== undefined) return value
    if (Date.now() > deadline) throw new Error(`deadline: ${label}`)
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
}

/** Holds and counts the engine's `offset=-1` catch-up reads of shape streams while armed. */
async function foldGate(upstreamUrl: string) {
  const upstream = new URL(upstreamUrl)
  const shapePrefix = `/${testPhysicalPath('shape/')}`
  let armed = false
  const folds: string[] = []
  let release!: () => void
  let released = new Promise<void>((resolve) => { release = resolve })
  const server = createServer((incoming, outgoing) => {
    void (async () => {
      const target = new URL(incoming.url ?? '/', upstream)
      if (
        armed && incoming.method === 'GET' && target.pathname.startsWith(shapePrefix)
        && target.searchParams.get('offset') === '-1' && !target.searchParams.has('live')
      ) {
        folds.push(target.pathname)
        await released
      }
      const forwarded = request(target, {
        method: incoming.method, headers: { ...incoming.headers, host: upstream.host },
      }, (response) => {
        outgoing.writeHead(response.statusCode ?? 502, response.headers)
        response.pipe(outgoing)
      })
      forwarded.on('error', (error) => {
        if (!outgoing.headersSent) outgoing.writeHead(502)
        outgoing.end(String(error))
      })
      outgoing.on('close', () => forwarded.destroy())
      incoming.pipe(forwarded)
    })().catch((error: unknown) => {
      if (!outgoing.headersSent) outgoing.writeHead(502)
      outgoing.end(String(error))
    })
  })
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', resolve)
  })
  const address = server.address()
  if (!address || typeof address === 'string') throw new Error('fold gate did not bind TCP')
  return {
    url: `http://127.0.0.1:${address.port}`,
    arm: () => {
      armed = true
      folds.length = 0
      released = new Promise<void>((resolve) => { release = resolve })
    },
    folds: () => [...folds],
    release: () => { armed = false; release() },
    close: async () => {
      release()
      server.closeAllConnections()
      await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()))
    },
  }
}

type Client = { grp: number; handle: string; offset: string; rows: Map<string, Row> }

// Apply Electric change messages the way Electric's client does: insert-of-existing and
// update/delete-of-missing are protocol violations.
function apply(client: Client, messages: Message[]): boolean {
  let upToDate = false
  for (const m of messages) {
    if (m.headers.control === 'up-to-date') upToDate = true
    const op = m.headers.operation
    if (!op || m.key === undefined) continue
    const has = client.rows.has(m.key)
    if (op === 'insert') {
      expect(has, `insert of a row the client holds: ${m.key}`).toBe(false)
      client.rows.set(m.key, m.value!)
    } else if (op === 'update') {
      expect(has, `update of a row the client lacks: ${m.key}`).toBe(true)
      client.rows.set(m.key, { ...client.rows.get(m.key)!, ...m.value! })
    } else if (op === 'delete') {
      expect(has, `delete of a row the client lacks: ${m.key}`).toBe(true)
      client.rows.delete(m.key)
    }
  }
  return upToDate
}

describe('conformance: /v1/shape reconnect storm', () => {
  let harness: Harness
  let pg: pgpkg.Client
  let gate: Awaited<ReturnType<typeof foldGate>>

  beforeAll(async () => {
    harness = await bootHarness(schema, {
      wrapEngineDs: async (url) => {
        gate = await foldGate(url)
        return gate
      },
      // The gate holds fold reads while every client joins; keep that hold inside the engine's
      // read deadline on a slow runner, so a stall fails at this test's named deadlines instead.
      engineEnv: { ELECTRIC_CIRCUITS_DS_READ_TIMEOUT_SECS: '120', ELECTRIC_CIRCUITS_DS_REQUEST_TIMEOUT_SECS: '180' },
      ddl: `
        CREATE TABLE storm_items (id int PRIMARY KEY, grp int NOT NULL, label text, flag bool);
        ALTER TABLE storm_items REPLICA IDENTITY FULL;
        INSERT INTO storm_items
          SELECT i, i % ${SHAPES},
                 CASE WHEN i % 7 = 0 THEN NULL ELSE 'item "' || i || '" \\ é' END,
                 CASE WHEN i % 5 = 0 THEN NULL ELSE i % 2 = 0 END
          FROM generate_series(1, ${ROWS}) AS i;
      `,
    })
    pg = new pgpkg.Client({ connectionString: harness.pgUrl })
    await pg.connect()
  }, 120_000)

  afterAll(async () => {
    await pg?.end()
    await harness?.shutdown()
  })

  it('serves every client of a storm PostgreSQL rows, the same bytes per shape, and its own key set', async () => {
    // The shapes exist before the storm, as they do after a restart: clients had subscribed, the
    // engine went down, and every handle went with it.
    const shapeIds = await Promise.all(
      Array.from({ length: SHAPES }, async (_, grp) => {
        const res = await fetch(shapeUrl(harness, grp, '&offset=-1'))
        expect(res.status, await res.text()).toBe(200)
        return res.headers.get('electric-handle')!.replace(/h\d+$/, '')
      }),
    )
    await harness.restartEngine()
    const claims = async () =>
      Promise.all(
        shapeIds.map(async (id) => {
          const res = await fetch(`${harness.engineUrl}/shapes/${id}`)
          expect(res.status).toBe(200)
          return ((await res.json()) as { subscriptions: number }).subscriptions
        }),
      )
    const restored = await claims()

    type Outcome = { grp: number; status: number; handle: string | null; offset: string | null; body: string }
    const snapshot = (grp: number, signal?: AbortSignal): Promise<Outcome> =>
      fetch(shapeUrl(harness, grp, '&offset=-1'), { signal }).then(async (res) => ({
        grp,
        status: res.status,
        handle: res.headers.get('electric-handle'),
        offset: res.headers.get('electric-offset'),
        body: await res.text(),
      }))
    const before = await snapshotCounters(harness)
    gate.arm()

    // 1. The clients that will leave go first, so every shape's build is started by one of them.
    const leaving = Array.from({ length: SHAPES * LEAVING_PER_SHAPE }, (_, n) => {
      const abort = new AbortController()
      return { abort, result: snapshot(n % SHAPES, abort.signal).then(() => 'answered', (e: Error) => e.name) }
    })
    await within('every shape has one build in flight', async () => {
      const folds = gate.folds()
      return new Set(folds).size === SHAPES ? folds : undefined
    })
    // 2. The clients that stay arrive while those builds are held; wait until all of them have
    //    their claim, i.e. are past create/join and waiting on the shared build.
    const staying = Array.from({ length: SHAPES * STAYING_PER_SHAPE }, (_, n) => snapshot(n % SHAPES))
    await within('every client has joined its shape', async () => {
      const now = await claims()
      return now.every((count, i) => count >= restored[i]! + LEAVING_PER_SHAPE + STAYING_PER_SHAPE) ? now : undefined
    })
    // 3. Every client that started a build disconnects while it is still held, then storage answers.
    for (const { abort } of leaving) abort.abort()
    expect(new Set(await Promise.all(leaving.map((l) => l.result))), 'the leaving clients got no answer').toEqual(
      new Set(['AbortError']),
    )
    const folds = gate.folds()
    gate.release()
    const outcomes = await Promise.all(staying)
    expect(folds.length, 'one fold per shape for the whole storm, not one per client').toBe(SHAPES)
    expect(gate.folds()).toEqual(folds)

    const bytesByTail = new Map<string, string>()
    const clients: Client[] = []
    for (const o of outcomes) {
      expect(o.status, o.body).toBe(200)
      expect(o.handle).toBeTruthy()
      expect(o.offset).toBeTruthy()
      const tail = `${o.grp}@${o.offset}`
      const seen = bytesByTail.get(tail)
      if (seen === undefined) bytesByTail.set(tail, o.body)
      else expect(o.body, `clients of shape grp=${o.grp} at one tail got different bytes`).toBe(seen)
      const client: Client = { grp: o.grp, handle: o.handle!, offset: o.offset!, rows: new Map() }
      expect(apply(client, JSON.parse(o.body) as Message[])).toBe(true)
      clients.push(client)
    }
    expect(new Set(clients.map((c) => c.handle)).size, 'every client gets its own handle').toBe(clients.length)

    // The engine's own counters agree: one build per shape, every client that stayed was answered
    // by joining one (a build's starter that left is answered by nobody).
    const after = await snapshotCounters(harness)
    const builds = after.builds - before.builds
    const shared = after.joins - before.joins + (after.reuses - before.reuses)
    expect(builds, 'snapshot builds must be one per shape and tail, not one per client').toBe(SHAPES)
    expect(shared).toBe(clients.length)
    // The same counters reach the Prometheus scrape, where a post-restart storm is watched.
    const prom = await (await fetch(`${harness.engineUrl}/metrics/prometheus`)).text()
    const scraped = (name: string) => Number(prom.match(new RegExp(`^${name}\\{[^}]*\\} (\\S+)`, 'm'))?.[1] ?? NaN)
    expect(scraped('engine_electric_snapshot_builds_total')).toBe(after.builds)
    expect(scraped('engine_electric_snapshot_joins_total')).toBe(after.joins)
    expect(scraped('engine_electric_snapshot_reuses_total')).toBe(after.reuses)

    const expected = new Map<number, Map<string, Row>>()
    for (let grp = 0; grp < SHAPES; grp++) expected.set(grp, await sourceRows(pg, grp))
    for (const client of clients) expect(client.rows).toEqual(expected.get(client.grp))

    // Later writes reach every client through its own handle: an update, an insert and a delete
    // per shape, each classified against that client's own key set.
    await pg.query(`
      UPDATE storm_items SET label = 'updated ' || id WHERE id <= ${SHAPES};
      INSERT INTO storm_items SELECT i, i % ${SHAPES}, 'new', true FROM generate_series(${ROWS + 1}, ${ROWS + SHAPES}) AS i;
      DELETE FROM storm_items WHERE id > ${SHAPES} AND id <= ${2 * SHAPES};
    `)
    await drainEngine(harness)
    for (let grp = 0; grp < SHAPES; grp++) expected.set(grp, await sourceRows(pg, grp))

    await Promise.all(
      clients.map(async (client) => {
        for (let page = 0; page < 20; page++) {
          const res = await fetch(shapeUrl(harness, client.grp, `&handle=${client.handle}&offset=${client.offset}`))
          const body = await res.text()
          expect(res.status, body).toBe(200)
          client.offset = res.headers.get('electric-offset')!
          if (apply(client, JSON.parse(body) as Message[]) || res.headers.has('electric-up-to-date')) break
        }
        expect(client.rows).toEqual(expected.get(client.grp))
      }),
    )
  }, 180_000)
})
