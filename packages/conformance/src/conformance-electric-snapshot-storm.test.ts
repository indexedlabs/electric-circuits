// A reconnect storm on `GET /v1/shape?offset=-1` (OTTO-11109, #v-shared-first-response).
//
// After an engine restart every Electric client's handle is gone, so all of them re-request
// `offset=-1` within seconds. Many clients per shape, a third of them disconnecting mid-request,
// must each end with exactly PostgreSQL's rows, and every client of one shape at one tail must be
// sent the same bytes. Each client then catches up on later writes with its own handle and key
// set. The engine's `/metrics` counters show the storm was served by at most one build per shape
// and tail, not one per client.
import type { Schema } from '@electric-circuits/protocol'
import pgpkg from 'pg'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { bootHarness, drainEngine, type Harness } from './harness.js'

const SHAPES = 60
const CLIENTS_PER_SHAPE = 20
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

  beforeAll(async () => {
    harness = await bootHarness(schema, {
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
    type Outcome = { grp: number; status: number; handle: string | null; offset: string | null; body: string } | null
    const before = await snapshotCounters(harness)
    const requests: Promise<Outcome>[] = []
    for (let n = 0; n < SHAPES * CLIENTS_PER_SHAPE; n++) {
      const grp = n % SHAPES
      const abort = new AbortController()
      const cancels = n % 3 === 0
      if (cancels) setTimeout(() => abort.abort(), n % 20)
      requests.push(
        (async () => {
          try {
            const res = await fetch(shapeUrl(harness, grp, '&offset=-1'), { signal: abort.signal })
            const body = await res.text()
            return {
              grp,
              status: res.status,
              handle: res.headers.get('electric-handle'),
              offset: res.headers.get('electric-offset'),
              body,
            }
          } catch (error) {
            if (cancels && (error as Error).name === 'AbortError') return null
            throw error
          }
        })(),
      )
    }
    const outcomes = (await Promise.all(requests)).filter((o): o is NonNullable<Outcome> => o !== null)
    expect(outcomes.length).toBeGreaterThanOrEqual((SHAPES * CLIENTS_PER_SHAPE * 2) / 3)

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

    // Nothing was written during the storm, so every shape has one tail: at most one build each,
    // and every other answered client joined or reused one. (A client that aborted may still have
    // started or joined a build, so the served clients bound the shared answers from below.)
    const after = await snapshotCounters(harness)
    const builds = after.builds - before.builds
    const shared = after.joins - before.joins + (after.reuses - before.reuses)
    expect(builds, 'snapshot builds must be one per shape and tail, not one per client').toBeLessThanOrEqual(SHAPES)
    expect(builds + shared).toBeGreaterThanOrEqual(clients.length)
    expect(shared).toBeGreaterThanOrEqual(clients.length - SHAPES)

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
