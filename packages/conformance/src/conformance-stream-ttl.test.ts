// A sliding `Stream-TTL` on every stream the engine creates (OTTO-12063, the deployment spec's
// "Stream expiry" section) — end to end against the live engine, real Postgres and the real
// durable-streams server.
//
//   1. With `ELECTRIC_CIRCUITS_STREAM_TTL_SECS` set, a new shape stream, a group's shared stream, the
//      change-log segments and `meta/catalog` each answer `HEAD` with that `Stream-TTL`
//      (`#v-ttl-on-create`).
//   2. Streams created before the TTL was set keep their configuration, and the engine still opens
//      them: it boots over them, and a restored shape keeps receiving writes (`#v-ttl-on-create`,
//      `#c-old-stream-config`).
//   3. A TTL under the floor refuses the boot with exit 78 (`#v-ttl-floor`).
//
// Second-scale retention knobs keep the floor (twice the larger of idle-plus-dormancy and the
// change log's retain window) small enough for a test to clear; production's defaults make it
// 14.5 days.

import pgpkg from 'pg'
import type { Row, Schema } from '@electric-circuits/protocol'
import { afterEach, describe, expect, it } from 'vitest'
import { bootHarness, buildEngine, drainEngine, engineChangesSegment, spawnRawEngine, type Harness, type RawEngine } from './harness.js'
import { testPhysicalPath } from './ds-mtls-access.js'

const schema: Schema = {
  tables: { items: { columns: { id: { type: 'int' }, n: { type: 'int' } }, primaryKey: 'id' } },
}

const TTL = '86400'

/** Floor = 2 × max(1 + 3600, 4) = 7202 s. */
const knobs = {
  ELECTRIC_CIRCUITS_CHANGES_SEGMENT_BYTES: '0',
  ELECTRIC_CIRCUITS_CHANGES_SEGMENT_SECS: '1',
  ELECTRIC_CIRCUITS_CHANGES_RETAIN_SECS: '4',
  ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS: '1',
  ELECTRIC_CIRCUITS_SHAPE_DORMANT_TTL_SECS: '3600',
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

let h: Harness | undefined
let raw: RawEngine | undefined
afterEach(async () => {
  raw?.proc.kill('SIGKILL')
  raw = undefined
  await h?.shutdown()
  h = undefined
})

async function pg(sql: string, params: unknown[] = []): Promise<Row[]> {
  const c = new pgpkg.Client({ connectionString: h!.pgUrl })
  await c.connect()
  try {
    return (await c.query(sql, params)).rows as Row[]
  } finally {
    await c.end().catch(() => {})
  }
}

async function post<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${h!.engineUrl}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!res.ok) throw new Error(`POST ${path} -> ${res.status} ${await res.text()}`)
  return (await res.json()) as T
}

const createShape = (where: unknown) => post<{ shapeId: string; streamPath: string }>('/shapes', { table: 'items', where })

/** The `Stream-TTL` a stream answers `HEAD` with, read straight from durable-streams. */
async function streamTtl(logicalPath: string): Promise<string | null> {
  const res = await fetch(`${h!.dsUrl}/${testPhysicalPath(logicalPath)}`, { method: 'HEAD' })
  if (res.status !== 200) throw new Error(`HEAD ${logicalPath} -> ${res.status}`)
  return res.headers.get('stream-ttl')
}

/** The keys of a shape stream, folded from raw durable-streams reads. */
async function streamKeys(logicalPath: string): Promise<Set<string>> {
  const keys = new Set<string>()
  const url = `${h!.dsUrl}/${testPhysicalPath(logicalPath)}`
  let offset = '-1'
  for (let i = 0; i < 200; i++) {
    const res = await fetch(`${url}?offset=${encodeURIComponent(offset)}`)
    if (res.status === 204) break
    if (!res.ok) throw new Error(`GET ${logicalPath} -> ${res.status}`)
    const body = (await res.text()).trim()
    for (const env of body ? (JSON.parse(body) as { key: string; headers: { operation?: string } }[]) : []) {
      if (env.headers.operation === 'delete') keys.delete(env.key)
      else keys.add(env.key)
    }
    const next = res.headers.get('stream-next-offset')
    if (!next || next === offset || res.headers.get('stream-up-to-date') !== null) break
    offset = next
  }
  return keys
}

async function waitFor(cond: () => Promise<boolean>, what: string, timeoutMs = 30000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (await cond()) return
    await sleep(150)
  }
  throw new Error(`timed out waiting for ${what}`)
}

describe('conformance: engine streams carry a sliding Stream-TTL', () => {
  it('creates shape, group, change-log and catalog streams with the configured TTL', async () => {
    h = await bootHarness(schema, { engineEnv: { ...knobs, ELECTRIC_CIRCUITS_STREAM_TTL_SECS: TTL } })
    await pg('INSERT INTO items (id, n) VALUES (1, 10)')
    await drainEngine(h)

    const shape = await createShape({ col: 'n', op: 'gte', value: 10 })
    const group = await post<{ streamPath: string }>('/v1/subsets/load', {
      subscription: 'ttl-group',
      members: [{ key: 'plain', table: 'public.items', columns: ['id', 'n'] }],
    })
    // A rotation creates a fresh segment through the same create.
    let id = 2
    await waitFor(async () => {
      await pg('INSERT INTO items (id, n) VALUES ($1, $2)', [id, id * 10])
      id += 1
      await sleep(300)
      return ((await engineChangesSegment(h!.engineUrl)) ?? 0) >= 1
    }, 'a change-log rotation')

    expect(await streamTtl(shape.streamPath), 'shape stream').toBe(TTL)
    expect(await streamTtl(group.streamPath), 'group stream').toBe(TTL)
    expect(await streamTtl('changes/1'), 'rotated change-log segment').toBe(TTL)
    expect(await streamTtl('meta/catalog'), 'catalog').toBe(TTL)
  }, 60000)

  it('still opens streams created before the TTL was set, and creates new ones with it', async () => {
    // `bootHarness` reads `engineEnv` at every engine start, so setting the TTL here and restarting
    // is the deploy that turns it on over an existing store.
    const env: Record<string, string> = { ...knobs, ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS: '3600' }
    h = await bootHarness(schema, { engineEnv: env })
    await pg('INSERT INTO items (id, n) VALUES (1, 10)')
    await drainEngine(h)
    const before = await createShape({ col: 'n', op: 'gte', value: 10 })
    expect(await streamTtl('meta/catalog')).toBeNull()
    expect(await streamTtl('changes/0')).toBeNull()
    expect(await streamTtl(before.streamPath)).toBeNull()

    env.ELECTRIC_CIRCUITS_STREAM_TTL_SECS = TTL
    await h.restartEngine()

    // The old streams keep their configuration...
    expect(await streamTtl('meta/catalog')).toBeNull()
    expect(await streamTtl(before.streamPath)).toBeNull()
    // ...and stay in use: the restored shape receives a post-restart write.
    await pg('INSERT INTO items (id, n) VALUES (2, 20)')
    await drainEngine(h)
    await waitFor(async () => (await streamKeys(before.streamPath)).size === 2, 'a post-restart write on the old shape stream')
    // A shape created now gets the TTL.
    const after = await createShape({ col: 'n', op: 'gte', value: 20 })
    expect(await streamTtl(after.streamPath)).toBe(TTL)
  }, 60000)

  it('refuses to start with exit 78 when the TTL is under the floor', async () => {
    buildEngine()
    // Default windows: 2 × max(6 h + 7 d, 7 d) = 1,252,800 s (14.5 days).
    raw = spawnRawEngine({ ELECTRIC_CIRCUITS_STREAM_TTL_SECS: '1252799' })
    const exit = await raw.waitForExit(20000)
    expect(exit.code, `expected EX_CONFIG (78), got ${JSON.stringify(exit)}\n${raw.stderr()}`).toBe(78)
    expect(raw.stderr()).toContain('boot refused')
    expect(raw.stderr()).toContain('ELECTRIC_CIRCUITS_STREAM_TTL_SECS')
  })
})
