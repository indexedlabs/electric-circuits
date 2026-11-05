// The engine keeps every stream it tracks inside its sliding `Stream-TTL` (OTTO-12064, the
// deployment spec's "Stream expiry" section) — end to end against the live engine, real Postgres and
// the real durable-streams server, whose own clock is what expires a stream.
//
//   1. After a restart, the first keep-alive sweep (its own timer, at the retention sweep interval)
//      reads every cataloged stream nothing has read since the boot at its tail, at once
//      rather than a quarter TTL after the boot, and a shape stream nothing else reads or appends to
//      outlives three TTLs (`#v-keep-alive`, `#g-tracked-streams-kept`, `#c-counted-touch`).
//   2. The same holds for a group's stream and an older change-log segment a consumer still pins
//      (`#g-tracked-streams-kept`).
//   3. A failed touch is counted, the untouched-age gauge (the age at the latest finished sweep plus
//      the time since) grows past its healthy bound, and the next sweep touches the stream once
//      storage answers again (`#v-touch-retry`, `#c-sweep-inside-ttl`).
//
// Second-scale knobs: the floor is 2 × max(1 + 2, 2) = 6 s, and the sweep (1 s) runs well inside a
// quarter of each TTL. Production runs a 21-day TTL with a 60 s sweep.

import { createServer, request } from 'node:http'
import pgpkg from 'pg'
import type { Schema } from '@electric-circuits/protocol'
import { afterEach, describe, expect, it } from 'vitest'
import { bootHarness, engineChangesSegment, type Harness } from './harness.js'
import { testPhysicalPath } from './ds-mtls-access.js'

const schema: Schema = {
  tables: {
    items: { columns: { id: { type: 'int' }, n: { type: 'int' } }, primaryKey: 'id' },
    // Writes that only move the change log along, so no shape or group stream sees an append.
    log: { columns: { id: { type: 'int' }, n: { type: 'int' } }, primaryKey: 'id' },
  },
}

const knobs = (ttl: number) => ({
  ELECTRIC_CIRCUITS_STREAM_TTL_SECS: String(ttl),
  ELECTRIC_CIRCUITS_RETENTION_SWEEP_SECS: '1',
  ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS: '1',
  ELECTRIC_CIRCUITS_SHAPE_DORMANT_TTL_SECS: '2',
  ELECTRIC_CIRCUITS_CHANGES_RETAIN_SECS: '2',
  // A held subscription keeps the shape active (not dormant, not evicted) for the whole test, so
  // only the keep-alive can stop its stream expiring.
  ELECTRIC_CIRCUITS_SUBSCRIPTION_LEASE_SECS: '3600',
})

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

let h: Harness | undefined
afterEach(async () => {
  await h?.shutdown()
  h = undefined
})

async function createShape(subscription: string): Promise<{ shapeId: string; streamPath: string }> {
  const res = await fetch(`${h!.engineUrl}/shapes`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ table: 'items', where: { col: 'n', op: 'gte', value: 10 }, subscription }),
  })
  if (!res.ok) throw new Error(`POST /shapes -> ${res.status} ${await res.text()}`)
  return (await res.json()) as { shapeId: string; streamPath: string }
}

/** `HEAD` straight at durable-streams: it reports the stream without sliding its TTL. */
async function head(logicalPath: string): Promise<{ status: number; ttl: string | null }> {
  const res = await fetch(`${h!.dsUrl}/${testPhysicalPath(logicalPath)}`, { method: 'HEAD' })
  return { status: res.status, ttl: res.headers.get('stream-ttl') }
}

/** One series from the engine's Prometheus scrape (summed over label sets). */
async function series(name: string): Promise<number> {
  const text = await (await fetch(`${h!.engineUrl}/metrics/prometheus`)).text()
  return text
    .split('\n')
    .filter((line) => line.startsWith(`${name}{`) || line.startsWith(`${name} `))
    .reduce((sum, line) => sum + Number(line.trim().split(/\s+/).pop()), 0)
}

async function waitFor(cond: () => Promise<boolean>, what: string, timeoutMs = 30000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (await cond()) return
    await sleep(150)
  }
  throw new Error(`timed out waiting for ${what}`)
}

interface TouchFaultProxy {
  url: string
  /** Physical paths of every keep-alive `GET` that reached durable-streams. */
  touched(): string[]
  failTouches(fail: boolean): void
  close(): Promise<void>
}

/** Record the keep-alive (`GET …?offset=now` without `live`), or refuse it with 503; pass everything else. */
async function startTouchFaultProxy(upstreamUrl: string): Promise<TouchFaultProxy> {
  let fail = false
  const touched: string[] = []
  const upstream = new URL(upstreamUrl)
  const server = createServer((incoming, outgoing) => {
    const target = new URL(incoming.url ?? '/', upstream)
    const touch =
      incoming.method === 'GET' && target.searchParams.get('offset') === 'now' && !target.searchParams.has('live')
    if (touch && !fail) touched.push(target.pathname)
    if (fail && touch) {
      incoming.resume()
      outgoing.writeHead(503, { 'content-type': 'text/plain' })
      outgoing.end('keep-alive temporarily unavailable')
      return
    }
    const forwarded = request(
      target,
      { method: incoming.method, headers: { ...incoming.headers, host: upstream.host } },
      (response) => {
        outgoing.writeHead(response.statusCode ?? 502, response.headers)
        response.pipe(outgoing)
      },
    )
    forwarded.on('error', (error) => {
      if (!outgoing.headersSent) outgoing.writeHead(502, { 'content-type': 'text/plain' })
      outgoing.end(String(error))
    })
    incoming.pipe(forwarded)
  })
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', () => resolve())
  })
  const address = server.address()
  if (!address || typeof address === 'string') throw new Error('touch fault proxy did not bind TCP')
  return {
    url: `http://127.0.0.1:${address.port}`,
    touched: () => [...touched],
    failTouches: (next) => {
      fail = next
    },
    close: () => new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve()))),
  }
}

async function pg(sql: string, params: unknown[] = []): Promise<void> {
  const c = new pgpkg.Client({ connectionString: h!.pgUrl })
  await c.connect()
  try {
    await c.query(sql, params)
  } finally {
    await c.end().catch(() => {})
  }
}

async function engineRequest(method: string, path: string, body: unknown): Promise<{ streamPath?: string }> {
  const res = await fetch(`${h!.engineUrl}${path}`, {
    method,
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!res.ok) throw new Error(`${method} ${path} -> ${res.status} ${await res.text()}`)
  return (await res.json()) as { streamPath?: string }
}

describe('conformance: the engine keeps its tracked streams inside their sliding TTL', () => {
  it('touches every cataloged stream after a restart, so an untouched shape stream outlives three TTLs', async () => {
    const ttl = 8
    h = await bootHarness(schema, { engineEnv: knobs(ttl) })
    const shape = await createShape('held')
    expect((await head(shape.streamPath)).ttl).toBe(String(ttl))

    // A restart forgets every touch: the sweeps must read the catalog and every stream it names.
    // Nothing but the keep-alive reads or appends to the shape stream from here on: no matching
    // writes, no client reads (HEAD does not slide the TTL).
    await h.restartEngine()
    await sleep(3 * ttl * 1000)
    for (const path of [shape.streamPath, 'meta/catalog', 'changes/0']) {
      expect(await head(path), `${path} is still there after three TTLs`).toEqual({ status: 200, ttl: String(ttl) })
    }
    expect(await series('engine_stream_ttl_touches_total')).toBeGreaterThanOrEqual(3)
    expect(await series('engine_stream_ttl_touch_failures_total')).toBe(0)
    // The exported age is the age at the latest sweep plus the time since: at most a quarter TTL
    // plus one sweep interval (1 s) while touches succeed.
    expect(await series('engine_stream_ttl_oldest_untouched_seconds')).toBeLessThanOrEqual(ttl / 4 + 1)
  }, 90000)

  it('keeps a group stream and a retained older change-log segment alive past three TTLs', async () => {
    const ttl = 8
    h = await bootHarness(schema, {
      engineEnv: {
        ...knobs(ttl),
        // Retain 0 never evicts a pin (and adds nothing to the floor, 2 × (1 + 2) = 6 s), so the
        // consumer pinned below keeps changes/0 for the whole test. Age-only rotation, every second.
        ELECTRIC_CIRCUITS_CHANGES_RETAIN_SECS: '0',
        ELECTRIC_CIRCUITS_CHANGES_SEGMENT_BYTES: '0',
        ELECTRIC_CIRCUITS_CHANGES_SEGMENT_SECS: '1',
      },
    })
    const group = await engineRequest('POST', '/v1/subsets/load', {
      subscription: 'held-group',
      members: [{ key: 'plain', table: 'public.items', columns: ['id', 'n'] }],
    })
    const groupPath = group.streamPath!
    await engineRequest('PUT', '/consumers/held', { segment: 0, offset: '-1' })
    let id = 1
    const deadline = Date.now() + 30000
    while (((await engineChangesSegment(h.engineUrl)) ?? 0) < 1) {
      if (Date.now() > deadline) throw new Error('timed out waiting for a change-log rotation')
      await pg('INSERT INTO log (id, n) VALUES ($1, $1)', [id++])
      await sleep(300)
    }
    for (const path of [groupPath, 'changes/0']) {
      expect(await head(path), `${path} before the restart`).toEqual({ status: 200, ttl: String(ttl) })
    }

    // After the restart nothing reads or appends to either: the group's table sees no writes, the
    // sequencer resumes in a later segment, and the consumer only pins. Only the keep-alive can.
    await h.restartEngine()
    await sleep(3 * ttl * 1000)
    for (const path of [groupPath, 'changes/0']) {
      expect(await head(path), `${path} is still there after three TTLs`).toEqual({ status: 200, ttl: String(ttl) })
    }
    expect(await series('engine_stream_ttl_touch_failures_total')).toBe(0)
  }, 120000)

  it('touches a stream the restarted engine has not read on its first sweep, not a quarter TTL later', async () => {
    // A quarter of this TTL is 10 s; the sweep runs every second. The held shape's stream, which
    // nothing reads or appends to after the boot, must be touched by the first sweep, because
    // before the boot the engine may have left it untouched for most of a TTL.
    const ttl = 40
    let proxy: TouchFaultProxy | undefined
    h = await bootHarness(schema, {
      engineEnv: knobs(ttl),
      wrapEngineDs: async (upstreamUrl) => {
        proxy = await startTouchFaultProxy(upstreamUrl)
        return proxy
      },
    })
    const shape = await createShape('held')
    // The baseline comes before the restart: the new process's first sweep can land before
    // `restartEngine` resolves, and the old one has no reason to touch a stream it just created.
    const restartedAt = proxy!.touched().length
    await h.restartEngine()
    const shapePath = `/${testPhysicalPath(shape.streamPath)}`
    await waitFor(
      () => Promise.resolve(proxy!.touched().slice(restartedAt).includes(shapePath)),
      "the first sweep after boot to touch the held shape's stream",
      3000,
    )
    expect(await series('engine_stream_ttl_touch_failures_total')).toBe(0)
  }, 60000)

  it('counts a failed touch, keeps the stream due, and touches it on the next sweep', async () => {
    const ttl = 20
    let proxy: TouchFaultProxy | undefined
    h = await bootHarness(schema, {
      engineEnv: knobs(ttl),
      wrapEngineDs: async (upstreamUrl) => {
        proxy = await startTouchFaultProxy(upstreamUrl)
        return proxy
      },
    })
    proxy!.failTouches(true)
    const shape = await createShape('held')

    await waitFor(async () => (await series('engine_stream_ttl_touch_failures_total')) >= 1, 'a failed touch')
    // Healthy, the age stays within a quarter TTL plus one sweep interval. Keep the fault on until
    // it is past that bound, which only touches failing sweep after sweep can produce.
    const healthyBound = ttl / 4 + 1
    await waitFor(
      async () => (await series('engine_stream_ttl_oldest_untouched_seconds')) > healthyBound,
      'the untouched age to grow past the healthy bound while touches fail',
    )
    const failures = await series('engine_stream_ttl_touch_failures_total')
    await waitFor(
      async () => (await series('engine_stream_ttl_touch_failures_total')) > failures,
      'the next sweep to try again',
    )

    const touched = await series('engine_stream_ttl_touches_total')
    proxy!.failTouches(false)
    await waitFor(async () => (await series('engine_stream_ttl_touches_total')) > touched, 'a touch once storage answers')
    await waitFor(
      async () => (await series('engine_stream_ttl_oldest_untouched_seconds')) <= healthyBound,
      'every tracked stream touched again',
    )
    expect(await head(shape.streamPath)).toEqual({ status: 200, ttl: String(ttl) })
  }, 90000)
})
