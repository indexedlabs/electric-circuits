// Restart under client traffic, with restore held at a real storage HEAD. No sleeps or
// production test hooks: the proxy releases restore only after the boot requests complete.
import { createServer, request } from 'node:http'
import type { Schema } from '@electric-circuits/protocol'
import { expect, it } from 'vitest'
import { createShape, foldStream, pgQuery } from './engine-native.js'
import { bootHarness, drainEngine, positionReached, type Harness, type LogPosition } from './harness.js'
import { testPhysicalPath } from './ds-mtls-access.js'

function deferred() {
  let resolve!: () => void
  const promise = new Promise<void>((done) => { resolve = done })
  return { promise, resolve }
}

async function deadline<T>(promise: Promise<T>, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([promise, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`deadline: ${label}`)), 10000)
    })])
  } finally {
    clearTimeout(timer)
  }
}

async function restoreGate(upstreamUrl: string) {
  const arrived = deferred()
  const released = deferred()
  let armed = false
  const upstream = new URL(upstreamUrl)
  const server = createServer((incoming, outgoing) => {
    void (async () => {
      const target = new URL(incoming.url ?? '/', upstream)
      if (armed && incoming.method === 'HEAD' && target.pathname.startsWith(`/${testPhysicalPath('shape/')}`)) {
        arrived.resolve()
        await released.promise
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
  if (!address || typeof address === 'string') throw new Error('restore gate did not bind TCP')
  return {
    url: `http://127.0.0.1:${address.port}`,
    arm: () => { armed = true },
    held: () => deadline(arrived.promise, 'restore HEAD arrived'),
    release: () => { armed = false; released.resolve() },
    close: async () => {
      released.resolve()
      server.closeAllConnections()
      await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()))
    },
  }
}

async function checkpoints(dsUrl: string): Promise<LogPosition[]> {
  const positions: LogPosition[] = []
  let offset = '-1'
  for (;;) {
    const response = await fetch(`${dsUrl}/${testPhysicalPath('meta/catalog')}?offset=${offset}`, {
      signal: AbortSignal.timeout(10000),
    })
    if (!response.ok) throw new Error(`catalog read: ${response.status}`)
    if (response.status === 204) break
    const events = await response.json() as Array<{ t: string; pos?: LogPosition }>
    for (const event of events) if (event.t === 'offset' && event.pos) positions.push(event.pos)
    const next = response.headers.get('stream-next-offset')
    if (!next || next === offset || response.headers.has('stream-up-to-date')) break
    offset = next
  }
  return positions
}

const schema: Schema = {
  tables: { items: { columns: { id: { type: 'int' }, n: { type: 'int' } }, primaryKey: 'id' } },
}

it('restart refuses boot traffic, resumes a retained segment, drains, and never regresses its checkpoint', async () => {
  let h: Harness | undefined
  let gate: Awaited<ReturnType<typeof restoreGate>> | undefined
  try {
    h = await bootHarness(schema, {
      durableStreamsDurability: 'wal',
      wrapEngineDs: async (url) => { gate = await restoreGate(url); return gate },
      engineEnv: {
        ELECTRIC_CIRCUITS_CHANGES_SEGMENT_BYTES: '1',
        ELECTRIC_CIRCUITS_CHANGES_SEGMENT_SECS: '0',
      },
    })
    const shape = await createShape(h, { table: 'items', subscription: 'boot-race-client' })
    await pgQuery(h, 'INSERT INTO items (id, n) VALUES (1, 10)')
    await drainEngine(h)
    h.signalEngine('SIGTERM')
    expect((await h.waitForEngineExit()).code).toBe(0)
    const before = await checkpoints(h.dsUrl)
    const floor = before.at(-1)!
    expect(floor.segment, 'fixture must rotate and checkpoint past changes/0').toBeGreaterThan(0)
    const removed = await fetch(`${h.dsUrl}/${testPhysicalPath('changes/0')}`, {
      method: 'DELETE', signal: AbortSignal.timeout(10000),
    })
    expect([200, 204, 404]).toContain(removed.status)
    gate!.arm()
    await h.startEngine(async (url) => {
      await gate!.held()
      try {
        expect((await fetch(`${url}/health`, { signal: AbortSignal.timeout(10000) })).status).toBe(200)
        expect((await fetch(`${url}/ready`, { signal: AbortSignal.timeout(10000) })).status).toBe(503)
        // Fixed operation floor: keep retrying the same subscription while restore is held.
        for (let i = 0; i < 8; i++) {
          const response = await fetch(`${url}/v1/shapes`, {
            method: 'POST', headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ table: 'items', subscription: 'boot-race-client' }),
            signal: AbortSignal.timeout(10000),
          })
          expect(response.status).toBe(503)
          expect(response.headers.get('retry-after')).toBe('1')
          expect(await response.json()).toEqual({ error: 'engine is not the active ready writer; retry' })
        }
        // Reads must also refuse the incomplete restored view.
        expect((await fetch(`${url}/shapes/${shape.shapeId}`, { signal: AbortSignal.timeout(10000) })).status).toBe(503)
      } finally {
        gate!.release()
      }
    })
    for (let i = 0; i < 8; i++) {
      const joined = await createShape(h, { table: 'items', subscription: 'boot-race-client' })
      expect(joined.shapeId).toBe(shape.shapeId)
    }
    await pgQuery(h, 'UPDATE items SET n = 20 WHERE id = 1')
    await drainEngine(h)
    expect([...((await foldStream(shape.streamUrl)).values())]).toEqual(await pgQuery(h, 'SELECT * FROM items ORDER BY id'))
    h.signalEngine('SIGTERM')
    expect((await h.waitForEngineExit()).code).toBe(0)
    expect(h.engineStderr()).not.toContain('the change-log segment it is reading is GONE')
    const after = await checkpoints(h.dsUrl)
    expect(after.length).toBeGreaterThan(before.length)
    expect(positionReached(after.at(-1)!, floor)).toBe(true)
    for (let i = 1; i < after.length; i++) {
      expect(positionReached(after[i]!, after[i - 1]!), `checkpoint ${i} regressed`).toBe(true)
    }
  } finally {
    gate?.release()
    await h?.shutdown()
  }
}, 60000)
