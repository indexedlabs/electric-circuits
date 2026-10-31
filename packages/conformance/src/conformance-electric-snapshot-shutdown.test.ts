// `SIGTERM` while a `/v1/shape` `offset=-1` request is waiting on its shared snapshot build
// (OTTO-11109). The standalone engine's graceful stop waits for every in-flight request, and Engine
// close — the only thing that would end the build — runs only after that; so a request that kept
// waiting would hold the process until the watchdog killed it (exit 70, catalog undrained). The
// request must instead answer `503` with `Retry-After` at the flip, and the engine exit 0 inside its
// grace, with and without the memory budget.
//
// The build's read of the shape stream from its beginning is held by a proxy in front of
// durable-streams (engine side only) for the whole test, so the build can never finish on its own.
import { createServer, request } from 'node:http'
import type { Schema } from '@electric-circuits/protocol'
import { afterEach, describe, expect, it } from 'vitest'
import { testPhysicalPath } from './ds-mtls-access.js'
import { bootHarness, type Harness } from './harness.js'

const schema: Schema = {
  tables: {
    items: { columns: { id: { type: 'int' }, label: { type: 'text' } }, primaryKey: 'id' },
  },
}

/** The engine's default `ELECTRIC_CIRCUITS_SHUTDOWN_GRACE_SECS`. */
const GRACE_MS = 25_000

/** Holds every `offset=-1` catch-up read of a shape stream while armed; `held` resolves on the first. */
async function foldHold(upstreamUrl: string) {
  const upstream = new URL(upstreamUrl)
  const shapePrefix = `/${testPhysicalPath('shape/')}`
  let armed = false
  let arrived!: () => void
  const held = new Promise<void>((resolve) => { arrived = resolve })
  let release!: () => void
  const released = new Promise<void>((resolve) => { release = resolve })
  const server = createServer((incoming, outgoing) => {
    void (async () => {
      const target = new URL(incoming.url ?? '/', upstream)
      if (
        armed && incoming.method === 'GET' && target.pathname.startsWith(shapePrefix)
        && target.searchParams.get('offset') === '-1' && !target.searchParams.has('live')
      ) {
        arrived()
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
  if (!address || typeof address === 'string') throw new Error('fold hold did not bind TCP')
  return {
    url: `http://127.0.0.1:${address.port}`,
    arm: () => { armed = true },
    held: () => Promise.race([
      held,
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error('deadline: build read held')), 30_000)),
    ]),
    close: async () => {
      release()
      server.closeAllConnections()
      await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()))
    },
  }
}

let h: Harness | undefined
afterEach(async () => {
  await h?.shutdown()
  h = undefined
})

describe('conformance: SIGTERM with a /v1/shape snapshot waiting on its build', () => {
  for (const [mode, engineEnv] of [
    ['unbudgeted', {}],
    ['memory budget', { ELECTRIC_CIRCUITS_MEMORY_BUDGET_BYTES: String(1 << 30) }],
  ] as const) {
    it(`answers 503 Retry-After and exits 0 inside the grace (${mode})`, async () => {
      let hold: Awaited<ReturnType<typeof foldHold>> | undefined
      h = await bootHarness(schema, {
        engineEnv,
        wrapEngineDs: async (url) => {
          hold = await foldHold(url)
          return hold
        },
      })
      hold!.arm()
      const snapshot = fetch(`${h.engineUrl}/v1/shape?table=items&offset=-1`)
      await hold!.held()

      const signalled = Date.now()
      h.signalEngine('SIGTERM')
      const res = await snapshot
      expect(res.status, await res.text()).toBe(503)
      expect(res.headers.get('retry-after')).toBe('1')

      const exit = await h.waitForEngineExit(GRACE_MS + 10_000)
      expect(exit.code, 'a snapshot waiting on its build must not hold the graceful stop').toBe(0)
      expect(Date.now() - signalled).toBeLessThan(GRACE_MS)
    }, 120_000)
  }
})
