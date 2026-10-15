// Process-level companion to the focused replay-loss tests. The fault is armed only
// after a real PostgreSQL/engine/streams boot; no engine-internal restart hook is called.
import { createServer, request } from 'node:http'
import type { Schema } from '@electric-circuits/protocol'
import { expect, it } from 'vitest'
import { testPhysicalPath } from './ds-mtls-access.js'
import { createShape, pgQuery } from './engine-native.js'
import { bootHarness, drainEngine, type Harness } from './harness.js'

const schema: Schema = {
  tables: { items: { columns: { id: { type: 'int' } }, primaryKey: 'id' } },
}

it('exits 75 when a live change-log segment is confirmed missing after boot', async () => {
  let h: Harness | undefined
  let armed = false
  let getFailed = false
  let headFailed = false
  try {
    h = await bootHarness(schema, {
      durableStreamsDurability: 'wal',
      engineEnv: {
        ELECTRIC_CIRCUITS_CHANGES_SEGMENT_BYTES: '0',
        ELECTRIC_CIRCUITS_CHANGES_SEGMENT_SECS: '0',
      },
      wrapEngineDs: async (upstreamUrl) => {
        const upstream = new URL(upstreamUrl)
        const server = createServer((incoming, outgoing) => {
          const target = new URL(incoming.url ?? '/', upstream)
          if (armed && target.pathname === `/${testPhysicalPath('changes/0')}`) {
            const liveGet = incoming.method === 'GET' && target.searchParams.has('live')
            const confirmingHead = getFailed && incoming.method === 'HEAD'
            if (liveGet || confirmingHead) {
              if (liveGet) getFailed = true
              if (confirmingHead) headFailed = true
              incoming.resume()
              outgoing.writeHead(404)
              outgoing.end()
              return
            }
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
        })
        await new Promise<void>((resolve, reject) => {
          server.once('error', reject)
          server.listen(0, '127.0.0.1', resolve)
        })
        const address = server.address()
        if (!address || typeof address === 'string') throw new Error('replay-loss proxy did not bind TCP')
        return {
          url: `http://127.0.0.1:${address.port}`,
          close: async () => {
            server.closeAllConnections()
            await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()))
          },
        }
      },
    })
    await createShape(h, { table: 'items' })
    await drainEngine(h)
    armed = true
    // Wake any GET already forwarded before arming. The next live GET fails, and its
    // confirming HEAD sees the same loss. The process exit is the terminal event.
    await pgQuery(h, 'INSERT INTO items (id) VALUES (1)')
    expect(await h.waitForEngineExit(10000)).toEqual({ code: 75, signal: null })
    expect(getFailed, 'the live-read fault must fire').toBe(true)
    expect(headFailed, 'the engine must confirm the missing segment').toBe(true)
  } finally {
    await h?.shutdown()
  }
}, 60000)
