import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { createServer } from 'node:http'

import { describe, expect, it } from 'vitest'

import { LIVE_WORKER_JS } from './live-worker.js'

// This is the same worker that shape-mem-scale.ts launches, observed at its HTTP boundary.
// The server holds the first response until cleanup: no timer manufactures request ordering,
// and no stand-in gateway or Durable Streams implementation is under test here.
async function firstLiveRequest(mode: 'sse' | 'long-poll' | undefined): Promise<URL> {
  const server = createServer()
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('missing listener address')
  const base = `http://127.0.0.1:${address.port}`
  const deadline = AbortSignal.timeout(5000)
  const arrived = once(server, 'request', { signal: deadline })
  const env = { ...process.env }
  delete env.WORKER_LIVE_MODE
  if (mode !== undefined) env.WORKER_LIVE_MODE = mode
  const worker = spawn(process.execPath, ['-e', LIVE_WORKER_JS], {
    env,
    stdio: ['pipe', 'ignore', 'pipe'],
  })
  const exited = once(worker, 'exit')
  let stderr = ''
  worker.stderr.on('data', (chunk: Buffer) => { stderr += chunk.toString() })
  try {
    worker.stdin.end(`${base}/v1/stream/shape/test\n`)
    const [request] = await Promise.race([
      arrived,
      exited.then(([code]) => { throw new Error(`worker exited ${code}: ${stderr}`) }),
    ])
    return new URL(request.url, base)
  } finally {
    worker.kill('SIGKILL')
    await exited
    server.closeAllConnections()
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()))
  }
}

describe('per-table bench live mode', () => {
  it('requests real SSE when selected, instead of stitching long-polls together', async () => {
    const request = await firstLiveRequest('sse')
    expect(request.pathname).toBe('/v1/stream/shape/test')
    expect(request.searchParams.get('offset')).toBe('-1')
    expect(request.searchParams.get('live')).toBe('sse')
  })

  it.each([undefined, 'long-poll'] as const)('keeps long-poll for mode %s', async (mode) => {
    const request = await firstLiveRequest(mode)
    expect(request.searchParams.get('live')).toBe('long-poll')
  })
})
