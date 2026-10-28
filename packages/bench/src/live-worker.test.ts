import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http'

import { describe, expect, it } from 'vitest'

import { LIVE_WORKER_JS } from './live-worker.js'

interface ListenerRun {
  requests: URL[]
  worker: ReturnType<typeof spawn>
  server: ReturnType<typeof createServer>
  firstRequest: Promise<URL>
  secondRequest: Promise<URL>
}

function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (error: unknown) => void
  const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej })
  return { promise, resolve, reject }
}

async function within<T>(promise: Promise<T>, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} did not arrive within 5s`)), 5000)
      }),
    ])
  } finally {
    if (timer !== undefined) clearTimeout(timer)
  }
}

async function startListener(mode: string | undefined): Promise<ListenerRun> {
  const server = createServer()
  const requests: URL[] = []
  const first = deferred<URL>()
  const second = deferred<URL>()
  let base = ''
  server.on('request', (req: IncomingMessage, res: ServerResponse) => {
    const request = new URL(req.url ?? '/', base)
    requests.push(request)
    if (requests.length === 1) {
      first.resolve(request)
      if (mode === 'sse') {
        res.writeHead(200, { 'content-type': 'text/event-stream; charset=utf-8' })
        res.write('event: data\ndata: {"value":"first"}\n\n')
        res.write('event: control\ndata: {}\n\n')
        res.write('event: control\ndata: {"streamNextOffset":"next-1"}\n\n')
        res.end()
      }
      return
    }
    second.resolve(request)
    // Keep the second response open until cleanup. The assertion only needs request metadata.
  })
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('missing listener address')
  base = `http://127.0.0.1:${address.port}`
  const env = { ...process.env }
  delete env.WORKER_LIVE_MODE
  if (mode !== undefined) env.WORKER_LIVE_MODE = mode
  const worker = spawn(process.execPath, ['-e', LIVE_WORKER_JS], {
    env,
    stdio: ['pipe', 'ignore', 'pipe'],
  })
  worker.stdin.end(`${base}/v1/stream/shape/test\n`)
  return { requests, worker, server, firstRequest: first.promise, secondRequest: second.promise }
}

async function stopListener(run: ListenerRun): Promise<void> {
  run.worker.kill('SIGKILL')
  await once(run.worker, 'exit')
  run.server.closeAllConnections()
  await new Promise<void>((resolve, reject) => run.server.close((error) => error ? reject(error) : resolve()))
}

describe('per-table bench live mode', () => {
  it('parses SSE control frames and resumes from the committed offset', async () => {
    const run = await startListener('sse')
    try {
      const first = await within(run.firstRequest, 'first SSE request')
      const second = await within(run.secondRequest, 'resumed SSE request')
      expect(first.pathname).toBe('/v1/stream/shape/test')
      expect(first.searchParams.get('offset')).toBe('-1')
      expect(first.searchParams.get('live')).toBe('sse')
      expect(second.searchParams.get('offset')).toBe('next-1')
      expect(second.searchParams.get('live')).toBe('sse')
    } finally {
      await stopListener(run)
    }
  })

  it.each([undefined, 'long-poll'] as const)('keeps long-poll for mode %s', async (mode) => {
    const run = await startListener(mode)
    try {
      const request = await within(run.firstRequest, 'long-poll request')
      expect(request.searchParams.get('offset')).toBe('-1')
      expect(request.searchParams.get('live')).toBe('long-poll')
      expect(run.requests).toHaveLength(1)
    } finally {
      await stopListener(run)
    }
  })

  it('rejects an unknown mode instead of silently measuring long-poll', async () => {
    const worker = spawn(process.execPath, ['-e', LIVE_WORKER_JS], {
      env: { ...process.env, WORKER_LIVE_MODE: 'SSE' },
      stdio: ['pipe', 'ignore', 'pipe'],
    })
    const [code] = await once(worker, 'exit')
    expect(code).not.toBe(0)
  })
})
