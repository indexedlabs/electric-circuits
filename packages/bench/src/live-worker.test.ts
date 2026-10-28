import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http'

import { describe, expect, it } from 'vitest'

import { LIVE_WORKER_JS } from './live-worker.js'
import { createLiveReadinessGate } from './live-readiness.js'

interface ListenerRun {
  requests: URL[]
  worker: ReturnType<typeof spawn>
  server: ReturnType<typeof createServer>
  stdout: string
  stderr: string
  firstRequest: Promise<URL>
  secondRequest: Promise<URL>
  thirdRequest: Promise<URL>
  readyLine: Promise<number>
}

function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (error: unknown) => void
  const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej })
  return { promise, resolve, reject }
}

function summaryOf(stdout: string): {
  reads: number
  failedReads: number
  reconnects: number
  maxHeld: number
  maxInFlight: number
  readySubscriptions: number
  measurementReady: boolean
  elapsedMs: number | null
  readyAt: number | null
  deliveries: number
  deliveryP50Ms: number | null
  deliveryP90Ms: number | null
} {
  const line = stdout.split('\n').findLast((value) => value.startsWith('{'))
  if (!line) throw new Error(`missing worker summary: ${stdout}`)
  return JSON.parse(line) as ReturnType<typeof summaryOf>
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

async function startListener(mode: string | undefined, expectedSubscriptions = 0, keepReadyOpenMs = 0): Promise<ListenerRun> {
  const server = createServer()
  const requests: URL[] = []
  const first = deferred<URL>()
  const second = deferred<URL>()
  const third = deferred<URL>()
  const ready = deferred<number>()
  const markerPrefix = 'bench-test-'
  const writtenAt = Date.now() - 25
  const secondWrittenAt = Date.now() - 35
  let base = ''
  server.on('request', (req: IncomingMessage, res: ServerResponse) => {
    const request = new URL(req.url ?? '/', base)
    requests.push(request)
    if (requests.length === 1) {
      first.resolve(request)
      if (mode === 'sse') {
        res.writeHead(200, { 'content-type': 'text/event-stream; charset=utf-8' })
        res.flushHeaders()
        res.write(`event: data\ndata: {"value":"${markerPrefix}`)
        setTimeout(() => {
          res.write(`${writtenAt} and ${markerPrefix}${secondWrittenAt}"}\n`)
          setTimeout(() => {
            res.write('\n')
            res.write('event: control\ndata: {}\n\n')
            res.write('event: control\ndata: {"streamNextOffset":"next-1","upToDate":true}\n\n')
            if (keepReadyOpenMs > 0) setTimeout(() => res.end(), keepReadyOpenMs)
            else res.end()
          }, 10)
        }, 10)
      } else {
        res.writeHead(200, { 'stream-next-offset': 'next-1', 'stream-up-to-date': 'true' })
        res.end(`${markerPrefix}${writtenAt} ${markerPrefix}${secondWrittenAt}`)
      }
      return
    }
    if (requests.length === 2) second.resolve(request)
    if (requests.length >= 3) third.resolve(request)
    if (requests.length >= 3) return
    if (mode === 'sse') {
      res.writeHead(200, { 'content-type': 'text/event-stream; charset=utf-8' })
      res.end('event: control\ndata: {"streamNextOffset":"next-1"}\n\n')
    } else {
      res.writeHead(200, { 'stream-next-offset': 'next-1' })
      res.end(`${markerPrefix}${writtenAt} ${markerPrefix}${secondWrittenAt}`)
    }
  })
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('missing listener address')
  base = `http://127.0.0.1:${address.port}`
  const env: NodeJS.ProcessEnv = {
    ...process.env,
    WORKER_WRITE_MARKER_PREFIX: markerPrefix,
    WORKER_EXPECTED_SUBSCRIPTIONS: String(expectedSubscriptions),
  }
  delete env.WORKER_LIVE_MODE
  if (mode !== undefined) env.WORKER_LIVE_MODE = mode
  const worker = spawn(process.execPath, ['-e', LIVE_WORKER_JS], {
    env,
    stdio: ['pipe', 'pipe', 'pipe'],
  })
  const run = { requests, worker, server, stdout: '', stderr: '', firstRequest: first.promise, secondRequest: second.promise, thirdRequest: third.promise, readyLine: ready.promise }
  worker.stdout!.on('data', (d: Buffer) => {
    run.stdout += d.toString()
    const match = run.stdout.match(/READY \d+ (\d+)/)
    if (match) ready.resolve(Number(match[1]))
  })
  worker.stderr!.on('data', (d: Buffer) => { run.stderr += d.toString() })
  worker.stdin.end(`${base}/v1/stream/shape/test\n`)
  return run
}

async function stopListener(run: ListenerRun): Promise<void> {
  if (run.worker.exitCode === null && run.worker.signalCode === null) run.worker.kill('SIGTERM')
  await once(run.worker, 'close')
  run.server.closeAllConnections()
  await new Promise<void>((resolve, reject) => run.server.close((error) => error ? reject(error) : resolve()))
}

describe('per-table bench live mode', () => {
  it('counts parent readiness lines and rejects a readiness timeout', async () => {
    const gate = createLiveReadinessGate(2, 20)
    gate.observe(1)
    expect(gate.count()).toBe(1)
    gate.observe(1)
    await expect(gate.ready).resolves.toEqual(expect.objectContaining({ elapsedMs: expect.any(Number) }))
    gate.dispose()

    const timedOut = createLiveReadinessGate(1, 10)
    await expect(timedOut.ready).rejects.toThrow('live readiness timed out: 0 of 1 subscriptions ready')
    timedOut.dispose()

    const closed = createLiveReadinessGate(2, 1000)
    closed.observe(1)
    closed.closed()
    await expect(closed.ready).rejects.toThrow('live readiness failed: 1 of 2 subscriptions ready')
    closed.dispose()
  })

  it('emits a syntactically valid worker script', async () => {
    const worker = spawn(process.execPath, ['--check', '-'], {
      stdio: ['pipe', 'ignore', 'pipe'],
    })
    let stderr = ''
    worker.stderr!.on('data', (d: Buffer) => { stderr += d.toString() })
    worker.stdin!.end(LIVE_WORKER_JS)
    const [code] = await once(worker, 'close')
    expect(code, stderr).toBe(0)
  })

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
    const summary = summaryOf(run.stdout)
    expect(summary.reads).toBeGreaterThanOrEqual(2)
    expect(summary.reconnects).toBeGreaterThanOrEqual(1)
    expect(summary.maxHeld).toBe(1)
    expect(summary.maxInFlight).toBe(1)
    expect(summary.deliveries).toBeGreaterThanOrEqual(2)
    expect(summary.deliveryP50Ms).not.toBeNull()
    expect(summary.deliveryP90Ms).not.toBeNull()
  })

  it('starts measurement counters after the readiness fence', async () => {
    const run = await startListener('sse', 1)
    try {
      await within(run.firstRequest, 'readiness SSE request')
      await within(run.secondRequest, 'post-readiness SSE request')
      await within(run.thirdRequest, 'held SSE request')
    } finally {
      await stopListener(run)
    }
    const summary = summaryOf(run.stdout)
    expect(run.stdout).toContain('READY 0 ')
    expect(summary.readySubscriptions).toBe(1)
    // The readiness response closes inside the measured window, and the next response is counted too.
    expect(summary.reads).toBe(2)
    expect(summary.reconnects).toBe(1)
  })

  it('starts the SSE measurement window at up-to-date while the response remains open', async () => {
    const run = await startListener('sse', 1, 300)
    let readyAt = 0
    let stoppedAt = 0
    try {
      readyAt = await within(run.readyLine, 'SSE readiness line')
      await new Promise((resolve) => setTimeout(resolve, 40))
      stoppedAt = Date.now()
    } finally {
      await stopListener(run)
    }
    const summary = summaryOf(run.stdout)
    expect(summary.measurementReady).toBe(true)
    expect(summary.readyAt).not.toBeNull()
    expect(summary.elapsedMs).not.toBeNull()
    expect(summary.elapsedMs!).toBeGreaterThanOrEqual(stoppedAt - readyAt - 20)
    expect(summary.elapsedMs!).toBeLessThan(250)
    expect(summary.reads).toBe(0)
    expect(summary.reconnects).toBe(0)
  })

  it('fences long-poll measurements at stream-up-to-date', async () => {
    const run = await startListener('long-poll', 1)
    try {
      await within(run.readyLine, 'long-poll readiness line')
      await within(run.secondRequest, 'post-readiness long-poll request')
    } finally {
      await stopListener(run)
    }
    const summary = summaryOf(run.stdout)
    expect(summary.measurementReady).toBe(true)
    expect(summary.readySubscriptions).toBe(1)
    // The readiness response closes inside the measured window, so it and the resumed response count.
    expect(summary.reads).toBeGreaterThanOrEqual(2)
  })

  it('reports an unfenced measurement as not ready', async () => {
    const request = deferred<void>()
    const server = createServer((_req, res) => {
      request.resolve(undefined)
      res.writeHead(200, { 'content-type': 'text/event-stream; charset=utf-8' })
      res.end('event: control\ndata: {"streamNextOffset":"next-1"}\n\n')
    })
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', () => resolve()))
    const address = server.address()
    if (address === null || typeof address === 'string') throw new Error('missing listener address')
    const worker = spawn(process.execPath, ['-e', LIVE_WORKER_JS], {
      env: { ...process.env, WORKER_LIVE_MODE: 'sse', WORKER_EXPECTED_SUBSCRIPTIONS: '1' },
      stdio: ['pipe', 'pipe', 'pipe'],
    })
    let stdout = ''
    worker.stdout!.on('data', (d: Buffer) => { stdout += d.toString() })
    worker.stdin!.end(`http://127.0.0.1:${address.port}/stream\n`)
    await within(request.promise, 'unfenced SSE request')
    await new Promise((resolve) => setTimeout(resolve, 30))
    worker.kill('SIGTERM')
    await once(worker, 'close')
    server.closeAllConnections()
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()))
    const summary = summaryOf(stdout)
    expect(summary.measurementReady).toBe(false)
    expect(summary.elapsedMs).toBeNull()
  })

  it.each([undefined, 'long-poll'] as const)('keeps long-poll for mode %s', async (mode) => {
    const run = await startListener(mode)
    try {
      const first = await within(run.firstRequest, 'long-poll request')
      const second = await within(run.secondRequest, 'resumed long-poll request')
      expect(first.searchParams.get('offset')).toBe('-1')
      expect(first.searchParams.get('live')).toBe('long-poll')
      expect(second.searchParams.get('offset')).toBe('next-1')
      expect(second.searchParams.get('live')).toBe('long-poll')
    } finally {
      await stopListener(run)
    }
    const summary = summaryOf(run.stdout)
    expect(summary.reads).toBeGreaterThanOrEqual(2)
    expect(summary.reconnects).toBeGreaterThanOrEqual(1)
    expect(summary.maxHeld).toBe(1)
    expect(summary.maxInFlight).toBe(1)
    expect(summary.deliveries).toBeGreaterThanOrEqual(2)
    expect(summary.deliveryP50Ms).not.toBeNull()
    expect(summary.deliveryP90Ms).not.toBeNull()
  })

  it('rejects an unknown mode instead of silently measuring long-poll', async () => {
    const worker = spawn(process.execPath, ['-e', LIVE_WORKER_JS], {
      env: { ...process.env, WORKER_LIVE_MODE: 'SSE' },
      stdio: ['pipe', 'ignore', 'pipe'],
    })
    let stderr = ''
    worker.stderr!.on('data', (d: Buffer) => { stderr += d.toString() })
    const [code] = await once(worker, 'close')
    expect(code).toBe(2)
    expect(stderr).toContain('unknown WORKER_LIVE_MODE=SSE')
  })

  it('counts a pending long-poll as in flight until shutdown', async () => {
    const first = deferred<void>()
    const server = createServer((_req, _res) => first.resolve(undefined))
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', () => resolve()))
    const address = server.address()
    if (address === null || typeof address === 'string') throw new Error('missing listener address')
    const worker = spawn(process.execPath, ['-e', LIVE_WORKER_JS], {
      env: { ...process.env, WORKER_LIVE_MODE: 'long-poll' },
      stdio: ['pipe', 'pipe', 'pipe'],
    })
    let stdout = ''
    worker.stdout!.on('data', (d: Buffer) => { stdout += d.toString() })
    worker.stdin!.end(`http://127.0.0.1:${address.port}/stream\n`)
    await within(first.promise, 'pending long-poll request')
    worker.kill('SIGTERM')
    await once(worker, 'close')
    server.closeAllConnections()
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()))
    const summary = summaryOf(stdout)
    expect(summary.maxHeld).toBe(0)
    expect(summary.maxInFlight).toBe(1)
    expect(summary.failedReads).toBe(0)
  })

  it('counts reset connections as failed without reconnects', async () => {
    const request = deferred<void>()
    const server = createServer((req, res) => {
      request.resolve(undefined)
      req.socket.destroy()
      res.destroy()
    })
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', () => resolve()))
    const address = server.address()
    if (address === null || typeof address === 'string') throw new Error('missing listener address')
    const worker = spawn(process.execPath, ['-e', LIVE_WORKER_JS], {
      env: { ...process.env, WORKER_LIVE_MODE: 'long-poll' },
      stdio: ['pipe', 'pipe', 'pipe'],
    })
    let stdout = ''
    worker.stdout!.on('data', (d: Buffer) => { stdout += d.toString() })
    worker.stdin!.end(`http://127.0.0.1:${address.port}/stream\n`)
    await within(request.promise, 'reset connection request')
    await new Promise((resolve) => setTimeout(resolve, 30))
    worker.kill('SIGTERM')
    await once(worker, 'close')
    server.closeAllConnections()
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()))
    const summary = summaryOf(stdout)
    expect(summary.failedReads).toBeGreaterThan(0)
    expect(summary.reconnects).toBe(0)
  })
})
