import { afterEach, expect, test, vi } from 'vitest'

import { Phone } from './phone.js'

afterEach(() => {
  vi.unstubAllGlobals()
  vi.useRealTimers()
})

// Exercise the actual Phone.hold delivery path. The fake network provides a
// controlled chunk; the clock checks the model, not real network timing.
test('streaming delivery accounts for response propagation, bandwidth and bytes', async () => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] })
  let incoming!: ReadableStreamDefaultController<Uint8Array>
  const network = new ReadableStream<Uint8Array>({
    start(controller) {
      incoming = controller
    },
  })
  const fetchMock = vi.fn(async (_url: unknown, init?: RequestInit) => {
    init?.signal?.addEventListener('abort', () => incoming.error(new DOMException('Aborted', 'AbortError')), { once: true })
    return new Response(network, { status: 200 })
  })
  vi.stubGlobal('fetch', fetchMock)
  const delivered: { body: string; at: number }[] = []
  const phone = new Phone({
    gateway: 'http://controlled-network',
    rttMs: 250,
    downMbps: 1,
    slots: null,
    design: 'scope-group',
    live: 'streaming',
    onWake: (body, at) => delivered.push({ body, at }),
  })
  phone.hold([{ key: 'group', streamUrl: '/stream/group/test', offset: '-1' }])
  try {
    await vi.advanceTimersByTimeAsync(125)
    expect(fetchMock).toHaveBeenCalledTimes(1)
    const payload = 'x'.repeat(100_000)
    const chunk = new TextEncoder().encode(`${JSON.stringify({ next: '1', body: payload })}\n`)
    const receivedAt = performance.now()
    incoming.enqueue(chunk)
    await vi.advanceTimersByTimeAsync(124)
    expect(delivered, 'a local chunk is not yet delivered over the modeled return link').toEqual([])
    const serializationMs = (chunk.byteLength * 8) / 1000
    await vi.advanceTimersByTimeAsync(serializationMs)
    expect(delivered, 'the whole payload must traverse the modeled bandwidth limit').toEqual([])
    await vi.advanceTimersByTimeAsync(10)
    expect(delivered).toHaveLength(1)
    expect(delivered[0]!.body).toBe(payload)
    expect(delivered[0]!.at).toBeGreaterThanOrEqual(receivedAt + 125 + serializationMs)
    expect(phone.bytesDown).toBeGreaterThanOrEqual(chunk.byteLength)
  } finally {
    const stopped = phone.stop()
    await vi.advanceTimersByTimeAsync(10_000)
    await stopped
  }
})
