// A phone on a mobile link. Every request pays half the round trip each way; response bytes share
// one downlink (FIFO at DOWN_MBPS). Loads take a slot in the fetch-policy proposal's admission
// gate (first > visible > background) unless the design runs ungated, as today's app does.
//
// Designs, i.e. how one scope is fetched:
//   per-table   today's protocol for each table: feed create, stream HEAD, snapshot query
//   table-load  one gateway call per table, all in parallel, each one round trip
//   fanin       one gateway call per scope; the gateway runs the per-table calls next to the engine
//   scope-load  one gateway call per scope, one engine call, one Postgres snapshot
//   scope-group one gateway call per scope, one shared engine stream, rows tagged by table
// Live modes, i.e. how changes reach the phone afterwards:
//   per-stream  one long-poll per table
//   mux         one long-poll per scope (gateway /gw/mux)

import type { Level, Scope } from './scenarios.js'

export type Design = 'per-table' | 'table-load' | 'table-load-gated' | 'fanin' | 'scope-load' | 'scope-group'
export type LiveMode = 'per-stream' | 'mux' | 'coalesce' | 'streaming'

export interface PhoneOpts {
  gateway: string
  rttMs: number
  downMbps: number
  slots: number | null
  design: Design
  live: LiveMode
  /** The signed-in account (default the heavy one). */
  user?: string
  /** Optional benchmark hook for measuring a marker's write-to-stream latency. */
  onWake?: (body: string, at: number) => void
  /** Linger after a wake before the next poll, allowing several appends to share a response. */
  coalesceLingerMs?: number
}

export interface Held {
  key: string
  streamUrl: string
  offset: string
}

const RANK: Record<Level, number> = { first: 0, visible: 1, background: 2 }
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

class Gate {
  private used = 0
  private waiters: { rank: number; seq: number; go: () => void }[] = []
  private seq = 0
  constructor(private readonly slots: number | null) {}
  async acquire(level: Level) {
    if (this.slots == null) return
    if (this.used < this.slots) {
      this.used++
      return
    }
    await new Promise<void>((go) => {
      this.waiters.push({ rank: RANK[level], seq: this.seq++, go })
      this.waiters.sort((a, b) => a.rank - b.rank || a.seq - b.seq)
    })
  }
  release() {
    if (this.slots == null) return
    const next = this.waiters.shift()
    if (next) next.go()
    else this.used--
  }
}

export class Phone {
  readonly gate: Gate
  private downFreeAt = 0
  requests = 0
  bytesDown = 0
  private stopped = false
  private liveTasks: Promise<void>[] = []
  private aborts = new Set<AbortController>()

  constructor(readonly o: PhoneOpts) {
    this.gate = new Gate(o.slots)
  }

  private async http(method: string, path: string, payload?: unknown, signal?: AbortSignal) {
    this.requests++
    await sleep(this.o.rttMs / 2)
    const r = await fetch(`${this.o.gateway}${path}`, {
      method,
      signal,
      headers: payload
        ? { 'content-type': 'application/json', 'x-user': this.o.user ?? 'u1' }
        : { 'x-user': this.o.user ?? 'u1' },
      body: payload ? JSON.stringify(payload) : undefined,
    })
    const buf = Buffer.from(await r.arrayBuffer())
    if (!r.ok && r.status !== 204) throw new Error(`${method} ${path} ${r.status}: ${buf.toString().slice(0, 300)}`)
    // Shared downlink: this response's bytes queue behind every byte already in flight.
    const bytes = buf.length + 300
    this.bytesDown += bytes
    const now = performance.now()
    const start = Math.max(now + this.o.rttMs / 2, this.downFreeAt)
    this.downFreeAt = start + (bytes * 8) / (this.o.downMbps * 1000)
    await sleep(this.downFreeAt - now)
    return { status: r.status, headers: r.headers, text: buf.toString() }
  }

  /** Account a streamed response chunk on the same shared downlink as ordinary HTTP replies. */
  private async receiveChunk(bytes: number) {
    this.bytesDown += bytes
    const now = performance.now()
    const start = Math.max(now + this.o.rttMs / 2, this.downFreeAt)
    this.downFreeAt = start + (bytes * 8) / (this.o.downMbps * 1000)
    await sleep(this.downFreeAt - now)
  }

  private async stream(path: string, signal: AbortSignal) {
    this.requests++
    await sleep(this.o.rttMs / 2)
    const r = await fetch(`${this.o.gateway}${path}`, {
      signal,
      headers: { 'x-user': this.o.user ?? 'u1' },
    })
    if (!r.ok || !r.body) throw new Error(`GET ${path} ${r.status}`)
    return r.body
  }

  /** When each table's rows reached the phone, by scope name and table key. */
  readonly arrivals: { scope: string; level: Level; key: string; at: number }[] = []
  arrived(scope: Scope, key: string) {
    this.arrivals.push({ scope: scope.name, level: scope.level, key, at: performance.now() })
  }

  /** Loads one scope; resolves when every table's rows are on the phone. */
  async load(scope: Scope): Promise<Held[]> {
    if (this.o.design === 'per-table') {
      return Promise.all(
        scope.tables.map(async (t) => {
          await this.gate.acquire(scope.level)
          try {
            const feed = JSON.parse(
              (await this.http('POST', '/v1/subset-feeds', { table: t.table, where: t.where })).text,
            )
            const head = await this.http('HEAD', feed.streamUrl)
            const offset = head.headers.get('stream-next-offset') ?? '-1'
            await this.http('POST', '/v1/subsets/query', { table: t.table, where: t.where })
            this.arrived(scope, t.key)
            return { key: t.key, streamUrl: feed.streamUrl, offset }
          } finally {
            this.gate.release()
          }
        }),
      )
    }
    if (this.o.design === 'table-load-gated') {
      // One request per table, each taking its own gate slot at the scope's level, so a screen's
      // first-level tables go out before background ones.
      return Promise.all(
        scope.tables.map(async (t) => {
          await this.gate.acquire(scope.level)
          try {
            const r = JSON.parse((await this.http('POST', '/gw/table-load', t)).text)
            this.arrived(scope, t.key)
            return { key: r.key, streamUrl: r.streamUrl, offset: r.offset }
          } finally {
            this.gate.release()
          }
        }),
      )
    }
    if (this.o.design === 'scope-group') {
      await this.gate.acquire(scope.level)
      try {
        const out = JSON.parse((await this.http('POST', '/gw/scope-group', { members: scope.tables })).text)
        for (const t of scope.tables) this.arrived(scope, t.key)
        const first = out.results[0]
        return [{ key: '__group__', streamUrl: first.streamUrl, offset: first.offset }]
      } finally {
        this.gate.release()
      }
    }
    await this.gate.acquire(scope.level)
    try {
      if (this.o.design === 'table-load') {
        // One request per table, all in parallel (HTTP/2-style), each one round trip.
        return await Promise.all(
          scope.tables.map(async (t) => {
            const r = JSON.parse((await this.http('POST', '/gw/table-load', t)).text)
            this.arrived(scope, t.key)
            return { key: r.key, streamUrl: r.streamUrl, offset: r.offset }
          }),
        )
      }
      const path = this.o.design === 'fanin' ? '/gw/fanin' : '/gw/scope-load'
      const out = JSON.parse((await this.http('POST', path, { tables: scope.tables })).text)
      for (const t of scope.tables) this.arrived(scope, t.key)
      return out.results.map((r: any) => ({ key: r.key, streamUrl: r.streamUrl, offset: r.offset }))
    } finally {
      this.gate.release()
    }
  }

  /** Reopen with stored cursors: catch each stream up from its offset instead of reloading. */
  async resume(level: Level, held: Held[]): Promise<Held[]> {
    await this.gate.acquire(level)
    try {
      if (this.o.live === 'mux') {
        const qs = held.map((s) => `s=${encodeURIComponent(`${s.streamUrl}@${s.offset}`)}`).join('&')
        const r = JSON.parse((await this.http('GET', `/gw/mux?catchup=1&${qs}`)).text)
        return held.map((h, i) => ({ ...h, offset: r[i].next }))
      }
      return await Promise.all(
        held.map(async (h) => {
          const sep = h.streamUrl.includes('?') ? '&' : '?'
          const r = await this.http('GET', `${h.streamUrl}${sep}offset=${encodeURIComponent(h.offset)}`)
          return { ...h, offset: r.headers.get('stream-next-offset') ?? h.offset }
        }),
      )
    } finally {
      this.gate.release()
    }
  }

  /** Keeps the scope live until stop(): one long-poll per stream, or one per scope. */
  hold(held: Held[]) {
    const loop = async (streams: Held[]) => {
      while (!this.stopped) {
        const ctl = new AbortController()
        this.aborts.add(ctl)
        try {
          if (this.o.live === 'streaming') {
            const s = streams[0]!
            const streamUrl = new URL(s.streamUrl, 'http://stream')
            const dsPath = streamUrl.pathname.slice('/stream'.length) + streamUrl.search
            const body = await this.stream(`/gw/streaming?path=${encodeURIComponent(dsPath)}&offset=${encodeURIComponent(s.offset)}`, ctl.signal)
            const reader = body.getReader()
            const decoder = new TextDecoder()
            let buffered = ''
            while (true) {
              const chunk = await reader.read()
              if (chunk.done) break
              // The provider chunk has arrived locally, but the phone observes it only
              // after the return half-RTT and its share of the serialized downlink.
              await this.receiveChunk(chunk.value.byteLength)
              buffered += decoder.decode(chunk.value, { stream: true })
              const lines = buffered.split('\n')
              buffered = lines.pop() ?? ''
              for (const line of lines) {
                if (!line) continue
                const x = JSON.parse(line)
                this.o.onWake?.(x.body ?? '', performance.now())
                s.offset = x.next
              }
            }
          } else if (this.o.live === 'mux' && streams.length > 1) {
            const qs = streams.map((s) => `s=${encodeURIComponent(`${s.streamUrl}@${s.offset}`)}`).join('&')
            const r = JSON.parse((await this.http('GET', `/gw/mux?${qs}`, undefined, ctl.signal)).text)
            this.o.onWake?.(JSON.stringify(r), performance.now())
            r.forEach((x: any, i: number) => (streams[i]!.offset = x.next))
          } else {
            const s = streams[0]!
            const sep = s.streamUrl.includes('?') ? '&' : '?'
            const linger = this.o.live === 'coalesce' ? `&linger=${this.o.coalesceLingerMs ?? 50}` : ''
            const r = await this.http('GET', `${s.streamUrl}${sep}offset=${encodeURIComponent(s.offset)}&live=long-poll${linger}`, undefined, ctl.signal)
            this.o.onWake?.(r.text, performance.now())
            s.offset = r.headers.get('stream-next-offset') ?? s.offset
          }
        } catch {
          if (!this.stopped) await sleep(500)
        } finally {
          this.aborts.delete(ctl)
        }
      }
    }
    if (this.o.live === 'mux') this.liveTasks.push(loop(held.map((h) => ({ ...h }))))
    else for (const h of held) this.liveTasks.push(loop([{ ...h }]))
  }

  get longPolls() {
    return this.aborts.size
  }

  async stop() {
    this.stopped = true
    for (const a of this.aborts) a.abort()
    await Promise.allSettled(this.liveTasks)
  }
}
