// A stand-in for Indexed's sync gateway (the FastAPI routes in front of the engine), run as its own
// process so it has one event loop the way one API worker does. Each request burns the CPU the
// production gateway was measured to spend, synchronously, so a burst queues here the way it
// queued on the API tasks:
//   - BASE_MS per request (authority read, parsing, tracing),
//   - PRED_MS per table predicate it authorizes,
//   - STREAM_MS per stream HEAD/GET it proxies.
// Routes:
//   POST /v1/subset-feeds, POST /v1/subsets/query   today's per-table calls, proxied to the engine
//   HEAD|GET /stream/<path>                          stream reads, proxied to durable streams
//   POST /gw/fanin                                   one call per scope; the gateway runs each
//                                                    table's feed create, HEAD and query itself
//   POST /gw/scope-load                              one call per scope, forwarded to the engine's
//                                                    /v1/scopes/load (one snapshot for all tables)
//   POST /gw/scope-group                             one call per scope, forwarded to the prototype
//                                                    group form (one stream, rows tagged by member)
//   GET  /gw/mux?s=<url>@<offset>&s=…                one long-poll covering several streams;
//                                                    with catchup=1, one read of each from its offset
//   GET  /gw/stats, POST /gw/stats/reset

import { createServer, type IncomingMessage, type ServerResponse } from 'node:http'

const ENGINE = process.env.ENGINE_URL!
const DS = process.env.DS_URL!
const BASE_MS = Number(process.env.GW_BASE_MS ?? 8)
const PRED_MS = Number(process.env.GW_PRED_MS ?? 12)
const STREAM_MS = Number(process.env.GW_STREAM_MS ?? 4)
const PORT = Number(process.env.GW_PORT ?? 0)

const stats = {
  requests: 0,
  byRoute: {} as Record<string, number>,
  burnedMs: 0,
  engineCalls: 0,
  dsCalls: 0,
  inFlight: 0,
  maxInFlight: 0,
  lagSamples: [] as number[],
  longPollsOpen: 0,
  maxLongPollsOpen: 0,
}

function burn(ms: number) {
  if (ms <= 0) return
  const end = performance.now() + ms
  while (performance.now() < end) {
    /* busy: the event loop is blocked, as a CPU-bound Python handler blocks its loop */
  }
  stats.burnedMs += ms
}

// Event-loop lag, sampled every 50 ms (the server proposal's api.event_loop_lag_ms).
let expected = performance.now() + 50
setInterval(() => {
  const now = performance.now()
  stats.lagSamples.push(Math.max(0, now - expected))
  expected = now + 50
}, 50).unref()

async function body(req: IncomingMessage): Promise<any> {
  const chunks: Buffer[] = []
  for await (const c of req) chunks.push(c as Buffer)
  return chunks.length ? JSON.parse(Buffer.concat(chunks).toString()) : undefined
}

function send(res: ServerResponse, status: number, value: unknown, headers: Record<string, string> = {}) {
  const buf = Buffer.from(JSON.stringify(value))
  res.writeHead(status, { 'content-type': 'application/json', 'content-length': String(buf.length), ...headers })
  res.end(buf)
}

// Authorization: the gateway ands the caller's authority onto every predicate it forwards.
function authorize(user: string, where: unknown) {
  const own = { col: 'user_id', op: 'eq', value: user }
  return where ? { and: [own, where] } : own
}

async function engine(path: string, payload: unknown) {
  stats.engineCalls++
  const r = await fetch(`${ENGINE}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(payload),
  })
  const text = await r.text()
  if (!r.ok) throw Object.assign(new Error(`engine ${path} ${r.status}: ${text.slice(0, 300)}`), { status: r.status })
  return JSON.parse(text)
}

const streamPath = (url: string) => {
  const u = new URL(url)
  return u.pathname + u.search
}

async function dsHead(path: string): Promise<string> {
  stats.dsCalls++
  const r = await fetch(`${DS}${path}`, { method: 'HEAD' })
  return r.headers.get('stream-next-offset') ?? '-1'
}

async function dsRead(path: string, offset: string, live: boolean, signal?: AbortSignal) {
  stats.dsCalls++
  const sep = path.includes('?') ? '&' : '?'
  const url = `${DS}${path}${sep}offset=${encodeURIComponent(offset)}${live ? '&live=long-poll' : ''}`
  const r = await fetch(url, { signal })
  const text = await r.text()
  return { status: r.status, next: r.headers.get('stream-next-offset') ?? offset, body: text }
}

function rewriteFeed(feed: any) {
  return { ...feed, streamUrl: `/stream${streamPath(feed.streamUrl)}` }
}

async function route(req: IncomingMessage, res: ServerResponse) {
  const url = new URL(req.url!, 'http://gw')
  const user = (req.headers['x-user'] as string) ?? 'u1'
  const key = `${req.method} ${url.pathname.startsWith('/stream/') ? '/stream' : url.pathname}`
  stats.byRoute[key] = (stats.byRoute[key] ?? 0) + 1
  stats.requests++

  if (url.pathname === '/gw/stats') {
    const lag = [...stats.lagSamples].sort((a, b) => a - b)
    const q = (p: number) => (lag.length ? lag[Math.min(lag.length - 1, Math.floor(p * lag.length))]! : 0)
    return send(res, 200, { ...stats, lagSamples: undefined, lagP50: q(0.5), lagP99: q(0.99), lagMax: q(1) })
  }
  if (url.pathname === '/gw/stats/reset') {
    Object.assign(stats, {
      requests: 0,
      byRoute: {},
      burnedMs: 0,
      engineCalls: 0,
      dsCalls: 0,
      maxInFlight: stats.inFlight,
      lagSamples: [],
      maxLongPollsOpen: stats.longPollsOpen,
    })
    return send(res, 200, { ok: true })
  }

  if (req.method === 'POST' && url.pathname === '/v1/subset-feeds') {
    const b = await body(req)
    burn(BASE_MS + PRED_MS)
    const feed = await engine('/v1/subset-feeds', { ...b, where: authorize(user, b.where) })
    return send(res, 200, rewriteFeed(feed))
  }
  if (req.method === 'POST' && url.pathname === '/v1/subsets/query') {
    const b = await body(req)
    burn(BASE_MS + PRED_MS)
    return send(res, 200, await engine('/v1/subsets/query', { ...b, where: authorize(user, b.where) }))
  }
  if (url.pathname.startsWith('/stream/')) {
    burn(STREAM_MS)
    const path = url.pathname.slice('/stream'.length)
    if (req.method === 'HEAD') {
      const off = await dsHead(path)
      res.writeHead(200, { 'stream-next-offset': off })
      return res.end()
    }
    const live = url.searchParams.get('live') === 'long-poll'
    if (live) stats.longPollsOpen++, (stats.maxLongPollsOpen = Math.max(stats.maxLongPollsOpen, stats.longPollsOpen))
    try {
      const r = await dsRead(path, url.searchParams.get('offset') ?? '-1', live)
      res.writeHead(r.status, { 'stream-next-offset': r.next, 'content-type': 'application/json' })
      return res.end(r.body)
    } finally {
      if (live) stats.longPollsOpen--
    }
  }
  if (req.method === 'POST' && url.pathname === '/gw/fanin') {
    const b = await body(req)
    burn(BASE_MS + PRED_MS * b.tables.length)
    const results = await Promise.all(
      b.tables.map(async (t: any) => {
        const where = authorize(user, t.where)
        const feed = await engine('/v1/subset-feeds', { table: t.table, where, columns: t.columns, subscription: t.subscription })
        burn(STREAM_MS)
        const offset = await dsHead(streamPath(feed.streamUrl))
        const page = await engine('/v1/subsets/query', { table: t.table, where, columns: t.columns })
        return { key: t.key, ...rewriteFeed(feed), offset, rows: page.rows, snapshot: page.snapshot, lsn: page.lsn }
      }),
    )
    return send(res, 200, { results })
  }
  if (req.method === 'POST' && url.pathname === '/gw/table-load') {
    // One table in one round trip: feed create, frontier read and snapshot query in one call.
    const t = await body(req)
    burn(BASE_MS + PRED_MS)
    const where = authorize(user, t.where)
    const feed = await engine('/v1/subset-feeds', { table: t.table, where, columns: t.columns })
    burn(STREAM_MS)
    const offset = await dsHead(streamPath(feed.streamUrl))
    const page = await engine('/v1/subsets/query', { table: t.table, where, columns: t.columns })
    return send(res, 200, { key: t.key, ...rewriteFeed(feed), offset, rows: page.rows, snapshot: page.snapshot, lsn: page.lsn })
  }
  if (req.method === 'POST' && url.pathname === '/gw/scope-load') {
    const b = await body(req)
    burn(BASE_MS + PRED_MS * b.tables.length)
    const out = await engine('/v1/scopes/load', {
      tables: b.tables.map((t: any) => ({ ...t, where: authorize(user, t.where) })),
    })
    return send(res, 200, { ...out, results: out.results.map(rewriteFeed) })
  }
  if (req.method === 'POST' && (url.pathname === '/gw/scope-group' || url.pathname === '/gw/scope-semijoin')) {
    const b = await body(req)
    burn(BASE_MS + PRED_MS * b.members.length)
    // The semijoin uses the same one-stream transport shape; its engine-side key
    // circuit is measured separately by semijoin-bench.ts until the runtime template
    // is wired into the native engine.
    const out = await engine('/v1/subset-loads', {
      members: b.members.map((t: any) => ({ ...t, where: authorize(user, t.where) })),
    })
    // A group has one upstream stream HEAD, even though its member page was one engine call.
    burn(STREAM_MS)
    const offset = await dsHead(streamPath(out.streamUrl))
    return send(res, 200, {
      ...out,
      offset,
      streamUrl: out.streamUrl ? `/stream${streamPath(out.streamUrl)}` : out.streamUrl,
      results: out.results.map((r: any) => ({ ...r, offset, streamUrl: `/stream${streamPath(r.streamUrl)}` })),
    })
  }
  if (req.method === 'GET' && url.pathname === '/gw/mux') {
    // One long-poll for several streams: wait on all upstream long-polls, answer with whichever
    // streams have data first (plus any others that are ready by then), and cancel the rest.
    const specs = url.searchParams.getAll('s').map((s) => {
      const at = s.lastIndexOf('@')
      return { path: s.slice('/stream'.length, at), offset: s.slice(at + 1) }
    })
    // Each stream a poll covers costs a stream read's authority check, as a per-stream poll does.
    burn(STREAM_MS * specs.length)
    if (url.searchParams.get('catchup') === '1') {
      // Resume: read every stream from its stored offset without waiting, in one call.
      const rs = await Promise.all(specs.map((sp) => dsRead(sp.path, sp.offset, false)))
      return send(res, 200, rs.map((r) => ({ next: r.next, body: r.status === 200 ? r.body : '' })))
    }
    stats.longPollsOpen++
    stats.maxLongPollsOpen = Math.max(stats.maxLongPollsOpen, stats.longPollsOpen)
    const ctl = new AbortController()
    try {
      const settled: any[] = []
      await new Promise<void>((resolve) => {
        let pending = specs.length
        specs.forEach((sp, i) =>
          dsRead(sp.path, sp.offset, true, ctl.signal)
            .then((r) => {
              settled[i] = { next: r.next, body: r.status === 200 ? r.body : '' }
              if (r.status === 200 && r.body && r.body !== '[]') resolve()
            })
            .catch(() => {})
            .finally(() => --pending === 0 && resolve()),
        )
      })
      // Let streams that answered in the same instant join this response.
      await new Promise((r) => setImmediate(r))
      return send(res, 200, specs.map((sp, i) => settled[i] ?? { next: sp.offset, body: '' }))
    } finally {
      ctl.abort()
      stats.longPollsOpen--
    }
  }
  send(res, 404, { error: `no route ${key}` })
}

const server = createServer((req, res) => {
  stats.inFlight++
  stats.maxInFlight = Math.max(stats.maxInFlight, stats.inFlight)
  route(req, res)
    .catch((e) => {
      if (!res.headersSent) send(res, e.status ?? 500, { error: String(e.message ?? e) })
    })
    .finally(() => stats.inFlight--)
})
server.keepAliveTimeout = 120_000
server.listen(PORT, '127.0.0.1', () => {
  const addr = server.address()
  process.stdout.write(`GATEWAY_LISTENING http://127.0.0.1:${typeof addr === 'object' && addr ? addr.port : PORT}\n`)
})
