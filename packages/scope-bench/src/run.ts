// Replays Indexed's screens against the real engine and synthetic tables, once per design, and
// prints how long each screen's first-level content took to reach the phone and what it cost the
// gateway, the engine and Postgres.
//
//   pnpm --filter @electric-circuits/scope-bench bench
//   RTT_MS=120 DOWN_MBPS=20 REPS=3 OTHER_USERS=30 ONLY=chat pnpm --filter … bench

import { type ChildProcess, execFileSync, spawn } from 'node:child_process'
import { appendFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { DurableStreamTestServer } from '@electric-circuits/ds-rust'
import pg from 'pg'

import { postgres18Tools } from '../../../scripts/postgres18.js'
import { type Design, type Held, type LiveMode, Phone } from './phone.js'
import { type Scenario, scenarios } from './scenarios.js'
import { ddl, INDEXES, TABLES } from './schema.js'
import { HEAVY, seed, type SeedSummary } from './seed.js'

const env = (k: string, d: string) => process.env[k] ?? d
const RTT_MS = Number(env('RTT_MS', '100'))
const DOWN_MBPS = Number(env('DOWN_MBPS', '20'))
const REPS = Number(env('REPS', '3'))
const OTHER_USERS = Number(env('OTHER_USERS', '30'))
const HOLD_MS = Number(env('HOLD_MS', '3000'))
// Phones running the scenario at once against the one gateway process (the incident's shape).
const PHONES = Number(env('PHONES', '1'))
// Durable Streams' long-poll timeout; production's is 30 s.
const DS_LONGPOLL_MS = Number(env('DS_LONGPOLL_MS', '5000'))
// STEADY=1: while held, a reply streams into the thread (20 events/s for 10 s) and a message lands
// every 5 s, besides the Record edits; hold-only gateway cost is reported separately.
const STEADY = process.env.STEADY === '1'
// DISTINCT_USERS=1: phone i signs in as u<i+2> (one of the lighter accounts) instead of all as u1.
const DISTINCT_USERS = process.env.DISTINCT_USERS === '1'
const phoneUser = (i: number) => (DISTINCT_USERS ? `u${i + 2}` : HEAVY)
const ONLY = process.env.ONLY
const ONLY_CONFIG = process.env.ONLY_CONFIG
const OUT = env('OUT', join(process.cwd(), 'scope-bench-results.json'))

interface Config {
  name: string
  variant: 'today' | 'scoped' | 'series'
  design: Design
  slots: number | null
  live: LiveMode
  /** On a reopen, catch up from stored cursors instead of reloading. */
  resume?: boolean
}
const CONFIGS: Config[] = ([
  { name: 'today', variant: 'today', design: 'per-table', slots: null, live: 'per-stream' },
  { name: 'scopes + gate', variant: 'scoped', design: 'per-table', slots: 8, live: 'per-stream' },
  { name: 'gateway fan-in', variant: 'scoped', design: 'fanin', slots: 8, live: 'per-stream' },
  { name: 'table load + mux', variant: 'scoped', design: 'table-load', slots: 8, live: 'mux' },
  { name: 'table load + mux + series', variant: 'series', design: 'table-load', slots: 8, live: 'mux' },
  { name: 'table load (gate per table)', variant: 'scoped', design: 'table-load-gated', slots: 8, live: 'per-stream' },
  { name: 'table load (gate per table) + mux', variant: 'scoped', design: 'table-load-gated', slots: 8, live: 'mux' },
  { name: 'table load (gate per table) + series', variant: 'series', design: 'table-load-gated', slots: 8, live: 'per-stream' },
  { name: 'scope load', variant: 'scoped', design: 'scope-load', slots: 8, live: 'per-stream' },
  { name: 'scope load + mux', variant: 'scoped', design: 'scope-load', slots: 8, live: 'mux' },
  { name: 'scope load + mux + resume', variant: 'scoped', design: 'scope-load', slots: 8, live: 'mux', resume: true },
  { name: 'scope load + mux + series', variant: 'series', design: 'scope-load', slots: 8, live: 'mux' },
  { name: 'scope group', variant: 'scoped', design: 'scope-group', slots: 8, live: 'per-stream' },
  { name: 'scope group + resume', variant: 'scoped', design: 'scope-group', slots: 8, live: 'per-stream', resume: true },
  { name: 'scope semijoin', variant: 'scoped', design: 'scope-semijoin', slots: 8, live: 'per-stream' },
] as Config[]).filter((c) => !ONLY_CONFIG || ONLY_CONFIG.split(',').includes(c.name))

function repoRoot(): string {
  let d = dirname(fileURLToPath(import.meta.url))
  while (!existsSync(join(d, 'Cargo.toml'))) d = dirname(d)
  return d
}

function cpuMs(pid: number): number {
  const f = readFileSync(`/proc/${pid}/stat`, 'utf8').split(') ')[1]!.split(' ')
  return ((Number(f[11]) + Number(f[12])) * 1000) / 100
}

function rssMb(pid: number): number {
  const pages = Number(readFileSync(`/proc/${pid}/statm`, 'utf8').split(' ')[1] ?? 0)
  return (pages * 4096) / 1e6
}

function waitFor(proc: ChildProcess, re: RegExp, what: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error(`${what} did not start within 60s`)), 60000)
    let buf = ''
    proc.stdout!.on('data', (d: Buffer) => {
      buf += d.toString()
      const m = buf.match(re)
      if (m) {
        clearTimeout(t)
        resolve(m[1]!)
      }
    })
    proc.on('exit', (c) => reject(new Error(`${what} exited early (${c})`)))
  })
}

async function bootPostgres() {
  const tools = postgres18Tools()
  const work = mkdtempSync(join(tmpdir(), 'scope-bench-'))
  const data = join(work, 'pg')
  execFileSync(tools.initdb, ['-D', data, '-U', 'postgres', '--auth=trust', '--no-sync'], { stdio: 'ignore' })
  const port = 56000 + Math.floor(Math.random() * 3000)
  appendFileSync(
    join(data, 'postgresql.conf'),
    `\nwal_level = logical\nmax_replication_slots = 200\nmax_wal_senders = 20\nlisten_addresses = '127.0.0.1'\n` +
      `unix_socket_directories = '/tmp'\nport = ${port}\nfsync = off\nsynchronous_commit = off\n` +
      `shared_buffers = 512MB\nmax_connections = 300\nshared_preload_libraries = 'pg_stat_statements'\n`,
  )
  execFileSync(tools.pgCtl, ['-D', data, '-l', join(work, 'pg.log'), '-w', 'start'], { stdio: 'ignore' })
  const url = `postgres://postgres@127.0.0.1:${port}/postgres`
  const stop = () => {
    try {
      execFileSync(tools.pgCtl, ['-D', data, '-m', 'immediate', '-w', 'stop'], { stdio: 'ignore' })
    } catch {}
    rmSync(work, { recursive: true, force: true })
  }
  return { url, stop }
}

let slotSeq = 0
// Records changed while the phone was away in the reopen scenario (set once the seed exists).
let reopenChanged: string[] = []
// The thread a steady-state reply streams into.
let steadyThread = ''
async function bootServers(pgUrl: string) {
  const ds = new DurableStreamTestServer({ longPollTimeout: DS_LONGPOLL_MS })
  const dsRaw = await ds.start()
  const dsUrl = `http://127.0.0.1:${new URL(dsRaw).port}`
  const slot = `scope_bench_${process.pid}_${++slotSeq}`
  const engine = spawn(process.env.ENGINE_BIN ?? join(repoRoot(), 'target', 'release', 'electric-circuits-engine'), [], {
    env: {
      ...process.env,
      ELECTRIC_CIRCUITS_DS_URL: dsUrl,
      // Loopback HTTP durable streams (needs an engine built with --features test-support).
      ELECTRIC_CIRCUITS_DS_IN_PROCESS_TEST: '1',
      ELECTRIC_CIRCUITS_INITIALIZE_NAMESPACE: '1',
      ELECTRIC_CIRCUITS_BIND: '127.0.0.1:0',
      ELECTRIC_CIRCUITS_LOG: 'warn',
      ELECTRIC_CIRCUITS_PG_URL: pgUrl,
      ELECTRIC_CIRCUITS_PG_TABLES: Object.keys(TABLES).join(','),
      ELECTRIC_CIRCUITS_PG_SLOT: slot,
      ELECTRIC_CIRCUITS_PG_POLL_MS: '25',
    },
    stdio: ['ignore', 'pipe', 'inherit'],
  })
  const engineUrl = await waitFor(engine, /ENGINE_LISTENING (\S+)/, 'engine')
  const gw = spawn(process.execPath, ['--import', 'tsx', join(dirname(fileURLToPath(import.meta.url)), 'gateway.ts')], {
    env: { ...process.env, ENGINE_URL: engineUrl, DS_URL: dsUrl },
    stdio: ['ignore', 'pipe', 'inherit'],
  })
  const gateway = await waitFor(gw, /GATEWAY_LISTENING (\S+)/, 'gateway')
  const stop = async (admin: pg.Client) => {
    gw.kill('SIGKILL')
    engine.kill('SIGKILL')
    await new Promise((r) => setTimeout(r, 300))
    await admin.query('select pg_drop_replication_slot($1)', [slot]).catch(() => {})
    await ds.stop().catch(() => {})
  }
  return { gateway, enginePid: engine.pid!, gwPid: gw.pid!, stop }
}

interface RunResult {
  scenario: string
  config: string
  rep: number
  screens: Record<string, { firstMs: number | null; allMs: number; firstP90?: number | null; firstTableMs?: number | null }>
  holdGatewayCpuMs?: number
  holdRequests?: number
  holdDsCalls?: number
  phoneRequests: number
  loadRequests: number
  bytesDown: number
  gatewayCpuMs: number
  gatewayLagP99: number
  gatewayLagMax: number
  engineCpuMs: number
  pgStatements: number
  pgExecMs: number
  longPollsHeld: number
  streamCount?: number
  engineRssMb?: number
  steadyWriteP50?: number | null
  steadyWriteP90?: number | null
  error?: string
}

let steadyRecordsByUser: Record<string, string> = {}

async function runOnce(admin: pg.Client, pgUrl: string, scs: Scenario[], cfg: Config, rep: number): Promise<RunResult> {
  const sc = scs[0]!
  const srv = await bootServers(pgUrl)
  const writeStarted = new Map<string, number>()
  const writeLatencies: number[] = []
  const onWake = (body: string, at: number) => {
    for (const [marker, started] of writeStarted) {
      if (body.includes(marker)) {
        writeLatencies.push(at - started)
        writeStarted.delete(marker)
      }
    }
  }
  const opts = { gateway: srv.gateway, rttMs: RTT_MS, downMbps: DOWN_MBPS, slots: cfg.slots, design: cfg.design, live: cfg.live, onWake }
  const phones = Array.from({ length: PHONES }, (_, i) => new Phone({ ...opts, user: phoneUser(i) }))
  // Statements the engine ran against Postgres and their execution time, from pg_stat_statements
  // (the bench's own writes are excluded by text match).
  const pgStats = async () => {
    const r = await admin.query(
      `select coalesce(sum(calls),0)::float8 as calls, coalesce(sum(total_exec_time),0)::float8 as ms from pg_stat_statements
       where query not ilike '%pg_stat_statements%' and query not ilike 'update record set title%'
         and query not ilike 'insert into assistant_stream_event%' and query not ilike 'insert into message%'`,
    )
    return { calls: Number(r.rows[0].calls), ms: Number(r.rows[0].ms) }
  }
  const result: RunResult = {
    scenario: sc.name,
    config: cfg.name,
    rep,
    screens: {},
    phoneRequests: 0,
    loadRequests: 0,
    bytesDown: 0,
    gatewayCpuMs: 0,
    gatewayLagP99: 0,
    gatewayLagMax: 0,
    engineCpuMs: 0,
    pgStatements: 0,
    pgExecMs: 0,
    longPollsHeld: 0,
  }
  const timers: NodeJS.Timeout[] = []
  const sum = (f: (p: Phone) => number) => phones.reduce((a, p) => a + f(p), 0)
  try {
    // A reopen first opens the screens once, unmeasured, keeps their cursors, leaves, and lets
    // some of the screen's rows change before coming back.
    const stored = new Map<string, Held[]>()
    if (sc.reopen) {
      const before = new Phone({ ...opts, slots: null })
      for (const ev of sc.events) for (const scope of ev.scopes) stored.set(scope.name, await before.load(scope))
      await before.stop()
      await admin.query(`update record set title = title || '!' where client_id = any($1)`, [reopenChanged])
    }
    await fetch(`${srv.gateway}/gw/stats/reset`)
    const cpu0 = cpuMs(srv.enginePid)
    const x0 = await pgStats()
    // Per phone and screen: when its first-level tables were all in, and the first of them.
    const perScreen = new Map<string, { first: number | null; firstTable: number | null; all: number }[]>()
    await Promise.all(
      phones.map((phone, pi) =>
        Promise.all(
          (scs[pi] ?? sc).events.map(async (ev) => {
            await new Promise((r) => setTimeout(r, ev.at))
            const start = performance.now()
            await Promise.all(
              ev.scopes.map(async (scope) => {
                const prior = stored.get(scope.name)
                const held = prior && cfg.resume ? await phone.resume(scope.level, prior) : await phone.load(scope)
                if (prior && cfg.resume) for (const t of scope.tables) phone.arrived(scope, t.key)
                phone.hold(held)
              }),
            )
            const names = new Set(ev.scopes.filter((x) => x.level === 'first').map((x) => x.name))
            const firsts = phone.arrivals.filter((a) => names.has(a.scope) && a.at >= start).map((a) => a.at - start)
            const row = {
              first: names.size ? Math.round(Math.max(...firsts)) : null,
              firstTable: names.size ? Math.round(Math.min(...firsts)) : null,
              all: Math.round(performance.now() - start),
            }
            perScreen.set(ev.screen, [...(perScreen.get(ev.screen) ?? []), row])
          }),
        ),
      ),
    )
    for (const [screen, rows] of perScreen) {
      const fs = rows.map((r) => r.first).filter((x): x is number => x != null)
      const ft = rows.map((r) => r.firstTable).filter((x): x is number => x != null)
      result.screens[screen] = {
        firstMs: fs.length ? median(fs) : null,
        firstP90: fs.length ? quantile(fs, 0.9) : null,
        firstTableMs: ft.length ? median(ft) : null,
        allMs: median(rows.map((r) => r.all)),
      }
    }
    result.loadRequests = sum((p) => p.requests)
    const gsLoad = (await (await fetch(`${srv.gateway}/gw/stats`)).json()) as any
    // Keep everything live for a while with writes landing on the heavy account.
    let n = 0
    timers.push(
      setInterval(() => {
        admin
          .query(`update record set title = title || '.' where user_id = $1 and id in (select id from record where user_id = $1 order by id limit 1 offset $2)`, [HEAVY, n++ % 50])
          .catch(() => {})
      }, 200),
    )
    if (STEADY) {
      let writeNo = 0
      let writeBusy = false
      const steadyWrite = async () => {
        if (writeBusy) return
        writeBusy = true
        const no = writeNo++
        try {
          for (let i = 0; i < PHONES; i++) {
            const user = phoneUser(i)
            const recordId = steadyRecordsByUser[user] ?? steadyRecordsByUser[HEAVY]
            if (!recordId) continue
            const marker = `steady-${rep}-${cfg.name}-${no}-${i}`
            writeStarted.set(marker, performance.now())
            const assignmentId = `${marker}-assignment`
            const taggedId = `${marker}-tag`
            await admin.query('begin')
            await admin.query(`update record set title = $1 where id = $2 and user_id = $3`, [marker, recordId, user])
            await admin.query(
              `insert into assignment (id, user_id, record_id, payload) values ($1, $2, $3, $4) on conflict (id) do update set payload = excluded.payload`,
              [assignmentId, user, recordId, marker],
            )
            await admin.query(
              `insert into tagged_item (id, user_id, record_id, tag_id) values ($1, $2, $3, $4) on conflict (id) do update set tag_id = excluded.tag_id`,
              [taggedId, user, recordId, marker],
            )
            await admin.query('commit')
          }
        } catch {
          await admin.query('rollback').catch(() => {})
        } finally {
          writeBusy = false
        }
      }
      await steadyWrite()
      timers.push(setInterval(() => void steadyWrite(), 500))
      let k = 0
      const thread = steadyThread
      const reply = setInterval(() => {
        if (k++ >= 200) return clearInterval(reply)
        admin
          .query(`insert into assistant_stream_event (id, user_id, thread_id, payload) values ($1, $2, $3, $4)`, [`steady-ase-${rep}-${cfg.name}-${k}`, HEAVY, thread, 'x'.repeat(300)])
          .catch(() => {})
      }, 50)
      timers.push(reply)
      let m = 0
      timers.push(
        setInterval(() => {
          admin
            .query(`insert into message (id, user_id, thread_id, record_id, created_at, payload) values ($1, $2, $3, null, $4, $5)`, [`steady-msg-${rep}-${cfg.name}-${m}`, HEAVY, thread, 100_000 + m++, 'y'.repeat(1900)])
            .catch(() => {})
        }, 5000),
      )
    }
    await new Promise((r) => setTimeout(r, HOLD_MS))
    result.longPollsHeld = sum((p) => p.longPolls)
    result.streamCount = result.longPollsHeld
    for (const t of timers) clearInterval(t)
    const gs = (await (await fetch(`${srv.gateway}/gw/stats`)).json()) as any
    result.phoneRequests = sum((p) => p.requests)
    result.bytesDown = sum((p) => p.bytesDown)
    result.gatewayCpuMs = Math.round(gs.burnedMs)
    result.gatewayLagP99 = Math.round(gs.lagP99)
    result.gatewayLagMax = Math.round(gs.lagMax)
    result.holdGatewayCpuMs = Math.round(gs.burnedMs - gsLoad.burnedMs)
    result.holdRequests = gs.requests - gsLoad.requests
    result.holdDsCalls = gs.dsCalls - gsLoad.dsCalls
    result.engineCpuMs = Math.round(cpuMs(srv.enginePid) - cpu0)
    result.engineRssMb = Math.round(rssMb(srv.enginePid) * 10) / 10
    if (STEADY) {
      result.steadyWriteP50 = writeLatencies.length ? Math.round(median(writeLatencies)) : null
      result.steadyWriteP90 = writeLatencies.length ? Math.round(quantile(writeLatencies, 0.9)) : null
    }
    const x1 = await pgStats()
    result.pgStatements = x1.calls - x0.calls
    result.pgExecMs = Math.round(x1.ms - x0.ms)
  } catch (e) {
    result.error = String((e as Error).message ?? e)
  } finally {
    for (const t of timers) clearInterval(t)
    await Promise.all(phones.map((p) => p.stop()))
    await srv.stop(admin)
  }
  return result
}

const median = (xs: number[]) => {
  const s = [...xs].sort((a, b) => a - b)
  return s.length ? s[Math.floor((s.length - 1) / 2)]! : NaN
}
const quantile = (xs: number[], q: number) => {
  const s = [...xs].sort((a, b) => a - b)
  return s.length ? s[Math.min(s.length - 1, Math.floor(q * s.length))]! : NaN
}

async function main() {
  const pgs = await bootPostgres()
  const admin = new pg.Client({ connectionString: pgs.url })
  await admin.connect()
  try {
    await admin.query('create extension pg_stat_statements')
    await admin.query(ddl())
    for (const ix of INDEXES) await admin.query(ix)
    const t = performance.now()
    const s: SeedSummary = await seed(admin, OTHER_USERS)
    console.log(`seeded in ${Math.round(performance.now() - t)} ms:`, JSON.stringify(s.rows))
    reopenChanged = s.heavy.recordClientIds.slice(0, 5)
    steadyThread = s.heavy.threads[0]!
    steadyRecordsByUser = Object.fromEntries(Object.entries(s.users).map(([u, ids]) => [u, ids.records[40] ?? ids.records[0]!]))
    const byVariant = { today: scenarios(s, 'today'), scoped: scenarios(s, 'scoped'), series: scenarios(s, 'series') }
    // Each phone's own copy of a scenario, built for its account.
    const perPhone = (variant: Config['variant'], name: string) =>
      Array.from({ length: PHONES }, (_, i) => scenarios(s, variant, phoneUser(i)).find((x) => x.name === name)!)
    const names = byVariant.scoped.map((x) => x.name).filter((n) => !ONLY || n.includes(ONLY))

    // Warm Postgres' buffer cache with one discarded pass so no design pays for a cold cache.
    for (const name of names) {
      const sc = byVariant.scoped.find((x) => x.name === name)!
      void sc
      await runOnce(admin, pgs.url, perPhone('scoped', name), { name: 'warm', variant: 'scoped', design: 'scope-load', slots: null, live: 'per-stream' }, 0)
    }

    const results: RunResult[] = []
    for (let rep = 1; rep <= REPS; rep++)
      for (const name of names)
        for (const cfg of CONFIGS) {
          const sc = byVariant[cfg.variant].find((x) => x.name === name)!
          if (cfg.resume && !sc.reopen) continue
          const r = await runOnce(admin, pgs.url, perPhone(cfg.variant, name), cfg, rep)
          results.push(r)
          console.log(
            `[${rep}] ${name.padEnd(14)} ${cfg.name.padEnd(17)} ` +
              Object.entries(r.screens)
                .filter(([, v]) => v.firstMs != null)
                .map(([k, v]) => `${k} first=${v.firstMs}ms`)
                .join(' ') +
              ` all=${Math.max(...Object.values(r.screens).map((v) => v.allMs))}ms req=${r.loadRequests}/${r.phoneRequests} gwCpu=${r.gatewayCpuMs}ms ` +
              `lagP99=${r.gatewayLagP99}ms engCpu=${r.engineCpuMs}ms pg=${r.pgStatements} stmts/${r.pgExecMs}ms polls=${r.longPollsHeld}` +
              (r.error ? ` ERROR ${r.error}` : ''),
          )
        }
    mkdirSync(dirname(OUT), { recursive: true })
    writeFileSync(OUT, JSON.stringify({ params: { RTT_MS, DOWN_MBPS, REPS, OTHER_USERS, HOLD_MS, PHONES, DS_LONGPOLL_MS, STEADY }, seed: s.rows, results }, null, 2))

    // Summary: medians over reps.
    console.log(`\nRTT ${RTT_MS} ms, downlink ${DOWN_MBPS} Mbps, ${PHONES} phone(s), ${REPS} reps (medians)\n`)
    console.log(
      '| Scenario | Design | First content | First content p90 (phones) | First table | Everything | Load requests | Gateway CPU (load) | Gateway lag p99 | Hold: gateway CPU | Hold: requests | Hold: upstream reads | Long-polls held/streams | Engine RSS MB | Write p50/p90 | PG statements | MB down |',
    )
    console.log('| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |')
    for (const name of names)
      for (const cfg of CONFIGS) {
        if (!results.some((r) => r.scenario === name && r.config === cfg.name)) continue
        const rs = results.filter((r) => r.scenario === name && r.config === cfg.name && !r.error)
        if (!rs.length) {
          console.log(`| ${name} | ${cfg.name} | error |`)
          continue
        }
        const m = (f: (r: RunResult) => number) => median(rs.map(f))
        const scr = (r: RunResult, k: 'firstMs' | 'firstP90' | 'firstTableMs' | 'allMs') =>
          Math.max(...Object.values(r.screens).map((v) => (v[k] as number | null | undefined) ?? 0))
        console.log(
          `| ${name} | ${cfg.name} | ${m((r) => scr(r, 'firstMs'))} ms | ${m((r) => scr(r, 'firstP90'))} ms | ${m((r) => scr(r, 'firstTableMs'))} ms | ` +
            `${m((r) => scr(r, 'allMs'))} ms | ${m((r) => r.loadRequests)} | ${m((r) => r.gatewayCpuMs - (r.holdGatewayCpuMs ?? 0))} ms | ` +
            `${m((r) => r.gatewayLagP99)} ms | ${m((r) => r.holdGatewayCpuMs ?? 0)} ms | ${m((r) => r.holdRequests ?? 0)} | ${m((r) => r.holdDsCalls ?? 0)} | ` +
            `${m((r) => r.longPollsHeld)} | ${m((r) => r.engineRssMb ?? 0).toFixed(1)} | ${m((r) => r.steadyWriteP50 ?? 0)}/${m((r) => r.steadyWriteP90 ?? 0)} ms | ` +
            `${m((r) => r.pgStatements)} | ${(m((r) => r.bytesDown) / 1e6).toFixed(2)} |`,
        )
      }
  } finally {
    await admin.end().catch(() => {})
    pgs.stop()
  }
}

main().catch((e) => {
  console.error(e)
  process.exit(1)
})
