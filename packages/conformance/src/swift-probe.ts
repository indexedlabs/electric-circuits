// Drives the Swift SDK's probe (`ElectricCircuitsSwiftProbe` in the electric-circuits-swift
// package) from a conformance test: spawn it against this harness's engine, read the JSON lines it
// prints, and send it commands. The probe runs the SDK's real collection stack
// (`CollectionCoordinator` + `CircuitsSubsetSource` / `CircuitsSubsetGroupSource`) with a
// file-backed store, so a second probe started on the same store file is a relaunch.
//
// The binary is `ECS_SWIFT_PROBE_BIN`, or `.build/debug/ElectricCircuitsSwiftProbe` under
// `ECS_SWIFT_SDK` (default: the sibling checkout `../electric-circuits-swift-wt-engine-contract`).
// Build it first with `swift build --product ElectricCircuitsSwiftProbe` in that checkout.

import { type ChildProcess, spawn } from 'node:child_process'
import { appendFileSync, existsSync, mkdirSync, mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

export interface ProbeEvent {
  event: string
  at: number
  scope?: string
  [field: string]: unknown
}

export interface ProbeMember {
  table: string
  key: string
  where?: unknown
  columns?: string[]
}

/** One table (`table`, `key`, `where`) or a group of tables (`group`). */
export type ProbeScope = ({ name: string } & ProbeMember) | { name: string; group: ProbeMember[] }

export type ProbeCounters = Record<string, Record<string, number>>

export interface ProbeOptions {
  engineUrl: string
  /** The store file. A probe started on a file an earlier probe wrote continues from it. */
  store: string
  scopes: ProbeScope[]
  /** The scope's generation; a new one per process when omitted, as an app launch has. */
  generation?: string
  /** Label for the evidence file (`ECS_SWIFT_PROBE_EVIDENCE`). */
  label?: string
}

export function probeBin(): string {
  const here = dirname(fileURLToPath(import.meta.url))
  const sdk = process.env.ECS_SWIFT_SDK ?? resolve(here, '../../../../electric-circuits-swift-wt-engine-contract')
  const bin = process.env.ECS_SWIFT_PROBE_BIN ?? join(sdk, '.build/debug/ElectricCircuitsSwiftProbe')
  if (!existsSync(bin)) {
    throw new Error(`Swift probe not built at ${bin}: run \`swift build --product ElectricCircuitsSwiftProbe\` in the SDK checkout, or set ECS_SWIFT_PROBE_BIN`)
  }
  return bin
}

/** A fresh directory for one test's store files; removed by `cleanup()`. */
export function probeStoreDir(): { dir: string; cleanup(): void } {
  const dir = mkdtempSync(join(tmpdir(), 'ecs-swift-probe-'))
  return { dir, cleanup: () => rmSync(dir, { recursive: true, force: true }) }
}

interface Waiter {
  from: number
  match: (event: ProbeEvent) => boolean
  resolve: (event: ProbeEvent) => void
}

export class SwiftProbe {
  readonly events: ProbeEvent[] = []
  private waiters: Waiter[] = []
  private stderr = ''
  private exit: { code: number | null; signal: NodeJS.Signals | null } | undefined
  private exitListeners: Array<() => void> = []
  private nextId = 0

  private constructor(
    private readonly proc: ChildProcess,
    private readonly label: string,
  ) {
    let pending = ''
    proc.stdout!.on('data', (chunk: Buffer) => {
      pending += chunk.toString()
      for (;;) {
        const end = pending.indexOf('\n')
        if (end < 0) break
        const line = pending.slice(0, end)
        pending = pending.slice(end + 1)
        if (line.trim()) this.accept(JSON.parse(line) as ProbeEvent, line)
      }
    })
    proc.stderr!.on('data', (chunk: Buffer) => {
      this.stderr += chunk.toString()
    })
    proc.on('exit', (code, signal) => {
      this.exit = { code, signal }
      for (const listener of this.exitListeners.splice(0)) listener()
    })
  }

  /** Spawn the probe and wait until it reports `started`. */
  static async start(options: ProbeOptions): Promise<SwiftProbe> {
    const args = ['--engine', options.engineUrl, '--store', options.store]
    if (options.generation) args.push('--generation', options.generation)
    for (const scope of options.scopes) args.push('--scope', JSON.stringify(scope))
    const proc = spawn(probeBin(), args, { stdio: ['pipe', 'pipe', 'pipe'] })
    const probe = new SwiftProbe(proc, options.label ?? 'probe')
    await probe.waitFor((event) => event.event === 'started', 'the probe to start')
    return probe
  }

  private accept(event: ProbeEvent, line: string): void {
    this.events.push(event)
    const evidence = process.env.ECS_SWIFT_PROBE_EVIDENCE
    if (evidence) {
      mkdirSync(evidence, { recursive: true })
      appendFileSync(join(evidence, `${this.label}.jsonl`), `${line}\n`)
    }
    const index = this.events.length - 1
    this.waiters = this.waiters.filter((waiter) => {
      if (index < waiter.from || !waiter.match(event)) return true
      waiter.resolve(event)
      return false
    })
  }

  /**
   * The first event at or after index `from` that matches. Event-driven: it resolves when the
   * probe prints the event, and fails if the probe exits or `timeoutMs` passes first.
   */
  waitFor(match: (event: ProbeEvent) => boolean, what: string, opts: { from?: number; timeoutMs?: number } = {}): Promise<ProbeEvent> {
    const from = opts.from ?? 0
    const seen = this.events.slice(from).find(match)
    if (seen) return Promise.resolve(seen)
    return new Promise<ProbeEvent>((resolvePromise, reject) => {
      const fail = (reason: string) => {
        this.waiters = this.waiters.filter((waiter) => waiter !== waiter_)
        reject(new Error(`${reason} waiting for ${what}\nlast events:\n${this.tail()}\nstderr:\n${this.stderr}`))
      }
      const timer = setTimeout(() => fail('timed out'), opts.timeoutMs ?? 30000)
      const onExit = () => {
        clearTimeout(timer)
        fail(`the probe exited (${JSON.stringify(this.exit)})`)
      }
      const waiter_: Waiter = {
        from,
        match,
        resolve: (event) => {
          clearTimeout(timer)
          this.exitListeners = this.exitListeners.filter((listener) => listener !== onExit)
          resolvePromise(event)
        },
      }
      if (this.exit) return onExit()
      this.exitListeners.push(onExit)
      this.waiters.push(waiter_)
    })
  }

  /** The index the next event will have: pass it as `from` to wait only for what happens next. */
  mark(): number {
    return this.events.length
  }

  private tail(count = 25): string {
    return this.events
      .slice(-count)
      .map((event) => JSON.stringify(event))
      .join('\n')
  }

  send(command: Record<string, unknown>): void {
    this.proc.stdin!.write(`${JSON.stringify(command)}\n`)
  }

  private async ask(cmd: string, event: string, scope?: string): Promise<ProbeEvent[]> {
    const id = `${cmd}-${++this.nextId}`
    const from = this.mark()
    this.send({ cmd, id, ...(scope ? { scope } : {}) })
    // The probe answers a command's events, then the `sync` that follows it, in order.
    this.send({ cmd: 'sync', id })
    await this.waitFor((e) => e.event === 'sync' && e.id === id, `the probe to answer ${cmd}`, { from })
    return this.events.slice(from).filter((e) => e.event === event && e.id === id)
  }

  /** A single-table scope's rows as the store holds them, in key order. */
  async rows(scope: string): Promise<Array<Record<string, unknown>>> {
    const [answer] = await this.ask('rows', 'rows', scope)
    return (answer?.rows ?? []) as Array<Record<string, unknown>>
  }

  /** A group scope's rows by table. */
  async groupRows(scope: string): Promise<Record<string, Array<Record<string, unknown>>>> {
    const [answer] = await this.ask('rows', 'rows', scope)
    return (answer?.tables ?? {}) as Record<string, Array<Record<string, unknown>>>
  }

  /** Requests by scope and kind since the process started. */
  async counters(): Promise<ProbeCounters> {
    const [answer] = await this.ask('counters', 'counters')
    return (answer?.counters ?? {}) as ProbeCounters
  }

  async position(scope: string): Promise<ProbeEvent> {
    const [answer] = await this.ask('position', 'position', scope)
    return answer!
  }

  /** Events of one kind for one scope, optionally from an index on. */
  of(event: string, scope?: string, from = 0): ProbeEvent[] {
    return this.events.slice(from).filter((e) => e.event === event && (scope === undefined || e.scope === scope))
  }

  /** The state a scope last reported. */
  state(scope: string): string | undefined {
    return this.of('state', scope).at(-1)?.state as string | undefined
  }

  waitForState(scope: string, state: string, from = 0): Promise<ProbeEvent> {
    return this.waitFor((e) => e.event === 'state' && e.scope === scope && e.state === state, `scope ${scope} to be ${state}`, { from })
  }

  private waitForExit(): Promise<void> {
    if (this.exit) return Promise.resolve()
    return new Promise((resolvePromise) => this.exitListeners.push(resolvePromise))
  }

  /**
   * Stop the process. `release: true` releases every lease first (the SDK sends its claim
   * releases); `release: false` exits holding the claims. Returns the final request counters.
   */
  async stop(opts: { release: boolean }): Promise<ProbeCounters> {
    if (this.exit) return this.lastCounters()
    this.send({ cmd: 'stop', release: opts.release })
    await this.waitForExit()
    return this.lastCounters()
  }

  /** Kill the process without warning, as the system kills a suspended app. */
  async kill(): Promise<void> {
    if (this.exit) return
    this.proc.kill('SIGKILL')
    await this.waitForExit()
  }

  private lastCounters(): ProbeCounters {
    return (this.of('counters').at(-1)?.counters ?? {}) as ProbeCounters
  }
}
