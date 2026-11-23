// The Swift SDK after a resume, against the real engine, Postgres and durable-streams: when a
// scope that continued from a stored cursor reports live, and what a caller-requested snapshot
// reads. A companion to `conformance-swift-client.test.ts`: the same probe (`swift-probe.ts`) and
// harness, and the same build step (`swift build --product ElectricCircuitsSwiftProbe` in the SDK
// checkout). A group resumes by `POST /v1/subsets/rejoin` here; against the engine at `1be10fb`,
// which has no such route, the same SDK falls back to a load (that checkout's copy of this file).
//
// The stream server holds a long-poll at a stream's head for its poll window, 30 s here (the
// harness leaves the default). A scope that resumed with nothing new must not wait that out to
// report live, so these tests give it 5 s.
//
// Run: CARGO_PROFILE_DEV_DEBUG=0 COREPACK_ENABLE_DOWNLOAD_PROMPT=0 corepack pnpm exec vitest run \
//   packages/conformance/src/conformance-swift-client-resume.test.ts

import type { Row, Schema } from '@electric-circuits/protocol'
import { afterEach, describe, expect, it } from 'vitest'

import { pgQuery } from './engine-native.js'
import { bootHarness, drainEngine, type Harness } from './harness.js'
import { probeStoreDir, SwiftProbe, type ProbeEvent, type ProbeScope } from './swift-probe.js'

const schema: Schema = {
  tables: {
    lists: { columns: { id: { type: 'int' }, owner: { type: 'text' } }, primaryKey: 'id' },
    items: {
      columns: { id: { type: 'int' }, list_id: { type: 'int' }, title: { type: 'text' } },
      primaryKey: 'id',
    },
  },
}

const plain: ProbeScope = { name: 'plain', table: 'items', key: 'id', where: { col: 'list_id', op: 'eq', value: 1 } }
const group: ProbeScope = {
  name: 'group',
  group: [
    { table: 'lists', key: 'id', where: { col: 'owner', op: 'eq', value: 'u1' } },
    { table: 'items', key: 'id', where: { col: 'list_id', op: 'eq', value: 1 } },
  ],
}

const sql = {
  plain: 'SELECT id, list_id, title FROM items WHERE list_id = 1 ORDER BY id',
  lists: "SELECT id, owner FROM lists WHERE owner = 'u1' ORDER BY id",
}

/** How long a resumed scope may take to report live: far inside the 30 s poll window. */
const promptMs = 5000

let h: Harness | undefined
let stores: ReturnType<typeof probeStoreDir> | undefined
const probes: SwiftProbe[] = []

afterEach(async () => {
  delete process.env.ECS_SWIFT_PROBE_LIVE
  for (const probe of probes.splice(0)) await probe.kill()
  await h?.shutdown()
  h = undefined
  stores?.cleanup()
  stores = undefined
})

async function boot(engineEnv: Record<string, string> = {}): Promise<Harness> {
  h = await bootHarness(schema, { dsTransport: 'http', engineEnv })
  stores = probeStoreDir()
  await pgQuery(h, "INSERT INTO lists (id, owner) VALUES (1, 'u1'), (2, 'u2')")
  await pgQuery(h, "INSERT INTO items (id, list_id, title) VALUES (1, 1, 'one'), (2, 1, 'two'), (3, 2, 'three')")
  await drainEngine(h)
  return h
}

/** Start a probe on the shared store. `live` is the SDK's live read mode for this process. */
async function startProbe(label: string, live?: 'long-poll' | 'sse'): Promise<SwiftProbe> {
  if (live) process.env.ECS_SWIFT_PROBE_LIVE = live
  else delete process.env.ECS_SWIFT_PROBE_LIVE
  const probe = await SwiftProbe.start({ engineUrl: h!.engineUrl, store: `${stores!.dir}/store.json`, scopes: [plain, group], label })
  probes.push(probe)
  if (live) expect(probe.events[0]!.live).toBe(live)
  return probe
}

/** One process to live on both scopes, then killed holding its claims, as a suspended app is. */
async function firstProcess(): Promise<void> {
  const first = await startProbe('first')
  await first.waitForState('plain', 'live')
  await first.waitForState('group', 'live')
  expect(first.of('path', 'plain').map((e) => e.path)).toEqual(['snapshot'])
  expect(first.of('path', 'group').map((e) => e.path)).toEqual(['snapshot'])
  await first.kill()
}

function liveWithin(probe: SwiftProbe, scope: string, from = 0): Promise<ProbeEvent> {
  return probe.waitFor((e) => e.event === 'state' && e.scope === scope && e.state === 'live', `scope ${scope} to be live within ${promptMs} ms`, { from, timeoutMs: promptMs })
}

async function expectRowsMatchPostgres(probe: SwiftProbe): Promise<void> {
  expect(await probe.rows('plain')).toEqual(await pgQuery(h!, sql.plain))
  expect(await probe.groupRows('group')).toEqual({ lists: await pgQuery(h!, sql.lists), items: await pgQuery(h!, sql.plain) })
}

function note(title: string, value: unknown): void {
  console.log(`[swift-client-resume] ${title}: ${JSON.stringify(value)}`)
}

describe.each(['long-poll', 'sse'] as const)('Swift SDK against the engine: live after a resume (%s)', (live) => {
  it('reports a resumed scope and group live at once when the stream holds nothing new', async () => {
    await boot()
    await firstProcess()

    const second = await startProbe(`idle-${live}`, live)
    const liveEvents = { plain: await liveWithin(second, 'plain'), group: await liveWithin(second, 'group') }
    const started = second.events[0]!
    // The probe stamps its events in milliseconds.
    const elapsed = { plain: liveEvents.plain.at - started.at, group: liveEvents.group.at - started.at }
    note(`milliseconds from start to live (${live})`, elapsed)
    expect(elapsed.plain).toBeLessThan(promptMs)
    expect(elapsed.group).toBeLessThan(promptMs)

    // Both continued from the stored cursor: no frontier read, no snapshot, nothing applied.
    expect(second.of('path', 'plain').map((e) => e.path)).toEqual(['resume'])
    expect(second.of('path', 'group').map((e) => e.path)).toEqual(['resume'])
    expect(second.of('caught-up', 'plain')).toHaveLength(1)
    expect(second.of('caught-up', 'group')).toHaveLength(1)
    expect(second.of('change')).toEqual([])
    expect(second.of('snapshot')).toEqual([])
    const counters = await second.counters()
    expect(counters.plain).toMatchObject({ feedCreate: 1, streamHead: 0, snapshotQuery: 0 })
    // The group is re-joined, for which the engine reads no rows; nothing is loaded.
    expect(counters.group).toMatchObject({ groupRejoin: 1, groupLoad: 0, streamHead: 0, snapshotQuery: 0 })
    const stored = started.stored as Record<string, Row>
    for (const scope of ['plain', 'group']) {
      const first = second.of('request', scope).find((e) => e.kind === 'streamRead')!
      expect(first.offset).toBe(stored[scope]!.cursor)
      if (live === 'long-poll') {
        // The read that asks where the head is carries no `live`, and the stream answers it at
        // once: empty, at the same offset, marked up to date.
        expect(first).toMatchObject({ live: 'none', status: 200, next: stored[scope]!.cursor, upToDate: true })
        expect(first.envelopes).toBeUndefined()
      } else {
        expect(first.live).toBe('sse')
      }
    }
    await expectRowsMatchPostgres(second)

    // The tail is live from the same offset: a change made now arrives on it.
    const beforeChange = second.mark()
    await pgQuery(h!, "UPDATE items SET title = 'two-b' WHERE id = 2")
    await drainEngine(h!)
    for (const scope of ['plain', 'group']) {
      await second.waitFor((e) => e.event === 'change' && e.scope === scope && (e.row as Row | undefined)?.title === 'two-b', `the ${scope} tail to apply the live change`, { from: beforeChange, timeoutMs: promptMs })
    }
    await expectRowsMatchPostgres(second)
    expect(second.state('plain')).toBe('live')
    expect(second.state('group')).toBe('live')
  }, 120000)

  it('reports live only after every change made while it was down is applied', async () => {
    await boot()
    await firstProcess()
    // Three transactions while no process is running: the stream holds them all at the resume.
    for (const title of ['two-b', 'two-c', 'two-d']) {
      await pgQuery(h!, `UPDATE items SET title = '${title}' WHERE id = 2`)
      await drainEngine(h!)
    }

    const second = await startProbe(`behind-${live}`, live)
    await liveWithin(second, 'plain')
    await liveWithin(second, 'group')

    for (const scope of ['plain', 'group']) {
      expect(second.of('path', scope).map((e) => e.path)).toEqual(['resume'])
      // In the order the probe saw them: every change, then caught up, then live.
      const order = second.events
        .filter((e) => e.scope === scope && (e.event === 'change' || e.event === 'caught-up' || (e.event === 'state' && e.state === 'live')))
        .map((e) => (e.event === 'change' ? (e.row as Row).title : e.event === 'state' ? 'live' : e.event))
      note(`order (${live}, ${scope})`, order)
      expect(order).toEqual(['two-b', 'two-c', 'two-d', 'caught-up', 'live'])
    }
    const counters = await second.counters()
    expect(counters.plain).toMatchObject({ feedCreate: 1, streamHead: 0, snapshotQuery: 0 })
    expect(counters.group).toMatchObject({ groupRejoin: 1, groupLoad: 0, snapshotQuery: 0 })
    await expectRowsMatchPostgres(second)
  }, 120000)
})

describe('Swift SDK against the engine: a snapshot the caller asks for', () => {
  it('reads the source again on a snapshot refresh, where a plain refresh continues from the cursor', async () => {
    await boot()
    const probe = await startProbe('modes')
    await probe.waitForState('plain', 'live')
    await probe.waitForState('group', 'live')
    const afterLoad = await probe.counters()
    expect(afterLoad.plain).toMatchObject({ feedCreate: 1, streamHead: 1, snapshotQuery: 1 })
    expect(afterLoad.group).toMatchObject({ groupLoad: 1 })

    // A plain refresh may resume: one feed create (one group re-join), no frontier read, no page.
    const plainRefresh = probe.mark()
    probe.send({ cmd: 'refresh', scope: 'plain' })
    probe.send({ cmd: 'refresh', scope: 'group' })
    await liveWithin(probe, 'plain', plainRefresh)
    await liveWithin(probe, 'group', plainRefresh)
    expect(probe.of('path', 'plain', plainRefresh).map((e) => e.path)).toEqual(['resume'])
    expect(probe.of('path', 'group', plainRefresh).map((e) => e.path)).toEqual(['resume'])
    expect(probe.of('snapshot', undefined, plainRefresh)).toEqual([])
    const afterResume = await probe.counters()
    expect(afterResume.plain).toMatchObject({ feedCreate: 2, streamHead: 1, snapshotQuery: 1 })
    expect(afterResume.group).toMatchObject({ groupLoad: 1, groupRejoin: 1 })

    // A snapshot refresh does not ask to resume: frontier, page and a replaced snapshot.
    const snapshotRefresh = probe.mark()
    probe.send({ cmd: 'refresh', scope: 'plain', mode: 'snapshot' })
    probe.send({ cmd: 'refresh', scope: 'group', mode: 'snapshot' })
    await liveWithin(probe, 'plain', snapshotRefresh)
    await liveWithin(probe, 'group', snapshotRefresh)
    expect(probe.of('path', 'plain', snapshotRefresh).map((e) => e.path)).toEqual(['snapshot'])
    expect(probe.of('path', 'group', snapshotRefresh).map((e) => e.path)).toEqual(['snapshot'])
    expect(probe.of('snapshot', 'plain', snapshotRefresh)).toHaveLength(1)
    expect(probe.of('snapshot', 'group', snapshotRefresh)).toHaveLength(1)
    const afterSnapshot = await probe.counters()
    expect(afterSnapshot.plain).toMatchObject({ feedCreate: 3, streamHead: 2, snapshotQuery: 2 })
    expect(afterSnapshot.group).toMatchObject({ groupLoad: 2, groupRejoin: 1 })
    await expectRowsMatchPostgres(probe)

    // The snapshot is taken; a plain refresh after it resumes again.
    const again = probe.mark()
    probe.send({ cmd: 'refresh', scope: 'plain' })
    await liveWithin(probe, 'plain', again)
    expect(probe.of('path', 'plain', again).map((e) => e.path)).toEqual(['resume'])
    expect((await probe.counters()).plain).toMatchObject({ snapshotQuery: 2 })
  }, 120000)
})

describe('Swift SDK against the engine: a claim has an identity', () => {
  /** How many claims each member of the group holds, as the engine reports it. */
  async function held(members: Array<{ shapeId: string }>): Promise<number[]> {
    return Promise.all(
      members.map(async (member) => {
        const res = await fetch(`${h!.engineUrl}/shapes/${encodeURIComponent(member.shapeId)}`)
        return ((await res.json()) as { subscriptions: number }).subscriptions
      }),
    )
  }
  const releases = (probe: SwiftProbe, scope: string) => probe.of('request', scope).filter((e) => e.kind === 'release' && e.status === 200)

  it('names the claim it holds when it renews and releases, so an earlier launch’s release leaves a later launch’s claim', async () => {
    // A 9 s lease, so the SDK renews every 3 s and a renewal falls inside the test.
    await boot({ ELECTRIC_CIRCUITS_SUBSCRIPTION_LEASE_SECS: '9', ELECTRIC_CIRCUITS_RETENTION_SWEEP_SECS: '1' })

    // Launch 1 loads the group and stops cleanly: its release names the claim its load answered.
    const first = await startProbe('claim-first')
    await first.waitForState('plain', 'live')
    await first.waitForState('group', 'live')
    const load = first.of('request', 'group').find((e) => e.kind === 'groupLoad' && e.status === 200)!
    const members = load.members as Array<{ shapeId: string }>
    const firstClaim = load.claim as string
    expect(firstClaim, 'the load answers the identity of its claim').toBeTypeOf('string')
    expect(load.named, 'a launch that holds no claim names none').toBeUndefined()
    await first.stop({ release: true })
    const firstRelease = releases(first, 'group').at(-1)!
    expect(firstRelease.named, 'the release names the claim the launch held').toBe(firstClaim)
    expect(await held(members)).toEqual([0, 0])

    // Launch 2 resumes the stored group in a new process, under the stored subscription. It
    // stored no claim and names none, so the engine answers it one of its own.
    const second = await startProbe('claim-second')
    await liveWithin(second, 'plain')
    await liveWithin(second, 'group')
    expect(second.of('path', 'group').map((e) => e.path)).toEqual(['resume'])
    const rejoin = second.of('request', 'group').find((e) => e.kind === 'groupRejoin' && e.status === 200)!
    expect(rejoin.subscription).toBe(firstRelease.subscription)
    expect(rejoin.named).toBeUndefined()
    const secondClaim = rejoin.claim as string
    expect(secondClaim).toBeTypeOf('string')
    expect(secondClaim).not.toBe(firstClaim)
    expect(await held(members)).toEqual([1, 1])

    // Launch 1's release arrives again, exactly as the SDK sent it.
    const replayed = await fetch(
      `${firstRelease.url as string}?subscription=${encodeURIComponent(firstRelease.subscription as string)}&claim=${encodeURIComponent(firstClaim)}`,
      { method: 'DELETE' },
    )
    expect(replayed.status).toBe(200)
    expect(await held(members), 'launch 2 still holds the group').toEqual([1, 1])

    // Launch 2's lease renewals name the claim it holds, and the engine keeps it: for the
    // group (a re-join) and for the single feed (a repeat of its create).
    const renewal = await second.waitFor((e) => e.event === 'request' && e.scope === 'group' && e.kind === 'groupRejoin' && e.status === 200 && e.named !== undefined, 'the group’s lease renewal', { timeoutMs: 15000 })
    expect(renewal.named).toBe(secondClaim)
    expect(renewal.claim, 'a renewal by the claim’s holder keeps its identity').toBe(secondClaim)
    const feedClaim = second.of('request', 'plain').find((e) => e.kind === 'feedCreate' && e.status === 200)!.claim as string
    const feedRenewal = await second.waitFor((e) => e.event === 'request' && e.scope === 'plain' && e.kind === 'feedCreate' && e.status === 200 && e.named !== undefined, 'the feed’s lease renewal', { timeoutMs: 15000 })
    expect(feedRenewal.named).toBe(feedClaim)
    expect(feedRenewal.claim).toBe(feedClaim)
    expect(await held(members)).toEqual([1, 1])

    // After those renewals its own release still names the claim it holds, and drops it.
    await second.stop({ release: true })
    expect(releases(second, 'group').at(-1)!.named).toBe(secondClaim)
    expect(releases(second, 'plain').at(-1)!.named).toBe(feedClaim)
    expect(await held(members), 'launch 2’s own release dropped its claim').toEqual([0, 0])
  }, 120000)
})
