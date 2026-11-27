// The visualizer's node state is built only for a reader (OTTO-14463), against a running engine
// on real Postgres replication.
//
// The node-state map has an entry per shape, so building it costs time in proportion to every
// shape the engine holds. The sequencer used to build it after every page of commits and every
// shape change, whether or not anyone read it. The contract checked here:
//
// - nobody reading: commits and shape changes build nothing;
// - `GET /state`: one build on request, and the answer is current after commits and after a shape
//   is added or removed;
// - a `/trace` subscriber: the state arrives after each commit while it is connected, and the
//   building stops again once it leaves;
// - `ELECTRIC_CIRCUITS_TRACE=0`, the production setting: the routes are not served, nothing is
//   ever built, and the per-table stats (`GET /tables/:name/families`) stay current.
//
// How often the map was built is the engine's own counter, `node_state_builds_total` on
// `GET /metrics`, and `trace_subscribers` beside it says how many clients are on `/trace`. Order
// comes from those two, from `drainEngine` and from the state events themselves, never from a sleep.

import type { Schema, ShapeDef } from '@electric-circuits/protocol'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { createShape, type ShapeResp, streamKeys } from './engine-native.js'
import { applyOp, bootHarness, drainEngine, type Harness } from './harness.js'

const schema: Schema = {
  tables: {
    viz_items: {
      columns: { id: { type: 'int' }, grp: { type: 'int' }, label: { type: 'text' } },
      primaryKey: 'id',
    },
  },
}
const TABLE_NODE = 'table:public.viz_items'

type Where = NonNullable<ShapeDef['where']>
const inGroup = (grp: number): Where => ({ col: 'grp', op: 'eq', value: grp })

/** One node's summary as `GET /state` and a `/trace` state event carry it. */
interface NodeState {
  kind: string
  processedOffset?: string
  envelopes?: number
  emitted?: number
}
type Nodes = Record<string, NodeState>

async function insert(h: Harness, id: number, grp: number): Promise<void> {
  await applyOp(h, 'viz_items', { op: 'insert', pk: String(id), row: { id, grp, label: `row ${id}` } })
}

interface Readers {
  /** `node_state_builds_total`: how many times the engine has built the node-state map. */
  builds: number
  /** `trace_subscribers`: clients on `/trace` right now. */
  subscribers: number
}
async function readers(h: Harness): Promise<Readers> {
  const res = await fetch(`${h.engineUrl}/metrics`)
  if (!res.ok) throw new Error(`GET /metrics -> ${res.status}`)
  const body = (await res.json()) as { counters: Record<string, number>; gauges: Record<string, number> }
  const builds = body.counters.node_state_builds_total
  const subscribers = body.gauges.trace_subscribers
  if (builds === undefined || subscribers === undefined) {
    throw new Error('GET /metrics has no node_state_builds_total counter or trace_subscribers gauge')
  }
  return { builds, subscribers }
}

async function state(h: Harness): Promise<Nodes> {
  const res = await fetch(`${h.engineUrl}/state`)
  if (!res.ok) throw new Error(`GET /state -> ${res.status}`)
  return ((await res.json()) as { nodes: Nodes }).nodes
}

/** How many shapes the per-table stats count on `viz_items`, whichever way they are filed. */
async function shapesInStats(h: Harness): Promise<number> {
  const res = await fetch(`${h.engineUrl}/tables/viz_items/families`)
  if (!res.ok) throw new Error(`GET /tables/viz_items/families -> ${res.status}`)
  const stats = (await res.json()) as { families: { shapes: number }[]; standalone: number; circuit: number }
  return stats.families.reduce((sum, family) => sum + family.shapes, 0) + stats.standalone + stats.circuit
}

async function purge(h: Harness, shape: ShapeResp): Promise<void> {
  const res = await fetch(`${h.engineUrl}/shapes/${shape.shapeId}?purge=true`, { method: 'DELETE' })
  if (!res.ok) throw new Error(`DELETE /shapes/${shape.shapeId}?purge=true -> ${res.status} ${await res.text()}`)
}

async function within<T>(label: string | (() => string), wait: () => Promise<T | undefined>): Promise<T> {
  const deadline = Date.now() + 30_000
  for (;;) {
    const value = await wait()
    if (value !== undefined) return value
    if (Date.now() > deadline) throw new Error(`deadline: ${typeof label === 'string' ? label : label()}`)
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
}

/** A client on the engine's `/trace` SSE feed that keeps the `{"type":"state"}` events it is sent. */
async function subscribeToTrace(h: Harness) {
  const abort = new AbortController()
  const res = await fetch(`${h.engineUrl}/trace`, { signal: abort.signal })
  if (!res.ok || !res.body) throw new Error(`GET /trace -> ${res.status}`)
  const states: Nodes[] = []
  let failure: unknown
  let closed = false
  const reading = (async () => {
    const decoder = new TextDecoder()
    let buffered = ''
    for await (const chunk of res.body as unknown as AsyncIterable<Uint8Array>) {
      buffered += decoder.decode(chunk, { stream: true })
      for (let end = buffered.indexOf('\n\n'); end >= 0; end = buffered.indexOf('\n\n')) {
        const frame = buffered.slice(0, end)
        buffered = buffered.slice(end + 2)
        const data = frame
          .split('\n')
          .filter((line) => line.startsWith('data:'))
          .map((line) => line.slice('data:'.length).trimStart())
          .join('\n')
        if (!data) continue // a keep-alive comment
        const event = JSON.parse(data) as { type?: string; nodes?: Nodes }
        if (event.type === 'state' && event.nodes) states.push(event.nodes)
      }
    }
  })().catch((error: unknown) => {
    if (!closed) failure = error
  })
  return {
    /** The first state event from `from` onwards that `matches`, and the index after it. */
    async nextState(label: string, from: number, matches: (nodes: Nodes) => boolean): Promise<number> {
      const waitingFor = () => `a /trace state event: ${label}; the last one sent was ${JSON.stringify(states.at(-1))}`
      return within(waitingFor, async () => {
        if (failure) throw failure
        const at = states.findIndex((nodes, index) => index >= from && matches(nodes))
        return at < 0 ? undefined : at + 1
      })
    },
    async close(): Promise<void> {
      closed = true
      abort.abort()
      await reading
    },
  }
}

describe('conformance: the visualizer state is built only for a reader (OTTO-14463)', () => {
  let h: Harness
  let odd: ShapeResp
  /** Changes committed to `viz_items` since the engine booted. */
  let committed = 0
  /** Envelopes the engine has appended to the `grp = 1` shape's stream. */
  let sentToOdd = 0
  const oddKeys: string[] = []

  const insertOdd = async (id: number) => {
    await insert(h, id, 1)
    committed += 1
    sentToOdd += 1
    oddKeys.push(String(id))
  }
  const insertEven = async (id: number) => {
    await insert(h, id, 2)
    committed += 1
  }

  beforeAll(async () => {
    h = await bootHarness(schema)
  }, 60000)
  afterAll(async () => {
    await h?.shutdown()
  })

  it('builds nothing for a new shape and five commits while nobody is reading', async () => {
    const before = await readers(h)
    expect(before.subscribers).toBe(0)

    odd = await createShape(h, { table: 'viz_items', where: inGroup(1) })
    await insertOdd(1)
    await insertEven(2)
    await insertOdd(3)
    await insertEven(4)
    await insertOdd(5)
    await drainEngine(h)
    expect(await streamKeys(odd.streamUrl), 'the engine processed the commits').toEqual(oddKeys)

    const after = await readers(h)
    expect(after.builds - before.builds, 'node-state builds with no reader').toBe(0)
  })

  it('answers GET /state with the current map, built once for the request', async () => {
    const before = await readers(h)
    const nodes = await state(h)
    const after = await readers(h)

    expect(nodes[TABLE_NODE]).toMatchObject({ kind: 'table', envelopes: committed })
    expect(nodes[TABLE_NODE]?.processedOffset, 'the table has a processed position').not.toBe('-1')
    expect(nodes[`shape:${odd.shapeId}`]).toEqual({ kind: 'shape', emitted: sentToOdd })
    expect(after.builds - before.builds, 'node-state builds for one GET /state').toBe(1)
  })

  it('shows a shape added, a shape removed and a later commit in the next GET /state', async () => {
    const even = await createShape(h, { table: 'viz_items', where: inGroup(2) })
    // Its backfill: the two `grp = 2` rows committed before it existed.
    expect((await state(h))[`shape:${even.shapeId}`]).toEqual({ kind: 'shape', emitted: 2 })

    await purge(h, even)
    const afterPurge = await state(h)
    expect(afterPurge[`shape:${even.shapeId}`], 'a purged shape leaves the state').toBeUndefined()
    expect(afterPurge[`shape:${odd.shapeId}`]).toEqual({ kind: 'shape', emitted: sentToOdd })

    await insertOdd(6)
    await drainEngine(h)
    const afterCommit = await state(h)
    expect(afterCommit[TABLE_NODE]).toMatchObject({ kind: 'table', envelopes: committed })
    expect(afterCommit[`shape:${odd.shapeId}`]).toEqual({ kind: 'shape', emitted: sentToOdd })
  })

  it('sends a /trace subscriber the state after each commit, and stops building once it leaves', async () => {
    const trace = await subscribeToTrace(h)
    let builtWhileSubscribed: number
    try {
      await within('the engine counts the /trace subscriber', async () =>
        (await readers(h)).subscribers === 1 ? true : undefined,
      )
      const before = await readers(h)
      let seen = 0
      for (const id of [7, 8, 9]) {
        await insertOdd(id)
        seen = await trace.nextState(
          `row ${id} committed (${committed} table envelopes, ${sentToOdd} sent to the shape)`,
          seen,
          (nodes) =>
            nodes[TABLE_NODE]?.envelopes === committed && nodes[`shape:${odd.shapeId}`]?.emitted === sentToOdd,
        )
      }
      await drainEngine(h)
      builtWhileSubscribed = (await readers(h)).builds - before.builds
    } finally {
      await trace.close()
    }
    expect(builtWhileSubscribed, 'one build per commit page while subscribed').toBeGreaterThanOrEqual(3)

    await within('the engine sees the /trace subscriber leave', async () =>
      (await readers(h)).subscribers === 0 ? true : undefined,
    )
    const left = await readers(h)
    for (const id of [10, 11, 12, 13, 14]) await insertOdd(id)
    await drainEngine(h)
    expect(await streamKeys(odd.streamUrl), 'the engine processed the commits').toEqual(oddKeys)
    const end = await readers(h)
    expect(end.builds - left.builds, 'node-state builds after the subscriber left').toBe(0)

    // A reader that asks still gets the current state.
    const nodes = await state(h)
    expect(nodes[TABLE_NODE]).toMatchObject({ kind: 'table', envelopes: committed })
    expect(nodes[`shape:${odd.shapeId}`]).toEqual({ kind: 'shape', emitted: sentToOdd })
  })
})

describe('conformance: with introspection off (ELECTRIC_CIRCUITS_TRACE=0) the engine never builds the visualizer state', () => {
  let h: Harness

  beforeAll(async () => {
    h = await bootHarness(schema, { engineEnv: { ELECTRIC_CIRCUITS_TRACE: '0' } })
  }, 60000)
  afterAll(async () => {
    await h?.shutdown()
  })

  it('does not serve the visualizer routes', async () => {
    for (const path of ['/state', '/trace', `/state/node?id=${encodeURIComponent(TABLE_NODE)}`, '/graph']) {
      const res = await fetch(`${h.engineUrl}${path}`)
      await res.body?.cancel()
      expect(res.status, `GET ${path}`).toBe(404)
    }
  })

  it('processes commits and shape changes without one build, and keeps the per-table stats current', async () => {
    const odd = await createShape(h, { table: 'viz_items', where: inGroup(1) })
    const even = await createShape(h, { table: 'viz_items', where: inGroup(2) })
    await within('the stats count both shapes', async () => ((await shapesInStats(h)) === 2 ? true : undefined))

    for (const id of [1, 2, 3, 4, 5]) await insert(h, id, id % 2 === 1 ? 1 : 2)
    await drainEngine(h)
    expect(await streamKeys(odd.streamUrl)).toEqual(['1', '3', '5'])
    expect(await streamKeys(even.streamUrl)).toEqual(['2', '4'])

    await purge(h, even)
    await within('the stats drop the purged shape', async () => ((await shapesInStats(h)) === 1 ? true : undefined))
    await insert(h, 7, 1)
    await drainEngine(h)
    expect(await streamKeys(odd.streamUrl)).toEqual(['1', '3', '5', '7'])

    expect(await readers(h), 'since boot').toEqual({ builds: 0, subscribers: 0 })
  })
})
