// Native shape handles promise a durable, appendable stream. Storage failures must not turn an
// acknowledged shape into a silently stale stream or cause a new subscriber to receive a dead URL.

import { createServer, request } from 'node:http'

import type { Schema } from '@electric-circuits/protocol'
import { afterEach, describe, expect, it } from 'vitest'

import { createShape, foldStream, pgQuery, waitFor } from './engine-native.js'
import { bootHarness, drainEngine, type Harness } from './harness.js'
import { testMutualTlsRequest, testPhysicalPath } from './ds-mtls-access.js'

const schema: Schema = {
  tables: {
    items: {
      columns: { id: { type: 'int' }, payload: { type: 'text' } },
      primaryKey: 'id',
    },
  },
}

let h: Harness | undefined
afterEach(async () => {
  await h?.shutdown()
  h = undefined
})

interface AppendFault {
  armed: boolean
  status: number
  hits: number
  headArmed?: boolean
  headStatus?: number
  headHits?: number
}

function oneShotShapeAppendProxy(fault: AppendFault) {
  return async (upstreamUrl: string) => {
    const upstream = new URL(upstreamUrl)
    const server = createServer((incoming, outgoing) => {
      const target = new URL(incoming.url ?? '/', upstream)
      if (fault.headArmed && incoming.method === 'HEAD' && target.pathname.startsWith(`/${testPhysicalPath('shape/')}`)) {
        fault.headArmed = false
        fault.headHits = (fault.headHits ?? 0) + 1
        incoming.resume()
        outgoing.writeHead(fault.headStatus ?? 503, { 'content-type': 'text/plain' })
        outgoing.end('one-shot HEAD failure')
        return
      }
      if (fault.armed && incoming.method === 'POST' && target.pathname.startsWith(`/${testPhysicalPath('shape/')}`)) {
        fault.armed = false
        fault.hits += 1
        incoming.resume()
        outgoing.writeHead(fault.status, { 'content-type': 'text/plain' })
        outgoing.end('one-shot storage failure')
        return
      }
      const forwarded = request(
        target,
        { method: incoming.method, headers: { ...incoming.headers, host: upstream.host } },
        (response) => {
          outgoing.writeHead(response.statusCode ?? 502, response.headers)
          response.pipe(outgoing)
        },
      )
      forwarded.on('error', (error) => {
        if (!outgoing.headersSent) outgoing.writeHead(502)
        outgoing.end(String(error))
      })
      incoming.pipe(forwarded)
    })
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject)
      server.listen(0, '127.0.0.1', resolve)
    })
    const address = server.address()
    if (!address || typeof address === 'string') throw new Error('proxy did not bind')
    return {
      url: `http://127.0.0.1:${address.port}`,
      close: () => new Promise<void>((resolve, reject) => server.close((e) => (e ? reject(e) : resolve()))),
    }
  }
}

describe('native shape storage loss', () => {
  it('does not silently lose a PostgreSQL change after one false 404 from the shape-stream service', async () => {
    const fault: AppendFault = { armed: false, status: 404, hits: 0 }
    h = await bootHarness(schema, {
      wrapEngineDs: oneShotShapeAppendProxy(fault),
    })

    const shape = await createShape(h, { table: 'items' })
    fault.armed = true
    await pgQuery(h, 'INSERT INTO items (id, payload) VALUES (1, $1)', ['lost'])
    await drainEngine(h)
    expect(fault.hits).toBe(1)

    await pgQuery(h, 'INSERT INTO items (id, payload) VALUES (2, $1)', ['later'])
    await drainEngine(h)
    const rows = await foldStream(shape.streamUrl)
    expect(rows.has('1'), 'the change rejected once by storage must not be discarded').toBe(true)
    expect(rows.has('2'), 'the same returned stream remains healthy for later changes').toBe(true)
  })

  it('does not join a retained shape whose returned durable stream has disappeared', async () => {
    h = await bootHarness(schema)
    await pgQuery(h, 'INSERT INTO items (id, payload) VALUES (1, $1)', ['present'])
    await drainEngine(h)

    const first = await createShape(h, { table: 'items' })
    expect((await testMutualTlsRequest(first.streamUrl, 'DELETE')).ok).toBe(true)
    expect((await fetch(first.streamUrl)).status).toBe(404)

    const replacement = await createShape(h, { table: 'items' })
    expect(replacement.shapeId, 'a new subscription must not reuse the known-dead handle').not.toBe(first.shapeId)
    expect((await fetch(replacement.streamUrl)).status).toBe(200)
  })

  it('does not return a dead retained handle when its storage check is transiently unavailable', async () => {
    const fault: AppendFault = { armed: false, status: 503, hits: 0 }
    h = await bootHarness(schema, { wrapEngineDs: oneShotShapeAppendProxy(fault) })
    const first = await createShape(h, { table: 'items' })
    expect((await testMutualTlsRequest(first.streamUrl, 'DELETE')).ok).toBe(true)
    expect((await fetch(first.streamUrl)).status).toBe(404)

    // The stream is really gone, but the engine sees one ordinary transient failure while checking
    // that external service. Uncertainty must not be converted into a successful known-dead handle.
    fault.headArmed = true
    fault.headStatus = 503
    const replacement = await createShape(h, { table: 'items' })
    expect(fault.headHits).toBe(1)
    expect(replacement.shapeId).not.toBe(first.shapeId)
    expect((await fetch(replacement.streamUrl)).status).toBe(200)
  })

  // #v-gone-append: a stream the store removed behind the engine (an expired TTL, an operator
  // delete) retires its shape on the next append; the sequencer keeps applying every other shape's
  // changes, and the client refetches into a fresh shape.
  it('retires a shape whose stream the store removed and keeps applying other shapes changes', async () => {
    h = await bootHarness(schema)
    const where = 'id < 100'
    const snapshot = await fetch(`${h.engineUrl}/v1/shape?${new URLSearchParams({ table: 'items', offset: '-1', where })}`)
    expect(snapshot.status).toBe(200)
    const handle = snapshot.headers.get('electric-handle') as string
    const offset = snapshot.headers.get('electric-offset') as string
    await snapshot.text()
    // A handle is `<shapeId>h<scope>.<claim>` over the engine shape.
    const goneId = handle.split('h')[0]
    const gone = (await (await fetch(`${h.engineUrl}/shapes/${goneId}`)).json()) as { streamUrl: string }
    const other = await createShape(h, { table: 'items', where: { col: 'id', op: 'gte', value: 100 } })

    expect((await testMutualTlsRequest(gone.streamUrl, 'DELETE')).ok).toBe(true)
    // One committed transaction carries a row for the gone stream and a row for the healthy shape.
    await pgQuery(h, 'INSERT INTO items (id, payload) VALUES (1, $1), (101, $2)', [
      'to the removed stream',
      'to the other shape in the same transaction',
    ])
    await drainEngine(h)

    await waitFor(
      async () => (await fetch(`${h!.engineUrl}/shapes/${goneId}`)).status === 404,
      'the shape whose stream is gone to be retired',
    )
    expect((await foldStream(other.streamUrl)).has('101'), 'the healthy shape gets its row of the same transaction').toBe(true)

    const stale = await fetch(`${h.engineUrl}/v1/shape?${new URLSearchParams({ table: 'items', offset, handle, where })}`)
    expect(stale.status).toBe(409)
    const control = (await stale.json()) as Array<{ headers: { control?: string } }>
    expect(control[0]?.headers.control).toBe('must-refetch')

    const refetch = await fetch(`${h.engineUrl}/v1/shape?${new URLSearchParams({ table: 'items', offset: '-1', where })}`)
    expect(refetch.status).toBe(200)
    expect(refetch.headers.get('electric-handle')).not.toBe(handle)
    const rows = (await refetch.json()) as Array<{ value?: { id?: unknown } }>
    expect(rows.some((m) => String(m.value?.id) === '1'), 'the refetch carries the change').toBe(true)

    await pgQuery(h, 'INSERT INTO items (id, payload) VALUES (102, $1)', ['after the retirement'])
    await drainEngine(h)
    expect((await foldStream(other.streamUrl)).has('102')).toBe(true)
  })

  it('releases a group whose shared stream the store removed and keeps applying other shapes changes', async () => {
    h = await bootHarness(schema)
    const load = async () => {
      const res = await fetch(`${h!.engineUrl}/v1/subsets/load`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          subscription: 'gone-group',
          members: [{ table: 'items', where: { col: 'id', op: 'lt', value: 100 } }],
        }),
      })
      expect(res.status).toBe(200)
      return (await res.json()) as { streamUrl: string; results: Array<{ shapeId: string }> }
    }
    const group = await load()
    const member = group.results[0]!.shapeId
    expect((await fetch(`${h.engineUrl}/shapes/${member}`)).status).toBe(200)
    const other = await createShape(h, { table: 'items', where: { col: 'id', op: 'gte', value: 100 } })

    expect((await testMutualTlsRequest(group.streamUrl, 'DELETE')).ok).toBe(true)
    await pgQuery(h, 'INSERT INTO items (id, payload) VALUES (2, $1), (103, $2)', [
      'to the removed group stream',
      'to the other shape in the same transaction',
    ])
    await drainEngine(h)

    await waitFor(
      async () => (await fetch(`${h!.engineUrl}/shapes/${member}`)).status === 404,
      'the group whose stream is gone to be released',
    )
    expect((await foldStream(other.streamUrl)).has('103'), 'the healthy shape gets its row of the same transaction').toBe(true)

    const reloaded = await load()
    expect(reloaded.streamUrl, 'a reload starts a fresh group stream').not.toBe(group.streamUrl)
    expect((await fetch(reloaded.streamUrl)).status).toBe(200)
  })

  it('does not retire an acknowledged aggregate after one transient append failure during restore', async () => {
    const fault: AppendFault = { armed: false, status: 503, hits: 0 }
    h = await bootHarness(schema, { wrapEngineDs: oneShotShapeAppendProxy(fault) })
    await pgQuery(h, 'INSERT INTO items (id, payload) VALUES (1, $1)', ['present'])
    await drainEngine(h)

    const response = await fetch(`${h.engineUrl}/aggregate`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ table: 'items', fn: 'count' }),
    })
    expect(response.status).toBe(200)
    const aggregate = (await response.json()) as { shapeId: string; streamUrl: string }

    fault.armed = true
    await h.restartEngine()
    expect(fault.hits).toBe(1)

    expect(
      (await fetch(`${h.engineUrl}/shapes/${aggregate.shapeId}`)).status,
      'a transient storage response during boot must not permanently remove an acknowledged aggregate',
    ).toBe(200)
    expect((await fetch(aggregate.streamUrl)).status).toBe(200)
  })
})
