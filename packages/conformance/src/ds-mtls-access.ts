import { createServer, request as httpsRequest } from 'node:https'
import { createServer as createHttpServer, request, type ClientRequest, type IncomingMessage, type ServerResponse } from 'node:http'
import { readFileSync } from 'node:fs'
import { once } from 'node:events'
import type { Duplex } from 'node:stream'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const pki = join(dirname(fileURLToPath(import.meta.url)), '..', 'test-pki')
const identity = {
  store_id: '2bc96d0b-9740-4f50-97c6-754b2b27d6b0',
  store_generation: 'ff8b5fa6-e786-4994-8da0-f14e9e79f318',
  protocol_version: 1,
  layout_version: 1,
  durability_mode: 'wal',
  wal_shard_count: 2,
  stream_lane_count: 1,
  filesystem_uuid: '253f14d5-cbee-4df8-9e3c-e44c6e41501b',
}

export const testStoreIdentity = identity
export function testPhysicalPath(logical: string): string {
  return `circuits/v1/test-stack/stores/${identity.store_generation}/queries/test-query/${logical.replace(/^\/+/, '')}`
}

export async function testMutualTlsRequest(url: string, method: string) {
  return new Promise<{ status: number; ok: boolean; body: string }>((resolve, reject) => {
    const request = httpsRequest(
      url,
      {
        method,
        ca: readFileSync(join(pki, 'ca.pem')),
        cert: readFileSync(join(pki, 'client.pem')),
        key: readFileSync(join(pki, 'client.key')),
      },
      (response) => {
        const chunks: Buffer[] = []
        response.on('data', (chunk: Buffer) => chunks.push(chunk))
        response.on('end', () => {
          const status = response.statusCode ?? 0
          resolve({ status, ok: status >= 200 && status < 300, body: Buffer.concat(chunks).toString() })
        })
      },
    )
    request.once('error', reject)
    request.end()
  })
}

function isLoopback(address: string | undefined): boolean {
  return address === '127.0.0.1' || address === '::1' || address === '::ffff:127.0.0.1'
}

export type DsTransport = 'http' | 'https' | 'mtls'

// Readiness is fixture metadata; all stream operations reach the real Rust server.
export async function mtlsAccess(rawUrl: string, transport: DsTransport = 'mtls') {
  const raw = new URL(rawUrl)
  const inboundSockets = new Set<Duplex>()
  const upstreamRequests = new Set<ClientRequest>()
  let closePromise: Promise<void> | undefined
  const handler = (incoming: IncomingMessage, outgoing: ServerResponse) => {
    const requestUrl = new URL(incoming.url ?? '/', 'https://conformance.invalid')
    const readOnly = incoming.method === 'GET' || incoming.method === 'HEAD'
    const tls = incoming.socket as import('node:tls').TLSSocket
    const hasAuthorizedClient = transport !== 'mtls' || (tls.authorized && Object.keys(tls.getPeerCertificate()).length > 0)
    // Non-pilot legacy conformance clients read raw loopback streams without a certificate. The
    // façade permits only those reads; readiness and every mutation still exercise mTLS.
    const legacyStreamRead = readOnly
      && requestUrl.pathname.startsWith(`/${testPhysicalPath('')}`)
      && isLoopback(incoming.socket.remoteAddress)
    if (!hasAuthorizedClient && !legacyStreamRead) {
      outgoing.writeHead(401).end()
      return
    }
    if (requestUrl.pathname === '/_admin/ready') {
      outgoing.setHeader('content-type', 'application/json')
      outgoing.end(JSON.stringify({ contract_version: 'durable-streams-store-ready-v1', status: 'ready', artifact_digest: `sha256:${'0'.repeat(64)}`, manifest: { ...identity, creation_time: '2026-08-27T19:00:00Z' }, recovery: { completed: true, wal_shards: [{ shard: 0, durable_lsn: 0, checkpoint_lsn: 0 }, { shard: 1, durable_lsn: 0, checkpoint_lsn: 0 }] }, reserve: { free_bytes: 100, free_inodes: 100, minimum_free_bytes: 1, minimum_free_inodes: 1, satisfied: true } }))
      return
    }
    const upstream = request({ hostname: raw.hostname, port: raw.port, path: incoming.url, method: incoming.method, headers: incoming.headers }, (response) => {
      outgoing.writeHead(response.statusCode ?? 502, response.headers)
      response.pipe(outgoing)
    })
    upstreamRequests.add(upstream)
    upstream.once('close', () => upstreamRequests.delete(upstream))
    upstream.on('error', () => {
      if (!outgoing.headersSent) outgoing.writeHead(502)
      outgoing.end()
    })
    incoming.once('aborted', () => upstream.destroy())
    outgoing.once('close', () => {
      if (!outgoing.writableEnded) upstream.destroy()
    })
    incoming.pipe(upstream)
  }
  const server = transport === 'http'
    ? createHttpServer(handler)
    : createServer(
      { key: readFileSync(join(pki, 'server.key')), cert: readFileSync(join(pki, 'server.pem')), ca: readFileSync(join(pki, 'ca.pem')), requestCert: transport === 'mtls', rejectUnauthorized: false },
      handler,
    )
  server.on('connection', (socket) => {
    inboundSockets.add(socket)
    socket.once('close', () => inboundSockets.delete(socket))
  })
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  const address = server.address()
  if (!address || typeof address === 'string') throw new Error('mTLS facade did not bind')
  return {
    url: `${transport === 'http' ? 'http' : 'https'}://127.0.0.1:${address.port}`,
    env: {
      ELECTRIC_CIRCUITS_DS_NAMESPACE: 'test-stack', ELECTRIC_CIRCUITS_DS_STORE_ID: identity.store_id,
      ELECTRIC_CIRCUITS_DS_STORE_GENERATION: identity.store_generation, ELECTRIC_CIRCUITS_DS_PROTOCOL_VERSION: '1',
      ELECTRIC_CIRCUITS_DS_LAYOUT_VERSION: '1', ELECTRIC_CIRCUITS_DS_DURABILITY_MODE: 'wal',
      ELECTRIC_CIRCUITS_DS_WAL_SHARDS: '2', ELECTRIC_CIRCUITS_DS_STREAM_LANES: '1',
      ELECTRIC_CIRCUITS_DS_FILESYSTEM_UUID: identity.filesystem_uuid, ELECTRIC_CIRCUITS_QUERY_GENERATION: 'test-query',
      ELECTRIC_CIRCUITS_DS_CA_BUNDLE: transport === 'http' ? '' : join(pki, 'ca.pem'), ELECTRIC_CIRCUITS_DS_CLIENT_CERT: transport === 'mtls' ? join(pki, 'client.pem') : '', ELECTRIC_CIRCUITS_DS_CLIENT_KEY: transport === 'mtls' ? join(pki, 'client.key') : '',
      ELECTRIC_CIRCUITS_INITIALIZE_NAMESPACE: '1',
    },
    close: () => {
      closePromise ??= new Promise<void>((resolve, reject) => {
        for (const request of upstreamRequests) request.destroy()
        for (const socket of inboundSockets) socket.destroy()
        server.close((error) => (error ? reject(error) : resolve()))
        server.closeAllConnections()
      })
      return closePromise
    },
  }
}
