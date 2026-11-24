import { appendFileSync, writeFileSync } from 'node:fs'
import { createServer, request } from 'node:http'
import { once } from 'node:events'
import { mtlsAccess } from '../../packages/conformance/src/ds-mtls-access.ts'

const output = process.argv[2]
if (!output) throw new Error('Expected a readiness JSON output path')
const events = `${output}.events.jsonl`
const transport = createServer((incoming, outgoing) => {
  const path = new URL(incoming.url ?? '/', 'http://127.0.0.1').pathname
  const observe = incoming.method === 'GET' && /\/changes\/\d+$/.test(path)
  const started = Date.now() / 1000
  const upstream = request({ hostname: '127.0.0.1', port: 8791, path: incoming.url, method: incoming.method, headers: incoming.headers }, (response) => {
    const chunks: Buffer[] = []
    let bytes = 0
    let firstChunk: number | undefined
    outgoing.writeHead(response.statusCode ?? 502, response.headers)
    if (observe) {
      response.on('data', (chunk: Buffer) => {
        firstChunk ??= Date.now() / 1000
        bytes += chunk.length
        if (bytes <= 256 * 1024 * 1024) chunks.push(chunk)
      })
      response.once('end', () => {
        const event: Record<string, unknown> = { started, first_chunk: firstChunk, ended: Date.now() / 1000, path, status: response.statusCode, bytes, offset: response.headers['stream-next-offset'] }
        try {
          const envelopes = JSON.parse(Buffer.concat(chunks).toString()) as Array<{ headers?: { txid?: string } }>
          const transactions: Record<string, number> = {}
          for (const envelope of envelopes) {
            const txid = envelope.headers?.txid
            if (txid) transactions[txid] = (transactions[txid] ?? 0) + 1
          }
          event.transactions = transactions
        } catch (error) {
          event.parse_error = String(error)
        }
        appendFileSync(events, JSON.stringify(event) + '\n')
      })
    }
    response.pipe(outgoing)
  })
  upstream.once('error', () => {
    if (!outgoing.headersSent) outgoing.writeHead(502)
    outgoing.end()
  })
  incoming.once('aborted', () => upstream.destroy())
  outgoing.once('close', () => {
    if (!outgoing.writableEnded) upstream.destroy()
  })
  incoming.pipe(upstream)
})
transport.listen(0, '127.0.0.1')
await once(transport, 'listening')
const address = transport.address()
if (!address || typeof address === 'string') throw new Error('Observation proxy did not bind')
const access = await mtlsAccess(`http://127.0.0.1:${address.port}`, 'http')
writeFileSync(output, JSON.stringify({ url: access.url, env: access.env }))
for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.once(signal, async () => {
    await access.close()
    transport.closeAllConnections()
    transport.close()
    process.exit(0)
  })
}
