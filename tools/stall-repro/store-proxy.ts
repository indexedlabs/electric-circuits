import { writeFileSync } from 'node:fs'
import { mtlsAccess } from '../../packages/conformance/src/ds-mtls-access.ts'

const output = process.argv[2]
if (!output) throw new Error('Expected a readiness JSON output path')
const access = await mtlsAccess('http://127.0.0.1:8791', 'http')
writeFileSync(output, JSON.stringify({ url: access.url, env: access.env }))
for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.once(signal, async () => {
    await access.close()
    process.exit(0)
  })
}
