import type { Schema, ShapeDef } from '@electric-circuits/protocol'
import { fileURLToPath } from 'node:url'
import { expect, it } from 'vitest'

import { bootHarness, drainEngine, waitForConvergence } from './harness.js'
import { pgQuery } from './engine-native.js'

const schema: Schema = {
  tables: { items: { columns: { id: { type: 'int' }, name: { type: 'text' } }, primaryKey: 'id' } },
}
const def: ShapeDef = { table: 'items' }

// The access fixture supplies readiness metadata. Writes, reads and long-polls use the real
// durable-streams-server from the same wrapper as docker/ds-server.ts. No test-support bypass.
it.each(['http', 'https', 'mtls'] as const)('engine snapshots and streams live changes over DS %s', async (transport) => {
  const h = await bootHarness(schema, { dsTransport: transport, durableStreamsDurability: 'wal' })
  try {
    await pgQuery(h, "INSERT INTO items VALUES (1, 'snapshot')")
    const shape = await h.client.shape(def)
    const target = { def, shape, columns: ['id', 'name'], pk: 'id' }
    expect((await waitForConvergence(h, target)).equal).toBe(true)
    expect(shape.currentRows()).toHaveLength(1)
    expect(shape.handle.streamUrl.startsWith(transport === 'http' ? 'http://' : 'https://')).toBe(true)

    await pgQuery(h, "UPDATE items SET name = 'live' WHERE id = 1")
    await drainEngine(h)
    const compared = await waitForConvergence(h, target)
    expect(compared.equal, JSON.stringify(compared)).toBe(true)
    expect(shape.currentRows().map((row) => row.name)).toEqual(['live'])
  } finally {
    await h.shutdown()
  }
})

it.each<{ name: string; transport: 'http' | 'https'; env: Record<string, string> }>([
  {
    name: 'HTTP ignores unreadable, half-configured TLS material',
    transport: 'http' as const,
    env: {
      ELECTRIC_CIRCUITS_DS_CA_BUNDLE: '/missing/ds-ca.pem',
      ELECTRIC_CIRCUITS_DS_CLIENT_CERT: '',
      ELECTRIC_CIRCUITS_DS_CLIENT_KEY: '/missing/ds-key.pem',
    },
  },
  {
    name: 'HTTPS without a DS CA bundle uses the native root loader',
    transport: 'https' as const,
    env: {
      ELECTRIC_CIRCUITS_DS_CA_BUNDLE: '',
      // Configure the native loader for this child only; never modify the host trust store.
      SSL_CERT_FILE: fileURLToPath(new URL('../test-pki/ca.pem', import.meta.url)),
      SSL_CERT_DIR: '',
    },
  },
])('$name', async ({ transport, env }) => {
  const h = await bootHarness(schema, { dsTransport: transport, engineEnv: env, durableStreamsDurability: 'wal' })
  try {
    await pgQuery(h, "INSERT INTO items VALUES (1, 'transport')")
    const shape = await h.client.shape(def)
    const compared = await waitForConvergence(h, { def, shape, columns: ['id', 'name'], pk: 'id' })
    expect(compared.equal, JSON.stringify(compared)).toBe(true)
    expect(shape.currentRows().map((row) => row.name)).toEqual(['transport'])
  } finally {
    await h.shutdown()
  }
})
