// Global test setup: build the engine once (so parallel workers don't race the cargo lock) and boot
// one ephemeral Postgres with logical replication enabled. Each harness then creates its own database
// + slot inside it (logical slots are per-database), so test files stay isolated. The admin
// connection string is exported via ELECTRIC_CIRCUITS_TEST_PG_URL (inherited by forked workers).
import { execFileSync } from 'node:child_process'
import { appendFileSync, mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { postgres18Tools } from './scripts/postgres18.js'

export default function setup() {
  // The conformance Durable Streams facade uses the checked-in test CA. Forked
  // Vitest workers are new Node processes, so publish the CA before the pool is
  // started; ordinary fetch() calls can then verify stream URLs without turning
  // off TLS validation globally.
  process.env.NODE_EXTRA_CA_CERTS = join(
    dirname(fileURLToPath(import.meta.url)),
    'packages',
    'conformance',
    'test-pki',
    'ca.pem',
  )
  execFileSync('cargo', ['build', '-p', 'electric-circuits-engine'], { stdio: 'inherit' })
  process.env.ELECTRIC_CIRCUITS_ENGINE_PREBUILT = '1'

  const postgres = postgres18Tools()

  const dir = mkdtempSync(join(tmpdir(), 'el-pg-'))
  const data = join(dir, 'data')
  // ICU collation fixtures require UTF8 even when the host locale is C/POSIX.
  execFileSync(postgres.initdb, ['-D', data, '-U', 'postgres', '--auth=trust', '--no-sync', '--encoding=UTF8'], { stdio: 'ignore' })

  // Try a few ports; initdb is done, only the chosen port differs per attempt.
  let port = 0
  let started = false
  for (let attempt = 0; attempt < 8 && !started; attempt++) {
    port = 55432 + Math.floor(Math.random() * 4000)
    appendFileSync(
      join(data, 'postgresql.conf'),
      `\n# test config (attempt ${attempt})\n` +
        `wal_level = logical\nmax_replication_slots = 80\nmax_wal_senders = 80\n` +
        `listen_addresses = '127.0.0.1'\nunix_socket_directories = '/tmp'\nport = ${port}\n` +
        `fsync = off\nsynchronous_commit = off\nfull_page_writes = off\n`,
    )
    try {
      execFileSync(postgres.pgCtl, ['-D', data, '-l', join(dir, 'log'), '-w', 'start'], { stdio: 'ignore' })
      started = true
    } catch {
      /* port likely in use; loop appends a new port (last one wins in postgresql.conf) */
    }
  }
  if (!started) throw new Error('failed to start ephemeral postgres for tests')

  process.env.ELECTRIC_CIRCUITS_TEST_PG_URL = `postgres://postgres@127.0.0.1:${port}/postgres`

  return () => {
    try {
      execFileSync(postgres.pgCtl, ['-D', data, '-m', 'immediate', '-w', 'stop'], { stdio: 'ignore' })
    } catch {
      /* already down */
    }
    try {
      rmSync(dir, { recursive: true, force: true })
    } catch {
      /* ignore */
    }
  }
}
