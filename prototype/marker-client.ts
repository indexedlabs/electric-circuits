import { createClient } from '@electric-circuits/client'

const marker = process.env.MARKER ?? ''
const commitMs = Number(process.env.MARKER_COMMIT_MS ?? Date.now())
const timeoutMs = Number(process.env.MARKER_TIMEOUT_MS ?? 120_000)
const client = createClient({
  apiUrl: process.env.API_URL ?? 'http://api:8790',
  dsBaseUrl: process.env.DS_URL ?? 'http://ds:8791',
  liveMode: (process.env.LIVE_MODE ?? 'long-poll') as 'long-poll' | 'sse',
  schema: { tables: { 'public.plain_events': { primaryKey: 'id', columns: { id: { type: 'int' }, group_id: { type: 'int' }, body: { type: 'text' }, updated_at: { type: 'text' } } } } },
})
const sub = await client.subset({ table: 'public.plain_events', where: { col: 'body', op: 'eq', value: marker }, limit: 10 })
let seenMs: number | null = null
let resolveSeen: (() => void) | undefined
const seen = new Promise<void>((resolve) => { resolveSeen = resolve })
const check = () => {
  const rows = sub.collection.toArray as unknown as Array<{ body?: string }>
  if (rows.some((row) => row.body === marker) && seenMs === null) {
    seenMs = Date.now()
    resolveSeen?.()
  }
}
const unsubscribe = sub.collection.subscribe(() => check())
check()
await Promise.race([seen, new Promise((resolve) => setTimeout(resolve, timeoutMs))])
unsubscribe()
console.log(JSON.stringify({ marker, commitMs, seenMs, latencyMs: seenMs === null ? null : seenMs - commitMs }))
await sub.close()
await client.close()
