import { createClient } from '@electric-circuits/client'

const mode = (process.env.LIVE_MODE ?? 'long-poll') as 'long-poll' | 'sse'
const seconds = Number(process.env.CLIENT_SECONDS ?? 60)
const client = createClient({
  apiUrl: process.env.API_URL ?? 'http://api:8790',
  dsBaseUrl: process.env.DS_URL ?? 'http://ds:8791',
  liveMode: mode,
  schema: {
    tables: {
      'public.plain_events': { primaryKey: 'id', columns: { id: { type: 'int' }, group_id: { type: 'int' }, body: { type: 'text' }, updated_at: { type: 'text' } } },
      'public.record': { primaryKey: 'id', columns: { id: { type: 'int' }, group_id: { type: 'int' }, body: { type: 'text' }, updated_at: { type: 'text' } } },
    },
  },
})
const sub = await client.subset({ table: 'public.plain_events', limit: 100, orderBy: { col: 'id' } })
const started = Date.now()
let changes = 0
sub.collection.subscribe(() => { changes += 1 })
await new Promise((resolve) => setTimeout(resolve, seconds * 1000))
console.log(JSON.stringify({ mode, seconds, changes, hasMore: sub.hasMore(), elapsedMs: Date.now() - started }))
await sub.close()
await client.close()
