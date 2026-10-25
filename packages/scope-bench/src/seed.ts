// Seeds one heavy account (`u1`, sized to the heaviest production sessions the pathological
// fixtures came from) plus other accounts at a quarter of its volume, so every predicate is
// selective the way it is in production. Row counts and payload sizes follow the medians of the
// `sync.subset.load` spans (rows per load, response bytes per row) in Indexed's SyncScenarios
// fixtures. Day numbers are integers with today = 0.

import { randomBytes } from 'node:crypto'

import type pg from 'pg'

import type { TableName } from './schema.js'

export const HEAVY = 'u1'
export const DAYS = { from: -90, to: 90 }
export const THREADS_PER_USER = 12

const pad = (n: number) => randomBytes(Math.ceil((n * 3) / 4)).toString('base64').slice(0, n)

type Row = Record<string, unknown>

export interface UserIds {
  records: string[]
  recordClientIds: string[]
  threads: string[]
  seriesParents: string[]
}

export interface SeedSummary {
  rows: Partial<Record<TableName, number>>
  heavy: UserIds
  users: Record<string, UserIds>
}

function rng(seed: number) {
  let s = seed >>> 0
  return () => {
    s = (s * 1664525 + 1013904223) >>> 0
    return s / 2 ** 32
  }
}

function userRows(user: string, scale: number, rand: () => number) {
  const out: Partial<Record<TableName, Row[]>> = {}
  const push = (t: TableName, r: Row) => (out[t] ??= []).push(r)
  const n = (k: number) => Math.max(1, Math.round(k * scale))
  let seq = 0
  const id = (p: string) => `${p}-${user}-${++seq}`

  push('user', { id: user, user_id: user, name: `User ${user}`, payload: pad(450) })
  for (let i = 0; i < 2; i++) push('user', { id: id('peer'), user_id: user, name: 'Peer', payload: pad(450) })
  const spaces: string[] = []
  for (let i = 0; i < 3; i++) {
    const s = id('space')
    spaces.push(s)
    push('space', { id: s, user_id: user, name: `Space ${i}`, payload: pad(2900) })
    push('space_member', { id: id('sm'), user_id: user, space_id: s, payload: pad(300) })
  }
  const calendars: string[] = []
  for (let i = 0; i < n(13); i++) {
    const c = id('cal')
    calendars.push(c)
    push('calendar', { id: c, user_id: user, name: `Calendar ${i}`, payload: pad(900) })
  }
  for (let i = 0; i < 3; i++) push('external_account', { id: id('ea'), user_id: user, payload: pad(700) })
  push('invite', { id: id('inv'), user_id: user, payload: pad(550) })
  const tags: string[] = []
  for (let i = 0; i < n(160); i++) {
    const t = id('tag')
    tags.push(t)
    push('tag', { id: t, user_id: user, name: `tag ${i}`, payload: pad(2900) })
  }
  for (let i = 0; i < n(563); i++) push('file', { id: id('file'), user_id: user, name: `f${i}`, payload: pad(620) })
  for (let i = 0; i < n(250); i++) push('feedback', { id: id('fb'), user_id: user, payload: pad(440) })
  for (let i = 0; i < n(217); i++) push('assistant_state', { id: id('as'), user_id: user, payload: pad(290) })
  for (let i = 0; i < n(2383); i++)
    push('activity', { id: id('act'), user_id: user, created_at: 1_000_000 + i, payload: pad(540) })
  for (let i = 0; i < n(19); i++) {
    const r = id('res')
    push('resource', { id: r, user_id: user, space_id: spaces[0], payload: pad(680) })
    for (let j = 0; j < 2; j++) push('resource_fact', { id: id('rf'), user_id: user, resource_id: r, payload: pad(740) })
    for (let j = 0; j < 3; j++) push('graph_edge', { id: id('ge'), user_id: user, resource_id: r, payload: pad(2500) })
  }

  const records: string[] = []
  const recordClientIds: string[] = []
  const addRecord = (day: number | null, extra: Partial<Row> = {}) => {
    const rid = id('rec')
    const cid = `c-${rid}`
    records.push(rid)
    recordClientIds.push(cid)
    push('record', {
      id: rid,
      client_id: cid,
      user_id: user,
      space_id: spaces[Math.floor(rand() * spaces.length)],
      title: `Record ${rid}`,
      is_series_parent: false,
      series_parent_id: null,
      last_composer_activity_at: 1_000_000 + records.length,
      payload: pad(9000),
      ...extra,
    })
    if (day != null) {
      push('record_date_index', { id: id('rdi'), user_id: user, record_id: rid, day })
      if (rand() < 0.1) push('record_date_index', { id: id('rdi'), user_id: user, record_id: rid, day: day + 1 })
    }
    if (rand() < 0.25) push('assignment', { id: id('asg'), user_id: user, record_id: rid, payload: pad(500) })
    if (rand() < 0.05) push('record_assignee_index', { id: id('rai'), user_id: user, record_id: rid })
    if (rand() < 0.1) push('record_completed_index', { id: id('rci'), user_id: user, record_id: rid })
    if (rand() < 0.15)
      push('tagged_item', { id: id('ti'), user_id: user, record_id: rid, tag_id: tags[Math.floor(rand() * tags.length)] })
    return { rid, cid }
  }
  const perDay = Math.max(1, Math.round(6 * scale))
  for (let day = DAYS.from; day <= DAYS.to; day++) for (let i = 0; i < perDay; i++) addRecord(day)
  const seriesParents: string[] = []
  for (let i = 0; i < n(20); i++) {
    const { rid } = addRecord(null, { is_series_parent: true })
    seriesParents.push(rid)
    push('record_recurrence', { id: id('rr'), user_id: user, record_id: rid, payload: pad(350) })
  }
  for (let i = 0; i < n(10); i++) {
    const day = DAYS.from + Math.floor(rand() * (DAYS.to - DAYS.from))
    addRecord(day, { series_parent_id: seriesParents[i % seriesParents.length] })
  }
  for (let i = 0; i < n(25); i++)
    push('calendar_event', {
      id: id('evm'),
      user_id: user,
      calendar_id: calendars[i % calendars.length],
      start_day: DAYS.from - 30,
      end_day: null,
      is_master: true,
      payload: pad(7500),
    })
  const evPerDay = Math.max(1, Math.round(4 * scale))
  for (let day = DAYS.from; day <= DAYS.to; day++)
    for (let i = 0; i < evPerDay; i++)
      push('calendar_event', {
        id: id('ev'),
        user_id: user,
        calendar_id: calendars[i % calendars.length],
        start_day: day,
        end_day: day,
        is_master: false,
        payload: pad(7500),
      })

  const threads: string[] = []
  for (let t = 0; t < n(THREADS_PER_USER); t++) {
    const th = id('thr')
    threads.push(th)
    for (let m = 0; m < n(40); m++)
      push('message', { id: id('msg'), user_id: user, thread_id: th, record_id: null, created_at: m, payload: pad(1900) })
    for (let s = 0; s < 6; s++) {
      // An accepted multi-event card names the client ids of the Records it created.
      const created = recordClientIds.slice((t * 6 + s) * 7, (t * 6 + s) * 7 + 7)
      push('suggestion', {
        id: id('sug'),
        user_id: user,
        thread_id: th,
        status: s < 5 ? 'accepted' : 'pending',
        created_record_client_ids: created.join(','),
        payload: pad(40000),
      })
    }
    for (let e = 0; e < 5; e++) push('assistant_stream_event', { id: id('ase'), user_id: user, thread_id: th, payload: pad(300) })
  }
  for (let i = 0; i < n(200); i++)
    push('message', {
      id: id('msg'),
      user_id: user,
      thread_id: threads[0],
      record_id: records[i % records.length],
      created_at: 10_000 + i,
      payload: pad(1900),
    })
  return { out, records, recordClientIds, threads, seriesParents }
}

async function insert(client: pg.Client, table: string, rows: Row[]) {
  if (!rows.length) return
  const cols = Object.keys(rows[0]!)
  const CHUNK = Math.max(1, Math.floor(30000 / cols.length))
  for (let s = 0; s < rows.length; s += CHUNK) {
    const slice = rows.slice(s, s + CHUNK)
    const params: unknown[] = []
    let p = 1
    const tuples = slice.map((r) => `(${cols.map(() => `$${p++}`).join(',')})`)
    for (const r of slice) for (const c of cols) params.push(r[c])
    await client.query(`insert into "${table}" (${cols.map((c) => `"${c}"`).join(',')}) values ${tuples.join(',')}`, params)
  }
}

export async function seed(client: pg.Client, otherUsers: number): Promise<SeedSummary> {
  const rand = rng(42)
  const summary: SeedSummary = { rows: {}, heavy: { records: [], recordClientIds: [], threads: [], seriesParents: [] }, users: {} }
  const users = [HEAVY, ...Array.from({ length: otherUsers }, (_, i) => `u${i + 2}`)]
  for (const u of users) {
    const r = userRows(u, u === HEAVY ? 1 : 0.25, rand)
    summary.users[u] = { records: r.records, recordClientIds: r.recordClientIds, threads: r.threads, seriesParents: r.seriesParents }
    if (u === HEAVY) summary.heavy = summary.users[u]!
    for (const [t, rows] of Object.entries(r.out)) {
      await insert(client, t, rows!)
      summary.rows[t as TableName] = (summary.rows[t as TableName] ?? 0) + rows!.length
    }
  }
  await client.query('analyze')
  return summary
}
