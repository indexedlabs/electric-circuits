// The screens Indexed's iOS app opens, written as the scopes the fetch-policy proposal gives them,
// plus today's unscoped equivalents for the two incidents (per-card Record leases in a chat, one
// day window per Stack card). Timings follow the logged scenarios: a cold launch with the deck in
// its first seconds, a chat open, an agenda fling, a Stack visit and a Record open.

import type { TableName } from './schema.js'
import { HEAVY, type SeedSummary, type UserIds } from './seed.js'

export type Level = 'first' | 'visible' | 'background'
export type Pred = Record<string, unknown>
export interface TableDemand {
  key: string
  table: TableName
  where?: Pred
}
export interface Scope {
  name: string
  level: Level
  tables: TableDemand[]
}
export interface ScreenEvent {
  at: number
  screen: string
  scopes: Scope[]
}
export interface Scenario {
  name: string
  description: string
  events: ScreenEvent[]
  /** The phone opened these screens before (stored rows and cursors), left, and comes back. */
  reopen?: boolean
}

const eq = (col: string, value: unknown) => ({ col, op: 'eq', value })
const gte = (col: string, value: unknown) => ({ col, op: 'gte', value })
const lte = (col: string, value: unknown) => ({ col, op: 'lte', value })
const and = (...p: Pred[]) => ({ and: p })
const or = (...p: Pred[]) => ({ or: p })
const isIn = (col: string, table: TableName, project: string, where: Pred) => ({ col, in: { table, project, where } })
// The account the scenarios are built for; set by scenarios() before it builds any scope.
let own: Pred = eq('user_id', HEAVY)

// --- Session ---------------------------------------------------------------------------------

const INVENTORIES: TableName[] = [
  'space', 'space_member', 'user', 'calendar', 'external_account', 'invite', 'tag', 'file', 'feedback',
  'assistant_state', 'activity', 'resource', 'resource_fact', 'graph_edge',
]
export const sessionScope = (): Scope => ({
  name: 'session',
  level: 'background',
  tables: INVENTORIES.map((t) => ({ key: t, table: t })),
})

export const deckScope = (s: { heavy: UserIds }): Scope => ({
  name: 'deck',
  level: 'first',
  tables: [
    { key: 'suggestion', table: 'suggestion', where: eq('status', 'pending') },
    { key: 'record', table: 'record', where: or(...s.heavy.records.slice(0, 12).map((id) => eq('id', id))) },
  ],
})

// --- Record scopes ---------------------------------------------------------------------------

// A day window: dated Records through the window's date index, series parents, or their
// overrides; companions select through the same Record predicate; calendar events beside them.
// With a session Series scope, a window carries only its dated rows: series parents, their
// recurrence rules and open-ended calendar masters come from the Series scope instead.
export function windowRecordPred(from: number, to: number, withSeries = true): Pred {
  const dated = isIn('id', 'record_date_index', 'record_id', and(own, gte('day', from), lte('day', to)))
  if (!withSeries) return dated
  return or(dated, eq('is_series_parent', true), { col: 'series_parent_id', isNull: false })
}
const WINDOW_COMPANIONS: TableName[] = [
  'record_date_index', 'record_recurrence', 'assignment', 'tagged_item', 'record_assignee_index', 'record_completed_index',
]
export const dayWindow = (from: number, to: number, level: Level, withSeries = true): Scope => ({
  name: `window ${from}..${to}`,
  level,
  tables: [
    { key: 'record', table: 'record', where: windowRecordPred(from, to, withSeries) },
    ...WINDOW_COMPANIONS.filter((t) => withSeries || t !== 'record_recurrence').map((t) => ({
      key: t,
      table: t,
      where: isIn('record_id', 'record', 'id', and(own, windowRecordPred(from, to, withSeries))),
    })),
    {
      key: 'calendar_event',
      table: 'calendar_event',
      where: withSeries
        ? or(and(lte('start_day', to), gte('end_day', from)), eq('is_master', true))
        : and(lte('start_day', to), gte('end_day', from)),
    },
  ],
})

// Every recurring series once per session: series-parent Records and their rules, overrides'
// parents included, and open-ended calendar masters. Held live and resumed like any scope, so after
// the first sync only new or changed series arrive.
export const seriesScope = (level: Level): Scope => ({
  name: 'series',
  level,
  tables: [
    { key: 'record', table: 'record', where: eq('is_series_parent', true) },
    {
      key: 'record_recurrence',
      table: 'record_recurrence',
      where: isIn('record_id', 'record', 'id', and(own, eq('is_series_parent', true))),
    },
    { key: 'calendar_event', table: 'calendar_event', where: eq('is_master', true) },
  ],
})

const RECORD_COMPANIONS: TableName[] = [
  'record_date_index', 'record_recurrence', 'assignment', 'tagged_item', 'record_assignee_index', 'record_completed_index',
  'message',
]
export const oneRecord = (id: string, level: Level): Scope => ({
  name: `record ${id}`,
  level,
  tables: [
    { key: 'record', table: 'record', where: or(eq('id', id), eq('client_id', id)) },
    ...RECORD_COMPANIONS.map((t) => ({ key: t, table: t, where: eq('record_id', id) })),
  ],
})

export const threadScope = (thread: string): Scope => ({
  name: `thread ${thread}`,
  level: 'first',
  tables: (['message', 'suggestion', 'assistant_stream_event'] as TableName[]).map((t) => ({
    key: t,
    table: t,
    where: eq('thread_id', thread),
  })),
})

// The transcript's id set: the Records every accepted card in the thread created (by client id),
// with their date index selected through the Record subquery.
export const transcriptScope = (clientIds: string[]): Scope => {
  const ids = or(...clientIds.map((c) => eq('client_id', c)))
  return {
    name: `transcript (${clientIds.length} records)`,
    level: 'visible',
    tables: [
      { key: 'record', table: 'record', where: ids },
      { key: 'record_date_index', table: 'record_date_index', where: isIn('record_id', 'record', 'id', and(own, ids)) },
    ],
  }
}

// Today's chat: every created Record on a card takes its own two leases (the incident's 228).
export const perCardLeases = (clientIds: string[]): Scope[] =>
  clientIds.map((c) => ({
    name: `card ${c}`,
    level: 'visible' as Level,
    tables: [
      { key: 'record', table: 'record' as TableName, where: or(eq('id', c), eq('client_id', c)) },
      {
        key: 'record_date_index',
        table: 'record_date_index' as TableName,
        where: isIn('record_id', 'record', 'id', and(own, eq('client_id', c))),
      },
    ],
  }))

// --- Scenarios -------------------------------------------------------------------------------

export function scenarios(seed: SeedSummary, variant: 'today' | 'scoped' | 'series', user: string = HEAVY): Scenario[] {
  own = eq('user_id', user)
  const s = { heavy: seed.users[user] ?? seed.heavy }
  // The series variant holds a Series scope from launch; windows then carry only dated rows.
  const ws = variant !== 'series'
  const thread = s.heavy.threads[0]!
  // The incident thread: 114 created Records (228 per-card leases today).
  const transcript = s.heavy.recordClientIds.slice(0, 114)
  const out: Scenario[] = [
    {
      name: 'cold launch',
      description: 'Launch: session inventories (background), the deck and Home\'s today window (first)',
      events: [{ at: 0, screen: 'home', scopes: [deckScope(s), dayWindow(0, 0, 'first', ws), ...(ws ? [] : [seriesScope('first')]), sessionScope()] }],
    },
    {
      name: 'chat open',
      description: 'Open the incident thread: messages, suggestions and the 114 Records its cards created',
      events: [
        {
          at: 0,
          screen: 'chat',
          scopes:
            variant === 'today'
              ? [threadScope(thread), ...perCardLeases(transcript)]
              : [threadScope(thread), transcriptScope(transcript)],
        },
      ],
    },
    {
      name: 'agenda fling',
      description: 'Fling the agenda across 12 three-day blocks in 3 s; the last block is the one in view',
      events: Array.from({ length: 12 }, (_, i) => ({
        at: i * 250,
        screen: i === 11 ? 'agenda' : `agenda-pass-${i}`,
        scopes: [dayWindow(1 + i * 3, 3 + i * 3, i === 11 ? 'first' : 'visible', ws)],
      })),
    },
    {
      name: 'stack visit',
      description: 'Open the Stack: 7 day cards',
      events: [
        {
          at: 0,
          screen: 'stack',
          scopes:
            variant === 'today'
              ? Array.from({ length: 7 }, (_, d) => dayWindow(d, d, d === 0 ? 'first' : 'visible'))
              : [dayWindow(0, 6, 'first', ws)],
        },
      ],
    },
    {
      name: 'chat reopen',
      description: 'Come back to the incident thread minutes later, after 5 of its Records changed',
      reopen: true,
      events: [{ at: 0, screen: 'chat', scopes: [threadScope(thread), transcriptScope(transcript)] }],
    },
    {
      name: 'record open',
      description: 'Tap a Record: the Record and its companions',
      events: [{ at: 0, screen: 'record', scopes: [oneRecord(s.heavy.records[40]!, 'first')] }],
    },
  ]
  return out
}
