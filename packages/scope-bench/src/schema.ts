// Synthetic tables shaped like Indexed's iOS sync tables. Column sets are trimmed to what the
// predicates read plus one `payload` column sized to the bytes per row the production
// `sync.subset.load` spans logged for that table. Every table carries the owner's `user_id` so the
// gateway can authorize a predicate the way Indexed's gateway does (an `and` with the authority).
// No customer data: names, ids and payloads are generated.

export const TABLES = {
  // Account inventories (session scope).
  user: 'id text primary key, user_id text not null, name text not null, payload text not null',
  space: 'id text primary key, user_id text not null, name text not null, payload text not null',
  space_member: 'id text primary key, user_id text not null, space_id text not null, payload text not null',
  calendar: 'id text primary key, user_id text not null, name text not null, payload text not null',
  external_account: 'id text primary key, user_id text not null, payload text not null',
  invite: 'id text primary key, user_id text not null, payload text not null',
  tag: 'id text primary key, user_id text not null, name text not null, payload text not null',
  file: 'id text primary key, user_id text not null, name text not null, payload text not null',
  feedback: 'id text primary key, user_id text not null, payload text not null',
  assistant_state: 'id text primary key, user_id text not null, payload text not null',
  activity: 'id text primary key, user_id text not null, created_at bigint not null, payload text not null',
  // Resource graph.
  resource: 'id text primary key, user_id text not null, space_id text not null, payload text not null',
  resource_fact: 'id text primary key, user_id text not null, resource_id text not null, payload text not null',
  graph_edge: 'id text primary key, user_id text not null, resource_id text not null, payload text not null',
  // Records and their projections.
  record:
    'id text primary key, client_id text not null, user_id text not null, space_id text not null, ' +
    'title text not null, is_series_parent boolean not null, series_parent_id text, ' +
    'last_composer_activity_at bigint not null, payload text not null',
  record_date_index: 'id text primary key, user_id text not null, record_id text not null, day int not null',
  record_recurrence: 'id text primary key, user_id text not null, record_id text not null, payload text not null',
  assignment: 'id text primary key, user_id text not null, record_id text not null, payload text not null',
  record_assignee_index: 'id text primary key, user_id text not null, record_id text not null',
  record_completed_index: 'id text primary key, user_id text not null, record_id text not null',
  tagged_item: 'id text primary key, user_id text not null, record_id text not null, tag_id text not null',
  calendar_event:
    'id text primary key, user_id text not null, calendar_id text not null, start_day int not null, ' +
    'end_day int, is_master boolean not null, payload text not null',
  // Thread rows.
  message:
    'id text primary key, user_id text not null, thread_id text not null, record_id text, ' +
    'created_at bigint not null, payload text not null',
  suggestion:
    'id text primary key, user_id text not null, thread_id text not null, status text not null, ' +
    'created_record_client_ids text not null, payload text not null',
  assistant_stream_event: 'id text primary key, user_id text not null, thread_id text not null, payload text not null',
} as const

export type TableName = keyof typeof TABLES

export const INDEXES = [
  'create index on record (user_id, last_composer_activity_at)',
  'create index on record (client_id)',
  'create index on record (series_parent_id)',
  'create index on record_date_index (user_id, day)',
  'create index on record_date_index (record_id)',
  'create index on record_recurrence (record_id)',
  'create index on assignment (record_id)',
  'create index on record_assignee_index (record_id)',
  'create index on record_completed_index (record_id)',
  'create index on tagged_item (record_id)',
  'create index on calendar_event (user_id, start_day)',
  'create index on message (thread_id)',
  'create index on message (record_id)',
  'create index on suggestion (thread_id)',
  'create index on assistant_stream_event (thread_id)',
  'create index on resource_fact (resource_id)',
  'create index on graph_edge (resource_id)',
  ...[
    'user', 'space', 'space_member', 'calendar', 'external_account', 'invite', 'tag', 'file', 'feedback',
    'assistant_state', 'activity', 'resource',
  ].map((t) => `create index on "${t}" (user_id)`),
]

export function ddl(): string {
  return Object.entries(TABLES)
    .map(([t, cols]) => `create table "${t}" (${cols});\nalter table "${t}" replica identity full;`)
    .join('\n')
}
