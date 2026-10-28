/**
 * Key-only semijoin prototype used by the scope benchmark.
 *
 * The engine keeps only `(member, record_id)` keys.  Companion rows remain in
 * Postgres and a move-in is materialized with one entered-key read per member,
 * rather than one deferred query-back per table.  This is intentionally a small
 * executable model: it lets the benchmark measure the state and seed costs without
 * pretending that the DBSP operator is already a production engine feature.
 */

export interface KeyRow {
  member: string
  recordId: string
}

export interface CompanionRow {
  member: string
  recordId: string
  payload?: unknown
}

export class NullSemanticsError extends Error {
  constructor() {
    super('key-only semijoin rejects NOT IN/NULL predicates; use NOT EXISTS or an explicit IS NULL branch')
    this.name = 'NullSemanticsError'
  }
}

export function assertSupportedPredicate(predicate: unknown): void {
  const json = JSON.stringify(predicate)
  if (json.includes('"notIn"') || json.includes('"not in"') || json.includes('"isNull"')) {
    throw new NullSemanticsError()
  }
  if (json.includes('"value":null')) throw new NullSemanticsError()
}

export class KeyOnlySemijoin {
  private readonly keys = new Map<string, Set<string>>()

  seed(rows: Iterable<KeyRow>): void {
    for (const row of rows) this.enter(row.member, row.recordId)
  }

  enter(member: string, recordId: string): boolean {
    let set = this.keys.get(member)
    if (!set) this.keys.set(member, (set = new Set()))
    const before = set.size
    set.add(recordId)
    return set.size !== before
  }

  leave(member: string, recordId: string): boolean {
    return this.keys.get(member)?.delete(recordId) ?? false
  }

  has(member: string, recordId: string): boolean {
    return this.keys.get(member)?.has(recordId) ?? false
  }

  keysFor(member: string): string[] {
    return [...(this.keys.get(member) ?? [])]
  }

  /** One Postgres read over all entered keys, grouped back by member. */
  materialize(rows: Iterable<CompanionRow>): Map<string, CompanionRow[]> {
    const out = new Map<string, CompanionRow[]>()
    for (const row of rows) {
      if (!this.has(row.member, row.recordId)) continue
      const list = out.get(row.member) ?? []
      list.push(row)
      out.set(row.member, list)
    }
    return out
  }

  keyCount(): number {
    let count = 0
    for (const set of this.keys.values()) count += set.size
    return count
  }

  /** Approximate retained state, excluding V8 object headers. */
  stateBytes(): number {
    let bytes = 0
    for (const [member, ids] of this.keys) bytes += 48 + member.length * 2 + ids.size * 40
    return bytes
  }
}

