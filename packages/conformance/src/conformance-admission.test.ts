// OTTO-8532: a slow seed/backfill may delay creates that need its membership node,
// but must not serialize unrelated shapes. The hold is an external PostgreSQL
// relation lock; public HTTP completion and stream contents are the assertions.
import { performance } from 'node:perf_hooks'
import type { Schema } from '@electric-circuits/protocol'
import { afterEach, describe, expect, it } from 'vitest'
import { bootHarness, drainEngine, type Harness } from './harness.js'
import { createShape, lockTable, pgQuery, streamKeys, tableLockWaiters, waitFor } from './engine-native.js'

const schema: Schema = {
  tables: {
    slow_parent: { columns: { id: { type: 'int' } }, primaryKey: 'id' },
    fast_parent: { columns: { id: { type: 'int' } }, primaryKey: 'id' },
    slow_child: { columns: { id: { type: 'int' }, parent_id: { type: 'int' } }, primaryKey: 'id' },
    fast_child: { columns: { id: { type: 'int' }, parent_id: { type: 'int' } }, primaryKey: 'id' },
  },
}
const N = 4
const RESPONSE_BUDGET_MS = 1500

describe('native: independent subquery admission', () => {
  let h: Harness | undefined
  afterEach(async () => { await h?.shutdown() })

  it.each(['slow_parent', 'slow_child'])('progresses while %s holds a seed or outer backfill', async (heldTable) => {
    h = await bootHarness(schema, {
      durableStreamsDurability: 'wal',
      engineEnv: { ELECTRIC_CIRCUITS_LOG: 'warn,electric_circuits_engine::shape_create=info' },
    })
    const harness = h
    await pgQuery(h, `
      INSERT INTO slow_parent VALUES (1);
      INSERT INTO fast_parent VALUES (2);
      INSERT INTO slow_child VALUES (1, 1);
      INSERT INTO fast_child VALUES (2, 2);
    `)
    await drainEngine(h)

    const slowWhere = { col: 'parent_id', in: { table: 'slow_parent', project: 'id' } }
    const hold = await lockTable(h, heldTable)
    const slow = createShape(h, { table: 'slow_child', where: slowWhere })
    // Observe the held SELECT, rather than guessing when a seed starts with a sleep.
    // This is a diagnostic deadline; failure here is setup failure, never red evidence.
    const pending: Array<Promise<unknown>> = [slow]
    let finishedWhileHeld = 0
    let dependentFinishedWhileHeld = false
    let released = false
    let timer: ReturnType<typeof setTimeout> | undefined
    const elapsed: number[] = []
    try {
      await waitFor(async () => (await tableLockWaiters(harness, heldTable)).length > 0, 'held seed/backfill SELECT')
      // A different outer predicate still depends on the SAME not-yet-installed node.
      const dependent = createShape(h, {
        table: 'slow_child', where: { and: [slowWhere, { col: 'id', op: 'gte', value: 0 }] },
      }).then((shape) => {
        dependentFinishedWhileHeld = !released
        return shape
      })
      pending.push(dependent)
      const fast = Array.from({ length: N }, (_, i) => {
        const started = performance.now()
        return createShape(harness, {
          table: 'fast_child',
          where: { and: [
            { col: 'parent_id', in: { table: 'fast_parent', project: 'id' } },
            { col: 'id', op: 'gte', value: -i },
          ] },
        }).then((shape) => {
          elapsed.push(performance.now() - started)
          if (!released) finishedWhileHeld++
          return shape
        })
      })
      pending.push(...fast)
      // The timer measures the promised response budget. Release the hold and collect
      // every result even on main; the failure is the completion-count assertion below,
      // never a Vitest timeout or an abandoned request.
      await Promise.race([
        Promise.all(fast),
        new Promise<void>((resolve) => { timer = setTimeout(resolve, RESPONSE_BUDGET_MS) }),
      ])
    } finally {
      clearTimeout(timer)
      released = true
      await hold.release()
      await Promise.all(pending)
    }
    await drainEngine(h)
    const results = await Promise.all(pending) as Awaited<ReturnType<typeof createShape>>[]
    for (const [i, shape] of results.entries()) {
      expect(await streamKeys(shape.streamUrl)).toEqual(i < 2 ? ['1'] : ['2'])
    }
    const admissionWaits = h.engineStderr().split('\n').flatMap((line) => {
      try {
        const entry = JSON.parse(line) as { fields?: { admission_wait_ms?: number } }
        return entry.fields?.admission_wait_ms === undefined ? [] : [entry.fields.admission_wait_ms]
      } catch { return [] }
    })
    console.log(JSON.stringify({ heldTable, finishedWhileHeld, responseMs: elapsed, admissionWaitsMs: admissionWaits }))
    expect(dependentFinishedWhileHeld, 'a shared node cannot be used before installation').toBe(false)
    expect(finishedWhileHeld, `all ${N} unrelated creates must finish within ${RESPONSE_BUDGET_MS}ms while the hold remains`).toBe(N)
  }, 60000)
})
