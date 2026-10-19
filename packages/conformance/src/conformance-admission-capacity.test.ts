// OTTO-8532: bound retained fresh seeds without serializing live-node backfills.
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
const RESPONSE_BUDGET_MS = 1500

describe('native: subquery seed capacity', () => {
  let h: Harness | undefined
  afterEach(async () => { await h?.shutdown() })

  async function boot(capacity: number) {
    h = await bootHarness(schema, {
      durableStreamsDurability: 'wal',
      engineEnv: { ELECTRIC_CIRCUITS_SUBQUERY_INIT_CONCURRENCY: String(capacity) },
    })
    await pgQuery(h, `
      INSERT INTO slow_parent VALUES (1), (2);
      INSERT INTO fast_parent VALUES (2);
      INSERT INTO slow_child VALUES (1, 1), (2, 2);
      INSERT INTO fast_child VALUES (2, 2);
    `)
    await drainEngine(h)
    return h
  }

  it.each([
    { capacity: 1, heldTable: 'slow_parent' },
    { capacity: 2, heldTable: 'slow_parent' },
    { capacity: 1, heldTable: 'slow_child' },
    { capacity: 2, heldTable: 'slow_child' },
  ])('queues fresh seed N+1 when N=$capacity and $heldTable is held', async ({ capacity, heldTable }) => {
    const harness = await boot(capacity)
    const hold = await lockTable(harness, heldTable)
    const pending: Array<ReturnType<typeof createShape>> = []
    let released = false
    let overflowFinishedWhileHeld = false
    let timer: ReturnType<typeof setTimeout> | undefined
    try {
      for (let i = 1; i <= capacity; i++) {
        pending.push(createShape(harness, {
          table: 'slow_child', where: { col: 'parent_id', in: {
            table: 'slow_parent', project: 'id', where: { col: 'id', op: 'eq', value: i },
          } },
        }))
      }
      // Holding slow_child lets each inner seed finish and keeps its collected rows
      // alive during the outer backfill: capacity must cover that retained state too.
      await waitFor(async () => (await tableLockWaiters(harness, heldTable)).length === capacity, 'N held initializers')
      const overflow = createShape(harness, {
        table: 'fast_child', where: { col: 'parent_id', in: { table: 'fast_parent', project: 'id' } },
      }).then((shape) => {
        overflowFinishedWhileHeld = !released
        return shape
      })
      pending.push(overflow)
      await Promise.race([
        overflow,
        new Promise<void>((resolve) => { timer = setTimeout(resolve, RESPONSE_BUDGET_MS) }),
      ])
    } finally {
      clearTimeout(timer)
      released = true
      await hold.release()
      await Promise.all(pending)
    }
    await drainEngine(harness)
    for (const [i, shape] of (await Promise.all(pending)).entries()) {
      expect(await streamKeys(shape.streamUrl)).toEqual([String(i < capacity ? i + 1 : 2)])
    }
    expect(overflowFinishedWhileHeld, 'fresh seed N+1 must wait for a capacity slot').toBe(false)
  }, 60000)

  it('backfills another shape using a live node while a shared-node outer backfill is held', async () => {
    const harness = await boot(1)
    const shared = { col: 'parent_id', in: { table: 'fast_parent', project: 'id' } }
    const existing = await createShape(harness, { table: 'fast_child', where: shared })
    expect(await streamKeys(existing.streamUrl)).toEqual(['2'])

    const hold = await lockTable(harness, 'slow_child')
    const slow = createShape(harness, { table: 'slow_child', where: shared })
    const pending = [slow]
    let released = false
    let reusedFinishedWhileHeld = false
    let timer: ReturnType<typeof setTimeout> | undefined
    try {
      await waitFor(async () => (await tableLockWaiters(harness, 'slow_child')).length > 0, 'held reused-node backfill')
      const reused = createShape(harness, {
        table: 'fast_child', where: { and: [shared, { col: 'id', op: 'gte', value: 0 }] },
      }).then((shape) => {
        reusedFinishedWhileHeld = !released
        return shape
      })
      pending.push(reused)
      await Promise.race([
        reused,
        new Promise<void>((resolve) => { timer = setTimeout(resolve, RESPONSE_BUDGET_MS) }),
      ])
    } finally {
      clearTimeout(timer)
      released = true
      await hold.release()
      await Promise.all(pending)
    }
    await drainEngine(harness)
    for (const shape of await Promise.all(pending)) expect(await streamKeys(shape.streamUrl)).toEqual(['2'])
    expect(reusedFinishedWhileHeld, 'live-node reuse must not queue behind another outer backfill').toBe(true)
  }, 60000)
})
