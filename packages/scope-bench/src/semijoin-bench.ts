import { performance } from 'node:perf_hooks'

import { KeyOnlySemijoin } from './semijoin.ts'

const records = Number(process.env.SEMIJOIN_RECORDS ?? 12_000)
const scopes = Number(process.env.SEMIJOIN_SCOPES ?? 1)
const companionSizes = (process.env.SEMIJOIN_COMPANION_SIZES ?? '101900,75900,6100,300,300,300')
  .split(',')
  .map((x) => Number(x))
  .filter((x) => Number.isFinite(x) && x > 0)
const companions = companionSizes.length

const rows = Array.from({ length: records * scopes }, (_, i) => ({
  member: `scope-${i % scopes}`,
  recordId: `r-${i}`,
}))
const t0 = performance.now()
const join = new KeyOnlySemijoin()
join.seed(rows)
const seedMs = performance.now() - t0

// A move-in reads the entered keys once per member in one batched query.  Keep the
// companion fixture small enough to run in the sandbox while preserving the measured
// production cardinality ratio.
const companionRows = companionSizes.flatMap((size, memberIndex) =>
  Array.from({ length: size }, (_, i) => ({
    member: `scope-${memberIndex % scopes}`,
    recordId: `r-${i % records}`,
    payload: i,
  })),
)
const moveInStart = performance.now()
const materialized = join.materialize(companionRows)
const moveInMs = performance.now() - moveInStart

const rssMb = process.memoryUsage().rss / 1e6
const keyBytes = join.stateBytes()
const result = {
  model: 'key-only-semijoin',
  records,
  scopes,
  companionMembers: companions,
  keyCount: join.keyCount(),
  stateBytes: keyBytes,
  stateMiB: keyBytes / 2 ** 20,
  seedMs,
  moveInMs,
  materializedRows: [...materialized.values()].reduce((n, x) => n + x.length, 0),
  rssMb,
  scaleNote: records < 12_000 ? 'scaled below the production 12k-record heavy account' : 'production record cardinality',
}
console.log(JSON.stringify(result, null, 2))

