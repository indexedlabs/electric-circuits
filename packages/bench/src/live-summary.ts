export type LiveMode = 'sse' | 'long-poll'

export interface LiveWorkerSummary {
  liveMode: LiveMode
  reads: number
  failedReads?: number
  reconnects: number
  maxHeld: number
  maxInFlight?: number
  subscriptions?: number
  readySubscriptions?: number
  readyAt?: number | null
  measurementReady?: boolean
  elapsedMs: number | null
  deliveryMs?: number[]
}

export interface LiveSummaryRun {
  expected: number
  summaries: LiveWorkerSummary[]
}

export interface LiveSummary {
  reads: number
  failedReads: number
  heldConnections: number
  inFlightConnections: number
  totalReconnectsPerMinute: number[]
  reconnectsPerSubscriptionMinute: number
  readinessSpreadsMs: Array<number | null>
  deliveryP50Ms: number | null
  deliveryP90Ms: number | null
  deliveries: number
}

function percentile(values: number[], p: number): number | null {
  const sorted = [...values].sort((a, b) => a - b)
  return sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor(p * sorted.length))]! : null
}

export function summarizeLiveRuns(runs: LiveSummaryRun[]): LiveSummary {
  for (const [index, run] of runs.entries()) {
    if (run.summaries.length !== run.expected) {
      throw new Error('live worker summaries missing for run ' + index + ': received ' + run.summaries.length + ', expected ' + run.expected)
    }
    const notReady = run.summaries.find((summary) => summary.measurementReady === false || summary.elapsedMs === null)
    if (notReady) throw new Error('live worker measurement not ready for run ' + index)
  }

  const summaries = runs.flatMap((run) => run.summaries)
  const reads = summaries.reduce((n, summary) => n + summary.reads, 0)
  const failedReads = summaries.reduce((n, summary) => n + (summary.failedReads ?? 0), 0)
  const heldConnections = summaries.reduce((n, summary) => n + summary.maxHeld, 0)
  const inFlightConnections = summaries.reduce((n, summary) => n + (summary.maxInFlight ?? summary.maxHeld), 0)
  const totalReconnectsPerMinute = runs.map((run) => {
    const reconnects = run.summaries.reduce((n, summary) => n + summary.reconnects, 0)
    const measurementWindowMs = Math.max(...run.summaries.map((summary) => summary.elapsedMs ?? 0), 0)
    return measurementWindowMs ? reconnects * 60000 / measurementWindowMs : 0
  })
  const reconnects = summaries.reduce((n, summary) => n + summary.reconnects, 0)
  const subscriptionMinutes = summaries.reduce((n, summary) =>
    n + (summary.subscriptions ?? 1) * (summary.elapsedMs ?? 0) / 60000, 0)
  const readinessSpreadsMs = runs.map((run) => {
    const readyTimes = run.summaries
      .map((summary) => summary.readyAt)
      .filter((value): value is number => typeof value === 'number')
    return readyTimes.length > 1 ? Math.max(...readyTimes) - Math.min(...readyTimes) : readyTimes.length ? 0 : null
  })
  const deliveries = summaries.flatMap((summary) => summary.deliveryMs ?? [])
  return {
    reads,
    failedReads,
    heldConnections,
    inFlightConnections,
    totalReconnectsPerMinute,
    reconnectsPerSubscriptionMinute: subscriptionMinutes ? reconnects / subscriptionMinutes : 0,
    readinessSpreadsMs,
    deliveryP50Ms: percentile(deliveries, 0.5),
    deliveryP90Ms: percentile(deliveries, 0.9),
    deliveries: deliveries.length,
  }
}

export function assertLiveMeasurements(summary: LiveSummary, requestedSubscriptions: boolean): void {
  if (requestedSubscriptions && summary.reads === 0) {
    throw new Error('live workers produced zero reads for requested subscriptions')
  }
  if (requestedSubscriptions && summary.failedReads * 2 >= Math.max(1, summary.reads)) {
    throw new Error('live workers had too many failed reads: ' + summary.failedReads + '/' + summary.reads)
  }
}
