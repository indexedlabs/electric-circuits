import { describe, expect, it } from 'vitest'

import { assertLiveMeasurements, summarizeLiveRuns, type LiveWorkerSummary } from './live-summary.js'

function worker(overrides: Partial<LiveWorkerSummary> = {}): LiveWorkerSummary {
  return {
    liveMode: 'sse' as const,
    reads: 10,
    failedReads: 0,
    reconnects: 2,
    maxHeld: 1,
    maxInFlight: 1,
    subscriptions: 2,
    readySubscriptions: 2,
    readyAt: 100,
    measurementReady: true,
    elapsedMs: 60000,
    deliveryMs: [],
    ...overrides,
  }
}

describe('live summary aggregation', () => {
  it('reports reconnect rate and readiness spread per run', () => {
    const summary = summarizeLiveRuns([
      { expected: 2, summaries: [worker({ reconnects: 2, readyAt: 100 }), worker({ reconnects: 1, readyAt: 120 })] },
      { expected: 1, summaries: [worker({ reconnects: 1, subscriptions: 1, readyAt: 500, elapsedMs: 20000 })] },
    ])
    expect(summary.totalReconnectsPerMinute).toEqual([3, 3])
    expect(summary.estimatedFinalLevelReconnectsPerMinute).toBeCloseTo(6)
    expect(summary.readinessSpreadsMs).toEqual([20, 0])
    expect(summary.reconnectsPerSubscriptionMinute).toBeCloseTo(12 / 13)
  })

  it('fails when a worker summary is missing', () => {
    expect(() => summarizeLiveRuns([{ expected: 2, summaries: [worker()] }])).toThrow('summaries missing')
  })

  it('fails when a worker never reaches readiness', () => {
    expect(() => summarizeLiveRuns([{ expected: 1, summaries: [worker({ measurementReady: false, elapsedMs: null })] }])).toThrow('not ready')
  })

  it('guards zero reads and excessive failed reads', () => {
    const zero = summarizeLiveRuns([{ expected: 1, summaries: [worker({ reads: 0 })] }])
    expect(() => assertLiveMeasurements(zero, true)).toThrow('zero reads')

    const failed = summarizeLiveRuns([{ expected: 1, summaries: [worker({ failedReads: 5 })] }])
    expect(() => assertLiveMeasurements(failed, true)).toThrow('too many failed reads')
    expect(() => assertLiveMeasurements(failed, false)).not.toThrow()
  })
})
