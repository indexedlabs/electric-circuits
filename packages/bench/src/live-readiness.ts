export interface LiveReadinessGate {
  ready: Promise<{ elapsedMs: number }>
  observe(count: number): void
  closed(): void
  dispose(): void
  count(): number
}

export function createLiveReadinessGate(expected: number, timeoutMs: number): LiveReadinessGate {
  const startedAt = Date.now()
  let seen = 0
  let settled = false
  let timer: ReturnType<typeof setTimeout> | undefined
  let resolveReady!: (value: { elapsedMs: number }) => void
  let rejectReady!: (error: Error) => void
  const ready = new Promise<{ elapsedMs: number }>((resolve, reject) => {
    resolveReady = (value) => { settled = true; resolve(value) }
    rejectReady = (error) => { settled = true; reject(error) }
  })

  const gate: LiveReadinessGate = {
    ready,
    observe(count) {
      seen += count
      if (!settled && seen >= expected) resolveReady({ elapsedMs: Date.now() - startedAt })
    },
    closed() {
      if (!settled) rejectReady(new Error(`live readiness failed: ${seen} of ${expected} subscriptions ready`))
    },
    dispose() {
      if (timer !== undefined) clearTimeout(timer)
    },
    count: () => seen,
  }
  if (expected === 0) resolveReady({ elapsedMs: 0 })
  else timer = setTimeout(() => {
    if (!settled) rejectReady(new Error(`live readiness timed out: ${seen} of ${expected} subscriptions ready`))
  }, timeoutMs)
  return gate
}
