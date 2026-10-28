// The live worker is embedded in shape-mem-scale.ts' child processes. It speaks directly to
// Durable Streams, so the SSE path deliberately parses the wire events instead of treating an SSE
// response as a JSON response or rebuilding it from long-polls.
export const LIVE_WORKER_JS = `
const liveMode = process.env.WORKER_LIVE_MODE ?? 'long-poll';
if (liveMode !== 'sse' && liveMode !== 'long-poll') {
  console.error('unknown WORKER_LIVE_MODE=' + liveMode + '; expected sse or long-poll');
  process.exit(2);
}

let buf = '';
let stopping = false;
let reads = 0;
let failedReads = 0;
let reconnects = 0;
let held = 0;
let maxHeld = 0;
let inFlight = 0;
let maxInFlight = 0;
let readySubscriptions = 0;
let readyAt = null;
const controllers = new Set();
const startedAt = Date.now();
const deliveryMs = [];
const seenWrites = new Set();
const readyIds = new Set();
const markerPrefix = process.env.WORKER_WRITE_MARKER_PREFIX;
const expectedSubscriptions = Number(process.env.WORKER_EXPECTED_SUBSCRIPTIONS || 0);
let nextSubscriptionId = 0;
let measurementStartedAt = expectedSubscriptions > 0 ? null : startedAt;
let measurementReadsAtStart = 0;
let measurementFailedReadsAtStart = 0;
let measurementReconnectsAtStart = 0;
let measurementReady = expectedSubscriptions === 0;

function reportReady(subscriptionId) {
  if (readyIds.has(subscriptionId)) return;
  readyIds.add(subscriptionId);
  readySubscriptions++;
  const now = Date.now();
  readyAt = now;
  if (expectedSubscriptions > 0 && readySubscriptions >= expectedSubscriptions && measurementStartedAt === null) {
    measurementStartedAt = now;
    measurementReadsAtStart = reads;
    measurementFailedReadsAtStart = failedReads;
    measurementReconnectsAtStart = reconnects;
    measurementReady = true;
  }
  process.stdout.write('READY ' + subscriptionId + ' ' + now + '\\n');
}

function observeData(text, subscriptionId) {
  if (!markerPrefix) return;
  let from = 0;
  while (true) {
    const at = text.indexOf(markerPrefix, from);
    if (at < 0) return;
    const match = text.slice(at + markerPrefix.length).match(/^\\d+/);
    if (match) {
      const writtenAt = Number(match[0]);
      const key = subscriptionId + ':' + writtenAt;
      if (Number.isFinite(writtenAt) && !seenWrites.has(key)) {
        seenWrites.add(key);
        deliveryMs.push(Math.max(0, Date.now() - writtenAt));
      }
    }
    from = at + markerPrefix.length;
  }
}

function applySseEvent(block, offset, subscriptionId) {
  let kind = 'message';
  const data = [];
  for (const raw of block.split('\\n')) {
    const line = raw.endsWith('\\r') ? raw.slice(0, -1) : raw;
    if (line.startsWith('event:')) kind = line.slice(6).trim();
    else if (line.startsWith('data:')) data.push(line.slice(5).replace(/^ /, ''));
  }
  const payload = data.join('\\n');
  if (kind === 'control' && payload) {
    try {
      const frame = JSON.parse(payload);
      if (typeof frame.streamNextOffset === 'string') offset = frame.streamNextOffset;
      if (frame.upToDate === true) reportReady(subscriptionId);
    } catch {
      // A malformed data event is not a resume token. The next complete control frame remains
      // authoritative; the response is otherwise allowed to end and retry from the last token.
    }
  } else if (kind !== 'control') {
    observeData(payload, subscriptionId);
  }
  return offset;
}

async function readSse(response, offset, subscriptionId) {
  const contentType = response.headers.get('content-type') ?? '';
  if (!contentType.toLowerCase().startsWith('text/event-stream')) {
    throw new Error('SSE response was ' + contentType);
  }
  let pending = '';
  const decoder = new TextDecoder();
  if (!response.body) return offset;
  for await (const chunk of response.body) {
    pending += decoder.decode(chunk, { stream: true });
    let boundary;
    while ((boundary = pending.match(/\\r?\\n\\r?\\n/)) !== null) {
      offset = applySseEvent(pending.slice(0, boundary.index), offset, subscriptionId);
      pending = pending.slice(boundary.index + boundary[0].length);
    }
  }
  pending += decoder.decode();
  // An incomplete final event is intentionally dropped. Durable Streams repeats it from the last
  // complete control-frame offset on the next response.
  return offset;
}

async function readLongPoll(response, offset, subscriptionId) {
  const next = response.headers.get('stream-next-offset');
  if ((response.headers.get('stream-up-to-date') ?? '').toLowerCase() === 'true') reportReady(subscriptionId);
  if (response.body) observeData(await response.text(), subscriptionId);
  return next || offset;
}

async function hold(url) {
  const subscriptionId = String(nextSubscriptionId++);
  let offset = '-1';
  let reconnectPending = false;
  while (!stopping) {
    const ctl = new AbortController();
    controllers.add(ctl);
    let countedHeld = false;
    inFlight++;
    maxInFlight = Math.max(maxInFlight, inFlight);
    try {
      const live = liveMode === 'sse' ? 'sse' : 'long-poll';
      const r = await fetch(url + '?offset=' + encodeURIComponent(offset) + '&live=' + live, { signal: ctl.signal });
      if (!r.ok) {
        throw new Error('live response was ' + r.status);
      }
      countedHeld = true;
      held++;
      maxHeld = Math.max(maxHeld, held);
      if (reconnectPending) reconnects++;
      reconnectPending = false;
      offset = liveMode === 'sse'
        ? await readSse(r, offset, subscriptionId)
        : await readLongPoll(r, offset, subscriptionId);
      reads++;
      reconnectPending = true;
    } catch {
      if (!stopping) {
        failedReads++;
        await new Promise((res) => setTimeout(res, 1000));
      }
    } finally {
      inFlight--;
      if (countedHeld) held--;
      controllers.delete(ctl);
    }
  }
}

function percentile(values, p) {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor(p * sorted.length))] : null;
}

process.on('SIGTERM', () => {
  if (stopping) return;
  stopping = true;
  for (const ctl of controllers) ctl.abort();
  const elapsedMs = measurementStartedAt === null ? null : Math.max(1, Date.now() - measurementStartedAt);
  const measuredReads = measurementReady ? reads - measurementReadsAtStart : 0;
  const measuredFailedReads = measurementReady ? failedReads - measurementFailedReadsAtStart : 0;
  const measuredReconnects = measurementReady ? reconnects - measurementReconnectsAtStart : 0;
  const summary = {
    liveMode,
    measurementReady,
    reads: measuredReads,
    failedReads: measuredFailedReads,
    reconnects: measuredReconnects,
    maxHeld,
    maxInFlight,
    subscriptions: nextSubscriptionId,
    readySubscriptions,
    readyAt,
    elapsedMs,
    reconnectsPerMinute: elapsedMs === null ? null : measuredReconnects * 60000 / elapsedMs,
    deliveries: deliveryMs.length,
    deliveryMs,
    deliveryP50Ms: percentile(deliveryMs, 0.5),
    deliveryP90Ms: percentile(deliveryMs, 0.9),
  };
  process.stdout.write(JSON.stringify(summary) + '\\n', () => process.exit(0));
});

process.stdin.on('data', (d) => {
  buf += d.toString();
  let i;
  while ((i = buf.indexOf('\\n')) >= 0) {
    const line = buf.slice(0, i); buf = buf.slice(i + 1);
    if (line) hold(line);
  }
});
process.stdin.on('end', () => console.error('live worker mode=' + liveMode));
`
