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
let reconnects = 0;
let active = 0;
let maxActive = 0;
const controllers = new Set();
const startedAt = Date.now();
const deliveryMs = [];
const seenWrites = new Set();
const markerPrefix = process.env.WORKER_WRITE_MARKER_PREFIX;

function observeData(text) {
  if (!markerPrefix) return;
  const at = text.indexOf(markerPrefix);
  if (at < 0) return;
  const match = text.slice(at + markerPrefix.length).match(/^\d+/);
  if (!match) return;
  const writtenAt = Number(match[0]);
  if (Number.isFinite(writtenAt) && !seenWrites.has(writtenAt)) {
    seenWrites.add(writtenAt);
    deliveryMs.push(Math.max(0, Date.now() - writtenAt));
  }
}

function applySseEvent(block, offset) {
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
    } catch {
      // A malformed data event is not a resume token. The next complete control frame remains
      // authoritative; the response is otherwise allowed to end and retry from the last token.
    }
  } else if (kind !== 'control') {
    observeData(payload);
  }
  return offset;
}

async function readSse(response, offset) {
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
    while ((boundary = pending.match(/\r?\n\r?\n/)) !== null) {
      offset = applySseEvent(pending.slice(0, boundary.index), offset);
      pending = pending.slice(boundary.index + boundary[0].length);
    }
  }
  pending += decoder.decode();
  // An incomplete final event is intentionally dropped. Durable Streams repeats it from the last
  // complete control-frame offset on the next response.
  return offset;
}

async function readLongPoll(response, offset) {
  const next = response.headers.get('stream-next-offset');
  if (response.body) observeData(await response.text());
  return next || offset;
}

async function hold(url) {
  let offset = '-1';
  let opened = false;
  while (!stopping) {
    const ctl = new AbortController();
    active++;
    controllers.add(ctl);
    maxActive = Math.max(maxActive, active);
    reads++;
    if (opened) reconnects++;
    opened = true;
    try {
      const live = liveMode === 'sse' ? 'sse' : 'long-poll';
      const r = await fetch(url + '?offset=' + encodeURIComponent(offset) + '&live=' + live, { signal: ctl.signal });
      offset = liveMode === 'sse' ? await readSse(r, offset) : await readLongPoll(r, offset);
    } catch {
      if (!stopping) await new Promise((res) => setTimeout(res, 1000));
    } finally {
      controllers.delete(ctl);
      active--;
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
  const elapsedMs = Math.max(1, Date.now() - startedAt);
  const summary = {
    liveMode,
    reads,
    reconnects,
    maxActive,
    elapsedMs,
    reconnectsPerMinute: reconnects * 60000 / elapsedMs,
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
