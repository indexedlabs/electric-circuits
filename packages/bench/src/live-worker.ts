// Live worker: reads stream URLs from stdin; for each, holds a long-poll loop open forever.
export const LIVE_WORKER_JS = `
let buf = ''; let count = 0;
async function hold(url) {
  let offset = '-1';
  for (;;) {
    try {
      const r = await fetch(url + '?offset=' + encodeURIComponent(offset) + '&live=long-poll');
      const next = r.headers.get('stream-next-offset');
      if (r.body) { for await (const _ of r.body) {} } // drain
      if (next) offset = next;
    } catch { await new Promise((res) => setTimeout(res, 1000)); }
  }
}
process.stdin.on('data', (d) => {
  buf += d.toString();
  let i;
  while ((i = buf.indexOf('\\n')) >= 0) {
    const line = buf.slice(0, i); buf = buf.slice(i + 1);
    if (line) { count++; hold(line); }
  }
});
process.stdin.on('end', () => console.error('live worker holding ' + count + ' subscriptions'));
`
