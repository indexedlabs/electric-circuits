#!/bin/sh
set -eu
label=${1:?label}; duration=${2:-60}; interval=${INTERVAL_S:-1}
out="${OUT_DIR:-harness/otto-10992/results}/${label}-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$out"
base=http://127.0.0.1:7010
compose='docker compose -f docker/compose.yaml -f harness/otto-10992/compose.yaml'

cat >"$out/scenario.json" <<EOF
{"label":"$label","duration_s":$duration,"interval_s":$interval,"fault_match":"${FAULT_MATCH:-}","delay_ms":"${DELAY_MS:-0}","hang":"${HANG:-0}","outage":"${OUTAGE:-0}"}
EOF
 : >"$out/samples.jsonl"
end=$(( $(date +%s) + duration ))
while [ "$(date +%s)" -lt "$end" ]; do
  ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  rss=$($compose exec -T engine sh -c 'awk "/VmRSS/{print $2*1024}" /proc/1/status' 2>/dev/null | tr -d '\r' || echo null)
  ds_bytes=$($compose exec -T ds sh -c 'du -sb /data 2>/dev/null | cut -f1' 2>/dev/null || echo null)
  memory=$(curl -fsS "$base/memory" 2>/dev/null || echo '{}')
  metrics=$(curl -fsS "$base/metrics" 2>/dev/null || echo '{}')
  repl=$(curl -fsS "$base/replication/lsn" 2>/dev/null || echo '{}')
  roff=$(curl -fsS "$base/tables/public.record/offset" 2>/dev/null || echo '{}')
  poff=$(curl -fsS "$base/tables/public.plain_events/offset" 2>/dev/null || echo '{}')
  slots=$($compose exec -T postgres psql -U postgres -d electric -At -c "SELECT coalesce(json_agg(x),'[]') FROM (SELECT slot_name,active,pg_current_wal_lsn() AS current_lsn,confirmed_flush_lsn,pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn) AS retained_wal_bytes FROM pg_replication_slots) x" 2>/dev/null || echo '[]')
  latency=$(curl -sS -o /dev/null -w '%{time_total}' "$base/v1/health" 2>/dev/null || echo null)
  python3 - "$ts" "$rss" "$ds_bytes" "$memory" "$metrics" "$repl" "$roff" "$poff" "$slots" "$latency" >>"$out/samples.jsonl" <<'PY'
import json, sys
keys = ('ts','rss_bytes','ds_bytes','engine_memory','engine_metrics','replication_lsn','record_offset','plain_offset','slot_stats','latency_ms')
row = dict(zip(keys, sys.argv[1:]))
for key in keys[1:]:
    if key in ('engine_memory','engine_metrics','replication_lsn','record_offset','plain_offset','slot_stats'):
        try: row[key] = json.loads(row[key])
        except Exception: pass
    elif key in ('rss_bytes','ds_bytes'):
        try: row[key] = int(row[key])
        except Exception: pass
    elif key == 'latency_ms':
        try: row[key] = float(row[key]) * 1000
        except Exception: pass
print(json.dumps(row, separators=(',', ':')))
PY
  sleep "$interval"
done
printf '%s\n' "$out"
