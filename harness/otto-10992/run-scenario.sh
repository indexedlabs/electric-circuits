#!/bin/sh
# Background sampler: one row per interval into $out/samples.jsonl until $stop exists.
# usage: run-scenario.sh OUT_DIR STOP_FILE PHASE_FILE
set -eu
out=${1:?out}; stop=${2:?stop}; phase_file=${3:?phase}; interval=${INTERVAL_S:-1}
mkdir -p "$out"
base=http://127.0.0.1:7010
compose='docker compose -f docker/compose.yaml -f harness/otto-10992/compose.yaml'
: >"$out/samples.jsonl"
while [ ! -e "$stop" ]; do
  ts=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
  ts_ms=$(date +%s%3N)
  phase=$(cat "$phase_file" 2>/dev/null || echo unknown)
  rss=$($compose exec -T engine cat /proc/1/status 2>/dev/null | awk '/VmRSS/{print $2*1024}' || true)
  memory=$(curl --max-time 2 -fsS "$base/memory" 2>/dev/null || echo '{}')
  metrics=$(curl --max-time 2 -fsS "$base/metrics" 2>/dev/null || echo '{}')
  repl=$(curl --max-time 2 -fsS "$base/replication/lsn" 2>/dev/null || echo '{}')
  proxy_stats=$(curl --max-time 2 -fsS http://127.0.0.1:8792/__fault_stats 2>/dev/null || echo '{}')
  slots=$($compose exec -T postgres psql -U postgres -d electric -At -c "SELECT coalesce(json_agg(x),'[]') FROM (SELECT slot_name,active,pg_current_wal_lsn() AS current_lsn,confirmed_flush_lsn,pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn) AS retained_wal_bytes FROM pg_replication_slots) x" 2>/dev/null || echo '[]')
  health_s=$(curl --max-time 2 -sS -o /dev/null -w '%{time_total}' "$base/v1/health" 2>/dev/null || echo null)
  load1=$(awk '{print $1}' /proc/loadavg)
  mem_avail=$(awk '/MemAvailable:/{print $2*1024}' /proc/meminfo)
  delivery=$(tail -n 1 "$out/delivery.jsonl" 2>/dev/null || echo '{}')
  python3 - "$ts" "$ts_ms" "$phase" "${rss:-null}" "$memory" "$metrics" "$repl" "$slots" "$health_s" "$load1" "$mem_avail" "$proxy_stats" "$delivery" >>"$out/samples.jsonl" <<'PY'
import json, sys
keys = ('ts','ts_ms','phase','rss_bytes','engine_memory','engine_metrics','replication_lsn','slot_stats','health_ms','host_load1','host_mem_available_bytes','proxy_stats','delivery')
row = dict(zip(keys, sys.argv[1:]))
for key in ('ts_ms','rss_bytes','engine_memory','engine_metrics','replication_lsn','slot_stats','host_load1','host_mem_available_bytes','proxy_stats','delivery'):
    try: row[key] = json.loads(row[key])
    except Exception: pass
try: row['health_ms'] = float(row['health_ms']) * 1000
except Exception: row['health_ms'] = None
print(json.dumps(row, separators=(',', ':')))
PY
  sleep "$interval"
done
