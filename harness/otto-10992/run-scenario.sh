#!/bin/sh
set -eu
label=${1:?label}; duration=${2:-60}; interval=${INTERVAL_S:-1}
out="${OUT_DIR:-harness/otto-10992/results}/${label}-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$out"
base=http://127.0.0.1:7010
compose='docker compose -f docker/compose.yaml -f harness/otto-10992/compose.yaml'

probe() {
  python3 harness/otto-10992/stream_probe.py "$1" "$2" "${3:-0000000000000000_0000000000000000}"
}

cat >"$out/scenario.json" <<EOF
{"label":"$label","duration_s":$duration,"interval_s":$interval,"fault_match":"${FAULT_MATCH:-}","delay_ms":"${DELAY_MS:-0}","hang":"${HANG:-0}","outage":"${OUTAGE:-0}"}
EOF
 : >"$out/samples.jsonl"
plain_offset=$(probe "${UNAFFECTED_PLAIN_PATH:-shape/none}" head | python3 -c 'import json,sys; print(json.load(sys.stdin)["next_offset"])')
group_offset=$(probe "${UNAFFECTED_GROUP_PATH:-shape/none}" head | python3 -c 'import json,sys; print(json.load(sys.stdin)["next_offset"])')
plain_envelopes=0
group_envelopes=0
end=$(( $(date +%s) + duration ))
while [ "$(date +%s)" -lt "$end" ]; do
  ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  rss=$($compose exec -T engine sh -c 'awk "/VmRSS/{print $2*1024}" /proc/1/status' 2>/dev/null | tr -d '\r' || echo null)
  ds_bytes=$($compose exec -T ds sh -c 'du -sb /data 2>/dev/null | cut -f1' 2>/dev/null || echo null)
  memory=$(curl --max-time 1 -fsS "$base/memory" 2>/dev/null || echo '{}')
  metrics=$(curl --max-time 1 -fsS "$base/metrics" 2>/dev/null || echo '{}')
  repl=$(curl --max-time 1 -fsS "$base/replication/lsn" 2>/dev/null || echo '{}')
  roff=$(curl --max-time 1 -fsS "$base/tables/public.record/offset" 2>/dev/null || echo '{}')
  poff=$(curl --max-time 1 -fsS "$base/tables/public.plain_events/offset" 2>/dev/null || echo '{}')
  proxy_stats=$(curl --max-time 1 -fsS http://127.0.0.1:8792/__fault_stats 2>/dev/null || echo '{}')
  plain_probe=$(probe "${UNAFFECTED_PLAIN_PATH:-shape/none}" head)
  group_probe=$(probe "${UNAFFECTED_GROUP_PATH:-shape/none}" head)
  plain_next=$(printf '%s' "$plain_probe" | python3 -c 'import json,sys; print(json.load(sys.stdin)["next_offset"])')
  group_next=$(printf '%s' "$group_probe" | python3 -c 'import json,sys; print(json.load(sys.stdin)["next_offset"])')
  plain_delta=$(printf '%s' "$plain_probe" | python3 -c 'import json,sys; print(json.load(sys.stdin)["envelopes"])')
  group_delta=$(printf '%s' "$group_probe" | python3 -c 'import json,sys; print(json.load(sys.stdin)["envelopes"])')
  plain_envelopes=$((plain_envelopes + plain_delta))
  group_envelopes=$((group_envelopes + group_delta))
  plain_offset=$plain_next
  group_offset=$group_next
  slots=$($compose exec -T postgres psql -U postgres -d electric -At -c "SELECT coalesce(json_agg(x),'[]') FROM (SELECT slot_name,active,pg_current_wal_lsn() AS current_lsn,confirmed_flush_lsn,pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn) AS retained_wal_bytes FROM pg_replication_slots) x" 2>/dev/null || echo '[]')
  latency=$(curl --max-time 1 -sS -o /dev/null -w '%{time_total}' "$base/v1/health" 2>/dev/null || echo null)
  host_load1=$(awk '{print $1}' /proc/loadavg 2>/dev/null || echo null)
  host_mem_available=$(awk '/MemAvailable:/{print $2*1024}' /proc/meminfo 2>/dev/null || echo null)
  host_mem_total=$(awk '/MemTotal:/{print $2*1024}' /proc/meminfo 2>/dev/null || echo null)
  host_nproc=$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo null)
  python3 - "$ts" "$rss" "$ds_bytes" "$memory" "$metrics" "$repl" "$roff" "$poff" "$slots" "$latency" "$host_load1" "$host_mem_available" "$host_mem_total" "$host_nproc" "$proxy_stats" "$plain_offset" "$plain_envelopes" "$group_offset" "$group_envelopes" >>"$out/samples.jsonl" <<'PY'
import json, sys
keys = ('ts','rss_bytes','ds_bytes','engine_memory','engine_metrics','replication_lsn','source_record_offset','source_plain_offset','slot_stats','latency_ms','host_load1','host_mem_available_bytes','host_mem_total_bytes','host_nproc','proxy_stats','unaffected_plain_offset','unaffected_plain_envelopes','unaffected_group_offset','unaffected_group_envelopes')
row = dict(zip(keys, sys.argv[1:]))
for key in keys[1:]:
    if key in ('engine_memory','engine_metrics','replication_lsn','source_record_offset','source_plain_offset','slot_stats','proxy_stats'):
        try: row[key] = json.loads(row[key])
        except Exception: pass
    elif key in ('rss_bytes','ds_bytes','host_mem_available_bytes','host_mem_total_bytes','host_nproc','unaffected_plain_envelopes','unaffected_group_envelopes'):
        try: row[key] = int(row[key])
        except Exception: pass
    elif key == 'latency_ms':
        try: row[key] = float(row[key]) * 1000
        except Exception: pass
    elif key == 'host_load1':
        try: row[key] = float(row[key])
        except Exception: pass
print(json.dumps(row, separators=(',', ':')))
PY
  sleep "$interval"
done
printf '%s\n' "$out"
