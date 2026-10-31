#!/bin/bash
# One scenario on a fresh stack. SCENARIO is `name` or `name:rate`:
#   control | control-900  no fault, hold 300 s | 900 s
#   a        one stalled group (every append to its stream held), hold 300 s
#   b-N      DS outage for every engine request, hold N minutes
#   c-MS     MS ms delay on every append to one group's stream, hold 300 s
#   d        stalled group whose only source is sparse (one write per 10 s), hold 300 s
#   e        200 groups registered as a burst, no fault, window 300 s
# Writes run at RATE transactions/s (4 rows each) through warmup, hold and recovery.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BASE=(docker compose -f "$ROOT/docker/compose.yaml" -f "$ROOT/harness/otto-10992/compose.yaml")
OUT=${OUT_DIR:-$ROOT/harness/otto-10992/results}
SPEC=${SCENARIO:?SCENARIO}
NAME=${SPEC%%:*}
RATE=${RATE:-200}
[ "$SPEC" != "$NAME" ] && RATE=${SPEC#*:}
WARMUP_S=${WARMUP_S:-30}
RECOVER_S=${RECOVER_S:-120}
mkdir -p "$OUT"
cd "$ROOT"

psql() { "${BASE[@]}" exec -T postgres psql -U postgres -d electric "$@"; }
fault() { curl --max-time 5 -fsS "http://127.0.0.1:8792/__fault?$1" > "$OUT/fault-$2.json"; }
wait_http() { for _ in $(seq 1 120); do curl --max-time 5 -fsS http://127.0.0.1:7010/v1/health >/dev/null && return; sleep 1; done; return 1; }
now_ms() { date +%s%3N; }
phase() { printf '%s' "$1" > "$OUT/phase"; printf '%s\t%s\n' "$1" "$(now_ms)" >> "$OUT/phases.tsv"; }

seed() {
  psql -c "INSERT INTO record(group_id,body) SELECT 1, repeat('r', 256) FROM generate_series(1,1000)" >/dev/null
  psql -c "INSERT INTO record_items(record_id,body) SELECT id, repeat('i', 128) FROM record" >/dev/null
  psql -c "INSERT INTO record_meta(record_id,body) SELECT id, repeat('m', 128) FROM record" >/dev/null
  psql -c "INSERT INTO plain_events(group_id,body) SELECT 1, repeat('p', 256) FROM generate_series(1,1000)" >/dev/null
  psql -c "INSERT INTO sparse_events(group_id,body) VALUES (1, 'seed')" >/dev/null
}

stream_path() { python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["streamPath"])' "$1"; }

load_group() { # name members-json
  curl --max-time 60 -fsS -H content-type:application/json -X POST http://127.0.0.1:7010/v1/subsets/load \
    -d '{"subscription":"otto-'"$1"'","members":'"$2"'}' > "$OUT/$1-load.json"
  stream_path "$OUT/$1-load.json"
}

RECORD_MEMBERS='[{"table":"public.record","where":{"col":"group_id","op":"eq","value":1}},{"table":"public.record_items"},{"table":"public.record_meta"}]'

marker() { # name
  local m="otto-marker-$NAME-$1-$(now_ms)"
  local commit_ms
  commit_ms=$(psql -qAt -c "INSERT INTO plain_events(group_id,body) VALUES (1, '$m') RETURNING (extract(epoch from clock_timestamp())*1000)::bigint" | head -1)
  printf '%s\t%s\t%s\n' "$1" "$commit_ms" "$m" >> "$OUT/markers.tsv"
}

{
  printf '{"runner_name":"%s","runner_os":"%s","runner_arch":"%s","image_os":"%s","image_version":"%s","nproc":%s,"mem_total_bytes":%s,"cpu_model":"%s"}\n' \
    "${RUNNER_NAME:-}" "${RUNNER_OS:-}" "${RUNNER_ARCH:-}" "${ImageOS:-}" "${ImageVersion:-}" "$(nproc)" \
    "$(awk '/MemTotal:/{print $2*1024}' /proc/meminfo)" "$(awk -F': ' '/model name/{print $2; exit}' /proc/cpuinfo)"
} > "$OUT/runner.json"

wait_http
seed
: > "$OUT/markers.tsv"; : > "$OUT/phases.tsv"
UNAFFECTED_PLAIN=$(curl --max-time 60 -fsS -H content-type:application/json -X POST http://127.0.0.1:7010/v1/shapes \
  -d '{"table":"public.plain_events","subscription":"otto-unaffected-plain"}' | tee "$OUT/unaffected-plain-load.json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["streamPath"])')
UNAFFECTED_GROUP=$(load_group unaffected '[{"table":"public.plain_events"},{"table":"public.sparse_events"}]')

hold=300; faulted=""; sparse=0; burst=0; fault_q=""
case "$NAME" in
  control) ;;
  control-900) hold=900 ;;
  a) faulted=$(load_group stalled "$RECORD_MEMBERS"); fault_q="match=/$faulted&hang=1" ;;
  b-*) hold=$(( ${NAME#b-} * 60 )); faulted=$(load_group outage "$RECORD_MEMBERS"); fault_q="outage=1" ;;
  c-*) faulted=$(load_group delayed "$RECORD_MEMBERS"); fault_q="match=/$faulted&delay_ms=${NAME#c-}" ;;
  d) sparse=1; faulted=$(load_group sparse '[{"table":"public.sparse_events","where":{"col":"group_id","op":"eq","value":1}}]'); fault_q="match=/$faulted&hang=1" ;;
  e) burst=200 ;;
  *) echo "unknown scenario $NAME" >&2; exit 2 ;;
esac

stop="$OUT/stop"; rm -f "$stop"
python3 "$ROOT/harness/otto-10992/delivery_probe.py" "$OUT" "$stop" \
  "plain=$UNAFFECTED_PLAIN" "group=$UNAFFECTED_GROUP" "faulted=$faulted" > "$OUT/delivery-probe.log" 2>&1 &
probe_pid=$!
phase warmup
INTERVAL_S=1 "$ROOT/harness/otto-10992/run-scenario.sh" "$OUT" "$stop" "$OUT/phase" > "$OUT/sampler.log" 2>&1 &
sampler_pid=$!

total=$((WARMUP_S + hold + RECOVER_S))
"${BASE[@]}" exec -T postgres sh -c 'cat > /tmp/otto-10992-pgbench.sql' < "$ROOT/harness/otto-10992/pgbench.sql"
workers=8; [ "$RATE" -ge 1000 ] && workers=16
"${BASE[@]}" exec -T postgres pgbench -n -U postgres -d electric -f /tmp/otto-10992-pgbench.sql \
  -c "$workers" -j 4 -T "$total" -R "$RATE" -P 10 > "$OUT/pgbench.log" 2>&1 &
writer=$!
sparse_writer=""
if [ "$sparse" = 1 ]; then
  ( end=$(( $(date +%s) + total )); while [ "$(date +%s)" -lt "$end" ]; do
      psql -c "INSERT INTO sparse_events(group_id,body) VALUES (1, repeat('s', 256))" >/dev/null || true; sleep 10; done ) &
  sparse_writer=$!
fi

sleep "$WARMUP_S"
phase hold
hold_start=$(now_ms)
if [ -n "$fault_q" ]; then fault "$fault_q" set; fi
burst_pid=""
if [ "$burst" -gt 0 ]; then
  ( for i in $(seq 1 "$burst"); do
      curl --max-time 120 -sS -o /dev/null -w '%{http_code} %{time_total}\n' -H content-type:application/json -X POST http://127.0.0.1:7010/v1/subsets/load \
        -d '{"subscription":"otto-burst-'"$i"'","members":[{"table":"public.plain_events"},{"table":"public.record"}]}'
    done > "$OUT/burst.log" 2>&1 ) &
  burst_pid=$!
fi
sleep 1
marker during
sleep $(( hold - 1 - ($(now_ms) - hold_start) / 1000 > 0 ? hold - 1 - ($(now_ms) - hold_start) / 1000 : 0 ))
phase recovery
clear_ms=$(now_ms)
if [ -n "$fault_q" ]; then fault "" clear; fi
marker after
sleep "$RECOVER_S"
touch "$stop"
wait "$writer" 2>/dev/null || true
[ -n "$sparse_writer" ] && kill "$sparse_writer" 2>/dev/null || true
[ -n "$burst_pid" ] && wait "$burst_pid" 2>/dev/null || true
wait "$probe_pid" "$sampler_pid" 2>/dev/null || true
phase done

printf '{"scenario":"%s","name":"%s","rate_tps":%s,"rows_per_txn":4,"warmup_s":%s,"hold_s":%s,"recover_s":%s,"hold_start_ms":%s,"clear_ms":%s,"faulted_stream":"%s","unaffected_plain_stream":"%s","unaffected_group_stream":"%s","fault":"%s","burst_groups":%s,"engine_ref":"%s"}\n' \
  "$SPEC" "$NAME" "$RATE" "$WARMUP_S" "$hold" "$RECOVER_S" "$hold_start" "$clear_ms" "$faulted" "$UNAFFECTED_PLAIN" "$UNAFFECTED_GROUP" \
  "$fault_q" "$burst" "${ENGINE_REF:-}" > "$OUT/workload.json"
