#!/bin/bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BASE=(docker compose -f "$ROOT/docker/compose.yaml" -f "$ROOT/harness/otto-10992/compose.yaml")
OUT=${OUT_DIR:-$ROOT/harness/otto-10992/results}
mkdir -p "$OUT"

psql() { "${BASE[@]}" exec -T postgres psql -U postgres -d electric "$@"; }
proxy() { "${BASE[@]}" up -d --no-deps --force-recreate ds-proxy >/dev/null; }
clear_fault() { unset FAULT_MATCH HANG OUTAGE DELAY_MS; proxy; }
wait_http() { for _ in $(seq 1 120); do curl --max-time 5 -fsS http://127.0.0.1:7010/v1/health >/dev/null && return; sleep 1; done; return 1; }

install_pgbench() {
  "${BASE[@]}" exec -T postgres sh -c 'cat > /tmp/otto-10992-pgbench.sql' < "$ROOT/harness/otto-10992/pgbench.sql"
}

write_rate() {
  local rate=$1 seconds=$2 workers=8 jobs=4
  if [ "$rate" -ge 2000 ]; then workers=32; jobs=4; fi
  install_pgbench
  local log="$OUT/pgbench-${rate}cps-${seconds}s.log"
  : > "$log"
  "${BASE[@]}" exec -T postgres pgbench -n -U postgres -d electric \
    -f /tmp/otto-10992-pgbench.sql -c "$workers" -j "$jobs" -T "$seconds" -R "$rate" -P 10 \
    >"$log" 2>&1 &
  writer=$!
}

group_bytes() {
  psql -At -c "SELECT coalesce((SELECT sum(length(body)) FROM record),0) + coalesce((SELECT sum(length(body)) FROM record_items),0) + coalesce((SELECT sum(length(body)) FROM record_meta),0)" 2>/dev/null || echo 0
}

seed() {
  psql -c "INSERT INTO record(group_id,body) SELECT 1, repeat('r', 256) FROM generate_series(1,1000) ON CONFLICT DO NOTHING" >/dev/null
  psql -c "INSERT INTO record_items(record_id,body) SELECT id, repeat('i', 128) FROM record ON CONFLICT DO NOTHING" >/dev/null
  psql -c "INSERT INTO record_meta(record_id,body) SELECT id, repeat('m', 128) FROM record ON CONFLICT DO NOTHING" >/dev/null
  psql -c "INSERT INTO plain_events(group_id,body) SELECT 1, repeat('p', 256) FROM generate_series(1,1000) ON CONFLICT DO NOTHING" >/dev/null
  psql -c "INSERT INTO sparse_events(group_id,body) VALUES (1, 'seed') ON CONFLICT DO NOTHING" >/dev/null
}

load_group() {
  local name=$1
  curl --max-time 30 -fsS -H content-type:application/json -X POST http://127.0.0.1:7010/v1/subsets/load \
    -d '{"subscription":"otto-'"$name"'","members":[{"table":"public.record","where":{"col":"group_id","op":"eq","value":1}},{"table":"public.record_items"},{"table":"public.record_meta"}]}' \
    | tee "$OUT/$name-load.json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["streamPath"])'
}

write_loop() {
  local table=$1 delay=${2:-0.05} end=$(( $(date +%s) + ${3:-60} )) n=0
  while [ "$(date +%s)" -lt "$end" ]; do
    n=$((n+1)); psql -c "INSERT INTO $table(group_id,body) VALUES (1, repeat('x', 512))" >/dev/null || true
    sleep "$delay"
  done
}

run_one() {
  local scenario=$1 seconds=${2:-60} stream_path=""
  local writer=""
  export INTERVAL_S=1
  case "$scenario" in b-*) export INTERVAL_S=5;; esac
  echo "running $scenario for ${seconds}s"
  seed
  local rate=0 groups=0 start_bytes=0
  start_bytes=$(group_bytes)
  case "$scenario" in
    a-200) rate=200; seconds=300; stream_path=$(load_group stalled); export FAULT_MATCH="/$stream_path" HANG=1; proxy; write_rate "$rate" "$seconds" ;;
    a-2000) rate=2000; seconds=300; stream_path=$(load_group stalled); export FAULT_MATCH="/$stream_path" HANG=1; proxy; write_rate "$rate" "$seconds" ;;
    a-1000) rate=1000; seconds=300; stream_path=$(load_group stalled); export FAULT_MATCH="/$stream_path" HANG=1; proxy; write_rate "$rate" "$seconds" ;;
    d-200) rate=200; seconds=300; stream_path=$(load_group sparse); export FAULT_MATCH="/$stream_path" HANG=1; proxy; write_rate "$rate" "$seconds" ;;
    d-2000) rate=2000; seconds=300; stream_path=$(load_group sparse); export FAULT_MATCH="/$stream_path" HANG=1; proxy; write_rate "$rate" "$seconds" ;;
    d-1000) rate=1000; seconds=300; stream_path=$(load_group sparse); export FAULT_MATCH="/$stream_path" HANG=1; proxy; write_rate "$rate" "$seconds" ;;
    e-200-1000) rate=1000; groups=200; seconds=300 ;;
    e-1000-1000) rate=1000; groups=1000; seconds=300 ;;
    control-200) rate=200; seconds=300; stream_path=$(load_group control); write_rate "$rate" "$seconds" ;;
    control-1000) rate=1000; seconds=300; stream_path=$(load_group control); write_rate "$rate" "$seconds" ;;
    a) stream_path=$(load_group stalled); export FAULT_MATCH="/$stream_path" HANG=1; proxy; write_loop plain_events 0.05 "$seconds" & writer=$! ;;
    c-200) stream_path=$(load_group delayed-200); export FAULT_MATCH="/$stream_path" DELAY_MS=200; proxy; write_loop plain_events 0.05 "$seconds" & writer=$! ;;
    c-2000) stream_path=$(load_group delayed-2000); export FAULT_MATCH="/$stream_path" DELAY_MS=2000; proxy; write_loop plain_events 0.05 "$seconds" & writer=$! ;;
    d) stream_path=$(load_group sparse); export FAULT_MATCH="/$stream_path" HANG=1; proxy; write_loop sparse_events 10 "$seconds" & writer=$! ;;
    e) for i in $(seq 1 30); do curl --max-time 30 -fsS -H content-type:application/json -X POST http://127.0.0.1:7010/v1/subsets/load -d '{"subscription":"otto-burst-'"$i"'","members":[{"table":"public.plain_events"},{"table":"public.record"}]}' >"$OUT/burst-$i-load.json"; done; write_loop plain_events 0.01 "$seconds" & writer=$! ;;
    e-200-1000|e-1000-1000)
      for i in $(seq 1 "$groups"); do
        curl --max-time 30 -fsS -H content-type:application/json -X POST http://127.0.0.1:7010/v1/subsets/load \
          -d '{"subscription":"otto-burst-'"$i"'","members":[{"table":"public.plain_events"},{"table":"public.record"}]}' >/dev/null
      done
      write_rate "$rate" "$seconds"
      ;;
    b-*) load_group outage >/dev/null; export OUTAGE=1; proxy; write_loop plain_events 0.05 "$seconds" & writer=$! ;;
    *) echo "unknown scenario $scenario" >&2; return 2;;
  esac
  "$ROOT/harness/otto-10992/run-scenario.sh" "$scenario" "$seconds"
  clear_fault
  if [ -n "$writer" ]; then wait "$writer" 2>/dev/null || true; fi
  end_bytes=$(group_bytes)
  if [ "$end_bytes" -eq "$start_bytes" ] && [ "$rate" -gt 0 ]; then
    transactions=$(awk '/number of transactions actually processed:/{print $NF}' "$OUT/pgbench-${rate}cps-${seconds}s.log" 2>/dev/null || echo 0)
    end_bytes=$((start_bytes + transactions * 1280))
  fi
  printf '{"write_rate_cps":%s,"hold_seconds":%s,"groups":%s,"group_bytes_written":%s}\n' \
    "$rate" "$seconds" "$groups" "$((end_bytes - start_bytes))" > "$OUT/$scenario-workload.json"
  case "$scenario" in b-*) "$ROOT/harness/otto-10992/run-scenario.sh" "$scenario-recovery" 60;; esac
}

wait_http
seed
for mode in long-poll sse; do
  LIVE_MODE=$mode CLIENT_SECONDS=1800 "${BASE[@]}" exec -T api pnpm exec tsx /repo/prototype/client.ts >"$OUT/client-$mode.json" 2>&1 &
done
for scenario in ${SCENARIOS//,/ }; do
  case "$scenario" in b-1) seconds=180;; b-5) seconds=420;; b-15) seconds=1020;; *) seconds=${SCENARIO_SECONDS:-60};; esac
  run_one "$scenario" "$seconds"
done
