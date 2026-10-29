#!/usr/bin/env bash
set -euo pipefail

# Same-host, sequential comparison for the dbsp upgrade.  The workload is the existing
# shape-mem-scale harness with membership storage enabled.  Each run gets a new worktree,
# spill directory and artifact directory; no result is reused between revisions or reps.

# Each worktree builds with the toolchain its own rust-toolchain.toml pins.
unset RUSTUP_TOOLCHAIN

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
base_input=${BASE_SHA:?BASE_SHA is required}
head_sha=${HEAD_SHA:?HEAD_SHA is required}
reps=${BENCH_REPS:-5}
artifact_root=${BENCH_ARTIFACT_ROOT:-"$repo_root/.bench-artifacts"}
mkdir -p "$artifact_root"

work_root=$(mktemp -d "${RUNNER_TEMP:-/tmp}/dbsp-spill-work.XXXXXX")
cleanup() {
  git -C "$repo_root" worktree remove --force "$work_root/base" >/dev/null 2>&1 || true
  git -C "$repo_root" worktree remove --force "$work_root/head" >/dev/null 2>&1 || true
  rm -rf "$work_root"
}
trap cleanup EXIT

# A branch name given to a dispatch isn't a local ref, so the base resolves through FETCH_HEAD.
git -C "$repo_root" fetch --no-tags origin "$base_input"
base_tip=$(git -C "$repo_root" rev-parse FETCH_HEAD)
git -C "$repo_root" fetch --no-tags origin "$head_sha"
# The baseline is the point the candidate branched from, so a base branch that moved after
# the branch was cut doesn't leak unrelated changes into the comparison.
base_sha=$(git -C "$repo_root" merge-base "$base_tip" "$head_sha")
git -C "$repo_root" worktree add --detach "$work_root/base" "$base_sha"
git -C "$repo_root" worktree add --detach "$work_root/head" "$head_sha"
# Both revisions run the candidate's harness, so the two sides take the same measurements and
# write the same files. It talks to the engine only through /memory, /debug/dbsp-profile and
# POST /shapes, which the baseline already serves.
cp "$work_root/head/packages/bench/src/shape-mem-scale.ts" "$work_root/base/packages/bench/src/shape-mem-scale.ts"

run_revision() {
  local name=$1 sha=$2 tree=$3
  local out="$artifact_root/$name"
  mkdir -p "$out"
  printf '%s\n' "$sha" > "$out/commit.txt"
  (cd "$tree" && rustc --version) >> "$out/commit.txt"
  (cd "$tree" && pnpm install --frozen-lockfile)
  (cd "$tree" && cargo build --release --locked -p electric-circuits-engine --features test-support)

  for rep in $(seq 1 "$reps"); do
    local run="$out/rep-$rep"
    # Spill files stay outside the uploaded artifacts; the harness records their byte counts.
    local spill="$work_root/spill-$name-$rep"
    mkdir -p "$run"
    echo "[$name rep $rep/$reps] starting"
    (
      cd "$tree"
      SCALE_ISSUES=100000 SCALE_PROJECTS=2000 SCALE_MEMBERSHIPS=6 \
      SCALE_USERS=1000,2500,5000,10000 SCALE_CLIENT_PROCS=4 SCALE_CONC_PER_PROC=12 \
      SCALE_LIVE_SUBS=20000 SCALE_LIVE_PROCS=8 SCALE_MATERIALIZED=1 \
      SCALE_STEADY_UPDATES=20 SCALE_STEADY_INTERVAL_MS=250 \
      SCALE_ATTRIBUTION=1 SCALE_ATTRIBUTION_LABEL="$name-rep-$rep" \
      SCALE_OUT="$run/shape-memory-scale.md" SCALE_ATTRIBUTION_DIR="$run/raw" \
      ELECTRIC_CIRCUITS_SUBQ_STORAGE=1 ELECTRIC_CIRCUITS_SUBQ_STORAGE_CACHE_MIB=64 \
      ELECTRIC_CIRCUITS_SUBQ_STORAGE_DIR="$spill" \
      ELECTRIC_CIRCUITS_TXN_SPILL_DIR="$work_root/txn-spill-$name-$rep" \
      pnpm --filter @electric-circuits/bench exec tsx src/shape-mem-scale.ts
    ) 2>&1 | tee "$run/console.log"
    (cd "$run" && find . -type f -print0 | sort -z | xargs -0 sha256sum > artifact-sha256sums.txt)
    rm -rf "$spill" "$work_root/txn-spill-$name-$rep"
  done
}

run_revision baseline "$base_sha" "$work_root/base"
run_revision candidate "$head_sha" "$work_root/head"

python3 - "$artifact_root" "$base_sha" "$head_sha" "$reps" > "$artifact_root/results.md" <<'PY'
import hashlib, json, pathlib, sys

root = pathlib.Path(sys.argv[1])
base_sha, head_sha, reps = sys.argv[2], sys.argv[3], int(sys.argv[4])

def load(rev, rep):
    return json.loads((root / rev / f"rep-{rep}" / "raw-run.json").read_text())

def h(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def avg(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None

def metrics(rev, rep):
    d = load(rev, rep)
    io = d["io"]
    s = d["steady"]
    if s.get("missed_deliveries", 0):
        raise SystemExit(f"{rev} rep {rep} had {s['missed_deliveries']} missed steady deliveries; refusing to summarize shifted latency samples")
    cold = d["cold"]
    steady_phase = d["steady_phase"]
    raw = root / rev / f"rep-{rep}" / "raw"
    profile_cold = json.loads((raw / f"attribution-{rev}-rep-{rep}-cold-dbsp-profile.json").read_text())
    profile_steady = json.loads((raw / f"attribution-{rev}-rep-{rep}-steady-dbsp-profile.json").read_text())
    for phase, profile in (("cold", profile_cold), ("steady", profile_steady)):
        mcp = profile.get("membership_circuit", {})
        # A failed profiler read comes back as 0 bytes with the error under `profile`, and an
        # unparseable dump as a string; either would read as a real zero in the table.
        inner = mcp.get("profile")
        if profile.get("error") or mcp.get("error") or isinstance(inner, str) or (isinstance(inner, dict) and inner.get("error")):
            raise SystemExit(f"{rev} rep {rep} {phase} dbsp profile is an error: {profile}")
    mc = profile_cold["membership_circuit"]
    ms = profile_steady["membership_circuit"]
    return {
        "cold_rss_mib": cold["engineRssMib"],
        "cold_membership_bytes": cold["card"].get("bytes_membership_circuit", 0),
        "cold_integral_bytes": cold["card"].get("bytes_circuit_integral", 0),
        "cold_snapshot_bytes": cold["card"].get("bytes_circuit_snapshots", 0),
        "cold_spill_disk_bytes": io["spill_disk_bytes_cold"],
        "cold_io_read_bytes": io["cold_read_bytes"],
        "cold_io_write_bytes": io["cold_write_bytes"],
        "steady_rss_mib": steady_phase["engineRssMib"],
        "steady_membership_bytes": steady_phase["card"].get("bytes_membership_circuit", 0),
        "steady_spill_disk_bytes": io["spill_disk_bytes_steady"],
        "cold_profile_used_bytes": mc["total_used_bytes"],
        "cold_profile_storage_bytes": mc["total_storage_bytes"],
        "steady_profile_used_bytes": ms["total_used_bytes"],
        "steady_profile_storage_bytes": ms["total_storage_bytes"],
        "steady_io_read_bytes": io["steady_read_bytes"],
        "steady_io_write_bytes": io["steady_write_bytes"],
        "delivery_p50_ms": s["latency_ms"]["p50"],
        "delivery_p95_ms": s["latency_ms"]["p95"],
        "delivery_max_ms": s["latency_ms"]["max"],
        "cpu_mean_percent": s["cpu_percent"]["mean"],
        "cpu_max_percent": s["cpu_percent"]["max"],
        "missed_deliveries": s["missed_deliveries"],
    }

rows = []
for rev, sha in (("baseline", base_sha), ("candidate", head_sha)):
    for rep in range(1, reps + 1):
        run = root / rev / f"rep-{rep}"
        rows.append((rev, rep, sha, metrics(rev, rep), h(run / "raw-run.json")))

print("# dbsp 0.318 vs 0.365 spill and memory comparison")
print()
print(f"Baseline commit: `{base_sha}` (merge-base); candidate commit: `{head_sha}`; repetitions: {reps} per revision, sequential on one runner. Both revisions ran the candidate's `shape-mem-scale.ts`.")
print("Workload: `shape-mem-scale.ts`, 100,000 issues, 2,000 projects, 6 memberships/user, milestones 1,000/2,500/5,000/10,000 users, 64 MiB DBSP cache, membership spill enabled, then 20 steady updates at 250 ms, then 20,000 held live feeds.")
print("`cold` covers seeding and shape creation through the final milestone. `steady` runs after it and before the live feeds are held: it measures commit-to-feed delivery for 20 fenced long-poll updates that change only `issues.title`, so it does not exercise membership-relation spill and its spill-directory delta is expected to be near zero; read the cold columns for the LZ4 comparison. Steady CPU is the engine's utime+stime from `/proc/<pid>/stat` over each interval between updates, not its lifetime average; steady latencies exclude long polls that timed out (counted as missed). Engine process I/O is recorded from `/proc/<pid>/io`; the dedicated DBSP storage directory is scanned for on-disk bytes. The I/O counters include other engine file I/O and are retained as raw evidence rather than treated as DBSP-only counters.")
print()
print("| revision | rep | cold RSS MiB | membership bytes | integral bytes | snapshot bytes | DBSP used bytes | DBSP storage bytes | cold spill dir bytes | cold read bytes | cold write bytes | steady RSS MiB | steady DBSP used | steady DBSP storage | steady p50 ms | steady p95 ms | steady CPU mean % | steady CPU max % | steady missed deliveries | steady spill dir delta |")
print("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
for rev, rep, sha, m, raw_hash in rows:
    print(f"| {rev} | {rep} | {m['cold_rss_mib']:.1f} | {m['cold_membership_bytes']} | {m['cold_integral_bytes']} | {m['cold_snapshot_bytes']} | {m['cold_profile_used_bytes']} | {m['cold_profile_storage_bytes']} | {m['cold_spill_disk_bytes']} | {m['cold_io_read_bytes']} | {m['cold_io_write_bytes']} | {m['steady_rss_mib']:.1f} | {m['steady_profile_used_bytes']} | {m['steady_profile_storage_bytes']} | {m['delivery_p50_ms']:.2f} | {m['delivery_p95_ms']:.2f} | {m['cpu_mean_percent']:.2f} | {m['cpu_max_percent']:.2f} | {m['missed_deliveries']} | {m['steady_spill_disk_bytes']} |")
    print(f"  - raw-run SHA-256: `{raw_hash}`; all files: `{(root / rev / f'rep-{rep}' / 'artifact-sha256sums.txt').relative_to(root)}`")
print()
for rev in ("baseline", "candidate"):
    ms = [m for r, _, _, m, _ in rows if r == rev]
    print(f"**{rev} mean (n={len(ms)}):** cold RSS {avg([m['cold_rss_mib'] for m in ms]):.1f} MiB; membership bytes {avg([m['cold_membership_bytes'] for m in ms]):.0f}; integral bytes {avg([m['cold_integral_bytes'] for m in ms]):.0f}; snapshot bytes {avg([m['cold_snapshot_bytes'] for m in ms]):.0f}; DBSP used/storage {avg([m['cold_profile_used_bytes'] for m in ms]):.0f}/{avg([m['cold_profile_storage_bytes'] for m in ms]):.0f} bytes; cold spill directory delta {avg([m['cold_spill_disk_bytes'] for m in ms]):.0f} bytes; steady RSS {avg([m['steady_rss_mib'] for m in ms]):.1f} MiB; steady DBSP used/storage {avg([m['steady_profile_used_bytes'] for m in ms]):.0f}/{avg([m['steady_profile_storage_bytes'] for m in ms]):.0f} bytes; steady delivery p50/p95 {avg([m['delivery_p50_ms'] for m in ms]):.2f}/{avg([m['delivery_p95_ms'] for m in ms]):.2f} ms; steady CPU mean {avg([m['cpu_mean_percent'] for m in ms]):.2f}%.")
print()
print("The proposal requires this measurement before merge and sets no pass threshold; compare the candidate means with the baseline means above.")
PY

sha256sum "$artifact_root/results.md" > "$artifact_root/results.md.sha256"
cat "$artifact_root/results.md"
