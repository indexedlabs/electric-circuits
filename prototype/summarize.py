#!/usr/bin/env python3
"""Summarize one scenario's results directory into summary.json (one row)."""
import json
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])


def load_jsonl(name):
    path = root / name
    if not path.exists():
        return []
    rows = []
    for line in path.read_text().splitlines():
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    return rows


def load_json(name, default=None):
    try:
        return json.loads((root / name).read_text())
    except (OSError, json.JSONDecodeError):
        return default


def offset(value):
    try:
        return int(str(value["offset"]).split("_")[-1])
    except (KeyError, TypeError, ValueError):
        return None


def get(row, *keys):
    for key in keys:
        if not isinstance(row, dict):
            return None
        row = row.get(key)
    return row


workload = load_json("workload.json", {})
runner = load_json("runner.json", {})
samples = load_jsonl("samples.jsonl")
delivery = load_jsonl("delivery.jsonl")
seen = load_json("markers-seen.json", {})
hold_start, clear = workload.get("hold_start_ms"), workload.get("clear_ms")
hold = [s for s in samples if hold_start and clear and hold_start <= s.get("ts_ms", 0) <= clear]
warm = [s for s in samples if hold_start and s.get("ts_ms", 0) < hold_start]
after = [s for s in samples if clear and s.get("ts_ms", 0) > clear]


def series(rows, fn):
    out = []
    for row in rows:
        try:
            value = fn(row)
        except (KeyError, TypeError, AttributeError):
            value = None
        if isinstance(value, (int, float)):
            out.append(value)
    return out


def rate(rows, fn):
    pts = [(r["ts_ms"], fn(r)) for r in rows if isinstance(get(r, "ts_ms"), int)]
    pts = [(t, v) for t, v in pts if isinstance(v, (int, float))]
    if len(pts) < 2 or pts[-1][0] == pts[0][0]:
        return None
    return (pts[-1][1] - pts[0][1]) / ((pts[-1][0] - pts[0][0]) / 1000)


envelopes = lambda r: get(r, "engine_metrics", "counters", "envelopes_processed")
rss = lambda r: get(r, "engine_memory", "process", "rss_bytes") or r.get("rss_bytes")
lane_bytes = lambda r: sum(v.get("bytes", 0) for v in (get(r, "engine_metrics", "group_lane_backlog") or {}).values())
lane_batches = lambda r: sum(v.get("batches", 0) for v in (get(r, "engine_metrics", "group_lane_backlog") or {}).values())
receipts = lambda r: get(r, "engine_metrics", "gauges", "receipt_queue_depth")
ckpt_lag = lambda r: offset(get(r, "engine_metrics", "sequencer", "processed")) - offset(get(r, "engine_metrics", "sequencer", "checkpoint"))
log_lag = lambda r: offset(get(r, "replication_lsn", "changes")) - offset(get(r, "engine_metrics", "sequencer", "processed"))
wal = lambda r: max(int(s.get("retained_wal_bytes") or 0) for s in r["slot_stats"])
load1 = lambda r: r.get("host_load1")


def delivered(label, start, end):
    """Envelopes a client tailing `label` received between two wall-clock instants."""
    pts = [(d["ts_ms"], get(d, "streams", label, "envelopes")) for d in delivery]
    pts = [(t, v) for t, v in pts if isinstance(v, int)]
    before = [v for t, v in pts if t <= start]
    upto = [v for t, v in pts if t <= end]
    if not pts or not upto:
        return None
    return upto[-1] - (before[-1] if before else pts[0][1])


def first_growth_after(label, instant):
    pts = [(d["ts_ms"], get(d, "streams", label, "envelopes")) for d in delivery]
    pts = [(t, v) for t, v in pts if isinstance(v, int)]
    base = [v for t, v in pts if t <= instant]
    if not base:
        return None
    for t, v in pts:
        if t > instant and v > base[-1]:
            return t - instant
    return None


def marker(stream, name):
    hit = seen.get(f"{stream}:{name}")
    return hit["latency_ms"] if hit else None


def slot_field(rows, field):
    vals = [s.get(field) for r in rows for s in (r.get("slot_stats") or []) if isinstance(r.get("slot_stats"), list)]
    return vals


pgbench = (root / "pgbench.log").read_text() if (root / "pgbench.log").exists() else ""
tps = re.search(r"tps = ([0-9.]+)", pgbench)


def catchup_estimate():
    """Seconds after the clear until the engine has processed everything committed during the
    hold, from envelope rates: backlog = warmup rate x hold - processed in hold, drained at
    (recovery rate - warmup rate). The after-marker latency is the measured value when seen."""
    seen_after = marker("plain", "after")
    if seen_after is not None:
        return round(seen_after / 1000)
    # Incoming is what the writers produced (4 rows per transaction), not what the engine kept up with.
    incoming = float(tps.group(1)) * 4 if tps else None
    recovery = rate(after, envelopes)
    held = rate(hold, envelopes)
    if None in (incoming, recovery, held, hold_s):
        return None
    backlog = max(0.0, (incoming - held) * hold_s)
    if backlog == 0:
        return 0
    drain = recovery - incoming
    if drain <= 0:
        return None
    return round(backlog / drain)


hold_s = (clear - hold_start) / 1000 if hold_start and clear else None
row = {
    "scenario": workload.get("scenario"),
    "engine_ref": workload.get("engine_ref"),
    "runner": runner,
    "rate_tps": workload.get("rate_tps"),
    "pgbench_tps": float(tps.group(1)) if tps else None,
    "hold_s": round(hold_s) if hold_s else None,
    "fault": workload.get("fault"),
    "samples": {"warmup": len(warm), "hold": len(hold), "recovery": len(after)},
    "envelopes_per_s_warmup": rate(warm, envelopes),
    "envelopes_per_s_hold": rate(hold, envelopes),
    "envelopes_per_s_recovery": rate(after, envelopes),
    "unaffected_plain_envelopes_hold": delivered("plain", hold_start, clear) if hold_start else None,
    "unaffected_group_envelopes_hold": delivered("group", hold_start, clear) if hold_start else None,
    "faulted_group_envelopes_hold": delivered("faulted", hold_start, clear) if hold_start else None,
    "unaffected_plain_envelopes_warmup": delivered("plain", 0, hold_start) if hold_start else None,
    "marker_during_ms": {"plain": marker("plain", "during"), "group": marker("group", "during")},
    "marker_after_ms": {"plain": marker("plain", "after"), "group": marker("group", "after")},
    "faulted_resume_ms": first_growth_after("faulted", clear) if clear else None,
    "plain_resume_ms": first_growth_after("plain", clear) if clear else None,
    "unaffected_plain_envelopes_recovery": delivered("plain", clear, 10**15) if clear else None,
    "change_log_lag_at_clear_bytes": (series(hold, log_lag) or [None])[-1],
    "catchup_estimate_s": catchup_estimate(),
    "rss_peak_bytes": max(series(hold + after, rss), default=None),
    "rss_peak_hold_bytes": max(series(hold, rss), default=None),
    "rss_peak_recovery_bytes": max(series(after, rss), default=None),
    "rss_warmup_bytes": max(series(warm, rss), default=None),
    "lane_backlog_peak_bytes": max(series(hold + after, lane_bytes), default=None),
    "lane_backlog_peak_batches": max(series(hold + after, lane_batches), default=None),
    "receipt_queue_peak_depth": max(series(hold + after, receipts), default=None),
    "checkpoint_lag_peak_bytes": max(series(hold + after, ckpt_lag), default=None),
    "change_log_lag_peak_bytes": max(series(hold + after, log_lag), default=None),
    "change_log_lag_end_bytes": (series(after, log_lag) or [None])[-1],
    "retained_wal_hold_start_bytes": (series(hold, wal) or [None])[0],
    "retained_wal_peak_bytes": max(series(hold + after, wal), default=None),
    "retained_wal_end_bytes": (series(after, wal) or [None])[-1],
    "confirmed_flush_hold": [x for x in (slot_field(hold[:1], "confirmed_flush_lsn") + slot_field(hold[-1:], "confirmed_flush_lsn"))],
    "host_load1_peak_hold": max(series(hold, load1), default=None),
    "health_ms_p50_hold": sorted(series(hold, lambda r: r.get("health_ms")))[len(hold) // 2] if series(hold, lambda r: r.get("health_ms")) else None,
    "proxy_stats_end_hold": (hold[-1].get("proxy_stats") if hold else None),
    "delivery_errors": {k: get(delivery[-1], "streams", k, "errors") for k in ("plain", "group", "faulted")} if delivery else None,
}
(root / "summary.json").write_text(json.dumps(row, indent=2, sort_keys=True) + "\n")
print(json.dumps(row, indent=2, sort_keys=True))
