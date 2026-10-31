#!/usr/bin/env python3
"""Render summary.json files (one per scenario job) as a Markdown table.

usage: table.py RUN_URL summary.json [summary.json ...]
"""
import json
import sys

run_url, paths = sys.argv[1], sys.argv[2:]
rows = [json.load(open(p)) for p in paths]
order = ["control", "control-900", "a", "b-1", "b-5", "b-15", "c-200", "c-2000", "d", "e"]


def key(r):
    name, _, rate = (r.get("scenario") or "").partition(":")
    return (int(rate or r.get("rate_tps") or 0), order.index(name) if name in order else 99, name)


def n(v, digits=0):
    if v is None:
        return "–"
    if isinstance(v, float):
        return f"{v:,.{digits}f}"
    return f"{v:,}"


def mib(v):
    return "–" if v is None else f"{v / 1048576:,.1f}"


def ms(v):
    if v is None:
        return "not seen"
    return f"{v / 1000:,.1f} s" if v >= 1000 else f"{v} ms"


head = ["scenario", "runner", "rate (txn/s)", "hold", "env/s warmup → hold", "unaffected plain env (hold)",
        "unaffected group env (hold)", "faulted group env (hold)", "marker during (plain / group)",
        "marker after (plain / group)", "faulted resumes after clear", "peak RSS MiB", "lane backlog peak (batches / MiB)",
        "receipt queue peak", "checkpoint lag peak MiB", "change-log lag peak MiB", "retained WAL MiB (hold start → peak)",
        "host load1 peak"]
print("| " + " | ".join(head) + " |")
print("|" + "---|" * len(head))
for r in sorted(rows, key=key):
    runner = r.get("runner") or {}
    cells = [
        f"`{r.get('scenario')}`",
        f"{runner.get('runner_name', '?')} ({runner.get('nproc', '?')} vCPU)",
        n(r.get("rate_tps")),
        f"{r.get('hold_s')} s",
        f"{n(r.get('envelopes_per_s_warmup'))} → {n(r.get('envelopes_per_s_hold'))}",
        n(r.get("unaffected_plain_envelopes_hold")),
        n(r.get("unaffected_group_envelopes_hold")),
        n(r.get("faulted_group_envelopes_hold")),
        f"{ms(r['marker_during_ms']['plain'])} / {ms(r['marker_during_ms']['group'])}",
        f"{ms(r['marker_after_ms']['plain'])} / {ms(r['marker_after_ms']['group'])}",
        ms(r.get("faulted_resume_ms")) if r.get("faulted_group_envelopes_hold") is not None else "–",
        mib(r.get("rss_peak_bytes")),
        f"{n(r.get('lane_backlog_peak_batches'))} / {mib(r.get('lane_backlog_peak_bytes'))}",
        n(r.get("receipt_queue_peak_depth")),
        mib(r.get("checkpoint_lag_peak_bytes")),
        mib(r.get("change_log_lag_peak_bytes")),
        f"{mib(r.get('retained_wal_hold_start_bytes'))} → {mib(r.get('retained_wal_peak_bytes'))}",
        n(r.get("host_load1_peak_hold"), 1),
    ]
    print("| " + " | ".join(cells) + " |")
print(f"\nRaw: {run_url}")
