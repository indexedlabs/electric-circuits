"""Tables for results/simple-keys/W1-NO-REBUILD.md: the W1 2,000-account cell, profiled, on prod's
binary and on the engine without the per-page node-state rebuild (OTTO-14463).

    python3 tools/stall-repro/w1_nr_report.py tools/stall-repro/results/simple-keys
"""
from __future__ import annotations

import gzip
import json
import sys
from pathlib import Path

from plain_words_report import load, runner

PHASES = (("w1-load", "Loads, import and deletion"), ("w1-control", "Loads only"))


def shares(path):
    """Share of the engine's CPU samples by area, from folded stacks."""
    total, areas = 0, {"publish_all": 0, "queries": 0, "sequencer": 0, "http": 0, "other": 0}
    for line in gzip.open(path, "rt"):
        stack, _, count = line.rpartition(" ")
        count = int(count)
        total += count
        if "sequencer::publish_all" in stack:
            areas["publish_all"] += count
        elif "sequencer::sequencer_loop" in stack:
            areas["sequencer"] += count
        elif "query_subset" in stack:
            areas["queries"] += count
        elif "http::" in stack or "hyper" in stack or "axum" in stack:
            areas["http"] += count
        else:
            areas["other"] += count
    return total, {area: count / total for area, count in areas.items()}


def phase(folder, workload):
    result = load(folder / f"{workload}-result.json")
    total, share = shares(folder / f"{workload}.folded.gz")
    cpu_s = result["engine_cpu_percent"] / 100 * result["elapsed_s"]
    writers = {entry["operation"]: entry for entry in result.get("w1", [])}
    commits = sum(entry["receipts"]["commits"] for entry in writers.values()) + 2 * result["probe"]["drain"]["attempts"]
    loads = result["loads"]["all"]
    probe = result["probe"]
    return {
        "samples": total, "share": share, "cpu": result["engine_cpu_percent"], "elapsed": result["elapsed_s"], "commits": commits,
        "publish_ms": 1000 * cpu_s * share["publish_all"] / commits, "ok": loads["ok"], "due": loads["due"], "shed": loads["shed"],
        "load_p50": loads["wall_p50_s"], "load_p95": loads["wall_p95_s"], "late_p95": loads["lag_p95_s"], "requests_s": loads["requests"] / result["loads"]["span_s"],
        "drain": probe["drain"], "live": probe["live"], "subset": probe["subset"],
        "import_s": writers.get("import", {}).get("wall_s"), "delete_s": writers.get("delete", {}).get("wall_s"),
        "import_p95": writers.get("import", {}).get("receipts", {}).get("p95_s"), "delete_p50": writers.get("delete", {}).get("receipts", {}).get("p50_s"),
    }


def column(label, folder):
    return label, runner(folder), {workload: phase(folder, workload) for workload, _ in PHASES if (folder / f"{workload}-result.json").exists() and (folder / f"{workload}.folded.gz").exists()}


def table(columns, workload, title):
    print(f"**{title}**\n")
    print("| | " + " | ".join(f"{label} ({machine})" for label, machine, _ in columns) + " |")
    print("| --- | " + " | ".join("---:" for _ in columns) + " |")
    def row(name, value):
        print(f"| {name} | " + " | ".join(value(data[workload]) if workload in data else "—" for _, _, data in columns) + " |")
    row("`publish_all` share of engine CPU", lambda p: f"{100 * p['share']['publish_all']:.1f}%")
    row("`publish_all` CPU per commit", lambda p: f"{p['publish_ms']:.1f} ms")
    row("Subset queries' share (the loads)", lambda p: f"{100 * p['share']['queries']:.1f}%")
    row("Sequencer loop outside `publish_all`", lambda p: f"{100 * p['share']['sequencer']:.1f}%")
    row("Engine CPU (of 1)", lambda p: f"{p['cpu']:.0f}%")
    row("Loads answered of those due", lambda p: f"{p['ok']:,} of {p['due']:,}")
    row("Loads not started within 30 s", lambda p: f"{p['shed']:,}")
    row("Load time p50 / p95", lambda p: f"{p['load_p50']:.2f} s / {p['load_p95']:.2f} s")
    row("Loads started late by, p95", lambda p: f"{p['late_p95']:.2f} s")
    row("Requests served a second", lambda p: f"{p['requests_s']:,.0f}")
    row("Bystander receipt p95 (drains ok/attempts)", lambda p: f"{p['drain']['successful_p95_s']:.2f} s ({p['drain']['successes']}/{p['drain']['attempts']})")
    row("Bystander live change p95", lambda p: f"{p['live']['successful_p95_s']:.2f} s ({p['live']['successes']}/{p['live']['attempts']})")
    if workload == "w1-load":
        row("Import wall time (commit receipt p95)", lambda p: f"{p['import_s']:.0f} s ({p['import_p95']:.2f} s)")
        row("Deletion wall time (commit receipt p50)", lambda p: f"{p['delete_s']:.0f} s ({p['delete_p50']:.3f} s)")
    row("Phase length; commits; samples", lambda p: f"{p['elapsed']:.0f} s; {p['commits']:,}; {p['samples']:,}")
    print()


def main(root):
    columns = []
    today = sorted(root.glob("paired-w1-2000-profile-*-W1-2000-prod9bc-1cpu"))
    if today:
        columns.append(column("Today, W1-PROFILE", today[-1]))
    for folder in sorted(root.glob("paired-w1-2000-nr-pair-*-W1-2000-prod9bc-1cpu")):
        columns.append(column("Today, same runner as the next column", folder))
    for folder in sorted(root.glob("paired-w1-2000-nr-pair-*-W1-2000-nr-1cpu")):
        columns.append(column("Without the rebuild, same runner", folder))
    for folder in sorted(root.glob("paired-w1-2000-nr-[0-9]*-W1-2000-nr-1cpu")):
        columns.append(column("Without the rebuild", folder))
    for workload, title in PHASES:
        table(columns, workload, title)


if __name__ == "__main__":
    main(Path(sys.argv[1]))
