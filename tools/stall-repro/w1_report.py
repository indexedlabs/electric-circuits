"""Tables for results/simple-keys/W1-TARGET-DESIGN.md, from the retained `paired-w1-*` folders.

    python3 tools/stall-repro/w1_report.py tools/stall-repro/results/simple-keys
"""
from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path

from plain_words_report import load, runner

PHASES = (("w1-control", "loads only"), ("w1-load", "loads, import and deletion"))


def number(value, digits=2):
    return "—" if value is None else f"{value:,.{digits}f}"


def probe(entry):
    if not entry["attempts"]:
        return "none"
    lost = entry["attempts"] - entry["successes"]
    return f"{entry['successes']:,}/{entry['attempts']:,}; p95 {number(entry['successful_p95_s'], 3)} s, max {number(entry['successful_max_s'], 2)} s; {lost} timed out"


def loads(entry):
    if not entry["due"]:
        return "none"
    return f"{entry['ok']:,}/{entry['due']:,} answered; p50 {number(entry['wall_p50_s'], 3)} s, p95 {number(entry['wall_p95_s'], 3)} s, max {number(entry['wall_max_s'], 2)} s; {entry['timed_out']} timed out, {entry['shed']} shed, {entry['errors']} failed; started p95 {number(entry['lag_p95_s'], 3)} s late"


def megabytes(value):
    return f"{value / 1e6:,.1f} MB"


def cpu_samples(folder, workload):
    path = folder / f"{workload}-cpu.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def longest_gap(folder, workload, kind="drain"):
    """The longest run of the bystander's requests of one kind that all failed, in seconds."""
    path = folder / f"{workload}-probes.jsonl"
    if not path.exists():
        return None
    samples = sorted((json.loads(line) for line in path.read_text().splitlines()), key=lambda sample: sample["at_s"])
    longest, began = 0.0, None
    for sample in samples:
        if sample["kind"] != kind:
            continue
        if sample["success"]:
            began = None
        else:
            began = sample["at_s"] if began is None else began
            longest = max(longest, sample["at_s"] + sample["duration_s"] - began)
    return longest


def cell(root, count):
    found = sorted(root.glob(f"paired-w1-{count}-*-W1-{count}-prod9bc-1cpu"))
    if not found:
        return False
    folder = found[-1]
    results = {workload: load(folder / f"{workload}-result.json") for workload, _ in PHASES if (folder / f"{workload}-result.json").exists()}
    if not results:
        print(f"### {count:,} active accounts\n\nNo phase completed in `{folder.name}`.\n")
        return True
    calibration = load(folder / "catalog-calibration.json")["actual"]["cardinalities"]
    seed = load(folder / "seed.json") if (folder / "seed.json").exists() else {}
    print(f"### {count:,} active accounts ({runner(folder)}, `{folder.name}`)\n")
    print(f"Engine shapes: {calibration['shapes']:,} (all standalone: {calibration['standalone']:,}; subquery shapes {calibration['subquery_shapes']}). Family rows seeded: {sum(seed.get('family_rows', {}).values()):,}.\n")
    print("| | " + " | ".join(label for workload, label in PHASES if workload in results) + " |")
    print("| --- | " + " | ".join("---" for workload, _ in PHASES if workload in results) + " |")
    def row(label, value):
        print(f"| {label} | " + " | ".join(value(workload, results[workload]) for workload, _ in PHASES if workload in results) + " |")
    row("Phase length", lambda workload, result: f"{result['elapsed_s']:.0f} s")
    row("Bystander drains (receipts)", lambda workload, result: probe(result["probe"]["drain"]))
    row("Bystander subset loads", lambda workload, result: probe(result["probe"]["subset"]))
    row("Bystander live changes", lambda workload, result: probe(result["probe"]["live"]))
    row("Longest run of lost drains", lambda workload, result: f"{number(longest_gap(folder, workload), 1)} s")
    for kind, label in (("day", "Day loads"), ("week", "Week loads"), ("detail", "Record detail opens")):
        row(f"Other accounts' {label.lower()}", lambda workload, result, kind=kind: loads(result["loads"][kind]))
    row("Loads served", lambda workload, result: f"{result['loads']['all']['requests'] / max(1, result['loads'].get('span_s', 1)):,.0f} requests/s, {megabytes(result['loads']['all']['bytes'] / max(1, result['loads'].get('span_s', 1)))}/s")
    row("Rows per load, p50 / p95", lambda workload, result: " ; ".join(f"{kind} {result['loads'][kind]['rows_p50']:,} / {result['loads'][kind]['rows_p95']:,}" for kind in ("day", "week", "detail")))
    row("Engine CPU (of 1)", lambda workload, result: f"{result['engine_cpu_percent']:.0f}%")
    def memory(workload, result):
        samples = cpu_samples(folder, workload)
        return f"{max(sample['rss_bytes'] for sample in samples) / 2**20:,.0f} MiB" if samples else "—"
    row("Engine peak RSS", memory)
    def machine(workload, result):
        samples = [sample["machine_cpu_percent"] for sample in cpu_samples(folder, workload)[1:] if "machine_cpu_percent" in sample]
        return f"{statistics.mean(samples):.0f}% mean, {max(samples):.0f}% peak" if samples else "—"
    row("Runner CPU, all 4 cores", machine)
    row("Sequencer held: peak / longest run", lambda workload, result: f"{result['sequencer_held']['peak_held_bytes']:,} B / {result['sequencer_held']['longest_held_run_s']:.0f} s")
    def writer(name):
        def value(workload, result):
            entry = next((entry for entry in result.get("w1", []) if entry["operation"] == name), None)
            if entry is None:
                return "—"
            receipts = entry["receipts"]
            return f"{entry['wall_s']:.0f} s; {receipts['commits']:,} commits, receipt p50 {number(receipts.get('p50_s'), 3)} s, p95 {number(receipts.get('p95_s'), 3)} s, max {number(receipts.get('max_s'), 2)} s"
        return value
    row("Import: wall time and per-commit receipt", writer("import"))
    row("Deletion: wall time and per-commit receipt", writer("delete"))
    print()
    if "w1-load" in results and results["w1-load"].get("followers"):
        result = results["w1-load"]
        walls = {entry["operation"]: entry["wall_s"] for entry in result.get("w1", [])}
        print("| A phone that follows | Rows delivered | Bytes delivered | Over | Rows a minute | Bytes a minute | Of which a day-window phone would get | Of which a week-window phone would get |")
        print("| --- | ---: | ---: | --- | ---: | ---: | --- | --- |")
        for name, label, span in (("import", "the importing family, all tables", walls.get("import")), ("delete", "the deleted account's family, all tables", walls.get("delete")), ("bystander", "its own (bystander) family, all tables", result["elapsed_s"])):
            follower = result["followers"].get(name)
            if follower is None or not span:
                continue
            total = follower["total"]
            windows = " | ".join(f"{total[window]['rows']:,} rows, {megabytes(total[window]['bytes'])}" if name != "bystander" else "—" for window in ("day", "week"))
            print(f"| {label} | {total['rows']:,} | {megabytes(total['bytes'])} | {span:.0f} s | {total['rows'] * 60 / span:,.0f} | {megabytes(total['bytes'] * 60 / span)} | {windows} |")
        print()
        for name in ("import", "delete"):
            follower = result["followers"].get(name)
            if follower:
                print(f"By table, {name}: " + ", ".join(f"`{table}` {entry['rows']:,} rows / {megabytes(entry['bytes'])}" for table, entry in follower["tables"].items()) + f". Rows with no date to place: {follower['total'].get('unplaced_rows', 0):,}.\n")
    return True


if __name__ == "__main__":
    base = Path(sys.argv[1])
    for size in (20, 200, 2000):
        cell(base, size)
