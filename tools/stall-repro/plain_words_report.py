"""Tables for results/simple-keys/PLAIN-WORDS-3.md, from the retained run folders.

    python3 tools/stall-repro/plain_words_report.py tools/stall-repro/results/simple-keys

Run 1 reads `paired-pw1-*` (and H1's `paired-h1b-*` as the old baseline), run 2 `paired-pw2-*`,
run 3 `paired-pw3-*`. Every time is one hosted runner's; a commit cell is three commits on one engine.
"""
from __future__ import annotations

import json
import re
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path

BAR_S = 0.5


def load(path):
    return json.loads(path.read_text())


def runner(folder):
    machine = folder / "machine.txt"
    if not machine.exists():
        return "?"
    match = re.search(r"Model name:\s+(.*)", machine.read_text())
    name = match.group(1) if match else "?"
    return re.sub(r"\s+", " ", name.replace("AMD ", "").replace("Intel(R) Xeon(R) Platinum ", "Xeon ").replace("INTEL(R) XEON(R) PLATINUM ", "Xeon ").replace("Intel(R) Xeon(R) ", "Xeon ").replace(" 80-Core Processor", "").replace(" 64-Core Processor", "").replace(" 96-Core Processor", "").replace(" CPU @ 2.80GHz", "")).strip()


def seconds(value, digits=2):
    return "—" if value is None else f"{value:.{digits}f}"


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[int((len(ordered) - 1) * fraction)] if ordered else None


def probes(folder, workload, result):
    """Bystander samples that began inside the commit: from the first change-log delivery to the receipt."""
    path = folder / f"{workload}-probes.jsonl"
    if not path.exists():
        return {}
    samples = [json.loads(line) for line in path.read_text().splitlines()]
    return {kind: [sample for sample in samples if sample["kind"] == kind] for kind in ("drain", "subset", "live")}


def bystander(summary, kind="drain"):
    entry = summary[kind]
    if not entry["attempts"]:
        return "none in the commit"
    text = f"{entry['successes']}/{entry['attempts']}"
    if entry["successes"]:
        text += f"; p95 {seconds(entry['successful_p95_s'])} s, max {seconds(entry['successful_max_s'])} s"
    return text


def pooled(results, kind):
    attempts = sum(result["probe"][kind]["attempts"] for result in results)
    successes = sum(result["probe"][kind]["successes"] for result in results)
    worst = [result["probe"][kind]["successful_max_s"] for result in results if result["probe"][kind]["successful_max_s"] is not None]
    p95 = [result["probe"][kind]["successful_p95_s"] for result in results if result["probe"][kind]["successful_p95_s"] is not None]
    if not attempts:
        return "none fell in a commit"
    return f"{successes}/{attempts}" + (f"; p95 {seconds(max(p95))} s, max {seconds(max(worst))} s" if worst else "")


def commit_cell(folder, prefix):
    """One engine's commits (`<prefix>`, `-r2`, `-r3`): times, CPU, RSS and the bystander."""
    results = [load(path) for path in sorted(folder.glob(f"{prefix}*-result.json"))]
    results = [result for result in results if result.get("completed")]
    if not results:
        return None
    times = [result["first_read_to_empty_s"] for result in results if result.get("first_read_to_empty_s") is not None]
    rss = 0
    for path in folder.glob(f"{prefix}*-cpu.jsonl"):
        rss = max([rss, *(json.loads(line)["rss_bytes"] for line in path.read_text().splitlines())])
    calibration = load(folder / "catalog-calibration.json")["actual"]["cardinalities"]
    return {
        "folder": folder.name, "runner": runner(folder), "times": times, "median": statistics.median(times), "low": min(times), "high": max(times),
        "cpu": statistics.median(result["engine_cpu_percent"] for result in results), "rss_mib": rss / 2**20, "shapes": calibration["shapes"],
        "drain": pooled(results, "drain"), "live": pooled(results, "live"), "subset": pooled(results, "subset"),
        "idle_drain_p95": statistics.median(result["idle_probe"]["drain"]["successful_p95_s"] or 0 for result in results), "rows": results[0]["row_changes"],
        "backfills": sum(result["backfills"] for result in results),
    }


def spread(cell):
    return f"{seconds(cell['median'])} ({seconds(cell['low'])}–{seconds(cell['high'])})"


def run1(root):
    kinds = {"one": "(a) every account one-family, fix", "mix": "(b) prod mix, fix", "one-today": "control: (a)'s accounts, today's lowering"}
    print("## Run 1\n")
    print("| Keyed `record` shapes | Accounts and lowering | Runner | Sequencer time, 3 commits: median (range) s | Per row | Engine CPU % | Peak RSS | Bystander receipts (drains ok/attempts) | Bystander live changes | Under 0.5 s |")
    print("| ---: | --- | --- | ---: | ---: | ---: | ---: | --- | --- | --- |")
    cells = {}
    for count in (40, 1000, 10000, 20000):
        for kind, label in kinds.items():
            found = sorted(root.glob(f"paired-pw1-{count}-*-S-main3-h1p-{count}-{kind}-prod9bc-1cpu"))
            if not found:
                continue
            cell = commit_cell(found[0], "h1-commit")
            if cell is None:
                print(f"| {count:,} | {label} | — | did not complete | | | | | | |")
                continue
            cells[(count, kind)] = cell
            print(f"| {count:,} | {label} | {cell['runner']} | {spread(cell)} | {cell['median']:.2f} ms | {cell['cpu']:.0f} | {cell['rss_mib']:,.0f} MiB | {cell['drain']} | {cell['live']} | {'yes' if cell['high'] < BAR_S else 'no'} |")
        old = sorted(root.glob(f"paired-h1b-{count}-1-*"))
        if old:
            cell = commit_cell(old[0], "h1-commit")
            cells[(count, "h1")] = cell
            print(f"| {count:,} | (c) H1, Nov 26 (one commit, 1 s probe) | {cell['runner']} | {seconds(cell['median'])} | {cell['median']:.2f} ms | {cell['cpu']:.0f} | {cell['rss_mib']:,.0f} MiB | {cell['drain']} | {cell['live']} | {'yes' if cell['high'] < BAR_S else 'no'} |")
    return cells


def fit(points):
    """Least squares t = a + b * n."""
    count = len(points)
    mean_n = sum(n for n, _ in points) / count
    mean_t = sum(t for _, t in points) / count
    slope = sum((n - mean_n) * (t - mean_t) for n, t in points) / sum((n - mean_n) ** 2 for n, _ in points)
    return mean_t - slope * mean_n, slope


def run2(root, per_account=5):
    ways = {"today": "today (main's lowering)", "fix": "run 1's plain-test fix", "split": "selective equality first, session feed split by end"}
    print("\n## Run 2\n")
    print("| Keyed `graph_edge` filters today (accounts) | Lowering | Engine shapes (with probe) | Runner | Sequencer time, 3 commits: median (range) s | Per row | Per row per filter | Engine CPU % | Peak RSS | Bystander receipts (drains ok/attempts) | Bystander live changes | Under 0.5 s |")
    print("| ---: | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |")
    points = defaultdict(list)
    for count in (100, 1000, 5000):
        for way, label in ways.items():
            found = sorted(root.glob(f"paired-pw2-{count}-*-S-main3-e1-{count}-{way}-prod9bc-1cpu"))
            if not found:
                continue
            cell = commit_cell(found[0], "e1-commit")
            if cell is None:
                print(f"| {count:,} | {label} | | — | did not complete | | | | | | | |")
                continue
            points[way].append((count, cell["median"]))
            print(f"| {count:,} | {label} | {cell['shapes']:,} | {cell['runner']} | {spread(cell)} | {cell['median']:.2f} ms | {cell['median'] / cell['rows'] / count * 1e6:.2f} µs | {cell['cpu']:.0f} | {cell['rss_mib']:,.0f} MiB | {cell['drain']} | {cell['live']} | {'yes' if cell['high'] < BAR_S else 'no'} |")
    print("\nRepeats of the 5,000-filter cells, in run order on each runner:\n")
    print("| Job | Order | Lowering | Runner | Sequencer time, 3 commits: median (range) s | Engine CPU % | Bystander receipts (drains ok/attempts) |")
    print("| --- | ---: | --- | --- | ---: | ---: | --- |")
    for folder in sorted(root.glob("paired-pw2-5000-[a-z]-*")):
        _, _, _, job, order, *rest = folder.name.split("-")
        cell = commit_cell(folder, "e1-commit")
        way = next(way for way in ways if f"-5000-{way}-" in folder.name)
        points[f"repeat-{way}"].append(cell["median"])
        print(f"| {job} | {order} | {ways[way]} | {cell['runner']} | {spread(cell)} | {cell['cpu']:.0f} | {cell['drain']} |")
    print("\n| Lowering | Fit: commit s = a + b × filters | Passes 0.5 s at | Passes 2 s at |")
    print("| --- | --- | --- | --- |")
    for way, label in ways.items():
        if len(points[way]) < 2:
            continue
        # The 5,000-filter point is the median over the first pass and the repeats.
        at_5000 = [time for count, time in points[way] if count == 5000] + points[f"repeat-{way}"]
        points[way] = [(count, time) for count, time in points[way] if count != 5000] + [(5000, statistics.median(at_5000))]
        intercept, slope = fit(points[way])
        def crossing(limit):
            if slope <= 0 or (limit - intercept) / slope > 1e6:
                return "not within 1,000,000 filters"
            filters = (limit - intercept) / slope
            return f"{filters:,.0f} filters, about {filters / per_account:,.0f} accounts"
        print(f"| {label} | {intercept:.3f} + {slope * 1e3:.3f} ms × filters | {crossing(0.5)} | {crossing(2.0)} |")
    return points


def run3(root):
    print("\n## Run 3\n")
    ways = {"bundle": "(a) bundled group load", "seq": "(b) table by table, in turn", "par": "(b) table by table, at once"}
    for job in sorted({path.name.split("-")[2] for path in root.glob("paired-pw3-*")}):
        folders = sorted(root.glob(f"paired-pw3-{job}-*"))
        floor = {}
        for folder in folders:
            if "-S-prod9bc" in folder.name and (folder / "idle-60-result.json").exists():
                floor = load(folder / "idle-60-result.json")["probe"]
        print(f"### Job {job} ({runner(folders[0]) if folders else '?'})\n")
        print("| Window | Rows wanted | Way | Loads | Rows returned, median | Wall to a complete window p50 / p95 s | Engine CPU inside the request, median ms | Shapes created per load (subquery) | Subquery nodes per load |")
        print("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |")
        phases = []
        for folder in folders:
            idle = folder / "idle-60-result.json"
            idle_rate = load(idle)["engine_cpu_percent"] / 100 if idle.exists() else 0
            for kind in ("day", "week"):
                for way, label in ways.items():
                    path = folder / f"window-{kind}-{way}-writes.json"
                    if not path.exists():
                        continue
                    journal = load(path)
                    loads = [entry for entry in journal if entry["table"] == "window"]
                    statements = next((entry for entry in journal if entry["table"] == "pg_stat_statements"), {})
                    result = load(folder / f"window-{kind}-{way}-result.json")
                    phases.append((kind, label, folder, result, loads, statements.get("statements", []), idle_rate))
                    failed = [entry for entry in loads if entry["status"] != 200]
                    for size in ("p50", "p95"):
                        chosen = [entry for entry in loads if entry["size"] == size and entry["status"] == 200]
                        if not chosen:
                            continue
                        walls = [entry["wall_s"] for entry in chosen]
                        created = statistics.mean(entry["created"].get("shapes", 0) for entry in chosen)
                        subquery = statistics.mean(entry["created"].get("subquery_shapes", 0) for entry in chosen)
                        nodes = statistics.mean(entry["created"].get("subquery_nodes", 0) for entry in chosen)
                        print(f"| {kind} | {size} | {label} | {len(chosen)} | {statistics.median(entry['rows'] for entry in chosen):,.0f} | {percentile(walls, 0.5):.3f} / {percentile(walls, 0.95):.3f} | {statistics.median(entry['engine_cpu_s'] for entry in chosen) * 1e3:.1f} | {created:.1f} ({subquery:.1f}) | {nodes:.1f} |")
                    if failed:
                        print(f"| {kind} | — | {label} | {len(failed)} failed | {Counter(entry['status'] for entry in failed)} | {failed[0].get('error', failed[0].get('steps'))!s:.200} | | | |")
        # The phase table: Postgres and engine CPU are per phase, net of the bystander's own probes.
        print("\n| Window | Way | Loads | Phase s | Engine CPU % | Engine CPU per load, net of idle, ms | Postgres row selects per load | Read transactions per load | Postgres time per load ms | Other readers' receipts: ok/attempts, p95 s (× S idle) | Subset p95 s (× S idle) | Live change p95 s (× S idle) | Shapes added (subquery) | Query-backs |")
        print("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | ---: | ---: |")
        def against(result, kind):
            entry = result["probe"][kind]
            if not entry["attempts"]:
                return "none"
            base = floor.get(kind, {}).get("successful_p95_s")
            ratio = f" ({entry['successful_p95_s'] / base:.1f}×)" if base and entry["successful_p95_s"] else ""
            return f"{entry['successes']}/{entry['attempts']}; {seconds(entry['successful_p95_s'], 3)}{ratio}"
        for folder in folders:
            idle = folder / "idle-60-result.json"
            if idle.exists():
                result = load(idle)
                name = "S idle" if "-S-prod9bc" in folder.name else "idle before the loads"
                print(f"| — | {name} ({folder.name.split('-')[3]}) | 0 | {result['elapsed_s']:.0f} | {result['engine_cpu_percent']:.0f} | — | — | — | — | {against(result, 'drain')} | {against(result, 'subset')} | {against(result, 'live')} | 0 | 0 |")
        for kind, label, folder, result, loads, statements, idle_rate in phases:
            change = result.get("subquery_change", {})
            count = len(loads)
            # The bystander's subset probe is one row select in one read transaction per attempt.
            probes_made = result["probe"]["subset"]["attempts"]
            engine = [entry for entry in statements if not entry["harness"]]
            selects = [entry for entry in engine if "jsonb_build_object" in entry["query"][:60]]
            transactions = sum(entry["calls"] for entry in engine if entry["query"].startswith("BEGIN ISOLATION LEVEL REPEATABLE READ"))
            net = (result["engine_cpu_percent"] / 100 - idle_rate) * result["elapsed_s"] / count
            print(f"| {kind} | {label} | {count} | {result['elapsed_s']:.0f} | {result['engine_cpu_percent']:.0f} | {net * 1e3:.0f} | {(sum(entry['calls'] for entry in selects) - probes_made) / count:.1f} | {(transactions - probes_made) / count:.1f} | {sum(entry['total_ms'] for entry in selects) / count:.0f} | {against(result, 'drain')} | {against(result, 'subset')} | {against(result, 'live')} | {change.get('shapes', 0)} ({change.get('subquery_shapes', 0)}) | {result['backfills']} |")
        print()


if __name__ == "__main__":
    base = Path(sys.argv[1])
    run1(base)
    run2(base)
    run3(base)
