"""Tables for results/simple-keys/W6-LIST-KEYS.md, from the retained `paired-w6-*` folders.

    python3 tools/stall-repro/w6_report.py tools/stall-repro/results/simple-keys
"""
from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path

from plain_words_report import load, runner

CELLS = (("on", "ON: `ELECTRIC_CIRCUITS_LIST_KEYS=1`"), ("off", "OFF: the setting unset (control)"))
COUNTS = (100, 1000, 5000)
FIRST = ("e1-commit", "e1-commit-r2", "e1-commit-r3")


def folder_of(root, count, cell):
    found = sorted(root.glob(f"paired-w6-{count}-*-S-main3-w6-{count}-{cell}-main46d-1cpu"))
    return found[-1] if found else None


def commits(folder, names):
    results = [load(folder / f"{name}-result.json") for name in names if (folder / f"{name}-result.json").exists()]
    return [result for result in results if result.get("completed")]


def times(results):
    values = [result["first_read_to_empty_s"] for result in results]
    if not values:
        return "—"
    return f"{statistics.median(values):.2f} ({min(values):.2f}–{max(values):.2f})" if len(values) > 1 else f"{values[0]:.2f}"


def bystander(results, kind):
    attempts = sum(result["probe"][kind]["attempts"] for result in results)
    successes = sum(result["probe"][kind]["successes"] for result in results)
    worst = [result["probe"][kind]["successful_p95_s"] for result in results if result["probe"][kind]["successful_p95_s"] is not None]
    longest = [result["probe"][kind]["successful_max_s"] for result in results if result["probe"][kind]["successful_max_s"] is not None]
    if not attempts:
        return "none fell in a commit"
    return f"{successes}/{attempts}" + (f"; p95 {max(worst):.2f} s, max {max(longest):.2f} s" if worst else "")


def rss(folder, names):
    peak = 0
    for name in names:
        path = folder / f"{name}-cpu.jsonl"
        if path.exists():
            peak = max([peak, *(json.loads(line)["rss_bytes"] for line in path.read_text().splitlines())])
    return f"{peak / 2**20:,.0f} MiB" if peak else "—"


def placement(step):
    if step is None:
        return ("—", "—", "—")
    engine = step["engine_table"]
    copy = step["harness_copy_of_the_rule"]
    filed = f"{engine['standalone'] - engine.get('scan', 0):,} filed, {engine.get('scan', 0)} on the scan list (of {engine['standalone']:,})" if "standalone" in engine else str(engine)
    keys = ", ".join(f"`{columns}` {count:,}" for columns, count in sorted(copy["by_columns"].items(), key=lambda item: -item[1]))
    largest = copy["largest_buckets"][0]
    return (filed, keys, f"`{largest['column']} = {largest['value'][:8]}…`: {largest['shapes']:,} shapes" if len(largest["value"]) > 12 else f"`{largest['column']} = '{largest['value']}'`: {largest['shapes']:,} shapes")


def step_of(folder, name):
    path = folder / f"{name}-result.json"
    return load(path) if path.exists() else None


def cell(root, key, label):
    print(f"### {label}\n")
    print("| Keyed `graph_edge` shapes | Runner | Sequencer time, 3 commits: median (range) s | Per row | Per row per shape | Engine CPU % | Peak RSS | Bystander receipts (drains ok/attempts) | Bystander live changes | Engine: shapes filed under a key | Harness's copy of the rule: filed under | Largest key bucket (harness's copy) | Final check: shapes whose rows differ from Postgres |")
    print("| ---: | --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | --- | --- | --- |")
    for count in COUNTS:
        folder = folder_of(root, count, key)
        if folder is None:
            continue
        results = commits(folder, FIRST)
        if not results:
            print(f"| {count:,} | {runner(folder)} | no commit completed | | | | | | | | | | |")
            continue
        median = statistics.median(result["first_read_to_empty_s"] for result in results)
        filed, keys, largest = placement(step_of(folder, "w6-placement"))
        check = step_of(folder, "w6-check")
        checked = f"{check['mismatched_shapes']} of {check['shapes_checked']:,} ({check['shapes_with_rows']:,} hold rows, {check['rows_expected']:,} rows)" if check else "not reached"
        print(f"| {count:,} | {runner(folder)} | {times(results)} | {median:.2f} ms | {median / 1000 / count * 1e6:.2f} µs | {statistics.median(result['engine_cpu_percent'] for result in results):.0f} | {rss(folder, FIRST)} | {bystander(results, 'drain')} | {bystander(results, 'live')} | {filed} | {keys} | {largest} | {checked} |")
    folder = folder_of(root, 5000, key)
    if folder is not None and step_of(folder, "w6-restart"):
        restart, access = step_of(folder, "w6-restart"), step_of(folder, "w6-access")
        before, after = step_of(folder, "w6-placement"), step_of(folder, "w6-placement-restarted")
        print(f"\nThe 5,000 run's restart and access changes ({folder.name}):\n")
        print("| Step | What happened | 1,000-edge commit after it (s) | Bystander receipts in that commit | Engine: shapes filed under a key | Harness's copy: filed under |")
        print("| --- | --- | ---: | --- | --- | --- |")
        first = commits(folder, FIRST)
        print(f"| Before | {before['cardinalities']['shapes']:,} shapes | {times(first)} | {bystander(first, 'drain')} | {placement(before)[0]} | {placement(before)[1]} |")
        fourth = commits(folder, ("e1-commit-r4",))
        print(f"| Engine restart | ready again in {restart['restart_to_ready_s']:.1f} s; {restart['cardinalities']['shapes']:,} shapes back from the catalog | {times(fourth)} | {bystander(fourth, 'drain') if fourth else '—'} | {placement(after)[0]} | {placement(after)[1]} |")
        if access:
            changed = step_of(folder, "w6-placement-access")
            fifth = commits(folder, ("e1-commit-r5",))
            gained = sum(entry["change"] == "gain" for entry in access["accounts"])
            print(f"| Access changes | {gained} accounts gain a partner Space and {len(access['accounts']) - gained} lose theirs (new feeds made, old ones released: {access['released']}); {access['edges_moved']} edges change Space | {times(fifth)} | {bystander(fifth, 'drain') if fifth else '—'} | {placement(changed)[0]} | {placement(changed)[1]} |")
        check = step_of(folder, "w6-check")
        if check:
            print(f"\nFinal check after both: {check['mismatched_shapes']} of {check['shapes_checked']:,} shapes differ from Postgres ({check['shapes_with_rows']:,} hold rows, {check['rows_expected']:,} rows expected, {check['envelopes_read']:,} changes replayed).")
            for mismatch in check.get("mismatches", [])[:10]:
                print(f"- mismatch: shape `{mismatch['shape']}` (account {mismatch['account']}, {mismatch['kind']}): expected {mismatch['expected']}, holds {mismatch['held']}; missing {mismatch['missing']}, extra {mismatch['extra']}")
    print()


if __name__ == "__main__":
    base = Path(sys.argv[1])
    for cell_key, cell_label in CELLS:
        cell(base, cell_key, cell_label)
