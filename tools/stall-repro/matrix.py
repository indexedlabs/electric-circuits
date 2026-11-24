"""Render the bridge matrix: one row per variant and workload, compared with the unprofiled A control.

A row counts as "served" when the other account's drain, subset and live-change probes each
succeed in at least 90% of attempts during the workload; a 2 s timeout is a failure.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

WORKLOADS = ("import", "large-delete", "small-delete")
SERVED_RATE = 0.9


def rate(probe):
    return probe["successes"] / probe["attempts"] if probe["attempts"] else None


def cell(probe):
    value = rate(probe)
    p95 = probe.get("successful_p95_s")
    return "—" if value is None else f"{value * 100:.0f}% ({probe['successes']}/{probe['attempts']}); p95 {'—' if p95 is None else f'{p95:.2f}'}"


def runner_cpu(folder):
    machine = folder / "machine.txt"
    if not machine.exists():
        return "?"
    for line in machine.read_text().splitlines():
        if line.startswith("Model name:"):
            model = line.split(":", 1)[1].strip()
            match = re.search(r"(EPYC \w+)|Xeon\(R\) (?:Platinum )?(\w+(?:-\w+)?)", model, re.IGNORECASE)
            return ("" if not match else match.group(1) or f"Xeon {match.group(2)}") or model
    return "?"


def load(folder):
    rows = {}
    for workload in WORKLOADS:
        path = folder / f"{workload}-result.json"
        if path.exists():
            rows[workload] = json.loads(path.read_text())
    return rows


def render(folders, control):
    baseline = load(control)
    print("| Variant | Runner CPU | Workload | First delivery → empty s | × A control | CPU % | Backfills | Drain | Subset | Live change | Served? |")
    print("| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- | --- | --- |")
    for folder in folders:
        manifest = json.loads((folder / "manifest.json").read_text())
        cpu = runner_cpu(folder)
        label = manifest["variant"] + ("" if manifest["cpu_quota"] == 1 else f", {manifest['cpu_quota']:g} CPU")
        for workload, result in load(folder).items():
            if not result.get("completed"):
                print(f"| {label} | {cpu} | {workload} | did not complete: {result.get('error', '?')[:80]} | | | | | | | |")
                continue
            elapsed = result.get("first_read_to_empty_s") or result["elapsed_s"]
            control_elapsed = baseline.get(workload, {}).get("first_read_to_empty_s")
            ratio = f"{elapsed / control_elapsed:.2f}" if control_elapsed else "—"
            probes = result["probe"]
            served = all((rate(probes[kind]) or 0) >= SERVED_RATE for kind in ("drain", "subset", "live"))
            print(f"| {label} | {cpu} | {workload} | {elapsed:,.1f} | {ratio} | {result['engine_cpu_percent']:.0f} | {result['backfills']:,} | {cell(probes['drain'])} | {cell(probes['subset'])} | {cell(probes['live'])} | {'**yes**' if served else 'no'} |")


def holds(folders):
    print("| Variant | Workload | Lock interval | Calls | Wall s | ≤1 ms | ≤10 ms | ≤100 ms | ≤1 s | ≤5 s | >5 s |")
    print("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for folder in folders:
        manifest = json.loads((folder / "manifest.json").read_text())
        for workload, result in load(folder).items():
            delta = result.get("wall_profile", {}).get("delta", {})
            for category in ("sequencer_registry_hold", "flip_registry_hold", "sampler_registry_hold", "marker_wait"):
                values = delta.get(category)
                if not values or "le_1ms" not in values:
                    continue
                buckets = [values[label] for label in ("le_1ms", "le_10ms", "le_100ms", "le_1s", "le_5s", "gt_5s")]
                print(f"| {manifest['variant']} | {workload} | {category} | {values['calls']:,} | {values['wall_seconds']:.1f} | " + " | ".join(f"{count:,}" for count in buckets) + " |")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--control", type=Path, required=True)
    parser.add_argument("--holds", action="store_true")
    parser.add_argument("folders", nargs="+", type=Path)
    arguments = parser.parse_args()
    (holds if arguments.holds else lambda folders: render(folders, arguments.control))(arguments.folders)
