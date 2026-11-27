"""The table for results/simple-keys/B2-SINGLE-KEY-PLACEMENT.md, from the retained `paired-b2-*` folders.

    python3 tools/stall-repro/b2_report.py tools/stall-repro/results/simple-keys
"""
from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path

from plain_words_report import load, runner

# A commit counts as growing with readers when 1,000 readers add this much over 40, in seconds and in ratio.
GROWS_S, GROWS_RATIO = 0.08, 1.3


def filters():
    return json.loads((Path(__file__).parent / "b2_filters.json").read_text())["collections"]


def placement(root, binary, flag):
    found = sorted(root.glob(f"paired-b2-counts-*-S-main3-b2-{flag}-1-{binary}-1cpu"))
    if not found or not (found[-1] / "b2-placement-result.json").exists():
        return {}
    return {entry["table"]: entry for entry in load(found[-1] / "b2-placement-result.json")["placements"]}


def counted(entry, has_scan):
    """What one filter added to its table's counts, in words."""
    if entry is None:
        return "not observed", None
    before, after = entry["before"]["table"], entry["after"]["table"]
    families = sum(family["shapes"] for family in after["families"]) - sum(family["shapes"] for family in before["families"])
    standalone = after["standalone"] - before["standalone"]
    if families == 1 and standalone == 0:
        return "family +1 (an equality family: hash lookup by key)", "family"
    if standalone == 1 and has_scan:
        scan = after.get("scan", 0) - before.get("scan", 0)
        return (f"standalone +1, scan +{scan}: " + ("on the scan list" if scan else "filed under an equality or range, not on the scan list")), "scan" if scan else "filed"
    if standalone == 1:
        return "standalone +1 (this binary does not say filed or scan list)", "standalone"
    return f"families {families:+d}, standalone {standalone:+d}", None


def cost(root, flag, readers, table):
    found = sorted(root.glob(f"paired-b2-cost-{flag}-*-S-main3-b2-{flag}-{readers}-prod9bc-1cpu"))
    if not found:
        return None
    values = []
    for suffix in ("", "-r2", "-r3"):
        path = found[-1] / f"b2-commit-{table}{suffix}-result.json"
        if path.exists():
            result = load(path)
            if result.get("completed") and result.get("first_read_to_empty_s") is not None:
                values.append(result["first_read_to_empty_s"])
    return values or None


def main(root):
    main_counts = {flag: placement(root, "main46d", flag) for flag in ("off", "on")}
    prod_counts = {flag: placement(root, "prod9bc", flag) for flag in ("off", "on")}
    print("| Collection | Single key | Expected placement | Observed on main (counts) | Observed on 9bc1563 (counts) | Observed on 9bc1563 (cost, 40 → 1,000 readers; median of 3 commits, s) | Match |")
    print("| --- | --- | --- | --- | --- | --- | --- |")
    matches, misses = 0, []
    for entry in filters():
        table = entry["table"]
        for flag in ("off", "on"):
            expected = entry[flag]["expected"]
            words, kind = counted(main_counts[flag].get(table), True)
            prod_words, prod_kind = counted(prod_counts[flag].get(table), False)
            small, large = cost(root, flag, 40, table), cost(root, flag, 1000, table)
            if small and large:
                low, high = statistics.median(small), statistics.median(large)
                grows = high - low >= GROWS_S and high >= low * GROWS_RATIO
                cost_words = f"{low:.2f} → {high:.2f} ({min(large):.2f}–{max(large):.2f}): " + ("grows with readers" if grows else "flat")
            else:
                grows, cost_words = None, "not observed"
            if expected.startswith("scan list"):
                want_kind, want_grows, reading = {"scan"}, True, "on the scan list"
            elif "shared value" in expected:
                want_kind, want_grows, reading = {"filed"}, True, "filed under a value every reader shares"
            else:
                want_kind, want_grows, reading = {"filed", "family"}, False, "filed under the reader's own key"
            ok = kind in want_kind and grows is want_grows
            matches += ok
            if not ok:
                misses.append(f"{table}, single key {flag}")
            print(f"| `{table}` | {flag} | {expected} | {words} | {prod_words} | {cost_words} | {'yes: ' + reading if ok else 'NO'} |")
    print(f"\n{matches} of {2 * len(filters())} rows match" + ("." if not misses else "; not matching: " + "; ".join(misses) + "."))


if __name__ == "__main__":
    main(Path(sys.argv[1]))
