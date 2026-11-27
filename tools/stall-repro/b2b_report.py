"""The table for results/simple-keys/B2B-SERVED-PREDICATES.md, from the retained `paired-b2b-*` folders.

    python3 tools/stall-repro/b2b_report.py tools/stall-repro/results/simple-keys
"""
from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path

from plain_words_report import load

GROWS_S, GROWS_RATIO = 0.08, 1.3
FORMS = (("group", "group member, as served today"), ("feed", "feed or subset query, single key applied"))


def filters():
    return json.loads((Path(__file__).parent / "b2b_filters.json").read_text())["tables"]


def placement(root, binary, form):
    found = sorted(root.glob(f"paired-b2b-counts-*-S-main3-b2b-{form}-all-1-{binary}-1cpu"))
    if not found or not (found[-1] / "b2-placement-result.json").exists():
        return {}
    return {entry["table"]: entry for entry in load(found[-1] / "b2-placement-result.json")["placements"]}


def counted(entry, has_scan):
    if entry is None:
        return "not observed", None
    if entry["status"] != 200:
        return f"refused ({entry['status']}): {entry['refused'][:160]}", "refused"
    before, after = entry["before"], entry["after"]
    engine = {key: after["engine"][key] - before["engine"][key] for key in after["engine"]}
    families = sum(family["shapes"] for family in after["table"]["families"]) - sum(family["shapes"] for family in before["table"]["families"])
    standalone = after["table"]["standalone"] - before["table"]["standalone"]
    if engine["subquery_shapes"] == 1:
        return f"subquery shape +1 ({engine['subquery_nodes']} nodes), standalone +{standalone}: not in the standalone index", "subquery"
    if families == 1 and standalone == 0:
        return f"equality family +1 (key column {after['table']['families'][0]['key_cols'][0]}), standalone +0", "family"
    if standalone == 1 and has_scan:
        scan = after["table"].get("scan", 0) - before["table"].get("scan", 0)
        return f"standalone +1, scan +{scan}: " + ("on the scan list" if scan else "filed, not on the scan list"), "scan" if scan else "filed"
    if standalone == 1:
        return "standalone +1", "standalone"
    return f"families {families:+d}, standalone {standalone:+d}, subquery shapes {engine['subquery_shapes']:+d}", None


def cost(root, form, readers, table):
    found = sorted(root.glob(f"paired-b2b-cost-*-S-main3-b2b-{form}-{table}-{readers}-prod9bc-1cpu"))
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
    counts = {(binary, form): placement(root, binary, form) for binary in ("main46d", "prod9bc") for form, _ in FORMS}
    print("| Table | Form | Expected placement | Observed on main (counts) | Observed on 9bc1563 (cost, 40 → 1,000 readers) | Match |")
    print("| --- | --- | --- | --- | --- | --- |")
    matches, misses = 0, []
    for entry in filters():
        table = entry["table"]
        for form, label in FORMS:
            expected = entry[form]["expected"]
            words, kind = counted(counts[("main46d", form)].get(table), True)
            prod_words, _ = counted(counts[("prod9bc", form)].get(table), False)
            small, large = cost(root, form, 40, table), cost(root, form, 1000, table)
            if small and large:
                low, high = statistics.median(small), statistics.median(large)
                grows = high - low >= GROWS_S and high >= low * GROWS_RATIO
                cost_words = f"{low:.2f} s → {high:.2f} s: " + ("grows" if grows else "flat") + f"; its count: {prod_words}"
            else:
                grows, cost_words = None, f"not observed; its count: {prod_words}"
            if expected.startswith("scan list"):
                ok = kind == "scan" and grows is True
            elif "shared value" in expected:
                ok = kind == "filed" and grows is True
            elif expected.startswith("range"):
                ok = kind == "filed"
            else:
                ok = kind in ("filed", "family") and grows is False
            matches += ok
            if not ok:
                misses.append(f"{table}, {form}")
            print(f"| `{table}` | {label} | {expected} | {words} | {cost_words} | {'yes' if ok else 'NO'} |")
    print(f"\n{matches} of {2 * len(filters())} rows match" + ("." if not misses else "; not matching: " + "; ".join(misses) + "."))


if __name__ == "__main__":
    main(Path(sys.argv[1]))
