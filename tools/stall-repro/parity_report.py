"""Render parity.py's JSON as Markdown: counts per table, reasons, example ids and the planted rows."""

import json
import sys
from pathlib import Path


def examples(ids):
    return "<br>".join(f"`{identity}`" for identity in ids) or "—"


def reasons(entry):
    return "; ".join(f"{why}: {count}" for why, count in sorted(entry.items(), key=lambda item: -item[1])) or "—"


folder = Path(sys.argv[1])
base, extended = (json.loads((folder / f"{name}.json").read_text()) for name in ("base", "fixtures"))
print("# Variant S against today's gateway predicates\n")
print("(a) is today's gateway predicate with no client scope; (b) is the union of the account's S feeds. Counts are (account, row) pairs over 20 accounts.\n")
for title, result in (("Base: the seeded database (with shares) plus the first calendar import", base), ("Base plus the planted coverage and drift rows", extended)):
    print(f"## {title}\n\nSnapshot `{result['snapshot']}` at start and `{result['snapshot_at_end']}` at end, {result['postgres'].split(' on ')[0]}, taken {result['taken_at']}.\n")
    print("| Table | Admitted (a) | Admitted (b) | (a) only | (b) only | Why (a) only | Why (b) only | Examples (a) only | Examples (b) only |\n| --- | ---: | ---: | ---: | ---: | --- | --- | --- | --- |")
    for table, row in result["tables"].items():
        print(f"| `{table}` | {row['admitted_a']:,} | {row['admitted_b']:,} | {row['a_only']} | {row['b_only']} | {reasons(row['reasons_a_only'])} | {reasons(row['reasons_b_only'])} | {examples(row['examples_a_only'])} | {examples(row['examples_b_only'])} |")
    print("\n`graph_edge` under the readings S leaves open: " + "; ".join(f"{name}: (a) only {counts['a_only']}, (b) only {counts['b_only']}" for name, counts in result["graph_edge_alternates"].items()) + ".\n")
    print("Drift check (no accounts; rows against their parents): " + ("; ".join(f"`{table}` {rows['rows']} ({reasons(rows['reasons'])})" for table, rows in result["drift_check"].items()) or "nothing found") + ".\n")
report = extended["fixtures"]
print(f"## Planted rows\n\nDrift rows found by the drift check: {report['drift_rows_found']} of {report['drift_rows']}. Labels whose differences were not exactly the ones predicted before the run: {len(report['surprises'])}.\n")
print("| Planted row | Tables | (a) only, accounts | (b) only, accounts | Reason | Examples |\n| --- | --- | --- | --- | --- | --- |")
for label, group in report["labels"].items():
    print(f"| {label} | {', '.join(group['tables'])} | {group['a_only']}: {group['accounts_a'] or '—'} | {group['b_only']}: {group['accounts_b'] or '—'} | {'; '.join(group['reasons'])} | {examples(group['examples'])} |")
