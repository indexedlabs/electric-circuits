from __future__ import annotations

import argparse
import gzip
import json
from collections import Counter
from pathlib import Path


def responsibility(stack):
    if "electric_circuits_engine::pg::" in stack or "tokio_postgres" in stack:
        return "postgres_client"
    if any(name in stack for name in ("append_batch", "append_commit", "append_reliable", "append_retrying")):
        return "stream_append"
    if "on_table_delta" in stack:
        return "row_membership_on_table_delta"
    if "propagate_one" in stack or "propagate_flips" in stack:
        return "flip_walk_including_requery_evaluation"
    if "wait_for_source_effects" in stack:
        return "marker_wait_cpu"
    if any(name in stack for name in ("mem_totals", "mem::spawn_sampler", "Engine::mem_cardinalities")):
        return "memory_sampler"
    if any(name in stack for name in ("tokio::sync::mutex", "parking_lot", "futex")):
        return "other_lock_cpu_not_registry_wait_wall"
    return "other_or_unresolved"


def report(path):
    categories = Counter()
    inclusive = Counter()
    leaves = Counter()
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt") as source:
        for line in source:
            stack, count = line.rstrip().rsplit(" ", 1)
            count = int(count)
            categories[responsibility(stack)] += count
            leaves[stack.split(";")[-1]] += count
            for token in ("on_table_delta", "propagate_one", "wait_for_source_effects", "MembershipCircuit::contains", "subq_circuit::seek", "mem_totals", "parking_lot", "tokio::sync::mutex", "futex"):
                if token in stack:
                    inclusive[token] += count
    total = sum(categories.values())
    return {
        "method": "exclusive owning operation, with recognized PG/append frames assigned first; inclusive inner functions overlap; async ancestry may be absent; no off-CPU claims",
        "samples": total,
        "categories": {name: {"samples": count, "percent": count * 100 / total} for name, count in categories.items()},
        "inclusive_overlapping": {name: {"samples": count, "percent": count * 100 / total} for name, count in inclusive.items()},
        "top_leaf_frames": leaves.most_common(25),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("folded", type=Path)
    arguments = parser.parse_args()
    print(json.dumps(report(arguments.folded), indent=2))
