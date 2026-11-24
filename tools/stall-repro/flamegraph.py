from __future__ import annotations

import argparse
import hashlib
import html
import json
from collections import Counter
from pathlib import Path


def fold(text):
    stacks = Counter()
    frames = []
    for line in text.splitlines() + [""]:
        if not line.strip():
            if frames:
                stacks[";".join(reversed(frames))] += 1
            frames = []
        elif line.startswith(("\t", " ")):
            fields = line.strip().split(maxsplit=1)
            if len(fields) == 2 and all(character in "0123456789abcdefABCDEF" for character in fields[0]):
                symbol = fields[1].rsplit(" (", 1)[0].split("+0x", 1)[0]
                frames.append(symbol.replace(";", ":"))
    return stacks


def render(stacks, title):
    root = {"name": "all CPU samples", "count": 0, "children": {}}
    depth = 1
    for stack, count in stacks.items():
        node = root
        node["count"] += count
        names = stack.split(";")
        depth = max(depth, len(names) + 1)
        for name in names:
            node = node["children"].setdefault(name, {"name": name, "count": 0, "children": {}})
            node["count"] += count
    height = depth * 20 + 65
    output = [f'<svg xmlns="http://www.w3.org/2000/svg" width="1400" height="{height}" viewBox="0 0 1400 {height}">', '<rect width="100%" height="100%" fill="#fafafa"/>', f'<text x="10" y="24" font-family="sans-serif" font-size="16">{html.escape(title)} — {root["count"]} samples</text>']
    total = root["count"] or 1
    def draw(node, left, level):
        width = node["count"] / total * 1380
        top = height - 25 - level * 20
        color = hashlib.sha256(node["name"].encode()).digest()
        fill = f"rgb(240,{90 + color[0] % 130},{35 + color[1] % 100})"
        label = html.escape(node["name"])
        output.append(f'<g><title>{label}: {node["count"]} samples ({node["count"] / total:.2%})</title><rect x="{left:.3f}" y="{top}" width="{max(0, width - 0.3):.3f}" height="19" fill="{fill}"/>')
        if width > 24:
            visible = html.escape(node["name"][:max(1, int(width / 7) - 1)])
            output.append(f'<text x="{left + 3:.3f}" y="{top + 14}" font-family="monospace" font-size="11">{visible}</text>')
        output.append('</g>')
        for child in sorted(node["children"].values(), key=lambda item: item["name"]):
            draw(child, left, level + 1)
            left += child["count"] / total * 1380
    draw(root, 10, 0)
    output.append('</svg>')
    return "\n".join(output)


def attribute(stacks):
    categories = Counter()
    for stack, count in stacks.items():
        category = "everything_else_or_unresolved"
        for symbol in reversed(stack.split(";")):
            if "on_table_delta" in symbol or "membership" in symbol or "dbsp::" in symbol:
                category = "row_and_membership_evaluation"
                break
            if "tokio::sync::mutex" in symbol or "parking_lot" in symbol or "futex" in symbol:
                category = "lock_cpu_not_lock_wall_time"
                break
            if "propagate_one" in symbol or "propagate_flips" in symbol:
                category = "flip_propagation"
                break
            if "electric_circuits_engine::pg::" in symbol or "tokio_postgres" in symbol:
                category = "postgres_client"
                break
            if "append_batch" in symbol or "append_commit" in symbol:
                category = "stream_append"
                break
            if "wait_for_source_effects" in symbol:
                category = "marker_wait_cpu_not_wall_time"
                break
        categories[category] += count
    return {key: {"samples": count, "percent": count / sum(stacks.values()) * 100} for key, count in categories.items()}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("script", type=Path)
    arguments = parser.parse_args()
    stacks = fold(arguments.script.read_text())
    if not stacks:
        raise RuntimeError("No call stacks found; no flamegraph may be claimed")
    arguments.script.with_suffix(".folded").write_text("\n".join(f"{stack} {count}" for stack, count in sorted(stacks.items())) + "\n")
    arguments.script.with_suffix(".svg").write_text(render(stacks, arguments.script.stem))
    arguments.script.with_suffix(".attribution.json").write_text(json.dumps({"method": "deepest recognized CPU frame; async ancestry may be missing; lock holding overlaps evaluation and off-CPU waits are not measured", "categories": attribute(stacks)}, indent=2))
