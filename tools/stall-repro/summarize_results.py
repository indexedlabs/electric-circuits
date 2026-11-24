from __future__ import annotations

import argparse
import json
from pathlib import Path


def number(value, digits=2):
    return "—" if value is None else f"{value:,.{digits}f}"


def probe_summary(probe):
    return f"{probe['successes']}/{probe['attempts']}; " + "/".join(number(probe.get(field), 3) for field in ("successful_p50_s", "successful_p95_s", "successful_max_s"))


def summarize(roots):
    print("| Run / workload | SQL start → empty s | First delivery → empty s | Changes/s (SQL clock) | CPU % (one core) | Backfills / min | Pinned | Drain ok/attempts; p50/p95/max s | Subset ok/attempts; p50/p95/max s | Live ok/attempts; p50/p95/max s | Harness commit |")
    print("| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | --- |")
    for root in roots:
        for manifest_path in sorted(root.rglob("manifest.json")):
            folder = manifest_path.parent
            manifest = json.loads(manifest_path.read_text())
            if "engine_sha256" not in manifest:
                continue
            for workload in ("import", "large-delete", "small-delete"):
                result_path = folder / f"{workload}-result.json"
                if not result_path.exists():
                    continue
                result = json.loads(result_path.read_text())
                values = [f"{folder.name} / {workload}", number(result["elapsed_s"]), number(result.get("first_read_to_empty_s")), number(result.get("changes_per_s")), number(result.get("engine_cpu_percent")), f"{result['backfills']:,} / {number(result['backfills_per_minute'])}", str(result["pinned_warnings"]), *[probe_summary(result["probe"][kind]) for kind in ("drain", "subset", "live")], f"`{manifest['harness_commit']}`"]
                print("| " + " | ".join(values) + " |")
            print(f"\nCommand for `{folder.name}`: `{manifest['command']}`. Engine SHA256 `{manifest['engine_sha256']}`.\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("roots", nargs="+", type=Path)
    summarize(parser.parse_args().roots)
