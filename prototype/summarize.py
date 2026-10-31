#!/usr/bin/env python3
import json, pathlib, statistics, sys

root = pathlib.Path(sys.argv[1])
rows = []
for path in sorted(root.glob("*/samples.jsonl")):
    vals = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    if not vals: continue
    rss = [x.get("rss_bytes") for x in vals if isinstance(x.get("rss_bytes"), int)]
    lat = [x.get("latency_ms") for x in vals if isinstance(x.get("latency_ms"), (int,float))]
    rows.append({"scenario": path.parent.name, "samples": len(vals), "rss_peak_bytes": max(rss, default=None), "latency_p50_ms": statistics.median(lat) if lat else None, "latency_p99_ms": sorted(lat)[max(0, int(len(lat)*.99)-1)] if lat else None, "first": vals[0], "last": vals[-1]})
(root / "summary.json").write_text(json.dumps(rows, indent=2, sort_keys=True) + "\n")
print(json.dumps(rows, indent=2, sort_keys=True))
