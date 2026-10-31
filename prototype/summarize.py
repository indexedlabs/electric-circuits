#!/usr/bin/env python3
import json, pathlib, statistics, sys

root = pathlib.Path(sys.argv[1])
rows = []
for path in sorted(root.glob("*/samples.jsonl")):
    vals = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    if not vals: continue
    rss = [x.get("engine_memory", {}).get("process", {}).get("rss_bytes") for x in vals]
    rss = [x for x in rss if isinstance(x, int)]
    lat = [x.get("latency_ms") for x in vals if isinstance(x.get("latency_ms"), (int,float))]
    lane_bytes = [sum(v.get("bytes", 0) for v in x.get("engine_metrics", {}).get("group_lane_backlog", {}).values()) for x in vals]
    lane_batches = [sum(v.get("batches", 0) for v in x.get("engine_metrics", {}).get("group_lane_backlog", {}).values()) for x in vals]
    receipt_depth = [x.get("engine_metrics", {}).get("gauges", {}).get("receipt_queue_depth", 0) for x in vals]
    def offset(value):
        try: return int(value["offset"].split("_")[-1], 16)
        except (KeyError, TypeError, ValueError, AttributeError): return None
    processed = [offset(x.get("engine_metrics", {}).get("sequencer", {}).get("processed")) for x in vals]
    checkpoint = [offset(x.get("engine_metrics", {}).get("sequencer", {}).get("checkpoint")) for x in vals]
    lag = [p - c for p, c in zip(processed, checkpoint) if p is not None and c is not None]
    workload_path = root / f"{path.parent.name.split('-2026')[0]}-workload.json"
    workload = json.loads(workload_path.read_text()) if workload_path.exists() else {}
    rows.append({"scenario": path.parent.name, "samples": len(vals), "rss_peak_bytes": max(rss, default=None),
                 "lane_backlog_peak_bytes": max(lane_bytes, default=0), "lane_backlog_peak_batches": max(lane_batches, default=0),
                 "receipt_queue_peak_depth": max(receipt_depth, default=0), "processed_checkpoint_lag_peak_bytes": max(lag, default=None),
                 **workload, "latency_p50_ms": statistics.median(lat) if lat else None,
                 "latency_p99_ms": sorted(lat)[max(0, int(len(lat)*.99)-1)] if lat else None,
                 "first": vals[0], "last": vals[-1]})
(root / "summary.json").write_text(json.dumps(rows, indent=2, sort_keys=True) + "\n")
print(json.dumps(rows, indent=2, sort_keys=True))
