#!/usr/bin/env python3
"""Tail Durable Streams the way a client does and record what was delivered.

usage: delivery_probe.py OUT_DIR STOP_FILE label=stream/path [label=stream/path ...]

Each tick reads every stream from its last offset (a catch-up read, following pages until
`stream-up-to-date`), counts envelopes and bytes, and looks for marker strings listed in
OUT_DIR/markers.tsv (`name<TAB>commit_ms<TAB>marker`). It reads DS directly, not through
the fault proxy: the faults are on the engine's write path, and a client's read path is
not part of the fault.

Writes OUT_DIR/delivery.jsonl (one cumulative row per tick), OUT_DIR/markers-seen.json and
OUT_DIR/delivery-debug.jsonl (the first responses per stream and every error, rate-limited).
"""
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

out_dir, stop_file, specs = sys.argv[1], sys.argv[2], sys.argv[3:]
namespace = os.environ.get("ELECTRIC_CIRCUITS_DS_NAMESPACE", "otto-10992")
generation = os.environ["ELECTRIC_CIRCUITS_DS_STORE_GENERATION"]
query = os.environ.get("ELECTRIC_CIRCUITS_QUERY_GENERATION", "otto-10992")
base = f"http://127.0.0.1:8791/circuits/v1/{namespace}/stores/{generation}/queries/{query}/"
tick_s = float(os.environ.get("PROBE_TICK_S", "0.5"))

streams = {}
for spec in specs:
    label, path = spec.split("=", 1)
    if path:
        streams[label] = {"path": path, "offset": None, "envelopes": 0, "bytes": 0, "reads": 0,
                          "errors": 0, "last_status": None, "last_growth_ms": None, "debug": 0}

debug = open(os.path.join(out_dir, "delivery-debug.jsonl"), "a")
rows = open(os.path.join(out_dir, "delivery.jsonl"), "a")
seen = {}


def now_ms():
    return int(time.time() * 1000)


def log_debug(label, kind, **fields):
    s = streams[label]
    if s["debug"] >= 40:
        return
    s["debug"] += 1
    debug.write(json.dumps({"ts_ms": now_ms(), "label": label, "kind": kind, **fields}) + "\n")
    debug.flush()


def request(url, method="GET"):
    req = urllib.request.Request(url, method=method)
    with urllib.request.urlopen(req, timeout=5) as response:
        return response.status, dict(response.headers.items()), (response.read() if method == "GET" else b"")


def count_envelopes(body):
    if not body:
        return 0
    try:
        payload = json.loads(body)
        return len(payload) if isinstance(payload, list) else 1
    except json.JSONDecodeError:
        return sum(1 for line in body.splitlines() if line.strip())


def markers():
    try:
        with open(os.path.join(out_dir, "markers.tsv")) as f:
            return [line.rstrip("\n").split("\t") for line in f if line.count("\t") == 2]
    except OSError:
        return []


def start_offset(label):
    s = streams[label]
    try:
        status, headers, _ = request(base + s["path"], "HEAD")
        s["offset"] = headers.get("stream-next-offset") or headers.get("Stream-Next-Offset") or "-1"
        log_debug(label, "head", status=status, headers=headers)
    except (OSError, urllib.error.URLError) as exc:
        s["errors"] += 1
        log_debug(label, "head-error", error=repr(exc))


def read(label, known_markers):
    s = streams[label]
    if s["offset"] is None:
        start_offset(label)
        if s["offset"] is None:
            return
    for _ in range(50):
        # A catch-up read is one without `live`; DS 0.1.5 answers `live=false` with 400.
        url = base + s["path"] + "?offset=" + urllib.parse.quote(s["offset"], safe="")
        try:
            status, headers, body = request(url)
        except urllib.error.HTTPError as exc:
            s["errors"] += 1
            s["last_status"] = exc.code
            log_debug(label, "http-error", status=exc.code, body=exc.read()[:500].decode("utf-8", "replace"))
            return
        except (OSError, urllib.error.URLError) as exc:
            s["errors"] += 1
            log_debug(label, "error", error=repr(exc))
            return
        s["reads"] += 1
        s["last_status"] = status
        lower = {k.lower(): v for k, v in headers.items()}
        next_offset = lower.get("stream-next-offset") or s["offset"]
        if s["reads"] <= 3 or (body and s["envelopes"] == 0):
            log_debug(label, "read", status=status, headers=headers, offset=s["offset"], body_bytes=len(body),
                      body_prefix=body[:400].decode("utf-8", "replace"))
        if body:
            s["envelopes"] += count_envelopes(body)
            s["bytes"] += len(body)
            s["last_growth_ms"] = now_ms()
            text = body.decode("utf-8", "replace")
            for name, commit_ms, marker in known_markers:
                key = f"{label}:{name}"
                if key not in seen and marker in text:
                    seen[key] = {"stream": label, "marker": name, "commit_ms": int(commit_ms),
                                 "seen_ms": now_ms(), "latency_ms": now_ms() - int(commit_ms)}
                    with open(os.path.join(out_dir, "markers-seen.json"), "w") as f:
                        json.dump(seen, f, indent=2, sort_keys=True)
        moved = next_offset != s["offset"]
        s["offset"] = next_offset
        if lower.get("stream-up-to-date", "").lower() == "true" or not moved:
            return


while not os.path.exists(stop_file):
    started = time.time()
    known = markers()
    for label in streams:
        read(label, known)
    rows.write(json.dumps({"ts_ms": now_ms(), "streams": {
        label: {k: s[k] for k in ("path", "offset", "envelopes", "bytes", "reads", "errors", "last_status", "last_growth_ms")}
        for label, s in streams.items()}}, separators=(",", ":")) + "\n")
    rows.flush()
    time.sleep(max(0.0, tick_s - (time.time() - started)))
