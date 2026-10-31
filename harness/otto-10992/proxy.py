#!/usr/bin/env python3
"""Small real HTTP fault proxy for Durable Streams.

It forwards every request to DS and can delay or hold only paths containing
FAULT_MATCH. OUTAGE=1 cuts all traffic. This intentionally sits on the engine
network path; no engine or DS call is mocked.
"""
import http.client
import json
import os
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

TARGET_HOST = os.getenv("TARGET_HOST", "ds")
TARGET_PORT = int(os.getenv("TARGET_PORT", "8791"))
# The fault is switched in place through /__fault (never by recreating the proxy, which would
# drop every unrelated engine connection at the start and end of a fault).
FAULT = {
    "match": os.getenv("FAULT_MATCH", ""),
    "delay_ms": int(os.getenv("DELAY_MS", "0")),
    "hang": os.getenv("HANG", "0") == "1",
    "outage": os.getenv("OUTAGE", "0") == "1",
    "generation": 0,
}
STATS = {"targeted": 0, "held": 0, "released": 0, "forwarded": 0, "upstream_errors": 0}
STATS_LOCK = threading.Lock()


def readiness_body():
    return json.dumps(
        {
            "contract_version": "durable-streams-store-ready-v1",
            "status": "ready",
            "artifact_digest": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "manifest": {
                "store_id": os.getenv("DS_STORE_ID"),
                "store_generation": os.getenv("DS_STORE_GENERATION"),
                "protocol_version": int(os.getenv("DS_PROTOCOL_VERSION", "1")),
                "layout_version": int(os.getenv("DS_LAYOUT_VERSION", "1")),
                "durability_mode": os.getenv("DS_DURABILITY_MODE", "wal"),
                "wal_shard_count": int(os.getenv("DS_WAL_SHARDS", "2")),
                "stream_lane_count": int(os.getenv("DS_STREAM_LANES", "1")),
                "filesystem_uuid": os.getenv("DS_FILESYSTEM_UUID"),
                "creation_time": "2026-08-27T19:00:00Z",
            },
            "recovery": {
                "completed": True,
                "wal_shards": [
                    {"shard": 0, "durable_lsn": 0, "checkpoint_lsn": 0},
                    {"shard": 1, "durable_lsn": 0, "checkpoint_lsn": 0},
                ],
            },
            "reserve": {"free_bytes": 0, "free_inodes": 0, "minimum_free_bytes": 0, "minimum_free_inodes": 0, "satisfied": True},
        },
        separators=(",", ":"),
    ).encode()


class Proxy(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_HEAD(self):
        self.forward()

    def do_GET(self):
        self.forward()

    def do_POST(self):
        self.forward()

    def do_PUT(self):
        self.forward()

    def do_DELETE(self):
        self.forward()

    def forward(self):
        if self.path.startswith("/__fault?") or self.path == "/__fault_clear":
            query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            with STATS_LOCK:
                FAULT.update({
                    "match": query.get("match", [""])[0],
                    "delay_ms": int(query.get("delay_ms", ["0"])[0]),
                    "hang": query.get("hang", ["0"])[0] == "1",
                    "outage": query.get("outage", ["0"])[0] == "1",
                    "generation": FAULT["generation"] + 1,
                })
                if query:
                    for key in ("targeted", "held", "released"):
                        STATS[key] = 0
            self.path = "/__fault_stats"
        if self.path == "/__fault_stats":
            with STATS_LOCK:
                payload = json.dumps({**STATS, "fault_match": FAULT["match"], "hang": FAULT["hang"],
                                      "outage": FAULT["outage"], "delay_ms": FAULT["delay_ms"]}).encode()
            self.send_response(200, "OK")
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        if self.path == "/_admin/ready":
            payload = readiness_body()
            self.send_response(200, "OK")
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        with STATS_LOCK:
            fault = dict(FAULT)
        targeted = bool(fault["match"]) and fault["match"] in self.path
        if targeted:
            with STATS_LOCK:
                STATS["targeted"] += 1
        if fault["outage"] or (targeted and fault["hang"]):
            with STATS_LOCK:
                STATS["held"] += 1
            # Keep the TCP request open so the engine experiences a real stalled append/read.
            # When the fault is cleared the held request is dropped unanswered (the engine sees a
            # closed connection and retries), never forwarded late as a duplicate.
            while True:
                time.sleep(0.2)
                with STATS_LOCK:
                    if FAULT["generation"] != fault["generation"]:
                        STATS["released"] += 1
                        break
            self.close_connection = True
            return
        if targeted and fault["delay_ms"]:
            time.sleep(fault["delay_ms"] / 1000)
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length else None
        headers = {k: v for k, v in self.headers.items() if k.lower() not in {"host", "connection"}}
        with STATS_LOCK:
            STATS["forwarded"] += 1
        # One upstream keep-alive connection per downstream connection. A fresh TCP connection
        # per request exhausted the proxy's ephemeral ports (EADDRNOTAVAIL) within a minute at
        # a few hundred requests per second and broke the no-fault controls.
        for attempt in range(2):
            conn = getattr(self, "upstream", None)
            if conn is None:
                conn = self.upstream = http.client.HTTPConnection(TARGET_HOST, TARGET_PORT, timeout=300)
            try:
                conn.request(self.command, self.path, body=body, headers=headers)
                response = conn.getresponse()
                payload = response.read()
                break
            except (OSError, http.client.HTTPException):
                conn.close()
                self.upstream = None
                with STATS_LOCK:
                    STATS["upstream_errors"] += 1
                if attempt:
                    raise
        if response.getheader("connection", "").lower() == "close":
            conn.close()
            self.upstream = None
        self.send_response(response.status, response.reason)
        for key, value in response.getheaders():
            if key.lower() not in {"connection", "transfer-encoding", "content-length"}:
                self.send_header(key, value)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(payload)

    def finish(self):
        super().finish()
        conn = getattr(self, "upstream", None)
        if conn is not None:
            conn.close()

    def log_message(self, fmt, *args):
        if os.getenv("PROXY_ACCESS_LOG") == "1":
            print(fmt % args, flush=True)


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 1024


Server(("0.0.0.0", 8792), Proxy).serve_forever()
