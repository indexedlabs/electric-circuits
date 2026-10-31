#!/usr/bin/env python3
"""Small real HTTP fault proxy for Durable Streams.

It forwards every request to DS and can delay or hold only paths containing
FAULT_MATCH. OUTAGE=1 cuts all traffic. This intentionally sits on the engine
network path; no engine or DS call is mocked.
"""
import http.client
import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

TARGET_HOST = os.getenv("TARGET_HOST", "ds")
TARGET_PORT = int(os.getenv("TARGET_PORT", "8791"))
MATCH = os.getenv("FAULT_MATCH", "")
DELAY_MS = int(os.getenv("DELAY_MS", "0"))
HANG = os.getenv("HANG", "0") == "1"
OUTAGE = os.getenv("OUTAGE", "0") == "1"


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
            "recovery": {"completed": True, "wal_shards": []},
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
        if self.path == "/_admin/ready":
            payload = readiness_body()
            self.send_response(200, "OK")
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        targeted = not MATCH or MATCH in self.path
        if OUTAGE or (targeted and HANG):
            # Keep the TCP request open so the engine experiences a real stalled append/read.
            while True:
                time.sleep(60)
        if targeted and DELAY_MS:
            time.sleep(DELAY_MS / 1000)
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length else None
        conn = http.client.HTTPConnection(TARGET_HOST, TARGET_PORT, timeout=300)
        headers = {k: v for k, v in self.headers.items() if k.lower() not in {"host", "connection"}}
        try:
            conn.request(self.command, self.path, body=body, headers=headers)
            response = conn.getresponse()
            payload = response.read()
            self.send_response(response.status, response.reason)
            for key, value in response.getheaders():
                if key.lower() not in {"connection", "transfer-encoding"}:
                    self.send_header(key, value)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        finally:
            conn.close()

    def log_message(self, fmt, *args):
        print(fmt % args, flush=True)


ThreadingHTTPServer(("0.0.0.0", 8792), Proxy).serve_forever()
