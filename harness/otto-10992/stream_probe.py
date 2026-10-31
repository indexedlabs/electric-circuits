#!/usr/bin/env python3
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

namespace = os.environ.get("ELECTRIC_CIRCUITS_DS_NAMESPACE", "otto-10992")
generation = os.environ["ELECTRIC_CIRCUITS_DS_STORE_GENERATION"]
query = os.environ.get("ELECTRIC_CIRCUITS_QUERY_GENERATION", "otto-10992")
path = sys.argv[1]
mode = sys.argv[2]
offset = sys.argv[3] if len(sys.argv) > 3 else "0000000000000000_0000000000000000"
url = f"http://127.0.0.1:8791/circuits/v1/{namespace}/stores/{generation}/queries/{query}/{path}"

try:
    target = url if mode == "head" else f"{url}?offset={urllib.parse.quote(offset, safe='')}&live=false"
    req = urllib.request.Request(target, method="HEAD" if mode == "head" else "GET")
    with urllib.request.urlopen(req, timeout=2) as response:
        next_offset = response.headers.get("stream-next-offset") or offset
        if mode == "head":
            count = 0
        else:
            body = response.read()
            try:
                payload = json.loads(body) if body else []
                count = len(payload) if isinstance(payload, list) else 0
            except json.JSONDecodeError:
                count = 0
except (OSError, urllib.error.URLError, urllib.error.HTTPError, TimeoutError):
    next_offset, count = offset, 0

print(json.dumps({"next_offset": next_offset, "envelopes": count}, separators=(",", ":")))
