from __future__ import annotations

import argparse
import gzip
import json
from pathlib import Path


def trim(bundle_path, provenance_paths):
    bundle = json.loads(gzip.decompress(bundle_path.read_bytes()))
    selected = {("api.services.circuits_sync", "_is_drained_receipt")}
    for path in provenance_paths:
        provenance = json.loads(path.read_text())
        selected.update((entry["module"], entry["name"]) for entry in (provenance if isinstance(provenance, list) else provenance["loaded"]))
    original = len(bundle["modules"])
    modules = {}
    for module, descriptor in bundle["modules"].items():
        definitions = {name: source for name, source in descriptor["definitions"].items() if (module, name) in selected}
        modules[module] = {**descriptor, "definitions": definitions}
    bundle["modules"] = modules
    bundle["export_scope"] = "Runtime-observed schema/catalog declarations plus the original receipt validator; import metadata retained, unrelated application definitions removed; regenerate for new execution paths"
    with bundle_path.open("wb") as raw:
        with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0, filename="") as compressed:
            compressed.write(json.dumps(bundle, sort_keys=True, separators=(",", ":")).encode())
    print(json.dumps({"original_modules": original, "retained_modules": len(modules), "retained_definitions": len(selected), "bytes": bundle_path.stat().st_size}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("bundle", type=Path)
    parser.add_argument("provenance", nargs="+", type=Path)
    arguments = parser.parse_args()
    trim(arguments.bundle, arguments.provenance)
