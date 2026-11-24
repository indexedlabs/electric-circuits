from __future__ import annotations

import argparse
import ast
import gzip
import json
from pathlib import Path


def trim(bundle_path, provenance_paths):
    bundle = json.loads(gzip.decompress(bundle_path.read_bytes()))
    pending = [("api.services.circuits_sync", "_is_drained_receipt")]
    for path in provenance_paths:
        provenance = json.loads(path.read_text())
        pending.extend((entry["module"], entry["name"]) for entry in (provenance if isinstance(provenance, list) else provenance["loaded"]))
    selected = set()
    while pending:
        module, name = pending.pop()
        if (module, name) in selected or name == "ASSISTANT_USER_ID":
            continue
        descriptor = bundle["modules"].get(module)
        if not descriptor:
            continue
        if name in descriptor["imports"]:
            imported, symbol = descriptor["imports"][name]
            if symbol:
                pending.append((imported, symbol))
            continue
        source = descriptor["definitions"].get(name)
        if source is None:
            continue
        selected.add((module, name))
        syntax = ast.parse(source)
        for node in ast.walk(syntax):
            if isinstance(node, ast.Name) and (node.id in descriptor["definitions"] or node.id in descriptor["imports"]):
                pending.append((module, node.id))
            elif isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
                imported, symbol = descriptor["imports"].get(node.value.id, (None, None))
                if imported and not symbol:
                    pending.append((imported, node.attr))
            elif isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("api."):
                pending.extend((node.module, alias.name) for alias in node.names)
    original = len(bundle["modules"])
    modules = {}
    for module, descriptor in bundle["modules"].items():
        definitions = {name: source for name, source in descriptor["definitions"].items() if (module, name) in selected}
        if definitions:
            modules[module] = {**descriptor, "definitions": definitions}
    bundle["modules"] = modules
    bundle["export_scope"] = "Runtime-observed schema/catalog declarations plus their static declaration dependency closure; no unrelated application definitions"
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
