import argparse
import gzip
import hashlib
import json
import shutil
from pathlib import Path


parser = argparse.ArgumentParser()
parser.add_argument("artifacts", type=Path)
parser.add_argument("destination", type=Path)
arguments = parser.parse_args()
for source in sorted(arguments.artifacts.glob("stall-experiment-*/run-*")):
    if not (source / "import-result.json").exists():
        continue
    manifest = json.loads((source / "manifest.json").read_text())
    name = manifest.get("label") or f"{manifest.get('variant', 'A')}-{manifest['catalog']}-{manifest['cpu_quota']}cpu-{manifest['harness_commit'][:7]}"
    target = arguments.destination / name
    target.mkdir(parents=True, exist_ok=True)
    for pattern in ("*-result.json", "*-writes.json", "*-probes.jsonl", "*-cpu.jsonl", "catalog-*.json", "manifest.json", "cpu.max", "engine-threads-at-ready.json", "store-config.json", "audience-schema.sql", "*-batch-check.txt"):
        for path in source.glob(pattern):
            shutil.copyfile(path, target / path.name)
    for name in ("engine.log", "store-proxy.json.events.jsonl"):
        if (source / name).exists():
            with (source / name).open("rb") as input_file, (target / f"{name}.gz").open("wb") as output_file:
                with gzip.GzipFile(filename="", mode="wb", fileobj=output_file, mtime=0) as compressed:
                    shutil.copyfileobj(input_file, compressed)
    machine = source.parent / "fixture/experiment-machine.txt"
    if machine.exists():
        shutil.copyfile(machine, target / "machine.txt")
    hashes = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(target.iterdir()) if path.name != "evidence-sha256.json"}
    (target / "evidence-sha256.json").write_text(json.dumps(hashes, indent=2) + "\n")
    print(target)
