#!/usr/bin/env python3
"""Extract the comparison medians from four downloaded benchmark directories.

Each directory must contain semijoin-cold.json, semijoin-steady.json,
subquery-cold.json, and subquery-steady.json. The output is intentionally a
small, auditable JSON object; the checked-in manifest adds artifact metadata
and raw-file hashes around the same values.
"""

import argparse
import hashlib
import json
import pathlib
import statistics


def median(results, field):
    values = [row[field] for row in results if row.get(field) is not None]
    return statistics.median(values) if values else None


def dbsp(profile):
    return profile["membership_circuit"]["total_used_bytes"]


def extract(path):
    output = {}
    for mode in ("subquery", "semijoin"):
        output[mode] = {}
        for scenario in ("cold", "steady"):
            filename = f"{mode}-{scenario}.json"
            raw = (path / filename).read_bytes()
            document = json.loads(raw)
            rows = document["results"]
            output[mode][scenario] = {
                "raw_sha256": hashlib.sha256(raw).hexdigest(),
                "groupLoadMs": median(rows, "groupLoadMs"),
                "engineRssMb": median(rows, "engineRssMb"),
                "dbspLoadUsedBytes": statistics.median(
                    dbsp(row["dbspProfile"]) for row in rows
                ),
                "dbspHoldEndUsedBytes": statistics.median(
                    dbsp(row["dbspProfileEnd"]) for row in rows
                ),
                "engineCpuMs": median(rows, "engineCpuMs"),
                "gatewayCpuMs": median(rows, "gatewayCpuMs"),
                "steadyWriteP50": median(rows, "steadyWriteP50"),
                "steadyWriteP90": median(rows, "steadyWriteP90"),
                "readsPerChange": median(rows, "readsPerChange"),
            }
    return output


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--artifacts",
        type=pathlib.Path,
        required=True,
        help="directory containing one subdirectory per replicate",
    )
    args = parser.parse_args()
    result = {
        replicate.name: extract(replicate)
        for replicate in sorted(args.artifacts.iterdir())
        if replicate.is_dir()
    }
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
