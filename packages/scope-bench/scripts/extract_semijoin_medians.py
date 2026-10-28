#!/usr/bin/env python3
"""Extract the comparison medians from four downloaded benchmark directories.

Each directory must contain semijoin-cold.json, semijoin-steady.json,
subquery-cold.json, and subquery-steady.json. The output is intentionally a
small, auditable per-directory JSON object; the checked-in manifest wraps the
same values with commit, artifact, and raw-file metadata.
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
    if not profile:
        return None
    membership = profile.get("membership_circuit")
    return membership.get("total_used_bytes") if membership else None


def dbsp_median(results, field):
    values = [value for row in results if (value := dbsp(row.get(field))) is not None]
    return statistics.median(values) if values else None


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
                "dbspUsedBytes": dbsp_median(rows, "dbspProfile"),
                "dbspHoldEndUsedBytes": dbsp_median(rows, "dbspProfileEnd"),
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
