#!/usr/bin/env bash
set -euo pipefail
run_id=${1:?Usage: collect.sh ACTIONS_RUN_ID OUTPUT_DIRECTORY}
output=${2:?Usage: collect.sh ACTIONS_RUN_ID OUTPUT_DIRECTORY}
attachment=att-25b07cbe1e7e2f0be253ff42
repository=/repos/indexedlabs/electric-circuits
mkdir -p "$output"
call() {
  mighty invoke mighty.repositories/github_call --json "$(jq -nc --arg attachment "$attachment" --arg path "$1" '{attachment_id:$attachment,method:"GET",path:$path}')"
}
call "$repository/actions/runs/$run_id/artifacts" > "$output/artifacts.json"
while IFS=$'\t' read -r artifact_id artifact_name; do
  if [[ "$artifact_name" != stall-experiment-* && "$artifact_name" != stall-fixture-* && "$artifact_name" != stall-checkpoint-* && "$artifact_name" != stall-observer-build-* && "$artifact_name" != stall-historical-build-* ]]; then continue; fi
  receipt=$(mktemp)
  call "$repository/actions/artifacts/$artifact_id/zip" > "$receipt"
  location=$(jq -r '.headers[] | select(.name=="location") | .value' "$receipt")
  curl --fail --silent --show-error --location "$location" --output "$output/$artifact_name.zip"
  rm "$receipt"
  mkdir -p "$output/$artifact_name"
  unzip -q -o "$output/$artifact_name.zip" -d "$output/$artifact_name"
done < <(jq -r '.body.artifacts[] | [.id,.name] | @tsv' "$output/artifacts.json")
