# Generated readiness plan

- Authoritative task inventory: 169
- Normative conditional edges: 36
- Future release profiles: unavailable until GOV-005
- Future scenario registry: unavailable until E2E-000S

## First ready sets

- `shared-pre-registry` after `PLAN-001`: `GOV-001`, `TST-000`
- `shared-pre-registry` after `PLAN-001,GOV-001`: `CMP-000`, `GOV-002`, `GOV-003`, `SEC-008B`, `TST-000`

## Ownership and gate matrix

- 169 principal boundaries, 507 phase rows, and one owner/applicability binding per row.
- Direct, merge, and qualification commands are chosen by task capability family; PLAN-001 author direct runs its contract suite.

## Deterministic identities

The non-self-referential projection below binds the five peer outputs. The `identity` command emits all six current output identities, including this report, for packet handoff.

- docs/production/readiness-tasks.json: git blob 0fae81d53d331b222c8d8aaed7ae4ceb863e1291; SHA-256 2ff8d9992ce8d2f0dbade580801cf5178bafe16566bce5a3306b8b0ede0f0952
- docs/production/readiness-task.schema.json: git blob 7dd4023fbaaddd13c3c2b1c8ff2b95d0e5addfa1; SHA-256 4843615b8ad5b0ee3ef61ec42281a349a44658f635ea129d5d1f60d59b11a6c6
- docs/production/readiness-gates.json: git blob 04e879171c01336dacd95fc522ecad72d9403089; SHA-256 6e00f7f126eaa1780f8d026c75b9f53dfda1aa21c21a41774ea7a990b9b894c2
- scripts/readiness-plan.ts: git blob 1b8b3c275e9ebedc91228c7d8bd0f58596f8dbb3; SHA-256 b019e69ffe7f59790ebd4af2bfcb5d2d8eb84080a4ad6f294a4aadc43ebc5829
- scripts/readiness-plan.test.ts: git blob 2950a3b20b8c3100096b24819fb1a39f9b7754e6; SHA-256 7c9e6ce2cede322c33c1c8e773abecd151b11ccfc2761bef744dc01135576cfe

Use `pnpm exec tsx scripts/readiness-plan.ts identity` to emit the complete six-output identity set.
