# OTTO-13916 — measurements pending

No M1, M2, M3, or M4 milestone has been reached. No stall, CPU attribution,
throughput, or improvement has been measured. In particular, a build or an
environment check is not a reproduction.

## Provenance

| Input | Identity |
| --- | --- |
| Production engine base | `5b2cf3e2731d7fcecc3f95b9c05a5963b9c6b363` |
| Scratch branch | `profile/stall-repro-2026-11-24` |
| Data | Synthetic only; no prod/dev access permitted |
| Engine profile | Release, debug level 1, force-frame-pointers |
| Required engine limit | 1 CPU quota, at least two visible cores, 8 flip workers |
| Required shape limit | `ELECTRIC_CIRCUITS_MAX_SHAPES=20000` |

## Environment evidence

| Attempt | Command | Outcome |
| --- | --- | --- |
| Mighty Linux, 8 CPU / 16 GiB | Exact preflight in `../README.md`; command `01a20c4b-6aab-7c63-9c3a-2b08991a1af1` | Admission failed: `fleet mighty.agents.run.fleet-missing-capability: mighty.compute.os.linux`. No workload ran. Switched to GitHub Actions immediately rather than spending the environment budget retrying an absent capability. |
| GitHub Actions | `.github/workflows/stall-repro.yml` | Pending. |

## Important interpretation limit

The requested model B is a measurement idealization where hot rows carry an
audience key. The attached proposal explicitly retains scopes for chat, edges,
Resources, and direct shares. An all-plain-shape result must not be described as
proof that the proposal's retained subqueries are harmless. The measured schema,
retained scopes, and any omitted costs must be stated with the eventual results.
