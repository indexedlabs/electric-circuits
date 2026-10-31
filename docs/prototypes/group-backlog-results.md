# OTTO-10992 group backlog prototype results

Status: **Blocked before baseline execution**.

The harness is on branch `proto/otto-10992-harness`. It contains a PostgreSQL
18.6 / Durable Streams / engine compose overlay, a real HTTP fault proxy,
schema seed, and the sampler under `harness/otto-10992/`. No engine or
Durable Streams calls are mocked.

## Remote execution blocker

Every attempted `mighty remote --provider tensorlake` command was rejected by
the Run kernel before a remote session was allocated:

```
invalid: remote project profile is unavailable
```

The same refusal occurred for a no-op probe (`echo remote-ok`) and for the
compose startup command. `mighty.agents/list_remote_providers` reports the
Tensorlake provider as ready, but `get_remote_project` for
`indexedlabs/electric-circuits` returns the same unavailable-profile error.
The refusal has `can_request: false`, so there is no access request that this
Run can make. The local sandbox has no `docker` executable and therefore
cannot run the stack as a fallback.

## Measurement matrix

| Variant | a stalled group | b DS outage 1/5/15 min | c 200 ms–2 s delay | d sparse source | e burst groups | Result |
|---|---:|---:|---:|---:|---:|---|
| `main` (`ac68597`) | not run | not run | not run | not run | not run | blocked at remote admission |
| `56f3dd1` | not run | not run | not run | not run | not run | blocked at remote admission |
| S1 prototype | not run | not run | not run | not run | not run | blocked at remote admission |
| S2 prototype | not run | not run | not run | not run | not run | blocked at remote admission |
| S3 prototype | not run | not run | not run | not run | not run | blocked at remote admission |

No baseline numbers, raw samples, or variant claims are reported because no
remote command reached Docker or the engine. Once the repository receives a
Tensorlake project profile, start with:

```sh
docker compose -f docker/compose.yaml -f harness/otto-10992/compose.yaml up -d --build
docker compose exec -T postgres psql -U postgres -d electric < harness/otto-10992/schema.sql
./harness/otto-10992/run-scenario.sh baseline-a 60
```

The sampler records engine RSS, DS bytes, engine memory/metrics and replay
fields, per-table offsets, replication-slot retained WAL, and health latency.
