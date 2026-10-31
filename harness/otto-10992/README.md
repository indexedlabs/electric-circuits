# OTTO-10992 real-stack backlog harness

This harness runs the checked-out engine against PostgreSQL 18.6 and the Rust
Durable Streams server. `compose.yaml` inserts `proxy.py` between the engine
and DS. `FAULT_MATCH` selects one DS HTTP path; `HANG=1` holds that path, and
`OUTAGE=1` holds every path. `DELAY_MS` adds a real network delay. No engine or
stream call is mocked. The pinned DS 0.1.5 image predates the engine's
`/_admin/ready` contract, so the proxy supplies that one identity response;
all stream operations still go to the real file-backed DS server.

The measurement runner is intentionally split into short remote commands:

```sh
docker compose -f docker/compose.yaml -f harness/otto-10992/compose.yaml up -d --build
docker compose exec -T postgres psql -U postgres -d electric < harness/otto-10992/schema.sql
./harness/otto-10992/run-scenario.sh baseline-a 60
```

Each run writes CSV samples under `harness/otto-10992/results/` and captures
`/memory`, `/metrics`, `/metrics/prometheus`, `/replication/lsn`, `/state`,
`/tables/*/offset`, and `pg_replication_slots` at every sample. The script
records engine RSS, DS disk usage, per-process/container stats, receipt and
replay fields exposed by `/metrics`, and p50/p99 client request latency.
