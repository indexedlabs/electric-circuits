# OTTO-13916 — isolated stall reproduction (scratch only)

This branch starts at engine commit `5b2cf3e2731d7fcecc3f95b9c05a5963b9c6b363`.
No changes here are intended for merge, deployment, or review. Never point this
harness at production or development; all rows and identities must be synthetic.

The harness is under construction. SQL-only workload validation passes, but no
engine workload measurements have been made. See `results/RESULTS.md` for the
evidence status, including environment failures and fidelity limitations.

## Execution route

The initial remote admission command was:

```sh
mighty remote --provider tensorlake --cpu 8 --mem 16 --timeout 300 -- bash -lc \
  'date -u; uname -a; nproc; command -v docker || true; docker version 2>&1; command -v perf || true; cat /proc/sys/kernel/perf_event_paranoid; command -v sudo || true; sudo -n true 2>&1; command -v cargo; rustc --version; command -v psql || true'
```

Admission failed before any command executed: the fleet has no Linux capability.
The authorized fallback is `.github/workflows/stall-repro.yml`, triggered by pushes
to this scratch branch. It builds the unmodified production engine in release mode
with debug symbols and frame pointers, and records its machine and binary hash.
PostgreSQL, file-backed Durable Streams and its loopback fixture identity proxy
run outside the engine's CPU cgroup. The proxy reuses the repository's conformance
fixture: actual stream operations reach the Rust store, but readiness metadata is
synthetic. This does not qualify the production storage implementation.

## Reproduce on an isolated Actions runner

The workflow is the complete executable recipe. It pins the production-source
engine and store artifacts by their originating run IDs, so a Python harness fix
does not require rebuilding Rust. Push a commit containing `[smoke]` for a reduced
40-request smoke, `[measure]` for the full Nov 24 catalog, `[engine]` to rebuild
the engine, or `[store]` to rebuild the store. Ordinary pushes validate fixtures.
Documentation-only commits do not launch another experiment.

```sh
python -m unittest discover -s tools/stall-repro -p 'test_*.py'
python tools/stall-repro/export_fixture.py
python tools/stall-repro/catalog.py
docker run -d --name stall-experiment-pg -p 127.0.0.1:55432:5432 \
  -e POSTGRES_PASSWORD=synthetic -e POSTGRES_DB=stall_repro \
  postgres:18 -c wal_level=logical -c max_replication_slots=10
docker exec -i stall-experiment-pg psql -v ON_ERROR_STOP=1 \
  -U postgres -d stall_repro < tools/stall-repro/results/fixture/schema.sql
python tools/stall-repro/run.py \
  --engine tools/stall-repro/bin/electric-circuits-engine \
  --store tools/stall-repro/bin/durable-streams-server \
  --name run-nov24 --catalog nov24 --cpus 1
```

Wait for `pg_isready` before applying the schema, as the workflow does. The driver
requires `GITHUB_ACTIONS=true` or an explicitly authorized remote machine setting
`STALL_REPRO_REMOTE_MACHINE=1`. Do not set either to run in a Mighty Run sandbox.
Python dependencies and artifact download steps are specified in the workflow.
Node 22 runs `store-proxy.ts` using native type stripping; no pnpm install is needed.
Systemd/sudo is required for the engine cgroup. Setup warms at 4 CPUs, measured
work runs at the requested verified quota. Each run requires a fresh database.

## Files

- `indexed-source.json.gz`: pinned, hashed source definitions from the read-only
  Indexed checkout; no customer rows, credentials or runtime application imports.
- `export_fixture.py`, `source_loader.py`: actual SQLAlchemy schema and gateway
  predicate functions, loaded without application secrets or side effects.
- `catalog.py`: weighted, synthetic historical gateway requests and logged targets.
- `data.py`: deterministic synthetic accounts, import and two deletion workloads.
- `probes.py`: independent-account drain, subset and live-change observations.
- `run.py`: isolated setup, cgroup verification, catalog snapshots and measurements.
- `results/`: retained evidence; raw per-run artifacts also live in Actions.

The source bundle is refreshed with `export_indexed.py` only after explicitly
choosing an Indexed revision. Do not modify or push to the attached Indexed repo.
