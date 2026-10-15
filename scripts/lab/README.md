# Local hardening lab

`hardening.py` runs PostgreSQL 18, the Rust durable-streams server in WAL mode, an
engine, and live native-stream clients. It uses the topology and stream fold from
`packages/conformance/src/harness.ts` and the large-transaction/restart tests.
Python 3.11+ is the only runner dependency. This is a macOS local experiment, not
a release-qualification runner or an HA deployment service.

Build the pinned engine with `--locked --offline --features test-support`. Use an
isolated Cargo home populated with the lockfile's cached crates, and a target
directory inside this worktree. Debug info and incremental compilation can be
disabled to fit the disk budget; retain those build settings with measurements.
No production credentials or TLS fixture keys are needed: the existing explicit
loopback test transport is selected. PostgreSQL uses local trust authentication.

```sh
python3 scripts/lab/hardening.py steady --engine /absolute/path/to/engine
python3 scripts/lab/hardening.py transaction --engine /absolute/path/to/engine --mb 500 --tracked
python3 scripts/lab/hardening.py transaction --engine /absolute/path/to/engine --mb 500
python3 scripts/lab/hardening.py backlog --engine /absolute/path/to/engine --mb 2000
python3 scripts/lab/hardening.py restart --engine /absolute/path/to/engine
python3 scripts/lab/hardening.py catalog --engine /absolute/path/to/engine --mb 200
python3 scripts/lab/hardening.py rolling --engine /absolute/path/to/engine
python3 scripts/lab/rolling_contract.py --engine /absolute/path/to/engine
python3 scripts/lab/append_limit.py --engine /absolute/path/to/engine --mb 500
```

`--ds` selects an already-installed durable-streams binary; the runner never
downloads tools. Defaults are eight distinct tick shapes plus one wide bulk
shape, ten tick transactions/second, and Homebrew PostgreSQL 18. The large
transaction payload is decimal MB, rounded down to 64 KiB rows. `STORAGE EXTERNAL`
prevents TOAST compression from turning the payload into a tiny WAL workload.
The untracked table is still in the development `FOR ALL TABLES` publication.
Backlog generation updates/vacuums a bounded 32 MiB table while retaining slot
WAL. Catalog measurements explicitly label synthetic history amplification;
only the initial 25 join/release pairs measure the real lifecycle API.

Each tick transaction inserts its source UUID marker last. After writes stop,
the runner waits for `/_admin/drained-through/{uuid}`, starts a new public stream
read through the tail, commits its independent fold, and compares every row with
SQL. Bulk payload equality uses MD5 plus byte length to bound client retention.
This direct development topology is not the authenticated gateway/cache profile.

Latency starts when psql confirms commit and ends when the native client folds
the event. It excludes connection setup and can understate by the commit-response
transit time; negative values are clamped to zero. Percentiles use nearest rank.
Debug builds and local filesystem caches affect results. Memory is sampled RSS,
not a proof of a strict peak bound. Native clients read storage directly, so an
engine restart can delay changes while their HTTP reads remain successful.

Runs own `.agent/lab-data/<scenario>-<timestamp>` and remove it in `finally`.
`.agent/results/` retains JSON results, a PID/operation journal, resource samples,
and raw process logs. Processes have an explicit environment and process group;
cleanup signals only recorded children and waits for exit. Disk monitoring counts
all `.agent` artifacts, including builds, with an 8 GB ceiling and a 12 GiB free
floor; it stops 300 MB early. Run scenarios serially. Keep unrelated generated
data out of this checkout while running, and account separately for any build
artifacts placed outside `.agent`.

The rolling contract runs the local controller prototype in `rolling_prototype.py`.
It uses the existing `ELECTRIC_CIRCUITS_SHUTDOWN_DRAIN_SECS=0` setting, with the same
close → fresh receipt → quiesce → old exit → slot release → promote → ready → open
sequence as the baseline. The unchanged contract checks exact materialization and
a maximum two-second progress gap. A real load balancer needs its own proven route
withdrawal; this experiment does not qualify zero drain there. It also does not
pre-fold catalogs or provide automatic failover after owner death.

Two expensive known-red regression tests are opt-in Rust integration tests:

```sh
cargo test -p electric-circuits-engine --features test-support \
  --test hardening_lab -- --ignored --test-threads=1
python3 -B -m unittest discover -s scripts/lab -v
```

On the installed DS 0.1.5, a 500 MB atomic append returns `500 wal stage failed`,
and a 200 MB catalog prevents engine restart because an unpaged response exceeds
the 64 MiB admission cap. The tests keep their success assertions and fail visibly;
they are not expected-failure passes. The storage/protocol design needs resolution
before a green result is possible. See `.agent/REPORT.md` for exact evidence and
the distinction between this provider artifact and production storage.
