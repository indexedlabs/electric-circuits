from __future__ import annotations

import argparse
import asyncio
import datetime
import hashlib
import json
import os
import subprocess
import time
from collections import Counter
from pathlib import Path
from uuid import uuid4

import httpx
import psutil
import psycopg

from catalog import GatewayShapes, TARGETS
from data import IMPORT_ACCOUNT, LARGE_ACCOUNT, load_data, row_id
from flamegraph import attribute, fold, render
from probes import OtherAccountProbe, summarize


ENGINE_URL = "http://127.0.0.1:7010"
PG_URL = "postgresql://postgres:synthetic@127.0.0.1:55432/stall_repro?sslmode=disable"
SECRET = "stall-repro-synthetic-secret"


def command(*arguments, **kwargs):
    return subprocess.run(arguments, check=True, text=True, **kwargs)


def save(path, value):
    path.write_text(json.dumps(value, indent=2, default=str))


def marker(connection, user_id, generation, source_commit_id):
    connection.execute("INSERT INTO public.native_sync_authority_fence (user_id, generation, source_commit_id) VALUES (%s,%s,%s) ON CONFLICT (user_id) DO UPDATE SET generation=EXCLUDED.generation, source_commit_id=EXCLUDED.source_commit_id", (user_id, generation, source_commit_id))


async def until_ready(client, unit, timeout=300):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            response = await client.get(f"{ENGINE_URL}/ready", timeout=2)
            if response.status_code == 200:
                return
        except httpx.HTTPError:
            pass
        active = subprocess.check_output(["systemctl", "show", unit, "--property=ActiveState", "--value"], text=True).strip()
        if active not in {"active", "activating"}:
            raise RuntimeError(f"Engine service exited ({active}); inspect engine.log")
        await asyncio.sleep(1)
    raise TimeoutError("Engine did not become ready; inspect engine.log")


async def wait_for_marker(client, validator, source_commit_id, user_id, generation, timeout):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            response = await client.get(f"{ENGINE_URL}/_runtime/drained-through/{source_commit_id}", params={"user_id": str(user_id), "generation": generation}, timeout=2)
            last = {"status": response.status_code, "body": response.text[:1000]}
            if response.status_code == 200 and validator(response.json(), marker=source_commit_id, user_id=user_id, generation=generation):
                state = await client.get(f"{ENGINE_URL}/replication/lsn", timeout=2)
                state.raise_for_status()
                if state.json()["pendingFlips"] == 0 and state.json()["failingAppends"] == 0:
                    return {"receipt": response.json(), "queue": state.json()}
        except httpx.HTTPError as error:
            last = repr(error)
        await asyncio.sleep(0.2)
    raise TimeoutError(f"Workload did not drain in {timeout}s; last observation: {last}")


def read_events(path, after, before=float("inf")):
    with path.open(errors="replace") as handle:
        for line in handle:
            try:
                event = json.loads(line)
                timestamp = datetime.datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00")).timestamp()
                if after <= timestamp <= before:
                    yield event
            except (ValueError, KeyError, TypeError):
                continue


async def sample_cpu(process, cgroup, output, stopped):
    with output.open("w") as handle:
        while not stopped.is_set():
            handle.write(json.dumps({"at": time.time(), "cpu_s": sum(process.cpu_times()[:2]), "rss_bytes": process.memory_info().rss, "threads": process.num_threads(), "cpu_stat": (cgroup / "cpu.stat").read_text()}) + "\n")
            handle.flush()
            try:
                await asyncio.wait_for(stopped.wait(), timeout=1)
            except TimeoutError:
                pass


async def run(args):
    if os.environ.get("GITHUB_ACTIONS") != "true" and os.environ.get("STALL_REPRO_REMOTE_MACHINE") != "1":
        raise RuntimeError("Run only on the authorized isolated remote machine or GitHub Actions, never in a Mighty Run sandbox")
    folder = Path(__file__).parent.resolve()
    output = folder / "results" / args.name
    output.mkdir(parents=True, exist_ok=False)
    fixture = folder / "results/fixture"
    gateway = GatewayShapes(folder / "indexed-source.json.gz")
    data = load_data(fixture / "columns.json")
    if args.variant in {"B", "B-retained"}:
        statements = await asyncio.to_thread(data.add_audience_columns, PG_URL, [table.removeprefix("public.") for table in gateway.loader.bundle["publication"]])
        (output / "audience-schema.sql").write_text(";\n".join(statements) + ";\n")
    help_probe = subprocess.run([str(Path(args.store).resolve()), "--help"], capture_output=True, text=True)
    (output / "store-help.txt").write_text(help_probe.stdout + help_probe.stderr)
    supports_page_cap = b"--max-chunk-bytes" in Path(args.store).read_bytes()
    store_arguments = [str(Path(args.store).resolve()), "--host", "127.0.0.1", "--port", "8791", "--data-dir", str(output / "store-data"), "--durability", "wal"]
    if supports_page_cap:
        store_arguments.extend(["--max-chunk-bytes", "4194304"])
    save(output / "store-config.json", {"command": store_arguments, "sha256": hashlib.file_digest(Path(args.store).open("rb"), "sha256").hexdigest(), "matches_indexed_4mib_page_setting": supports_page_cap, "cap_detection": "option literal in pinned binary, then validated by server startup", "help_exit": help_probe.returncode})
    print("Seeding synthetic database", flush=True)
    save(output / "seed.json", await asyncio.to_thread(data.seed, PG_URL))
    store_log = (output / "store.log").open("w")
    store = subprocess.Popen(store_arguments, stdout=store_log, stderr=subprocess.STDOUT)
    async with httpx.AsyncClient(timeout=1) as store_client:
        for attempt in range(100):
            try:
                response = await store_client.head("http://127.0.0.1:8791/stall-readiness-nonexistent")
                if response.status_code == 404:
                    break
            except httpx.HTTPError:
                pass
            if store.poll() is not None:
                raise RuntimeError("Stream store exited before readiness")
            await asyncio.sleep(0.1)
        else:
            store.terminate()
            raise TimeoutError("Stream store did not accept requests")
    proxy_log = (output / "store-proxy.log").open("w")
    proxy_ready = output / "store-proxy.json"
    proxy = subprocess.Popen(["node", str(folder / "store-proxy.ts"), str(proxy_ready)], stdout=proxy_log, stderr=subprocess.STDOUT)
    for attempt in range(100):
        if proxy_ready.exists():
            break
        if proxy.poll() is not None:
            store.terminate()
            raise RuntimeError("Store proxy exited; inspect store-proxy.log")
        await asyncio.sleep(0.1)
    access = json.loads(proxy_ready.read_text())
    unit = f"stall-repro-{uuid4().hex[:10]}"
    engine_log = output / "engine.log"
    engine_log.touch()
    env = {
        **access["env"],
        "ELECTRIC_CIRCUITS_DS_URL": access["url"],
        "ELECTRIC_CIRCUITS_PG_URL": PG_URL,
        "ELECTRIC_CIRCUITS_PG_TABLES": ",".join(gateway.loader.bundle["publication"]),
        "ELECTRIC_CIRCUITS_PG_SLOT": "stall_repro",
        "ELECTRIC_CIRCUITS_BIND": "127.0.0.1:7010",
        "ELECTRIC_CIRCUITS_MAX_SHAPES": "20000",
        "ELECTRIC_CIRCUITS_FLIP_WORKERS": "8",
        "ELECTRIC_CIRCUITS_SUBSCRIPTION_LEASE_SECS": "0",
        "ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS": "0",
        "ELECTRIC_CIRCUITS_LOG_JSON": "1",
        "ELECTRIC_CIRCUITS_TRACE": "0",
        "ELECTRIC_SECRET": SECRET,
        "ELECTRIC_STORAGE_DIR": str(output / "engine-data"),
    }
    if args.variant == "marker-bypass":
        env["STALL_REPRO_UNSAFE_SKIP_MARKER_WAIT"] = "1"
    if args.variant == "commit-membership":
        env["STALL_REPRO_COMMIT_MEMBERSHIP"] = "1"
    command("sudo", "systemd-run", "--unit", unit, "--collect", f"--property=CPUQuota={int(args.cpus * 100)}%", f"--property=StandardOutput=append:{engine_log}", f"--property=StandardError=append:{engine_log}", "/usr/bin/env", *[f"{key}={value}" for key, value in env.items()], str(Path(args.engine).resolve()))
    manifest = {"command": " ".join(os.sys.argv), "harness_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(), "engine_base": "5b2cf3e2731d7fcecc3f95b9c05a5963b9c6b363", "engine_sha256": hashlib.file_digest(Path(args.engine).open("rb"), "sha256").hexdigest(), "indexed_commit": gateway.loader.bundle["commit"], "catalog": args.catalog, "scale": args.scale, "cpu_quota": args.cpus, "visible_cpus": os.cpu_count(), "environment": {key: value for key, value in env.items() if key != "ELECTRIC_SECRET"}, "uname": os.uname(), "target": TARGETS[args.catalog]}
    manifest["variant"] = args.variant
    manifest["engine_source"] = args.engine_source
    save(output / "manifest.json", manifest)
    try:
        async with httpx.AsyncClient(headers={"Authorization": f"Bearer {SECRET}"}, timeout=120) as client:
            await until_ready(client, unit)
            print("Engine ready; generating gateway requests", flush=True)
            pid = int(subprocess.check_output(["systemctl", "show", unit, "--property=MainPID", "--value"], text=True).strip())
            process = psutil.Process(pid)
            save(output / "engine-threads-at-ready.json", {thread.id: Path(f"/proc/{pid}/task/{thread.id}/comm").read_text().strip() for thread in process.threads()})
            catalog = await gateway.generate(args.catalog, args.scale)
            if args.variant in {"B", "B-retained"}:
                catalog = await gateway.keyed_catalog(catalog, retained=args.variant == "B-retained")
            if args.smoke:
                catalog = catalog[:20] + catalog[-20:]
            semaphore = asyncio.Semaphore(8)
            created = 0
            async def create(request):
                nonlocal created
                async with semaphore:
                    response = await client.post(f"{ENGINE_URL}/v1/subset-feeds", json=request["body"])
                    if response.status_code != 200:
                        raise RuntimeError(f"Shape creation failed: {response.status_code} {response.text[:2000]} table={request['body']['table']}")
                    created += 1
                    if created % 500 == 0:
                        print(f"Created {created}/{len(catalog)} shapes", flush=True)
                    return response.json()
            handles = await asyncio.gather(*(create(request) for request in catalog))
            save(output / "shape-handles.json", handles)
            for table in ("record", "record_date_index", "record_recurrence", "assignment", "calendar_event", "graph_edge", "resource"):
                keyed = args.variant == "B" or args.variant == "B-retained" and table not in {"graph_edge", "resource"}
                for calendar in (range(15) if keyed and table == "calendar_event" else [0]):
                    body = gateway.keyed_body(table, IMPORT_ACCOUNT, calendar=calendar, identity=f"import-{table}") if keyed else await gateway.body(table, IMPORT_ACCOUNT, identity=f"import-{table}")
                    await create({"body": body})
            probe_body = gateway.keyed_body("record", 0, identity="other-account-probe") if args.variant in {"B", "B-retained"} else await gateway.body("record", 0, identity="other-account-probe")
            probe_feed = await create({"body": probe_body})
            head = await client.head(probe_feed["streamUrl"])
            head.raise_for_status()
            probe_feed["offset"] = head.headers["stream-next-offset"]
            memory = await client.get(f"{ENGINE_URL}/memory")
            memory.raise_for_status()
            save(output / "catalog-before-warmup.json", memory.json())
            print(json.dumps({"before_warmup": memory.json()["cardinalities"]}), flush=True)
            for endpoint in ("graph", "state"):
                response = await client.get(f"{ENGINE_URL}/{endpoint}")
                save(output / f"catalog-{endpoint}.json", {"status": response.status_code, "body": response.json() if response.status_code == 200 else response.text})
            validator = gateway.gateway._is_drained_receipt
            fence_user = row_id("user", 1)
            fence_generation = gateway.authority(1).generation
            warm_marker = uuid4()
            def warm():
                with psycopg.connect(PG_URL) as connection:
                    for account in range(19):
                        data.update(connection, "record", account, 1 if args.smoke else 9 if args.catalog == "nov23" else 19)
                    marker(connection, fence_user, fence_generation, warm_marker)
            await asyncio.to_thread(warm)
            await wait_for_marker(client, validator, warm_marker, fence_user, fence_generation, 900)
            memory = await client.get(f"{ENGINE_URL}/memory")
            memory.raise_for_status()
            save(output / "catalog-calibration.json", {"target": TARGETS[args.catalog], "actual": memory.json(), "source_shape_requests": len(catalog), "smoke_only": args.smoke})
            print(json.dumps({"calibration": memory.json()["cardinalities"]}), flush=True)
            if args.calibrate_only:
                return
            command("sudo", "systemctl", "set-property", unit, f"CPUQuota={int(args.cpus * 100)}%")
            cgroup = subprocess.check_output(["systemctl", "show", unit, "--property=ControlGroup", "--value"], text=True).strip()
            quota = Path("/sys/fs/cgroup") / cgroup.lstrip("/") / "cpu.max"
            (output / "cpu.max").write_text(quota.read_text())
            quota_value, period = quota.read_text().split()
            if float(quota_value) / float(period) != args.cpus or os.cpu_count() < 2:
                raise AssertionError("Engine CPU topology does not match requested quota")
            subset = {key: value for key, value in probe_body.items() if key not in {"subscription", "changesOnly"}}
            subset["where"] = {"and": [subset["where"], {"col": "id", "op": "eq", "value": str(row_id("record", 0))}]}
            subset["limit"] = 1
            for workload in args.workloads:
                probe = OtherAccountProbe(engine_url=ENGINE_URL, postgres_url=PG_URL, user_id=row_id("user", 0), record_id=row_id("record", 0), generation=gateway.authority(0).generation, feed=probe_feed, subset=subset, receipt_validator=validator, output=output / f"{workload}-probes.jsonl")
                probe_task = asyncio.create_task(probe.run())
                await asyncio.sleep(3)
                profile_before = (await client.get(f"{ENGINE_URL}/memory")).json().get("stall_profile")
                fence_id = uuid4()
                transactions = []
                def mark(connection, transaction):
                    current = uuid4() if workload == "import" and transaction != 5 else fence_id
                    marker(connection, fence_user, fence_generation, current)
                    transactions.append({"transaction": transaction, "txid": connection.execute("SELECT pg_current_xact_id()::text").fetchone()[0], "before_commit": time.time(), "marker": str(current)})
                began_wall = time.time()
                began = time.monotonic()
                profiler = None
                perf = os.environ.get("STALL_REPRO_PERF")
                if perf and workload == "import":
                    perf_log = (output / "perf.log").open("w")
                    profiler = subprocess.Popen(["sudo", perf, "record", "-e", "cpu-clock", "-F", "99", "-g", "--call-graph", "fp", "-p", str(pid), "-o", str(output / "import.perf.data"), "--", "sleep", str(args.timeout + 120)], stdout=perf_log, stderr=subprocess.STDOUT, start_new_session=True)
                cpu_before = sum(process.cpu_times()[:2])
                cpu_stopped = asyncio.Event()
                cpu_task = asyncio.create_task(sample_cpu(process, quota.parent, output / f"{workload}-cpu.jsonl", cpu_stopped))
                result = {"workload": workload, "measurement": "source SQL start to workload receipt and zero pending flips/appends", "first_change_read_time": None, "timeout_s": args.timeout}
                try:
                    operation = data.calendar_import if workload == "import" else lambda url, callback: data.delete_account(url, LARGE_ACCOUNT if workload == "large-delete" else IMPORT_ACCOUNT, callback)
                    journal = await asyncio.to_thread(operation, PG_URL, mark)
                    save(output / f"{workload}-writes.json", journal)
                    result["row_changes"] = sum(entry["rows"] for entry in journal)
                    result["source_write_s"] = time.monotonic() - began
                    result["completion"] = await wait_for_marker(client, validator, fence_id, fence_user, fence_generation, args.timeout)
                    result["completed"] = True
                except Exception as error:
                    result["completed"] = False
                    result["error"] = repr(error)
                result["elapsed_s"] = time.monotonic() - began
                result["ended_wall"] = time.time()
                cpu_after = sum(process.cpu_times()[:2])
                result["began_wall"] = began_wall
                result["transactions"] = transactions
                cpu_stopped.set()
                probe.stop()
                if profiler:
                    subprocess.run(["sudo", "kill", "-INT", "--", f"-{profiler.pid}"], check=False)
                    profiler.wait(timeout=30)
                    subprocess.run(["sudo", "chmod", "a+r", str(output / "import.perf.data")], check=True)
                    perf_log.close()
                await cpu_task
                await probe_task
                if profiler:
                    with (output / "import.perf.script").open("w") as handle:
                        extracted = subprocess.run(["sudo", perf, "script", "-i", str(output / "import.perf.data")], stdout=handle, stderr=subprocess.PIPE, text=True)
                    stacks = fold((output / "import.perf.script").read_text())
                    result["profile"] = {"elapsed_s": result["elapsed_s"], "samples": sum(stacks.values()), "script_exit": extracted.returncode, "script_stderr": extracted.stderr, "at_least_three_minutes": result["elapsed_s"] >= 180, "attribution": attribute(stacks) if stacks else {}}
                    if stacks:
                        (output / "import.folded").write_text("\n".join(f"{stack} {count}" for stack, count in sorted(stacks.items())) + "\n")
                        (output / "import.svg").write_text(render(stacks, f"{args.name} import CPU"))
                result["engine_cpu_percent"] = (cpu_after - cpu_before) / result["elapsed_s"] * 100
                result["changes_per_s"] = result.get("row_changes", 0) / result["elapsed_s"] if result["completed"] else None
                result["probe"] = summarize([sample for sample in probe.samples if began - probe.started <= sample["at_s"] <= began + result["elapsed_s"] - probe.started])
                result["idle_probe"] = summarize([sample for sample in probe.samples if sample["at_s"] < began - probe.started])
                if profile_before is not None:
                    profile_after = (await client.get(f"{ENGINE_URL}/memory")).json()["stall_profile"]
                    result["wall_profile"] = {"before": profile_before, "after": profile_after, "delta": {category: {metric: values[metric] - profile_before["categories"][category][metric] for metric in values} for category, values in profile_after["categories"].items()}}
                events = read_events(engine_log, began_wall, result["ended_wall"])
                by_table = Counter()
                empty = Counter()
                result["pinned_warnings"] = 0
                result["large_transactions"] = []
                for event in events:
                    fields = event.get("fields", {})
                    if fields.get("message") == "backfill finished":
                        by_table[fields["table"]] += 1
                        if fields.get("rows") == 0:
                            empty[fields["table"]] += 1
                    if "replay position is pinned" in str(fields.get("message", "")):
                        result["pinned_warnings"] += 1
                    if fields.get("message") == "large transaction appended to the change log":
                        result["large_transactions"].append(fields)
                result["backfills"] = sum(by_table.values())
                result["backfills_by_table"] = dict(by_table)
                result["empty_backfills_by_table"] = dict(empty)
                result["backfills_per_minute"] = result["backfills"] * 60 / result["elapsed_s"]
                txids = {entry["txid"] for entry in transactions}
                stream_events = output / "store-proxy.json.events.jsonl"
                if stream_events.exists():
                    with stream_events.open() as handle:
                        matching = [event for line in handle if txids.intersection((event := json.loads(line)).get("transactions", {}))]
                    if matching:
                        result["first_change_read_time"] = min(event["ended"] for event in matching)
                        result["first_read_to_empty_s"] = result["ended_wall"] - result["first_change_read_time"]
                        result["first_read_definition"] = "first completed change-log HTTP delivery containing a workload xid, at the out-of-cgroup loopback proxy"
                        result["engine_changes_per_s"] = result.get("row_changes", 0) / result["first_read_to_empty_s"] if result["completed"] else None
                        with (output / f"{workload}-cpu.jsonl").open() as handle:
                            samples = [sample for line in handle if result["first_change_read_time"] <= (sample := json.loads(line))["at"] <= result["ended_wall"]]
                        if len(samples) >= 2:
                            result["cpu_sample_window_s"] = samples[-1]["at"] - samples[0]["at"]
                            result["engine_cpu_after_first_read_percent"] = (samples[-1]["cpu_s"] - samples[0]["cpu_s"]) * 100 / result["cpu_sample_window_s"]
                save(output / f"{workload}-result.json", result)
                print(json.dumps(result, default=str), flush=True)
                if not result["completed"]:
                    raise RuntimeError(f"{workload} did not complete; later workloads not contaminated with its backlog")
    finally:
        subprocess.run(["sudo", "systemctl", "stop", unit], timeout=120, check=False)
        proxy.terminate()
        try:
            proxy.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proxy.kill()
            proxy.wait()
        proxy_log.close()
        store.terminate()
        try:
            store.wait(timeout=30)
        except subprocess.TimeoutExpired:
            store.kill()
            store.wait()
        store_log.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", required=True)
    parser.add_argument("--store", required=True)
    parser.add_argument("--catalog", choices=TARGETS, default="nov24")
    parser.add_argument("--name", required=True)
    parser.add_argument("--scale", type=float, default=1)
    parser.add_argument("--cpus", type=float, default=1)
    parser.add_argument("--timeout", type=int, default=3600)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--calibrate-only", action="store_true")
    parser.add_argument("--variant", choices=("A", "A-observed", "B", "B-retained", "marker-bypass", "commit-membership", "coalesced-base", "coalesced-head"), default="A")
    parser.add_argument("--engine-source", default="5b2cf3e2731d7fcecc3f95b9c05a5963b9c6b363")
    parser.add_argument("--workloads", nargs="+", choices=("import", "large-delete", "small-delete"), default=["import", "large-delete", "small-delete"])
    asyncio.run(run(parser.parse_args()))
