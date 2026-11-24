from __future__ import annotations

import argparse
import asyncio
import datetime
import hashlib
import json
import os
import subprocess
import time
from pathlib import Path
from uuid import uuid4

import httpx
import psutil
import psycopg

from catalog import GatewayShapes, TARGETS
from data import IMPORT_ACCOUNT, LARGE_ACCOUNT, load_data, row_id
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


async def until_ready(client, timeout=300):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            response = await client.get(f"{ENGINE_URL}/ready", timeout=2)
            if response.status_code == 200:
                return
        except httpx.HTTPError:
            pass
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


def read_events(path, after):
    events = []
    for line in path.read_text(errors="replace").splitlines():
        try:
            event = json.loads(line)
            timestamp = datetime.datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00")).timestamp()
            if timestamp >= after:
                events.append(event)
        except (ValueError, KeyError, TypeError):
            continue
    return events


async def run(args):
    if os.environ.get("GITHUB_ACTIONS") != "true" and os.environ.get("STALL_REPRO_REMOTE_MACHINE") != "1":
        raise RuntimeError("Run only on the authorized isolated remote machine or GitHub Actions, never in a Mighty Run sandbox")
    folder = Path(__file__).parent.resolve()
    output = folder / "results" / args.name
    output.mkdir(parents=True, exist_ok=False)
    fixture = folder / "results/fixture"
    gateway = GatewayShapes(folder / "indexed-source.json.gz")
    data = load_data(fixture / "columns.json")
    save(output / "seed.json", await asyncio.to_thread(data.seed, PG_URL))
    store_log = (output / "store.log").open("w")
    store = subprocess.Popen([str(Path(args.store).resolve()), "--host", "127.0.0.1", "--port", "8791", "--data-dir", str(output / "store-data"), "--durability", "wal"], stdout=store_log, stderr=subprocess.STDOUT)
    unit = f"stall-repro-{uuid4().hex[:10]}"
    engine_log = output / "engine.log"
    engine_log.touch()
    env = {
        "ELECTRIC_CIRCUITS_DS_URL": "http://127.0.0.1:8791",
        "ELECTRIC_CIRCUITS_DS_IN_PROCESS_TEST": "1",
        "ELECTRIC_CIRCUITS_PG_URL": PG_URL,
        "ELECTRIC_CIRCUITS_PG_TABLES": ",".join(gateway.loader.bundle["publication"]),
        "ELECTRIC_CIRCUITS_PG_SLOT": "stall_repro",
        "ELECTRIC_CIRCUITS_BIND": "127.0.0.1:7010",
        "ELECTRIC_CIRCUITS_MAX_SHAPES": "20000",
        "ELECTRIC_CIRCUITS_FLIP_WORKERS": "8",
        "ELECTRIC_CIRCUITS_SUBSCRIPTION_LEASE_SECS": "0",
        "ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS": "0",
        "ELECTRIC_CIRCUITS_LOG_JSON": "1",
        "ELECTRIC_SECRET": SECRET,
        "ELECTRIC_STORAGE_DIR": str(output / "engine-data"),
    }
    command("sudo", "systemd-run", "--unit", unit, "--collect", "--property=CPUQuota=400%", f"--property=StandardOutput=append:{engine_log}", f"--property=StandardError=append:{engine_log}", "/usr/bin/env", *[f"{key}={value}" for key, value in env.items()], str(Path(args.engine).resolve()))
    manifest = {"command": " ".join(os.sys.argv), "harness_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(), "engine_base": "5b2cf3e2731d7fcecc3f95b9c05a5963b9c6b363", "engine_sha256": hashlib.file_digest(Path(args.engine).open("rb"), "sha256").hexdigest(), "indexed_commit": gateway.loader.bundle["commit"], "catalog": args.catalog, "scale": args.scale, "cpu_quota": args.cpus, "visible_cpus": os.cpu_count(), "environment": {key: value for key, value in env.items() if key != "ELECTRIC_SECRET"}, "uname": os.uname(), "target": TARGETS[args.catalog]}
    save(output / "manifest.json", manifest)
    try:
        async with httpx.AsyncClient(headers={"Authorization": f"Bearer {SECRET}"}, timeout=120) as client:
            await until_ready(client)
            pid = int(subprocess.check_output(["systemctl", "show", unit, "--property=MainPID", "--value"], text=True).strip())
            process = psutil.Process(pid)
            catalog = await gateway.generate(args.catalog, args.scale)
            if args.smoke:
                catalog = catalog[:20] + catalog[-20:]
            semaphore = asyncio.Semaphore(8)
            async def create(request):
                async with semaphore:
                    response = await client.post(f"{ENGINE_URL}/v1/subset-feeds", json=request["body"])
                    if response.status_code != 200:
                        raise RuntimeError(f"Shape creation failed: {response.status_code} {response.text[:2000]} table={request['body']['table']}")
                    return response.json()
            handles = await asyncio.gather(*(create(request) for request in catalog))
            save(output / "shape-handles.json", handles)
            for table in ("record", "record_date_index", "record_recurrence", "assignment", "calendar_event", "graph_edge", "resource"):
                await create({"body": await gateway.body(table, IMPORT_ACCOUNT, identity=f"import-{table}")})
            probe_body = await gateway.body("record", 0, identity="other-account-probe")
            probe_feed = await create({"body": probe_body})
            head = await client.head(probe_feed["streamUrl"])
            head.raise_for_status()
            probe_feed["offset"] = head.headers["stream-next-offset"]
            memory = await client.get(f"{ENGINE_URL}/memory")
            memory.raise_for_status()
            save(output / "catalog-before-warmup.json", memory.json())
            validator = gateway.gateway._is_drained_receipt
            fence_user = row_id("user", 1)
            fence_generation = gateway.authority(1).generation
            warm_marker = uuid4()
            def warm():
                with psycopg.connect(PG_URL) as connection:
                    for account in range(19):
                        data.update(connection, "record", account, 1 if args.smoke else 5 if args.catalog == "nov23" else 12)
                    marker(connection, fence_user, fence_generation, warm_marker)
            await asyncio.to_thread(warm)
            await wait_for_marker(client, validator, warm_marker, fence_user, fence_generation, 900)
            memory = await client.get(f"{ENGINE_URL}/memory")
            memory.raise_for_status()
            save(output / "catalog-calibration.json", {"target": TARGETS[args.catalog], "actual": memory.json(), "source_shape_requests": len(catalog), "smoke_only": args.smoke})
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
            for workload in ("import", "large-delete", "small-delete"):
                probe = OtherAccountProbe(engine_url=ENGINE_URL, postgres_url=PG_URL, user_id=row_id("user", 0), record_id=row_id("record", 0), generation=gateway.authority(0).generation, feed=probe_feed, subset=subset, receipt_validator=validator, output=output / f"{workload}-probes.jsonl")
                probe_task = asyncio.create_task(probe.run())
                await asyncio.sleep(3)
                fence_id = uuid4()
                def mark(connection, transaction):
                    current = uuid4() if workload == "import" and transaction != 5 else fence_id
                    marker(connection, fence_user, fence_generation, current)
                began_wall = time.time()
                began = time.monotonic()
                cpu_before = sum(process.cpu_times()[:2])
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
                result["engine_cpu_percent"] = (sum(process.cpu_times()[:2]) - cpu_before) / result["elapsed_s"] * 100
                result["changes_per_s"] = result.get("row_changes", 0) / result["elapsed_s"] if result["completed"] else None
                probe.stop()
                await probe_task
                result["probe"] = summarize([sample for sample in probe.samples if sample["at_s"] >= began - probe.started])
                events = read_events(engine_log, began_wall)
                result["backfills"] = sum(event.get("fields", {}).get("message") == "backfill finished" for event in events)
                result["backfills_per_minute"] = result["backfills"] * 60 / result["elapsed_s"]
                result["pinned_warnings"] = sum("replay position is pinned" in str(event.get("fields", {}).get("message", "")) for event in events)
                save(output / f"{workload}-result.json", result)
                print(json.dumps(result, default=str), flush=True)
                if not result["completed"]:
                    raise RuntimeError(f"{workload} did not complete; later workloads not contaminated with its backlog")
    finally:
        subprocess.run(["sudo", "systemctl", "stop", unit], timeout=120, check=False)
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
    asyncio.run(run(parser.parse_args()))
