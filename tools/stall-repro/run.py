from __future__ import annotations

import argparse
import concurrent.futures
import asyncio
import datetime
import hashlib
import json
import os
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path
from uuid import UUID, uuid4

import httpx
import psutil
import psycopg
from psycopg import sql

from catalog import RECORD_FAMILY, GatewayShapes, TARGETS, control_catalog, plain_control_body
from export_fixture import synthetic_id
from data import DELETE_ORDER, IMPORT_ACCOUNT, LARGE_ACCOUNT, SHARE_ACCOUNT, SHARE_TABLES, load_data, row_id
from flamegraph import attribute, fold, render
from probes import OtherAccountProbe, summarize
from simple_keys import EDGE_VARIANTS, H1_DATA_ACCOUNTS, H1P_VARIANTS, H1_VARIANTS, IMPORT_REST_TABLES, MAIN3_VARIANTS, edge_catalog, h1_body, h1_catalog, h1p_catalog, unwrap_single_or, SIMPLE_KEYED, SIMPLE_VARIANTS, account_lists, main3_bodies, main3_catalog, main3_keyed_lists, simple_bodies, simple_catalog, window_plan
from simple_keys import HOME_TODAY, W6_ACCESS_ACCOUNTS, W6_MOVED_EDGES, W6_VARIANTS, placement_summary, plain_words_account, predicate_sql, w6_catalog, w6_requests, window_subsets
from simple_keys import B2_TABLES, B2_VARIANTS, B2_WRITER, B2B_VARIANTS, b2_catalog, b2b_catalog
from simple_keys import W1_DELETED, W1_FAMILY_ROWS, W1_VARIANTS, w1_catalog, w1_feeds, w1_seed_accounts, w1_seed_families


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


# indexed `BulkWriteSettings` at 52e79b875b: a paced commit holds at most this many tracked rows
# and rows in all, and the writer waits for the engine's receipt before the next (`PacedCommits`).
PACED_TRACKED_ROWS = 10
PACED_TOTAL_ROWS = 500
RECEIPT_POLL_S = 0.05


def wait_for_receipt(http, validator, source_commit_id, user_id, generation, timeout=900):
    """Poll the engine's own drained-through receipt every 50 ms; return seconds until it arrived."""
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        try:
            response = http.get(f"{ENGINE_URL}/_runtime/drained-through/{source_commit_id}", params={"user_id": str(user_id), "generation": generation}, timeout=2)
            if response.status_code == 200 and validator(response.json(), marker=source_commit_id, user_id=user_id, generation=generation):
                return time.monotonic() - started
        except httpx.HTTPError:
            pass
        time.sleep(RECEIPT_POLL_S)
    raise TimeoutError(f"no receipt for {source_commit_id} in {timeout}s")


# One imported calendar event's tracked rows (about 6 per event, as indexed's import writes them);
# its untracked rows (sync state and the like) are not published and cost the engine nothing.
EVENT_TRACKED_ROWS = (("calendar_event", 1), ("record", 1), ("record_date_index", 1), ("assignment", 1), ("graph_edge", 2))
PACED_IMPORT_EVENTS = 1000


# Coverage audit (AUDIT-1.md) one-commit workloads. Each returns its journal; the caller's marker
# commit closes it.
COLD_TABLES = ("space_member", "file_share", "resource_share", "record_share", "suggestion", "tag")
COLD_ACCOUNT = 3
FLIP_SPACES = (4, 5, 6, 7, 8)
SUBTREE_SPACE, SUBTREE_TARGET, SUBTREE_MEMBERS = 5, 6, 30
FILES_ACCOUNT, FILES_DELETED = 6, 500
# `h1-commit-r2` and `-r3` repeat `h1-commit` on the same engine, for a spread and more bystander samples.
H1_COMMITS = ("h1-commit", "h1-commit-r2", "h1-commit-r3")
# PLAIN-WORDS-3 run 2: 1,000 `graph_edge` updates, 20 in each of the 50 written Spaces, and repeats.
EDGE_COMMITS = ("e1-commit", "e1-commit-r2", "e1-commit-r3", "e1-commit-r4", "e1-commit-r5")
# W6 steps between the timed commits of a list-rule cell (results/simple-keys/W6-LIST-KEYS.md):
# where shapes are filed, one engine restart, a round of access changes, and the final check of
# every shape's delivered rows against Postgres.
W6_STEPS = ("w6-placement", "w6-restart", "w6-placement-restarted", "w6-access", "w6-placement-access", "w6-check")
# W7: 100 one-shot subset queries that carry a subquery, with the engine's counts before, right
# after and W7_WAIT_S later (results/simple-keys/W7-SUBSET-RESIDUE.md).
W7_WORKLOADS = ("w7-subsets",)
# B2 (results/simple-keys/B2-SINGLE-KEY-PLACEMENT.md): `b2-commit-<table>` inserts 1,000 rows of one
# table for an account no reader holds (`-r2`, `-r3` repeat it); `b2-placement` is the step of a
# one-reader cell, whose counts are read while its filters are created.
B2_COMMITS = tuple(f"b2-commit-{table}{suffix}" for table in B2_TABLES for suffix in ("", "-r2", "-r3"))
B2_STEPS = ("b2-placement",)
B2_ROWS = 1000


def b2_add_columns(data, url):
    """Columns the literal filters read that the harness's pinned schema (indexed 52e79b8) lacks."""
    statements = []
    if not any(column["name"] == "resource_space_id" for column in data.columns["resource_fact"]):
        statement = "ALTER TABLE public.resource_fact ADD COLUMN resource_space_id uuid"
        with psycopg.connect(url) as connection:
            connection.execute(statement)
        data.columns["resource_fact"].append({"name": "resource_space_id", "type": "UUID", "nullable": True, "primary_key": False, "server_default": None, "enum": None, "foreign_keys": []})
        statements.append(statement)
    return statements


def b2_commit(data, workload):
    """1,000 new rows of one table, all in the writer account's Space and under its user."""
    name = workload.removeprefix("b2-commit-")
    repeat = {"-r2": 1, "-r3": 2}.get(name[-3:], 0)
    table = name[:-3] if repeat else name
    def write(connection):
        columns = [column["name"] for column in data.columns[table]]
        rows = []
        for ordinal in range(repeat * B2_ROWS, (repeat + 1) * B2_ROWS):
            row = data.make(table, B2_WRITER, ordinal)
            if table == "resource_fact":
                # A live fact is unique per (Resource, fact type): one Resource per fact.
                row |= {"resource_id": row_id("resource", B2_WRITER, ordinal), "resource_space_id": row_id("space", B2_WRITER)}
            rows.append(row)
        command = sql.SQL("COPY {} ({}) FROM STDIN").format(sql.Identifier("public", table), sql.SQL(",").join(map(sql.Identifier, columns)))
        with connection.cursor().copy(command) as copier:
            for row in rows:
                copier.write_row([row[column] for column in columns])
        return [{"table": table, "operation": "insert", "rows": len(rows)}]
    return write


async def table_counts(client, table):
    """What the engine says it holds on one table: `GET /tables/{table}/families` and `/memory`'s counts."""
    response = await client.get(f"{ENGINE_URL}/tables/public.{table}/families")
    counts = response.json() if response.status_code == 200 else {"families": [], "standalone": 0, "scan": 0, "circuit": 0, "no_tailer": True}
    cardinalities = (await client.get(f"{ENGINE_URL}/memory")).json()["cardinalities"]
    return {"table": counts, "engine": {key: cardinalities[key] for key in ("shapes", "standalone", "families", "family_shapes", "subquery_shapes", "subquery_nodes", "subquery_edges")}}

W7_QUERIES = 100
W7_WAIT_S = 120
W7_TABLES = ("record", "record_date_index", "record_recurrence", "assignment", "graph_edge", "calendar_event")


def engine_counts(http, process):
    """What the engine holds: shape counts from the catalog, each table's standalone index, the
    subquery registry, the `/metrics` gauges that count shapes, and its memory."""
    memory = http.get(f"{ENGINE_URL}/memory").json()
    tables = {}
    for table in W7_TABLES:
        response = http.get(f"{ENGINE_URL}/tables/public.{table}/families")
        tables[table] = response.json() if response.status_code == 200 else {"status": response.status_code, "body": response.text[:200]}
    metrics = http.get(f"{ENGINE_URL}/metrics").json()
    gauges = metrics.get("gauges", metrics)
    subqueries = http.get(f"{ENGINE_URL}/subqueries")
    return {"at": time.time(), "cardinalities": memory.get("cardinalities"), "tables": tables, "metrics": {key: value for key, value in gauges.items() if isinstance(value, (int, float)) and any(word in key for word in ("shape", "subquer", "circuit", "standalone", "famil", "tailer", "node"))},
            "subqueries": subqueries.json() if subqueries.status_code == 200 else subqueries.text[:200], "rss_bytes": process.memory_info().rss}


def w7_operation(queries, process):
    def operation(url, callback):
        with httpx.Client(headers={"Authorization": f"Bearer {SECRET}"}, timeout=120) as http:
            before = engine_counts(http, process)
            answers = []
            for body in queries:
                response = http.post(f"{ENGINE_URL}/v1/subsets/query", json=body)
                answers.append({"table": body["table"], "status": response.status_code, "rows": len(response.json()["rows"]) if response.status_code == 200 else None, "error": None if response.status_code == 200 else response.text[:300]})
            after = engine_counts(http, process)
            time.sleep(W7_WAIT_S)
            later = engine_counts(http, process)
        with psycopg.connect(url) as connection:
            callback(connection, "final")
        return [{"table": "w7", "operation": "subset-queries", "rows": sum(answer["rows"] or 0 for answer in answers), "answers": answers, "before": before, "after": after, "after_wait": later, "wait_s": W7_WAIT_S}]
    return operation

AUDIT_WORKLOADS = (*H1_COMMITS, *EDGE_COMMITS, *B2_COMMITS, *(f"cold-{table}" for table in COLD_TABLES), "flip-add", "flip-remove", "import-noresource", "writer-subtree", "writer-user", "writer-merge", "writer-files")


# PLAIN-WORDS-3 run 3: window loads, each way on its own engine. `window-<kind>-<way>` loads every
# planned window of that kind (`simple_keys.window_plan`) one after another: `bundle` as one group
# load, `seq` as a feed and a query per member in turn, `par` as the six members at once.
WINDOW_WORKLOADS = tuple(f"window-{kind}-{way}" for kind in ("day", "week") for way in ("bundle", "seq", "par"))
IDLE_WORKLOADS = ("idle-60",)
WINDOW_GAP_S = 0.5
HARNESS_STATEMENTS = ("pg_stat_statements", "native_sync_authority_fence", "UPDATE public.record SET name", "pg_current_xact_id", "SET LOCAL session_replication_role")


def cgroup_cpu_s(cgroup):
    return next(int(line.split()[1]) for line in (cgroup / "cpu.stat").read_text().splitlines() if line.startswith("usage_usec")) / 1e6


def window_member(http, feed, subset):
    """One member table by table: its changes-only feed, then its rows."""
    started = time.monotonic()
    created = http.post(f"{ENGINE_URL}/v1/subset-feeds", json=feed)
    fed = time.monotonic()
    queried = http.post(f"{ENGINE_URL}/v1/subsets/query", json=subset)
    step = {"table": subset["table"], "feed_s": fed - started, "query_s": time.monotonic() - fed, "feed_status": created.status_code, "query_status": queried.status_code}
    if created.status_code != 200:
        step["feed_error"] = created.text[:500]
    if queried.status_code == 200:
        step["rows"] = len(queried.json()["rows"])
    else:
        step["query_error"] = queried.text[:500]
    return step


def window_operation(plan, kind, way, cgroup):
    def operation(url, callback):
        journal = []
        with httpx.Client(headers={"Authorization": f"Bearer {SECRET}"}, timeout=120) as http, psycopg.connect(url, autocommit=True) as statistics, concurrent.futures.ThreadPoolExecutor(6) as pool:
            def shapes():
                return (http.get(f"{ENGINE_URL}/memory").json()).get("cardinalities", {})
            try:
                statistics.execute("SELECT pg_stat_statements_reset()")
                statements_error = None
            except psycopg.Error as error:
                statements_error = repr(error)
            for load in (entry for entry in plan if entry["kind"] == kind):
                requests = load["requests"]
                before = shapes()
                cpu_before = cgroup_cpu_s(cgroup)
                started = time.monotonic()
                entry = {"table": "window", "operation": way, "account": load["account"], "start": load["start"], "size": load["size"], "expected_rows": load["expected_rows"]}
                if way == "bundle":
                    response = http.post(f"{ENGINE_URL}/v1/subsets/load", json=requests["bundle"])
                    entry["wall_s"] = time.monotonic() - started
                    entry["status"] = response.status_code
                    if response.status_code == 200:
                        entry["rows_by_table"] = dict(Counter(row["table"] for row in response.json()["rows"]))
                    else:
                        entry["error"] = response.text[:500]
                else:
                    members = list(zip(requests["feeds"], requests["subsets"]))
                    steps = [window_member(http, feed, subset) for feed, subset in members] if way == "seq" else list(pool.map(lambda member: window_member(http, *member), members))
                    entry["wall_s"] = time.monotonic() - started
                    entry["steps"] = steps
                    entry["status"] = max(max(step["feed_status"], step["query_status"]) for step in steps)
                    entry["rows_by_table"] = {step["table"]: step.get("rows", 0) for step in steps}
                entry["engine_cpu_s"] = cgroup_cpu_s(cgroup) - cpu_before
                after = shapes()
                entry["created"] = {key: after[key] - before.get(key, 0) for key in after if isinstance(after[key], (int, float)) and after[key] != before.get(key, 0)}
                entry["rows"] = sum(entry.get("rows_by_table", {}).values())
                journal.append(entry)
                time.sleep(WINDOW_GAP_S)
            summary = {"table": "pg_stat_statements", "operation": "summary", "rows": 0, "error": statements_error}
            if statements_error is None:
                rows = statistics.execute("SELECT left(query, 300), calls, total_exec_time, rows FROM pg_stat_statements ORDER BY total_exec_time DESC").fetchall()
                summary["statements"] = [{"query": query, "calls": calls, "total_ms": total, "rows": returned, "harness": any(text in query for text in HARNESS_STATEMENTS)} for query, calls, total, returned in rows]
            journal.append(summary)
        with psycopg.connect(url) as connection:
            callback(connection, "final")
        return journal
    return operation


# W1, the target design under one combined load (results/simple-keys/W1-TARGET-DESIGN.md).
# `w1-control`: only the active families' steady loads, for W1_CONTROL_S seconds.
# `w1-load`: the same loads, and at the same time one family's first import (W1_IMPORT_EVENTS events,
# W1_IMPORT_COMMIT a commit, paced on the engine's receipt) and one prod-p95 family deleted at the
# shipped bulk-write limits (PACED_TRACKED_ROWS a commit, engine-paced).
W1_WORKLOADS = ("w1-control", "w1-load")
W1_CONTROL_S = 120
W1_IMPORT_EVENTS = 5000
W1_IMPORT_COMMIT = 100
W1_SETTLE_S = 5
# The importing family's rows carry the fixture's dates (2026-09-01 plus ordinal % 84 days); a phone
# on Home's day and the calendar's week would follow these days of them.
W1_WINDOW_DAY = 14
W1_WINDOW_WEEK = range(14, 21)


def w1_seed(data, url, families):
    with psycopg.connect(url) as connection:
        connection.execute("SET LOCAL session_replication_role = replica")
        accounts = w1_seed_accounts(connection, data)
        generated = w1_seed_families(connection, data, families)
    with psycopg.connect(url, autocommit=True) as connection:
        # The target design reads by key, so each table a load reads has an index on it (prod's tables
        # index `space_id` the same way); the date index adds its date for window reads.
        for table in W1_FAMILY_ROWS:
            connection.execute(sql.SQL("CREATE INDEX ON {} (audience_key)").format(sql.Identifier("public", table)))
        connection.execute("CREATE INDEX ON public.record_date_index (audience_key, date_value)")
        connection.execute("ANALYZE")
    return {"tables": accounts, "families": families, "family_rows": generated}


def receipt_summary(receipts):
    ordered = sorted(receipts)
    if not ordered:
        return {"commits": 0}
    return {"commits": len(ordered), "p50_s": ordered[len(ordered) // 2], "p95_s": ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))], "max_s": ordered[-1], "sum_s": sum(ordered)}


def w1_operation(data, workload, families, output, validator, fences, transactions):
    """Start the load generator, run the phase, stop the generator; the caller's marker closes it."""
    folder = Path(__file__).parent
    def operation(url, callback):
        stop = output / f"{workload}-loads.stop"
        shards = 2 if families >= 1000 else 1
        generators = [subprocess.Popen([sys.executable, str(folder / "w1_loads.py"), "--families", str(families), "--shard", str(shard), "--shards", str(shards), "--out", str(output / f"{workload}-loads-{shard}.jsonl"), "--stop", str(stop), "--seed", str(shard + 1)]) for shard in range(shards)]
        journal = []
        try:
            if workload == "w1-control":
                time.sleep(W1_CONTROL_S)
            else:
                time.sleep(W1_SETTLE_S)
                import_receipts, delete_receipts = [], []
                def timed(name, write, receipts):
                    began_wall, began = time.time(), time.monotonic()
                    entries = write(receipts)
                    return entries, {"table": "w1", "operation": name, "rows": 0, "began_wall": began_wall, "wall_s": time.monotonic() - began, "receipts": receipt_summary(receipts), "receipt_s": receipts}
                (fence_user, fence_generation), (delete_user, delete_generation) = fences
                with concurrent.futures.ThreadPoolExecutor(2) as pool:
                    imported = pool.submit(timed, "import", lambda receipts: paced_first_import(data, url, W1_IMPORT_COMMIT, fence_user, fence_generation, validator, transactions, receipts, events=W1_IMPORT_EVENTS), import_receipts)
                    deleted = pool.submit(timed, "delete", lambda receipts: paced_delete_account(data, url, W1_DELETED, delete_user, delete_generation, validator, transactions, receipts, tables=["file", *DELETE_ORDER]), delete_receipts)
                    for future in (imported, deleted):
                        entries, summary = future.result()
                        journal += entries + [summary]
                time.sleep(W1_SETTLE_S)
        finally:
            stop.touch()
            for generator in generators:
                try:
                    generator.wait(timeout=40)
                except subprocess.TimeoutExpired:
                    generator.kill()
        with psycopg.connect(url) as connection:
            callback(connection, "final")
        return journal
    return operation


def loads_summary(output, workload):
    """The generator's loads by kind: how many were due, answered, shed or timed out, and how long they took."""
    samples = [json.loads(line) for path in sorted(output.glob(f"{workload}-loads-*.jsonl")) for line in path.read_text().splitlines()]
    summary = {}
    for kind in ("day", "week", "detail", "all"):
        chosen = [sample for sample in samples if kind in (sample["kind"], "all")]
        sent = [sample for sample in chosen if not sample.get("shed")]
        walls = sorted(sample["wall_s"] for sample in sent)
        lags = sorted(sample["lag_s"] for sample in chosen)
        def at(values, fraction):
            return values[int((len(values) - 1) * fraction)] if values else None
        summary[kind] = {"due": len(chosen), "sent": len(sent), "ok": sum(sample["ok"] for sample in sent), "shed": len(chosen) - len(sent), "timed_out": sum(bool(sample.get("timeouts")) for sample in sent), "errors": sum(bool(sample.get("errors")) for sample in sent),
                         "wall_p50_s": at(walls, 0.5), "wall_p95_s": at(walls, 0.95), "wall_p99_s": at(walls, 0.99), "wall_max_s": walls[-1] if walls else None, "lag_p95_s": at(lags, 0.95), "lag_max_s": lags[-1] if lags else None,
                         "rows_p50": at(sorted(sample.get("rows", 0) for sample in sent), 0.5), "rows_p95": at(sorted(sample.get("rows", 0) for sample in sent), 0.95), "bytes": sum(sample.get("bytes", 0) for sample in sent), "requests": sum({"day": 6, "week": 6, "detail": 5}[sample["kind"]] for sample in sent)}
    if samples:
        summary["span_s"] = max(sample["at"] for sample in samples) - min(sample["at"] for sample in samples)
    return summary


async def stream_offset(client, handle):
    head = await client.head(handle["streamUrl"])
    head.raise_for_status()
    return head.headers["stream-next-offset"]


async def stream_volume(client, handle, offset, ordinal_of=None):
    """Everything one feed delivered after `offset`: rows and bytes, and (with `ordinal_of`) the share
    a phone following only a day and a week window of the same rows would have been sent."""
    volume = {"rows": 0, "bytes": 0}
    windows = {"day": {"rows": 0, "bytes": 0}, "week": {"rows": 0, "bytes": 0}, "unplaced_rows": 0}
    for _ in range(100000):
        response = await client.get(handle["streamUrl"], params={"offset": offset}, timeout=120)
        if response.status_code == 204:
            break
        response.raise_for_status()
        envelopes = response.json()
        if not envelopes:
            break
        volume["rows"] += len(envelopes)
        volume["bytes"] += len(response.content)
        volume.setdefault("sample", json.dumps(envelopes[0])[:400])
        if ordinal_of is not None:
            for envelope in envelopes:
                ordinal = ordinal_of(envelope)
                if ordinal is None:
                    windows["unplaced_rows"] += 1
                    continue
                size = len(json.dumps(envelope, separators=(",", ":")))
                if ordinal % 84 == W1_WINDOW_DAY:
                    windows["day"]["rows"] += 1
                    windows["day"]["bytes"] += size
                if ordinal % 84 in W1_WINDOW_WEEK:
                    windows["week"]["rows"] += 1
                    windows["week"]["bytes"] += size
        following = response.headers.get("stream-next-offset", offset)
        if following == offset:
            break
        offset = following
    if ordinal_of is not None:
        volume["windows"] = windows
    return volume


def w1_ordinals(data, account, table):
    """Map one of an account's delivered rows to the ordinal its date comes from, or None."""
    column = {"record": "id", "record_date_index": "record_id", "assignment": "record_id", "graph_edge": "from_id", "calendar_event": "id", "record_recurrence": "record_id"}.get(table)
    if column is None:
        return lambda envelope: None
    source = "calendar_event" if table == "calendar_event" else "record"
    known = {}
    def ordinal_of(envelope):
        value = envelope.get("value") if isinstance(envelope.get("value"), dict) else {}
        identifier = value.get(column)
        if identifier is None:
            # A delete may carry only its key: look for one of the account's ids in it.
            text = json.dumps(envelope.get("key", ""))
            identifier = next((part for part in text.replace('"', " ").replace("/", " ").split() if len(part) == 36 and part.count("-") == 4), None)
        if identifier is None:
            return None
        if not known:
            # Enough ordinals for the import's rows and the deleted family's.
            known.update({str(row_id(source, account, ordinal)): ordinal for ordinal in range(12000)})
        return known.get(str(identifier))
    return ordinal_of


async def follower_volumes(client, data, followers, offsets):
    result = {}
    for name, (account, handles) in followers.items():
        tables = {}
        for table, handle in handles:
            tables[table] = await stream_volume(client, handle, offsets[name][table], w1_ordinals(data, account, table) if name != "bystander" else None)
        total = {"rows": sum(entry["rows"] for entry in tables.values()), "bytes": sum(entry["bytes"] for entry in tables.values())}
        for window in ("day", "week"):
            total[window] = {key: sum(entry.get("windows", {}).get(window, {}).get(key, 0) for entry in tables.values()) for key in ("rows", "bytes")}
        total["unplaced_rows"] = sum(entry.get("windows", {}).get("unplaced_rows", 0) for entry in tables.values())
        result[name] = {"account": account, "total": total, "tables": {table: entry for table, entry in tables.items() if entry["rows"]}}
    return result


def audit_prepare(data, url, variant, workloads):
    """Rows the audit workloads act on, written before the engine starts (part of the initial snapshot)."""
    with psycopg.connect(url) as connection:
        connection.execute("SET LOCAL session_replication_role = replica")
        if variant in H1_LIKE:
            for account in H1_DATA_ACCOUNTS:
                if account >= 20:
                    data.insert(connection, "graph_edge" if variant in EDGE_VARIANTS or variant in W6_VARIANTS else "record", account, 20)
        if "writer-subtree" in workloads:
            for index in range(SUBTREE_MEMBERS):
                connection.execute("INSERT INTO public.space_member (id, space_id, user_id, created_at, updated_at) VALUES (%s, %s, %s, now(), now())", (synthetic_id(f"subtree/{index}"), row_id("space", SUBTREE_SPACE), synthetic_id(f"subtree-user/{index}")))
        if "writer-files" in workloads:
            data.insert(connection, "file", FILES_ACCOUNT, FILES_DELETED)
        if any(workload in WINDOW_WORKLOADS for workload in workloads):
            # The fixture's calendar events carry no time; give each an hour on its creation day so a
            # window's `calendar_event` member returns rows.
            connection.execute("UPDATE public.calendar_event SET start_datetime = created_at + interval '9 hours', end_datetime = created_at + interval '10 hours'")
    if any(workload in WINDOW_WORKLOADS for workload in workloads):
        try:
            with psycopg.connect(url, autocommit=True) as connection:
                connection.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        except psycopg.Error as error:
            print(f"pg_stat_statements unavailable: {error!r}", flush=True)


def audit_operation(data, workload):
    """The one commit an audit workload makes, as (connection) -> journal."""
    if workload in H1_COMMITS:
        # 1,000 Record updates, 20 in each of the 50 Spaces that hold Records.
        return lambda connection: [{"table": "record", "operation": "update", "rows": data.update(connection, "record", account, 20)} for account in H1_DATA_ACCOUNTS]
    if workload in B2_COMMITS:
        return b2_commit(data, workload)
    if workload in EDGE_COMMITS:
        return lambda connection: [{"table": "graph_edge", "operation": "update", "rows": data.update(connection, "graph_edge", account, 20)} for account in H1_DATA_ACCOUNTS]
    if workload == "cold-resource_share":
        # One Resource per share: the table's active-provenance key (Resource, target, source, grant;
        # NULLs not distinct) allows one live direct share per Resource and target.
        def shares(connection):
            columns = [column["name"] for column in data.columns["resource_share"]]
            rows = [data.make("resource_share", COLD_ACCOUNT, 100 + ordinal) | {"resource_id": row_id("resource", COLD_ACCOUNT, 100 + ordinal)} for ordinal in range(1000)]
            command = sql.SQL("COPY {} ({}) FROM STDIN").format(sql.Identifier("public", "resource_share"), sql.SQL(",").join(map(sql.Identifier, columns)))
            with connection.cursor().copy(command) as copier:
                for row in rows:
                    copier.write_row([row[column] for column in columns])
            return [{"table": "resource_share", "operation": "insert", "rows": len(rows)}]
        return shares
    if workload.startswith("cold-"):
        table = workload.removeprefix("cold-")
        return lambda connection: [{"table": table, "operation": "insert", "rows": data.insert(connection, table, COLD_ACCOUNT, 1000)}]
    if workload == "flip-add":
        def add(connection):
            for space in FLIP_SPACES:
                connection.execute("INSERT INTO public.space_member (id, space_id, user_id, created_at, updated_at) VALUES (%s, %s, %s, now(), now())", (synthetic_id(f"flip/{space}"), row_id("space", space), row_id("user", COLD_ACCOUNT)))
            return [{"table": "space_member", "operation": "insert", "rows": len(FLIP_SPACES)}]
        return add
    if workload == "flip-remove":
        def remove(connection):
            changed = connection.execute("DELETE FROM public.space_member WHERE id = ANY(%s)", ([synthetic_id(f"flip/{space}") for space in FLIP_SPACES],)).rowcount
            return [{"table": "space_member", "operation": "delete", "rows": changed}]
        return remove
    if workload == "writer-subtree":
        def subtree(connection):
            changed = connection.execute("UPDATE public.space_member SET space_id = %s, updated_at = now() WHERE id = ANY(%s)", (row_id("space", SUBTREE_TARGET), [synthetic_id(f"subtree/{index}") for index in range(SUBTREE_MEMBERS)])).rowcount
            return [{"table": "space_member", "operation": "update", "rows": changed}]
        return subtree
    if workload == "writer-user":
        return lambda connection: [{"table": "user", "operation": "update", "rows": connection.execute('UPDATE public."user" SET updated_at = now()').rowcount}]
    if workload == "writer-merge":
        def merge(connection):
            target = row_id("resource", 5, 0)
            superseded = connection.execute("UPDATE public.resource SET superseded_by_resource_id = %s, updated_at = now() WHERE id = ANY(%s)", (target, [row_id("resource", 5, ordinal) for ordinal in range(1, 100)])).rowcount
            # A live fact is unique per (Resource, fact type), so a merge absorbs the moved facts:
            # each moves to the target and is soft-deleted in the same update.
            facts = connection.execute("UPDATE public.resource_fact SET resource_id = %s, deleted_at = now(), updated_at = now() WHERE resource_id = ANY(%s)", (target, [row_id("resource", account, ordinal) for account in (5, 6) for ordinal in range(100) if (account, ordinal) != (5, 0)])).rowcount
            return [{"table": "resource", "operation": "update", "rows": superseded}, {"table": "resource_fact", "operation": "update", "rows": facts}]
        return merge
    if workload == "writer-files":
        def files(connection):
            start = len(data.rows[("file", FILES_ACCOUNT)]) - FILES_DELETED
            ids = [row[ "id"] for row in data.rows[("file", FILES_ACCOUNT)][start:]]
            return [{"table": "file", "operation": "update", "rows": connection.execute("UPDATE public.file SET deleted_at = now(), updated_at = now() WHERE id = ANY(%s)", (ids,)).rowcount}]
        return files
    raise ValueError(workload)


def import_without_resources(data, url, marker_callback):
    """`SyntheticData.calendar_import` without its 40 `resource` rows per commit."""
    from data import IMPORT_TRANSACTIONS
    journal = []
    for transaction, target in enumerate(IMPORT_TRANSACTIONS):
        with psycopg.connect(url) as connection:
            connection.execute("SET LOCAL session_replication_role = replica")
            operations = [("record", "insert", 350), ("record", "update", 250), ("record_date_index", "insert", 300), ("record_date_index", "delete", 100), ("record_recurrence", "insert", 200), ("record_recurrence", "update", 200), ("record_recurrence", "delete", 100), ("assignment", "insert", 200), ("calendar_event", "insert", 100), ("calendar_event", "update", 50), ("calendar", "update", 75), ("resource", "insert", 20), ("resource", "update", 20)]
            if transaction == 0:
                operations.insert(0, ("calendar", "insert", 15))
            operations.append(("graph_edge", "insert", target - sum(count for _, _, count in operations)))
            for table, operation, count in operations:
                if table == "resource":
                    continue
                journal.append({"transaction": transaction, "table": table, "operation": operation, "rows": getattr(data, operation)(connection, table, IMPORT_ACCOUNT, count)})
            marker_callback(connection, transaction)
    return journal


def paced_first_import(data, url, events_per_commit, fence_user, fence_generation, validator, transactions, receipts, events=PACED_IMPORT_EVENTS):
    """A first import paced as #8955 would: commits of `events_per_commit` events, each followed by the engine's receipt."""
    journal = []
    with httpx.Client(headers={"Authorization": f"Bearer {SECRET}"}) as http:
        for first in range(0, events, events_per_commit):
            count = min(events_per_commit, events - first)
            with psycopg.connect(url) as connection:
                connection.execute("SET LOCAL session_replication_role = replica")
                for table, per_event in EVENT_TRACKED_ROWS:
                    journal.append({"table": table, "operation": "insert", "rows": data.insert(connection, table, IMPORT_ACCOUNT, count * per_event), "commit": len(receipts)})
                current = uuid4()
                marker(connection, fence_user, fence_generation, current)
                transactions.append({"transaction": f"paced-import-{len(receipts)}", "txid": connection.execute("SELECT pg_current_xact_id()::text").fetchone()[0], "before_commit": time.time(), "marker": str(current)})
            receipts.append(wait_for_receipt(http, validator, current, fence_user, fence_generation))
    return journal


def paced_delete_account(data, url, account, fence_user, fence_generation, validator, transactions, receipts, tables=DELETE_ORDER):
    """Delete an account's rows as a paced bulk write: bounded commits, each followed by the engine's receipt."""
    pending = [(table, len(data.rows.get((table, account), [])) if table != "user" else min(1, len(data.rows.get((table, account), [])))) for table in tables]
    journal = []
    with httpx.Client(headers={"Authorization": f"Bearer {SECRET}"}) as http:
        while any(count for _, count in pending):
            with psycopg.connect(url) as connection:
                connection.execute("SET LOCAL session_replication_role = replica")
                budget = min(PACED_TRACKED_ROWS, PACED_TOTAL_ROWS)
                for index, (table, count) in enumerate(pending):
                    take = min(count, budget)
                    if take:
                        journal.append({"table": table, "operation": "delete", "rows": data.delete(connection, table, account, take), "commit": len(receipts)})
                        pending[index] = (table, count - take)
                        budget -= take
                    if not budget:
                        break
                current = uuid4()
                marker(connection, fence_user, fence_generation, current)
                transactions.append({"transaction": f"paced-{len(receipts)}", "txid": connection.execute("SELECT pg_current_xact_id()::text").fetchone()[0], "before_commit": time.time(), "marker": str(current)})
            receipts.append(wait_for_receipt(http, validator, current, fence_user, fence_generation))
    return journal


async def wait_for_marker(client, validator, source_commit_id, user_id, generation, timeout, process=None, poll=0.2):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        if process is not None and not process.is_running():
            raise RuntimeError("Engine exited while workload effects were outstanding; inspect engine.log and cgroup evidence")
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
        await asyncio.sleep(poll)
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


async def sample_metrics(client, output, stopped):
    """Sample the engine's sequencer-hold gauges once a second; return the peak bytes and the longest held run."""
    peak, longest, run_started = 0, 0.0, None
    with output.open("w") as handle:
        while not stopped.is_set():
            try:
                body = (await client.get(f"{ENGINE_URL}/metrics", timeout=2)).json()
                gauges = body.get("gauges", body)
                held_run = gauges.get("sequencer_held_run", 0)
                held_bytes = gauges.get("sequencer_held_bytes", gauges.get("sequencer_held", 0))
                handle.write(json.dumps({"at": time.time(), "sequencer_held_run": held_run, "sequencer_held_bytes": held_bytes}) + "\n")
                peak = max(peak, held_bytes or 0)
                now = time.monotonic()
                if held_run:
                    run_started = run_started or now
                    longest = max(longest, now - run_started)
                else:
                    run_started = None
            except (httpx.HTTPError, ValueError):
                pass
            try:
                await asyncio.wait_for(stopped.wait(), 1)
            except TimeoutError:
                pass
    return {"peak_held_bytes": peak, "longest_held_run_s": longest, "sampled_every_s": 1}


async def sample_cpu(process, cgroup, output, stopped):
    with output.open("w") as handle:
        while not stopped.is_set():
            try:
                sample = {"at": time.time(), "cpu_s": sum(process.cpu_times()[:2]), "rss_bytes": process.memory_info().rss, "threads": process.num_threads(), "cpu_stat": (cgroup / "cpu.stat").read_text(), "machine_cpu_percent": psutil.cpu_percent(None), "load_1m": os.getloadavg()[0]}
            except (psutil.NoSuchProcess, FileNotFoundError):
                break
            handle.write(json.dumps(sample) + "\n")
            handle.flush()
            try:
                await asyncio.wait_for(stopped.wait(), timeout=1)
            except TimeoutError:
                pass


RETAINED_CLASSES = {"chat": {"message", "workflow_run"}, "edge": {"graph_edge"}, "resource": {"resource", "resource_fact"}, "file": {"file"}, "user": {"user"}}
KEYED_VARIANTS = {"B", "B-retained", *(f"B-retained-key-{name}" for name in RETAINED_CLASSES)}
# E-only / E-plus-R: today's catalog (A) with only these tables keyed as B keys them.
PARTLY_KEYED = {"E-only": {"graph_edge"}, "E-plus-R": {"graph_edge", *RECORD_FAMILY}}
# A-edge-*: today's catalog with one graph_edge authorization branch keyed (or the synthetic date window dropped).
EDGE_ATTRIBUTION = {"A-edge-key-attendee": "attendee", "A-edge-key-generated": "generated", "A-edge-key-resource": "resource", "A-edge-no-window": "no-window"}
PARTLY_KEYED.update({variant: {"graph_edge"} for variant, mode in EDGE_ATTRIBUTION.items() if mode != "no-window"})
# B-cold-key-edges: B-cold with every graph_edge branch keyed, to attribute B-cold's remaining cost.
COLD_VARIANTS = {"B-cold", "B-cold-key-edges"}
AUDIENCE_VARIANTS = KEYED_VARIANTS | COLD_VARIANTS | set(PARTLY_KEYED)
# The catalogs made of one table's keyed filters and the bystander's probe feed only: H1 and
# PLAIN-WORDS-3 run 1 (H1 with the hypothetical plain-test fix) and run 2 (keyed `graph_edge` filters).
H1_LIKE = set(H1_VARIANTS) | set(H1P_VARIANTS) | set(EDGE_VARIANTS) | set(W6_VARIANTS) | set(B2_VARIANTS) | set(B2B_VARIANTS)
# B2 and B2b cells with one reader: the counts cells.
B2_ONE_READER = {variant: flag for variant, (flag, readers) in B2_VARIANTS.items() if readers == 1} | {variant: form for variant, (form, _, readers) in B2B_VARIANTS.items() if readers == 1}
FIXED_VARIANTS = {variant for variant, (_, _, fixed) in H1P_VARIANTS.items() if fixed} | {variant for variant, (_, way) in EDGE_VARIANTS.items() if way != "today"}
ALL_VARIANTS = AUDIENCE_VARIANTS | set(EDGE_ATTRIBUTION) | SIMPLE_VARIANTS | MAIN3_VARIANTS | H1_LIKE | set(W1_VARIANTS)
BRIDGE_ENVIRONMENT = {
    "marker-bypass": {"STALL_REPRO_UNSAFE_SKIP_MARKER_WAIT": "1"},
    "commit-membership": {"STALL_REPRO_COMMIT_MEMBERSHIP": "1"},
    "commit-membership-slices": {"STALL_REPRO_COMMIT_MEMBERSHIP": "1", "STALL_REPRO_LOCK_SLICE_ROWS": "256"},
    "yield-rows": {"STALL_REPRO_SEQUENCER_YIELD": "1"},
    "sampler-off": {"STALL_REPRO_MEM_SAMPLER_MS": "0"},
    "sampler-10x": {"STALL_REPRO_MEM_SAMPLER_MS": "5000"},
    "bridge-control": {},
}


def keyed(variant, table):
    """Whether this variant reads `table` through B's audience-key equality shapes."""
    if variant in EDGE_ATTRIBUTION:
        return False
    if variant in PARTLY_KEYED:
        return table in PARTLY_KEYED[variant]
    return variant in KEYED_VARIANTS and table not in preserved_tables(variant)


def preserved_tables(variant):
    """Tables B-retained keeps on their original gateway predicates; a key-<class> variant keys that one class."""
    if variant == "B":
        return set()
    keyed = variant.removeprefix("B-retained-key-") if variant.startswith("B-retained-key-") else None
    return set().union(*(tables for name, tables in RETAINED_CLASSES.items() if name != keyed))


async def run(args):
    if os.environ.get("GITHUB_ACTIONS") != "true" and os.environ.get("STALL_REPRO_REMOTE_MACHINE") != "1":
        raise RuntimeError("Run only on the authorized isolated remote machine or GitHub Actions, never in a Mighty Run sandbox")
    folder = Path(__file__).parent.resolve()
    output = folder / "results" / args.name
    output.mkdir(parents=True, exist_ok=False)
    fixture = folder / "results/fixture"
    gateway = GatewayShapes(folder / "indexed-source.json.gz")
    data = load_data(fixture / "columns.json")
    if args.variant in AUDIENCE_VARIANTS or args.variant in W1_VARIANTS:
        data.family_keys = args.variant in W1_VARIANTS
        published = [table.removeprefix("public.") for table in gateway.loader.bundle["publication"]]
        statements = await asyncio.to_thread(data.add_audience_columns, PG_URL, [table for table in published if args.variant not in PARTLY_KEYED or table in PARTLY_KEYED[args.variant]])
        (output / "audience-schema.sql").write_text(";\n".join(statements) + ";\n")
    if args.variant in SIMPLE_VARIANTS:
        statements = await asyncio.to_thread(data.add_assignment_space, PG_URL)
        if args.variant != "S-rest-today":
            # The tables S does not change keep B-cold's shapes, which read B-cold's columns.
            published = [table.removeprefix("public.") for table in gateway.loader.bundle["publication"]]
            statements += await asyncio.to_thread(data.add_audience_columns, PG_URL, [table for table in published if table not in SIMPLE_KEYED])
        (output / "audience-schema.sql").write_text(";\n".join(statements) + ";\n")
    if args.variant in B2_VARIANTS:
        (output / "audience-schema.sql").write_text(";\n".join(await asyncio.to_thread(b2_add_columns, data, PG_URL)) + ";\n")
    help_probe = subprocess.run([str(Path(args.store).resolve()), "--help"], capture_output=True, text=True)
    (output / "store-help.txt").write_text(help_probe.stdout + help_probe.stderr)
    supports_page_cap = b"--max-chunk-bytes" in Path(args.store).read_bytes()
    store_arguments = [str(Path(args.store).resolve()), "--host", "127.0.0.1", "--port", "8791", "--data-dir", str(output / "store-data"), "--durability", "wal"]
    if supports_page_cap:
        store_arguments.extend(["--max-chunk-bytes", "4194304"])
    save(output / "store-config.json", {"command": store_arguments, "sha256": hashlib.file_digest(Path(args.store).open("rb"), "sha256").hexdigest(), "matches_indexed_4mib_page_setting": supports_page_cap, "cap_detection": "option literal in pinned binary, then validated by server startup", "help_exit": help_probe.returncode})
    print("Seeding synthetic database", flush=True)
    if args.variant in W1_VARIANTS:
        save(output / "seed.json", await asyncio.to_thread(w1_seed, data, PG_URL, W1_VARIANTS[args.variant]))
    else:
        save(output / "seed.json", await asyncio.to_thread(data.seed, PG_URL, args.variant in COLD_VARIANTS | SIMPLE_VARIANTS | MAIN3_VARIANTS or "share-delete" in args.workloads))
    await asyncio.to_thread(audit_prepare, data, PG_URL, args.variant, args.workloads)
    lists = {}
    keyed_lists = {}
    if args.variant in SIMPLE_VARIANTS | MAIN3_VARIANTS:
        with psycopg.connect(PG_URL) as connection:
            lists = {account: account_lists(connection, gateway, account) for account in range(IMPORT_ACCOUNT + 1)}
            if args.variant in MAIN3_VARIANTS:
                keyed_lists = {account: main3_keyed_lists(connection, gateway, entry) for account, entry in lists.items()}
        save(output / "account-lists.json", {account: vars(entry) for account, entry in lists.items()})
    store_log = (output / "store.log").open("w")
    store = subprocess.Popen(store_arguments, stdout=store_log, stderr=subprocess.STDOUT)
    if args.variant in W1_VARIANTS:
        # The file-backed store holds a file per stream; 2,000 families are about 76,000 streams, more
        # than a hosted runner's 65,536 open files.
        raised = subprocess.run(["sudo", "prlimit", f"--pid={store.pid}", "--nofile=1048576:1048576"], capture_output=True, text=True)
        print(f"store open-file limit raised: exit {raised.returncode} {raised.stderr.strip()}", flush=True)
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
        # H1 measures up to 20,000 shapes plus the probe, so its cap sits above that.
        "ELECTRIC_CIRCUITS_MAX_SHAPES": "100000" if args.variant in W1_VARIANTS else "25000" if args.variant in H1_LIKE else "20000",
        "ELECTRIC_CIRCUITS_FLIP_WORKERS": "8",
        "ELECTRIC_CIRCUITS_SUBSCRIPTION_LEASE_SECS": "0",
        "ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS": "0",
        "ELECTRIC_CIRCUITS_LOG_JSON": "1",
        "ELECTRIC_CIRCUITS_TRACE": "0",
        "ELECTRIC_SECRET": SECRET,
        "ELECTRIC_STORAGE_DIR": str(output / "engine-data"),
    }
    env.update(BRIDGE_ENVIRONMENT.get(args.variant, {}))
    if W6_VARIANTS.get(args.variant, (0, False))[1]:
        # The list rule: the only difference between a W6 cell and its control.
        env["ELECTRIC_CIRCUITS_LIST_KEYS"] = "1"
    command("sudo", "systemd-run", "--unit", unit, "--collect", "--property=LimitNOFILE=1048576", f"--property=CPUQuota={int(args.cpus * 100)}%", f"--property=StandardOutput=append:{engine_log}", f"--property=StandardError=append:{engine_log}", "/usr/bin/env", *[f"{key}={value}" for key, value in env.items()], str(Path(args.engine).resolve()))
    manifest = {"command": " ".join(os.sys.argv), "harness_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(), "engine_base": "5b2cf3e2731d7fcecc3f95b9c05a5963b9c6b363", "engine_sha256": hashlib.file_digest(Path(args.engine).open("rb"), "sha256").hexdigest(), "indexed_commit": gateway.loader.bundle["commit"], "catalog": args.catalog, "scale": args.scale, "cpu_quota": args.cpus, "visible_cpus": os.cpu_count(), "environment": {key: value for key, value in env.items() if key != "ELECTRIC_SECRET"}, "uname": os.uname(), "target": TARGETS[args.catalog]}
    manifest["variant"] = args.variant
    manifest["label"] = args.label
    manifest["probe_interval_s"] = args.probe_interval
    manifest["engine_source"] = args.engine_source
    save(output / "manifest.json", manifest)
    try:
        async with httpx.AsyncClient(headers={"Authorization": f"Bearer {SECRET}"}, timeout=120) as client:
            await until_ready(client, unit)
            print("Engine ready; generating gateway requests", flush=True)
            pid = int(subprocess.check_output(["systemctl", "show", unit, "--property=MainPID", "--value"], text=True).strip())
            process = psutil.Process(pid)
            save(output / "engine-threads-at-ready.json", {thread.id: Path(f"/proc/{pid}/task/{thread.id}/comm").read_text().strip() for thread in process.threads()})
            catalog = w1_catalog(gateway, W1_VARIANTS[args.variant]) if args.variant in W1_VARIANTS else await gateway.generate(args.catalog, args.scale)
            if args.variant in KEYED_VARIANTS:
                catalog = await gateway.keyed_catalog(catalog, preserved=preserved_tables(args.variant))
            if args.variant in EDGE_ATTRIBUTION:
                catalog = await gateway.edge_attribution_catalog(catalog, EDGE_ATTRIBUTION[args.variant])
            elif args.variant in PARTLY_KEYED:
                catalog = await gateway.keyed_catalog(catalog, preserved={request["body"]["table"].removeprefix("public.") for request in catalog} - PARTLY_KEYED[args.variant])
            if args.variant in COLD_VARIANTS:
                catalog = await gateway.cold_catalog(catalog, key_all_edges=args.variant == "B-cold-key-edges")
            if args.variant in SIMPLE_VARIANTS:
                catalog = await simple_catalog(gateway, args.variant, catalog, lists)
            if args.variant in MAIN3_VARIANTS:
                catalog = await main3_catalog(gateway, catalog, keyed_lists, args.variant != "S-main3-off", history=args.variant not in {"S-main3-live", "S-main3-live-plus"}, keyed_all=args.variant == "S-main3-keyed-all", plus=args.variant == "S-main3-live-plus")
                save(output / "main3-demands.json", [{key: request[key] for key in ("account", "kind", "has_subquery", "demand")} | {"table": request["body"]["table"]} for request in catalog])
            if args.variant in H1_VARIANTS:
                catalog = await h1_catalog(gateway, *H1_VARIANTS[args.variant])
            if args.variant in H1P_VARIANTS:
                catalog = await h1p_catalog(gateway, *H1P_VARIANTS[args.variant])
            if args.variant in EDGE_VARIANTS:
                catalog = edge_catalog(gateway, *EDGE_VARIANTS[args.variant])
            if args.variant in W6_VARIANTS:
                catalog = w6_catalog(gateway, W6_VARIANTS[args.variant][0])
            if args.variant in B2_VARIANTS:
                catalog = b2_catalog(gateway, *B2_VARIANTS[args.variant])
            if args.variant in B2B_VARIANTS:
                catalog = b2b_catalog(gateway, *B2B_VARIANTS[args.variant])
            if args.variant in {"half-subqueries", "plain-only"}:
                retained = round(TARGETS[args.catalog]["subquery_shapes"] / 2) - 8 if args.variant == "half-subqueries" else 0
                catalog = control_catalog(catalog, retained)
            if args.smoke:
                catalog = catalog[:20] + catalog[-20:]
            semaphore = asyncio.Semaphore(32 if args.variant in W1_VARIANTS else 8)
            created = 0
            async def create(request):
                nonlocal created
                async with semaphore:
                    response = await client.post(f"{ENGINE_URL}/v1/subset-feeds", json=request["body"])
                    for attempt in range(5 if args.variant in W1_VARIANTS else 0):
                        # The fixture's store proxy now and then closes a kept-alive connection under the
                        # engine's stream PUT; creating a feed again renews it, so a 5xx is retried.
                        if response.status_code < 500:
                            break
                        print(f"retrying feed creation after {response.status_code}: {response.text[:300]}", flush=True)
                        await asyncio.sleep(0.5 * (attempt + 1))
                        response = await client.post(f"{ENGINE_URL}/v1/subset-feeds", json=request["body"])
                    if response.status_code != 200:
                        raise RuntimeError(f"Shape creation failed: {response.status_code} {response.text[:2000]} table={request['body']['table']}")
                    created += 1
                    if created % 500 == 0:
                        print(f"Created {created}/{len(catalog)} shapes", flush=True)
                    return response.json()
            if args.variant in B2_ONE_READER:
                # The one-reader cell: each filter is created on its own, with the engine's counts for
                # its table read just before and just after, before any other shape exists on it.
                placements = []
                for request in catalog:
                    table = request["body"]["table"].removeprefix("public.")
                    before = await table_counts(client, table)
                    # B2b: a predicate the engine refuses is recorded with its reason, and the rest go on.
                    response = await client.post(f"{ENGINE_URL}/v1/subset-feeds", json=request["body"])
                    after = await table_counts(client, table)
                    placements.append({"table": table, "single_key": B2_ONE_READER[args.variant], "status": response.status_code, "shape": response.json().get("shapeId") if response.status_code == 200 else None, "refused": None if response.status_code == 200 else response.text[:1000], "has_subquery": request["has_subquery"], "where": request["body"]["where"], "before": before, "after": after})
                save(output / "b2-placement-result.json", {"workload": "b2-placement", "engine_source": args.engine_source, "list_keys": env.get("ELECTRIC_CIRCUITS_LIST_KEYS"), "placements": placements})
                catalog = []
            try:
                handles = await asyncio.gather(*(create(request) for request in catalog))
            except RuntimeError:
                # Say what the store held when a shape's stream could not be made.
                descriptors = len(os.listdir(f"/proc/{store.pid}/fd")) if store.poll() is None else None
                limits = [line for line in Path(f"/proc/{store.pid}/limits").read_text().splitlines() if "open files" in line] if store.poll() is None else None
                print(json.dumps({"store_open_files": descriptors, "store_limit": limits, "store_log_tail": (output / "store.log").read_text(errors="replace")[-1500:], "proxy_log_tail": (output / "store-proxy.log").read_text(errors="replace")[-800:]}), flush=True)
                raise
            save(output / "shape-handles.json", handles if args.variant not in W1_VARIANTS else {"created": len(handles)})
            # W6 keeps each live shape with its request and where its stream stood when it was made.
            w6_live = [(request, handle, await stream_offset(client, handle)) for request, handle in zip(catalog, handles)] if args.variant in W6_VARIANTS else []
            import_tables = ("record", "record_date_index", "record_recurrence", "assignment", "calendar_event", "graph_edge", "resource")
            if args.variant == "S-rest-today":
                # This variant asks what today's remaining filters cost, so the importing account holds them too.
                import_tables = tuple(dict.fromkeys((*import_tables, *IMPORT_REST_TABLES)))
            if args.variant in MAIN3_VARIANTS:
                # The importing account's phone holds the same demands as every other account.
                for request in await main3_bodies(gateway, IMPORT_ACCOUNT, keyed_lists[IMPORT_ACCOUNT], flags=args.variant != "S-main3-off", keyed_all=args.variant == "S-main3-keyed-all", plus=args.variant == "S-main3-live-plus"):
                    await create({"body": request["body"]})
                import_tables = ()
            if args.variant in H1_LIKE or args.variant in W1_VARIANTS:
                import_tables = ()
            followers = {}
            if args.variant in W1_VARIANTS:
                # The bystander's phone, a phone in the importing family and one in the family that is deleted.
                for name, account in (("bystander", 0), ("import", IMPORT_ACCOUNT), ("delete", W1_DELETED)):
                    feeds = w1_feeds(gateway, account, str(row_id("space", account)))
                    followers[name] = (account, [(body["table"].removeprefix("public."), await create({"body": body})) for body in feeds])
            for table in import_tables:
                if args.variant in SIMPLE_VARIANTS:
                    for body in await simple_bodies(gateway, args.variant, table, IMPORT_ACCOUNT, lists[IMPORT_ACCOUNT]):
                        await create({"body": body})
                    continue
                if args.variant in COLD_VARIANTS:
                    for body in await gateway.cold_bodies(table, IMPORT_ACCOUNT, args.variant == "B-cold-key-edges"):
                        await create({"body": body})
                    continue
                if args.variant in EDGE_ATTRIBUTION and table == "graph_edge":
                    for body in gateway.edge_attribution(await gateway.body(table, IMPORT_ACCOUNT, identity=f"import-{table}"), IMPORT_ACCOUNT, EDGE_ATTRIBUTION[args.variant]):
                        await create({"body": body})
                    continue
                table_keyed = keyed(args.variant, table)
                for calendar in (range(15) if table_keyed and table == "calendar_event" else [0]):
                    body = gateway.keyed_body(table, IMPORT_ACCOUNT, calendar=calendar, identity=f"import-{table}") if table_keyed else await gateway.body(table, IMPORT_ACCOUNT, identity=f"import-{table}")
                    if args.variant == "plain-only":
                        body = plain_control_body(body, IMPORT_ACCOUNT, f"import-{table}")
                    await create({"body": body})
            if args.variant in H1_LIKE:
                record_sync = gateway.loader.module("api.models.record_sync_authority")
                probe_lists = record_sync.KeyedAudienceLists(space_ids=(UUID(str(row_id("space", 0))),), partner_space_ids=(), calendar_ids=(), partner_calendar_ids=(), shared_record_ids=(), shared_resource_ids=(), generation="h1-probe")
                probe_body = await h1_body(gateway, 99999, probe_lists, identity="other-account-probe")
                if args.variant in FIXED_VARIANTS:
                    # The bystander is a one-family account, so the hypothetical fix covers its feed too.
                    probe_body = {**probe_body, "where": unwrap_single_or(probe_body["where"])}
            elif args.variant in W1_VARIANTS:
                probe_body = next(body for body in w1_feeds(gateway, 0, str(row_id("space", 0))) if body["table"] == "public.record")
            elif args.variant in MAIN3_VARIANTS:
                # The bystander's session-long Record feed (workflow roots): keyed with the flags on.
                probe_body = next(request["body"] for request in await main3_bodies(gateway, 0, keyed_lists[0], flags=args.variant != "S-main3-off", keyed_all=args.variant == "S-main3-keyed-all") if request["body"]["table"] == "public.record" and request["demand"]["origin"] == "session")
                probe_body = {**probe_body, "subscription": gateway._subscription("record", 0, "other-account-probe")}
            elif args.variant in SIMPLE_VARIANTS:
                probe_body = (await simple_bodies(gateway, args.variant, "record", 0, lists[0]))[0]
            else:
                probe_body = gateway.keyed_body("record", 0, identity="other-account-probe") if args.variant in COLD_VARIANTS or keyed(args.variant, "record") else await gateway.body("record", 0, identity="other-account-probe")
            if args.variant == "plain-only":
                probe_body = plain_control_body(probe_body, 0, "other-account-probe")
            probe_feed = await create({"body": probe_body})
            head = await client.head(probe_feed["streamUrl"])
            head.raise_for_status()
            probe_feed["offset"] = head.headers["stream-next-offset"]
            memory = await client.get(f"{ENGINE_URL}/memory")
            memory.raise_for_status()
            save(output / "catalog-before-warmup.json", memory.json())
            print(json.dumps({"before_warmup": memory.json()["cardinalities"]}), flush=True)
            for endpoint in ("graph", "state") if args.variant not in W1_VARIANTS else ():
                response = await client.get(f"{ENGINE_URL}/{endpoint}")
                save(output / f"catalog-{endpoint}.json", {"status": response.status_code, "body": response.json() if response.status_code == 200 else response.text})
            validator = gateway.gateway._is_drained_receipt
            fence_user = row_id("user", 1)
            fence_generation = gateway.authority(1).generation
            warm_marker = uuid4()
            def warm():
                with psycopg.connect(PG_URL) as connection:
                    for account in range(19) if args.variant not in W1_VARIANTS else (0,):
                        data.update(connection, "record", account, 1 if args.smoke else 9 if args.catalog == "nov23" else 19)
                    marker(connection, fence_user, fence_generation, warm_marker)
            await asyncio.to_thread(warm)
            await wait_for_marker(client, validator, warm_marker, fence_user, fence_generation, 900, process)
            memory = await client.get(f"{ENGINE_URL}/memory")
            memory.raise_for_status()
            save(output / "catalog-calibration.json", {"target": TARGETS[args.catalog], "actual": memory.json(), "source_shape_requests": len(catalog), "smoke_only": args.smoke})
            subqueries = await client.get(f"{ENGINE_URL}/subqueries", timeout=60)
            save(output / "catalog-subqueries.json", {"status": subqueries.status_code, "body": subqueries.json() if subqueries.status_code == 200 else subqueries.text[:2000]})
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
            plan = []
            if any(workload in WINDOW_WORKLOADS for workload in args.workloads):
                print("Sizing window loads", flush=True)
                with psycopg.connect(PG_URL, autocommit=True) as connection:
                    plan = await window_plan(gateway, keyed_lists, connection)
                save(output / "window-plan.json", [{key: value for key, value in load.items() if key != "requests"} for load in plan])
                print(json.dumps({"window_loads": dict(Counter(f"{load['kind']}-{load['size']}" for load in plan))}), flush=True)
            w7_queries = []
            if any(workload in W7_WORKLOADS for workload in args.workloads):
                for index in range(W7_QUERIES):
                    account = index % 19
                    subsets = await window_subsets(gateway, account, keyed_lists[account], "day", HOME_TODAY + datetime.timedelta(days=index % 30))
                    carrying = [body for body in subsets if '"in":' in json.dumps(body["where"])]
                    w7_queries.append(carrying[index % len(carrying)])
            for workload in args.workloads:
                if workload in B2_STEPS:
                    continue
                if workload in W6_STEPS:
                    step = {"workload": workload, "at": time.time()}
                    if workload.startswith("w6-placement"):
                        families = await client.get(f"{ENGINE_URL}/tables/public.graph_edge/families")
                        step["engine_table"] = families.json() if families.status_code == 200 else {"status": families.status_code, "body": families.text[:300]}
                        step["cardinalities"] = {key: value for key, value in (await client.get(f"{ENGINE_URL}/memory")).json()["cardinalities"].items() if not key.startswith("bytes")}
                        step["harness_copy_of_the_rule"] = placement_summary([request["body"]["where"] for request, _, _ in w6_live], W6_VARIANTS[args.variant][1])
                        step["rss_bytes"] = process.memory_info().rss
                    elif workload == "w6-restart":
                        began = time.monotonic()
                        command("sudo", "systemctl", "restart", unit)
                        await until_ready(client, unit)
                        step["restart_to_ready_s"] = time.monotonic() - began
                        pid = int(subprocess.check_output(["systemctl", "show", unit, "--property=MainPID", "--value"], text=True).strip())
                        process = psutil.Process(pid)
                        step["cardinalities"] = {key: value for key, value in (await client.get(f"{ENGINE_URL}/memory")).json()["cardinalities"].items() if not key.startswith("bytes")}
                    elif workload == "w6-access":
                        ordinals = sorted({request["ordinal"] for request, _, _ in w6_live})
                        changes = [(ordinal, "gain") for ordinal in ordinals if not plain_words_account(ordinal, "mix")[0]][:W6_ACCESS_ACCOUNTS] + [(ordinal, "lose") for ordinal in ordinals if plain_words_account(ordinal, "mix")[0]][:W6_ACCESS_ACCOUNTS]
                        released = Counter()
                        for ordinal, change in changes:
                            # The gateway re-subscribes the account with its new lists, then lets the old feeds go.
                            renewed = []
                            for request in w6_requests(gateway, ordinal, change):
                                handle = await create(request)
                                renewed.append((request, handle, await stream_offset(client, handle)))
                            for request, handle, _ in [entry for entry in w6_live if entry[0]["ordinal"] == ordinal]:
                                response = await client.delete(f"{ENGINE_URL}/v1/shapes/{handle['shapeId']}", params={"subscription": handle["subscription"]})
                                released[response.status_code] += 1
                            w6_live = [entry for entry in w6_live if entry[0]["ordinal"] != ordinal] + renewed
                        step["accounts"] = [{"ordinal": ordinal, "change": change} for ordinal, change in changes]
                        step["released"] = dict(released)
                        moved_marker = uuid4()
                        def move_edges():
                            moved = 0
                            with psycopg.connect(PG_URL) as connection:
                                # As every fixture write: the written Spaces of accounts 20-50 have no `space` row.
                                connection.execute("SET LOCAL session_replication_role = replica")
                                for index, account in enumerate(H1_DATA_ACCOUNTS):
                                    target = row_id("space", H1_DATA_ACCOUNTS[(index + 1) % len(H1_DATA_ACCOUNTS)])
                                    ids = [row["id"] for row in data.rows[("graph_edge", account)][:W6_MOVED_EDGES]]
                                    moved += connection.execute("UPDATE public.graph_edge SET from_space_id = %s, to_space_id = %s, version = version + 1 WHERE id = ANY(%s)", (target, target, ids)).rowcount
                                marker(connection, fence_user, fence_generation, moved_marker)
                            return moved
                        step["edges_moved"] = await asyncio.to_thread(move_edges)
                        await wait_for_marker(client, validator, moved_marker, fence_user, fence_generation, 900, process)
                    elif workload == "w6-check":
                        # Every live shape's delivered rows, replayed, against a direct query of its filter
                        # over the rows the run changed (a changes-only feed carries no others).
                        changed = [row["id"] for account in H1_DATA_ACCOUNTS for row in data.rows[("graph_edge", account)][:20]]
                        mismatches, with_rows, delivered, expected_rows = [], 0, 0, 0
                        with psycopg.connect(PG_URL, autocommit=True) as connection:
                            for request, handle, offset in w6_live:
                                expected = {str(row[0]) for row in connection.execute(sql.SQL("SELECT q.id FROM public.graph_edge q WHERE q.id = ANY(%s) AND {}").format(predicate_sql(request["body"]["where"], "q")), (changed,))}
                                present = {}
                                for _ in range(10000):
                                    response = await client.get(handle["streamUrl"], params={"offset": offset}, timeout=120)
                                    if response.status_code == 204:
                                        break
                                    response.raise_for_status()
                                    envelopes = response.json()
                                    if not envelopes:
                                        break
                                    for envelope in envelopes:
                                        if "key" in envelope:
                                            delivered += 1
                                            present[str(envelope["key"])] = envelope.get("headers", {}).get("operation") != "delete"
                                    following = response.headers.get("stream-next-offset", offset)
                                    if following == offset:
                                        break
                                    offset = following
                                held = {key for key, live in present.items() if live}
                                with_rows += bool(expected)
                                expected_rows += len(expected)
                                if held != expected:
                                    mismatches.append({"shape": handle["shapeId"], "account": request["ordinal"], "kind": request["kind"], "expected": len(expected), "held": len(held), "missing": sorted(expected - held)[:5], "extra": sorted(held - expected)[:5]})
                        step.update(shapes_checked=len(w6_live), shapes_with_rows=with_rows, rows_expected=expected_rows, envelopes_read=delivered, mismatched_shapes=len(mismatches), mismatches=mismatches[:50])
                    save(output / f"{workload}-result.json", step)
                    print(json.dumps(step, default=str)[:3000], flush=True)
                    continue
                probe = OtherAccountProbe(engine_url=ENGINE_URL, postgres_url=PG_URL, user_id=row_id("user", 0), record_id=row_id("record", 0), generation=gateway.authority(0).generation, feed=probe_feed, subset=subset, receipt_validator=validator, output=output / f"{workload}-probes.jsonl", interval=args.probe_interval)
                probe_task = asyncio.create_task(probe.run())
                await asyncio.sleep(3)
                memory_before = (await client.get(f"{ENGINE_URL}/memory")).json()
                profile_before = memory_before.get("stall_profile")
                receipts = []
                follower_offsets = {name: {table: await stream_offset(client, handle) for table, handle in handles} for name, (_, handles) in followers.items()}
                metrics_stopped = asyncio.Event()
                metrics_task = asyncio.create_task(sample_metrics(client, output / f"{workload}-metrics.jsonl", metrics_stopped))
                fence_id = uuid4()
                transactions = []
                def mark(connection, transaction):
                    multi = {"import", "import+paced-delete", "import-noresource"}
                    current = fence_id if transaction in {5, "final"} and workload in multi or workload not in multi else uuid4()
                    marker(connection, fence_user, fence_generation, current)
                    transactions.append({"transaction": transaction, "txid": connection.execute("SELECT pg_current_xact_id()::text").fetchone()[0], "before_commit": time.time(), "marker": str(current)})
                began_wall = time.time()
                began = time.monotonic()
                profiler = None
                perf = os.environ.get("STALL_REPRO_PERF")
                if perf:
                    perf_log = (output / "perf.log").open("w")
                    profiler = subprocess.Popen(["sudo", perf, "record", "-e", "cpu-clock", "-F", "99", "-g", "--call-graph", "fp", "-p", str(pid), "-o", str(output / f"{workload}.perf.data"), "--", "sleep", str(args.timeout + 120)], stdout=perf_log, stderr=subprocess.STDOUT, start_new_session=True)
                cpu_before = sum(process.cpu_times()[:2])
                cpu_stopped = asyncio.Event()
                cpu_task = asyncio.create_task(sample_cpu(process, quota.parent, output / f"{workload}-cpu.jsonl", cpu_stopped))
                result = {"workload": workload, "measurement": "source SQL start to workload receipt and zero pending flips/appends", "first_change_read_time": None, "timeout_s": args.timeout}
                try:
                    if workload == "import":
                        operation = data.calendar_import
                    elif workload == "import-noresource":
                        operation = lambda url, callback: import_without_resources(data, url, callback)
                    elif workload in W7_WORKLOADS:
                        operation = w7_operation(w7_queries, process)
                    elif workload in W1_WORKLOADS:
                        operation = w1_operation(data, workload, W1_VARIANTS[args.variant], output, validator, ((fence_user, fence_generation), (row_id("user", 2), gateway.authority(2).generation)), transactions)
                    elif workload in WINDOW_WORKLOADS:
                        operation = window_operation(plan, *workload.removeprefix("window-").split("-"), quota.parent)
                    elif workload in IDLE_WORKLOADS:
                        def operation(url, callback, seconds=int(workload.removeprefix("idle-"))):
                            time.sleep(seconds)
                            with psycopg.connect(url) as connection:
                                callback(connection, "final")
                            return []
                    elif workload in AUDIT_WORKLOADS:
                        def operation(url, callback, write=audit_operation(data, workload)):
                            with psycopg.connect(url) as connection:
                                connection.execute("SET LOCAL session_replication_role = replica")
                                journal = write(connection)
                                callback(connection, "final")
                            return journal
                    elif workload.startswith("paced-import-"):
                        def operation(url, callback, size=int(workload.removeprefix("paced-import-"))):
                            journal = paced_first_import(data, url, size, fence_user, fence_generation, validator, transactions, receipts)
                            with psycopg.connect(url) as connection:
                                callback(connection, "final")
                            return journal
                    elif workload == "paced-delete":
                        def operation(url, callback):
                            journal = paced_delete_account(data, url, LARGE_ACCOUNT, fence_user, fence_generation, validator, transactions, receipts)
                            with psycopg.connect(url) as connection:
                                callback(connection, "final")
                            return journal
                    elif workload == "import+paced-delete":
                        def operation(url, callback):
                            # The first import (unpaced, as prod commits it) and the paced deletion at once;
                            # a marker-only commit after both closes the workload.
                            with concurrent.futures.ThreadPoolExecutor(2) as pool:
                                imported = pool.submit(data.calendar_import, url, lambda connection, transaction: mark(connection, f"import-{transaction}"))
                                deleted = pool.submit(paced_delete_account, data, url, LARGE_ACCOUNT, fence_user, fence_generation, validator, transactions, receipts)
                                journal = imported.result() + deleted.result()
                            with psycopg.connect(url) as connection:
                                callback(connection, "final")
                            return journal
                    elif workload == "share-delete":
                        operation = lambda url, callback: data.delete_account(url, SHARE_ACCOUNT, callback, extra_tables=SHARE_TABLES)
                    else:
                        operation = lambda url, callback: data.delete_account(url, LARGE_ACCOUNT if workload == "large-delete" else IMPORT_ACCOUNT, callback)
                    journal = await asyncio.to_thread(operation, PG_URL, mark)
                    save(output / f"{workload}-writes.json", journal)
                    result["row_changes"] = sum(entry["rows"] for entry in journal)
                    result["source_write_s"] = time.monotonic() - began
                    # B2 compares commits near the 0.2 s floor, so its receipt is polled every 20 ms.
                    result["completion"] = await wait_for_marker(client, validator, fence_id, fence_user, fence_generation, args.timeout, process, poll=0.02 if workload in B2_COMMITS else 0.2)
                    result["completed"] = True
                    if workload in W1_WORKLOADS:
                        result["loads"] = loads_summary(output, workload)
                        result["followers"] = await follower_volumes(client, data, followers, follower_offsets)
                        result["w1"] = [{key: value for key, value in entry.items() if key != "receipt_s"} for entry in journal if entry.get("table") == "w1"]
                except Exception as error:
                    result["completed"] = False
                    result["error"] = repr(error)
                result["elapsed_s"] = time.monotonic() - began
                result["ended_wall"] = time.time()
                metrics_stopped.set()
                result["sequencer_held"] = await metrics_task
                if receipts:
                    ordered = sorted(receipts)
                    result["receipts"] = {"commits": len(receipts), "p50_s": ordered[len(ordered) // 2], "p95_s": ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))], "max_s": ordered[-1], "sum_s": sum(receipts)}
                try:
                    memory_after = (await client.get(f"{ENGINE_URL}/memory")).json()
                    result["subquery_change"] = {key: memory_after["cardinalities"].get(key, 0) - memory_before["cardinalities"].get(key, 0) for key in memory_before["cardinalities"] if key.startswith("subquery_") or key in {"shapes", "standalone"}}
                except (httpx.HTTPError, KeyError, ValueError):
                    pass
                try:
                    cpu_after = sum(process.cpu_times()[:2])
                except psutil.NoSuchProcess:
                    samples = [json.loads(line) for line in (output / f"{workload}-cpu.jsonl").read_text().splitlines()]
                    cpu_after = samples[-1]["cpu_s"] if samples else cpu_before
                    result["cpu_incomplete"] = "process exited; CPU total stops at last retained one-second sample"
                result["began_wall"] = began_wall
                result["transactions"] = transactions
                cpu_stopped.set()
                probe.stop()
                if profiler:
                    subprocess.run(["sudo", "kill", "-INT", "--", f"-{profiler.pid}"], check=False)
                    profiler.wait(timeout=30)
                    subprocess.run(["sudo", "chmod", "a+r", str(output / f"{workload}.perf.data")], check=True)
                    perf_log.close()
                await cpu_task
                await probe_task
                if profiler:
                    with (output / f"{workload}.perf.script").open("w") as handle:
                        extracted = subprocess.run(["sudo", perf, "script", "-i", str(output / f"{workload}.perf.data")], stdout=handle, stderr=subprocess.PIPE, text=True)
                    stacks = fold((output / f"{workload}.perf.script").read_text())
                    result["profile"] = {"elapsed_s": result["elapsed_s"], "samples": sum(stacks.values()), "script_exit": extracted.returncode, "script_stderr": extracted.stderr, "at_least_three_minutes": result["elapsed_s"] >= 180, "attribution": attribute(stacks) if stacks else {}}
                    if stacks:
                        (output / f"{workload}.folded").write_text("\n".join(f"{stack} {count}" for stack, count in sorted(stacks.items())) + "\n")
                        (output / f"{workload}.svg").write_text(render(stacks, f"{args.name} {workload} CPU"))
                result["engine_cpu_percent"] = (cpu_after - cpu_before) / result["elapsed_s"] * 100
                result["changes_per_s"] = result.get("row_changes", 0) / result["elapsed_s"] if result["completed"] else None
                result["probe"] = summarize([sample for sample in probe.samples if began - probe.started <= sample["at_s"] <= began + result["elapsed_s"] - probe.started])
                result["idle_probe"] = summarize([sample for sample in probe.samples if sample["at_s"] < began - probe.started])
                if profile_before is not None and result["completed"]:
                    profile_after = (await client.get(f"{ENGINE_URL}/memory")).json()["stall_profile"]
                    result["wall_profile"] = {"before": profile_before, "after": profile_after, "delta": {category: {metric: values[metric] - profile_before["categories"][category][metric] for metric in values} for category, values in profile_after["categories"].items()}}
                events = read_events(engine_log, began_wall, result["ended_wall"])
                by_table = Counter()
                empty = Counter()
                result["pinned_warnings"] = 0
                result["large_transactions"] = []
                backfill_times = []
                for event in events:
                    fields = event.get("fields", {})
                    if fields.get("message") == "backfill finished":
                        by_table[fields["table"]] += 1
                        backfill_times.append(event["timestamp"])
                        if fields.get("rows") == 0:
                            empty[fields["table"]] += 1
                    if "replay position is pinned" in str(fields.get("message", "")):
                        result["pinned_warnings"] += 1
                    if fields.get("message") == "large transaction appended to the change log":
                        result["large_transactions"].append(fields)
                result["backfills"] = sum(by_table.values())
                if backfill_times:
                    # The flip walks' query-backs: first to last `backfill finished` in the workload window.
                    stamps = sorted(datetime.datetime.fromisoformat(stamp.replace("Z", "+00:00")).timestamp() for stamp in backfill_times)
                    result["backfill_window_s"] = stamps[-1] - stamps[0]
                    result["backfill_last_after_start_s"] = stamps[-1] - began_wall
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
    parser.add_argument("--variant", choices=("A", "A-observed", *sorted(ALL_VARIANTS), *BRIDGE_ENVIRONMENT, "coalesced-base", "coalesced-head", "half-subqueries", "plain-only"), default="A")
    parser.add_argument("--probe-interval", type=float, default=1.0, help="seconds between the bystander's probe ticks")
    parser.add_argument("--label", default=None, help="retained folder name; paired runs on one runner set it so they do not collide")
    parser.add_argument("--engine-source", default="5b2cf3e2731d7fcecc3f95b9c05a5963b9c6b363")
    parser.add_argument("--workloads", nargs="+", choices=("import", "large-delete", "small-delete", "share-delete", "paced-delete", "import+paced-delete", "paced-import-1", "paced-import-10", "paced-import-50", "paced-import-100", *AUDIT_WORKLOADS, *WINDOW_WORKLOADS, *IDLE_WORKLOADS, *W1_WORKLOADS, *W6_STEPS, *W7_WORKLOADS, *B2_STEPS), default=["import", "large-delete", "small-delete"])
    asyncio.run(run(parser.parse_args()))
