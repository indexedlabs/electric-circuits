"""W1's steady background: every active family's phone issuing screen-sized subset loads.

Each family makes, every minute and at its own fixed offsets within it, one day load, one week load
and two Record detail opens. A load is the one-shot `/v1/subsets/query` requests a screen needs, sent
together; each filter starts with the family's `audience_key = K`. The schedule is open-loop: a load
is due at its time whether or not earlier ones have answered, up to `--inflight` loads at once; a
load that cannot start within `--shed` seconds of its time is counted as shed and not sent.

Run by `run.py` as a separate process (two shards at 2,000 families) so the generator does not share
an event loop with the probes and the writers. One JSON line per load.
"""
from __future__ import annotations

import argparse
import asyncio
import datetime
import hashlib
import json
import random
import time
from pathlib import Path
from uuid import UUID

import aiohttp

ENGINE_URL = "http://127.0.0.1:7010"
SECRET = "stall-repro-synthetic-secret"
BIG_EVERY = 20
RECORDS = (3, 2200)
DATE_DAYS = 365
FIRST_DAY = datetime.date(2026, 1, 1)
LOADS_PER_MINUTE = (("day", 1), ("week", 1), ("detail", 2))
REQUEST_TIMEOUT_S = 10
JSON = {"content-type": "application/json", "authorization": f"Bearer {SECRET}"}


def uuid_of(text):
    return str(UUID(hashlib.md5(text.encode()).hexdigest()))


def eq(column, value):
    return {"col": column, "op": "eq", "value": value}


def window_queries(key, start, days):
    """A day or week window as six one-shot queries on the family's key."""
    end = start + datetime.timedelta(days=days)
    own = eq("audience_key", key)
    dated = {"and": [own, {"col": "date_value", "op": "gte", "value": start.isoformat()}, {"col": "date_value", "op": "lt", "value": end.isoformat()}]}
    in_window = {"table": "public.record_date_index", "project": "record_id", "where": dated}
    return [
        {"table": "public.record_date_index", "where": dated},
        {"table": "public.record", "where": {"and": [own, {"col": "id", "in": in_window}]}},
        {"table": "public.record_recurrence", "where": {"and": [own, {"col": "dtstart", "op": "lt", "value": f"{(end + datetime.timedelta(days=2)).isoformat()}T00:00:00Z"}]}},
        {"table": "public.assignment", "where": {"and": [own, {"col": "record_id", "in": in_window}]}},
        {"table": "public.graph_edge", "where": {"and": [own, eq("from_type", "record"), {"col": "from_id", "in": in_window}, eq("edge_type", "has_attendee")]}},
        {"table": "public.calendar_event", "where": {"and": [own, {"col": "start_datetime", "op": "lt", "value": f"{end.isoformat()}T00:00:00Z"}, {"col": "end_datetime", "op": "gte", "value": f"{start.isoformat()}T00:00:00Z"}]}},
    ]


def detail_queries(key, record):
    """One Record's detail and its thread, as five one-shot queries on the family's key."""
    own = eq("audience_key", key)
    return [
        {"table": "public.record", "where": {"and": [own, eq("id", record)]}},
        {"table": "public.assignment", "where": {"and": [own, eq("record_id", record)]}},
        {"table": "public.graph_edge", "where": {"and": [own, eq("from_id", record)]}},
        {"table": "public.record_date_index", "where": {"and": [own, eq("record_id", record)]}},
        {"table": "public.message", "where": {"and": [own, eq("target_type", "record"), eq("target_id", record)]}},
    ]


async def query(client, body):
    # aiohttp, not the harness's httpx: at 2,000 families the generator sends about 730 requests a second.
    try:
        async with client.post(f"{ENGINE_URL}/v1/subsets/query", data=json.dumps(body, separators=(",", ":")), headers=JSON) as response:
            content = await response.read()
            return response.status, len(content), content.count(b'"audience_key"')
    except asyncio.TimeoutError:
        return "timeout", 0, 0
    except aiohttp.ClientError as error:
        return type(error).__name__, 0, 0


async def main(args):
    rng = random.Random(args.seed)
    families = [family for family in range(args.families) if family % args.shards == args.shard]
    schedule = []
    for family in families:
        for kind, per_minute in LOADS_PER_MINUTE:
            for _ in range(per_minute):
                schedule.append((rng.uniform(0, 60), family, kind))
    schedule.sort()
    output = Path(args.out).open("w")
    stop = Path(args.stop)
    inflight = asyncio.Semaphore(args.inflight)
    tasks = set()
    started = time.monotonic()
    started_wall = time.time()

    async def load(due, family, kind):
        big = family % BIG_EVERY == BIG_EVERY - 1
        key = uuid_of(f"w1/space/{family}")
        if kind == "detail":
            queries = detail_queries(key, uuid_of(f"w1/record/{family}/{rng.randrange(RECORDS[1] if big else RECORDS[0])}"))
        else:
            queries = window_queries(key, FIRST_DAY + datetime.timedelta(days=rng.randrange(DATE_DAYS - 7)), 1 if kind == "day" else 7)
        async with inflight:
            began = time.monotonic()
            lag = began - (started + due)
            sample = {"at": started_wall + due, "kind": kind, "family": family, "big": big, "lag_s": lag}
            if lag > args.shed:
                sample.update(shed=True, ok=False)
            else:
                answers = await asyncio.gather(*(query(client, body) for body in queries))
                sample.update(wall_s=time.monotonic() - began, bytes=sum(answer[1] for answer in answers), rows=sum(answer[2] for answer in answers), timeouts=sum(answer[0] == "timeout" for answer in answers), errors=[answer[0] for answer in answers if answer[0] not in (200, "timeout")], ok=all(answer[0] == 200 for answer in answers))
        output.write(json.dumps(sample) + "\n")

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=args.connections), timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_S)) as client:
        minute = 0
        while not stop.exists():
            for offset, family, kind in schedule:
                due = minute * 60 + offset
                delay = started + due - time.monotonic()
                if delay > 0:
                    # Sleep in short steps so a stop is seen within a quarter second.
                    while delay > 0 and not stop.exists():
                        await asyncio.sleep(min(delay, 0.25))
                        delay = started + due - time.monotonic()
                if stop.exists():
                    break
                task = asyncio.create_task(load(due, family, kind))
                tasks.add(task)
                task.add_done_callback(tasks.discard)
            minute += 1
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=REQUEST_TIMEOUT_S + 5)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
    output.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--families", type=int, required=True)
    parser.add_argument("--shard", type=int, default=0)
    parser.add_argument("--shards", type=int, default=1)
    parser.add_argument("--out", required=True)
    parser.add_argument("--stop", required=True)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--inflight", type=int, default=64, help="loads in flight at once, per process")
    parser.add_argument("--connections", type=int, default=128, help="engine connections, per process")
    parser.add_argument("--shed", type=float, default=30)
    asyncio.run(main(parser.parse_args()))
