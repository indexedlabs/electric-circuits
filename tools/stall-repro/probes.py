from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4

import httpx
import psycopg


def require_isolated(engine_url: str, postgres_url: str) -> None:
    for url in (engine_url, postgres_url):
        if urlparse(url).hostname not in {"localhost", "127.0.0.1", "::1"}:
            raise ValueError("The reproduction only accepts loopback services")
    if urlparse(postgres_url).path != "/stall_repro":
        raise ValueError("The database must be the disposable stall_repro database")


class OtherAccountProbe:
    def __init__(self, *, engine_url, postgres_url, user_id, record_id, generation, feed, subset, receipt_validator, output: Path, secret="stall-repro-synthetic-secret"):
        require_isolated(engine_url, postgres_url)
        require_isolated(feed["streamUrl"], postgres_url)
        self.engine_url = engine_url
        self.postgres_url = postgres_url
        self.user_id = user_id
        self.record_id = record_id
        self.generation = generation
        self.feed = feed
        self.subset = subset
        self.receipt_validator = receipt_validator
        self.output = output
        self.secret = secret
        self.observed = {}
        self.expected = {}
        self.stopped = asyncio.Event()
        self.started = time.monotonic()
        self.samples = []
        self.tasks = set()

    def record(self, kind, started, success, **extra):
        sample = {"kind": kind, "at_s": started - self.started, "duration_s": time.monotonic() - started, "success": success, **extra}
        self.samples.append(sample)
        with self.output.open("a") as handle:
            handle.write(json.dumps(sample) + "\n")

    async def drain(self, client):
        started = time.monotonic()
        marker = uuid4()
        polls = 0
        try:
            async with asyncio.timeout(2):
                async with await psycopg.AsyncConnection.connect(self.postgres_url, application_name="stall-probe-drain") as connection:
                    await connection.execute("INSERT INTO public.native_sync_authority_fence (user_id, generation, source_commit_id) VALUES (%s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET generation=EXCLUDED.generation, source_commit_id=EXCLUDED.source_commit_id", (self.user_id, self.generation, marker))
                delay = 0.01
                while True:
                    response = await client.get(f"{self.engine_url}/_runtime/drained-through/{marker}", params={"user_id": str(self.user_id), "generation": self.generation})
                    polls += 1
                    if response.status_code == 200 and self.receipt_validator(response.json(), marker=marker, user_id=self.user_id, generation=self.generation):
                        self.record("drain", started, True, polls=polls, receipt=response.json(), marker=str(marker))
                        return
                    if response.status_code not in {200, 503}:
                        response.raise_for_status()
                    await asyncio.sleep(min(delay, 0.2))
                    delay *= 2
        except Exception as error:
            self.record("drain", started, False, polls=polls, error=repr(error), marker=str(marker), censored=isinstance(error, TimeoutError))

    async def query(self, client):
        started = time.monotonic()
        try:
            async with asyncio.timeout(2):
                response = await client.post(f"{self.engine_url}/v1/subsets/query", json=self.subset)
                response.raise_for_status()
                payload = response.json()
                if not any(str(row.get("id")) == str(self.record_id) for row in payload["rows"]):
                    raise AssertionError("Subset did not contain the independent account's fixture row")
                self.record("subset", started, True, rows=len(payload["rows"]))
        except Exception as error:
            self.record("subset", started, False, error=repr(error), censored=isinstance(error, TimeoutError))

    async def live(self):
        started = time.monotonic()
        token = f"stall-probe-{uuid4()}"
        self.expected[token] = started
        try:
            async with asyncio.timeout(2):
                async with await psycopg.AsyncConnection.connect(self.postgres_url, application_name="stall-probe-live") as connection:
                    cursor = await connection.execute("UPDATE public.record SET name=%s, version=version+1 WHERE id=%s", (token, self.record_id))
                    if cursor.rowcount != 1:
                        raise AssertionError("Live probe did not update exactly one fixture row")
                while token not in self.observed:
                    await asyncio.sleep(0.01)
                self.record("live", started, True, token=token, delivery_s=self.observed[token] - started)
        except Exception as error:
            self.record("live", started, False, token=token, error=repr(error), censored=isinstance(error, TimeoutError))

    async def tail(self, client):
        offset = self.feed["offset"]
        while not self.stopped.is_set():
            try:
                response = await client.get(self.feed["streamUrl"], params={"offset": offset, "live": "long-poll"}, timeout=35)
                response.raise_for_status()
                if response.status_code != 204:
                    for envelope in response.json():
                        value = envelope.get("value") or {}
                        token = value.get("name")
                        if token in self.expected and token not in self.observed:
                            received = time.monotonic()
                            self.observed[token] = received
                            self.record("live_delivery", self.expected[token], True, token=token, delivery_s=received - self.expected[token])
                offset = response.headers.get("stream-next-offset", offset)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.record("tail_error", time.monotonic(), False, error=repr(error))
                await asyncio.sleep(0.1)

    async def run(self):
        self.output.parent.mkdir(parents=True, exist_ok=True)
        async with httpx.AsyncClient(headers={"Authorization": f"Bearer {self.secret}"}, timeout=2) as client:
            tail = asyncio.create_task(self.tail(client))
            tick = 0
            try:
                while not self.stopped.is_set():
                    for coroutine in (self.drain(client), self.query(client), self.live()):
                        task = asyncio.create_task(coroutine)
                        self.tasks.add(task)
                        task.add_done_callback(self.tasks.discard)
                    tick += 1
                    try:
                        await asyncio.wait_for(self.stopped.wait(), timeout=max(0.001, self.started + tick - time.monotonic()))
                    except TimeoutError:
                        pass
            finally:
                if self.tasks:
                    await asyncio.gather(*self.tasks)
                tail.cancel()
                await asyncio.gather(tail, return_exceptions=True)

    def stop(self):
        self.stopped.set()


def summarize(samples):
    result = {}
    for kind in ("drain", "subset", "live"):
        selected = [sample for sample in samples if sample["kind"] == kind]
        successes = [sample for sample in selected if sample["success"]]
        durations = sorted(sample["duration_s"] for sample in successes)
        result[kind] = {"attempts": len(selected), "successes": len(successes), "success_rate": len(successes) / len(selected) if selected else None, "successful_p50_s": durations[int((len(durations) - 1) * 0.5)] if durations else None, "successful_p95_s": durations[int((len(durations) - 1) * 0.95)] if durations else None, "successful_max_s": max(durations) if durations else None, "timeouts_censored_at_s": 2}
    return result
