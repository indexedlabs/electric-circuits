from __future__ import annotations

import asyncio
import datetime
import hashlib
import json
from collections import Counter
from dataclasses import replace
from pathlib import Path

from export_fixture import synthetic_id
from source_loader import SourceLoader


TARGETS = {
    "nov23": {"shapes": 8376, "standalone": 5991, "family_shapes": 31, "families": 1, "subquery_shapes": 2354, "subquery_nodes": 808, "subquery_edges": 14339, "subquery_contributors": 141628, "subquery_distinct_values": 141614, "subquery_feed_entries": 5850, "tailers": 34, "tables": 44},
    "nov24": {"shapes": 10571, "standalone": 6251, "family_shapes": 33, "families": 1, "subquery_shapes": 4287, "subquery_nodes": 1382, "subquery_edges": 25219, "subquery_contributors": 235310, "subquery_distinct_values": 235292, "subquery_feed_entries": 16388, "tailers": 37, "tables": 44},
}
ACCOUNT_WEIGHTS = [519, 345, 220, 217, 201, 176, 158, 152, 94, 76, 53, 43, 41, 41, 38, 36, 27, 19, 9]
SUBQUERY_TABLE_WEIGHTS = {"record": 27, "graph_edge": 16, "calendar_event": 11, "record_date_index": 10, "record_recurrence": 9, "assignment": 8, "resource": 6.5, "resource_fact": 4, "message": 1, "record_completed_index": 1, "record_assignee_index": 1, "user": 1, "file": 1, "workflow_run": 1}
PLAIN_TABLE_WEIGHTS = {"calendar_event": 196, "calendar": 39, "external_account": 39, "activity": 60, "space_member": 50, "invite": 37, "space": 34, "assistant_state": 32, "feedback": 32, "suggestion": 101, "standing_grant": 23}


def apportion(total, weights):
    denominator = sum(weights.values())
    result = {key: int(total * weight / denominator) for key, weight in weights.items()}
    remainder = total - sum(result.values())
    order = sorted(weights, key=lambda key: (total * weights[key] / denominator - result[key]), reverse=True)
    for key in order[:remainder]:
        result[key] += 1
    return result


def contains_subquery(predicate):
    if isinstance(predicate, dict):
        return "in" in predicate or any(contains_subquery(value) for value in predicate.values())
    return isinstance(predicate, list) and any(contains_subquery(value) for value in predicate)


class GatewayShapes:
    def __init__(self, bundle: Path):
        self.loader = SourceLoader(bundle)
        self.gateway = self.loader.module("api.services.circuits_sync")
        self.authority_module = self.loader.module("api.services.circuits_sync_authority")

    def authority(self, account, generation=0):
        return self.authority_module.NativeSyncAuthority(record_scoped=True, fingerprint=f"synthetic-{account}-{generation}", authorized_space_ids=(synthetic_id(f"space/{account}"),), direct_content_policy_enabled=True, direct_calendar_grantor_user_ids=(synthetic_id(f"user/{(account + 1) % 18}"),) if account < 18 else (), generation=hashlib.sha256(f"synthetic-generation-{account}".encode()).hexdigest())

    async def body(self, table, account, scope=None, identity="fixture", chat=False, generation=0, plain=False):
        collection = self.gateway.registered_collection(table, allow_chat_projection_dependency=True)
        authority = self.authority(account, generation)
        if plain:
            authority = replace(authority, direct_content_policy_enabled=False, direct_calendar_grantor_user_ids=(), fingerprint=f"{authority.fingerprint}-owner-only")
        authorization = None
        if chat:
            authorization = await self.gateway._chat_authorization_predicate(collection, synthetic_id(f"user/{account}"), authority=authority)
        where = await self.gateway.authorize_predicate(scope, collection, synthetic_id(f"user/{account}"), authority=authority, chat=chat, chat_authorization=authorization)
        return {"table": f"public.{table}", "columns": sorted(collection.columns), "where": where, "changesOnly": True, "subscription": self.gateway._internal_subscription(synthetic_id(f"user/{account}"), identity, authority)}

    def window_scope(self, table, ordinal, windows):
        start = datetime.date(2026, 9, 1) + datetime.timedelta(days=ordinal % windows)
        end = start + datetime.timedelta(days=14)
        date_range = {"and": [{"col": "date_value", "op": "gte", "value": start.isoformat()}, {"col": "date_value", "op": "lt", "value": end.isoformat()}]}
        records = {"col": "id", "in": {"table": "record_date_index", "project": "record_id", "where": date_range}}
        if table == "record":
            return records
        if table in {"assignment", "record_recurrence", "record_date_index", "record_assignee_index", "record_completed_index", "workflow_run"}:
            return {"col": "record_id", "in": {"table": "record", "project": "id", "where": records}}
        if table == "graph_edge":
            return {"col": "from_id", "in": {"table": "record", "project": "id", "where": records}}
        collection = self.gateway.registered_collection(table, allow_chat_projection_dependency=True)
        if "created_at" in collection.columns:
            return {"col": "created_at", "op": "gte", "value": f"{start.isoformat()}T00:00:00Z"}
        return {"col": "id", "op": "neq", "value": str(synthetic_id(f"scope/{ordinal}"))}

    async def generate(self, day="nov24", scale=1, windows=None):
        target = TARGETS[day]
        windows = windows or (28 if day == "nov23" else 56)
        requests = []
        table_counts = apportion(round(target["subquery_shapes"] * scale), SUBQUERY_TABLE_WEIGHTS)
        account_weights = dict(enumerate(ACCOUNT_WEIGHTS))
        for table, count in table_counts.items():
            for account, account_count in apportion(count, account_weights).items():
                for ordinal in range(account_count):
                    scope = self.window_scope(table, ordinal, windows)
                    scope = {"and": [scope, {"col": "id" if "id" in self.gateway.registered_collection(table, allow_chat_projection_dependency=True).columns else "record_id", "op": "neq", "value": str(synthetic_id(f"shape-identity/{day}/{table}/{account}/{ordinal}"))}]}
                    body = await self.body(table, account, scope, identity=f"{day}-{table}-{ordinal}", chat=table in {"message", "workflow_run"}, generation=ordinal // windows)
                    requests.append({"kind": "subquery-target", "account": account, "body": body, "has_subquery": contains_subquery(body["where"])})
        plain_weights = {}
        for table, weight in PLAIN_TABLE_WEIGHTS.items():
            body = await self.body(table, 0, plain=True, chat=table == "suggestion")
            if not contains_subquery(body["where"]):
                plain_weights[table] = weight
        for table, count in apportion(target["standalone"] + target["family_shapes"], plain_weights).items():
            for ordinal in range(count):
                account = ordinal % len(ACCOUNT_WEIGHTS)
                body = await self.body(table, account, {"col": "id", "op": "neq", "value": str(synthetic_id(f"plain-shape/{table}/{ordinal}"))}, identity=f"plain-{table}-{ordinal}", plain=True, chat=table == "suggestion")
                requests.append({"kind": "plain-target", "account": account, "body": body, "has_subquery": contains_subquery(body["where"])})
        return requests


async def main():
    folder = Path(__file__).parent
    output = folder / "results/fixture"
    output.mkdir(parents=True, exist_ok=True)
    gateway = GatewayShapes(folder / "indexed-source.json.gz")
    for day in TARGETS:
        requests = await gateway.generate(day)
        (output / f"{day}-requests.json").write_text(json.dumps(requests, separators=(",", ":")))
        print(day, len(requests), dict(Counter(request["body"]["table"] for request in requests)))


if __name__ == "__main__":
    asyncio.run(main())
