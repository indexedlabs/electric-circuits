"""Coverage audit H4: shapes per table in a catalog, plain against subquery, and for plain shapes
whether the engine indexes them or keeps them on its scan list.

The placement mirrors `CompiledPredicate::access_leaf` (apps/engine/src/predicate.rs at 9bc1563): a
plain shape is indexed when its filter's top-level AND chain (nested ANDs included) has an equality
or range leaf on a non-NULL value; a top-level OR (even of one child; compile does not collapse it),
NOT, IN list, `!=`, LIKE, IS NULL or no filter leaves it on the scan list. Catalogs are rebuilt with
the code each run used; the account lists are stand-ins (the runs read theirs from the database).
"""
from __future__ import annotations

import asyncio
import json
import sys
from collections import Counter
from pathlib import Path

from catalog import GatewayShapes, contains_subquery
from export_fixture import synthetic_id

INDEXING_OPS = {"eq", "gt", "gte", "lt", "lte"}


def indexed(where) -> bool:
    if where is None:
        return False
    if "and" in where:
        return any(indexed(child) for child in where["and"])
    return where.get("op") in INDEXING_OPS and where.get("value") is not None and "in" not in where


def placement(body) -> str:
    where = body["where"]
    if contains_subquery(where):
        return "subquery"
    return "plain-indexed" if indexed(where) else "plain-scan"


def table_counts(requests):
    counts = Counter()
    for request in requests:
        counts[(request["body"]["table"].removeprefix("public."), placement(request["body"]))] += 1
    tables = sorted({table for table, _ in counts})
    rows = [(table, counts[(table, "plain-indexed")], counts[(table, "plain-scan")], counts[(table, "subquery")]) for table in tables]
    return rows


async def catalogs():
    import run
    from simple_keys import AccountLists, main3_catalog, simple_catalog
    folder = Path(__file__).parent
    pinned = GatewayShapes(folder / "indexed-source.json.gz")
    main = GatewayShapes(folder / "indexed-source-52e79b8.json.gz")
    requests = await pinned.generate("nov24")
    record_sync = main.loader.module("api.models.record_sync_authority")
    def partner(account):
        return (account + 1) % 18 if account < 18 else 17 if account == 18 else None
    def lists(account):
        p = partner(account)
        return AccountLists(account, str(synthetic_id(f"user/{account}")), (str(synthetic_id(f"space/{account}")),), (str(synthetic_id(f"space/{p}")),) if p is not None else (), (str(synthetic_id(f"user/{p}")),) if p is not None else (), tuple(str(synthetic_id(f"calendar/{p}/{i}")) for i in range(15)) if p is not None else (), ())
    def keyed(account):
        entry = lists(account)
        return record_sync.KeyedAudienceLists(space_ids=(synthetic_id(f"space/{account}"),), partner_space_ids=tuple(synthetic_id(f"space/{partner(account)}") for _ in [0] if partner(account) is not None), calendar_ids=tuple(synthetic_id(f"calendar/{account}/{i}") for i in range(15)), partner_calendar_ids=tuple(synthetic_id(c) if False else __import__("uuid").UUID(c) for c in entry.partner_calendars), shared_record_ids=(), shared_resource_ids=(), generation="h4")
    yield "B (M3's audience-key prototype, all-plain)", await pinned.keyed_catalog(requests, preserved=run.preserved_tables("B"))
    yield "S (simplified plain keys)", await simple_catalog(pinned, "S", requests, {account: lists(account) for account in range(20)})
    keyed_lists = {account: keyed(account) for account in range(20)}
    yield "S-main3 (main's lowering, keyed flags on)", await main3_catalog(main, requests, keyed_lists, True)
    yield "S-main3-keyed-all (graph_edge, assignment, day-window record/rdi keyed too)", await main3_catalog(main, requests, keyed_lists, True, keyed_all=True)


async def main():
    out = []
    async for name, catalog in catalogs():
        rows = table_counts(catalog)
        total = [sum(column) for column in zip(*[row[1:] for row in rows])]
        out.append({"catalog": name, "shapes": len(catalog), "rows": rows, "totals": total})
    json.dump(out, sys.stdout, indent=1)


if __name__ == "__main__" and len(sys.argv) == 1:
    asyncio.run(main())


def leaf_column(where):
    """The column of the leaf `access_leaf` would pick: an equality first, else the first range."""
    found = []
    def walk(node):
        if node is None:
            return
        if "and" in node:
            for child in node["and"]:
                walk(child)
        elif node.get("op") in INDEXING_OPS and node.get("value") is not None and "in" not in node:
            found.append((node["op"] != "eq", node["col"]))
    walk(where)
    return sorted(found)[0][1] if found else None


def inner_tables(where, out=None):
    out = set() if out is None else out
    if isinstance(where, dict):
        if "in" in where and isinstance(where["in"], dict):
            out.add(where["in"]["table"].removeprefix("public."))
            inner_tables(where["in"].get("where"), out)
        for key, value in where.items():
            if key != "in":
                inner_tables(value, out)
    elif isinstance(where, list):
        for value in where:
            inner_tables(value, out)
    return out


def per_table(requests):
    """Per outer table: indexed shapes by leaf column, scan-list shapes, subquery shapes by inner table."""
    tables = {}
    for request in requests:
        body = request["body"]
        entry = tables.setdefault(body["table"].removeprefix("public."), {"indexed": Counter(), "scan": 0, "subquery": 0, "inner": Counter()})
        kind = placement(body)
        if kind == "plain-indexed":
            entry["indexed"][leaf_column(body["where"])] += 1
        elif kind == "plain-scan":
            entry["scan"] += 1
        else:
            entry["subquery"] += 1
            for inner in inner_tables(body["where"]):
                entry["inner"][inner] += 1
    return tables


async def detail():
    folder = Path(__file__).parent
    pinned = GatewayShapes(folder / "indexed-source.json.gz")
    requests = await pinned.generate("nov24")
    out = {"A (Nov 24 fixture, today's gateway)": per_table(requests)}
    async for name, catalog in catalogs():
        if name.startswith("S ("):
            out[name] = per_table(catalog)
    json.dump(out, sys.stdout, indent=1, default=dict)


if __name__ == "__main__" and len(sys.argv) > 1 and sys.argv[1] == "detail":
    asyncio.run(detail())
