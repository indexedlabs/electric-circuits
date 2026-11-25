"""Parity of variant S with today's gateway predicates, on the synthetic database, without an engine.

For every synthetic account and keyed table, in one repeatable-read snapshot, compare (a) the rows
today's gateway predicate admits with no client scope and (b) the union of the account's S feeds.
Phase `base` is the seeded database plus the first calendar import. Phase `fixtures` adds labelled
rows the seed lacks: every edge branch, partner and share cases, and the drift the model depends on
not having. Each label carries the differences it must produce, so a surprise fails the run.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime
import json
import os
from collections import Counter, defaultdict
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from catalog import RECORD_FAMILY, GatewayShapes
from data import IMPORT_ACCOUNT, load_data, row_id
from export_fixture import synthetic_id
from probes import require_isolated
from simple_keys import ASSIGNMENT_VARIABLES, HIDDEN_RECORD_TYPES, SIMPLE_KEYED, account_lists, edge_kinds, predicate_sql, simple_feeds, today_authority, today_predicate

ACCOUNTS = range(IMPORT_ACCOUNT + 1)
CHILDREN = tuple(table for table in RECORD_FAMILY if table != "record")
NOW = datetime.datetime(2026, 11, 24, tzinfo=datetime.timezone.utc)
# Reasons the brief expects: rows on deleted or hidden-type Records, or whose Space differs.
EXPECTED = ("on a deleted Record", "on a hidden-type Record", "Space differs from its Record's", "edge Space differs from its end's")


def member_readers(space):
    """Accounts that read account `space`'s Space: its member, then its partner grantees."""
    return space, [grantee for grantee in range(19) if (grantee + 1) % 18 == space and grantee < 18 or grantee == 18 and space == 17]


class Fixtures:
    """Labelled rows, with the (account, side) differences each must produce."""

    def __init__(self, connection, data):
        self.connection = connection
        self.data = data
        self.ordinal = 900_000
        self.labels = {}

    def add(self, label, table, account, a=(), b=(), drift=False, **overrides):
        self.ordinal += 1
        values = self.data.make(table, account, self.ordinal)
        if table in {"space"}:
            values["id"] = synthetic_id(f"fixture/{table}/{self.ordinal}")
        values.update(overrides)
        values = {column: Jsonb(value) if isinstance(value, dict) else value for column, value in values.items()}
        columns = list(values)
        self.connection.execute(sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(sql.Identifier("public", table), sql.SQL(",").join(map(sql.Identifier, columns)), sql.SQL(",").join(sql.Placeholder() * len(columns))), list(values.values()))
        primary = [column["name"] for column in self.data.columns[table] if column["primary_key"]]
        self.labels[(table, "/".join(str(values[name]) for name in primary))] = {"label": label, "drift": drift, "expect": {(account, "a") for account in a} | {(account, "b") for account in b}}
        return values

    def record(self, label, account, **overrides):
        return self.add(label, "record", account, **overrides)["id"]

    def children(self, label, account, record, only=CHILDREN, **overrides):
        for table in only:
            extra = {"completed_at": NOW} if table == "record_completed_index" else {}
            self.add(label, table, account, record_id=record, **extra, **overrides)

    def edge(self, label, account, edge_type, from_, to, spaces=None, **overrides):
        spaces = spaces or (account, account)
        self.add(label, "graph_edge", account, edge_type=edge_type, from_type=from_[0], from_id=from_[1], to_type=to[0], to_id=to[1], from_space_id=row_id("space", spaces[0]) if isinstance(spaces[0], int) else spaces[0], to_space_id=row_id("space", spaces[1]) if isinstance(spaces[1], int) else spaces[1], **overrides)


def coverage(fixtures, kinds):
    """Rows the seed lacks, to exercise every branch; healthy data, so each difference is the model's."""
    space = lambda account: row_id("space", account)
    resource = lambda account, ordinal=0: ("resource", row_id("resource", account, ordinal))
    f = fixtures
    for label, type_ in (("Record with no type", None), ("thread Record", "thread")):
        record = f.record(label, 0, type=type_)
        f.children(label, 0, record)
    for type_ in HIDDEN_RECORD_TYPES:
        f.record(f"{type_} Record", 0, type=type_)
    # Calendar events: today's own-row conditions.
    f.add("busy-block event", "calendar_event", 1, is_busy_block=True)
    f.add("deleted event", "calendar_event", 1, deleted_at=NOW)
    f.add("cancelled event", "calendar_event", 1, status="cancelled")
    f.add("cancelled exception", "calendar_event", 1, status="cancelled", original_start_time="2026-11-01T00:00:00Z")
    f.add("deleted cancelled exception", "calendar_event", 1, status="cancelled", original_start_time="2026-11-01T00:00:00Z", deleted_at=NOW)
    quiet = f.add("grantor calendar that does not sync", "calendar", 1, should_sync=False)["id"]
    f.add("event on a calendar that does not sync", "calendar_event", 1, calendar_id=quiet)
    gone = f.add("deleted grantor calendar", "calendar", 1, deleted_at=NOW)["id"]
    f.add("event on a deleted calendar", "calendar_event", 1, calendar_id=gone)
    mirrored = f.add("partner mirror copy", "calendar_event", 1, b=(0, 1))["id"]
    f.add("partner mirror row", "partner_calendar_event_mirror", 1, calendar_event_id=mirrored)
    f.add("event of another user on a grantor calendar", "calendar_event", 5, b=(0,), calendar_id=row_id("calendar", 1, 0))
    # Same-Space edges of every kind, open and deleted. Account 17 is Space 0's partner reader.
    every = sorted({*kinds["roles"], *kinds["mutations"], *kinds["forward"], *kinds["bidirectional"], *kinds["generic"], "relates_to", "holds_time_for"})
    for edge_type in every:
        for from_type in ("record", "resource"):
            for to_type in ("record", "resource"):
                for deleted in (None, NOW):
                    ends = [("record", f.record("edge end", 0)) if kind == "record" else ("resource", f.add("edge end", "resource", 0)["id"]) for kind in (from_type, to_type)]
                    role = edge_type in kinds["roles"] and (from_type, to_type) == ("record", "resource")
                    mutation = edge_type in kinds["mutations"] and from_type == "record"
                    related = edge_type in kinds["forward"] and (from_type, to_type) == ("resource", "resource")
                    generic = edge_type in kinds["generic"] or edge_type == "relates_to" and (from_type, to_type) == ("record", "resource")
                    broadened = deleted is None and generic and not (role or mutation or related)
                    f.edge(f"same-Space {edge_type} {from_type}→{to_type}{' (deleted)' if deleted else ''}", 0, edge_type, *ends, deleted_at=deleted, b=(17,) if broadened else ())
    live = ("record", f.record("edge end", 0))
    f.edge("same-Space generic edge scoped to its Space", 0, "tagged_with", live, resource(0), scope_type="space", scope_id=space(0), b=(17,))
    f.edge("same-Space generic edge scoped to another Space", 0, "tagged_with", live, resource(0, 1), scope_type="space", scope_id=space(5))
    f.edge("same-Space attendee edge with a scope", 0, "has_attendee", live, resource(0, 2), scope_type="space", scope_id=space(0))
    # Cross-Space edges: account 0 reads Spaces 0 (member) and 1 (partner); nobody in 0 or 17 reads 5.
    partner = ("record", f.record("edge end", 1))
    f.edge("generated edge to a partner's Record", 0, "generated", live, partner, spaces=(0, 1))
    f.edge("updated edge to a partner's Resource", 0, "updated", live, resource(1), spaces=(0, 1))
    f.edge("generated edge to an unreadable Record", 0, "generated", live, ("record", row_id("record", 5, 400)), spaces=(0, 5))
    f.edge("attendee Resource in an unreadable Space", 0, "has_attendee", live, resource(5), spaces=(0, 5))
    f.edge("cross-home parent_of", 0, "parent_of", resource(0), resource(5), spaces=(0, 5))
    f.edge("cross-home attends_school", 0, "attends_school", resource(0), resource(5), spaces=(0, 5))
    f.edge("generic edge into a partner's Space", 0, "tagged_with", live, partner, spaces=(0, 1), b=(0,))
    f.edge("relates_to a partner's Resource", 0, "relates_to", live, resource(1), spaces=(0, 1), b=(0,))
    # A partner reads the member's feed: removed and deleted Records stay in it.
    removed = f.record("removed Record", 0, removed_at=NOW, b=(17,))
    f.children("child of a removed Record", 0, removed, b=(17,))
    f.edge("attendee edge of a removed Record", 0, "has_attendee", ("record", removed), resource(0, 3), b=(17,))
    f.edge("updated edge from a removed Record", 0, "updated", ("record", removed), live, b=(17,))
    f.record("deleted Record with no children", 0, deleted_at=NOW, b=(17,))
    # Resource ends that today's filter drops and an edge's own columns cannot show.
    account_resource = f.add("account Resource", "resource", 0, resource_type="account")["id"]
    f.edge("family edge to an account Resource", 0, "parent_of", ("resource", account_resource), ("resource", account_resource), b=(17,))
    superseded = ("resource", f.add("superseded Resource", "resource", 0, superseded_by_resource_id=resource(0)[1])["id"])
    f.edge("family edge from a superseded Resource", 0, "parent_of", superseded, resource(0, 4), b=(17,))
    f.edge("cross-home family edge from a superseded Resource", 0, "parent_of", superseded, resource(5, 1), spaces=(0, 5), b=(0, 17, 5, 4))
    # Today's Resource rule keeps a grantor's archived homes; the Record rule, which fills the list, does not.
    archived = f.add("grantor's archived Space", "space", 1, archived_at=NOW)["id"]
    old = [("resource", f.add("Resource in an archived Space", "resource", 1, space_id=archived)["id"]) for _ in range(2)]
    f.edge("family edge in a grantor's archived Space", 1, "parent_of", *old, spaces=(archived, archived), a=(0,))
    f.edge("generated edge to a Resource in a grantor's archived Space", 0, "generated", live, old[0], spaces=(0, archived), a=(0,))
    # Direct shares to account 0 of Records in Space 5.
    share = lambda record: f.add("direct Record share", "record_share", 5, record_id=record, target_id=row_id("user", 0))
    shared = ("record", f.record("shared Record", 5))
    share(shared[1])
    f.children("child of a shared Record", 5, shared[1])
    f.edge("attendee edge of a shared Record", 5, "has_attendee", shared, resource(5, 2), a=(0,))
    f.edge("generated edge from an own Record to a shared Record", 0, "generated", live, shared, spaces=(0, 5), a=(0,))
    f.edge("updated edge between shared Records", 5, "updated", shared, shared, a=(0,))
    hidden = f.record("shared hidden-type Record", 5, type="busy_block", b=(0,))
    share(hidden)
    shared_removed = f.record("shared removed Record", 5, removed_at=NOW, b=(4,))
    share(shared_removed)
    f.children("child of a shared removed Record", 5, shared_removed, b=(0, 4))
    f.edge("attendee edge of a shared removed Record", 5, "has_attendee", ("record", shared_removed), resource(5, 3), b=(4,))
    shared_resource = resource(5, 60)
    f.add("direct Resource share", "resource_share", 5, resource_id=shared_resource[1], target_id=row_id("user", 0))
    f.edge("generated edge to a directly shared Resource", 0, "generated", live, shared_resource, spaces=(0, 5), a=(0,))
    f.add("assignment with another variable", "assignment", 0, record_id=live[1], variable="assignee", a=(0, 17))


def drift(fixtures):
    """The drift the model depends on not having: the check below must find every row."""
    f = fixtures
    resource = lambda account, ordinal=0: ("resource", row_id("resource", account, ordinal))
    live = f.record("drift parent", 0)
    f.children("child whose Space differs from its Record's", 0, live, space_id=row_id("space", 5), a=(0, 17), b=(5, 4), drift=True)
    deleted = f.record("deleted drift parent", 0, deleted_at=NOW, b=(17,))
    f.children("child on a deleted Record", 0, deleted, b=(0, 17), drift=True)
    f.edge("open attendee edge on a deleted Record", 0, "has_attendee", ("record", deleted), resource(0, 5), b=(0, 17), drift=True)
    f.edge("open updated edge on a deleted Record", 0, "updated", ("record", deleted), ("record", live), b=(0, 17), drift=True)
    # `generated` is also a generic edge type, so today's generic branch admits it to Space 0's member.
    f.edge("open generated edge on a deleted Record", 0, "generated", ("record", deleted), ("record", live), b=(17,), drift=True)
    hidden = f.record("hidden drift parent", 0, type="family_context_report")
    f.children("child on a hidden-type Record", 0, hidden, b=(0, 17), drift=True)
    f.edge("open attendee edge on a hidden-type Record", 0, "has_attendee", ("record", hidden), resource(0, 6), b=(0, 17), drift=True)
    f.edge("attendee edge whose Space differs from its Record's", 0, "has_attendee", ("record", live), resource(0, 7), spaces=(5, 0), a=(0, 17), b=(5, 4), drift=True)
    shared = f.record("deleted shared drift parent", 5, deleted_at=NOW, b=(4,))
    f.add("direct Record share", "record_share", 5, record_id=shared, target_id=row_id("user", 0))
    f.children("child on a deleted shared Record", 5, shared, b=(0, 5, 4), drift=True)


class Snapshot:
    """Everything one parity pass reads, inside one repeatable-read transaction."""

    def __init__(self, connection, gateway, columns):
        self.connection = connection
        self.gateway = gateway
        self.columns = columns
        self.kinds = edge_kinds(gateway)
        self.records = {str(row[0]): row[1:] for row in connection.execute("SELECT id, space_id::text, type, deleted_at, removed_at FROM public.record")}
        self.resources = {str(row[0]): row[1:] for row in connection.execute("SELECT id, space_id::text, resource_type, deleted_at, superseded_by_resource_id FROM public.resource")}
        self.mirrored = {str(row[0]) for row in connection.execute("SELECT calendar_event_id FROM public.partner_calendar_event_mirror")}
        self.calendar_owner = {str(row[0]): str(row[1]) for row in connection.execute("SELECT id, user_id FROM public.calendar")}
        self.resource_shares = {(str(row[0]), str(row[1])) for row in connection.execute("SELECT resource_id, target_id FROM public.resource_share WHERE deleted_at IS NULL")}
        self.lists = {account: account_lists(connection, gateway, account) for account in ACCOUNTS}

    def primary(self, table):
        return [column["name"] for column in self.columns[table] if column["primary_key"]]

    def compare(self, table, today, feeds):
        """Rows admitted by exactly one side, with the columns the reasons need."""
        wanted = {"record": ["space_id", "type", "deleted_at", "removed_at"], "calendar_event": ["user_id", "calendar_id"], "graph_edge": ["from_type", "from_id", "to_type", "to_id", "edge_type", "scope_type", "from_space_id", "to_space_id"]}.get(table, ["record_id", "space_id"])
        if table == "assignment":
            wanted = [*wanted, "variable"]
        primary = self.primary(table)
        a = sql.SQL("({}) IS TRUE").format(predicate_sql(today, "q"))
        b = sql.SQL("({}) IS TRUE").format(sql.SQL(" OR ").join(predicate_sql(feed, "q") for feed in feeds)) if feeds else sql.SQL("FALSE")
        statement = sql.SQL("SELECT a, b, {} FROM (SELECT q.*, {} AS a, {} AS b FROM {} q) w WHERE a OR b").format(sql.SQL(",").join(map(sql.Identifier, [*primary, *wanted])), a, b, sql.Identifier("public", table))
        admitted = Counter()
        differences = []
        for row in self.connection.execute(statement):
            admitted["a"] += row[0]
            admitted["b"] += row[1]
            if row[0] != row[1]:
                values = dict(zip([*primary, *wanted], row[2:]))
                differences.append(("a" if row[0] else "b", "/".join(str(values[name]) for name in primary), {name: str(value) if value is not None else None for name, value in values.items()}))
        return admitted, differences

    def reason(self, table, side, key, row, lists):
        """Why one side admits this row and the other does not; None when nothing here explains it."""
        partner = set(lists.partner_spaces)
        shared = set(lists.shared_records)
        if table == "calendar_event":
            if side == "b" and key in self.mirrored:
                return "partner-mirror copy: S has no mirror exclusion"
            if side == "b" and self.calendar_owner.get(row["calendar_id"]) != row["user_id"]:
                return "event's user is not its calendar's owner"
            return None
        if table == "record":
            if side != "b":
                return None
            if row["type"] in HIDDEN_RECORD_TYPES and key in shared:
                return "hidden-type Record in the direct-share list"
            if row["space_id"] in partner and row["deleted_at"]:
                return "partner reads the member feed: deleted Record row"
            if row["space_id"] in partner and row["removed_at"]:
                return "partner reads the member feed: removed Record row"
            return None
        if table in CHILDREN:
            space, type_, deleted, removed = self.records.get(row["record_id"], (None, None, None, None))
            if space is None:
                return "Record row is missing"
            if row["space_id"] != space:
                return "Space differs from its Record's"
            if side == "a":
                return "assignment variable outside calendar, origin, driver" if table == "assignment" and row["variable"] not in ASSIGNMENT_VARIABLES else None
            if deleted:
                return "on a deleted Record"
            if type_ in HIDDEN_RECORD_TYPES:
                return "on a hidden-type Record"
            if removed and space in partner:
                return "partner reads the member feed: child of a removed Record"
            if removed and row["record_id"] in shared:
                return "child of a removed Record in the direct-share list"
            return None
        return self.edge_reason(side, row, lists, partner, shared)

    def edge_reason(self, side, row, lists, partner, shared):
        ends = []
        for end in ("from", "to"):
            kind, identity, space = row[f"{end}_type"], row[f"{end}_id"], row[f"{end}_space_id"]
            actual = (self.records if kind == "record" else self.resources).get(identity)
            ends.append((kind, identity, space, actual))
            if actual and actual[0] != space:
                return "edge Space differs from its end's"
        records = [actual for kind, _, _, actual in ends if kind == "record" and actual]
        resources = [(identity, space, actual) for kind, identity, space, actual in ends if kind == "resource" and actual]
        role, mutation = row["edge_type"] in self.kinds["roles"], row["edge_type"] in self.kinds["mutations"]
        if side == "a":
            if (role or mutation) and any(identity in shared for kind, identity, _, _ in ends if kind == "record"):
                return "edge on a directly shared Record: S as written gives graph_edge no share list"
            if mutation and any((identity, lists.user) in self.resource_shares and space not in lists.spaces for identity, space, _ in resources):
                return "generated/updated edge to a directly shared Resource in an unlisted Space"
            if any(space not in lists.spaces for _, space, _ in resources):
                return "Resource in a grantor's archived or removed Space, which the Record rule leaves out of the list"
            return None
        if (role or mutation) and any(actual[2] for actual in records):
            return "on a deleted Record"
        if role and any(actual[1] in HIDDEN_RECORD_TYPES for actual in records):
            return "on a hidden-type Record"
        if any(actual[2] or actual[3] for _, _, actual in resources):
            return "on a deleted or superseded Resource"
        if any(actual[0] in partner and actual[3] for actual in records):
            return "partner reads the member feed: edge of a removed Record"
        if any(actual[0] in partner and actual[1] == "account" for _, _, actual in resources):
            return "partner reads the member feed: edge of an account Resource"
        if row["edge_type"] in [*self.kinds["generic"], "relates_to"] and {row["from_space_id"], row["to_space_id"]} & partner:
            return "partner reads the member feed: generic or relates_to edge in a grantor's Space"
        return None

    async def run(self):
        """Every account and keyed table, plus the two graph_edge readings S leaves open."""
        tables = {}
        differences = []
        alternates = {"graph_edge with the direct-share Record list": {"edge_shares": True}, "graph_edge with generic and relates_to tested against member Spaces only": {"generic": True}, "both": {"edge_shares": True, "generic": True}}
        alternate_counts = {name: Counter() for name in alternates}
        for table in SIMPLE_KEYED:
            totals, reasons, rows = Counter(), {"a": Counter(), "b": Counter()}, {"a": set(), "b": set()}
            for account, lists in self.lists.items():
                today = await today_predicate(self.gateway, table, lists)
                admitted, found = self.compare(table, today, [predicate for _, predicate in simple_feeds(self.gateway, table, lists)])
                totals.update(admitted)
                for side, key, row in found:
                    reason = self.reason(table, side, key, row, lists)
                    reasons[side][reason or "UNEXPLAINED"] += 1
                    rows[side].add(key)
                    differences.append({"account": account, "table": table, "side": side, "key": key, "reason": reason, "expected_by_brief": reason in EXPECTED})
                if table == "graph_edge":
                    for name, options in alternates.items():
                        _, other = self.compare(table, today, [predicate for _, predicate in simple_feeds(self.gateway, table, lists, edge_shares=options.get("edge_shares", False), generic_spaces=lists.member_spaces if options.get("generic") else None)])
                        alternate_counts[name].update(side for side, _, _ in other)
            tables[table] = {
                "admitted_a": totals["a"], "admitted_b": totals["b"],
                "a_only": sum(reasons["a"].values()), "b_only": sum(reasons["b"].values()),
                "a_only_rows": len(rows["a"]), "b_only_rows": len(rows["b"]),
                "examples_a_only": sorted(rows["a"])[:5], "examples_b_only": sorted(rows["b"])[:5],
                "reasons_a_only": dict(reasons["a"]), "reasons_b_only": dict(reasons["b"]),
            }
        return {"tables": tables, "differences": differences, "graph_edge_alternates": {name: {"a_only": counts["a"], "b_only": counts["b"]} for name, counts in alternate_counts.items()}, "drift_check": self.drift_check()}

    def drift_check(self):
        """The check the model needs running in production: no account, only rows against their parents."""
        hidden = sql.SQL(",").join(map(sql.Literal, HIDDEN_RECORD_TYPES))
        found = defaultdict(list)
        for table in CHILDREN:
            key = sql.SQL(" || '/' || ").join(sql.SQL("c.{}::text").format(sql.Identifier(name)) for name in self.primary(table))
            statement = sql.SQL("SELECT {}, CASE WHEN r.id IS NULL THEN 'Record row is missing' WHEN r.space_id IS DISTINCT FROM c.space_id THEN 'Space differs from its Record''s' WHEN r.deleted_at IS NOT NULL THEN 'on a deleted Record' ELSE 'on a hidden-type Record' END FROM {} c LEFT JOIN public.record r ON r.id = c.record_id WHERE r.id IS NULL OR r.space_id IS DISTINCT FROM c.space_id OR r.deleted_at IS NOT NULL OR r.type IN ({})").format(key, sql.Identifier("public", table), hidden)
            for identity, why in self.connection.execute(statement):
                found[table].append([identity, why])
        roles, mutations = self.kinds["roles"], self.kinds["mutations"]
        ends = [
            ("from", "record", "edge Space differs from its end's", "e.from_space_id IS DISTINCT FROM p.space_id", None),
            ("to", "record", "edge Space differs from its end's", "e.to_space_id IS DISTINCT FROM p.space_id", None),
            ("from", "resource", "edge Space differs from its end's", "e.from_space_id IS DISTINCT FROM p.space_id", None),
            ("to", "resource", "edge Space differs from its end's", "e.to_space_id IS DISTINCT FROM p.space_id", None),
            ("from", "record", "on a deleted Record", "p.deleted_at IS NOT NULL", [*roles, *mutations]),
            ("to", "record", "on a deleted Record", "p.deleted_at IS NOT NULL", mutations),
            ("from", "record", "on a hidden-type Record", f"p.type IN ({','.join(repr(name) for name in HIDDEN_RECORD_TYPES)})", roles),
            ("from", "resource", "on a deleted or superseded Resource", "(p.deleted_at IS NOT NULL OR p.superseded_by_resource_id IS NOT NULL)", self.kinds["forward"]),
            ("to", "resource", "on a deleted or superseded Resource", "(p.deleted_at IS NOT NULL OR p.superseded_by_resource_id IS NOT NULL)", [*self.kinds["forward"], *mutations]),
        ]
        for end, kind, why, condition, types in ends:
            only = sql.SQL(" AND e.edge_type IN ({})").format(sql.SQL(",").join(map(sql.Literal, types))) if types else sql.SQL("")
            statement = sql.SQL("SELECT e.id::text FROM public.graph_edge e JOIN {} p ON p.id = e.{} WHERE e.deleted_at IS NULL AND e.{} = {} AND {}{}").format(sql.Identifier("public", kind), sql.Identifier(f"{end}_id"), sql.Identifier(f"{end}_type"), sql.Literal(kind), sql.SQL(condition), only)
            found["graph_edge"].extend([identity, why] for (identity,) in self.connection.execute(statement))
        return {table: {"rows": len({identity for identity, _ in rows}), "reasons": dict(Counter(why for _, why in rows)), "examples": sorted({identity for identity, _ in rows})[:5], "ids": sorted({identity for identity, _ in rows})} for table, rows in found.items() if rows}


def check_fixtures(result, labels):
    """Each labelled row must produce exactly its stated differences, and every drift row must be found."""
    actual = defaultdict(set)
    for difference in result["differences"]:
        actual[(difference["table"], difference["key"])].add((difference["account"], difference["side"]))
    surprises = []
    for key, entry in labels.items():
        if key[0] in SIMPLE_KEYED and actual.get(key, set()) != entry["expect"]:
            surprises.append({"table": key[0], "key": key[1], "label": entry["label"], "expected": sorted(entry["expect"]), "actual": sorted(actual.get(key, set()))})
    drifted = {key for key, entry in labels.items() if entry["drift"]}
    found = {(table, identity) for table, rows in result["drift_check"].items() for identity in rows["ids"]}
    by_label = defaultdict(lambda: {"rows": 0, "a_only": 0, "b_only": 0, "accounts_a": set(), "accounts_b": set(), "reasons": set(), "tables": set(), "examples": []})
    for difference in result["differences"]:
        entry = labels.get((difference["table"], difference["key"]))
        if entry is None:
            continue
        group = by_label[entry["label"]]
        group[f"{difference['side']}_only"] += 1
        group[f"accounts_{difference['side']}"].add(difference["account"])
        group["reasons"].add(difference["reason"] or "UNEXPLAINED")
        group["tables"].add(difference["table"])
        if difference["key"] not in group["examples"] and len(group["examples"]) < 5:
            group["examples"].append(difference["key"])
    for key, entry in labels.items():
        if entry["label"] in by_label:
            by_label[entry["label"]]["rows"] += 1
    return {"surprises": surprises, "drift_rows": len(drifted), "drift_rows_found": len(drifted & found), "drift_rows_missed": sorted(map(list, drifted - found)), "labels": {label: {name: sorted(value) if isinstance(value, set) else value for name, value in group.items()} for label, group in by_label.items()}}


async def snapshot(url, gateway, columns):
    with psycopg.connect(url) as connection:
        connection.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        connection.read_only = True
        version = connection.execute("SELECT version(), pg_current_snapshot()::text, now()").fetchone()
        result = await Snapshot(connection, gateway, columns).run()
        again = connection.execute("SELECT pg_current_snapshot()::text").fetchone()[0]
        return {"postgres": version[0], "snapshot": version[1], "snapshot_at_end": again, "taken_at": str(version[2]), **result}


async def main(arguments):
    require_isolated("http://127.0.0.1", arguments.postgres)
    folder = Path(__file__).parent
    arguments.output.mkdir(parents=True, exist_ok=True)
    gateway = GatewayShapes(folder / "indexed-source.json.gz")
    data = load_data(folder / "results/fixture/columns.json")
    data.padding = 16
    data.add_assignment_space(arguments.postgres)
    data.seed(arguments.postgres, shares=True)
    data.calendar_import(arguments.postgres, lambda connection, transaction: None)
    base = await snapshot(arguments.postgres, gateway, data.columns)
    (arguments.output / "base.json").write_text(json.dumps(base, indent=1, default=str) + "\n")
    with psycopg.connect(arguments.postgres) as connection:
        connection.execute("SET LOCAL session_replication_role = replica")
        fixtures = Fixtures(connection, data)
        coverage(fixtures, edge_kinds(gateway))
        drift(fixtures)
    extended = await snapshot(arguments.postgres, gateway, data.columns)
    extended["fixtures"] = check_fixtures(extended, fixtures.labels)
    (arguments.output / "fixtures.json").write_text(json.dumps(extended, indent=1, default=str) + "\n")
    failures = []
    for name, result in (("base", base), ("fixtures", extended)):
        unexplained = [difference for difference in result["differences"] if difference["reason"] is None]
        print(f"== {name}: {len(result['differences'])} differences, {len(unexplained)} unexplained")
        for table, row in result["tables"].items():
            print(f"{table:24} a={row['admitted_a']:6} b={row['admitted_b']:6} a_only={row['a_only']:5} b_only={row['b_only']:5} {row['reasons_a_only']} {row['reasons_b_only']}")
        print("graph_edge alternates", result["graph_edge_alternates"])
        print("drift check", {table: rows["reasons"] for table, rows in result["drift_check"].items()})
        if unexplained:
            failures.append(f"{name}: {len(unexplained)} unexplained")
            print(json.dumps(unexplained[:20], indent=1))
    report = extended["fixtures"]
    print(f"fixtures: {len(fixtures.labels)} rows, {len(report['surprises'])} surprises, drift found {report['drift_rows_found']}/{report['drift_rows']}")
    if report["surprises"]:
        failures.append(f"{len(report['surprises'])} fixture surprises")
        print(json.dumps(report["surprises"][:20], indent=1))
    if report["drift_rows_missed"]:
        failures.append(f"{len(report['drift_rows_missed'])} drift rows missed")
    if base["drift_check"]:
        failures.append("drift in the base data")
    if failures:
        raise SystemExit("; ".join(failures))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--postgres", default=os.environ.get("STALL_REPRO_PG_URL"))
    parser.add_argument("--output", type=Path, default=Path(__file__).parent / "results/simple-keys/parity")
    asyncio.run(main(parser.parse_args()))
