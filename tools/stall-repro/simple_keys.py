"""Variant S, the simplified audience-key model: plain feeds on each row's own columns.

`simple_feeds` returns the plain predicates one account holds on one keyed table, one shape per Space
or calendar in the account's literal lists. `account_lists` reads those lists from the database, the
way a gateway would when it builds a token. `predicate_sql` renders an engine predicate as SQL so
`parity.py` can compare S with today's gateway predicates inside one snapshot.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from uuid import UUID

from psycopg import sql

from catalog import RECORD_FAMILY, contains_subquery
from export_fixture import synthetic_id


SIMPLE_KEYED = (*RECORD_FAMILY, "calendar_event", "graph_edge")
# S: the proposal as written, every other table exactly as in the passing B-cold-key-edges run.
# S-edge-shares: S plus the direct-share Record id list on graph_edge, which S as written omits.
# S-rest-today: S, with every other table (chat included) on today's real gateway predicate.
# B-per-account: S with one stream per account and keyed table (OTTO-14032, q-keyed-stream-sharing):
# S's own row conditions, the account's Space list and direct-share ids as literal lists.
# S-assign-today: S, with `assignment` on today's gateway filter instead of S's keyed feed.
# S-chat-narrow: S, with chat keyed only where indexed main keys it (`_keyed_chat_demand` over
# `KEYED_CHAT_COLLECTIONS`, api/src/api/services/circuits_sync.py at 90893c5): `suggestion` for any
# demand without a subquery, `message` only for a thread's target pair or one message id, and
# `workflow_run` only for one thread's runs. Every `message` and `workflow_run` demand in the
# synthetic catalog is a created_at-windowed history read naming no thread, so all of them keep
# today's filter; `suggestion` stays keyed as in S.
SIMPLE_VARIANTS = {"S", "S-edge-shares", "S-rest-today", "B-per-account", "S-assign-today", "S-chat-narrow"}
NARROW_CHAT_TODAY = {"message", "workflow_run"}
CHAT_TABLES = {"message", "workflow_run", "suggestion"}
IMPORT_REST_TABLES = ("message", "workflow_run", "suggestion", "resource", "resource_fact", "file", "user")
HIDDEN_RECORD_TYPES = ("busy_block", "legacy_preference", "family_context_report")
ASSIGNMENT_VARIABLES = ("calendar", "origin", "driver")


@dataclass(frozen=True)
class AccountLists:
    account: int
    user: str
    member_spaces: tuple[str, ...]
    partner_spaces: tuple[str, ...]
    grantors: tuple[str, ...]
    partner_calendars: tuple[str, ...]
    shared_records: tuple[str, ...]

    @property
    def spaces(self):
        return tuple(dict.fromkeys((*self.member_spaces, *self.partner_spaces)))


def eq(column, value):
    return {"col": column, "op": "eq", "value": value}


def neq(column, value):
    return {"col": column, "op": "neq", "value": value}


def null(column, is_null=True):
    return {"col": column, "isNull": is_null}


def one_of(column, values):
    values = list(values)
    return eq(column, values[0]) if len(values) == 1 else {"or": [eq(column, value) for value in values]}


def all_of(*conditions):
    return {"and": list(conditions)}


UNSCOPED = (null("scope_type"), null("scope_id"))


def edge_kinds(gateway):
    """The edge-type lists of today's gateway, read from its own source."""
    module = gateway.loader.module("api.types.graph_edge")
    return {
        "roles": list(module.RECORD_ROLE_EDGE_TYPE_VALUES),
        "mutations": ["generated", "updated"],
        "forward": list(module.CROSS_HOME_FORWARD_RESOURCE_EDGE_TYPES),
        "bidirectional": list(module.CROSS_HOME_BIDIRECTIONAL_RESOURCE_EDGE_TYPES),
        "generic": list(module.GENERIC_SYNC_EDGE_TYPES),
    }


def edge_feed(kinds, space, spaces, generic_spaces=None):
    """One Space's graph_edge feed: today's five branches as conditions on the edge's own columns.

    `spaces` is the account's whole list, for branches that test both ends. `generic_spaces` narrows
    the generic and relates_to branches (parity alternate only); S uses the whole list.
    """
    generic_spaces = spaces if generic_spaces is None else generic_spaces
    branches = [
        all_of(*UNSCOPED, eq("from_type", "record"), eq("to_type", "resource"), one_of("edge_type", kinds["roles"]), eq("from_space_id", space)),
        all_of(*UNSCOPED, eq("from_type", "record"), one_of("to_type", ["record", "resource"]), one_of("edge_type", kinds["mutations"]), eq("from_space_id", space), one_of("to_space_id", spaces)),
        all_of(null("scope_type"), eq("from_type", "resource"), eq("to_type", "resource"), one_of("edge_type", kinds["forward"]), eq("from_space_id", space)),
        all_of(null("scope_type"), eq("from_type", "resource"), eq("to_type", "resource"), one_of("edge_type", kinds["bidirectional"]), eq("to_space_id", space)),
    ]
    if space in generic_spaces:
        branches += [
            all_of(eq("from_space_id", space), one_of("to_space_id", generic_spaces), {"or": [null("scope_type"), all_of(eq("scope_type", "space"), one_of("scope_id", generic_spaces))]}, one_of("edge_type", kinds["generic"])),
            all_of(*UNSCOPED, eq("from_type", "record"), eq("to_type", "resource"), eq("edge_type", "relates_to"), eq("from_space_id", space), one_of("to_space_id", generic_spaces)),
        ]
    return all_of(null("deleted_at"), {"or": branches})


def edge_share_feed(kinds, lists):
    """Edges on directly shared Records, by literal Record id. Not in S as written."""
    ids, spaces = lists.shared_records, lists.spaces
    return all_of(null("deleted_at"), *UNSCOPED, eq("from_type", "record"), {"or": [
        all_of(eq("to_type", "resource"), one_of("edge_type", kinds["roles"]), one_of("from_id", ids)),
        all_of(one_of("edge_type", kinds["mutations"]), one_of("from_id", ids), one_of("to_type", ["record", "resource"]), {"or": [one_of("to_space_id", spaces), all_of(eq("to_type", "record"), one_of("to_id", ids))]}),
        all_of(one_of("edge_type", kinds["mutations"]), one_of("from_space_id", spaces), eq("to_type", "record"), one_of("to_id", ids)),
    ]})


EVENT_ROW_STATE = (eq("is_busy_block", False), {"or": [all_of(null("deleted_at"), neq("status", "cancelled")), all_of(eq("status", "cancelled"), null("original_start_time", False))]})


def simple_feeds(gateway, table, lists, edge_shares=False, generic_spaces=None):
    """The (key, predicate) feeds an account holds on one keyed table under variant S."""
    feeds = []
    kinds = edge_kinds(gateway)
    if table == "calendar_event":
        feeds.append((f"user:{lists.user}", all_of(eq("user_id", lists.user), *EVENT_ROW_STATE)))
        feeds.extend((f"calendar:{calendar}", all_of(eq("calendar_id", calendar), *EVENT_ROW_STATE)) for calendar in lists.partner_calendars)
        return feeds
    for space in lists.spaces:
        if table == "record":
            predicate = all_of(eq("space_id", space), {"or": [null("type"), all_of(*(neq("type", hidden) for hidden in HIDDEN_RECORD_TYPES))]})
        elif table == "assignment":
            predicate = all_of(eq("space_id", space), one_of("variable", ASSIGNMENT_VARIABLES))
        elif table == "graph_edge":
            predicate = edge_feed(kinds, space, lists.spaces, generic_spaces)
        else:
            predicate = eq("space_id", space)
        feeds.append((f"space:{space}", predicate))
    if lists.shared_records and table in RECORD_FAMILY:
        shared = one_of("id" if table == "record" else "record_id", lists.shared_records)
        feeds.append(("shared", all_of(shared, one_of("variable", ASSIGNMENT_VARIABLES)) if table == "assignment" else shared))
    if lists.shared_records and table == "graph_edge" and edge_shares:
        feeds.append(("shared", edge_share_feed(kinds, lists)))
    return feeds


def edge_feed_per_account(kinds, spaces):
    """`edge_feed` for every Space in the list at once: each `= space` becomes `IN (spaces)`."""
    branches = [
        all_of(*UNSCOPED, eq("from_type", "record"), eq("to_type", "resource"), one_of("edge_type", kinds["roles"]), one_of("from_space_id", spaces)),
        all_of(*UNSCOPED, eq("from_type", "record"), one_of("to_type", ["record", "resource"]), one_of("edge_type", kinds["mutations"]), one_of("from_space_id", spaces), one_of("to_space_id", spaces)),
        all_of(null("scope_type"), eq("from_type", "resource"), eq("to_type", "resource"), one_of("edge_type", kinds["forward"]), one_of("from_space_id", spaces)),
        all_of(null("scope_type"), eq("from_type", "resource"), eq("to_type", "resource"), one_of("edge_type", kinds["bidirectional"]), one_of("to_space_id", spaces)),
        all_of(one_of("from_space_id", spaces), one_of("to_space_id", spaces), {"or": [null("scope_type"), all_of(eq("scope_type", "space"), one_of("scope_id", spaces))]}, one_of("edge_type", kinds["generic"])),
        all_of(*UNSCOPED, eq("from_type", "record"), eq("to_type", "resource"), eq("edge_type", "relates_to"), one_of("from_space_id", spaces), one_of("to_space_id", spaces)),
    ]
    return all_of(null("deleted_at"), {"or": branches})


def per_account_feed(gateway, table, lists):
    """The one predicate an account holds on a keyed table under B-per-account: S's feeds, ORed into one."""
    spaces, shared = lists.spaces, lists.shared_records
    if table == "calendar_event":
        owners = [eq("user_id", lists.user), *([one_of("calendar_id", lists.partner_calendars)] if lists.partner_calendars else [])]
        return all_of(owners[0] if len(owners) == 1 else {"or": owners}, *EVENT_ROW_STATE)
    if table == "graph_edge":
        return edge_feed_per_account(edge_kinds(gateway), spaces)
    if table == "record":
        own = all_of(one_of("space_id", spaces), {"or": [null("type"), all_of(*(neq("type", hidden) for hidden in HIDDEN_RECORD_TYPES))]})
    elif table == "assignment":
        own = all_of(one_of("space_id", spaces), one_of("variable", ASSIGNMENT_VARIABLES))
    else:
        own = one_of("space_id", spaces)
    if not shared:
        return own
    direct = one_of("id" if table == "record" else "record_id", shared)
    if table == "assignment":
        direct = all_of(direct, one_of("variable", ASSIGNMENT_VARIABLES))
    return {"or": [own, direct]}


async def simple_bodies(gateway, variant, table, account, lists):
    # Today's filter, one shape per account and table, as `S-rest-today` builds it.
    if (variant == "S-assign-today" and table == "assignment") or (variant == "S-chat-narrow" and table in NARROW_CHAT_TODAY):
        return [await gateway.body(table, account, None, identity=f"today-{table}", chat=table in CHAT_TABLES)]
    if table in SIMPLE_KEYED and variant == "B-per-account":
        return [gateway._plain_body(table, account, per_account_feed(gateway, table, lists), "per-account", keyed=False)]
    if table in SIMPLE_KEYED:
        return [gateway._plain_body(table, account, predicate, f"simple:{key}", keyed=False) for key, predicate in simple_feeds(gateway, table, lists, edge_shares=variant == "S-edge-shares")]
    if variant == "S-rest-today":
        return [await gateway.body(table, account, None, identity=f"today-{table}", chat=table in CHAT_TABLES)]
    return await gateway.cold_bodies(table, account, key_all_edges=True)


async def simple_catalog(gateway, variant, requests, lists):
    """One request per account and feed, deduplicated like B and B-cold."""
    result = {}
    for table, account in dict.fromkeys((request["body"]["table"].removeprefix("public."), request["account"]) for request in requests):
        for body in await simple_bodies(gateway, variant, table, account, lists[account]):
            result[(account, table, str(body["where"]))] = {"kind": "simple-retained" if contains_subquery(body["where"]) else "simple-plain", "account": account, "body": body, "has_subquery": contains_subquery(body["where"])}
    return list(result.values())


def predicate_sql(predicate, alias):
    """Render an engine predicate as SQL over `alias` (the engine's leaves follow SQL's three-valued logic)."""
    if "and" in predicate or "or" in predicate:
        operator = "and" if "and" in predicate else "or"
        return sql.SQL("({})").format(sql.SQL(f" {operator.upper()} ").join(predicate_sql(child, alias) for child in predicate[operator]))
    if "not" in predicate:
        return sql.SQL("NOT ({})").format(predicate_sql(predicate["not"], alias))
    column = sql.Identifier(alias, predicate["col"])
    if "isNull" in predicate:
        return sql.SQL("{} IS NULL" if predicate["isNull"] else "{} IS NOT NULL").format(column)
    if "in" in predicate:
        subquery = predicate["in"]
        inner = f"{alias}i"
        where = predicate_sql(subquery["where"], inner) if subquery.get("where") else sql.SQL("TRUE")
        return sql.SQL("{} {} (SELECT {} FROM {} {} WHERE {})").format(column, sql.SQL("NOT IN" if predicate.get("negated") else "IN"), sql.Identifier(inner, subquery["project"]), sql.Identifier("public", subquery["table"].removeprefix("public.")), sql.Identifier(inner), where)
    operator = {"eq": "=", "neq": "<>", "lt": "<", "lte": "<=", "gt": ">", "gte": ">="}[predicate["op"]]
    return sql.SQL("{} {} {}").format(column, sql.SQL(operator), sql.Literal(predicate["value"]))


def query_ids(connection, lowered):
    """Run a lowered `{table, project, where}` select and return its ids as strings."""
    statement = sql.SQL("SELECT {} FROM {} q WHERE {}").format(sql.Identifier("q", lowered["project"]), sql.Identifier("public", lowered["table"].removeprefix("public.")), predicate_sql(lowered["where"], "q"))
    return tuple(str(row[0]) for row in connection.execute(statement).fetchall())


def account_lists(connection, gateway, account):
    """The account's literal lists: its Spaces, its grantors' Spaces and syncing calendars, and Records shared with it."""
    authority_module = gateway.loader.module("api.services.circuits_sync_authority")
    user_id = synthetic_id(f"user/{account}")
    lower = authority_module._lower_select
    grantors = query_ids(connection, lower(authority_module.active_all_editor_standing_grant_grantor_query(grantee_user_id=user_id)))
    partner_spaces = query_ids(connection, lower(gateway.loader.module("api.models.record").direct_partner_home_space_ids_query(user_id=user_id)))
    member_spaces = tuple(str(space) for space in gateway.authority(account).authorized_space_ids)
    live = {str(row[0]) for row in connection.execute("SELECT id FROM public.space WHERE deleted_at IS NULL")}
    calendars = tuple(str(row[0]) for row in connection.execute("SELECT id FROM public.calendar WHERE user_id = ANY(%s::uuid[]) AND deleted_at IS NULL AND should_sync ORDER BY id", (list(grantors),)))
    shared = tuple(str(row[0]) for row in connection.execute("SELECT DISTINCT record_id FROM public.record_share WHERE deleted_at IS NULL AND target_type = 'user' AND target_id = %s ORDER BY record_id", (user_id,)))
    return AccountLists(account, str(user_id), tuple(space for space in member_spaces if space in live), tuple(sorted(set(partner_spaces) - set(member_spaces))), tuple(sorted(grantors)), calendars, shared)


def today_authority(gateway, lists):
    """Today's gateway authority for the account, with its grantors as the database has them."""
    return replace(gateway.authority(lists.account), record_scoped=True, direct_calendar_grantor_user_ids=tuple(UUID(grantor) for grantor in lists.grantors))


async def today_predicate(gateway, table, lists):
    """What today's gateway admits for the account on `table`, with no client scope (no week filter)."""
    collection = gateway.gateway.registered_collection(table, allow_chat_projection_dependency=True)
    return await gateway.gateway.authorize_predicate(None, collection, UUID(lists.user), authority=today_authority(gateway, lists))
