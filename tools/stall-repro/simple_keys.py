"""Variant S, the simplified audience-key model: plain feeds on each row's own columns.

`simple_feeds` returns the plain predicates one account holds on one keyed table, one shape per Space
or calendar in the account's literal lists. `account_lists` reads those lists from the database, the
way a gateway would when it builds a token. `predicate_sql` renders an engine predicate as SQL so
`parity.py` can compare S with today's gateway predicates inside one snapshot.
"""

from __future__ import annotations

import datetime
import json
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
# S-chat-thread: S-chat-narrow with the fixture's chat demands replaced by the ones phones send
# (indexed main 2cd606c5: NativeCircuitsRecordRailPredicates.swift, web/src/lib/chat/demands.ts):
# a Record thread's Messages, a run thread's workflow_runs and one Message by id, each keyed as
# main lowers it. Cards (`suggestion`) stay as S keys them.
SIMPLE_VARIANTS = {"S", "S-edge-shares", "S-rest-today", "B-per-account", "S-assign-today", "S-chat-narrow", "S-chat-thread"}
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


def any_equal(column, values):
    """Main's `_any_equal`: always an OR, and no values matches no row."""
    values = list(values)
    if not values:
        return all_of(null(column), null(column, False))
    return {"or": [eq(column, value) for value in values]}


def keyed_message_authorization(lists, message_id=None):
    """Main's keyed `message` authorization (`_chat_authorization_predicate` over
    `keyed_message_target_predicates`, circuits_sync_authority.py at 2cd606c5), with the by-id
    demand's Space-posted branch."""
    records = [any_equal("space_id", lists.spaces)]
    if lists.shared_records:
        records.append(any_equal("target_id", lists.shared_records))
    targets = [all_of(eq("target_type", "record"), {"or": records}), all_of(eq("target_type", "composer_submission"), eq("user_id", lists.user))]
    if message_id is not None and lists.member_spaces:
        targets.append(all_of(eq("id", message_id), eq("target_type", "space"), {"or": [eq("target_id", space) for space in sorted(lists.member_spaces)]}))
    return all_of(null("deleted_at"), {"or": targets})


async def chat_thread_bodies(gateway, table, account, ordinal, lists):
    """One fixture `message` or `workflow_run` demand as the demand a phone sends, keyed as main does.

    The ordinal-th `message` demand becomes the Record thread of the account's ordinal-th Record,
    or every third one a single-Message-id demand; the ordinal-th `workflow_run` demand becomes that
    Record's run thread. A run thread keeps its client subquery on `workflow`, with main's keyed
    Workflow filter injected, and the run's Space on `audience_key` (the harness column S uses).
    """
    from data import row_id
    record = str(row_id("record", account, ordinal))
    if table == "message" and ordinal % 3 == 2:
        message_id = str(row_id("message", account, ordinal))
        where = all_of(eq("id", message_id), keyed_message_authorization(lists, message_id))
        return [gateway._plain_body(table, account, where, f"phone-message-id:{ordinal}")]
    if table == "message":
        where = all_of(all_of(eq("target_type", "record"), eq("target_id", record)), keyed_message_authorization(lists))
        return [gateway._plain_body(table, account, where, f"phone-record-thread:{ordinal}")]
    workflows = all_of(eq("record_id", record), all_of(null("record_id", False), any_equal("space_id", lists.spaces)))
    where = all_of({"col": "workflow_id", "in": {"table": "public.workflow", "project": "id", "where": workflows}}, any_equal("audience_key", lists.spaces))
    return [gateway._plain_body(table, account, where, f"phone-run-thread:{ordinal}")]


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
    if variant == "S-chat-thread":
        # Phones send only thread-named chat demands: map each fixture message/workflow_run demand,
        # in order per account, to one; the rest of the catalog follows S below.
        ordinals = {}
        for request in requests:
            table = request["body"]["table"].removeprefix("public.")
            if table not in NARROW_CHAT_TODAY:
                continue
            account = request["account"]
            ordinal = ordinals[(table, account)] = ordinals.get((table, account), -1) + 1
            for body in await chat_thread_bodies(gateway, table, account, ordinal, lists[account]):
                result[(account, table, str(body["where"]))] = {"kind": "phone-chat", "account": account, "body": body, "has_subquery": contains_subquery(body["where"])}
        requests = [request for request in requests if request["body"]["table"].removeprefix("public.") not in NARROW_CHAT_TODAY]
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


# S-main3: the demands phones send, lowered by indexed main's own gateway code at 52e79b875b with
# `circuits_keyed_scopes`, `circuits_keyed_index_tables` and `circuits_keyed_chat_rows` on. The run
# loads main's bundle (indexed-source-52e79b8.json.gz) and schema, so `gateway` here is main's.
# S-main3-off: the same phone demands lowered by main with all three flags off (today's filters).
# S-main3-live: S-main3 with only what phones hold now (session feeds, Home's today, tomorrow and
# recurring groups, thread and rail groups): no day window from earlier days, as if engine 9bc1563's
# idle-shape retirement had let every one of them go.
# S-main3-keyed-all: S-main3 with the pieces main still leaves on subquery filters keyed too
# (`graph_edge`, `assignment`, and the day-window `record` / `record_date_index` members); see
# `keyed_all_lowering`.
# S-main3-live-plus: S-main3-live with the `file` and `standing_grant_backfill_status` feeds phones
# hold: `file` (nil predicate) for the session on every account (RecordRailCircuitsSubscription.
# inventoryTables, the shell's TierThree), `standing_grant_backfill_status` (nil predicate) on the
# Settings family screen (SettingsFamilyView.swift:455), taken here as one account in ten.
MAIN3_VARIANTS = {"S-main3", "S-main3-off", "S-main3-live", "S-main3-keyed-all", "S-main3-live-plus"}
KEYED_ALL_TABLES = {"graph_edge", "assignment", "record", "record_date_index"}


def _strip_subqueries(predicate):
    """The predicate without its `in` leaves: an AND drops them, an OR drops branches that were only
    one. Returns None when nothing plain is left (no client filter: the whole keyed stream)."""
    if predicate is None:
        return None
    if "in" in predicate:
        return None
    for operator in ("and", "or"):
        if operator in predicate:
            kept = [child for child in (_strip_subqueries(member) for member in predicate[operator]) if child is not None]
            if not kept:
                return None
            return kept[0] if len(kept) == 1 else {operator: kept}
    return predicate


async def keyed_all_lowering(gateway, table, predicate, user, keyed_authority):
    """Plain keyed filters for the demands main still serves on subquery filters.

    - `graph_edge`: main's `keyed_authorization_predicate("graph_edge")` (PR 8879's
      `_keyed_graph_edge_predicate`, lowered but not served at 52e79b875b), ANDed with the phone's
      predicate; a day-window member's `in` into `record_date_index` is dropped (an edge has no date).
    - `assignment` (hypothetical, OTTO-14247's plan): main's index-table keyed filter
      (`keyed_authorization_predicate("record_date_index")`: `space_id` in the caller's Spaces or
      `record_id` in its shared Records), which reads the same two columns `assignment` has.
    - `record_date_index`: main's keyed lowering of the phone's predicate with its `in` into
      `record_recurrence` dropped, so `allRecordDateIndicesForDayWindow` keeps its own date window
      and `recurrenceDateIndicesForDayWindow` becomes the account's keyed index stream.
    - `record` (hypothetical for the window): a Record has no date of its own, so a day-window
      member becomes the account's keyed Record stream.
    """
    sync = gateway.gateway
    authority_module = gateway.authority_module
    collection = sync.registered_collection(table, allow_chat_projection_dependency=True)
    client = None if table == "record" else _strip_subqueries(predicate)
    if table in {"record", "record_date_index"}:
        return await sync.authorize_predicate(client, collection, user, authority=keyed_authority)
    authorization = authority_module.keyed_authorization_predicate(table="graph_edge" if table == "graph_edge" else "record_date_index", authority=keyed_authority.keyed)
    return authorization if client is None else {"and": [client, authorization]}


def main3_keyed_lists(connection, gateway, lists):
    """Main's `_read_keyed_audience_lists` (api/models/record_sync_authority.py) as SQL over the fixture,
    derived Resource access off: the caller's Spaces, partner homes, its own calendars, its partners'
    syncing calendars, the Records shared with it in live Spaces and its direct Resource shares."""
    record_sync = gateway.loader.module("api.models.record_sync_authority")
    user = lists.user
    def ids(statement, *parameters):
        return tuple(sorted({row[0] for row in connection.execute(statement, parameters).fetchall()}))
    member = tuple(sorted(UUID(space) for space in lists.member_spaces))
    return record_sync.KeyedAudienceLists(
        space_ids=member,
        partner_space_ids=tuple(UUID(space) for space in lists.partner_spaces if UUID(space) not in member),
        calendar_ids=ids("SELECT id FROM public.calendar WHERE user_id = %s AND deleted_at IS NULL", user),
        partner_calendar_ids=tuple(UUID(calendar) for calendar in lists.partner_calendars),
        shared_record_ids=ids("SELECT r.id FROM public.record r JOIN public.space s ON s.id = r.space_id AND s.deleted_at IS NULL WHERE r.id = ANY(%s::uuid[])", list(lists.shared_records)),
        shared_resource_ids=ids("SELECT resource_id FROM public.resource_share WHERE target_id = %s AND deleted_at IS NULL AND source = 'direct'", user),
        generation=f"stall-repro-{lists.account}",
    )


async def main3_lower(gateway, table, predicate, operation, account, keyed_lists, flags=True, keyed_all=False):
    """Lower one app demand the way main's `_authorize_collection_read` does with all three flags on.

    Returns (where, keyed). `operation` is "feed" (changes-only stream), "group" (a group member) or
    "subset" (one-shot query). A feed drops its date window before the keyed test; a group member keeps
    it, so it keys only when its filter reads no other table; a chat demand keys by `_keyed_chat_demand`.
    """
    sync = gateway.gateway
    authority_module = gateway.authority_module
    collection = sync.registered_collection(table, allow_chat_projection_dependency=True)
    user = UUID(str(synthetic_id(f"user/{account}")))
    chat = collection.chat_only or table in sync._CHAT_TABLES
    record_scoped = sync._uses_record_authority(collection, predicate) or (chat and table != "composer_submission")
    keyed_predicate = sync.without_date_window(predicate, table) if operation == "feed" and not chat else predicate
    scope = sync._chat_scope_from_predicate(predicate)
    if chat:
        keyed_demand = sync._keyed_chat_demand(collection, predicate, scope, chat_rows=True)
    else:
        keyed_demand = table in authority_module.KEYED_COLLECTIONS and (operation == "subset" or not sync._looks_into_another_table(keyed_predicate))
    keyed = flags and record_scoped and keyed_demand
    if keyed_all and not keyed and table in KEYED_ALL_TABLES:
        keyed_authority = replace(gateway.authority(account), record_scoped=True, keyed=authority_module.KeyedSyncAuthority(user_id=user, lists=keyed_lists, index_tables=True, chat_rows=True))
        return await keyed_all_lowering(gateway, table, predicate, user, keyed_authority), True
    base = replace(gateway.authority(account), record_scoped=record_scoped)
    authority = replace(base, keyed=authority_module.KeyedSyncAuthority(user_id=user, lists=keyed_lists, index_tables=True, chat_rows=True)) if keyed else base
    client = keyed_predicate if keyed else predicate
    if chat and table == "suggestion" and scope is not None and scope.get("source_context_type") == "record":
        # Main validates the thread against the database on admission (`_validate_exact_chat_scope`)
        # and then authorizes exactly the thread's cards; the harness takes that filter directly.
        authorization = sync._card_thread_predicate(scope["source_context_id"])
        where = await sync.authorize_predicate(client, collection, user, chat=True, chat_authorization=authorization, authority=authority)
    elif chat:
        authorization = await sync._chat_authorization_predicate(collection, user, authority=authority, scope=scope)
        where = await sync.authorize_predicate(client, collection, user, chat=True, chat_authorization=authorization, authority=authority)
    else:
        where = await sync.authorize_predicate(client, collection, user, authority=authority)
    return where, keyed


# The app's predicates (ios/Index at 52e79b875b), with the app time zone taken as UTC and Home's
# "today" fixed at 2026-09-15 inside the fixture's date range.
HOME_TODAY = datetime.date(2026, 9, 15)
FIXTURE_DAYS = 84


def _day(value):
    return value.isoformat()


def _instant(value):
    return f"{value.isoformat()}T00:00:00Z"


def _leaf(column, op, value):
    return {"col": column, "op": op, "value": value}


def _in(column, table, project, where=None):
    # Phones name the inner table bare (PredicateSubquery(table: "workflow")); main's thread test
    # (`_workflow_run_thread_record_id`) matches that spelling.
    subquery = {"table": table, "project": project}
    if where is not None:
        subquery["where"] = where
    return {"col": column, "in": subquery}


def app_date(start, end):
    """NativeCircuitsCalendarPredicates.recordDateIndicesForDayWindow."""
    return {"or": [
        all_of(_leaf("date_value", "lt", _day(end)), _leaf("end_date_value", "gte", _day(start))),
        all_of(_leaf("date_value", "gte", _day(start)), _leaf("date_value", "lt", _day(end)), null("end_date_value")),
        all_of(_leaf("datetime_value", "lt", _instant(end)), _leaf("end_datetime_value", "gte", _instant(start))),
        all_of(_leaf("datetime_value", "gte", _instant(start)), _leaf("datetime_value", "lt", _instant(end)), null("end_datetime_value")),
    ]}


def app_recurrences(start, end):
    """recurrencesForDayWindow."""
    return all_of({"or": [null("recurring_until"), _leaf("recurring_until", "gte", _instant(start - datetime.timedelta(days=2)))]}, _leaf("dtstart", "lt", _instant(end + datetime.timedelta(days=2))))


def app_override_records(start, end):
    """recurrenceOverrideRecords."""
    return all_of(_in("recurring_parent_id", "record_recurrence", "record_id", app_recurrences(start, end)), _leaf("original_start_time", "gte", _day(start - datetime.timedelta(days=2))), _leaf("original_start_time", "lt", _day(end + datetime.timedelta(days=2))))


def app_calendar_events(start, end):
    """calendarEventsForDayWindow."""
    non_recurring = {"or": [_leaf("is_recurring", "eq", False), null("is_recurring")]}
    return {"or": [
        all_of(null("original_start_time", False), _leaf("original_start_time", "gte", _day(start - datetime.timedelta(days=2))), _leaf("original_start_time", "lt", _day(end + datetime.timedelta(days=2)))),
        all_of(_leaf("is_recurring", "eq", True), {"or": [null("recurring_until"), _leaf("recurring_until", "gte", _day(start - datetime.timedelta(days=3)))]}),
        all_of(non_recurring, null("start_datetime"), _leaf("start_date", "lt", _day(end)), _leaf("end_date", "gte", _day(start))),
        all_of(non_recurring, null("start_datetime"), _leaf("start_date", "gte", _day(start)), _leaf("start_date", "lt", _day(end)), null("end_date")),
        all_of(non_recurring, null("start_date"), _leaf("start_datetime", "lt", _instant(end)), _leaf("end_datetime", "gte", _instant(start))),
        all_of(non_recurring, null("start_date"), _leaf("start_datetime", "gte", _instant(start)), _leaf("start_datetime", "lt", _instant(end)), null("end_datetime")),
    ]}


def app_day_window(start, end):
    """RecordDateIndexWindowDemand.windowDemands plus calendar_event: one day window's six group members."""
    return [
        ("record_date_index", {"or": [app_date(start, end), _in("record_id", "record_recurrence", "record_id", app_recurrences(start, end))]}),
        ("record", {"or": [_in("id", "record_date_index", "record_id", app_date(start, end)), _in("id", "record_recurrence", "record_id", app_recurrences(start, end)), app_override_records(start, end)]}),
        ("record_recurrence", app_recurrences(start, end)),
        ("assignment", _in("record_id", "record_date_index", "record_id", app_date(start, end))),
        ("graph_edge", all_of(eq("from_type", "record"), _in("from_id", "record_date_index", "record_id", app_date(start, end)), eq("edge_type", "has_attendee"))),
        ("calendar_event", app_calendar_events(start, end)),
    ]


def app_home_recurring(start, end):
    """Home's recurring window: groups [record_recurrence, record, record_date_index] and [overrides]."""
    return [
        ("record_recurrence", app_recurrences(start, end)),
        ("record", _in("id", "record_recurrence", "record_id", app_recurrences(start, end))),
        ("record_date_index", _in("record_id", "record_recurrence", "record_id", app_recurrences(start, end))),
        ("record", app_override_records(start, end)),
    ]


def app_session(account, spaces):
    """What the shell holds for the session as per-table feeds (AuthenticatedContentView)."""
    demands = [
        ("record", all_of(null("deleted_at"), null("archived_at"), null("removed_at"), null("parent_id"), null("recurring_parent_id"))),
        ("resource", None),
        ("suggestion", eq("status", "pending")),
        *((table, None) for table in ("calendar", "space", "user", "space_member", "assistant_state")),
    ]
    for space in spaces:
        demands += [
            ("resource", eq("space_id", space)),
            ("resource_fact", eq("space_id", space)),
            ("graph_edge", all_of({"or": [eq("from_space_id", space), eq("to_space_id", space)]}, eq("to_type", "resource"), {"or": [eq("from_type", "resource"), all_of(eq("from_type", "suggestion"), eq("edge_type", "accepted_as"))]})),
        ]
    return demands


def app_thread_rail(record):
    """recordRailCircuitsSubscription.projectionDemands: one group of four."""
    one = {"or": [eq("record_id", record)]}
    return [("assignment", one), ("graph_edge", all_of(eq("from_type", "record"), {"or": [eq("from_id", record)]}, eq("edge_type", "has_attendee"))), ("record_assignee_index", one), ("record_completed_index", one)]


def app_thread(record):
    """thread(id), grouped: record, message, suggestion and workflow_run."""
    return [
        ("record", {"or": [eq("id", record), eq("client_id", record)]}),
        ("message", all_of(eq("target_type", "record"), eq("target_id", record))),
        ("suggestion", all_of(eq("source_context_type", "record"), eq("source_context_id", record))),
        ("workflow_run", _in("workflow_id", "workflow", "id", eq("record_id", record))),
    ]


DAY_WINDOW_TABLES = {"record", "record_date_index", "record_recurrence", "assignment", "graph_edge", "calendar_event"}


def main3_app_demands(account, keyed_lists, fixture_counts, plus=False):
    """(table, predicate, operation, origin) one account's phones hold.

    Fixture demands map in order onto the shapes phones send: the i-th demand on a day-window table
    becomes that table's member of the day window i days into the fixture's range (day windows move
    daily and nothing retires an idle one); index-table demands become thread-rail groups, chat
    demands thread groups; everything else is the session's feeds, held once."""
    from data import row_id
    spaces = [str(space) for space in (*keyed_lists.space_ids, *keyed_lists.partner_space_ids)]
    demands = [(table, predicate, "feed", "session") for table, predicate in app_session(account, spaces)]
    if plus:
        demands.append(("file", None, "feed", "session-tier-three"))
        if account % 10 == 0:
            demands.append(("standing_grant_backfill_status", None, "feed", "settings-family"))
    for offset, origin in ((0, "home-today"), (1, "home-tomorrow")):
        start = HOME_TODAY + datetime.timedelta(days=offset)
        demands += [(table, predicate, "group", origin) for table, predicate in app_day_window(start, start + datetime.timedelta(days=1))]
    demands += [(table, predicate, "group", "home-recurring") for table, predicate in app_home_recurring(HOME_TODAY, HOME_TODAY + datetime.timedelta(days=2))]
    for table in DAY_WINDOW_TABLES:
        for ordinal in range(fixture_counts.get(table, 0)):
            start = datetime.date(2026, 9, 1) + datetime.timedelta(days=ordinal % FIXTURE_DAYS)
            demands += [(member, predicate, "group", f"day-window:{start}") for member, predicate in app_day_window(start, start + datetime.timedelta(days=1)) if member == table]
    rail = fixture_counts.get("record_assignee_index", 0) + fixture_counts.get("record_completed_index", 0)
    for ordinal in range(rail):
        demands += [(table, predicate, "group", "thread-rail") for table, predicate in app_thread_rail(str(row_id("record", account, ordinal)))]
    for ordinal in range(max(fixture_counts.get("message", 0), fixture_counts.get("workflow_run", 0))):
        demands += [(table, predicate, "group", "thread") for table, predicate in app_thread(str(row_id("record", account, ordinal)))]
    return demands


async def main3_bodies(gateway, account, keyed_lists, fixture_counts=None, flags=True, keyed_all=False, plus=False):
    """Every demand of one account's phone, lowered by main; duplicates (same table and filter) once."""
    result = {}
    for table, predicate, operation, origin in main3_app_demands(account, keyed_lists, fixture_counts or {}, plus):
        where, keyed = await main3_lower(gateway, table, predicate, operation, account, keyed_lists, flags, keyed_all)
        key = (table, json.dumps(where, sort_keys=True))
        if key in result:
            continue
        identity = f"main3:{len(result)}:{origin}:{operation}"
        result[key] = {"kind": "keyed" if keyed else "today", "account": account, "body": gateway._plain_body(table, account, where, identity, keyed=False), "has_subquery": contains_subquery(where), "demand": {"table": table, "operation": operation, "origin": origin, "keyed": keyed}}
    return list(result.values())


async def main3_catalog(gateway, requests, keyed_lists, flags=True, history=True, keyed_all=False, plus=False):
    counts = {}
    for request in requests:
        key = (request["account"], request["body"]["table"].removeprefix("public."))
        counts[key] = counts.get(key, 0) + 1
    catalog = []
    for account in sorted({account for account, _ in counts}):
        catalog += await main3_bodies(gateway, account, keyed_lists[account], {table: count for (owner, table), count in counts.items() if owner == account and (history or table not in DAY_WINDOW_TABLES)}, flags, keyed_all, plus)
    return catalog


# Coverage audit H1: N keyed `record` shapes as main lowers them (`keyed_authorization_predicate`
# through `authorize_predicate`, 52e79b875b), one whole-table feed per synthetic account, each
# account reading 1-3 of the 50 Spaces that hold Records plus two shared Records. With `half`, every
# other shape also carries a client `id = X` equality (a Record's own key), which the engine indexes.
H1_VARIANTS = {f"S-main3-h1-{count}": (count, False) for count in (40, 1000, 10000, 20000)} | {"S-main3-h1-10000-half": (10000, True)}
H1_DATA_ACCOUNTS = [*range(19), *range(20, 51)]


def h1_lists(gateway, ordinal):
    """One synthetic account's lists: its own home Space (unique, as every prod account has one),
    0-2 of the 50 Spaces that hold Records (so about 1-3 Spaces in all), and two Records shared with
    it alone. Every account's filter therefore differs, so the engine cannot fold two into one shape."""
    record_sync = gateway.loader.module("api.models.record_sync_authority")
    data_spaces = [H1_DATA_ACCOUNTS[(ordinal * step) % len(H1_DATA_ACCOUNTS)] for step in (1, 7)][: ordinal % 3]
    spaces = {UUID(str(synthetic_id(f"h1-home/{ordinal}")))} | {UUID(str(synthetic_id(f"space/{account}"))) for account in data_spaces}
    return record_sync.KeyedAudienceLists(
        space_ids=tuple(sorted(spaces)),
        partner_space_ids=(), calendar_ids=(), partner_calendar_ids=(),
        shared_record_ids=tuple(UUID(str(synthetic_id(f"h1-shared/{ordinal}/{index}"))) for index in range(2)),
        shared_resource_ids=(), generation=f"h1-{ordinal}",
    )


async def h1_body(gateway, ordinal, lists, client=None, identity=None):
    sync = gateway.gateway
    user = UUID(str(synthetic_id(f"user/h1/{ordinal}")))
    authority = replace(gateway.authority(0), record_scoped=True, keyed=gateway.authority_module.KeyedSyncAuthority(user_id=user, lists=lists, index_tables=True, chat_rows=True))
    where = await sync.authorize_predicate(client, sync.registered_collection("record", allow_chat_projection_dependency=True), user, authority=authority)
    return gateway._plain_body("record", 1000 + ordinal, where, identity or f"h1:{ordinal}", keyed=False)


async def h1_catalog(gateway, count, half):
    from data import row_id
    catalog = []
    for ordinal in range(count):
        client = {"col": "id", "op": "eq", "value": str(row_id("record", H1_DATA_ACCOUNTS[ordinal % len(H1_DATA_ACCOUNTS)], ordinal % 20))} if half and ordinal % 2 else None
        body = await h1_body(gateway, ordinal, h1_lists(gateway, ordinal), client)
        catalog.append({"kind": "h1-indexed" if client else "h1-scan", "account": 1000 + ordinal, "body": body, "has_subquery": contains_subquery(body["where"])})
    return catalog


# PLAIN-WORDS-3 run 1. The plain-test fix is HYPOTHETICAL and gateway-side: main does not do it.
# The harness applies it as a rewrite of the filter main's own code lowers (`unwrap_single_or`):
# every `{"or": [x]}` with exactly one child becomes `x`, at every level. Main's `_any_equal` writes
# a one-value list as a one-child OR and `keyed_authorization_predicate` wraps its paths in another,
# so this one rewrite is both halves of the fix (a one-value keyed list as a plain equality, and a
# one-child OR unwrapped). A one-family account's `record` filter then reads
# `AND(space_id = X, type test, stream identity)`, which the engine indexes under `space_id`.
def unwrap_single_or(predicate):
    if not isinstance(predicate, dict):
        return predicate
    for operator in ("and", "or"):
        if operator in predicate:
            children = [unwrap_single_or(child) for child in predicate[operator]]
            return children[0] if operator == "or" and len(children) == 1 else {operator: children}
    if "not" in predicate:
        return {"not": unwrap_single_or(predicate["not"])}
    if isinstance(predicate.get("in"), dict) and predicate["in"].get("where") is not None:
        return {**predicate, "in": {**predicate["in"], "where": unwrap_single_or(predicate["in"]["where"])}}
    return predicate


def access_leaf(where):
    """The leaf engine 9bc1563 indexes a standalone shape under (`CompiledPredicate::access_leaf`,
    predicate.rs:369-416): the first equality on a non-NULL value in the top-level AND chain, nested
    ANDs included, in order; else the first range bound; else None (the table's scan list)."""
    best = None
    def collect(node):
        nonlocal best
        if node is None:
            return
        if "and" in node:
            for child in node["and"]:
                collect(child)
        elif node.get("op") in {"eq", "gt", "gte", "lt", "lte"} and node.get("value") is not None and "in" not in node:
            if best is None or (best[1] != "eq" and node["op"] == "eq"):
                best = (node["col"], node["op"], node["value"])
    collect(where)
    return best


# S-main3-h1p-N-one: N one-family accounts (one Space, no partner Spaces, no shared Records), every
# filter lowered by main and then rewritten by the fix. Each account has its own home Space, so the
# N filters differ; 49 of them are the homes the commit writes to, and the bystander's is the 50th.
# S-main3-h1p-N-mix: prod's mix. 19 accounts in 20 as above; 1 in 20 keeps H1's lists (its home,
# 0-2 of the written Spaces and two shared Records), which the same rewrite leaves on the scan list.
# S-main3-h1p-N-one-today: the control. The same N one-family accounts as `-one`, lowered by main
# as it is today (no rewrite), so every filter is on the scan list and only the lowering differs.
H1P_COUNTS = (40, 1000, 10000, 20000)
H1P_VARIANTS = {f"S-main3-h1p-{count}-{name}": (count, mix, fixed) for count in H1P_COUNTS for name, mix, fixed in (("one", "one", True), ("mix", "mix", True), ("one-today", "one", False))}
MIX_EVERY = 20


def plain_words_account(ordinal, mix):
    """(multi, rank): whether this account is the 1 in 20 with several Spaces or shares, and its
    rank among the one-family accounts otherwise."""
    if mix == "mix" and ordinal % MIX_EVERY == MIX_EVERY - 1:
        return True, None
    return False, ordinal - (ordinal + 1) // MIX_EVERY if mix == "mix" else ordinal


def one_family_space(rank, ordinal):
    """A one-family account's only Space: one of the 49 written Spaces that are not the bystander's
    for the first 49 such accounts, its own unwritten home after that."""
    if rank < len(H1_DATA_ACCOUNTS) - 1:
        return UUID(str(synthetic_id(f"space/{H1_DATA_ACCOUNTS[rank + 1]}")))
    return UUID(str(synthetic_id(f"h1p-home/{ordinal}")))


def h1p_lists(gateway, ordinal, mix):
    multi, rank = plain_words_account(ordinal, mix)
    if multi:
        return h1_lists(gateway, ordinal)
    record_sync = gateway.loader.module("api.models.record_sync_authority")
    return record_sync.KeyedAudienceLists(space_ids=(one_family_space(rank, ordinal),), partner_space_ids=(), calendar_ids=(), partner_calendar_ids=(), shared_record_ids=(), shared_resource_ids=(), generation=f"h1p-{ordinal}")


async def h1p_catalog(gateway, count, mix, fixed):
    catalog = []
    for ordinal in range(count):
        body = await h1_body(gateway, ordinal, h1p_lists(gateway, ordinal, mix))
        if fixed:
            body = {**body, "where": unwrap_single_or(body["where"])}
        catalog.append({"kind": "h1p-indexed" if access_leaf(body["where"]) else "h1p-scan", "account": 1000 + ordinal, "body": body, "has_subquery": contains_subquery(body["where"])})
    return catalog


# PLAIN-WORDS-3 run 2: N live keyed `graph_edge` filters, as phones hold them (S-main3-live's phone
# model): per account one session Resource-edge feed per Space (`app_session`) and four open
# thread rails' attendee members (`app_thread_rail`), each ANDed after main's
# `keyed_authorization_predicate(table="graph_edge")` (`_keyed_graph_edge_predicate`, the same text
# at 52e79b875b and at main 8fe2aadd) and followed by main's stream identity, in the order main's
# `authorize_predicate` writes them. Accounts are prod's mix (`plain_words_account`): 19 in 20 with
# one Space, 1 in 20 with H1's lists. N counts today's filters; accounts are added until N is reached.
# - `today`: main's lowering as it is.
# - `fix`: run 1's HYPOTHETICAL plain-test rewrite (`unwrap_single_or`) over every filter.
# - `split` (HYPOTHETICAL, more than run 1's fix): `fix`, and for one-Space accounts a selective
#   equality written first, where the engine takes its leaf: a rail filter starts with its own
#   `from_id = R`; a session feed becomes two filters, one starting `from_space_id = S` and one
#   starting `to_space_id = S AND from_space_id != S`. Together they admit exactly the rows `fix`
#   admits. The 1 in 20 keep `fix`.
EDGE_COUNTS = (100, 1000, 5000)
EDGE_WAYS = ("today", "fix", "split")
EDGE_VARIANTS = {f"S-main3-e1-{count}-{way}": (count, way) for count in EDGE_COUNTS for way in EDGE_WAYS}
EDGE_RAILS = 4


def main_stream_identity(gateway, column="id"):
    """Main's `_stream_identity` for the harness authority every H1-style filter uses."""
    import hashlib
    identity = str(UUID(bytes=hashlib.sha256(gateway.authority(0).fingerprint.encode()).digest()[:16]))
    return {"or": [{"col": column, "op": "eq", "value": identity}, {"col": column, "op": "neq", "value": identity}]}


def edge_account_filters(gateway, ordinal, way):
    """[(identity, where, kind)] for one synthetic account's keyed `graph_edge` filters, and how many
    filters it holds today."""
    from data import row_id
    multi, rank = plain_words_account(ordinal, "mix")
    lists = h1p_lists(gateway, ordinal, "mix")
    user = UUID(str(synthetic_id(f"user/e1/{ordinal}")))
    authority = gateway.authority_module.KeyedSyncAuthority(user_id=user, lists=lists, index_tables=True, chat_rows=True)
    authorization = gateway.authority_module.keyed_authorization_predicate(table="graph_edge", authority=authority)
    identity = main_stream_identity(gateway)
    spaces = [str(space) for space in lists.space_ids]
    # A one-Space account in a written Space holds rails on four Records whose edges the commit updates.
    written = not multi and rank < len(H1_DATA_ACCOUNTS) - 1
    records = [str(row_id("record", H1_DATA_ACCOUNTS[rank + 1], index)) if written else str(synthetic_id(f"e1-record/{ordinal}/{index}")) for index in range(EDGE_RAILS)]
    clients = [(f"session:{index}", "session", predicate, space) for index, (space, (table, predicate)) in enumerate((space, demand) for space in spaces for demand in app_session(ordinal, [space]) if demand[0] == "graph_edge" and demand[1] is not None)]
    clients += [(f"rail:{index}", "rail", next(predicate for table, predicate in app_thread_rail(record) if table == "graph_edge"), record) for index, record in enumerate(records)]
    filters = []
    for name, kind, client, key in clients:
        where = {"and": [{"and": [authorization, client]}, identity]}
        if way != "today":
            where = unwrap_single_or(where)
        if way == "split" and not multi and kind == "rail":
            filters.append((name, {"and": [eq("from_id", key), where]}, kind))
        elif way == "split" and not multi:
            filters.append((f"{name}:from", {"and": [eq("from_space_id", key), where]}, kind))
            filters.append((f"{name}:to", {"and": [eq("to_space_id", key), neq("from_space_id", key), where]}, kind))
        else:
            filters.append((name, where, kind))
    return filters, len(clients)


def edge_catalog(gateway, count, way):
    catalog, today, ordinal = [], 0, 0
    while today < count:
        filters, held = edge_account_filters(gateway, ordinal, way)
        if today + held > count:
            # The last account holds only the filters that fit: drop its last rails.
            dropped = {f"rail:{index}" for index in range(EDGE_RAILS - (today + held - count), EDGE_RAILS)}
            filters, held = [entry for entry in filters if entry[0] not in dropped], count - today
        for name, where, kind in filters:
            body = gateway._plain_body("graph_edge", 1000 + ordinal, where, f"e1:{ordinal}:{name}", keyed=False)
            catalog.append({"kind": f"e1-{kind}", "account": 1000 + ordinal, "body": body, "has_subquery": contains_subquery(where)})
        today += held
        ordinal += 1
    return catalog
