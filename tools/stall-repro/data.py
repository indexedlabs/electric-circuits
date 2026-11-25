from __future__ import annotations

import datetime
import json
import re
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from export_fixture import synthetic_id
from probes import require_isolated


LARGE_ACCOUNT = 18
IMPORT_ACCOUNT = 19
IMPORT_TRANSACTIONS = [2086, 2134, 2160, 2101, 2139, 2461]
LARGE_ROWS = {"message": 22300, "activity": 3000, "calendar_event": 1100, "record": 1000, "record_date_index": 800, "record_recurrence": 500, "assignment": 500, "graph_edge": 250, "resource": 100, "resource_fact": 150, "workflow": 50, "calendar": 15, "space": 1, "user": 1, "external_account": 1, "space_member": 1, "standing_grant": 1, "record_completed_index": 10, "record_assignee_index": 13}
BACKGROUND_ROWS = {"user": 1, "space": 1, "space_member": 1, "external_account": 1, "calendar": 15, "standing_grant": 1, "record": 500, "record_date_index": 500, "record_recurrence": 150, "assignment": 150, "graph_edge": 125, "resource": 100, "resource_fact": 50, "calendar_event": 100}
# B-cold only: share, grant and membership rows the kept subquery scopes read. Every account shares
# a few Records, Files and Resources with other accounts; SHARE_ACCOUNT holds about 1,000 of each
# kind (and 1,000 extra memberships of other accounts' Spaces), which `share-delete` removes in one
# transaction together with the account's own rows.
SHARE_ACCOUNT = 17
SHARE_ROWS = {"file": 20, "record_share": 20, "file_share": 20, "resource_share": 20}
SHARE_ACCOUNT_ROWS = {"file": 100, "record_share": 1000, "file_share": 1000, "resource_share": 100, "space_member": 1000, "user": 1000}
SHARE_TABLES = ["record_share", "file_share", "resource_share", "file"]
DELETE_ORDER = ["message", "activity", "graph_edge", "assignment", "record_recurrence", "record_date_index", "record_completed_index", "record_assignee_index", "resource_fact", "workflow", "calendar_event", "record", "calendar", "resource", "space_member", "standing_grant", "space", "external_account", "user"]


def row_id(table, account, ordinal=0):
    return synthetic_id(f"{table}/{account}" if table in {"user", "space"} else f"{table}/{account}/{ordinal}")


def other_account(account, ordinal):
    """A background account other than `account`, cycling through all of them (never the import account)."""
    other = (account + 1 + ordinal) % 19
    return (other + 1) % 19 if other == account else other


def member_id(account, ordinal):
    return synthetic_id(f"member/{account}/{ordinal}")


class SyntheticData:
    def __init__(self, columns, padding=3500):
        self.columns = columns
        self.padding = padding
        self.rows = {}
        self.next_ordinal = {}

    def make(self, table, account, ordinal):
        values = {}
        for column in self.columns[table]:
            name = column["name"]
            datatype = column["type"]
            if column["nullable"]:
                value = None
            elif datatype == "UUID":
                if column["foreign_keys"]:
                    target = column["foreign_keys"][0].split(".")[-2]
                    target_count = (LARGE_ROWS if account == LARGE_ACCOUNT else BACKGROUND_ROWS).get(target, 1)
                    value = row_id(target, account, ordinal % target_count)
                else:
                    value = row_id(table, account, ordinal) if name == "id" else synthetic_id(f"{table}/{name}/{account}/{ordinal}")
            elif datatype == "BYTEA":
                value = b"s" * self.padding
            elif datatype == "JSONB":
                value = Jsonb({"synthetic_padding": "s" * self.padding})
            elif datatype.endswith("[]"):
                value = []
            elif datatype == "BOOLEAN":
                value = False
            elif datatype == "DATE":
                value = datetime.date(2026, 9, 1) + datetime.timedelta(days=ordinal % 84)
            elif "TIMESTAMP" in datatype:
                value = datetime.datetime(2026, 9, 1, tzinfo=datetime.timezone.utc) + datetime.timedelta(days=ordinal % 84)
            elif any(kind in datatype for kind in ("INT", "NUMERIC", "REAL", "DOUBLE", "FLOAT")):
                value = ordinal + 1 if column["primary_key"] else 1
            elif column.get("enum"):
                value = column["enum"][0]
            else:
                value = f"synthetic-{table}-{name}-{account}-{ordinal}"
                limit = re.search(r"\((\d+)\)", datatype)
                if limit:
                    value = value[:int(limit.group(1))]
            values[name] = value
        overrides = {
            "user": {"name": f"Synthetic account {account}", "email": f"synthetic-{account}@example.invalid", "timezone": "UTC"},
            "space": {"type": "home", "created_by_user_id": row_id("user", account)},
            "space_member": {"user_id": row_id("user", account), "space_id": row_id("space", account)},
            "record": {"type": "event", "space_id": row_id("space", account), "created_by_user_id": row_id("user", account)},
            "record_date_index": {"record_id": row_id("record", account, ordinal), "space_id": row_id("space", account), "field_id": "start", "field_type": "start_at", "date_value": datetime.date(2026, 9, 1) + datetime.timedelta(days=ordinal % 84)},
            "record_recurrence": {"record_id": row_id("record", account, ordinal), "space_id": row_id("space", account), "rrule": "FREQ=DAILY;COUNT=10"},
            "record_completed_index": {"record_id": row_id("record", account, ordinal), "space_id": row_id("space", account)},
            "record_assignee_index": {"record_id": row_id("record", account, ordinal), "space_id": row_id("space", account)},
            "assignment": {"record_id": row_id("record", account, ordinal), "space_id": row_id("space", account), "variable": ("calendar", "origin", "driver")[ordinal % 3], "target_type": "resource", "target_id": row_id("resource", account, ordinal % 100), "source": "user", "scope": None, "instance_date": None},
            "calendar": {"user_id": row_id("user", account), "external_account_id": row_id("external_account", account), "provider": "google", "should_sync": True},
            "calendar_event": {"user_id": row_id("user", account), "calendar_id": row_id("calendar", account, ordinal % 15), "provider": "google", "status": "confirmed", "is_busy_block": False},
            "resource": {"resource_type": "person", "space_id": row_id("space", account), "user_id": row_id("user", account) if ordinal == 0 else None},
            "resource_fact": {"resource_id": row_id("resource", account, ordinal % 100), "fact_type": "note" if ordinal < 100 else "full_name"},
            "graph_edge": {"from_type": "record", "from_id": row_id("record", account, ordinal), "to_type": "resource", "to_id": row_id("resource", account, ordinal % 100), "edge_type": "has_attendee"},
            "message": {"target_type": "record", "target_id": row_id("record", account, ordinal % 1000), "source_type": "user", "source_id": row_id("user", account)},
            "activity": {"target_type": "user", "target_id": row_id("user", account), "is_hidden": False},
            "workflow": {"record_id": row_id("record", account, ordinal)},
            "record_share": {"space_id": row_id("space", account), "record_id": row_id("record", account, ordinal % (LARGE_ROWS if account == LARGE_ACCOUNT else BACKGROUND_ROWS)["record"]), "target_type": "user", "target_id": row_id("user", other_account(account, ordinal)), "created_by_user_id": row_id("user", account), "access_level": "read"},
            "file": {"scope_type": "user", "user_id": row_id("user", account), "status": "ready"},
            "file_share": {"user_id": row_id("user", account), "file_id": row_id("file", account, ordinal % (SHARE_ACCOUNT_ROWS if account == SHARE_ACCOUNT else SHARE_ROWS)["file"]), "target_type": "space", "target_space_id": row_id("space", other_account(account, ordinal)), "target_id": row_id("space", other_account(account, ordinal))},
            "resource_share": {"resource_id": row_id("resource", account, ordinal % 100), "target_type": "user", "target_id": row_id("user", other_account(account, ordinal)), "source": "direct", "access_level": "view", "created_by_user_id": row_id("user", account)},
            "standing_grant": {"grantor_user_id": row_id("user", (account + 1) % 18 if account < 18 else 17), "grantee_user_id": row_id("user", account), "state": "active", "scope": "all", "tier": "editor", "activated_at": datetime.datetime(2026, 9, 1, tzinfo=datetime.timezone.utc)},
        }
        values.update({key: value for key, value in overrides.get(table, {}).items() if key in values})
        if table == "space_member" and ordinal > 0:
            # Extra memberships (share seed only): synthetic people of this account in other accounts' Spaces.
            values.update({"space_id": row_id("space", other_account(account, ordinal)), "user_id": member_id(account, ordinal)})
        if table == "user" and ordinal > 0:
            values.update({"id": member_id(account, ordinal), "email": f"synthetic-member-{account}-{ordinal}@example.invalid"})
        if "audience_key" in values:
            values["audience_key"] = row_id("calendar", account, ordinal % 15) if table == "calendar_event" else row_id("space", account)
            values["reader_class"] = "live"
        return values

    def add_audience_columns(self, postgres_url, tables):
        statements = []
        with psycopg.connect(postgres_url) as connection:
            for table in tables:
                if table in {"native_sync_authority_fence", "circuits_source_fence"}:
                    continue
                statement = sql.SQL("ALTER TABLE {} ADD COLUMN audience_key uuid, ADD COLUMN reader_class text").format(sql.Identifier("public", table))
                connection.execute(statement)
                statements.append(statement.as_string(connection))
                self.columns[table].extend([{"name": name, "type": kind, "nullable": True, "primary_key": False, "server_default": None, "enum": None, "foreign_keys": []} for name, kind in (("audience_key", "UUID"), ("reader_class", "TEXT"))])
        return statements

    def add_assignment_space(self, postgres_url):
        """Variant S: `assignment.space_id`, which the synthetic writer fills from the row's Record."""
        statement = "ALTER TABLE public.assignment ADD COLUMN space_id uuid"
        with psycopg.connect(postgres_url) as connection:
            connection.execute(statement)
        self.columns["assignment"].append({"name": "space_id", "type": "UUID", "nullable": True, "primary_key": False, "server_default": None, "enum": None, "foreign_keys": []})
        return [statement]

    def insert(self, connection, table, account, count, start=None):
        rows = self.rows.setdefault((table, account), [])
        start = self.next_ordinal.get((table, account), 0) if start is None else start
        generated = [self.make(table, account, ordinal) for ordinal in range(start, start + count)]
        columns = [column["name"] for column in self.columns[table]]
        command = sql.SQL("COPY {} ({}) FROM STDIN").format(sql.Identifier("public", table), sql.SQL(",").join(map(sql.Identifier, columns)))
        with connection.cursor().copy(command) as copier:
            for row in generated:
                copier.write_row([row[column] for column in columns])
        rows.extend(generated)
        self.next_ordinal[(table, account)] = start + count
        return count

    def update(self, connection, table, account, count):
        rows = self.rows[(table, account)]
        columns = self.columns[table]
        names = {column["name"] for column in columns}
        primary = [column["name"] for column in columns if column["primary_key"]]
        column = "version" if "version" in names else "updated_at" if "updated_at" in names else primary[0]
        expression = sql.SQL("{} + 1").format(sql.Identifier(column)) if column == "version" else sql.Identifier(column)
        command = sql.SQL("UPDATE {} SET {}={} WHERE {}").format(sql.Identifier("public", table), sql.Identifier(column), expression, sql.SQL(" AND ").join(sql.SQL("{}=%s").format(sql.Identifier(key)) for key in primary))
        changed = 0
        for ordinal in range(count):
            row = rows[ordinal % len(rows)]
            changed += connection.execute(command, [row[key] for key in primary]).rowcount
        if changed != count:
            raise AssertionError((table, count, changed))
        return changed

    def delete(self, connection, table, account, count):
        rows = self.rows.get((table, account), [])
        chosen = rows[:count]
        primary = [column["name"] for column in self.columns[table] if column["primary_key"]]
        command = sql.SQL("DELETE FROM {} WHERE {}").format(sql.Identifier("public", table), sql.SQL(" AND ").join(sql.SQL("{}=%s").format(sql.Identifier(key)) for key in primary))
        changed = 0
        for row in chosen:
            changed += connection.execute(command, [row[key] for key in primary]).rowcount
        if changed != count:
            raise AssertionError((table, count, changed))
        self.rows[(table, account)] = rows[count:]
        return changed

    def seed(self, postgres_url, shares=False):
        require_isolated("http://127.0.0.1", postgres_url)
        with psycopg.connect(postgres_url) as connection:
            connection.execute("SET LOCAL session_replication_role = replica")
            for account in range(19):
                distribution = LARGE_ROWS if account == LARGE_ACCOUNT else BACKGROUND_ROWS
                for table, count in distribution.items():
                    self.insert(connection, table, account, count)
                if shares:
                    for table, count in (SHARE_ACCOUNT_ROWS if account == SHARE_ACCOUNT else SHARE_ROWS).items():
                        # space_member and user already hold the account's own row at ordinal 0.
                        self.insert(connection, table, account, count)
            for table in ("user", "space", "external_account"):
                self.insert(connection, table, IMPORT_ACCOUNT, 1)
        return {"tables": {f"{table}/{account}": len(rows) for (table, account), rows in self.rows.items()}, "large_account_rows": sum(LARGE_ROWS.values())}

    def calendar_import(self, postgres_url, marker_callback):
        journal = []
        for transaction, target in enumerate(IMPORT_TRANSACTIONS):
            with psycopg.connect(postgres_url) as connection:
                connection.execute("SET LOCAL session_replication_role = replica")
                operations = [("record", "insert", 350), ("record", "update", 250), ("record_date_index", "insert", 300), ("record_date_index", "delete", 100), ("record_recurrence", "insert", 200), ("record_recurrence", "update", 200), ("record_recurrence", "delete", 100), ("assignment", "insert", 200), ("calendar_event", "insert", 100), ("calendar_event", "update", 50), ("calendar", "update", 75), ("resource", "insert", 20), ("resource", "update", 20)]
                if transaction == 0:
                    operations.insert(0, ("calendar", "insert", 15))
                operations.append(("graph_edge", "insert", target - sum(count for _, _, count in operations)))
                for table, operation, count in operations:
                    changed = getattr(self, operation)(connection, table, IMPORT_ACCOUNT, count)
                    journal.append({"transaction": transaction, "table": table, "operation": operation, "rows": changed})
                marker_callback(connection, transaction)
        return journal

    def delete_account(self, postgres_url, account, marker_callback, extra_tables=()):
        journal = []
        with psycopg.connect(postgres_url) as connection:
            connection.execute("SET LOCAL session_replication_role = replica")
            for table in [*extra_tables, *DELETE_ORDER]:
                # Only the account's own user row; share-seed member people stay, like any other person.
                count = min(1, len(self.rows.get((table, account), []))) if table == "user" else len(self.rows.get((table, account), []))
                if count:
                    journal.append({"table": table, "operation": "delete", "rows": self.delete(connection, table, account, count)})
            marker_callback(connection, 0)
        return journal


def load_data(path: Path):
    return SyntheticData(json.loads(path.read_text()))
