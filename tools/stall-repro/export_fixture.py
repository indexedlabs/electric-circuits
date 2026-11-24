from __future__ import annotations

import asyncio
import hashlib
import json
import traceback
from pathlib import Path
from uuid import UUID, uuid5

from sqlalchemy.schema import AddConstraint, CreateIndex, CreateTable
from sqlalchemy.dialects import postgresql

from source_loader import SourceLoader


NAMESPACE = UUID("eca13916-2026-4024-8000-000000000000")


def synthetic_id(label: str) -> UUID:
    return uuid5(NAMESPACE, label)


async def export() -> None:
    folder = Path(__file__).parent
    output = folder / "results/fixture"
    output.mkdir(parents=True, exist_ok=True)
    loader = SourceLoader(folder / "indexed-source.json.gz")
    gateway = loader.module("api.services.circuits_sync")
    authority_module = loader.module("api.services.circuits_sync_authority")
    schemas = {}
    failures = {}
    pending = [table.removeprefix("public.") for table in loader.bundle["publication"]]
    while pending:
        name = pending.pop()
        if name in schemas or name in failures:
            continue
        try:
            table = loader.table_model(name).__table__
            schemas[name] = table
            pending.extend(foreign_key.target_fullname.split(".")[-2] for foreign_key in table.foreign_keys)
        except Exception:
            failures[name] = traceback.format_exc()
    (output / "schema-import-status.json").write_text(json.dumps({"loaded": sorted(schemas), "failures": failures}, indent=2))
    if failures:
        for name, failure in failures.items():
            print(f"{name}: {failure.splitlines()[-1]}")
        raise RuntimeError(f"{len(failures)} model declarations could not be imported; see schema-import-status.json")
    dialect = postgresql.dialect()
    ddl = []
    columns = {}
    for name, table in sorted(schemas.items()):
        ddl.append(str(CreateTable(table, include_foreign_key_constraints=[]).compile(dialect=dialect)))
        ddl.extend(str(CreateIndex(index).compile(dialect=dialect)) for index in table.indexes)
        columns[name] = [{"name": column.name, "type": str(column.type.compile(dialect=dialect)), "nullable": column.nullable, "primary_key": column.primary_key, "server_default": str(getattr(column.server_default, "arg", column.server_default)) if column.server_default else None, "enum": [member.value for member in column.type.enum_class] if hasattr(column.type, "enum_class") else None, "foreign_keys": [foreign_key.target_fullname for foreign_key in column.foreign_keys]} for column in table.columns]
    for table in schemas.values():
        ddl.extend(str(AddConstraint(constraint).compile(dialect=dialect)) for constraint in table.foreign_key_constraints)
    (output / "schema.sql").write_text(";\n".join(ddl) + ";\n")
    (output / "columns.json").write_text(json.dumps(columns, indent=2))
    samples = []
    for account in range(2):
        user_id = synthetic_id(f"user/{account}")
        authority = authority_module.NativeSyncAuthority(record_scoped=True, fingerprint=f"synthetic-{account}", authorized_space_ids=(synthetic_id(f"space/{account}"),), direct_content_policy_enabled=True, generation=f"synthetic-{account}")
        for table in ["record", "record_date_index", "record_recurrence", "assignment", "graph_edge", "calendar_event", "resource", "resource_fact"]:
            collection = gateway.registered_collection(table)
            predicate = await gateway.authorize_predicate(None, collection, user_id, authority=authority)
            samples.append({"account": account, "operation": "feed", "path": "/v1/subset-feeds", "body": {"table": f"public.{collection.table}", "columns": sorted(collection.columns), "where": predicate, "changesOnly": True, "subscription": gateway._internal_subscription(user_id, f"synthetic-{table}", authority)}})
    (output / "gateway-samples.json").write_text(json.dumps(samples, indent=2))
    (output / "provenance.json").write_text(json.dumps({"indexed_commit": loader.bundle["commit"], "publication": loader.bundle["publication"], "tables_with_fk_dependencies": sorted(schemas), "loaded": loader.loaded, "bundle_sha256": hashlib.sha256((folder / "indexed-source.json.gz").read_bytes()).hexdigest()}, indent=2))
    print(f"Exported {len(schemas)} schema tables and {len(samples)} gateway-authored feed requests")


if __name__ == "__main__":
    asyncio.run(export())
