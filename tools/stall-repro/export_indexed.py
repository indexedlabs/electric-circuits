from __future__ import annotations

import argparse
import ast
import gzip
import hashlib
import json
import subprocess
from pathlib import Path


class StripDocumentation(ast.NodeTransformer):
    def visit_Expr(self, node):
        if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
            return None
        return self.generic_visit(node)


def export(root: Path, destination: Path) -> None:
    source_root = root / "api/src"
    paths = sorted((source_root / "api/models").rglob("*.py"))
    paths += sorted((source_root / "api/types").rglob("*.py"))
    paths += sorted((source_root / "api/lib").rglob("*.py"))
    paths += sorted((source_root / "api/services").glob("circuits_sync*.py"))
    bundle = {}
    table_classes = {}
    for path in paths:
        source = path.read_text()
        module = ".".join(path.relative_to(source_root).with_suffix("").parts)
        if module.endswith(".__init__"):
            module = module.removesuffix(".__init__")
        syntax = ast.parse(source)
        imports = {}
        definitions = {}
        for statement in syntax.body:
            if isinstance(statement, ast.ImportFrom):
                parent = module.split(".")[:-statement.level] if statement.level else []
                imported_module = ".".join(parent + ([statement.module] if statement.module else []))
                for alias in statement.names:
                    imports[alias.asname or alias.name] = [imported_module, alias.name]
            elif isinstance(statement, ast.Import):
                for alias in statement.names:
                    imports[alias.asname or alias.name.split(".")[0]] = [alias.name if alias.asname else alias.name.split(".")[0], None]
            elif isinstance(statement, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef, ast.Assign, ast.AnnAssign, ast.TypeAlias)):
                if isinstance(statement, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                    names = [statement.name]
                elif isinstance(statement, ast.TypeAlias):
                    names = [statement.name.id]
                else:
                    targets = statement.targets if isinstance(statement, ast.Assign) else [statement.target]
                    names = [target.id for target in targets if isinstance(target, ast.Name)]
                table = None
                if isinstance(statement, ast.ClassDef):
                    for member in statement.body:
                        if isinstance(member, ast.Assign) and any(isinstance(target, ast.Name) and target.id == "__tablename__" for target in member.targets):
                            table = ast.literal_eval(member.value)
                    if table:
                        table_classes[table] = [module, statement.name]
                        statement.body = [member for member in statement.body if isinstance(member, (ast.Assign, ast.AnnAssign)) and not (isinstance(member.value, ast.Call) and ast.unparse(member.value.func) == "relationship")]
                statement = StripDocumentation().visit(statement)
                if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    statement.decorator_list = [decorator for decorator in statement.decorator_list if not ast.unparse(decorator).startswith("logfire.")]
                for name in names:
                    definitions[name] = ast.unparse(statement)
        bundle[module] = {
            "imports": imports,
            "definitions": definitions,
            "sha256": hashlib.sha256(source.encode()).hexdigest(),
            "path": str(path.relative_to(root)),
        }
    infra_path = root / "infra/infra/services/electric_circuits.py"
    infra = ast.parse(infra_path.read_text())
    publications = []
    for node in ast.walk(infra):
        if isinstance(node, ast.Dict):
            pairs = {key.value: value for key, value in zip(node.keys, node.values) if isinstance(key, ast.Constant)}
            if isinstance(pairs.get("name"), ast.Constant) and pairs["name"].value == "ELECTRIC_CIRCUITS_PG_TABLES":
                publications = ast.literal_eval(pairs["value"]).split(",")
    if not publications or any(table.removeprefix("public.") not in table_classes for table in publications):
        raise RuntimeError("Could not bind every tracked table to its SQLAlchemy declaration")
    payload = {
        "commit": subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip(),
        "publication": publications,
        "publication_source": str(infra_path.relative_to(root)),
        "publication_source_sha256": hashlib.sha256(infra_path.read_bytes()).hexdigest(),
        "table_classes": table_classes,
        "modules": bundle,
    }
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("wb") as raw:
        with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0, filename="") as compressed:
            compressed.write(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode())
    print(json.dumps({"commit": payload["commit"], "tracked_tables": len(publications), "modules": len(bundle), "bytes": destination.stat().st_size}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("indexed", type=Path)
    parser.add_argument("output", type=Path)
    arguments = parser.parse_args()
    export(arguments.indexed, arguments.output)
