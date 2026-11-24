from __future__ import annotations

import ast
import builtins
import gzip
import importlib
import json
import sys
import types
from pathlib import Path


class Namespace(dict):
    def __init__(self, loader, module):
        super().__init__(__name__=module, __builtins__=vars(builtins))
        self.loader = loader
        self.module = module

    def __missing__(self, name):
        if hasattr(builtins, name):
            return getattr(builtins, name)
        return self.loader.resolve(self.module, name)


class SourceModule(types.ModuleType):
    def __init__(self, loader, name):
        super().__init__(name)
        self.loader = loader
        self.namespace = Namespace(loader, name)

    def __getattr__(self, name):
        if name.startswith("__"):
            raise AttributeError(name)
        return self.namespace[name]


class SourceLoader:
    def __init__(self, path: Path):
        self.bundle = json.loads(gzip.decompress(path.read_bytes()))
        self.modules = {}
        self.loaded = []
        self.resolving = set()

    def module(self, name):
        if name not in self.modules:
            self.modules[name] = SourceModule(self, name)
            sys.modules[name] = self.modules[name]
        return self.modules[name]

    def resolve(self, module, name):
        namespace = self.module(module).namespace
        if name in namespace:
            return namespace[name]
        if name == "ASSISTANT_USER_ID":
            from uuid import UUID
            namespace[name] = UUID("00000000-0000-4000-8000-000000000001")
            return namespace[name]
        descriptor = self.bundle["modules"].get(module)
        if descriptor is None:
            raise RuntimeError(f"Unexported application module {module}.{name}; no live application imports allowed")
        identity = (module, name)
        if identity in self.resolving:
            raise RuntimeError(f"Declaration cycle at {identity}")
        self.resolving.add(identity)
        try:
            if name in descriptor["imports"]:
                target, attribute = descriptor["imports"][name]
                imported = self.module(target) if target.startswith("api") else importlib.import_module(target)
                namespace[name] = getattr(imported, attribute) if attribute else imported
            elif name in descriptor["definitions"]:
                source = descriptor["definitions"][name]
                syntax = ast.parse(source)
                node = syntax.body[0]
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    node.returns = None
                    for argument in [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]:
                        argument.annotation = None
                    if node.args.vararg:
                        node.args.vararg.annotation = None
                    if node.args.kwarg:
                        node.args.kwarg.annotation = None
                if isinstance(node, ast.ClassDef):
                    for dependency in ast.walk(node):
                        if isinstance(dependency, ast.Name) and isinstance(dependency.ctx, ast.Load) and dependency.id in descriptor["imports"]:
                            imported_module, imported_name = descriptor["imports"][dependency.id]
                            if not imported_module.startswith("api.database"):
                                try:
                                    self.resolve(module, dependency.id)
                                except (ImportError, RuntimeError):
                                    pass
                exec(compile(ast.fix_missing_locations(syntax), descriptor["path"], "exec"), namespace)
                self.loaded.append({"module": module, "name": name, "source_sha256": descriptor["sha256"]})
            else:
                raise NameError(f"No exported definition {module}.{name}")
            return namespace[name]
        finally:
            self.resolving.remove(identity)

    def table_model(self, name):
        module, symbol = self.bundle["table_classes"][name]
        return self.resolve(module, symbol)
