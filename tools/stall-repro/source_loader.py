from __future__ import annotations

import ast
import builtins
import gzip
import importlib
import json
import symtable
import sys
import types
from pathlib import Path


class Namespace(dict):
    def __init__(self, loader, module):
        super().__init__(__name__=module, __builtins__={**vars(builtins), "__import__": loader.import_module})
        self.loader = loader
        self.module = module
        self["__source_lookup__"] = lambda name: loader.resolve(module, name)

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


class OmitFunctionAnnotations(ast.NodeTransformer):
    def visit_FunctionDef(self, node):
        node.returns = None
        for argument in [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]:
            argument.annotation = None
        if node.args.vararg:
            node.args.vararg.annotation = None
        if node.args.kwarg:
            node.args.kwarg.annotation = None
        return self.generic_visit(node)

    visit_AsyncFunctionDef = visit_FunctionDef


class ResolveGlobals(ast.NodeTransformer):
    def __init__(self, source, descriptor):
        self.scope = symtable.symtable(source, descriptor["path"], "exec")
        self.names = set(descriptor["imports"]) | set(descriptor["definitions"])

    def visit_Name(self, node):
        if not isinstance(node.ctx, ast.Load) or node.id not in self.names:
            return node
        try:
            symbol = self.scope.lookup(node.id)
        except KeyError:
            return node
        if symbol.is_global():
            return ast.copy_location(ast.Call(func=ast.Name(id="__source_lookup__", ctx=ast.Load()), args=[ast.Constant(node.id)], keywords=[]), node)
        return node

    def visit_scope(self, node, name):
        previous = self.scope
        candidates = [child for child in previous.get_children() if child.get_name() == name and child.get_lineno() == node.lineno]
        if not candidates:
            return self.generic_visit(node)
        self.scope = candidates[0]
        while str(self.scope.get_type()) == "type parameters":
            self.scope = next(child for child in self.scope.get_children() if child.get_name() == name)
        result = self.generic_visit(node)
        self.scope = previous
        return result

    def visit_ClassDef(self, node):
        node.bases = [self.visit(base) for base in node.bases]
        node.decorator_list = [self.visit(decorator) for decorator in node.decorator_list]
        return self.visit_scope(node, node.name)

    def visit_FunctionDef(self, node):
        node.decorator_list = [self.visit(decorator) for decorator in node.decorator_list]
        node.args.defaults = [self.visit(default) for default in node.args.defaults]
        node.args.kw_defaults = [self.visit(default) if default else None for default in node.args.kw_defaults]
        return self.visit_scope(node, node.name)

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_Lambda(self, node):
        return self.visit_scope(node, "lambda")

    def visit_ListComp(self, node):
        return self.visit_scope(node, "listcomp")

    def visit_SetComp(self, node):
        return self.visit_scope(node, "setcomp")

    def visit_DictComp(self, node):
        return self.visit_scope(node, "dictcomp")

    def visit_GeneratorExp(self, node):
        return self.visit_scope(node, "genexpr")


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
            if "." in name:
                parent, child = name.rsplit(".", 1)
                self.module(parent).namespace[child] = self.modules[name]
        return self.modules[name]

    def import_module(self, name, globals=None, locals=None, fromlist=(), level=0):
        if level:
            owner = globals["__name__"]
            descriptor = self.bundle["modules"][owner]
            package = owner if descriptor["path"].endswith("/__init__.py") else owner.rsplit(".", 1)[0]
            name = importlib.util.resolve_name("." * level + name, package)
        if name == "api" or name.startswith("api."):
            module = self.module(name)
            return module if fromlist else self.module(name.split(".")[0])
        return builtins.__import__(name, globals, locals, fromlist, 0)

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
                syntax = OmitFunctionAnnotations().visit(ast.parse(source))
                source = ast.unparse(syntax)
                syntax = ResolveGlobals(source, descriptor).visit(ast.parse(source))
                exec(compile(ast.fix_missing_locations(syntax), descriptor["path"], "exec", dont_inherit=True), namespace)
                vars(self.module(module)).update(namespace)
                self.loaded.append({"module": module, "name": name, "source_sha256": descriptor["sha256"]})
            else:
                raise NameError(f"No exported definition {module}.{name}")
            return namespace[name]
        finally:
            self.resolving.remove(identity)

    def table_model(self, name):
        module, symbol = self.bundle["table_classes"][name]
        return self.resolve(module, symbol)
