from __future__ import annotations

import importlib
import importlib.abc
import importlib.machinery
import importlib.util
import sys
from collections.abc import Sequence
from types import ModuleType

__all__ = ["alias_httpx"]


class _AliasLoader(importlib.abc.Loader):
    def create_module(self, spec: importlib.machinery.ModuleSpec) -> ModuleType:
        return importlib.import_module("httpx2" + spec.name.removeprefix("httpx"))

    def exec_module(self, module: ModuleType) -> None:
        pass


class _AliasFinder(importlib.abc.MetaPathFinder):
    def find_spec(
        self,
        fullname: str,
        path: Sequence[str] | None = None,
        target: ModuleType | None = None,
    ) -> importlib.machinery.ModuleSpec | None:
        if fullname != "httpx" and not fullname.startswith("httpx."):
            return None
        real_name = "httpx2" + fullname.removeprefix("httpx")
        if real_name not in sys.modules and importlib.util.find_spec(real_name) is None:
            return None
        return importlib.machinery.ModuleSpec(fullname, _AliasLoader())


def alias_httpx() -> None:
    """
    Make `import httpx` resolve to `httpx2`, process-wide.

    Intended for applications migrating from `httpx`, so that dependencies still
    importing `httpx` share the `httpx2` classes. Libraries should never call this.

    Must be called before anything imports `httpx`. Calling it again is a no-op.
    """
    import httpx2

    existing = sys.modules.get("httpx")
    if existing is not None and existing is not httpx2:
        raise RuntimeError("httpx was already imported; call `alias_httpx()` before any `import httpx`.")

    sys.modules["httpx"] = httpx2
    if not any(isinstance(finder, _AliasFinder) for finder in sys.meta_path):
        sys.meta_path.insert(0, _AliasFinder())
