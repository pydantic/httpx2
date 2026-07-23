from __future__ import annotations

import importlib
import importlib.abc
import importlib.machinery
import importlib.util
import sys
from collections.abc import Sequence
from types import ModuleType

__all__ = ["alias_httpx"]

_ALIASES = {"httpx": "httpx2", "httpcore": "httpcore2"}


class _AliasLoader(importlib.abc.Loader):
    _original_spec: importlib.machinery.ModuleSpec | None

    def __init__(self, real_name: str) -> None:
        self._real_name = real_name

    def create_module(self, spec: importlib.machinery.ModuleSpec) -> ModuleType:
        module = importlib.import_module(self._real_name)
        self._original_spec = module.__spec__
        return module

    def exec_module(self, module: ModuleType) -> None:
        module.__spec__ = self._original_spec


class _AliasFinder(importlib.abc.MetaPathFinder):
    def find_spec(
        self,
        fullname: str,
        path: Sequence[str] | None = None,
        target: ModuleType | None = None,
    ) -> importlib.machinery.ModuleSpec | None:
        top = fullname.partition(".")[0]
        real_top = _ALIASES.get(top)
        if real_top is None:
            return None
        real_name = real_top + fullname.removeprefix(top)
        if real_name not in sys.modules and importlib.util.find_spec(real_name) is None:
            return None
        return importlib.machinery.ModuleSpec(fullname, _AliasLoader(real_name))


def alias_httpx() -> None:
    """
    Make `import httpx` resolve to `httpx2`, and `import httpcore` to `httpcore2`, process-wide.

    Intended for applications migrating from `httpx`, so that dependencies still
    importing `httpx` or `httpcore` share the `httpx2` classes. Libraries should never call this.

    Must be called before anything imports `httpx` or `httpcore`. Calling it again is a no-op.
    """
    import httpcore2
    import httpx2

    real_modules = {"httpx": httpx2, "httpcore": httpcore2}
    for alias, real in real_modules.items():
        existing = sys.modules.get(alias)
        if existing is not None and existing is not real:
            raise RuntimeError(f"{alias} was already imported; call `alias_httpx()` before any `import {alias}`.")

    if not any(isinstance(finder, _AliasFinder) for finder in sys.meta_path):
        sys.meta_path.insert(0, _AliasFinder())
    sys.modules.update(real_modules)
