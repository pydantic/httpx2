from __future__ import annotations

import importlib
import importlib.util
import sys
import types
from collections.abc import Iterator

import pytest

import httpcore2
import httpx2
from httpx2._alias import _AliasFinder


def _is_aliased(name: str) -> bool:
    return name.partition(".")[0] in ("httpx", "httpcore")


@pytest.fixture(autouse=True)
def restore_import_state() -> Iterator[None]:
    saved_meta_path = list(sys.meta_path)
    saved_modules = {name: module for name, module in sys.modules.items() if _is_aliased(name)}
    yield
    sys.meta_path[:] = saved_meta_path
    for name in [name for name in sys.modules if _is_aliased(name)]:
        del sys.modules[name]
    sys.modules.update(saved_modules)


def test_alias_top_level_import() -> None:
    httpx2.alias_httpx()

    import httpx

    assert httpx is httpx2
    assert httpx.Client is httpx2.Client


def test_alias_submodules_share_modules() -> None:
    httpx2.alias_httpx()

    from httpx._exceptions import HTTPError

    assert HTTPError is httpx2.HTTPError
    assert sys.modules["httpx._exceptions"] is sys.modules["httpx2._exceptions"]

    with pytest.raises(HTTPError):
        raise httpx2.ConnectError("boom")


def test_alias_httpcore_top_level_import() -> None:
    httpx2.alias_httpx()

    import httpcore

    assert httpcore is httpcore2
    assert httpcore.ConnectionPool is httpcore2.ConnectionPool


def test_alias_httpcore_submodules_share_modules() -> None:
    httpx2.alias_httpx()

    from httpcore._exceptions import ConnectError

    assert ConnectError is httpcore2.ConnectError
    assert sys.modules["httpcore._exceptions"] is sys.modules["httpcore2._exceptions"]


def test_alias_finder_handles_top_level_import() -> None:
    httpx2.alias_httpx()
    del sys.modules["httpx"]

    import httpx

    assert httpx is httpx2


def test_alias_preserves_canonical_spec() -> None:
    httpx2.alias_httpx()

    importlib.import_module("httpx._exceptions")

    spec = sys.modules["httpx2._exceptions"].__spec__
    assert spec is not None
    assert spec.name == "httpx2._exceptions"


def test_alias_is_idempotent() -> None:
    httpx2.alias_httpx()
    assert sum(isinstance(finder, _AliasFinder) for finder in sys.meta_path) == 2

    httpx2.alias_httpx()
    assert sum(isinstance(finder, _AliasFinder) for finder in sys.meta_path) == 2


def test_alias_raises_if_httpx_already_imported() -> None:
    sys.modules["httpx"] = types.ModuleType("httpx")

    with pytest.raises(RuntimeError, match="httpx was already imported"):
        httpx2.alias_httpx()


def test_alias_raises_if_httpcore_already_imported() -> None:
    sys.modules["httpcore"] = types.ModuleType("httpcore")

    with pytest.raises(RuntimeError, match="httpcore was already imported"):
        httpx2.alias_httpx()


def test_finder_ignores_other_modules() -> None:
    assert _AliasFinder("httpx", "httpx2").find_spec("json") is None


def test_finder_returns_none_for_missing_submodules() -> None:
    httpx2.alias_httpx()

    assert importlib.util.find_spec("httpx._does_not_exist") is None

    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("httpx._does_not_exist")


def test_finder_locates_unloaded_submodules(monkeypatch: pytest.MonkeyPatch) -> None:
    httpx2.alias_httpx()
    monkeypatch.delitem(sys.modules, "httpx2._main", raising=False)

    assert importlib.util.find_spec("httpx._main") is not None
