from __future__ import annotations

import sys
import types
from collections.abc import Iterator

import pytest

import httpx2
from httpx2._alias import _AliasFinder


@pytest.fixture(autouse=True)
def restore_import_state() -> Iterator[None]:
    yield
    sys.meta_path[:] = [finder for finder in sys.meta_path if not isinstance(finder, _AliasFinder)]
    for name in [name for name in sys.modules if name == "httpx" or name.startswith("httpx.")]:
        del sys.modules[name]


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


def test_alias_finder_handles_top_level_import() -> None:
    httpx2.alias_httpx()
    del sys.modules["httpx"]

    import httpx

    assert httpx is httpx2


def test_alias_is_idempotent() -> None:
    httpx2.alias_httpx()
    httpx2.alias_httpx()

    assert sum(isinstance(finder, _AliasFinder) for finder in sys.meta_path) == 1


def test_alias_raises_if_httpx_already_imported() -> None:
    sys.modules["httpx"] = types.ModuleType("httpx")

    with pytest.raises(RuntimeError, match="httpx was already imported"):
        httpx2.alias_httpx()


def test_finder_ignores_other_modules() -> None:
    assert _AliasFinder().find_spec("json") is None
