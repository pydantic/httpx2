import sys
import types
import typing
from importlib import metadata

import pytest

import httpx2
from httpx2 import _alias


@pytest.fixture(autouse=True)
def restore_httpx_modules() -> typing.Iterator[None]:
    original_modules = {name: module for name, module in sys.modules.items() if _is_httpx_module(name)}
    _remove_httpx_modules()

    yield

    _remove_httpx_modules()
    sys.modules.update(original_modules)


def _remove_httpx_modules() -> None:
    for name in [name for name in sys.modules if _is_httpx_module(name)]:
        del sys.modules[name]


def _is_httpx_module(name: str) -> bool:
    return name == "httpx" or name.startswith("httpx.")


def _httpx_distribution_missing() -> bool:
    return False


def _httpx_distribution_installed() -> bool:
    return True


def test_httpx_distribution_installed_returns_false_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    def distribution(name: str) -> object:
        assert name == "httpx"
        raise metadata.PackageNotFoundError

    monkeypatch.setattr(metadata, "distribution", distribution)

    assert not _alias._httpx_distribution_installed()


def test_httpx_distribution_installed_returns_true_when_found(monkeypatch: pytest.MonkeyPatch) -> None:
    def distribution(name: str) -> object:
        assert name == "httpx"
        return object()

    monkeypatch.setattr(metadata, "distribution", distribution)

    assert _alias._httpx_distribution_installed()


def test_enable_httpx_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_alias, "_httpx_distribution_installed", _httpx_distribution_missing)

    httpx2.enable_httpx_alias()

    import httpx

    assert httpx is httpx2
    assert httpx.Client is httpx2.Client


def test_enable_httpx_alias_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_alias, "_httpx_distribution_installed", _httpx_distribution_missing)

    httpx2.enable_httpx_alias()
    httpx2.enable_httpx_alias()

    assert sys.modules["httpx"] is httpx2


def test_enable_httpx_alias_aliases_loaded_submodules(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_alias, "_httpx_distribution_installed", _httpx_distribution_missing)

    httpx2.enable_httpx_alias()

    import httpx._client as httpx_client

    assert httpx_client is sys.modules["httpx2._client"]


def test_enable_httpx_alias_fails_when_httpx_distribution_is_installed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_alias, "_httpx_distribution_installed", _httpx_distribution_installed)

    with pytest.raises(RuntimeError, match="httpx distribution is installed"):
        httpx2.enable_httpx_alias()


def test_enable_httpx_alias_fails_when_httpx_is_loaded() -> None:
    sys.modules["httpx"] = types.ModuleType("httpx")

    with pytest.raises(RuntimeError, match="another httpx module is already loaded"):
        httpx2.enable_httpx_alias()


def test_enable_httpx_alias_fails_when_httpx_submodule_is_loaded() -> None:
    sys.modules["httpx._client"] = types.ModuleType("httpx._client")

    with pytest.raises(RuntimeError, match="another httpx module is already loaded"):
        httpx2.enable_httpx_alias()


def test_enable_httpx_alias_fails_when_alias_submodule_is_replaced(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_alias, "_httpx_distribution_installed", _httpx_distribution_missing)

    httpx2.enable_httpx_alias()
    sys.modules["httpx._client"] = types.ModuleType("httpx._client")

    with pytest.raises(RuntimeError, match="another httpx module is already loaded"):
        httpx2.enable_httpx_alias()
