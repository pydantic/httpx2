from __future__ import annotations

import sys
from importlib import metadata

__all__ = ["enable_httpx_alias"]


def _httpx_distribution_installed() -> bool:
    try:
        metadata.distribution("httpx")
    except metadata.PackageNotFoundError:
        return False

    return True


def _alias_loaded_httpx2_modules() -> None:
    for name, module in list(sys.modules.items()):
        if name == "httpx2" or name.startswith("httpx2."):
            alias_name = f"httpx{name.removeprefix('httpx2')}"
            existing_module = sys.modules.get(alias_name)
            if existing_module is not None and existing_module is not module:
                raise RuntimeError(
                    "Cannot alias httpx2 as httpx because another httpx module is already loaded. "
                    "Call httpx2.enable_httpx_alias() before anything imports httpx."
                )
            sys.modules[alias_name] = module


def enable_httpx_alias() -> None:
    """
    Alias `httpx2` as `httpx` for compatibility with existing code.

    This must be called before anything imports `httpx`. It will fail if the
    real `httpx` package is installed or if another `httpx` module has already
    been imported in the current process.
    """
    httpx2_module = sys.modules["httpx2"]

    existing_httpx_module = sys.modules.get("httpx")
    if existing_httpx_module is httpx2_module:
        _alias_loaded_httpx2_modules()
        return

    loaded_httpx_modules = [name for name in sys.modules if name == "httpx" or name.startswith("httpx.")]
    if loaded_httpx_modules:
        raise RuntimeError(
            "Cannot alias httpx2 as httpx because another httpx module is already loaded. "
            "Call httpx2.enable_httpx_alias() before anything imports httpx."
        )

    if _httpx_distribution_installed():
        raise RuntimeError(
            "Cannot alias httpx2 as httpx because the httpx distribution is installed. "
            "Uninstall httpx or use import httpx2 directly."
        )

    sys.modules["httpx"] = httpx2_module
    _alias_loaded_httpx2_modules()
