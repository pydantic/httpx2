# Emscripten-specific test fixtures
from __future__ import annotations

from typing import Any, Callable, Iterator

import pytest

import httpx2

try:
    import pytest_pyodide
    from pytest_pyodide.runner import SeleniumChromeRunner

    _has_pytest_pyodide = True
except ImportError:  # pragma: nocover
    # pytest-pyodide (and a browser/node runtime) is only available when a
    # Pyodide environment has been set up via `scripts/download-pyodide`. When
    # it isn't installed we skip collecting the emscripten tests entirely so
    # that a plain `pytest` run on the host still works.
    _has_pytest_pyodide = False
    collect_ignore_glob = ["*"]


if _has_pytest_pyodide:
    # Make our ssl certificates work in Chrome
    pyodide_config = pytest_pyodide.config.get_global_config()
    pyodide_config.set_flags("chrome", ["ignore-certificate-errors"] + pyodide_config.get_flags("chrome"))


def patch_javascript_setup(
    orig: Callable[[SeleniumChromeRunner], None],
) -> Callable[[SeleniumChromeRunner], None]:
    """Remove WebAssembly.Suspending when jspi is False

    Pyodide uses WebAssembly.Suspending to feature detect JSPI. Removing it
    ensures that we actually use the no-JSPI code path when self.jspi is False.
    """

    def javascript_setup(self: SeleniumChromeRunner) -> None:
        orig(self)
        if not self.jspi:
            self.run_js(
                "delete WebAssembly.Suspending;",
                pyodide_checks=False,
            )

    return javascript_setup


if _has_pytest_pyodide:
    SeleniumChromeRunner.javascript_setup = patch_javascript_setup(SeleniumChromeRunner.javascript_setup)


def selenium_runner_helper(
    request: pytest.FixtureRequest,
    has_jspi: bool,
    wheel_url: httpx2.URL,
    is_worker: bool,
) -> SeleniumChromeRunner:
    if has_jspi:
        fixture_name = "selenium_jspi"
    else:
        fixture_name = "selenium"
    if is_worker:
        fixture_name += "_worker"
    result = request.getfixturevalue(fixture_name)
    if result.browser == "node":
        # stop node.js checking our https certificates
        result.run_js('process.env["NODE_TLS_REJECT_UNAUTHORIZED"] = 0;')

    result.run_js(
        f"""
        await pyodide.loadPackage("micropip");
        await pyodide.runPythonAsync(`
            import micropip
            await micropip.install({str(wheel_url)!r})
        `);
        """
    )
    return result


@pytest.fixture
def selenium_runner(request: pytest.FixtureRequest, runtime: str, has_jspi: bool, wheel_url: httpx2.URL) -> Any:
    worker = False
    return selenium_runner_helper(request, has_jspi, wheel_url, worker)


@pytest.fixture
def selenium_worker_runner(request: pytest.FixtureRequest, runtime: str, has_jspi: bool, wheel_url: httpx2.URL) -> Any:
    worker = True
    return selenium_runner_helper(request, has_jspi, wheel_url, worker)


@pytest.fixture(scope="session", params=["https", "http"])
def server_url(request: pytest.FixtureRequest, server: Any, https_server: Any) -> Iterator[httpx2.URL]:
    if request.param == "https":
        yield https_server.url.copy_with(path="/emscripten")
    else:
        yield server.url.copy_with(path="/emscripten")


@pytest.fixture
def wheel_url(server_url: httpx2.URL) -> Iterator[httpx2.URL]:
    ver = httpx2.__version__
    yield server_url.copy_with(path=f"/wheel_download/httpx2-{ver}-py3-none-any.whl")


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """Generate Webassembly Javascript Promise Integration based tests
    only for platforms that support it.

    Currently:
    1) NodeJS requires JSPI because it doesn't support XMLHttpRequest
    2) Firefox doesn't support JSPI
    3) Chrome supports JSPI on or off.
    """
    if "has_jspi" in metafunc.fixturenames:  # pragma: no cover
        if metafunc.config.getoption("--runtime").startswith("node"):
            metafunc.parametrize("has_jspi", [True])
        elif metafunc.config.getoption("--runtime").startswith("firefox"):
            metafunc.parametrize("has_jspi", [False])
        else:
            metafunc.parametrize("has_jspi", [True, False])
