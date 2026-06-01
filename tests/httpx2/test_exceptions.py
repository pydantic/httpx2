from __future__ import annotations

import typing

import pytest

import httpcore2
import httpx2

if typing.TYPE_CHECKING:
    from conftest import TestServer


def test_httpcore_all_exceptions_mapped() -> None:
    """
    All exception classes exposed by HTTPCore are properly mapped to an HTTPX-specific
    exception class.
    """
    expected_mapped_httpcore_exceptions = {
        value.__name__
        for _, value in vars(httpcore2).items()
        if isinstance(value, type) and issubclass(value, Exception) and value is not httpcore2.ConnectionNotAvailable
    }

    httpx_exceptions = {
        value.__name__ for _, value in vars(httpx2).items() if isinstance(value, type) and issubclass(value, Exception)
    }

    unmapped_exceptions = expected_mapped_httpcore_exceptions - httpx_exceptions

    if unmapped_exceptions:  # pragma: no cover
        pytest.fail(f"Unmapped httpcore exceptions: {unmapped_exceptions}")


def test_httpcore_exception_mapping(server: TestServer) -> None:
    """
    HTTPCore exception mapping works as expected.
    """
    impossible_port = 123456
    with pytest.raises(httpx2.ConnectError):
        httpx2.get(server.url.copy_with(port=impossible_port))

    with pytest.raises(httpx2.ReadTimeout):
        httpx2.get(
            server.url.copy_with(path="/slow_response"),
            timeout=httpx2.Timeout(5, read=0.01),
        )


def test_request_attribute() -> None:
    # Exception without request attribute
    exc = httpx2.ReadTimeout("Read operation timed out")
    with pytest.raises(RuntimeError):
        exc.request  # noqa: B018

    # Exception with request attribute
    request = httpx2.Request("GET", "https://www.example.com")
    exc = httpx2.ReadTimeout("Read operation timed out", request=request)
    assert exc.request == request
