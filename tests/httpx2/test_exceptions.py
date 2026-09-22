from __future__ import annotations

import copy
import pickle
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


def _sensitive_request() -> httpx2.Request:
    return httpx2.Request(
        "POST",
        "https://user:s3cr3t-password@example.com/pay",
        headers={"Authorization": "Bearer s3cr3t-token", "Cookie": "session=s3cr3t-cookie"},
        json={"card_number": "4111111111111111"},
    )


@pytest.mark.parametrize(
    "exc_class",
    [
        httpx2.RequestError,
        httpx2.TransportError,
        httpx2.TimeoutException,
        httpx2.ConnectTimeout,
        httpx2.ReadTimeout,
        httpx2.WriteTimeout,
        httpx2.PoolTimeout,
        httpx2.NetworkError,
        httpx2.ReadError,
        httpx2.WriteError,
        httpx2.ConnectError,
        httpx2.CloseError,
        httpx2.ProxyError,
        httpx2.UnsupportedProtocol,
        httpx2.ProtocolError,
        httpx2.LocalProtocolError,
        httpx2.RemoteProtocolError,
        httpx2.DecodingError,
        httpx2.TooManyRedirects,
    ],
)
def test_pickle_does_not_leak_request_data(exc_class: type[httpx2.RequestError]) -> None:
    request = _sensitive_request()
    exc = exc_class("Something went wrong", request=request)

    pickled = pickle.dumps(exc)
    for secret in (b"s3cr3t-token", b"s3cr3t-cookie", b"s3cr3t-password", b"4111111111111111"):
        assert secret not in pickled

    restored = pickle.loads(pickled)
    assert isinstance(restored, exc_class)
    assert str(restored) == "Something went wrong"
    with pytest.raises(RuntimeError, match="The .request property has not been set."):
        restored.request  # noqa: B018


def test_pickle_http_status_error_does_not_leak_request_or_response_data() -> None:
    request = _sensitive_request()
    response = httpx2.Response(500, request=request, headers={"Set-Cookie": "s=s3cr3t-set-cookie"})
    exc = httpx2.HTTPStatusError("Server error", request=request, response=response)

    pickled = pickle.dumps(exc)
    for secret in (b"s3cr3t-token", b"s3cr3t-cookie", b"s3cr3t-password", b"s3cr3t-set-cookie", b"4111111111111111"):
        assert secret not in pickled

    # Prior to the fix, HTTPStatusError couldn't even survive a pickle round-trip,
    # because BaseException's default `__reduce__` reconstructs via `cls(*self.args)`,
    # which fails against HTTPStatusError's keyword-only `request`/`response` params.
    restored = pickle.loads(pickled)
    assert isinstance(restored, httpx2.HTTPStatusError)
    assert str(restored) == "Server error"
    with pytest.raises(RuntimeError, match="The .request property has not been set."):
        restored.request  # noqa: B018
    with pytest.raises(RuntimeError, match="The .response property has not been set."):
        restored.response  # noqa: B018


def test_copy_does_not_leak_request_or_response_data() -> None:
    request = _sensitive_request()
    response = httpx2.Response(500, request=request)
    exc = httpx2.HTTPStatusError("Server error", request=request, response=response)

    copied = copy.copy(exc)
    assert str(copied) == "Server error"
    with pytest.raises(RuntimeError, match="The .request property has not been set."):
        copied.request  # noqa: B018
    with pytest.raises(RuntimeError, match="The .response property has not been set."):
        copied.response  # noqa: B018

    # The original is unaffected by copying.
    assert exc.request is request
    assert exc.response is response


def test_live_attribute_access_is_not_over_redacted() -> None:
    request = _sensitive_request()
    response = httpx2.Response(500, request=request)
    exc = httpx2.HTTPStatusError("Server error", request=request, response=response)

    assert exc.request is request
    assert exc.response is response
    assert exc.request.headers["Authorization"] == "Bearer s3cr3t-token"
