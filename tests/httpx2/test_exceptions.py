from __future__ import annotations

import socket
import ssl
import threading
import typing

import pytest
import trustme

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


@pytest.fixture
def ragged_eof_tls_server(cert_authority: trustme.CA) -> typing.Iterator[int]:
    server_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    cert_authority.issue_cert("localhost").configure_cert(server_context)

    def serve() -> None:
        raw, _ = listener.accept()
        with server_context.wrap_socket(raw, server_side=True) as conn:
            # Ends the TCP stream without sending a TLS `close_notify`.
            conn.shutdown(socket.SHUT_WR)
            while conn.recv(65536):
                pass  # pragma: no cover

    with socket.create_server(("127.0.0.1", 0)) as listener:
        thread = threading.Thread(target=serve)
        thread.start()
        yield listener.getsockname()[1]
        thread.join()


@pytest.mark.anyio
async def test_anyio_write_after_ragged_tls_eof(cert_authority: trustme.CA, ragged_eof_tls_server: int) -> None:
    client_context = ssl.create_default_context()
    cert_authority.configure_trust(client_context)

    stream = await httpcore2.AnyIOBackend().connect_tcp("127.0.0.1", ragged_eof_tls_server)
    stream = await stream.start_tls(client_context, server_hostname="localhost")
    try:
        assert await stream.read(65536) == b""
        with pytest.raises(httpcore2.WriteError):
            await stream.write(b"GET / HTTP/1.1\r\n\r\n")
    finally:
        await stream.aclose()
