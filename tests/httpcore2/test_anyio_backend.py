import socket
import ssl
import threading
from collections.abc import Iterator

import pytest
import trustme

import httpcore2


@pytest.fixture
def ragged_eof_tls_server(cert_authority: trustme.CA) -> Iterator[int]:
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


@pytest.fixture
def cert_authority() -> trustme.CA:
    return trustme.CA()


@pytest.mark.anyio
async def test_write_after_ragged_tls_eof(cert_authority: trustme.CA, ragged_eof_tls_server: int) -> None:
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
