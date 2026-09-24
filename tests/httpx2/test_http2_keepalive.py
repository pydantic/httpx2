from __future__ import annotations

import socket
import ssl
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack

import anyio
import h2.config
import h2.connection
import h2.events
import pytest
import trustme

import httpx2


@pytest.fixture(params=["tls", "tcp", "ping", "open"])
def http2_peer(
    request: pytest.FixtureRequest, localhost_cert: trustme.LeafCert
) -> Iterator[tuple[str, threading.Event, threading.Event, bool]]:
    response_read = threading.Event()
    peer_ready = threading.Event()
    close = request.param != "open"
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    localhost_cert.configure_cert(context)
    context.set_alpn_protocols(["h2"])

    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        listener.settimeout(5)
        url = f"https://localhost:{listener.getsockname()[1]}"

        def serve() -> None:
            bodies = []
            with ExitStack() as stack:
                stream = None
                for index in range(2):
                    if stream is None:
                        sock, _ = listener.accept()
                        sock.settimeout(5)
                        stream = stack.enter_context(context.wrap_socket(sock, server_side=True))
                        connection = h2.connection.H2Connection(h2.config.H2Configuration(client_side=False))
                        connection.initiate_connection()
                        stream.sendall(connection.data_to_send())
                    body = b""
                    stream_id = None
                    while stream_id is None:
                        data = stream.recv(65536)
                        assert data
                        for event in connection.receive_data(data):
                            if isinstance(event, h2.events.DataReceived):
                                body += event.data
                            elif isinstance(event, h2.events.StreamEnded):
                                stream_id = event.stream_id
                        stream.sendall(connection.data_to_send())
                    bodies.append(body)
                    connection.send_headers(stream_id, [(":status", "200")])
                    connection.send_data(stream_id, body, end_stream=True)
                    stream.sendall(connection.data_to_send())
                    if index == 0:
                        assert response_read.wait(5)
                        if request.param == "tls":
                            stream.setblocking(False)
                            with pytest.raises(ssl.SSLWantReadError):
                                while stream.recv(65536):
                                    pass
                            with pytest.raises(ssl.SSLWantReadError):
                                stream.unwrap()
                            stream = None
                        elif request.param == "tcp":
                            stream.close()
                            stream = None
                        elif request.param == "ping":
                            connection.ping(b"12345678")
                            stream.sendall(connection.data_to_send())
                            stream = None
                        peer_ready.set()
                assert bodies == [b"first", b"second"]

        with ThreadPoolExecutor(max_workers=1) as executor:
            server = executor.submit(serve)
            try:
                yield url, response_read, peer_ready, close
            finally:
                response_read.set()
                server.result(timeout=10)


@pytest.mark.parametrize("keepalive_expiry", [None, 100])
def test_http2_keepalive(
    http2_peer: tuple[str, threading.Event, threading.Event, bool], keepalive_expiry: float | None
) -> None:
    url, response_read, peer_ready, close = http2_peer
    with httpx2.Client(http2=True, verify=False, limits=httpx2.Limits(keepalive_expiry=keepalive_expiry)) as client:
        first = client.post(url, content=iter([b"first"]))
        assert first.http_version == "HTTP/2"
        assert first.content == b"first"
        response_read.set()
        assert peer_ready.wait(5)
        second = client.post(url, content=iter([b"second"]))
        assert second.content == b"second"
        assert (first.extensions["network_stream"] is not second.extensions["network_stream"]) == close


@pytest.mark.anyio
@pytest.mark.parametrize("keepalive_expiry", [None, 100])
async def test_async_http2_keepalive(
    http2_peer: tuple[str, threading.Event, threading.Event, bool], keepalive_expiry: float | None
) -> None:
    url, response_read, peer_ready, close = http2_peer
    async with httpx2.AsyncClient(
        http2=True, verify=False, limits=httpx2.Limits(keepalive_expiry=keepalive_expiry)
    ) as client:
        first = await client.post(url, content=b"first")
        assert first.http_version == "HTTP/2"
        assert first.content == b"first"
        response_read.set()
        assert await anyio.to_thread.run_sync(peer_ready.wait, 5)
        second = await client.post(url, content=b"second")
        assert second.content == b"second"
        assert (first.extensions["network_stream"] is not second.extensions["network_stream"]) == close
