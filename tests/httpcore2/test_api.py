import json
import socket
import ssl
from collections.abc import Callable
from functools import partial
from typing import Any

import anyio
import anyio.streams.stapled
import anyio.streams.tls
import pytest
import trio
import trustme
from pytest_httpbin.serve import Server

import httpcore2


def test_request(httpbin: Server) -> None:
    response = httpcore2.request("GET", httpbin.url)
    assert response.status == 200


def test_stream(httpbin: Server) -> None:
    with httpcore2.stream("GET", httpbin.url) as response:
        assert response.status == 200


def test_request_with_content(httpbin: Server) -> None:
    url = f"{httpbin.url}/post"
    response = httpcore2.request("POST", url, content=b'{"hello":"world"}')
    assert response.status == 200
    assert json.loads(response.content)["json"] == {"hello": "world"}


@pytest.mark.anyio
@pytest.mark.parametrize(
    "anyio_backend,backend_type",
    [("asyncio", httpcore2.AnyIOBackend), ("trio", httpcore2.AnyIOBackend), ("trio", httpcore2.TrioBackend)],
)
async def test_read_available(anyio_backend: str, backend_type: type[httpcore2.AsyncNetworkBackend]) -> None:
    connected, send_data, data_sent, closed = (anyio.Event() for _ in range(4))

    async def serve(peer: anyio.abc.SocketStream) -> None:
        async with peer:
            connected.set()
            await send_data.wait()
            await peer.send(b"abcdef")
            data_sent.set()
            assert await peer.receive() == b"request"
        closed.set()

    listener = await anyio.create_tcp_listener(local_host="127.0.0.1")
    with anyio.fail_after(5):
        async with listener, anyio.create_task_group() as tasks:
            tasks.start_soon(listener.serve, serve)
            stream = await backend_type().connect_tcp("127.0.0.1", listener.extra(anyio.abc.SocketAttribute.local_port))
            try:
                await connected.wait()
                probe = stream.get_extra_info("read_available")
                assert callable(probe)
                assert stream.get_extra_info("start_reading") is None
                with pytest.raises(ValueError, match="max_bytes"):
                    await probe(0)
                with pytest.raises(httpcore2.ReadTimeout):
                    await probe(1, timeout=0)
                with anyio.CancelScope() as scope:
                    scope.cancel()
                    await probe(1)
                assert scope.cancelled_caught
                assert await probe(1, timeout=None) is None
                send_data.set()
                await data_sent.wait()
                data = await stream.read(1, timeout=5)
                while len(data) < 6:
                    chunk = await probe(2, timeout=5)
                    if chunk is not None:
                        assert 0 < len(chunk) <= 2
                        data += chunk
                assert data == b"abcdef"
                assert await probe(1) is None
                await stream.write(b"request", timeout=5)
                await closed.wait()
                assert await stream.read(1, timeout=5) == b""
                assert await probe(1, timeout=5) == b""
            finally:
                await stream.aclose()
                tasks.cancel_scope.cancel()
        with pytest.raises(httpcore2.ReadError):
            await probe(1, timeout=5)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "anyio_backend,backend_type",
    [
        ("asyncio", httpcore2.AnyIOBackend),
        ("trio", httpcore2.AnyIOBackend),
        ("trio", httpcore2.TrioBackend),
        ("asyncio", httpcore2.SyncBackend),
        ("trio", httpcore2.SyncBackend),
    ],
)
@pytest.mark.parametrize("layers", [1, 2])
@pytest.mark.parametrize("flush_on_read", [False, True])
async def test_read_available_post_handshake_auth(
    anyio_backend: str,
    backend_type: type[httpcore2.AsyncNetworkBackend | httpcore2.NetworkBackend],
    layers: int,
    flush_on_read: bool,
) -> None:
    async def call(method: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        if backend_type is httpcore2.SyncBackend:
            return await anyio.to_thread.run_sync(partial(method, *args, **kwargs))
        return await method(*args, **kwargs)

    ca = trustme.CA()
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ca.issue_cert("localhost").configure_cert(server_context)
    ca.configure_trust(server_context)
    server_context.verify_mode = ssl.CERT_OPTIONAL
    server_context.post_handshake_auth = True
    client_context = ssl.create_default_context()
    ca.configure_trust(client_context)
    ca.issue_cert("client").configure_cert(client_context)
    client_context.post_handshake_auth = True
    server_context.minimum_version = client_context.minimum_version = ssl.TLSVersion.TLSv1_3
    ready, challenge_sent, send_response, done = (anyio.Event() for _ in range(4))

    async def serve(peer: anyio.abc.SocketStream) -> None:
        stream: anyio.abc.ByteStream = peer
        wrapped = []
        async with peer:
            for _ in range(layers):
                stream = await anyio.streams.tls.TLSStream.wrap(
                    stream, ssl_context=server_context, server_side=True, standard_compatible=False
                )
                wrapped.append(stream)
            await ready.wait()
            for layer in wrapped:
                ssl_object = layer.extra(anyio.streams.tls.TLSAttribute.ssl_object)
                assert ssl_object.getpeercert() is None
                ssl_object.verify_client_post_handshake()
                await layer.send(b"")
            challenge_sent.set()
            await send_response.wait()
            await stream.send(b"response")
            assert await stream.receive() == b"request"
            for layer in wrapped:
                assert layer.extra(anyio.streams.tls.TLSAttribute.ssl_object).getpeercert()
            done.set()

    listener = await anyio.create_tcp_listener(local_host="127.0.0.1")
    with anyio.fail_after(5):
        async with listener, anyio.create_task_group() as tasks:
            tasks.start_soon(listener.serve, serve)
            stream = await call(
                backend_type().connect_tcp, "127.0.0.1", listener.extra(anyio.abc.SocketAttribute.local_port)
            )
            try:
                for _ in range(layers):
                    stream = await call(stream.start_tls, client_context, server_hostname="localhost", timeout=5)
                ready.set()
                await challenge_sent.wait()
                probe = stream.get_extra_info("read_available")
                assert await call(probe, 65536, timeout=5) is None
                assert await call(probe, 65536, timeout=5) is None
                send_response.set()
                if flush_on_read:
                    assert await call(stream.read, 65536, timeout=5) == b"response"
                await call(stream.write, b"request", timeout=5)
                if not flush_on_read:
                    assert await call(stream.read, 65536, timeout=5) == b"response"
                await done.wait()
            finally:
                if backend_type is httpcore2.SyncBackend:
                    stream.close()
                else:
                    await stream.aclose()
                tasks.cancel_scope.cancel()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "anyio_backend,backend_type",
    [("asyncio", httpcore2.AnyIOBackend), ("trio", httpcore2.AnyIOBackend), ("trio", httpcore2.TrioBackend)],
)
@pytest.mark.parametrize("tls", [False, True])
async def test_read_available_with_custom_transport(
    anyio_backend: str, backend_type: type[httpcore2.AsyncNetworkBackend], tls: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    ca = trustme.CA()
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ca.issue_cert("localhost").configure_cert(server_context)
    client_context = ssl.create_default_context()
    ca.configure_trust(client_context)

    async def serve(peer: anyio.abc.SocketStream) -> None:
        async with peer:
            transport: anyio.abc.ByteStream = peer
            if tls:
                transport = await anyio.streams.tls.TLSStream.wrap(
                    peer, ssl_context=server_context, server_side=True, standard_compatible=False
                )
            assert await transport.receive() == b"request"
            await transport.send(b"response")

    module = trio if backend_type is httpcore2.TrioBackend else anyio
    name = "open_tcp_stream" if backend_type is httpcore2.TrioBackend else "connect_tcp"
    connect: Callable[..., Any] = trio.open_tcp_stream if backend_type is httpcore2.TrioBackend else anyio.connect_tcp

    async def custom_connect(*args: Any, **kwargs: Any) -> Any:
        transport = await connect(*args, **kwargs)
        if backend_type is httpcore2.TrioBackend:
            if not tls:
                return trio.StapledStream(transport, transport)
            wrapped = trio.SSLStream(transport, client_context, server_hostname="localhost", https_compatible=True)
            await wrapped.do_handshake()
            return wrapped
        if not tls:
            return anyio.streams.stapled.StapledByteStream(transport, transport)
        return await anyio.streams.tls.TLSStream.wrap(
            transport, ssl_context=client_context, hostname="localhost", standard_compatible=False
        )

    monkeypatch.setattr(module, name, custom_connect)
    listener = await anyio.create_tcp_listener(local_host="127.0.0.1")
    with anyio.fail_after(5):
        async with listener, anyio.create_task_group() as tasks:
            tasks.start_soon(listener.serve, serve)
            stream = await backend_type().connect_tcp("127.0.0.1", listener.extra(anyio.abc.SocketAttribute.local_port))
            try:
                assert await stream.get_extra_info("read_available")(65536, timeout=5) is None
                await stream.write(b"request", timeout=5)
                assert await stream.read(65536, timeout=5) == b"response"
            finally:
                await stream.aclose()
                tasks.cancel_scope.cancel()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "anyio_backend,backend_type",
    [("asyncio", httpcore2.AnyIOBackend), ("trio", httpcore2.AnyIOBackend), ("trio", httpcore2.TrioBackend)],
)
@pytest.mark.parametrize("layers", [1, 2])
async def test_read_during_blocked_write(
    anyio_backend: str, backend_type: type[httpcore2.AsyncNetworkBackend], layers: int
) -> None:
    ca = trustme.CA()
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ca.issue_cert("localhost").configure_cert(server_context)
    client_context = ssl.create_default_context()
    ca.configure_trust(client_context)
    connected, send_response, read_request, write_done, server_done = (anyio.Event() for _ in range(5))
    body = b"x" * (8 * 1024 * 1024)

    async def serve(peer: anyio.abc.SocketStream) -> None:
        stream: anyio.abc.ByteStream = peer
        async with peer:
            for _ in range(layers):
                stream = await anyio.streams.tls.TLSStream.wrap(
                    stream, ssl_context=server_context, server_side=True, standard_compatible=False
                )
            connected.set()
            await send_response.wait()
            await stream.send(b"ready")
            await read_request.wait()
            received = 0
            while received < len(body):
                data = await stream.receive()
                assert data == b"x" * len(data)
                received += len(data)
            server_done.set()

    listener = await anyio.create_tcp_listener(local_host="127.0.0.1")
    with anyio.fail_after(5):
        async with listener, anyio.create_task_group() as tasks:
            tasks.start_soon(listener.serve, serve)
            stream = await backend_type().connect_tcp(
                "127.0.0.1",
                listener.extra(anyio.abc.SocketAttribute.local_port),
                socket_options=[(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)],
            )
            try:
                for _ in range(layers):
                    stream = await stream.start_tls(client_context, server_hostname="localhost", timeout=5)
                await connected.wait()

                async def write() -> None:
                    await stream.write(body, timeout=5)
                    write_done.set()

                tasks.start_soon(write)
                await anyio.wait_all_tasks_blocked()
                assert not write_done.is_set()
                send_response.set()
                assert await stream.read(65536, timeout=5) == b"ready"
                assert not write_done.is_set()
                read_request.set()
                await write_done.wait()
                await server_done.wait()
            finally:
                await stream.aclose()
                tasks.cancel_scope.cancel()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "anyio_backend,backend_type",
    [("asyncio", httpcore2.AnyIOBackend), ("trio", httpcore2.AnyIOBackend), ("trio", httpcore2.TrioBackend)],
)
@pytest.mark.parametrize("read_available", [False, True])
async def test_read_invalid_tls_record(
    anyio_backend: str, backend_type: type[httpcore2.AsyncNetworkBackend], read_available: bool
) -> None:
    ca = trustme.CA()
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ca.issue_cert("localhost").configure_cert(server_context)
    client_context = ssl.create_default_context()
    ca.configure_trust(client_context)
    corrupt, sent = anyio.Event(), anyio.Event()

    async def serve(peer: anyio.abc.SocketStream) -> None:
        async with peer:
            await anyio.streams.tls.TLSStream.wrap(
                peer, ssl_context=server_context, server_side=True, standard_compatible=False
            )
            await corrupt.wait()
            await peer.send(b"\x17\x03\x03\x00\x01\x00")
            sent.set()
            await anyio.sleep_forever()

    listener = await anyio.create_tcp_listener(local_host="127.0.0.1")
    with anyio.fail_after(5):
        async with listener, anyio.create_task_group() as tasks:
            tasks.start_soon(listener.serve, serve)
            stream = await backend_type().connect_tcp("127.0.0.1", listener.extra(anyio.abc.SocketAttribute.local_port))
            try:
                stream = await stream.start_tls(client_context, server_hostname="localhost", timeout=5)
                if not read_available:
                    corrupt.set()
                read = stream.get_extra_info("read_available") if read_available else stream.read
                with pytest.raises(httpcore2.ReadError):
                    while await read(65536, timeout=5) is None:
                        corrupt.set()
                        await sent.wait()
            finally:
                await stream.aclose()
                tasks.cancel_scope.cancel()
