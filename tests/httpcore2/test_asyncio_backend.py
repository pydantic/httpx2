from __future__ import annotations

import asyncio
import contextlib
import socket
import ssl
import sys
import typing
from collections.abc import AsyncIterator, Awaitable, Callable

import pytest
import trustme

import httpcore2
from httpcore2._backends.asyncio import (
    RECEIVE_HIGH_WATER,
    RECEIVE_LOW_WATER,
    AsyncioStream,
    AsyncioStreamProtocol,
)

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


Handler = Callable[[asyncio.StreamReader, asyncio.StreamWriter], Awaitable[None]]


async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    # The client may drop the connection mid-way, for example when a TLS handshake fails.
    with contextlib.suppress(ConnectionError, ssl.SSLError):
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    writer.close()


async def silent(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    # Hold the connection open, without answering, until the client goes away.
    await reader.read()
    writer.close()


@pytest.fixture
async def serve() -> AsyncIterator[Callable[..., Awaitable[int]]]:
    servers: list[asyncio.base_events.Server] = []

    async def start(handler: Handler = echo, **kwargs: typing.Any) -> int:
        server = await asyncio.start_server(handler, "127.0.0.1", 0, **kwargs)
        servers.append(server)
        port: int = server.sockets[0].getsockname()[1]
        return port

    yield start
    for server in servers:
        server.close()
        await server.wait_closed()


async def test_tcp_roundtrip_and_extra_info(serve: Callable[..., Awaitable[int]]) -> None:
    port = await serve()
    backend = httpcore2.AsyncioBackend()
    stream = await backend.connect_tcp("127.0.0.1", port, socket_options=[(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)])
    assert stream.get_extra_info("socket").getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY) == 1
    assert stream.get_extra_info("client_addr")[0] == "127.0.0.1"
    assert stream.get_extra_info("server_addr") == ("127.0.0.1", port)
    assert stream.get_extra_info("ssl_object") is None
    assert stream.get_extra_info("invalid") is None
    assert not stream.get_extra_info("is_readable")

    await stream.write(b"hello")
    assert await stream.read(1024) == b"hello"
    assert not stream.get_extra_info("is_readable")

    await stream.aclose()
    assert stream.get_extra_info("is_readable")


async def test_read_at_eof(serve: Callable[..., Awaitable[int]]) -> None:
    async def close_after_hello(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.write(b"hello")
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    port = await serve(close_after_hello)
    stream = await httpcore2.AsyncioBackend().connect_tcp("127.0.0.1", port)
    assert await stream.read(1024) == b"hello"
    assert await stream.read(1024) == b""
    assert await stream.read(1024) == b""
    await stream.aclose()


async def test_read_timeout(serve: Callable[..., Awaitable[int]]) -> None:
    port = await serve(silent)
    stream = await httpcore2.AsyncioBackend().connect_tcp("127.0.0.1", port)
    with pytest.raises(httpcore2.ReadTimeout):
        await stream.read(1024, timeout=0.05)
    await stream.aclose()


async def test_invalid_socket_option(serve: Callable[..., Awaitable[int]]) -> None:
    port = await serve()
    with pytest.raises(httpcore2.ConnectError):
        await httpcore2.AsyncioBackend().connect_tcp("127.0.0.1", port, socket_options=[(socket.SOL_SOCKET, -1, 1)])


async def test_connect_refused() -> None:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    with pytest.raises(httpcore2.ConnectError):
        await httpcore2.AsyncioBackend().connect_tcp("127.0.0.1", port)


async def test_connect_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    async def never_connects(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
        await asyncio.sleep(10)

    monkeypatch.setattr(asyncio.get_running_loop(), "create_connection", never_connects)
    with pytest.raises(httpcore2.ConnectTimeout):
        await httpcore2.AsyncioBackend().connect_tcp("127.0.0.1", 1, timeout=0.05)


@pytest.mark.skipif(sys.platform == "win32", reason="Unix sockets are not available on Windows.")
async def test_unix_socket(tmp_path: typing.Any) -> None:
    path = str(tmp_path / "socket")
    server = await asyncio.start_unix_server(echo, path)
    try:
        stream = await httpcore2.AsyncioBackend().connect_unix_socket(path)
        await stream.write(b"hello")
        assert await stream.read(1024) == b"hello"
        await stream.aclose()
    finally:
        server.close()
        await server.wait_closed()

    with pytest.raises(httpcore2.ConnectError):
        await httpcore2.AsyncioBackend().connect_unix_socket(str(tmp_path / "missing"))


async def test_start_tls(serve: Callable[..., Awaitable[int]]) -> None:
    ca = trustme.CA()
    server_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ca.issue_cert("localhost").configure_cert(server_context)
    client_context = ssl.create_default_context()
    ca.configure_trust(client_context)

    port = await serve(ssl=server_context)
    stream = await httpcore2.AsyncioBackend().connect_tcp("127.0.0.1", port)
    tls_stream = await stream.start_tls(client_context, server_hostname="localhost", timeout=5.0)
    assert tls_stream.get_extra_info("ssl_object").version() == "TLSv1.3"
    await tls_stream.write(b"hello")
    assert await tls_stream.read(1024) == b"hello"
    await tls_stream.aclose()


async def test_start_tls_failure(serve: Callable[..., Awaitable[int]]) -> None:
    # A plain echo server answers the ClientHello with the ClientHello.
    port = await serve()
    stream = await httpcore2.AsyncioBackend().connect_tcp("127.0.0.1", port)
    with pytest.raises(httpcore2.ConnectError):
        await stream.start_tls(ssl.create_default_context(), server_hostname="localhost", timeout=5.0)


async def test_start_tls_timeout(serve: Callable[..., Awaitable[int]]) -> None:
    port = await serve(silent)
    stream = await httpcore2.AsyncioBackend().connect_tcp("127.0.0.1", port)
    with pytest.raises(httpcore2.ConnectTimeout):
        await stream.start_tls(ssl.create_default_context(), server_hostname="localhost", timeout=0.05)


async def test_sleep() -> None:
    await httpcore2.AsyncioBackend().sleep(0)


# The remaining behaviour is driven through a fake transport, so the timing
# of the real network is not involved.


class FakeTransport(asyncio.Transport):
    def __init__(self) -> None:
        super().__init__()
        self.written: list[bytes] = []
        self.reading_paused = False
        self.closed = False

    def write(self, data: typing.Any) -> None:
        self.written.append(bytes(data))

    def is_closing(self) -> bool:
        return self.closed

    def close(self) -> None:
        self.closed = True

    def abort(self) -> None:
        self.closed = True

    def pause_reading(self) -> None:
        self.reading_paused = True

    def resume_reading(self) -> None:
        self.reading_paused = False


def fake_stream() -> tuple[AsyncioStream, AsyncioStreamProtocol, FakeTransport]:
    transport = FakeTransport()
    protocol = AsyncioStreamProtocol()
    protocol.connection_made(transport)
    return AsyncioStream(transport, protocol), protocol, transport


async def test_read_splits_large_chunks() -> None:
    stream, protocol, _ = fake_stream()
    protocol.data_received(b"abcdef")
    assert await stream.read(4) == b"abcd"
    assert await stream.read(4) == b"ef"
    assert protocol.buffered == 0


async def test_backpressure_pauses_and_resumes_reading() -> None:
    stream, protocol, transport = fake_stream()
    protocol.data_received(b"x" * RECEIVE_HIGH_WATER)
    assert transport.reading_paused
    protocol.data_received(b"y")
    assert transport.reading_paused

    await stream.read(RECEIVE_HIGH_WATER - RECEIVE_LOW_WATER - 1)
    assert protocol.buffered == RECEIVE_LOW_WATER + 2
    assert transport.reading_paused
    await stream.read(2)
    assert not transport.reading_paused


async def test_pending_read_wakes_on_eof() -> None:
    stream, protocol, _ = fake_stream()
    read = asyncio.ensure_future(stream.read(1024))
    await asyncio.sleep(0)
    assert protocol.eof_received() is False
    assert await read == b""


async def test_pending_read_wakes_on_connection_lost_with_error() -> None:
    stream, protocol, _ = fake_stream()
    read = asyncio.ensure_future(stream.read(1024))
    await asyncio.sleep(0)
    protocol.connection_lost(OSError("reset"))
    with pytest.raises(httpcore2.ReadError):
        await read
    with pytest.raises(httpcore2.ReadError):
        await stream.read(1024)


async def test_connection_lost_without_error_reads_as_eof() -> None:
    stream, protocol, _ = fake_stream()
    protocol.data_received(b"tail")
    protocol.connection_lost(None)
    assert await stream.read(1024) == b"tail"
    assert await stream.read(1024) == b""
    assert stream.get_extra_info("is_readable")


async def test_write() -> None:
    stream, protocol, transport = fake_stream()
    await stream.write(b"")
    await stream.write(b"hello")
    assert transport.written == [b"hello"]

    transport.close()
    with pytest.raises(httpcore2.WriteError):
        await stream.write(b"more")

    protocol.connection_lost(None)
    with pytest.raises(httpcore2.WriteError):
        await stream.write(b"more")


async def test_aclose_forces_a_stuck_close() -> None:
    stream, protocol, transport = fake_stream()
    # The fake transport never reports the connection as lost by itself.
    await stream.aclose()
    assert transport.closed
    protocol.connection_lost(None)
    await stream.aclose()


async def test_write_waits_for_the_transport_to_drain() -> None:
    stream, protocol, transport = fake_stream()
    protocol.pause_writing()
    write = asyncio.ensure_future(stream.write(b"hello"))
    await asyncio.sleep(0)
    assert not write.done()
    protocol.resume_writing()
    await write
    assert transport.written == [b"hello"]


async def test_write_timeout_while_draining() -> None:
    stream, protocol, _ = fake_stream()
    protocol.pause_writing()
    with pytest.raises(httpcore2.WriteTimeout):
        await stream.write(b"hello", timeout=0.01)


async def test_write_fails_when_the_connection_is_lost_while_draining() -> None:
    stream, protocol, _ = fake_stream()
    protocol.pause_writing()
    write = asyncio.ensure_future(stream.write(b"hello"))
    await asyncio.sleep(0)
    protocol.connection_lost(OSError("reset"))
    with pytest.raises(httpcore2.WriteError):
        await write
