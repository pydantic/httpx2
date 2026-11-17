from __future__ import annotations

import asyncio
import contextlib
import pathlib
import socket
import ssl
import struct
import threading
import typing

import pytest
import trustme

import httpcore2
from httpcore2._backends import asyncio as asyncio_backend
from httpcore2._backends.asyncio import AddrInfo, AsyncIOBackend, AsyncIOStream

Handler = typing.Callable[[asyncio.StreamReader, asyncio.StreamWriter], typing.Awaitable[None]]

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(scope="module")
def cert_authority() -> trustme.CA:
    return trustme.CA()


@pytest.fixture(scope="module")
def server_context(cert_authority: trustme.CA) -> ssl.SSLContext:
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    cert_authority.issue_cert("localhost").configure_cert(context)
    return context


@pytest.fixture(scope="module")
def client_context(cert_authority: trustme.CA) -> ssl.SSLContext:
    context = ssl.create_default_context()
    cert_authority.configure_trust(context)
    return context


@pytest.fixture
def silent_port() -> typing.Iterator[int]:
    """
    A listening socket that never accepts: connections succeed, but nothing is ever read or sent.
    """
    with socket.create_server(("127.0.0.1", 0)) as listener:
        yield listener.getsockname()[1]


@pytest.fixture
def closed_port() -> int:
    with socket.create_server(("127.0.0.1", 0)) as listener:
        return int(listener.getsockname()[1])


@contextlib.asynccontextmanager
async def serve(handler: Handler, ssl_context: ssl.SSLContext | None = None) -> typing.AsyncIterator[int]:
    server = await asyncio.start_server(handler, "127.0.0.1", 0, ssl=ssl_context)
    try:
        yield server.sockets[0].getsockname()[1]
    finally:
        server.close()


async def wait_until(condition: typing.Callable[[], bool]) -> None:
    """
    Wait for the event loop to process network events, without assuming how long that takes.
    """
    async with asyncio.timeout(5):
        while True:
            await asyncio.sleep(0.001)
            if condition():
                return


def internals(stream: httpcore2.AsyncNetworkStream) -> AsyncIOStream:
    assert isinstance(stream, AsyncIOStream)
    return stream


async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    while data := await reader.read(65536):
        writer.write(data)
    writer.close()


async def test_read_write() -> None:
    async with serve(echo) as port:
        stream = await AsyncIOBackend().connect_tcp("127.0.0.1", port)
        await stream.write(b"")
        await stream.write(b"hello")
        assert await stream.read(65536, timeout=5) == b"hello"
        await stream.aclose()
        await stream.aclose()

        with pytest.raises(httpcore2.ReadError):
            await stream.read(65536)
        with pytest.raises(httpcore2.WriteError):
            await stream.write(b"hello")


async def test_extra_info() -> None:
    async with serve(echo) as port:
        stream = await AsyncIOBackend().connect_tcp("127.0.0.1", port)
        try:
            assert stream.get_extra_info("client_addr")[0] == "127.0.0.1"
            assert stream.get_extra_info("server_addr") == ("127.0.0.1", port)
            assert stream.get_extra_info("socket").getpeername() == ("127.0.0.1", port)
            assert stream.get_extra_info("ssl_object") is None
            assert stream.get_extra_info("is_readable") is False
            assert stream.get_extra_info("unknown") is None
        finally:
            await stream.aclose()
        assert stream.get_extra_info("is_readable") is True


async def test_read_timeout(silent_port: int) -> None:
    stream = await AsyncIOBackend().connect_tcp("127.0.0.1", silent_port)
    try:
        with pytest.raises(httpcore2.ReadTimeout):
            await stream.read(65536, timeout=0.01)
    finally:
        await stream.aclose()


async def test_write_timeout(silent_port: int) -> None:
    stream = await AsyncIOBackend().connect_tcp("127.0.0.1", silent_port)
    try:
        with pytest.raises(httpcore2.WriteTimeout):
            # Larger than the socket buffers, which nobody drains.
            await stream.write(b"x" * 64 * 1024 * 1024, timeout=0.1)
    finally:
        await stream.aclose()


async def test_server_closes_connection() -> None:
    async def close(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.close()

    async with serve(close) as port:
        stream = await AsyncIOBackend().connect_tcp("127.0.0.1", port)
        try:
            await wait_until(internals(stream)._reader.at_eof)
            # asyncio has already read the EOF from the socket, so this relies on the reader's state.
            assert stream.get_extra_info("is_readable") is True
            assert await stream.read(65536) == b""
        finally:
            await stream.aclose()


async def test_server_resets_connection() -> None:
    async def reset(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.get_extra_info("socket").setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        writer.transport.abort()

    async with serve(reset) as port:
        stream = await AsyncIOBackend().connect_tcp("127.0.0.1", port)
        await wait_until(lambda: internals(stream)._reader.exception() is not None)
        assert stream.get_extra_info("is_readable") is True
        with pytest.raises(httpcore2.ReadError):
            await stream.read(65536)
        with pytest.raises(httpcore2.WriteError):
            await stream.write(b"hello")
        await stream.aclose()


async def test_unexpected_data_is_readable() -> None:
    async def send_unprompted(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.write(b"unexpected")
        await reader.read()
        writer.close()

    async with serve(send_unprompted) as port:
        stream = await AsyncIOBackend().connect_tcp("127.0.0.1", port)
        try:
            await wait_until(internals(stream)._has_buffered_data)
            assert stream.get_extra_info("is_readable") is True
        finally:
            await stream.aclose()


async def test_start_tls(server_context: ssl.SSLContext, client_context: ssl.SSLContext) -> None:
    async def tls_echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await writer.start_tls(server_context)
        await echo(reader, writer)

    async with serve(tls_echo) as port:
        tcp_stream = await AsyncIOBackend().connect_tcp("127.0.0.1", port)
        stream = await tcp_stream.start_tls(client_context, server_hostname="localhost", timeout=5)
        try:
            assert stream.get_extra_info("ssl_object").version() == "TLSv1.3"
            await stream.write(b"hello")
            assert await stream.read(65536, timeout=5) == b"hello"
            # The TCP stream handed its connection over to the TLS stream.
            with pytest.raises(httpcore2.ReadError):
                await tcp_stream.read(65536)
        finally:
            await stream.aclose()


@pytest.fixture
def data_then_ragged_eof_tls_server(server_context: ssl.SSLContext) -> typing.Iterator[int]:
    """
    A TLS server that sends a response, then ends its side without a TLS `close_notify`,
    keeping the connection open until the test is done.
    """
    done = threading.Event()

    def serve() -> None:
        raw, _ = listener.accept()
        with server_context.wrap_socket(raw, server_side=True) as conn:
            conn.sendall(b"response")
            conn.shutdown(socket.SHUT_WR)
            done.wait()

    with socket.create_server(("127.0.0.1", 0)) as listener:
        thread = threading.Thread(target=serve)
        thread.start()
        yield listener.getsockname()[1]
        done.set()
        thread.join()


async def test_write_after_tls_eof_with_unread_data(
    data_then_ragged_eof_tls_server: int, client_context: ssl.SSLContext
) -> None:
    """
    asyncio drops writes after a TLS EOF, even while received data is still unread, so they must fail.
    """
    stream = await AsyncIOBackend().connect_tcp("127.0.0.1", data_then_ragged_eof_tls_server)
    stream = await stream.start_tls(client_context, server_hostname="localhost", timeout=5)
    try:
        # Write as soon as the EOF arrives, before asyncio finishes shutting down the TLS layer.
        reader = internals(stream)._reader
        async with asyncio.timeout(5):
            while not getattr(reader, "_eof", False):
                await asyncio.sleep(0)
        with pytest.raises(httpcore2.WriteError):
            await stream.write(b"request")
        assert await stream.read(65536) == b"response"
        assert await stream.read(65536) == b""
    finally:
        await stream.aclose()


async def test_start_tls_rejects_data_received_before_handshake(client_context: ssl.SSLContext) -> None:
    """
    Data that arrives before the TLS handshake is plaintext, and must not be read as if it came over TLS.
    """

    async def inject(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.write(b"injected")
        await reader.read()
        writer.close()

    async with serve(inject) as port:
        stream = await AsyncIOBackend().connect_tcp("127.0.0.1", port)
        await wait_until(internals(stream)._has_buffered_data)
        with pytest.raises(httpcore2.ConnectError, match="unexpected data"):
            await stream.start_tls(client_context, server_hostname="localhost")
        with pytest.raises(httpcore2.ReadError):
            await stream.read(65536)


async def test_start_tls_timeout(silent_port: int, client_context: ssl.SSLContext) -> None:
    stream = await AsyncIOBackend().connect_tcp("127.0.0.1", silent_port)
    with pytest.raises(httpcore2.ConnectTimeout):
        await stream.start_tls(client_context, server_hostname="localhost", timeout=0.05)
    with pytest.raises(httpcore2.ReadError):
        await stream.read(65536)


async def test_start_tls_error(client_context: ssl.SSLContext) -> None:
    # The echoed ClientHello is not a valid server response.
    async with serve(echo) as port:
        stream = await AsyncIOBackend().connect_tcp("127.0.0.1", port)
        with pytest.raises(httpcore2.ConnectError):
            await stream.start_tls(client_context, server_hostname="localhost", timeout=5)


@pytest.mark.parametrize(
    ("timeout", "handshake_timeout"), [(None, asyncio_backend.NO_HANDSHAKE_TIMEOUT), (120.0, 121.0)]
)
async def test_start_tls_handshake_timeout(
    monkeypatch: pytest.MonkeyPatch,
    server_context: ssl.SSLContext,
    client_context: ssl.SSLContext,
    timeout: float | None,
    handshake_timeout: float,
) -> None:
    """
    asyncio's own handshake timeout must never fire before ours, or at all without one.
    """
    start_tls = asyncio.StreamWriter.start_tls
    seen: list[float | None] = []

    async def spy(self: asyncio.StreamWriter, *args: typing.Any, **kwargs: typing.Any) -> None:
        seen.append(kwargs["ssl_handshake_timeout"])
        await start_tls(self, *args, **kwargs)

    monkeypatch.setattr(asyncio.StreamWriter, "start_tls", spy)
    async with serve(echo, ssl_context=server_context) as port:
        stream = await AsyncIOBackend().connect_tcp("127.0.0.1", port)
        stream = await stream.start_tls(client_context, server_hostname="localhost", timeout=timeout)
        await stream.aclose()
    assert seen == [handshake_timeout]


async def test_connect_refused(closed_port: int) -> None:
    with pytest.raises(httpcore2.ConnectError):
        await AsyncIOBackend().connect_tcp("127.0.0.1", closed_port)


async def test_connect_invalid_hostname() -> None:
    with pytest.raises(httpcore2.ConnectError):
        await AsyncIOBackend().connect_tcp("invalid..hostname", 80)


async def test_connect_localhost() -> None:
    async with serve(echo) as port:
        stream = await AsyncIOBackend().connect_tcp("localhost", port)
        await stream.aclose()


async def test_connect_local_address() -> None:
    async with serve(echo) as port:
        stream = await AsyncIOBackend().connect_tcp("127.0.0.1", port, local_address="127.0.0.1")
        try:
            assert stream.get_extra_info("client_addr")[0] == "127.0.0.1"
        finally:
            await stream.aclose()


async def test_connect_socket_options() -> None:
    async with serve(echo) as port:
        option = (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        stream = await AsyncIOBackend().connect_tcp("127.0.0.1", port, socket_options=[option])
        try:
            assert stream.get_extra_info("socket").getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE)
        finally:
            await stream.aclose()

        with pytest.raises(httpcore2.ConnectError):
            await AsyncIOBackend().connect_tcp("127.0.0.1", port, socket_options=[(socket.SOL_SOCKET, 12345, 1)])


async def test_connect_stream_setup_error(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fail(**kwargs: typing.Any) -> typing.NoReturn:
        raise OSError("setup failed")

    async with serve(echo) as port:
        monkeypatch.setattr(asyncio, "open_connection", fail)
        with pytest.raises(httpcore2.ConnectError, match="setup failed"):
            await AsyncIOBackend().connect_tcp("127.0.0.1", port)


async def test_unix_socket(tmp_path: pathlib.Path) -> None:
    path = str(tmp_path / "socket")
    server = await asyncio.start_unix_server(echo, path)
    try:
        stream = await AsyncIOBackend().connect_unix_socket(path, timeout=5)
        await stream.write(b"hello")
        assert await stream.read(65536, timeout=5) == b"hello"
        await stream.aclose()

        option = (socket.SOL_SOCKET, socket.SO_RCVBUF, 65536)
        stream = await AsyncIOBackend().connect_unix_socket(path, socket_options=[option])
        await stream.aclose()

        with pytest.raises(httpcore2.ConnectError):
            await AsyncIOBackend().connect_unix_socket(path, socket_options=[(socket.SOL_SOCKET, 12345, 1)])
    finally:
        server.close()


async def test_sleep() -> None:
    await AsyncIOBackend().sleep(0)


# Happy eyeballs


def tcp_v4(port: int) -> AddrInfo:
    return (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("127.0.0.1", port))


def tcp_v6(port: int) -> AddrInfo:
    return (socket.AF_INET6, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("::1", port, 0, 0))


def resolve_to(monkeypatch: pytest.MonkeyPatch, infos: list[AddrInfo]) -> None:
    async def getaddrinfo(host: str, port: int, family: int) -> list[AddrInfo]:
        return infos

    monkeypatch.setattr(asyncio_backend, "_getaddrinfo", getaddrinfo)


def hang_on_port(monkeypatch: pytest.MonkeyPatch, hanging_port: int) -> list[int]:
    """
    Make connection attempts to `hanging_port` never complete. Returns the ports attempted.
    """
    connect_socket = asyncio_backend._connect_socket
    attempted: list[int] = []

    async def fake(info: AddrInfo, local_addr: tuple[typing.Any, ...] | None) -> socket.socket:
        attempted.append(info[4][1])
        if info[4][1] == hanging_port:
            await asyncio.sleep(3600)
        return await connect_socket(info, local_addr)

    monkeypatch.setattr(asyncio_backend, "_connect_socket", fake)
    monkeypatch.setattr(asyncio_backend, "HAPPY_EYEBALLS_DELAY", 0.01)
    return attempted


async def test_happy_eyeballs_slow_address(monkeypatch: pytest.MonkeyPatch) -> None:
    async with serve(echo) as port:
        resolve_to(monkeypatch, [tcp_v4(1), tcp_v4(port)])
        attempted = hang_on_port(monkeypatch, 1)
        stream = await AsyncIOBackend().connect_tcp("example.com", port, timeout=5)
        await stream.aclose()
    assert attempted == [1, port]


async def test_happy_eyeballs_failed_address(monkeypatch: pytest.MonkeyPatch, closed_port: int) -> None:
    async with serve(echo) as port:
        resolve_to(monkeypatch, [tcp_v4(closed_port), tcp_v4(port)])
        stream = await AsyncIOBackend().connect_tcp("example.com", port, timeout=5)
        await stream.aclose()


async def test_happy_eyeballs_all_addresses_fail(monkeypatch: pytest.MonkeyPatch, closed_port: int) -> None:
    resolve_to(monkeypatch, [tcp_v4(closed_port), tcp_v4(closed_port)])
    with pytest.raises(httpcore2.ConnectError, match="All connection attempts failed"):
        await AsyncIOBackend().connect_tcp("example.com", closed_port, timeout=5)


async def test_happy_eyeballs_unexpected_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Only network errors move on to the next address. Anything else is a bug, and is raised as is.
    """

    async def connect_socket(info: AddrInfo, local_addr: tuple[typing.Any, ...] | None) -> socket.socket:
        raise RuntimeError("bug")

    resolve_to(monkeypatch, [tcp_v4(1), tcp_v4(2)])
    monkeypatch.setattr(asyncio_backend, "_connect_socket", connect_socket)
    with pytest.raises(RuntimeError, match="bug"):
        await AsyncIOBackend().connect_tcp("example.com", 80)


async def test_happy_eyeballs_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    resolve_to(monkeypatch, [tcp_v4(1), tcp_v4(1), tcp_v4(1)])
    attempted = hang_on_port(monkeypatch, 1)
    with pytest.raises(httpcore2.ConnectTimeout):
        await AsyncIOBackend().connect_tcp("example.com", 1, timeout=1)
    assert attempted == [1, 1, 1]


async def test_happy_eyeballs_simultaneous_success(monkeypatch: pytest.MonkeyPatch) -> None:
    sockets: list[socket.socket] = []
    both_started = asyncio.Event()

    async def connect_socket(info: AddrInfo, local_addr: tuple[typing.Any, ...] | None) -> socket.socket:
        sock = socket.socket()
        sockets.append(sock)
        if len(sockets) == 2:
            both_started.set()
        await both_started.wait()
        return sock

    resolve_to(monkeypatch, [tcp_v4(1), tcp_v4(2)])
    monkeypatch.setattr(asyncio_backend, "_connect_socket", connect_socket)
    monkeypatch.setattr(asyncio_backend, "HAPPY_EYEBALLS_DELAY", 0.01)
    winner = await asyncio_backend._happy_eyeballs_connect("example.com", 80, None)
    try:
        # Both attempts complete together: one wins, and the other is closed.
        assert winner in sockets
        assert sorted(sock.fileno() == -1 for sock in sockets) == [False, True]
    finally:
        winner.close()


async def test_getaddrinfo_ip_literals() -> None:
    assert await asyncio_backend._getaddrinfo("127.0.0.1", 80, socket.AF_UNSPEC) == [tcp_v4(80)]
    assert await asyncio_backend._getaddrinfo("::1", 80, socket.AF_UNSPEC) == [tcp_v6(80)]


def test_interleave_families() -> None:
    infos = [tcp_v6(1), tcp_v6(2), tcp_v6(3), tcp_v4(4)]
    assert asyncio_backend._interleave_families(infos) == [tcp_v6(1), tcp_v4(4), tcp_v6(2), tcp_v6(3)]
