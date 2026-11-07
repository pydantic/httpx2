from __future__ import annotations

import asyncio
import ipaddress
import socket
import ssl
import typing

from .._exceptions import (
    ConnectError,
    ConnectTimeout,
    ExceptionMapping,
    ReadError,
    ReadTimeout,
    WriteError,
    WriteTimeout,
    map_exceptions,
)
from .base import SOCKET_OPTION, AsyncNetworkBackend, AsyncNetworkStream

# Delay between connection attempts, see RFC 8305.
HAPPY_EYEBALLS_DELAY = 0.25
# asyncio always limits the TLS handshake, so this stands in for no timeout.
NO_HANDSHAKE_TIMEOUT = 365 * 24 * 60 * 60.0


class AsyncIOStream(AsyncNetworkStream):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader = reader
        self._writer = writer
        self._closed = False

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        exc_map: ExceptionMapping = {
            TimeoutError: ReadTimeout,
            OSError: ReadError,
        }
        with map_exceptions(exc_map):
            if self._closed:
                raise OSError("Stream is closed.")
            async with asyncio.timeout(timeout):
                return await self._reader.read(max_bytes)

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        if not buffer:
            return

        exc_map: ExceptionMapping = {
            TimeoutError: WriteTimeout,
            OSError: WriteError,
        }
        with map_exceptions(exc_map):
            if self._closed or self._writer.is_closing() or self._is_tls_closed_by_peer():
                raise OSError("Stream is closed.")
            async with asyncio.timeout(timeout):
                self._writer.write(buffer)
                await self._writer.drain()

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        transport = self._writer.transport
        if transport.is_closing():
            return
        # Don't wait for the peer, as a TLS shutdown could block for `ssl_shutdown_timeout`.
        transport.close()
        try:
            await asyncio.sleep(0)
        finally:
            transport.abort()

    async def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> AsyncNetworkStream:
        exc_map: ExceptionMapping = {
            TimeoutError: ConnectTimeout,
            OSError: ConnectError,
        }
        # Stay above `timeout`, so that it's reported as a timeout rather than a connection error.
        handshake_timeout = NO_HANDSHAKE_TIMEOUT if timeout is None else timeout + 1.0
        with map_exceptions(exc_map):
            try:
                if self._has_buffered_data():
                    # This is plaintext, and must not be read back as if it came over TLS.
                    raise OSError("Received unexpected data before the TLS handshake.")
                async with asyncio.timeout(timeout):
                    await self._writer.start_tls(
                        ssl_context, server_hostname=server_hostname, ssl_handshake_timeout=handshake_timeout
                    )
            except BaseException:
                await self.aclose()
                raise
        # `start_tls` upgrades the reader and writer in place, so move them to the new stream.
        tls_stream = AsyncIOStream(self._reader, self._writer)
        self._closed = True
        return tls_stream

    def get_extra_info(self, info: str) -> typing.Any:
        if info == "ssl_object":
            return self._writer.get_extra_info("ssl_object")
        if info == "client_addr":
            return self._writer.get_extra_info("sockname")
        if info == "server_addr":
            return self._writer.get_extra_info("peername")
        if info == "socket":
            return self._writer.get_extra_info("socket")
        if info == "is_readable":
            return self._is_readable()
        return None

    def _is_readable(self) -> bool:
        # asyncio reads from the socket whenever the event loop runs, so the reader already
        # knows about any EOF, error or data from the server, without polling the socket.
        if self._closed or self._writer.is_closing():
            return True
        return self._reader.at_eof() or self._reader.exception() is not None or self._has_buffered_data()

    def _has_buffered_data(self) -> bool:
        return bool(getattr(self._reader, "_buffer", None))

    def _is_tls_closed_by_peer(self) -> bool:
        # After the peer's EOF, asyncio's TLS transport drops writes silently, even while
        # received data is still unread, and only reports itself as closing from Python 3.13.
        return bool(getattr(self._reader, "_eof", False)) and self._writer.get_extra_info("ssl_object") is not None


class AsyncIOBackend(AsyncNetworkBackend):
    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: typing.Iterable[SOCKET_OPTION] | None = None,
    ) -> AsyncNetworkStream:
        # By default TCP sockets opened in `asyncio` include TCP_NODELAY.
        if socket_options is None:
            socket_options = []
        exc_map: ExceptionMapping = {
            TimeoutError: ConnectTimeout,
            OSError: ConnectError,
            UnicodeError: ConnectError,  # Invalid hostnames fail IDNA encoding.
        }
        with map_exceptions(exc_map):
            async with asyncio.timeout(timeout):
                sock = await _happy_eyeballs_connect(host, port, local_address)
                try:
                    reader, writer = await asyncio.open_connection(sock=sock)
                except BaseException:
                    sock.close()
                    raise
            stream = AsyncIOStream(reader, writer)
            try:
                _apply_socket_options(writer, socket_options)
            except BaseException:
                await stream.aclose()
                raise
        return stream

    async def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,
        socket_options: typing.Iterable[SOCKET_OPTION] | None = None,
    ) -> AsyncNetworkStream:
        if socket_options is None:
            socket_options = []
        exc_map: ExceptionMapping = {
            TimeoutError: ConnectTimeout,
            OSError: ConnectError,
        }
        with map_exceptions(exc_map):
            async with asyncio.timeout(timeout):
                reader, writer = await asyncio.open_unix_connection(path)
            stream = AsyncIOStream(reader, writer)
            try:
                _apply_socket_options(writer, socket_options)
            except BaseException:
                await stream.aclose()
                raise
        return stream

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


def _apply_socket_options(writer: asyncio.StreamWriter, socket_options: typing.Iterable[SOCKET_OPTION]) -> None:
    sock: socket.socket | None = writer.get_extra_info("socket")
    if sock is None:  # pragma: no cover
        return
    for option in socket_options:
        sock.setsockopt(*option)


AddrInfo = tuple[int, int, int, str, tuple[typing.Any, ...]]


async def _happy_eyeballs_connect(host: str, port: int, local_address: str | None) -> socket.socket:
    """
    Connect to `host`, racing the resolved addresses as described in RFC 8305.

    A new attempt starts every `HAPPY_EYEBALLS_DELAY` seconds, or as soon as the previous one fails.
    The first attempt to succeed wins and the others are cancelled.
    """
    loop = asyncio.get_running_loop()
    family: int = socket.AF_UNSPEC
    local_addr = None
    if local_address is not None:
        family, *_, local_addr = (await _getaddrinfo(local_address, 0, socket.AF_UNSPEC))[0]
    infos = _interleave_families(await _getaddrinfo(host, port, family))

    winner: socket.socket | None = None
    errors: list[OSError] = []
    pending: set[asyncio.Task[socket.socket]] = set()
    attempts = iter(infos)
    info = next(attempts, None)
    try:
        while winner is None and (info is not None or pending):
            if info is not None:
                pending.add(loop.create_task(_connect_socket(info, local_addr)))
                info = next(attempts, None)
            delay = None if info is None else HAPPY_EYEBALLS_DELAY
            done, pending = await asyncio.wait(pending, timeout=delay, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                exc = task.exception()
                if exc is None:
                    if winner is None:
                        winner = task.result()
                    else:
                        task.result().close()
                elif isinstance(exc, OSError):
                    errors.append(exc)
                else:
                    raise exc
    finally:
        # A cancelled attempt closes its own socket.
        for task in pending:
            task.cancel()

    if winner is None:
        if len(errors) == 1:
            raise errors[0]
        raise OSError(f"All connection attempts failed: {errors}")
    return winner


async def _getaddrinfo(host: str, port: int, family: int) -> list[AddrInfo]:
    # Skip the resolver thread for IP literals.
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        if ip.version == 4 and family in (socket.AF_UNSPEC, socket.AF_INET):
            return [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (host, port))]
        if ip.version == 6 and family in (socket.AF_UNSPEC, socket.AF_INET6) and "%" not in host:
            return [(socket.AF_INET6, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (host, port, 0, 0))]
    loop = asyncio.get_running_loop()
    infos = await loop.getaddrinfo(host, port, family=family, type=socket.SOCK_STREAM)
    return [(af, kind, proto, name, addr) for af, kind, proto, name, addr in infos]


def _interleave_families(infos: list[AddrInfo]) -> list[AddrInfo]:
    # Alternate address families, starting with the resolver's first choice.
    by_family: dict[int, list[AddrInfo]] = {}
    for info in infos:
        by_family.setdefault(info[0], []).append(info)
    queues = list(by_family.values())
    result: list[AddrInfo] = []
    while queues:
        result.extend(queue.pop(0) for queue in queues)
        queues = [queue for queue in queues if queue]
    return result


async def _connect_socket(info: AddrInfo, local_addr: tuple[typing.Any, ...] | None) -> socket.socket:
    family, type_, proto, _, address = info
    sock = socket.socket(family, type_, proto)
    try:
        sock.setblocking(False)
        if local_addr is not None:
            sock.bind(local_addr)
        await asyncio.get_running_loop().sock_connect(sock, address)
    except BaseException:
        sock.close()
        raise
    return sock
