from __future__ import annotations

import asyncio
import collections
import ssl
import typing

from .._exceptions import (
    ConnectError,
    ConnectTimeout,
    ReadError,
    ReadTimeout,
    WriteError,
    WriteTimeout,
)
from .base import SOCKET_OPTION, AsyncNetworkBackend, AsyncNetworkStream

# Stop reading from the socket once this much data is buffered but unread,
# and start again once the buffer drains below the low-water mark.
RECEIVE_HIGH_WATER = 256 * 1024
RECEIVE_LOW_WATER = 64 * 1024


class _Timeout(Exception):
    """
    Raised on a waiter future when its deadline passes.
    """


def _timeout(waiter: asyncio.Future[None]) -> None:
    if not waiter.done():
        waiter.set_exception(_Timeout())


def _wake(waiter: asyncio.Future[None] | None) -> None:
    if waiter is not None and not waiter.done():
        waiter.set_result(None)


class AsyncioStreamProtocol(asyncio.Protocol):
    """
    Buffers received data for `AsyncioStream`, applying backpressure to the
    transport once too much is buffered, and wakes up pending reads and writes.
    """

    def __init__(self) -> None:
        self.transport: asyncio.Transport | None = None
        self.chunks: collections.deque[bytes] = collections.deque()
        self.buffered = 0
        self.reading_paused = False
        self.writing_paused = False
        self.eof = False
        self.closed = False
        self.exception: Exception | None = None
        self.read_waiter: asyncio.Future[None] | None = None
        self.write_waiter: asyncio.Future[None] | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        # The transport implements the interface without necessarily subclassing it.
        self.transport = typing.cast(asyncio.Transport, transport)

    def data_received(self, data: bytes) -> None:
        self.chunks.append(data)
        self.buffered += len(data)
        if self.buffered >= RECEIVE_HIGH_WATER and not self.reading_paused:
            assert self.transport is not None
            self.transport.pause_reading()
            self.reading_paused = True
        _wake(self.read_waiter)

    def eof_received(self) -> bool:
        self.eof = True
        _wake(self.read_waiter)
        # Let the transport close: an HTTP peer that has sent a FIN is done.
        return False

    def connection_lost(self, exc: Exception | None) -> None:
        self.closed = True
        self.exception = exc
        _wake(self.read_waiter)
        _wake(self.write_waiter)

    def pause_writing(self) -> None:
        self.writing_paused = True

    def resume_writing(self) -> None:
        self.writing_paused = False
        _wake(self.write_waiter)


class AsyncioStream(AsyncNetworkStream):
    def __init__(self, transport: asyncio.Transport, protocol: AsyncioStreamProtocol) -> None:
        self._transport = transport
        self._protocol = protocol

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        protocol = self._protocol
        if not protocol.chunks:
            if protocol.eof or protocol.closed:
                return self._read_at_end()
            try:
                await self._wait("read", timeout)
            except _Timeout:
                raise ReadTimeout("timed out") from None
            if not protocol.chunks:
                return self._read_at_end()

        chunk = protocol.chunks[0]
        if len(chunk) <= max_bytes:
            protocol.chunks.popleft()
        else:
            protocol.chunks[0] = chunk[max_bytes:]
            chunk = chunk[:max_bytes]
        protocol.buffered -= len(chunk)
        if protocol.reading_paused and protocol.buffered <= RECEIVE_LOW_WATER:
            protocol.reading_paused = False
            self._transport.resume_reading()
        return chunk

    def _read_at_end(self) -> bytes:
        # No buffered data and no more coming: a clean EOF reads as empty,
        # a connection dropped by an error is a read error.
        if self._protocol.exception is not None:
            raise ReadError(str(self._protocol.exception)) from self._protocol.exception
        return b""

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        if not buffer:
            return
        protocol = self._protocol
        if protocol.closed or self._transport.is_closing():
            raise WriteError("Connection closed")
        self._transport.write(buffer)
        if protocol.writing_paused:
            # The transport's send buffer is full; wait for it to drain.
            try:
                await self._wait("write", timeout)
            except _Timeout:
                raise WriteTimeout("timed out") from None
            if protocol.closed:
                raise WriteError(str(protocol.exception or "Connection closed"))

    async def _wait(self, kind: str, timeout: float | None) -> None:
        loop = asyncio.get_running_loop()
        waiter: asyncio.Future[None] = loop.create_future()
        protocol = self._protocol
        if kind == "read":
            protocol.read_waiter = waiter
        else:
            protocol.write_waiter = waiter
        handle = None if timeout is None else loop.call_later(timeout, _timeout, waiter)
        try:
            await waiter
        finally:
            if handle is not None:
                handle.cancel()
            if kind == "read":
                protocol.read_waiter = None
            else:
                protocol.write_waiter = None

    async def aclose(self) -> None:
        if self._protocol.closed:
            return
        self._transport.close()
        # Closing only schedules the socket close on the event loop. Yield once
        # so it runs now, then force it if unsent data is still holding it up.
        await asyncio.sleep(0)
        if not self._protocol.closed:
            self._transport.abort()

    async def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> AsyncNetworkStream:
        loop = asyncio.get_running_loop()
        # The loop reports its own handshake timeout as a connection error,
        # so the deadline is applied here instead to raise a timeout.
        handshake = loop.start_tls(self._transport, self._protocol, ssl_context, server_hostname=server_hostname)
        try:
            transport = await asyncio.wait_for(handshake, timeout)
        except (TimeoutError, asyncio.TimeoutError):
            self._transport.close()
            raise ConnectTimeout("timed out") from None
        except (OSError, ssl.SSLError) as exc:
            self._transport.close()
            raise ConnectError(str(exc)) from exc
        if transport is None:  # pragma: no cover
            raise ConnectError("TLS handshake failed")
        self._protocol.transport = transport
        return AsyncioStream(transport, self._protocol)

    def get_extra_info(self, info: str) -> typing.Any:
        if info == "ssl_object":
            return self._transport.get_extra_info("ssl_object")
        if info == "client_addr":
            return self._transport.get_extra_info("sockname")
        if info == "server_addr":
            return self._transport.get_extra_info("peername")
        if info == "socket":
            return self._transport.get_extra_info("socket")
        if info == "is_readable":
            # The event loop keeps reading while the connection is idle, so a
            # FIN or stray data from the server is already known here without
            # touching the socket.
            protocol = self._protocol
            return bool(protocol.chunks) or protocol.eof or protocol.closed
        return None


class AsyncioBackend(AsyncNetworkBackend):
    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: typing.Iterable[SOCKET_OPTION] | None = None,
    ) -> AsyncNetworkStream:
        loop = asyncio.get_running_loop()
        local_addr = None if local_address is None else (local_address, 0)
        # By default TCP sockets opened in `asyncio` include TCP_NODELAY.
        connect = loop.create_connection(AsyncioStreamProtocol, host, port, local_addr=local_addr)
        return await self._connect(connect, timeout, socket_options)

    async def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,
        socket_options: typing.Iterable[SOCKET_OPTION] | None = None,
    ) -> AsyncNetworkStream:
        loop = asyncio.get_running_loop()
        connect = loop.create_unix_connection(AsyncioStreamProtocol, path)
        return await self._connect(connect, timeout, socket_options)

    async def _connect(
        self,
        connect: typing.Coroutine[typing.Any, typing.Any, tuple[asyncio.BaseTransport, AsyncioStreamProtocol]],
        timeout: float | None,
        socket_options: typing.Iterable[SOCKET_OPTION] | None,
    ) -> AsyncioStream:
        try:
            transport, protocol = await asyncio.wait_for(connect, timeout)
        except (TimeoutError, asyncio.TimeoutError):
            raise ConnectTimeout("timed out") from None
        except OSError as exc:
            raise ConnectError(str(exc)) from exc
        if socket_options:
            sock = transport.get_extra_info("socket")
            for option in socket_options:
                sock.setsockopt(*option)
        return AsyncioStream(typing.cast(asyncio.Transport, transport), protocol)

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)
