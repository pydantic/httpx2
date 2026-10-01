from __future__ import annotations

import ssl
import typing
from collections.abc import Callable, Mapping

import anyio
import anyio.abc
import anyio.streams.tls
from anyio._backends._asyncio import SocketStream as AsyncIOSocketStream

from .._exceptions import (
    ConnectError,
    ConnectTimeout,
    ReadError,
    ReadTimeout,
    WriteError,
    WriteTimeout,
    map_exceptions,
)
from .._utils import is_socket_readable
from .base import SOCKET_OPTION, AsyncNetworkBackend, AsyncNetworkStream


class AnyIOStream(AsyncNetworkStream):
    def __init__(self, stream: anyio.abc.ByteStream) -> None:
        self._stream = stream

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        exc_map: dict[type[Exception], type[Exception]] = {
            TimeoutError: ReadTimeout,
            ssl.SSLError: ReadError,
            anyio.BrokenResourceError: ReadError,
            anyio.ClosedResourceError: ReadError,
            anyio.EndOfStream: ReadError,
        }
        with map_exceptions(exc_map):
            with anyio.fail_after(timeout):
                try:
                    await _flush_pending(self._stream)
                    return await self._stream.receive(max_bytes=max_bytes)
                except anyio.EndOfStream:  # pragma: no cover
                    return b""

    async def read_available(self, max_bytes: int, timeout: float | None = None) -> bytes | None:
        if max_bytes < 1:
            raise ValueError("max_bytes must be a positive integer")
        exc_map: dict[type[Exception], type[Exception]] = {
            TimeoutError: ReadTimeout,
            ssl.SSLError: ReadError,
            anyio.BrokenResourceError: ReadError,
            anyio.ClosedResourceError: ReadError,
            anyio.EndOfStream: ReadError,
        }
        with map_exceptions(exc_map):
            with anyio.fail_after(timeout):
                await anyio.lowlevel.checkpoint()
                try:
                    return await _receive_available(self._stream, max_bytes)
                except anyio.WouldBlock:
                    return None
                except anyio.EndOfStream:
                    return b""

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        if not buffer:
            return

        exc_map: dict[type[Exception], type[Exception]] = {
            TimeoutError: WriteTimeout,
            anyio.BrokenResourceError: WriteError,
            anyio.ClosedResourceError: WriteError,
        }
        with map_exceptions(exc_map):
            with anyio.fail_after(timeout):
                await _flush_pending(self._stream)
                await self._stream.send(item=buffer)

    async def aclose(self) -> None:
        await self._stream.aclose()

    async def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> AsyncNetworkStream:
        exc_map: dict[type[Exception], type[Exception]] = {
            TimeoutError: ConnectTimeout,
            anyio.BrokenResourceError: ConnectError,
            anyio.EndOfStream: ConnectError,
            ssl.SSLError: ConnectError,
        }
        with map_exceptions(exc_map):
            try:
                with anyio.fail_after(timeout):
                    ssl_stream = await anyio.streams.tls.TLSStream.wrap(
                        _TLSStreamAdapter(self._stream),
                        ssl_context=ssl_context,
                        hostname=server_hostname,
                        standard_compatible=False,
                        server_side=False,
                    )
            except Exception as exc:  # pragma: no cover
                await self.aclose()
                raise exc
        return AnyIOStream(ssl_stream)

    def get_extra_info(self, info: str) -> typing.Any:
        if info == "read_available":
            return self.read_available
        if info == "ssl_object":
            return self._stream.extra(anyio.streams.tls.TLSAttribute.ssl_object, None)
        if info == "client_addr":
            return self._stream.extra(anyio.abc.SocketAttribute.local_address, None)
        if info == "server_addr":
            return self._stream.extra(anyio.abc.SocketAttribute.remote_address, None)
        if info == "socket":
            return self._stream.extra(anyio.abc.SocketAttribute.raw_socket, None)
        if info == "is_readable":
            sock = self._stream.extra(anyio.abc.SocketAttribute.raw_socket, None)
            return is_socket_readable(sock)
        return None


class AnyIOBackend(AsyncNetworkBackend):
    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: typing.Iterable[SOCKET_OPTION] | None = None,
    ) -> AsyncNetworkStream:  # pragma: no cover
        if socket_options is None:
            socket_options = []
        exc_map: dict[type[Exception], type[Exception]] = {
            TimeoutError: ConnectTimeout,
            OSError: ConnectError,
            anyio.BrokenResourceError: ConnectError,
        }
        with map_exceptions(exc_map):
            with anyio.fail_after(timeout):
                stream: anyio.abc.ByteStream = await anyio.connect_tcp(
                    remote_host=host,
                    remote_port=port,
                    local_host=local_address,
                )
                # By default TCP sockets opened in `asyncio` include TCP_NODELAY.
                for option in socket_options:
                    stream._raw_socket.setsockopt(*option)  # type: ignore[attr-defined] # pragma: no cover
        return AnyIOStream(stream)

    async def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,
        socket_options: typing.Iterable[SOCKET_OPTION] | None = None,
    ) -> AsyncNetworkStream:  # pragma: no cover
        if socket_options is None:
            socket_options = []
        exc_map: dict[type[Exception], type[Exception]] = {
            TimeoutError: ConnectTimeout,
            OSError: ConnectError,
            anyio.BrokenResourceError: ConnectError,
        }
        with map_exceptions(exc_map):
            with anyio.fail_after(timeout):
                stream: anyio.abc.ByteStream = await anyio.connect_unix(path)
                for option in socket_options:
                    stream._raw_socket.setsockopt(*option)  # type: ignore[attr-defined] # pragma: no cover
        return AnyIOStream(stream)

    async def sleep(self, seconds: float) -> None:
        await anyio.sleep(seconds)  # pragma: no cover


class _TLSStreamAdapter(anyio.abc.ByteStream):
    def __init__(self, stream: anyio.abc.ByteStream) -> None:
        self.stream = stream
        self.probe = False
        self._pending = bytearray()
        self._send_lock = anyio.Lock(fast_acquire=True)

    async def receive(self, max_bytes: int = 65536) -> bytes:
        if self.probe:
            return await _receive_available(self.stream, max_bytes)
        await self.flush()
        return await self.stream.receive(max_bytes)

    async def send(self, item: bytes) -> None:
        if self.probe:
            # A TLS read can generate output without producing application data.
            self._pending.extend(item)
            return
        async with self._send_lock:
            await _flush_pending(self.stream)
            item = bytes(self._pending) + item
            self._pending.clear()
            if item:
                await self.stream.send(item)

    async def flush(self) -> None:
        await _flush_pending(self.stream)
        if self._pending:
            await self.send(b"")

    async def send_eof(self) -> None:  # pragma: no cover - TLSStream does not forward half-close to its transport.
        await self.flush()
        await self.stream.send_eof()

    async def aclose(self) -> None:
        await self.stream.aclose()

    @property
    def extra_attributes(self) -> Mapping[typing.Any, Callable[[], typing.Any]]:
        return self.stream.extra_attributes


async def _flush_pending(stream: anyio.abc.ByteStream) -> None:
    if isinstance(stream, anyio.streams.tls.TLSStream) and isinstance(stream.transport_stream, _TLSStreamAdapter):
        await stream.transport_stream.flush()


async def _receive_available(stream: anyio.abc.ByteStream, max_bytes: int) -> bytes:
    if isinstance(stream, anyio.streams.tls.TLSStream):
        adapter = stream.transport_stream
        if not isinstance(adapter, _TLSStreamAdapter):
            raise anyio.WouldBlock
        previous_probe = adapter.probe
        adapter.probe = True
        try:
            return await stream.receive(max_bytes)
        finally:
            adapter.probe = previous_probe

    if not isinstance(stream, anyio.abc.SocketStream):
        raise anyio.WouldBlock
    ready = False
    if isinstance(stream, AsyncIOSocketStream):
        # asyncio transports can hold bytes that are no longer in the socket.
        stream._transport.resume_reading()
        try:
            await anyio.lowlevel.checkpoint()
        finally:
            stream._transport.pause_reading()
        ready = stream._protocol.read_event.is_set() or stream._transport.is_closing()
    sock = stream.extra(anyio.abc.SocketAttribute.raw_socket, None)
    if not ready and not is_socket_readable(sock):
        raise anyio.WouldBlock
    return await stream.receive(max_bytes)
