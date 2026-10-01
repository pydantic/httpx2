from __future__ import annotations

import ssl
import typing

import trio

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


class TrioStream(AsyncNetworkStream):
    def __init__(self, stream: trio.abc.Stream) -> None:
        self._stream = stream

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        timeout_or_inf = float("inf") if timeout is None else timeout
        exc_map: ExceptionMapping = {
            trio.TooSlowError: ReadTimeout,
            trio.BrokenResourceError: ReadError,
            trio.ClosedResourceError: ReadError,
        }
        with map_exceptions(exc_map):
            with trio.fail_after(timeout_or_inf):
                await _flush_pending(self._stream)
                data: bytes = await self._stream.receive_some(max_bytes=max_bytes)
                return data

    async def read_available(self, max_bytes: int, timeout: float | None = None) -> bytes | None:
        if max_bytes < 1:
            raise ValueError("max_bytes must be a positive integer")
        timeout_or_inf = float("inf") if timeout is None else timeout
        exc_map: ExceptionMapping = {
            trio.TooSlowError: ReadTimeout,
            trio.BrokenResourceError: ReadError,
            trio.ClosedResourceError: ReadError,
        }
        with map_exceptions(exc_map):
            with trio.fail_after(timeout_or_inf):
                await trio.lowlevel.checkpoint()
                try:
                    return await _receive_available(self._stream, max_bytes)
                except trio.WouldBlock:
                    return None

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        if not buffer:
            return

        timeout_or_inf = float("inf") if timeout is None else timeout
        exc_map: ExceptionMapping = {
            trio.TooSlowError: WriteTimeout,
            trio.BrokenResourceError: WriteError,
            trio.ClosedResourceError: WriteError,
        }
        with map_exceptions(exc_map):
            with trio.fail_after(timeout_or_inf):
                await _flush_pending(self._stream)
                await self._stream.send_all(data=buffer)

    async def aclose(self) -> None:
        await self._stream.aclose()

    async def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> AsyncNetworkStream:
        timeout_or_inf = float("inf") if timeout is None else timeout
        exc_map: ExceptionMapping = {
            trio.TooSlowError: ConnectTimeout,
            trio.BrokenResourceError: ConnectError,
        }
        ssl_stream = trio.SSLStream(
            _TLSStreamAdapter(self._stream),
            ssl_context=ssl_context,
            server_hostname=server_hostname,
            https_compatible=True,
            server_side=False,
        )
        with map_exceptions(exc_map):
            try:
                with trio.fail_after(timeout_or_inf):
                    await ssl_stream.do_handshake()
            except Exception as exc:  # pragma: no cover
                await self.aclose()
                raise exc
        return TrioStream(ssl_stream)

    def get_extra_info(self, info: str) -> typing.Any:
        if info == "read_available":
            return self.read_available
        if info == "ssl_object" and isinstance(self._stream, trio.SSLStream):
            # Type checkers cannot see `_ssl_object` attribute because trio._ssl.SSLStream uses __getattr__/__setattr__.
            # Tracked at https://github.com/python-trio/trio/issues/542
            return self._stream._ssl_object  # type: ignore[attr-defined]
        if info == "client_addr":
            return self._get_socket_stream().socket.getsockname()
        if info == "server_addr":
            return self._get_socket_stream().socket.getpeername()
        if info == "socket":
            return self._get_socket_stream().socket
        if info == "is_readable":
            socket = self.get_extra_info("socket")
            return socket.is_readable()
        return None

    def _get_socket_stream(self) -> trio.SocketStream:
        stream = self._stream
        while isinstance(stream, (trio.SSLStream, _TLSStreamAdapter)):
            stream = stream.transport_stream if isinstance(stream, trio.SSLStream) else stream.stream
        assert isinstance(stream, trio.SocketStream)
        return stream


class TrioBackend(AsyncNetworkBackend):
    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: typing.Iterable[SOCKET_OPTION] | None = None,
    ) -> AsyncNetworkStream:
        # By default for TCP sockets, trio enables TCP_NODELAY.
        # https://trio.readthedocs.io/en/stable/reference-io.html#trio.SocketStream
        if socket_options is None:
            socket_options = []  # pragma: no cover
        timeout_or_inf = float("inf") if timeout is None else timeout
        exc_map: ExceptionMapping = {
            trio.TooSlowError: ConnectTimeout,
            trio.BrokenResourceError: ConnectError,
            OSError: ConnectError,
        }
        with map_exceptions(exc_map):
            with trio.fail_after(timeout_or_inf):
                stream: trio.abc.Stream = await trio.open_tcp_stream(host=host, port=port, local_address=local_address)
                for option in socket_options:
                    stream.setsockopt(*option)  # type: ignore[attr-defined] # pragma: no cover
        return TrioStream(stream)

    async def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,
        socket_options: typing.Iterable[SOCKET_OPTION] | None = None,
    ) -> AsyncNetworkStream:  # pragma: no cover
        if socket_options is None:
            socket_options = []
        timeout_or_inf = float("inf") if timeout is None else timeout
        exc_map: ExceptionMapping = {
            trio.TooSlowError: ConnectTimeout,
            trio.BrokenResourceError: ConnectError,
            OSError: ConnectError,
        }
        with map_exceptions(exc_map):
            with trio.fail_after(timeout_or_inf):
                stream: trio.abc.Stream = await trio.open_unix_socket(path)
                for option in socket_options:
                    stream.setsockopt(*option)  # type: ignore[attr-defined] # pragma: no cover
        return TrioStream(stream)

    async def sleep(self, seconds: float) -> None:
        await trio.sleep(seconds)  # pragma: no cover


class _TLSStreamAdapter(trio.abc.Stream):
    def __init__(self, stream: trio.abc.Stream) -> None:
        self.stream = stream
        self.probe = False
        self._pending = bytearray()
        self._send_lock = trio.Lock()

    async def receive_some(self, max_bytes: int | None = None) -> bytes:
        if self.probe:
            return await _receive_available(self.stream, max_bytes)
        await self.flush()
        return await self.stream.receive_some(max_bytes)

    async def send_all(self, data: bytes | bytearray | memoryview) -> None:
        if self.probe:
            # A TLS read can generate output without producing application data.
            self._pending.extend(data)
            return
        async with self._send_lock:
            await _flush_pending(self.stream)
            data = bytes(self._pending) + data
            self._pending.clear()
            if data:
                await self.stream.send_all(data)

    async def flush(self) -> None:
        await _flush_pending(self.stream)
        if self._pending:
            await self.send_all(b"")

    async def wait_send_all_might_not_block(self) -> None:  # pragma: no cover - No write-readiness API in HTTP streams.
        await self.flush()
        await self.stream.wait_send_all_might_not_block()

    async def aclose(self) -> None:
        await self.stream.aclose()


async def _flush_pending(stream: trio.abc.Stream) -> None:
    if isinstance(stream, trio.SSLStream) and isinstance(stream.transport_stream, _TLSStreamAdapter):
        await stream.transport_stream.flush()


async def _receive_available(stream: trio.abc.Stream, max_bytes: int | None) -> bytes:
    if isinstance(stream, trio.SSLStream):
        adapter = stream.transport_stream
        if not isinstance(adapter, _TLSStreamAdapter):
            raise trio.WouldBlock
        previous_probe = adapter.probe
        adapter.probe = True
        try:
            return await stream.receive_some(max_bytes)
        finally:
            adapter.probe = previous_probe

    if not isinstance(stream, trio.SocketStream):
        raise trio.WouldBlock
    if stream.socket.fileno() >= 0 and not stream.socket.is_readable():
        raise trio.WouldBlock
    return await stream.receive_some(max_bytes)
