from __future__ import annotations

import ssl
import typing

import anyio
import anyio.abc
import anyio.streams.tls

from .._exceptions import (
    ConnectError,
    ConnectTimeout,
    ReadError,
    ReadTimeout,
    WriteError,
    WriteTimeout,
)
from .._utils import is_socket_readable
from .base import SOCKET_OPTION, AsyncNetworkBackend, AsyncNetworkStream


class AnyIOStream(AsyncNetworkStream):
    def __init__(self, stream: anyio.abc.ByteStream) -> None:
        self._stream = stream

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        try:
            with anyio.fail_after(timeout):
                try:
                    return await self._stream.receive(max_bytes=max_bytes)
                except anyio.EndOfStream:  # pragma: no cover
                    return b""
        except TimeoutError as exc:  # pragma: no cover
            raise ReadTimeout(exc) from exc
        except (
            anyio.BrokenResourceError,
            anyio.ClosedResourceError,
            anyio.EndOfStream,
        ) as exc:  # pragma: no cover
            raise ReadError(exc) from exc

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        if not buffer:
            return

        try:
            with anyio.fail_after(timeout):
                await self._stream.send(item=buffer)
        except TimeoutError as exc:  # pragma: no cover
            raise WriteTimeout(exc) from exc
        except (
            anyio.BrokenResourceError,
            anyio.ClosedResourceError,
        ) as exc:  # pragma: no cover
            raise WriteError(exc) from exc

    async def aclose(self) -> None:
        await self._stream.aclose()

    async def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> AsyncNetworkStream:
        try:
            try:
                with anyio.fail_after(timeout):
                    ssl_stream = await anyio.streams.tls.TLSStream.wrap(
                        self._stream,
                        ssl_context=ssl_context,
                        hostname=server_hostname,
                        standard_compatible=False,
                        server_side=False,
                    )
            except Exception as exc:  # pragma: no cover
                await self.aclose()
                raise exc
        except TimeoutError as exc:  # pragma: no cover
            raise ConnectTimeout(exc) from exc
        except (
            anyio.BrokenResourceError,
            anyio.EndOfStream,
            ssl.SSLError,
        ) as exc:  # pragma: no cover
            raise ConnectError(exc) from exc
        return AnyIOStream(ssl_stream)

    def get_extra_info(self, info: str) -> typing.Any:
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
        try:
            with anyio.fail_after(timeout):
                stream: anyio.abc.ByteStream = await anyio.connect_tcp(
                    remote_host=host,
                    remote_port=port,
                    local_host=local_address,
                )
                # By default TCP sockets opened in `asyncio` include TCP_NODELAY.
                for option in socket_options:
                    stream._raw_socket.setsockopt(*option)  # type: ignore[attr-defined] # pragma: no cover
        except TimeoutError as exc:
            raise ConnectTimeout(exc) from exc
        except (OSError, anyio.BrokenResourceError) as exc:
            raise ConnectError(exc) from exc
        return AnyIOStream(stream)

    async def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,
        socket_options: typing.Iterable[SOCKET_OPTION] | None = None,
    ) -> AsyncNetworkStream:  # pragma: no cover
        if socket_options is None:
            socket_options = []
        try:
            with anyio.fail_after(timeout):
                stream: anyio.abc.ByteStream = await anyio.connect_unix(path)
                for option in socket_options:
                    stream._raw_socket.setsockopt(*option)  # type: ignore[attr-defined] # pragma: no cover
        except TimeoutError as exc:
            raise ConnectTimeout(exc) from exc
        except (OSError, anyio.BrokenResourceError) as exc:
            raise ConnectError(exc) from exc
        return AnyIOStream(stream)

    async def sleep(self, seconds: float) -> None:
        await anyio.sleep(seconds)  # pragma: no cover
