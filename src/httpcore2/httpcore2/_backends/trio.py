from __future__ import annotations

import ssl
import typing

import trio

from .._exceptions import (
    ConnectError,
    ConnectTimeout,
    ReadError,
    ReadTimeout,
    WriteError,
    WriteTimeout,
)
from .base import SOCKET_OPTION, AsyncNetworkBackend, AsyncNetworkStream


class TrioStream(AsyncNetworkStream):
    def __init__(self, stream: trio.abc.Stream) -> None:
        self._stream = stream

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        timeout_or_inf = float("inf") if timeout is None else timeout
        try:
            with trio.fail_after(timeout_or_inf):
                data: bytes = await self._stream.receive_some(max_bytes=max_bytes)
                return data
        except trio.TooSlowError as exc:  # pragma: no cover
            raise ReadTimeout(exc) from exc
        except (
            trio.BrokenResourceError,
            trio.ClosedResourceError,
        ) as exc:  # pragma: no cover
            raise ReadError(exc) from exc

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        if not buffer:
            return

        timeout_or_inf = float("inf") if timeout is None else timeout
        try:
            with trio.fail_after(timeout_or_inf):
                await self._stream.send_all(data=buffer)
        except trio.TooSlowError as exc:  # pragma: no cover
            raise WriteTimeout(exc) from exc
        except (
            trio.BrokenResourceError,
            trio.ClosedResourceError,
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
        timeout_or_inf = float("inf") if timeout is None else timeout
        ssl_stream = trio.SSLStream(
            self._stream,
            ssl_context=ssl_context,
            server_hostname=server_hostname,
            https_compatible=True,
            server_side=False,
        )
        try:
            try:
                with trio.fail_after(timeout_or_inf):
                    await ssl_stream.do_handshake()
            except Exception as exc:  # pragma: no cover
                await self.aclose()
                raise exc
        except trio.TooSlowError as exc:  # pragma: no cover
            raise ConnectTimeout(exc) from exc
        except trio.BrokenResourceError as exc:  # pragma: no cover
            raise ConnectError(exc) from exc
        return TrioStream(ssl_stream)

    def get_extra_info(self, info: str) -> typing.Any:
        if info == "ssl_object" and isinstance(self._stream, trio.SSLStream):
            # Type checkers cannot see `_ssl_object` attribute because trio._ssl.SSLStream uses __getattr__/__setattr__.
            # Tracked at https://github.com/python-trio/trio/issues/542
            return self._stream._ssl_object  # type: ignore[attr-defined]
        if info == "client_addr":
            return self._get_socket_stream().socket.getsockname()
        if info == "server_addr":
            return self._get_socket_stream().socket.getpeername()
        if info == "socket":
            stream = self._stream
            while isinstance(stream, trio.SSLStream):
                stream = stream.transport_stream
            assert isinstance(stream, trio.SocketStream)
            return stream.socket
        if info == "is_readable":
            socket = self.get_extra_info("socket")
            return socket.is_readable()
        return None

    def _get_socket_stream(self) -> trio.SocketStream:
        stream = self._stream
        while isinstance(stream, trio.SSLStream):
            stream = stream.transport_stream
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
        try:
            with trio.fail_after(timeout_or_inf):
                stream: trio.abc.Stream = await trio.open_tcp_stream(host=host, port=port, local_address=local_address)
                for option in socket_options:
                    stream.setsockopt(*option)  # type: ignore[attr-defined] # pragma: no cover
        except trio.TooSlowError as exc:  # pragma: no cover
            raise ConnectTimeout(exc) from exc
        except (trio.BrokenResourceError, OSError) as exc:  # pragma: no cover
            raise ConnectError(exc) from exc
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
        try:
            with trio.fail_after(timeout_or_inf):
                stream: trio.abc.Stream = await trio.open_unix_socket(path)
                for option in socket_options:
                    stream.setsockopt(*option)  # type: ignore[attr-defined] # pragma: no cover
        except trio.TooSlowError as exc:
            raise ConnectTimeout(exc) from exc
        except (trio.BrokenResourceError, OSError) as exc:
            raise ConnectError(exc) from exc
        return TrioStream(stream)

    async def sleep(self, seconds: float) -> None:
        await trio.sleep(seconds)  # pragma: no cover
