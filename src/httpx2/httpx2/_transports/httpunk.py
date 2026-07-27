from __future__ import annotations

import asyncio
import contextlib
import ssl
import typing
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

from .._exceptions import UnsupportedProtocol
from .._models import Request, Response
from .._types import AsyncByteStream
from .base import AsyncBaseTransport

__all__ = ["AsyncHTTPunkTransport"]


@dataclass
class _HTTPunkConnection:
    protocol: typing.Any
    connection: typing.Any

    async def aclose(self) -> None:
        await self.protocol.aclose()


@dataclass
class _HTTPunkOriginPool:
    max_connections: int
    idle: asyncio.Queue[_HTTPunkConnection] = field(default_factory=asyncio.Queue)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    count: int = 0
    all_connections: list[_HTTPunkConnection] = field(default_factory=list)


class _HTTPunkStream(AsyncByteStream):
    def __init__(
        self,
        response: typing.Any,
        transport: AsyncHTTPunkTransport,
        origin: tuple[str, str, int],
        connection: _HTTPunkConnection,
    ) -> None:
        self._response = response
        self._transport = transport
        self._origin = origin
        self._connection = connection
        self._closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        try:
            async for chunk in self._response.aiter_bytes():
                yield chunk
        finally:
            await self.aclose()

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        with contextlib.suppress(Exception):
            await self._response.aclose()
        await self._transport._release(self._origin, self._connection)


class AsyncHTTPunkTransport(AsyncBaseTransport):
    """An experimental async HTTP/1.1 transport backed by httpunk."""

    def __init__(
        self,
        *,
        max_connections: int = 100,
        verify: bool | ssl.SSLContext = True,
    ) -> None:
        self._max_connections = max_connections
        self._verify = verify
        self._pools: dict[tuple[str, str, int], _HTTPunkOriginPool] = {}
        self._closed = False

    async def handle_async_request(self, request: Request) -> Response:
        if self._closed:
            raise RuntimeError("Cannot send a request after transport is closed.")

        scheme = request.url.scheme
        if scheme not in ("http", "https"):
            raise UnsupportedProtocol(f"Request URL has an unsupported protocol '{scheme}://'.")

        host = request.url.host
        port = request.url.port or (443 if scheme == "https" else 80)
        origin = (scheme, host, port)
        connection = await self._acquire(origin)

        target = request.url.raw_path.decode("ascii") or "/"
        body = await request.aread()

        try:
            response = await connection.connection.request(
                request.method,
                target,
                headers=request.headers.multi_items(),
                body=body or None,
            )
        except BaseException:
            await self._discard(origin, connection)
            raise

        return Response(
            response.status,
            headers=list(response.headers.items()),
            stream=_HTTPunkStream(response, self, origin, connection),
            extensions={"http_version": b"HTTP/1.1"},
        )

    async def _acquire(self, origin: tuple[str, str, int]) -> _HTTPunkConnection:
        pool = self._pools.get(origin)
        if pool is None:
            pool = self._pools[origin] = _HTTPunkOriginPool(max_connections=self._max_connections)

        try:
            return pool.idle.get_nowait()
        except asyncio.QueueEmpty:
            pass

        async with pool.lock:
            if pool.count < pool.max_connections:
                pool.count += 1
                connection = await self._connect(origin)
                pool.all_connections.append(connection)
                return connection

        return await pool.idle.get()

    async def _connect(self, origin: tuple[str, str, int]) -> _HTTPunkConnection:
        try:
            import httpunk.asyncio
        except ImportError as exc:  # pragma: no cover
            msg = "Using 'AsyncHTTPunkTransport' requires installing the 'httpunk' package."
            raise RuntimeError(msg) from exc

        scheme, host, port = origin
        ssl_context = self._ssl_context(scheme)
        authority = f"{host}:{port}"

        loop = asyncio.get_running_loop()
        _transport, protocol = await loop.create_connection(
            lambda: httpunk.asyncio.H1ClientProtocol(authority=authority),
            host,
            port,
            ssl=ssl_context,
        )
        connection = await protocol.ready()
        return _HTTPunkConnection(protocol=protocol, connection=connection)

    def _ssl_context(self, scheme: str) -> ssl.SSLContext | None:
        if scheme != "https":
            return None
        if isinstance(self._verify, ssl.SSLContext):
            ssl_context = self._verify
        elif self._verify:
            ssl_context = ssl.create_default_context()
        else:
            ssl_context = ssl._create_unverified_context()
        ssl_context.set_alpn_protocols(["http/1.1"])
        return ssl_context

    async def _release(self, origin: tuple[str, str, int], connection: _HTTPunkConnection) -> None:
        pool = self._pools.get(origin)
        if pool is None or self._closed:
            await connection.aclose()
            return
        pool.idle.put_nowait(connection)

    async def _discard(self, origin: tuple[str, str, int], connection: _HTTPunkConnection) -> None:
        with contextlib.suppress(Exception):
            await connection.aclose()

        pool = self._pools.get(origin)
        if pool is None:
            return
        with contextlib.suppress(ValueError):
            pool.all_connections.remove(connection)
        pool.count -= 1

    async def aclose(self) -> None:
        self._closed = True
        connections = [connection for pool in self._pools.values() for connection in pool.all_connections]
        self._pools.clear()
        for connection in connections:
            with contextlib.suppress(Exception):
                await connection.aclose()
