from __future__ import annotations

import asyncio
import ssl
import sys
from collections import OrderedDict
from collections.abc import AsyncIterator
from typing import TypeAlias, cast

import httpcore2

from .core_transport import CoreStream

OriginKey: TypeAlias = tuple[bytes, bytes, int]


class OriginPool:
    def __init__(
        self,
        *,
        max_connections: int | None = None,
        max_keepalive_connections: int | None = None,
        keepalive_expiry: float = 95.0,
        network_backend: httpcore2.AsyncNetworkBackend | None = None,
        ssl_context: ssl.SSLContext | None = None,
    ) -> None:
        if max_connections is not None and max_connections < 1:
            raise ValueError("max_connections must be positive")
        if max_keepalive_connections is not None and max_keepalive_connections < 0:
            raise ValueError("max_keepalive_connections cannot be negative")
        self.limit = sys.maxsize if max_connections is None else max_connections
        self.idle_limit = (
            self.limit if max_keepalive_connections is None else min(max_keepalive_connections, self.limit)
        )
        self.slots = asyncio.Semaphore(self.limit)
        self.expiry = keepalive_expiry
        self.backend = network_backend
        self.ssl_context = ssl_context
        self.idle: dict[OriginKey, dict[httpcore2.AsyncHTTPConnection, None]] = {}
        self.lru: OrderedDict[httpcore2.AsyncHTTPConnection, OriginKey] = OrderedDict()
        self.connections: set[httpcore2.AsyncHTTPConnection] = set()
        self.closed = False

    async def handle_async_request(self, request: httpcore2.Request) -> httpcore2.Response:
        if self.closed:
            raise RuntimeError("The pool is closed")
        try:
            async with asyncio.timeout(request.extensions.get("timeout", {}).get("pool")):
                await self.slots.acquire()
        except TimeoutError as exc:
            raise httpcore2.PoolTimeout(str(exc)) from exc
        connection = None
        origin = request.url.origin
        key = (origin.scheme, origin.host, origin.port)
        try:
            if self.closed:
                raise RuntimeError("The pool is closed")
            candidates = self.idle.get(key, {})
            while candidates:
                candidate, _ = candidates.popitem()
                del self.lru[candidate]
                if candidate.has_expired() or candidate.is_closed():
                    self.connections.discard(candidate)
                    await candidate.aclose()
                else:
                    connection = candidate
                    break
            if not candidates:
                self.idle.pop(key, None)
            if connection is None:
                if len(self.connections) >= self.limit:
                    evicted, old_key = self.lru.popitem(last=False)
                    del self.idle[old_key][evicted]
                    if not self.idle[old_key]:
                        del self.idle[old_key]
                    self.connections.discard(evicted)
                    await evicted.aclose()
                connection = httpcore2.AsyncHTTPConnection(
                    origin,
                    keepalive_expiry=self.expiry,
                    network_backend=self.backend,
                    ssl_context=self.ssl_context,
                    http2=False,
                )
                self.connections.add(connection)
            response = await connection.handle_async_request(request)
        except BaseException:
            self.slots.release()
            if connection is not None:
                self.connections.discard(connection)
                await connection.aclose()
            raise
        return httpcore2.Response(
            response.status,
            headers=response.headers,
            content=PooledStream(cast(CoreStream, response.stream), self, connection, key),
            extensions=response.extensions,
        )

    async def release(self, connection: httpcore2.AsyncHTTPConnection, key: OriginKey, reusable: bool) -> None:
        self.slots.release()
        if not self.closed and reusable and connection.is_available() and len(self.lru) < self.idle_limit:
            self.idle.setdefault(key, {})[connection] = None
            self.lru[connection] = key
        else:
            self.connections.discard(connection)
            await connection.aclose()

    async def aclose(self) -> None:
        self.closed = True
        connections, self.connections = self.connections, set()
        self.idle.clear()
        self.lru.clear()
        for connection in connections:
            await connection.aclose()


class PooledStream:
    def __init__(
        self, stream: CoreStream, pool: OriginPool, connection: httpcore2.AsyncHTTPConnection, key: OriginKey
    ) -> None:
        self.stream = stream
        self.pool = pool
        self.connection = connection
        self.key = key
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        async for chunk in self.stream:
            yield chunk

    async def aclose(self) -> None:
        if self.closed:
            return
        self.closed = True
        reusable = False
        try:
            await self.stream.aclose()
            reusable = True
        finally:
            await self.pool.release(self.connection, self.key, reusable)
