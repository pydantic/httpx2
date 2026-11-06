from __future__ import annotations

import asyncio
import ssl
import sys
from collections import OrderedDict
from datetime import timedelta
from time import perf_counter

import zttp

import httpx2

from .fused_stream import FusedConnection, FusedStream, WireConnection
from .protocol_connection import ProtocolConnection
from .response_buffer import ResponseBuffer


class FusedTransport(httpx2.AsyncBaseTransport):
    def __init__(
        self,
        *,
        max_connections: int | None = None,
        max_keepalive_connections: int | None = None,
        keepalive_expiry: float = 95.0,
        ssl_context: ssl.SSLContext | None = None,
        buffered: bool = False,
        read_size: int = 65536,
        protocol_io: bool = False,
        aggregate: bool = False,
    ) -> None:
        if max_connections is not None and max_connections < 1:
            raise ValueError("max_connections must be positive")
        if max_keepalive_connections is not None and max_keepalive_connections < 0:
            raise ValueError("max_keepalive_connections cannot be negative")
        if read_size < 1:
            raise ValueError("read_size must be positive")
        self.limit = sys.maxsize if max_connections is None else max_connections
        self.idle_limit = self.limit if max_keepalive_connections is None else max_keepalive_connections
        self.slots = asyncio.Semaphore(self.limit)
        self.expiry = keepalive_expiry
        self.ssl_context = ssl_context or ssl.create_default_context()
        self.buffered = buffered
        self.read_size = read_size
        self.protocol_io = protocol_io
        self.aggregate = aggregate
        self.idle: dict[tuple[str, str, int], dict[WireConnection, None]] = {}
        self.lru: OrderedDict[WireConnection, None] = OrderedDict()
        self.connections: set[WireConnection] = set()
        self.closed = False

    async def handle_async_request(self, request: httpx2.Request) -> httpx2.Response:
        start = perf_counter()
        if self.closed:
            raise RuntimeError("The transport is closed")
        if request.url.scheme not in ("http", "https"):
            raise httpx2.UnsupportedProtocol("This experiment supports HTTP and HTTPS only")
        if request.method == "CONNECT" or "upgrade" in request.headers:
            raise httpx2.UnsupportedProtocol("This experiment does not support tunnels or upgrades")
        if not isinstance(request.stream, httpx2.AsyncByteStream):
            raise RuntimeError("Cannot send a synchronous request stream")
        key = (
            request.url.scheme,
            request.url.raw_host.decode("ascii"),
            request.url.port or (443 if request.url.scheme == "https" else 80),
        )
        timeouts = request.extensions.get("timeout", {})
        try:
            if not self.slots.locked():
                await self.slots.acquire()
            else:
                async with asyncio.timeout(timeouts.get("pool")):
                    await self.slots.acquire()
        except TimeoutError as exc:
            raise httpx2.PoolTimeout(str(exc), request=request) from exc
        connection: WireConnection | None = None
        released = False
        try:
            if self.closed:
                raise RuntimeError("The transport is closed")
            candidates = self.idle.get(key, {})
            while candidates:
                candidate, _ = candidates.popitem()
                del self.lru[candidate]
                if candidate.expires <= start or candidate.is_closed():
                    self.connections.discard(candidate)
                    await candidate.aclose()
                else:
                    connection = candidate
                    break
            if not candidates:
                self.idle.pop(key, None)
            if connection is None:
                if len(self.connections) >= self.limit:
                    evicted, _ = self.lru.popitem(last=False)
                    del self.idle[evicted.key][evicted]
                    if not self.idle[evicted.key]:
                        del self.idle[evicted.key]
                    self.connections.discard(evicted)
                    await evicted.aclose()
                try:
                    async with asyncio.timeout(timeouts.get("connect")):
                        if self.protocol_io:
                            _, connection = await asyncio.get_running_loop().create_connection(
                                lambda: ProtocolConnection(key, self.read_size),
                                key[1],
                                key[2],
                                ssl=self.ssl_context if key[0] == "https" else None,
                            )
                        else:
                            reader, writer = await asyncio.open_connection(
                                key[1], key[2], ssl=self.ssl_context if key[0] == "https" else None
                            )
                            connection = FusedConnection(reader, writer, key, self.read_size)
                except TimeoutError as exc:
                    raise httpx2.ConnectTimeout(str(exc), request=request) from exc
                except OSError as exc:
                    raise httpx2.ConnectError(str(exc), request=request) from exc
                self.connections.add(connection)
            connection.done = False
            buffered = self.buffered and request.extensions.get("httpx2_buffer_response", True)
            if self.aggregate and buffered and isinstance(connection, ProtocolConnection):
                connection.response_buffer = ResponseBuffer()
            connection.parser.send_request(
                request.method.encode("ascii"), request.url.raw_path, b"1.1", request.headers.raw
            )
            try:
                content = request.content
            except httpx2.RequestNotRead:
                await connection.write(connection.parser.data_to_send(), timeouts.get("write"))
                async for chunk in request.stream:
                    if chunk:
                        connection.parser.send_data(chunk)
                        await connection.write(connection.parser.data_to_send(), timeouts.get("write"))
            else:
                if content:
                    connection.parser.send_data(content)
            connection.parser.end_message()
            await connection.write(connection.parser.data_to_send(), timeouts.get("write"))
            if isinstance(connection, ProtocolConnection) and connection.response_buffer is not None:
                head, body = await connection.response(timeouts.get("read"))
                released = True
                await self.release(connection)
                connection = None
                response = httpx2.Response(
                    head.status_code,
                    headers=list(head.headers),
                    stream=httpx2.ByteStream(body),
                    extensions={"http_version": b"HTTP/" + head.http_version, "reason_phrase": head.reason},
                    request=request,
                )
                response.read()
                response.elapsed = timedelta(seconds=perf_counter() - start)
                return response
            while True:
                event = await connection.event(timeouts.get("read"))
                if isinstance(event, zttp.Response) and event.status_code >= 200:
                    break
                if not isinstance(event, zttp.Response) or event.status_code == 101:
                    raise httpx2.RemoteProtocolError("Unexpected event before response headers", request=request)
            response = httpx2.Response(
                event.status_code,
                headers=list(event.headers),
                stream=FusedStream(connection, self, timeouts.get("read")),
                extensions={"http_version": b"HTTP/" + event.http_version, "reason_phrase": event.reason},
                request=request,
            )
            if buffered:
                chunks: list[bytes] = []
                while True:
                    event = await connection.event(timeouts.get("read"))
                    if isinstance(event, zttp.Data):
                        chunks.append(event.data)
                    elif isinstance(event, zttp.EndOfMessage):
                        connection.done = True
                        break
                    else:
                        raise httpx2.RemoteProtocolError("Unexpected event in response body", request=request)
                released = True
                await self.release(connection)
                connection = None
                response.stream = httpx2.ByteStream(b"".join(chunks))
                response.read()
                response.elapsed = timedelta(seconds=perf_counter() - start)
            return response
        except BaseException as exc:
            if connection is not None:
                self.connections.discard(connection)
                await connection.aclose()
            if not released:
                self.slots.release()
            if isinstance(exc, zttp.LocalProtocolError):
                raise httpx2.LocalProtocolError(str(exc), request=request) from exc
            if isinstance(exc, zttp.RemoteProtocolError):
                raise httpx2.RemoteProtocolError(str(exc), request=request) from exc
            raise

    async def release(self, connection: WireConnection) -> None:
        self.slots.release()
        if (
            not self.closed
            and connection.done
            and not connection.parser.should_close()
            and len(self.lru) < self.idle_limit
        ):
            connection.parser.start_next_cycle()
            connection.expires = perf_counter() + self.expiry
            self.idle.setdefault(connection.key, {})[connection] = None
            self.lru[connection] = None
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
