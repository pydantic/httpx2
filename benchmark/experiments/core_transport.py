from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from datetime import timedelta
from time import perf_counter
from typing import cast

from typing_extensions import Protocol

import httpcore2
import httpx2


class CorePool(Protocol):
    async def handle_async_request(self, request: httpcore2.Request) -> httpcore2.Response: ...
    async def aclose(self) -> None: ...


class CoreStream(Protocol):
    def __aiter__(self) -> AsyncIterator[bytes]: ...
    async def aclose(self) -> None: ...


class CoreTransport(httpx2.AsyncBaseTransport):
    def __init__(self, pool: CorePool, *, buffered: bool = False, materialized: bool = False) -> None:
        self.pool = pool
        self.buffered = buffered
        self.materialized = materialized

    async def handle_async_request(self, request: httpx2.Request) -> httpx2.Response:
        start = perf_counter()
        if request.url.scheme not in ("http", "https"):
            raise httpx2.UnsupportedProtocol("This experiment supports HTTP and HTTPS only")
        core_request = httpcore2.Request(
            method=request.method,
            url=httpcore2.URL(
                scheme=request.url.raw_scheme,
                host=request.url.raw_host,
                port=request.url.port,
                target=request.url.raw_path,
            ),
            headers=request.headers.raw,
            content=request.stream,
            extensions=request.extensions,
        )
        with map_errors():
            response = await self.pool.handle_async_request(core_request)
            if self.buffered:
                try:
                    body = await response.aread()
                finally:
                    await response.aclose()
                result = httpx2.Response(
                    response.status,
                    headers=response.headers,
                    stream=httpx2.ByteStream(body),
                    extensions=response.extensions,
                    request=request,
                )
                if self.materialized:
                    result.read()
                    result.elapsed = timedelta(seconds=perf_counter() - start)
                return result
        return httpx2.Response(
            response.status,
            headers=response.headers,
            stream=ResponseStream(cast(CoreStream, response.stream)),
            extensions=response.extensions,
        )

    async def aclose(self) -> None:
        await self.pool.aclose()


class ResponseStream(httpx2.AsyncByteStream):
    def __init__(self, stream: CoreStream) -> None:
        self.stream = stream

    async def __aiter__(self) -> AsyncIterator[bytes]:
        with map_errors():
            async for chunk in self.stream:
                yield chunk

    async def aclose(self) -> None:
        with map_errors():
            await self.stream.aclose()


@contextmanager
def map_errors() -> Iterator[None]:
    try:
        yield
    except (httpcore2.TimeoutException, httpcore2.NetworkError, httpcore2.ProtocolError) as exc:
        errors = {
            httpcore2.ConnectTimeout: httpx2.ConnectTimeout,
            httpcore2.ReadTimeout: httpx2.ReadTimeout,
            httpcore2.WriteTimeout: httpx2.WriteTimeout,
            httpcore2.PoolTimeout: httpx2.PoolTimeout,
            httpcore2.ConnectError: httpx2.ConnectError,
            httpcore2.ReadError: httpx2.ReadError,
            httpcore2.WriteError: httpx2.WriteError,
            httpcore2.LocalProtocolError: httpx2.LocalProtocolError,
            httpcore2.RemoteProtocolError: httpx2.RemoteProtocolError,
        }
        raise errors.get(type(exc), httpx2.TransportError)(str(exc)) from exc
