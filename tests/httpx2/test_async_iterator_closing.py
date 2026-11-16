"""
Every async iterator that httpx2 takes from another stream is closed when its consumer stops early.

Otherwise the inner async generator stays suspended until the garbage collector finalizes it, on whatever
event loop collects it (Trio reports a `ResourceWarning` for that).
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable, MutableMapping
from contextlib import aclosing
from typing import Any

import pytest

import httpx2
from httpx2._transports.default import AsyncResponseStream

REQUEST = httpx2.Request("GET", "http://www.example.org/")


class RecordedStream(httpx2.AsyncByteStream):
    def __init__(self, chunk: bytes = b"line\n") -> None:
        self._chunk = chunk
        self.closed_early = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        try:
            for _ in range(16):
                yield self._chunk
        except GeneratorExit:
            self.closed_early = True
            raise


async def take_first(iterator: AsyncIterator[Any]) -> None:
    assert isinstance(iterator, AsyncGenerator)
    async with aclosing(iterator):
        async for _ in iterator:
            break


@pytest.mark.anyio
@pytest.mark.parametrize("method", ["aiter_raw", "aiter_bytes", "aiter_text", "aiter_lines"])
async def test_response_iterator_closes_the_stream(method: str) -> None:
    stream = RecordedStream()
    response = httpx2.Response(200, stream=stream, request=REQUEST)

    await take_first(getattr(response, method)())

    assert stream.closed_early


@pytest.mark.anyio
async def test_event_source_closes_the_stream() -> None:
    stream = RecordedStream(b"data: hello\n\n")
    response = httpx2.Response(200, headers={"Content-Type": "text/event-stream"}, stream=stream, request=REQUEST)

    await take_first(aiter(httpx2.EventSource(response)))

    assert stream.closed_early


@pytest.mark.anyio
async def test_transport_response_stream_closes_the_httpcore_stream() -> None:
    stream = RecordedStream()

    await take_first(aiter(AsyncResponseStream(stream)))

    assert stream.closed_early


@pytest.mark.anyio
async def test_asgi_transport_closes_the_unread_request_body() -> None:
    async def app(
        scope: MutableMapping[str, Any],
        receive: Callable[[], Awaitable[MutableMapping[str, Any]]],
        send: Callable[[MutableMapping[str, Any]], Awaitable[None]],
    ) -> None:
        await receive()  # the first chunk only
        await send({"type": "http.response.start", "status": 413, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    stream = RecordedStream()
    transport = httpx2.ASGITransport(app=app)
    async with httpx2.AsyncClient(transport=transport) as client:
        response = await client.send(httpx2.Request("POST", "http://www.example.org/", stream=stream))

        assert response.status_code == 413
        assert stream.closed_early


class ChunkIterator:
    """A plain async iterator, with no `aclose()` to call."""

    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = iter(chunks)

    def __aiter__(self) -> ChunkIterator:
        return self

    async def __anext__(self) -> bytes:
        try:
            return next(self._chunks)
        except StopIteration:
            raise StopAsyncIteration from None


@pytest.mark.anyio
async def test_plain_async_iterator_is_read_as_is() -> None:
    response = httpx2.Response(200, content=ChunkIterator([b"Hello, ", b"world!"]), request=REQUEST)

    assert await response.aread() == b"Hello, world!"
