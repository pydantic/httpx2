"""Closing pool streams should release pool slots only after a successful close."""

from collections.abc import AsyncIterator, Iterator

import pytest

import httpcore2
from httpcore2._async.connection_pool import AsyncPoolRequest, PoolByteStream as AsyncPoolByteStream
from httpcore2._sync.connection_pool import PoolByteStream as SyncPoolByteStream, PoolRequest


class FailingOnceStream:
    def __init__(self) -> None:
        self.close_attempts = 0

    def close(self) -> None:
        self.close_attempts += 1
        if self.close_attempts == 1:
            raise OSError("stream close failed")

    async def aclose(self) -> None:
        self.close_attempts += 1
        if self.close_attempts == 1:
            raise OSError("stream close failed")

    def __iter__(self) -> Iterator[bytes]:
        return iter(())

    async def __aiter__(self) -> AsyncIterator[bytes]:
        if False:
            yield b""


def test_sync_stream_close_can_retry_after_failure() -> None:
    pool = httpcore2.ConnectionPool()
    pool_request = PoolRequest(httpcore2.Request("GET", "http://example.test/"))
    pool._requests.append(pool_request)
    inner = FailingOnceStream()
    stream = SyncPoolByteStream(inner, pool_request, pool)

    with pytest.raises(OSError, match="stream close failed"):
        stream.close()
    assert pool._requests == [pool_request]
    assert not stream._closed

    stream.close()
    assert inner.close_attempts == 2
    assert pool._requests == []
    assert stream._closed

    stream.close()
    assert inner.close_attempts == 2


@pytest.mark.anyio
async def test_async_stream_close_can_retry_after_failure() -> None:
    pool = httpcore2.AsyncConnectionPool()
    pool_request = AsyncPoolRequest(httpcore2.Request("GET", "http://example.test/"))
    pool._requests.append(pool_request)
    inner = FailingOnceStream()
    stream = AsyncPoolByteStream(inner, pool_request, pool)

    with pytest.raises(OSError, match="stream close failed"):
        await stream.aclose()
    assert pool._requests == [pool_request]
    assert not stream._closed

    await stream.aclose()
    assert inner.close_attempts == 2
    assert pool._requests == []
    assert stream._closed

    await stream.aclose()
    assert inner.close_attempts == 2
