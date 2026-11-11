"""Closing pool streams should release pool slots only after a successful close."""

from collections.abc import AsyncIterator, Iterator

import pytest

import httpcore2
from httpcore2._async.connection_pool import AsyncPoolRequest, PoolByteStream as AsyncPoolByteStream
from httpcore2._sync.connection_pool import PoolByteStream as SyncPoolByteStream, PoolRequest


class InterruptedClose(BaseException):
    """Simulate a BaseException raised while closing an inner stream."""


class InitiallyFailingStream:
    def __init__(self, failures: int = 1, error_type: type[BaseException] = OSError) -> None:
        self.close_attempts = 0
        self.failures = failures
        self.error_type = error_type

    def close(self) -> None:
        self.close_attempts += 1
        if self.close_attempts <= self.failures:
            raise self.error_type("stream close failed")

    async def aclose(self) -> None:
        self.close_attempts += 1
        if self.close_attempts <= self.failures:
            raise self.error_type("stream close failed")

    def __iter__(self) -> Iterator[bytes]:
        return iter(())

    async def __aiter__(self) -> AsyncIterator[bytes]:
        if False:
            yield b""


@pytest.mark.parametrize("failures", [1, 2])
@pytest.mark.parametrize("error_type", [OSError, InterruptedClose])
def test_sync_stream_close_can_retry_after_failure(failures: int, error_type: type[BaseException]) -> None:
    pool = httpcore2.ConnectionPool()
    pool_request = PoolRequest(httpcore2.Request("GET", "http://example.test/"))
    pool._requests.append(pool_request)
    inner = InitiallyFailingStream(failures=failures, error_type=error_type)
    assert list(inner) == []
    stream = SyncPoolByteStream(inner, pool_request, pool)

    for attempt in range(failures):
        with pytest.raises(error_type, match="stream close failed"):
            stream.close()
        assert inner.close_attempts == attempt + 1
        assert pool._requests == [pool_request]
        assert not stream._closed

    stream.close()
    assert inner.close_attempts == failures + 1
    assert pool._requests == []
    assert stream._closed

    stream.close()
    assert inner.close_attempts == failures + 1


@pytest.mark.anyio
@pytest.mark.parametrize("failures", [1, 2])
@pytest.mark.parametrize("error_type", [OSError, InterruptedClose])
async def test_async_stream_close_can_retry_after_failure(failures: int, error_type: type[BaseException]) -> None:
    pool = httpcore2.AsyncConnectionPool()
    pool_request = AsyncPoolRequest(httpcore2.Request("GET", "http://example.test/"))
    pool._requests.append(pool_request)
    inner = InitiallyFailingStream(failures=failures, error_type=error_type)
    assert [chunk async for chunk in inner] == []
    stream = AsyncPoolByteStream(inner, pool_request, pool)

    for attempt in range(failures):
        with pytest.raises(error_type, match="stream close failed"):
            await stream.aclose()
        assert inner.close_attempts == attempt + 1
        assert pool._requests == [pool_request]
        assert not stream._closed

    await stream.aclose()
    assert inner.close_attempts == failures + 1
    assert pool._requests == []
    assert stream._closed

    await stream.aclose()
    assert inner.close_attempts == failures + 1
