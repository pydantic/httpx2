import asyncio
import ssl
from collections.abc import Iterable
from typing import Any
from unittest.mock import patch

import anyio
import hpack
import hyperframe.frame
import pytest
from trio.testing import MockClock

import httpcore2
from httpcore2._backends.anyio import AnyIOStream


@pytest.fixture(params=["asyncio", "trio"])
def anyio_backend(request: pytest.FixtureRequest) -> str | tuple[str, dict[str, MockClock]]:
    if request.param == "trio":
        return "trio", {"clock": MockClock(autojump_threshold=0)}
    return "asyncio"


class StalledTLSStream(anyio.abc.ByteStream):
    def __init__(self, close_error: Exception | None = None) -> None:
        self.read_started = anyio.Event()
        self.closed = False
        self.close_error = close_error

    async def send(self, item: bytes) -> None:
        pass

    async def receive(self, max_bytes: int = 65536) -> bytes:
        self.read_started.set()
        await anyio.sleep_forever()
        raise AssertionError("TLS handshake unexpectedly resumed")  # pragma: no cover

    async def send_eof(self) -> None:
        pass

    async def aclose(self) -> None:
        # Closing a stream may yield before releasing its underlying resources.
        await anyio.lowlevel.checkpoint()
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


@pytest.mark.anyio
@pytest.mark.parametrize("close_error", [None, anyio.BrokenResourceError("close failed")])
async def test_anyio_tls_cancellation_closes_stream(close_error: Exception | None) -> None:
    stream = StalledTLSStream(close_error=close_error)
    scope = anyio.CancelScope()

    async def cancel_handshake() -> None:
        await stream.read_started.wait()
        scope.cancel()

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(cancel_handshake)
        with scope:
            with pytest.raises(anyio.get_cancelled_exc_class()):
                await AnyIOStream(stream).start_tls(
                    ssl.create_default_context(), server_hostname="example.com", timeout=10
                )

    assert stream.closed


@pytest.mark.anyio
@pytest.mark.parametrize("close_error", [None, anyio.BrokenResourceError("close failed")])
async def test_anyio_tls_asyncio_deadline_closes_stream(anyio_backend_name: str, close_error: Exception | None) -> None:
    if anyio_backend_name != "asyncio":
        pytest.skip("asyncio deadlines require the asyncio backend")

    stream = StalledTLSStream(close_error=close_error)
    deadline = asyncio.timeout(None)

    async def expire_deadline() -> None:
        await stream.read_started.wait()
        deadline.reschedule(asyncio.get_running_loop().time())

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(expire_deadline)
        with pytest.raises(TimeoutError):
            async with deadline:
                await AnyIOStream(stream).start_tls(
                    ssl.create_default_context(), server_hostname="example.com", timeout=10
                )

    assert stream.closed


@pytest.mark.anyio
@pytest.mark.parametrize("close_error", [None, anyio.BrokenResourceError("close failed")])
@pytest.mark.parametrize(
    "error, expected",
    [
        (ssl.SSLError("handshake failed"), httpcore2.ConnectError),
        (anyio.BrokenResourceError(), httpcore2.ConnectError),
        (anyio.EndOfStream(), httpcore2.ConnectError),
        (TimeoutError(), httpcore2.ConnectTimeout),
    ],
)
async def test_anyio_tls_failure_closes_stream(
    error: Exception, expected: type[Exception], close_error: Exception | None
) -> None:
    stream = StalledTLSStream(close_error=close_error)

    with patch("anyio.streams.tls.TLSStream.wrap", side_effect=error), pytest.raises(expected) as exc_info:
        await AnyIOStream(stream).start_tls(ssl.create_default_context(), server_hostname="example.com")

    assert exc_info.value.__cause__ is error
    assert stream.closed


class SlowWriteStream(httpcore2.AsyncNetworkStream):
    """
    A stream that we can use to test cancellations during
    the request writing.
    """

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        await anyio.sleep(999)

    async def aclose(self) -> None:
        await anyio.sleep(0)


class HandshakeThenSlowWriteStream(httpcore2.AsyncNetworkStream):
    """
    A stream that we can use to test cancellations during
    the HTTP/2 request writing, after allowing the initial
    handshake to complete.
    """

    def __init__(self) -> None:
        self._handshake_complete = False

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        if not self._handshake_complete:
            self._handshake_complete = True
        else:
            await anyio.sleep(999)

    async def aclose(self) -> None:
        await anyio.sleep(0)


class SlowReadStream(httpcore2.AsyncNetworkStream):
    """
    A stream that we can use to test cancellations during
    the response reading.
    """

    def __init__(self, buffer: list[bytes]):
        self._buffer = buffer

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        pass

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        if not self._buffer:
            await anyio.sleep(999)
        return self._buffer.pop(0)

    async def aclose(self) -> None:
        await anyio.sleep(0)


class SlowWriteBackend(httpcore2.AsyncNetworkBackend):
    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: Iterable[httpcore2.SOCKET_OPTION] | None = None,
    ) -> httpcore2.AsyncNetworkStream:
        return SlowWriteStream()


class SlowReadBackend(httpcore2.AsyncNetworkBackend):
    def __init__(self, buffer: list[bytes]):
        self._buffer = buffer

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: Iterable[httpcore2.SOCKET_OPTION] | None = None,
    ) -> httpcore2.AsyncNetworkStream:
        return SlowReadStream(self._buffer)


@pytest.mark.anyio
async def test_connection_pool_timeout_during_request() -> None:
    """
    An async timeout when writing an HTTP/1.1 response on the connection pool
    should leave the pool in a consistent state.

    In this case, that means the connection will become closed, and no
    longer remain in the pool.
    """
    network_backend = SlowWriteBackend()
    async with httpcore2.AsyncConnectionPool(network_backend=network_backend) as pool:
        with anyio.move_on_after(0.01):
            await pool.request("GET", "http://example.com")
        assert not pool.connections


@pytest.mark.anyio
async def test_connection_pool_timeout_during_response() -> None:
    """
    An async timeout when reading an HTTP/1.1 response on the connection pool
    should leave the pool in a consistent state.

    In this case, that means the connection will become closed, and no
    longer remain in the pool.
    """
    network_backend = SlowReadBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 1000\r\n",
            b"\r\n",
            b"Hello, world!...",
        ]
    )
    async with httpcore2.AsyncConnectionPool(network_backend=network_backend) as pool:
        with anyio.move_on_after(0.01):
            await pool.request("GET", "http://example.com")
        assert not pool.connections


@pytest.mark.anyio
async def test_connection_pool_cancellation_during_waiting_for_connection() -> None:
    """
    A cancellation while a request is waiting for a connection should leave
    the pool in a consistent state.

    In this case, that means the new (not-yet-connected) connection is
    discarded and no longer remains in the pool.
    """

    async def wait_for_connection(self: Any, *args: Any, **kwargs: Any) -> None:
        await anyio.sleep(999)

    with patch(
        "httpcore2._async.connection_pool.AsyncPoolRequest.wait_for_connection",
        new=wait_for_connection,
    ):
        async with httpcore2.AsyncConnectionPool() as pool:
            with anyio.move_on_after(0.01):
                await pool.request("GET", "http://example.com")
            assert not pool.connections


@pytest.mark.anyio
async def test_h11_timeout_during_request() -> None:
    """
    An async timeout on an HTTP/1.1 during the request writing
    should leave the connection in a neatly closed state.
    """
    origin = httpcore2.Origin(b"http", b"example.com", 80)
    stream = SlowWriteStream()
    async with httpcore2.AsyncHTTP11Connection(origin, stream) as conn:
        with anyio.move_on_after(0.01):
            await conn.request("GET", "http://example.com")
        assert conn.is_closed()


@pytest.mark.anyio
async def test_h11_timeout_during_response() -> None:
    """
    An async timeout on an HTTP/1.1 during the response reading
    should leave the connection in a neatly closed state.
    """
    origin = httpcore2.Origin(b"http", b"example.com", 80)
    stream = SlowReadStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 1000\r\n",
            b"\r\n",
            b"Hello, world!...",
        ]
    )
    async with httpcore2.AsyncHTTP11Connection(origin, stream) as conn:
        with anyio.move_on_after(0.01):
            await conn.request("GET", "http://example.com")
        assert conn.is_closed()


@pytest.mark.anyio
@pytest.mark.parametrize("cancel_waiter", [False, True])
@pytest.mark.parametrize("close_before_settings", [False, True])
async def test_h2_queued_request_after_stream_limit_decrease(cancel_waiter: bool, close_before_settings: bool) -> None:
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
            hyperframe.frame.SettingsFrame(
                settings={hyperframe.frame.SettingsFrame.MAX_CONCURRENT_STREAMS: 2}
            ).serialize(),
            *[
                hyperframe.frame.HeadersFrame(
                    stream_id=stream_id,
                    data=hpack.Encoder().encode([(b":status", b"200")]),
                    flags=["END_HEADERS"],
                ).serialize()
                for stream_id in (1, 3)
            ],
            hyperframe.frame.DataFrame(stream_id=1, data=b"response a", flags=["END_STREAM"]).serialize(),
            hyperframe.frame.SettingsFrame(
                settings={hyperframe.frame.SettingsFrame.MAX_CONCURRENT_STREAMS: 1}
            ).serialize(),
            hyperframe.frame.DataFrame(stream_id=3, data=b"response b").serialize(),
            hyperframe.frame.DataFrame(stream_id=3, flags=["END_STREAM"]).serialize(),
            *[
                hyperframe.frame.HeadersFrame(
                    stream_id=stream_id,
                    data=hpack.Encoder().encode([(b":status", b"200")]),
                    flags=["END_HEADERS", "END_STREAM"],
                ).serialize()
                for stream_id in (5, 7)
            ],
        ]
    )
    statuses: list[int] = []
    waiter_scope = anyio.CancelScope()

    async def fetch(conn: httpcore2.AsyncHTTP2Connection) -> None:
        with waiter_scope:
            response = await conn.request("GET", "https://example.com/queued")
            statuses.append(response.status)

    async with httpcore2.AsyncHTTP2Connection(origin=origin, stream=stream) as conn:
        with anyio.fail_after(2):
            async with conn.stream("GET", "https://example.com/a") as response_a:

                async def trace(name: str, info: dict[str, Any]) -> None:
                    if name == "http2.receive_remote_settings.started" and not close_before_settings:
                        await response_a.aclose()
                        await anyio.wait_all_tasks_blocked()

                async with conn.stream("GET", "https://example.com/b", extensions={"trace": trace}) as response_b:
                    async with anyio.create_task_group() as task_group:
                        task_group.start_soon(fetch, conn)
                        await anyio.wait_all_tasks_blocked()
                        assert await response_a.aread() == b"response a"
                        if close_before_settings:
                            await response_a.aclose()

                        iterator = response_b.aiter_stream()
                        assert await anext(iterator) == b"response b"
                        await anyio.wait_all_tasks_blocked()
                        if not close_before_settings:
                            assert statuses == []
                        expected_statuses = statuses.copy() if cancel_waiter else [200]
                        if cancel_waiter:
                            waiter_scope.cancel()
                            await anyio.wait_all_tasks_blocked()
                        async for chunk in iterator:
                            assert chunk == b""
                        await response_b.aclose()

            assert statuses == expected_statuses
            response = await conn.request("GET", "https://example.com/reused")
            assert response.status == 200
            assert conn.is_idle()


@pytest.mark.anyio
async def test_h2_stream_limit_capacity() -> None:
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    buffer = [
        hyperframe.frame.SettingsFrame(settings={hyperframe.frame.SettingsFrame.MAX_CONCURRENT_STREAMS: 2}).serialize(),
        *[
            hyperframe.frame.HeadersFrame(
                stream_id=stream_id,
                data=hpack.Encoder().encode([(b":status", b"200")]),
                flags=["END_HEADERS"],
            ).serialize()
            for stream_id in (1, 3)
        ],
        hyperframe.frame.SettingsFrame(settings={hyperframe.frame.SettingsFrame.MAX_CONCURRENT_STREAMS: 1}).serialize(),
        hyperframe.frame.DataFrame(stream_id=3, data=b"response b").serialize(),
        hyperframe.frame.DataFrame(stream_id=1, data=b"response a", flags=["END_STREAM"]).serialize(),
        hyperframe.frame.SettingsFrame(settings={hyperframe.frame.SettingsFrame.MAX_CONCURRENT_STREAMS: 2}).serialize(),
        hyperframe.frame.DataFrame(stream_id=3, data=b"more").serialize(),
        hyperframe.frame.HeadersFrame(
            stream_id=5,
            data=hpack.Encoder().encode([(b":status", b"200")]),
            flags=["END_HEADERS", "END_STREAM"],
        ).serialize(),
    ]
    stream = httpcore2.AsyncMockStream(buffer)
    statuses: list[int] = []

    async def fetch(conn: httpcore2.AsyncHTTP2Connection) -> None:
        response = await conn.request("GET", "https://example.com/queued")
        statuses.append(response.status)

    async with httpcore2.AsyncHTTP2Connection(origin=origin, stream=stream) as conn:
        with anyio.fail_after(2):
            async with conn.stream("GET", "https://example.com/a") as response_a:
                async with conn.stream("GET", "https://example.com/b") as response_b:
                    iterator = response_b.aiter_stream()
                    assert await anext(iterator) == b"response b"
                    assert await response_a.aread() == b"response a"
                    await response_a.aclose()

                    async with anyio.create_task_group() as task_group:
                        task_group.start_soon(fetch, conn)
                        await anyio.wait_all_tasks_blocked()
                        assert statuses == []

                        assert await anext(iterator) == b"more"
                        await anyio.wait_all_tasks_blocked()
                        assert statuses == [200]

                    buffer.append(hyperframe.frame.DataFrame(stream_id=3, flags=["END_STREAM"]).serialize())
                    async for chunk in iterator:
                        assert chunk == b""

            assert conn.is_idle()


@pytest.mark.anyio
async def test_h2_timeout_during_handshake() -> None:
    """
    An async timeout on an HTTP/2 during the initial handshake
    should leave the connection in a neatly closed state.
    """
    origin = httpcore2.Origin(b"http", b"example.com", 80)
    stream = SlowWriteStream()
    async with httpcore2.AsyncHTTP2Connection(origin, stream) as conn:
        with anyio.move_on_after(0.01):
            await conn.request("GET", "http://example.com")
        assert conn.is_closed()


@pytest.mark.anyio
async def test_h2_timeout_during_request() -> None:
    """
    An async timeout on an HTTP/2 during a request
    should leave the connection in a neatly idle state.

    The connection is not closed because it is multiplexed,
    and a timeout on one request does not require the entire
    connection be closed.
    """
    origin = httpcore2.Origin(b"http", b"example.com", 80)
    stream = HandshakeThenSlowWriteStream()
    async with httpcore2.AsyncHTTP2Connection(origin, stream) as conn:
        with anyio.move_on_after(0.01):
            await conn.request("GET", "http://example.com")

        assert not conn.is_closed()
        assert conn.is_idle()


@pytest.mark.anyio
async def test_h2_timeout_during_response() -> None:
    """
    An async timeout on an HTTP/2 during the response reading
    should leave the connection in a neatly idle state.

    The connection is not closed because it is multiplexed,
    and a timeout on one request does not require the entire
    connection be closed.
    """
    origin = httpcore2.Origin(b"http", b"example.com", 80)
    stream = SlowReadStream(
        [
            hyperframe.frame.SettingsFrame().serialize(),
            hyperframe.frame.HeadersFrame(
                stream_id=1,
                data=hpack.Encoder().encode(
                    [
                        (b":status", b"200"),
                        (b"content-type", b"plain/text"),
                    ]
                ),
                flags=["END_HEADERS"],
            ).serialize(),
            hyperframe.frame.DataFrame(stream_id=1, data=b"Hello, world!...", flags=[]).serialize(),
        ]
    )
    async with httpcore2.AsyncHTTP2Connection(origin, stream) as conn:
        with anyio.move_on_after(0.01):
            await conn.request("GET", "http://example.com")

        assert not conn.is_closed()
        assert conn.is_idle()
