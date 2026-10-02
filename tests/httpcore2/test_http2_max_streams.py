"""
This test lives outside the `_async`/`_sync` unasync mirror deliberately.
The deadlock it covers also existed in the sync code path, but its sync form
blocks forever in `threading.Semaphore.acquire()`, which cannot be bounded
by a timeout, so only the async variant can be tested without risking a hang.
"""

import anyio
import hpack
import hyperframe.frame
import pytest

import httpcore2


@pytest.mark.anyio
async def test_http2_remote_max_streams_decrease_below_in_flight() -> None:
    """
    If the remote server lowers MAX_CONCURRENT_STREAMS below the number of
    streams currently in flight, the connection should apply the new limit
    without deadlocking, and the in-flight responses should still complete.
    The server sends both responses *after* the SETTINGS decrease, so a
    TimeoutError here is the deadlock, not a server that never replied.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
            # Raise the stream limit so that two requests can be in flight.
            hyperframe.frame.SettingsFrame(
                settings={hyperframe.frame.SettingsFrame.MAX_CONCURRENT_STREAMS: 100}
            ).serialize(),
            # Response headers for request A. Its body is left pending, so
            # stream 1 stays in flight.
            hyperframe.frame.HeadersFrame(
                stream_id=1,
                data=hpack.Encoder().encode([(b":status", b"200")]),
                flags=["END_HEADERS"],
            ).serialize(),
            # With two streams in flight, lower the limit to 1.
            hyperframe.frame.SettingsFrame(
                settings={hyperframe.frame.SettingsFrame.MAX_CONCURRENT_STREAMS: 1}
            ).serialize(),
            # Both responses complete *after* the decrease.
            hyperframe.frame.HeadersFrame(
                stream_id=3,
                data=hpack.Encoder().encode([(b":status", b"200")]),
                flags=["END_HEADERS"],
            ).serialize(),
            hyperframe.frame.DataFrame(stream_id=1, data=b"response a", flags=["END_STREAM"]).serialize(),
            hyperframe.frame.DataFrame(stream_id=3, data=b"response b", flags=["END_STREAM"]).serialize(),
        ]
    )

    responses: dict[str, bytes] = {}

    async def fetch_b(conn: httpcore2.AsyncHTTP2Connection) -> None:
        response = await conn.request("GET", "https://example.com/b")
        assert response.status == 200
        responses["b"] = response.content

    async with httpcore2.AsyncHTTP2Connection(origin=origin, stream=stream) as conn:
        with anyio.fail_after(2):
            async with conn.stream("GET", "https://example.com/a") as response_a:
                assert response_a.status == 200
                async with anyio.create_task_group() as task_group:
                    task_group.start_soon(fetch_b, conn)
                    # Wait until request B holds a stream permit, so that two
                    # streams are in flight before any further reads occur.
                    # (On a non-deadlocking connection, B may already have run
                    # to completion, deleting its stream entry again.)
                    while 3 not in conn._events and "b" not in responses:
                        await anyio.sleep(0.001)
                    responses["a"] = await response_a.aread()

        assert responses == {"a": b"response a", "b": b"response b"}
        assert conn._max_streams == 1


@pytest.mark.anyio
async def test_http2_remote_max_streams_raise_after_decrease() -> None:
    """
    If the remote server raises MAX_CONCURRENT_STREAMS again after a decrease
    below the in-flight stream count, the outstanding deficit must be retired
    before any permits are released, restoring the full capacity without
    over-releasing. (An over-release would raise from the bounded semaphore.)
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
            # Raise the stream limit so that two requests can be in flight.
            hyperframe.frame.SettingsFrame(
                settings={hyperframe.frame.SettingsFrame.MAX_CONCURRENT_STREAMS: 100}
            ).serialize(),
            # Response headers for request A. Its body is left pending, so
            # stream 1 stays in flight.
            hyperframe.frame.HeadersFrame(
                stream_id=1,
                data=hpack.Encoder().encode([(b":status", b"200")]),
                flags=["END_HEADERS"],
            ).serialize(),
            # With two streams in flight, lower the limit to 1, booking a
            # deficit of one revoked-but-held permit...
            hyperframe.frame.SettingsFrame(
                settings={hyperframe.frame.SettingsFrame.MAX_CONCURRENT_STREAMS: 1}
            ).serialize(),
            # ...then raise it straight back up again.
            hyperframe.frame.SettingsFrame(
                settings={hyperframe.frame.SettingsFrame.MAX_CONCURRENT_STREAMS: 100}
            ).serialize(),
            hyperframe.frame.HeadersFrame(
                stream_id=3,
                data=hpack.Encoder().encode([(b":status", b"200")]),
                flags=["END_HEADERS"],
            ).serialize(),
            hyperframe.frame.DataFrame(stream_id=1, data=b"response a", flags=["END_STREAM"]).serialize(),
            hyperframe.frame.DataFrame(stream_id=3, data=b"response b", flags=["END_STREAM"]).serialize(),
        ]
    )

    responses: dict[str, bytes] = {}

    async def fetch_b(conn: httpcore2.AsyncHTTP2Connection) -> None:
        response = await conn.request("GET", "https://example.com/b")
        assert response.status == 200
        responses["b"] = response.content

    async with httpcore2.AsyncHTTP2Connection(origin=origin, stream=stream) as conn:
        with anyio.fail_after(2):
            async with conn.stream("GET", "https://example.com/a") as response_a:
                assert response_a.status == 200
                async with anyio.create_task_group() as task_group:
                    task_group.start_soon(fetch_b, conn)
                    # Wait until request B holds a stream permit, so that two
                    # streams are in flight before any further reads occur.
                    # (On a non-deadlocking connection, B may already have run
                    # to completion, deleting its stream entry again.)
                    while 3 not in conn._events and "b" not in responses:
                        await anyio.sleep(0.001)
                    responses["a"] = await response_a.aread()

        assert responses == {"a": b"response a", "b": b"response b"}
        assert conn._max_streams == 100
        assert conn._max_streams_deficit == 0
