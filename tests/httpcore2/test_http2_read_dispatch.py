"""Regressions for HTTP/2 multiplexed response wakeups (issue #1300)."""

import anyio
import h2.events
import pytest

import httpcore2
from httpcore2._async.http2 import AsyncHTTP2Connection


@pytest.mark.anyio
async def test_completed_stream_is_not_blocked_by_another_network_read(monkeypatch: pytest.MonkeyPatch) -> None:
    """A ready event must not wait for a different stream's next socket read."""
    connection = AsyncHTTP2Connection(
        origin=httpcore2.Origin(b"https", b"example.com", 443),
        stream=httpcore2.AsyncMockStream([], http2=True),
    )
    connection._events = {1: [], 3: [], 5: []}
    request = httpcore2.Request("GET", "https://example.com/")

    first_read_started = anyio.Event()
    allow_first_read = anyio.Event()
    second_read_started = anyio.Event()
    completed_response_received = anyio.Event()
    read_count = 0

    async def receive_events(_request: httpcore2.Request, _stream_id: int | None = None) -> None:
        nonlocal read_count
        read_count += 1
        if read_count == 1:
            first_read_started.set()
            await allow_first_read.wait()
            connection._events[1].append(h2.events.StreamEnded(stream_id=1))
            connection._events[5].append(h2.events.StreamEnded(stream_id=5))
        else:
            second_read_started.set()
            await anyio.sleep_forever()

    async def write_outgoing_data(_request: httpcore2.Request) -> None:
        pass

    monkeypatch.setattr(connection, "_receive_events", receive_events)
    monkeypatch.setattr(connection, "_write_outgoing_data", write_outgoing_data)

    async def consume(stream_id: int) -> None:
        await connection._receive_stream_event(request, stream_id)
        if stream_id == 5:
            completed_response_received.set()

    async with anyio.create_task_group() as tasks:
        tasks.start_soon(consume, 1)
        await first_read_started.wait()
        # Stream 3 waits for the in-flight read to finish, ahead of stream 5.
        tasks.start_soon(consume, 3)
        await anyio.sleep(0)
        tasks.start_soon(consume, 5)
        await anyio.sleep(0)
        allow_first_read.set()

        with anyio.fail_after(2):
            await second_read_started.wait()
            # Stream 5 has an event, so it must complete even while the
            # next network read for stream 3 is still blocked.
            await completed_response_received.wait()

        tasks.cancel_scope.cancel()
