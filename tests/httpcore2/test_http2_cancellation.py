from __future__ import annotations

from typing import Any

import anyio
import h2.config
import h2.connection
import h2.events
import h2.exceptions
import pytest

import httpcore2


@pytest.mark.anyio
async def test_cancelled_reader_preserves_other_stream_response() -> None:
    reader_waiting = anyio.Event()
    writer_waiting = anyio.Event()
    allow_read = anyio.Event()
    allow_write = anyio.Event()
    bytes_read = anyio.Event()
    reader_done = anyio.Event()
    send, receive = anyio.create_memory_object_stream[bytes](10)
    peer = h2.connection.H2Connection(h2.config.H2Configuration(client_side=False))
    peer.initiate_connection()
    survivor_id = 0
    pause_read = False
    pause_write = False

    class PeerStream(httpcore2.AsyncNetworkStream):
        async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
            nonlocal pause_read
            if pause_read:
                pause_read = False
                reader_waiting.set()
                await allow_read.wait()
                peer.send_data(survivor_id, b"survived", end_stream=True)
                data = peer.data_to_send()
                bytes_read.set()
                return data
            return await receive.receive()

        async def write(self, buffer: bytes, timeout: float | None = None) -> None:
            nonlocal survivor_id, pause_read, pause_write
            if pause_write:
                pause_write = False
                writer_waiting.set()
                await allow_write.wait()
            for event in peer.receive_data(buffer):
                if isinstance(event, h2.events.RequestReceived):
                    path = dict(event.headers)[b":path"]
                    if path == b"/reader":
                        pause_read = True
                        pause_write = True
                    else:
                        peer.send_headers(event.stream_id, [(b":status", b"200")])
                        if path == b"/survivor":
                            survivor_id = event.stream_id
                        else:
                            peer.send_data(event.stream_id, b"ok", end_stream=True)
            if data := peer.data_to_send():
                send.send_nowait(data)

        async def aclose(self) -> None:
            await send.aclose()
            await receive.aclose()

    origin = httpcore2.Origin(b"https", b"example.com", 443)
    with anyio.fail_after(5):
        async with httpcore2.AsyncHTTP2Connection(origin, PeerStream()) as connection:
            assert (await connection.request("GET", "https://example.com/warmup")).content == b"ok"
            async with connection.stream("GET", "https://example.com/survivor") as survivor:

                async def read_response(*, task_status: anyio.abc.TaskStatus[anyio.CancelScope]) -> None:
                    with anyio.CancelScope() as scope:
                        task_status.started(scope)
                        await connection.request("GET", "https://example.com/reader")
                    reader_done.set()

                async def write_request() -> None:
                    response = await connection.request("GET", "https://example.com/writer")
                    assert response.content == b"ok"

                async with anyio.create_task_group() as group:
                    scope = await group.start(read_response)
                    await reader_waiting.wait()
                    group.start_soon(write_request)
                    await writer_waiting.wait()
                    allow_read.set()
                    await bytes_read.wait()
                    scope.cancel()
                    await reader_done.wait()
                    allow_write.set()
                    assert await survivor.aread() == b"survived"
            assert (await connection.request("GET", "https://example.com/after")).content == b"ok"
            assert connection.is_idle()


@pytest.mark.anyio
@pytest.mark.parametrize("stage", ["started", "complete"])
@pytest.mark.parametrize(
    "error", [RuntimeError, httpcore2.ConnectionNotAvailable, h2.exceptions.NoAvailableStreamIDError]
)
async def test_header_trace_failure_releases_stream(stage: str, error: type[Exception]) -> None:
    peer = h2.connection.H2Connection(h2.config.H2Configuration(client_side=False))
    peer.initiate_connection()

    class EchoStream(httpcore2.AsyncNetworkStream):
        async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
            return peer.data_to_send()

        async def write(self, buffer: bytes, timeout: float | None = None) -> None:
            for event in peer.receive_data(buffer):
                if isinstance(event, h2.events.RequestReceived):
                    peer.send_headers(event.stream_id, [(b":status", b"200")], end_stream=True)

        async def aclose(self) -> None:
            pass

    async def trace(name: str, info: dict[str, Any]) -> None:
        if name == f"http2.send_request_headers.{stage}":
            raise error("trace failed")

    expected = httpcore2.LocalProtocolError if error is h2.exceptions.NoAvailableStreamIDError else error
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    async with httpcore2.AsyncHTTP2Connection(origin, EchoStream(), keepalive_expiry=5) as connection:
        with pytest.raises(expected, match="trace failed"):
            await connection.request("GET", "https://example.com/", extensions={"trace": trace})
        assert connection.is_idle()
        assert connection.is_available()
        assert not connection.has_expired()
        assert (await connection.request("GET", "https://example.com/")).status == 200
