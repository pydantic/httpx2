from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from functools import partial

import pytest

import httpx2

from .conftest import Origin
from .fused_transport import FusedTransport

pytestmark = pytest.mark.anyio


@pytest.fixture(params=[(False, False), (True, False), (True, True)])
def transport_factory(request: pytest.FixtureRequest) -> Callable[..., FusedTransport]:
    return partial(FusedTransport, protocol_io=request.param[0], aggregate=request.param[1])


@pytest.mark.parametrize("buffered", [False, True])
async def test_reuse_tls_redirects_decoding_and_uploads(
    origin: Origin, transport_factory: Callable[..., FusedTransport], buffered: bool
) -> None:
    pool = transport_factory(ssl_context=origin.ssl_context, max_connections=2, buffered=buffered)
    async with httpx2.AsyncClient(transport=pool) as client:
        for url in origin.urls:
            first = await client.get(url + "/body")
            second = await client.get(url + "/body")
            assert first.content == second.content == b"hello" * 100
            assert first.headers["X-Peer"] == second.headers["X-Peer"]
            assert (await client.head(url + "/body")).content == b""
            assert (await client.get(url + "/gzip")).content == b"hello" * 100
            response = await client.get(url + "/redirect", follow_redirects=True)
            assert len(response.history) == 1
            assert response.headers["X-Cookie"] == "token=yes"
            assert (await client.post(url + "/body", content=b"upload")).content == b"upload"

            async def body() -> AsyncIterator[bytes]:
                yield b"one"
                yield b"two"

            assert (await client.post(url + "/body", content=body())).content == b"onetwo"
            payload = bytes(range(256)) * 2048
            assert (await client.post(url + "/body", content=payload)).content == payload
            assert first.elapsed.total_seconds() >= 0
            async with client.stream("GET", url + "/stream", extensions={"httpx2_buffer_response": False}) as response:
                assert b"".join([chunk async for chunk in response.aiter_raw()]) == b"firstsecond"


async def test_limits_and_origin_eviction(origin: Origin, transport_factory: Callable[..., FusedTransport]) -> None:
    pool = transport_factory(max_connections=2)
    async with httpx2.AsyncClient(transport=pool) as client:
        responses = await asyncio.gather(*(client.get(origin.urls[i % 2] + "/delay") for i in range(12)))
        assert all(response.content == b"hello" * 100 for response in responses)
        assert origin.peak <= 2
    pool = transport_factory(max_connections=1)
    async with httpx2.AsyncClient(transport=pool) as client:
        first = await client.get(origin.urls[0] + "/body")
        await client.get(origin.urls[1] + "/body")
        last = await client.get(origin.urls[0] + "/body")
        assert first.headers["X-Peer"] != last.headers["X-Peer"]


async def test_pool_timeout_cancellation_and_partial_close(
    origin: Origin, transport_factory: Callable[..., FusedTransport]
) -> None:
    pool = transport_factory(max_connections=1)
    url = origin.urls[0]
    async with httpx2.AsyncClient(transport=pool) as client:
        async with client.stream("GET", url + "/stream", extensions={"httpx2_buffer_response": False}) as response:
            async for chunk in response.aiter_raw():
                assert chunk == b"first"
                with pytest.raises(httpx2.PoolTimeout):
                    await client.get(url + "/body", timeout=httpx2.Timeout(1, pool=0.01))
                waiting = asyncio.create_task(client.get(url + "/body"))
                await asyncio.sleep(0)
                waiting.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await waiting
                break
        assert (await client.get(url + "/body")).content == b"hello" * 100
        active = asyncio.create_task(client.get(url + "/delay"))
        await asyncio.sleep(0.02)
        active.cancel()
        with pytest.raises(asyncio.CancelledError):
            await active
        assert (await client.get(url + "/body")).content == b"hello" * 100
        with pytest.raises(httpx2.ReadTimeout):
            await client.get(url + "/delay", timeout=httpx2.Timeout(1, read=0.01))
        assert (await client.get(url + "/body")).content == b"hello" * 100


@pytest.mark.parametrize("expiry,idle_limit", [(0.001, None), (95.0, 0)])
async def test_expiry_and_server_close(
    origin: Origin, transport_factory: Callable[..., FusedTransport], expiry: float, idle_limit: int | None
) -> None:
    pool = transport_factory(keepalive_expiry=expiry, max_keepalive_connections=idle_limit)
    async with httpx2.AsyncClient(transport=pool) as client:
        first = await client.get(origin.urls[0] + "/body")
        await asyncio.sleep(0.005)
        second = await client.get(origin.urls[0] + "/body")
        assert first.headers["X-Peer"] != second.headers["X-Peer"]
        await client.get(origin.urls[0] + "/close")
        assert (await client.get(origin.urls[0] + "/body")).status_code == 200


@pytest.mark.parametrize("buffered", [False, True])
async def test_protocol_errors_and_tls_failure_release_connection(
    origin: Origin, transport_factory: Callable[..., FusedTransport], buffered: bool
) -> None:
    async def incomplete(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.readuntil(b"\r\n\r\n")
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nshort")
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    async with await asyncio.start_server(incomplete, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        async with httpx2.AsyncClient(transport=transport_factory(max_connections=1, buffered=buffered)) as client:
            for _ in range(2):
                with pytest.raises(httpx2.RemoteProtocolError):
                    await client.get(f"http://127.0.0.1:{port}/")
                assert (await client.get(origin.urls[0] + "/body")).status_code == 200
            with pytest.raises(httpx2.ConnectError):
                await client.get(origin.urls[2] + "/body")
            assert (await client.get(origin.urls[0] + "/body")).status_code == 200


@pytest.mark.parametrize("buffered", [False, True])
async def test_changed_read_deadlines_and_backpressure(
    origin: Origin, transport_factory: Callable[..., FusedTransport], buffered: bool
) -> None:
    url = origin.urls[0]
    async with httpx2.AsyncClient(
        transport=transport_factory(max_connections=1, read_size=1, buffered=buffered)
    ) as client:
        first = await client.get(url + "/body", timeout=httpx2.Timeout(1, read=0.01))
        second = await client.get(url + "/delay", timeout=httpx2.Timeout(1, read=0.2))
        assert first.headers["X-Peer"] == second.headers["X-Peer"]
        with pytest.raises(httpx2.ReadTimeout):
            await client.get(url + "/delay", timeout=httpx2.Timeout(1, read=0.005))
        await client.get(url + "/body", timeout=httpx2.Timeout(1, read=0.01))
        await asyncio.sleep(0.02)
        assert (await client.get(url + "/delay", timeout=httpx2.Timeout(1, read=None))).content == b"hello" * 100
        async with client.stream("GET", url + "/stream", extensions={"httpx2_buffer_response": False}) as response:
            await asyncio.sleep(0.15)
            assert b"".join([part async for part in response.aiter_bytes(chunk_size=3)]) == b"firstsecond"
        assert (await client.get(url + "/body")).content == b"hello" * 100
