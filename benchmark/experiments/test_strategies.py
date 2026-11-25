from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import cast

import pytest

import httpcore2
import httpx2

from .asyncio_backend import AsyncioBackend
from .conftest import Origin
from .core_transport import CoreTransport
from .origin_pool import OriginPool

pytestmark = pytest.mark.anyio


@pytest.fixture(params=[False, True])
def backend(request: pytest.FixtureRequest) -> httpcore2.AsyncNetworkBackend | None:
    return cast(httpcore2.AsyncNetworkBackend, AsyncioBackend()) if request.param else None


@pytest.mark.parametrize("buffered", [False, True])
async def test_reuse_tls_redirects_decoding_and_uploads(
    origin: Origin, backend: httpcore2.AsyncNetworkBackend | None, buffered: bool
) -> None:
    pool = OriginPool(network_backend=backend, ssl_context=origin.ssl_context, max_connections=2)
    async with httpx2.AsyncClient(transport=CoreTransport(pool, buffered=buffered)) as client:
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
            async with client.stream("GET", url + "/stream") as response:
                assert b"".join([chunk async for chunk in response.aiter_raw()]) == b"firstsecond"


async def test_limits_and_origin_eviction(origin: Origin, backend: httpcore2.AsyncNetworkBackend | None) -> None:
    pool = OriginPool(network_backend=backend, max_connections=2)
    async with httpx2.AsyncClient(transport=CoreTransport(pool)) as client:
        responses = await asyncio.gather(*(client.get(origin.urls[i % 2] + "/delay") for i in range(12)))
        assert all(response.content == b"hello" * 100 for response in responses)
        assert origin.peak <= 2
    pool = OriginPool(network_backend=backend, max_connections=1)
    async with httpx2.AsyncClient(transport=CoreTransport(pool)) as client:
        first = await client.get(origin.urls[0] + "/body")
        await client.get(origin.urls[1] + "/body")
        last = await client.get(origin.urls[0] + "/body")
        assert first.headers["X-Peer"] != last.headers["X-Peer"]


async def test_pool_timeout_cancellation_and_partial_close(
    origin: Origin, backend: httpcore2.AsyncNetworkBackend | None
) -> None:
    pool = OriginPool(network_backend=backend, max_connections=1)
    url = origin.urls[0]
    async with httpx2.AsyncClient(transport=CoreTransport(pool)) as client:
        async with client.stream("GET", url + "/stream") as response:
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
    origin: Origin, backend: httpcore2.AsyncNetworkBackend | None, expiry: float, idle_limit: int | None
) -> None:
    pool = OriginPool(network_backend=backend, keepalive_expiry=expiry, max_keepalive_connections=idle_limit)
    async with httpx2.AsyncClient(transport=CoreTransport(pool)) as client:
        first = await client.get(origin.urls[0] + "/body")
        await asyncio.sleep(0.005)
        second = await client.get(origin.urls[0] + "/body")
        assert first.headers["X-Peer"] != second.headers["X-Peer"]
        await client.get(origin.urls[0] + "/close")
        assert (await client.get(origin.urls[0] + "/body")).status_code == 200
