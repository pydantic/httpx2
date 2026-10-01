from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

import httpx2

from .aiohttp_transport import AiohttpTransport
from .conftest import Origin

pytestmark = pytest.mark.anyio


async def test_aiohttp_transport(origin: Origin) -> None:
    transport = AiohttpTransport(max_connections=2, ssl_context=origin.ssl_context)
    timeout = httpx2.Timeout(1, write=None)
    async with httpx2.AsyncClient(transport=transport, timeout=timeout) as client:
        for url in origin.urls:
            first = await client.get(url + "/body")
            second = await client.get(url + "/body")
            assert first.headers["X-Peer"] == second.headers["X-Peer"]
            assert first.content == second.content == b"hello" * 100
            assert (await client.head(url + "/body")).content == b""
            assert (await client.get(url + "/gzip")).content == b"hello" * 100
            response = await client.get(url + "/redirect", follow_redirects=True)
            assert len(response.history) == 1
            assert response.headers["X-Cookie"] == "token=yes"

            async def upload() -> AsyncIterator[bytes]:
                yield b"one"
                yield b"two"

            assert (await client.post(url + "/body", content=upload())).content == b"onetwo"
            async with client.stream("GET", url + "/stream") as response:
                async for chunk in response.aiter_raw():
                    assert chunk == b"first"
                    break
            assert (await client.get(url + "/body")).status_code == 200
        with pytest.raises(httpx2.ReadTimeout):
            await client.get(origin.urls[0] + "/delay", timeout=httpx2.Timeout(1, read=0.01, write=None))
        assert (await client.get(origin.urls[0] + "/body")).status_code == 200
        with pytest.raises(NotImplementedError, match="write=None"):
            await client.get(origin.urls[0] + "/body", timeout=1)
        with pytest.raises(httpx2.UnsupportedProtocol):
            await client.get("ftp://127.0.0.1/test")
