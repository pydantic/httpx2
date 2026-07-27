from __future__ import annotations

import asyncio

import pytest

import httpx2

pytest.importorskip("httpunk")


def test_async_httpunk_transport(server: object) -> None:
    async def run() -> httpx2.Response:
        async with httpx2.AsyncClient(transport=httpx2.AsyncHTTPunkTransport()) as client:
            return await client.get(str(server.url))  # type: ignore[attr-defined]

    response = asyncio.run(run())

    assert response.status_code == 200
    assert response.text == "Hello, world!"
    assert response.http_version == "HTTP/1.1"


def test_async_httpunk_transport_request_body(server: object) -> None:
    async def run() -> httpx2.Response:
        async with httpx2.AsyncClient(transport=httpx2.AsyncHTTPunkTransport()) as client:
            return await client.post(str(server.url.copy_with(path="/echo_body")), content=b"hello")  # type: ignore[attr-defined]

    response = asyncio.run(run())

    assert response.status_code == 200
    assert response.content == b"hello"


def test_async_httpunk_transport_connection_reuse(server: object) -> None:
    async def run() -> None:
        transport = httpx2.AsyncHTTPunkTransport()
        async with httpx2.AsyncClient(transport=transport) as client:
            response = await client.get(str(server.url))  # type: ignore[attr-defined]
            assert response.status_code == 200
            response = await client.get(str(server.url))  # type: ignore[attr-defined]
            assert response.status_code == 200
            assert sum(pool.count for pool in transport._pools.values()) == 1

    asyncio.run(run())
