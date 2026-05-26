from __future__ import annotations

import typing

import pytest

import httpx2

if typing.TYPE_CHECKING:
    from conftest import TestServer


@pytest.mark.anyio
async def test_read_timeout(server: TestServer) -> None:
    timeout = httpx2.Timeout(None, read=1e-6)

    async with httpx2.AsyncClient(timeout=timeout) as client:
        with pytest.raises(httpx2.ReadTimeout):
            await client.get(server.url.copy_with(path="/slow_response"))


@pytest.mark.anyio
async def test_write_timeout(server: TestServer) -> None:
    timeout = httpx2.Timeout(None, write=1e-6)

    async with httpx2.AsyncClient(timeout=timeout) as client:
        with pytest.raises(httpx2.WriteTimeout):
            data = b"*" * 1024 * 1024 * 100
            await client.put(server.url.copy_with(path="/slow_response"), content=data)


@pytest.mark.anyio
@pytest.mark.network
async def test_connect_timeout(server: TestServer) -> None:
    timeout = httpx2.Timeout(None, connect=1e-6)

    async with httpx2.AsyncClient(timeout=timeout) as client:
        with pytest.raises(httpx2.ConnectTimeout):
            # See https://stackoverflow.com/questions/100841/
            await client.get("http://10.255.255.1/")


@pytest.mark.anyio
async def test_pool_timeout(server: TestServer) -> None:
    limits = httpx2.Limits(max_connections=1)
    timeout = httpx2.Timeout(None, pool=1e-4)

    async with httpx2.AsyncClient(limits=limits, timeout=timeout) as client:
        with pytest.raises(httpx2.PoolTimeout):
            async with client.stream("GET", server.url):
                await client.get(server.url)


@pytest.mark.anyio
async def test_async_client_new_request_send_timeout(server: TestServer) -> None:
    timeout = httpx2.Timeout(1e-6)

    async with httpx2.AsyncClient(timeout=timeout) as client:
        with pytest.raises(httpx2.TimeoutException):
            await client.send(httpx2.Request("GET", server.url.copy_with(path="/slow_response")))
