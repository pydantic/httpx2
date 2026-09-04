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


@pytest.mark.parametrize("name", ["connect", "read", "write", "pool"])
def test_timeout_extension_value_must_be_a_number(name: str) -> None:
    request = httpx2.Request("GET", "http://127.0.0.1:1/")
    request.extensions["timeout"] = {name: httpx2.Timeout(5.0)}

    with pytest.raises(TypeError, match=f"extensions\\['timeout'\\]\\[{name!r}\\]"):
        httpx2.Client().send(request)


def test_timeout_extension_must_be_a_mapping() -> None:
    request = httpx2.Request("GET", "http://127.0.0.1:1/")
    request.extensions["timeout"] = httpx2.Timeout(5.0)

    with pytest.raises(TypeError, match="extensions\\['timeout'\\] must be a mapping"):
        httpx2.Client().send(request)


@pytest.mark.parametrize("timeout", [{"connect": 5.0}, {"read": 5}, {"connect": None}, {}])
def test_timeout_extension_accepts_numbers_and_none(timeout: dict[str, float | None]) -> None:
    request = httpx2.Client().build_request("GET", "http://127.0.0.1:1/", extensions={"timeout": timeout})

    assert request.extensions["timeout"] == timeout
