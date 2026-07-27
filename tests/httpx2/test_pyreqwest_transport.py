from __future__ import annotations

import asyncio

import pytest

import httpx2

pytest.importorskip("pyreqwest")


def test_pyreqwest_transport(server: object) -> None:
    with httpx2.Client(transport=httpx2.PyreqwestTransport()) as client:
        response = client.get(str(server.url))  # type: ignore[attr-defined]

    assert response.status_code == 200
    assert response.text == "Hello, world!"
    assert response.http_version == "HTTP/1.1"


def test_async_pyreqwest_transport(server: object) -> None:
    async def run() -> httpx2.Response:
        async with httpx2.AsyncClient(transport=httpx2.AsyncPyreqwestTransport()) as client:
            return await client.get(str(server.url))  # type: ignore[attr-defined]

    response = asyncio.run(run())

    assert response.status_code == 200
    assert response.text == "Hello, world!"
    assert response.http_version == "HTTP/1.1"


def test_pyreqwest_transport_request_body(server: object) -> None:
    with httpx2.Client(transport=httpx2.PyreqwestTransport()) as client:
        response = client.post(str(server.url.copy_with(path="/echo_body")), content=b"hello")  # type: ignore[attr-defined]

    assert response.status_code == 200
    assert response.content == b"hello"
