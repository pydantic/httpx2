from __future__ import annotations

import asyncio
import gzip
import ssl
from collections.abc import AsyncIterator
from dataclasses import dataclass

import pytest
import trustme
from aiohttp import web


@dataclass
class Origin:
    urls: list[str]
    ssl_context: ssl.SSLContext
    active: int = 0
    peak: int = 0


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
async def origin() -> AsyncIterator[Origin]:
    ca = trustme.CA()
    cert = ca.issue_cert("127.0.0.1")
    server_ssl = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    cert.configure_cert(server_ssl)
    client_ssl = ssl.create_default_context()
    ca.configure_trust(client_ssl)
    result = Origin([], client_ssl)

    async def handler(request: web.Request) -> web.StreamResponse:
        assert request.transport is not None
        peer = str(request.transport.get_extra_info("peername"))
        if request.path == "/redirect":
            return web.Response(status=302, headers={"Location": "/body", "Set-Cookie": "token=yes; Path=/"})
        if request.path == "/gzip":
            return web.Response(body=gzip.compress(b"hello" * 100), headers={"Content-Encoding": "gzip"})
        if request.path == "/delay":
            result.active += 1
            result.peak = max(result.peak, result.active)
            try:
                await asyncio.sleep(0.08)
            finally:
                result.active -= 1
        if request.path == "/stream":
            response = web.StreamResponse(headers={"X-Peer": peer})
            await response.prepare(request)
            await response.write(b"first")
            await asyncio.sleep(0.1)
            try:
                await response.write(b"second")
            except ConnectionResetError:
                pass
            return response
        body = await request.read() if request.method == "POST" else b"hello" * 100
        response = web.Response(body=body, headers={"X-Peer": peer, "X-Cookie": request.headers.get("Cookie", "")})
        if request.path == "/close":
            response.force_close()
        return response

    runners = []
    for tls in (False, False, True):
        app = web.Application()
        app.router.add_route("*", "/{path:.*}", handler)
        runner = web.AppRunner(app, shutdown_timeout=0.5)
        await runner.setup()
        runners.append(runner)
        await web.TCPSite(runner, "127.0.0.1", 0, ssl_context=server_ssl if tls else None).start()
        result.urls.append(f"{'https' if tls else 'http'}://127.0.0.1:{runner.addresses[0][1]}")
    try:
        yield result
    finally:
        for runner in runners:
            await runner.cleanup()
