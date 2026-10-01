from __future__ import annotations

import ssl
from collections.abc import AsyncIterator

import aiohttp
from yarl import URL

import httpx2


class AiohttpTransport(httpx2.AsyncBaseTransport):
    def __init__(self, max_connections: int | None = None, ssl_context: ssl.SSLContext | bool = True) -> None:
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=max_connections or 0, keepalive_timeout=95.0, ssl=ssl_context),
            cookie_jar=aiohttp.DummyCookieJar(),
            auto_decompress=False,
            trust_env=False,
            skip_auto_headers={"User-Agent", "Accept", "Accept-Encoding", "Content-Type"},
        )

    async def handle_async_request(self, request: httpx2.Request) -> httpx2.Response:
        timeouts = request.extensions.get("timeout", {})
        if timeouts.get("write") is not None:
            raise NotImplementedError("The aiohttp experiment requires Timeout(write=None)")
        if request.url.scheme not in ("http", "https"):
            raise httpx2.UnsupportedProtocol("The aiohttp experiment supports HTTP and HTTPS only")
        body: bytes | httpx2.AsyncByteStream
        try:
            body = request.content
        except httpx2.RequestNotRead:
            if not isinstance(request.stream, httpx2.AsyncByteStream):
                raise RuntimeError("The aiohttp experiment requires an asynchronous stream")
            body = request.stream
        transfer_encoding = request.headers.get("transfer-encoding")
        if transfer_encoding is not None and transfer_encoding.lower() != "chunked":
            raise httpx2.LocalProtocolError("The aiohttp experiment supports only chunked transfer encoding")
        try:
            response = await self.session.request(
                request.method,
                URL(str(request.url), encoded=True),
                headers=[
                    (key.decode("ascii"), value.decode("ascii"))
                    for key, value in request.headers.raw
                    if key.lower() != b"transfer-encoding"
                ],
                data=body,
                chunked=True if transfer_encoding is not None else None,
                allow_redirects=False,
                timeout=aiohttp.ClientTimeout(
                    total=None,
                    connect=timeouts.get("pool"),
                    sock_connect=timeouts.get("connect"),
                    sock_read=timeouts.get("read"),
                ),
            )
        except aiohttp.ConnectionTimeoutError as exc:
            raise httpx2.ConnectTimeout(str(exc)) from exc
        except aiohttp.SocketTimeoutError as exc:
            raise httpx2.ReadTimeout(str(exc)) from exc
        except aiohttp.ClientError as exc:
            raise httpx2.TransportError(str(exc)) from exc
        assert response.version is not None
        return httpx2.Response(
            response.status,
            headers=response.raw_headers,
            stream=AiohttpStream(response),
            extensions={
                "http_version": f"HTTP/{response.version.major}.{response.version.minor}".encode(),
                "reason_phrase": (response.reason or "").encode(),
            },
        )

    async def aclose(self) -> None:
        await self.session.close()


class AiohttpStream(httpx2.AsyncByteStream):
    def __init__(self, response: aiohttp.ClientResponse) -> None:
        self.response = response

    async def __aiter__(self) -> AsyncIterator[bytes]:
        try:
            async for chunk in self.response.content.iter_chunked(64 * 1024):
                yield chunk
        except aiohttp.SocketTimeoutError as exc:
            raise httpx2.ReadTimeout(str(exc)) from exc
        except aiohttp.ClientError as exc:
            raise httpx2.ReadError(str(exc)) from exc

    async def aclose(self) -> None:
        self.response.close()
