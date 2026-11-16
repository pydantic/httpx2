from collections.abc import AsyncIterator, Generator, Iterator

import pytest

import httpx2


def test_streamed_redirect_skips_intermediate_body() -> None:
    consumed: list[bytes] = []

    def body() -> Iterator[bytes]:  # pragma: no cover
        for _ in range(3):
            consumed.append(b"x")
            yield b"x" * 4096

    def handler(request: httpx2.Request) -> httpx2.Response:
        if request.url.path == "/start":
            return httpx2.Response(302, headers={"location": "/final"}, content=body())
        return httpx2.Response(200, content=b"done")

    with httpx2.Client(transport=httpx2.MockTransport(handler), follow_redirects=True) as client:
        with client.stream("GET", "https://example.test/start") as response:
            assert response.read() == b"done"
            assert response.status_code == 200
            assert len(response.history) == 1
            assert response.history[0].is_closed
    assert consumed == []


@pytest.mark.anyio
async def test_async_streamed_redirect_skips_intermediate_body() -> None:
    consumed: list[bytes] = []

    async def body() -> AsyncIterator[bytes]:  # pragma: no cover
        for _ in range(3):
            consumed.append(b"x")
            yield b"x" * 4096

    async def handler(request: httpx2.Request) -> httpx2.Response:
        if request.url.path == "/start":
            return httpx2.Response(302, headers={"location": "/final"}, content=body())
        return httpx2.Response(200, content=b"done")

    async with httpx2.AsyncClient(transport=httpx2.MockTransport(handler), follow_redirects=True) as client:
        async with client.stream("GET", "https://example.test/start") as response:
            assert await response.aread() == b"done"
            assert response.status_code == 200
            assert len(response.history) == 1
            assert response.history[0].is_closed
    assert consumed == []


class RetryAuth(httpx2.Auth):
    def auth_flow(self, request: httpx2.Request) -> Generator[httpx2.Request, httpx2.Response, None]:
        yield request
        request.headers["Authorization"] = "retry"
        yield request


def test_streamed_auth_skips_intermediate_body() -> None:
    consumed: list[bytes] = []

    def body() -> Iterator[bytes]:  # pragma: no cover
        for _ in range(3):
            consumed.append(b"x")
            yield b"x" * 4096

    def handler(request: httpx2.Request) -> httpx2.Response:
        if request.headers.get("authorization") != "retry":
            return httpx2.Response(401, content=body())
        return httpx2.Response(200, content=b"done")

    with httpx2.Client(transport=httpx2.MockTransport(handler), auth=RetryAuth()) as client:
        with client.stream("GET", "https://example.test/") as response:
            assert response.read() == b"done"
            assert len(response.history) == 1
            assert response.history[0].is_closed
    assert consumed == []


def test_streamed_redirect_and_digest_auth_skip_all_intermediates() -> None:
    consumed: list[int] = []

    def body() -> Iterator[bytes]:  # pragma: no cover
        for _ in range(3):
            consumed.append(1)
            yield b"x" * 4096

    def handler(request: httpx2.Request) -> httpx2.Response:
        if request.url.path == "/start":
            return httpx2.Response(302, headers={"location": "/digest"}, content=body())
        if not request.headers.get("authorization", "").startswith("Digest "):
            challenge = 'Digest realm="test", nonce="abc", qop="auth"'
            return httpx2.Response(401, headers={"www-authenticate": challenge}, content=body())
        return httpx2.Response(200, content=b"done")

    with httpx2.Client(
        transport=httpx2.MockTransport(handler), follow_redirects=True, auth=httpx2.DigestAuth("user", "pass")
    ) as client:
        with client.stream("GET", "https://example.test/start") as response:
            assert response.read() == b"done"
            assert response.status_code == 200
            assert response.history
            assert all(item.is_closed for item in response.history)
    assert consumed == []


@pytest.mark.anyio
async def test_async_streamed_redirect_and_digest_auth_skip_all_intermediates() -> None:
    consumed: list[int] = []

    async def body() -> AsyncIterator[bytes]:  # pragma: no cover
        for _ in range(3):
            consumed.append(1)
            yield b"x" * 4096

    async def handler(request: httpx2.Request) -> httpx2.Response:
        if request.url.path == "/start":
            return httpx2.Response(302, headers={"location": "/digest"}, content=body())
        if not request.headers.get("authorization", "").startswith("Digest "):
            challenge = 'Digest realm="test", nonce="abc", qop="auth"'
            return httpx2.Response(401, headers={"www-authenticate": challenge}, content=body())
        return httpx2.Response(200, content=b"done")

    async with httpx2.AsyncClient(
        transport=httpx2.MockTransport(handler), follow_redirects=True, auth=httpx2.DigestAuth("user", "pass")
    ) as client:
        async with client.stream("GET", "https://example.test/start") as response:
            assert await response.aread() == b"done"
            assert response.status_code == 200
            assert response.history
            assert all(item.is_closed for item in response.history)
    assert consumed == []


class ResponseBodyAuth(httpx2.Auth):
    requires_response_body = True

    def auth_flow(self, request: httpx2.Request) -> Generator[httpx2.Request, httpx2.Response, None]:
        response = yield request
        request.headers["Authorization"] = response.text
        yield request


def test_streamed_auth_reads_body_when_required() -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        if request.headers.get("authorization") == "token":
            return httpx2.Response(200, content=b"done")
        return httpx2.Response(401, content=b"token")

    with httpx2.Client(transport=httpx2.MockTransport(handler), auth=ResponseBodyAuth()) as client:
        with client.stream("GET", "https://example.test/") as response:
            assert response.read() == b"done"
            assert response.history[0].content == b"token"


@pytest.mark.anyio
async def test_async_streamed_auth_reads_body_when_required() -> None:
    async def handler(request: httpx2.Request) -> httpx2.Response:
        if request.headers.get("authorization") == "token":
            return httpx2.Response(200, content=b"done")
        return httpx2.Response(401, content=b"token")

    async with httpx2.AsyncClient(transport=httpx2.MockTransport(handler), auth=ResponseBodyAuth()) as client:
        async with client.stream("GET", "https://example.test/") as response:
            assert await response.aread() == b"done"
            assert response.history[0].content == b"token"


def test_eager_redirect_retains_history_body() -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        if request.url.path == "/start":
            return httpx2.Response(302, headers={"location": "/final"}, content=b"history")
        return httpx2.Response(200, content=b"done")

    with httpx2.Client(transport=httpx2.MockTransport(handler), follow_redirects=True) as client:
        response = client.get("https://example.test/start")
        assert response.content == b"done"
        assert response.history[0].content == b"history"
