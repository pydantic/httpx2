import json
from collections.abc import AsyncIterator, Awaitable, Callable, MutableMapping
from typing import Any

import anyio
import pytest

import httpx2

Message = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[MutableMapping[str, Any]], Awaitable[None]]
Scope = MutableMapping[str, Any]


async def hello_world(scope: Scope, receive: Receive, send: Send) -> None:
    status = 200
    output = b"Hello, World!"
    headers = [(b"content-type", "text/plain"), (b"content-length", str(len(output)))]

    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": output})


async def echo_path(scope: Scope, receive: Receive, send: Send) -> None:
    status = 200
    output = json.dumps({"path": scope["path"]}).encode("utf-8")
    headers = [(b"content-type", "text/plain"), (b"content-length", str(len(output)))]

    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": output})


async def echo_raw_path(scope: Scope, receive: Receive, send: Send) -> None:
    status = 200
    output = json.dumps({"raw_path": scope["raw_path"].decode("ascii")}).encode("utf-8")
    headers = [(b"content-type", "text/plain"), (b"content-length", str(len(output)))]

    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": output})


async def echo_body(scope: Scope, receive: Receive, send: Send) -> None:
    status = 200
    headers = [(b"content-type", "text/plain")]

    await send({"type": "http.response.start", "status": status, "headers": headers})
    more_body = True
    while more_body:
        message = await receive()
        body = message.get("body", b"")
        more_body = message.get("more_body", False)
        await send({"type": "http.response.body", "body": body, "more_body": more_body})


async def echo_headers(scope: Scope, receive: Receive, send: Send) -> None:
    status = 200
    output = json.dumps({"headers": [[k.decode(), v.decode()] for k, v in scope["headers"]]}).encode("utf-8")
    headers = [(b"content-type", "text/plain"), (b"content-length", str(len(output)))]

    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": output})


async def raise_exc(scope: Scope, receive: Receive, send: Send) -> None:
    raise RuntimeError()


async def raise_exc_after_response(scope: Scope, receive: Receive, send: Send) -> None:
    status = 200
    output = b"Hello, World!"
    headers = [(b"content-type", "text/plain"), (b"content-length", str(len(output)))]

    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": output})
    raise RuntimeError()


@pytest.mark.anyio
async def test_asgi_transport() -> None:
    async with httpx2.ASGITransport(app=hello_world) as transport:
        request = httpx2.Request("GET", "http://www.example.com/")
        response = await transport.handle_async_request(request)
        await response.aread()
        assert response.status_code == 200
        assert response.content == b"Hello, World!"


@pytest.mark.anyio
async def test_asgi_transport_no_body() -> None:
    async with httpx2.ASGITransport(app=echo_body) as transport:
        request = httpx2.Request("GET", "http://www.example.com/")
        response = await transport.handle_async_request(request)
        await response.aread()
        assert response.status_code == 200
        assert response.content == b""


@pytest.mark.anyio
async def test_asgi() -> None:
    transport = httpx2.ASGITransport(app=hello_world)
    async with httpx2.AsyncClient(transport=transport) as client:
        response = await client.get("http://www.example.org/")

    assert response.status_code == 200
    assert response.text == "Hello, World!"


@pytest.mark.anyio
async def test_asgi_urlencoded_path() -> None:
    transport = httpx2.ASGITransport(app=echo_path)
    async with httpx2.AsyncClient(transport=transport) as client:
        url = httpx2.URL("http://www.example.org/").copy_with(path="/user@example.org")
        response = await client.get(url)

    assert response.status_code == 200
    assert response.json() == {"path": "/user@example.org"}


@pytest.mark.anyio
async def test_asgi_raw_path() -> None:
    transport = httpx2.ASGITransport(app=echo_raw_path)
    async with httpx2.AsyncClient(transport=transport) as client:
        url = httpx2.URL("http://www.example.org/").copy_with(path="/user@example.org")
        response = await client.get(url)

    assert response.status_code == 200
    assert response.json() == {"raw_path": "/user@example.org"}


@pytest.mark.anyio
async def test_asgi_raw_path_should_not_include_querystring_portion() -> None:
    """
    See https://github.com/encode/httpx/issues/2810
    """
    transport = httpx2.ASGITransport(app=echo_raw_path)
    async with httpx2.AsyncClient(transport=transport) as client:
        url = httpx2.URL("http://www.example.org/path?query")
        response = await client.get(url)

    assert response.status_code == 200
    assert response.json() == {"raw_path": "/path"}


@pytest.mark.anyio
async def test_asgi_upload() -> None:
    transport = httpx2.ASGITransport(app=echo_body)
    async with httpx2.AsyncClient(transport=transport) as client:
        response = await client.post("http://www.example.org/", content=b"example")

    assert response.status_code == 200
    assert response.text == "example"


@pytest.mark.anyio
async def test_asgi_headers() -> None:
    transport = httpx2.ASGITransport(app=echo_headers)
    async with httpx2.AsyncClient(transport=transport) as client:
        response = await client.get("http://www.example.org/")

    assert response.status_code == 200
    assert response.json() == {
        "headers": [
            ["host", "www.example.org"],
            ["accept", "*/*"],
            ["accept-encoding", "gzip, deflate, br, zstd"],
            ["connection", "keep-alive"],
            ["user-agent", f"python-httpx2/{httpx2.__version__}"],
        ]
    }


@pytest.mark.anyio
async def test_asgi_exc() -> None:
    transport = httpx2.ASGITransport(app=raise_exc)
    async with httpx2.AsyncClient(transport=transport) as client:
        with pytest.raises(RuntimeError):
            await client.get("http://www.example.org/")


@pytest.mark.anyio
async def test_asgi_exc_after_response() -> None:
    transport = httpx2.ASGITransport(app=raise_exc_after_response)
    async with httpx2.AsyncClient(transport=transport) as client:
        with pytest.raises(RuntimeError):
            await client.get("http://www.example.org/")


@pytest.mark.anyio
async def test_asgi_disconnect_after_response_complete() -> None:
    disconnect = False

    async def read_body(scope: Scope, receive: Receive, send: Send) -> None:
        nonlocal disconnect

        status = 200
        headers = [(b"content-type", "text/plain")]

        await send({"type": "http.response.start", "status": status, "headers": headers})
        more_body = True
        while more_body:
            message = await receive()
            more_body = message.get("more_body", False)

        await send({"type": "http.response.body", "body": b"", "more_body": False})

        # The ASGI spec says of the Disconnect message:
        # "Sent to the application when a HTTP connection is closed or if receive is
        # called after a response has been sent."
        # So if receive() is called again, the disconnect message should be received
        message = await receive()
        disconnect = message.get("type") == "http.disconnect"

    transport = httpx2.ASGITransport(app=read_body)
    async with httpx2.AsyncClient(transport=transport) as client:
        response = await client.post("http://www.example.org/", content=b"example")

    assert response.status_code == 200
    assert disconnect


@pytest.mark.anyio
async def test_asgi_exc_no_raise() -> None:
    transport = httpx2.ASGITransport(app=raise_exc, raise_app_exceptions=False)
    async with httpx2.AsyncClient(transport=transport) as client:
        response = await client.get("http://www.example.org/")

        assert response.status_code == 500


class RecordedBody(httpx2.AsyncByteStream):
    def __init__(self) -> None:
        self.closed_early = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        try:
            for _ in range(16):
                yield b"x" * 1024
        except GeneratorExit:
            self.closed_early = True
            raise


@pytest.mark.anyio
async def test_asgi_closes_the_unread_request_body() -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await receive()  # the first chunk only
        await send({"type": "http.response.start", "status": 413, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    body = RecordedBody()
    transport = httpx2.ASGITransport(app=app)
    async with httpx2.AsyncClient(transport=transport) as client:
        response = await client.send(httpx2.Request("POST", "http://www.example.org/", stream=body))

    assert response.status_code == 413
    assert body.closed_early


@pytest.mark.anyio
async def test_asgi_closes_the_unread_request_body_when_the_app_raises() -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await receive()  # the first chunk only
        raise RuntimeError()

    body = RecordedBody()
    transport = httpx2.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx2.AsyncClient(transport=transport) as client:
        response = await client.send(httpx2.Request("POST", "http://www.example.org/", stream=body))

    assert response.status_code == 500
    assert body.closed_early


@pytest.mark.anyio
async def test_asgi_leaves_a_request_body_still_being_received() -> None:
    async def slow_body() -> AsyncIterator[bytes]:
        yield b"first"
        await anyio.sleep(3600)
        yield b"never"  # pragma: no cover

    async with anyio.create_task_group() as tg:

        async def app(scope: Scope, receive: Receive, send: Send) -> None:
            async def listen() -> None:
                await receive()

            await receive()
            tg.start_soon(listen)  # outlives the app
            await anyio.wait_all_tasks_blocked()
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b""})

        transport = httpx2.ASGITransport(app=app)
        async with httpx2.AsyncClient(transport=transport) as client:
            response = await client.post("http://www.example.org/", content=slow_body())

        assert response.status_code == 200
        tg.cancel_scope.cancel()
