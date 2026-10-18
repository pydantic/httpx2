import json
import typing

import anyio
import pytest

import httpx2

Message = typing.MutableMapping[str, typing.Any]
Receive = typing.Callable[[], typing.Awaitable[Message]]
Send = typing.Callable[[typing.MutableMapping[str, typing.Any]], typing.Awaitable[None]]
Scope = typing.MutableMapping[str, typing.Any]
ASGIApp = typing.Callable[[Scope, Receive, Send], typing.Awaitable[None]]


def run_in_task_group(app: ASGIApp) -> ASGIApp:
    async def wrapped_app(scope: Scope, receive: Receive, send: Send) -> None:
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(app, scope, receive, send)

    return wrapped_app


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


async def raise_exc_after_response_start(scope: Scope, receive: Receive, send: Send) -> None:
    status = 200
    output = b"Hello, World!"
    headers = [(b"content-type", "text/plain"), (b"content-length", str(len(output)))]

    await send({"type": "http.response.start", "status": status, "headers": headers})
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
async def test_asgi_exc_after_response_start() -> None:
    transport = httpx2.ASGITransport(app=raise_exc_after_response_start)
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


@pytest.mark.anyio
async def test_asgi_exc_no_raise_after_response_start() -> None:
    transport = httpx2.ASGITransport(app=raise_exc_after_response_start, raise_app_exceptions=False)
    async with httpx2.AsyncClient(transport=transport) as client:
        response = await client.get("http://www.example.org/")

        assert response.status_code == 200


@pytest.mark.anyio
async def test_asgi_exc_no_raise_after_response() -> None:
    transport = httpx2.ASGITransport(app=raise_exc_after_response, raise_app_exceptions=False)
    async with httpx2.AsyncClient(transport=transport) as client:
        response = await client.get("http://www.example.org/")

        assert response.status_code == 200


@pytest.mark.parametrize(
    "send_in_sub_task", [pytest.param(False, id="no_sub_task"), pytest.param(True, id="with_sub_task")]
)
@pytest.mark.anyio
async def test_asgi_stream_returns_before_waiting_for_body(send_in_sub_task: bool) -> None:
    start_response_body = anyio.Event()

    async def send_response_body_after_event(scope: Scope, receive: Receive, send: Send) -> None:
        status = 200
        headers = [(b"content-type", b"text/plain")]
        await send({"type": "http.response.start", "status": status, "headers": headers})
        await start_response_body.wait()
        await send({"type": "http.response.body", "body": b"body", "more_body": False})

    app = run_in_task_group(send_response_body_after_event) if send_in_sub_task else send_response_body_after_event

    transport = httpx2.ASGITransport(app=app)
    async with httpx2.AsyncClient(transport=transport) as client:
        with anyio.fail_after(1):
            async with client.stream("GET", "http://www.example.org/") as response:
                assert response.status_code == 200
                start_response_body.set()
                await response.aread()
                assert response.text == "body"


@pytest.mark.parametrize(
    "send_in_sub_task", [pytest.param(False, id="no_sub_task"), pytest.param(True, id="with_sub_task")]
)
@pytest.mark.anyio
async def test_asgi_stream_allows_iterative_streaming(send_in_sub_task: bool) -> None:
    stream_events = [anyio.Event() for _ in range(4)]

    async def send_response_body_after_event(scope: Scope, receive: Receive, send: Send) -> None:
        status = 200
        headers = [(b"content-type", b"text/plain")]
        await send({"type": "http.response.start", "status": status, "headers": headers})
        for event in stream_events:
            await event.wait()
            await send({"type": "http.response.body", "body": b"chunk", "more_body": event is not stream_events[-1]})

    app = run_in_task_group(send_response_body_after_event) if send_in_sub_task else send_response_body_after_event

    transport = httpx2.ASGITransport(app=app)
    async with httpx2.AsyncClient(transport=transport) as client:
        with anyio.fail_after(1):
            async with client.stream("GET", "http://www.example.org/") as response:
                assert response.status_code == 200
                iterator = response.aiter_raw()
                for event in stream_events:
                    event.set()
                    assert await iterator.__anext__() == b"chunk"
                with pytest.raises(StopAsyncIteration):
                    await iterator.__anext__()


@pytest.mark.anyio
async def test_asgi_stream_early_close() -> None:
    async def stream_forever(scope: Scope, receive: Receive, send: Send) -> None:
        status = 200
        headers = [(b"content-type", b"text/plain")]
        await send({"type": "http.response.start", "status": status, "headers": headers})
        while True:
            await send({"type": "http.response.body", "body": b"chunk", "more_body": True})

    transport = httpx2.ASGITransport(app=stream_forever)
    async with httpx2.AsyncClient(transport=transport) as client:
        with anyio.fail_after(1):
            async with client.stream("GET", "http://www.example.org/") as response:
                assert response.status_code == 200
