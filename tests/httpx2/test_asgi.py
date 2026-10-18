import contextlib
import json
import typing

import anyio
import pytest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import StreamingResponse
from starlette.routing import Route

import httpx2

Message = typing.MutableMapping[str, typing.Any]
Receive = typing.Callable[[], typing.Awaitable[Message]]
Send = typing.Callable[[typing.MutableMapping[str, typing.Any]], typing.Awaitable[None]]
Scope = typing.MutableMapping[str, typing.Any]


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


@pytest.mark.anyio
async def test_asgi_exc_without_context_manager() -> None:
    client = httpx2.AsyncClient(transport=httpx2.ASGITransport(app=raise_exc))
    with pytest.raises(RuntimeError):
        await client.get("http://www.example.org/")


@pytest.mark.anyio
async def test_asgi_without_context_manager_buffers_response() -> None:
    disconnected = False

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        nonlocal disconnected
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"Hello, ", "more_body": True})
        await send({"type": "http.response.body", "body": b"World!"})
        disconnected = (await receive())["type"] == "http.disconnect"

    client = httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app))
    response = await client.get("http://www.example.org/")

    assert disconnected
    assert response.text == "Hello, World!"


@pytest.mark.anyio
async def test_asgi_exc_after_response_start() -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        raise RuntimeError()

    async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app)) as client:
        async with client.stream("GET", "http://www.example.org/") as response:
            assert response.status_code == 200
            with pytest.raises(RuntimeError):
                await response.aread()


@pytest.mark.anyio
async def test_asgi_exc_no_raise_after_response_start() -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"partial", "more_body": True})
        raise RuntimeError()

    transport = httpx2.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx2.AsyncClient(transport=transport) as client:
        response = await client.get("http://www.example.org/")

    assert response.status_code == 200
    assert response.text == "partial"


@pytest.mark.anyio
async def test_asgi_streaming_response() -> None:
    chunks = iter([b"first", b"second"])
    next_chunk = anyio.Event()

    async def stream() -> typing.AsyncIterator[bytes]:
        yield next(chunks)
        await next_chunk.wait()
        yield next(chunks)

    async def endpoint(request: Request) -> StreamingResponse:
        return StreamingResponse(stream())

    app = Starlette(routes=[Route("/", endpoint)])

    async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app)) as client:
        with anyio.fail_after(1):
            async with client.stream("GET", "http://www.example.org/") as response:
                parts = response.aiter_bytes()
                assert await anext(parts) == b"first"
                next_chunk.set()
                assert await anext(parts) == b"second"
                with pytest.raises(StopAsyncIteration):
                    await anext(parts)


@pytest.mark.anyio
async def test_asgi_streaming_response_close_disconnects_app() -> None:
    disconnected = anyio.Event()

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        async with anyio.create_task_group() as task_group:

            async def send_forever() -> None:
                while True:
                    await send({"type": "http.response.body", "body": b"chunk", "more_body": True})

            task_group.start_soon(send_forever)
            while (await receive())["type"] == "http.request":
                pass
            disconnected.set()
            task_group.cancel_scope.cancel()

    async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app)) as client:
        with anyio.fail_after(1):
            async with client.stream("GET", "http://www.example.org/") as response:
                async with contextlib.aclosing(response.aiter_bytes()) as parts:
                    assert await anext(parts) == b"chunk"
            await disconnected.wait()


@pytest.mark.anyio
async def test_asgi_closing_response_wakes_concurrent_reader() -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await anyio.sleep_forever()

    read_done = anyio.Event()

    async def read(response: httpx2.Response) -> None:
        await response.aread()
        read_done.set()

    async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app)) as client:
        async with client.stream("GET", "http://www.example.org/") as response:
            async with anyio.create_task_group() as task_group:
                task_group.start_soon(read, response)
                await anyio.wait_all_tasks_blocked()
                assert not read_done.is_set()
                await response.aclose()
                with anyio.fail_after(1):
                    await read_done.wait()


@pytest.mark.anyio
async def test_asgi_running_app_is_cancelled_on_exit() -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await anyio.sleep_forever()

    with anyio.fail_after(1):
        async with httpx2.ASGITransport(app=app) as transport:
            response = await transport.handle_async_request(httpx2.Request("GET", "http://www.example.org/"))
            assert response.status_code == 200
            await response.aclose()


@pytest.mark.anyio
async def test_asgi_exception_in_client_context_is_not_wrapped() -> None:
    with pytest.raises(KeyError):
        async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=hello_world)):
            raise KeyError()


@pytest.mark.anyio
async def test_asgi_mounted_transports() -> None:
    mounts = {"http://mounted": httpx2.ASGITransport(app=echo_path)}
    async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=hello_world), mounts=mounts) as client:
        assert (await client.get("http://www.example.org/")).text == "Hello, World!"
        assert (await client.get("http://mounted/path")).json() == {"path": "/path"}


@pytest.mark.anyio
async def test_asgi_transport_mounted_more_than_once() -> None:
    transport = httpx2.ASGITransport(app=echo_path)
    mounts = {"http://a": transport, "http://b": transport}
    with anyio.CancelScope():
        async with httpx2.AsyncClient(transport=transport, mounts=mounts) as client:
            assert (await client.get("http://a/a")).json() == {"path": "/a"}
            assert (await client.get("http://b/b")).json() == {"path": "/b"}


@pytest.mark.anyio
async def test_asgi_transport_shared_by_nested_clients() -> None:
    transport = httpx2.ASGITransport(app=hello_world)
    with anyio.CancelScope():
        async with httpx2.AsyncClient(transport=transport) as outer:
            async with httpx2.AsyncClient(transport=transport) as inner:
                assert (await inner.get("http://www.example.org/")).text == "Hello, World!"
            async with outer.stream("GET", "http://www.example.org/") as response:
                assert await response.aread() == b"Hello, World!"


@pytest.mark.parametrize("raise_app_exceptions", [True, False])
@pytest.mark.anyio
async def test_asgi_incomplete_response_raises_regardless_of_raise_app_exceptions(raise_app_exceptions: bool) -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})

    transport = httpx2.ASGITransport(app=app, raise_app_exceptions=raise_app_exceptions)
    async with httpx2.AsyncClient(transport=transport) as client:
        with pytest.raises(RuntimeError, match="without completing the response"):
            await client.get("http://www.example.org/")


@pytest.mark.anyio
async def test_asgi_body_before_response_start() -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({"type": "http.response.body", "body": b"too early"})

    async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app)) as client:
        with anyio.fail_after(1), pytest.raises(RuntimeError, match="before starting the response"):
            await client.get("http://www.example.org/")


@pytest.mark.anyio
async def test_asgi_no_response() -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        pass

    async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app)) as client:
        with pytest.raises(RuntimeError, match="without completing the response"):
            await client.get("http://www.example.org/")


@pytest.mark.anyio
async def test_asgi_incomplete_response() -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"partial", "more_body": True})

    async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app)) as client:
        with pytest.raises(RuntimeError, match="without completing the response"):
            await client.get("http://www.example.org/")


@pytest.mark.anyio
async def test_asgi_receive_disconnect_after_response_before_request_body() -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({"type": "http.response.start", "status": 413, "headers": []})
        await send({"type": "http.response.body", "body": b""})
        assert (await receive())["type"] == "http.disconnect"

    async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app)) as client:
        response = await client.post("http://www.example.org/", content=b"unread")

    assert response.status_code == 413


@pytest.mark.anyio
async def test_asgi_response_closed_while_reading_request_body() -> None:
    upload_paused = anyio.Event()
    resume_upload = anyio.Event()
    message_after_close: Message = {}
    app_done = anyio.Event()

    async def upload() -> typing.AsyncIterator[bytes]:
        yield b"first"
        upload_paused.set()
        await resume_upload.wait()

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        nonlocal message_after_close
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        message_after_close = await receive()
        app_done.set()

    async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app)) as client:
        with anyio.fail_after(1):
            async with client.stream("POST", "http://www.example.org/", content=upload()):
                await upload_paused.wait()
            resume_upload.set()
            await app_done.wait()

    assert message_after_close == {"type": "http.disconnect"}


@pytest.mark.anyio
async def test_asgi_cancelled_before_response_start_disconnects_app() -> None:
    start_response = anyio.Event()
    app_done = anyio.Event()

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await start_response.wait()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"late"})
        app_done.set()

    async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app)) as client:
        with anyio.move_on_after(0.01):
            await client.get("http://www.example.org/")
        start_response.set()
        with anyio.fail_after(1):
            await app_done.wait()
