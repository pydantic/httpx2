from __future__ import annotations

import base64
import secrets
import sys
from typing import Any

import anyio
import pytest
import wsproto
from anyio import CancelScope, ClosedResourceError, create_task_group
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse
from starlette.routing import Route, WebSocketRoute
from starlette.websockets import WebSocket

import httpx2 as httpx
from httpx2.websockets._api import aconnect_ws
from httpx2.websockets._exceptions import WebSocketDisconnect, WebSocketUpgradeError
from httpx2.websockets._transport import (
    ASGIWebSocketAsyncNetworkStream,
    ASGIWebSocketTransport,
    Receive,
    Scope,
    Send,
    UnhandledASGIMessageType,
    UnhandledWebSocketEvent,
)

if sys.version_info < (3, 11):
    from exceptiongroup import ExceptionGroup  # pragma: no cover


@pytest.fixture
def websocket_request_headers() -> dict[str, str]:
    return {
        "connection": "upgrade",
        "upgrade": "websocket",
        "sec-websocket-key": base64.b64encode(secrets.token_bytes(16)).decode("utf-8"),
        "sec-websocket-version": "13",
    }


@pytest.fixture
def scope(websocket_request_headers: dict[str, str]) -> Scope:
    return {
        "type": "websocket",
        "path": "/ws",
        "raw_path": "/ws",
        "root_path": "/",
        "scheme": "ws",
        "headers": [
            ("host", "localhost"),
            *websocket_request_headers.items(),
        ],
        "subprotocols": [],
        "server": ("localhost", 8000),
    }


@pytest.mark.anyio
class TestASGIWebSocketAsyncNetworkStream:
    async def test_write(self, scope: Scope) -> None:
        received_messages = []

        async def app(scope: Scope, receive: Receive, send: Send) -> None:
            await send({"type": "websocket.accept"})
            message = await receive()
            received_messages.append(message)
            while message["type"] != "websocket.disconnect":
                message = await receive()
                received_messages.append(message)

        connection = wsproto.connection.Connection(wsproto.connection.CLIENT)
        async with (
            create_task_group() as tg,
            ASGIWebSocketAsyncNetworkStream(app, scope, tg) as (stream, _),
        ):
            text_event = wsproto.events.TextMessage("CLIENT_MESSAGE")
            await stream.write(connection.send(text_event))

            bytes_event = wsproto.events.BytesMessage(b"CLIENT_MESSAGE")
            await stream.write(connection.send(bytes_event))

            close_event = wsproto.events.CloseConnection(1000)
            await stream.write(connection.send(close_event))

            # Add a small delay to ensure the app has processed all messages
            await anyio.sleep(0.1)

        assert received_messages == [
            {"type": "websocket.connect"},
            {"type": "websocket.receive", "text": "CLIENT_MESSAGE"},
            {"type": "websocket.receive", "bytes": b"CLIENT_MESSAGE"},
            {"type": "websocket.disconnect", "code": 1000, "reason": ""},
        ]

    async def test_write_unhandled_event(self, scope: Scope) -> None:
        async def app(scope: Scope, receive: Receive, send: Send) -> None:
            await send({"type": "websocket.accept"})
            await receive()

        connection = wsproto.connection.Connection(wsproto.connection.CLIENT)
        async with (
            create_task_group() as tg,
            ASGIWebSocketAsyncNetworkStream(app, scope, tg) as (stream, _),
        ):
            with pytest.raises(UnhandledWebSocketEvent):
                ping_event = wsproto.events.Ping(b"PING")
                await stream.write(connection.send(ping_event))

    async def test_read(self, scope: Scope) -> None:
        async def app(scope: Scope, receive: Receive, send: Send) -> None:
            await send({"type": "websocket.accept"})
            await send({"type": "websocket.send", "text": "SERVER_MESSAGE"})
            await send({"type": "websocket.send", "bytes": b"SERVER_MESSAGE"})
            await send({"type": "websocket.close", "code": 1000, "reason": ""})

        connection = wsproto.connection.Connection(wsproto.connection.CLIENT)
        events = []
        async with (
            create_task_group() as tg,
            ASGIWebSocketAsyncNetworkStream(app, scope, tg) as (stream, _),
        ):
            for _ in range(3):
                data = await stream.read(4096)
                connection.receive_data(data)

        events = list(connection.events())
        assert events == [
            wsproto.events.TextMessage("SERVER_MESSAGE"),
            wsproto.events.BytesMessage(bytearray(b"SERVER_MESSAGE")),
            wsproto.events.CloseConnection(1000, ""),
        ]

    async def test_read_unhandled_asgi_message(self, scope: Scope) -> None:
        async def app(scope: Scope, receive: Receive, send: Send) -> None:
            await send({"type": "websocket.accept"})
            await send({"type": "websocket.foo"})

        async with (
            create_task_group() as tg,
            ASGIWebSocketAsyncNetworkStream(app, scope, tg) as (stream, _),
        ):
            with pytest.raises(UnhandledASGIMessageType):
                await stream.read(4096)

    async def test_enter_twice(self, scope: Scope) -> None:
        async def app(scope: Scope, receive: Receive, send: Send) -> None:
            await send({"type": "websocket.accept"})
            await receive()

        async with (
            create_task_group() as tg,
            ASGIWebSocketAsyncNetworkStream(app, scope, tg) as (stream, _),
        ):
            with pytest.raises(RuntimeError, match="context manager twice"):
                async with stream:
                    pass  # pragma: no cover

    async def test_close_immediately(self, scope: Scope) -> None:
        async def app(scope: Scope, receive: Receive, send: Send) -> None:
            await send({"type": "websocket.close", "code": 1000, "reason": ""})

        with pytest.raises(ExceptionGroup) as excinfo:
            async with (
                create_task_group() as tg,
                ASGIWebSocketAsyncNetworkStream(app, scope, tg),
            ):
                pass  # pragma: no cover
        assert excinfo.group_contains(WebSocketDisconnect)

    async def test_denial_response(self, scope: Scope) -> None:
        async def app(scope: Scope, receive: Receive, send: Send) -> None:
            await send({"type": "websocket.http.response.start", "status": 401, "headers": []})
            await send({"type": "websocket.http.response.body", "body": b"Unauthorized"})

        with pytest.raises(ExceptionGroup) as excinfo:
            async with (
                create_task_group() as tg,
                ASGIWebSocketAsyncNetworkStream(app, scope, tg),
            ):
                pass  # pragma: no cover
        assert excinfo.group_contains(WebSocketUpgradeError)
        error = excinfo.value.exceptions[0]
        assert isinstance(error, WebSocketUpgradeError)
        assert error.response.status_code == 401
        assert error.response.content == b"Unauthorized"

    async def test_exception(self, scope: Scope) -> None:
        async def app(scope: Scope, receive: Receive, send: Send) -> None:
            raise Exception("Error")

        with pytest.raises(ExceptionGroup) as excinfo:
            async with (
                create_task_group() as tg,
                ASGIWebSocketAsyncNetworkStream(app, scope, tg),
            ):
                pass  # pragma: no cover
        assert excinfo.group_contains(WebSocketDisconnect)
        error = excinfo.value.exceptions[0]
        assert isinstance(error, WebSocketDisconnect)
        assert error.code == 1011
        assert error.reason == "Error"

    async def test_never_accepts(self, scope: Scope) -> None:
        async def app(scope: Scope, receive: Receive, send: Send) -> None:
            return

        with pytest.raises(ExceptionGroup) as excinfo:
            async with (
                create_task_group() as tg,
                ASGIWebSocketAsyncNetworkStream(app, scope, tg),
            ):
                pass  # pragma: no cover

        assert excinfo.group_contains(RuntimeError)

    async def test_app_exception_with_closed_send_queue(self, scope: Scope) -> None:
        async def app(scope: Scope, receive: Receive, send: Send) -> None:
            await send({"type": "websocket.accept"})
            await receive()
            raise Exception("App error")

        async with (
            create_task_group() as tg,
            ASGIWebSocketAsyncNetworkStream(app, scope, tg) as (stream, _),
        ):
            await stream._send_queue.aclose()
            await stream.send({"type": "websocket.receive", "text": "trigger"})


@pytest.fixture
def test_app() -> Starlette:
    async def http_endpoint(request: Request) -> PlainTextResponse:
        return PlainTextResponse("Hello, world!")

    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.receive_text()
        await websocket.close()  # pragma: no cover

    routes = [
        Route("/http", endpoint=http_endpoint),
        WebSocketRoute("/ws", endpoint=websocket_endpoint),
    ]

    return Starlette(routes=routes)


@pytest.mark.anyio
class TestASGIWebSocketTransport:
    async def test_http(self, test_app: Starlette) -> None:
        async with ASGIWebSocketTransport(app=test_app) as transport:
            request = httpx.Request("GET", "http://localhost:8000/http")
            response = await transport.handle_async_request(request)
            assert response.status_code == 200

    @pytest.mark.parametrize(
        "url,headers",
        [
            ("ws://localhost:8000/ws", {}),
            ("wss://localhost:8000/ws", {}),
            ("http://localhost:8000/ws", {"upgrade": "websocket"}),
        ],
    )
    async def test_websocket(
        self,
        url: str,
        headers: dict[str, Any],
        test_app: Starlette,
        websocket_request_headers: dict[str, str],
    ) -> None:
        async with ASGIWebSocketTransport(app=test_app) as transport:
            request = httpx.Request("GET", url, headers={**websocket_request_headers, **headers})
            response = await transport.handle_async_request(request)
            assert response.status_code == 101

            assert isinstance(response.extensions["network_stream"], ASGIWebSocketAsyncNetworkStream)

    @pytest.mark.parametrize("stream_count", [1, 3])
    async def test_transport_exit_closes_stream_queues(
        self,
        stream_count: int,
        test_app: Starlette,
        websocket_request_headers: dict[str, str],
    ) -> None:
        async with ASGIWebSocketTransport(app=test_app) as transport:
            streams = []
            for _ in range(stream_count):
                request = httpx.Request(
                    "GET",
                    "ws://localhost:8000/ws",
                    headers=websocket_request_headers,
                )
                response = await transport.handle_async_request(request)
                streams.append(response.extensions["network_stream"])

        for stream in streams:
            with pytest.raises(ClosedResourceError):
                await stream._receive_queue.send({})
            with pytest.raises(ClosedResourceError):
                await stream._send_queue.send({})

    async def test_aclose_after_transport_exit_does_not_raise(
        self,
        test_app: Starlette,
        websocket_request_headers: dict[str, str],
    ) -> None:
        async with ASGIWebSocketTransport(app=test_app) as transport:
            request = httpx.Request("GET", "ws://localhost:8000/ws", headers=websocket_request_headers)
            response = await transport.handle_async_request(request)
            stream = response.extensions["network_stream"]

        await stream.aclose()


@pytest.mark.anyio
async def test_subprotocol_support() -> None:
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept("custom_protocol")
        assert websocket.scope.get("subprotocols") == ["custom_protocol"]
        await websocket.send_text("SERVER_MESSAGE")
        await websocket.close()

    app = Starlette(
        routes=[
            WebSocketRoute("/ws", endpoint=websocket_endpoint),
        ]
    )

    async with httpx.AsyncClient(transport=ASGIWebSocketTransport(app)) as client:
        async with aconnect_ws("ws://localhost:8000/ws", client, subprotocols=["custom_protocol"]) as ws:
            await ws.receive_text()
            assert ws.subprotocol == "custom_protocol"


@pytest.mark.anyio
async def test_keepalive_ping_disabled() -> None:
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.receive_text()
        await websocket.close()  # pragma: no cover

    app = Starlette(
        routes=[
            WebSocketRoute("/ws", endpoint=websocket_endpoint),
        ]
    )

    async with httpx.AsyncClient(transport=ASGIWebSocketTransport(app)) as client:
        async with aconnect_ws("ws://localhost:8000/ws", client) as ws:
            assert ws._keepalive_ping_interval_seconds is None


@pytest.mark.anyio
async def test_cancel_scope_integrity() -> None:
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.receive_text()
        await websocket.close()  # pragma: no cover

    app = Starlette(
        routes=[
            WebSocketRoute("/ws", endpoint=websocket_endpoint),
        ]
    )

    async with httpx.AsyncClient(transport=ASGIWebSocketTransport(app)) as client:
        with CancelScope():
            async with aconnect_ws("ws://localhost:8000/ws", client):
                pass


@pytest.mark.anyio
async def test_receive() -> None:
    messages: list[str] = []

    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        messages.append(await websocket.receive_text())
        await websocket.close()

    app = Starlette(
        routes=[
            WebSocketRoute("/ws", endpoint=websocket_endpoint),
        ]
    )

    async with httpx.AsyncClient(transport=ASGIWebSocketTransport(app)) as client:
        async with aconnect_ws("ws://localhost:8000/ws", client) as ws:
            await ws.send_text("RESULT")

    assert len(messages) == 1
    assert messages[0] == "RESULT"
