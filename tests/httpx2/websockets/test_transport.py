import base64
import secrets
from typing import Any, Dict

import httpx
import pytest
import wsproto
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route, WebSocketRoute
from starlette.websockets import WebSocket

from httpx_ws import WebSocketDisconnect, aconnect_ws
from httpx_ws.transport import (
    ASGIWebSocketAsyncNetworkStream,
    ASGIWebSocketTransport,
    Scope,
    UnhandledASGIMessageType,
    UnhandledWebSocketEvent,
)


@pytest.fixture
def websocket_request_headers() -> Dict[str, str]:
    return {
        "connection": "upgrade",
        "upgrade": "websocket",
        "sec-websocket-key": base64.b64encode(secrets.token_bytes(16)).decode("utf-8"),
        "sec-websocket-version": "13",
    }


@pytest.fixture
def scope(websocket_request_headers: Dict[str, str]) -> Scope:
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
    async def test_write(self, scope: Scope):
        received_messages = []

        async def app(scope, receive, send):
            await send({"type": "websocket.accept"})
            message = await receive()
            received_messages.append(message)
            while message["type"] != "websocket.close":
                message = await receive()
                received_messages.append(message)

        connection = wsproto.connection.Connection(wsproto.connection.CLIENT)
        async with ASGIWebSocketAsyncNetworkStream(app, scope) as (stream, _):
            text_event = wsproto.events.TextMessage("CLIENT_MESSAGE")
            await stream.write(connection.send(text_event))

            bytes_event = wsproto.events.BytesMessage(b"CLIENT_MESSAGE")
            await stream.write(connection.send(bytes_event))

            close_event = wsproto.events.CloseConnection(1000)
            await stream.write(connection.send(close_event))

        assert received_messages == [
            {"type": "websocket.connect"},
            {"type": "websocket.receive", "text": "CLIENT_MESSAGE"},
            {"type": "websocket.receive", "bytes": b"CLIENT_MESSAGE"},
            {"type": "websocket.close", "code": 1000, "reason": ""},
        ]

    async def test_write_unhandled_event(self, scope: Scope):
        async def app(scope, receive, send):
            await send({"type": "websocket.accept"})
            await receive()

        connection = wsproto.connection.Connection(wsproto.connection.CLIENT)
        async with ASGIWebSocketAsyncNetworkStream(app, scope) as (stream, _):
            with pytest.raises(UnhandledWebSocketEvent):
                ping_event = wsproto.events.Ping(b"PING")
                await stream.write(connection.send(ping_event))

    async def test_read(self, scope):
        async def app(scope, receive, send):
            await send({"type": "websocket.accept"})
            await send({"type": "websocket.send", "text": "SERVER_MESSAGE"})
            await send({"type": "websocket.send", "bytes": b"SERVER_MESSAGE"})
            await send({"type": "websocket.close", "code": 1000, "reason": ""})

        connection = wsproto.connection.Connection(wsproto.connection.CLIENT)
        events = []
        async with ASGIWebSocketAsyncNetworkStream(app, scope) as (stream, _):
            for _ in range(3):
                data = await stream.read(4096)
                connection.receive_data(data)

        events = list(connection.events())
        assert events == [
            wsproto.events.TextMessage("SERVER_MESSAGE"),
            wsproto.events.BytesMessage(bytearray(b"SERVER_MESSAGE")),
            wsproto.events.CloseConnection(1000, ""),
        ]

    async def test_read_unhandled_asgi_message(self, scope):
        async def app(scope, receive, send):
            await send({"type": "websocket.accept"})
            await send({"type": "websocket.foo"})

        async with ASGIWebSocketAsyncNetworkStream(app, scope) as (stream, _):
            with pytest.raises(UnhandledASGIMessageType):
                await stream.read(4096)

    async def test_close_immediately(self, scope):
        async def app(scope, receive, send):
            await send({"type": "websocket.close", "code": 1000, "reason": ""})

        with pytest.raises(WebSocketDisconnect):
            async with ASGIWebSocketAsyncNetworkStream(app, scope):
                pass

    async def test_exception(self, scope):
        async def app(scope, receive, send):
            raise Exception("Error")

        with pytest.raises(WebSocketDisconnect) as excinfo:
            async with ASGIWebSocketAsyncNetworkStream(app, scope):
                pass
        assert excinfo.value.code == 1011
        assert excinfo.value.reason == "Error"


@pytest.fixture
def test_app() -> Starlette:
    async def http_endpoint(request):
        return PlainTextResponse("Hello, world!")

    async def websocket_endpoint(websocket: WebSocket):
        await websocket.accept()
        await websocket.receive_text()
        await websocket.close()

    routes = [
        Route("/http", endpoint=http_endpoint),
        WebSocketRoute("/ws", endpoint=websocket_endpoint),
    ]

    return Starlette(routes=routes)


@pytest.mark.anyio
class TestASGIWebSocketTransport:
    async def test_http(self, test_app: Starlette):
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
        headers: Dict[str, Any],
        test_app: Starlette,
        websocket_request_headers: Dict[str, str],
    ):
        async with ASGIWebSocketTransport(app=test_app) as transport:
            request = httpx.Request(
                "GET", url, headers={**websocket_request_headers, **headers}
            )
            response = await transport.handle_async_request(request)
            assert response.status_code == 101

            assert isinstance(
                response.extensions["network_stream"], ASGIWebSocketAsyncNetworkStream
            )


@pytest.mark.anyio
async def test_subprotocol_support():
    async def websocket_endpoint(websocket: WebSocket):
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
        async with aconnect_ws(
            "ws://localhost:8000/ws", client, subprotocols=["custom_protocol"]
        ) as ws:
            await ws.receive_text()
            assert ws.subprotocol == "custom_protocol"


@pytest.mark.anyio
async def test_keepalive_ping_disabled():
    async def websocket_endpoint(websocket: WebSocket):
        await websocket.accept()
        await websocket.receive_text()
        await websocket.close()

    app = Starlette(
        routes=[
            WebSocketRoute("/ws", endpoint=websocket_endpoint),
        ]
    )

    async with httpx.AsyncClient(transport=ASGIWebSocketTransport(app)) as client:
        async with aconnect_ws("ws://localhost:8000/ws", client) as ws:
            assert ws._keepalive_ping_interval_seconds is None
