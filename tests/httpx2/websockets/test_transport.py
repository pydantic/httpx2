from typing import Any, Dict

import httpx
import pytest
import wsproto
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route, WebSocketRoute
from starlette.websockets import WebSocket

from httpx_ws import WebSocketDisconnect
from httpx_ws.transport import (
    ASGIWebSocketAsyncNetworkStream,
    ASGIWebSocketTransport,
    UnhandledASGIMessageType,
    UnhandledWebSocketEvent,
)


@pytest.mark.asyncio
class TestASGIWebSocketAsyncNetworkStream:
    async def test_write(self):
        received_messages = []

        async def app(scope, receive, send):
            await send({"type": "websocket.accept"})
            message = await receive()
            received_messages.append(message)
            while message["type"] != "websocket.close":
                message = await receive()
                received_messages.append(message)

        connection = wsproto.connection.Connection(wsproto.connection.CLIENT)
        async with ASGIWebSocketAsyncNetworkStream(app, {}) as stream:
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

    async def test_write_unhandled_event(self):
        async def app(scope, receive, send):
            await send({"type": "websocket.accept"})
            await receive()

        connection = wsproto.connection.Connection(wsproto.connection.CLIENT)
        async with ASGIWebSocketAsyncNetworkStream(app, {}) as stream:
            with pytest.raises(UnhandledWebSocketEvent):
                ping_event = wsproto.events.Ping(b"PING")
                await stream.write(connection.send(ping_event))

    async def test_read(self):
        async def app(scope, receive, send):
            await send({"type": "websocket.accept"})
            await send({"type": "websocket.send", "text": "SERVER_MESSAGE"})
            await send({"type": "websocket.send", "bytes": b"SERVER_MESSAGE"})
            await send({"type": "websocket.close", "code": 1000, "reason": ""})

        connection = wsproto.connection.Connection(wsproto.connection.CLIENT)
        events = []
        async with ASGIWebSocketAsyncNetworkStream(app, {}) as stream:
            for _ in range(3):
                data = await stream.read(4096)
                connection.receive_data(data)

        events = list(connection.events())
        assert events == [
            wsproto.events.TextMessage("SERVER_MESSAGE"),
            wsproto.events.BytesMessage(bytearray(b"SERVER_MESSAGE")),
            wsproto.events.CloseConnection(1000, ""),
        ]

    async def test_read_unhandled_asgi_message(self):
        async def app(scope, receive, send):
            await send({"type": "websocket.accept"})
            await send({"type": "websocket.foo"})

        async with ASGIWebSocketAsyncNetworkStream(app, {}) as stream:
            with pytest.raises(UnhandledASGIMessageType):
                await stream.read(4096)

    async def test_close_immediately(self):
        async def app(scope, receive, send):
            await send({"type": "websocket.close", "code": 1000, "reason": ""})

        with pytest.raises(WebSocketDisconnect):
            async with ASGIWebSocketAsyncNetworkStream(app, {}):
                pass


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


@pytest.mark.asyncio
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
        self, url: str, headers: Dict[str, Any], test_app: Starlette
    ):
        async with ASGIWebSocketTransport(app=test_app) as transport:
            request = httpx.Request("GET", url, headers=headers)
            response = await transport.handle_async_request(request)
            assert response.status_code == 101

            assert isinstance(
                response.extensions["network_stream"], ASGIWebSocketAsyncNetworkStream
            )
