from typing import Callable
from unittest.mock import MagicMock

import httpx
import pytest
from starlette.types import ASGIApp
from starlette.websockets import WebSocket

from httpx_ws import WebSocketDisconnect, aconnect_ws
from httpx_ws.transport import ASGIWebSocketTransport


@pytest.mark.asyncio
async def test_send_message(
    websocket_app_factory: Callable[[Callable], ASGIApp], on_receive_message: MagicMock
):
    async def websocket_endpoint(websocket: WebSocket):
        await websocket.accept()

        message = await websocket.receive_text()
        on_receive_message(message)

        await websocket.close()

    app = websocket_app_factory(websocket_endpoint)

    async with httpx.AsyncClient(
        base_url="http://localhost:8000", transport=ASGIWebSocketTransport(app)
    ) as client:
        async with aconnect_ws(client, "/ws") as ws:
            await ws.send("CLIENT_MESSAGE")

    on_receive_message.assert_called_once_with("CLIENT_MESSAGE")


@pytest.mark.asyncio
async def test_receive_message(websocket_app_factory: Callable[[Callable], ASGIApp]):
    async def websocket_endpoint(websocket: WebSocket):
        await websocket.accept()

        await websocket.send_text("SERVER_MESSAGE")

        await websocket.close()

    app = websocket_app_factory(websocket_endpoint)

    async with httpx.AsyncClient(
        base_url="http://localhost:8000", transport=ASGIWebSocketTransport(app)
    ) as client:
        async with aconnect_ws(client, "/ws") as ws:
            message = await ws.receive()
            assert message == "SERVER_MESSAGE"


@pytest.mark.asyncio
async def test_send_close(websocket_app_factory: Callable[[Callable], ASGIApp]):
    async def websocket_endpoint(websocket: WebSocket):
        await websocket.accept()
        await websocket.receive_text()

    app = websocket_app_factory(websocket_endpoint)

    async with httpx.AsyncClient(
        base_url="http://localhost:8000", transport=ASGIWebSocketTransport(app)
    ) as client:
        async with aconnect_ws(client, "/ws"):
            pass


@pytest.mark.asyncio
async def test_receive_close(websocket_app_factory: Callable[[Callable], ASGIApp]):
    async def websocket_endpoint(websocket: WebSocket):
        await websocket.accept()
        await websocket.close()

    app = websocket_app_factory(websocket_endpoint)

    async with httpx.AsyncClient(
        base_url="http://localhost:8000", transport=ASGIWebSocketTransport(app)
    ) as client:
        with pytest.raises(WebSocketDisconnect):
            async with aconnect_ws(client, "/ws") as ws:
                await ws.receive()
