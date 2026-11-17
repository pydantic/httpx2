from __future__ import annotations

import subprocess
import sys
import threading
import typing
from unittest.mock import patch

import anyio
import pytest
import wsproto
from starlette.websockets import WebSocket

import httpx2 as httpx
from httpcore2 import AsyncNetworkStream, NetworkStream
from httpx2.websockets import AsyncWebSocketSession, WebSocketSession, _api
from tests.httpx2.websockets.conftest import ServerFactoryFixture

FRAGMENTED_MESSAGES = [
    pytest.param(
        [
            wsproto.events.TextMessage("FRAG", message_finished=False),
            wsproto.events.TextMessage("", message_finished=False),
            wsproto.events.TextMessage("MEN", message_finished=False),
            wsproto.events.TextMessage("TED", message_finished=True),
        ],
        "FRAGMENTED",
        id="text",
    ),
    pytest.param(
        [
            wsproto.events.BytesMessage(b"FRAG", message_finished=False),
            wsproto.events.BytesMessage(b"", message_finished=False),
            wsproto.events.BytesMessage(b"MEN", message_finished=False),
            wsproto.events.BytesMessage(b"TED", message_finished=True),
        ],
        b"FRAGMENTED",
        id="binary",
    ),
]


def test_importing_httpx2_does_not_import_wsproto() -> None:
    code = "import sys; import httpx2; assert 'wsproto' not in sys.modules"
    assert subprocess.run([sys.executable, "-c", code], capture_output=True).returncode == 0


def test_top_level_websocket_uses_a_dedicated_client() -> None:
    with patch.object(_api, "WebSocketClient") as mock_client:
        with httpx.websocket("http://socket/ws"):
            pass
    mock_client.return_value.connect.assert_called_once()
    client = mock_client.call_args[1]["client"]
    assert isinstance(client, httpx.Client)
    assert client.is_closed


@pytest.mark.anyio
async def test_client_websocket(server_factory: ServerFactoryFixture) -> None:
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.send_text("SERVER_MESSAGE")
        await websocket.close()

    with server_factory(websocket_endpoint) as socket:
        with httpx.Client(transport=httpx.HTTPTransport(uds=socket)) as client:
            with client.websocket("http://socket/ws") as ws:
                assert ws.receive_text() == "SERVER_MESSAGE"

        async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(uds=socket)) as aclient:
            async with aclient.websocket("http://socket/ws") as aws:
                assert await aws.receive_text() == "SERVER_MESSAGE"


@pytest.mark.anyio
async def test_client_websocket_forwards_request_params(server_factory: ServerFactoryFixture) -> None:
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.send_text(websocket.headers.get("x-token", ""))
        await websocket.close()

    with server_factory(websocket_endpoint) as socket:
        with httpx.Client(transport=httpx.HTTPTransport(uds=socket)) as client:
            with client.websocket("http://socket/ws", headers={"x-token": "secret"}) as ws:
                assert ws.receive_text() == "secret"

        async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(uds=socket)) as aclient:
            async with aclient.websocket("http://socket/ws", headers={"x-token": "secret"}) as aws:
                assert await aws.receive_text() == "secret"


@pytest.mark.parametrize(("events", "expected"), FRAGMENTED_MESSAGES)
def test_receive_reassembles_fragmented_message(
    events: list[wsproto.events.Message[typing.Any]], expected: str | bytes
) -> None:
    server = wsproto.connection.Connection(wsproto.connection.ConnectionType.SERVER)
    fragments = b"".join(server.send(event) for event in events)

    class MockNetworkStream(NetworkStream):
        def __init__(self) -> None:
            self._sent = False
            self._closed = threading.Event()

        def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
            if self._sent:
                self._closed.wait()
                return b""
            self._sent = True
            return fragments

        def write(self, buffer: bytes, timeout: float | None = None) -> None: ...

        def close(self) -> None:
            self._closed.set()

    with WebSocketSession(MockNetworkStream()) as ws:
        event = ws.receive()
        assert isinstance(event, wsproto.events.Message)
        assert event.data == expected


@pytest.mark.anyio
@pytest.mark.parametrize(("events", "expected"), FRAGMENTED_MESSAGES)
async def test_async_receive_reassembles_fragmented_message(
    events: list[wsproto.events.Message[typing.Any]], expected: str | bytes
) -> None:
    server = wsproto.connection.Connection(wsproto.connection.ConnectionType.SERVER)
    fragments = b"".join(server.send(event) for event in events)

    class AsyncMockNetworkStream(AsyncNetworkStream):
        def __init__(self) -> None:
            self._sent = False

        async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
            if self._sent:
                await anyio.sleep_forever()
            self._sent = True
            return fragments

        async def write(self, buffer: bytes, timeout: float | None = None) -> None: ...

        async def aclose(self) -> None: ...

    async with AsyncWebSocketSession(AsyncMockNetworkStream()) as ws:
        event = await ws.receive()
        assert isinstance(event, wsproto.events.Message)
        assert event.data == expected
