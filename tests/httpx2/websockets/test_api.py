from __future__ import annotations

import concurrent.futures
import queue
import threading
import time
from unittest.mock import MagicMock, call, patch

import anyio
import pytest
import wsproto
from starlette.websockets import WebSocket, WebSocketDisconnect as StarletteWebSocketDisconnect

import httpcore2 as httpcore
import httpx2 as httpx
from httpcore2 import AsyncNetworkStream, NetworkStream
from httpx2.websockets import _api
from httpx2.websockets._api import (
    AsyncWebSocketClient,
    AsyncWebSocketSession,
    JSONMode,
    WebSocketClient,
    WebSocketSession,
    aconnect_ws,
    connect_ws,
)
from httpx2.websockets._exceptions import (
    WebSocketDisconnect,
    WebSocketInvalidTypeReceived,
    WebSocketNetworkError,
    WebSocketUpgradeError,
)
from tests.httpx2.websockets.conftest import ServerFactoryFixture


@pytest.mark.anyio
async def test_upgrade_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400)

    with httpx.Client(base_url="http://localhost:8000", transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(WebSocketUpgradeError):
            with connect_ws("http://socket/ws", client):
                pass  # pragma: no cover

    async with httpx.AsyncClient(base_url="http://localhost:8000", transport=httpx.MockTransport(handler)) as aclient:
        with pytest.raises(WebSocketUpgradeError):
            async with aconnect_ws("http://socket/ws", aclient):
                pass  # pragma: no cover


@pytest.mark.anyio
class TestSend:
    async def test_send_error(self) -> None:
        class MockNetworkStream(NetworkStream):
            def __init__(self) -> None:
                self.connection = wsproto.connection.Connection(wsproto.connection.ConnectionType.SERVER)
                self._should_close = False

            def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
                while not self._should_close:  # pragma: no cover
                    time.sleep(0.1)
                raise httpcore.ReadError()  # pragma: no cover

            def write(self, buffer: bytes, timeout: float | None = None) -> None:
                raise httpcore.WriteError()

            def close(self) -> None:
                self._should_close = True

        stream = MockNetworkStream()
        with pytest.raises(WebSocketNetworkError):
            with WebSocketSession(stream) as websocket_session:
                websocket_session.send(wsproto.events.Ping())

    async def test_async_send_error(self) -> None:
        class AsyncMockNetworkStream(AsyncNetworkStream):
            def __init__(self) -> None:
                self.connection = wsproto.connection.Connection(wsproto.connection.ConnectionType.SERVER)
                self._should_close = False

            async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
                while not self._should_close:  # pragma: no cover
                    await anyio.sleep(0.1)
                raise httpcore.ReadError()  # pragma: no cover

            async def write(self, buffer: bytes, timeout: float | None = None) -> None:
                raise httpcore.WriteError()

            async def aclose(self) -> None:
                self._should_close = True

        stream = AsyncMockNetworkStream()
        with pytest.RaisesGroup(WebSocketNetworkError):
            async with AsyncWebSocketSession(stream) as websocket_session:
                await websocket_session.send(wsproto.events.Ping())

    async def test_send(
        self,
        server_factory: ServerFactoryFixture,
        on_receive_message: MagicMock,
    ) -> None:
        async def websocket_endpoint(websocket: WebSocket) -> None:
            await websocket.accept()

            message = await websocket.receive_text()
            on_receive_message(message)

            await websocket.close()

        with server_factory(websocket_endpoint) as socket:
            with httpx.Client(transport=httpx.HTTPTransport(uds=socket)) as client:
                with connect_ws("http://socket/ws", client) as ws:
                    ws.send(wsproto.events.TextMessage(data="CLIENT_MESSAGE"))

            async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(uds=socket)) as aclient:
                async with aconnect_ws("http://socket/ws", aclient) as aws:
                    await aws.send(wsproto.events.TextMessage(data="CLIENT_MESSAGE"))

        on_receive_message.assert_has_calls([call("CLIENT_MESSAGE"), call("CLIENT_MESSAGE")])

    async def test_send_text(
        self,
        server_factory: ServerFactoryFixture,
        on_receive_message: MagicMock,
    ) -> None:
        async def websocket_endpoint(websocket: WebSocket) -> None:
            await websocket.accept()

            message = await websocket.receive_text()
            on_receive_message(message)

            await websocket.close()

        with server_factory(websocket_endpoint) as socket:
            with httpx.Client(transport=httpx.HTTPTransport(uds=socket)) as client:
                with connect_ws("http://socket/ws", client) as ws:
                    ws.send_text("CLIENT_MESSAGE")

            async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(uds=socket)) as aclient:
                async with aconnect_ws("http://socket/ws", aclient) as aws:
                    await aws.send_text("CLIENT_MESSAGE")

        on_receive_message.assert_has_calls([call("CLIENT_MESSAGE"), call("CLIENT_MESSAGE")])

    async def test_send_bytes(
        self,
        server_factory: ServerFactoryFixture,
        on_receive_message: MagicMock,
    ) -> None:
        async def websocket_endpoint(websocket: WebSocket) -> None:
            await websocket.accept()

            message = await websocket.receive_bytes()
            on_receive_message(message)

            await websocket.close()

        with server_factory(websocket_endpoint) as socket:
            with httpx.Client(transport=httpx.HTTPTransport(uds=socket)) as client:
                with connect_ws("http://socket/ws", client) as ws:
                    ws.send_bytes(b"CLIENT_MESSAGE")

            async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(uds=socket)) as aclient:
                async with aconnect_ws("http://socket/ws", aclient) as aws:
                    await aws.send_bytes(b"CLIENT_MESSAGE")

        on_receive_message.assert_has_calls([call(b"CLIENT_MESSAGE"), call(b"CLIENT_MESSAGE")])

    @pytest.mark.parametrize("mode", ["text", "binary"])
    async def test_send_json(
        self,
        mode: JSONMode,
        server_factory: ServerFactoryFixture,
        on_receive_message: MagicMock,
    ) -> None:
        async def websocket_endpoint(websocket: WebSocket) -> None:
            await websocket.accept()

            message = await websocket.receive_json(mode=mode)
            on_receive_message(message)

            await websocket.close()

        with server_factory(websocket_endpoint) as socket:
            with httpx.Client(transport=httpx.HTTPTransport(uds=socket)) as client:
                with connect_ws("http://socket/ws", client) as ws:
                    ws.send_json({"message": "CLIENT_MESSAGE"}, mode=mode)

            async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(uds=socket)) as aclient:
                async with aconnect_ws("http://socket/ws", aclient) as aws:
                    await aws.send_json({"message": "CLIENT_MESSAGE"}, mode=mode)

        on_receive_message.assert_has_calls([call({"message": "CLIENT_MESSAGE"}), call({"message": "CLIENT_MESSAGE"})])


@pytest.mark.anyio
class TestReceive:
    async def test_receive_error(self) -> None:
        class MockNetworkStream(NetworkStream):
            def __init__(self) -> None:
                self.connection = wsproto.connection.Connection(wsproto.connection.ConnectionType.SERVER)

            def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
                raise httpcore.ReadError()

            def write(self, buffer: bytes, timeout: float | None = None) -> None:
                pass

            def close(self) -> None:
                pass

        stream = MockNetworkStream()
        with pytest.raises(WebSocketNetworkError):
            with WebSocketSession(stream) as websocket_session:
                websocket_session.receive()

    def test_receive_closed_socket(self) -> None:
        class MockNetworkStream(NetworkStream):
            def __init__(self) -> None:
                self.connection = wsproto.connection.Connection(wsproto.connection.ConnectionType.SERVER)

            def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
                return b""

            def write(self, buffer: bytes, timeout: float | None = None) -> None:
                pass

            def close(self) -> None:
                pass

        stream = MockNetworkStream()
        with pytest.raises(WebSocketNetworkError):
            with WebSocketSession(stream) as websocket_session:
                websocket_session.receive()

    def test_receive_timeout(self) -> None:
        class MockNetworkStream(NetworkStream):
            def __init__(self) -> None:
                self.connection = wsproto.connection.Connection(wsproto.connection.ConnectionType.SERVER)

            def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
                time.sleep(0.2)
                return b""

            def write(self, buffer: bytes, timeout: float | None = None) -> None:
                pass

            def close(self) -> None:
                pass

        stream = MockNetworkStream()
        with pytest.raises(TimeoutError):
            with WebSocketSession(stream) as websocket_session:
                websocket_session.receive(timeout=0.1)

    async def test_async_receive_error(self) -> None:
        class AsyncMockNetworkStream(AsyncNetworkStream):
            def __init__(self) -> None:
                self.connection = wsproto.connection.Connection(wsproto.connection.ConnectionType.SERVER)

            async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
                raise httpcore.ReadError()

            async def write(self, buffer: bytes, timeout: float | None = None) -> None:
                pass

            async def aclose(self) -> None:
                pass

        stream = AsyncMockNetworkStream()
        with pytest.RaisesGroup(WebSocketNetworkError):
            async with AsyncWebSocketSession(stream) as websocket_session:
                await websocket_session.receive()

    async def test_async_receive_closed_socket(self) -> None:
        class AsyncMockNetworkStream(AsyncNetworkStream):
            def __init__(self) -> None:
                self.connection = wsproto.connection.Connection(wsproto.connection.ConnectionType.SERVER)

            async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
                return b""

            async def write(self, buffer: bytes, timeout: float | None = None) -> None:
                pass

            async def aclose(self) -> None:
                pass

        stream = AsyncMockNetworkStream()
        with pytest.RaisesGroup(WebSocketNetworkError):
            async with AsyncWebSocketSession(stream) as websocket_session:
                await websocket_session.receive()

    async def test_receive(self, server_factory: ServerFactoryFixture) -> None:
        async def websocket_endpoint(websocket: WebSocket) -> None:
            await websocket.accept()

            await websocket.send_text("SERVER_MESSAGE")

            await websocket.close()

        with server_factory(websocket_endpoint) as socket:
            with httpx.Client(transport=httpx.HTTPTransport(uds=socket)) as client:
                with connect_ws("http://socket/ws", client) as ws:
                    event = ws.receive()
                    assert isinstance(event, wsproto.events.TextMessage)
                    assert event.data == "SERVER_MESSAGE"

            async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(uds=socket)) as aclient:
                async with aconnect_ws("http://socket/ws", aclient) as aws:
                    event = await aws.receive()
                    assert isinstance(event, wsproto.events.TextMessage)
                    assert event.data == "SERVER_MESSAGE"

    @pytest.mark.parametrize(
        "full_message,send_method",
        [
            pytest.param(b"A" * 1024 * 4, "send_bytes", id="bytes"),
            pytest.param("A" * 1024 * 4, "send_text", id="text"),
        ],
    )
    async def test_receive_oversized_message(
        self,
        full_message: str | bytes,
        send_method: str,
        server_factory: ServerFactoryFixture,
    ) -> None:
        async def websocket_endpoint(websocket: WebSocket) -> None:
            await websocket.accept()

            method = getattr(websocket, send_method)
            await method(full_message)

            await websocket.close()

        with server_factory(websocket_endpoint) as socket:
            with httpx.Client(transport=httpx.HTTPTransport(uds=socket)) as client:
                with connect_ws("http://socket/ws", client, max_message_size_bytes=1024) as ws:
                    event = ws.receive()
                    assert isinstance(event, wsproto.events.Message)
                    assert event.data == full_message

            async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(uds=socket)) as aclient:
                async with aconnect_ws("http://socket/ws", aclient, max_message_size_bytes=1024) as aws:
                    event = await aws.receive()
                    assert isinstance(event, wsproto.events.Message)
                    assert event.data == full_message

    async def test_receive_text(self, server_factory: ServerFactoryFixture) -> None:
        async def websocket_endpoint(websocket: WebSocket) -> None:
            await websocket.accept()

            await websocket.send_text("SERVER_MESSAGE")

            await websocket.close()

        with server_factory(websocket_endpoint) as socket:
            with httpx.Client(transport=httpx.HTTPTransport(uds=socket)) as client:
                with connect_ws("http://socket/ws", client) as ws:
                    data = ws.receive_text()
                    assert data == "SERVER_MESSAGE"

            async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(uds=socket)) as aclient:
                async with aconnect_ws("http://socket/ws", aclient) as aws:
                    data = await aws.receive_text()
                    assert data == "SERVER_MESSAGE"

    async def test_receive_text_invalid_type(self, server_factory: ServerFactoryFixture) -> None:
        async def websocket_endpoint(websocket: WebSocket) -> None:
            await websocket.accept()

            await websocket.send_bytes(b"SERVER_MESSAGE")

            await websocket.close()

        with server_factory(websocket_endpoint) as socket:
            with httpx.Client(transport=httpx.HTTPTransport(uds=socket)) as client:
                with connect_ws("http://socket/ws", client) as ws:
                    with pytest.raises(WebSocketInvalidTypeReceived):
                        ws.receive_text()

            async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(uds=socket)) as aclient:
                async with aconnect_ws("http://socket/ws", aclient) as aws:
                    with pytest.raises(WebSocketInvalidTypeReceived):
                        await aws.receive_text()

    async def test_receive_bytes(self, server_factory: ServerFactoryFixture) -> None:
        async def websocket_endpoint(websocket: WebSocket) -> None:
            await websocket.accept()

            await websocket.send_bytes(b"SERVER_MESSAGE")

            await websocket.close()

        with server_factory(websocket_endpoint) as socket:
            with httpx.Client(transport=httpx.HTTPTransport(uds=socket)) as client:
                with connect_ws("http://socket/ws", client) as ws:
                    data = ws.receive_bytes()
                    assert data == b"SERVER_MESSAGE"

            async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(uds=socket)) as aclient:
                async with aconnect_ws("http://socket/ws", aclient) as aws:
                    data = await aws.receive_bytes()
                    assert data == b"SERVER_MESSAGE"

    async def test_receive_bytes_invalid_type(self, server_factory: ServerFactoryFixture) -> None:
        async def websocket_endpoint(websocket: WebSocket) -> None:
            await websocket.accept()

            await websocket.send_text("SERVER_MESSAGE")

            await websocket.close()

        with server_factory(websocket_endpoint) as socket:
            with httpx.Client(transport=httpx.HTTPTransport(uds=socket)) as client:
                with connect_ws("http://socket/ws", client) as ws:
                    with pytest.raises(WebSocketInvalidTypeReceived):
                        ws.receive_bytes()

            async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(uds=socket)) as aclient:
                async with aconnect_ws("http://socket/ws", aclient) as aws:
                    with pytest.raises(WebSocketInvalidTypeReceived):
                        await aws.receive_bytes()

    @pytest.mark.parametrize("mode", ["text", "binary"])
    async def test_receive_json(self, mode: JSONMode, server_factory: ServerFactoryFixture) -> None:
        async def websocket_endpoint(websocket: WebSocket) -> None:
            await websocket.accept()

            await websocket.send_json({"message": "SERVER_MESSAGE"}, mode=mode)

            await websocket.close()

        with server_factory(websocket_endpoint) as socket:
            with httpx.Client(transport=httpx.HTTPTransport(uds=socket)) as client:
                with connect_ws("http://socket/ws", client) as ws:
                    data = ws.receive_json(mode=mode)
                    assert data == {"message": "SERVER_MESSAGE"}

            async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(uds=socket)) as aclient:
                async with aconnect_ws("http://socket/ws", aclient) as aws:
                    data = await aws.receive_json(mode=mode)
                    assert data == {"message": "SERVER_MESSAGE"}


@pytest.mark.anyio
class TestReceivePing:
    async def test_receive_ping(self) -> None:
        class MockNetworkStream(NetworkStream):
            def __init__(self) -> None:
                self.connection = wsproto.connection.Connection(wsproto.connection.ConnectionType.SERVER)
                self.events_to_send = [
                    wsproto.events.Ping(b"SERVER_PING"),
                    wsproto.events.CloseConnection(1000),
                ]

            def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
                try:
                    event = self.events_to_send.pop(0)
                    return self.connection.send(event)
                except IndexError:  # pragma: no cover
                    raise httpcore.ReadError()

            def write(self, buffer: bytes, timeout: float | None = None) -> None:
                self.connection.receive_data(buffer)

            def close(self) -> None:
                pass

        stream = MockNetworkStream()
        with WebSocketSession(stream):
            await anyio.sleep(0.1)

        received_events = list(stream.connection.events())
        assert received_events == [
            wsproto.events.Pong(b"SERVER_PING"),
            wsproto.events.CloseConnection(1000, ""),
        ]

    async def test_async_receive_ping(self) -> None:
        class MockAsyncNetworkStream(AsyncNetworkStream):
            def __init__(self) -> None:
                self.connection = wsproto.connection.Connection(wsproto.connection.ConnectionType.SERVER)
                self.events_to_send = [
                    wsproto.events.Ping(b"SERVER_PING"),
                    wsproto.events.CloseConnection(1000),
                ]

            async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
                try:
                    event = self.events_to_send.pop(0)
                    return self.connection.send(event)
                except IndexError:  # pragma: no cover
                    raise httpcore.ReadError()

            async def write(self, buffer: bytes, timeout: float | None = None) -> None:
                self.connection.receive_data(buffer)

            async def aclose(self) -> None:
                pass

        stream = MockAsyncNetworkStream()
        async with AsyncWebSocketSession(stream):
            await anyio.sleep(0.1)

        received_events = list(stream.connection.events())
        assert received_events == [
            wsproto.events.Pong(b"SERVER_PING"),
            wsproto.events.CloseConnection(1000, ""),
        ]


@pytest.mark.anyio
class TestKeepalivePing:
    async def test_keepalive_ping(self) -> None:
        class MockNetworkStream(NetworkStream):
            def __init__(self) -> None:
                self.connection = wsproto.connection.Connection(wsproto.connection.ConnectionType.SERVER)
                self._should_close = False
                self.ping_received = 0
                self.ping_answered = 0
                self.events_to_send: queue.Queue[wsproto.events.Event] = queue.Queue()

            def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
                while not self._should_close:
                    try:
                        event = self.events_to_send.get_nowait()
                        self.ping_answered += 1
                        return self.connection.send(event)
                    except queue.Empty:
                        pass
                raise httpcore.ReadError()

            def write(self, buffer: bytes, timeout: float | None = None) -> None:
                self.connection.receive_data(buffer)
                for event in self.connection.events():
                    if isinstance(event, wsproto.events.Ping):
                        self.ping_received += 1
                        self.events_to_send.put(event.response())

            def close(self) -> None:
                self._should_close = True

        stream = MockNetworkStream()
        with WebSocketSession(
            stream,
            keepalive_ping_interval_seconds=0.1,
            keepalive_ping_timeout_seconds=0.1,
        ):
            await anyio.sleep(0.2)

        assert stream.ping_received >= 1
        assert stream.ping_answered >= 1

    async def test_keepalive_ping_timeout(self) -> None:
        class MockNetworkStream(NetworkStream):
            def __init__(self) -> None:
                self.connection = wsproto.connection.Connection(wsproto.connection.ConnectionType.SERVER)
                self._should_close = False

            def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
                while not self._should_close:  # pragma: no cover
                    time.sleep(0.1)
                raise httpcore.ReadError()  # pragma: no cover

            def write(self, buffer: bytes, timeout: float | None = None) -> None:
                pass

            def close(self) -> None:
                self._should_close = True

        stream = MockNetworkStream()
        with pytest.raises(WebSocketNetworkError):
            with WebSocketSession(
                stream,
                keepalive_ping_interval_seconds=0.1,
                keepalive_ping_timeout_seconds=0.1,
            ) as websocket_session:
                websocket_session.receive()

    async def test_async_keepalive_ping(self) -> None:
        class MockAsyncNetworkStream(AsyncNetworkStream):
            def __init__(self) -> None:
                self.connection = wsproto.connection.Connection(wsproto.connection.ConnectionType.SERVER)
                self._should_close = False
                self.ping_received = 0
                self.ping_answered = 0
                (
                    self.send_events,
                    self.receive_events,
                ) = anyio.create_memory_object_stream[wsproto.events.Event]()

            async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
                while not self._should_close:
                    try:
                        event = self.receive_events.receive_nowait()
                        self.ping_answered += 1
                        return self.connection.send(event)
                    except anyio.WouldBlock:
                        await anyio.sleep(0.1)
                raise httpcore.ReadError()  # pragma: no cover

            async def write(self, buffer: bytes, timeout: float | None = None) -> None:
                self.connection.receive_data(buffer)
                for event in self.connection.events():
                    if isinstance(event, wsproto.events.Ping):
                        self.ping_received += 1
                        await self.send_events.send(event.response())

            async def aclose(self) -> None:
                self._should_close = True
                await self.send_events.aclose()
                await self.receive_events.aclose()

        stream = MockAsyncNetworkStream()
        async with AsyncWebSocketSession(
            stream,
            keepalive_ping_interval_seconds=0.1,
            keepalive_ping_timeout_seconds=0.1,
        ):
            await anyio.sleep(0.3)

        assert stream.ping_received >= 1
        assert stream.ping_answered >= 1

    async def test_async_keepalive_ping_exits_when_closing(self) -> None:
        writes = 0

        class MockAsyncNetworkStream(AsyncNetworkStream):
            async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
                return b""  # pragma: no cover

            async def write(self, buffer: bytes, timeout: float | None = None) -> None:
                nonlocal writes
                writes += 1  # pragma: no cover

            async def aclose(self) -> None: ...  # pragma: no cover

        session = AsyncWebSocketSession(MockAsyncNetworkStream(), keepalive_ping_interval_seconds=0.1)
        session.connection.send(wsproto.events.CloseConnection(1000))
        with anyio.fail_after(1):
            await session._background_keepalive_ping(0.01)

        assert writes == 0

    async def test_async_keepalive_ping_timeout(self) -> None:
        class MockAsyncNetworkStream(AsyncNetworkStream):
            def __init__(self) -> None:
                self.connection = wsproto.connection.Connection(wsproto.connection.ConnectionType.SERVER)
                self._should_close = False

            async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
                while not self._should_close:  # pragma: no cover
                    await anyio.sleep(0.1)
                raise httpcore.ReadError()  # pragma: no cover

            async def write(self, buffer: bytes, timeout: float | None = None) -> None:
                pass

            async def aclose(self) -> None:
                self._should_close = True

        stream = MockAsyncNetworkStream()
        with pytest.RaisesGroup(WebSocketNetworkError):
            async with AsyncWebSocketSession(
                stream,
                keepalive_ping_interval_seconds=0.1,
                keepalive_ping_timeout_seconds=0.1,
            ) as websocket_session:
                await websocket_session.receive()


@pytest.mark.anyio
async def test_ping_pong(server_factory: ServerFactoryFixture) -> None:
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        try:
            await websocket.receive_text()
        except StarletteWebSocketDisconnect:
            pass

    with server_factory(websocket_endpoint) as socket:
        with httpx.Client(transport=httpx.HTTPTransport(uds=socket)) as client:
            with connect_ws("http://socket/ws", client) as ws:
                ping_callback = ws.ping()
                result = ping_callback.wait()
                assert result is True

        async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(uds=socket)) as aclient:
            async with aconnect_ws("http://socket/ws", aclient) as aws:
                aping_callback = await aws.ping()
                await aping_callback.wait()
                assert aping_callback.is_set()


@pytest.mark.anyio
async def test_send_close(server_factory: ServerFactoryFixture, on_receive_message: MagicMock) -> None:
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        try:
            await websocket.receive_text()
        except StarletteWebSocketDisconnect as e:
            print(e)
            on_receive_message(e.code, e.reason)

    with server_factory(websocket_endpoint) as socket:
        with httpx.Client(transport=httpx.HTTPTransport(uds=socket)) as client:
            with connect_ws("http://socket/ws", client) as ws:
                ws.close(code=1001, reason="CLOSE_REASON")

        async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(uds=socket)) as aclient:
            async with aconnect_ws("http://socket/ws", aclient) as aws:
                await aws.close(code=1001, reason="CLOSE_REASON")

    on_receive_message.assert_has_calls([call(1001, "CLOSE_REASON"), call(1001, "CLOSE_REASON")])


@pytest.mark.anyio
async def test_receive_close(server_factory: ServerFactoryFixture) -> None:
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.close()

    with server_factory(websocket_endpoint) as socket:
        with httpx.Client(transport=httpx.HTTPTransport(uds=socket)) as client:
            with connect_ws("http://socket/ws", client) as ws:
                with pytest.raises(WebSocketDisconnect):
                    ws.receive()

        async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(uds=socket)) as aclient:
            async with aconnect_ws("http://socket/ws", aclient) as aws:
                with pytest.raises(WebSocketDisconnect):
                    await aws.receive()


@pytest.mark.anyio
async def test_default_httpx_client() -> None:
    with patch.object(_api, "WebSocketClient") as mock_client:
        with connect_ws("http://socket/ws"):
            pass
    mock_client.return_value.connect.assert_called_once()
    httpx_client = mock_client.call_args[1]["client"]
    assert isinstance(httpx_client, httpx.Client)
    assert httpx_client.is_closed

    with patch.object(_api, "AsyncWebSocketClient") as mock_async_client:
        async with aconnect_ws("http://socket/ws"):
            pass
    mock_async_client.return_value.connect.assert_called_once()
    httpx_client = mock_async_client.call_args[1]["client"]
    assert isinstance(httpx_client, httpx.AsyncClient)
    assert httpx_client.is_closed


@pytest.mark.anyio
async def test_subprotocol_and_response() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["sec-websocket-protocol"] == "custom_protocol, unsupported_protocol"

        return httpx.Response(
            101,
            headers={"sec-websocket-protocol": "custom_protocol"},
            extensions={"network_stream": MagicMock(spec=NetworkStream)},
        )

    def async_handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["sec-websocket-protocol"] == "custom_protocol, unsupported_protocol"

        return httpx.Response(
            101,
            headers={"sec-websocket-protocol": "custom_protocol"},
            extensions={"network_stream": MagicMock(spec=AsyncNetworkStream)},
        )

    with httpx.Client(base_url="http://localhost:8000", transport=httpx.MockTransport(handler)) as client:
        with connect_ws(
            "http://socket/ws",
            client,
            subprotocols=["custom_protocol", "unsupported_protocol"],
        ) as ws:
            assert isinstance(ws.response, httpx.Response)
            assert ws.subprotocol == "custom_protocol"
            assert ws.response.headers["sec-websocket-protocol"] == ws.subprotocol

    async with httpx.AsyncClient(
        base_url="http://localhost:8000", transport=httpx.MockTransport(async_handler)
    ) as aclient:
        async with aconnect_ws(
            "http://socket/ws",
            aclient,
            subprotocols=["custom_protocol", "unsupported_protocol"],
        ) as aws:
            assert isinstance(aws.response, httpx.Response)
            assert aws.subprotocol == "custom_protocol"
            assert aws.response.headers["sec-websocket-protocol"] == aws.subprotocol


@pytest.mark.anyio
async def test_threads_wont_hang(server_factory: ServerFactoryFixture) -> None:
    """
    Check that all threads spawned in WebSocketSession are properly terminated during
    a series of messages exchange. This used to be the cause of a memory leak in the
    connect_ws client, see https://github.com/frankie567/httpx-ws/issues/76.
    """

    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        for _ in range(50):
            await websocket.send_text("SERVER_MESSAGE")
            await websocket.receive_text()
        await websocket.close()

    def session_threads() -> set[threading.Thread]:
        return set(threading.enumerate()) - threads_before

    def wait_for_session_threads(expected: int) -> None:
        for _ in range(100):
            if len(session_threads()) == expected:
                return
            time.sleep(0.05)
        assert len(session_threads()) == expected  # pragma: no cover

    with server_factory(websocket_endpoint) as socket:
        with httpx.Client(transport=httpx.HTTPTransport(uds=socket)) as client:
            threads_before = set(threading.enumerate())
            with connect_ws("http://socket/ws", client, keepalive_ping_interval_seconds=None) as ws:
                for _ in range(50):
                    ws.receive()
                    ws.send_text("CLIENT_MESSAGE")
                wait_for_session_threads(2)
            wait_for_session_threads(0)


@pytest.mark.anyio
async def test_concurrency_write(server_factory: ServerFactoryFixture) -> None:
    """
    Check that there is no error because of two tasks trying to write the stream at the
    same time. Typically, this is when a background ping tries to send a ping while the
    main task is sending a message.

    See: https://github.com/frankie567/httpx-ws/issues/29
    """

    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        while True:
            message = await websocket.receive_text()
            await websocket.send_text(message)

    with server_factory(websocket_endpoint) as socket:
        # Added for completeness, but were not able to reproduce the issue with the sync client
        with httpx.Client(transport=httpx.HTTPTransport(uds=socket)) as client:
            with connect_ws("http://socket/ws", client) as ws:
                with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                    for _ in range(10):
                        executor.submit(ws.send_text, "CLIENT_MESSAGE")

        async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(uds=socket)) as aclient:
            async with aconnect_ws("http://socket/ws", aclient) as aws:
                async with anyio.create_task_group() as tg:
                    for _ in range(10):
                        tg.start_soon(aws.send_text, "CLIENT_MESSAGE")


@pytest.mark.anyio
async def test_client() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(101, extensions={"network_stream": MagicMock(spec=NetworkStream)})

    def async_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(101, extensions={"network_stream": MagicMock(spec=AsyncNetworkStream)})

    with httpx.Client(base_url="http://localhost:8000", transport=httpx.MockTransport(handler)) as client:
        ws_client = WebSocketClient(client)
        with ws_client.connect("http://socket/ws") as ws:
            assert isinstance(ws.response, httpx.Response)

    async with httpx.AsyncClient(
        base_url="http://localhost:8000", transport=httpx.MockTransport(async_handler)
    ) as aclient:
        async_ws_client = AsyncWebSocketClient(aclient)
        async with async_ws_client.connect("http://socket/ws") as aws:
            assert isinstance(aws.response, httpx.Response)
