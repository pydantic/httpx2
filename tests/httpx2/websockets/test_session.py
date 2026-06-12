import concurrent.futures
import queue
import threading
import time
from unittest.mock import MagicMock, call, patch

import anyio
import pytest
import wsproto
from starlette.websockets import WebSocket, WebSocketDisconnect as StarletteWebSocketDisconnect

import httpcore2
import httpx2
from httpcore2 import AsyncNetworkStream, NetworkStream
from httpx2 import (
    AsyncWebSocketSession,
    WebSocketDisconnect,
    WebSocketInvalidTypeReceived,
    WebSocketNetworkError,
    WebSocketSession,
    WebSocketUpgradeError,
)
from httpx2._websockets._session import JSONMode
from tests.httpx2.websockets.conftest import ServerFactoryFixture


@pytest.mark.anyio
async def test_upgrade_error() -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(400)

    with httpx2.Client(base_url="http://localhost:8000", transport=httpx2.MockTransport(handler)) as client:
        with pytest.raises(WebSocketUpgradeError):
            with client.websocket("http://socket/ws"):
                pass  # pragma: no cover

    async with httpx2.AsyncClient(base_url="http://localhost:8000", transport=httpx2.MockTransport(handler)) as aclient:
        with pytest.raises(WebSocketUpgradeError):
            async with aclient.websocket("http://socket/ws"):
                pass  # pragma: no cover


def test_top_level_websocket() -> None:
    with patch("httpx2._api.Client") as mock_client_cls:
        mock_client = mock_client_cls.return_value.__enter__.return_value
        with httpx2.websocket("ws://socket/ws", subprotocols=["custom_protocol"]):
            pass
    mock_client.websocket.assert_called_once()
    assert mock_client.websocket.call_args[1]["subprotocols"] == ["custom_protocol"]


@pytest.mark.anyio
class TestSend:
    async def test_send_error(self) -> None:
        class MockNetworkStream(NetworkStream):
            def __init__(self) -> None:
                self.connection = wsproto.connection.Connection(wsproto.connection.ConnectionType.SERVER)
                self._should_close = False

            def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
                while not self._should_close:
                    time.sleep(0.1)
                raise httpcore2.ReadError()

            def write(self, buffer: bytes, timeout: float | None = None) -> None:
                raise httpcore2.WriteError()

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
                while not self._should_close:
                    await anyio.sleep(0.1)
                raise httpcore2.ReadError()

            async def write(self, buffer: bytes, timeout: float | None = None) -> None:
                raise httpcore2.WriteError()

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
            with httpx2.Client(transport=httpx2.HTTPTransport(uds=socket)) as client:
                with client.websocket("http://socket/ws") as ws:
                    ws.send(wsproto.events.TextMessage(data="CLIENT_MESSAGE"))

            async with httpx2.AsyncClient(transport=httpx2.AsyncHTTPTransport(uds=socket)) as aclient:
                async with aclient.websocket("http://socket/ws") as aws:
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
            with httpx2.Client(transport=httpx2.HTTPTransport(uds=socket)) as client:
                with client.websocket("http://socket/ws") as ws:
                    ws.send_text("CLIENT_MESSAGE")

            async with httpx2.AsyncClient(transport=httpx2.AsyncHTTPTransport(uds=socket)) as aclient:
                async with aclient.websocket("http://socket/ws") as aws:
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
            with httpx2.Client(transport=httpx2.HTTPTransport(uds=socket)) as client:
                with client.websocket("http://socket/ws") as ws:
                    ws.send_bytes(b"CLIENT_MESSAGE")

            async with httpx2.AsyncClient(transport=httpx2.AsyncHTTPTransport(uds=socket)) as aclient:
                async with aclient.websocket("http://socket/ws") as aws:
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
            with httpx2.Client(transport=httpx2.HTTPTransport(uds=socket)) as client:
                with client.websocket("http://socket/ws") as ws:
                    ws.send_json({"message": "CLIENT_MESSAGE"}, mode=mode)

            async with httpx2.AsyncClient(transport=httpx2.AsyncHTTPTransport(uds=socket)) as aclient:
                async with aclient.websocket("http://socket/ws") as aws:
                    await aws.send_json({"message": "CLIENT_MESSAGE"}, mode=mode)

        on_receive_message.assert_has_calls([call({"message": "CLIENT_MESSAGE"}), call({"message": "CLIENT_MESSAGE"})])


@pytest.mark.anyio
class TestReceive:
    async def test_receive_error(self) -> None:
        class MockNetworkStream(NetworkStream):
            def __init__(self) -> None:
                self.connection = wsproto.connection.Connection(wsproto.connection.ConnectionType.SERVER)

            def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
                raise httpcore2.ReadError()

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
                raise httpcore2.ReadError()

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
            with httpx2.Client(transport=httpx2.HTTPTransport(uds=socket)) as client:
                with client.websocket("http://socket/ws") as ws:
                    event = ws.receive()
                    assert isinstance(event, wsproto.events.TextMessage)
                    assert event.data == "SERVER_MESSAGE"

            async with httpx2.AsyncClient(transport=httpx2.AsyncHTTPTransport(uds=socket)) as aclient:
                async with aclient.websocket("http://socket/ws") as aws:
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
            with httpx2.Client(transport=httpx2.HTTPTransport(uds=socket)) as client:
                with client.websocket("http://socket/ws", max_message_size_bytes=1024) as ws:
                    event = ws.receive()
                    assert isinstance(event, wsproto.events.Message)
                    assert event.data == full_message

            async with httpx2.AsyncClient(transport=httpx2.AsyncHTTPTransport(uds=socket)) as aclient:
                async with aclient.websocket("http://socket/ws", max_message_size_bytes=1024) as aws:
                    event = await aws.receive()
                    assert isinstance(event, wsproto.events.Message)
                    assert event.data == full_message

    async def test_receive_text(self, server_factory: ServerFactoryFixture) -> None:
        async def websocket_endpoint(websocket: WebSocket) -> None:
            await websocket.accept()

            await websocket.send_text("SERVER_MESSAGE")

            await websocket.close()

        with server_factory(websocket_endpoint) as socket:
            with httpx2.Client(transport=httpx2.HTTPTransport(uds=socket)) as client:
                with client.websocket("http://socket/ws") as ws:
                    data = ws.receive_text()
                    assert data == "SERVER_MESSAGE"

            async with httpx2.AsyncClient(transport=httpx2.AsyncHTTPTransport(uds=socket)) as aclient:
                async with aclient.websocket("http://socket/ws") as aws:
                    data = await aws.receive_text()
                    assert data == "SERVER_MESSAGE"

    async def test_receive_text_invalid_type(self, server_factory: ServerFactoryFixture) -> None:
        async def websocket_endpoint(websocket: WebSocket) -> None:
            await websocket.accept()

            await websocket.send_bytes(b"SERVER_MESSAGE")

            await websocket.close()

        with server_factory(websocket_endpoint) as socket:
            with httpx2.Client(transport=httpx2.HTTPTransport(uds=socket)) as client:
                with client.websocket("http://socket/ws") as ws:
                    with pytest.raises(WebSocketInvalidTypeReceived):
                        ws.receive_text()

            async with httpx2.AsyncClient(transport=httpx2.AsyncHTTPTransport(uds=socket)) as aclient:
                async with aclient.websocket("http://socket/ws") as aws:
                    with pytest.raises(WebSocketInvalidTypeReceived):
                        await aws.receive_text()

    async def test_receive_bytes(self, server_factory: ServerFactoryFixture) -> None:
        async def websocket_endpoint(websocket: WebSocket) -> None:
            await websocket.accept()

            await websocket.send_bytes(b"SERVER_MESSAGE")

            await websocket.close()

        with server_factory(websocket_endpoint) as socket:
            with httpx2.Client(transport=httpx2.HTTPTransport(uds=socket)) as client:
                with client.websocket("http://socket/ws") as ws:
                    data = ws.receive_bytes()
                    assert data == b"SERVER_MESSAGE"

            async with httpx2.AsyncClient(transport=httpx2.AsyncHTTPTransport(uds=socket)) as aclient:
                async with aclient.websocket("http://socket/ws") as aws:
                    data = await aws.receive_bytes()
                    assert data == b"SERVER_MESSAGE"

    async def test_receive_bytes_invalid_type(self, server_factory: ServerFactoryFixture) -> None:
        async def websocket_endpoint(websocket: WebSocket) -> None:
            await websocket.accept()

            await websocket.send_text("SERVER_MESSAGE")

            await websocket.close()

        with server_factory(websocket_endpoint) as socket:
            with httpx2.Client(transport=httpx2.HTTPTransport(uds=socket)) as client:
                with client.websocket("http://socket/ws") as ws:
                    with pytest.raises(WebSocketInvalidTypeReceived):
                        ws.receive_bytes()

            async with httpx2.AsyncClient(transport=httpx2.AsyncHTTPTransport(uds=socket)) as aclient:
                async with aclient.websocket("http://socket/ws") as aws:
                    with pytest.raises(WebSocketInvalidTypeReceived):
                        await aws.receive_bytes()

    @pytest.mark.parametrize("mode", ["text", "binary"])
    async def test_receive_json(self, mode: JSONMode, server_factory: ServerFactoryFixture) -> None:
        async def websocket_endpoint(websocket: WebSocket) -> None:
            await websocket.accept()

            await websocket.send_json({"message": "SERVER_MESSAGE"}, mode=mode)

            await websocket.close()

        with server_factory(websocket_endpoint) as socket:
            with httpx2.Client(transport=httpx2.HTTPTransport(uds=socket)) as client:
                with client.websocket("http://socket/ws") as ws:
                    data = ws.receive_json(mode=mode)
                    assert data == {"message": "SERVER_MESSAGE"}

            async with httpx2.AsyncClient(transport=httpx2.AsyncHTTPTransport(uds=socket)) as aclient:
                async with aclient.websocket("http://socket/ws") as aws:
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
                except IndexError:
                    raise httpcore2.ReadError()

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
                except IndexError:
                    raise httpcore2.ReadError()

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
                raise httpcore2.ReadError()

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
                while not self._should_close:
                    time.sleep(0.1)
                raise httpcore2.ReadError()

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
                raise httpcore2.ReadError()

            async def write(self, buffer: bytes, timeout: float | None = None) -> None:
                self.connection.receive_data(buffer)
                for event in self.connection.events():
                    if isinstance(event, wsproto.events.Ping):
                        self.ping_received += 1
                        await self.send_events.send(event.response())

            async def aclose(self) -> None:
                self._should_close = True
                self.send_events.close()
                self.receive_events.close()

        stream = MockAsyncNetworkStream()
        async with AsyncWebSocketSession(
            stream,
            keepalive_ping_interval_seconds=0.1,
            keepalive_ping_timeout_seconds=0.1,
        ):
            await anyio.sleep(0.3)

        assert stream.ping_received >= 1
        assert stream.ping_answered >= 1

    async def test_async_keepalive_ping_timeout(self) -> None:
        class MockAsyncNetworkStream(AsyncNetworkStream):
            def __init__(self) -> None:
                self.connection = wsproto.connection.Connection(wsproto.connection.ConnectionType.SERVER)
                self._should_close = False

            async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
                while not self._should_close:
                    await anyio.sleep(0.1)
                raise httpcore2.ReadError()

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
        with httpx2.Client(transport=httpx2.HTTPTransport(uds=socket)) as client:
            with client.websocket("http://socket/ws") as ws:
                ping_callback = ws.ping()
                result = ping_callback.wait()
                assert result is True

        async with httpx2.AsyncClient(transport=httpx2.AsyncHTTPTransport(uds=socket)) as aclient:
            async with aclient.websocket("http://socket/ws") as aws:
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
            on_receive_message(e.code, e.reason)

    with server_factory(websocket_endpoint) as socket:
        with httpx2.Client(transport=httpx2.HTTPTransport(uds=socket)) as client:
            with client.websocket("http://socket/ws") as ws:
                ws.close(code=1001, reason="CLOSE_REASON")

        async with httpx2.AsyncClient(transport=httpx2.AsyncHTTPTransport(uds=socket)) as aclient:
            async with aclient.websocket("http://socket/ws") as aws:
                await aws.close(code=1001, reason="CLOSE_REASON")

    on_receive_message.assert_has_calls([call(1001, "CLOSE_REASON"), call(1001, "CLOSE_REASON")])


@pytest.mark.anyio
async def test_receive_close(server_factory: ServerFactoryFixture) -> None:
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.close()

    with server_factory(websocket_endpoint) as socket:
        with httpx2.Client(transport=httpx2.HTTPTransport(uds=socket)) as client:
            with client.websocket("http://socket/ws") as ws:
                with pytest.raises(WebSocketDisconnect):
                    ws.receive()

        async with httpx2.AsyncClient(transport=httpx2.AsyncHTTPTransport(uds=socket)) as aclient:
            async with aclient.websocket("http://socket/ws") as aws:
                with pytest.raises(WebSocketDisconnect):
                    await aws.receive()


@pytest.mark.anyio
async def test_subprotocol_and_response() -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        assert request.headers["sec-websocket-protocol"] == "custom_protocol, unsupported_protocol"

        return httpx2.Response(
            101,
            headers={"sec-websocket-protocol": "custom_protocol"},
            extensions={"network_stream": MagicMock(spec=NetworkStream)},
        )

    def async_handler(request: httpx2.Request) -> httpx2.Response:
        assert request.headers["sec-websocket-protocol"] == "custom_protocol, unsupported_protocol"

        return httpx2.Response(
            101,
            headers={"sec-websocket-protocol": "custom_protocol"},
            extensions={"network_stream": MagicMock(spec=AsyncNetworkStream)},
        )

    with httpx2.Client(base_url="http://localhost:8000", transport=httpx2.MockTransport(handler)) as client:
        with client.websocket(
            "http://socket/ws",
            subprotocols=["custom_protocol", "unsupported_protocol"],
        ) as ws:
            assert isinstance(ws.response, httpx2.Response)
            assert ws.subprotocol == "custom_protocol"
            assert ws.response.headers["sec-websocket-protocol"] == ws.subprotocol

    async with httpx2.AsyncClient(
        base_url="http://localhost:8000", transport=httpx2.MockTransport(async_handler)
    ) as aclient:
        async with aclient.websocket(
            "http://socket/ws",
            subprotocols=["custom_protocol", "unsupported_protocol"],
        ) as aws:
            assert isinstance(aws.response, httpx2.Response)
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

    with server_factory(websocket_endpoint) as socket:
        with httpx2.Client(transport=httpx2.HTTPTransport(uds=socket)) as client:
            initial_threads_count = threading.active_count()
            with client.websocket("http://socket/ws", keepalive_ping_interval_seconds=None) as ws:
                for _ in range(50):
                    ws.receive()
                    ws.send_text("CLIENT_MESSAGE")
                time.sleep(0.1)  # Let the websocket endpoint finish its handling.
                threads_count = threading.active_count()
                assert initial_threads_count + 2 == threads_count
            time.sleep(0.1)
            final_threads_count = threading.active_count()
            assert initial_threads_count == final_threads_count


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
        async for message in websocket.iter_text():
            await websocket.send_text(message)

    with server_factory(websocket_endpoint) as socket:
        # Added for completeness, but were not able to reproduce the issue with the sync client
        with httpx2.Client(transport=httpx2.HTTPTransport(uds=socket)) as client:
            with client.websocket("http://socket/ws") as ws:
                with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                    for _ in range(10):
                        executor.submit(ws.send_text, "CLIENT_MESSAGE")

        async with httpx2.AsyncClient(transport=httpx2.AsyncHTTPTransport(uds=socket)) as aclient:
            async with aclient.websocket("http://socket/ws") as aws:
                async with anyio.create_task_group() as tg:
                    for _ in range(10):
                        tg.start_soon(aws.send_text, "CLIENT_MESSAGE")


@pytest.mark.anyio
async def test_client_websocket_with_mock_stream() -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(101, extensions={"network_stream": MagicMock(spec=NetworkStream)})

    def async_handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(101, extensions={"network_stream": MagicMock(spec=AsyncNetworkStream)})

    with httpx2.Client(base_url="http://localhost:8000", transport=httpx2.MockTransport(handler)) as client:
        with client.websocket("http://socket/ws") as ws:
            assert isinstance(ws.response, httpx2.Response)

    async with httpx2.AsyncClient(
        base_url="http://localhost:8000", transport=httpx2.MockTransport(async_handler)
    ) as aclient:
        async with aclient.websocket("http://socket/ws") as aws:
            assert isinstance(aws.response, httpx2.Response)
