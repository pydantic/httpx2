import asyncio
import typing
from unittest.mock import MagicMock, call

import httpcore
import httpx
import pytest
import wsproto
from httpcore.backends.base import AsyncNetworkStream, NetworkStream
from starlette.websockets import WebSocket
from starlette.websockets import WebSocketDisconnect as StarletteWebSocketDisconnect

from httpx_ws import (
    AsyncWebSocketSession,
    JSONMode,
    WebSocketDisconnect,
    WebSocketInvalidTypeReceived,
    WebSocketSession,
    WebSocketUpgradeError,
    aconnect_ws,
    connect_ws,
)
from tests.conftest import ServerFactoryFixture


@pytest.mark.asyncio
async def test_upgrade_error():
    def handler(request):
        return httpx.Response(400)

    with httpx.Client(
        base_url="http://localhost:8000", transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(WebSocketUpgradeError):
            with connect_ws("http://socket/ws", client):
                pass

    async with httpx.AsyncClient(
        base_url="http://localhost:8000", transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(WebSocketUpgradeError):
            async with aconnect_ws("http://socket/ws", client):
                pass


@pytest.mark.asyncio
class TestSend:
    async def test_send(
        self,
        server_factory: ServerFactoryFixture,
        on_receive_message: MagicMock,
    ):
        async def websocket_endpoint(websocket: WebSocket):
            await websocket.accept()

            message = await websocket.receive_text()
            on_receive_message(message)

            await websocket.close()

        with server_factory(websocket_endpoint) as socket:
            with httpx.Client(transport=httpx.HTTPTransport(uds=socket)) as client:
                try:
                    with connect_ws("http://socket/ws", client) as ws:
                        ws.send(wsproto.events.TextMessage(data="CLIENT_MESSAGE"))
                except WebSocketDisconnect:
                    pass

            async with httpx.AsyncClient(
                transport=httpx.AsyncHTTPTransport(uds=socket)
            ) as aclient:
                try:
                    async with aconnect_ws("http://socket/ws", aclient) as aws:
                        await aws.send(
                            wsproto.events.TextMessage(data="CLIENT_MESSAGE")
                        )
                except WebSocketDisconnect:
                    pass

        on_receive_message.assert_has_calls(
            [call("CLIENT_MESSAGE"), call("CLIENT_MESSAGE")]
        )

    async def test_send_text(
        self,
        server_factory: ServerFactoryFixture,
        on_receive_message: MagicMock,
    ):
        async def websocket_endpoint(websocket: WebSocket):
            await websocket.accept()

            message = await websocket.receive_text()
            on_receive_message(message)

            await websocket.close()

        with server_factory(websocket_endpoint) as socket:
            with httpx.Client(transport=httpx.HTTPTransport(uds=socket)) as client:
                try:
                    with connect_ws("http://socket/ws", client) as ws:
                        ws.send_text("CLIENT_MESSAGE")
                except WebSocketDisconnect:
                    pass

            async with httpx.AsyncClient(
                transport=httpx.AsyncHTTPTransport(uds=socket)
            ) as aclient:
                try:
                    async with aconnect_ws("http://socket/ws", aclient) as aws:
                        await aws.send_text("CLIENT_MESSAGE")
                except WebSocketDisconnect:
                    pass

        on_receive_message.assert_has_calls(
            [call("CLIENT_MESSAGE"), call("CLIENT_MESSAGE")]
        )

    async def test_send_bytes(
        self,
        server_factory: ServerFactoryFixture,
        on_receive_message: MagicMock,
    ):
        async def websocket_endpoint(websocket: WebSocket):
            await websocket.accept()

            message = await websocket.receive_bytes()
            on_receive_message(message)

            await websocket.close()

        with server_factory(websocket_endpoint) as socket:
            with httpx.Client(transport=httpx.HTTPTransport(uds=socket)) as client:
                try:
                    with connect_ws("http://socket/ws", client) as ws:
                        ws.send_bytes(b"CLIENT_MESSAGE")
                except WebSocketDisconnect:
                    pass

            async with httpx.AsyncClient(
                transport=httpx.AsyncHTTPTransport(uds=socket)
            ) as aclient:
                try:
                    async with aconnect_ws("http://socket/ws", aclient) as aws:
                        await aws.send_bytes(b"CLIENT_MESSAGE")
                except WebSocketDisconnect:
                    pass

        on_receive_message.assert_has_calls(
            [call(b"CLIENT_MESSAGE"), call(b"CLIENT_MESSAGE")]
        )

    @pytest.mark.parametrize("mode", ["text", "binary"])
    async def test_send_json(
        self,
        mode: JSONMode,
        server_factory: ServerFactoryFixture,
        on_receive_message: MagicMock,
    ):
        async def websocket_endpoint(websocket: WebSocket):
            await websocket.accept()

            message = await websocket.receive_json(mode=mode)
            on_receive_message(message)

            await websocket.close()

        with server_factory(websocket_endpoint) as socket:
            with httpx.Client(transport=httpx.HTTPTransport(uds=socket)) as client:
                try:
                    with connect_ws("http://socket/ws", client) as ws:
                        ws.send_json({"message": "CLIENT_MESSAGE"}, mode=mode)
                except WebSocketDisconnect:
                    pass

            async with httpx.AsyncClient(
                transport=httpx.AsyncHTTPTransport(uds=socket)
            ) as aclient:
                try:
                    async with aconnect_ws("http://socket/ws", aclient) as aws:
                        await aws.send_json({"message": "CLIENT_MESSAGE"}, mode=mode)
                except WebSocketDisconnect:
                    pass

        on_receive_message.assert_has_calls(
            [call({"message": "CLIENT_MESSAGE"}), call({"message": "CLIENT_MESSAGE"})]
        )


@pytest.mark.asyncio
class TestReceive:
    async def test_receive(self, server_factory: ServerFactoryFixture):
        async def websocket_endpoint(websocket: WebSocket):
            await websocket.accept()
            await asyncio.sleep(0.1)  # FIXME: see #7

            await websocket.send_text("SERVER_MESSAGE")

            await websocket.close()

        with server_factory(websocket_endpoint) as socket:
            with httpx.Client(transport=httpx.HTTPTransport(uds=socket)) as client:
                try:
                    with connect_ws("http://socket/ws", client) as ws:
                        event = ws.receive()
                        assert isinstance(event, wsproto.events.TextMessage)
                        assert event.data == "SERVER_MESSAGE"
                except WebSocketDisconnect:
                    pass

            async with httpx.AsyncClient(
                transport=httpx.AsyncHTTPTransport(uds=socket)
            ) as aclient:
                try:
                    async with aconnect_ws("http://socket/ws", aclient) as aws:
                        event = await aws.receive()
                        assert isinstance(event, wsproto.events.TextMessage)
                        assert event.data == "SERVER_MESSAGE"
                except WebSocketDisconnect:
                    pass

    async def test_receive_text(self, server_factory: ServerFactoryFixture):
        async def websocket_endpoint(websocket: WebSocket):
            await websocket.accept()
            await asyncio.sleep(0.1)  # FIXME: see #7

            await websocket.send_text("SERVER_MESSAGE")

            await websocket.close()

        with server_factory(websocket_endpoint) as socket:
            with httpx.Client(transport=httpx.HTTPTransport(uds=socket)) as client:
                try:
                    with connect_ws("http://socket/ws", client) as ws:
                        data = ws.receive_text()
                        assert data == "SERVER_MESSAGE"
                except WebSocketDisconnect:
                    pass

            async with httpx.AsyncClient(
                transport=httpx.AsyncHTTPTransport(uds=socket)
            ) as aclient:
                try:
                    async with aconnect_ws("http://socket/ws", aclient) as aws:
                        data = await aws.receive_text()
                        assert data == "SERVER_MESSAGE"
                except WebSocketDisconnect:
                    pass

    async def test_receive_text_invalid_type(
        self, server_factory: ServerFactoryFixture
    ):
        async def websocket_endpoint(websocket: WebSocket):
            await websocket.accept()
            await asyncio.sleep(0.1)  # FIXME: see #7

            await websocket.send_bytes(b"SERVER_MESSAGE")

            await websocket.close()

        with server_factory(websocket_endpoint) as socket:
            with httpx.Client(transport=httpx.HTTPTransport(uds=socket)) as client:
                try:
                    with pytest.raises(WebSocketInvalidTypeReceived):
                        with connect_ws("http://socket/ws", client) as ws:
                            ws.receive_text()
                except WebSocketDisconnect:
                    pass

            async with httpx.AsyncClient(
                transport=httpx.AsyncHTTPTransport(uds=socket)
            ) as aclient:
                try:
                    with pytest.raises(WebSocketInvalidTypeReceived):
                        async with aconnect_ws("http://socket/ws", aclient) as aws:
                            await aws.receive_text()
                except WebSocketDisconnect:
                    pass

    async def test_receive_bytes(self, server_factory: ServerFactoryFixture):
        async def websocket_endpoint(websocket: WebSocket):
            await websocket.accept()
            await asyncio.sleep(0.1)  # FIXME: see #7

            await websocket.send_bytes(b"SERVER_MESSAGE")

            await websocket.close()

        with server_factory(websocket_endpoint) as socket:
            with httpx.Client(transport=httpx.HTTPTransport(uds=socket)) as client:
                try:
                    with connect_ws("http://socket/ws", client) as ws:
                        data = ws.receive_bytes()
                        assert data == b"SERVER_MESSAGE"
                except WebSocketDisconnect:
                    pass

            async with httpx.AsyncClient(
                transport=httpx.AsyncHTTPTransport(uds=socket)
            ) as aclient:
                try:
                    async with aconnect_ws("http://socket/ws", aclient) as aws:
                        data = await aws.receive_bytes()
                        assert data == b"SERVER_MESSAGE"
                except WebSocketDisconnect:
                    pass

    async def test_receive_bytes_invalid_type(
        self, server_factory: ServerFactoryFixture
    ):
        async def websocket_endpoint(websocket: WebSocket):
            await websocket.accept()
            await asyncio.sleep(0.1)  # FIXME: see #7

            await websocket.send_text("SERVER_MESSAGE")

            await websocket.close()

        with server_factory(websocket_endpoint) as socket:
            with httpx.Client(transport=httpx.HTTPTransport(uds=socket)) as client:
                with pytest.raises(WebSocketInvalidTypeReceived):
                    with connect_ws("http://socket/ws", client) as ws:
                        ws.receive_bytes()

            async with httpx.AsyncClient(
                transport=httpx.AsyncHTTPTransport(uds=socket)
            ) as aclient:
                with pytest.raises(WebSocketInvalidTypeReceived):
                    async with aconnect_ws("http://socket/ws", aclient) as aws:
                        await aws.receive_bytes()

    @pytest.mark.parametrize("mode", ["text", "binary"])
    async def test_receive_json(
        self, mode: JSONMode, server_factory: ServerFactoryFixture
    ):
        async def websocket_endpoint(websocket: WebSocket):
            await websocket.accept()
            await asyncio.sleep(0.1)  # FIXME: see #7

            await websocket.send_json({"message": "SERVER_MESSAGE"}, mode=mode)

            await websocket.close()

        with server_factory(websocket_endpoint) as socket:
            with httpx.Client(transport=httpx.HTTPTransport(uds=socket)) as client:
                try:
                    with connect_ws("http://socket/ws", client) as ws:
                        data = ws.receive_json(mode=mode)
                        assert data == {"message": "SERVER_MESSAGE"}
                except WebSocketDisconnect:
                    pass

            async with httpx.AsyncClient(
                transport=httpx.AsyncHTTPTransport(uds=socket)
            ) as aclient:
                try:
                    async with aconnect_ws("http://socket/ws", aclient) as aws:
                        data = await aws.receive_json(mode=mode)
                        assert data == {"message": "SERVER_MESSAGE"}
                except WebSocketDisconnect:
                    pass


@pytest.mark.asyncio
class TestReceivePing:
    async def test_receive_ping(self):
        class MockNetworkStream(NetworkStream):
            def __init__(self) -> None:
                self.connection = wsproto.connection.Connection(
                    wsproto.connection.ConnectionType.SERVER
                )
                self.events_to_send = [wsproto.events.Ping(b"SERVER_PING")]

            def read(
                self, max_bytes: int, timeout: typing.Optional[float] = None
            ) -> bytes:
                try:
                    event = self.events_to_send.pop(0)
                    return self.connection.send(event)
                except IndexError:
                    raise httpcore.ReadError()

            def write(
                self, buffer: bytes, timeout: typing.Optional[float] = None
            ) -> None:
                self.connection.receive_data(buffer)

        stream = MockNetworkStream()
        websocket_session = WebSocketSession(stream)
        await asyncio.sleep(0.1)
        websocket_session.close()

        received_events = list(stream.connection.events())
        assert received_events == [
            wsproto.events.Pong(b"SERVER_PING"),
            wsproto.events.CloseConnection(1000, ""),
        ]

    async def test_async_receive_ping(self):
        class MockAsyncNetworkStream(AsyncNetworkStream):
            def __init__(self) -> None:
                self.connection = wsproto.connection.Connection(
                    wsproto.connection.ConnectionType.SERVER
                )
                self.events_to_send = [wsproto.events.Ping(b"SERVER_PING")]

            async def read(
                self, max_bytes: int, timeout: typing.Optional[float] = None
            ) -> bytes:
                try:
                    event = self.events_to_send.pop(0)
                    return self.connection.send(event)
                except IndexError:
                    raise httpcore.ReadError()

            async def write(
                self, buffer: bytes, timeout: typing.Optional[float] = None
            ) -> None:
                self.connection.receive_data(buffer)

        stream = MockAsyncNetworkStream()
        websocket_session = AsyncWebSocketSession(stream)
        await asyncio.sleep(0.1)
        await websocket_session.close()

        received_events = list(stream.connection.events())
        assert received_events == [
            wsproto.events.Pong(b"SERVER_PING"),
            wsproto.events.CloseConnection(1000, ""),
        ]


@pytest.mark.asyncio
async def test_ping_pong(server_factory: ServerFactoryFixture):
    async def websocket_endpoint(websocket: WebSocket):
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

        async with httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(uds=socket)
        ) as aclient:
            async with aconnect_ws("http://socket/ws", aclient) as aws:
                aping_callback = await aws.ping()
                aresult = await aping_callback.wait()
                assert aresult is True


@pytest.mark.asyncio
async def test_send_close(server_factory: ServerFactoryFixture):
    async def websocket_endpoint(websocket: WebSocket):
        await websocket.accept()
        try:
            await websocket.receive_text()
        except StarletteWebSocketDisconnect:
            pass

    with server_factory(websocket_endpoint) as socket:
        with httpx.Client(transport=httpx.HTTPTransport(uds=socket)) as client:
            with connect_ws("http://socket/ws", client):
                pass

        async with httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(uds=socket)
        ) as aclient:
            async with aconnect_ws("http://socket/ws", aclient):
                pass


@pytest.mark.asyncio
async def test_receive_close(server_factory: ServerFactoryFixture):
    async def websocket_endpoint(websocket: WebSocket):
        await websocket.accept()
        await asyncio.sleep(0.1)  # FIXME: see #7
        await websocket.close()

    with server_factory(websocket_endpoint) as socket:
        with httpx.Client(transport=httpx.HTTPTransport(uds=socket)) as client:
            with pytest.raises(WebSocketDisconnect):
                with connect_ws("http://socket/ws", client) as ws:
                    ws.receive()

        async with httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(uds=socket)
        ) as aclient:
            with pytest.raises(WebSocketDisconnect):
                async with aconnect_ws("http://socket/ws", aclient) as aws:
                    await aws.receive()
