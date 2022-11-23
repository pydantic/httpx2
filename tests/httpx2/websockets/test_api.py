from unittest.mock import MagicMock, call

import httpx
import pytest
import wsproto
from starlette.websockets import WebSocket

from httpx_ws import (
    JSONMode,
    WebSocketDisconnect,
    WebSocketInvalidTypeReceived,
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
            with connect_ws(client, "/ws"):
                pass

    async with httpx.AsyncClient(
        base_url="http://localhost:8000", transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(WebSocketUpgradeError):
            async with aconnect_ws(client, "/ws"):
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

        with server_factory(websocket_endpoint):
            with httpx.Client(base_url="http://localhost:8000") as client:
                with connect_ws(client, "/ws") as ws:
                    ws.send(wsproto.events.TextMessage(data="CLIENT_MESSAGE"))

            async with httpx.AsyncClient(base_url="http://localhost:8000") as aclient:
                async with aconnect_ws(aclient, "/ws") as aws:
                    await aws.send(wsproto.events.TextMessage(data="CLIENT_MESSAGE"))

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

        with server_factory(websocket_endpoint):
            with httpx.Client(base_url="http://localhost:8000") as client:
                with connect_ws(client, "/ws") as ws:
                    ws.send_text("CLIENT_MESSAGE")

            async with httpx.AsyncClient(base_url="http://localhost:8000") as aclient:
                async with aconnect_ws(aclient, "/ws") as aws:
                    await aws.send_text("CLIENT_MESSAGE")

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

        with server_factory(websocket_endpoint):
            with httpx.Client(base_url="http://localhost:8000") as client:
                with connect_ws(client, "/ws") as ws:
                    ws.send_bytes(b"CLIENT_MESSAGE")

            async with httpx.AsyncClient(base_url="http://localhost:8000") as aclient:
                async with aconnect_ws(aclient, "/ws") as aws:
                    await aws.send_bytes(b"CLIENT_MESSAGE")

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

        with server_factory(websocket_endpoint):
            with httpx.Client(base_url="http://localhost:8000") as client:
                with connect_ws(client, "/ws") as ws:
                    ws.send_json({"message": "CLIENT_MESSAGE"}, mode=mode)

            async with httpx.AsyncClient(base_url="http://localhost:8000") as aclient:
                async with aconnect_ws(aclient, "/ws") as aws:
                    await aws.send_json({"message": "CLIENT_MESSAGE"}, mode=mode)

        on_receive_message.assert_has_calls(
            [call({"message": "CLIENT_MESSAGE"}), call({"message": "CLIENT_MESSAGE"})]
        )


@pytest.mark.asyncio
class TestReceive:
    async def test_receive(self, server_factory: ServerFactoryFixture):
        async def websocket_endpoint(websocket: WebSocket):
            await websocket.accept()

            await websocket.send_text("SERVER_MESSAGE")

            await websocket.close()

        with server_factory(websocket_endpoint):
            with httpx.Client(base_url="http://localhost:8000") as client:
                with connect_ws(client, "/ws") as ws:
                    event = ws.receive()
                    assert isinstance(event, wsproto.events.TextMessage)
                    assert event.data == "SERVER_MESSAGE"

            async with httpx.AsyncClient(base_url="http://localhost:8000") as aclient:
                async with aconnect_ws(aclient, "/ws") as aws:
                    event = await aws.receive()
                    assert isinstance(event, wsproto.events.TextMessage)
                    assert event.data == "SERVER_MESSAGE"

    async def test_receive_text(self, server_factory: ServerFactoryFixture):
        async def websocket_endpoint(websocket: WebSocket):
            await websocket.accept()

            await websocket.send_text("SERVER_MESSAGE")

            await websocket.close()

        with server_factory(websocket_endpoint):
            with httpx.Client(base_url="http://localhost:8000") as client:
                with connect_ws(client, "/ws") as ws:
                    data = ws.receive_text()
                    assert data == "SERVER_MESSAGE"

            async with httpx.AsyncClient(base_url="http://localhost:8000") as aclient:
                async with aconnect_ws(aclient, "/ws") as aws:
                    data = await aws.receive_text()
                    assert data == "SERVER_MESSAGE"

    async def test_receive_text_invalid_type(
        self, server_factory: ServerFactoryFixture
    ):
        async def websocket_endpoint(websocket: WebSocket):
            await websocket.accept()

            await websocket.send_bytes(b"SERVER_MESSAGE")

            await websocket.close()

        with server_factory(websocket_endpoint):
            with httpx.Client(base_url="http://localhost:8000") as client:
                with pytest.raises(WebSocketInvalidTypeReceived):
                    with connect_ws(client, "/ws") as ws:
                        ws.receive_text()

            async with httpx.AsyncClient(base_url="http://localhost:8000") as aclient:
                with pytest.raises(WebSocketInvalidTypeReceived):
                    async with aconnect_ws(aclient, "/ws") as aws:
                        await aws.receive_text()

    async def test_receive_bytes(self, server_factory: ServerFactoryFixture):
        async def websocket_endpoint(websocket: WebSocket):
            await websocket.accept()

            await websocket.send_bytes(b"SERVER_MESSAGE")

            await websocket.close()

        with server_factory(websocket_endpoint):
            with httpx.Client(base_url="http://localhost:8000") as client:
                with connect_ws(client, "/ws") as ws:
                    data = ws.receive_bytes()
                    assert data == b"SERVER_MESSAGE"

            async with httpx.AsyncClient(base_url="http://localhost:8000") as aclient:
                async with aconnect_ws(aclient, "/ws") as aws:
                    data = await aws.receive_bytes()
                    assert data == b"SERVER_MESSAGE"

    async def test_receive_bytes_invalid_type(
        self, server_factory: ServerFactoryFixture
    ):
        async def websocket_endpoint(websocket: WebSocket):
            await websocket.accept()

            await websocket.send_text("SERVER_MESSAGE")

            await websocket.close()

        with server_factory(websocket_endpoint):
            with httpx.Client(base_url="http://localhost:8000") as client:
                with pytest.raises(WebSocketInvalidTypeReceived):
                    with connect_ws(client, "/ws") as ws:
                        ws.receive_bytes()

            async with httpx.AsyncClient(base_url="http://localhost:8000") as aclient:
                with pytest.raises(WebSocketInvalidTypeReceived):
                    async with aconnect_ws(aclient, "/ws") as aws:
                        await aws.receive_bytes()

    @pytest.mark.parametrize("mode", ["text", "binary"])
    async def test_receive_json(
        self, mode: JSONMode, server_factory: ServerFactoryFixture
    ):
        async def websocket_endpoint(websocket: WebSocket):
            await websocket.accept()

            await websocket.send_json({"message": "SERVER_MESSAGE"}, mode=mode)

            await websocket.close()

        with server_factory(websocket_endpoint):
            with httpx.Client(base_url="http://localhost:8000") as client:
                with connect_ws(client, "/ws") as ws:
                    data = ws.receive_json(mode=mode)
                    assert data == {"message": "SERVER_MESSAGE"}

            async with httpx.AsyncClient(base_url="http://localhost:8000") as aclient:
                async with aconnect_ws(aclient, "/ws") as aws:
                    data = await aws.receive_json(mode=mode)
                    assert data == {"message": "SERVER_MESSAGE"}


@pytest.mark.asyncio
async def test_send_close(server_factory: ServerFactoryFixture):
    async def websocket_endpoint(websocket: WebSocket):
        await websocket.accept()
        await websocket.receive_text()

    with server_factory(websocket_endpoint):
        with httpx.Client(base_url="http://localhost:8000") as client:
            with connect_ws(client, "/ws"):
                pass

        async with httpx.AsyncClient(base_url="http://localhost:8000") as aclient:
            async with aconnect_ws(aclient, "/ws"):
                pass


@pytest.mark.asyncio
async def test_receive_close(server_factory: ServerFactoryFixture):
    async def websocket_endpoint(websocket: WebSocket):
        await websocket.accept()
        await websocket.close()

    with server_factory(websocket_endpoint):
        with httpx.Client(base_url="http://localhost:8000") as client:
            with pytest.raises(WebSocketDisconnect):
                with connect_ws(client, "/ws") as ws:
                    ws.receive()

        async with httpx.AsyncClient(base_url="http://localhost:8000") as aclient:
            with pytest.raises(WebSocketDisconnect):
                async with aconnect_ws(aclient, "/ws") as aws:
                    await aws.receive()
