import base64
import contextlib
import json
import secrets
import sys
import typing

if sys.version_info < (3, 8):
    from typing_extensions import Literal  # pragma: no cover
else:
    from typing import Literal  # pragma: no cover

import httpx
import wsproto

JSONMode = Literal["text", "binary"]


class HTTPXWSException(Exception):
    pass


class WebSocketUpgradeError(HTTPXWSException):
    def __init__(self, response: httpx.Response) -> None:
        self.response = response


class WebSocketDisconnect(HTTPXWSException):
    def __init__(self, code: int = 1000, reason: typing.Optional[str] = None) -> None:
        self.code = code
        self.reason = reason or ""


class WebSocketInvalidTypeReceived(HTTPXWSException):
    def __init__(self, event: wsproto.events.Event) -> None:
        self.event = event


class WebSocketSession:
    def __init__(self, response: httpx.Response) -> None:
        self.stream = response.extensions["network_stream"]
        self.connection = wsproto.Connection(wsproto.ConnectionType.CLIENT)

    async def send(self, event: wsproto.events.Event) -> None:
        await self._send_event(event)

    async def send_text(self, data: str) -> None:
        event = wsproto.events.TextMessage(data=data)
        await self.send(event)

    async def send_bytes(self, data: bytes) -> None:
        event = wsproto.events.BytesMessage(data=data)
        await self.send(event)

    async def send_json(self, data: typing.Any, mode: JSONMode = "text") -> None:
        assert mode in ["text", "binary"]
        serialized_data = json.dumps(data)
        if mode == "text":
            await self.send_text(serialized_data)
        else:
            await self.send_bytes(serialized_data.encode("utf-8"))

    async def receive(self) -> wsproto.events.Event:
        data = await self.stream.read(max_bytes=4096)
        self.connection.receive_data(data)
        for event in self.connection.events():
            if isinstance(event, wsproto.events.CloseConnection):
                raise WebSocketDisconnect(event.code, event.reason)
            return event
        raise HTTPXWSException()  # pragma: no cover

    async def receive_text(self) -> str:
        event = await self.receive()
        if isinstance(event, wsproto.events.TextMessage):
            return event.data
        raise WebSocketInvalidTypeReceived(event)

    async def receive_bytes(self) -> bytes:
        event = await self.receive()
        if isinstance(event, wsproto.events.BytesMessage):
            return event.data
        raise WebSocketInvalidTypeReceived(event)

    async def receive_json(self, mode: JSONMode = "text") -> typing.Any:
        assert mode in ["text", "binary"]
        data: typing.Union[str, bytes]
        if mode == "text":
            data = await self.receive_text()
        elif mode == "binary":
            data = await self.receive_bytes()
        return json.loads(data)

    async def close(self, code: int = 1000, reason: typing.Optional[str] = None):
        event = wsproto.events.CloseConnection(code, reason)
        await self._send_event(event)

    async def _send_event(self, event: wsproto.events.Event):
        data = self.connection.send(event)
        await self.stream.write(data)


@contextlib.asynccontextmanager
async def aconnect_ws(
    client: httpx.AsyncClient, url: str, **kwargs: typing.Any
) -> typing.AsyncGenerator[WebSocketSession, None]:
    headers = kwargs.pop("headers", {})
    headers["connection"] = "upgrade"
    headers["upgrade"] = "websocket"
    headers["sec-websocket-key"] = base64.b64encode(secrets.token_bytes(16)).decode(
        "utf-8"
    )
    headers["sec-websocket-version"] = "13"

    async with client.stream("GET", url, headers=headers) as response:
        if response.status_code != 101:
            raise WebSocketUpgradeError(response)

        session = WebSocketSession(response)
        yield session
        await session.close()
