import base64
import contextlib
import json
import secrets
import typing

import httpx
import wsproto


class HTTPXWSException(Exception):
    pass


class WebSocketUpgradeError(HTTPXWSException):
    def __init__(self, response: httpx.Response) -> None:
        self.response = response


class WebSocketDisconnect(HTTPXWSException):
    def __init__(self, code: int = 1000, reason: typing.Optional[str] = None) -> None:
        self.code = code
        self.reason = reason or ""


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

    async def send_json(self, data: typing.Any, mode: str = "text") -> None:
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
