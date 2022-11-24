import base64
import collections
import contextlib
import json
import secrets
import sys
import typing

if sys.version_info < (3, 8):
    from typing_extensions import Literal  # pragma: no cover
else:
    from typing import Literal  # pragma: no cover

import httpcore
import httpx
import wsproto
from httpcore.backends.base import AsyncNetworkStream, NetworkStream

JSONMode = Literal["text", "binary"]

DEFAULT_RECEIVE_MAX_BYTES = 65_536


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
        self.stream: NetworkStream = response.extensions["network_stream"]
        self.connection = wsproto.Connection(wsproto.ConnectionType.CLIENT)
        self._events: typing.Deque[wsproto.events.Event] = collections.deque()

    def ping(self, payload: bytes = b"") -> None:
        event = wsproto.events.Ping(payload)
        self._send_event(event)

    def send(self, event: wsproto.events.Event) -> None:
        self._send_event(event)

    def send_text(self, data: str) -> None:
        event = wsproto.events.TextMessage(data=data)
        self.send(event)

    def send_bytes(self, data: bytes) -> None:
        event = wsproto.events.BytesMessage(data=data)
        self.send(event)

    def send_json(self, data: typing.Any, mode: JSONMode = "text") -> None:
        assert mode in ["text", "binary"]
        serialized_data = json.dumps(data)
        if mode == "text":
            self.send_text(serialized_data)
        else:
            self.send_bytes(serialized_data.encode("utf-8"))

    def receive(
        self, max_bytes: int = DEFAULT_RECEIVE_MAX_BYTES
    ) -> wsproto.events.Event:
        while len(self._events) == 0:
            data = self.stream.read(max_bytes=max_bytes)
            self.connection.receive_data(data)
            self._events.extend(self.connection.events())
        event = self._events.popleft()
        if isinstance(event, wsproto.events.CloseConnection):
            raise WebSocketDisconnect(event.code, event.reason)
        return event

    def receive_text(self, max_bytes: int = DEFAULT_RECEIVE_MAX_BYTES) -> str:
        event = self.receive(max_bytes)
        if isinstance(event, wsproto.events.TextMessage):
            return event.data
        raise WebSocketInvalidTypeReceived(event)

    def receive_bytes(self, max_bytes: int = DEFAULT_RECEIVE_MAX_BYTES) -> bytes:
        event = self.receive(max_bytes)
        if isinstance(event, wsproto.events.BytesMessage):
            return event.data
        raise WebSocketInvalidTypeReceived(event)

    def receive_json(
        self, max_bytes: int = DEFAULT_RECEIVE_MAX_BYTES, mode: JSONMode = "text"
    ) -> typing.Any:
        assert mode in ["text", "binary"]
        data: typing.Union[str, bytes]
        if mode == "text":
            data = self.receive_text(max_bytes)
        elif mode == "binary":
            data = self.receive_bytes(max_bytes)
        return json.loads(data)

    def close(self, code: int = 1000, reason: typing.Optional[str] = None):
        if self.connection.state != wsproto.connection.ConnectionState.CLOSED:
            event = wsproto.events.CloseConnection(code, reason)
            try:
                self._send_event(event)
            except httpcore.WriteError:
                pass

    def _send_event(self, event: wsproto.events.Event):
        data = self.connection.send(event)
        self.stream.write(data)


class AsyncWebSocketSession:
    def __init__(self, response: httpx.Response) -> None:
        self.stream: AsyncNetworkStream = response.extensions["network_stream"]
        self.connection = wsproto.Connection(wsproto.ConnectionType.CLIENT)
        self._events: typing.Deque[wsproto.events.Event] = collections.deque()

    async def ping(self, payload: bytes = b"") -> None:
        event = wsproto.events.Ping(payload)
        await self._send_event(event)

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

    async def receive(
        self, max_bytes: int = DEFAULT_RECEIVE_MAX_BYTES
    ) -> wsproto.events.Event:
        while len(self._events) == 0:
            data = await self.stream.read(max_bytes=max_bytes)
            self.connection.receive_data(data)
            self._events.extend(self.connection.events())
        event = self._events.popleft()
        if isinstance(event, wsproto.events.CloseConnection):
            raise WebSocketDisconnect(event.code, event.reason)
        return event

    async def receive_text(self, max_bytes: int = DEFAULT_RECEIVE_MAX_BYTES) -> str:
        event = await self.receive(max_bytes)
        if isinstance(event, wsproto.events.TextMessage):
            return event.data
        raise WebSocketInvalidTypeReceived(event)

    async def receive_bytes(self, max_bytes: int = DEFAULT_RECEIVE_MAX_BYTES) -> bytes:
        event = await self.receive(max_bytes)
        if isinstance(event, wsproto.events.BytesMessage):
            return event.data
        raise WebSocketInvalidTypeReceived(event)

    async def receive_json(
        self, max_bytes: int = DEFAULT_RECEIVE_MAX_BYTES, mode: JSONMode = "text"
    ) -> typing.Any:
        assert mode in ["text", "binary"]
        data: typing.Union[str, bytes]
        if mode == "text":
            data = await self.receive_text(max_bytes)
        elif mode == "binary":
            data = await self.receive_bytes(max_bytes)
        return json.loads(data)

    async def close(self, code: int = 1000, reason: typing.Optional[str] = None):
        if self.connection.state != wsproto.connection.ConnectionState.CLOSED:
            event = wsproto.events.CloseConnection(code, reason)
            try:
                await self._send_event(event)
            except httpcore.WriteError:
                pass

    async def _send_event(self, event: wsproto.events.Event):
        data = self.connection.send(event)
        await self.stream.write(data)


def _get_headers() -> typing.Dict[str, typing.Any]:
    return {
        "connection": "upgrade",
        "upgrade": "websocket",
        "sec-websocket-key": base64.b64encode(secrets.token_bytes(16)).decode("utf-8"),
        "sec-websocket-version": "13",
    }


@contextlib.contextmanager
def connect_ws(
    url: str, client: typing.Optional[httpx.Client] = None, **kwargs: typing.Any
) -> typing.Generator[WebSocketSession, None, None]:
    client = httpx.Client() if client is None else client
    headers = kwargs.pop("headers", {})
    headers.update(_get_headers())

    with client.stream("GET", url, headers=headers, **kwargs) as response:
        if response.status_code != 101:
            raise WebSocketUpgradeError(response)

        session = WebSocketSession(response)
        yield session
        session.close()


@contextlib.asynccontextmanager
async def aconnect_ws(
    url: str, client: typing.Optional[httpx.AsyncClient] = None, **kwargs: typing.Any
) -> typing.AsyncGenerator[AsyncWebSocketSession, None]:
    client = httpx.AsyncClient() if client is None else client
    headers = kwargs.pop("headers", {})
    headers.update(_get_headers())

    async with client.stream("GET", url, headers=headers, **kwargs) as response:
        if response.status_code != 101:
            raise WebSocketUpgradeError(response)

        session = AsyncWebSocketSession(response)
        yield session
        await session.close()
