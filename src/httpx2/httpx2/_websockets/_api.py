import asyncio
import base64
import contextlib
import json
import queue
import secrets
import sys
import threading
import typing

if sys.version_info < (3, 8):
    from typing_extensions import Literal  # pragma: no cover
else:
    from typing import Literal  # pragma: no cover

import httpcore
import httpx
import wsproto
from httpcore.backends.base import AsyncNetworkStream, NetworkStream

from httpx_ws._ping import AsyncPingManager, PingManager

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
    def __init__(self, stream: NetworkStream) -> None:
        self.stream = stream
        self.connection = wsproto.Connection(wsproto.ConnectionType.CLIENT)
        self._events: queue.Queue[wsproto.events.Event] = queue.Queue()

        self._ping_manager = PingManager()

        self._should_close = False
        self._background_receive_task = threading.Thread(
            target=self._background_receive
        )
        self._background_receive_task.start()

    def ping(self, payload: bytes = b"") -> threading.Event:
        ping_id, callback = self._ping_manager.create(payload)
        event = wsproto.events.Ping(ping_id)
        self._send_event(event)
        return callback

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

    def receive(self, timeout: typing.Optional[float] = None) -> wsproto.events.Event:
        event = self._events.get(block=True, timeout=timeout)
        if isinstance(event, wsproto.events.CloseConnection):
            raise WebSocketDisconnect(event.code, event.reason)
        return event

    def receive_text(self, timeout: typing.Optional[float] = None) -> str:
        event = self.receive(timeout)
        if isinstance(event, wsproto.events.TextMessage):
            return event.data
        raise WebSocketInvalidTypeReceived(event)

    def receive_bytes(self, timeout: typing.Optional[float] = None) -> bytes:
        event = self.receive(timeout)
        if isinstance(event, wsproto.events.BytesMessage):
            return event.data
        raise WebSocketInvalidTypeReceived(event)

    def receive_json(
        self, timeout: typing.Optional[float] = None, mode: JSONMode = "text"
    ) -> typing.Any:
        assert mode in ["text", "binary"]
        data: typing.Union[str, bytes]
        if mode == "text":
            data = self.receive_text(timeout)
        elif mode == "binary":
            data = self.receive_bytes(timeout)
        return json.loads(data)

    def close(self, code: int = 1000, reason: typing.Optional[str] = None):
        self._should_close = True
        if self.connection.state != wsproto.connection.ConnectionState.CLOSED:
            event = wsproto.events.CloseConnection(code, reason)
            try:
                self._send_event(event)
            except httpcore.WriteError:
                pass

    def _background_receive(self, max_bytes: int = DEFAULT_RECEIVE_MAX_BYTES) -> None:
        try:
            while not self._should_close:
                data = self.stream.read(max_bytes=max_bytes)
                self.connection.receive_data(data)
                for event in self.connection.events():
                    if isinstance(event, wsproto.events.Ping):
                        self.send(event.response())
                        continue
                    if isinstance(event, wsproto.events.Pong):
                        self._ping_manager.ack(event.payload)
                        continue
                    if isinstance(event, wsproto.events.CloseConnection):
                        self._should_close = True
                    self._events.put(event)
        except httpcore.ReadError:
            pass

    def _send_event(self, event: wsproto.events.Event):
        data = self.connection.send(event)
        self.stream.write(data)


class AsyncWebSocketSession:
    def __init__(self, stream: AsyncNetworkStream) -> None:
        self.stream = stream
        self.connection = wsproto.Connection(wsproto.ConnectionType.CLIENT)
        self._events: asyncio.Queue[wsproto.events.Event] = asyncio.Queue()

        self._ping_manager = AsyncPingManager()

        self._should_close = False
        self._background_receive_task = asyncio.create_task(self._background_receive())

    async def ping(self, payload: bytes = b"") -> asyncio.Event:
        ping_id, callback = self._ping_manager.create(payload)
        event = wsproto.events.Ping(ping_id)
        await self._send_event(event)
        return callback

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
        self, timeout: typing.Optional[float] = None
    ) -> wsproto.events.Event:
        event = await asyncio.wait_for(self._events.get(), timeout)
        if isinstance(event, wsproto.events.CloseConnection):
            raise WebSocketDisconnect(event.code, event.reason)
        return event

    async def receive_text(self, timeout: typing.Optional[float] = None) -> str:
        event = await self.receive(timeout)
        if isinstance(event, wsproto.events.TextMessage):
            return event.data
        raise WebSocketInvalidTypeReceived(event)

    async def receive_bytes(self, timeout: typing.Optional[float] = None) -> bytes:
        event = await self.receive(timeout)
        if isinstance(event, wsproto.events.BytesMessage):
            return event.data
        raise WebSocketInvalidTypeReceived(event)

    async def receive_json(
        self, timeout: typing.Optional[float] = None, mode: JSONMode = "text"
    ) -> typing.Any:
        assert mode in ["text", "binary"]
        data: typing.Union[str, bytes]
        if mode == "text":
            data = await self.receive_text(timeout)
        elif mode == "binary":
            data = await self.receive_bytes(timeout)
        return json.loads(data)

    async def close(self, code: int = 1000, reason: typing.Optional[str] = None):
        self._should_close = True
        if self.connection.state != wsproto.connection.ConnectionState.CLOSED:
            event = wsproto.events.CloseConnection(code, reason)
            try:
                await self._send_event(event)
            except httpcore.WriteError:
                pass

    async def _background_receive(
        self, max_bytes: int = DEFAULT_RECEIVE_MAX_BYTES
    ) -> None:
        try:
            while not self._should_close:
                data = await self.stream.read(max_bytes=max_bytes)
                self.connection.receive_data(data)
                for event in self.connection.events():
                    if isinstance(event, wsproto.events.Ping):
                        await self.send(event.response())
                        continue
                    if isinstance(event, wsproto.events.Pong):
                        self._ping_manager.ack(event.payload)
                        continue
                    if isinstance(event, wsproto.events.CloseConnection):
                        self._should_close = True
                    await self._events.put(event)
        except httpcore.ReadError:
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

        session = WebSocketSession(response.extensions["network_stream"])
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

        session = AsyncWebSocketSession(response.extensions["network_stream"])
        yield session
        await session.close()
