from __future__ import annotations

import base64
import concurrent.futures
import contextlib
import json
import queue
import secrets
import threading
import typing
from collections.abc import AsyncGenerator, Generator
from types import TracebackType

import anyio
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from websockets.exceptions import InvalidState
from websockets.frames import Close, Frame, Opcode
from websockets.protocol import Protocol, Side, State

from .._models import Headers
from .._urls import URL
from ._exceptions import (
    WebSocketDisconnect,
    WebSocketException,
    WebSocketInvalidTypeReceived,
    WebSocketNetworkError,
    WebSocketUpgradeError,
)
from ._ping import AsyncPingManager, PingManager
from ._transport import ASGIWebSocketAsyncNetworkStream

if typing.TYPE_CHECKING:
    from httpcore2 import AsyncNetworkStream, NetworkStream

    from .._client import AsyncClient, Client, UseClientDefault
    from .._models import Response
    from .._types import AuthTypes, CookieTypes, HeaderTypes, QueryParamTypes, RequestExtensions, TimeoutTypes

JSONMode = typing.Literal["text", "binary"]
TaskResult = typing.TypeVar("TaskResult")

DEFAULT_MAX_MESSAGE_SIZE_BYTES = 65_536
DEFAULT_QUEUE_SIZE = 512
DEFAULT_KEEPALIVE_PING_INTERVAL_SECONDS = 20.0
DEFAULT_KEEPALIVE_PING_TIMEOUT_SECONDS = 20.0

INTERNAL_ERROR = 1011


class ShouldClose(Exception):
    pass


class EndOfStream(Exception):
    pass


class MessageAssembler:
    """
    Assembles data frames, possibly fragmented, into complete messages.
    """

    def __init__(self) -> None:
        self._buffer = bytearray()
        self._text = False

    def feed(self, frame: Frame) -> str | bytes | None:
        if frame.opcode is Opcode.TEXT or frame.opcode is Opcode.BINARY:
            self._buffer = bytearray(frame.data)
            self._text = frame.opcode is Opcode.TEXT
        else:
            self._buffer += frame.data
        if not frame.fin:
            return None
        data = bytes(self._buffer)
        self._buffer = bytearray()
        return data.decode("utf-8") if self._text else data


class WebSocketSession:
    """
    Sync context manager representing an opened WebSocket session.

    Attributes:
        subprotocol: Optional protocol that has been accepted by the server.
        response: The WebSocket handshake response.
    """

    subprotocol: str | None
    response: Response | None

    def __init__(
        self,
        stream: NetworkStream,
        *,
        max_message_size_bytes: int = DEFAULT_MAX_MESSAGE_SIZE_BYTES,
        queue_size: int = DEFAULT_QUEUE_SIZE,
        keepalive_ping_interval_seconds: float | None = DEFAULT_KEEPALIVE_PING_INTERVAL_SECONDS,
        keepalive_ping_timeout_seconds: float | None = DEFAULT_KEEPALIVE_PING_TIMEOUT_SECONDS,
        response: Response | None = None,
    ) -> None:
        self.stream = stream
        self.protocol = Protocol(Side.CLIENT, state=State.OPEN, max_size=None)
        self.response = response
        if self.response is not None:
            self.subprotocol = self.response.headers.get("sec-websocket-protocol")
        else:
            self.subprotocol = None

        self._events: queue.Queue[str | bytes | WebSocketException] = queue.Queue(queue_size)
        self._assembler = MessageAssembler()

        self._ping_manager = PingManager()
        self._should_close = threading.Event()
        self._write_lock = threading.Lock()
        self._should_close_task: concurrent.futures.Future[bool] | None = None
        self._executor: concurrent.futures.ThreadPoolExecutor | None = None

        self._max_message_size_bytes = max_message_size_bytes
        self._queue_size = queue_size
        self._keepalive_ping_interval_seconds = keepalive_ping_interval_seconds
        self._keepalive_ping_timeout_seconds = keepalive_ping_timeout_seconds

    def _get_executor_should_close_task(
        self,
    ) -> tuple[concurrent.futures.ThreadPoolExecutor, concurrent.futures.Future[bool]]:
        if self._should_close_task is None:
            self._executor = concurrent.futures.ThreadPoolExecutor()
            self._should_close_task = self._executor.submit(self._should_close.wait)
        assert self._executor is not None
        return self._executor, self._should_close_task

    def __enter__(self) -> WebSocketSession:
        self._background_receive_task = threading.Thread(
            target=self._background_receive, args=(self._max_message_size_bytes,)
        )
        self._background_receive_task.start()

        self._background_keepalive_ping_task: threading.Thread | None = None
        if self._keepalive_ping_interval_seconds is not None:
            self._background_keepalive_ping_task = threading.Thread(
                target=self._background_keepalive_ping,
                args=(
                    self._keepalive_ping_interval_seconds,
                    self._keepalive_ping_timeout_seconds,
                ),
            )
            self._background_keepalive_ping_task.start()

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()
        self._background_receive_task.join()
        if self._background_keepalive_ping_task is not None:
            self._background_keepalive_ping_task.join()

    def ping(self, payload: bytes = b"") -> threading.Event:
        """
        Send a Ping message.

        The payload is used internally to track this specific event.
        If left empty, a random one will be generated.

        Returns an event that can be used to wait for the corresponding Pong response:

        ```python
        pong_callback = ws.ping()
        pong_callback.wait()
        ```
        """
        ping_id, callback = self._ping_manager.create(payload)
        self._send(self.protocol.send_ping, ping_id)
        return callback

    def send_text(self, data: str) -> None:
        """
        Send a text message.

        Raises `WebSocketNetworkError` if a network error occurred.
        """
        self._send(self.protocol.send_text, data.encode("utf-8"))

    def send_bytes(self, data: bytes) -> None:
        """
        Send a bytes message.

        Raises `WebSocketNetworkError` if a network error occurred.
        """
        self._send(self.protocol.send_binary, data)

    def send_json(self, data: typing.Any, mode: JSONMode = "text") -> None:
        """
        Send JSON data, serialized with `json.dumps()`, in `'text'` or `'binary'` mode.

        Raises `WebSocketNetworkError` if a network error occurred.
        """
        assert mode in ["text", "binary"]
        serialized_data = json.dumps(data)
        if mode == "text":
            self.send_text(serialized_data)
        else:
            self.send_bytes(serialized_data.encode("utf-8"))

    def receive(self, timeout: float | None = None) -> str | bytes:
        """
        Receive a message from the server, either text or bytes.

        If `timeout` is `None`, this blocks until a message is available.

        Raises:
            TimeoutError: No message was received before the timeout delay.
            WebSocketDisconnect: The server closed the WebSocket.
            WebSocketNetworkError: A network error occurred.
        """
        try:
            event = self._events.get(block=True, timeout=timeout)
        except queue.Empty as e:
            raise TimeoutError from e
        if isinstance(event, WebSocketException):
            raise event
        return event

    def receive_text(self, timeout: float | None = None) -> str:
        """
        Receive text from the server.

        If `timeout` is `None`, this blocks until a message is available.

        Raises:
            TimeoutError: No message was received before the timeout delay.
            WebSocketDisconnect: The server closed the WebSocket.
            WebSocketNetworkError: A network error occurred.
            WebSocketInvalidTypeReceived: The received message was not a text message.
        """
        message = self.receive(timeout)
        if isinstance(message, str):
            return message
        raise WebSocketInvalidTypeReceived(message)

    def receive_bytes(self, timeout: float | None = None) -> bytes:
        """
        Receive bytes from the server.

        If `timeout` is `None`, this blocks until a message is available.

        Raises:
            TimeoutError: No message was received before the timeout delay.
            WebSocketDisconnect: The server closed the WebSocket.
            WebSocketNetworkError: A network error occurred.
            WebSocketInvalidTypeReceived: The received message was not a bytes message.
        """
        message = self.receive(timeout)
        if isinstance(message, bytes):
            return message
        raise WebSocketInvalidTypeReceived(message)

    def receive_json(self, timeout: float | None = None, mode: JSONMode = "text") -> typing.Any:
        """
        Receive JSON data from the server, parsed with `json.loads()`, in `'text'` or `'binary'` mode.

        If `timeout` is `None`, this blocks until a message is available.

        Raises:
            TimeoutError: No message was received before the timeout delay.
            WebSocketDisconnect: The server closed the WebSocket.
            WebSocketNetworkError: A network error occurred.
            WebSocketInvalidTypeReceived: The received message didn't correspond to the specified mode.
        """
        assert mode in ["text", "binary"]
        data: str | bytes
        if mode == "text":
            data = self.receive_text(timeout)
        else:
            data = self.receive_bytes(timeout)
        return json.loads(data)

    def close(self, code: int = 1000, reason: str | None = None) -> None:
        """
        Close the WebSocket session.

        Internally, it'll send a Close frame.

        *This method is automatically called when exiting the context manager.*
        """
        import httpcore2

        self._should_close.set()
        if self._executor is not None:
            self._executor.shutdown(False)
        try:
            with self._write_lock:
                if self.protocol.state is State.OPEN:
                    self.protocol.send_close(code, reason or "")
                    self._write_protocol_data()
        except (httpcore2.WriteError, InvalidState):
            pass
        self.stream.close()

    def _send(self, send_event: typing.Callable[[bytes], None], data: bytes) -> None:
        import httpcore2

        try:
            with self._write_lock:
                send_event(data)
                self._write_protocol_data()
        except httpcore2.WriteError as e:
            self.close(INTERNAL_ERROR, "Stream write error")
            raise WebSocketNetworkError() from e

    def _write_protocol_data(self) -> None:
        for data in self.protocol.data_to_send():
            if data:
                self.stream.write(data)

    def _background_receive(self, max_bytes: int) -> None:
        """
        Background thread listening for data from the server.

        Internally, it'll:

        * Answer to Ping frames.
        * Acknowledge Pong frames.
        * Put messages in the `_events` queue that'll eventually be consumed by the user.
        """
        import httpcore2

        try:
            while not self._should_close.is_set():
                data = self._wait_until_closed(self._read_stream, max_bytes)
                # The protocol is not thread-safe: keep every interaction with it
                # under the write lock, so it can't race user sends and closes.
                with self._write_lock:
                    self.protocol.receive_data(data)
                    frames = self.protocol.events_received()
                    try:
                        self._write_protocol_data()
                    except httpcore2.WriteError:
                        # Tolerate failing to reply once the peer started the closing handshake.
                        if self.protocol.state is State.OPEN:
                            raise
                for frame in frames:
                    assert isinstance(frame, Frame)
                    if frame.opcode is Opcode.PING:
                        continue
                    if frame.opcode is Opcode.PONG:
                        self._ping_manager.ack(frame.data)
                        continue
                    if frame.opcode is Opcode.CLOSE:
                        self._should_close.set()
                        close = Close.parse(frame.data)
                        self._events.put(WebSocketDisconnect(close.code, close.reason))
                        continue
                    message = self._assembler.feed(frame)
                    if message is not None:
                        self._events.put(message)
        except (httpcore2.ReadError, httpcore2.WriteError, EndOfStream):
            self.close(INTERNAL_ERROR, "Stream error")
            self._events.put(WebSocketNetworkError())
        except ShouldClose:
            pass

    def _background_keepalive_ping(self, interval_seconds: float, timeout_seconds: float | None = None) -> None:
        try:
            while not self._should_close.is_set():
                should_close = self._wait_until_closed(self._should_close.wait, interval_seconds)
                if should_close:  # pragma: no cover
                    raise ShouldClose()

                try:
                    pong_callback = self.ping()
                # Connection is closing, exit the task
                except InvalidState:
                    return

                if timeout_seconds is not None:
                    acknowledged = self._wait_until_closed(pong_callback.wait, timeout_seconds)
                    if not acknowledged:
                        self.close(INTERNAL_ERROR, "Keepalive ping timeout")
                        self._events.put(WebSocketNetworkError())
        except ShouldClose:
            pass

    def _wait_until_closed(
        self, callable: typing.Callable[..., TaskResult], *args: typing.Any, **kwargs: typing.Any
    ) -> TaskResult:
        try:
            executor, should_close_task = self._get_executor_should_close_task()
            todo_task = executor.submit(callable, *args, **kwargs)
        except RuntimeError as e:
            raise ShouldClose() from e
        else:
            done, _ = concurrent.futures.wait(
                (todo_task, should_close_task),  # type: ignore[misc]
                return_when=concurrent.futures.FIRST_COMPLETED,
            )
            if should_close_task in done:
                raise ShouldClose()
            assert todo_task in done
            result = todo_task.result()
        return result

    def _read_stream(self, max_bytes: int) -> bytes:
        data = self.stream.read(max_bytes)
        if data == b"":
            raise EndOfStream()
        return data


class AsyncWebSocketSession(anyio.AsyncContextManagerMixin):
    """
    Async context manager representing an opened WebSocket session.

    Internally, this session uses an anyio task group to manage background tasks.
    As a result, exceptions that are not caught inside the context manager
    and propagate out of the `async with` block will be wrapped in an `ExceptionGroup`.

    To handle them, use the `except*` syntax:

    ```python
    async with AsyncWebSocketSession(stream) as ws:
        try:
            data = await ws.receive_text()
        except WebSocketDisconnect:
            # Caught inside the context manager: plain exception.
            print("Connection closed")

    # If not caught inside:
    try:
        async with AsyncWebSocketSession(stream) as ws:
            data = await ws.receive_text()
    except* WebSocketDisconnect:
        # Propagated out of the context manager: wrapped in ExceptionGroup.
        print("Connection closed")
    ```

    Attributes:
        subprotocol: Optional protocol that has been accepted by the server.
        response: The WebSocket handshake response.
    """

    subprotocol: str | None
    response: Response | None
    _send_event: MemoryObjectSendStream[str | bytes | WebSocketException]
    _receive_event: MemoryObjectReceiveStream[str | bytes | WebSocketException]

    def __init__(
        self,
        stream: AsyncNetworkStream,
        *,
        max_message_size_bytes: int = DEFAULT_MAX_MESSAGE_SIZE_BYTES,
        queue_size: int = DEFAULT_QUEUE_SIZE,
        keepalive_ping_interval_seconds: float | None = DEFAULT_KEEPALIVE_PING_INTERVAL_SECONDS,
        keepalive_ping_timeout_seconds: float | None = DEFAULT_KEEPALIVE_PING_TIMEOUT_SECONDS,
        response: Response | None = None,
    ) -> None:
        self.stream = stream
        self.protocol = Protocol(Side.CLIENT, state=State.OPEN, max_size=None)
        self.response = response
        if self.response is not None:
            self.subprotocol = self.response.headers.get("sec-websocket-protocol")
        else:
            self.subprotocol = None

        self._ping_manager = AsyncPingManager()
        self._should_close = anyio.Event()
        self._write_lock = anyio.Lock()
        self._assembler = MessageAssembler()

        self._max_message_size_bytes = max_message_size_bytes
        self._queue_size = queue_size

        # Always disable keepalive ping when emulating ASGI
        if isinstance(stream, ASGIWebSocketAsyncNetworkStream):
            self._keepalive_ping_interval_seconds = None
            self._keepalive_ping_timeout_seconds = None
        else:
            self._keepalive_ping_interval_seconds = keepalive_ping_interval_seconds
            self._keepalive_ping_timeout_seconds = keepalive_ping_timeout_seconds

    @contextlib.asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[AsyncWebSocketSession]:
        self._send_event, self._receive_event = anyio.create_memory_object_stream[str | bytes | WebSocketException]()
        self._background_task_group = anyio.create_task_group()

        async with self._send_event, self._receive_event, self._background_task_group:
            self._background_task_group.start_soon(self._background_receive, self._max_message_size_bytes)
            if self._keepalive_ping_interval_seconds is not None:
                self._background_task_group.start_soon(
                    self._background_keepalive_ping,
                    self._keepalive_ping_interval_seconds,
                    self._keepalive_ping_timeout_seconds,
                )

            try:
                yield self
            finally:
                self._background_task_group.cancel_scope.cancel()
                with anyio.CancelScope(shield=True):
                    await self.close()

    async def ping(self, payload: bytes = b"") -> anyio.Event:
        """
        Send a Ping message.

        The payload is used internally to track this specific event.
        If left empty, a random one will be generated.

        Returns an event that can be used to wait for the corresponding Pong response:

        ```python
        pong_callback = await ws.ping()
        await pong_callback.wait()
        ```
        """
        ping_id, callback = self._ping_manager.create(payload)
        await self._send(self.protocol.send_ping, ping_id)
        return callback

    async def send_text(self, data: str) -> None:
        """
        Send a text message.

        Raises `WebSocketNetworkError` if a network error occurred.
        """
        await self._send(self.protocol.send_text, data.encode("utf-8"))

    async def send_bytes(self, data: bytes) -> None:
        """
        Send a bytes message.

        Raises `WebSocketNetworkError` if a network error occurred.
        """
        await self._send(self.protocol.send_binary, data)

    async def send_json(self, data: typing.Any, mode: JSONMode = "text") -> None:
        """
        Send JSON data, serialized with `json.dumps()`, in `'text'` or `'binary'` mode.

        Raises `WebSocketNetworkError` if a network error occurred.
        """
        assert mode in ["text", "binary"]
        serialized_data = json.dumps(data)
        if mode == "text":
            await self.send_text(serialized_data)
        else:
            await self.send_bytes(serialized_data.encode("utf-8"))

    async def receive(self, timeout: float | None = None) -> str | bytes:
        """
        Receive a message from the server, either text or bytes.

        If `timeout` is `None`, this blocks until a message is available.

        Raises:
            TimeoutError: No message was received before the timeout delay.
            WebSocketDisconnect: The server closed the WebSocket.
            WebSocketNetworkError: A network error occurred.
        """
        with anyio.fail_after(timeout):
            event = await self._receive_event.receive()
        if isinstance(event, WebSocketException):
            raise event
        return event

    async def receive_text(self, timeout: float | None = None) -> str:
        """
        Receive text from the server.

        If `timeout` is `None`, this blocks until a message is available.

        Raises:
            TimeoutError: No message was received before the timeout delay.
            WebSocketDisconnect: The server closed the WebSocket.
            WebSocketNetworkError: A network error occurred.
            WebSocketInvalidTypeReceived: The received message was not a text message.
        """
        message = await self.receive(timeout)
        if isinstance(message, str):
            return message
        raise WebSocketInvalidTypeReceived(message)

    async def receive_bytes(self, timeout: float | None = None) -> bytes:
        """
        Receive bytes from the server.

        If `timeout` is `None`, this blocks until a message is available.

        Raises:
            TimeoutError: No message was received before the timeout delay.
            WebSocketDisconnect: The server closed the WebSocket.
            WebSocketNetworkError: A network error occurred.
            WebSocketInvalidTypeReceived: The received message was not a bytes message.
        """
        message = await self.receive(timeout)
        if isinstance(message, bytes):
            return message
        raise WebSocketInvalidTypeReceived(message)

    async def receive_json(self, timeout: float | None = None, mode: JSONMode = "text") -> typing.Any:
        """
        Receive JSON data from the server, parsed with `json.loads()`, in `'text'` or `'binary'` mode.

        If `timeout` is `None`, this blocks until a message is available.

        Raises:
            TimeoutError: No message was received before the timeout delay.
            WebSocketDisconnect: The server closed the WebSocket.
            WebSocketNetworkError: A network error occurred.
            WebSocketInvalidTypeReceived: The received message didn't correspond to the specified mode.
        """
        assert mode in ["text", "binary"]
        data: str | bytes
        if mode == "text":
            data = await self.receive_text(timeout)
        else:
            data = await self.receive_bytes(timeout)
        return json.loads(data)

    async def close(self, code: int = 1000, reason: str | None = None) -> None:
        """
        Close the WebSocket session.

        Internally, it'll send a Close frame.

        *This method is automatically called when exiting the context manager.*
        """
        import httpcore2

        self._should_close.set()
        try:
            async with self._write_lock:
                if self.protocol.state is State.OPEN:
                    self.protocol.send_close(code, reason or "")
                    await self._write_protocol_data()
        except (httpcore2.WriteError, InvalidState):
            pass
        await self.stream.aclose()

    async def _send(self, send_event: typing.Callable[[bytes], None], data: bytes) -> None:
        import httpcore2

        try:
            async with self._write_lock:
                send_event(data)
                await self._write_protocol_data()
        except httpcore2.WriteError as e:
            await self.close(INTERNAL_ERROR, "Stream write error")
            raise WebSocketNetworkError() from e

    async def _write_protocol_data(self) -> None:
        for data in self.protocol.data_to_send():
            if data:
                await self.stream.write(data)

    async def _background_receive(self, max_bytes: int) -> None:
        """
        Background task listening for data from the server.

        Internally, it'll:

        * Answer to Ping frames.
        * Acknowledge Pong frames.
        * Put messages in the `_events` queue that'll eventually be consumed by the user.
        """
        import httpcore2

        try:
            while not self._should_close.is_set():
                data = await self._read_stream(max_bytes)
                async with self._write_lock:
                    self.protocol.receive_data(data)
                    frames = self.protocol.events_received()
                    try:
                        await self._write_protocol_data()
                    except httpcore2.WriteError:
                        # Tolerate failing to reply once the peer started the closing handshake.
                        if self.protocol.state is State.OPEN:
                            raise
                for frame in frames:
                    assert isinstance(frame, Frame)
                    if frame.opcode is Opcode.PING:
                        continue
                    if frame.opcode is Opcode.PONG:
                        self._ping_manager.ack(frame.data)
                        continue
                    if frame.opcode is Opcode.CLOSE:
                        self._should_close.set()
                        close = Close.parse(frame.data)
                        await self._send_event.send(WebSocketDisconnect(close.code, close.reason))
                        continue
                    message = self._assembler.feed(frame)
                    if message is not None:
                        await self._send_event.send(message)
        except (httpcore2.ReadError, httpcore2.WriteError, EndOfStream):
            await self.close(INTERNAL_ERROR, "Stream error")
            await self._send_event.send(WebSocketNetworkError())

    async def _background_keepalive_ping(self, interval_seconds: float, timeout_seconds: float | None = None) -> None:
        while not self._should_close.is_set():
            await anyio.sleep(interval_seconds)

            try:
                pong_callback = await self.ping()
            # Connection is closing, exit the task
            except InvalidState:
                return

            if timeout_seconds is not None:
                try:
                    with anyio.fail_after(timeout_seconds):
                        await pong_callback.wait()
                except TimeoutError:
                    await self.close(INTERNAL_ERROR, "Keepalive ping timeout")
                    await self._send_event.send(WebSocketNetworkError())

    async def _read_stream(self, max_bytes: int) -> bytes:
        data = await self.stream.read(max_bytes)
        if data == b"":
            raise EndOfStream()
        return data


def _get_headers(subprotocols: list[str] | None) -> dict[str, str]:
    headers = {
        "connection": "upgrade",
        "upgrade": "websocket",
        "sec-websocket-key": base64.b64encode(secrets.token_bytes(16)).decode("utf-8"),
        "sec-websocket-version": "13",
    }
    if subprotocols is not None:
        headers["sec-websocket-protocol"] = ", ".join(subprotocols)
    return headers


def _get_url(url: URL | str) -> URL:
    url = URL(url)
    if url.scheme == "ws":
        return url.copy_with(scheme="http")
    if url.scheme == "wss":
        return url.copy_with(scheme="https")
    return url


@contextlib.contextmanager
def connect_ws(
    client: Client,
    url: URL | str,
    *,
    params: QueryParamTypes | None,
    headers: HeaderTypes | None,
    cookies: CookieTypes | None,
    auth: AuthTypes | UseClientDefault | None,
    follow_redirects: bool | UseClientDefault,
    timeout: TimeoutTypes | UseClientDefault,
    extensions: RequestExtensions | None,
    subprotocols: list[str] | None,
    max_message_size_bytes: int,
    queue_size: int,
    keepalive_ping_interval_seconds: float | None,
    keepalive_ping_timeout_seconds: float | None,
) -> Generator[WebSocketSession]:
    merged_headers = Headers(headers)
    merged_headers.update(_get_headers(subprotocols))

    with client.stream(
        "GET",
        _get_url(url),
        params=params,
        headers=merged_headers,
        cookies=cookies,
        auth=auth,
        follow_redirects=follow_redirects,
        timeout=timeout,
        extensions=extensions,
    ) as response:
        if response.status_code != 101:
            raise WebSocketUpgradeError(response)

        session = WebSocketSession(
            response.extensions["network_stream"],
            max_message_size_bytes=max_message_size_bytes,
            queue_size=queue_size,
            keepalive_ping_interval_seconds=keepalive_ping_interval_seconds,
            keepalive_ping_timeout_seconds=keepalive_ping_timeout_seconds,
            response=response,
        )
        with session:
            yield session


@contextlib.asynccontextmanager
async def aconnect_ws(
    client: AsyncClient,
    url: URL | str,
    *,
    params: QueryParamTypes | None,
    headers: HeaderTypes | None,
    cookies: CookieTypes | None,
    auth: AuthTypes | UseClientDefault | None,
    follow_redirects: bool | UseClientDefault,
    timeout: TimeoutTypes | UseClientDefault,
    extensions: RequestExtensions | None,
    subprotocols: list[str] | None,
    max_message_size_bytes: int,
    queue_size: int,
    keepalive_ping_interval_seconds: float | None,
    keepalive_ping_timeout_seconds: float | None,
) -> AsyncGenerator[AsyncWebSocketSession]:
    merged_headers = Headers(headers)
    merged_headers.update(_get_headers(subprotocols))

    async with client.stream(
        "GET",
        _get_url(url),
        params=params,
        headers=merged_headers,
        cookies=cookies,
        auth=auth,
        follow_redirects=follow_redirects,
        timeout=timeout,
        extensions=extensions,
    ) as response:
        if response.status_code != 101:
            raise WebSocketUpgradeError(response)

        session = AsyncWebSocketSession(
            response.extensions["network_stream"],
            max_message_size_bytes=max_message_size_bytes,
            queue_size=queue_size,
            keepalive_ping_interval_seconds=keepalive_ping_interval_seconds,
            keepalive_ping_timeout_seconds=keepalive_ping_timeout_seconds,
            response=response,
        )
        async with session:
            yield session
