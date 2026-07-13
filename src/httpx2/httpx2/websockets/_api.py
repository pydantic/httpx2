from __future__ import annotations

import base64
import concurrent.futures
import contextlib
import json
import queue
import secrets
import threading
import typing
from types import TracebackType

import anyio
import wsproto
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from wsproto.frame_protocol import CloseReason

from .._client import USE_CLIENT_DEFAULT
from .._config import (
    DEFAULT_KEEPALIVE_PING_INTERVAL_SECONDS,
    DEFAULT_KEEPALIVE_PING_TIMEOUT_SECONDS,
    DEFAULT_MAX_MESSAGE_SIZE_BYTES,
    DEFAULT_QUEUE_SIZE,
)
from .._models import Headers
from ._exceptions import (
    HTTPXWSException,
    WebSocketDisconnect,
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
    from .._types import (
        AuthTypes,
        CookieTypes,
        HeaderTypes,
        QueryParamTypes,
        RequestExtensions,
        TimeoutTypes,
    )

JSONMode = typing.Literal["text", "binary"]
TaskFunction = typing.TypeVar("TaskFunction")
TaskResult = typing.TypeVar("TaskResult")


class ShouldClose(Exception):
    pass


class WebSocketSession:
    """
    Sync context manager representing an opened WebSocket session.

    Attributes:
        subprotocol (typing.Optional[str]):
            Optional protocol that has been accepted by the server.
        response (Response | None):
            The webSocket handshake response.
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
        self.connection = wsproto.connection.Connection(wsproto.ConnectionType.CLIENT)
        self.response = response
        if self.response is not None:
            self.subprotocol = self.response.headers.get("sec-websocket-protocol")
        else:
            self.subprotocol = None

        self._events: queue.Queue[wsproto.events.Event | HTTPXWSException] = queue.Queue(queue_size)

        self._ping_manager = PingManager()
        self._should_close = threading.Event()
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

        Args:
            payload:
                Payload to attach to the Ping event.
                Internally, it's used to track this specific event.
                If left empty, a random one will be generated.

        Returns:
            An event that can be used to wait for the corresponding Pong response.

        Examples:
            Send a Ping and wait for the Pong

                pong_callback = ws.ping()
                # Will block until the corresponding Pong is received.
                pong_callback.wait()
        """
        ping_id, callback = self._ping_manager.create(payload)
        event = wsproto.events.Ping(ping_id)
        self.send(event)
        return callback

    def send(self, event: wsproto.events.Event) -> None:
        """
        Send an Event message.

        Mainly useful to send events that are not supported by the library.
        Most of the time, [ping()][httpx_ws.WebSocketSession.ping],
        [send_text()][httpx_ws.WebSocketSession.send_text],
        [send_bytes()][httpx_ws.WebSocketSession.send_bytes]
        and [send_json()][httpx_ws.WebSocketSession.send_json] are preferred.

        Args:
            event: The event to send.

        Raises:
            WebSocketNetworkError: A network error occured.

        Examples:
            Send an event.

                event = wsproto.events.Message(b"Hello!")
                ws.send(event)
        """
        import httpcore2

        try:
            data = self.connection.send(event)
            self.stream.write(data)
        except httpcore2.WriteError as e:
            self.close(CloseReason.INTERNAL_ERROR, "Stream write error")
            raise WebSocketNetworkError() from e

    def send_text(self, data: str) -> None:
        """
        Send a text message.

        Args:
            data: The text to send.

        Raises:
            WebSocketNetworkError: A network error occured.

        Examples:
            Send a text message.

                ws.send_text("Hello!")
        """
        event = wsproto.events.TextMessage(data=data)
        self.send(event)

    def send_bytes(self, data: bytes) -> None:
        """
        Send a bytes message.

        Args:
            data: The data to send.

        Raises:
            WebSocketNetworkError: A network error occured.

        Examples:
            Send a bytes message.

                ws.send_bytes(b"Hello!")
        """
        event = wsproto.events.BytesMessage(data=data)
        self.send(event)

    def send_json(self, data: typing.Any, mode: JSONMode = "text") -> None:
        """
        Send JSON data.

        Args:
            data:
                The data to send. Must be serializable by [json.dumps][json.dumps].
            mode:
                The sending mode. Should either be `'text'` or `'bytes'`.

        Raises:
            WebSocketNetworkError: A network error occured.

        Examples:
            Send JSON data.

                data = {"message": "Hello!"}
                ws.send_json(data)
        """
        assert mode in ["text", "binary"]
        serialized_data = json.dumps(data)
        if mode == "text":
            self.send_text(serialized_data)
        else:
            self.send_bytes(serialized_data.encode("utf-8"))

    def receive(self, timeout: float | None = None) -> wsproto.events.Event:
        """
        Receive an event from the server.

        Mainly useful to receive raw [wsproto.events.Event][wsproto.events.Event].
        Most of the time, [receive_text()][httpx_ws.WebSocketSession.receive_text],
        [receive_bytes()][httpx_ws.WebSocketSession.receive_bytes],
        and [receive_json()][httpx_ws.WebSocketSession.receive_json] are preferred.

        Args:
            timeout:
                Number of seconds to wait for an event.
                If `None`, will block until an event is available.

        Returns:
            A raw [wsproto.events.Event][wsproto.events.Event].

        Raises:
            queue.Empty: No event was received before the timeout delay.
            WebSocketDisconnect: The server closed the websocket.
            WebSocketNetworkError: A network error occured.

        Examples:
            Wait for an event until one is available.

                try:
                    event = ws.receive()
                except WebSocketDisconnect:
                    print("Connection closed")

            Wait for an event for 2 seconds.

                try:
                    event = ws.receive(timeout=2.)
                except queue.Empty:
                    print("No event received.")
                except WebSocketDisconnect:
                    print("Connection closed")
        """
        event = self._events.get(block=True, timeout=timeout)
        if isinstance(event, HTTPXWSException):
            raise event
        if isinstance(event, wsproto.events.CloseConnection):
            raise WebSocketDisconnect(event.code, event.reason)
        return event

    def receive_text(self, timeout: float | None = None) -> str:
        """
        Receive text from the server.

        Args:
            timeout:
                Number of seconds to wait for an event.
                If `None`, will block until an event is available.

        Returns:
            Text data.

        Raises:
            queue.Empty: No event was received before the timeout delay.
            WebSocketDisconnect: The server closed the websocket.
            WebSocketNetworkError: A network error occured.
            WebSocketInvalidTypeReceived: The received event was not a text message.

        Examples:
            Wait for text until available.

                try:
                    text = ws.receive_text()
                except WebSocketDisconnect:
                    print("Connection closed")

            Wait for text for 2 seconds.

                try:
                    event = ws.receive_text(timeout=2.)
                except queue.Empty:
                    print("No text received.")
                except WebSocketDisconnect:
                    print("Connection closed")
        """
        event = self.receive(timeout)
        if isinstance(event, wsproto.events.TextMessage):
            return event.data
        raise WebSocketInvalidTypeReceived(event)

    def receive_bytes(self, timeout: float | None = None) -> bytes:
        """
        Receive bytes from the server.

        Args:
            timeout:
                Number of seconds to wait for an event.
                If `None`, will block until an event is available.

        Returns:
            Bytes data.

        Raises:
            queue.Empty: No event was received before the timeout delay.
            WebSocketDisconnect: The server closed the websocket.
            WebSocketNetworkError: A network error occured.
            WebSocketInvalidTypeReceived: The received event was not a bytes message.

        Examples:
            Wait for bytes until available.

                try:
                    data = ws.receive_bytes()
                except WebSocketDisconnect:
                    print("Connection closed")

            Wait for bytes for 2 seconds.

                try:
                    data = ws.receive_bytes(timeout=2.)
                except queue.Empty:
                    print("No data received.")
                except WebSocketDisconnect:
                    print("Connection closed")
        """
        event = self.receive(timeout)
        if isinstance(event, wsproto.events.BytesMessage):
            return bytes(event.data)
        raise WebSocketInvalidTypeReceived(event)

    def receive_json(self, timeout: float | None = None, mode: JSONMode = "text") -> typing.Any:
        """
        Receive JSON data from the server.

        The received data should be parseable by [json.loads][json.loads].

        Args:
            timeout:
                Number of seconds to wait for an event.
                If `None`, will block until an event is available.
            mode:
                Receive mode. Should either be `'text'` or `'bytes'`.

        Returns:
            Parsed JSON data.

        Raises:
            queue.Empty: No event was received before the timeout delay.
            WebSocketDisconnect: The server closed the websocket.
            WebSocketNetworkError: A network error occured.
            WebSocketInvalidTypeReceived: The received event
                didn't correspond to the specified mode.

        Examples:
            Wait for data until available.

                try:
                    data = ws.receive_json()
                except WebSocketDisconnect:
                    print("Connection closed")

            Wait for data for 2 seconds.

                try:
                    data = ws.receive_json(timeout=2.)
                except queue.Empty:
                    print("No data received.")
                except WebSocketDisconnect:
                    print("Connection closed")
        """
        assert mode in ["text", "binary"]
        data: str | bytes
        if mode == "text":
            data = self.receive_text(timeout)
        elif mode == "binary":
            data = self.receive_bytes(timeout)
        return json.loads(data)

    def close(self, code: int = 1000, reason: str | None = None) -> None:
        """
        Close the WebSocket session.

        Internally, it'll send the
        [CloseConnection][wsproto.events.CloseConnection] event.

        *This method is automatically called when exiting the context manager.*

        Args:
            code:
                The integer close code to indicate why the connection has closed.
            reason:
                Additional reasoning for why the connection has closed.

        Examples:
            Close the WebSocket session.

                ws.close()
        """
        import httpcore2

        self._should_close.set()
        if self._executor is not None:
            self._executor.shutdown(False)
        if self.connection.state not in {
            wsproto.connection.ConnectionState.LOCAL_CLOSING,
            wsproto.connection.ConnectionState.CLOSED,
        }:
            event = wsproto.events.CloseConnection(code, reason)
            data = self.connection.send(event)
            try:
                self.stream.write(data)
            except httpcore2.WriteError:
                pass
        self.stream.close()

    def _background_receive(self, max_bytes: int) -> None:
        """
        Background thread listening for data from the server.

        Internally, it'll:

        * Answer to Ping events.
        * Acknowledge Pong events.
        * Put other events in the [_events][_events]
        queue that'll eventually be consumed by the user.

        Args:
            max_bytes: The maximum chunk size to read at each iteration.
        """
        import httpcore2

        partial_message_buffer: str | bytes | None = None
        try:
            while not self._should_close.is_set():
                data = self._wait_until_closed(self.stream.read, max_bytes)
                self.connection.receive_data(data)
                for event in self.connection.events():
                    if isinstance(event, wsproto.events.Ping):
                        data = self.connection.send(event.response())
                        self.stream.write(data)
                        continue
                    if isinstance(event, wsproto.events.Pong):
                        self._ping_manager.ack(event.payload)
                        continue
                    if isinstance(event, wsproto.events.CloseConnection):
                        self._should_close.set()
                    if isinstance(event, wsproto.events.Message):
                        # Unfinished message: bufferize
                        if not event.message_finished:
                            if partial_message_buffer is None:
                                partial_message_buffer = event.data
                            else:
                                partial_message_buffer += event.data
                        # Finished message but no buffer: just emit the event
                        elif partial_message_buffer is None:
                            self._events.put(event)
                        # Finished message with buffer: emit the full event
                        else:
                            event_type = type(event)
                            full_message_event = event_type(partial_message_buffer + event.data)
                            partial_message_buffer = None
                            self._events.put(full_message_event)
                        continue
                    self._events.put(event)
        except (httpcore2.ReadError, httpcore2.WriteError):
            self.close(CloseReason.INTERNAL_ERROR, "Stream error")
            self._events.put(WebSocketNetworkError())
        except ShouldClose:
            pass

    def _background_keepalive_ping(self, interval_seconds: float, timeout_seconds: float | None = None) -> None:
        try:
            while not self._should_close.is_set():
                should_close = self._wait_until_closed(self._should_close.wait, interval_seconds)
                if should_close:  # pragma: no cover
                    raise ShouldClose()
                pong_callback = self.ping()
                if timeout_seconds is not None:
                    acknowledged = self._wait_until_closed(pong_callback.wait, timeout_seconds)
                    if not acknowledged:
                        self.close(CloseReason.INTERNAL_ERROR, "Keepalive ping timeout")
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


class AsyncWebSocketSession:
    """
    Async context manager representing an opened WebSocket session.

    Attributes:
        subprotocol (typing.Optional[str]):
            Optional protocol that has been accepted by the server.
        response (Response | None):
            The webSocket handshake response.
    """

    subprotocol: str | None
    response: Response | None
    _send_event: MemoryObjectSendStream[wsproto.events.Event | HTTPXWSException]
    _receive_event: MemoryObjectReceiveStream[wsproto.events.Event | HTTPXWSException]

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
        self.connection = wsproto.connection.Connection(wsproto.ConnectionType.CLIENT)
        self.response = response
        if self.response is not None:
            self.subprotocol = self.response.headers.get("sec-websocket-protocol")
        else:
            self.subprotocol = None

        self._ping_manager = AsyncPingManager()
        self._should_close = anyio.Event()

        self._max_message_size_bytes = max_message_size_bytes
        self._queue_size = queue_size

        # Always disable keepalive ping when emulating ASGI
        if isinstance(stream, ASGIWebSocketAsyncNetworkStream):
            self._keepalive_ping_interval_seconds = None
            self._keepalive_ping_timeout_seconds = None
        else:
            self._keepalive_ping_interval_seconds = keepalive_ping_interval_seconds
            self._keepalive_ping_timeout_seconds = keepalive_ping_timeout_seconds

    async def __aenter__(self) -> AsyncWebSocketSession:
        async with contextlib.AsyncExitStack() as exit_stack:
            self._send_event, self._receive_event = anyio.create_memory_object_stream[
                wsproto.events.Event | HTTPXWSException
            ]()
            exit_stack.enter_context(self._send_event)
            exit_stack.enter_context(self._receive_event)

            self._background_task_group = anyio.create_task_group()
            await exit_stack.enter_async_context(self._background_task_group)

            self._background_task_group.start_soon(self._background_receive, self._max_message_size_bytes)
            if self._keepalive_ping_interval_seconds is not None:
                self._background_task_group.start_soon(
                    self._background_keepalive_ping,
                    self._keepalive_ping_interval_seconds,
                    self._keepalive_ping_timeout_seconds,
                )

            exit_stack.callback(self._background_task_group.cancel_scope.cancel)
            exit_stack.push_async_callback(self.close)
            self._exit_stack = exit_stack.pop_all()

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self._exit_stack.aclose()

    async def ping(self, payload: bytes = b"") -> anyio.Event:
        """
        Send a Ping message.

        Args:
            payload:
                Payload to attach to the Ping event.
                Internally, it's used to track this specific event.
                If left empty, a random one will be generated.

        Returns:
            An event that can be used to wait for the corresponding Pong response.

        Examples:
            Send a Ping and wait for the Pong

                pong_callback = await ws.ping()
                # Will block until the corresponding Pong is received.
                await pong_callback.wait()
        """
        ping_id, callback = self._ping_manager.create(payload)
        event = wsproto.events.Ping(ping_id)
        await self.send(event)
        return callback

    async def send(self, event: wsproto.events.Event) -> None:
        """
        Send an Event message.

        Mainly useful to send events that are not supported by the library.
        Most of the time, [ping()][httpx_ws.AsyncWebSocketSession.ping],
        [send_text()][httpx_ws.AsyncWebSocketSession.send_text],
        [send_bytes()][httpx_ws.AsyncWebSocketSession.send_bytes]
        and [send_json()][httpx_ws.AsyncWebSocketSession.send_json] are preferred.

        Args:
            event: The event to send.

        Raises:
            WebSocketNetworkError: A network error occured.

        Examples:
            Send an event.

                event = await wsproto.events.Message(b"Hello!")
                ws.send(event)
        """
        import httpcore2

        try:
            data = self.connection.send(event)
            await self.stream.write(data)
        except httpcore2.WriteError as e:
            await self.close(CloseReason.INTERNAL_ERROR, "Stream write error")
            raise WebSocketNetworkError() from e

    async def send_text(self, data: str) -> None:
        """
        Send a text message.

        Args:
            data: The text to send.

        Raises:
            WebSocketNetworkError: A network error occured.

        Examples:
            Send a text message.

                await ws.send_text("Hello!")
        """
        event = wsproto.events.TextMessage(data=data)
        await self.send(event)

    async def send_bytes(self, data: bytes) -> None:
        """
        Send a bytes message.

        Args:
            data: The data to send.

        Raises:
            WebSocketNetworkError: A network error occured.

        Examples:
            Send a bytes message.

                await ws.send_bytes(b"Hello!")
        """
        event = wsproto.events.BytesMessage(data=data)
        await self.send(event)

    async def send_json(self, data: typing.Any, mode: JSONMode = "text") -> None:
        """
        Send JSON data.

        Args:
            data:
                The data to send. Must be serializable by [json.dumps][json.dumps].
            mode:
                The sending mode. Should either be `'text'` or `'bytes'`.

        Raises:
            WebSocketNetworkError: A network error occured.

        Examples:
            Send JSON data.

                data = {"message": "Hello!"}
                await ws.send_json(data)
        """
        assert mode in ["text", "binary"]
        serialized_data = json.dumps(data)
        if mode == "text":
            await self.send_text(serialized_data)
        else:
            await self.send_bytes(serialized_data.encode("utf-8"))

    async def receive(self, timeout: float | None = None) -> wsproto.events.Event:
        """
        Receive an event from the server.

        Mainly useful to receive raw [wsproto.events.Event][wsproto.events.Event].
        Most of the time, [receive_text()][httpx_ws.AsyncWebSocketSession.receive_text],
        [receive_bytes()][httpx_ws.AsyncWebSocketSession.receive_bytes],
        and [receive_json()][httpx_ws.AsyncWebSocketSession.receive_json] are preferred.

        Args:
            timeout:
                Number of seconds to wait for an event.
                If `None`, will block until an event is available.

        Returns:
            A raw [wsproto.events.Event][wsproto.events.Event].

        Raises:
            TimeoutError: No event was received before the timeout delay.
            WebSocketDisconnect: The server closed the websocket.
            WebSocketNetworkError: A network error occured.

        Examples:
            Wait for an event until one is available.

                try:
                    event = await ws.receive()
                except WebSocketDisconnect:
                    print("Connection closed")

            Wait for an event for 2 seconds.

                try:
                    event = await ws.receive(timeout=2.)
                except TimeoutError:
                    print("No event received.")
                except WebSocketDisconnect:
                    print("Connection closed")
        """
        with anyio.fail_after(timeout):
            event = await self._receive_event.receive()
        if isinstance(event, HTTPXWSException):
            raise event
        if isinstance(event, wsproto.events.CloseConnection):
            raise WebSocketDisconnect(event.code, event.reason)
        return event

    async def receive_text(self, timeout: float | None = None) -> str:
        """
        Receive text from the server.

        Args:
            timeout:
                Number of seconds to wait for an event.
                If `None`, will block until an event is available.

        Returns:
            Text data.

        Raises:
            TimeoutError: No event was received before the timeout delay.
            WebSocketDisconnect: The server closed the websocket.
            WebSocketNetworkError: A network error occured.
            WebSocketInvalidTypeReceived: The received event was not a text message.

        Examples:
            Wait for text until available.

                try:
                    text = await ws.receive_text()
                except WebSocketDisconnect:
                    print("Connection closed")

            Wait for text for 2 seconds.

                try:
                    event = await ws.receive_text(timeout=2.)
                except TimeoutError:
                    print("No text received.")
                except WebSocketDisconnect:
                    print("Connection closed")
        """
        event = await self.receive(timeout)
        if isinstance(event, wsproto.events.TextMessage):
            return event.data
        raise WebSocketInvalidTypeReceived(event)

    async def receive_bytes(self, timeout: float | None = None) -> bytes:
        """
        Receive bytes from the server.

        Args:
            timeout:
                Number of seconds to wait for an event.
                If `None`, will block until an event is available.

        Returns:
            Bytes data.

        Raises:
            TimeoutError: No event was received before the timeout delay.
            WebSocketDisconnect: The server closed the websocket.
            WebSocketNetworkError: A network error occured.
            WebSocketInvalidTypeReceived: The received event was not a bytes message.

        Examples:
            Wait for bytes until available.

                try:
                    data = await ws.receive_bytes()
                except WebSocketDisconnect:
                    print("Connection closed")

            Wait for bytes for 2 seconds.

                try:
                    data = await ws.receive_bytes(timeout=2.)
                except TimeoutError:
                    print("No data received.")
                except WebSocketDisconnect:
                    print("Connection closed")
        """
        event = await self.receive(timeout)
        if isinstance(event, wsproto.events.BytesMessage):
            return bytes(event.data)
        raise WebSocketInvalidTypeReceived(event)

    async def receive_json(self, timeout: float | None = None, mode: JSONMode = "text") -> typing.Any:
        """
        Receive JSON data from the server.

        The received data should be parseable by [json.loads][json.loads].

        Args:
            timeout:
                Number of seconds to wait for an event.
                If `None`, will block until an event is available.
            mode:
                Receive mode. Should either be `'text'` or `'bytes'`.

        Returns:
            Parsed JSON data.

        Raises:
            TimeoutError: No event was received before the timeout delay.
            WebSocketDisconnect: The server closed the websocket.
            WebSocketNetworkError: A network error occured.
            WebSocketInvalidTypeReceived: The received event
                didn't correspond to the specified mode.

        Examples:
            Wait for data until available.

                try:
                    data = await ws.receive_json()
                except WebSocketDisconnect:
                    print("Connection closed")

            Wait for data for 2 seconds.

                try:
                    data = await ws.receive_json(timeout=2.)
                except TimeoutError:
                    print("No data received.")
                except WebSocketDisconnect:
                    print("Connection closed")
        """
        assert mode in ["text", "binary"]
        data: str | bytes
        if mode == "text":
            data = await self.receive_text(timeout)
        elif mode == "binary":
            data = await self.receive_bytes(timeout)
        return json.loads(data)

    async def close(self, code: int = 1000, reason: str | None = None) -> None:
        """
        Close the WebSocket session.

        Internally, it'll send the
        [CloseConnection][wsproto.events.CloseConnection] event.

        *This method is automatically called when exiting the context manager.*

        Args:
            code:
                The integer close code to indicate why the connection has closed.
            reason:
                Additional reasoning for why the connection has closed.

        Examples:
            Close the WebSocket session.

                await ws.close()
        """
        import httpcore2

        self._should_close.set()
        if self.connection.state not in {
            wsproto.connection.ConnectionState.LOCAL_CLOSING,
            wsproto.connection.ConnectionState.CLOSED,
        }:
            event = wsproto.events.CloseConnection(code, reason)
            data = self.connection.send(event)
            try:
                await self.stream.write(data)
            except httpcore2.WriteError:
                pass
        await self.stream.aclose()

    async def _background_receive(self, max_bytes: int) -> None:
        """
        Background task listening for data from the server.

        Internally, it'll:

        * Answer to Ping events.
        * Acknowledge Pong events.
        * Put other events in the [_events][_events]
        queue that'll eventually be consumed by the user.

        Args:
            max_bytes: The maximum chunk size to read at each iteration.
        """
        import httpcore2

        partial_message_buffer: str | bytes | None = None
        try:
            while not self._should_close.is_set():
                data = await self.stream.read(max_bytes=max_bytes)
                self.connection.receive_data(data)
                for event in self.connection.events():
                    if isinstance(event, wsproto.events.Ping):
                        data = self.connection.send(event.response())
                        await self.stream.write(data)
                        continue
                    if isinstance(event, wsproto.events.Pong):
                        self._ping_manager.ack(event.payload)
                        continue
                    if isinstance(event, wsproto.events.CloseConnection):
                        self._should_close.set()
                    if isinstance(event, wsproto.events.Message):
                        # Unfinished message: bufferize
                        if not event.message_finished:
                            if partial_message_buffer is None:
                                partial_message_buffer = event.data
                            else:
                                partial_message_buffer += event.data
                        # Finished message but no buffer: just emit the event
                        elif partial_message_buffer is None:
                            await self._send_event.send(event)
                        # Finished message with buffer: emit the full event
                        else:
                            event_type = type(event)
                            full_message_event = event_type(partial_message_buffer + event.data)
                            partial_message_buffer = None
                            await self._send_event.send(full_message_event)
                        continue
                    await self._send_event.send(event)
        except (httpcore2.ReadError, httpcore2.WriteError):
            await self.close(CloseReason.INTERNAL_ERROR, "Stream error")
            await self._send_event.send(WebSocketNetworkError())

    async def _background_keepalive_ping(self, interval_seconds: float, timeout_seconds: float | None = None) -> None:
        while not self._should_close.is_set():
            await anyio.sleep(interval_seconds)
            if self._should_close.is_set():
                return
            pong_callback = await self.ping()
            if timeout_seconds is not None:
                try:
                    with anyio.fail_after(timeout_seconds):
                        await pong_callback.wait()
                except TimeoutError:
                    await self.close(CloseReason.INTERNAL_ERROR, "Keepalive ping timeout")
                    await self._send_event.send(WebSocketNetworkError())


def _get_headers(
    subprotocols: list[str] | None,
) -> dict[str, typing.Any]:
    headers = {
        "connection": "upgrade",
        "upgrade": "websocket",
        "sec-websocket-key": base64.b64encode(secrets.token_bytes(16)).decode("utf-8"),
        "sec-websocket-version": "13",
    }
    if subprotocols is not None:
        headers["sec-websocket-protocol"] = ", ".join(subprotocols)
    return headers


@contextlib.contextmanager
def _connect_ws(
    url: str,
    client: Client,
    *,
    max_message_size_bytes: int = DEFAULT_MAX_MESSAGE_SIZE_BYTES,
    queue_size: int = DEFAULT_QUEUE_SIZE,
    keepalive_ping_interval_seconds: float | None = DEFAULT_KEEPALIVE_PING_INTERVAL_SECONDS,
    keepalive_ping_timeout_seconds: float | None = DEFAULT_KEEPALIVE_PING_TIMEOUT_SECONDS,
    subprotocols: list[str] | None = None,
    params: QueryParamTypes | None = None,
    headers: HeaderTypes | None = None,
    cookies: CookieTypes | None = None,
    auth: AuthTypes | UseClientDefault | None = USE_CLIENT_DEFAULT,
    follow_redirects: bool | UseClientDefault = USE_CLIENT_DEFAULT,
    timeout: TimeoutTypes | UseClientDefault = USE_CLIENT_DEFAULT,
    extensions: RequestExtensions | None = None,
) -> typing.Generator[WebSocketSession, None, None]:
    with client.stream(
        "GET",
        url,
        params=params,
        headers=Headers(headers) | _get_headers(subprotocols),
        cookies=cookies,
        auth=auth,
        follow_redirects=follow_redirects,
        timeout=timeout,
        extensions=extensions,
    ) as response:
        if response.status_code != 101:
            raise WebSocketUpgradeError(response)

        with WebSocketSession(
            response.extensions["network_stream"],
            max_message_size_bytes=max_message_size_bytes,
            queue_size=queue_size,
            keepalive_ping_interval_seconds=keepalive_ping_interval_seconds,
            keepalive_ping_timeout_seconds=keepalive_ping_timeout_seconds,
            response=response,
        ) as session:
            yield session


@contextlib.contextmanager
def connect_ws(
    url: str,
    client: Client | None = None,
    *,
    max_message_size_bytes: int = DEFAULT_MAX_MESSAGE_SIZE_BYTES,
    queue_size: int = DEFAULT_QUEUE_SIZE,
    keepalive_ping_interval_seconds: float | None = DEFAULT_KEEPALIVE_PING_INTERVAL_SECONDS,
    keepalive_ping_timeout_seconds: float | None = DEFAULT_KEEPALIVE_PING_TIMEOUT_SECONDS,
    subprotocols: list[str] | None = None,
    params: QueryParamTypes | None = None,
    headers: HeaderTypes | None = None,
    cookies: CookieTypes | None = None,
    auth: AuthTypes | UseClientDefault | None = USE_CLIENT_DEFAULT,
    follow_redirects: bool | UseClientDefault = USE_CLIENT_DEFAULT,
    timeout: TimeoutTypes | UseClientDefault = USE_CLIENT_DEFAULT,
    extensions: RequestExtensions | None = None,
) -> typing.Generator[WebSocketSession, None, None]:
    """
    Start a sync WebSocket session.

    It returns a context manager that'll automatically
    call [close()][httpx_ws.WebSocketSession.close] when exiting.

    Args:
        url: The WebSocket URL.
        client:
            HTTPX client to use.
            If not provided, a default one will be initialized.
        max_message_size_bytes:
            Message size in bytes to receive from the server.
            Defaults to 65 KiB.
        queue_size:
            Size of the queue where the received messages will be held
            until they are consumed.
            If the queue is full, the client will stop receive messages
            from the server until the queue has room available.
            Defaults to 512.
        keepalive_ping_interval_seconds:
            Interval at which the client will automatically send a Ping event
            to keep the connection alive. Set it to `None` to disable this mechanism.
            Defaults to 20 seconds.
        keepalive_ping_timeout_seconds:
            Maximum delay the client will wait for an answer to its Ping event.
            If the delay is exceeded,
            [WebSocketNetworkError][httpx_ws.WebSocketNetworkError]
            will be raised and the connection closed.
            Defaults to 20 seconds.
        subprotocols:
            Optional list of suprotocols to negotiate with the server.
        params:
            Query parameters to include in the handshake request.
        headers:
            Headers to include in the handshake request.
        cookies:
            Cookies to include in the handshake request.
        auth:
            Authentication to use for the handshake request.
        follow_redirects:
            Whether to follow redirects on the handshake request.
        timeout:
            Timeout configuration for the handshake request.
        extensions:
            Request extensions for the handshake request.

    Returns:
        A [context manager][contextlib.AbstractContextManager]
            for [WebSocketSession][httpx_ws.WebSocketSession].

    Examples:
        Without explicit HTTPX client.

            with connect_ws("http://localhost:8000/ws") as ws:
                message = ws.receive_text()
                print(message)
                ws.send_text("Hello!")

        With explicit HTTPX client.

            with httpx2.Client() as client:
                with connect_ws("http://localhost:8000/ws", client) as ws:
                    message = ws.receive_text()
                    print(message)
                    ws.send_text("Hello!")
    """
    if client is None:
        from .._client import Client

        owned_client: contextlib.AbstractContextManager[Client] = Client()
    else:
        owned_client = contextlib.nullcontext(client)

    with owned_client as client:
        with _connect_ws(
            url,
            client=client,
            max_message_size_bytes=max_message_size_bytes,
            queue_size=queue_size,
            keepalive_ping_interval_seconds=keepalive_ping_interval_seconds,
            keepalive_ping_timeout_seconds=keepalive_ping_timeout_seconds,
            subprotocols=subprotocols,
            params=params,
            headers=headers,
            cookies=cookies,
            auth=auth,
            follow_redirects=follow_redirects,
            timeout=timeout,
            extensions=extensions,
        ) as websocket:
            yield websocket


@contextlib.asynccontextmanager
async def _aconnect_ws(
    url: str,
    client: AsyncClient,
    *,
    max_message_size_bytes: int = DEFAULT_MAX_MESSAGE_SIZE_BYTES,
    queue_size: int = DEFAULT_QUEUE_SIZE,
    keepalive_ping_interval_seconds: float | None = DEFAULT_KEEPALIVE_PING_INTERVAL_SECONDS,
    keepalive_ping_timeout_seconds: float | None = DEFAULT_KEEPALIVE_PING_TIMEOUT_SECONDS,
    subprotocols: list[str] | None = None,
    params: QueryParamTypes | None = None,
    headers: HeaderTypes | None = None,
    cookies: CookieTypes | None = None,
    auth: AuthTypes | UseClientDefault | None = USE_CLIENT_DEFAULT,
    follow_redirects: bool | UseClientDefault = USE_CLIENT_DEFAULT,
    timeout: TimeoutTypes | UseClientDefault = USE_CLIENT_DEFAULT,
    extensions: RequestExtensions | None = None,
) -> typing.AsyncGenerator[AsyncWebSocketSession, None]:
    async with client.stream(
        "GET",
        url,
        params=params,
        headers=Headers(headers) | _get_headers(subprotocols),
        cookies=cookies,
        auth=auth,
        follow_redirects=follow_redirects,
        timeout=timeout,
        extensions=extensions,
    ) as response:
        if response.status_code != 101:
            raise WebSocketUpgradeError(response)

        async with AsyncWebSocketSession(
            response.extensions["network_stream"],
            max_message_size_bytes=max_message_size_bytes,
            queue_size=queue_size,
            keepalive_ping_interval_seconds=keepalive_ping_interval_seconds,
            keepalive_ping_timeout_seconds=keepalive_ping_timeout_seconds,
            response=response,
        ) as session:
            yield session


@contextlib.asynccontextmanager
async def aconnect_ws(
    url: str,
    client: AsyncClient | None = None,
    *,
    max_message_size_bytes: int = DEFAULT_MAX_MESSAGE_SIZE_BYTES,
    queue_size: int = DEFAULT_QUEUE_SIZE,
    keepalive_ping_interval_seconds: float | None = DEFAULT_KEEPALIVE_PING_INTERVAL_SECONDS,
    keepalive_ping_timeout_seconds: float | None = DEFAULT_KEEPALIVE_PING_TIMEOUT_SECONDS,
    subprotocols: list[str] | None = None,
    params: QueryParamTypes | None = None,
    headers: HeaderTypes | None = None,
    cookies: CookieTypes | None = None,
    auth: AuthTypes | UseClientDefault | None = USE_CLIENT_DEFAULT,
    follow_redirects: bool | UseClientDefault = USE_CLIENT_DEFAULT,
    timeout: TimeoutTypes | UseClientDefault = USE_CLIENT_DEFAULT,
    extensions: RequestExtensions | None = None,
) -> typing.AsyncGenerator[AsyncWebSocketSession, None]:
    """
    Start an async WebSocket session.

    It returns an async context manager that'll automatically
    call [close()][httpx_ws.AsyncWebSocketSession.close] when exiting.

    Args:
        url: The WebSocket URL.
        client:
            HTTPX client to use.
            If not provided, a default one will be initialized.
        max_message_size_bytes:
            Message size in bytes to receive from the server.
            Defaults to 65 KiB.
        queue_size:
            Size of the queue where the received messages will be held
            until they are consumed.
            If the queue is full, the client will stop receive messages
            from the server until the queue has room available.
            Defaults to 512.
        keepalive_ping_interval_seconds:
            Interval at which the client will automatically send a Ping event
            to keep the connection alive. Set it to `None` to disable this mechanism.
            Defaults to 20 seconds.
        keepalive_ping_timeout_seconds:
            Maximum delay the client will wait for an answer to its Ping event.
            If the delay is exceeded,
            [WebSocketNetworkError][httpx_ws.WebSocketNetworkError]
            will be raised and the connection closed.
            Defaults to 20 seconds.
        subprotocols:
            Optional list of suprotocols to negotiate with the server.
        params:
            Query parameters to include in the handshake request.
        headers:
            Headers to include in the handshake request.
        cookies:
            Cookies to include in the handshake request.
        auth:
            Authentication to use for the handshake request.
        follow_redirects:
            Whether to follow redirects on the handshake request.
        timeout:
            Timeout configuration for the handshake request.
        extensions:
            Request extensions for the handshake request.

    Returns:
        An [async context manager][contextlib.AbstractAsyncContextManager]
            for [AsyncWebSocketSession][httpx_ws.AsyncWebSocketSession].

    Examples:
        Without explicit HTTPX client.

            async with aconnect_ws("http://localhost:8000/ws") as ws:
                message = await ws.receive_text()
                print(message)
                await ws.send_text("Hello!")

        With explicit HTTPX client.

            async with httpx2.AsyncClient() as client:
                async with aconnect_ws("http://localhost:8000/ws", client) as ws:
                    message = await ws.receive_text()
                    print(message)
                    await ws.send_text("Hello!")
    """
    if client is None:
        from .._client import AsyncClient

        owned_client: contextlib.AbstractAsyncContextManager[AsyncClient] = AsyncClient()
    else:
        owned_client = contextlib.nullcontext(client)

    async with owned_client as client:
        async with _aconnect_ws(
            url,
            client=client,
            max_message_size_bytes=max_message_size_bytes,
            queue_size=queue_size,
            keepalive_ping_interval_seconds=keepalive_ping_interval_seconds,
            keepalive_ping_timeout_seconds=keepalive_ping_timeout_seconds,
            subprotocols=subprotocols,
            params=params,
            headers=headers,
            cookies=cookies,
            auth=auth,
            follow_redirects=follow_redirects,
            timeout=timeout,
            extensions=extensions,
        ) as websocket:
            yield websocket
