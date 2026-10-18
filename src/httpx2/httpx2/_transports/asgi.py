from __future__ import annotations

import contextlib
import typing
from types import TracebackType

from .._models import Request, Response
from .._types import AsyncByteStream
from .base import AsyncBaseTransport

if typing.TYPE_CHECKING:
    import asyncio

    import anyio.abc
    import trio

    Event = asyncio.Event | trio.Event


_Message = typing.MutableMapping[str, typing.Any]
_Receive = typing.Callable[[], typing.Awaitable[_Message]]
_Send = typing.Callable[[typing.MutableMapping[str, typing.Any]], typing.Awaitable[None]]
_ASGIApp = typing.Callable[[typing.MutableMapping[str, typing.Any], _Receive, _Send], typing.Awaitable[None]]

__all__ = ["ASGITransport"]


def is_running_trio() -> bool:
    try:
        # sniffio is a dependency of trio.

        # See https://github.com/python-trio/trio/issues/2802
        import sniffio

        if sniffio.current_async_library() == "trio":
            return True
    except ImportError:  # pragma: no cover
        pass

    return False


def create_event() -> Event:
    if is_running_trio():
        import trio

        return trio.Event()

    import asyncio

    return asyncio.Event()


class ASGIResponseStream(AsyncByteStream):
    def __init__(self, body: list[bytes]) -> None:
        self._body = body

    async def __aiter__(self) -> typing.AsyncIterator[bytes]:
        yield b"".join(self._body)


class ASGIStreamingResponseStream(AsyncByteStream):
    def __init__(self, disconnected: Event) -> None:
        import anyio

        self._send, self._receive = anyio.create_memory_object_stream[bytes | Exception]()
        self._disconnected = disconnected

    async def put(self, part: bytes | Exception) -> None:
        import anyio

        # The client may close the response before reading the whole body.
        with contextlib.suppress(anyio.BrokenResourceError, anyio.ClosedResourceError):
            await self._send.send(part)

    def end(self) -> None:
        self._send.close()

    async def __aiter__(self) -> typing.AsyncIterator[bytes]:
        async for part in self._receive:
            if isinstance(part, Exception):
                raise part
            yield part

    async def aclose(self) -> None:
        self._disconnected.set()
        # Closing the send side wakes a reader in another task that is waiting for the next chunk.
        self._send.close()
        self._receive.close()


class ASGITransport(AsyncBaseTransport):
    """
    A custom AsyncTransport that handles sending requests directly to an ASGI app.

    ```python
    transport = httpx2.ASGITransport(
        app=app,
        root_path="/submount",
        client=("1.2.3.4", 123)
    )
    async with httpx2.AsyncClient(transport=transport) as client:
        ...
    ```

    Inside `async with`, the app runs in a task owned by the transport, and the response
    is returned as soon as the app sends `http.response.start`, so the body can be streamed.
    Outside of it, the app runs to completion before the response is returned.

    Arguments:
        app: The ASGI application.
        raise_app_exceptions: Boolean indicating if exceptions in the application
            should be raised. Default to `True`. Can be set to `False` for use cases
            such as testing the content of a client 500 response.
        root_path: The root path on which the ASGI application should be mounted.
        client: A two-tuple indicating the client IP and port of incoming requests.
    """

    def __init__(
        self,
        app: _ASGIApp,
        raise_app_exceptions: bool = True,
        root_path: str = "",
        client: tuple[str, int] = ("127.0.0.1", 123),
    ) -> None:
        self.app = app
        self.raise_app_exceptions = raise_app_exceptions
        self.root_path = root_path
        self.client = client
        self._task_group: anyio.abc.TaskGroup | None = None
        self._exit_stack: contextlib.AsyncExitStack | None = None
        self._open_count = 0

    async def __aenter__(self) -> ASGITransport:
        # Clients sharing this transport share one task group, closed when the last one exits.
        self._open_count += 1
        if self._exit_stack is not None:
            return self

        try:
            import anyio
        except ImportError:  # pragma: no cover - anyio is not installed on Emscripten, so the app runs inline
            return self

        async with contextlib.AsyncExitStack() as stack:
            self._task_group = await stack.enter_async_context(anyio.create_task_group())
            # Apps still running when the transport closes, such as unconsumed infinite streams, are cancelled.
            stack.callback(self._task_group.cancel_scope.cancel)
            self._exit_stack = stack.pop_all()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        self._open_count -= 1
        if self._open_count == 0:
            await self.aclose()

    async def aclose(self) -> None:
        if self._exit_stack is not None:
            await self._exit_stack.aclose()
            self._task_group = None
            self._exit_stack = None

    async def handle_async_request(self, request: Request) -> Response:
        assert isinstance(request.stream, AsyncByteStream)

        # ASGI scope.
        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": request.method,
            "headers": [(k.lower(), v) for (k, v) in request.headers.raw],
            "scheme": request.url.scheme,
            "path": request.url.path,
            "raw_path": request.url.raw_path.split(b"?")[0],
            "query_string": request.url.query,
            "server": (request.url.host, request.url.port),
            "client": self.client,
            "root_path": self.root_path,
        }

        # Request.
        request_body_chunks = request.stream.__aiter__()
        request_complete = False

        # Response.
        status_code = None
        response_headers = None
        response_started = create_event()
        response_complete = False
        disconnected = create_event()
        app_exception: Exception | None = None
        task_group = self._task_group
        body_parts: list[bytes] = []
        stream = None if task_group is None else ASGIStreamingResponseStream(disconnected)

        # ASGI callables.

        async def receive() -> dict[str, typing.Any]:
            nonlocal request_complete

            if request_complete or disconnected.is_set():
                await disconnected.wait()
                return {"type": "http.disconnect"}

            try:
                body = await request_body_chunks.__anext__()
            except StopAsyncIteration:
                request_complete = True
                body = b""
            # The response may have been closed while waiting for the request body.
            if disconnected.is_set():
                return {"type": "http.disconnect"}
            return {"type": "http.request", "body": body, "more_body": not request_complete}

        async def send(message: typing.MutableMapping[str, typing.Any]) -> None:
            nonlocal status_code, response_headers, response_complete

            if message["type"] == "http.response.start":
                assert not response_started.is_set()

                status_code = message["status"]
                response_headers = message.get("headers", [])
                response_started.set()

            elif message["type"] == "http.response.body":
                if not response_started.is_set():
                    raise RuntimeError("The ASGI app sent a response body before starting the response.")
                assert not response_complete
                body = message.get("body", b"")
                more_body = message.get("more_body", False)

                if body and request.method != "HEAD":
                    if stream is None:
                        body_parts.append(body)
                    else:
                        await stream.put(body)

                if not more_body:
                    response_complete = True
                    disconnected.set()

        async def run_app() -> None:
            nonlocal app_exception
            try:
                error: Exception | None = None
                try:
                    await self.app(scope, receive, send)
                except Exception as exc:
                    if self.raise_app_exceptions:
                        error = exc
                else:
                    # An incomplete response is a protocol error, raised even when app exceptions are not.
                    if not response_complete:
                        error = RuntimeError("The ASGI app returned without completing the response.")
                if error is not None and stream is not None and response_started.is_set():
                    await stream.put(error)
                elif error is not None:
                    app_exception = error
            finally:
                if stream is not None:
                    stream.end()
                response_started.set()

        if task_group is None:
            await run_app()
        else:
            import anyio

            assert stream is not None
            task_group.start_soon(run_app)
            try:
                await response_started.wait()
            except anyio.get_cancelled_exc_class():
                await stream.aclose()
                raise

        if app_exception is not None:
            if stream is not None:
                await stream.aclose()
            raise app_exception

        return Response(
            status_code or 500,
            headers=response_headers or [],
            stream=ASGIResponseStream(body_parts) if stream is None else stream,
        )
