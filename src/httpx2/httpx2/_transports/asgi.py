from __future__ import annotations

import contextlib
import typing
from types import TracebackType

import anyio
import anyio.abc
import anyio.streams.memory

from .._models import Request, Response
from .._types import AsyncByteStream
from .base import AsyncBaseTransport

_Message = typing.MutableMapping[str, typing.Any]
_Receive = typing.Callable[[], typing.Awaitable[_Message]]
_Send = typing.Callable[[typing.MutableMapping[str, typing.Any]], typing.Awaitable[None]]
_ASGIApp = typing.Callable[[typing.MutableMapping[str, typing.Any], _Receive, _Send], typing.Awaitable[None]]

__all__ = ["ASGITransport"]


class ASGIResponseStream(AsyncByteStream):
    def __init__(self, body_parts_stream: anyio.streams.memory.MemoryObjectReceiveStream[bytes | Exception]) -> None:
        self._body_parts = body_parts_stream

    async def __aiter__(self) -> typing.AsyncIterator[bytes]:
        async for part in self._body_parts:
            if isinstance(part, bytes):
                yield part
            elif isinstance(part, Exception):
                raise part
            else:
                raise TypeError(part)
        self._body_parts.close()

    async def aclose(self) -> None:
        self._body_parts.close()

    def __del__(self) -> None:
        self._body_parts.close()


class ASGITransport(AsyncBaseTransport):
    """
    A custom AsyncTransport that handles sending requests directly to an ASGI app.

    ```python
    transport = httpx2.ASGITransport(
        app=app,
        root_path="/submount",
        client=("1.2.3.4", 123)
    )
    client = httpx2.AsyncClient(transport=transport)
    ```

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

    async def __aenter__(self) -> ASGITransport:
        await super().__aenter__()

        async with contextlib.AsyncExitStack() as stack:
            self._task_group = await stack.enter_async_context(anyio.create_task_group())
            # Make sure all remaining tasks are cancelled after normal cleanup
            stack.callback(self._task_group.cancel_scope.cancel)
            self._exit_stack = stack.pop_all()

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
    ) -> None:
        if self._exit_stack is not None:
            await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)
            self._task_group = None
            self._exit_stack = None

        await super().__aexit__(exc_type, exc_val, exc_tb)

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
        body_send, body_receive = anyio.create_memory_object_stream[bytes | Exception](16)
        response_started = anyio.Event()
        response_complete = anyio.Event()
        app_exception = None

        # ASGI callables.

        async def receive() -> dict[str, typing.Any]:
            nonlocal request_complete

            if request_complete:
                await response_complete.wait()
                return {"type": "http.disconnect"}

            try:
                body = await request_body_chunks.__anext__()
            except StopAsyncIteration:
                request_complete = True
                return {"type": "http.request", "body": b"", "more_body": False}
            return {"type": "http.request", "body": body, "more_body": True}

        async def send(message: typing.MutableMapping[str, typing.Any]) -> None:
            nonlocal status_code, response_headers

            if message["type"] == "http.response.start":
                assert not response_started.is_set()

                status_code = message["status"]
                response_headers = message.get("headers", [])
                response_started.set()

            elif message["type"] == "http.response.body":
                assert not response_complete.is_set()
                body = message.get("body", b"")
                more_body = message.get("more_body", False)

                if body and request.method != "HEAD":
                    await body_send.send(body)

                if not more_body:
                    response_complete.set()

        async def app_wrapper() -> None:
            nonlocal app_exception, status_code, response_headers
            try:
                await self.app(scope, receive, send)
            except Exception as exc:
                if status_code is None:
                    status_code = 500
                if response_headers is None:
                    response_headers = {}
                if self.raise_app_exceptions:
                    await body_send.send(exc)
                    app_exception = exc
                response_started.set()
                response_complete.set()
            finally:
                body_send.close()

        if self._task_group is None:
            raise RuntimeError("ASGITransport.__aenter__ not called")
        self._task_group.start_soon(app_wrapper)
        await response_started.wait()

        assert status_code is not None
        assert response_headers is not None

        if app_exception is not None:
            body_receive.close()
            raise app_exception

        stream = ASGIResponseStream(body_receive)

        return Response(status_code, headers=response_headers, stream=stream)
