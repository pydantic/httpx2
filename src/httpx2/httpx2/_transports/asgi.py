from __future__ import annotations

import typing

import anyio

from .._models import Request, Response
from .._types import AsyncByteStream
from .base import AsyncBaseTransport

_Message = typing.MutableMapping[str, typing.Any]
_Receive = typing.Callable[[], typing.Awaitable[_Message]]
_Send = typing.Callable[[_Message], typing.Awaitable[None]]
_ASGIApp = typing.Callable[[typing.MutableMapping[str, typing.Any], _Receive, _Send], typing.Awaitable[None]]

__all__ = ["ASGITransport"]


class ASGIResponseStream(AsyncByteStream):
    def __init__(
        self,
        ignore_body: bool,
        asgi_messages: typing.AsyncGenerator[_Message, None],
        disconnect_request: anyio.Event,
    ) -> None:
        self._ignore_body = ignore_body
        self._asgi_messages = asgi_messages
        self._disconnect_request = disconnect_request

    async def __aiter__(self) -> typing.AsyncIterator[bytes]:
        more_body = True
        async for message in self._asgi_messages:
            if message["type"] == "http.response.body":
                assert more_body
                body = message.get("body", b"")
                more_body = message.get("more_body", False)
                if body and not self._ignore_body:
                    yield body
                if not more_body:
                    self._disconnect_request.set()

    async def aclose(self) -> None:
        self._disconnect_request.set()
        await self._asgi_messages.aclose()


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

    The app is run in a separate task, and response events are streamed as soon as
    they arrive. A response is returned as soon as the app sends the response start,
    which generally happens before the app has fully run.

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

    async def handle_async_request(self, request: Request) -> Response:
        disconnect_request = anyio.Event()
        asgi_messages = self._run_app(request, disconnect_request)

        async for message in asgi_messages:
            if message["type"] == "http.response.start":
                return Response(
                    status_code=message["status"],
                    headers=message.get("headers", []),
                    stream=ASGIResponseStream(
                        ignore_body=request.method == "HEAD",
                        asgi_messages=asgi_messages,
                        disconnect_request=disconnect_request,
                    ),
                )

        disconnect_request.set()
        return Response(status_code=500, headers=[])

    async def _run_app(
        self, request: Request, disconnect_request: anyio.Event
    ) -> typing.AsyncGenerator[_Message, None]:
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
        send_channel, receive_channel = anyio.create_memory_object_stream[_Message]()
        app_exception: Exception | None = None

        # ASGI callables.

        async def receive() -> _Message:
            nonlocal request_complete

            if request_complete:
                await disconnect_request.wait()
                return {"type": "http.disconnect"}

            try:
                body = await request_body_chunks.__anext__()
            except StopAsyncIteration:
                request_complete = True
                return {"type": "http.request", "body": b"", "more_body": False}
            return {"type": "http.request", "body": body, "more_body": True}

        async def run_app() -> None:
            nonlocal app_exception
            try:
                await self.app(scope, receive, send_channel.send)
            except Exception as exc:
                app_exception = exc
            finally:
                send_channel.close()

        closed = False

        async with anyio.create_task_group() as task_group:
            task_group.start_soon(run_app)
            async with receive_channel:
                try:
                    async for message in receive_channel:
                        yield message
                except GeneratorExit:
                    # A `GeneratorExit` must not propagate through the task group,
                    # which would wrap it in an exception group. Cancel the app and
                    # return instead.
                    closed = True
                    task_group.cancel_scope.cancel()

        if not closed and app_exception is not None and self.raise_app_exceptions:
            raise app_exception
