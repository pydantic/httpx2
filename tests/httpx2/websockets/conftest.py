from __future__ import annotations

import contextlib
import pathlib
import queue
import tempfile
import time
import typing
from unittest.mock import MagicMock

import pytest
import uvicorn
from anyio.from_thread import start_blocking_portal
from starlette.applications import Starlette
from starlette.routing import WebSocketRoute
from starlette.websockets import WebSocket

WebSocketEndpoint = typing.Callable[[WebSocket], typing.Awaitable[None]]


@pytest.fixture
def on_receive_message() -> MagicMock:
    return MagicMock()


@pytest.fixture(params=("wsproto", "websockets-sansio"))
def websocket_implementation(request: pytest.FixtureRequest) -> typing.Literal["wsproto", "websockets-sansio"]:
    return request.param  # type: ignore[no-any-return]


class ServerFactoryFixture(typing.Protocol):
    def __call__(self, endpoint: WebSocketEndpoint) -> contextlib.AbstractContextManager[str]: ...


@pytest.fixture
def server_factory(websocket_implementation: typing.Literal["wsproto", "websockets-sansio"]) -> ServerFactoryFixture:
    @contextlib.contextmanager
    def _server_factory(endpoint: WebSocketEndpoint) -> typing.Iterator[str]:
        shutdown_queue: queue.Queue[bool] = queue.Queue()

        def create_app() -> Starlette:
            routes = [WebSocketRoute("/ws", endpoint=endpoint)]
            return Starlette(routes=routes)

        def create_server(app: Starlette, socket: str) -> uvicorn.Server:
            config = uvicorn.Config(app, uds=socket, ws=websocket_implementation, lifespan="off")
            return uvicorn.Server(config)

        def on_server_stopped(_task: object) -> None:
            shutdown_queue.put(True)

        with start_blocking_portal(backend="asyncio") as portal:
            with tempfile.TemporaryDirectory() as socket_directory:
                socket = str(pathlib.Path(socket_directory) / "socket.sock")
                app = create_app()
                server = create_server(app, socket)
                task = portal.start_task_soon(server.serve)
                task.add_done_callback(on_server_stopped)
                while not server.started and not task.done():
                    time.sleep(0.01)
                if task.done() and task.exception() is not None:  # pragma: no cover
                    raise typing.cast(BaseException, task.exception())
                yield socket
                server.should_exit = True
                shutdown_queue.get(True)

    return _server_factory
