import asyncio
import contextlib
import pathlib
import queue
import sys
import tempfile
from typing import Callable, ContextManager
from unittest.mock import MagicMock

if sys.version_info < (3, 8):
    from typing_extensions import Literal, Protocol  # pragma: no cover
else:
    from typing import Literal, Protocol  # pragma: no cover

import pytest
import uvicorn
from anyio.from_thread import start_blocking_portal
from starlette.applications import Starlette
from starlette.routing import WebSocketRoute


@pytest.fixture(scope="session")
def event_loop():
    """Force the pytest-asyncio loop to be the main one."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def on_receive_message():
    return MagicMock()


@pytest.fixture(params=("wsproto", "websockets"))
def websocket_implementation(request) -> Literal["wsproto", "websockets"]:
    return request.param


class ServerFactoryFixture(Protocol):
    def __call__(self, endpoint: Callable) -> ContextManager[str]:
        ...


@pytest.fixture
def server_factory(
    websocket_implementation: Literal["wsproto", "websockets"]
) -> ServerFactoryFixture:
    @contextlib.contextmanager
    def _server_factory(endpoint: Callable):
        startup_queue: queue.Queue[bool] = queue.Queue()
        shutdown_queue: queue.Queue[bool] = queue.Queue()

        def create_app() -> Starlette:
            routes = [
                WebSocketRoute("/ws", endpoint=endpoint),
            ]

            async def on_startup():
                startup_queue.put(True)

            return Starlette(routes=routes, on_startup=[on_startup])

        def create_server(app: Starlette, socket: str):
            config = uvicorn.Config(app, uds=socket, ws=websocket_implementation)
            return uvicorn.Server(config)

        def on_server_stopped(_task):
            shutdown_queue.put(True)

        with start_blocking_portal(backend="asyncio") as portal:
            with tempfile.TemporaryDirectory() as socket_directory:
                socket = str(pathlib.Path(socket_directory) / "socket.sock")
                app = create_app()
                server = create_server(app, socket)
                task = portal.start_task_soon(server.serve)
                task.add_done_callback(on_server_stopped)
                startup_queue.get(True)
                yield socket
                server.should_exit = True
                shutdown_queue.get(True)

    return _server_factory
