import asyncio
import contextlib
import pathlib
import queue
import tempfile
from typing import Callable, ContextManager
from unittest.mock import MagicMock

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


ServerFactoryFixture = Callable[[Callable], ContextManager[str]]


@pytest.fixture
def server_factory() -> ServerFactoryFixture:
    @contextlib.contextmanager
    def _server_factory(endpoint: Callable):
        startup_queue: queue.Queue[bool] = queue.Queue()

        async def start_uvicorn(socket: str):
            routes = [
                WebSocketRoute("/ws", endpoint=endpoint),
            ]

            async def on_startup():
                startup_queue.put(True)

            app = Starlette(routes=routes, on_startup=[on_startup])
            config = uvicorn.Config(app, uds=socket)
            server = uvicorn.Server(config)
            await server.serve()

        with start_blocking_portal(backend="asyncio") as portal:
            with tempfile.TemporaryDirectory() as socket_directory:
                socket_path = str(pathlib.Path(socket_directory) / "socket.sock")
                future = portal.start_task_soon(start_uvicorn, socket_path)
                startup_queue.get(True)
                yield socket_path
                future.cancel()

    return _server_factory
