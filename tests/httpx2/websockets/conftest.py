import asyncio
import contextlib
import queue
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
    loop = asyncio.get_event_loop()
    yield loop


@pytest.fixture
def on_receive_message():
    return MagicMock()


ServerFactoryFixture = Callable[[Callable], ContextManager[None]]


@pytest.fixture
def server_factory() -> ServerFactoryFixture:
    @contextlib.contextmanager
    def _server_factory(endpoint: Callable):
        startup_queue: queue.Queue[bool] = queue.Queue()

        async def start_uvicorn():
            routes = [
                WebSocketRoute("/ws", endpoint=endpoint),
            ]

            async def on_startup():
                startup_queue.put(True)

            app = Starlette(routes=routes, on_startup=[on_startup])
            config = uvicorn.Config(app, port=8000)
            server = uvicorn.Server(config)
            await server.serve()

        with start_blocking_portal(backend="asyncio") as portal:
            future = portal.start_task_soon(start_uvicorn)
            startup_queue.get(True)
            yield
            future.cancel()

    return _server_factory
