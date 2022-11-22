import asyncio
from typing import Callable
from unittest.mock import MagicMock

import pytest
from starlette.applications import Starlette
from starlette.routing import WebSocketRoute
from starlette.types import ASGIApp


@pytest.fixture(scope="session")
def event_loop():
    """Force the pytest-asyncio loop to be the main one."""
    loop = asyncio.get_event_loop()
    yield loop


@pytest.fixture
def on_receive_message():
    return MagicMock()


@pytest.fixture
def websocket_app_factory() -> Callable[[Callable], ASGIApp]:
    def _websocket_app_factory(endpoint: Callable):
        routes = [
            WebSocketRoute("/ws", endpoint=endpoint),
        ]
        return Starlette(routes=routes)

    return _websocket_app_factory
