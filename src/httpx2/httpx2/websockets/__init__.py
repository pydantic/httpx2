"""
WebSocket support, derived from httpx-ws (https://github.com/frankie567/httpx-ws).

Copyright (c) 2021 François Voron, MIT License (https://github.com/frankie567/httpx-ws/blob/main/LICENSE).
"""

from ._api import AsyncWebSocketSession, JSONMode, WebSocketSession
from ._exceptions import (
    HTTPXWSException,
    WebSocketDisconnect,
    WebSocketInvalidTypeReceived,
    WebSocketNetworkError,
    WebSocketUpgradeError,
)
from ._transport import ASGIWebSocketTransport

__all__ = [
    "ASGIWebSocketTransport",
    "AsyncWebSocketSession",
    "HTTPXWSException",
    "JSONMode",
    "WebSocketDisconnect",
    "WebSocketInvalidTypeReceived",
    "WebSocketNetworkError",
    "WebSocketSession",
    "WebSocketUpgradeError",
]
