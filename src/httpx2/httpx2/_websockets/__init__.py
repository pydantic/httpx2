"""
WebSocket support, derived from httpx-ws (https://github.com/frankie567/httpx-ws).

Copyright (c) 2021 François Voron, MIT License (https://github.com/frankie567/httpx-ws/blob/main/LICENSE).
"""

from ._defaults import (
    DEFAULT_KEEPALIVE_PING_INTERVAL_SECONDS,
    DEFAULT_KEEPALIVE_PING_TIMEOUT_SECONDS,
    DEFAULT_MAX_MESSAGE_SIZE_BYTES,
    DEFAULT_QUEUE_SIZE,
)

__all__ = [
    "ASGIWebSocketTransport",
    "AsyncWebSocketSession",
    "DEFAULT_KEEPALIVE_PING_INTERVAL_SECONDS",
    "DEFAULT_KEEPALIVE_PING_TIMEOUT_SECONDS",
    "DEFAULT_MAX_MESSAGE_SIZE_BYTES",
    "DEFAULT_QUEUE_SIZE",
    "HTTPXWSException",
    "JSONMode",
    "WebSocketDisconnect",
    "WebSocketInvalidTypeReceived",
    "WebSocketNetworkError",
    "WebSocketSession",
    "WebSocketUpgradeError",
    "aconnect_ws",
    "connect_ws",
]

_API_NAMES = {
    "AsyncWebSocketSession",
    "JSONMode",
    "WebSocketSession",
    "aconnect_ws",
    "connect_ws",
}
_EXCEPTION_NAMES = {
    "HTTPXWSException",
    "WebSocketDisconnect",
    "WebSocketInvalidTypeReceived",
    "WebSocketNetworkError",
    "WebSocketUpgradeError",
}


def __getattr__(name: str) -> object:
    if name in _API_NAMES:
        from . import _api

        return getattr(_api, name)
    if name in _EXCEPTION_NAMES:
        from . import _exceptions

        return getattr(_exceptions, name)
    if name == "ASGIWebSocketTransport":
        from ._transport import ASGIWebSocketTransport

        return ASGIWebSocketTransport
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
