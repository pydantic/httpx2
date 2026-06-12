"""
Our exception hierarchy:

* WebSocketException
  x WebSocketUpgradeError
  x WebSocketDisconnect
  x WebSocketInvalidTypeReceived
  x WebSocketNetworkError
"""

from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    import wsproto

    from .._models import Response  # pragma: no cover

__all__ = [
    "WebSocketDisconnect",
    "WebSocketException",
    "WebSocketInvalidTypeReceived",
    "WebSocketNetworkError",
    "WebSocketUpgradeError",
]


class WebSocketException(Exception):
    """
    Base class for all WebSocket exceptions.
    """


class WebSocketUpgradeError(WebSocketException):
    """
    The initial connection didn't correctly upgrade to a WebSocket session.
    """

    def __init__(self, response: Response) -> None:
        self.response = response


class WebSocketDisconnect(WebSocketException):
    """
    The server closed the WebSocket session.
    """

    def __init__(self, code: int = 1000, reason: str | None = None) -> None:
        self.code = code
        self.reason = reason or ""


class WebSocketInvalidTypeReceived(WebSocketException):
    """
    A received event was not of the expected type.
    """

    def __init__(self, event: wsproto.events.Event) -> None:
        self.event = event


class WebSocketNetworkError(WebSocketException):
    """
    A network error occurred, typically because the underlying stream has closed or timed out.
    """
