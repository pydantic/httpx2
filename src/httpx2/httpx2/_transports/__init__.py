import sys

from .asgi import ASGITransport
from .base import AsyncBaseTransport, BaseTransport
from .mock import MockTransport
from .wsgi import WSGITransport

if sys.platform == "emscripten":  # pragma: nocover
    # in emscripten we use javascript fetch
    from httpx2_jsfetch import (
        AsyncJavascriptFetchTransport as HTTPTransport,
        JavascriptFetchTransport as AsyncHTTPTransport,
    )
else:
    # everywhere else we use default
    from .default import AsyncHTTPTransport, HTTPTransport

__all__ = [
    "ASGITransport",
    "AsyncBaseTransport",
    "BaseTransport",
    "AsyncHTTPTransport",
    "HTTPTransport",
    "MockTransport",
    "WSGITransport",
]
