import sys

from .asgi import ASGITransport
from .base import AsyncBaseTransport, BaseTransport
from .mock import MockTransport
from .wsgi import WSGITransport

if sys.platform == "emscripten":  # pragma: nocover
    # in emscripten we use javascript fetch
    from .jsfetch import (
        AsyncJavascriptFetchTransport,
        JavascriptFetchTransport,
    )

    # override default transport names
    HTTPTransport = JavascriptFetchTransport
    AsyncHTTPTransport = AsyncJavascriptFetchTransport
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
