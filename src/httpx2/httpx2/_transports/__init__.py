import sys

from .asgi import ASGITransport
from .base import AsyncBaseTransport, BaseTransport
from .mock import MockTransport
from .wsgi import WSGITransport

if sys.platform == "emscripten":  # pragma: nocover
    if sys.version_info < (3, 12):
        raise RuntimeError(
            "Python 3.12 or later is required on emscripten platforms."
        )

    # in emscripten we use javascript fetch
    from httpx2_jsfetch import (
        JavascriptFetchTransport as HTTPTransport,
        AsyncJavascriptFetchTransport as AsyncHTTPTransport,
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
