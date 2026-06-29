import typing as _typing

from .__version__ import __description__, __title__, __version__
from ._api import *
from ._auth import *
from ._client import *
from ._config import *
from ._content import *
from ._exceptions import *
from ._models import *
from ._sse import *
from ._status_codes import *
from ._transports import *
from ._types import *
from ._urls import *

if _typing.TYPE_CHECKING:
    from ._websockets._api import AsyncWebSocketSession, WebSocketSession
    from ._websockets._exceptions import (
        HTTPXWSException,
        WebSocketDisconnect,
        WebSocketInvalidTypeReceived,
        WebSocketNetworkError,
        WebSocketUpgradeError,
    )
    from ._websockets._transport import ASGIWebSocketTransport

__all__ = [
    "__description__",
    "__title__",
    "__version__",
    "ASGITransport",
    "ASGIWebSocketTransport",
    "AsyncBaseTransport",
    "AsyncByteStream",
    "AsyncClient",
    "AsyncHTTPTransport",
    "AsyncWebSocketSession",
    "Auth",
    "BaseTransport",
    "BasicAuth",
    "ByteStream",
    "Client",
    "CloseError",
    "codes",
    "ConnectError",
    "ConnectTimeout",
    "CookieConflict",
    "Cookies",
    "create_ssl_context",
    "DecodingError",
    "delete",
    "DigestAuth",
    "EventSource",
    "FunctionAuth",
    "get",
    "head",
    "Headers",
    "HTTPError",
    "HTTPStatusError",
    "HTTPTransport",
    "HTTPXWSException",
    "InvalidURL",
    "Limits",
    "LocalProtocolError",
    "MockTransport",
    "NetRCAuth",
    "NetworkError",
    "options",
    "patch",
    "PoolTimeout",
    "post",
    "ProtocolError",
    "Proxy",
    "ProxyError",
    "put",
    "QueryParams",
    "ReadError",
    "ReadTimeout",
    "RemoteProtocolError",
    "request",
    "Request",
    "RequestError",
    "RequestNotRead",
    "Response",
    "ResponseNotRead",
    "ServerSentEvent",
    "SSEError",
    "stream",
    "StreamClosed",
    "StreamConsumed",
    "StreamError",
    "SyncByteStream",
    "Timeout",
    "TimeoutException",
    "TooManyRedirects",
    "TransportError",
    "UnsupportedProtocol",
    "URL",
    "USE_CLIENT_DEFAULT",
    "websocket",
    "WebSocketDisconnect",
    "WebSocketInvalidTypeReceived",
    "WebSocketNetworkError",
    "WebSocketSession",
    "WebSocketUpgradeError",
    "WriteError",
    "WriteTimeout",
    "WSGITransport",
]


_WEBSOCKET_NAMES = {
    "ASGIWebSocketTransport",
    "AsyncWebSocketSession",
    "HTTPXWSException",
    "WebSocketDisconnect",
    "WebSocketInvalidTypeReceived",
    "WebSocketNetworkError",
    "WebSocketSession",
    "WebSocketUpgradeError",
}

__locals = locals()
for __name in __all__:
    if not __name.startswith("__") and __name not in _WEBSOCKET_NAMES:
        setattr(__locals[__name], "__module__", "httpx2")  # noqa


def __getattr__(name: str) -> object:
    if name == "main":  # pragma: no cover
        import warnings

        warnings.warn(
            "`httpx2.main` is deprecated and will be removed in a future release. "
            "Use the `httpx2` CLI entry point instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        from ._main import main

        return main

    if name in _WEBSOCKET_NAMES:
        from . import _websockets
        from ._websockets._defaults import WS_EXTRA_INSTALL_MESSAGE

        try:
            return getattr(_websockets, name)
        except ImportError:
            raise ImportError(WS_EXTRA_INSTALL_MESSAGE)

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
