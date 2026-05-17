from .__version__ import __description__, __title__, __version__
from ._api import *
from ._auth import *
from ._client import *
from ._config import *
from ._content import *
from ._exceptions import *
from ._models import *
from ._status_codes import *
from ._transports import *
from ._types import *
from ._urls import *

__all__ = [
    # Package metadata
    "__description__",
    "__title__",
    "__version__",
    # Top-level API (_api)
    "delete",
    "get",
    "head",
    "options",
    "patch",
    "post",
    "put",
    "request",
    "stream",
    # Authentication (_auth)
    "Auth",
    "BasicAuth",
    "DigestAuth",
    "FunctionAuth",
    "NetRCAuth",
    # Clients (_client)
    "AsyncClient",
    "Client",
    "USE_CLIENT_DEFAULT",
    "UseClientDefault",
    # Configuration (_config)
    "create_ssl_context",
    "Limits",
    "Proxy",
    "Timeout",
    # Content (_content)
    "ByteStream",
    # Exceptions (_exceptions)
    "CloseError",
    "ConnectError",
    "ConnectTimeout",
    "CookieConflict",
    "DecodingError",
    "HTTPError",
    "HTTPStatusError",
    "InvalidURL",
    "LocalProtocolError",
    "NetworkError",
    "PoolTimeout",
    "ProtocolError",
    "ProxyError",
    "ReadError",
    "ReadTimeout",
    "RemoteProtocolError",
    "RequestError",
    "RequestNotRead",
    "ResponseNotRead",
    "StreamClosed",
    "StreamConsumed",
    "StreamError",
    "TimeoutException",
    "TooManyRedirects",
    "TransportError",
    "UnsupportedProtocol",
    "WriteError",
    "WriteTimeout",
    # Models (_models)
    "Cookies",
    "Headers",
    "Request",
    "Response",
    # Status codes (_status_codes)
    "codes",
    # Transports (_transports)
    "ASGITransport",
    "AsyncBaseTransport",
    "AsyncHTTPTransport",
    "BaseTransport",
    "HTTPTransport",
    "MockTransport",
    "WSGITransport",
    # Typing (_types)
    "AsyncByteStream",
    "AuthTypes",
    "CertTypes",
    "CookieTypes",
    "FileContent",
    "FileTypes",
    "HeaderTypes",
    "PrimitiveData",
    "ProxyTypes",
    "QueryParamTypes",
    "RequestContent",
    "RequestData",
    "RequestExtensions",
    "RequestFiles",
    "ResponseContent",
    "ResponseExtensions",
    "SyncByteStream",
    "TimeoutTypes",
    "URLTypes",
    # URLs (_urls)
    "QueryParams",
    "URL",
]


__locals = locals()
for __name in __all__:
    if not __name.startswith("__"):
        try:
            setattr(__locals[__name], "__module__", "httpx2")  # noqa
        except (AttributeError, TypeError):
            # Type aliases (typing.Union, typing.Optional, ...) don't support __module__ assignment.
            pass


def __getattr__(name: str) -> object:  # pragma: no cover
    if name == "main":
        import warnings

        warnings.warn(
            "`httpx2.main` is deprecated and will be removed in a future release. "
            "Use the `httpx2` CLI entry point instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        from ._main import main

        return main

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
