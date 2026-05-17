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
    "__description__",
    "__title__",
    "__version__",
    "ASGITransport",
    "AsyncBaseTransport",
    "AsyncByteStream",
    "AsyncClient",
    "AsyncHTTPTransport",
    "Auth",
    "AuthTypes",
    "BaseTransport",
    "BasicAuth",
    "ByteStream",
    "CertTypes",
    "Client",
    "CloseError",
    "codes",
    "ConnectError",
    "ConnectTimeout",
    "CookieConflict",
    "Cookies",
    "CookieTypes",
    "create_ssl_context",
    "DecodingError",
    "delete",
    "DigestAuth",
    "FileContent",
    "FileTypes",
    "FunctionAuth",
    "get",
    "head",
    "Headers",
    "HeaderTypes",
    "HTTPError",
    "HTTPStatusError",
    "HTTPTransport",
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
    "PrimitiveData",
    "ProtocolError",
    "Proxy",
    "ProxyError",
    "ProxyTypes",
    "put",
    "QueryParams",
    "QueryParamTypes",
    "ReadError",
    "ReadTimeout",
    "RemoteProtocolError",
    "request",
    "Request",
    "RequestContent",
    "RequestData",
    "RequestError",
    "RequestExtensions",
    "RequestFiles",
    "RequestNotRead",
    "Response",
    "ResponseContent",
    "ResponseExtensions",
    "ResponseNotRead",
    "stream",
    "StreamClosed",
    "StreamConsumed",
    "StreamError",
    "SyncByteStream",
    "Timeout",
    "TimeoutException",
    "TimeoutTypes",
    "TooManyRedirects",
    "TransportError",
    "UnsupportedProtocol",
    "URL",
    "URLTypes",
    "USE_CLIENT_DEFAULT",
    "UseClientDefault",
    "WriteError",
    "WriteTimeout",
    "WSGITransport",
]


__locals = locals()
for __name in __all__:
    if not __name.startswith("__"):
        try:
            setattr(__locals[__name], "__module__", "httpx2")  # noqa
        except (AttributeError, TypeError):  # pragma: no cover
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
