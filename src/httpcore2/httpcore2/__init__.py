from importlib.metadata import version

from ._api import request
from ._api import stream
from ._async import AsyncConnectionInterface
from ._async import AsyncConnectionPool
from ._async import AsyncHTTP2Connection
from ._async import AsyncHTTP11Connection
from ._async import AsyncHTTPConnection
from ._async import AsyncHTTPProxy
from ._async import AsyncSOCKSProxy
from ._backends.base import SOCKET_OPTION
from ._backends.base import AsyncNetworkBackend
from ._backends.base import AsyncNetworkStream
from ._backends.base import NetworkBackend
from ._backends.base import NetworkStream
from ._backends.mock import AsyncMockBackend
from ._backends.mock import AsyncMockStream
from ._backends.mock import MockBackend
from ._backends.mock import MockStream
from ._backends.sync import SyncBackend
from ._exceptions import ConnectError
from ._exceptions import ConnectionNotAvailable
from ._exceptions import ConnectTimeout
from ._exceptions import LocalProtocolError
from ._exceptions import NetworkError
from ._exceptions import PoolTimeout
from ._exceptions import ProtocolError
from ._exceptions import ProxyError
from ._exceptions import ReadError
from ._exceptions import ReadTimeout
from ._exceptions import RemoteProtocolError
from ._exceptions import TimeoutException
from ._exceptions import UnsupportedProtocol
from ._exceptions import WriteError
from ._exceptions import WriteTimeout
from ._models import URL
from ._models import Origin
from ._models import Proxy
from ._models import Request
from ._models import Response
from ._ssl import default_ssl_context
from ._sync import ConnectionInterface
from ._sync import ConnectionPool
from ._sync import HTTP2Connection
from ._sync import HTTP11Connection
from ._sync import HTTPConnection
from ._sync import HTTPProxy
from ._sync import SOCKSProxy

# The 'httpcore2.AnyIOBackend' class is conditional on 'anyio' being installed.
try:
    from ._backends.anyio import AnyIOBackend
except ImportError:  # pragma: no cover

    class AnyIOBackend:  # type: ignore
        def __init__(self, *args, **kwargs):  # type: ignore
            msg = "Attempted to use 'httpcore2.AnyIOBackend' but 'anyio' is not installed."
            raise RuntimeError(msg)


# The 'httpcore2.TrioBackend' class is conditional on 'trio' being installed.
try:
    from ._backends.trio import TrioBackend
except ImportError:  # pragma: no cover

    class TrioBackend:  # type: ignore
        def __init__(self, *args, **kwargs):  # type: ignore
            msg = "Attempted to use 'httpcore2.TrioBackend' but 'trio' is not installed."
            raise RuntimeError(msg)


__all__ = [
    # top-level requests
    "request",
    "stream",
    # models
    "Origin",
    "URL",
    "Request",
    "Response",
    "Proxy",
    # async
    "AsyncHTTPConnection",
    "AsyncConnectionPool",
    "AsyncHTTPProxy",
    "AsyncHTTP11Connection",
    "AsyncHTTP2Connection",
    "AsyncConnectionInterface",
    "AsyncSOCKSProxy",
    # sync
    "HTTPConnection",
    "ConnectionPool",
    "HTTPProxy",
    "HTTP11Connection",
    "HTTP2Connection",
    "ConnectionInterface",
    "SOCKSProxy",
    # network backends, implementations
    "SyncBackend",
    "AnyIOBackend",
    "TrioBackend",
    # network backends, mock implementations
    "AsyncMockBackend",
    "AsyncMockStream",
    "MockBackend",
    "MockStream",
    # network backends, interface
    "AsyncNetworkStream",
    "AsyncNetworkBackend",
    "NetworkStream",
    "NetworkBackend",
    # util
    "default_ssl_context",
    "SOCKET_OPTION",
    # exceptions
    "ConnectionNotAvailable",
    "ProxyError",
    "ProtocolError",
    "LocalProtocolError",
    "RemoteProtocolError",
    "UnsupportedProtocol",
    "TimeoutException",
    "PoolTimeout",
    "ConnectTimeout",
    "ReadTimeout",
    "WriteTimeout",
    "NetworkError",
    "ConnectError",
    "ReadError",
    "WriteError",
]

__version__ = version("httpcore2")


__locals = locals()
for __name in __all__:
    # Exclude SOCKET_OPTION, it causes AttributeError on Python 3.14
    if not __name.startswith(("__", "SOCKET_OPTION")):
        setattr(__locals[__name], "__module__", "httpcore2")  # noqa
