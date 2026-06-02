from .connection import AsyncHTTPConnection
from .connection_pool import AsyncConnectionPool
from .http11 import AsyncHTTP11Connection
from .http_proxy import AsyncHTTPProxy
from .interfaces import AsyncConnectionInterface

try:
    from .http2 import AsyncHTTP2Connection
except ImportError:  # pragma: no cover

    class _AsyncHTTP2Connection(AsyncConnectionInterface):  # type: ignore
        def __init__(self, *args, **kwargs) -> None:  # type: ignore
            raise RuntimeError(
                "Attempted to use http2 support, but the `h2` package is not "
                "installed. Use 'pip install httpcore[http2]'."
            )

    AsyncHTTP2Connection = _AsyncHTTP2Connection


try:
    from .socks_proxy import AsyncSOCKSProxy
except ImportError:  # pragma: no cover

    class _AsyncSOCKSProxy(AsyncConnectionPool):  # type: ignore
        def __init__(self, *args, **kwargs) -> None:  # type: ignore
            raise RuntimeError(
                "Attempted to use SOCKS support, but the `socksio` package is not "
                "installed. Use 'pip install httpcore[socks]'."
            )

    AsyncSOCKSProxy = _AsyncSOCKSProxy


__all__ = [
    "AsyncHTTPConnection",
    "AsyncConnectionPool",
    "AsyncHTTPProxy",
    "AsyncHTTP11Connection",
    "AsyncHTTP2Connection",
    "AsyncConnectionInterface",
    "AsyncSOCKSProxy",
]
