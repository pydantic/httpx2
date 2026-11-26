from __future__ import annotations

import contextlib
from collections.abc import Generator

from ._types import ExceptionMapping


@contextlib.contextmanager
def map_exceptions(map: ExceptionMapping) -> Generator[None]:
    try:
        yield
    except Exception as exc:
        for from_exc, to_exc in map.items():
            if isinstance(exc, from_exc):
                raise to_exc(exc) from exc
        raise  # pragma: no cover


class ConnectionNotAvailable(Exception):
    pass


class ProxyError(Exception):
    pass


class UnsupportedProtocol(Exception):
    pass


class ProtocolError(Exception):
    pass


class RemoteProtocolError(ProtocolError):
    pass


class LocalProtocolError(ProtocolError):
    pass


# Timeout errors


class TimeoutException(Exception):
    pass


class PoolTimeout(TimeoutException):
    pass


class ConnectTimeout(TimeoutException):
    pass


class ReadTimeout(TimeoutException):
    pass


class WriteTimeout(TimeoutException):
    pass


# Network errors


class NetworkError(Exception):
    pass


class ConnectError(NetworkError):
    pass


class ReadError(NetworkError):
    pass


class WriteError(NetworkError):
    pass


ConnectionNotAvailable.__module__ = "httpcore2"
ProxyError.__module__ = "httpcore2"
ProtocolError.__module__ = "httpcore2"
LocalProtocolError.__module__ = "httpcore2"
RemoteProtocolError.__module__ = "httpcore2"
UnsupportedProtocol.__module__ = "httpcore2"
TimeoutException.__module__ = "httpcore2"
PoolTimeout.__module__ = "httpcore2"
ConnectTimeout.__module__ = "httpcore2"
ReadTimeout.__module__ = "httpcore2"
WriteTimeout.__module__ = "httpcore2"
NetworkError.__module__ = "httpcore2"
ConnectError.__module__ = "httpcore2"
ReadError.__module__ = "httpcore2"
WriteError.__module__ = "httpcore2"
