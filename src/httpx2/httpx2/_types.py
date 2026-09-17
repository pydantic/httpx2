"""
Type definitions for type checking purposes.
"""

import inspect
import sys
from collections.abc import AsyncIterable, AsyncIterator, Callable, Iterable, Iterator, Mapping, Sequence
from http.cookiejar import CookieJar
from types import BuiltinFunctionType
from typing import IO, TYPE_CHECKING, Any, Protocol, Union

if sys.version_info >= (3, 13):
    from typing import TypeIs  # pragma: no cover
else:
    from typing_extensions import TypeIs  # pragma: no cover

if TYPE_CHECKING:
    from ._auth import Auth  # noqa: F401
    from ._config import Proxy, Timeout  # noqa: F401
    from ._models import Cookies, Headers, Request  # noqa: F401
    from ._urls import URL, QueryParams  # noqa: F401


PrimitiveData = str | int | float | bool | None

URLTypes = Union["URL", str]

QueryParamTypes = Union[
    "QueryParams",
    Mapping[str, PrimitiveData | Sequence[PrimitiveData]],
    list[tuple[str, PrimitiveData]],
    tuple[tuple[str, PrimitiveData], ...],
    str,
    bytes,
]

HeaderTypes = Union[
    "Headers",
    Mapping[str, str],
    Mapping[bytes, bytes],
    Sequence[tuple[str, str]],
    Sequence[tuple[bytes, bytes]],
]

CookieTypes = Union["Cookies", CookieJar, dict[str, str], list[tuple[str, str]]]

TimeoutTypes = Union[float | None, tuple[float | None, float | None, float | None, float | None], "Timeout"]
ProxyTypes = Union["URL", str, "Proxy"]
CertTypes = str | tuple[str, str] | tuple[str, str, str]

AuthTypes = Union[tuple[str | bytes, str | bytes], Callable[["Request"], "Request"], "Auth"]

RequestContent = str | bytes | Iterable[bytes] | AsyncIterable[bytes]
ResponseContent = str | bytes | Iterable[bytes] | AsyncIterable[bytes]
ResponseExtensions = Mapping[str, Any]

RequestData = Mapping[str, Any]


class AsyncReadableFile(Protocol):
    """
    A file-like object with awaitable reads, as returned by `anyio.open_file()`,
    `trio.open_file()` or `aiofiles.open()`.
    """

    async def read(self, size: int = -1, /) -> bytes: ...

    async def seek(self, offset: int, whence: int = ..., /) -> int: ...


def is_async_readable_file(fileobj: Any) -> TypeIs[AsyncReadableFile]:
    """
    Determine whether a file-like object has to be read with `await`.

    Only an awaitable `read()` is required. `seek()` and `fileno()` are used
    when present, so that non-seekable or in-memory async streams still upload.
    """
    read = getattr(fileobj, "read", None)
    if read is None or isinstance(read, BuiltinFunctionType):
        # `iscoroutinefunction()` is an order of magnitude more expensive than
        # this, and always answers `False` for C callables such as the `read()`
        # of `open(...)` or `io.BytesIO`, which can carry neither `CO_COROUTINE`
        # nor the `markcoroutinefunction()` marker.
        return False
    return inspect.iscoroutinefunction(read)


FileContent = IO[bytes] | bytes | str | AsyncReadableFile
FileTypes = (
    # # file (or bytes)
    FileContent
    # # (filename, file (or bytes))
    | tuple[str | None, FileContent]
    # # (filename, file (or bytes), content_type)
    | tuple[str | None, FileContent, str | None]
    | tuple[str | None, FileContent, str | None, Mapping[str, str]]
)
RequestFiles = Mapping[str, FileTypes] | Sequence[tuple[str, FileTypes]]

RequestExtensions = Mapping[str, Any]

__all__ = ["AsyncByteStream", "SyncByteStream"]


class SyncByteStream:
    def __iter__(self) -> Iterator[bytes]:
        raise NotImplementedError("The '__iter__' method must be implemented.")  # pragma: no cover
        yield b""  # pragma: no cover

    def close(self) -> None:
        """
        Subclasses can override this method to release any network resources
        after a request/response cycle is complete.
        """


class AsyncByteStream:
    async def __aiter__(self) -> AsyncIterator[bytes]:
        raise NotImplementedError("The '__aiter__' method must be implemented.")  # pragma: no cover
        yield b""  # pragma: no cover

    async def aclose(self) -> None:
        pass
