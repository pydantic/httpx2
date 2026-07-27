from __future__ import annotations

import typing
from collections.abc import AsyncIterator, Iterator
from contextlib import AsyncExitStack, ExitStack

from .._models import Request, Response
from .._types import AsyncByteStream, SyncByteStream
from .base import AsyncBaseTransport, BaseTransport

if typing.TYPE_CHECKING:  # pragma: no cover
    from pyreqwest.client import Client as PyreqwestClient, SyncClient as PyreqwestSyncClient
    from pyreqwest.response import Response as PyreqwestResponse, SyncResponse as PyreqwestSyncResponse

__all__ = ["AsyncPyreqwestTransport", "PyreqwestTransport"]


def _load_pyreqwest_sync_client_builder() -> typing.Any:
    try:
        from pyreqwest.client import SyncClientBuilder
    except ImportError as exc:  # pragma: no cover
        msg = "Using 'PyreqwestTransport' requires installing the 'pyreqwest' package."
        raise RuntimeError(msg) from exc
    return SyncClientBuilder


def _load_pyreqwest_async_client_builder() -> typing.Any:
    try:
        from pyreqwest.client import ClientBuilder
    except ImportError as exc:  # pragma: no cover
        msg = "Using 'AsyncPyreqwestTransport' requires installing the 'pyreqwest' package."
        raise RuntimeError(msg) from exc
    return ClientBuilder


def _http_version(version: str) -> bytes:
    if version == "HTTP/2.0":
        version = "HTTP/2"
    return version.encode("ascii", errors="ignore")


class _PyreqwestStream(SyncByteStream):
    def __init__(self, response: PyreqwestSyncResponse, exit_stack: ExitStack) -> None:
        self._reader = response.body_reader
        self._exit_stack = exit_stack

    def __iter__(self) -> Iterator[bytes]:
        while (chunk := self._reader.read()) is not None:
            yield bytes(chunk)

    def close(self) -> None:
        self._exit_stack.close()


class _AsyncPyreqwestStream(AsyncByteStream):
    def __init__(self, response: PyreqwestResponse, exit_stack: AsyncExitStack) -> None:
        self._reader = response.body_reader
        self._exit_stack = exit_stack

    async def __aiter__(self) -> AsyncIterator[bytes]:
        while (chunk := await self._reader.read()) is not None:
            yield bytes(chunk)

    async def aclose(self) -> None:
        await self._exit_stack.aclose()


class PyreqwestTransport(BaseTransport):
    """A sync transport backed by pyreqwest/reqwest."""

    def __init__(self, client: PyreqwestSyncClient | None = None, *, close_client: bool = True) -> None:
        SyncClientBuilder = _load_pyreqwest_sync_client_builder()
        self._client = client or SyncClientBuilder().follow_redirects(False).default_cookie_store(False).build()
        self._close_client = client is None or close_client

    def handle_request(self, request: Request) -> Response:
        content = request.read()
        builder = self._client.request(request.method, str(request.url)).headers(request.headers.multi_items())
        if content:
            builder = builder.body_bytes(content)

        exit_stack = ExitStack()
        try:
            response = exit_stack.enter_context(builder.build_streamed())
            return Response(
                response.status,
                headers=list(response.headers.items()),
                stream=_PyreqwestStream(response, exit_stack),
                extensions={"http_version": _http_version(response.version)},
            )
        except BaseException:
            exit_stack.close()
            raise

    def close(self) -> None:
        if self._close_client:
            self._client.close()


class AsyncPyreqwestTransport(AsyncBaseTransport):
    """An async transport backed by pyreqwest/reqwest."""

    def __init__(self, client: PyreqwestClient | None = None, *, close_client: bool = True) -> None:
        ClientBuilder = _load_pyreqwest_async_client_builder()
        self._client = client or ClientBuilder().follow_redirects(False).default_cookie_store(False).build()
        self._close_client = client is None or close_client

    async def handle_async_request(self, request: Request) -> Response:
        content = await request.aread()
        builder = self._client.request(request.method, str(request.url)).headers(request.headers.multi_items())
        if content:
            builder = builder.body_bytes(content)

        exit_stack = AsyncExitStack()
        try:
            response = await exit_stack.enter_async_context(builder.build_streamed())
            return Response(
                response.status,
                headers=list(response.headers.items()),
                stream=_AsyncPyreqwestStream(response, exit_stack),
                extensions={"http_version": _http_version(response.version)},
            )
        except BaseException:
            await exit_stack.aclose()
            raise

    async def aclose(self) -> None:
        if self._close_client:
            await self._client.close()
