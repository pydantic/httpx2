from __future__ import annotations

import asyncio
import ssl
from collections.abc import Iterable
from typing import Any

import httpcore2


class AsyncioBackend:
    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: Iterable[httpcore2.SOCKET_OPTION] | None = None,
    ) -> AsyncioStream:
        try:
            async with asyncio.timeout(timeout):
                reader, writer = await asyncio.open_connection(
                    host, port, local_addr=None if local_address is None else (local_address, 0)
                )
        except TimeoutError as exc:
            raise httpcore2.ConnectTimeout(str(exc)) from exc
        except OSError as exc:
            raise httpcore2.ConnectError(str(exc)) from exc
        try:
            for option in socket_options or ():
                writer.get_extra_info("socket").setsockopt(*option)
        except OSError:
            writer.close()
            raise
        return AsyncioStream(reader, writer)

    async def connect_unix_socket(
        self, path: str, timeout: float | None = None, socket_options: Iterable[httpcore2.SOCKET_OPTION] | None = None
    ) -> AsyncioStream:
        raise NotImplementedError("The asyncio experiment supports TCP only")

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


class AsyncioStream:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.reader = reader
        self.writer = writer

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        try:
            async with asyncio.timeout(timeout):
                return await self.reader.read(max_bytes)
        except TimeoutError as exc:
            raise httpcore2.ReadTimeout(str(exc)) from exc
        except OSError as exc:
            raise httpcore2.ReadError(str(exc)) from exc

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        if not buffer:
            return
        try:
            async with asyncio.timeout(timeout):
                self.writer.write(buffer)
                await self.writer.drain()
        except TimeoutError as exc:
            raise httpcore2.WriteTimeout(str(exc)) from exc
        except OSError as exc:
            raise httpcore2.WriteError(str(exc)) from exc

    async def aclose(self) -> None:
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except OSError:
            pass

    async def start_tls(
        self, ssl_context: ssl.SSLContext, server_hostname: str | None = None, timeout: float | None = None
    ) -> AsyncioStream:
        try:
            async with asyncio.timeout(timeout):
                await self.writer.start_tls(ssl_context, server_hostname=server_hostname)
        except TimeoutError as exc:
            self.writer.close()
            raise httpcore2.ConnectTimeout(str(exc)) from exc
        except OSError as exc:
            self.writer.close()
            raise httpcore2.ConnectError(str(exc)) from exc
        except BaseException:
            self.writer.close()
            raise
        return self

    def get_extra_info(self, info: str) -> Any:
        if info == "is_readable":
            return self.reader.at_eof() or self.reader.exception() is not None or self.writer.is_closing()
        return self.writer.get_extra_info({"client_addr": "sockname", "server_addr": "peername"}.get(info, info))
