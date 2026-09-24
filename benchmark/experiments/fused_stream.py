from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, TypeAlias

import zttp

import httpx2

from .protocol_connection import ProtocolConnection

if TYPE_CHECKING:
    from .fused_transport import FusedTransport


class FusedConnection:
    def __init__(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, key: tuple[str, str, int], read_size: int
    ) -> None:
        self.reader = reader
        self.writer = writer
        self.key = key
        self.parser = zttp.Connection(zttp.CLIENT)
        self.expires = 0.0
        self.done = False
        self.read_size = read_size

    async def write(self, data: bytes, timeout: float | None) -> None:
        try:
            self.writer.write(data)
            if self.writer.transport.get_write_buffer_size() or self.writer.is_closing():
                async with asyncio.timeout(timeout):
                    await self.writer.drain()
        except TimeoutError as exc:
            raise httpx2.WriteTimeout(str(exc)) from exc
        except OSError as exc:
            raise httpx2.WriteError(str(exc)) from exc

    async def event(self, timeout: float | None) -> zttp.Response | zttp.Data | zttp.EndOfMessage:
        try:
            event = self.parser.next_event()
            while event is zttp.NEED_DATA:
                try:
                    async with asyncio.timeout(timeout):
                        data = await self.reader.read(self.read_size)
                except TimeoutError as exc:
                    raise httpx2.ReadTimeout(str(exc)) from exc
                except OSError as exc:
                    raise httpx2.ReadError(str(exc)) from exc
                event = self.parser.receive_event(data)
                if not data and event is zttp.NEED_DATA:
                    raise httpx2.RemoteProtocolError("Server disconnected before completing response")
            if isinstance(event, (zttp.Response, zttp.Data, zttp.EndOfMessage)):
                return event
            raise httpx2.RemoteProtocolError("Server disconnected before completing response")
        except zttp.RemoteProtocolError as exc:
            raise httpx2.RemoteProtocolError(str(exc)) from exc

    async def aclose(self) -> None:
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except OSError:
            pass

    def is_closed(self) -> bool:
        return self.reader.at_eof() or self.writer.is_closing()


WireConnection: TypeAlias = FusedConnection | ProtocolConnection


class FusedStream(httpx2.AsyncByteStream):
    def __init__(self, connection: WireConnection, pool: FusedTransport, timeout: float | None) -> None:
        self.connection = connection
        self.pool = pool
        self.timeout = timeout
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        while True:
            event = await self.connection.event(self.timeout)
            if isinstance(event, zttp.Data):
                yield event.data
            elif isinstance(event, zttp.EndOfMessage):
                self.connection.done = True
                return
            else:
                raise httpx2.RemoteProtocolError("Unexpected event in response body")

    async def aclose(self) -> None:
        if self.closed:
            return
        self.closed = True
        await self.pool.release(self.connection)
