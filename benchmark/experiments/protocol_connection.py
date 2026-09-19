from __future__ import annotations

import asyncio
from collections import deque
from typing import cast

import zttp

import httpx2

from .read_deadline import ReadDeadline
from .response_buffer import ResponseBuffer


class ProtocolConnection(asyncio.Protocol):
    def __init__(self, key: tuple[str, str, int], read_size: int) -> None:
        self.key = key
        self.parser = zttp.Connection(zttp.CLIENT)
        self.expires = 0.0
        self.done = False
        self.read_size = read_size
        self.transport: asyncio.Transport
        self.events: deque[zttp.Response | zttp.Data | zttp.EndOfMessage] = deque()
        self.waiter: asyncio.Future[None] | None = None
        self.drain_waiter: asyncio.Future[None] | None = None
        self.closed = asyncio.get_running_loop().create_future()
        self.error: httpx2.TransportError | None = None
        self.buffered = 0
        self.read_paused = False
        self.write_paused = False
        self.receiving = False
        self.read_deadline = ReadDeadline()
        self.response_buffer: ResponseBuffer | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = cast(asyncio.Transport, transport)

    def data_received(self, data: bytes) -> None:
        if not self.receiving:
            self.error = httpx2.RemoteProtocolError("Received data on an idle connection")
            self.transport.close()
            return
        try:
            event: zttp.Event = self.parser.receive_event(data)
            while isinstance(event, (zttp.Response, zttp.Data, zttp.EndOfMessage)):
                if self.response_buffer is not None:
                    self.response_buffer.receive(event)
                else:
                    self.events.append(event)
                    if isinstance(event, zttp.Data):
                        self.buffered += len(event.data)
                if isinstance(event, zttp.EndOfMessage):
                    self.receiving = False
                    break
                event = self.parser.next_event()
            if self.receiving and self.buffered > self.read_size and not self.read_paused:
                self.transport.pause_reading()
                self.read_paused = True
        except (zttp.RemoteProtocolError, httpx2.RemoteProtocolError) as exc:
            self.error = httpx2.RemoteProtocolError(str(exc))
            self.transport.close()
        if self.response_buffer is not None and not self.response_buffer.done and self.error is None:
            self.response_buffer.refresh(self.read_deadline, self.waiter)
            return
        if self.waiter is not None and not self.waiter.done():
            self.waiter.set_result(None)

    def eof_received(self) -> None:
        if self.receiving:
            self.data_received(b"")

    def connection_lost(self, exc: Exception | None) -> None:
        self.read_deadline.close()
        if self.error is None:
            self.error = httpx2.ReadError(str(exc)) if exc else httpx2.RemoteProtocolError("Server disconnected")
        for waiter in (self.waiter, self.drain_waiter, self.closed):
            if waiter is not None and not waiter.done():
                waiter.set_result(None)

    def pause_writing(self) -> None:
        self.write_paused = True

    def resume_writing(self) -> None:
        self.write_paused = False
        if self.drain_waiter is not None and not self.drain_waiter.done():
            self.drain_waiter.set_result(None)

    def is_closed(self) -> bool:
        return self.closed.done() or self.transport.is_closing() or self.error is not None

    async def write(self, data: bytes, timeout: float | None) -> None:
        if self.is_closed():
            raise httpx2.WriteError("The connection is closed")
        self.receiving = True
        try:
            self.transport.write(data)
            if self.write_paused:
                self.drain_waiter = asyncio.get_running_loop().create_future()
                async with asyncio.timeout(timeout):
                    await self.drain_waiter
                if self.is_closed():
                    raise httpx2.WriteError("The connection closed during a write")
        except TimeoutError as exc:
            raise httpx2.WriteTimeout(str(exc)) from exc
        except OSError as exc:
            raise httpx2.WriteError(str(exc)) from exc
        finally:
            self.drain_waiter = None

    async def event(self, timeout: float | None) -> zttp.Response | zttp.Data | zttp.EndOfMessage:
        while not self.events:
            if self.error is not None:
                raise self.error
            self.waiter = asyncio.get_running_loop().create_future()
            self.read_deadline.arm(self.waiter, timeout)
            try:
                await self.waiter
            finally:
                self.read_deadline.disarm()
                self.waiter = None
        event = self.events.popleft()
        if isinstance(event, zttp.Data):
            self.buffered -= len(event.data)
            if self.read_paused and self.buffered <= self.read_size // 2:
                self.transport.resume_reading()
                self.read_paused = False
        return event

    async def response(self, timeout: float | None) -> tuple[zttp.Response, bytes]:
        buffer = self.response_buffer
        assert buffer is not None
        buffer.timeout = timeout
        while not buffer.done:
            if self.error is not None:
                raise self.error
            self.waiter = asyncio.get_running_loop().create_future()
            self.read_deadline.arm(self.waiter, timeout)
            try:
                await self.waiter
            finally:
                self.read_deadline.disarm()
                self.waiter = None
        if buffer.head is None:
            raise httpx2.RemoteProtocolError("Response body arrived without headers")
        self.done = True
        self.response_buffer = None
        return buffer.head, b"".join(buffer.chunks)

    async def aclose(self) -> None:
        self.read_deadline.close()
        self.transport.close()
        await asyncio.shield(self.closed)
