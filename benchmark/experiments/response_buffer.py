from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

import zttp

import httpx2

from .read_deadline import ReadDeadline


@dataclass
class ResponseBuffer:
    head: zttp.Response | None = None
    chunks: list[bytes] = field(default_factory=list)
    done: bool = False
    timeout: float | None = None

    def receive(self, event: zttp.Response | zttp.Data | zttp.EndOfMessage) -> None:
        if isinstance(event, zttp.Response):
            if event.status_code >= 200:
                self.head = event
            elif event.status_code == 101:
                raise httpx2.RemoteProtocolError("Upgrades are not supported")
        elif isinstance(event, zttp.Data):
            self.chunks.append(event.data)
        else:
            self.done = True

    def refresh(self, deadline: ReadDeadline, waiter: asyncio.Future[None] | None) -> None:
        if waiter is not None and not waiter.done():
            deadline.arm(waiter, self.timeout)
