from __future__ import annotations

import asyncio

import httpx2


class ReadDeadline:
    def __init__(self) -> None:
        self.loop = asyncio.get_running_loop()
        self.handle: asyncio.TimerHandle | None = None
        self.deadline: float | None = None
        self.waiter: asyncio.Future[None] | None = None

    def arm(self, waiter: asyncio.Future[None], timeout: float | None) -> None:
        now = self.loop.time()
        if self.waiter is waiter and self.deadline is not None and now >= self.deadline:
            self.fire()
            return
        self.waiter = waiter
        self.deadline = None if timeout is None else now + timeout
        if self.deadline is not None and (self.handle is None or self.deadline < self.handle.when()):
            if self.handle is not None:
                self.handle.cancel()
            self.handle = self.loop.call_at(self.deadline, self.fire)

    def fire(self) -> None:
        self.handle = None
        if self.deadline is None or self.waiter is None or self.waiter.done():
            return
        if self.loop.time() < self.deadline:
            self.handle = self.loop.call_at(self.deadline, self.fire)
        else:
            self.waiter.set_exception(httpx2.ReadTimeout("Timed out waiting for response data"))

    def disarm(self) -> None:
        self.waiter = None
        self.deadline = None

    def close(self) -> None:
        self.disarm()
        if self.handle is not None:
            self.handle.cancel()
            self.handle = None
