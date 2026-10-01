from __future__ import annotations

import threading

import anyio
import pytest

from httpcore2 import PoolTimeout
from httpcore2._synchronization import AsyncEvent, Event


@pytest.mark.anyio
async def test_async_event_set_before_wait() -> None:
    event = AsyncEvent()
    event.set()
    await event.wait()


@pytest.mark.anyio
async def test_async_event_wait_then_set() -> None:
    event = AsyncEvent()
    waited = False

    async def waiter() -> None:
        nonlocal waited
        await event.wait(timeout=5.0)
        waited = True

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(waiter)
        await anyio.sleep(0)
        event.set()

    assert waited


@pytest.mark.anyio
async def test_async_event_wait_timeout() -> None:
    event = AsyncEvent()
    with pytest.raises(PoolTimeout):
        await event.wait(timeout=0.01)


def test_event_set_before_wait() -> None:
    event = Event()
    event.set()
    event.wait()


def test_event_wait_then_set() -> None:
    event = Event()
    thread = threading.Thread(target=event.set)
    thread.start()
    event.wait(timeout=5.0)
    thread.join()


def test_event_wait_timeout() -> None:
    event = Event()
    with pytest.raises(PoolTimeout):
        event.wait(timeout=0.01)
