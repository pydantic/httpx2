from __future__ import annotations

import secrets
import threading

import anyio


class PingManager:
    def __init__(self) -> None:
        self._pings: dict[bytes, threading.Event] = {}

    def create(self, ping_id: bytes | None = None) -> tuple[bytes, threading.Event]:
        ping_id = secrets.token_bytes() if not ping_id else ping_id
        event = threading.Event()
        self._pings[ping_id] = event
        return ping_id, event

    def ack(self, ping_id: bytes | bytearray) -> None:
        event = self._pings.pop(bytes(ping_id))
        event.set()


class AsyncPingManager:
    def __init__(self) -> None:
        self._pings: dict[bytes, anyio.Event] = {}

    def create(self, ping_id: bytes | None = None) -> tuple[bytes, anyio.Event]:
        ping_id = secrets.token_bytes() if not ping_id else ping_id
        event = anyio.Event()
        self._pings[ping_id] = event
        return ping_id, event

    def ack(self, ping_id: bytes | bytearray) -> None:
        event = self._pings.pop(bytes(ping_id))
        event.set()
