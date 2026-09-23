from __future__ import annotations

import secrets
import threading

import pytest

from httpcore2 import NetworkStream
from httpx2.websockets import WebSocketSession


def test_keepalive_ping_exits_when_closing(monkeypatch: pytest.MonkeyPatch) -> None:
    ping_started = threading.Event()
    resume_ping = threading.Event()
    read_started = threading.Event()
    closed = threading.Event()
    writes: list[bytes] = []

    def token_bytes(nbytes: int | None = None) -> bytes:
        ping_started.set()
        assert resume_ping.wait(5)
        return b"ping"

    class Stream(NetworkStream):
        def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
            read_started.set()
            assert closed.wait(5)
            return b""

        def write(self, buffer: bytes, timeout: float | None = None) -> None:
            writes.append(buffer)

        def close(self) -> None:
            closed.set()

    monkeypatch.setattr(secrets, "token_bytes", token_bytes)
    with WebSocketSession(Stream(), keepalive_ping_interval_seconds=0) as session:
        try:
            assert read_started.wait(5)
            assert ping_started.wait(5)
            session.close()
        finally:
            resume_ping.set()

    assert closed.is_set()
    assert len(writes) == 1
