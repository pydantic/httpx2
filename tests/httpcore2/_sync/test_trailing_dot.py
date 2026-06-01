"""Async tests for trailing-dot FQDN hostname normalisation (issue #1063)."""

from __future__ import annotations

import ssl
import typing

import pytest

import httpcore2
from httpcore2 import (
    SOCKET_OPTION,
    MockBackend,
    MockStream,
    NetworkStream,
    Origin,
)


class RecordingAsyncStream(MockStream):
    """MockStream that records the server_hostname passed to start_tls()."""

    def __init__(self, buffer: list[bytes]) -> None:
        super().__init__(buffer)
        self.start_tls_hostname: str | None = None

    def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> NetworkStream:
        self.start_tls_hostname = server_hostname
        return self


class RecordingAsyncBackend(MockBackend):
    def __init__(self, buffer: list[bytes]) -> None:
        super().__init__(buffer)
        self.stream: RecordingAsyncStream | None = None

    def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: typing.Iterable[SOCKET_OPTION] | None = None,
    ) -> NetworkStream:
        self.stream = RecordingAsyncStream(list(self._buffer))
        return self.stream



def test_sni_hostname_strips_trailing_dot() -> None:
    """server_hostname passed to start_tls() must not carry the trailing dot."""
    origin = Origin(b"https", b"myhost.internal.", 443)
    network_backend = RecordingAsyncBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Length: 0\r\n",
            b"\r\n",
        ]
    )
    with httpcore2.HTTPConnection(origin=origin, network_backend=network_backend) as conn:
        with conn.stream("GET", "https://myhost.internal./") as response:
            response.read()

    assert network_backend.stream is not None
    assert network_backend.stream.start_tls_hostname == "myhost.internal"
