"""
Minimal HTTP/1.1 and HTTP/2 keep-alive origin for the benchmark harness.

The server is deliberately trivial so that the client under test is the bottleneck:

* `/<n>` responds with an `n`-byte body; a `POST` body is read and discarded.
* `/echo` echoes the request body.

A connection that opens with the HTTP/2 preface is served as cleartext HTTP/2 (prior knowledge, RFC 9113
§3.3), which is what `AsyncClient(http1=False, http2=True)` sends to an `http://` origin.

Usage: `python server.py [--port 8765] [--cpu N] [--no-zuvloop]`
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from collections.abc import Coroutine
from typing import Any, TypeVar, cast

import h2.config
import h2.connection
import h2.events

DEFAULT_PORT = 8765
RESPONSE_HEAD = b"HTTP/1.1 200 OK\r\ncontent-type: application/octet-stream\r\ncontent-length: %d\r\n\r\n"
BAD_REQUEST = b"HTTP/1.1 400 Bad Request\r\ncontent-length: 0\r\n\r\n"
HTTP2_PREFACE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
MAX_BODY_SIZE = 64 * 1024 * 1024

T = TypeVar("T")

_bodies: dict[int, bytes] = {}


def response_body(target: bytes) -> bytes | None:
    try:
        size = int(target.rsplit(b"/", 1)[-1] or b"0")
    except ValueError:
        return None
    if not 0 <= size <= MAX_BODY_SIZE:
        return None
    body = _bodies.get(size)
    if body is None:
        body = _bodies[size] = b"x" * size
    return body


class HTTP2Origin:
    """
    One cleartext HTTP/2 connection. Request bodies are acknowledged as they arrive, and response
    bodies are sent as the client's flow-control window allows.
    """

    def __init__(self, transport: asyncio.Transport) -> None:
        self._transport = transport
        self._conn = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=False))
        self._requests: dict[int, tuple[bytes, bytearray]] = {}
        self._pending: dict[int, memoryview] = {}
        self._conn.initiate_connection()
        self._transport.write(self._conn.data_to_send())

    def data_received(self, data: bytes) -> None:
        for event in self._conn.receive_data(data):
            if isinstance(event, h2.events.RequestReceived):
                assert event.headers is not None and event.stream_id is not None
                path = dict(event.headers)[b":path"]
                self._requests[event.stream_id] = (path, bytearray())
            elif isinstance(event, h2.events.DataReceived):
                assert event.data is not None and event.stream_id is not None
                self._requests[event.stream_id][1].extend(event.data)
                self._conn.acknowledge_received_data(event.flow_controlled_length or 0, event.stream_id)
            elif isinstance(event, h2.events.StreamEnded):
                assert event.stream_id is not None
                self._respond(event.stream_id)
            elif isinstance(event, h2.events.WindowUpdated):
                for stream_id in list(self._pending) if event.stream_id == 0 else [event.stream_id]:
                    if stream_id in self._pending:
                        self._send_pending(stream_id)
        self._transport.write(self._conn.data_to_send())

    def _respond(self, stream_id: int) -> None:
        target, body = self._requests.pop(stream_id)
        payload = bytes(body) if target == b"/echo" else response_body(target)
        if payload is None:
            self._conn.send_headers(stream_id, [(b":status", b"400"), (b"content-length", b"0")], end_stream=True)
            return
        headers = [(b":status", b"200"), (b"content-length", str(len(payload)).encode())]
        self._conn.send_headers(stream_id, headers, end_stream=not payload)
        if payload:
            self._pending[stream_id] = memoryview(payload)
            self._send_pending(stream_id)

    def _send_pending(self, stream_id: int) -> None:
        pending = self._pending[stream_id]
        while pending:
            window = min(self._conn.local_flow_control_window(stream_id), self._conn.max_outbound_frame_size)
            if window <= 0:
                break
            chunk, pending = pending[:window], pending[window:]
            self._conn.send_data(stream_id, bytes(chunk), end_stream=not pending)
        if pending:
            self._pending[stream_id] = pending
        else:
            del self._pending[stream_id]


class OriginProtocol(asyncio.Protocol):
    def __init__(self) -> None:
        self._buffer = bytearray()
        self._transport: asyncio.Transport | None = None
        self._http2: HTTP2Origin | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        # zuvloop transports implement the interface without subclassing `asyncio.Transport`.
        self._transport = cast(asyncio.Transport, transport)

    def data_received(self, data: bytes) -> None:
        if self._http2 is not None:
            self._http2.data_received(data)
            return
        self._buffer += data
        if HTTP2_PREFACE.startswith(bytes(self._buffer[: len(HTTP2_PREFACE)])):
            if len(self._buffer) < len(HTTP2_PREFACE):
                return
            assert self._transport is not None
            self._http2 = HTTP2Origin(self._transport)
            self._http2.data_received(bytes(self._buffer))
            self._buffer.clear()
            return
        while self._respond_once():
            pass

    def _respond_once(self) -> bool:
        head_end = self._buffer.find(b"\r\n\r\n")
        if head_end < 0:
            return False
        request_line, _, header_block = bytes(self._buffer[:head_end]).partition(b"\r\n")
        method, target, _ = request_line.split(b" ", 2)
        content_length = 0
        for line in header_block.split(b"\r\n"):
            name, _, value = line.partition(b":")
            if name.strip().lower() == b"content-length":
                content_length = int(value)
        total = head_end + 4 + content_length
        if len(self._buffer) < total:
            return False
        body = bytes(self._buffer[head_end + 4 : total])
        del self._buffer[:total]

        payload = body if target == b"/echo" else response_body(target)
        assert self._transport is not None
        if payload is None:
            self._transport.write(BAD_REQUEST)
        else:
            self._transport.writelines([RESPONSE_HEAD % len(payload), payload])
        return True


async def serve(port: int) -> None:
    loop = asyncio.get_running_loop()
    server = await loop.create_server(OriginProtocol, "127.0.0.1", port, backlog=4096)
    # The orchestrator waits for this line; a bind failure ends stdout without it.
    print(f"listening on 127.0.0.1:{port}", flush=True)
    async with server:
        await server.serve_forever()


def pin_to_cpu(cpu: int | None) -> None:
    if cpu is not None and hasattr(os, "sched_setaffinity"):
        os.sched_setaffinity(0, {cpu})


def run(coro: Coroutine[Any, Any, T], use_zuvloop: bool) -> T:
    loop: asyncio.AbstractEventLoop
    if use_zuvloop:
        try:
            import zuvloop
        except ImportError:
            loop = asyncio.new_event_loop()
        else:
            loop = zuvloop.new_event_loop()
    else:
        loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--cpu", type=int, default=None, help="Pin the server to this CPU (Linux only).")
    parser.add_argument(
        "--no-zuvloop", action="store_true", help="Use the stdlib event loop even if zuvloop is installed."
    )
    args = parser.parse_args(argv)

    pin_to_cpu(args.cpu)
    try:
        run(serve(args.port), use_zuvloop=not args.no_zuvloop)
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()
