"""
Minimal HTTP/1.1 keep-alive origin for the benchmark harness.

The server is deliberately trivial so that the client under test is the bottleneck:

* `GET /<n>` responds with an `n`-byte body.
* `POST /echo` echoes the request body.

Usage: `python server.py [--port 8765] [--cpu N] [--no-zuvloop]`
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from collections.abc import Coroutine
from typing import Any, TypeVar, cast

DEFAULT_PORT = 8765
RESPONSE_HEAD = b"HTTP/1.1 200 OK\r\ncontent-type: application/octet-stream\r\ncontent-length: %d\r\n\r\n"
BAD_REQUEST = b"HTTP/1.1 400 Bad Request\r\ncontent-length: 0\r\n\r\n"
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


class OriginProtocol(asyncio.Protocol):
    def __init__(self) -> None:
        self._buffer = bytearray()
        self._transport: asyncio.Transport | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        # zuvloop transports implement the interface without subclassing `asyncio.Transport`.
        self._transport = cast(asyncio.Transport, transport)

    def data_received(self, data: bytes) -> None:
        self._buffer += data
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

        payload = body if method == b"POST" else response_body(target)
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
