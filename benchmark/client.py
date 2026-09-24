"""
Single benchmark worker: drives one client library at one concurrency and body size, then emits a JSON line.

Normally launched by `run.py`, but it can be used directly against a running `server.py`:

    python client.py --lib httpx2 --concurrency 16 --size 1024

Modes:

* `stream` (default) mirrors a proxy hot path: a hand-built request, `stream=True`, raw chunk iteration.
* `read` is the plain "request and read the whole body" path.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Coroutine
from dataclasses import dataclass
from typing import Any, TypeVar

DEFAULT_PORT = 8765
DEFAULT_CHUNK_SIZE = 256 * 1024
KEEPALIVE_EXPIRY = 95.0
CONNECT_TIMEOUT = 5.0
READ_TIMEOUT = 60.0

T = TypeVar("T")
RequestFn = Callable[[], Awaitable[None]]
CloseFn = Callable[[], Awaitable[None]]
Builder = Callable[["Scenario", bytes], tuple[RequestFn, CloseFn]]


@dataclass(frozen=True)
class Scenario:
    lib: str
    concurrency: int
    size: int
    post: bool
    mode: str
    chunk_size: int
    max_connections: int | None
    host: str
    port: int

    @property
    def method(self) -> str:
        return "POST" if self.post else "GET"

    @property
    def url(self) -> str:
        path = "/echo" if self.post else f"/{self.size}"
        return f"http://{self.host}:{self.port}{path}"


def check_size(received: int, expected: int) -> None:
    if received != expected:
        raise RuntimeError(f"Expected a {expected} byte body, received {received} bytes.")


def build_httpx2(scenario: Scenario, payload: bytes) -> tuple[RequestFn, CloseFn]:
    import httpx2

    client = httpx2.AsyncClient(
        limits=httpx2.Limits(
            max_connections=scenario.max_connections,
            max_keepalive_connections=None,
            keepalive_expiry=KEEPALIVE_EXPIRY,
        ),
        timeout=httpx2.Timeout(CONNECT_TIMEOUT, read=READ_TIMEOUT),
        # Keep the benchmark direct even if HTTP_PROXY or ALL_PROXY is set in the environment.
        trust_env=False,
    )
    headers = [("host", scenario.host), ("user-agent", "httpx2-benchmark")]
    if scenario.post:
        headers.append(("content-length", str(len(payload))))

    class RequestBody(httpx2.AsyncByteStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            if scenario.post:
                yield payload

    async def stream_one() -> None:
        request = httpx2.Request(scenario.method, scenario.url, headers=headers, stream=RequestBody())
        response = await client.send(request, stream=True, follow_redirects=False)
        received = 0
        async for chunk in response.aiter_raw(scenario.chunk_size):
            received += len(chunk)
        check_size(received, scenario.size)

    async def read_one() -> None:
        response = await client.request(scenario.method, scenario.url, content=payload if scenario.post else None)
        check_size(len(response.content), scenario.size)

    return (stream_one if scenario.mode == "stream" else read_one), client.aclose


def build_httpcore2(scenario: Scenario, payload: bytes) -> tuple[RequestFn, CloseFn]:
    import httpcore2

    pool = httpcore2.AsyncConnectionPool(
        max_connections=scenario.max_connections,
        max_keepalive_connections=None,
        keepalive_expiry=KEEPALIVE_EXPIRY,
    )
    extensions: dict[str, Any] = {
        "timeout": {"connect": CONNECT_TIMEOUT, "read": READ_TIMEOUT, "write": CONNECT_TIMEOUT, "pool": CONNECT_TIMEOUT}
    }
    content = payload if scenario.post else None

    async def stream_one() -> None:
        async with pool.stream(scenario.method, scenario.url, content=content, extensions=extensions) as response:
            assert isinstance(response.stream, AsyncIterable)
            received = 0
            async for chunk in response.stream:
                received += len(chunk)
        check_size(received, scenario.size)

    async def read_one() -> None:
        response = await pool.request(scenario.method, scenario.url, content=content, extensions=extensions)
        check_size(len(response.content), scenario.size)

    return (stream_one if scenario.mode == "stream" else read_one), pool.aclose


def build_aiohttp(scenario: Scenario, payload: bytes) -> tuple[RequestFn, CloseFn]:
    import aiohttp

    session = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=scenario.max_connections or 0, keepalive_timeout=KEEPALIVE_EXPIRY),
        timeout=aiohttp.ClientTimeout(connect=CONNECT_TIMEOUT, sock_read=READ_TIMEOUT),
    )
    data = payload if scenario.post else None

    async def stream_one() -> None:
        async with session.request(scenario.method, scenario.url, data=data) as response:
            received = 0
            async for chunk in response.content.iter_chunked(scenario.chunk_size):
                received += len(chunk)
        check_size(received, scenario.size)

    async def read_one() -> None:
        async with session.request(scenario.method, scenario.url, data=data) as response:
            body = await response.read()
        check_size(len(body), scenario.size)

    return (stream_one if scenario.mode == "stream" else read_one), session.close


def build_punkreq(scenario: Scenario, payload: bytes) -> tuple[RequestFn, CloseFn]:
    import punkreq
    from punkreq.asyncio import Client

    client = Client(
        limits=punkreq.Limits(
            max_connections=scenario.max_connections,
            max_keepalive_connections=None,
            keepalive_expiry=KEEPALIVE_EXPIRY,
        ),
        timeout=punkreq.Timeout(connect=CONNECT_TIMEOUT, read=READ_TIMEOUT, pool=CONNECT_TIMEOUT, total=None),
        follow_redirects=False,
        http2=False,
        trust_env=False,
    )
    headers = [("host", scenario.host), ("user-agent", "httpx2-benchmark")]
    if scenario.post:
        headers.append(("content-length", str(len(payload))))

    async def request_body(self: object) -> AsyncIterator[bytes]:
        yield payload

    # Built dynamically so this module type-checks when punkreq is not installed.
    request_body_stream = type("RequestBody", (punkreq.AsyncByteStream,), {"__aiter__": request_body})

    async def stream_one() -> None:
        request = punkreq.Request(
            scenario.method,
            scenario.url,
            headers=headers,
            stream=request_body_stream() if scenario.post else None,
        )
        response = await client.send(request, follow_redirects=False)
        received = 0
        async for chunk in response.iter_raw(scenario.chunk_size):
            received += len(chunk)
        check_size(received, scenario.size)

    async def read_one() -> None:
        response = await client.request(scenario.method, scenario.url, content=payload if scenario.post else None)
        check_size(len(await response.read()), scenario.size)

    return (stream_one if scenario.mode == "stream" else read_one), client.close


BUILDERS: dict[str, Builder] = {
    "httpx2": build_httpx2,
    "httpcore2": build_httpcore2,
    "aiohttp": build_aiohttp,
    "punkreq": build_punkreq,
}


async def drive(one: RequestFn, concurrency: int, seconds: float) -> list[float]:
    deadline = time.perf_counter() + seconds
    latencies: list[float] = []

    async def worker() -> None:
        while True:
            start = time.perf_counter()
            await one()
            end = time.perf_counter()
            latencies.append(end - start)
            if end >= deadline:
                return

    await asyncio.gather(*(worker() for _ in range(concurrency)))
    return latencies


def percentile(sorted_values: list[float], fraction: float) -> float:
    return sorted_values[min(len(sorted_values) - 1, int(len(sorted_values) * fraction))]


async def run_scenario(
    scenario: Scenario, seconds: float, warmup_seconds: float, profile_path: str | None
) -> dict[str, Any]:
    payload = b"y" * scenario.size if scenario.post else b""
    one, aclose = BUILDERS[scenario.lib](scenario, payload)
    try:
        # Establish `concurrency` connections, then run unmeasured for a while.
        await asyncio.gather(*(one() for _ in range(scenario.concurrency)))
        await drive(one, scenario.concurrency, warmup_seconds)

        profiler = None
        if profile_path is not None:
            import pyinstrument

            profiler = pyinstrument.Profiler(async_mode="disabled")
            profiler.start()

        cpu_start = time.process_time()
        wall_start = time.perf_counter()
        latencies = await drive(one, scenario.concurrency, seconds)
        wall = time.perf_counter() - wall_start
        cpu = time.process_time() - cpu_start

        if profiler is not None:
            profiler.stop()
            with open(profile_path, "w") as f:  # type: ignore[arg-type]
                f.write(profiler.output_text(unicode=True, color=False))
    finally:
        await aclose()

    latencies.sort()
    count = len(latencies)
    return {
        "lib": scenario.lib,
        "mode": scenario.mode,
        "concurrency": scenario.concurrency,
        "size": scenario.size,
        "post": scenario.post,
        "requests": count,
        "seconds": round(wall, 3),
        "rps": round(count / wall, 1),
        "p50_ms": round(percentile(latencies, 0.5) * 1000, 2),
        "p99_ms": round(percentile(latencies, 0.99) * 1000, 2),
        "max_ms": round(latencies[-1] * 1000, 2),
        "cpu_us": round(cpu * 1_000_000 / count, 1),
    }


def pin_to_cpu(cpu: int | None) -> None:
    if cpu is not None and hasattr(os, "sched_setaffinity"):
        os.sched_setaffinity(0, {cpu})


def run(coro: Coroutine[Any, Any, T], use_zuvloop: bool) -> tuple[T, bool]:
    loop: asyncio.AbstractEventLoop
    zuvloop_active = False
    if use_zuvloop:
        try:
            import zuvloop
        except ImportError:
            loop = asyncio.new_event_loop()
        else:
            loop = zuvloop.new_event_loop()
            zuvloop_active = True
    else:
        loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro), zuvloop_active
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


def interpreter_info() -> dict[str, Any]:
    gil_enabled = True
    if sys.version_info >= (3, 13):
        gil_enabled = sys._is_gil_enabled()
    return {"python": sys.version.split()[0], "gil": gil_enabled}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--lib", choices=sorted(BUILDERS), required=True)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--size", type=int, default=1024, help="Response body size in bytes (request body for --post).")
    parser.add_argument("--post", action="store_true", help="POST a body of --size bytes to the echo endpoint.")
    parser.add_argument("--mode", choices=("stream", "read"), default="stream")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument("--max-connections", type=int, default=None, help="Pool limit; unbounded by default.")
    parser.add_argument("--seconds", type=float, default=4.0, help="Measured duration.")
    parser.add_argument("--warmup", type=float, default=0.5, help="Unmeasured duration before measuring.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--cpu", type=int, default=None, help="Pin this worker to a CPU (Linux only).")
    parser.add_argument("--no-zuvloop", action="store_true")
    parser.add_argument("--profile", default=None, help="Write a pyinstrument profile of the measured phase here.")
    args = parser.parse_args(argv)

    pin_to_cpu(args.cpu)
    scenario = Scenario(
        lib=args.lib,
        concurrency=args.concurrency,
        size=args.size,
        post=args.post,
        mode=args.mode,
        chunk_size=args.chunk_size,
        max_connections=args.max_connections,
        host=args.host,
        port=args.port,
    )
    result, zuvloop_active = run(
        run_scenario(scenario, args.seconds, args.warmup, args.profile), use_zuvloop=not args.no_zuvloop
    )
    result.update(interpreter_info(), zuvloop=zuvloop_active)
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
