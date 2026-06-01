import asyncio
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import Any, Callable, Coroutine, Iterator, List

import aiohttp
import matplotlib.pyplot as plt
import pyinstrument
import urllib3
from matplotlib.axes import Axes

import httpcore2

PORT = 1234
URL = f"http://localhost:{PORT}/req"
REPEATS = 10
REQUESTS = 500
CONCURRENCY = 20
POOL_LIMIT = 100
PROFILE = False
os.environ["HTTPCORE_PREFER_ANYIO"] = "0"


def duration(start: float) -> int:
    return int((time.monotonic() - start) * 1000)


@contextmanager
def profile() -> Iterator[None]:
    if not PROFILE:
        yield
        return
    with pyinstrument.Profiler() as profiler:
        yield
    profiler.open_in_browser()


async def run_async_requests(axis: Axes) -> None:
    async def gather_limited_concurrency(
        coros: Iterator[Coroutine[Any, Any, Any]], concurrency: int = CONCURRENCY
    ) -> None:
        sem = asyncio.Semaphore(concurrency)

        async def coro_with_sem(coro: Coroutine[Any, Any, Any]) -> None:
            async with sem:
                await coro

        await asyncio.gather(*(coro_with_sem(c) for c in coros))

    async def httpcore_get(pool: httpcore2.AsyncConnectionPool, timings: List[int]) -> None:
        start = time.monotonic()
        res = await pool.request("GET", URL)
        assert len(await res.aread()) == 2000
        assert res.status == 200, f"status_code={res.status}"
        timings.append(duration(start))

    async def aiohttp_get(session: aiohttp.ClientSession, timings: List[int]) -> None:
        start = time.monotonic()
        async with session.request("GET", URL) as res:
            assert len(await res.read()) == 2000
            assert res.status == 200, f"status={res.status}"
        timings.append(duration(start))

    async with httpcore2.AsyncConnectionPool(max_connections=POOL_LIMIT) as pool:
        # warmup
        await gather_limited_concurrency((httpcore_get(pool, []) for _ in range(REQUESTS)), CONCURRENCY * 2)

        timings: List[int] = []
        start = time.monotonic()
        with profile():
            for _ in range(REPEATS):
                await gather_limited_concurrency((httpcore_get(pool, timings) for _ in range(REQUESTS)))
        axis.plot([*range(len(timings))], timings, label=f"httpcore (tot={duration(start)}ms)")

    connector = aiohttp.TCPConnector(limit=POOL_LIMIT)
    async with aiohttp.ClientSession(connector=connector) as session:
        # warmup
        await gather_limited_concurrency((aiohttp_get(session, []) for _ in range(REQUESTS)), CONCURRENCY * 2)

        timings = []
        start = time.monotonic()
        for _ in range(REPEATS):
            await gather_limited_concurrency((aiohttp_get(session, timings) for _ in range(REQUESTS)))
        axis.plot([*range(len(timings))], timings, label=f"aiohttp (tot={duration(start)}ms)")


def run_sync_requests(axis: Axes) -> None:
    def run_in_executor(fns: Iterator[Callable[[], None]], executor: ThreadPoolExecutor) -> None:
        futures = [executor.submit(fn) for fn in fns]
        for future in futures:
            future.result()

    def httpcore_get(pool: httpcore2.ConnectionPool, timings: List[int]) -> None:
        start = time.monotonic()
        res = pool.request("GET", URL)
        assert len(res.read()) == 2000
        assert res.status == 200, f"status_code={res.status}"
        timings.append(duration(start))

    def urllib3_get(pool: urllib3.HTTPConnectionPool, timings: List[int]) -> None:
        start = time.monotonic()
        res = pool.request("GET", "/req")
        assert len(res.data) == 2000
        assert res.status == 200, f"status={res.status}"
        timings.append(duration(start))

    with httpcore2.ConnectionPool(max_connections=POOL_LIMIT) as pool:
        # warmup
        with ThreadPoolExecutor(max_workers=CONCURRENCY * 2) as exec:
            run_in_executor(
                (lambda: httpcore_get(pool, []) for _ in range(REQUESTS)),
                exec,
            )

        timings: List[int] = []
        exec = ThreadPoolExecutor(max_workers=CONCURRENCY)
        start = time.monotonic()
        with profile():
            for _ in range(REPEATS):
                run_in_executor((lambda: httpcore_get(pool, timings) for _ in range(REQUESTS)), exec)
        exec.shutdown(wait=True)
        axis.plot([*range(len(timings))], timings, label=f"httpcore (tot={duration(start)}ms)")

    with urllib3.HTTPConnectionPool("localhost", PORT, maxsize=POOL_LIMIT) as urllib3_pool:
        # warmup
        with ThreadPoolExecutor(max_workers=CONCURRENCY * 2) as exec:
            run_in_executor(
                (lambda: urllib3_get(urllib3_pool, []) for _ in range(REQUESTS)),
                exec,
            )

        timings = []
        exec = ThreadPoolExecutor(max_workers=CONCURRENCY)
        start = time.monotonic()
        for _ in range(REPEATS):
            run_in_executor(
                (lambda: urllib3_get(urllib3_pool, timings) for _ in range(REQUESTS)),
                exec,
            )
        exec.shutdown(wait=True)
        axis.plot([*range(len(timings))], timings, label=f"urllib3 (tot={duration(start)}ms)")


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) == 2 else None
    assert mode in ("async", "sync"), "Usage: python client.py <async|sync>"

    fig, ax = plt.subplots()

    if mode == "async":
        asyncio.run(run_async_requests(ax))
    else:
        run_sync_requests(ax)

    plt.legend(loc="upper left")
    ax.set_xlabel("# request")
    ax.set_ylabel("[ms]")
    plt.show()
    print("DONE", flush=True)


if __name__ == "__main__":
    main()
