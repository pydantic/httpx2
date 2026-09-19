from __future__ import annotations

import sys
from pathlib import Path
from typing import cast

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import client as harness

import httpcore2
import httpx2
from experiments.aiohttp_transport import AiohttpTransport
from experiments.asyncio_backend import AsyncioBackend
from experiments.core_transport import CorePool, CoreTransport
from experiments.fused_transport import FusedTransport
from experiments.origin_pool import OriginPool
from experiments.pyreqwest_reference import build_pyreqwest


def build_experiment(scenario: harness.Scenario, payload: bytes) -> tuple[harness.RequestFn, harness.CloseFn]:
    native_io = "asyncio" in scenario.lib
    indexed_pool = "origin" in scenario.lib
    backend = cast(httpcore2.AsyncNetworkBackend, AsyncioBackend()) if native_io else None
    pool: CorePool
    if indexed_pool:
        pool = OriginPool(max_connections=scenario.max_connections, network_backend=backend)
    else:
        pool = httpcore2.AsyncConnectionPool(
            max_connections=scenario.max_connections,
            max_keepalive_connections=None,
            keepalive_expiry=harness.KEEPALIVE_EXPIRY,
            network_backend=backend,
        )
    aiohttp_backend = scenario.lib.startswith("aiohttp-transport")
    client = httpx2.AsyncClient(
        transport=(
            FusedTransport(
                max_connections=scenario.max_connections,
                buffered="buffered" in scenario.lib,
                read_size=262144 if "wide" in scenario.lib else 65536,
                protocol_io="protocol" in scenario.lib,
                aggregate="aggregate" in scenario.lib,
            )
            if scenario.lib.startswith("fused")
            else AiohttpTransport(scenario.max_connections)
            if aiohttp_backend
            else CoreTransport(pool, buffered="buffered" in scenario.lib, materialized="materialized" in scenario.lib)
        ),
        timeout=httpx2.Timeout(
            harness.CONNECT_TIMEOUT,
            read=harness.READ_TIMEOUT,
            write=None if aiohttp_backend else harness.CONNECT_TIMEOUT,
        ),
        trust_env=False,
    )
    url = httpx2.URL(scenario.url) if "url" in scenario.lib else scenario.url
    headers = [("Host", scenario.host), ("User-Agent", "httpx2-benchmark")]
    if scenario.post:
        headers.append(("Content-Length", str(len(payload))))

    async def one() -> None:
        if scenario.mode == "read":
            if "minimal" in scenario.lib:
                request = httpx2.Request(scenario.method, url, headers=headers, stream=httpx2.ByteStream(payload))
                response = await client.send(request)
            else:
                response = await client.request(scenario.method, url, content=payload if scenario.post else None)
            harness.check_size(len(response.content), scenario.size)
            return
        async with client.stream(scenario.method, url, content=payload if scenario.post else None) as response:
            size = 0
            async for chunk in response.aiter_raw(scenario.chunk_size):
                size += len(chunk)
            harness.check_size(size, scenario.size)

    return one, client.aclose


for name in (
    "fused",
    "fused-buffered",
    "fused-buffered-wide",
    "fused-buffered-protocol",
    "fused-buffered-protocol-aggregate",
    "fused-url",
    "fused-url-buffered",
    "core",
    "asyncio",
    "origin",
    "origin-asyncio",
    "origin-asyncio-url",
    "origin-asyncio-url-buffered",
    "origin-asyncio-url-buffered-minimal",
    "origin-asyncio-url-buffered-materialized",
    "aiohttp-transport",
    "aiohttp-transport-url",
):
    harness.BUILDERS[name] = build_experiment

harness.BUILDERS["pyreqwest"] = build_pyreqwest
harness.BUILDERS["pyreqwest-multithreaded"] = build_pyreqwest

if __name__ == "__main__":
    harness.main()
