from __future__ import annotations

from datetime import timedelta

import client as harness


def build_pyreqwest(scenario: harness.Scenario, payload: bytes) -> tuple[harness.RequestFn, harness.CloseFn]:
    from pyreqwest.client import ClientBuilder

    client = (
        ClientBuilder()
        .runtime_multithreaded(scenario.lib == "pyreqwest-multithreaded")
        .max_connections(scenario.max_connections)
        .pool_max_idle_per_host(scenario.concurrency)
        .pool_idle_timeout(timedelta(seconds=harness.KEEPALIVE_EXPIRY))
        .connect_timeout(timedelta(seconds=harness.CONNECT_TIMEOUT))
        .read_timeout(timedelta(seconds=harness.READ_TIMEOUT))
        .pool_timeout(timedelta(seconds=harness.CONNECT_TIMEOUT))
        .no_proxy()
        .http1_only()
        .follow_redirects(False)
        .build()
    )

    async def one() -> None:
        request = client.request(scenario.method, scenario.url)
        if scenario.post:
            request = request.body_bytes(payload)
        if scenario.mode == "read":
            response = await request.build().send()
            body = (await response.bytes()).to_bytes()
            harness.check_size(len(body), scenario.size)
        else:
            async with request.build_streamed() as response:
                size = 0
                while (chunk := await response.body_reader.read(scenario.chunk_size)) is not None:
                    size += len(chunk.to_bytes())
                harness.check_size(size, scenario.size)

    return one, client.close
