# HTTPX2 performance experiments

These are optional research transports, not supported HTTPX2 backends. They do not change the default client or package dependencies. See [REPORT.md](REPORT.md) for measurements, rejected strategies, and compatibility limits.

## Run with h11

```sh
uv sync --python 3.14
uv pip install --python .venv/bin/python zttp==0.0.31 pyreqwest==0.13.0
.venv/bin/python benchmark/experiments/run.py \
  --lib httpx2 --lib origin-asyncio-url --lib aiohttp --lib pyreqwest \
  --scenario c16/1k --scenario c128/1k --scenario c64/100k \
  --rounds 3 --seconds 2 --no-pin --no-zuvloop --mode read \
  --output /tmp/httpx2-h11-results.json
```

The experiment runner uses the existing benchmark's separate origin process, warmup, body-size validation, and CPU/latency measurements. The `pyreqwest` reference explicitly materializes a Python `bytes` body, just as the other clients do. Its Tokio runtime is configured as single-threaded, but the benchmark does not constrain the whole process to one CPU.

## Add zttp

```sh
.venv/bin/python benchmark/experiments/prepare_zttp.py /tmp/httpx2-zttp-core
PYTHONPATH=/tmp/httpx2-zttp-core:src/httpx2 .venv/bin/python benchmark/experiments/run.py \
  --lib httpx2 --lib origin-asyncio-url --lib aiohttp --lib pyreqwest \
  --scenario c16/1k --scenario c128/1k --scenario c64/100k \
  --rounds 3 --seconds 2 --no-pin --no-zuvloop --mode read \
  --output /tmp/httpx2-zttp-results.json
```

Use a new output directory for preparation. The command copies `httpcore2` and applies [zttp.patch](zttp.patch); it never edits the original package. A patch failure requires reviewing the new source before running measurements.

The zttp patch supports the benchmark's ordinary HTTP/1.1 exchanges. It deliberately rejects CONNECT and 101 upgrades. It does not preserve h11's configurable header limit or exact exception messages. Only the async connection is patched.

## Compare individual strategies

| `--lib` | Change |
| --- | --- |
| `fused` | Direct zttp HTTP/1.1 transport, returning at response headers |
| `fused-buffered` | Also complete eligible bodies inside the transport |
| `fused-buffered-wide` | Also increase maximum socket read size to 256 KiB |
| `httpx2` | Current HTTPX2 using the selected h11/zttp environment |
| `core` | Control: thin HTTPX2 transport over the existing HTTPCore2 pool |
| `asyncio` | Existing pool with asyncio streams and deadlines |
| `origin` | Origin-indexed pool with the existing AnyIO backend |
| `origin-asyncio` | Both pool and I/O changes |
| `origin-asyncio-url` | Both changes plus reuse of an immutable `httpx2.URL` |
| `origin-asyncio-url-buffered` | Read the raw response inside the transport before returning headers |
| `origin-asyncio-url-buffered-minimal` | Also use an explicit `Request` with only benchmark headers |
| `aiohttp-transport` | Keep HTTPX2's API, use aiohttp for networking |
| `aiohttp-transport-url` | Also reuse an immutable URL |
| `aiohttp`, `pyreqwest` | Direct reference clients |

Remove `--no-zuvloop` to try zuvloop if installed. Check the resulting JSON's `zuvloop` field. Use `--mode stream` to consume raw chunks instead of a buffered body. The minimal-request shortcut only applies to read mode. Do not use buffered variants to evaluate streaming latency: they wait for the entire body before returning headers.

## Use the combined transport

```python
import asyncio
import sys
from typing import cast

sys.path.insert(0, "benchmark")

import httpcore2
import httpx2
from experiments.asyncio_backend import AsyncioBackend
from experiments.core_transport import CoreTransport
from experiments.origin_pool import OriginPool


async def main() -> None:
    pool = OriginPool(
        max_connections=100,
        max_keepalive_connections=20,
        network_backend=cast(httpcore2.AsyncNetworkBackend, AsyncioBackend()),
    )
    url = httpx2.URL("https://www.example.org/")
    async with httpx2.AsyncClient(transport=CoreTransport(pool)) as client:
        response = await client.get(url)
        response.raise_for_status()


asyncio.run(main())
```

You need Python 3.11 or newer and asyncio for the experimental pool/backend. It supports TCP HTTP/1.1 and TLS. HTTPX2 still handles redirects, cookies, authentication, decompression, and event hooks. The pool expires idle connections when you select them for reuse, rather than sweeping every idle origin on each request. Bound the idle pool or close it to release connections to origins you no longer use.

The experiments do not implement proxy routing, connection retries, HTTP/2 pooling, or Unix sockets. The aiohttp adapter additionally requires `httpx2.Timeout(5, write=None)`, accepts only ASCII request headers, and uses aiohttp's connection timeout semantics; it is not a drop-in transport.

## Verify behavior

```sh
.venv/bin/python -m pytest benchmark/experiments --strict-config --strict-markers
PYTHONPATH=/tmp/httpx2-zttp-core:src/httpx2 .venv/bin/python -m pytest \
  benchmark/experiments --strict-config --strict-markers
.venv/bin/ruff check benchmark/experiments
.venv/bin/ruff format --check benchmark/experiments
.venv/bin/mypy benchmark/experiments --follow-imports=silent
```

The integration tests use local HTTP and TLS servers. They cover connection reuse, origin eviction, concurrency limits, redirects, cookies, compression, uploads, streaming, early close, expiry, timeouts, and cancellation recovery. They do not establish full compatibility with HTTPX2's transport contract or exhaustive coverage of the experimental code.

## Run the direct zttp transport and frontend fast paths

```sh
.venv/bin/python benchmark/experiments/prepare_frontend.py /tmp/httpx2-fast
PYTHONPATH=/tmp/httpx2-fast:src/httpcore2 .venv/bin/python benchmark/experiments/run.py \
  --lib fused-buffered --lib aiohttp --lib pyreqwest \
  --scenario c16/1k --scenario c128/1k --scenario c64/100k --scenario c64/1k/post \
  --rounds 3 --seconds 2 --no-pin --no-zuvloop --mode read \
  --output /tmp/httpx2-fast-results.json
```

This applies [frontend.patch](frontend.patch) to a separate HTTPX2 copy. You use the direct zttp transport without patching HTTPCore2. `fused-buffered` passes ordinary string URLs. The frontend caches up to 128 immutable parsed URLs, avoids redundant header reconstruction, bypasses the default no-op authentication generator, and directly reads unencoded in-memory byte streams.

The frontend marks fully-read requests as eligible for eager buffering. Streaming requests, custom authentication, and response hooks disable that optimization. The transport still supports an explicit `httpx2_buffer_response=False` extension when you use it without the frontend patch. Eager buffering changes when response headers become available; use `fused` for a transport that always returns at the headers.

## Compile the frontend

```sh
uv pip install --python .venv/bin/python cython==3.3.0 setuptools==84.0.0
CFLAGS="-O3 -DNDEBUG" .venv/bin/python benchmark/experiments/compile_frontend.py /tmp/httpx2-fast
PYTHONPATH=/tmp/httpx2-fast:src/httpcore2 .venv/bin/python -m pytest \
  tests/httpx2 benchmark/experiments/test_fused.py --strict-config --strict-markers
PYTHONPATH=/tmp/httpx2-fast:src/httpcore2 .venv/bin/python benchmark/experiments/run.py \
  --lib fused-buffered --lib aiohttp --lib pyreqwest \
  --scenario c16/1k --scenario c128/1k --scenario c64/100k --scenario c64/1k/post \
  --rounds 3 --seconds 2 --no-pin --no-zuvloop --mode read \
  --output /tmp/httpx2-native-results.json
```

You need a working C compiler and platform SDK. The build compiles `_models`, `_client`, `_urls`, `_decoders`, and `_utils` with Cython. It keeps `_content` in Python because compiling its asynchronous generators caused warnings in ASGI and WebSocket tests. The transport stays Python; zttp supplies native protocol processing. This is an experiment, not a packaging change.

On the measured Mac, the default SDK failed to link. The build used `MacOSX14.4.sdk` with `SDKROOT`, `-isysroot` in both `CFLAGS` and `LDFLAGS`, and explicitly retained `-O3 -DNDEBUG`. Do not overwrite optimization flags when selecting an SDK.

The direct transport currently supports asyncio, HTTP/1.1, TLS, streamed request and response bodies, and bounded connection pools. It does not implement proxies, HTTP/2, HTTP/3, CONNECT, upgrades, Unix sockets, retry policy, or HTTPCore2 trace/network-stream extensions. Idle expiry is lazy. Its idle-peer handling and cancellation during cleanup still need production-level review. The tests are focused validation, not 100% coverage of a supported new backend.

## Compare the latest protocol transport with pyreqwest

```sh
.venv/bin/python benchmark/experiments/prepare_frontend.py /tmp/httpx2-protocol
CFLAGS="-O3 -DNDEBUG" .venv/bin/python benchmark/experiments/compile_frontend.py /tmp/httpx2-protocol
PYTHONPATH=/tmp/httpx2-protocol:src/httpcore2 .venv/bin/python benchmark/experiments/compare.py \
  --lib fused-buffered-protocol-aggregate --lib pyreqwest \
  --scenario c16/1k --scenario c128/1k --scenario c64/100k --scenario c64/1k/post \
  --rounds 5 --seconds 3 --seed 20260919 --no-zuvloop --mode read \
  --output /tmp/httpx2-protocol-results.json
```

The comparison runner shuffles all workload/client pairs within each repetition. It saves each run's order and seed, writes partial results after each run, and fails if any run fails. You can use `--mode stream` to test the incremental response path, or omit `--no-zuvloop` to compare both clients with zuvloop.

`fused-buffered-protocol` parses incoming socket data directly in an asyncio protocol callback. It reuses read timers across requests, updates deadlines on incoming fragments, and applies backpressure while an incomplete streamed response is buffered. It avoids pausing the socket after a complete response has already arrived.

`fused-buffered-protocol-aggregate` additionally collects an eligible buffered response in the protocol callback and wakes the request coroutine when the response is complete. This avoids separate hand-offs for response headers and body fragments. Streaming, response hooks, and custom auth keep the incremental path through the frontend eligibility flag. The protocol callbacks stay Python; compiling them did not show a useful improvement.

The latest frontend patch also reads exact in-memory request streams directly, avoids exception-driven header lookup, reduces header-encoding allocations, uses direct redirect-status comparisons, and avoids wrapping an already-completed response stream. [frontend-phase2.patch](frontend-phase2.patch) preserves the previous frontend for comparison with the [phase-two report](PHASE2_REPORT.md).
