# HTTPX2 performance investigation

Date: 2026-09-19. Source revision: `a1f507e0`.

## Result

**Aiohttp-level throughput is achievable on the measured small-response workloads.** A larger HTTPX2 prototype exceeded aiohttp by 14-28% in repeated GET and POST tests. It remained 10-17% behind pyreqwest on those workloads. The 100 KiB workload was slower than both references.

The earlier investigation stopped at incremental improvements. Its conclusion was a limit of that prototype, not a performance ceiling for HTTPX2. The [initial report](INITIAL_REPORT.md) preserves those measurements.

The new prototype combines direct zttp HTTP/1.1 processing, a pool indexed by origin, asyncio I/O, HTTPX2 frontend fast paths, and selective Cython compilation. Replacing h11 alone did not produce these results. The public client still builds a fresh request from an ordinary string URL on every iteration.

These remain optional experiments. The default implementation and package dependencies are unchanged.

## Repeated buffered results

Median completed requests/s across three runs. Each run has a 0.5-second warmup and a 2-second measurement. All clients use the standard asyncio loop and consume the entire body. Pyreqwest materializes Python `bytes` too.

| Workload | HTTPX2 prototype | aiohttp | pyreqwest | Prototype vs aiohttp | Prototype vs pyreqwest |
| --- | ---: | ---: | ---: | ---: | ---: |
| GET 1 KiB, concurrency 16 | **28,868** | 23,760 | 34,955 | +21.5% | -17.4% |
| GET 1 KiB, concurrency 128 | **29,887** | 26,138 | 35,909 | +14.3% | -16.8% |
| GET 100 KiB, concurrency 64 | **17,032** | 19,883 | 26,363 | -14.3% | -35.4% |
| POST/echo 1 KiB, concurrency 64 | **27,356** | 21,416 | 30,502 | +27.7% | -10.3% |

At concurrency 16, CPU cost was about 34 µs/request for the prototype, 41 for aiohttp, and 40 for pyreqwest. Lower process CPU cost does not imply higher throughput: overlap between native work and Python is one possible explanation. Its runtime is configured as single-threaded, but the whole process is not restricted to one CPU.

The earlier stock HTTPX2 measurement was 4,254 requests/s for GET 1 KiB at concurrency 16. The new result is about 6.8x that earlier baseline. This comparison spans separate benchmark matrices; the reference clients in the table above were measured together with the new prototype.

Raw data: [buffered results](results/fused-final-read.json), [environment](results/environment.json).

## Repeated streaming results

Streaming requests disable eager buffering. At concurrency 16 with 1 KiB bodies, the prototype reached **24,327 requests/s**, versus aiohttp's **23,108** and pyreqwest's **33,782**. With 100 KiB bodies at concurrency 64, it reached **14,445**, versus **19,831** and **20,618**. These are three-run medians from [streaming results](results/fused-final-stream.json). The small-body improvement therefore does not depend entirely on pre-reading responses.

## What made the difference

1. **Remove intermediate transport work.** `FusedTransport` uses zttp directly with asyncio streams. It avoids HTTPX-to-HTTPCore request/response conversion, repeated header normalization, connection wrappers, and nested stream adapters. The first direct-transport run increased throughput from approximately 10.8k to 17.6k requests/s.
2. **Reduce frontend work.** The isolated frontend patch avoids rebuilding prepared headers, takes a simpler header-membership path, skips the exact default no-op auth generator, and avoids computing log arguments when INFO is disabled. It caches up to 128 immutable parsed URLs. Custom auth and ordinary feature paths remain available.
3. **Use an in-memory response fast path.** An exact `ByteStream` response with no content encoding can be read directly while preserving closed/consumed/downloaded state. Encoded responses retain the existing decoding path. Fully-read requests can finish in the transport; streaming, custom auth, and response hooks disable eager buffering through an experimental extension.
4. **Avoid unnecessary timeout machinery.** Pool acquisition skips creating a timer when a semaphore slot is immediately available. Writes drain under a timeout when bytes are buffered or the writer is closing. Read timeouts, connection timeouts, queue timeouts, and write backpressure remain.
5. **Compile selected Python modules.** Cython compiles `_models`, `_client`, `_urls`, `_decoders`, and `_utils`. The content iterators and transport stay Python. Zttp supplies native protocol processing.

The uncompiled frontend with the direct transport already reached approximately 26k requests/s in exploratory runs. Selective compilation supplied an additional improvement; it was not required for the first aiohttp-level result.

## Additional experiments

- A shared zuvloop run produced approximately 37.9k requests/s for the uncompiled prototype, 36.1k for aiohttp, and 54.8k for pyreqwest at concurrency 16. Switching loops helps the references too. These are exploratory, one-round results.
- Increasing the direct transport's read size from 64 KiB to 256 KiB improved the 100 KiB test from 17.2k to 18.8k requests/s in one run; aiohttp reached 20.5k. This optional `fused-buffered-wide` variant is not used in the repeated table.
- Compiling the transport itself did not show a clear extra gain, so it is not part of the retained selective build.
- Compiling `_content` caused async-generator warnings in ASGI and WebSocket tests. That build was rejected. Retaining the Python content iterators restored a passing full suite.
- Merely pre-reading a response through the original wrapper layers had negligible benefit. Removing those layers and adding the frontend response fast path mattered together.

These results do not establish pyreqwest parity across workloads. They also provide no basis for declaring parity impossible. The remaining large-body costs and overlap between native I/O and Python work warrant separate profiling.

## Validation and limits

The frontend patch passed **1,742 existing HTTPX2 tests**, with one expected skip. The selective native frontend passed the same full suite. This includes models, client behavior, content handling, ASGI, and WebSocket tests. [Python results](results/tests-frontend.txt) and [selective native results](results/tests-selective-native.txt) retain the outputs.

The direct transport passed **8 focused real-network tests** ([results](results/tests-fused.txt)), with checks for connection reuse, verified TLS, redirects, cookies, gzip, HEAD, fixed/chunked uploads, streamed downloads, limits, idle eviction, timeouts, cancellation, incomplete responses, and recovery after failure. These do not establish 100% coverage or production readiness.

The transport supports asyncio and HTTP/1.1. It does not implement HTTP/2, HTTP/3, proxies, CONNECT, upgrades, Unix sockets, retries, or HTTPCore2 trace/network-stream extensions. Eager buffering changes response-header timing. Idle expiry is lazy; idle-peer handling and cancellation during cleanup need further review. Zttp's limits and error messages differ from h11's.

The benchmark uses a separate local server process, warm persistent connections, unbounded pools, closed-loop concurrency, and no CPU affinity. It measures this macOS ARM64/CPython 3.14.3 environment. It does not measure remote-network latency, TLS throughput, overload behavior, platform portability, or full feature parity. The follow-up comparison targets aiohttp and pyreqwest; it does not add a new niquests measurement.

## Reproduce

See [README.md](README.md) for `prepare_frontend.py`, `compile_frontend.py`, benchmark commands, and public API tests. The exact changes are retained in [frontend.patch](frontend.patch), [fused_transport.py](fused_transport.py), and [fused_stream.py](fused_stream.py).
