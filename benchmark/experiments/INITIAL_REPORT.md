# HTTPX2 performance investigation

Date: 2026-09-19. Source revision: `a1f507e0`.

## Result

Combining zttp, an origin-indexed HTTP/1.1 pool, a direct asyncio network backend, and reuse of an immutable `httpx2.URL` improved buffered throughput by **2.5-3.5 times** over this checkout's default HTTPX2 client. CPU time per request fell by **57-69%**. The changes also largely removed the throughput decline between concurrency 16 and 128.

This combination still does not match direct aiohttp or pyreqwest. At concurrency 16, aiohttp remained about **2.3 times** faster and pyreqwest **3.4 times** faster than the combined prototype.

The work is retained as optional experiments under `benchmark/experiments`. No default implementation or package dependency changed. These are research implementations with explicit compatibility gaps, not release-ready backends.

## Repeated buffered results

Numbers are median completed requests per second across three runs. Every response body was consumed. The baseline and optimized clients use the same event loop in this table.

| Workload | HTTPX2, h11 | HTTPX2, zttp only | Combined, h11 | Combined, zttp | Gain over baseline |
| --- | ---: | ---: | ---: | ---: | ---: |
| GET 1 KiB, concurrency 16 | 4,254 | 5,375 | 7,013 | **10,765** | **2.53x** |
| GET 1 KiB, concurrency 128 | 3,017 | 3,856 | 6,990 | **10,596** | **3.51x** |
| GET 100 KiB, concurrency 64 | 3,301 | 4,188 | 5,331 | **8,241** | **2.50x** |
| POST/echo 1 KiB, concurrency 64 | 3,670 | 4,249 | 6,497 | **9,723** | **2.65x** |

The pool, I/O, and URL changes help even without zttp. With h11, that combination improved throughput by about 1.6-2.3 times across these workloads.

| Workload | Baseline CPU, µs/request | Combined zttp CPU, µs/request | Baseline p99, ms | Combined zttp p99, ms |
| --- | ---: | ---: | ---: | ---: |
| GET 1 KiB, concurrency 16 | 215.0 | **92.6** | 4.5 | **1.9** |
| GET 1 KiB, concurrency 128 | 298.2 | **93.4** | 75.6 | **13.2** |
| GET 100 KiB, concurrency 64 | 281.3 | **120.1** | 22.1 | **8.9** |
| POST/echo 1 KiB, concurrency 64 | 250.8 | **102.7** | 19.4 | **7.4** |

The p99 values describe this closed-loop load generator, which starts a request only when its worker finishes the previous one. They do not establish latency under an independently scheduled arrival rate or overload.

The repeated streaming check also improved throughput: **4,913 to 10,897 requests/s** for 1 KiB at concurrency 16, and **3,539 to 8,441** for 100 KiB at concurrency 64. These are approximately 2.2x and 2.4x improvements. The streaming harness uses different request-construction paths for the baseline and combined strategy, as detailed below.

## Comparison with the reference clients

These reference numbers come from the same repeated matrix. The table uses the reference runs in the h11 environment; loading zttp does not change either reference client. Both sets remain in the raw data.

| Workload | HTTPX2 combined zttp | aiohttp | pyreqwest |
| --- | ---: | ---: | ---: |
| GET 1 KiB, concurrency 16 | 10,765 | 24,336 | 36,893 |
| GET 1 KiB, concurrency 128 | 10,596 | 26,545 | 37,365 |
| GET 100 KiB, concurrency 64 | 8,241 | 20,826 | 31,333 |
| POST/echo 1 KiB, concurrency 64 | 9,723 | 21,404 | 32,806 |

At concurrency 16, CPU time was 92.6 µs/request for the combined prototype, 41.0 for aiohttp, and 37.9 for pyreqwest. The remaining difference is not explained solely by native runtime parallelism.

## What worked

**Origin-indexed acquisition.** The existing pool rebuilds reservation and availability collections and probes idle connections when requests arrive and leave. The zttp-only profile attributed about 23% of samples to pool assignment at concurrency 16, rising to about 59% at concurrency 128. The experimental pool keeps per-origin idle collections, a global idle eviction order, and a connection-slot semaphore. It checks the selected connection for expiry instead of scanning the entire pool for every exchange.

**Direct asyncio I/O.** The optional backend uses `asyncio.open_connection`, stream reads/writes, and `asyncio.timeout`. It retains read/connect/write deadlines, write backpressure, and TLS verification. It avoids the AnyIO stream adaptation in this path. This is an asyncio-specific experiment, not evidence that all AnyIO workloads are slower.

**Native protocol processing.** The zttp patch replaces h11 request serialization and response events while preserving the surrounding HTTPCore2 connection logic. Its benefit becomes larger after the pool/I/O costs are reduced: at concurrency 16 the combined stack reached 10,765 requests/s with zttp versus 7,013 with h11.

**URL reuse.** Passing an existing `httpx2.URL` avoids reparsing the same URL for every request. This is already supported by the public API. The gain applies when your application can reuse URLs; it is not an automatic improvement for requests to distinct URLs.

The first exploratory round at concurrency 16 measured approximately 161 µs/request for the zttp control, 137 with direct asyncio alone, 127 with the origin pool alone, 105 with both, and 91 with URL reuse added. These one-round figures identify promising strategies; the repeated combined measurements above are the stronger evidence.

## Other strategies tested

| Strategy | Observation | Decision |
| --- | --- | --- |
| Read the entire body inside the transport | About 96 versus 94 µs/request at concurrency 16; no useful gain | Reject. It also delays response headers and changes streaming behavior. |
| Explicit minimal `Request` construction | About 85 µs/request with the buffered combination | Keep as an application-specific experiment. It skips normal default-header construction, so do not count it as a transparent library speedup. |
| aiohttp-backed HTTPX2 transport | About 110 µs/request, or 99 with URL reuse, versus 94 for the combined custom transport | Reject as the preferred performance route in this implementation. URL/header conversion and HTTPX2 processing remain. |
| zuvloop on both client and server | Combined throughput rose to 13,216 requests/s at concurrency 16; aiohttp rose to 36,497 | Useful optional addition, but it does not close the competitor gap. This was one exploratory round, not the repeated headline matrix. |

The aiohttp exploration was measured before a subsequent chunked-upload translation correction. That adapter is not part of the recommended combined strategy or the headline matrix.

## Compatibility and validation

The experiment suite passed **13 tests with h11 and 13 with zttp**. It uses local HTTP and certificate-verified TLS servers. It checks reuse, origin eviction, concurrency limits, HEAD responses, redirects, cookies, gzip decoding, fixed and chunked uploads, raw streaming, partial close, expiry, peer-requested close, pool/read timeouts, and cancellation recovery. Both normal and eager-buffered core transports are exercised. Ruff lint/format checks and strict mypy checks passed.

This is focused validation, not full transport compatibility or 100% coverage of the experimental code. In particular:

- The origin pool and direct asyncio backend require Python 3.11 or newer and asyncio. The experiments do not provide proxy routing, retries, HTTP/2 pooling, Unix sockets, or a Trio-compatible pool.
- Idle expiry is lazy. Connections to abandoned origins can remain open until eviction or pool close; use an idle limit. This differs from scanning for expired connections on every request.
- The asyncio reader prefetches bytes. Idle EOF/error detection uses the reader's state. Unsolicited bytes, reset races, TLS cancellation edge cases, and cross-platform behavior need further adversarial testing before adoption.
- The zttp adapter deliberately rejects successful CONNECT and 101 upgrades because it lacks an equivalent public trailing-data interface. It does not preserve h11's 100 KiB header-limit behavior or exact error messages. The earlier upstream HTTP/1.1 test check passed 18 cases, failed four on limits/messages, and excluded four upgrade cases. This patch only replaces the async HTTP/1.1 implementation.
- The aiohttp adapter requires `write=None`, has different pool/connect deadline semantics, accepts only ASCII request headers, and lacks full extension/error mapping. Its behavior is not equivalent to the default transport.
- Eager buffering returns headers only after the body is read. The minimal-request experiment also changes the request construction path. Neither is included in the recommended combined result.

## What to adopt next

Prioritize the origin-indexed pool design. It produced a substantial improvement without requiring a native parser and prevented the tested concurrency-related throughput decline. A production implementation must preserve global limits, fair waiting, expiry semantics, cancellation cleanup, proxies, and HTTP/2 stream capacity.

Develop zttp as an optional protocol implementation after adding the missing upgrade/trailing-data and header-limit capabilities. Continue evaluating the asyncio backend independently, especially its buffering and cancellation semantics.

The combined profile no longer shows the original pool scan dominating. Remaining work is spread across HTTPX2 request construction, repeated header validation between layers, connection/trace wrappers, deadlines, and response iteration/cleanup. Matching aiohttp or pyreqwest will require reducing those costs too; another parser-only change is unlikely to be enough.

## Reproduction and evidence

See [README.md](README.md) for complete setup, benchmark, and test commands.

- [Repeated buffered matrix](results/final-read.json): 96 successful worker runs, 32 groups with three samples each.
- [Repeated streaming matrix](results/final-stream.json): separate raw-body consumption measurements. The default HTTPX2 harness uses a manually constructed request in this mode; the combined strategy uses the high-level streaming API, so frontend work is not identical.
- [Environment and versions](results/environment.json): macOS ARM64, GIL-enabled CPython 3.14.3, h11 0.16.0, zttp 0.0.31, aiohttp 3.14.3, pyreqwest 0.13.0, AnyIO 4.14.2, and zuvloop 0.0.8.
- [Pool/I/O exploration](results/strategies-first.json), [buffering exploration](results/strategies-second.json), [aiohttp adapter exploration](results/strategies-third.json), and [zuvloop exploration](results/strategies-zuvloop.json).
- [Zttp-only profile, concurrency 16](results/httpx2-zttp-c16-1k.txt), [concurrency 128](results/httpx2-zttp-c128-1k.txt), and [combined profile](results/origin-asyncio-url-zttp-c16-1k.txt).
- [h11 test output](results/tests-h11.txt) and [zttp test output](results/tests-zttp.txt).

Measurements use a separate local HTTP/1.1 origin, reused connections, a 0.5-second warmup, and two-second measured runs. The final matrices use standard asyncio, unbounded pools, and a 95-second keepalive. Interpreter/library runs are interleaved in a fixed order. No CPU affinity or equal single-core budget is enforced. Pyreqwest uses its single-threaded Tokio configuration and materializes Python `bytes`; its runtime can still execute alongside Python on another thread. TLS correctness was tested, but HTTPS throughput, HTTP/2, WAN latency, RSS, and an open-loop arrival model were not measured. Treat these as local engineering results, not universal performance claims.
