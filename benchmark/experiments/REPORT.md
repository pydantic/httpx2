# HTTPX2 performance investigation

Date: 2026-09-19. Source revision: `a1f507e0`.

## Result

**On standard asyncio, the latest HTTPX2 prototype beat pyreqwest's default runtime on all four buffered workloads in the five-round comparison.** Median gains ranged from 4.7% to 19.8%. The comparison shuffled workload/client order within each round and consumed every response body.

This is a measured result for the tested HTTP/1.1 workloads, not a universal ranking. Pyreqwest still led the streaming and zuvloop checks. Pyreqwest's multithreaded runtime remained about 2% faster on 100 KiB responses in a separate check, with substantially higher CPU cost. The prototype still has compatibility gaps and does not change HTTPX2's default implementation or dependencies.

The previous experiments are retained in the [phase-two report](PHASE2_REPORT.md) and [initial report](INITIAL_REPORT.md).

## Five-round buffered comparison

Standard asyncio, CPython 3.14.3, macOS ARM64. Each subprocess warms up for 0.5 seconds and measures for 3 seconds. The runner shuffles all workload/client pairs in each repetition using seed `20260919`.

| Workload | HTTPX2 prototype, requests/s | pyreqwest, requests/s | Gain |
| --- | ---: | ---: | ---: |
| GET 1 KiB, concurrency 16 | **37,116** | 35,445 | **4.7%** |
| GET 1 KiB, concurrency 128 | **36,762** | 33,736 | **9.0%** |
| GET 100 KiB, concurrency 64 | **27,564** | 23,004 | **19.8%** |
| POST/echo 1 KiB, concurrency 64 | **34,446** | 29,904 | **15.2%** |

These are medians of five runs, not the best observed runs. At concurrency 16, CPU cost was about **27 µs/request** for the prototype versus **39 µs/request** for pyreqwest.

The large-body reference varied substantially: pyreqwest ranged from 20,005 to 27,240 requests/s, while the prototype ranged from 26,854 to 27,575. The median improvement should not be read as a guaranteed 20% advantage on every run. In another three-round check, the large-body improvement over default pyreqwest was about 5%.

Raw data: [five-round comparison](results/protocol-final-read.json), [environment and source hashes](results/protocol-environment.json).

## Pyreqwest runtime configuration

The installed pyreqwest 0.13.0 API documents single-threaded Tokio as its default. The main comparison explicitly selects that default. A separate three-round shuffled comparison also enables its multithreaded runtime.

| Workload | HTTPX2 prototype | pyreqwest default | pyreqwest multithreaded |
| --- | ---: | ---: | ---: |
| GET 1 KiB, concurrency 16 | **37,312** | 35,360 | 32,203 |
| GET 100 KiB, concurrency 64 | 27,703 | 26,415 | **28,274** |

At 100 KiB, the prototype used approximately **36 µs of CPU per request**, versus **94 µs** for multithreaded pyreqwest. The latter's roughly 2% throughput advantage used about 2.6 times as much process CPU per request. No process was constrained to one CPU.

Raw data: [runtime comparison](results/protocol-runtime-comparison.json).

## Faster event loop

With zuvloop enabled for both clients, pyreqwest retained the throughput lead in the three-round check:

| Workload | HTTPX2 prototype | pyreqwest |
| --- | ---: | ---: |
| GET 1 KiB, concurrency 16 | 47,990 | **54,970** |
| GET 100 KiB, concurrency 64 | 36,840 | **42,310** |

The prototype used less process CPU per request, but the prototype's throughput was approximately 13% below pyreqwest's. The standard-asyncio win does not establish a win under every event loop. [Raw results](results/protocol-zuvloop.json).

A separate bytes/text request-construction shortcut passed 1,193 model/client tests but added little throughput. With zuvloop it reached about 49.2k requests/s at 1 KiB, still behind pyreqwest's 56.1k. It is retained separately in [bytes-request.patch](bytes-request.patch), not included in the primary prototype. [Measurements](results/bytes-zuvloop.json), [tests](results/tests-bytes-request.txt).

## Streaming

With aggregation disabled by the streaming request path, the three-round standard-asyncio comparison favored pyreqwest:

| Workload | HTTPX2 prototype | pyreqwest |
| --- | ---: | ---: |
| GET 1 KiB, concurrency 16 | 28,864 | **35,450** |
| GET 100 KiB, concurrency 64 | 21,404 | **22,250** |

The buffered result does not establish streaming parity. Both clients consume all raw body chunks in this comparison. [Raw results](results/protocol-final-stream.json).

## Changes that closed the gap

1. **Direct protocol callbacks.** Incoming socket bytes go directly into zttp. This removes the `StreamReader` buffer copy and some coroutine hand-offs. The initial trial increased small-body throughput from 29.6k to 31.9k requests/s and large-body throughput from 17.5k to 20.5k.
2. **Reusable read deadlines.** A connection retains its scheduled timer instead of allocating and cancelling one for each read. Shorter deadlines replace the timer; longer deadlines are checked and rescheduled when it fires. Incoming fragments refresh an active buffered read without extending an already-expired deadline. Timeouts remain enabled.
3. **Avoid unnecessary socket pausing.** The protocol applies read backpressure while an incomplete streamed response is queued. It does not unregister and re-register the socket after a complete response has already arrived. That change brought the 100 KiB workload from roughly 23.7k to 27.1k requests/s in exploratory comparisons.
4. **Reduce remaining frontend work.** Exact in-memory request streams can be read directly. Header lookup avoids an exception for missing fields and retains overridden lookup behavior for subclasses. Encoding detection avoids an intermediate raw-header list. Redirect checks use the fixed status values directly. Already-completed response streams avoid an extra wrapper.
5. **Deliver buffered responses once.** Eligible responses are collected in the protocol callback. The request coroutine receives the complete response rather than separately retrieving headers and body events. This provided a smaller additional gain.

These build on the previous origin-indexed pool, direct zttp HTTP/1.1 processing, bounded URL parse cache, selective Cython compilation, and in-memory response fast path. **A parser swap alone does not account for the result.** Requests still go through HTTPX2's normal client API, with an ordinary string URL and a fresh request object each time.

The protocol, pool, timers, and response collector remain Python. Compiling the callback path did not produce a clear additional gain. The retained Cython build compiles only `_models`, `_client`, `_urls`, `_decoders`, and `_utils`. Content iterators remain Python because compiling them caused warnings in earlier ASGI and WebSocket tests.

## Compatibility and validation

The final frontend and transport tests passed **1,772 tests**, with one expected skip: **1,742 existing HTTPX2 tests plus 30 real-network transport checks**. The frontend patch was reapplied into a fresh directory and verified byte-for-byte against the measured source. Ruff and mypy checks passed for the experimental Python modules.

The network checks cover both asyncio-stream and protocol transports, with and without response aggregation. They exercise TLS verification and failure recovery, reuse, cookies, redirects, gzip, HEAD, fixed/chunked uploads, a 512 KiB byte-exact echo, streamed downloads, limits, idle eviction, cancellation, incomplete responses, backpressure, and changing read deadlines. Deadline checks cover shortening, extending, disabling, and expiry while idle.

Streaming requests, response hooks, and custom authentication disable eager aggregation through the frontend eligibility flag. Those requests retain the incremental response path. This preserves those features but does not claim the same performance for every feature combination.

The prototype supports asyncio and HTTP/1.1. It does not implement HTTP/2, HTTP/3, proxies, CONNECT, upgrades, Unix sockets, retry policy, or HTTPCore2 trace/network-stream extensions. Idle expiry is lazy; idle-peer handling and cancellation during cleanup still need production review. Zttp's limits and exception messages differ from h11's. These focused tests do not establish 100% coverage or release readiness.

Validation output: [final tests](results/tests-protocol-final.txt). Implementation: [protocol connection](protocol_connection.py), [read deadlines](read_deadline.py), [response collector](response_buffer.py), [transport](fused_transport.py), and [frontend patch](frontend.patch).

## Measurement limits and reproduction

The server runs in a separate process on loopback. Connections are warm and persistent, pools are unbounded, TLS throughput is not measured, and no CPU affinity is set. The load generator starts a new request when its preceding request completes; it does not model an independently scheduled arrival rate or overload.

The results apply to this machine and configuration. They do not establish performance across platforms, cold connections, real network latency, every payload size, or every HTTP version. No new niquests comparison was added in this phase.

[README.md](README.md) includes the preparation, selective compilation, shuffled comparison, and test commands. [compare.py](compare.py) records each run's order and seed, saves partial results, and fails if any run fails.
