# HTTP/2 in httpx2: study notes

This records an HTTP/2 study done on 2026-10-21, and reviews the prototype it produced on the local `http2-background-reader` branch.

- **Code:** uncommitted changes on `http2-background-reader`, not pushed. The diff is +797/-212 lines across 13 files.
- **Experiments:** local scripts, not in the repository. `/tmp/duplex/` covers gRPC duplex, `/tmp/h2exp/` the bug reproductions and `/tmp/tgfix/` the background-task prototypes.

The study went through four questions in order:

1. Can httpx2 do gRPC?
2. What is wrong with the HTTP/2 implementation?
3. What architecture fixes it?
4. How to keep the public API and stay structured?

---

## 1. gRPC, trailers and full duplex

### Where httpx2 stands today

`_async/http2.py` handles a request in strict order: send the headers, send the whole body, then read the response. It also drops `h2.events.TrailersReceived`. HTTP/1.1 drops trailers too, on h11's end-of-message event.

| gRPC call type | Needs trailers | Needs full duplex |
|---|---|---|
| Unary | Yes | No |
| Server streaming | Yes | No |
| Client streaming | Yes | No |
| Bidirectional streaming | Yes | Yes |

So trailers alone unlock three of the four call types. Full duplex ([#436](https://github.com/pydantic/httpx2/issues/436)) is needed for bidirectional streaming.

### Trailers ([#435](https://github.com/pydantic/httpx2/issues/435))

Upstream tried twice. Neither attempt was merged:

| PR | Scope | Outcome |
|---|---|---|
| [encode/httpcore#582](https://github.com/encode/httpcore/pull/582) (Tom, 2022) | HTTP/1.1, a `"trailing_headers"` response extension | Closed. Coverage was below 100% and it was low priority. |
| [encode/httpcore#1012](https://github.com/encode/httpcore/pull/1012) (2025) | HTTP/2, same extension name, +402 lines | Still open. Tom declined it. |

Tom's objections to #1012 were about cost, not correctness:

- "the HTTP/2 implementation is already fiddly and hard to follow"
- "Configuration dials == user facing complexity", against an opt-in
- anuraaga (connect-python) noted that the Connect protocol doesn't use trailers, only gRPC does

**Smaller approach for httpx2:** pass the response's `extensions` dict into the body iterator, and set `extensions["trailing_headers"]` when trailers arrive. That covers both protocols in a few lines, with no opt-in. One catch: `Response.__init__` copies the dict (`dict(extensions)` in `_models.py`), so that copy has to go or the trailers need another route.

### Full duplex ([#436](https://github.com/pydantic/httpx2/issues/436))

Tom's framing: CONNECT, Upgrade and HTTP/2 bidirectional streams are all "a raw byte stream over an HTTP request". The `network_stream` extension already does this for HTTP/1.1 CONNECT and Upgrade, which is how httpx2 WebSockets work. On HTTP/2 it would hand back one stream instead of the whole TCP connection.

Proposed API:

```python
import struct

import httpx2


def grpc_frame(message: bytes) -> bytes:
    return struct.pack(">?I", False, len(message)) + message


async def send(stream: httpx2.NetworkStream) -> None:
    await stream.write(grpc_frame(b"one"))
    await stream.write(grpc_frame(b"two"))
    await stream.send_eof()


async with httpx2.AsyncClient(http1=False, http2=True) as client:
    async with client.stream(
        "POST",
        "http://localhost:50051/t.T/ClientStream",
        headers={"content-type": "application/grpc", "te": "trailers"},
        extensions={"duplex": send},
    ) as response:
        stream = response.extensions["network_stream"]
        while chunk := await stream.read(65536):
            print(chunk)
        print(stream.get_extra_info("trailers"))
```

| Piece | Behaviour |
|---|---|
| `extensions={"duplex": callback}` | Opt-in. The callback runs after the request headers are sent and before the transport waits for the response headers. |
| `network_stream` on HTTP/2 | One stream. `write` uses flow control, `read` returns `b""` at the end. |
| `send_eof()` | New. Sends END_STREAM and keeps reading. |
| `get_extra_info("trailers")` | Available once `read()` returns `b""`. |
| `aclose()` | Resets the stream if it is still open. |

> [!WARNING]
> **The first design deadlocked against real gRPC servers.** The first sketch sent a `content=` prefix, then waited for the response headers. A test against `grpcio` (`/tmp/duplex/exp.py`) showed that `grpcio` holds back the response headers until the handler sends its first message or the call ends. Client-streaming calls therefore always deadlocked. The callback fixes this: the server can't send body data before its headers, so anything the client sends before them can't depend on the server's output and can all be written up front (`/tmp/duplex/exp2.py`).

**Prior art for this design:**

| Library | Opt-in | Trailers |
|---|---|---|
| OkHttp | `RequestBody.isDuplex()` | `response.trailers()` after the body |
| Go `net/http` | None. The body is always sent from a goroutine. | `Response.Trailer`, filled at EOF |
| .NET `HttpClient` | `ResponseHeadersRead` plus a custom `HttpContent` | `TrailingHeaders` |
| hyper / h2 | `send_request(req, end_of_stream=false)` returns `(ResponseFuture, SendStream)` | `RecvStream::trailers()` |
| Fetch | `duplex: "half"` | Not exposed |

**Demand:** [connect-python](https://github.com/connectrpc/connect-python) was built on `httpx` and listed "half-duplex only" and "no gRPC protocol" as limitations. It moved to `pyqwest` (Rust reqwest bindings) in [connectrpc/connect-python#88](https://github.com/connectrpc/connect-python/pull/88). Trailers and duplex are exactly those two gaps. aiohttp has no HTTP/2 client at all ([aio-libs/aiohttp#5999](https://github.com/aio-libs/aiohttp/issues/5999)).

**Verdict from the session:** ship trailers. Treat duplex as a prototype until a real user, such as connect-python, commits to it.

---

## 2. Audit of the current HTTP/2 implementation

Three new bugs were reproduced against a raw `h2` server (`/tmp/h2exp/`). None of them is tracked in an issue yet.

| # | Bug | Cause |
|---|---|---|
| 1 | Closing a response early stalls the connection. The next request hangs until `ReadTimeout`. Upstream also reports `Max outbound streams is 100, 100 open` ([encode/httpcore#943](https://github.com/encode/httpcore/issues/943), closed by the stale bot; its fix [encode/httpcore#944](https://github.com/encode/httpcore/pull/944) was never merged). | No `RST_STREAM` is sent, and data for untracked streams is never acknowledged, so it eats the 16 MB connection window. |
| 2 | One stream's read timeout fails every other stream on the connection. | `_read_incoming_data` stores any read error, timeouts included, in `_read_exception` and marks the connection broken. |
| 2b | Timeouts depend on which request reads first. A 0.3 s timeout fired at 1.31 s. | Whoever holds `_read_lock` reads for everyone with its own timeout. Time spent waiting for the lock isn't counted. |
| 3 | An early `413` plus `RST_STREAM(NO_ERROR)` during an upload is lost, and the client gets `ReadTimeout`. RFC 9113 §8.1 forbids discarding it. | The whole body is sent before reading. HTTP/1.1 already handles this case. |

Found by reading only: the stream window equals the 16 MB connection window, so one slow reader can block every other stream. Go uses 4 MB per stream and 1 GB per connection.

### Open PRs and issues at the time

| Item | Notes |
|---|---|
| [#1163](https://github.com/pydantic/httpx2/pull/1163) | Rechecks the flow window after taking the read lock. Fixes the upload deadlock in [#1133](https://github.com/pydantic/httpx2/issues/1133). Small, CI green. |
| [#1153](https://github.com/pydantic/httpx2/pull/1153) | Thread safety ([#802](https://github.com/pydantic/httpx2/issues/802), [#908](https://github.com/pydantic/httpx2/issues/908)). +569/-265, merge conflicts. |
| [#1232](https://github.com/pydantic/httpx2/pull/1232) | Reads pending input before reusing an idle connection ([encode/httpx#2983](https://github.com/encode/httpx/issues/2983)). About 36 lines in `http2.py` and 240 in the backends; the rest is tests. Merge conflicts. |
| [#1217](https://github.com/pydantic/httpx2/pull/1217) | Deadlock when the server lowers `MAX_CONCURRENT_STREAMS`. |
| [#849](https://github.com/pydantic/httpx2/issues/849) | Umbrella issue for HTTP/2 robustness. |

---

## 3. The architectural root cause

HTTP/2 has no dedicated reader. Whichever request holds `_read_lock` reads the socket for every stream, using its own timeout, and routes events into per-stream queues. httpcore2 runs no background tasks anywhere.

| Symptom | Traces back to |
|---|---|
| Bugs 2 and 2b, [#1133](https://github.com/pydantic/httpx2/issues/1133), [#1217](https://github.com/pydantic/httpx2/pull/1217) | Whoever holds the lock reads for everyone |
| [#1232](https://github.com/pydantic/httpx2/pull/1232) | Nobody reads an idle connection, so `GOAWAY` and EOF go unnoticed |
| [#1153](https://github.com/pydantic/httpx2/pull/1153) | Any thread can touch the h2 state machine at any time |
| Bugs 1 and 3, no duplex | The body is sent before reading, and nobody owns stream cleanup |

### Who reads the connection: the options considered

| Approach | Used by | Structured | Keeps the API |
|---|---|---|---|
| One reader task or thread per connection | Go, OkHttp, .NET | No | Yes |
| The caller drives the connection future | hyper / Rust `h2` | Yes | No |
| The caller drives everything | libcurl multi, niquests `multiplexed=True` | Yes | No |
| Event-loop callbacks (asyncio `Protocol`) | grpclib, aioquic, [#1170](https://github.com/pydantic/httpx2/pull/1170) | Yes | asyncio only |
| Shared pump that lives while anyone waits | Nathaniel Smith in [python-trio/trio#266](https://github.com/python-trio/trio/issues/266) | Mostly | Yes |
| **Leader/follower** | *POSA2* (Schmidt et al., 2000) | **Yes** | **Yes** |

The session went back and forth here:

1. **Background reader owned by the connection** (`create_task` / `spawn_system_task`). Rejected because it isn't structured concurrency.
2. **Shared pump**, after trio#266. Replaced because leader/follower in this narrow case is simpler: handing the reader role over is a plain `Event.set()` with no `await`, so cancellation can't interrupt it.
3. **Leader/follower everywhere.** A request with no data and no current reader reads one batch itself, with its own remaining time. Everyone else waits on a per-stream event. On exit, the reader wakes one waiter. Nothing outlives any request's scope.
4. **What leader/follower can't do:** read while no request is waiting. That means idle connections don't answer PINGs or notice `GOAWAY` until the next request. The session judged this minor:
   - `GOAWAY` on idle connections: covered by [#1232](https://github.com/pydantic/httpx2/pull/1232)'s check before reuse, the same as libcurl's `http2_connisalive`.
   - Streams above `last_stream_id` are already retried safely through `ConnectionNotAvailable`.
   - PINGs while a request waits are answered, because that request is the reader. [encode/httpcore#1080](https://github.com/encode/httpcore/issues/1080) (AWS Global Accelerator, 340 s idle limit) needs PINGs *sent* during a long wait, which the reader can do on an interval.

### Scoped background work without new API

The session then asked whether a scoped background reader is possible after all.

- **A hidden task group in `AsyncClient.__aenter__`: no.** Tested with pytest-asyncio 1.4.0. A fixture doing `async with Client() as c: yield c` fails on teardown with `RuntimeError: Attempted to exit cancel scope in a different task than it was entered in`. A reader crash would also cancel the user's code.
- **A user-passed `task_group=`: works, but rejected as API.**
- **A "host task": works.** `__aenter__` starts one task (`create_task` on asyncio, `spawn_system_task` on trio) that runs a task group. `__aexit__` stops it and waits for it. Readers run in that group. This is tested in `/tmp/tgfix/` under asyncio and trio, including pytest-asyncio fixtures, cancelled bodies and outer cancel scopes.

> [!WARNING]
> **A background reader must never raise.** Under trio, an exception escaping a system task raises `TrioInternalError` and crashes the program. Under asyncio it's silently lost.

The resulting split:

| Usage | Reading |
|---|---|
| `async with AsyncClient()` | One background reader per HTTP/2 connection |
| `client = AsyncClient()` | Leader/follower |
| `with Client()` | Leader/follower |

---

## 4. The plan the session proposed

| Phase | Work |
|---|---|
| 0 | Merge [#1163](https://github.com/pydantic/httpx2/pull/1163). Small PRs for bug 1 (`RST_STREAM(CANCEL)` and late acknowledgement) and bug 2 (read timeouts aren't fatal). Merge `main` into [#1153](https://github.com/pydantic/httpx2/pull/1153) and [#1232](https://github.com/pydantic/httpx2/pull/1232) and land them as the safety net. |
| 1 | Replace `_read_lock` with leader/follower. No public API change. Turn the `/tmp/h2exp` reproductions into tests. |
| 2 | Early responses (bug 3), trailers, PINGs while waiting, smaller per-stream window. |
| 3 | Duplex, only if someone commits to using it. |

The background reader for `async with` was originally filed under "later, only if someone asks". It moved into scope after the host-task prototype worked.

---

## 5. What is on the branch

The branch implements phase 1 plus the host-task background reader, and part of phase 0.

| File | Change |
|---|---|
| `_synchronization.py` | `AsyncBackground`: the host task, `start()`, `start_soon()` (returns a cancel function), `stop()`. A no-op sync `Background`. A `ContextVar` `AsyncBackground.current`. |
| `_async/connection_pool.py` | `__aenter__` starts the background and `__aexit__` stops it. `handle_async_request` sets `AsyncBackground.current` around `connection.handle_async_request`. |
| `_async/http2.py` | `_read_lock` and `_receive_events` are replaced by `_wait()` (leader/follower), `_read_once()`, `_dispatch()` and `_read_forever()`. The first request on a connection starts `_read_forever` if a background exists. `_response_closed` acknowledges unread data and queues `RST_STREAM(CANCEL)`. `_wait_for_outgoing_flow` waits on `WindowUpdated` instead of reading itself. Read timeouts no longer mark the connection broken. |
| `scripts/unasync.py` | `aclosing` → `closing` |
| Tests | 7 new tests: early close resets the stream; a read timeout leaves other streams alone (two orderings, wrapped in `fail_after` because it hangs on `main`); a server going away (EOF or `GOAWAY`) fails every waiter; an idle connection closes on `GOAWAY`; the reader stops on pool exit; the pool can be entered and exited in different tasks; a basic request inside `async with`. Four existing scripted pool and proxy HTTP/2 tests switched from `async with pool` to `async with aclosing(pool)`. |

### How the session ended

The last message reported:

- 2114 tests passing and 100% coverage on 3 runs in a row, with `scripts/check` clean.
- The new tests fail or hang on `main`, except the server-going-away and basic `async with` tests, which guard against regressions.
- An earlier full-suite hang was a test bug. The test server's empty SETTINGS allowed only one stream, so the second request never got to read.

The session flagged one compatibility risk itself: users who script several HTTP/2 responses up front with `AsyncMockBackend` inside `async with` will see protocol errors, because the background reader reads ahead. Their fix is the same `aclosing` change.

It listed as not done: idle PINGs, bug 3, trailers, and testing against a real server, uvloop or Python 3.10. Nothing was committed. I did not rerun the tests for this document.

---

## 6. Review: is it going in a good direction?

**Short answer: the diagnosis and the leader/follower core are sound. The branch has grown past what the study recommended, and that is where the risk is.**

### What is solid

- **The diagnosis.** "Whoever holds the lock reads for everyone" explains bugs 2 and 2b, [#1133](https://github.com/pydantic/httpx2/issues/1133) and [#1217](https://github.com/pydantic/httpx2/pull/1217). Every mature client avoids it.
- **Leader/follower.** It is structured, needs no new API and has one shape for asyncio, trio and sync. `_wait()` is about 40 lines and replaces both the old read loop and the flow-control read loop.
- **Regression tests drive the pool**, not private methods.

### Concerns

1. **Scope and order.** Phase 0 was skipped. One ~1000-line diff mixes four independent changes: stream reset on close, non-fatal read timeouts, leader/follower, and the background reader. It conflicts with [#1153](https://github.com/pydantic/httpx2/pull/1153), [#1163](https://github.com/pydantic/httpx2/pull/1163) and [#1232](https://github.com/pydantic/httpx2/pull/1232), which all touch the same locks. This is exactly the "fiddly and hard to follow" cost Tom objected to upstream. It will be hard to review as one PR.
2. **Two runtime modes.** HTTP/2 behaves differently with and without `async with`. Existing scripted tests had to move to `aclosing(...)` so the background reader wouldn't read ahead of the mock frames. As a result, most existing HTTP/2 pool tests now cover only the leader/follower path, and the background path has a few dedicated tests. The gain from the background mode (idle `GOAWAY` and PINGs) is the part the session had already rated low value, given [#1232](https://github.com/pydantic/httpx2/pull/1232) and the 5 s default `keepalive_expiry`.
3. **Implicit plumbing.** The pool hands the background to the connection through a `ContextVar` set around `handle_async_request`, not through a constructor argument. It works because both run in the same task, but it is invisible at the call site. It also means the reader inherits the first request's context, so logfire and OpenTelemetry spans will attribute reader activity to that request.
4. **Writes without a timeout.** `_read_forever` calls `_write_outgoing_data(None)`, which writes with no timeout while holding `_write_lock`. Requests take `_write_lock` without a timeout too. If the peer stops reading, a request may wait past its own write timeout. A write timeout also still poisons the whole connection. The read-side fix wasn't mirrored on the write side. This needs a test.
5. **`RST_STREAM` is queued, not sent.** `_response_closed` calls `reset_stream` but doesn't flush. The new test confirms the reset goes out with the *next* request's write. Without a background reader and no next request, the server keeps sending into the window. That makes bug 1 only partly fixed.
6. **One waiter per stream.** `_wakeups` is a `dict[int, AsyncEvent]`, so only one task can wait per stream. That holds today, but duplex needs a reader and a writer waiting on the same stream at once. Phase 3 would have to change this.
7. **Broad catch.** `_read_forever` swallows every `Exception`. That is required for a background task, but it should store the error on the connection and log it, not drop it silently.

### Not addressed yet

- Bug 3 (early response), trailers, PINGs while waiting, the per-stream window.
- Whether [#1163](https://github.com/pydantic/httpx2/pull/1163)'s and [#1232](https://github.com/pydantic/httpx2/pull/1232)'s tests pass on this branch.
- uvloop, Python 3.10, free-threaded builds, and TLS cancellation safety. The session named "a cancelled read loses no data over TLS" as the riskiest assumption and didn't test it.

### Suggested way forward

1. Merge [#1163](https://github.com/pydantic/httpx2/pull/1163) as is.
2. Split the branch:
   - **PR A:** reset abandoned streams, acknowledge late data and flush the reset right away. Small, and it stands alone.
   - **PR B:** leader/follower with per-request read timeouts. No `AsyncBackground`, no `ContextVar`, and no `aclosing` test churn.
   - **PR C, optional:** `AsyncBackground`. Only with a concrete need, such as gRPC keepalive or [encode/httpcore#1080](https://github.com/encode/httpcore/issues/1080). Pass the background explicitly, and fix the write-timeout issue first.
3. Merge `main` into [#1153](https://github.com/pydantic/httpx2/pull/1153) and [#1232](https://github.com/pydantic/httpx2/pull/1232) after PR B, and shrink them where leader/follower makes parts redundant.
4. File issues for bugs 1, 2, 2b and 3 with the `/tmp/h2exp` reproductions before `/tmp` is cleared.

---

## 7. Inspiration: hypercorn

[hypercorn](https://github.com/pgjones/hypercorn)'s `protocol/h2.py` (400 lines) is a useful reference, but only partly. Hypercorn is a server, so it always has a task per connection. A client doesn't, and that is the problem the branch is stuck on.

**What hypercorn does**

1. **The HTTP/2 logic never touches the socket.** `H2Protocol` takes bytes through `handle(RawData)` and emits bytes through a `send(RawData)` callback. `TCPServer` is a thin wrapper per backend: a read loop plus a write lock.
2. **One place handles incoming events.** `_handle_events` sends each h2 event to its stream, acknowledges received data, and writes out once per batch.
3. **The task group is passed in through the constructor.** No context variable.
4. **Writing uses one sender task.** Requests push data into a per-stream `StreamBuffer` with high and low water marks. One `send_task` picks the next stream from a priority tree and sends what the flow-control window allows. Writers never wait on the window themselves.
5. **Timeouts are per connection.** A read or idle timeout closes the whole connection.

**What fits the client**

| Idea | Fits? | Why |
|---|---|---|
| Socket-free HTTP/2 core plus a thin I/O driver | **Yes, the main win** | The core has no `await`, so it lives in one plain module. `unasync` then only duplicates the small driver, not the whole rewrite. |
| One dispatch point, one write per batch | Yes | Acknowledging data and resetting streams in one place also sends the reset right away, instead of with the next request's write. |
| Passing the task group explicitly | Yes, if a background reader stays | Replaces the `AsyncBackground.current` context variable. |
| A sender task and `StreamBuffer`s | No | It needs a task per connection. Leader/follower already handles waiting for the window. |
| Timeouts per connection | No | Each client request needs its own read timeout. |

**A possible shape**

- `_h2_state.py`: a plain class wrapping `h2.connection.H2Connection`.
  - `receive(data) -> set[int]` returns the stream ids to wake.
  - `start_request`, `send_data`, `close_stream` (acknowledge late data and queue the reset) and `data_to_send()`.
  - It also stores the connection-terminated error and the per-stream event queues.
- `_async/http2.py`: only the I/O. Leader/follower `_wait`, read with the request's own timeout, write under the lock, wake the right waiters. This is the only part `unasync` copies.

This doesn't shrink the first diff, since it moves more code. But most of it becomes a move into one plain module, so the async logic left to review is smaller. Trailers, early responses (bug 3) and duplex then become changes to the plain core instead of to both generated copies.
