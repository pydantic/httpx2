# Throughput benchmark

An end-to-end, keep-alive throughput benchmark for the async client stack, designed so that the client
under test is the bottleneck rather than the server.

* `server.py` is a raw `asyncio.Protocol` HTTP/1.1 origin: `GET /<n>` returns `n` bytes, `POST /echo`
  echoes the body. No parsing library, no per-request sleep. The server and clients run on
  [zuvloop](https://github.com/Kludex/zuvloop) when it is importable (`--no-zuvloop` for the stdlib loop).
* `client.py` runs one library at one concurrency and body size, and prints a JSON line with rps,
  p50/p99 latency and per-request CPU time.
* `run.py` starts the server, runs the matrix interleaved across interpreters and libraries, and
  prints a comparison table plus two headline numbers.

```
scripts/benchmark                 # default matrix, httpx2 vs aiohttp
scripts/benchmark --quick         # c16/1k, c512/1k, c64/100k
scripts/benchmark --lib httpx2 --lib httpcore2 --scenario c512/1k --rounds 3
scripts/benchmark --profile /tmp/profiles   # pyinstrument output per scenario
```

## Comparing two revisions

Install each revision in its own virtual environment and pass both interpreters. Rounds are interleaved,
so both columns see the same machine noise:

```
git worktree add ../httpx2-main main && (cd ../httpx2-main && uv sync)
scripts/benchmark --python main=../httpx2-main/.venv/bin/python --python work=.venv/bin/python --lib httpx2
```

`punkreq` is supported as an extra reference point (`--lib punkreq`) if it is installed.

## Reading the results

* `us/req` is CPU time per request, measured with `time.process_time()`, and is more stable than rps on
  noisy machines.
* The `stream` mode (default) mirrors a proxy hot path: a hand-built `Request`, `send(stream=True)`, and
  `aiter_raw(256 KiB)`. Use `--mode read` for `client.request(...)` with the body read in full.
* `retention` is rps at the highest 1KB GET concurrency divided by rps at the lowest. A client whose
  per-request cost grows with the number of pooled connections has low retention.
* Pools are unbounded by default (`--max-connections` to bound them), and the server and clients are
  pinned to separate CPUs when at least two are available (`--no-pin` to disable).

Results depend on the interpreter: the table header shows the version and whether the build is
free-threaded. Free-threaded builds are markedly slower on this workload, so use a regular CPython
build for representative numbers, for example `uv sync --python 3.14` before `scripts/benchmark`.
zuvloop requires CPython 3.14 or later; on other interpreters the harness uses the stdlib loop and the
JSON output records `"zuvloop": false`.
