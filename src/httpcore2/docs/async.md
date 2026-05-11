# Async Support

HTTPX offers a standard synchronous API by default, but also gives you the option of an async client if you need it.

Async is a concurrency model that is far more efficient than multi-threading, and can provide significant performance benefits and enable the use of long-lived network connections such as WebSockets.

If you're working with an async web framework then you'll also want to use an async client for sending outgoing HTTP requests.

Launching concurrent async tasks is far more resource efficient than spawning multiple threads. The Python interpreter should be able to comfortably handle switching between over 1000 concurrent tasks, while a sensible number of threads in a thread pool might be to enable around 10 or 20 concurrent threads.

## Enabling Async support

If you're using async with [Python's stdlib `asyncio` support](https://docs.python.org/3/library/asyncio.html), install the optional dependencies using:

```shell
$ pip install 'httpcore[asyncio]'
```

Alternatively, if you're working with [the Python `trio` package](https://trio.readthedocs.io/en/stable/):

```shell
$ pip install 'httpcore[trio]'
```

We highly recommend `trio` for async support. The `trio` project [pioneered the principles of structured concurrency](https://en.wikipedia.org/wiki/Structured_concurrency), and has a more carefully constrained API against which to work from.

## API differences

When using async support, you need make sure to use an async connection pool class:

```python
# The async variation of `httpcore.ConnectionPool`
async with httpcore.AsyncConnectionPool() as http:
    ...
```

### Sending requests

Sending requests with the async version of `httpcore` requires the `await` keyword:

```python
import asyncio
import httpcore

async def main():
    async with httpcore.AsyncConnectionPool() as http:
        response = await http.request("GET", "https://www.example.com/")


asyncio.run(main())
```

When including content in the request, the content must either be bytes or an *async iterable* yielding bytes.

### Streaming responses

Streaming responses also require a slightly different interface to the sync version:

* `with <pool>.stream(...) as response` → `async with <pool>.stream() as response`.
* `for chunk in response.iter_stream()` → `async for chunk in response.aiter_stream()`.
* `response.read()` → `await response.aread()`.
* `response.close()` → `await response.aclose()`

For example:

```python
import asyncio
import httpcore


async def main():
    async with httpcore.AsyncConnectionPool() as http:
        async with http.stream("GET", "https://www.example.com/") as response:
            async for chunk in response.aiter_stream():
                print(f"Downloaded: {chunk}")


asyncio.run(main())
```

### Pool lifespans

When using `httpcore` in an async environment it is strongly recommended that you instantiate and use connection pools using the context managed style:

```python
async with httpcore.AsyncConnectionPool() as http:
    ...
```

To benefit from connection pooling it is recommended that you instantiate a single connection pool in this style, and pass it around throughout your application.

If you do want to use a connection pool without this style then you'll need to ensure that you explicitly close the pool once it is no longer required:

```python
try:
    http = httpcore.AsyncConnectionPool()
    ...
finally:
    await http.aclose()
```

This is a little different to the threaded context, where it's okay to simply instantiate a globally available connection pool, and then allow Python's garbage collection to deal with closing any connections in the pool, once the `__del__` method is called.

The reason for this difference is that asynchronous code is not able to run within the context of the synchronous `__del__` method, so there is no way for connections to be automatically closed at the point of garbage collection. This can lead to unterminated TCP connections still remaining after the Python interpreter quits.

## Supported environments

HTTPX supports either `asyncio` or `trio` as an async environment.

It will auto-detect which of those two to use as the backend for socket operations and concurrency primitives.

### AsyncIO

AsyncIO is Python's [built-in library](https://docs.python.org/3/library/asyncio.html) for writing concurrent code with the async/await syntax.

Let's take a look at sending several outgoing HTTP requests concurrently, using `asyncio`:

```python
import asyncio
import httpcore
import time


async def download(http, year):
    await http.request("GET", f"https://en.wikipedia.org/wiki/{year}")


async def main():
    async with httpcore.AsyncConnectionPool() as http:
        started = time.time()
        # Here we use `asyncio.gather()` in order to run several tasks concurrently...
        tasks = [download(http, year) for year in range(2000, 2020)]
        await asyncio.gather(*tasks)
        complete = time.time()

        for connection in http.connections:
            print(connection)
        print("Complete in %.3f seconds" % (complete - started))


asyncio.run(main())
```

### Trio

Trio is [an alternative async library](https://trio.readthedocs.io/en/stable/), designed around the [the principles of structured concurrency](https://en.wikipedia.org/wiki/Structured_concurrency).

```python
import httpcore
import trio
import time


async def download(http, year):
    await http.request("GET", f"https://en.wikipedia.org/wiki/{year}")


async def main():
    async with httpcore.AsyncConnectionPool() as http:
        started = time.time()
        async with trio.open_nursery() as nursery:
            for year in range(2000, 2020):
                nursery.start_soon(download, http, year)
        complete = time.time()

        for connection in http.connections:
            print(connection)
        print("Complete in %.3f seconds" % (complete - started))


trio.run(main)
```

### AnyIO

AnyIO is an [asynchronous networking and concurrency library](https://anyio.readthedocs.io/) that works on top of either asyncio or trio. It blends in with native libraries of your chosen backend (defaults to asyncio).

The `anyio` library is designed around the [the principles of structured concurrency](https://en.wikipedia.org/wiki/Structured_concurrency), and brings many of the same correctness and usability benefits that Trio provides, while interoperating with existing `asyncio` libraries.

```python
import httpcore
import anyio
import time


async def download(http, year):
    await http.request("GET", f"https://en.wikipedia.org/wiki/{year}")


async def main():
    async with httpcore.AsyncConnectionPool() as http:
        started = time.time()
        async with anyio.create_task_group() as task_group:
            for year in range(2000, 2020):
                task_group.start_soon(download, http, year)
        complete = time.time()

        for connection in http.connections:
            print(connection)
        print("Complete in %.3f seconds" % (complete - started))


anyio.run(main)
```

---

# Reference

## `httpcore.AsyncConnectionPool`

::: httpcore.AsyncConnectionPool
    handler: python
    rendering:
        show_source: False
