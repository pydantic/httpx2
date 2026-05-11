# Connection Pools

While the top-level API provides convenience functions for working with `httpcore`,
in practice you'll almost always want to take advantage of the connection pooling
functionality that it provides.

To do so, instantiate a pool instance, and use it to send requests:

```python
import httpcore

http = httpcore.ConnectionPool()
r = http.request("GET", "https://www.example.com/")

print(r)
# <Response [200]>
```

Connection pools support the same `.request()` and `.stream()` APIs [as described in the Quickstart](../quickstart).

We can observe the benefits of connection pooling with a simple script like so:

```python
import httpcore
import time


http = httpcore.ConnectionPool()
for counter in range(5):
    started = time.time()
    response = http.request("GET", "https://www.example.com/")
    complete = time.time()
    print(response, "in %.3f seconds" % (complete - started))
```

The output *should* demonstrate the initial request as being substantially slower than the subsequent requests:

```
<Response [200]> in {0.529} seconds
<Response [200]> in {0.096} seconds
<Response [200]> in {0.097} seconds
<Response [200]> in {0.095} seconds
<Response [200]> in {0.098} seconds
```

This is to be expected. Once we've established a connection to `"www.example.com"` we're able to reuse it for following requests.

## Configuration

The connection pool instance is also the main point of configuration. Let's take a look at the various options that it provides:

### SSL configuration

* `ssl_context`: An SSL context to use for verifying connections.
                 If not specified, the default `httpcore.default_ssl_context()`
                 will be used.

### Pooling configuration

* `max_connections`: The maximum number of concurrent HTTP connections that the pool
                     should allow. Any attempt to send a request on a pool that would
                     exceed this amount will block until a connection is available.
* `max_keepalive_connections`: The maximum number of idle HTTP connections that will
                               be maintained in the pool.
* `keepalive_expiry`: The duration in seconds that an idle HTTP connection may be
                      maintained for before being expired from the pool.

### HTTP version support

* `http1`: A boolean indicating if HTTP/1.1 requests should be supported by the connection
           pool. Defaults to `True`.
* `http2`: A boolean indicating if HTTP/2 requests should be supported by the connection
           pool. Defaults to `False`.

### Other options

* `retries`: The maximum number of retries when trying to establish a connection.
* `local_address`: Local address to connect from. Can also be used to connect using
                   a particular address family. Using `local_address="0.0.0.0"` will
                   connect using an `AF_INET` address (IPv4), while using `local_address="::"`
                   will connect using an `AF_INET6` address (IPv6).
* `uds`: Path to a Unix Domain Socket to use instead of TCP sockets.
* `network_backend`: A backend instance to use for handling network I/O.
* `socket_options`: Socket options that have to be included in the TCP socket when the connection was established.

## Pool lifespans

Because connection pools hold onto network resources, careful developers may want to ensure that instances are properly closed once they are no longer required.

Working with a single global instance isn't a bad idea for many use case, since the connection pool will automatically be closed when the `__del__` method is called on it:

```python
# This is perfectly fine for most purposes.
# The connection pool will automatically be closed when it is garbage collected,
# or when the Python interpreter exits.
http = httpcore.ConnectionPool()
```

However, to be more explicit around the resource usage, we can use the connection pool within a context manager:

```python
with httpcore.ConnectionPool() as http:
    ...
```

Or else close the pool explicitly:

```python
http = httpcore.ConnectionPool()
try:
    ...
finally:
    http.close()
```

## Thread and task safety

Connection pools are designed to be thread-safe. Similarly, when using `httpcore` in an async context connection pools are task-safe.

This means that you can have a single connection pool instance shared by multiple threads.

---

# Reference

## `httpcore.ConnectionPool`

::: httpcore.ConnectionPool
    handler: python
    rendering:
        show_source: False
