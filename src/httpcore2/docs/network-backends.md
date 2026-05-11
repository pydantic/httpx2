# Network Backends

The API layer at which `httpcore` interacts with the network is described as the network backend. Various backend implementations are provided, allowing `httpcore` to handle networking in different runtime contexts.

## Working with network backends

### The default network backend

Typically you won't need to specify a network backend, as a default will automatically be selected. However, understanding how the network backends fit in may be useful if you want to better understand the underlying architecture. Let's start by seeing how we can explicitly select the network backend.

First we're making a standard HTTP request, using a connection pool:

```python
import httpcore

with httpcore.ConnectionPool() as http:
    response = http.request('GET', 'https://www.example.com')
    print(response)
```

We can also have the same behavior, but be explicit with our selection of the network backend:

```python
import httpcore

network_backend = httpcore.SyncBackend()
with httpcore.ConnectionPool(network_backend=network_backend) as http:
    response = http.request('GET', 'https://www.example.com')
    print(response)
```

The `httpcore.SyncBackend()` implementation handles the opening of TCP connections, and operations on the socket stream, such as reading, writing, and closing the connection.

We can get a better understanding of this by using a network backend to send a basic HTTP/1.1 request directly:

```python
import httpcore

# Create an SSL context using 'certifi' for the certificates.
ssl_context = httpcore.default_ssl_context()

# A basic HTTP/1.1 request as a plain bytestring.
request = b'\r\n'.join([
    b'GET / HTTP/1.1',
    b'Host: www.example.com',
    b'Accept: */*',
    b'Connection: close',
    b''
])

# Open a TCP stream and upgrade it to SSL.
network_backend = httpcore.SyncBackend()
network_stream = network_backend.connect_tcp("www.example.com", 443)
network_stream = network_stream.start_tls(ssl_context, server_hostname="www.example.com")

# Send the HTTP request.
network_stream.write(request)

# Read the HTTP response.
while True:
    response = network_stream.read(max_bytes=4096)
    if response == b'':
        break
    print(response)

# The output should look something like this:
#
# b'HTTP/1.1 200 OK\r\nAge: 600005\r\n [...] Content-Length: 1256\r\nConnection: close\r\n\r\n'
# b'<!doctype html>\n<html>\n<head>\n    <title>Example Domain</title> [...] </html>\n'
```

### Async network backends

If we're working with an `async` codebase, then we need to select a different backend.

The `httpcore.AnyIOBackend` is suitable for usage if you're running under `asyncio`. This is a networking backend implemented using [the `anyio` package](https://anyio.readthedocs.io/en/3.x/).

```python
import httpcore
import asyncio

async def main():
    network_backend = httpcore.AnyIOBackend()
    async with httpcore.AsyncConnectionPool(network_backend=network_backend) as http:
        response = await http.request('GET', 'https://www.example.com')
        print(response)

asyncio.run(main())
```

The `AnyIOBackend` will work when running under either `asyncio` or `trio`. However, if you're working with async using the [`trio` framework](https://trio.readthedocs.io/en/stable/), then we recommend using the `httpcore.TrioBackend`.

This will give you the same kind of networking behavior you'd have using `AnyIOBackend`, but there will be a little less indirection so it will be marginally more efficient and will present cleaner tracebacks in error cases.

```python
import httpcore
import trio

async def main():
    network_backend = httpcore.TrioBackend()
    async with httpcore.AsyncConnectionPool(network_backend=network_backend) as http:
        response = await http.request('GET', 'https://www.example.com')
        print(response)

trio.run(main)
```

### Mock network backends

There are also mock network backends available that can be useful for testing purposes.
These backends accept a list of bytes, and return network stream interfaces that return those byte streams.

Here's an example of mocking a simple HTTP/1.1 response...

```python
import httpcore

network_backend = httpcore.MockBackend([
    b"HTTP/1.1 200 OK\r\n",
    b"Content-Type: plain/text\r\n",
    b"Content-Length: 13\r\n",
    b"\r\n",
    b"Hello, world!",
])
with httpcore.ConnectionPool(network_backend=network_backend) as http:
    response = http.request("GET", "https://example.com/")
    print(response.extensions['http_version'])
    print(response.status)
    print(response.content)
```

Mocking a HTTP/2 response is more complex, since it uses a binary format...

```python
import hpack
import hyperframe.frame
import httpcore

content = [
    hyperframe.frame.SettingsFrame().serialize(),
    hyperframe.frame.HeadersFrame(
        stream_id=1,
        data=hpack.Encoder().encode(
            [
                (b":status", b"200"),
                (b"content-type", b"plain/text"),
            ]
        ),
        flags=["END_HEADERS"],
    ).serialize(),
    hyperframe.frame.DataFrame(
        stream_id=1, data=b"Hello, world!", flags=["END_STREAM"]
    ).serialize(),
]
# Note that we instantiate the mock backend with an `http2=True` argument.
# This ensures that the mock network stream acts as if the `h2` ALPN flag has been set,
# and causes the connection pool to interact with the connection using HTTP/2.
network_backend = httpcore.MockBackend(content, http2=True)
with httpcore.ConnectionPool(network_backend=network_backend) as http:
    response = http.request("GET", "https://example.com/")
    print(response.extensions['http_version'])
    print(response.status)
    print(response.content)
```

### Custom network backends

The base interface for network backends is provided as public API, allowing you to implement custom networking behavior.

You can use this to provide advanced networking functionality such as:

* Network recording / replay.
* In-depth debug tooling.
* Handling non-standard SSL or DNS requirements.

Here's an example that records the network response to a file on disk:

```python
import httpcore


class RecordingNetworkStream(httpcore.NetworkStream):
    def __init__(self, record_file, stream):
        self.record_file = record_file
        self.stream = stream

    def read(self, max_bytes, timeout=None):
        data = self.stream.read(max_bytes, timeout=timeout)
        self.record_file.write(data)
        return data

    def write(self, buffer, timeout=None):
        self.stream.write(buffer, timeout=timeout)

    def close(self) -> None:
        self.stream.close()

    def start_tls(
        self,
        ssl_context,
        server_hostname=None,
        timeout=None,
    ):
        self.stream = self.stream.start_tls(
            ssl_context, server_hostname=server_hostname, timeout=timeout
        )
        return self

    def get_extra_info(self, info):
        return self.stream.get_extra_info(info)


class RecordingNetworkBackend(httpcore.NetworkBackend):
    """
    A custom network backend that records network responses.
    """
    def __init__(self, record_file):
        self.record_file = record_file
        self.backend = httpcore.SyncBackend()

    def connect_tcp(
        self,
        host,
        port,
        timeout=None,
        local_address=None,
        socket_options=None,
    ):
        # Note that we're only using a single record file here,
        # so even if multiple connections are opened the network
        # traffic will all write to the same file.

        # An alternative implementation might automatically use
        # a new file for each opened connection.
        stream = self.backend.connect_tcp(
            host,
            port,
            timeout=timeout,
            local_address=local_address,
            socket_options=socket_options
        )
        return RecordingNetworkStream(self.record_file, stream)


# Once you make the request, the raw HTTP/1.1 response will be available
# in the 'network-recording' file.
#
# Try switching to `http2=True` to see the difference when recording HTTP/2 binary network traffic,
# or add `headers={'Accept-Encoding': 'gzip'}` to see HTTP content compression.
with open("network-recording", "wb") as record_file:
    network_backend = RecordingNetworkBackend(record_file)
    with httpcore.ConnectionPool(network_backend=network_backend) as http:
        response = http.request("GET", "https://www.example.com/")
        print(response)
```

---

## Reference

### Networking Backends

* `httpcore.SyncBackend`
* `httpcore.AnyIOBackend`
* `httpcore.TrioBackend`

### Mock Backends

* `httpcore.MockBackend`
* `httpcore.MockStream`
* `httpcore.AsyncMockBackend`
* `httpcore.AsyncMockStream`

### Base Interface

* `httpcore.NetworkBackend`
* `httpcore.NetworkStream`
* `httpcore.AsyncNetworkBackend`
* `httpcore.AsyncNetworkStream`
