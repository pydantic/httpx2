# Extensions

The request/response API used by `httpcore` is kept deliberately simple and explicit.

The `Request` and `Response` models are pretty slim wrappers around this core API:

```
# Pseudo-code expressing the essentials of the request/response model.
(
    status_code: int,
    headers: List[Tuple(bytes, bytes)],
    stream: Iterable[bytes]
) = handle_request(
    method: bytes,
    url: URL,
    headers: List[Tuple(bytes, bytes)],
    stream: Iterable[bytes]
)
```

This is everything that's needed in order to represent an HTTP exchange.

Well... almost.

There is a maxim in Computer Science that *"All non-trivial abstractions, to some degree, are leaky"*. When an expression is leaky, it's important that it ought to at least leak only in well-defined places.

In order to handle cases that don't otherwise fit inside this core abstraction, `httpcore` requests and responses have 'extensions'. These are a dictionary of optional additional information.

Let's expand on our request/response abstraction...

```
# Pseudo-code expressing the essentials of the request/response model,
# plus extensions allowing for additional API that does not fit into
# this abstraction.
(
    status_code: int,
    headers: List[Tuple(bytes, bytes)],
    stream: Iterable[bytes],
    extensions: dict
) = handle_request(
    method: bytes,
    url: URL,
    headers: List[Tuple(bytes, bytes)],
    stream: Iterable[bytes],
    extensions: dict
)
```

Several extensions are supported both on the request:

```python
r = httpcore.request(
    "GET",
    "https://www.example.com",
    extensions={"timeout": {"connect": 5.0}}
)
```

And on the response:

```python
r = httpcore.request("GET", "https://www.example.com")

print(r.extensions["http_version"])
# When using HTTP/1.1 on the client side, the server HTTP response
# could feasibly be one of b"HTTP/0.9", b"HTTP/1.0", or b"HTTP/1.1".
```

## Request Extensions

### `"timeout"`

A dictionary of `str: Optional[float]` timeout values.

May include values for `'connect'`, `'read'`, `'write'`, or `'pool'`.

For example:

```python
# Timeout if a connection takes more than 5 seconds to established, or if
# we are blocked waiting on the connection pool for more than 10 seconds.
r = httpcore.request(
    "GET",
    "https://www.example.com",
    extensions={"timeout": {"connect": 5.0, "pool": 10.0}}
)
```

### `"trace"`

The trace extension allows a callback handler to be installed to monitor the internal
flow of events within `httpcore`. The simplest way to explain this is with an example:

```python
import httpcore

def log(event_name, info):
    print(event_name, info)

r = httpcore.request("GET", "https://www.example.com/", extensions={"trace": log})
# connection.connect_tcp.started {'host': 'www.example.com', 'port': 443, 'local_address': None, 'timeout': None}
# connection.connect_tcp.complete {'return_value': <httpcore.backends.sync.SyncStream object at 0x1093f94d0>}
# connection.start_tls.started {'ssl_context': <ssl.SSLContext object at 0x1093ee750>, 'server_hostname': b'www.example.com', 'timeout': None}
# connection.start_tls.complete {'return_value': <httpcore.backends.sync.SyncStream object at 0x1093f9450>}
# http11.send_request_headers.started {'request': <Request [b'GET']>}
# http11.send_request_headers.complete {'return_value': None}
# http11.send_request_body.started {'request': <Request [b'GET']>}
# http11.send_request_body.complete {'return_value': None}
# http11.receive_response_headers.started {'request': <Request [b'GET']>}
# http11.receive_response_headers.complete {'return_value': (b'HTTP/1.1', 200, b'OK', [(b'Age', b'553715'), (b'Cache-Control', b'max-age=604800'), (b'Content-Type', b'text/html; charset=UTF-8'), (b'Date', b'Thu, 21 Oct 2021 17:08:42 GMT'), (b'Etag', b'"3147526947+ident"'), (b'Expires', b'Thu, 28 Oct 2021 17:08:42 GMT'), (b'Last-Modified', b'Thu, 17 Oct 2019 07:18:26 GMT'), (b'Server', b'ECS (nyb/1DCD)'), (b'Vary', b'Accept-Encoding'), (b'X-Cache', b'HIT'), (b'Content-Length', b'1256')])}
# http11.receive_response_body.started {'request': <Request [b'GET']>}
# http11.receive_response_body.complete {'return_value': None}
# http11.response_closed.started {}
# http11.response_closed.complete {'return_value': None}
```

The `event_name` and `info` arguments here will be one of the following:

* `{event_type}.{event_name}.started`, `<dictionary of keyword arguments>`
* `{event_type}.{event_name}.complete`, `{"return_value": <...>}`
* `{event_type}.{event_name}.failed`, `{"exception": <...>}`

Note that when using the async variant of `httpcore` the handler function passed to `"trace"` must be an `async def ...` function.

The following event types are currently exposed...

**Establishing the connection**

* `"connection.connect_tcp"`
* `"connection.connect_unix_socket"`
* `"connection.start_tls"`

**HTTP/1.1 events**

* `"http11.send_request_headers"`
* `"http11.send_request_body"`
* `"http11.receive_response"`
* `"http11.receive_response_body"`
* `"http11.response_closed"`

**HTTP/2 events**

* `"http2.send_connection_init"`
* `"http2.send_request_headers"`
* `"http2.send_request_body"`
* `"http2.receive_response_headers"`
* `"http2.receive_response_body"`
* `"http2.response_closed"`

The exact set of trace events may be subject to change across different versions of `httpcore`. If you need to rely on a particular set of events it is recommended that you pin installation of the package to a fixed version.

### `"sni_hostname"`

The server's hostname, which is used to confirm the hostname supplied by the SSL certificate.

For example:

``` python
headers = {"Host": "www.encode.io"}
extensions = {"sni_hostname": "www.encode.io"}
response = httpcore.request(
    "GET",
    "https://185.199.108.153",
    headers=headers,
    extensions=extensions
)
```

### `"target"`

The target that is used as [the HTTP target instead of the URL path](https://datatracker.ietf.org/doc/html/rfc2616#section-5.1.2).

This enables support constructing requests that would otherwise be unsupported. In particular...

* Forward proxy requests using an absolute URI.
* Tunneling proxy requests using `CONNECT` with hostname as the target.
* Server-wide `OPTIONS *` requests.

For example:

```python
extensions = {"target": b"www.encode.io:443"}
response = httpcore.request(
    "CONNECT",
    "http://your-tunnel-proxy.com",
    headers=headers,
    extensions=extensions
)
```

## Response Extensions

### `"http_version"`

The HTTP version, as bytes. Eg. `b"HTTP/1.1"`.

When using HTTP/1.1 the response line includes an explicit version, and the value of this key could feasibly be one of `b"HTTP/0.9"`, `b"HTTP/1.0"`, or `b"HTTP/1.1"`.

When using HTTP/2 there is no further response versioning included in the protocol, and the value of this key will always be `b"HTTP/2"`.

### `"reason_phrase"`

The reason-phrase of the HTTP response, as bytes. For example `b"OK"`. Some servers may include a custom reason phrase, although this is not recommended.

HTTP/2 onwards does not include a reason phrase on the wire.

When no key is included, a default based on the status code may be used.

### `"stream_id"`

When HTTP/2 is being used the `"stream_id"` response extension can be accessed to determine the ID of the data stream that the response was sent on.

### `"network_stream"`

The `"network_stream"` extension allows developers to handle HTTP `CONNECT` and `Upgrade` requests, by providing an API that steps outside the standard request/response model, and can directly read or write to the network.

The interface provided by the network stream:

* `read(max_bytes, timeout = None) -> bytes`
* `write(buffer, timeout = None)`
* `close()`
* `start_tls(ssl_context, server_hostname = None, timeout = None) -> NetworkStream`
* `get_extra_info(info) -> Any`

This API can be used as the foundation for working with HTTP proxies, WebSocket upgrades, and other advanced use-cases.

See the [network backends documentation](network-backends.md) for more information on working directly with network streams.

##### `CONNECT` requests

A proxy CONNECT request using the network stream:

```python
# Formulate a CONNECT request...
#
# This will establish a connection to 127.0.0.1:8080, and then send the following...
#
# CONNECT http://www.example.com HTTP/1.1
url = "http://127.0.0.1:8080"
extensions = {"target: "http://www.example.com"}
with httpcore.stream("CONNECT", url, extensions=extensions) as response:
    network_stream = response.extensions["network_stream"]

    # Upgrade to an SSL stream...
    network_stream = network_stream.start_tls(
        ssl_context=httpcore.default_ssl_context(),
        hostname=b"www.example.com",
    )

    # Manually send an HTTP request over the network stream, and read the response...
    #
    # For a more complete example see the httpcore `TunnelHTTPConnection` implementation.
    network_stream.write(b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")
    data = network_stream.read()
    print(data)
```

##### `Upgrade` requests

Using the `wsproto` package to handle a websockets session:

```python
import httpcore
import wsproto
import os
import base64


url = "http://127.0.0.1:8000/"
headers = {
    b"Connection": b"Upgrade",
    b"Upgrade": b"WebSocket",
    b"Sec-WebSocket-Key": base64.b64encode(os.urandom(16)),
    b"Sec-WebSocket-Version": b"13"
}
with httpcore.stream("GET", url, headers=headers) as response:
    if response.status != 101:
        raise Exception("Failed to upgrade to websockets", response)

    # Get the raw network stream.
    network_steam = response.extensions["network_stream"]

    # Write a WebSocket text frame to the stream.
    ws_connection = wsproto.Connection(wsproto.ConnectionType.CLIENT)
    message = wsproto.events.TextMessage("hello, world!")
    outgoing_data = ws_connection.send(message)
    network_steam.write(outgoing_data)

    # Wait for a response.
    incoming_data = network_steam.read(max_bytes=4096)
    ws_connection.receive_data(incoming_data)
    for event in ws_connection.events():
        if isinstance(event, wsproto.events.TextMessage):
            print("Got data:", event.data)

    # Write a WebSocket close to the stream.
    message = wsproto.events.CloseConnection(code=1000)
    outgoing_data = ws_connection.send(message)
    network_steam.write(outgoing_data)
```

##### Extra network information

The network stream abstraction also allows access to various low-level information that may be exposed by the underlying socket:

```python
response = httpcore.request("GET", "https://www.example.com")
network_stream = response.extensions["network_stream"]

client_addr = network_stream.get_extra_info("client_addr")
server_addr = network_stream.get_extra_info("server_addr")
print("Client address", client_addr)
print("Server address", server_addr)
```

The socket SSL information is also available through this interface, although you need to ensure that the underlying connection is still open, in order to access it...

```python
with httpcore.stream("GET", "https://www.example.com") as response:
    network_stream = response.extensions["network_stream"]

    ssl_object = network_stream.get_extra_info("ssl_object")
    print("TLS version", ssl_object.version())
```
