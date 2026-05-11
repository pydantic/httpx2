# HTTP/2

HTTP/2 is a major new iteration of the HTTP protocol, that provides a more efficient transport, with potential performance benefits. HTTP/2 does not change the core semantics of the request or response, but alters the way that data is sent to and from the server.

Rather than the text format that HTTP/1.1 uses, HTTP/2 is a binary format. The binary format provides full request and response multiplexing, and efficient compression of HTTP headers. The stream multiplexing means that where HTTP/1.1 requires one TCP stream for each concurrent request, HTTP/2 allows a single TCP stream to handle multiple concurrent requests.

HTTP/2 also provides support for functionality such as response prioritization, and server push.

For a comprehensive guide to HTTP/2 you may want to check out "[HTTP2 Explained](https://http2-explained.haxx.se)".

## Enabling HTTP/2

When using the `httpcore` client, HTTP/2 support is not enabled by default, because HTTP/1.1 is a mature, battle-hardened transport layer, and our HTTP/1.1 implementation may be considered the more robust option at this point in time. It is possible that a future version of `httpcore` may enable HTTP/2 support by default.

If you're issuing highly concurrent requests you might want to consider trying out our HTTP/2 support. You can do so by first making sure to install the optional HTTP/2 dependencies...

```shell
$ pip install 'httpcore[http2]'
```

And then instantiating a connection pool with HTTP/2 support enabled:

```python
import httpcore

pool = httpcore.ConnectionPool(http2=True)
```

We can take a look at the difference in behaviour by issuing several outgoing requests in parallel.

Start out by using a standard HTTP/1.1 connection pool:

```python
import httpcore
import concurrent.futures
import time


def download(http, year):
    http.request("GET", f"https://en.wikipedia.org/wiki/{year}")


def main():
    with httpcore.ConnectionPool() as http:
        started = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as threads:
            for year in range(2000, 2020):
                threads.submit(download, http, year)
        complete = time.time()

        for connection in http.connections:
            print(connection)
        print("Complete in %.3f seconds" % (complete - started))


main()
```

If you run this with an HTTP/1.1 connection pool, you ought to see output similar to the following:

```python
<HTTPConnection ['https://en.wikipedia.org:443', HTTP/1.1, IDLE, Request Count: 2]>,
<HTTPConnection ['https://en.wikipedia.org:443', HTTP/1.1, IDLE, Request Count: 3]>,
<HTTPConnection ['https://en.wikipedia.org:443', HTTP/1.1, IDLE, Request Count: 6]>,
<HTTPConnection ['https://en.wikipedia.org:443', HTTP/1.1, IDLE, Request Count: 5]>,
<HTTPConnection ['https://en.wikipedia.org:443', HTTP/1.1, IDLE, Request Count: 1]>,
<HTTPConnection ['https://en.wikipedia.org:443', HTTP/1.1, IDLE, Request Count: 1]>,
<HTTPConnection ['https://en.wikipedia.org:443', HTTP/1.1, IDLE, Request Count: 1]>,
<HTTPConnection ['https://en.wikipedia.org:443', HTTP/1.1, IDLE, Request Count: 1]>
Complete in 0.586 seconds
```

We can see that the connection pool required a number of connections in order to handle the parallel requests.

If we now upgrade our connection pool to support HTTP/2:

```python
with httpcore.ConnectionPool(http2=True) as http:
    ...
```

And run the same script again, we should end up with something like this:

```python
<HTTPConnection ['https://en.wikipedia.org:443', HTTP/2, IDLE, Request Count: 20]>
Complete in 0.573 seconds
```

All of our requests have been handled over a single connection.

Switching to HTTP/2 should not *necessarily* be considered an "upgrade". It is more complex, and requires more computational power, and so particularly in an interpreted language like Python it *could* be slower in some instances. Moreover, utilising multiple connections may end up connecting to multiple hosts, and could sometimes appear faster to the client, at the cost of requiring more server resources. Enabling HTTP/2 is most likely to be beneficial if you are sending requests in high concurrency, and may often be more well suited to an async context, rather than multi-threading.

## Inspecting the HTTP version

Enabling HTTP/2 support on the client does not *necessarily* mean that your requests and responses will be transported over HTTP/2, since both the client *and* the server need to support HTTP/2. If you connect to a server that only supports HTTP/1.1 the client will use a standard HTTP/1.1 connection instead.

You can determine which version of the HTTP protocol was used by examining the `"http_version"` response extension.

```python
import httpcore

pool = httpcore.ConnectionPool(http2=True)
response = pool.request("GET", "https://www.example.com/")

# Should be one of b"HTTP/2", b"HTTP/1.1", b"HTTP/1.0", or b"HTTP/0.9".
print(response.extensions["http_version"])
```

See [the extensions documentation](extensions.md) for more details.

## HTTP/2 negotiation

Robust servers need to support both HTTP/2 and HTTP/1.1 capable clients, and so need some way to "negotiate" with the client which protocol version will be used.

### HTTP/2 over HTTPS

Generally the method used is for the server to advertise if it has HTTP/2 support during the part of the SSL connection handshake. This is known as ALPN - "Application Layer Protocol Negotiation".

Most browsers only provide HTTP/2 support over HTTPS connections, and this is also the default behaviour that `httpcore` provides. If you enable HTTP/2 support you should still expect to see HTTP/1.1 connections for any `http://` URLs.

### HTTP/2 over HTTP

Servers can optionally also support HTTP/2 over HTTP by supporting the `Upgrade: h2c` header.

This mechanism is not supported by `httpcore`. It requires an additional round-trip between the client and server, and also requires any request body to be sent twice.

### Prior Knowledge

If you know in advance that the server you are communicating with will support HTTP/2, then you can enforce that the client uses HTTP/2, without requiring either ALPN support or an HTTP `Upgrade: h2c` header.

This is managed by disabling HTTP/1.1 support on the connection pool:

```python
pool = httpcore.ConnectionPool(http1=False, http2=True)
```

## Request & response headers

Because HTTP/2 frames the requests and responses somewhat differently to HTTP/1.1, there is a difference in some of the headers that are used.

In order for the `httpcore` library to support both HTTP/1.1 and HTTP/2 transparently, the HTTP/1.1 style is always used throughout the API. Any differences in header styles are only mapped onto HTTP/2 at the internal network layer.

## Request headers

The following pseudo-headers are used by HTTP/2 in the request:

* `:method` - The request method.
* `:path` - Taken from the URL of the request.
* `:authority` - Equivalent to the `Host` header in HTTP/1.1. In `httpcore` this is represented using the request `Host` header, which is automatically populated from the request URL if no `Host` header is explicitly included.
* `:scheme` - Taken from the URL of the request.

These pseudo-headers are included in `httpcore` as part of the `request.method` and `request.url` attributes, and through the `request.headers["Host"]` header. *They are not exposed directly by their psuedo-header names.*

The one other difference to be aware of is the `Transfer-Encoding: chunked` header.

In HTTP/2 this header is never used, since streaming data is framed using a different mechanism.

In `httpcore` the `Transfer-Encoding: chunked` header is always used to represent the presence of a streaming body on the request, and is automatically populated if required. However the header is only sent if the underlying connection ends up being HTTP/1.1, and is omitted if the underlying connection ends up being HTTP/2.

## Response headers

The following pseudo-header is used by HTTP/2 in the response:

* `:status` - The response status code.

In `httpcore` this *is represented by the `response.status` attribute, rather than being exposed as a psuedo-header*.
