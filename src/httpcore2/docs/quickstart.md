# Quickstart

For convenience, the `httpcore2` package provides a couple of top-level functions that you can use for sending HTTP requests. You probably don't want to integrate against functions if you're writing a library that uses `httpcore2`, but you might find them useful for testing `httpcore2` from the command-line, or if you're writing a simple script that doesn't require any of the connection pooling or advanced configuration that `httpcore2` offers.

## Sending a request

We'll start off by sending a request...

```python
import httpcore2

response = httpcore2.request("GET", "https://www.example.com/")

print(response)
# <Response [200]>
print(response.status)
# 200
print(response.headers)
# [(b'Accept-Ranges', b'bytes'), (b'Age', b'557328'), (b'Cache-Control', b'max-age=604800'), ...]
print(response.content)
# b'<!doctype html>\n<html>\n<head>\n<title>Example Domain</title>\n\n<meta charset="utf-8"/>\n ...'
```

## Request headers

Request headers may be included either in a dictionary style, or as a list of two-tuples.

```python
import httpcore2
import json

headers = {'User-Agent': 'httpcore2'}
r = httpcore2.request('GET', 'https://httpbin.org/headers', headers=headers)

print(json.loads(r.content))
# {
#     'headers': {
#         'Host': 'httpbin.org',
#         'User-Agent': 'httpcore2',
#         'X-Amzn-Trace-Id': 'Root=1-616ff5de-5ea1b7e12766f1cf3b8e3a33'
#     }
# }
```

The keys and values may either be provided as strings or as bytes. Where strings are provided they may only contain characters within the ASCII range `chr(0)` - `chr(127)`. To include characters outside this range you must deal with any character encoding explicitly, and pass bytes as the header keys/values.

The `Host` header will always be automatically included in any outgoing request, as it is strictly required to be present by the HTTP protocol.

*Note that the `X-Amzn-Trace-Id` header shown in the example above is not an outgoing request header, but has been added by a gateway server.*

## Request body

A request body can be included either as bytes...

```python
import httpcore2
import json

r = httpcore2.request('POST', 'https://httpbin.org/post', content=b'Hello, world')

print(json.loads(r.content))
# {
#     'args': {},
#     'data': 'Hello, world',
#     'files': {},
#     'form': {},
#     'headers': {
#         'Host': 'httpbin.org',
#         'Content-Length': '12',
#         'X-Amzn-Trace-Id': 'Root=1-61700258-00e338a124ca55854bf8435f'
#     },
#     'json': None,
#     'origin': '68.41.35.196',
#     'url': 'https://httpbin.org/post'
# }
```

Or as an iterable that returns bytes...

```python
import httpcore2
import json

with open("hello-world.txt", "rb") as input_file:
    r = httpcore2.request('POST', 'https://httpbin.org/post', content=input_file)

print(json.loads(r.content))
# {
#     'args': {},
#     'data': 'Hello, world',
#     'files': {},
#     'form': {},
#     'headers': {
#         'Host': 'httpbin.org',
#         'Transfer-Encoding': 'chunked',
#         'X-Amzn-Trace-Id': 'Root=1-61700258-00e338a124ca55854bf8435f'
#     },
#     'json': None,
#     'origin': '68.41.35.196',
#     'url': 'https://httpbin.org/post'
# }
```

When a request body is included, either a `Content-Length` header or a `Transfer-Encoding: chunked` header will be automatically included.

The `Content-Length` header is used when passing bytes, and indicates an HTTP request with a body of a pre-determined length.

The `Transfer-Encoding: chunked` header is the mechanism that HTTP/1.1 uses for sending HTTP request bodies without a pre-determined length.

## Streaming responses

When using the `httpcore2.request()` function, the response body will automatically be read to completion, and made available in the `response.content` attribute.

Sometimes you may be dealing with large responses and not want to read the entire response into memory. The `httpcore2.stream()` function provides a mechanism for sending a request and dealing with a streaming response:

```python
import httpcore2

with httpcore2.stream('GET', 'https://example.com') as response:
    for chunk in response.iter_stream():
        print(f"Downloaded: {chunk}")
```

Here's a more complete example that demonstrates downloading a response:

```python
import httpcore2

with httpcore2.stream('GET', 'https://speed.hetzner.de/100MB.bin') as response:
    with open("download.bin", "wb") as output_file:
        for chunk in response.iter_stream():
            output_file.write(chunk)
```

The `httpcore2.stream()` API also allows you to *conditionally* read the response...

```python
import httpcore2

with httpcore2.stream('GET', 'https://example.com') as response:
    content_length = [int(v) for k, v in response.headers if k.lower() == b'content-length'][0]
    if content_length > 100_000_000:
        raise Exception("Response too large.")
    response.read()  # `response.content` is now available.
```

---

# Reference

## `httpcore2.request()`

::: httpcore2.request
    handler: python
    rendering:
        show_source: False

## `httpcore2.stream()`

::: httpcore2.stream
    handler: python
    rendering:
        show_source: False
