# Requests, Responses, and URLs

TODO

## Requests

Request instances in `httpcore2` are deliberately simple, and only include the essential information required to represent an HTTP request.

Properties on the request are plain byte-wise representations.

```python
>>> request = httpcore2.Request("GET", "https://www.example.com/")
>>> request.method
b"GET"
>>> request.url
httpcore2.URL(scheme=b"https", host=b"www.example.com", port=None, target=b"/")
>>> request.headers
[(b'Host', b'www.example.com')]
>>> request.stream
<ByteStream [0 bytes]>
```

The interface is liberal in the types that it accepts, but specific in the properties that it uses to represent them. For example, headers may be specified as a dictionary of strings, but internally are represented as a list of `(byte, byte)` tuples.

```python
>>> headers = {"User-Agent": "custom"}
>>> request = httpcore2.Request("GET", "https://www.example.com/", headers=headers)
>>> request.headers
[(b'Host', b'www.example.com'), (b"User-Agent", b"custom")]

## Responses

...

## URLs

...

---

# Reference

## `httpcore2.Request`

::: httpcore2.Request
    handler: python
    rendering:
        show_source: False

## `httpcore2.Response`

::: httpcore2.Response
    handler: python
    rendering:
        show_source: False

## `httpcore2.URL`

::: httpcore2.URL
    handler: python
    rendering:
        show_source: False
