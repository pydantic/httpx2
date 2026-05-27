import typing

import pytest

import httpcore2

# URL


def test_url() -> None:
    url = httpcore2.URL("https://www.example.com/")
    assert url == httpcore2.URL(scheme="https", host="www.example.com", port=None, target="/")
    assert bytes(url) == b"https://www.example.com/"


def test_url_with_port() -> None:
    url = httpcore2.URL("https://www.example.com:443/")
    assert url == httpcore2.URL(scheme="https", host="www.example.com", port=443, target="/")
    assert bytes(url) == b"https://www.example.com:443/"


def test_url_with_invalid_argument() -> None:
    with pytest.raises(TypeError) as exc_info:
        httpcore2.URL(123)  # type: ignore
    assert str(exc_info.value) == "url must be bytes or str, but got int."


def test_url_cannot_include_unicode_strings() -> None:
    """
    URLs instantiated with strings outside of the plain ASCII range are disallowed,
    but the explicit style allows for these ambiguous cases to be precisely expressed.
    """
    with pytest.raises(TypeError) as exc_info:
        httpcore2.URL("https://www.example.com/☺")
    assert str(exc_info.value) == "url strings may not include unicode characters."

    httpcore2.URL(scheme=b"https", host=b"www.example.com", target="/☺".encode("utf-8"))


def test_url_origin_socks5() -> None:
    url = httpcore2.URL("socks5://127.0.0.1")
    origin = url.origin
    assert origin == httpcore2.Origin(scheme=b"socks5", host=b"127.0.0.1", port=1080)
    assert str(origin) == "socks5://127.0.0.1:1080"

    url = httpcore2.URL("socks5h://127.0.0.1")
    origin = url.origin
    assert origin == httpcore2.Origin(scheme=b"socks5h", host=b"127.0.0.1", port=1080)
    assert str(origin) == "socks5h://127.0.0.1:1080"


# Request


def test_request() -> None:
    request = httpcore2.Request("GET", "https://www.example.com/")
    assert request.method == b"GET"
    assert request.url == httpcore2.URL("https://www.example.com/")
    assert request.headers == []
    assert request.extensions == {}
    assert repr(request) == "<Request [b'GET']>"
    assert repr(request.url) == "URL(scheme=b'https', host=b'www.example.com', port=None, target=b'/')"
    assert repr(request.stream) == "<ByteStream [0 bytes]>"


def test_request_with_target_extension() -> None:
    extensions = {"target": b"/another_path"}
    request = httpcore2.Request("GET", "https://www.example.com/path", extensions=extensions)
    assert request.url.target == b"/another_path"

    extensions = {"target": b"/unescaped|path"}
    request = httpcore2.Request("GET", "https://www.example.com/path", extensions=extensions)
    assert request.url.target == b"/unescaped|path"


def test_request_with_invalid_method() -> None:
    with pytest.raises(TypeError) as exc_info:
        httpcore2.Request(123, "https://www.example.com/")  # type: ignore
    assert str(exc_info.value) == "method must be bytes or str, but got int."


def test_request_with_invalid_url() -> None:
    with pytest.raises(TypeError) as exc_info:
        httpcore2.Request("GET", 123)  # type: ignore
    assert str(exc_info.value) == "url must be a URL, bytes, or str, but got int."


def test_request_with_invalid_headers() -> None:
    with pytest.raises(TypeError) as exc_info:
        httpcore2.Request("GET", "https://www.example.com/", headers=123)  # type: ignore
    assert str(exc_info.value) == "headers must be a mapping or sequence of two-tuples, but got int."


# Response


def test_response() -> None:
    response = httpcore2.Response(200)
    assert response.status == 200
    assert response.headers == []
    assert response.extensions == {}
    assert repr(response) == "<Response [200]>"
    assert repr(response.stream) == "<ByteStream [0 bytes]>"


# Tests for reading and streaming sync byte streams...


class ByteIterator:
    def __init__(self, chunks: typing.List[bytes]) -> None:
        self._chunks = chunks

    def __iter__(self) -> typing.Iterator[bytes]:
        for chunk in self._chunks:
            yield chunk


def test_response_sync_read() -> None:
    stream = ByteIterator([b"Hello, ", b"world!"])
    response = httpcore2.Response(200, content=stream)
    assert response.read() == b"Hello, world!"
    assert response.content == b"Hello, world!"


def test_response_sync_streaming() -> None:
    stream = ByteIterator([b"Hello, ", b"world!"])
    response = httpcore2.Response(200, content=stream)
    content = b"".join(list(response.iter_stream()))
    assert content == b"Hello, world!"

    # We streamed the response rather than reading it, so .content is not available.
    with pytest.raises(RuntimeError):
        _ = response.content

    # Once we've streamed the response, we can't access the stream again.
    with pytest.raises(RuntimeError):
        for _chunk in response.iter_stream():
            pass  # pragma: nocover


# Tests for reading and streaming async byte streams...


class AsyncByteIterator:
    def __init__(self, chunks: typing.List[bytes]) -> None:
        self._chunks = chunks

    async def __aiter__(self) -> typing.AsyncIterator[bytes]:
        for chunk in self._chunks:
            yield chunk


@pytest.mark.trio
async def test_response_async_read() -> None:
    stream = AsyncByteIterator([b"Hello, ", b"world!"])
    response = httpcore2.Response(200, content=stream)
    assert await response.aread() == b"Hello, world!"
    assert response.content == b"Hello, world!"


@pytest.mark.trio
async def test_response_async_streaming() -> None:
    stream = AsyncByteIterator([b"Hello, ", b"world!"])
    response = httpcore2.Response(200, content=stream)
    content = b"".join([chunk async for chunk in response.aiter_stream()])
    assert content == b"Hello, world!"

    # We streamed the response rather than reading it, so .content is not available.
    with pytest.raises(RuntimeError):
        _ = response.content

    # Once we've streamed the response, we can't access the stream again.
    with pytest.raises(RuntimeError):
        async for _chunk in response.aiter_stream():
            pass  # pragma: nocover
