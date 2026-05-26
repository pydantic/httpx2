from __future__ import annotations

import typing

import pytest

import httpx2

if typing.TYPE_CHECKING:
    from conftest import TestServer


def test_get(server: TestServer) -> None:
    response = httpx2.get(server.url)
    assert response.status_code == 200
    assert response.reason_phrase == "OK"
    assert response.text == "Hello, world!"
    assert response.http_version == "HTTP/1.1"


def test_post(server: TestServer) -> None:
    response = httpx2.post(server.url, content=b"Hello, world!")
    assert response.status_code == 200
    assert response.reason_phrase == "OK"


def test_post_byte_iterator(server: TestServer) -> None:
    def data() -> typing.Iterator[bytes]:
        yield b"Hello"
        yield b", "
        yield b"world!"

    response = httpx2.post(server.url, content=data())
    assert response.status_code == 200
    assert response.reason_phrase == "OK"


def test_post_byte_stream(server: TestServer) -> None:
    class Data(httpx2.SyncByteStream):
        def __iter__(self) -> typing.Iterator[bytes]:
            yield b"Hello"
            yield b", "
            yield b"world!"

    response = httpx2.post(server.url, content=Data())
    assert response.status_code == 200
    assert response.reason_phrase == "OK"


def test_options(server: TestServer) -> None:
    response = httpx2.options(server.url)
    assert response.status_code == 200
    assert response.reason_phrase == "OK"


def test_head(server: TestServer) -> None:
    response = httpx2.head(server.url)
    assert response.status_code == 200
    assert response.reason_phrase == "OK"


def test_put(server: TestServer) -> None:
    response = httpx2.put(server.url, content=b"Hello, world!")
    assert response.status_code == 200
    assert response.reason_phrase == "OK"


def test_patch(server: TestServer) -> None:
    response = httpx2.patch(server.url, content=b"Hello, world!")
    assert response.status_code == 200
    assert response.reason_phrase == "OK"


def test_delete(server: TestServer) -> None:
    response = httpx2.delete(server.url)
    assert response.status_code == 200
    assert response.reason_phrase == "OK"


def test_stream(server: TestServer) -> None:
    with httpx2.stream("GET", server.url) as response:
        response.read()

    assert response.status_code == 200
    assert response.reason_phrase == "OK"
    assert response.text == "Hello, world!"
    assert response.http_version == "HTTP/1.1"


def test_get_invalid_url() -> None:
    with pytest.raises(httpx2.UnsupportedProtocol):
        httpx2.get("invalid://example.org")


# check that httpcore isn't imported until we do a request
def test_httpcore_lazy_loading(server: TestServer) -> None:
    import sys

    # unload our module if it is already loaded
    if "httpx2" in sys.modules:
        del sys.modules["httpx2"]
        del sys.modules["httpcore2"]
    import httpx2

    assert "httpcore2" not in sys.modules
    _response = httpx2.get(server.url)
    assert "httpcore2" in sys.modules
