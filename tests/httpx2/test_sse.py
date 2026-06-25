from __future__ import annotations

from collections.abc import AsyncIterator, Iterator

import pytest

import httpx2

SSE_BODY = b": this is a comment\nevent: ping\ndata: hello\nid: 1\nretry: 500\n\ndata: first\ndata: second\n\n"


def sse_handler(request: httpx2.Request) -> httpx2.Response:
    return httpx2.Response(200, content=SSE_BODY, headers={"Content-Type": "text/event-stream"})


def test_sse_sync() -> None:
    with httpx2.Client(transport=httpx2.MockTransport(sse_handler)) as client:
        with client.sse("http://testserver/sse") as source:
            events = list(source)

    assert events == [
        httpx2.ServerSentEvent(event="ping", data="hello", id="1", retry=500),
        httpx2.ServerSentEvent(event="message", data="first\nsecond", id="1"),
    ]


@pytest.mark.anyio
async def test_sse_async() -> None:
    async with httpx2.AsyncClient(transport=httpx2.MockTransport(sse_handler)) as client:
        async with client.sse("http://testserver/sse") as source:
            events = [event async for event in source]

    assert events == [
        httpx2.ServerSentEvent(event="ping", data="hello", id="1", retry=500),
        httpx2.ServerSentEvent(event="message", data="first\nsecond", id="1"),
    ]


def test_default_event_is_message() -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, content=b"data: hi\n\n", headers={"Content-Type": "text/event-stream"})

    with httpx2.Client(transport=httpx2.MockTransport(handler)) as client:
        with client.sse("http://testserver/sse") as source:
            (event,) = list(source)

    assert event.event == "message"
    assert event.data == "hi"
    assert event.id == ""
    assert event.retry is None


def test_json() -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, content=b'data: {"x": 1}\n\n', headers={"Content-Type": "text/event-stream"})

    with httpx2.Client(transport=httpx2.MockTransport(handler)) as client:
        with client.sse("http://testserver/sse") as source:
            (event,) = list(source)

    assert event.json() == {"x": 1}


def test_invalid_retry_is_ignored() -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        body = b"retry: not-a-number\ndata: hi\n\n"
        return httpx2.Response(200, content=body, headers={"Content-Type": "text/event-stream"})

    with httpx2.Client(transport=httpx2.MockTransport(handler)) as client:
        with client.sse("http://testserver/sse") as source:
            (event,) = list(source)

    assert event.retry is None


def test_id_with_null_is_ignored() -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        body = b"id: a\0b\ndata: hi\n\n"
        return httpx2.Response(200, content=body, headers={"Content-Type": "text/event-stream"})

    with httpx2.Client(transport=httpx2.MockTransport(handler)) as client:
        with client.sse("http://testserver/sse") as source:
            (event,) = list(source)

    assert event.id == ""


def test_field_without_value() -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, content=b"data\n\n", headers={"Content-Type": "text/event-stream"})

    with httpx2.Client(transport=httpx2.MockTransport(handler)) as client:
        with client.sse("http://testserver/sse") as source:
            (event,) = list(source)

    assert event.data == ""


def test_last_event_id_persists() -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        body = b"id: 1\ndata: a\n\ndata: b\n\n"
        return httpx2.Response(200, content=body, headers={"Content-Type": "text/event-stream"})

    with httpx2.Client(transport=httpx2.MockTransport(handler)) as client:
        with client.sse("http://testserver/sse") as source:
            events = list(source)

    assert [event.id for event in events] == ["1", "1"]


@pytest.mark.parametrize("content_type", ["text/event-stream", "text/event-stream; charset=utf-8"])
def test_content_type_accepted(content_type: str) -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, content=b"data: hi\n\n", headers={"Content-Type": content_type})

    with httpx2.Client(transport=httpx2.MockTransport(handler)) as client:
        with client.sse("http://testserver/sse") as source:
            (event,) = list(source)

    assert event.data == "hi"


def test_content_type_mismatch_raises() -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, content=b"data: hi\n\n", headers={"Content-Type": "application/json"})

    with httpx2.Client(transport=httpx2.MockTransport(handler)) as client:
        with client.sse("http://testserver/sse") as source:
            with pytest.raises(httpx2.SSEError, match="text/event-stream"):
                list(source)


@pytest.mark.anyio
async def test_content_type_mismatch_raises_async() -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, content=b"data: hi\n\n", headers={"Content-Type": "application/json"})

    async with httpx2.AsyncClient(transport=httpx2.MockTransport(handler)) as client:
        async with client.sse("http://testserver/sse") as source:
            with pytest.raises(httpx2.SSEError, match="text/event-stream"):
                [event async for event in source]


def test_sets_sse_headers() -> None:
    captured: dict[str, str] = {}

    def handler(request: httpx2.Request) -> httpx2.Response:
        captured.update(request.headers)
        return httpx2.Response(200, content=b"data: hi\n\n", headers={"Content-Type": "text/event-stream"})

    with httpx2.Client(transport=httpx2.MockTransport(handler)) as client:
        with client.sse("http://testserver/sse") as source:
            list(source)

    assert captured["accept"] == "text/event-stream"
    assert captured["cache-control"] == "no-store"


def test_user_headers_take_precedence() -> None:
    captured: dict[str, str] = {}

    def handler(request: httpx2.Request) -> httpx2.Response:
        captured.update(request.headers)
        return httpx2.Response(200, content=b"data: hi\n\n", headers={"Content-Type": "text/event-stream"})

    with httpx2.Client(transport=httpx2.MockTransport(handler)) as client:
        with client.sse("http://testserver/sse", headers={"Accept": "text/event-stream, application/json"}) as source:
            list(source)

    assert captured["accept"] == "text/event-stream, application/json"


def test_response_is_accessible() -> None:
    with httpx2.Client(transport=httpx2.MockTransport(sse_handler)) as client:
        with client.sse("http://testserver/sse") as source:
            assert isinstance(source.response, httpx2.Response)
            assert source.response.status_code == 200


def test_post_method() -> None:
    captured: list[str] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        captured.append(request.method)
        return httpx2.Response(200, content=b"data: hi\n\n", headers={"Content-Type": "text/event-stream"})

    with httpx2.Client(transport=httpx2.MockTransport(handler)) as client:
        with client.sse("http://testserver/sse", method="POST", json={"q": 1}) as source:
            list(source)

    assert captured == ["POST"]


def test_chunk_boundaries_and_crlf_sync() -> None:
    def chunks() -> Iterator[bytes]:
        yield b"data: he"
        yield b"llo\r\n\r"
        yield b"\ndata: wor"
        yield b"ld\r\n\r\n"

    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, content=chunks(), headers={"Content-Type": "text/event-stream"})

    with httpx2.Client(transport=httpx2.MockTransport(handler)) as client:
        with client.sse("http://testserver/sse") as source:
            events = list(source)

    assert [event.data for event in events] == ["hello", "world"]


@pytest.mark.anyio
async def test_chunk_boundaries_and_crlf_async() -> None:
    async def chunks() -> AsyncIterator[bytes]:
        yield b"data: he"
        yield b"llo\r\n\r"
        yield b"\ndata: wor"
        yield b"ld\r\n\r\n"

    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, content=chunks(), headers={"Content-Type": "text/event-stream"})

    async with httpx2.AsyncClient(transport=httpx2.MockTransport(handler)) as client:
        async with client.sse("http://testserver/sse") as source:
            events = [event async for event in source]

    assert [event.data for event in events] == ["hello", "world"]


def test_event_without_trailing_blank_line() -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, content=b"data: hi", headers={"Content-Type": "text/event-stream"})

    with httpx2.Client(transport=httpx2.MockTransport(handler)) as client:
        with client.sse("http://testserver/sse") as source:
            events = list(source)

    assert events == []


def test_leading_blank_line_is_ignored() -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, content=b"\ndata: hi\n\n", headers={"Content-Type": "text/event-stream"})

    with httpx2.Client(transport=httpx2.MockTransport(handler)) as client:
        with client.sse("http://testserver/sse") as source:
            (event,) = list(source)

    assert event.data == "hi"


def test_event_dispatched_at_eof_on_trailing_cr_sync() -> None:
    def chunks() -> Iterator[bytes]:
        yield b"data: hi\n"
        yield b"\r"

    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, content=chunks(), headers={"Content-Type": "text/event-stream"})

    with httpx2.Client(transport=httpx2.MockTransport(handler)) as client:
        with client.sse("http://testserver/sse") as source:
            (event,) = list(source)

    assert event.data == "hi"


@pytest.mark.anyio
async def test_event_dispatched_at_eof_on_trailing_cr_async() -> None:
    async def chunks() -> AsyncIterator[bytes]:
        yield b"data: hi\n"
        yield b"\r"

    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, content=chunks(), headers={"Content-Type": "text/event-stream"})

    async with httpx2.AsyncClient(transport=httpx2.MockTransport(handler)) as client:
        async with client.sse("http://testserver/sse") as source:
            events = [event async for event in source]

    assert [event.data for event in events] == ["hi"]
