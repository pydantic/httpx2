import pytest

import httpcore2


@pytest.mark.anyio
async def test_http11_connection() -> None:
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    async with httpcore2.AsyncHTTP11Connection(origin=origin, stream=stream, keepalive_expiry=5.0) as conn:
        response = await conn.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"

        assert conn.is_idle()
        assert not conn.is_closed()
        assert conn.is_available()
        assert not conn.has_expired()
        assert conn.is_connected()
        assert repr(conn) == "<AsyncHTTP11Connection ['https://example.com:443', IDLE, Request Count: 1]>"


@pytest.mark.anyio
async def test_http11_connection_unread_response() -> None:
    """
    If the client releases the response without reading it to termination,
    then the connection will not be reusable.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    async with httpcore2.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        async with conn.stream("GET", "https://example.com/") as response:
            assert response.status == 200

        assert not conn.is_idle()
        assert conn.is_closed()
        assert not conn.is_available()
        assert not conn.has_expired()
        assert repr(conn) == "<AsyncHTTP11Connection ['https://example.com:443', CLOSED, Request Count: 1]>"


@pytest.mark.anyio
async def test_http11_connection_with_remote_protocol_error() -> None:
    """
    If a remote protocol error occurs, then no response will be returned,
    and the connection will not be reusable.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream([b"Wait, this isn't valid HTTP!", b""])
    async with httpcore2.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(httpcore2.RemoteProtocolError):
            await conn.request("GET", "https://example.com/")

        assert not conn.is_idle()
        assert conn.is_closed()
        assert not conn.is_available()
        assert not conn.has_expired()
        assert repr(conn) == "<AsyncHTTP11Connection ['https://example.com:443', CLOSED, Request Count: 1]>"


@pytest.mark.anyio
async def test_http11_connection_with_incomplete_response() -> None:
    """
    We should be gracefully handling the case where the connection ends prematurely.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, wor",
        ]
    )
    async with httpcore2.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(httpcore2.RemoteProtocolError):
            await conn.request("GET", "https://example.com/")

        assert not conn.is_idle()
        assert conn.is_closed()
        assert not conn.is_available()
        assert not conn.has_expired()
        assert repr(conn) == "<AsyncHTTP11Connection ['https://example.com:443', CLOSED, Request Count: 1]>"


@pytest.mark.anyio
async def test_http11_connection_with_local_protocol_error() -> None:
    """
    If a local protocol error occurs, then no response will be returned,
    and the connection will not be reusable.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    async with httpcore2.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(httpcore2.LocalProtocolError) as exc_info:
            await conn.request("GET", "https://example.com/", headers={"Host": "\0"})

        assert str(exc_info.value) == "Illegal header value b'\\x00'"

        assert not conn.is_idle()
        assert conn.is_closed()
        assert not conn.is_available()
        assert not conn.has_expired()
        assert repr(conn) == "<AsyncHTTP11Connection ['https://example.com:443', CLOSED, Request Count: 1]>"


@pytest.mark.anyio
async def test_http11_connection_handles_one_active_request() -> None:
    """
    Attempting to send a request while one is already in-flight will raise
    a ConnectionNotAvailable exception.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    async with httpcore2.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        async with conn.stream("GET", "https://example.com/"):
            with pytest.raises(httpcore2.ConnectionNotAvailable):
                await conn.request("GET", "https://example.com/")


@pytest.mark.anyio
async def test_http11_connection_attempt_close() -> None:
    """
    A connection can only be closed when it is idle.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    async with httpcore2.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        async with conn.stream("GET", "https://example.com/") as response:
            await response.aread()
            assert response.status == 200
            assert response.content == b"Hello, world!"


@pytest.mark.anyio
async def test_http11_request_to_incorrect_origin() -> None:
    """
    A connection can only send requests to whichever origin it is connected to.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream([])
    async with httpcore2.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(RuntimeError):
            await conn.request("GET", "https://other.com/")


@pytest.mark.anyio
async def test_http11_expect_continue() -> None:
    """
    HTTP "100 Continue" is an interim response.
    We simply ignore it and return the final response.

    https://httpwg.org/specs/rfc9110.html#status.100
    https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/100
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
            b"HTTP/1.1 100 Continue\r\n",
            b"\r\n",
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    async with httpcore2.AsyncHTTP11Connection(origin=origin, stream=stream, keepalive_expiry=5.0) as conn:
        response = await conn.request(
            "GET",
            "https://example.com/",
            headers={"Expect": "continue"},
        )
        assert response.status == 200
        assert response.content == b"Hello, world!"


@pytest.mark.anyio
async def test_http11_upgrade_connection() -> None:
    """
    HTTP "101 Switching Protocols" indicates an upgraded connection.

    We should return the response, so that the network stream
    may be used for the upgraded connection.

    https://httpwg.org/specs/rfc9110.html#status.101
    https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/101
    """
    origin = httpcore2.Origin(b"wss", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
            b"HTTP/1.1 101 Switching Protocols\r\n",
            b"Connection: upgrade\r\n",
            b"Upgrade: custom\r\n",
            b"\r\n",
            b"...",
        ]
    )
    async with httpcore2.AsyncHTTP11Connection(origin=origin, stream=stream, keepalive_expiry=5.0) as conn:
        async with conn.stream(
            "GET",
            "wss://example.com/",
            headers={"Connection": "upgrade", "Upgrade": "custom"},
        ) as response:
            assert response.status == 101
            network_stream = response.extensions["network_stream"]
            content = await network_stream.read(max_bytes=1024)
            assert content == b"..."


@pytest.mark.anyio
async def test_http11_upgrade_with_trailing_data() -> None:
    """
    HTTP "101 Switching Protocols" indicates an upgraded connection.

    In `CONNECT` and `Upgrade:` requests, we need to handover the trailing data
    in the h11.Connection object.

    https://h11.readthedocs.io/en/latest/api.html#switching-protocols
    """
    origin = httpcore2.Origin(b"wss", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        # The first element of this mock network stream buffer simulates networking
        # in which response headers and data are received at once.
        # This means that "foobar" becomes trailing data.
        [
            (b"HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: custom\r\n\r\nfoobar"),
            b"baz",
        ]
    )
    async with httpcore2.AsyncHTTP11Connection(origin=origin, stream=stream, keepalive_expiry=5.0) as conn:
        async with conn.stream(
            "GET",
            "wss://example.com/",
            headers={"Connection": "upgrade", "Upgrade": "custom"},
        ) as response:
            assert response.status == 101
            network_stream = response.extensions["network_stream"]

            content = await network_stream.read(max_bytes=3)
            assert content == b"foo"
            content = await network_stream.read(max_bytes=3)
            assert content == b"bar"
            content = await network_stream.read(max_bytes=3)
            assert content == b"baz"

            # Lazy tests for AsyncHTTP11UpgradeStream
            await network_stream.write(b"spam")
            invalid = network_stream.get_extra_info("invalid")
            assert invalid is None
            await network_stream.aclose()


@pytest.mark.anyio
async def test_http11_early_hints() -> None:
    """
    HTTP "103 Early Hints" is an interim response.
    We simply ignore it and return the final response.

    https://datatracker.ietf.org/doc/rfc8297/
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
            b"HTTP/1.1 103 Early Hints\r\n",
            b"Link: </style.css>; rel=preload; as=style\r\n",
            b"Link: </script.js.css>; rel=preload; as=style\r\n",
            b"\r\n",
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: text/html; charset=utf-8\r\n",
            b"Content-Length: 30\r\n",
            b"Link: </style.css>; rel=preload; as=style\r\n",
            b"Link: </script.js>; rel=preload; as=script\r\n",
            b"\r\n",
            b"<html>Hello, world! ...</html>",
        ]
    )
    async with httpcore2.AsyncHTTP11Connection(origin=origin, stream=stream, keepalive_expiry=5.0) as conn:
        response = await conn.request(
            "GET",
            "https://example.com/",
            headers={"Expect": "continue"},
        )
        assert response.status == 200
        assert response.content == b"<html>Hello, world! ...</html>"


@pytest.mark.anyio
async def test_http11_connection_merges_duplicate_chunked_transfer_encoding() -> None:
    """
    Some servers send `Transfer-Encoding: chunked` twice on the wire (e.g.
    https://github.com/pydantic/httpx2/issues/622). Duplicate, byte-identical
    `Transfer-Encoding: chunked` header lines should be merged into one,
    mirroring how h11 already tolerates duplicate identical Content-Length
    headers, rather than raising `RemoteProtocolError`.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: text/plain\r\n",
            b"Transfer-Encoding: chunked\r\n",
            b"Transfer-Encoding: chunked\r\n",
            b"\r\n",
            b"5\r\nHello\r\n0\r\n\r\n",
        ]
    )
    async with httpcore2.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        response = await conn.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello"

        transfer_encodings = [v for k, v in response.headers if k.lower() == b"transfer-encoding"]
        assert transfer_encodings == [b"chunked"]


@pytest.mark.anyio
async def test_http11_connection_merges_duplicate_chunked_transfer_encoding_split_across_reads() -> None:
    """
    The merge must work even when the duplicate header line, and the
    terminating blank line, are split across separate network reads.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: text/plain\r\n",
            b"Transfer-Encoding: chunked\r\nTransfer-Enco",
            b"ding: chunked\r\n\r\n",
            b"5\r\nHello\r\n0\r\n\r\n",
        ]
    )
    async with httpcore2.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        response = await conn.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello"

        transfer_encodings = [v for k, v in response.headers if k.lower() == b"transfer-encoding"]
        assert transfer_encodings == [b"chunked"]


@pytest.mark.anyio
async def test_http11_connection_with_conflicting_transfer_encoding_headers() -> None:
    """
    Duplicate `Transfer-Encoding` headers with *differing* values are not a
    safe, unambiguous case, so they should still raise `RemoteProtocolError`
    exactly as before.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Transfer-Encoding: chunked\r\n",
            b"Transfer-Encoding: identity\r\n",
            b"\r\n",
            b"",
        ]
    )
    async with httpcore2.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(httpcore2.RemoteProtocolError):
            await conn.request("GET", "https://example.com/")


@pytest.mark.anyio
async def test_http11_connection_does_not_merge_transfer_encoding_alongside_content_length() -> None:
    """
    `Transfer-Encoding` combined with `Content-Length` is exactly the shape
    of the classic conflicting-framing request-smuggling primitive, so the
    merge must never apply when a `Content-Length` header is also present --
    even though the duplicate `Transfer-Encoding` lines are themselves
    byte-identical -- leaving h11 to reject the message as before.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Length: 46\r\n",
            b"Transfer-Encoding: chunked\r\n",
            b"Transfer-Encoding: chunked\r\n",
            b"\r\n",
            b"5\r\nHello\r\n0\r\n\r\n",
        ]
    )
    async with httpcore2.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(httpcore2.RemoteProtocolError):
            await conn.request("GET", "https://example.com/")


@pytest.mark.anyio
async def test_http11_connection_with_oversized_headers_and_no_terminator() -> None:
    """
    If the header block never terminates and grows past the incomplete-event
    size bound, we must still hand off to h11 (which enforces its own limit)
    rather than buffering unboundedly.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Cookie: " + b"x" * (100 * 1024) + b"\r\n",
            b"",
        ]
    )
    async with httpcore2.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(httpcore2.RemoteProtocolError):
            await conn.request("GET", "https://example.com/")


@pytest.mark.anyio
async def test_http11_connection_merges_duplicate_transfer_encoding_with_lf_terminated_headers() -> None:
    """
    h11 tolerates bare `\\n` (not just `\\r\\n`) as a header line ending, so
    the merge must recognize the header/body boundary and split lines the
    same way h11 does, not assume `\\r\\n` throughout.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
            b"HTTP/1.1 200 OK\n",
            b"Content-Type: text/plain\n",
            b"Transfer-Encoding: chunked\n",
            b"Transfer-Encoding: chunked\n",
            b"\n",
            b"5\r\nHello\r\n0\r\n\r\n",
        ]
    )
    async with httpcore2.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        response = await conn.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello"

        transfer_encodings = [v for k, v in response.headers if k.lower() == b"transfer-encoding"]
        assert transfer_encodings == [b"chunked"]


@pytest.mark.anyio
async def test_http11_connection_merges_duplicate_transfer_encoding_after_interim_response() -> None:
    """
    A `100 Continue` (or other 1xx) response ahead of the final response must
    not disable normalization for the final response's own headers.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
            b"HTTP/1.1 100 Continue\r\n",
            b"\r\n",
            b"HTTP/1.1 200 OK\r\n",
            b"Transfer-Encoding: chunked\r\n",
            b"Transfer-Encoding: chunked\r\n",
            b"\r\n",
            b"5\r\nHello\r\n0\r\n\r\n",
        ]
    )
    async with httpcore2.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        response = await conn.request(
            "GET",
            "https://example.com/",
            headers={"Expect": "continue"},
        )
        assert response.status == 200
        assert response.content == b"Hello"

        transfer_encodings = [v for k, v in response.headers if k.lower() == b"transfer-encoding"]
        assert transfer_encodings == [b"chunked"]


@pytest.mark.anyio
async def test_http11_connection_merges_duplicate_transfer_encoding_after_interim_response_same_read() -> None:
    """
    Same as above, but the interim response and the final response's headers
    arrive in a single network read together -- h11 doesn't need another
    `NEED_DATA` round trip to see the final response's headers, so they must
    still get normalized even though no further data is read from the
    network in between.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
            b"HTTP/1.1 100 Continue\r\n\r\n"
            b"HTTP/1.1 200 OK\r\n"
            b"Transfer-Encoding: chunked\r\n"
            b"Transfer-Encoding: chunked\r\n"
            b"\r\n"
            b"5\r\nHello\r\n0\r\n\r\n"
        ]
    )
    async with httpcore2.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        response = await conn.request(
            "GET",
            "https://example.com/",
            headers={"Expect": "continue"},
        )
        assert response.status == 200
        assert response.content == b"Hello"

        transfer_encodings = [v for k, v in response.headers if k.lower() == b"transfer-encoding"]
        assert transfer_encodings == [b"chunked"]


@pytest.mark.anyio
async def test_http11_connection_does_not_merge_transfer_encoding_with_space_before_colon() -> None:
    """
    `Transfer-Encoding : chunked` (space before the colon) is not the same
    raw header line as `Transfer-Encoding: chunked` -- it's illegal per the
    header-field grammar. It must not be treated as an equivalent duplicate;
    h11 should still see it and reject the message.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Transfer-Encoding: chunked\r\n",
            b"Transfer-Encoding : chunked\r\n",
            b"\r\n",
            b"5\r\nHello\r\n0\r\n\r\n",
        ]
    )
    async with httpcore2.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(httpcore2.RemoteProtocolError):
            await conn.request("GET", "https://example.com/")


@pytest.mark.anyio
async def test_http11_connection_does_not_merge_obsolete_line_folded_transfer_encoding() -> None:
    """
    Obsolete line folding (RFC 7230 3.2.4) means a header line starting with
    whitespace is a *continuation* of the previous header's value, not a
    standalone header. A folded line that happens to read
    `Transfer-Encoding: chunked` must never be treated as a duplicate to
    merge away -- doing so would delete part of an unrelated header's value
    and let an otherwise-invalid message through. h11 must still see the
    fold and reject the message exactly as it would unpatched.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"X-Cache: HIT\r\n",
            b" Transfer-Encoding: chunked\r\n",
            b"Content-Length: 5\r\n",
            b" Transfer-Encoding: chunked\r\n",
            b"\r\n",
            b"Hello",
        ]
    )
    async with httpcore2.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(httpcore2.RemoteProtocolError):
            await conn.request("GET", "https://example.com/")


@pytest.mark.anyio
async def test_http11_connection_does_not_merge_obsolete_line_folded_transfer_encoding_without_content_length() -> None:
    """
    Same fold-continuation hazard as above, but without a `Content-Length`
    header present, so this exercises the fold-continuation skip in
    `_merge_duplicate_chunked_transfer_encoding` directly rather than via
    the (separate) `Content-Length` bail-out.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b" Transfer-Encoding: chunked\r\n",
            b"Transfer-Encoding: chunked\r\n",
            b" folded-continuation\r\n",
            b"\r\n",
            b"",
        ]
    )
    async with httpcore2.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(httpcore2.RemoteProtocolError):
            await conn.request("GET", "https://example.com/")


@pytest.mark.anyio
async def test_http11_header_sub_100kb() -> None:
    """
    A connection should be able to handle a http header size up to 100kB.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
            b"HTTP/1.1 200 OK\r\n",  # 17
            b"Content-Type: plain/text\r\n",  # 43
            b"Cookie: " + b"x" * (100 * 1024 - 72) + b"\r\n",  # 102381
            b"Content-Length: 0\r\n",  # 102400
            b"\r\n",
            b"",
        ]
    )
    async with httpcore2.AsyncHTTP11Connection(origin=origin, stream=stream, keepalive_expiry=5.0) as conn:
        response = await conn.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b""
