import pytest

import httpcore2



def test_http11_connection() -> None:
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.MockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    with httpcore2.HTTP11Connection(origin=origin, stream=stream, keepalive_expiry=5.0) as conn:
        response = conn.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"

        assert conn.is_idle()
        assert not conn.is_closed()
        assert conn.is_available()
        assert not conn.has_expired()
        assert repr(conn) == "<HTTP11Connection ['https://example.com:443', IDLE, Request Count: 1]>"



def test_http11_connection_unread_response() -> None:
    """
    If the client releases the response without reading it to termination,
    then the connection will not be reusable.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.MockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    with httpcore2.HTTP11Connection(origin=origin, stream=stream) as conn:
        with conn.stream("GET", "https://example.com/") as response:
            assert response.status == 200

        assert not conn.is_idle()
        assert conn.is_closed()
        assert not conn.is_available()
        assert not conn.has_expired()
        assert repr(conn) == "<HTTP11Connection ['https://example.com:443', CLOSED, Request Count: 1]>"



def test_http11_connection_with_remote_protocol_error() -> None:
    """
    If a remote protocol error occurs, then no response will be returned,
    and the connection will not be reusable.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.MockStream([b"Wait, this isn't valid HTTP!", b""])
    with httpcore2.HTTP11Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(httpcore2.RemoteProtocolError):
            conn.request("GET", "https://example.com/")

        assert not conn.is_idle()
        assert conn.is_closed()
        assert not conn.is_available()
        assert not conn.has_expired()
        assert repr(conn) == "<HTTP11Connection ['https://example.com:443', CLOSED, Request Count: 1]>"



def test_http11_connection_with_incomplete_response() -> None:
    """
    We should be gracefully handling the case where the connection ends prematurely.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.MockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, wor",
        ]
    )
    with httpcore2.HTTP11Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(httpcore2.RemoteProtocolError):
            conn.request("GET", "https://example.com/")

        assert not conn.is_idle()
        assert conn.is_closed()
        assert not conn.is_available()
        assert not conn.has_expired()
        assert repr(conn) == "<HTTP11Connection ['https://example.com:443', CLOSED, Request Count: 1]>"



def test_http11_connection_with_local_protocol_error() -> None:
    """
    If a local protocol error occurs, then no response will be returned,
    and the connection will not be reusable.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.MockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    with httpcore2.HTTP11Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(httpcore2.LocalProtocolError) as exc_info:
            conn.request("GET", "https://example.com/", headers={"Host": "\0"})

        assert str(exc_info.value) == "Illegal header value b'\\x00'"

        assert not conn.is_idle()
        assert conn.is_closed()
        assert not conn.is_available()
        assert not conn.has_expired()
        assert repr(conn) == "<HTTP11Connection ['https://example.com:443', CLOSED, Request Count: 1]>"



def test_http11_connection_handles_one_active_request() -> None:
    """
    Attempting to send a request while one is already in-flight will raise
    a ConnectionNotAvailable exception.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.MockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    with httpcore2.HTTP11Connection(origin=origin, stream=stream) as conn:
        with conn.stream("GET", "https://example.com/"):
            with pytest.raises(httpcore2.ConnectionNotAvailable):
                conn.request("GET", "https://example.com/")



def test_http11_connection_attempt_close() -> None:
    """
    A connection can only be closed when it is idle.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.MockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    with httpcore2.HTTP11Connection(origin=origin, stream=stream) as conn:
        with conn.stream("GET", "https://example.com/") as response:
            response.read()
            assert response.status == 200
            assert response.content == b"Hello, world!"



def test_http11_request_to_incorrect_origin() -> None:
    """
    A connection can only send requests to whichever origin it is connected to.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.MockStream([])
    with httpcore2.HTTP11Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(RuntimeError):
            conn.request("GET", "https://other.com/")



def test_http11_expect_continue() -> None:
    """
    HTTP "100 Continue" is an interim response.
    We simply ignore it and return the final response.

    https://httpwg.org/specs/rfc9110.html#status.100
    https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/100
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.MockStream(
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
    with httpcore2.HTTP11Connection(origin=origin, stream=stream, keepalive_expiry=5.0) as conn:
        response = conn.request(
            "GET",
            "https://example.com/",
            headers={"Expect": "continue"},
        )
        assert response.status == 200
        assert response.content == b"Hello, world!"



def test_http11_upgrade_connection() -> None:
    """
    HTTP "101 Switching Protocols" indicates an upgraded connection.

    We should return the response, so that the network stream
    may be used for the upgraded connection.

    https://httpwg.org/specs/rfc9110.html#status.101
    https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/101
    """
    origin = httpcore2.Origin(b"wss", b"example.com", 443)
    stream = httpcore2.MockStream(
        [
            b"HTTP/1.1 101 Switching Protocols\r\n",
            b"Connection: upgrade\r\n",
            b"Upgrade: custom\r\n",
            b"\r\n",
            b"...",
        ]
    )
    with httpcore2.HTTP11Connection(origin=origin, stream=stream, keepalive_expiry=5.0) as conn:
        with conn.stream(
            "GET",
            "wss://example.com/",
            headers={"Connection": "upgrade", "Upgrade": "custom"},
        ) as response:
            assert response.status == 101
            network_stream = response.extensions["network_stream"]
            content = network_stream.read(max_bytes=1024)
            assert content == b"..."



def test_http11_upgrade_with_trailing_data() -> None:
    """
    HTTP "101 Switching Protocols" indicates an upgraded connection.

    In `CONNECT` and `Upgrade:` requests, we need to handover the trailing data
    in the h11.Connection object.

    https://h11.readthedocs.io/en/latest/api.html#switching-protocols
    """
    origin = httpcore2.Origin(b"wss", b"example.com", 443)
    stream = httpcore2.MockStream(
        # The first element of this mock network stream buffer simulates networking
        # in which response headers and data are received at once.
        # This means that "foobar" becomes trailing data.
        [
            (b"HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: custom\r\n\r\nfoobar"),
            b"baz",
        ]
    )
    with httpcore2.HTTP11Connection(origin=origin, stream=stream, keepalive_expiry=5.0) as conn:
        with conn.stream(
            "GET",
            "wss://example.com/",
            headers={"Connection": "upgrade", "Upgrade": "custom"},
        ) as response:
            assert response.status == 101
            network_stream = response.extensions["network_stream"]

            content = network_stream.read(max_bytes=3)
            assert content == b"foo"
            content = network_stream.read(max_bytes=3)
            assert content == b"bar"
            content = network_stream.read(max_bytes=3)
            assert content == b"baz"

            # Lazy tests for HTTP11UpgradeStream
            network_stream.write(b"spam")
            invalid = network_stream.get_extra_info("invalid")
            assert invalid is None
            network_stream.close()



def test_http11_early_hints() -> None:
    """
    HTTP "103 Early Hints" is an interim response.
    We simply ignore it and return the final response.

    https://datatracker.ietf.org/doc/rfc8297/
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.MockStream(
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
    with httpcore2.HTTP11Connection(origin=origin, stream=stream, keepalive_expiry=5.0) as conn:
        response = conn.request(
            "GET",
            "https://example.com/",
            headers={"Expect": "continue"},
        )
        assert response.status == 200
        assert response.content == b"<html>Hello, world! ...</html>"



def test_http11_header_sub_100kb() -> None:
    """
    A connection should be able to handle a http header size up to 100kB.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.MockStream(
        [
            b"HTTP/1.1 200 OK\r\n",  # 17
            b"Content-Type: plain/text\r\n",  # 43
            b"Cookie: " + b"x" * (100 * 1024 - 72) + b"\r\n",  # 102381
            b"Content-Length: 0\r\n",  # 102400
            b"\r\n",
            b"",
        ]
    )
    with httpcore2.HTTP11Connection(origin=origin, stream=stream, keepalive_expiry=5.0) as conn:
        response = conn.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b""
