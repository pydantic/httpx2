import hpack
import hyperframe.frame
import pytest

import httpcore2


@pytest.mark.anyio
async def test_http2_connection() -> None:
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
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
            hyperframe.frame.DataFrame(stream_id=1, data=b"Hello, world!", flags=["END_STREAM"]).serialize(),
        ]
    )
    async with httpcore2.AsyncHTTP2Connection(origin=origin, stream=stream, keepalive_expiry=5.0) as conn:
        response = await conn.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"

        assert conn.is_idle()
        assert conn.is_available()
        assert not conn.is_closed()
        assert not conn.has_expired()
        assert conn.info() == "'https://example.com:443', HTTP/2, IDLE, Request Count: 1"
        assert repr(conn) == "<AsyncHTTP2Connection ['https://example.com:443', IDLE, Request Count: 1]>"


@pytest.mark.anyio
async def test_http2_connection_closed() -> None:
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
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
            hyperframe.frame.DataFrame(stream_id=1, data=b"Hello, world!", flags=["END_STREAM"]).serialize(),
            # Connection is closed after the first response
            hyperframe.frame.GoAwayFrame(stream_id=0, error_code=0, last_stream_id=1).serialize(),
        ]
    )
    async with httpcore2.AsyncHTTP2Connection(origin=origin, stream=stream, keepalive_expiry=5.0) as conn:
        await conn.request("GET", "https://example.com/")

        with pytest.raises(httpcore2.ConnectionNotAvailable):
            await conn.request("GET", "https://example.com/")

        assert not conn.is_available()


@pytest.mark.anyio
async def test_http2_connection_post_request() -> None:
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
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
            hyperframe.frame.DataFrame(stream_id=1, data=b"Hello, world!", flags=["END_STREAM"]).serialize(),
        ]
    )
    async with httpcore2.AsyncHTTP2Connection(origin=origin, stream=stream) as conn:
        response = await conn.request(
            "POST",
            "https://example.com/",
            headers={b"content-length": b"17"},
            content=b'{"data": "upload"}',
        )
        assert response.status == 200
        assert response.content == b"Hello, world!"


@pytest.mark.anyio
async def test_http2_connection_with_remote_protocol_error() -> None:
    """
    If a remote protocol error occurs, then no response will be returned,
    and the connection will not be reusable.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream([b"Wait, this isn't valid HTTP!", b""])
    async with httpcore2.AsyncHTTP2Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(httpcore2.RemoteProtocolError):
            await conn.request("GET", "https://example.com/")


@pytest.mark.anyio
async def test_http2_connection_with_rst_stream() -> None:
    """
    If a stream reset occurs, then no response will be returned,
    but the connection will remain reusable for other requests.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
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
            # Stream is closed midway through the first response...
            hyperframe.frame.RstStreamFrame(stream_id=1, error_code=8).serialize(),
            # ...Which doesn't prevent the second response.
            hyperframe.frame.HeadersFrame(
                stream_id=3,
                data=hpack.Encoder().encode(
                    [
                        (b":status", b"200"),
                        (b"content-type", b"plain/text"),
                    ]
                ),
                flags=["END_HEADERS"],
            ).serialize(),
            hyperframe.frame.DataFrame(stream_id=3, data=b"Hello, world!", flags=["END_STREAM"]).serialize(),
            b"",
        ]
    )
    async with httpcore2.AsyncHTTP2Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(httpcore2.RemoteProtocolError):
            await conn.request("GET", "https://example.com/")
        response = await conn.request("GET", "https://example.com/")
        assert response.status == 200


@pytest.mark.anyio
async def test_http2_connection_with_goaway() -> None:
    """
    If a GoAway frame occurs, then no response will be returned,
    and the connection will not be reusable for other requests.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
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
            # Connection is closed midway through the first response...
            hyperframe.frame.GoAwayFrame(stream_id=0, error_code=0).serialize(),
            # ...We'll never get to this second response.
            hyperframe.frame.HeadersFrame(
                stream_id=3,
                data=hpack.Encoder().encode(
                    [
                        (b":status", b"200"),
                        (b"content-type", b"plain/text"),
                    ]
                ),
                flags=["END_HEADERS"],
            ).serialize(),
            hyperframe.frame.DataFrame(stream_id=3, data=b"Hello, world!", flags=["END_STREAM"]).serialize(),
            b"",
        ]
    )
    async with httpcore2.AsyncHTTP2Connection(origin=origin, stream=stream) as conn:
        # The initial request has been closed midway, with an unrecoverable error.
        with pytest.raises(httpcore2.RemoteProtocolError):
            await conn.request("GET", "https://example.com/")

        # The second request can receive a graceful `ConnectionNotAvailable`,
        # and may be retried on a new connection.
        with pytest.raises(httpcore2.ConnectionNotAvailable):
            await conn.request("GET", "https://example.com/")


@pytest.mark.anyio
async def test_http2_connection_with_negative_flow_control_window() -> None:
    """A negative stream flow-control window must be awaited, not sent into.

    After the 65535-byte window is exhausted, the server reduces INITIAL_WINDOW_SIZE
    by 32767, which adjusts the just-exhausted stream window from 0 to -32767.
    `_wait_for_outgoing_flow` must park the stream until WINDOW_UPDATE restores
    positive credit; otherwise `h2` raises `LocalProtocolError` on the next send_data.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    reduce_settings = hyperframe.frame.SettingsFrame(stream_id=0)
    reduce_settings.settings = {hyperframe.frame.SettingsFrame.INITIAL_WINDOW_SIZE: 32768}
    stream = httpcore2.AsyncMockStream(
        [
            hyperframe.frame.SettingsFrame(stream_id=0).serialize(),
            # This frame reduces INITIAL_WINDOW_SIZE to 32768, which adjusts the just-exhausted stream window to -32767.
            reduce_settings.serialize(),
            hyperframe.frame.WindowUpdateFrame(stream_id=0, window_increment=100_000).serialize(),
            hyperframe.frame.WindowUpdateFrame(stream_id=1, window_increment=100_000).serialize(),
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
            hyperframe.frame.DataFrame(stream_id=1, data=b"response", flags=["END_STREAM"]).serialize(),
        ]
    )
    async with httpcore2.AsyncHTTP2Connection(origin=origin, stream=stream) as conn:
        response = await conn.request("POST", "https://example.com/", content=b"x" * 100_000)
        assert response.status == 200
        assert response.content == b"response"


@pytest.mark.anyio
async def test_http2_connection_with_flow_control() -> None:
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
            hyperframe.frame.SettingsFrame().serialize(),
            # Available flow: 65,535
            hyperframe.frame.WindowUpdateFrame(stream_id=0, window_increment=10_000).serialize(),
            hyperframe.frame.WindowUpdateFrame(stream_id=1, window_increment=10_000).serialize(),
            # Available flow: 75,535
            hyperframe.frame.WindowUpdateFrame(stream_id=0, window_increment=10_000).serialize(),
            hyperframe.frame.WindowUpdateFrame(stream_id=1, window_increment=10_000).serialize(),
            # Available flow: 85,535
            hyperframe.frame.WindowUpdateFrame(stream_id=0, window_increment=10_000).serialize(),
            hyperframe.frame.WindowUpdateFrame(stream_id=1, window_increment=10_000).serialize(),
            # Available flow: 95,535
            hyperframe.frame.WindowUpdateFrame(stream_id=0, window_increment=10_000).serialize(),
            hyperframe.frame.WindowUpdateFrame(stream_id=1, window_increment=10_000).serialize(),
            # Available flow: 105,535
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
            hyperframe.frame.DataFrame(stream_id=1, data=b"100,000 bytes received", flags=["END_STREAM"]).serialize(),
        ]
    )
    async with httpcore2.AsyncHTTP2Connection(origin=origin, stream=stream) as conn:
        response = await conn.request(
            "POST",
            "https://example.com/",
            content=b"x" * 100_000,
        )
        assert response.status == 200
        assert response.content == b"100,000 bytes received"


@pytest.mark.anyio
async def test_http2_connection_attempt_close() -> None:
    """
    A connection can only be closed when it is idle.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
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
            hyperframe.frame.DataFrame(stream_id=1, data=b"Hello, world!", flags=["END_STREAM"]).serialize(),
        ]
    )
    async with httpcore2.AsyncHTTP2Connection(origin=origin, stream=stream) as conn:
        async with conn.stream("GET", "https://example.com/") as response:
            await response.aread()
            assert response.status == 200
            assert response.content == b"Hello, world!"

        await conn.aclose()
        with pytest.raises(httpcore2.ConnectionNotAvailable):
            await conn.request("GET", "https://example.com/")


@pytest.mark.anyio
async def test_http2_request_to_incorrect_origin() -> None:
    """
    A connection can only send requests to whichever origin it is connected to.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream([])
    async with httpcore2.AsyncHTTP2Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(RuntimeError):
            await conn.request("GET", "https://other.com/")


@pytest.mark.anyio
async def test_http2_remote_max_streams_update() -> None:
    """
    If the remote server updates the maximum concurrent streams value, we should
    be adjusting how many streams we will allow.
    """
    origin = httpcore2.Origin(b"https", b"example.com", 443)
    stream = httpcore2.AsyncMockStream(
        [
            hyperframe.frame.SettingsFrame(
                settings={hyperframe.frame.SettingsFrame.MAX_CONCURRENT_STREAMS: 1000}
            ).serialize(),
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
            hyperframe.frame.DataFrame(stream_id=1, data=b"Hello, world!").serialize(),
            hyperframe.frame.SettingsFrame(
                settings={hyperframe.frame.SettingsFrame.MAX_CONCURRENT_STREAMS: 50}
            ).serialize(),
            hyperframe.frame.DataFrame(stream_id=1, data=b"Hello, world...again!", flags=["END_STREAM"]).serialize(),
        ]
    )
    async with httpcore2.AsyncHTTP2Connection(origin=origin, stream=stream) as conn:
        async with conn.stream("GET", "https://example.com/") as response:
            i = 0
            async for chunk in response.aiter_stream():
                if i == 0:
                    assert chunk == b"Hello, world!"
                    assert conn._h2_state.remote_settings.max_concurrent_streams == 1000
                    assert conn._max_streams == min(
                        conn._h2_state.remote_settings.max_concurrent_streams,
                        conn._h2_state.local_settings.max_concurrent_streams,
                    )
                elif i == 1:
                    assert chunk == b"Hello, world...again!"
                    assert conn._h2_state.remote_settings.max_concurrent_streams == 50
                    assert conn._max_streams == min(
                        conn._h2_state.remote_settings.max_concurrent_streams,
                        conn._h2_state.local_settings.max_concurrent_streams,
                    )
                i += 1
