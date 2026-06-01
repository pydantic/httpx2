from __future__ import annotations

import io
import sys
import typing
import zlib

import chardet
import pytest

import httpx2

if sys.version_info >= (3, 14):  # pragma: no cover
    from compression import zstd
else:  # pragma: no cover
    import zstandard as zstd


def test_deflate() -> None:
    """
    Deflate encoding may use either 'zlib' or 'deflate' in the wild.

    https://stackoverflow.com/questions/1838699/how-can-i-decompress-a-gzip-stream-with-zlib#answer-22311297
    """
    body = b"test 123"
    compressor = zlib.compressobj(9, zlib.DEFLATED, -zlib.MAX_WBITS)
    compressed_body = compressor.compress(body) + compressor.flush()

    headers = [(b"Content-Encoding", b"deflate")]
    response = httpx2.Response(
        200,
        headers=headers,
        content=compressed_body,
    )
    assert response.content == body


def test_zlib() -> None:
    """
    Deflate encoding may use either 'zlib' or 'deflate' in the wild.

    https://stackoverflow.com/questions/1838699/how-can-i-decompress-a-gzip-stream-with-zlib#answer-22311297
    """
    body = b"test 123"
    compressed_body = zlib.compress(body)

    headers = [(b"Content-Encoding", b"deflate")]
    response = httpx2.Response(
        200,
        headers=headers,
        content=compressed_body,
    )
    assert response.content == body


def test_gzip() -> None:
    body = b"test 123"
    compressor = zlib.compressobj(9, zlib.DEFLATED, zlib.MAX_WBITS | 16)
    compressed_body = compressor.compress(body) + compressor.flush()

    headers = [(b"Content-Encoding", b"gzip")]
    response = httpx2.Response(
        200,
        headers=headers,
        content=compressed_body,
    )
    assert response.content == body


def test_brotli() -> None:
    body = b"test 123"
    compressed_body = b"\x8b\x03\x80test 123\x03"

    headers = [(b"Content-Encoding", b"br")]
    response = httpx2.Response(
        200,
        headers=headers,
        content=compressed_body,
    )
    assert response.content == body


def test_zstd() -> None:
    body = b"test 123"
    compressed_body = zstd.compress(body)

    headers = [(b"Content-Encoding", b"zstd")]
    response = httpx2.Response(
        200,
        headers=headers,
        content=compressed_body,
    )
    assert response.content == body


def test_zstd_decoding_error() -> None:
    compressed_body = "this_is_not_zstd_compressed_data"

    headers = [(b"Content-Encoding", b"zstd")]
    with pytest.raises(httpx2.DecodingError):
        httpx2.Response(
            200,
            headers=headers,
            content=compressed_body,
        )


def test_zstd_empty() -> None:
    headers = [(b"Content-Encoding", b"zstd")]
    response = httpx2.Response(200, headers=headers, content=b"")
    assert response.content == b""


def test_zstd_truncated() -> None:
    body = b"test 123"
    compressed_body = zstd.compress(body)

    headers = [(b"Content-Encoding", b"zstd")]
    with pytest.raises(httpx2.DecodingError):
        httpx2.Response(
            200,
            headers=headers,
            content=compressed_body[1:3],
        )


def test_zstd_multiframe() -> None:
    # test inspired by urllib3 test suite
    data = (
        # Zstandard frame
        zstd.compress(b"foo")
        # skippable frame (must be ignored)
        + bytes.fromhex(
            "50 2A 4D 18"  # Magic_Number (little-endian)
            "07 00 00 00"  # Frame_Size (little-endian)
            "00 00 00 00 00 00 00"  # User_Data
        )
        # Zstandard frame
        + zstd.compress(b"bar")
    )
    compressed_body = io.BytesIO(data)

    headers = [(b"Content-Encoding", b"zstd")]
    response = httpx2.Response(200, headers=headers, content=compressed_body)
    response.read()
    assert response.content == b"foobar"


def test_zstd_streaming_multiple_frames() -> None:
    body1 = b"test 123 "
    body2 = b"another frame"

    # Create two separate complete frames
    frame1 = zstd.compress(body1)
    frame2 = zstd.compress(body2)

    # Create an iterator that yields frames separately
    def content_iterator() -> typing.Iterator[bytes]:
        yield frame1
        yield frame2

    headers = [(b"Content-Encoding", b"zstd")]
    response = httpx2.Response(200, headers=headers, content=content_iterator())
    response.read()

    assert response.content == body1 + body2


def test_zstd_empty_decode_after_eof() -> None:
    # An empty `decode(b"")` after a complete frame must not raise EOFError on
    # stdlib `compression.zstd`. This path is reached via `MultiDecoder.flush()`,
    # which feeds b"" through each child to drain residue across stacked encodings.
    body = b"test 123"
    compressed_body = zstd.compress(body)

    headers = [(b"Content-Encoding", b"zstd, identity")]
    response = httpx2.Response(200, headers=headers, content=compressed_body)
    assert response.content == body


def test_multi() -> None:
    body = b"test 123"

    deflate_compressor = zlib.compressobj(9, zlib.DEFLATED, -zlib.MAX_WBITS)
    compressed_body = deflate_compressor.compress(body) + deflate_compressor.flush()

    gzip_compressor = zlib.compressobj(9, zlib.DEFLATED, zlib.MAX_WBITS | 16)
    compressed_body = gzip_compressor.compress(compressed_body) + gzip_compressor.flush()

    headers = [(b"Content-Encoding", b"deflate, gzip")]
    response = httpx2.Response(
        200,
        headers=headers,
        content=compressed_body,
    )
    assert response.content == body


def test_multi_with_identity() -> None:
    body = b"test 123"
    compressed_body = b"\x8b\x03\x80test 123\x03"

    headers = [(b"Content-Encoding", b"br, identity")]
    response = httpx2.Response(
        200,
        headers=headers,
        content=compressed_body,
    )
    assert response.content == body

    headers = [(b"Content-Encoding", b"identity, br")]
    response = httpx2.Response(
        200,
        headers=headers,
        content=compressed_body,
    )
    assert response.content == body


@pytest.mark.anyio
async def test_streaming() -> None:
    body = b"test 123"
    compressor = zlib.compressobj(9, zlib.DEFLATED, zlib.MAX_WBITS | 16)

    async def compress(body: bytes) -> typing.AsyncIterator[bytes]:
        yield compressor.compress(body)
        yield compressor.flush()

    headers = [(b"Content-Encoding", b"gzip")]
    response = httpx2.Response(
        200,
        headers=headers,
        content=compress(body),
    )
    assert not hasattr(response, "body")
    assert await response.aread() == body


@pytest.mark.parametrize("header_value", (b"deflate", b"gzip", b"br", b"identity"))
def test_empty_content(header_value: bytes) -> None:
    headers = [(b"Content-Encoding", header_value)]
    response = httpx2.Response(
        200,
        headers=headers,
        content=b"",
    )
    assert response.content == b""


@pytest.mark.parametrize("header_value", (b"deflate", b"gzip", b"br", b"identity"))
def test_decoders_empty_cases(header_value: bytes) -> None:
    headers = [(b"Content-Encoding", header_value)]
    response = httpx2.Response(content=b"", status_code=200, headers=headers)
    assert response.read() == b""


@pytest.mark.parametrize("header_value", (b"deflate", b"gzip", b"br"))
def test_decoding_errors(header_value: bytes) -> None:
    headers = [(b"Content-Encoding", header_value)]
    compressed_body = b"invalid"
    with pytest.raises(httpx2.DecodingError):
        request = httpx2.Request("GET", "https://example.org")
        httpx2.Response(200, headers=headers, content=compressed_body, request=request)

    with pytest.raises(httpx2.DecodingError):
        httpx2.Response(200, headers=headers, content=compressed_body)


@pytest.mark.parametrize(
    ["data", "encoding"],
    [
        ((b"Hello,", b" world!"), "ascii"),
        ((b"\xe3\x83", b"\x88\xe3\x83\xa9", b"\xe3", b"\x83\x99\xe3\x83\xab"), "utf-8"),
        ((b"Euro character: \x88! abcdefghijklmnopqrstuvwxyz", b""), "cp1252"),
        ((b"Accented: \xd6sterreich abcdefghijklmnopqrstuvwxyz", b""), "iso-8859-1"),
    ],
)
@pytest.mark.anyio
async def test_text_decoder_with_autodetect(data: tuple[bytes, ...], encoding: str) -> None:
    async def iterator() -> typing.AsyncIterator[bytes]:
        nonlocal data
        for chunk in data:
            yield chunk

    def autodetect(content: bytes) -> str | None:
        return chardet.detect(content).get("encoding")

    # Accessing `.text` on a read response.
    response = httpx2.Response(200, content=iterator(), default_encoding=autodetect)
    await response.aread()
    assert response.text == (b"".join(data)).decode(encoding)

    # Streaming `.aiter_text` iteratively.
    # Note that if we streamed the text *without* having read it first, then
    # we won't get a `charset_normalizer` guess, and will instead always rely
    # on utf-8 if no charset is specified.
    text = "".join([part async for part in response.aiter_text()])
    assert text == (b"".join(data)).decode(encoding)


@pytest.mark.anyio
async def test_text_decoder_known_encoding() -> None:
    async def iterator() -> typing.AsyncIterator[bytes]:
        yield b"\x83g"
        yield b"\x83"
        yield b"\x89\x83x\x83\x8b"

    response = httpx2.Response(
        200,
        headers=[(b"Content-Type", b"text/html; charset=shift-jis")],
        content=iterator(),
    )

    await response.aread()
    assert "".join(response.text) == "トラベル"


def test_text_decoder_empty_cases() -> None:
    response = httpx2.Response(200, content=b"")
    assert response.text == ""

    response = httpx2.Response(200, content=[b""])
    response.read()
    assert response.text == ""


@pytest.mark.parametrize(
    ["data", "expected"],
    [((b"Hello,", b" world!"), ["Hello,", " world!"])],
)
def test_streaming_text_decoder(data: typing.Iterable[bytes], expected: list[str]) -> None:
    response = httpx2.Response(200, content=iter(data))
    assert list(response.iter_text()) == expected


def test_line_decoder_nl() -> None:
    response = httpx2.Response(200, content=[b""])
    assert list(response.iter_lines()) == []

    response = httpx2.Response(200, content=[b"", b"a\n\nb\nc"])
    assert list(response.iter_lines()) == ["a", "", "b", "c"]

    # Issue #1033
    response = httpx2.Response(200, content=[b"", b"12345\n", b"foo ", b"bar ", b"baz\n"])
    assert list(response.iter_lines()) == ["12345", "foo bar baz"]


def test_line_decoder_cr() -> None:
    response = httpx2.Response(200, content=[b"", b"a\r\rb\rc"])
    assert list(response.iter_lines()) == ["a", "", "b", "c"]

    response = httpx2.Response(200, content=[b"", b"a\r\rb\rc\r"])
    assert list(response.iter_lines()) == ["a", "", "b", "c"]

    # Issue #1033
    response = httpx2.Response(200, content=[b"", b"12345\r", b"foo ", b"bar ", b"baz\r"])
    assert list(response.iter_lines()) == ["12345", "foo bar baz"]


def test_line_decoder_crnl() -> None:
    response = httpx2.Response(200, content=[b"", b"a\r\n\r\nb\r\nc"])
    assert list(response.iter_lines()) == ["a", "", "b", "c"]

    response = httpx2.Response(200, content=[b"", b"a\r\n\r\nb\r\nc\r\n"])
    assert list(response.iter_lines()) == ["a", "", "b", "c"]

    response = httpx2.Response(200, content=[b"", b"a\r", b"\n\r\nb\r\nc"])
    assert list(response.iter_lines()) == ["a", "", "b", "c"]

    # Issue #1033
    response = httpx2.Response(200, content=[b"", b"12345\r\n", b"foo bar baz\r\n"])
    assert list(response.iter_lines()) == ["12345", "foo bar baz"]


def test_invalid_content_encoding_header() -> None:
    headers = [(b"Content-Encoding", b"invalid-header")]
    body = b"test 123"

    response = httpx2.Response(
        200,
        headers=headers,
        content=body,
    )
    assert response.content == body
