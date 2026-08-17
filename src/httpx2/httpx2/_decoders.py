"""
Handlers for Content-Encoding.

See: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Encoding
"""

from __future__ import annotations

import codecs
import functools
import io
import itertools
import sys
import typing
import zlib

from ._exceptions import DecodingError

# Brotli support is optional
try:
    # The C bindings in `brotli` are recommended for CPython.
    import brotli
except ImportError:  # pragma: no cover
    try:
        # The CFFI bindings in `brotlicffi` are recommended for PyPy
        # and other environments.
        import brotlicffi as brotli
    except ImportError:
        brotli = None


# Zstandard support is optional on Python <= 3.13.
# On Python 3.14+, the stdlib includes an optional built-in zstd implementation.
if typing.TYPE_CHECKING:
    # We keep checking Python version in the type checker path because try..except doesn't help type checkers.
    if sys.version_info >= (3, 14):
        from compression.zstd import ZstdDecompressor, ZstdError
    else:
        from zstandard import ZstdDecompressor as _ZstdDecompressor, ZstdError

        ZstdDecompressor = functools.partial(_ZstdDecompressor().decompressobj)

    _zstandard_installed: bool = False
    # True only when the stdlib `compression.zstd` backend is in use (bounded, incremental decode).
    _zstd_stdlib_backend: bool = False
else:  # pragma: no cover
    _zstandard_installed = False
    _zstd_stdlib_backend = False
    try:
        from compression.zstd import ZstdDecompressor, ZstdError

        _zstandard_installed = True
        _zstd_stdlib_backend = True
    # Either Python <3.14 or the distro doesn't have `compression.zstd`.
    except ImportError:
        try:
            from zstandard import ZstdDecompressor as _ZstdDecompressor, ZstdError

            ZstdDecompressor = functools.partial(_ZstdDecompressor().decompressobj)
            _zstandard_installed = True
        except ImportError:
            pass


MAX_DECODE_CHUNK_SIZE = 2**20  # 1 MiB


class Decompressor(typing.Protocol):
    @property
    def unconsumed_tail(self) -> bytes: ...

    def decompress(self, data: bytes, max_length: int) -> bytes: ...

    def flush(self) -> bytes: ...


class ZlibDecompressor:
    """
    Drain a `zlib`/`gzip` decompressor in bounded pieces so a small compressed
    input cannot inflate to an unbounded buffer in a single call.
    """

    def __init__(self, decompressor: Decompressor) -> None:
        self.decompressor = decompressor

    def decompress(self, data: bytes) -> typing.Iterator[bytes]:
        decompressed = self.decompressor.decompress(data, MAX_DECODE_CHUNK_SIZE)
        while decompressed:
            yield decompressed
            decompressed = self.decompressor.decompress(self.decompressor.unconsumed_tail, MAX_DECODE_CHUNK_SIZE)

    def flush(self) -> bytes:
        return self.decompressor.flush()


class ContentDecoder:
    def decode(self, data: bytes) -> typing.Iterator[bytes]:
        raise NotImplementedError()  # pragma: no cover

    def flush(self) -> typing.Iterator[bytes]:
        raise NotImplementedError()  # pragma: no cover


class IdentityDecoder(ContentDecoder):
    """
    Handle unencoded data.
    """

    def decode(self, data: bytes) -> typing.Iterator[bytes]:
        yield data

    def flush(self) -> typing.Iterator[bytes]:
        yield from ()


class DeflateDecoder(ContentDecoder):
    """
    Handle 'deflate' decoding.

    See: https://stackoverflow.com/questions/1838699
    """

    def __init__(self) -> None:
        self.first_attempt = True
        self.decompressor = ZlibDecompressor(zlib.decompressobj())

    def decode(self, data: bytes) -> typing.Iterator[bytes]:
        was_first_attempt = self.first_attempt
        self.first_attempt = False
        try:
            yield from self.decompressor.decompress(data)
        except zlib.error as exc:
            if was_first_attempt:
                self.decompressor = ZlibDecompressor(zlib.decompressobj(-zlib.MAX_WBITS))
                yield from self.decode(data)
            else:
                raise DecodingError(str(exc)) from exc

    def flush(self) -> typing.Iterator[bytes]:
        try:
            yield self.decompressor.flush()
        except zlib.error as exc:  # pragma: no cover
            raise DecodingError(str(exc)) from exc


class GZipDecoder(ContentDecoder):
    """
    Handle 'gzip' decoding.

    See: https://stackoverflow.com/questions/1838699
    """

    def __init__(self) -> None:
        self.decompressor = ZlibDecompressor(zlib.decompressobj(zlib.MAX_WBITS | 16))

    def decode(self, data: bytes) -> typing.Iterator[bytes]:
        try:
            yield from self.decompressor.decompress(data)
        except zlib.error as exc:
            raise DecodingError(str(exc)) from exc

    def flush(self) -> typing.Iterator[bytes]:
        try:
            yield self.decompressor.flush()
        except zlib.error as exc:  # pragma: no cover
            raise DecodingError(str(exc)) from exc


class BrotliDecoder(ContentDecoder):
    """
    Handle 'brotli' decoding.

    Requires `pip install brotlipy`. See: https://brotlipy.readthedocs.io/
        or   `pip install brotli`. See https://github.com/google/brotli
    Supports both 'brotlipy' and 'Brotli' packages since they share an import
    name. The top branches are for 'brotlipy' and bottom branches for 'Brotli'
    """

    def __init__(self) -> None:
        if brotli is None:  # pragma: no cover
            raise ImportError(
                "Using 'BrotliDecoder', but neither of the 'brotlicffi' or 'brotli' "
                "packages have been installed. "
                "Make sure to install httpx using `pip install httpx[brotli]`."
            ) from None

        self.decompressor = brotli.Decompressor()
        self.seen_data = False
        self._decompress: typing.Callable[..., bytes]
        if hasattr(self.decompressor, "decompress"):
            # The 'brotlicffi' package.
            self._decompress = self.decompressor.decompress  # pragma: no cover
        else:
            # The 'brotli' package.
            self._decompress = self.decompressor.process

    def decode(self, data: bytes) -> typing.Iterator[bytes]:
        if not data:
            return
        self.seen_data = True
        try:
            decompressed = self._decompress(data, output_buffer_limit=MAX_DECODE_CHUNK_SIZE)
            while decompressed:
                yield decompressed
                decompressed = self._decompress(b"", output_buffer_limit=MAX_DECODE_CHUNK_SIZE)
        except TypeError:  # pragma: no cover
            # Backend without `output_buffer_limit` (e.g. brotlicffi < 1.2.0); fall back to unbounded.
            yield self._decompress(data)
        except brotli.error as exc:
            raise DecodingError(str(exc)) from exc

    def flush(self) -> typing.Iterator[bytes]:
        if not self.seen_data:
            return
        try:
            if hasattr(self.decompressor, "finish"):
                # Only available in the 'brotlicffi' package.

                # As the decompressor decompresses eagerly, this
                # will never actually emit any data. However, it will potentially throw
                # errors if a truncated or damaged data stream has been used.
                self.decompressor.finish()  # pragma: no cover
        except brotli.error as exc:  # pragma: no cover
            raise DecodingError(str(exc)) from exc
        yield from ()


class ZStandardDecoder(ContentDecoder):
    """Handle 'zstd' RFC 8878 decoding.

    If running on Python 3.14+ or a distro that doesn't have the `compression.zstd` stdlib module, requires either:
    `pip install zstandard` or `pip install httpx2[zstd]`.
    """

    # inspired by the ZstdDecoder implementation in urllib3
    def __init__(self) -> None:
        if not _zstandard_installed:  # pragma: no cover
            raise ImportError(
                "Using 'ZStandardDecoder', ...Make sure to install httpx using `pip install httpx[zstd]`."
            ) from None

        self.decompressor = ZstdDecompressor()
        self.seen_data = False

    def decode(self, data: bytes) -> typing.Iterator[bytes]:
        if not data:
            return
        self.seen_data = True
        try:
            if self.decompressor.eof:
                data = self.decompressor.unused_data + data
                self.decompressor = ZstdDecompressor()
            while True:
                yield from self._decompress_frame(data)
                if not (self.decompressor.eof and self.decompressor.unused_data):
                    break
                data = self.decompressor.unused_data
                self.decompressor = ZstdDecompressor()
        except ZstdError as exc:
            raise DecodingError(str(exc)) from exc

    def _decompress_frame(self, data: bytes) -> typing.Iterator[bytes]:
        # The `sys.version_info` guard is what lets the type checker pick the right backend type;
        # `_zstd_stdlib_backend` additionally handles a 3.14 install where `compression.zstd` is
        # absent and the third-party `zstandard` backend is used instead.
        if sys.version_info >= (3, 14) and _zstd_stdlib_backend:  # pragma: no cover
            # The stdlib `compression.zstd` decompressor bounds a single call's output.
            decompressed = self.decompressor.decompress(data, MAX_DECODE_CHUNK_SIZE)
            while decompressed:
                yield decompressed
                if self.decompressor.needs_input or self.decompressor.eof:
                    break
                decompressed = self.decompressor.decompress(b"", MAX_DECODE_CHUNK_SIZE)
        else:  # pragma: no cover
            # `zstandard`'s incremental (decompressobj) API has no per-call output bound, so a
            # frame is decompressed in one shot rather than in bounded pieces.
            yield self.decompressor.decompress(data)

    def flush(self) -> typing.Iterator[bytes]:
        if not self.seen_data:
            return
        if not self.decompressor.eof:
            raise DecodingError("Zstandard data is incomplete")  # pragma: no cover
        yield from ()


class MultiDecoder(ContentDecoder):
    """
    Handle the case where multiple encodings have been applied.
    """

    max_decode_links: typing.ClassVar[int] = 5

    def __init__(self, encodings: typing.Sequence[str]) -> None:
        """
        'encodings' should be the content codings in the order in which
        each was applied.
        """
        codings = [encoding for encoding in encodings if encoding in SUPPORTED_DECODERS]
        if len(codings) > self.max_decode_links:
            raise DecodingError(f"Cannot apply more than {self.max_decode_links} content encodings.")
        # Note that we reverse the order for decoding.
        self.children: list[ContentDecoder] = [SUPPORTED_DECODERS[coding]() for coding in reversed(codings)]

    def decode(self, data: bytes) -> typing.Iterator[bytes]:
        streams: typing.Iterator[bytes] = iter((data,))
        for child in self.children:
            streams = self._pipe(child.decode, streams)
        yield from streams

    def flush(self) -> typing.Iterator[bytes]:
        streams: typing.Iterator[bytes] = iter(())
        for child in self.children:
            streams = itertools.chain(self._pipe(child.decode, streams), child.flush())
        yield from streams

    @staticmethod
    def _pipe(
        decode: typing.Callable[[bytes], typing.Iterator[bytes]],
        upstream: typing.Iterator[bytes],
    ) -> typing.Iterator[bytes]:
        for chunk in upstream:
            yield from decode(chunk)


class ByteChunker:
    """
    Handles returning byte content in fixed-size chunks.
    """

    def __init__(self, chunk_size: int | None = None) -> None:
        self._buffer = io.BytesIO()
        self._chunk_size = chunk_size

    def decode(self, content: bytes) -> list[bytes]:
        if self._chunk_size is None:
            return [content] if content else []

        self._buffer.write(content)
        if self._buffer.tell() >= self._chunk_size:
            value = self._buffer.getvalue()
            chunks = [value[i : i + self._chunk_size] for i in range(0, len(value), self._chunk_size)]
            if len(chunks[-1]) == self._chunk_size:
                self._buffer.seek(0)
                self._buffer.truncate()
                return chunks
            else:
                self._buffer.seek(0)
                self._buffer.write(chunks[-1])
                self._buffer.truncate()
                return chunks[:-1]
        else:
            return []

    def flush(self) -> list[bytes]:
        value = self._buffer.getvalue()
        self._buffer.seek(0)
        self._buffer.truncate()
        return [value] if value else []


class TextChunker:
    """
    Handles returning text content in fixed-size chunks.
    """

    def __init__(self, chunk_size: int | None = None) -> None:
        self._buffer = io.StringIO()
        self._chunk_size = chunk_size

    def decode(self, content: str) -> list[str]:
        if self._chunk_size is None:
            return [content] if content else []

        self._buffer.write(content)
        if self._buffer.tell() >= self._chunk_size:
            value = self._buffer.getvalue()
            chunks = [value[i : i + self._chunk_size] for i in range(0, len(value), self._chunk_size)]
            if len(chunks[-1]) == self._chunk_size:
                self._buffer.seek(0)
                self._buffer.truncate()
                return chunks
            else:
                self._buffer.seek(0)
                self._buffer.write(chunks[-1])
                self._buffer.truncate()
                return chunks[:-1]
        else:
            return []

    def flush(self) -> list[str]:
        value = self._buffer.getvalue()
        self._buffer.seek(0)
        self._buffer.truncate()
        return [value] if value else []


class TextDecoder:
    """
    Handles incrementally decoding bytes into text
    """

    def __init__(self, encoding: str = "utf-8") -> None:
        self.decoder = codecs.getincrementaldecoder(encoding)(errors="replace")

    def decode(self, data: bytes) -> str:
        return self.decoder.decode(data)

    def flush(self) -> str:
        return self.decoder.decode(b"", True)


class LineDecoder:
    """
    Handles incrementally reading lines from text.

    Has the same behaviour as the stdllib splitlines,
    but handling the input iteratively.
    """

    def __init__(self) -> None:
        self.buffer: list[str] = []
        self.trailing_cr: bool = False

    def decode(self, text: str) -> list[str]:
        # See https://docs.python.org/3/library/stdtypes.html#str.splitlines
        NEWLINE_CHARS = "\n\r\x0b\x0c\x1c\x1d\x1e\x85\u2028\u2029"

        # We always push a trailing `\r` into the next decode iteration.
        if self.trailing_cr:
            text = "\r" + text
            self.trailing_cr = False
        if text.endswith("\r"):
            self.trailing_cr = True
            text = text[:-1]

        if not text:
            # NOTE: the edge case input of empty text doesn't occur in practice,
            # because other httpx internals filter out this value
            return []  # pragma: no cover

        trailing_newline = text[-1] in NEWLINE_CHARS
        lines = text.splitlines()

        if len(lines) == 1 and not trailing_newline:
            # No new lines, buffer the input and continue.
            self.buffer.append(lines[0])
            return []

        if self.buffer:
            # Include any existing buffer in the first portion of the
            # splitlines result.
            lines = ["".join(self.buffer) + lines[0]] + lines[1:]
            self.buffer = []

        if not trailing_newline:
            # If the last segment of splitlines is not newline terminated,
            # then drop it from our output and start a new buffer.
            self.buffer = [lines.pop()]

        return lines

    def flush(self) -> list[str]:
        if not self.buffer and not self.trailing_cr:
            return []

        lines = ["".join(self.buffer)]
        self.buffer = []
        self.trailing_cr = False
        return lines


SUPPORTED_DECODERS: dict[str, type[ContentDecoder]] = {
    "identity": IdentityDecoder,
    "gzip": GZipDecoder,
    "deflate": DeflateDecoder,
    "br": BrotliDecoder,
    "zstd": ZStandardDecoder,
}


if brotli is None:
    SUPPORTED_DECODERS.pop("br")  # pragma: no cover
if not _zstandard_installed:
    SUPPORTED_DECODERS.pop("zstd")  # pragma: no cover
