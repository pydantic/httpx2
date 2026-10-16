import anyio
import pytest

import httpcore2
from httpcore2._backends.anyio import AnyIOStream


class EndedByteStream(anyio.abc.ByteStream):
    """
    A byte stream whose peer has ended the connection,
    like `anyio.streams.tls.TLSStream` without a TLS `close_notify`.
    """

    async def receive(self, max_bytes: int = 65536) -> bytes:  # pragma: no cover
        raise anyio.EndOfStream

    async def send(self, item: bytes) -> None:
        raise anyio.EndOfStream

    async def send_eof(self) -> None:  # pragma: no cover
        pass

    async def aclose(self) -> None:  # pragma: no cover
        pass


@pytest.mark.anyio
async def test_anyio_write_end_of_stream() -> None:
    stream = AnyIOStream(EndedByteStream())
    with pytest.raises(httpcore2.WriteError):
        await stream.write(b"GET / HTTP/1.1\r\n\r\n")
