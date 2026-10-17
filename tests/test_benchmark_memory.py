from __future__ import annotations

from typing import TYPE_CHECKING

import hpack
import hyperframe.frame
import pytest

import httpcore2

if TYPE_CHECKING:
    from pytest_codspeed import BenchmarkFixture

pytestmark = pytest.mark.benchmark

H2_WINDOW = 2**31 - 1
H2_BODY = b"x" * (8 * 1024 * 1024)


def _h2_server_frames() -> list[bytes]:
    return [
        hyperframe.frame.SettingsFrame(
            settings={hyperframe.frame.SettingsFrame.INITIAL_WINDOW_SIZE: H2_WINDOW}
        ).serialize(),
        hyperframe.frame.WindowUpdateFrame(stream_id=0, window_increment=H2_WINDOW - 65535).serialize(),
        hyperframe.frame.HeadersFrame(
            stream_id=1,
            data=hpack.Encoder().encode([(b":status", b"200")]),
            flags=["END_HEADERS"],
        ).serialize(),
        hyperframe.frame.DataFrame(stream_id=1, data=b"", flags=["END_STREAM"]).serialize(),
    ]


def test_bench_http2_send_large_body(benchmark: BenchmarkFixture) -> None:
    origin = httpcore2.Origin(b"https", b"example.com", 443)

    def send() -> int:
        stream = httpcore2.MockStream(_h2_server_frames())
        with httpcore2.HTTP2Connection(origin=origin, stream=stream) as conn:
            response = conn.request("POST", "https://example.com/", content=H2_BODY)
            return response.status

    assert benchmark(send) == 200
