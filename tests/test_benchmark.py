from __future__ import annotations

import gzip
import io
import json
import socket
import threading

import pytest

import httpx2
from httpcore2._backends.sync import SyncStream

pytestmark = pytest.mark.benchmark

TYPICAL_URL = "https://www.example.org:8443/path/to/resource?key=value&other=1#frag"

HEADERS: list[tuple[str, str]] = [
    ("host", "example.org"),
    ("user-agent", "httpx2-bench/1.0"),
    ("accept", "*/*"),
    ("accept-encoding", "gzip, deflate, br"),
    *[(f"x-custom-{i}", f"value-{i}") for i in range(16)],
]

SMALL_JSON: dict[str, object] = {
    "id": 12345,
    "items": [{"sku": f"SKU-{i}", "qty": i, "price": i * 1.5} for i in range(50)],
}
LARGE_JSON: dict[str, object] = {
    "records": [
        {"id": i, "name": f"record-{i}", "tags": [f"t{j}" for j in range(8)], "active": bool(i % 2)}
        for i in range(2048)
    ],
}
SMALL_JSON_BODY = json.dumps(SMALL_JSON).encode()
LARGE_JSON_BODY = json.dumps(LARGE_JSON).encode()
GZIPPED_LARGE_JSON_BODY = gzip.compress(LARGE_JSON_BODY)


def test_bench_url_join() -> None:
    base = httpx2.URL(TYPICAL_URL)
    for _ in range(1024):
        base.join("/path/to/resource?key=value")


def test_bench_request_json_post() -> None:
    for _ in range(256):
        httpx2.Request("POST", TYPICAL_URL, headers=HEADERS, json=SMALL_JSON)


def test_bench_request_multipart() -> None:
    for _ in range(64):
        request = httpx2.Request(
            "POST",
            "https://example.org/upload",
            data={"name": "value", "other": "field", "description": "a longer text field"},
            files={
                "small": ("hello.txt", b"x" * 4096, "text/plain"),
                "large": ("payload.bin", io.BytesIO(b"y" * 65536), "application/octet-stream"),
            },
        )
        request.read()


def test_bench_response_gzip_decode_large() -> None:
    for _ in range(64):
        response = httpx2.Response(
            200,
            headers=[("content-type", "application/json"), ("content-encoding", "gzip")],
            content=GZIPPED_LARGE_JSON_BODY,
        )
        response.read()


def _large_json_handler(request: httpx2.Request) -> httpx2.Response:
    return httpx2.Response(200, content=LARGE_JSON_BODY, headers=[("content-type", "application/json")])


def _stream_handler(request: httpx2.Request) -> httpx2.Response:
    return httpx2.Response(200, content=b"x" * 1024 * 1024)


def test_bench_client_post_large_json() -> None:
    with httpx2.Client(transport=httpx2.MockTransport(_large_json_handler)) as client:
        for _ in range(16):
            client.post(TYPICAL_URL, json=LARGE_JSON).json()


def test_bench_client_stream_download() -> None:
    with httpx2.Client(transport=httpx2.MockTransport(_stream_handler)) as client:
        for _ in range(16):
            with client.stream("GET", TYPICAL_URL) as response:
                for _ in response.iter_bytes(chunk_size=8192):
                    pass


def test_bench_sync_stream_write_large() -> None:
    payload = b"x" * 64 * 1024 * 1024  # 64 MB
    reader_sock, writer_sock = socket.socketpair()
    try:
        # Small kernel buffers + small reader chunks force many partial sends on Linux,
        # which is what exercises the buffer-slicing loop inside SyncStream.write.
        writer_sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8192)
        reader_sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8192)

        drained: list[int] = []

        def drain() -> None:
            total = 0
            while True:
                chunk = reader_sock.recv(8192)
                if not chunk:
                    break
                total += len(chunk)
            drained.append(total)

        thread = threading.Thread(target=drain)
        thread.start()

        stream = SyncStream(writer_sock)
        stream.write(payload)
        stream.close()
        thread.join()

        assert drained == [len(payload)]
    finally:
        reader_sock.close()
