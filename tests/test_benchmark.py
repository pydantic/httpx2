from __future__ import annotations

import gzip
import io
import json

import pytest

import httpx2

pytestmark = pytest.mark.benchmark

TYPICAL_URL = "https://www.example.org:8443/path/to/resource?key=value&other=1#frag"

HEADERS: list[tuple[str, str]] = [
    ("host", "example.org"),
    ("user-agent", "httpx2-bench/1.0"),
    ("accept", "*/*"),
    ("accept-encoding", "gzip, deflate, br"),
    *[(f"x-custom-{i}", f"value-{i}") for i in range(16)],
]

JSON_PAYLOAD: dict[str, object] = {
    "id": 12345,
    "items": [{"sku": f"SKU-{i}", "qty": i, "price": i * 1.5} for i in range(50)],
}
JSON_BODY = json.dumps(JSON_PAYLOAD).encode()
GZIPPED_JSON_BODY = gzip.compress(JSON_BODY)

def test_bench_url_join() -> None:
    httpx2.URL(TYPICAL_URL).join("/path/to/resource?key=value")


def test_bench_request_json_post() -> None:
    httpx2.Request("POST", TYPICAL_URL, headers=HEADERS, json=JSON_PAYLOAD)


def test_bench_request_multipart() -> None:
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


def test_bench_response_gzip_decode() -> None:
    response = httpx2.Response(
        200,
        headers=[("content-type", "application/json"), ("content-encoding", "gzip")],
        content=GZIPPED_JSON_BODY,
    )
    response.read()


def test_bench_response_iter_bytes() -> None:
    response = httpx2.Response(200, content=b"x" * 1_048_576)
    for _ in response.iter_bytes(chunk_size=8192):
        pass


def _json_handler(request: httpx2.Request) -> httpx2.Response:
    return httpx2.Response(200, content=JSON_BODY, headers=[("content-type", "application/json")])


def test_bench_client_get_json() -> None:
    with httpx2.Client(transport=httpx2.MockTransport(_json_handler)) as client:
        client.get(TYPICAL_URL).json()


def test_bench_client_post_json() -> None:
    with httpx2.Client(transport=httpx2.MockTransport(_json_handler)) as client:
        client.post(TYPICAL_URL, json=JSON_PAYLOAD)
