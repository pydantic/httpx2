from __future__ import annotations

import gzip
import json

import pytest

import httpx2
from httpx2._urlparse import urlparse

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


def test_bench_urlparse() -> None:
    urlparse(TYPICAL_URL)


def test_bench_url_join() -> None:
    httpx2.URL(TYPICAL_URL).join("/path/to/resource?key=value")


def test_bench_queryparams() -> None:
    httpx2.QueryParams([("a", "1"), ("b", "2"), ("c", "3"), ("d", "4"), ("a", "5")])


def test_bench_headers_construct() -> None:
    httpx2.Headers(HEADERS)


def test_bench_request_json_post() -> None:
    httpx2.Request("POST", TYPICAL_URL, headers=HEADERS, json=JSON_PAYLOAD)


def test_bench_response_gzip_decode() -> None:
    response = httpx2.Response(
        200,
        headers=[("content-type", "application/json"), ("content-encoding", "gzip")],
        content=GZIPPED_JSON_BODY,
    )
    response.read()


def _json_handler(request: httpx2.Request) -> httpx2.Response:
    return httpx2.Response(200, content=JSON_BODY, headers=[("content-type", "application/json")])


def test_bench_client_get_json() -> None:
    with httpx2.Client(transport=httpx2.MockTransport(_json_handler)) as client:
        client.get(TYPICAL_URL).json()


def test_bench_client_post_json() -> None:
    with httpx2.Client(transport=httpx2.MockTransport(_json_handler)) as client:
        client.post(TYPICAL_URL, json=JSON_PAYLOAD)
