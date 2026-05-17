from __future__ import annotations

import pytest

import httpx2
from tests.benchmarks.http import (
    GZIPPED_JSON_BODY,
    JSON_BODY,
    JSON_PAYLOAD,
    LARGE_HEADERS,
    SIMPLE_HEADERS,
    make_gzip_response,
    make_json_response,
    make_plain_response,
    make_request,
)

pytestmark = pytest.mark.benchmark


def test_bench_headers_construct_simple() -> None:
    httpx2.Headers(SIMPLE_HEADERS)


def test_bench_headers_construct_large() -> None:
    httpx2.Headers(LARGE_HEADERS)


def test_bench_headers_getitem() -> None:
    headers = httpx2.Headers(LARGE_HEADERS)
    headers["user-agent"]
    headers["accept-encoding"]
    headers["x-custom-15"]


def test_bench_request_simple_get() -> None:
    httpx2.Request("GET", "https://example.org/path?x=1", headers=SIMPLE_HEADERS)


def test_bench_request_json_post() -> None:
    httpx2.Request("POST", "https://example.org/path", headers=SIMPLE_HEADERS, json=JSON_PAYLOAD)


def test_bench_request_multipart() -> None:
    httpx2.Request(
        "POST",
        "https://example.org/upload",
        data={"name": "value", "other": "field"},
        files={"file": ("hello.txt", b"x" * 4096, "text/plain")},
    )


def test_bench_response_construct_plain() -> None:
    make_plain_response()


def test_bench_response_read_json() -> None:
    response = make_json_response()
    response.read()
    response.json()


def test_bench_response_gzip_decode() -> None:
    response = make_gzip_response()
    response.read()


def test_bench_request_read_body() -> None:
    request = make_request()
    request.read()


@pytest.fixture
def mock_client() -> httpx2.Client:
    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, content=JSON_BODY, headers=[("content-type", "application/json")])

    transport = httpx2.MockTransport(handler)
    return httpx2.Client(transport=transport, headers=SIMPLE_HEADERS)


def test_bench_client_roundtrip_json(mock_client: httpx2.Client) -> None:
    response = mock_client.get("https://example.org/path?x=1")
    response.json()


def test_bench_client_roundtrip_post(mock_client: httpx2.Client) -> None:
    mock_client.post("https://example.org/path", json=JSON_PAYLOAD)


def test_bench_client_roundtrip_gzip() -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(
            200,
            content=GZIPPED_JSON_BODY,
            headers=[("content-type", "application/json"), ("content-encoding", "gzip")],
        )

    with httpx2.Client(transport=httpx2.MockTransport(handler)) as client:
        response = client.get("https://example.org/path")
        response.read()
