from __future__ import annotations

import gzip
import json
from typing import TYPE_CHECKING, Any

import pytest

import httpx2
from httpx2._decoders import GZipDecoder, LineDecoder

if TYPE_CHECKING:
    from pytest_codspeed import BenchmarkFixture

pytestmark = pytest.mark.benchmark

TYPICAL_URL = "https://www.example.org:8443/path/to/resource?key=value&other=1#frag"

HEADERS: list[tuple[str, str]] = [
    ("host", "example.org"),
    ("user-agent", "httpx2-bench/1.0"),
    ("accept", "*/*"),
    ("accept-encoding", "gzip, deflate, br"),
    *[(f"x-custom-{i}", f"value-{i}") for i in range(16)],
]

SMALL_JSON: dict[str, Any] = {
    "id": 12345,
    "items": [{"sku": f"SKU-{i}", "qty": i, "price": i * 1.5} for i in range(50)],
}
MEDIUM_JSON: dict[str, Any] = {
    "records": [
        {"id": i, "name": f"record-{i}", "tags": [f"t{j}" for j in range(8)], "active": bool(i % 2)} for i in range(256)
    ],
}
MEDIUM_JSON_BODY = json.dumps(MEDIUM_JSON).encode()

QUERY_STRING = "key=value&other=1&" + "&".join(f"f{i}={i}" for i in range(16))

GZIP_BODY = gzip.compress(MEDIUM_JSON_BODY)

SET_COOKIE_HEADERS: list[tuple[str, str]] = [
    ("set-cookie", f"session{i}=value{i}; Path=/; Domain=example.org; HttpOnly") for i in range(8)
]

DIGEST_CHALLENGE = 'Digest realm="httpx@example.org", qop="auth", nonce="abc123nonce", opaque="xyz789opaque"'


# --- Micro: pure httpx2 hot paths -------------------------------------------------


def test_bench_url_parse(benchmark: BenchmarkFixture) -> None:
    benchmark(httpx2.URL, TYPICAL_URL)


def test_bench_url_join(benchmark: BenchmarkFixture) -> None:
    base = httpx2.URL(TYPICAL_URL)
    benchmark(base.join, "/path/to/resource?key=value")


def test_bench_headers_construct(benchmark: BenchmarkFixture) -> None:
    benchmark(httpx2.Headers, HEADERS)


def test_bench_headers_raw(benchmark: BenchmarkFixture) -> None:
    headers = httpx2.Headers(HEADERS)
    benchmark(lambda: headers.raw)


def test_bench_headers_lookup(benchmark: BenchmarkFixture) -> None:
    headers = httpx2.Headers(HEADERS)
    benchmark(lambda: headers["accept-encoding"])


def test_bench_queryparams_parse(benchmark: BenchmarkFixture) -> None:
    benchmark(httpx2.QueryParams, QUERY_STRING)


def test_bench_queryparams_merge(benchmark: BenchmarkFixture) -> None:
    params = httpx2.QueryParams(QUERY_STRING)
    benchmark(lambda: params.merge({"added": "1", "other": "2"}))


def test_bench_gzip_decode(benchmark: BenchmarkFixture) -> None:
    def decode() -> bytes:
        decoder = GZipDecoder()
        return decoder.decode(GZIP_BODY) + decoder.flush()

    benchmark(decode)


def test_bench_line_decoder(benchmark: BenchmarkFixture) -> None:
    text = "\n".join(f"line number {i} with some content" for i in range(256)) + "\n"

    def split() -> list[str]:
        decoder = LineDecoder()
        return decoder.decode(text) + decoder.flush()

    benchmark(split)


def test_bench_extract_cookies(benchmark: BenchmarkFixture) -> None:
    request = httpx2.Request("GET", "https://example.org/")

    def extract() -> None:
        response = httpx2.Response(200, headers=SET_COOKIE_HEADERS, request=request)
        httpx2.Cookies().extract_cookies(response)

    benchmark(extract)


def test_bench_digest_auth_flow(benchmark: BenchmarkFixture) -> None:
    challenge = httpx2.Response(
        401,
        headers=[("www-authenticate", DIGEST_CHALLENGE)],
        request=httpx2.Request("GET", "https://example.org/path/to/resource"),
    )

    def flow() -> None:
        auth = httpx2.DigestAuth(username="user", password="password123")
        generator = auth.sync_auth_flow(httpx2.Request("GET", "https://example.org/path/to/resource"))
        next(generator)
        try:
            generator.send(challenge)
        except StopIteration:
            pass

    benchmark(flow)


def test_bench_request_json_post(benchmark: BenchmarkFixture) -> None:
    benchmark(lambda: httpx2.Request("POST", TYPICAL_URL, headers=HEADERS, json=SMALL_JSON))


def test_bench_request_multipart(benchmark: BenchmarkFixture) -> None:
    def build() -> None:
        request = httpx2.Request(
            "POST",
            "https://example.org/upload",
            data={"name": "value", "other": "field", "description": "a longer text field"},
            files={"small": ("hello.txt", b"x" * 4096, "text/plain")},
        )
        request.read()

    benchmark(build)


def test_bench_response_read_json(benchmark: BenchmarkFixture) -> None:
    def build() -> Any:
        response = httpx2.Response(
            200,
            headers=[("content-type", "application/json")],
            content=MEDIUM_JSON_BODY,
        )
        return response.json()

    benchmark(build)


# --- Macro: end-to-end client cycle via MockTransport -----------------------------


def _json_handler(request: httpx2.Request) -> httpx2.Response:
    return httpx2.Response(200, content=MEDIUM_JSON_BODY, headers=[("content-type", "application/json")])


def test_bench_client_post_json(benchmark: BenchmarkFixture) -> None:
    with httpx2.Client(transport=httpx2.MockTransport(_json_handler)) as client:
        benchmark(lambda: client.post(TYPICAL_URL, json=MEDIUM_JSON).json())
