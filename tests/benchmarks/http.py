from __future__ import annotations

import gzip
import json

from httpx2 import Request, Response

SIMPLE_HEADERS: list[tuple[str, str]] = [
    ("host", "example.org"),
    ("user-agent", "httpx2-bench/1.0"),
    ("accept", "*/*"),
    ("accept-encoding", "gzip, deflate, br"),
    ("connection", "keep-alive"),
]

LARGE_HEADERS: list[tuple[str, str]] = [
    *SIMPLE_HEADERS,
    *[(f"x-custom-{i}", f"value-{i}") for i in range(32)],
    ("cookie", "; ".join(f"k{i}=v{i}" for i in range(16))),
    ("authorization", "Bearer " + "a" * 256),
]

JSON_PAYLOAD: dict[str, object] = {
    "id": 12345,
    "items": [{"sku": f"SKU-{i}", "qty": i, "price": i * 1.5} for i in range(50)],
    "metadata": {"region": "eu-west-1", "tags": ["alpha", "beta", "gamma"]},
}
JSON_BODY: bytes = json.dumps(JSON_PAYLOAD).encode()

GZIPPED_JSON_BODY: bytes = gzip.compress(JSON_BODY)


def make_plain_response() -> Response:
    return Response(200, headers=SIMPLE_HEADERS, content=b"Hello, world!")


def make_json_response() -> Response:
    return Response(200, headers=[("content-type", "application/json")], content=JSON_BODY)


def make_gzip_response() -> Response:
    return Response(
        200,
        headers=[("content-type", "application/json"), ("content-encoding", "gzip")],
        content=GZIPPED_JSON_BODY,
    )


def make_request() -> Request:
    return Request("POST", "https://example.org/path?x=1", headers=SIMPLE_HEADERS, json=JSON_PAYLOAD)
