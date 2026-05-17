from __future__ import annotations

import json
import threading
from collections.abc import Callable, Generator, Iterable
from typing import Any
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server

import pytest

import httpx2

pytestmark = pytest.mark.benchmark

LARGE_JSON: dict[str, object] = {
    "records": [
        {"id": i, "name": f"record-{i}", "tags": [f"t{j}" for j in range(8)], "active": bool(i % 2)}
        for i in range(2048)
    ],
}
LARGE_JSON_BODY = json.dumps(LARGE_JSON).encode()
LARGE_UPLOAD_BODY = b"x" * 16 * 1024 * 1024  # 16 MB
LARGE_DOWNLOAD_BODY = b"y" * 16 * 1024 * 1024  # 16 MB


class _SilentHandler(WSGIRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:
        pass


StartResponse = Callable[[str, list[tuple[str, str]]], Any]


def _app(environ: dict[str, Any], start_response: StartResponse) -> Iterable[bytes]:
    path = environ.get("PATH_INFO", "/")
    content_length = int(environ.get("CONTENT_LENGTH") or 0)
    if content_length:
        environ["wsgi.input"].read(content_length)
    if path == "/download":
        body = LARGE_DOWNLOAD_BODY
        headers = [("content-type", "application/octet-stream"), ("content-length", str(len(body)))]
    else:
        body = LARGE_JSON_BODY
        headers = [("content-type", "application/json"), ("content-length", str(len(body)))]
    start_response("200 OK", headers)
    return [body]


@pytest.fixture(scope="module")
def server() -> Generator[str, None, None]:
    httpd: WSGIServer = make_server("127.0.0.1", 0, _app, handler_class=_SilentHandler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = httpd.server_address[:2]
        yield f"http://{host!s}:{port}"
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join()


def test_bench_url_parse_and_join() -> None:
    base = httpx2.URL("https://www.example.org:8443/path/to/resource?key=value&other=1#frag")
    for _ in range(1024):
        base.join("/another/path?x=1&y=2")


def test_bench_request_build_json() -> None:
    for _ in range(32):
        httpx2.Request("POST", "https://example.org/api", json=LARGE_JSON)


def test_bench_client_get_json(server: str) -> None:
    with httpx2.Client(base_url=server) as client:
        client.get("/json")  # warmup: establish connection + prime caches
        for _ in range(32):
            client.get("/json").json()


def test_bench_client_post_large(server: str) -> None:
    with httpx2.Client(base_url=server) as client:
        client.post("/upload", content=LARGE_UPLOAD_BODY)  # warmup
        for _ in range(8):
            client.post("/upload", content=LARGE_UPLOAD_BODY)


def test_bench_client_stream_download(server: str) -> None:
    with httpx2.Client(base_url=server) as client:
        with client.stream("GET", "/download") as response:  # warmup
            for _ in response.iter_bytes(chunk_size=65536):
                pass
        for _ in range(8):
            with client.stream("GET", "/download") as response:
                for _ in response.iter_bytes(chunk_size=65536):
                    pass


def test_bench_client_keepalive_burst(server: str) -> None:
    with httpx2.Client(base_url=server) as client:
        for _ in range(64):
            client.get("/json")
