from __future__ import annotations

import typing

import logfire
import pytest
from inline_snapshot import snapshot
from logfire.testing import CaptureLogfire

import httpx2
import httpx2._opentelemetry as otel_module


@pytest.fixture(autouse=True)
def clear_opentelemetry_cache() -> typing.Iterator[None]:
    otel_module._get_opentelemetry.cache_clear()
    yield
    otel_module._get_opentelemetry.cache_clear()


def test_sync_client_emits_current_http_client_span_and_duration_metric(capfire: CaptureLogfire) -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        assert request.headers["traceparent"].startswith("00-")
        return httpx2.Response(
            404,
            headers={"content-type": "text/plain"},
            extensions={"http_version": b"HTTP/2"},
        )

    transport = httpx2.MockTransport(handler)
    with httpx2.Client(transport=transport) as client:
        response = client.get("https://user:password@example.com:8443/example")

    assert response.status_code == 404
    assert capfire.exporter.exported_spans_as_dict(include_instrumentation_scope=True) == snapshot(
        [
            {
                "name": "GET",
                "context": {"trace_id": 1, "span_id": 1, "is_remote": False},
                "parent": None,
                "start_time": 1000000000,
                "end_time": 2000000000,
                "instrumentation_scope": "httpx2",
                "attributes": {
                    "http.request.method": "GET",
                    "server.address": "example.com",
                    "server.port": 8443,
                    "url.full": "https://REDACTED:REDACTED@example.com:8443/example",
                    "logfire.span_type": "span",
                    "logfire.msg": "GET",
                    "http.response.status_code": 404,
                    "network.protocol.version": "2",
                    "error.type": "404",
                    "logfire.level_num": 17,
                },
            }
        ]
    )

    metric = _duration_metric(capfire)
    assert metric["name"] == "http.client.request.duration"
    [data_point] = metric["data"]["data_points"]
    assert data_point["count"] == 1
    assert data_point["sum"] >= 0
    assert data_point["attributes"] == {
        "http.request.method": "GET",
        "server.address": "example.com",
        "server.port": 8443,
        "http.response.status_code": 404,
        "network.protocol.version": "2",
        "error.type": "404",
    }


@pytest.mark.anyio
async def test_async_client_emits_opentelemetry(capfire: CaptureLogfire) -> None:
    async def handler(request: httpx2.Request) -> httpx2.Response:
        assert request.headers["traceparent"].startswith("00-")
        return httpx2.Response(204)

    transport = httpx2.MockTransport(handler)
    async with httpx2.AsyncClient(transport=transport) as client:
        response = await client.get("https://example.com/")

    assert response.status_code == 204
    assert capfire.exporter.exported_spans_as_dict(include_instrumentation_scope=True) == snapshot(
        [
            {
                "name": "GET",
                "context": {"trace_id": 1, "span_id": 1, "is_remote": False},
                "parent": None,
                "start_time": 1000000000,
                "end_time": 2000000000,
                "instrumentation_scope": "httpx2",
                "attributes": {
                    "http.request.method": "GET",
                    "server.address": "example.com",
                    "server.port": 443,
                    "url.full": "https://example.com/",
                    "logfire.span_type": "span",
                    "logfire.msg": "GET",
                    "http.response.status_code": 204,
                    "network.protocol.version": "1.1",
                },
            }
        ]
    )
    assert _duration_metric(capfire)["data"]["data_points"]


def test_opentelemetry_honors_logfire_suppression_context(capfire: CaptureLogfire) -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        assert "traceparent" not in request.headers
        return httpx2.Response(200)

    transport = httpx2.MockTransport(handler)
    with logfire.suppress_instrumentation():
        with httpx2.Client(transport=transport) as client:
            response = client.get("https://example.com/")

    assert response.status_code == 200
    assert capfire.exporter.exported_spans_as_dict(include_instrumentation_scope=True) == snapshot([])
    assert _duration_metrics(capfire) == []


def test_opentelemetry_honors_context_suppression_fallback(capfire: CaptureLogfire) -> None:
    otel = otel_module.get_opentelemetry()
    assert otel is not None
    otel._is_http_instrumentation_enabled = None

    request = httpx2.Request("GET", "https://example.com/")
    assert otel.is_enabled(request) is True

    with logfire.suppress_instrumentation():
        assert otel.is_enabled(request) is False

    assert capfire.exporter.exported_spans_as_dict(include_instrumentation_scope=True) == snapshot([])


def test_opentelemetry_honors_http_context_suppression_fallback(capfire: CaptureLogfire) -> None:
    otel = otel_module.get_opentelemetry()
    assert otel is not None
    otel._is_http_instrumentation_enabled = None
    otel._suppress_http_instrumentation_key = "httpx2.suppress_http_instrumentation"

    request = httpx2.Request("GET", "https://example.com/")
    token = otel._context.attach(otel._context.set_value(otel._suppress_http_instrumentation_key, True))
    try:
        assert otel.is_enabled(request) is False
    finally:
        otel._context.detach(token)

    assert capfire.exporter.exported_spans_as_dict(include_instrumentation_scope=True) == snapshot([])


def test_opentelemetry_records_exceptions(capfire: CaptureLogfire) -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        raise httpx2.ConnectError("no route")

    transport = httpx2.MockTransport(handler)
    with httpx2.Client(transport=transport) as client:
        with pytest.raises(httpx2.ConnectError):
            client.get("https://example.com/")

    assert capfire.exporter.exported_spans_as_dict(include_instrumentation_scope=True) == snapshot(
        [
            {
                "name": "GET",
                "context": {"trace_id": 1, "span_id": 1, "is_remote": False},
                "parent": None,
                "start_time": 1000000000,
                "end_time": 4000000000,
                "instrumentation_scope": "httpx2",
                "attributes": {
                    "http.request.method": "GET",
                    "server.address": "example.com",
                    "server.port": 443,
                    "url.full": "https://example.com/",
                    "logfire.span_type": "span",
                    "logfire.msg": "GET",
                    "error.type": "httpx2.ConnectError",
                    "logfire.exception.fingerprint": "0000000000000000000000000000000000000000000000000000000000000000",
                    "logfire.level_num": 17,
                },
                "events": [
                    {
                        "name": "exception",
                        "timestamp": 2000000000,
                        "attributes": {
                            "exception.type": "httpx2.ConnectError",
                            "exception.message": "no route",
                            "exception.stacktrace": "httpx2.ConnectError: no route",
                            "exception.escaped": "False",
                        },
                    },
                    {
                        "name": "exception",
                        "timestamp": 3000000000,
                        "attributes": {
                            "exception.type": "httpx2.ConnectError",
                            "exception.message": "no route",
                            "exception.stacktrace": "httpx2.ConnectError: no route",
                            "exception.escaped": "False",
                        },
                    },
                ],
            }
        ]
    )
    assert _duration_metric(capfire)["data"]["data_points"][0]["attributes"]["error.type"] == "httpx2.ConnectError"


def test_opentelemetry_records_propagation_injection_exceptions(
    capfire: CaptureLogfire,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    otel = otel_module.get_opentelemetry()
    assert otel is not None

    def inject(headers: httpx2.Headers) -> None:
        raise RuntimeError("inject failed")

    def handler(request: httpx2.Request) -> httpx2.Response:
        pytest.fail("transport should not be called")  # pragma: no cover

    monkeypatch.setattr(otel._propagate, "inject", inject)

    transport = httpx2.MockTransport(handler)
    with httpx2.Client(transport=transport) as client:
        with pytest.raises(RuntimeError, match="inject failed"):
            client.get("https://example.com/")

    assert capfire.exporter.exported_spans_as_dict(include_instrumentation_scope=True) == snapshot(
        [
            {
                "name": "GET",
                "context": {"trace_id": 1, "span_id": 1, "is_remote": False},
                "parent": None,
                "start_time": 1000000000,
                "end_time": 4000000000,
                "instrumentation_scope": "httpx2",
                "attributes": {
                    "http.request.method": "GET",
                    "server.address": "example.com",
                    "server.port": 443,
                    "url.full": "https://example.com/",
                    "logfire.span_type": "span",
                    "logfire.msg": "GET",
                    "error.type": "builtins.RuntimeError",
                    "logfire.exception.fingerprint": "0000000000000000000000000000000000000000000000000000000000000000",
                    "logfire.level_num": 17,
                },
                "events": [
                    {
                        "name": "exception",
                        "timestamp": 2000000000,
                        "attributes": {
                            "exception.type": "RuntimeError",
                            "exception.message": "inject failed",
                            "exception.stacktrace": "RuntimeError: inject failed",
                            "exception.escaped": "False",
                        },
                    },
                    {
                        "name": "exception",
                        "timestamp": 3000000000,
                        "attributes": {
                            "exception.type": "RuntimeError",
                            "exception.message": "inject failed",
                            "exception.stacktrace": "RuntimeError: inject failed",
                            "exception.escaped": "False",
                        },
                    },
                ],
            }
        ]
    )
    assert _duration_metric(capfire)["data"]["data_points"][0]["attributes"]["error.type"] == "builtins.RuntimeError"


def test_opentelemetry_honors_configured_known_methods(
    capfire: CaptureLogfire,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OTEL_INSTRUMENTATION_HTTP_KNOWN_METHODS", "GET,PROPFIND")

    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200)

    transport = httpx2.MockTransport(handler)
    with httpx2.Client(transport=transport) as client:
        client.request("PROPFIND", "https://example.com/")

    assert capfire.exporter.exported_spans_as_dict(include_instrumentation_scope=True) == snapshot(
        [
            {
                "name": "PROPFIND",
                "context": {"trace_id": 1, "span_id": 1, "is_remote": False},
                "parent": None,
                "start_time": 1000000000,
                "end_time": 2000000000,
                "instrumentation_scope": "httpx2",
                "attributes": {
                    "http.request.method": "PROPFIND",
                    "server.address": "example.com",
                    "server.port": 443,
                    "url.full": "https://example.com/",
                    "logfire.span_type": "span",
                    "logfire.msg": "PROPFIND",
                    "http.response.status_code": 200,
                    "network.protocol.version": "1.1",
                },
            }
        ]
    )
    assert _duration_metric(capfire)["data"]["data_points"][0]["attributes"]["http.request.method"] == "PROPFIND"


def test_opentelemetry_uses_other_for_unknown_methods(capfire: CaptureLogfire) -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200)

    transport = httpx2.MockTransport(handler)
    with httpx2.Client(transport=transport) as client:
        client.request("BREW", "https://example.com/")

    assert capfire.exporter.exported_spans_as_dict(include_instrumentation_scope=True) == snapshot(
        [
            {
                "name": "HTTP",
                "context": {"trace_id": 1, "span_id": 1, "is_remote": False},
                "parent": None,
                "start_time": 1000000000,
                "end_time": 2000000000,
                "instrumentation_scope": "httpx2",
                "attributes": {
                    "http.request.method": "_OTHER",
                    "http.request.method_original": "BREW",
                    "server.address": "example.com",
                    "server.port": 443,
                    "url.full": "https://example.com/",
                    "logfire.span_type": "span",
                    "logfire.msg": "HTTP",
                    "http.response.status_code": 200,
                    "network.protocol.version": "1.1",
                },
            }
        ]
    )
    metric_attributes = _duration_metric(capfire)["data"]["data_points"][0]["attributes"]
    assert metric_attributes["http.request.method"] == "_OTHER"
    assert metric_attributes["http.request.method_original"] == "BREW"


def test_opentelemetry_captures_and_sanitizes_opt_in_headers(
    capfire: CaptureLogfire,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_CLIENT_REQUEST", "x-private,x-.*")
    monkeypatch.setenv("OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_CLIENT_RESPONSE", "x-response-private")
    monkeypatch.setenv(
        "OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SANITIZE_FIELDS",
        "x-private,x-response-private",
    )

    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, headers={"x-response-private": "secret"})

    transport = httpx2.MockTransport(handler)
    with httpx2.Client(transport=transport, headers={"x-private": "secret", "x-request-id": "abc"}) as client:
        client.get("https://example.com/")

    assert capfire.exporter.exported_spans_as_dict(include_instrumentation_scope=True) == snapshot(
        [
            {
                "name": "GET",
                "context": {"trace_id": 1, "span_id": 1, "is_remote": False},
                "parent": None,
                "start_time": 1000000000,
                "end_time": 2000000000,
                "instrumentation_scope": "httpx2",
                "attributes": {
                    "http.request.method": "GET",
                    "server.address": "example.com",
                    "server.port": 443,
                    "url.full": "https://example.com/",
                    "http.request.header.x_private": ("[REDACTED]",),
                    "http.request.header.x_request_id": ("abc",),
                    "logfire.span_type": "span",
                    "logfire.msg": "GET",
                    "http.response.status_code": 200,
                    "network.protocol.version": "1.1",
                    "http.response.header.x_response_private": ("[REDACTED]",),
                },
            }
        ]
    )
    assert "http.request.header.x_private" not in _duration_metric(capfire)["data"]["data_points"][0]["attributes"]


def _duration_metric(capfire: CaptureLogfire) -> dict[str, typing.Any]:
    [metric] = _duration_metrics(capfire)
    return metric


def _duration_metrics(capfire: CaptureLogfire) -> list[dict[str, typing.Any]]:
    try:
        metrics = typing.cast(typing.Any, capfire).get_collected_metrics()
    except AttributeError:
        return []
    return [metric for metric in metrics if metric["name"] == "http.client.request.duration"]
