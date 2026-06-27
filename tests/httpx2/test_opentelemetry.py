from __future__ import annotations

import typing

import logfire
import pytest
from logfire.testing import CaptureLogfire
from opentelemetry.trace import SpanKind, StatusCode

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
    [span] = _httpx2_spans(capfire)
    assert span.name == "GET"
    assert span.kind is SpanKind.CLIENT
    assert span.status.status_code is StatusCode.ERROR
    _assert_attributes_include(
        span.attributes,
        {
            "http.request.method": "GET",
            "url.full": "https://REDACTED:REDACTED@example.com:8443/example",
            "server.address": "example.com",
            "server.port": 8443,
            "http.response.status_code": 404,
            "network.protocol.version": "2",
            "error.type": "404",
        },
    )
    assert "http.method" not in span.attributes
    assert "http.status_code" not in span.attributes

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
    [span] = _httpx2_spans(capfire)
    assert span.kind is SpanKind.CLIENT
    assert span.attributes["http.response.status_code"] == 204
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
    assert _httpx2_spans(capfire) == []
    assert _duration_metrics(capfire) == []


def test_opentelemetry_records_exceptions(capfire: CaptureLogfire) -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        raise httpx2.ConnectError("no route")

    transport = httpx2.MockTransport(handler)
    with httpx2.Client(transport=transport) as client:
        with pytest.raises(httpx2.ConnectError):
            client.get("https://example.com/")

    [span] = _httpx2_spans(capfire)
    assert span.attributes["error.type"] == "httpx2.ConnectError"
    assert span.status.status_code is StatusCode.ERROR
    assert _duration_metric(capfire)["data"]["data_points"][0]["attributes"]["error.type"] == "httpx2.ConnectError"


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

    [span] = _httpx2_spans(capfire)
    assert span.attributes["http.request.header.x_private"] == ("[REDACTED]",)
    assert span.attributes["http.request.header.x_request_id"] == ("abc",)
    assert span.attributes["http.response.header.x_response_private"] == ("[REDACTED]",)
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


def _httpx2_spans(capfire: CaptureLogfire) -> list[typing.Any]:
    return [
        span
        for span in capfire.exporter.exported_spans
        if span.instrumentation_scope is not None
        and span.instrumentation_scope.name == "httpx2"
        and span.attributes is not None
        and span.attributes.get("logfire.span_type") == "span"
    ]


def _assert_attributes_include(
    attributes: typing.Mapping[str, typing.Any],
    expected: dict[str, typing.Any],
) -> None:
    for key, value in expected.items():
        assert attributes[key] == value
