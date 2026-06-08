from __future__ import annotations

import typing

import pytest
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF
from opentelemetry.trace import SpanKind
from opentelemetry.trace.span import Span
from opentelemetry.trace.status import StatusCode

import httpx2


class Telemetry(typing.NamedTuple):
    exporter: InMemorySpanExporter
    provider: TracerProvider


@pytest.fixture
def telemetry() -> typing.Iterator[Telemetry]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    yield Telemetry(exporter, provider)
    httpx2.uninstrument()
    exporter.clear()


def _handler(request: httpx2.Request) -> httpx2.Response:
    return httpx2.Response(200, text="ok", extensions={"http_version": b"HTTP/1.1"})


def _attrs(span: ReadableSpan) -> typing.Mapping[str, typing.Any]:
    assert span.attributes is not None
    return span.attributes


def test_records_client_span(telemetry: Telemetry) -> None:
    with httpx2.instrument(tracer_provider=telemetry.provider):
        with httpx2.Client(transport=httpx2.MockTransport(_handler)) as client:
            response = client.get("https://example.com:8443/foo")
    assert response.status_code == 200

    spans = telemetry.exporter.get_finished_spans()
    assert len(spans) == 1
    span = spans[0]
    assert span.name == "GET"
    assert span.kind == SpanKind.CLIENT
    assert _attrs(span)["http.request.method"] == "GET"
    assert _attrs(span)["url.full"] == "https://example.com:8443/foo"
    assert _attrs(span)["server.address"] == "example.com"
    assert _attrs(span)["server.port"] == 8443
    assert _attrs(span)["http.response.status_code"] == 200
    assert _attrs(span)["network.protocol.version"] == "1.1"


def test_scope_reverts_on_exit(telemetry: Telemetry) -> None:
    with httpx2.instrument(tracer_provider=telemetry.provider):
        with httpx2.Client(transport=httpx2.MockTransport(_handler)) as client:
            client.get("https://example.com/foo")
    assert len(telemetry.exporter.get_finished_spans()) == 1

    telemetry.exporter.clear()
    with httpx2.Client(transport=httpx2.MockTransport(_handler)) as client:
        client.get("https://example.com/bar")
    assert telemetry.exporter.get_finished_spans() == ()


def test_injects_propagation_headers(telemetry: Telemetry) -> None:
    seen: list[str] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        seen.append(request.headers["traceparent"])
        return httpx2.Response(204)

    with httpx2.instrument(tracer_provider=telemetry.provider):
        with httpx2.Client(transport=httpx2.MockTransport(handler)) as client:
            client.get("https://example.com/foo")
    assert len(seen) == 1


def test_server_error_sets_status(telemetry: Telemetry) -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(503)

    with httpx2.instrument(tracer_provider=telemetry.provider):
        with httpx2.Client(transport=httpx2.MockTransport(handler)) as client:
            client.get("https://example.com/foo")

    span = telemetry.exporter.get_finished_spans()[0]
    assert span.status.status_code == StatusCode.ERROR
    assert _attrs(span)["error.type"] == "503"


def test_records_exception(telemetry: Telemetry) -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        raise httpx2.ConnectError("boom")

    with httpx2.instrument(tracer_provider=telemetry.provider):
        with pytest.raises(httpx2.ConnectError):
            with httpx2.Client(transport=httpx2.MockTransport(handler)) as client:
                client.get("https://example.com/foo")

    span = telemetry.exporter.get_finished_spans()[0]
    assert _attrs(span)["error.type"] == "ConnectError"
    assert [event.name for event in span.events] == ["exception"]


def test_hooks(telemetry: Telemetry) -> None:
    def request_hook(span: Span, request: httpx2.Request) -> None:
        span.set_attribute("hook.request", request.method)

    def response_hook(span: Span, request: httpx2.Request, response: httpx2.Response) -> None:
        span.set_attribute("hook.response", response.status_code)

    with httpx2.instrument(tracer_provider=telemetry.provider, request_hook=request_hook, response_hook=response_hook):
        with httpx2.Client(transport=httpx2.MockTransport(_handler)) as client:
            client.get("https://example.com/foo")

    span = telemetry.exporter.get_finished_spans()[0]
    assert _attrs(span)["hook.request"] == "GET"
    assert _attrs(span)["hook.response"] == 200


def test_capture_headers(telemetry: Telemetry) -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, headers={"x-custom": "value"})

    with httpx2.instrument(tracer_provider=telemetry.provider, capture_headers=True):
        with httpx2.Client(transport=httpx2.MockTransport(handler)) as client:
            client.get("https://example.com/foo", headers={"x-request": "abc"})

    attributes = _attrs(telemetry.exporter.get_finished_spans()[0])
    assert attributes["http.request.header.x_request"] == ("abc",)
    assert attributes["http.response.header.x_custom"] == ("value",)


def test_instrument_specific_client(telemetry: Telemetry) -> None:
    traced = httpx2.Client(transport=httpx2.MockTransport(_handler))
    untraced = httpx2.Client(transport=httpx2.MockTransport(_handler))

    with httpx2.instrument(traced, tracer_provider=telemetry.provider):
        traced.get("https://example.com/traced")
        untraced.get("https://example.com/untraced")

    spans = telemetry.exporter.get_finished_spans()
    assert len(spans) == 1
    assert _attrs(spans[0])["url.full"] == "https://example.com/traced"


def test_non_standard_method_span_name(telemetry: Telemetry) -> None:
    with httpx2.instrument(tracer_provider=telemetry.provider):
        with httpx2.Client(transport=httpx2.MockTransport(_handler)) as client:
            client.request("FOO", "https://example.com/foo")

    span = telemetry.exporter.get_finished_spans()[0]
    assert span.name == "HTTP"
    assert _attrs(span)["http.request.method"] == "_OTHER"


def test_non_recording_span(telemetry: Telemetry) -> None:
    provider = TracerProvider(sampler=ALWAYS_OFF)
    provider.add_span_processor(SimpleSpanProcessor(telemetry.exporter))
    with httpx2.instrument(tracer_provider=provider):
        with httpx2.Client(transport=httpx2.MockTransport(_handler)) as client:
            response = client.get("https://example.com/foo")
    assert response.status_code == 200
    assert telemetry.exporter.get_finished_spans() == ()


def test_repeated_instrument_is_idempotent(telemetry: Telemetry) -> None:
    with httpx2.instrument(tracer_provider=telemetry.provider):
        with httpx2.instrument(tracer_provider=telemetry.provider):
            with httpx2.Client(transport=httpx2.MockTransport(_handler)) as client:
                client.get("https://example.com/foo")
    assert len(telemetry.exporter.get_finished_spans()) == 1


@pytest.mark.anyio
async def test_async_records_client_span(telemetry: Telemetry) -> None:
    with httpx2.instrument(tracer_provider=telemetry.provider):
        async with httpx2.AsyncClient(transport=httpx2.MockTransport(_handler)) as client:
            response = await client.get("https://example.com/foo")
    assert response.status_code == 200

    spans = telemetry.exporter.get_finished_spans()
    assert len(spans) == 1
    assert _attrs(spans[0])["url.full"] == "https://example.com/foo"


@pytest.mark.anyio
async def test_async_untraced_when_other_client_targeted(telemetry: Telemetry) -> None:
    target = httpx2.Client(transport=httpx2.MockTransport(_handler))
    with httpx2.instrument(target, tracer_provider=telemetry.provider):
        async with httpx2.AsyncClient(transport=httpx2.MockTransport(_handler)) as client:
            await client.get("https://example.com/foo")
    assert telemetry.exporter.get_finished_spans() == ()
