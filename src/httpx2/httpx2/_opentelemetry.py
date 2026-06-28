from __future__ import annotations

import os
import re
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from functools import cache
from types import TracebackType
from typing import TYPE_CHECKING, NamedTuple, Protocol, TypeAlias

from .__version__ import __version__
from ._models import Headers, Request, Response

if TYPE_CHECKING:
    from opentelemetry.metrics import Histogram, Meter, MeterProvider
    from opentelemetry.trace import Span, SpanKind, Status, StatusCode, Tracer, TracerProvider

AttributeValue: TypeAlias = str | bool | int | float | Sequence[str] | Sequence[bool] | Sequence[int] | Sequence[float]
Attributes: TypeAlias = dict[str, AttributeValue]


class PropagateModule(Protocol):
    def inject(self, carrier: Headers) -> None: ...


class MetricsModule(Protocol):
    def get_meter(
        self,
        name: str,
        version: str = "",
        meter_provider: MeterProvider | None = None,
        schema_url: str | None = None,
        attributes: Mapping[str, AttributeValue] | None = None,
    ) -> Meter: ...


class TraceModule(Protocol):
    def get_tracer(
        self,
        instrumenting_module_name: str,
        instrumenting_library_version: str | None = None,
        tracer_provider: TracerProvider | None = None,
        schema_url: str | None = None,
        attributes: Mapping[str, AttributeValue] | None = None,
    ) -> Tracer: ...


class OpenTelemetryDependencies(NamedTuple):
    propagate: PropagateModule
    span_kind: type[SpanKind]
    status: type[Status]
    status_code: type[StatusCode]
    is_http_instrumentation_enabled: Callable[[], bool]
    tracer: Tracer
    duration_histogram: Histogram


KNOWN_HTTP_METHODS = {
    "CONNECT",
    "DELETE",
    "GET",
    "HEAD",
    "OPTIONS",
    "PATCH",
    "POST",
    "PUT",
    "QUERY",
    "TRACE",
}

SEMCONV_SCHEMA_URL = "https://opentelemetry.io/schemas/1.42.0"
INSTRUMENTATION_NAME = "httpx2"
CLIENT_REQUEST_DURATION = "http.client.request.duration"
SENSITIVE_HEADERS = {"authorization", "proxy-authorization", "cookie", "set-cookie"}
HTTP_CLIENT_REQUEST_DURATION_BUCKETS = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.075,
    0.1,
    0.25,
    0.5,
    0.75,
    1,
    2.5,
    5,
    7.5,
    10,
)


@contextmanager
def trace_request(request: Request) -> Iterator[RequestTrace | NoOpRequestTrace]:
    dependencies = opentelemetry_dependencies()
    if dependencies is None or not is_enabled(request, dependencies):
        yield NOOP_REQUEST_TRACE
    else:
        with record_request(request, dependencies) as trace:
            yield trace


@cache
def opentelemetry_dependencies() -> OpenTelemetryDependencies | None:
    try:
        from opentelemetry import metrics, propagate, trace
        from opentelemetry.instrumentation.utils import is_http_instrumentation_enabled
        from opentelemetry.trace import SpanKind, Status, StatusCode
    except ImportError:  # pragma: no cover
        return None

    tracer = get_tracer(trace)
    meter = get_meter(metrics)
    return OpenTelemetryDependencies(
        propagate=propagate,
        span_kind=SpanKind,
        status=Status,
        status_code=StatusCode,
        is_http_instrumentation_enabled=is_http_instrumentation_enabled,
        tracer=tracer,
        duration_histogram=create_duration_histogram(meter),
    )


def is_enabled(request: Request, dependencies: OpenTelemetryDependencies) -> bool:
    return is_http_url(request) and dependencies.is_http_instrumentation_enabled()


def start_request(request: Request, dependencies: OpenTelemetryDependencies) -> RequestTrace:
    span_attributes = request_attributes(request)
    metric_attributes = request_metric_attributes(request)
    name = span_name(request.method)
    span_cm = dependencies.tracer.start_as_current_span(
        name,
        kind=dependencies.span_kind.CLIENT,
        attributes=span_attributes,
        end_on_exit=False,
    )
    span = span_cm.__enter__()
    trace = RequestTrace(
        span=span,
        duration_histogram=dependencies.duration_histogram,
        metric_attributes=metric_attributes,
        status=dependencies.status,
        status_code=dependencies.status_code,
        span_context_manager=span_cm,
        start=time.perf_counter(),
    )
    try:
        dependencies.propagate.inject(request.headers)
    except Exception as exc:
        trace.set_exception(exc)
        trace.detach_current(type(exc), exc, exc.__traceback__)
        trace.close()
        raise
    return trace


@contextmanager
def record_request(request: Request, dependencies: OpenTelemetryDependencies) -> Iterator[RequestTrace]:
    trace = start_request(request, dependencies)
    try:
        yield trace
    except Exception as exc:
        trace.set_exception(exc)
        trace.detach_current(type(exc), exc, exc.__traceback__)
        raise
    finally:
        trace.close()


class RequestTrace:
    def __init__(
        self,
        *,
        span: Span,
        duration_histogram: Histogram,
        metric_attributes: Attributes,
        status: type[Status],
        status_code: type[StatusCode],
        span_context_manager: AbstractContextManager[Span],
        start: float,
    ) -> None:
        self._span = span
        self._duration_histogram = duration_histogram
        self._metric_attributes = metric_attributes
        self._status = status
        self._status_code = status_code
        self._span_context_manager = span_context_manager
        self._start = start
        self._closed = False
        self._detached = False

    def set_response(self, response: Response) -> None:
        attributes = response_attributes(response)
        self._metric_attributes.update(response_metric_attributes(response))
        set_attributes(self._span, attributes)

        if is_error_status(response.status_code):
            self.set_error(str(response.status_code))

    def set_exception(self, exc: Exception) -> None:
        if self._span.is_recording():
            self._span.record_exception(exc)
        self.set_error(f"{type(exc).__module__}.{type(exc).__qualname__}")

    def record_duration(self, duration: float) -> None:
        self._duration_histogram.record(max(duration, 0), attributes=self._metric_attributes)

    def close(self) -> None:
        if self._closed:  # pragma: no cover
            return

        self._closed = True
        self.detach_current()
        self.record_duration(time.perf_counter() - self._start)
        self._span.end()

    def detach_current(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        if self._detached:
            return

        self._detached = True
        self._span_context_manager.__exit__(exc_type, exc_value, traceback)

    def set_error(self, error_type: str) -> None:
        self._metric_attributes["error.type"] = error_type
        if self._span.is_recording():
            self._span.set_attribute("error.type", error_type)
            self._span.set_status(self._status(self._status_code.ERROR))


class NoOpRequestTrace:
    def set_response(self, response: Response) -> None:
        pass


NOOP_REQUEST_TRACE = NoOpRequestTrace()


def request_attributes(request: Request) -> Attributes:
    attributes = request_metric_attributes(request)
    attributes["url.full"] = redact_url(request)

    header_patterns = captured_headers("OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_CLIENT_REQUEST")
    if header_patterns:
        attributes.update(header_attributes("http.request.header", request.headers, header_patterns))

    return attributes


def request_metric_attributes(request: Request) -> Attributes:
    method = known_method(request.method)
    attributes: Attributes = {
        "http.request.method": method,
    }

    if method == "_OTHER":
        attributes["http.request.method_original"] = request.method

    if request.url.host:
        attributes["server.address"] = request.url.host

    port = request.url.port or {"http": 80, "https": 443}.get(request.url.scheme)
    if port is not None:
        attributes["server.port"] = port

    return attributes


def response_attributes(response: Response) -> Attributes:
    attributes = response_metric_attributes(response)

    header_patterns = captured_headers("OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_CLIENT_RESPONSE")
    if header_patterns:
        attributes.update(header_attributes("http.response.header", response.headers, header_patterns))

    return attributes


def response_metric_attributes(response: Response) -> Attributes:
    attributes: Attributes = {"http.response.status_code": response.status_code}

    if response.http_version:
        attributes["network.protocol.version"] = response.http_version.removeprefix("HTTP/")

    return attributes


def is_http_url(request: Request) -> bool:
    return request.url.scheme in {"http", "https"}


def span_name(method: str) -> str:
    method = known_method(method)
    return "HTTP" if method == "_OTHER" else method


def known_method(method: str) -> str:
    return method if method in known_methods() else "_OTHER"


def known_methods() -> set[str]:
    configured_methods = os.environ.get("OTEL_INSTRUMENTATION_HTTP_KNOWN_METHODS")
    if configured_methods is None:
        return KNOWN_HTTP_METHODS

    return {method.strip().upper() for method in configured_methods.split(",") if method.strip()}


def redact_url(request: Request) -> str:
    if request.url.userinfo:
        return str(request.url.copy_with(username="REDACTED", password="REDACTED"))
    return str(request.url)


def is_error_status(status_code: int) -> bool:
    return status_code >= 400


def set_attributes(span: Span, attributes: Attributes) -> None:
    if not span.is_recording():
        return

    for name, value in attributes.items():
        span.set_attribute(name, value)


def captured_headers(name: str) -> list[re.Pattern[str]]:
    value = os.environ.get(name, "")
    return [re.compile(pattern.strip(), re.IGNORECASE) for pattern in value.split(",") if pattern.strip()]


def header_attributes(
    prefix: str,
    headers: Headers,
    captured_headers: list[re.Pattern[str]],
) -> Attributes:
    sensitive_headers = sensitive_header_patterns()
    attributes: Attributes = {}
    for key in headers.keys():
        if not any(pattern.fullmatch(key) for pattern in captured_headers):
            continue
        attribute = f"{prefix}.{key.lower().replace('-', '_')}"
        values = headers.get_list(key, split_commas=True)
        attributes[attribute] = tuple(
            "[REDACTED]" if is_sensitive_header(key, sensitive_headers) else value for value in values
        )
    return attributes


def sensitive_header_patterns() -> list[re.Pattern[str]]:
    value = os.environ.get("OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SANITIZE_FIELDS", "")
    names = [*SENSITIVE_HEADERS, *[item.strip() for item in value.split(",") if item.strip()]]
    return [re.compile(name, re.IGNORECASE) for name in names]


def is_sensitive_header(key: str, sensitive_headers: list[re.Pattern[str]]) -> bool:
    return any(pattern.fullmatch(key) for pattern in sensitive_headers)


def get_tracer(trace: TraceModule) -> Tracer:
    try:
        return trace.get_tracer(
            INSTRUMENTATION_NAME,
            instrumenting_library_version=__version__,
            schema_url=SEMCONV_SCHEMA_URL,
        )
    except TypeError:  # pragma: no cover
        return trace.get_tracer(INSTRUMENTATION_NAME, __version__)


def get_meter(metrics: MetricsModule) -> Meter:
    try:
        return metrics.get_meter(
            INSTRUMENTATION_NAME,
            version=__version__,
            schema_url=SEMCONV_SCHEMA_URL,
        )
    except TypeError:  # pragma: no cover
        return metrics.get_meter(INSTRUMENTATION_NAME, __version__)


def create_duration_histogram(meter: Meter) -> Histogram:
    try:
        return meter.create_histogram(
            name=CLIENT_REQUEST_DURATION,
            unit="s",
            description="Duration of HTTP client requests.",
            explicit_bucket_boundaries_advisory=HTTP_CLIENT_REQUEST_DURATION_BUCKETS,
        )
    except TypeError:  # pragma: no cover
        return meter.create_histogram(
            name=CLIENT_REQUEST_DURATION,
            unit="s",
            description="Duration of HTTP client requests.",
        )
