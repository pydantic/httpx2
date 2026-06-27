from __future__ import annotations

import contextlib
import os
import re
import time
import typing
from functools import cache

from .__version__ import __version__
from ._models import Headers, Request, Response

if typing.TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Generator

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


def get_opentelemetry() -> OpenTelemetry | None:
    return _get_opentelemetry()


@cache
def _get_opentelemetry() -> OpenTelemetry | None:
    try:
        from opentelemetry import context, metrics, propagate, trace
        from opentelemetry.trace import SpanKind, Status, StatusCode
    except ImportError:
        return None

    is_http_instrumentation_enabled: typing.Callable[[], bool] | None
    try:
        from opentelemetry.instrumentation.utils import is_http_instrumentation_enabled
    except ImportError:
        is_http_instrumentation_enabled = None

    return OpenTelemetry(
        context=context,
        metrics=metrics,
        propagate=propagate,
        trace=trace,
        span_kind=SpanKind,
        status=Status,
        status_code=StatusCode,
        is_http_instrumentation_enabled=is_http_instrumentation_enabled,
    )


class OpenTelemetry:
    def __init__(
        self,
        *,
        context: typing.Any,
        metrics: typing.Any,
        propagate: typing.Any,
        trace: typing.Any,
        span_kind: typing.Any,
        status: typing.Any,
        status_code: typing.Any,
        is_http_instrumentation_enabled: typing.Callable[[], bool] | None,
    ) -> None:
        self._context = context
        self._propagate = propagate
        self._span_kind = span_kind
        self._status = status
        self._status_code = status_code
        self._is_http_instrumentation_enabled = is_http_instrumentation_enabled
        self._suppress_instrumentation_key = getattr(context, "_SUPPRESS_INSTRUMENTATION_KEY", None)
        self._suppress_http_instrumentation_key = getattr(context, "_SUPPRESS_HTTP_INSTRUMENTATION_KEY", None)
        self._tracer = _get_tracer(trace)
        meter = _get_meter(metrics)
        self._duration_histogram = _create_duration_histogram(meter)

    def is_enabled(self, request: Request) -> bool:
        if not _is_http_url(request):
            return False

        if self._is_http_instrumentation_enabled is not None:
            return self._is_http_instrumentation_enabled()

        if self._suppress_instrumentation_key is not None and self._context.get_value(
            self._suppress_instrumentation_key
        ):
            return False

        if self._suppress_http_instrumentation_key is not None and self._context.get_value(
            self._suppress_http_instrumentation_key
        ):
            return False

        return True

    @contextlib.contextmanager
    def trace_request(self, request: Request) -> Generator[RequestTrace]:
        span_attributes = _request_attributes(request)
        metric_attributes = _request_metric_attributes(request)
        span_name = _span_name(request.method)
        span_cm = self._tracer.start_as_current_span(
            span_name,
            kind=self._span_kind.CLIENT,
            attributes=span_attributes,
        )
        span = span_cm.__enter__()
        trace = RequestTrace(
            span=span,
            duration_histogram=self._duration_histogram,
            metric_attributes=metric_attributes,
            status=self._status,
            status_code=self._status_code,
        )
        start = time.perf_counter()
        try:
            self._propagate.inject(request.headers)
            yield trace
        except BaseException as exc:
            trace.set_exception(exc)
            raise
        finally:
            trace.record_duration(time.perf_counter() - start)
            span_cm.__exit__(*trace.exc_info)


class RequestTrace:
    def __init__(
        self,
        *,
        span: typing.Any,
        duration_histogram: typing.Any,
        metric_attributes: dict[str, typing.Any],
        status: typing.Any,
        status_code: typing.Any,
    ) -> None:
        self._span = span
        self._duration_histogram = duration_histogram
        self._metric_attributes = metric_attributes
        self._status = status
        self._status_code = status_code
        self.exc_info: tuple[type[BaseException] | None, BaseException | None, typing.Any] = (None, None, None)

    def set_response(self, response: Response) -> None:
        response_attributes = _response_attributes(response)
        self._metric_attributes.update(_response_metric_attributes(response))
        _set_attributes(self._span, response_attributes)

        if _is_error_status(response.status_code):
            self._set_error(str(response.status_code))

    def set_exception(self, exc: BaseException) -> None:
        self.exc_info = (type(exc), exc, exc.__traceback__)
        self._set_error(f"{type(exc).__module__}.{type(exc).__qualname__}")

    def record_duration(self, duration: float) -> None:
        self._duration_histogram.record(max(duration, 0), attributes=self._metric_attributes)

    def _set_error(self, error_type: str) -> None:
        self._metric_attributes["error.type"] = error_type
        if self._span.is_recording():
            self._span.set_attribute("error.type", error_type)
            self._span.set_status(self._status(self._status_code.ERROR))


def _request_attributes(request: Request) -> dict[str, typing.Any]:
    attributes = _request_metric_attributes(request)
    attributes["url.full"] = _redact_url(request)

    captured_headers = _captured_headers("OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_CLIENT_REQUEST")
    if captured_headers:
        attributes.update(_header_attributes("http.request.header", request.headers, captured_headers))

    return attributes


def _request_metric_attributes(request: Request) -> dict[str, typing.Any]:
    method = _known_method(request.method)
    attributes: dict[str, typing.Any] = {
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


def _response_attributes(response: Response) -> dict[str, typing.Any]:
    attributes = _response_metric_attributes(response)

    captured_headers = _captured_headers("OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_CLIENT_RESPONSE")
    if captured_headers:
        attributes.update(_header_attributes("http.response.header", response.headers, captured_headers))

    return attributes


def _response_metric_attributes(response: Response) -> dict[str, typing.Any]:
    attributes: dict[str, typing.Any] = {"http.response.status_code": response.status_code}

    if response.http_version:
        attributes["network.protocol.version"] = response.http_version.removeprefix("HTTP/")

    return attributes


def _is_http_url(request: Request) -> bool:
    return request.url.scheme in {"http", "https"}


def _span_name(method: str) -> str:
    method = _known_method(method)
    return "HTTP" if method == "_OTHER" else method


def _known_method(method: str) -> str:
    return method if method in KNOWN_HTTP_METHODS else "_OTHER"


def _redact_url(request: Request) -> str:
    if request.url.userinfo:
        return str(request.url.copy_with(username="REDACTED", password="REDACTED"))
    return str(request.url)


def _is_error_status(status_code: int) -> bool:
    return status_code >= 400


def _set_attributes(span: typing.Any, attributes: dict[str, typing.Any]) -> None:
    if not span.is_recording():
        return

    for name, value in attributes.items():
        span.set_attribute(name, value)


def _captured_headers(name: str) -> list[re.Pattern[str]]:
    value = os.environ.get(name, "")
    return [re.compile(pattern.strip(), re.IGNORECASE) for pattern in value.split(",") if pattern.strip()]


def _header_attributes(
    prefix: str,
    headers: Headers,
    captured_headers: list[re.Pattern[str]],
) -> dict[str, list[str]]:
    sensitive_headers = _sensitive_headers()
    attributes: dict[str, list[str]] = {}
    for key in headers.keys():
        if not any(pattern.fullmatch(key) for pattern in captured_headers):
            continue
        attribute = f"{prefix}.{key.lower().replace('-', '_')}"
        values = headers.get_list(key, split_commas=True)
        attributes[attribute] = [
            "[REDACTED]" if _is_sensitive_header(key, sensitive_headers) else value for value in values
        ]
    return attributes


def _sensitive_headers() -> list[re.Pattern[str]]:
    value = os.environ.get("OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SANITIZE_FIELDS", "")
    names = [*SENSITIVE_HEADERS, *[item.strip() for item in value.split(",") if item.strip()]]
    return [re.compile(name, re.IGNORECASE) for name in names]


def _is_sensitive_header(key: str, sensitive_headers: list[re.Pattern[str]]) -> bool:
    return any(pattern.fullmatch(key) for pattern in sensitive_headers)


def _get_tracer(trace: typing.Any) -> typing.Any:
    try:
        return trace.get_tracer(
            INSTRUMENTATION_NAME,
            instrumenting_library_version=__version__,
            schema_url=SEMCONV_SCHEMA_URL,
        )
    except TypeError:
        return trace.get_tracer(INSTRUMENTATION_NAME, __version__)


def _get_meter(metrics: typing.Any) -> typing.Any:
    try:
        return metrics.get_meter(
            INSTRUMENTATION_NAME,
            instrumenting_library_version=__version__,
            schema_url=SEMCONV_SCHEMA_URL,
        )
    except TypeError:
        return metrics.get_meter(INSTRUMENTATION_NAME, __version__)


def _create_duration_histogram(meter: typing.Any) -> typing.Any:
    try:
        return meter.create_histogram(
            name=CLIENT_REQUEST_DURATION,
            unit="s",
            description="Duration of HTTP client requests.",
            explicit_bucket_boundaries_advisory=HTTP_CLIENT_REQUEST_DURATION_BUCKETS,
        )
    except TypeError:
        return meter.create_histogram(
            name=CLIENT_REQUEST_DURATION,
            unit="s",
            description="Duration of HTTP client requests.",
        )
