from __future__ import annotations

import contextlib
import typing
from contextvars import ContextVar
from dataclasses import dataclass
from timeit import default_timer

from ._models import Request, Response
from ._transports.asgi import ASGITransport
from ._transports.default import AsyncHTTPTransport, HTTPTransport
from ._transports.mock import MockTransport
from ._transports.wsgi import WSGITransport

if typing.TYPE_CHECKING:
    from opentelemetry.metrics import Histogram, MeterProvider
    from opentelemetry.trace import Span, Tracer, TracerProvider

__all__ = ["RequestHook", "ResponseHook", "instrument", "uninstrument"]

RequestHook = typing.Callable[["Span", Request], None]
ResponseHook = typing.Callable[["Span", Request, Response], None]

INSTRUMENTATION_NAME = "httpx2"
SCHEMA_URL = "https://opentelemetry.io/schemas/1.21.0"


@dataclass
class _Config:
    tracer: Tracer
    duration_histogram: Histogram
    capture_headers: bool = False
    request_hook: RequestHook | None = None
    response_hook: ResponseHook | None = None
    clients: set[int] | None = None
    """When set, only requests from these client ids are traced. ``None`` traces every request."""


_active: ContextVar[_Config | None] = ContextVar("httpx2_otel_config", default=None)
_patched = False


def _require_otel() -> None:
    try:
        import opentelemetry.trace  # noqa: F401
    except ImportError as exc:  # pragma: no cover
        raise ImportError(
            "OpenTelemetry is required to instrument httpx2. Install it with `pip install httpx2[otel]`."
        ) from exc


def _build_config(
    tracer_provider: TracerProvider | None,
    meter_provider: MeterProvider | None,
    capture_headers: bool,
    request_hook: RequestHook | None,
    response_hook: ResponseHook | None,
    clients: set[int] | None,
) -> _Config:
    from opentelemetry.metrics import get_meter
    from opentelemetry.trace import get_tracer

    tracer = get_tracer(INSTRUMENTATION_NAME, tracer_provider=tracer_provider, schema_url=SCHEMA_URL)
    meter = get_meter(INSTRUMENTATION_NAME, meter_provider=meter_provider, schema_url=SCHEMA_URL)
    duration_histogram = meter.create_histogram(
        name="http.client.request.duration",
        unit="s",
        description="Duration of HTTP client requests.",
    )
    return _Config(
        tracer=tracer,
        duration_histogram=duration_histogram,
        capture_headers=capture_headers,
        request_hook=request_hook,
        response_hook=response_hook,
        clients=clients,
    )


_STANDARD_METHODS = frozenset({"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "TRACE", "CONNECT"})


def _span_name(method: str) -> str:
    return method if method in _STANDARD_METHODS else "HTTP"


def _request_attributes(request: Request, capture_headers: bool) -> dict[str, typing.Any]:
    url = request.url
    method = request.method if request.method in _STANDARD_METHODS else "_OTHER"
    attributes: dict[str, typing.Any] = {
        "http.request.method": method,
        "url.full": str(url),
        "server.address": url.host,
    }
    if url.port is not None:
        attributes["server.port"] = url.port
    if capture_headers:
        for key, value in request.headers.multi_items():
            attributes[f"http.request.header.{key.lower().replace('-', '_')}"] = [value]
    return attributes


def _response_attributes(response: Response, capture_headers: bool) -> dict[str, typing.Any]:
    attributes: dict[str, typing.Any] = {"http.response.status_code": response.status_code}
    protocol = response.http_version.split("/", 1)
    if len(protocol) == 2:
        attributes["network.protocol.version"] = protocol[1]
    if capture_headers:
        for key, value in response.headers.multi_items():
            attributes[f"http.response.header.{key.lower().replace('-', '_')}"] = [value]
    return attributes


def _should_trace(config: _Config, instance: object) -> bool:
    return config.clients is None or id(instance) in config.clients


@contextlib.contextmanager
def _traced_span(config: _Config, request: Request) -> typing.Iterator[Span]:
    from opentelemetry.propagate import inject
    from opentelemetry.trace import SpanKind, use_span

    attributes = _request_attributes(request, config.capture_headers)
    span = config.tracer.start_span(_span_name(request.method), kind=SpanKind.CLIENT, attributes=attributes)
    start = default_timer()
    metric_attributes = {"http.request.method": attributes["http.request.method"]}
    if request.url.host:
        metric_attributes["server.address"] = request.url.host
    try:
        with use_span(span, end_on_exit=False, record_exception=False):
            inject(request.headers)
            if config.request_hook is not None:
                config.request_hook(span, request)
            yield span
    except BaseException as exc:
        span.set_attribute("error.type", type(exc).__qualname__)
        metric_attributes["error.type"] = type(exc).__qualname__
        span.record_exception(exc)
        raise
    finally:
        config.duration_histogram.record(max(default_timer() - start, 0), attributes=metric_attributes)
        span.end()


def _apply_response(config: _Config, span: Span, request: Request, response: Response) -> None:
    if not span.is_recording():
        return
    from opentelemetry.trace.status import StatusCode

    span.set_attributes(_response_attributes(response, config.capture_headers))
    if response.status_code >= 500:
        span.set_status(StatusCode.ERROR)
        span.set_attribute("error.type", str(response.status_code))
    if config.response_hook is not None:
        config.response_hook(span, request, response)


_SYNC_TRANSPORTS: tuple[type[typing.Any], ...] = (HTTPTransport, MockTransport, WSGITransport)
_ASYNC_TRANSPORTS: tuple[type[typing.Any], ...] = (AsyncHTTPTransport, MockTransport, ASGITransport)

SyncHandle = typing.Callable[[typing.Any, Request], Response]
AsyncHandle = typing.Callable[[typing.Any, Request], typing.Awaitable[Response]]


def _instrumented_sync_handle(wrapped: SyncHandle) -> SyncHandle:
    def handle_request(self: typing.Any, request: Request) -> Response:
        config = _active.get()
        if config is None or not _should_trace(config, self):
            return wrapped(self, request)
        with _traced_span(config, request) as span:
            response = wrapped(self, request)
            _apply_response(config, span, request, response)
        return response

    return handle_request


def _instrumented_async_handle(wrapped: AsyncHandle) -> AsyncHandle:
    async def handle_async_request(self: typing.Any, request: Request) -> Response:
        config = _active.get()
        if config is None or not _should_trace(config, self):
            return await wrapped(self, request)
        with _traced_span(config, request) as span:
            response = await wrapped(self, request)
            _apply_response(config, span, request, response)
        return response

    return handle_async_request


_originals: dict[tuple[type[typing.Any], str], typing.Any] = {}


def _ensure_patched() -> None:
    global _patched
    if _patched:
        return
    for transport in _SYNC_TRANSPORTS:
        original = transport.handle_request
        _originals[(transport, "handle_request")] = original
        transport.handle_request = _instrumented_sync_handle(original)
    for transport in _ASYNC_TRANSPORTS:
        original = transport.handle_async_request
        _originals[(transport, "handle_async_request")] = original
        transport.handle_async_request = _instrumented_async_handle(original)
    _patched = True


def uninstrument() -> None:
    """Remove the global patch and clear any active configuration.

    This restores the original transport methods. Scoped `with instrument(): ...` blocks normally
    clean up after themselves; call this only to fully tear instrumentation down.
    """
    global _patched
    _active.set(None)
    if _patched:
        for (transport, name), original in _originals.items():
            setattr(transport, name, original)
        _originals.clear()
        _patched = False


def _collect_client_ids(client: typing.Any) -> set[int]:
    ids = {id(client._transport)}
    ids.update(id(transport) for transport in client._mounts.values() if transport is not None)
    return ids


def instrument(
    client: typing.Any | None = None,
    *,
    tracer_provider: TracerProvider | None = None,
    meter_provider: MeterProvider | None = None,
    capture_headers: bool = False,
    request_hook: RequestHook | None = None,
    response_hook: ResponseHook | None = None,
) -> contextlib.AbstractContextManager[None]:
    """Instrument httpx2 to emit OpenTelemetry client spans and a request-duration metric.

    Takes effect immediately and also returns a context manager that reverts the configuration on exit,
    so it works both process-wide and scoped to a `with` block:

        httpx2.instrument()                  # process-wide

        with httpx2.instrument():            # scoped, reverted on exit
            httpx2.get(url)

        with httpx2.instrument(client=c):    # only requests made by `c`
            c.get(url)

    Args:
        client: When given, only requests issued by this `Client`/`AsyncClient` are traced.
            When `None`, every request is traced.
        tracer_provider: Tracer provider to use, defaults to the global one.
        meter_provider: Meter provider to use, defaults to the global one.
        capture_headers: Capture request and response headers as span attributes.
        request_hook: Called with the span and request right after the span is created.
        response_hook: Called with the span, request, and response right before the span ends.

    Returns:
        A context manager that restores the previous configuration when exited.
    """
    _require_otel()
    _ensure_patched()
    clients = _collect_client_ids(client) if client is not None else None
    config = _build_config(tracer_provider, meter_provider, capture_headers, request_hook, response_hook, clients)
    token = _active.set(config)

    @contextlib.contextmanager
    def _scope() -> typing.Iterator[None]:
        try:
            yield
        finally:
            _active.reset(token)

    return _scope()
