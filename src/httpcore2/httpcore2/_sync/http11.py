from __future__ import annotations

import enum
import logging
import re
import ssl
import time
import types
import typing
from collections.abc import Generator

import h11

from .._backends.base import NetworkStream
from .._exceptions import (
    ConnectionNotAvailable,
    LocalProtocolError,
    RemoteProtocolError,
    WriteError,
    map_exceptions,
)
from .._models import Origin, Request, Response
from .._synchronization import Lock, ShieldCancellation
from .._trace import Trace
from .._utils import safe_iterate
from .interfaces import ConnectionInterface

logger = logging.getLogger("httpcore2.http11")


# A subset of `h11.Event` types supported by `_send_event`
H11SendEvent = h11.Request | h11.Data | h11.EndOfMessage


class HTTPConnectionState(enum.IntEnum):
    NEW = 0
    ACTIVE = 1
    IDLE = 2
    CLOSED = 3


# Mirrors h11's own header/body boundary (`h11._receivebuffer.blank_line_regex`):
# h11 tolerates a bare `\n` or `\n\r\n`, not just `\r\n\r\n`.
_HEADER_BLOCK_TERMINATOR_RE = re.compile(rb"\n\r?\n")


def _merge_duplicate_chunked_transfer_encoding(header_block: bytes) -> bytes:
    """
    Merge an exact-duplicate `Transfer-Encoding: chunked` header line into an
    earlier one, mirroring h11's existing tolerance for duplicate identical
    Content-Length headers (see https://github.com/pydantic/httpx2/issues/622).

    `header_block` must end with the header/body boundary matched by
    `_HEADER_BLOCK_TERMINATOR_RE`, boundary bytes included. Lines are split
    the same way h11 splits them (on `\n`, with one optional trailing `\r`
    stripped per line -- see `h11._receivebuffer.ReceiveBuffer.maybe_extract_lines`)
    so a header block using non-`\r\n` line endings is parsed identically to
    how h11 will parse it.

    Only ever *removes* bytes that are provably an exact, unfolded repeat of
    an earlier `Transfer-Encoding: chunked` line:

    - Header names are matched case-insensitively (legitimate per RFC 9110),
      but never stripped of surrounding whitespace -- a real header field has
      no whitespace between the name and the colon, so anything like
      `Transfer-Encoding : chunked` fails to match and is left for h11 to
      reject as an illegal header line.
    - A candidate line is skipped entirely if it starts with a fold-indicating
      space/tab (RFC 9112 obsolete line folding: it's a continuation of the
      *previous* header's value, not a standalone header) or if the following
      line does -- in the latter case deleting it would orphan that
      continuation, changing which header it folds into.

    Any other case (differing values, folded lines, malformed lines) is left
    completely untouched, so h11 still raises for it exactly as before.

    Never applies if the header block also contains a `Content-Length`
    header: every `\n`-split line's field name (the part before the first
    `:`, matched case-insensitively, not stripped of whitespace -- same
    reasoning as the `Transfer-Encoding` match above) is checked against
    `content-length` exactly. `Transfer-Encoding` combined with
    `Content-Length` is exactly the shape of the classic conflicting-framing
    request-smuggling primitive that RFC 9112 requires treating as an error;
    issue #622's actual reproductions never combine the two, so giving up the
    merge here costs nothing while closing off that class of ambiguity. Like
    the `Transfer-Encoding` match, this doesn't account for obsolete line
    folding, so a `Content-Length` header expressed only via a folded
    continuation line won't be detected -- an accepted, narrow gap, since an
    undetected fold is left untouched either way (see above).
    """
    if any(line.partition(b":")[0].lower() == b"content-length" for line in header_block.split(b"\n")):
        return header_block

    line_spans: list[tuple[bytes, int, int]] = []
    start = 0
    for match in re.finditer(rb"\n", header_block):
        end = match.end()
        content_end = match.start()
        if header_block[content_end - 1 : content_end] == b"\r":
            content_end -= 1
        line_spans.append((header_block[start:content_end], start, end))
        start = end

    # The final span is always the second half of the header/body boundary
    # itself (mirroring h11's own `del lines[-2:]`), never a real header line.
    header_line_spans = line_spans[:-1]

    seen_chunked_transfer_encoding = False
    delete_spans: list[tuple[int, int]] = []
    for index, (content, span_start, span_end) in enumerate(header_line_spans):
        if index == 0:
            continue  # the status line

        if content[:1] in (b" ", b"\t"):
            continue  # obsolete-line-fold continuation of the previous line

        next_content = header_line_spans[index + 1][0] if index + 1 < len(header_line_spans) else b""
        if next_content[:1] in (b" ", b"\t"):
            continue  # this line has its own fold continuation; leave it alone

        name, sep, value = content.partition(b":")
        if not (sep and name.lower() == b"transfer-encoding" and value.strip(b" \t").lower() == b"chunked"):
            continue

        if seen_chunked_transfer_encoding:
            delete_spans.append((span_start, span_end))
        else:
            seen_chunked_transfer_encoding = True

    if not delete_spans:
        return header_block

    merged = bytearray()
    cursor = 0
    for delete_start, delete_end in delete_spans:
        merged += header_block[cursor:delete_start]
        cursor = delete_end
    merged += header_block[cursor:]
    return bytes(merged)


class HTTP11Connection(ConnectionInterface):
    READ_NUM_BYTES = 64 * 1024
    MAX_INCOMPLETE_EVENT_SIZE = 100 * 1024

    def __init__(
        self,
        origin: Origin,
        stream: NetworkStream,
        keepalive_expiry: float | None = None,
    ) -> None:
        self._origin = origin
        self._network_stream = stream
        self._keepalive_expiry: float | None = keepalive_expiry
        self._expire_at: float | None = None
        self._state = HTTPConnectionState.NEW
        self._state_lock = Lock()
        self._request_count = 0
        self._h11_state = h11.Connection(
            our_role=h11.CLIENT,
            max_incomplete_event_size=self.MAX_INCOMPLETE_EVENT_SIZE,
        )
        # Accumulates bytes for the response header block currently being
        # assembled, so they can be normalized (see
        # `_merge_duplicate_chunked_transfer_encoding`) before h11 sees them.
        # Only ever appended to while `self._h11_state.their_state` is
        # `h11.SEND_RESPONSE` -- see `_receive_event`. A `bytearray` (not
        # `bytes`) so repeated `+=` don't reallocate-and-copy the whole thing
        # each time.
        self._response_header_buffer = bytearray()
        # How far into `_response_header_buffer` the terminator search has
        # already ruled out a match, so each new read only rescans the tail
        # instead of the whole accumulated buffer -- mirrors h11's own
        # `ReceiveBuffer._multiple_lines_search` (see its module docstring:
        # "reading short segments out of a long buffer MUST be O(bytes read)
        # to avoid DoS issues"). The terminator is at most 3 bytes, so it's
        # always safe to resume 2 bytes before the end of what's already
        # been scanned.
        self._response_header_search_from = 0
        # Bytes already read from the network that logically belong to
        # whatever comes *after* the header block just flushed above (e.g. a
        # 1xx interim response's own headers, followed immediately by the
        # final response's headers in the same read) -- reprocessed through
        # the same logic before another real network read is attempted.
        self._pending_read_ahead = b""

    def handle_request(self, request: Request) -> Response:
        if not self.can_handle_request(request.url.origin):
            raise RuntimeError(f"Attempted to send request to {request.url.origin} on connection to {self._origin}")

        with self._state_lock:
            if self._state in (HTTPConnectionState.NEW, HTTPConnectionState.IDLE):
                self._request_count += 1
                self._state = HTTPConnectionState.ACTIVE
                self._expire_at = None
            else:
                raise ConnectionNotAvailable()

        try:
            kwargs = {"request": request}
            try:
                with Trace("send_request_headers", logger, request, kwargs) as trace:
                    self._send_request_headers(**kwargs)
                with Trace("send_request_body", logger, request, kwargs) as trace:
                    self._send_request_body(**kwargs)
            except WriteError:
                # If we get a write error while we're writing the request,
                # then we suppress this error and move on to attempting to
                # read the response. Servers can sometimes close the request
                # preemptively and then respond with a well formed HTTP
                # error response.
                pass

            with Trace("receive_response_headers", logger, request, kwargs) as trace:
                (
                    http_version,
                    status,
                    reason_phrase,
                    headers,
                    trailing_data,
                ) = self._receive_response_headers(**kwargs)
                trace.return_value = (
                    http_version,
                    status,
                    reason_phrase,
                    headers,
                )

            network_stream = self._network_stream

            # CONNECT or Upgrade request
            if (status == 101) or ((request.method == b"CONNECT") and (200 <= status < 300)):
                network_stream = HTTP11UpgradeStream(network_stream, trailing_data)

            return Response(
                status=status,
                headers=headers,
                content=HTTP11ConnectionByteStream(self, request),
                extensions={
                    "http_version": http_version,
                    "reason_phrase": reason_phrase,
                    "network_stream": network_stream,
                },
            )
        except BaseException as exc:
            with ShieldCancellation():
                with Trace("response_closed", logger, request) as trace:
                    self._response_closed()
            raise exc

    # Sending the request...

    def _send_request_headers(self, request: Request) -> None:
        timeouts = request.extensions.get("timeout", {})
        timeout = timeouts.get("write", None)

        with map_exceptions({h11.LocalProtocolError: LocalProtocolError}):
            event = h11.Request(
                method=request.method,
                target=request.url.target,
                headers=request.headers,
            )
        self._send_event(event, timeout=timeout)

    def _send_request_body(self, request: Request) -> None:
        timeouts = request.extensions.get("timeout", {})
        timeout = timeouts.get("write", None)

        assert isinstance(request.stream, typing.Iterable)
        with safe_iterate(request.stream) as iterator:
            for chunk in iterator:
                event = h11.Data(data=chunk)
                self._send_event(event, timeout=timeout)

        self._send_event(h11.EndOfMessage(), timeout=timeout)

    def _send_event(self, event: h11.Event, timeout: float | None = None) -> None:
        bytes_to_send = self._h11_state.send(event)
        if bytes_to_send is not None:
            self._network_stream.write(bytes_to_send, timeout=timeout)

    # Receiving the response...

    def _receive_response_headers(
        self, request: Request
    ) -> tuple[bytes, int, bytes, list[tuple[bytes, bytes]], bytes]:
        timeouts = request.extensions.get("timeout", {})
        timeout = timeouts.get("read", None)

        while True:
            event = self._receive_event(timeout=timeout)
            if isinstance(event, h11.Response):
                break
            if isinstance(event, h11.InformationalResponse) and event.status_code == 101:
                break

        http_version = b"HTTP/" + event.http_version

        # h11 version 0.11+ supports a `raw_items` interface to get the
        # raw header casing, rather than the enforced lowercase headers.
        headers = event.headers.raw_items()

        # `_pending_read_ahead` may hold bytes read alongside this response's
        # headers that h11 was never given (see `_receive_event`) -- e.g. the
        # leading bytes of an upgraded protocol, read in the same chunk as
        # the 101 response's own headers. Combine it with h11's own
        # (separately-tracked) trailing data, the same non-destructive read
        # in both cases: for an ordinary response, this value is unused by
        # the caller and `_pending_read_ahead` is left intact for
        # `_receive_response_body`'s own `_receive_event` calls to drain.
        h11_trailing_data, _ = self._h11_state.trailing_data
        trailing_data = h11_trailing_data + self._pending_read_ahead

        return http_version, event.status_code, event.reason, headers, trailing_data

    def _receive_response_body(self, request: Request) -> Generator[bytes]:
        timeouts = request.extensions.get("timeout", {})
        timeout = timeouts.get("read", None)

        while True:
            event = self._receive_event(timeout=timeout)
            if isinstance(event, h11.Data):
                yield bytes(event.data)
            elif isinstance(event, (h11.EndOfMessage, h11.PAUSED)):
                break

    def _receive_event(self, timeout: float | None = None) -> h11.Event | type[h11.PAUSED]:
        while True:
            with map_exceptions({h11.RemoteProtocolError: RemoteProtocolError}):
                event = self._h11_state.next_event()

            if event is h11.NEED_DATA:
                if self._pending_read_ahead:
                    # Bytes already read that belong to whatever comes next
                    # (see `_pending_read_ahead`'s docstring in `__init__`) --
                    # reprocess those before touching the network again.
                    data, self._pending_read_ahead = self._pending_read_ahead, b""
                else:
                    data = self._network_stream.read(self.READ_NUM_BYTES, timeout=timeout)

                    # If we feed this case through h11 we'll raise an exception
                    # like:
                    #
                    #     httpcore2.RemoteProtocolError: can't handle event type
                    #     ConnectionClosed when role=SERVER and state=SEND_RESPONSE
                    #
                    # Which is accurate, but not very informative from an
                    # end-user perspective. Instead we handle this case
                    # distinctly and treat it as a ConnectError.
                    if data == b"" and self._h11_state.their_state == h11.SEND_RESPONSE:
                        msg = "Server disconnected without sending a response."
                        raise RemoteProtocolError(msg)

                if self._h11_state.their_state != h11.SEND_RESPONSE or (
                    not self._response_header_buffer and self._h11_state.trailing_data[0]
                ):
                    # Either not currently receiving a response's
                    # status-line/headers (e.g. mid-body) -- nothing to
                    # normalize, feed it straight through as before.
                    #
                    # Or: we're about to *start* accumulating a new header
                    # block, but h11 is already sitting on unparsed bytes of
                    # its own (e.g. a pipelined response, or the tail end of
                    # a previous cycle that arrived in the same read as this
                    # one). Our boundary search only looks inside our own
                    # buffer, so if the real header/body boundary straddles
                    # that hidden junction, searching this new data alone
                    # could lock onto a later, coincidental match -- inside
                    # the response body -- and corrupt it. Bail out of
                    # normalizing this response rather than risk that; h11
                    # still handles the duplicate-header case exactly as it
                    # did before this fix existed.
                    self._h11_state.receive_data(data)
                else:
                    self._response_header_buffer += data
                    match = _HEADER_BLOCK_TERMINATOR_RE.search(
                        self._response_header_buffer, self._response_header_search_from
                    )
                    if match is not None:
                        header_block = bytes(self._response_header_buffer[: match.end()])
                        # Whatever follows is held back rather than fed to h11
                        # here -- it may be another header block (an interim
                        # response ahead of the final one) that still needs
                        # its own normalization pass, which the top of this
                        # loop will give it once h11 asks for more data.
                        self._pending_read_ahead = bytes(self._response_header_buffer[match.end() :])
                        self._response_header_buffer = bytearray()
                        self._response_header_search_from = 0
                        self._h11_state.receive_data(_merge_duplicate_chunked_transfer_encoding(header_block))
                    elif len(self._response_header_buffer) > self.MAX_INCOMPLETE_EVENT_SIZE:
                        # No boundary within the size bound h11 itself enforces
                        # -- stop buffering and let h11 apply its own limit.
                        buffered = bytes(self._response_header_buffer)
                        self._response_header_buffer = bytearray()
                        self._response_header_search_from = 0
                        self._h11_state.receive_data(buffered)
                    else:
                        # Boundary not found yet -- loop back without feeding
                        # h11 anything (and without touching
                        # `_pending_read_ahead`, which stays empty); next_event()
                        # will return NEED_DATA again, and since there's still
                        # no read-ahead to drain, this reads the network for
                        # more. The terminator is at most 3 bytes, so the next
                        # search can safely skip everything except the last 2
                        # bytes already scanned -- without this, accumulating
                        # a large header block byte-by-byte is O(n^2).
                        self._response_header_search_from = max(0, len(self._response_header_buffer) - 2)
            else:
                # mypy fails to narrow the type in the above if statement above
                return event  # type: ignore[return-value]

    def _response_closed(self) -> None:
        with self._state_lock:
            if self._h11_state.our_state is h11.DONE and self._h11_state.their_state is h11.DONE:
                self._state = HTTPConnectionState.IDLE
                self._h11_state.start_next_cycle()
                if self._keepalive_expiry is not None:
                    now = time.monotonic()
                    self._expire_at = now + self._keepalive_expiry
            else:
                self.close()

    # Once the connection is no longer required...

    def close(self) -> None:
        # Note that this method unilaterally closes the connection, and does
        # not have any kind of locking in place around it.
        self._state = HTTPConnectionState.CLOSED
        self._network_stream.close()

    # The ConnectionInterface methods provide information about the state of
    # the connection, allowing for a connection pooling implementation to
    # determine when to reuse and when to close the connection...

    def can_handle_request(self, origin: Origin) -> bool:
        return origin == self._origin

    def is_connected(self) -> bool:
        return not self.is_closed()

    def is_available(self) -> bool:
        # Note that HTTP/1.1 connections in the "NEW" state are not treated as
        # being "available". The control flow which created the connection will
        # be able to send an outgoing request, but the connection will not be
        # acquired from the connection pool for any other request.
        return self._state == HTTPConnectionState.IDLE

    def has_expired(self) -> bool:
        now = time.monotonic()
        # Read `_expire_at` once into a local: on free-threaded builds another
        # thread may reset it to `None` between the check and the comparison.
        expire_at = self._expire_at
        keepalive_expired = expire_at is not None and now > expire_at

        # If the HTTP connection is idle but the socket is readable, then the
        # only valid state is that the socket is about to return b"", indicating
        # a server-initiated disconnect.
        server_disconnected = self._state == HTTPConnectionState.IDLE and self._network_stream.get_extra_info(
            "is_readable"
        )

        return keepalive_expired or server_disconnected

    def is_idle(self) -> bool:
        return self._state == HTTPConnectionState.IDLE

    def is_closed(self) -> bool:
        return self._state == HTTPConnectionState.CLOSED

    def info(self) -> str:
        origin = str(self._origin)
        return f"{origin!r}, HTTP/1.1, {self._state.name}, Request Count: {self._request_count}"

    def __repr__(self) -> str:
        class_name = self.__class__.__name__
        origin = str(self._origin)
        return f"<{class_name} [{origin!r}, {self._state.name}, Request Count: {self._request_count}]>"

    # These context managers are not used in the standard flow, but are
    # useful for testing or working with connection instances directly.

    def __enter__(self) -> HTTP11Connection:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: types.TracebackType | None = None,
    ) -> None:
        self.close()


class HTTP11ConnectionByteStream:
    def __init__(self, connection: HTTP11Connection, request: Request) -> None:
        self._connection = connection
        self._request = request
        self._closed = False

    def __iter__(self) -> Generator[bytes]:
        kwargs = {"request": self._request}
        try:
            with Trace("receive_response_body", logger, self._request, kwargs):
                with safe_iterate(self._connection._receive_response_body(**kwargs)) as iterator:
                    for chunk in iterator:
                        yield chunk
        except BaseException as exc:
            # If we get an exception while streaming the response,
            # we want to close the response (and possibly the connection)
            # before raising that exception.
            with ShieldCancellation():
                self.close()
            raise exc

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            with Trace("response_closed", logger, self._request):
                self._connection._response_closed()


class HTTP11UpgradeStream(NetworkStream):
    def __init__(self, stream: NetworkStream, leading_data: bytes) -> None:
        self._stream = stream
        self._leading_data = leading_data

    def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        if self._leading_data:
            buffer = self._leading_data[:max_bytes]
            self._leading_data = self._leading_data[max_bytes:]
            return buffer
        else:
            return self._stream.read(max_bytes, timeout)

    def write(self, buffer: bytes, timeout: float | None = None) -> None:
        self._stream.write(buffer, timeout)

    def close(self) -> None:
        self._stream.close()

    def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> NetworkStream:
        return self._stream.start_tls(ssl_context, server_hostname, timeout)

    def get_extra_info(self, info: str) -> typing.Any:
        return self._stream.get_extra_info(info)
