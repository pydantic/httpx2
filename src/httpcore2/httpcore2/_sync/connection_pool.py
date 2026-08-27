from __future__ import annotations

import ssl
import sys
import time
import types
import typing
from collections import deque
from collections.abc import Generator

from .._backends.sync import SyncBackend
from .._backends.base import SOCKET_OPTION, NetworkBackend
from .._exceptions import ConnectionNotAvailable, UnsupportedProtocol
from .._models import Origin, Proxy, Request, Response
from .._synchronization import Event, ShieldCancellation, ThreadLock
from .._utils import safe_iterate
from .connection import HTTPConnection
from .interfaces import ConnectionInterface, RequestInterface


class PoolRequest:
    def __init__(self, request: Request) -> None:
        self.request = request
        self.connection: ConnectionInterface | None = None
        # Created only by a request that actually has to wait: in the common
        # case a connection is assigned before `wait_for_connection` is called.
        self._connection_acquired: Event | None = None

    def assign_to_connection(self, connection: ConnectionInterface | None) -> None:
        self.connection = connection
        if self._connection_acquired is not None:
            self._connection_acquired.set()

    def clear_connection(self) -> None:
        self.connection = None
        self._connection_acquired = None

    def wait_for_connection(self, timeout: float | None = None) -> ConnectionInterface:
        if self.connection is None:
            self._connection_acquired = Event()
            # Re-check after publishing the event: in the threaded case an
            # assignment may have landed between the first check and here.
            if self.connection is None:
                self._connection_acquired.wait(timeout=timeout)
        assert self.connection is not None
        return self.connection

    def is_queued(self) -> bool:
        return self.connection is None


class ConnectionPool(RequestInterface):
    """
    A connection pool for making HTTP requests.
    """

    def __init__(
        self,
        ssl_context: ssl.SSLContext | None = None,
        proxy: Proxy | None = None,
        max_connections: int | None = 10,
        max_keepalive_connections: int | None = None,
        keepalive_expiry: float | None = None,
        http1: bool = True,
        http2: bool = False,
        retries: int = 0,
        local_address: str | None = None,
        uds: str | None = None,
        network_backend: NetworkBackend | None = None,
        socket_options: typing.Iterable[SOCKET_OPTION] | None = None,
    ) -> None:
        """
        A connection pool for making HTTP requests.

        Parameters:
            ssl_context: An SSL context to use for verifying connections.
                If not specified, the default `httpcore2.default_ssl_context()`
                will be used.
            max_connections: The maximum number of concurrent HTTP connections that
                the pool should allow. Any attempt to send a request on a pool that
                would exceed this amount will block until a connection is available.
            max_keepalive_connections: The maximum number of idle HTTP connections
                that will be maintained in the pool.
            keepalive_expiry: The duration in seconds that an idle HTTP connection
                may be maintained for before being expired from the pool.
            http1: A boolean indicating if HTTP/1.1 requests should be supported
                by the connection pool. Defaults to True.
            http2: A boolean indicating if HTTP/2 requests should be supported by
                the connection pool. Defaults to False.
            retries: The maximum number of retries when trying to establish a
                connection.
            local_address: Local address to connect from. Can also be used to connect
                using a particular address family. Using `local_address="0.0.0.0"`
                will connect using an `AF_INET` address (IPv4), while using
                `local_address="::"` will connect using an `AF_INET6` address (IPv6).
            uds: Path to a Unix Domain Socket to use instead of TCP sockets.
            network_backend: A backend instance to use for handling network I/O.
            socket_options: Socket options that have to be included
             in the TCP socket when the connection was established.
        """
        self._ssl_context = ssl_context
        self._proxy = proxy
        self._max_connections = sys.maxsize if max_connections is None else max_connections
        self._max_keepalive_connections = (
            sys.maxsize if max_keepalive_connections is None else max_keepalive_connections
        )
        self._max_keepalive_connections = min(self._max_connections, self._max_keepalive_connections)

        self._keepalive_expiry = keepalive_expiry
        self._http1 = http1
        self._http2 = http2
        self._retries = retries
        self._local_address = local_address
        self._uds = uds

        self._network_backend = SyncBackend() if network_backend is None else network_backend
        self._socket_options = socket_options

        # The pool's state is kept incrementally, so that handling a request or
        # releasing a connection costs O(1) rather than a scan of every
        # connection and every in-flight request.
        #
        # Every connection owned by the pool, in creation order, with its origin.
        self._connections: dict[ConnectionInterface, Origin] = {}
        # Every in-flight request, in arrival order.
        self._requests: dict[PoolRequest, None] = {}
        # Requests still waiting for a connection, in arrival order.
        self._queued: deque[PoolRequest] = deque()
        # The number of in-flight requests holding each connection. An idle
        # HTTP/1.1 connection with a holder is reserved by a request that has
        # not sent on it yet.
        self._holders: dict[ConnectionInterface, int] = {}
        # Idle HTTP/1.1 connections free for reuse, oldest first, mapped to the
        # time they became idle, plus the same connections keyed by origin.
        self._idle: dict[ConnectionInterface, float] = {}
        self._idle_by_origin: dict[Origin, dict[ConnectionInterface, None]] = {}
        # Connections that may take a request while not idle: HTTP/2 connections,
        # and connections that may still negotiate HTTP/2.
        self._sharable_by_origin: dict[Origin, dict[ConnectionInterface, None]] = {}
        # Connections whose in-flight request has ended since the last pass,
        # to be re-examined by the next pass.
        self._released: list[ConnectionInterface] = []
        # When the oldest idle connection may have expired, or `None` while
        # no idle connection is subject to expiry.
        self._expiry_due: float | None = None

        # We only mutate the state of the connection pool within an 'optional_thread_lock'
        # context. This holds a threading lock unless we're running in async mode,
        # in which case it is a no-op.
        self._optional_thread_lock = ThreadLock()

    def create_connection(self, origin: Origin) -> ConnectionInterface:
        if self._proxy is not None:
            if self._proxy.url.scheme in (b"socks5", b"socks5h"):
                from .socks_proxy import Socks5Connection

                return Socks5Connection(
                    proxy_origin=self._proxy.url.origin,
                    proxy_auth=self._proxy.auth,
                    remote_origin=origin,
                    ssl_context=self._ssl_context,
                    keepalive_expiry=self._keepalive_expiry,
                    http1=self._http1,
                    http2=self._http2,
                    network_backend=self._network_backend,
                )
            elif origin.scheme == b"http":
                from .http_proxy import ForwardHTTPConnection

                return ForwardHTTPConnection(
                    proxy_origin=self._proxy.url.origin,
                    proxy_headers=self._proxy.headers,
                    proxy_ssl_context=self._proxy.ssl_context,
                    remote_origin=origin,
                    keepalive_expiry=self._keepalive_expiry,
                    network_backend=self._network_backend,
                )
            from .http_proxy import TunnelHTTPConnection

            return TunnelHTTPConnection(
                proxy_origin=self._proxy.url.origin,
                proxy_headers=self._proxy.headers,
                proxy_ssl_context=self._proxy.ssl_context,
                remote_origin=origin,
                ssl_context=self._ssl_context,
                keepalive_expiry=self._keepalive_expiry,
                http1=self._http1,
                http2=self._http2,
                network_backend=self._network_backend,
            )

        return HTTPConnection(
            origin=origin,
            ssl_context=self._ssl_context,
            keepalive_expiry=self._keepalive_expiry,
            http1=self._http1,
            http2=self._http2,
            retries=self._retries,
            local_address=self._local_address,
            uds=self._uds,
            network_backend=self._network_backend,
            socket_options=self._socket_options,
        )

    @property
    def connections(self) -> list[ConnectionInterface]:
        """
        Return a list of the connections currently in the pool.

        For example:

        ```python
        >>> pool.connections
        [
            <HTTPConnection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 6]>,
            <HTTPConnection ['https://example.com:443', HTTP/1.1, IDLE, Request Count: 9]> ,
            <HTTPConnection ['http://example.com:80', HTTP/1.1, IDLE, Request Count: 1]>,
        ]
        ```
        """
        return list(self._connections)

    def handle_request(self, request: Request) -> Response:
        """
        Send an HTTP request, and return an HTTP response.

        This is the core implementation that is called into by `.request()` or `.stream()`.
        """
        scheme = request.url.scheme.decode()
        if scheme == "":
            raise UnsupportedProtocol("Request URL is missing an 'http://' or 'https://' protocol.")
        if scheme not in ("http", "https", "ws", "wss"):
            raise UnsupportedProtocol(f"Request URL has an unsupported protocol '{scheme}://'.")

        timeouts = request.extensions.get("timeout", {})
        timeout = timeouts.get("pool", None)

        with self._optional_thread_lock:
            # Add the incoming request to our request queue.
            pool_request = PoolRequest(request)
            self._requests[pool_request] = None
            self._queued.append(pool_request)

        try:
            while True:
                with self._optional_thread_lock:
                    # Assign incoming requests to available connections,
                    # closing or creating new connections as required.
                    closing = self._assign_requests_to_connections()
                self._close_connections(closing)

                # Wait until this request has an assigned connection.
                connection = pool_request.wait_for_connection(timeout=timeout)

                try:
                    # Send the request on the assigned connection.
                    response = connection.handle_request(pool_request.request)
                except ConnectionNotAvailable:
                    # In some cases a connection may initially be available to
                    # handle a request, but then become unavailable.
                    #
                    # In this case we clear the connection and try again, keeping
                    # the request at the front of the queue.
                    with self._optional_thread_lock:
                        self._release_connection(pool_request)
                        pool_request.clear_connection()
                        self._queued.appendleft(pool_request)
                else:
                    break  # pragma: no cover

        except BaseException as exc:
            with self._optional_thread_lock:
                # For any exception or cancellation we remove the request from
                # the queue, and then re-assign requests to connections.
                self._release_connection(pool_request)
                if pool_request.is_queued():
                    self._queued.remove(pool_request)
                del self._requests[pool_request]
                closing = self._assign_requests_to_connections()

            self._close_connections(closing)
            raise exc from None

        # Return the response. Note that in this case we still have to manage
        # the point at which the response is closed.
        assert isinstance(response.stream, typing.Iterable)
        return Response(
            status=response.status,
            headers=response.headers,
            content=PoolByteStream(stream=response.stream, pool_request=pool_request, pool=self),
            extensions=response.extensions,
        )

    def _assign_requests_to_connections(self) -> list[ConnectionInterface]:
        """
        Manage the state of the connection pool, assigning queued
        requests to connections as available.

        Called whenever a request is added to the pool, or a request
        releases its connection.

        Any closing connections are returned, allowing the I/O for closing
        those connections to be handled separately.
        """
        closing_connections: list[ConnectionInterface] = []

        # Connections released since the last pass become reusable, or are
        # dropped if their request left them closed.
        if self._released:
            released, self._released = self._released, []
            for connection in released:
                self._examine_released_connection(connection, closing_connections)

        # Expire idle connections, but only once the oldest may have expired.
        if self._expiry_due is not None and time.monotonic() >= self._expiry_due:
            self._expire_idle_connections(closing_connections)

        # Assign queued requests to connections, in arrival order. A request
        # that cannot be served yet stays queued without blocking later
        # requests that can reuse an existing connection.
        if self._queued:
            still_queued: deque[PoolRequest] = deque()
            for pool_request in self._queued:
                acquired = self._acquire_connection(pool_request.request.url.origin, closing_connections)
                if acquired is None:
                    still_queued.append(pool_request)
                else:
                    self._reserve_connection(pool_request, acquired)
            self._queued = still_queued

        return closing_connections

    def _acquire_connection(
        self, origin: Origin, closing_connections: list[ConnectionInterface]
    ) -> ConnectionInterface | None:
        # There are three cases for how we may be able to handle the request:
        #
        # 1. There is an existing connection that can handle the request.
        # 2. We can create a new connection to handle the request.
        # 3. We can close an idle connection and then create a new connection
        #    to handle the request.
        idle = self._idle_by_origin.get(origin)
        while idle:
            connection = next(iter(idle))
            self._remove_idle(connection)
            # `has_expired()` also probes the socket, catching a connection
            # the server closed while it sat idle.
            if connection.has_expired():
                self._drop_connection(connection)
                closing_connections.append(connection)
                continue
            return connection

        for connection in self._live_sharable_connections(origin, closing_connections):
            if connection.is_available():
                return connection

        if len(self._connections) < self._max_connections:
            return self._add_connection(origin)

        if self._idle:
            connection = next(iter(self._idle))
            self._remove_idle(connection)
            self._drop_connection(connection)
            closing_connections.append(connection)
            return self._add_connection(origin)

        return None

    def _examine_released_connection(
        self, connection: ConnectionInterface, closing_connections: list[ConnectionInterface]
    ) -> None:
        origin = self._connections.get(connection)
        if origin is None:
            # Already dropped, for example by the pool closing.
            return
        if connection.is_closed():
            # The request left the connection closed; there is no socket to close.
            self._drop_connection(connection)
            return
        if self._holders.get(connection, 0):
            # Still serving another request, or reserved by a queued request.
            return
        if not connection.is_connected():
            # Garbage: a NEW-state connection whose request was cancelled
            # before the TCP handshake completed. Drop it without closing
            # (there is no socket to close yet).
            self._drop_connection(connection)
            return

        if connection.can_multiplex():
            self._sharable_by_origin.setdefault(origin, {})[connection] = None
            if connection.is_idle():
                self._schedule_expiry(time.monotonic())
            return

        # An HTTP/1.1 connection: no longer a candidate for HTTP/2 multiplexing.
        sharable = self._sharable_by_origin.get(origin)
        if sharable is not None:
            sharable.pop(connection, None)

        if connection.is_idle():
            now = time.monotonic()
            self._idle[connection] = now
            self._idle_by_origin.setdefault(origin, {})[connection] = None
            self._schedule_expiry(now)

            # Enforce `max_keepalive_connections`, closing the longest idle first.
            while len(self._idle) > self._max_keepalive_connections:
                surplus = next(iter(self._idle))
                self._remove_idle(surplus)
                self._drop_connection(surplus)
                closing_connections.append(surplus)

    def _expire_idle_connections(self, closing_connections: list[ConnectionInterface]) -> None:
        for connection in list(self._idle):
            if connection.has_expired():
                self._remove_idle(connection)
                self._drop_connection(connection)
                closing_connections.append(connection)

        sharable_idle = False
        for origin in list(self._sharable_by_origin):
            for connection in self._live_sharable_connections(origin, closing_connections):
                if connection.is_idle():
                    sharable_idle = True

        self._expiry_due = None
        assert self._keepalive_expiry is not None
        if self._idle:
            self._expiry_due = next(iter(self._idle.values())) + self._keepalive_expiry
        elif sharable_idle:
            self._expiry_due = time.monotonic() + self._keepalive_expiry

    def _live_sharable_connections(
        self, origin: Origin, closing_connections: list[ConnectionInterface]
    ) -> list[ConnectionInterface]:
        # Drop sharable connections that were closed or expired while the pool
        # was not looking at them, returning the ones still worth using.
        live: list[ConnectionInterface] = []
        for connection in list(self._sharable_by_origin.get(origin, ())):
            if connection.is_closed():
                self._drop_connection(connection)
            elif connection.has_expired():
                self._drop_connection(connection)
                closing_connections.append(connection)
            else:
                live.append(connection)
        return live

    def _schedule_expiry(self, now: float) -> None:
        if self._expiry_due is None and self._keepalive_expiry is not None:
            self._expiry_due = now + self._keepalive_expiry

    def _add_connection(self, origin: Origin) -> ConnectionInterface:
        connection = self.create_connection(origin)
        self._connections[connection] = origin
        if connection.is_available():
            # Not yet connected, but may negotiate HTTP/2 and then take
            # further requests concurrently.
            self._sharable_by_origin.setdefault(origin, {})[connection] = None
        return connection

    def _remove_idle(self, connection: ConnectionInterface) -> None:
        del self._idle[connection]
        origin = self._connections[connection]
        by_origin = self._idle_by_origin[origin]
        del by_origin[connection]
        if not by_origin:
            del self._idle_by_origin[origin]

    def _drop_connection(self, connection: ConnectionInterface) -> None:
        origin = self._connections.pop(connection)
        self._holders.pop(connection, None)
        sharable = self._sharable_by_origin.get(origin)
        if sharable is not None:
            sharable.pop(connection, None)
            if not sharable:
                del self._sharable_by_origin[origin]

    def _reserve_connection(self, pool_request: PoolRequest, connection: ConnectionInterface) -> None:
        self._holders[connection] = self._holders.get(connection, 0) + 1
        pool_request.assign_to_connection(connection)

    def _release_connection(self, pool_request: PoolRequest) -> None:
        connection = pool_request.connection
        if connection is None:
            return
        count = self._holders.get(connection, 0) - 1
        if count > 0:
            self._holders[connection] = count
        else:
            self._holders.pop(connection, None)
        self._released.append(connection)

    def _close_connections(self, closing: list[ConnectionInterface]) -> None:
        # Close connections which have been removed from the pool.
        if not closing:
            return
        with ShieldCancellation():
            for connection in closing:
                connection.close()

    def close(self) -> None:
        # Explicitly close the connection pool.
        # Clears all existing requests and connections.
        with self._optional_thread_lock:
            closing_connections = list(self._connections)
            self._connections = {}
            self._holders = {}
            self._idle = {}
            self._idle_by_origin = {}
            self._sharable_by_origin = {}
            self._released = []
            self._expiry_due = None
        self._close_connections(closing_connections)

    def __enter__(self) -> ConnectionPool:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: types.TracebackType | None = None,
    ) -> None:
        self.close()

    def __repr__(self) -> str:
        class_name = self.__class__.__name__
        with self._optional_thread_lock:
            num_queued_requests = len(self._queued)
            num_active_requests = len(self._requests) - num_queued_requests
            connection_is_idle = [connection.is_idle() for connection in self._connections]

            num_active_connections = connection_is_idle.count(False)
            num_idle_connections = connection_is_idle.count(True)

        requests_info = f"Requests: {num_active_requests} active, {num_queued_requests} queued"
        connection_info = f"Connections: {num_active_connections} active, {num_idle_connections} idle"

        return f"<{class_name} [{requests_info} | {connection_info}]>"


class PoolByteStream:
    def __init__(
        self,
        stream: typing.Iterable[bytes],
        pool_request: PoolRequest,
        pool: ConnectionPool,
    ) -> None:
        self._stream = stream
        self._pool_request = pool_request
        self._pool = pool
        self._closed = False

    def __iter__(self) -> Generator[bytes]:
        with safe_iterate(self._stream) as iterator:
            for chunk in iterator:
                yield chunk

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            with ShieldCancellation():
                if hasattr(self._stream, "close"):
                    self._stream.close()

            with self._pool._optional_thread_lock:
                self._pool._release_connection(self._pool_request)
                del self._pool._requests[self._pool_request]
                closing = self._pool._assign_requests_to_connections()

            self._pool._close_connections(closing)
