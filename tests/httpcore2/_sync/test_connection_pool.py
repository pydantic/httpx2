import logging
import typing

import hpack
import hyperframe.frame
import pytest
from tests.httpcore2 import concurrency

import httpcore2



def test_connection_pool_with_keepalive() -> None:
    """
    By default HTTP/1.1 requests should be returned to the connection pool.
    """
    network_backend = httpcore2.MockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    with httpcore2.ConnectionPool(network_backend=network_backend, max_keepalive_connections=1) as pool:
        # Sending an initial request, which once complete will return to the pool, IDLE.
        with pool.stream("GET", "https://example.com/") as response:
            info = [repr(c) for c in pool.connections]
            assert info == ["<HTTPConnection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>"]
            assert repr(pool) == "<ConnectionPool [Requests: 1 active, 0 queued | Connections: 1 active, 0 idle]>"
            response.read()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in pool.connections]
        assert info == ["<HTTPConnection ['https://example.com:443', HTTP/1.1, IDLE, Request Count: 1]>"]
        assert repr(pool) == "<ConnectionPool [Requests: 0 active, 0 queued | Connections: 0 active, 1 idle]>"

        # Sending a second request to the same origin will reuse the existing IDLE connection.
        with pool.stream("GET", "https://example.com/") as response:
            info = [repr(c) for c in pool.connections]
            assert info == ["<HTTPConnection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 2]>"]
            assert repr(pool) == "<ConnectionPool [Requests: 1 active, 0 queued | Connections: 1 active, 0 idle]>"
            response.read()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in pool.connections]
        assert info == ["<HTTPConnection ['https://example.com:443', HTTP/1.1, IDLE, Request Count: 2]>"]
        assert repr(pool) == "<ConnectionPool [Requests: 0 active, 0 queued | Connections: 0 active, 1 idle]>"

        # Sending a request to a different origin will not reuse the existing IDLE connection.
        with (
            pool.stream("GET", "http://example.com/") as response_1,
            pool.stream("GET", "http://example.com/") as response_2,
        ):
            info = [repr(c) for c in pool.connections]
            assert info == [
                "<HTTPConnection ['https://example.com:443', HTTP/1.1, IDLE, Request Count: 2]>",
                "<HTTPConnection ['http://example.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
                "<HTTPConnection ['http://example.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
            ]
            assert repr(pool) == "<ConnectionPool [Requests: 2 active, 0 queued | Connections: 2 active, 1 idle]>"
            response_1.read()
            response_2.read()

        assert response_1.status == 200
        assert response_1.content == b"Hello, world!"
        assert response_2.status == 200
        assert response_2.content == b"Hello, world!"
        info = [repr(c) for c in pool.connections]
        assert info == [
            "<HTTPConnection ['http://example.com:80', HTTP/1.1, IDLE, Request Count: 1]>",
        ]
        assert repr(pool) == "<ConnectionPool [Requests: 0 active, 0 queued | Connections: 0 active, 1 idle]>"



def test_connection_pool_with_close() -> None:
    """
    HTTP/1.1 requests that include a 'Connection: Close' header should
    not be returned to the connection pool.
    """
    network_backend = httpcore2.MockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    with httpcore2.ConnectionPool(network_backend=network_backend) as pool:
        # Sending an initial request, which once complete will not return to the pool.
        with pool.stream("GET", "https://example.com/", headers={"Connection": "close"}) as response:
            info = [repr(c) for c in pool.connections]
            assert info == ["<HTTPConnection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>"]
            response.read()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in pool.connections]
        assert info == []



def test_connection_pool_with_http2() -> None:
    """
    Test a connection pool with HTTP/2 requests.
    """
    network_backend = httpcore2.MockBackend(
        buffer=[
            hyperframe.frame.SettingsFrame().serialize(),
            hyperframe.frame.HeadersFrame(
                stream_id=1,
                data=hpack.Encoder().encode(
                    [
                        (b":status", b"200"),
                        (b"content-type", b"plain/text"),
                    ]
                ),
                flags=["END_HEADERS"],
            ).serialize(),
            hyperframe.frame.DataFrame(stream_id=1, data=b"Hello, world!", flags=["END_STREAM"]).serialize(),
            hyperframe.frame.HeadersFrame(
                stream_id=3,
                data=hpack.Encoder().encode(
                    [
                        (b":status", b"200"),
                        (b"content-type", b"plain/text"),
                    ]
                ),
                flags=["END_HEADERS"],
            ).serialize(),
            hyperframe.frame.DataFrame(stream_id=3, data=b"Hello, world!", flags=["END_STREAM"]).serialize(),
        ],
        http2=True,
    )

    with httpcore2.ConnectionPool(
        network_backend=network_backend,
    ) as pool:
        # Sending an initial request, which once complete will return to the pool, IDLE.
        response = pool.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"

        info = [repr(c) for c in pool.connections]
        assert info == ["<HTTPConnection ['https://example.com:443', HTTP/2, IDLE, Request Count: 1]>"]

        # Sending a second request to the same origin will reuse the existing IDLE connection.
        response = pool.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"

        info = [repr(c) for c in pool.connections]
        assert info == ["<HTTPConnection ['https://example.com:443', HTTP/2, IDLE, Request Count: 2]>"]



def test_connection_pool_with_http2_goaway() -> None:
    """
    Test a connection pool with HTTP/2 requests, that cleanly disconnects
    with a GoAway frame after the first request.
    """
    network_backend = httpcore2.MockBackend(
        buffer=[
            hyperframe.frame.SettingsFrame().serialize(),
            hyperframe.frame.HeadersFrame(
                stream_id=1,
                data=hpack.Encoder().encode(
                    [
                        (b":status", b"200"),
                        (b"content-type", b"plain/text"),
                    ]
                ),
                flags=["END_HEADERS"],
            ).serialize(),
            hyperframe.frame.DataFrame(stream_id=1, data=b"Hello, world!", flags=["END_STREAM"]).serialize(),
            hyperframe.frame.GoAwayFrame(stream_id=0, error_code=0, last_stream_id=1).serialize(),
            b"",
        ],
        http2=True,
    )

    with httpcore2.ConnectionPool(
        network_backend=network_backend,
    ) as pool:
        # Sending an initial request, which once complete will return to the pool, IDLE.
        response = pool.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"

        info = [repr(c) for c in pool.connections]
        assert info == ["<HTTPConnection ['https://example.com:443', HTTP/2, IDLE, Request Count: 1]>"]

        # Sending a second request to the same origin will require a new connection.
        # The original connection has now been closed.
        response = pool.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"

        info = [repr(c) for c in pool.connections]
        assert info == [
            "<HTTPConnection ['https://example.com:443', HTTP/2, IDLE, Request Count: 1]>",
        ]



def test_trace_request() -> None:
    """
    The 'trace' request extension allows for a callback function to inspect the
    internal events that occur while sending a request.
    """
    network_backend = httpcore2.MockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    called = []

    def trace(name: str, kwargs: dict[str, typing.Any]) -> None:
        called.append(name)

    with httpcore2.ConnectionPool(network_backend=network_backend) as pool:
        pool.request("GET", "https://example.com/", extensions={"trace": trace})

    assert called == [
        "connection.connect_tcp.started",
        "connection.connect_tcp.complete",
        "connection.start_tls.started",
        "connection.start_tls.complete",
        "http11.send_request_headers.started",
        "http11.send_request_headers.complete",
        "http11.send_request_body.started",
        "http11.send_request_body.complete",
        "http11.receive_response_headers.started",
        "http11.receive_response_headers.complete",
        "http11.receive_response_body.started",
        "http11.receive_response_body.complete",
        "http11.response_closed.started",
        "http11.response_closed.complete",
    ]



def test_debug_request(caplog: pytest.LogCaptureFixture) -> None:
    """
    The 'trace' request extension allows for a callback function to inspect the
    internal events that occur while sending a request.
    """
    caplog.set_level(logging.DEBUG)

    network_backend = httpcore2.MockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    with httpcore2.ConnectionPool(network_backend=network_backend) as pool:
        pool.request("GET", "http://example.com/")

    assert caplog.record_tuples == [
        (
            "httpcore2.connection",
            logging.DEBUG,
            "connect_tcp.started host='example.com' port=80 local_address=None timeout=None socket_options=None",
        ),
        (
            "httpcore2.connection",
            logging.DEBUG,
            "connect_tcp.complete return_value=<httpcore2.MockStream>",
        ),
        (
            "httpcore2.http11",
            logging.DEBUG,
            "send_request_headers.started request=<Request [b'GET']>",
        ),
        ("httpcore2.http11", logging.DEBUG, "send_request_headers.complete"),
        (
            "httpcore2.http11",
            logging.DEBUG,
            "send_request_body.started request=<Request [b'GET']>",
        ),
        ("httpcore2.http11", logging.DEBUG, "send_request_body.complete"),
        (
            "httpcore2.http11",
            logging.DEBUG,
            "receive_response_headers.started request=<Request [b'GET']>",
        ),
        (
            "httpcore2.http11",
            logging.DEBUG,
            "receive_response_headers.complete return_value="
            "(b'HTTP/1.1', 200, b'OK', [(b'Content-Type', b'plain/text'), (b'Content-Length', b'13')])",
        ),
        (
            "httpcore2.http11",
            logging.DEBUG,
            "receive_response_body.started request=<Request [b'GET']>",
        ),
        ("httpcore2.http11", logging.DEBUG, "receive_response_body.complete"),
        ("httpcore2.http11", logging.DEBUG, "response_closed.started"),
        ("httpcore2.http11", logging.DEBUG, "response_closed.complete"),
        ("httpcore2.connection", logging.DEBUG, "close.started"),
        ("httpcore2.connection", logging.DEBUG, "close.complete"),
    ]



def test_connection_pool_with_http_exception() -> None:
    """
    HTTP/1.1 requests that result in an exception during the connection should
    not be returned to the connection pool.
    """
    network_backend = httpcore2.MockBackend([b"Wait, this isn't valid HTTP!"])

    called = []

    def trace(name: str, kwargs: dict[str, typing.Any]) -> None:
        called.append(name)

    with httpcore2.ConnectionPool(network_backend=network_backend) as pool:
        # Sending an initial request, which once complete will not return to the pool.
        with pytest.raises(httpcore2.RemoteProtocolError):
            pool.request("GET", "https://example.com/", extensions={"trace": trace})

        info = [repr(c) for c in pool.connections]
        assert info == []

    assert called == [
        "connection.connect_tcp.started",
        "connection.connect_tcp.complete",
        "connection.start_tls.started",
        "connection.start_tls.complete",
        "http11.send_request_headers.started",
        "http11.send_request_headers.complete",
        "http11.send_request_body.started",
        "http11.send_request_body.complete",
        "http11.receive_response_headers.started",
        "http11.receive_response_headers.failed",
        "http11.response_closed.started",
        "http11.response_closed.complete",
    ]



def test_connection_pool_with_connect_exception() -> None:
    """
    HTTP/1.1 requests that result in an exception during connection should not
    be returned to the connection pool.
    """

    class FailedConnectBackend(httpcore2.MockBackend):
        def connect_tcp(
            self,
            host: str,
            port: int,
            timeout: typing.Optional[float] = None,
            local_address: typing.Optional[str] = None,
            socket_options: typing.Optional[typing.Iterable[httpcore2.SOCKET_OPTION]] = None,
        ) -> httpcore2.NetworkStream:
            raise httpcore2.ConnectError("Could not connect")

    network_backend = FailedConnectBackend([])

    called = []

    def trace(name: str, kwargs: dict[str, typing.Any]) -> None:
        called.append(name)

    with httpcore2.ConnectionPool(network_backend=network_backend) as pool:
        # Sending an initial request, which once complete will not return to the pool.
        with pytest.raises(httpcore2.ConnectError):
            pool.request("GET", "https://example.com/", extensions={"trace": trace})

        info = [repr(c) for c in pool.connections]
        assert info == []

    assert called == [
        "connection.connect_tcp.started",
        "connection.connect_tcp.failed",
    ]



def test_connection_pool_with_immediate_expiry() -> None:
    """
    Connection pools with keepalive_expiry=0.0 should immediately expire
    keep alive connections.
    """
    network_backend = httpcore2.MockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    with httpcore2.ConnectionPool(
        keepalive_expiry=0.0,
        network_backend=network_backend,
    ) as pool:
        # Sending an initial request, which once complete will not return to the pool.
        with pool.stream("GET", "https://example.com/") as response:
            info = [repr(c) for c in pool.connections]
            assert info == ["<HTTPConnection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>"]
            response.read()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in pool.connections]
        assert info == []



def test_connection_pool_with_no_keepalive_connections_allowed() -> None:
    """
    When 'max_keepalive_connections=0' is used, IDLE connections should not
    be returned to the pool.
    """
    network_backend = httpcore2.MockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    with httpcore2.ConnectionPool(max_keepalive_connections=0, network_backend=network_backend) as pool:
        # Sending an initial request, which once complete will not return to the pool.
        with pool.stream("GET", "https://example.com/") as response:
            info = [repr(c) for c in pool.connections]
            assert info == ["<HTTPConnection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>"]
            response.read()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in pool.connections]
        assert info == []



def test_connection_pool_concurrency() -> None:
    """
    HTTP/1.1 requests made in concurrency must not ever exceed the maximum number
    of allowable connection in the pool.
    """
    network_backend = httpcore2.MockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    def fetch(pool: httpcore2.ConnectionPool, domain: str, info_list: list[list[str]]) -> None:
        with pool.stream("GET", f"http://{domain}/") as response:
            info = [repr(c) for c in pool.connections]
            info_list.append(info)
            response.read()

    with httpcore2.ConnectionPool(max_connections=1, network_backend=network_backend) as pool:
        info_list: typing.List[typing.List[str]] = []
        with concurrency.open_nursery() as nursery:
            for domain in ["a.com", "b.com", "c.com", "d.com", "e.com"]:
                nursery.start_soon(fetch, pool, domain, info_list)

        for item in info_list:
            # Check that each time we inspected the connection pool, only a
            # single connection was established at any one time.
            assert len(item) == 1
            # Each connection was to a different host, and only sent a single
            # request on that connection.
            assert item[0] in [
                "<HTTPConnection ['http://a.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
                "<HTTPConnection ['http://b.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
                "<HTTPConnection ['http://c.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
                "<HTTPConnection ['http://d.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
                "<HTTPConnection ['http://e.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
            ]



def test_connection_pool_concurrency_same_domain_closing() -> None:
    """
    HTTP/1.1 requests made in concurrency must not ever exceed the maximum number
    of allowable connection in the pool.
    """
    network_backend = httpcore2.MockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"Connection: close\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    def fetch(pool: httpcore2.ConnectionPool, domain: str, info_list: list[list[str]]) -> None:
        with pool.stream("GET", f"https://{domain}/") as response:
            info = [repr(c) for c in pool.connections]
            info_list.append(info)
            response.read()

    with httpcore2.ConnectionPool(max_connections=1, network_backend=network_backend, http2=True) as pool:
        info_list: typing.List[typing.List[str]] = []
        with concurrency.open_nursery() as nursery:
            for domain in ["a.com", "a.com", "a.com", "a.com", "a.com"]:
                nursery.start_soon(fetch, pool, domain, info_list)

        for item in info_list:
            # Check that each time we inspected the connection pool, only a
            # single connection was established at any one time.
            assert len(item) == 1
            # Only a single request was sent on each connection.
            assert item[0] == "<HTTPConnection ['https://a.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>"



def test_connection_pool_concurrency_same_domain_keepalive() -> None:
    """
    HTTP/1.1 requests made in concurrency must not ever exceed the maximum number
    of allowable connection in the pool.
    """
    network_backend = httpcore2.MockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
        * 5
    )

    def fetch(pool: httpcore2.ConnectionPool, domain: str, info_list: list[list[str]]) -> None:
        with pool.stream("GET", f"https://{domain}/") as response:
            info = [repr(c) for c in pool.connections]
            info_list.append(info)
            response.read()

    with httpcore2.ConnectionPool(max_connections=1, network_backend=network_backend, http2=True) as pool:
        info_list: typing.List[typing.List[str]] = []
        with concurrency.open_nursery() as nursery:
            for domain in ["a.com", "a.com", "a.com", "a.com", "a.com"]:
                nursery.start_soon(fetch, pool, domain, info_list)

        for item in info_list:
            # Check that each time we inspected the connection pool, only a
            # single connection was established at any one time.
            assert len(item) == 1
            # The connection sent multiple requests.
            assert item[0] in [
                "<HTTPConnection ['https://a.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>",
                "<HTTPConnection ['https://a.com:443', HTTP/1.1, ACTIVE, Request Count: 2]>",
                "<HTTPConnection ['https://a.com:443', HTTP/1.1, ACTIVE, Request Count: 3]>",
                "<HTTPConnection ['https://a.com:443', HTTP/1.1, ACTIVE, Request Count: 4]>",
                "<HTTPConnection ['https://a.com:443', HTTP/1.1, ACTIVE, Request Count: 5]>",
            ]

    assert repr(pool) == "<ConnectionPool [Requests: 0 active, 0 queued | Connections: 0 active, 0 idle]>"



def test_unsupported_protocol() -> None:
    with httpcore2.ConnectionPool() as pool:
        with pytest.raises(httpcore2.UnsupportedProtocol):
            pool.request("GET", "ftp://www.example.com/")

        with pytest.raises(httpcore2.UnsupportedProtocol):
            pool.request("GET", "://www.example.com/")



def test_connection_pool_closed_while_request_in_flight() -> None:
    """
    Closing a connection pool while a request/response is still in-flight
    should raise an error.
    """
    network_backend = httpcore2.MockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    with httpcore2.ConnectionPool(
        network_backend=network_backend,
    ) as pool:
        # Send a request, and then close the connection pool while the
        # response has not yet been streamed.
        with pool.stream("GET", "https://example.com/") as response:
            pool.close()
            with pytest.raises(httpcore2.ReadError):
                response.read()



def test_connection_pool_timeout() -> None:
    """
    Ensure that exceeding max_connections can cause a request to timeout.
    """
    network_backend = httpcore2.MockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    with httpcore2.ConnectionPool(network_backend=network_backend, max_connections=1) as pool:
        # Send a request to a pool that is configured to only support a single
        # connection, and then ensure that a second concurrent request
        # fails with a timeout.
        with pool.stream("GET", "https://example.com/"):
            with pytest.raises(httpcore2.PoolTimeout):
                extensions = {"timeout": {"pool": 0.0001}}
                pool.request("GET", "https://example.com/", extensions=extensions)



def test_connection_pool_timeout_zero() -> None:
    """
    A pool timeout of 0 shouldn't raise a PoolTimeout if there's
    no need to wait on a new connection.
    """
    network_backend = httpcore2.MockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    # Use a pool timeout of zero.
    extensions = {"timeout": {"pool": 0}}

    # A connection pool configured to allow only one connection at a time.
    with httpcore2.ConnectionPool(network_backend=network_backend, max_connections=1) as pool:
        # Two consecutive requests with a pool timeout of zero.
        # Both succeed without raising a timeout.
        response = pool.request("GET", "https://example.com/", extensions=extensions)
        assert response.status == 200
        assert response.content == b"Hello, world!"

        response = pool.request("GET", "https://example.com/", extensions=extensions)
        assert response.status == 200
        assert response.content == b"Hello, world!"

    # A connection pool configured to allow only one connection at a time.
    with httpcore2.ConnectionPool(network_backend=network_backend, max_connections=1) as pool:
        # Two concurrent requests with a pool timeout of zero.
        # Only the first will succeed without raising a timeout.
        with pool.stream("GET", "https://example.com/", extensions=extensions) as response:
            # The first response hasn't yet completed.
            with pytest.raises(httpcore2.PoolTimeout):
                # So a pool timeout occurs.
                pool.request("GET", "https://example.com/", extensions=extensions)
            # The first response now completes.
            response.read()

        assert response.status == 200
        assert response.content == b"Hello, world!"



def test_http11_upgrade_connection() -> None:
    """
    HTTP "101 Switching Protocols" indicates an upgraded connection.

    We should return the response, so that the network stream
    may be used for the upgraded connection.

    https://httpwg.org/specs/rfc9110.html#status.101
    https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/101
    """
    network_backend = httpcore2.MockBackend(
        [
            b"HTTP/1.1 101 Switching Protocols\r\n",
            b"Connection: upgrade\r\n",
            b"Upgrade: custom\r\n",
            b"\r\n",
            b"...",
        ]
    )

    called = []

    def trace(name: str, kwargs: dict[str, typing.Any]) -> None:
        called.append(name)

    with httpcore2.ConnectionPool(network_backend=network_backend, max_connections=1) as pool:
        with pool.stream(
            "GET",
            "wss://example.com/",
            headers={"Connection": "upgrade", "Upgrade": "custom"},
            extensions={"trace": trace},
        ) as response:
            assert response.status == 101
            network_stream = response.extensions["network_stream"]
            content = network_stream.read(max_bytes=1024)
            assert content == b"..."

    assert called == [
        "connection.connect_tcp.started",
        "connection.connect_tcp.complete",
        "connection.start_tls.started",
        "connection.start_tls.complete",
        "http11.send_request_headers.started",
        "http11.send_request_headers.complete",
        "http11.send_request_body.started",
        "http11.send_request_body.complete",
        "http11.receive_response_headers.started",
        "http11.receive_response_headers.complete",
        "http11.response_closed.started",
        "http11.response_closed.complete",
    ]
