import ssl
import typing

import hpack
import hyperframe.frame
import pytest

from httpcore2 import (
    SOCKET_OPTION,
    ConnectionPool,
    MockBackend,
    MockStream,
    NetworkStream,
    Origin,
    Proxy,
    ProxyError,
)



def test_proxy_forwarding() -> None:
    """
    Send an HTTP request via a proxy.
    """
    network_backend = MockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    with ConnectionPool(
        proxy=Proxy("http://localhost:8080/"),
        max_connections=10,
        network_backend=network_backend,
    ) as proxy:
        # Sending an initial request, which once complete will return to the pool, IDLE.
        with proxy.stream("GET", "http://example.com/") as response:
            info = [repr(c) for c in proxy.connections]
            assert info == [
                "<ForwardHTTPConnection ['http://localhost:8080', HTTP/1.1, ACTIVE, Request Count: 1]>"
            ]
            response.read()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in proxy.connections]
        assert info == ["<ForwardHTTPConnection ['http://localhost:8080', HTTP/1.1, IDLE, Request Count: 1]>"]
        assert proxy.connections[0].is_idle()
        assert proxy.connections[0].is_available()
        assert not proxy.connections[0].is_closed()

        # A connection on a forwarding proxy can only handle HTTP requests to the same origin.
        assert proxy.connections[0].can_handle_request(Origin(b"http", b"example.com", 80))
        assert not proxy.connections[0].can_handle_request(Origin(b"http", b"other.com", 80))
        assert not proxy.connections[0].can_handle_request(Origin(b"https", b"example.com", 443))
        assert not proxy.connections[0].can_handle_request(Origin(b"https", b"other.com", 443))



def test_proxy_tunneling() -> None:
    """
    Send an HTTPS request via a proxy.
    """
    network_backend = MockBackend(
        [
            # The initial response to the proxy CONNECT
            b"HTTP/1.1 200 OK\r\n\r\n",
            # The actual response from the remote server
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    with ConnectionPool(
        proxy=Proxy("http://localhost:8080/"),
        network_backend=network_backend,
    ) as proxy:
        # Sending an initial request, which once complete will return to the pool, IDLE.
        with proxy.stream("GET", "https://example.com/") as response:
            info = [repr(c) for c in proxy.connections]
            assert info == [
                "<TunnelHTTPConnection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>"
            ]
            response.read()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in proxy.connections]
        assert info == ["<TunnelHTTPConnection ['https://example.com:443', HTTP/1.1, IDLE, Request Count: 1]>"]
        assert proxy.connections[0].is_idle()
        assert proxy.connections[0].is_available()
        assert not proxy.connections[0].is_closed()

        # A connection on a tunneled proxy can only handle HTTPS requests to the same origin.
        assert not proxy.connections[0].can_handle_request(Origin(b"http", b"example.com", 80))
        assert not proxy.connections[0].can_handle_request(Origin(b"http", b"other.com", 80))
        assert proxy.connections[0].can_handle_request(Origin(b"https", b"example.com", 443))
        assert not proxy.connections[0].can_handle_request(Origin(b"https", b"other.com", 443))


# We need to adapt the mock backend here slightly in order to deal
# with the proxy case. We do not want the initial connection to the proxy
# to indicate an HTTP/2 connection, but we do want it to indicate HTTP/2
# once the SSL upgrade has taken place.
class HTTP1ThenHTTP2Stream(MockStream):
    def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: typing.Optional[str] = None,
        timeout: typing.Optional[float] = None,
    ) -> NetworkStream:
        self._http2 = True
        return self


class HTTP1ThenHTTP2Backend(MockBackend):
    def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: typing.Optional[float] = None,
        local_address: typing.Optional[str] = None,
        socket_options: typing.Optional[typing.Iterable[SOCKET_OPTION]] = None,
    ) -> NetworkStream:
        return HTTP1ThenHTTP2Stream(list(self._buffer))



def test_proxy_tunneling_http2() -> None:
    """
    Send an HTTP/2 request via a proxy.
    """
    network_backend = HTTP1ThenHTTP2Backend(
        [
            # The initial response to the proxy CONNECT
            b"HTTP/1.1 200 OK\r\n\r\n",
            # The actual response from the remote server
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
        ],
    )

    with ConnectionPool(
        proxy=Proxy("http://localhost:8080/"),
        network_backend=network_backend,
        http2=True,
    ) as proxy:
        # Sending an initial request, which once complete will return to the pool, IDLE.
        with proxy.stream("GET", "https://example.com/") as response:
            info = [repr(c) for c in proxy.connections]
            assert info == ["<TunnelHTTPConnection ['https://example.com:443', HTTP/2, ACTIVE, Request Count: 1]>"]
            response.read()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in proxy.connections]
        assert info == ["<TunnelHTTPConnection ['https://example.com:443', HTTP/2, IDLE, Request Count: 1]>"]
        assert proxy.connections[0].is_idle()
        assert proxy.connections[0].is_available()
        assert not proxy.connections[0].is_closed()

        # A connection on a tunneled proxy can only handle HTTPS requests to the same origin.
        assert not proxy.connections[0].can_handle_request(Origin(b"http", b"example.com", 80))
        assert not proxy.connections[0].can_handle_request(Origin(b"http", b"other.com", 80))
        assert proxy.connections[0].can_handle_request(Origin(b"https", b"example.com", 443))
        assert not proxy.connections[0].can_handle_request(Origin(b"https", b"other.com", 443))



def test_proxy_tunneling_with_403() -> None:
    """
    Send an HTTPS request via a proxy.
    """
    network_backend = MockBackend(
        [
            b"HTTP/1.1 403 Permission Denied\r\n\r\n",
        ]
    )

    with ConnectionPool(
        proxy=Proxy("http://localhost:8080/"),
        network_backend=network_backend,
    ) as proxy:
        with pytest.raises(ProxyError) as exc_info:
            proxy.request("GET", "https://example.com/")
        assert str(exc_info.value) == "403 Permission Denied"
        assert not proxy.connections



def test_proxy_tunneling_with_auth() -> None:
    """
    Send an authenticated HTTPS request via a proxy.
    """
    network_backend = MockBackend(
        [
            # The initial response to the proxy CONNECT
            b"HTTP/1.1 200 OK\r\n\r\n",
            # The actual response from the remote server
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    with ConnectionPool(
        proxy=Proxy(
            url="http://localhost:8080/",
            auth=("username", "password"),
        ),
        network_backend=network_backend,
    ) as proxy:
        response = proxy.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"


def test_proxy_headers() -> None:
    proxy = Proxy(
        url="http://localhost:8080/",
        auth=("username", "password"),
    )
    assert proxy.headers == [(b"Proxy-Authorization", b"Basic dXNlcm5hbWU6cGFzc3dvcmQ=")]
