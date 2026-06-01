import pytest

import httpcore2


@pytest.mark.anyio
async def test_socks5_request() -> None:
    """
    Send an HTTP request via a SOCKS proxy.
    """
    network_backend = httpcore2.AsyncMockBackend(
        [
            # The initial socks CONNECT
            #   v5 NOAUTH
            b"\x05\x00",
            #   v5 SUC RSV IP4 127  .0  .0  .1     :80
            b"\x05\x00\x00\x01\xff\x00\x00\x01\x00\x50",
            # The actual response from the remote server
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async with httpcore2.AsyncConnectionPool(
        proxy=httpcore2.Proxy("socks5://localhost:8080/"),
        network_backend=network_backend,
    ) as proxy:
        # Sending an initial request, which once complete will return to the pool, IDLE.
        async with proxy.stream("GET", "https://example.com/") as response:
            info = [repr(c) for c in proxy.connections]
            assert info == ["<AsyncSocks5Connection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>"]
            await response.aread()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in proxy.connections]
        assert info == ["<AsyncSocks5Connection ['https://example.com:443', HTTP/1.1, IDLE, Request Count: 1]>"]
        assert proxy.connections[0].is_idle()
        assert proxy.connections[0].is_available()
        assert not proxy.connections[0].is_closed()

        # A connection on a tunneled proxy can only handle HTTPS requests to the same origin.
        assert not proxy.connections[0].can_handle_request(httpcore2.Origin(b"http", b"example.com", 80))
        assert not proxy.connections[0].can_handle_request(httpcore2.Origin(b"http", b"other.com", 80))
        assert proxy.connections[0].can_handle_request(httpcore2.Origin(b"https", b"example.com", 443))
        assert not proxy.connections[0].can_handle_request(httpcore2.Origin(b"https", b"other.com", 443))


@pytest.mark.anyio
async def test_authenticated_socks5_request() -> None:
    """
    Send an HTTP request via a SOCKS proxy.
    """
    network_backend = httpcore2.AsyncMockBackend(
        [
            # The initial socks CONNECT
            #   v5 USERNAME/PASSWORD
            b"\x05\x02",
            #   v1 VALID USERNAME/PASSWORD
            b"\x01\x00",
            #   v5 SUC RSV IP4 127  .0  .0  .1     :80
            b"\x05\x00\x00\x01\xff\x00\x00\x01\x00\x50",
            # The actual response from the remote server
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async with httpcore2.AsyncConnectionPool(
        proxy=httpcore2.Proxy(
            url="socks5://localhost:8080/",
            auth=(b"username", b"password"),
        ),
        network_backend=network_backend,
    ) as proxy:
        # Sending an initial request, which once complete will return to the pool, IDLE.
        async with proxy.stream("GET", "https://example.com/") as response:
            info = [repr(c) for c in proxy.connections]
            assert info == ["<AsyncSocks5Connection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>"]
            await response.aread()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in proxy.connections]
        assert info == ["<AsyncSocks5Connection ['https://example.com:443', HTTP/1.1, IDLE, Request Count: 1]>"]
        assert proxy.connections[0].is_idle()
        assert proxy.connections[0].is_available()
        assert not proxy.connections[0].is_closed()


@pytest.mark.anyio
async def test_socks5_request_connect_failed() -> None:
    """
    Attempt to send an HTTP request via a SOCKS proxy, resulting in a connect failure.
    """
    network_backend = httpcore2.AsyncMockBackend(
        [
            # The initial socks CONNECT
            #   v5 NOAUTH
            b"\x05\x00",
            #   v5  NO RSV IP4   0  .0  .0  .0     :00
            b"\x05\x05\x00\x01\x00\x00\x00\x00\x00\x00",
        ]
    )

    async with httpcore2.AsyncConnectionPool(
        proxy=httpcore2.Proxy("socks5://localhost:8080/"),
        network_backend=network_backend,
    ) as proxy:
        # Sending a request, which the proxy rejects
        with pytest.raises(httpcore2.ProxyError) as exc_info:
            await proxy.request("GET", "https://example.com/")
        assert str(exc_info.value) == "Proxy Server could not connect: Connection refused."

        assert not proxy.connections


@pytest.mark.anyio
async def test_socks5_request_failed_to_provide_auth() -> None:
    """
    Attempt to send an HTTP request via an authenticated SOCKS proxy,
    without providing authentication credentials.
    """
    network_backend = httpcore2.AsyncMockBackend(
        [
            #   v5 USERNAME/PASSWORD
            b"\x05\x02",
        ]
    )

    async with httpcore2.AsyncConnectionPool(
        proxy=httpcore2.Proxy("socks5://localhost:8080/"),
        network_backend=network_backend,
    ) as proxy:
        # Sending a request, which the proxy rejects
        with pytest.raises(httpcore2.ProxyError) as exc_info:
            await proxy.request("GET", "https://example.com/")
        assert (
            str(exc_info.value) == "Requested NO AUTHENTICATION REQUIRED from proxy server, but got USERNAME/PASSWORD."
        )

        assert not proxy.connections


@pytest.mark.anyio
async def test_socks5_request_incorrect_auth() -> None:
    """
    Attempt to send an HTTP request via an authenticated SOCKS proxy,
    with incorrect authentication credentials.
    """
    network_backend = httpcore2.AsyncMockBackend(
        [
            #   v5 USERNAME/PASSWORD
            b"\x05\x02",
            #   v1 INVALID USERNAME/PASSWORD
            b"\x01\x01",
        ]
    )

    async with httpcore2.AsyncConnectionPool(
        proxy=httpcore2.Proxy(
            url="socks5://localhost:8080/",
            auth=(b"invalid", b"invalid"),
        ),
        network_backend=network_backend,
    ) as proxy:
        # Sending a request, which the proxy rejects
        with pytest.raises(httpcore2.ProxyError) as exc_info:
            await proxy.request("GET", "https://example.com/")
        assert str(exc_info.value) == "Invalid username/password"

        assert not proxy.connections
