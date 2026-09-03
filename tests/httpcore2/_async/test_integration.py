import ssl

import pytest
from pytest_httpbin.serve import Server

import httpcore2


@pytest.mark.anyio
async def test_request(httpbin: Server) -> None:
    async with httpcore2.AsyncConnectionPool() as pool:
        response = await pool.request("GET", httpbin.url)
        assert response.status == 200


@pytest.mark.anyio
async def test_ssl_request(httpbin_secure: Server) -> None:
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    async with httpcore2.AsyncConnectionPool(ssl_context=ssl_context) as pool:
        response = await pool.request("GET", httpbin_secure.url)
        assert response.status == 200


@pytest.mark.anyio
async def test_ssl_verification_failure(httpbin_secure: Server) -> None:
    """
    A failed TLS handshake raises `SSLError`, which is a subclass of `ConnectError`
    so that existing `except ConnectError` handling keeps working.
    """
    async with httpcore2.AsyncConnectionPool() as pool:
        with pytest.raises(httpcore2.SSLError) as exc_info:
            await pool.request("GET", httpbin_secure.url)

    assert isinstance(exc_info.value, httpcore2.ConnectError)


@pytest.mark.trio
async def test_ssl_verification_failure_includes_reason(httpbin_secure: Server) -> None:
    """
    The underlying `ssl.SSLError` message is preserved.

    Some backends wrap the handshake failure in an exception that carries no
    message of its own, so the reason has to be recovered from the `__cause__`.
    """
    async with httpcore2.AsyncConnectionPool() as pool:
        with pytest.raises(httpcore2.SSLError) as exc_info:
            await pool.request("GET", httpbin_secure.url)

    # Match the lower-case reason text rather than the `CERTIFICATE_VERIFY_FAILED`
    # mnemonic, which is not emitted by every OpenSSL/LibreSSL build.
    assert "certificate verify failed" in str(exc_info.value)


@pytest.mark.anyio
async def test_extra_info(httpbin_secure: Server) -> None:
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    async with httpcore2.AsyncConnectionPool(ssl_context=ssl_context) as pool:
        async with pool.stream("GET", httpbin_secure.url) as response:
            assert response.status == 200
            stream = response.extensions["network_stream"]

            ssl_object = stream.get_extra_info("ssl_object")
            assert ssl_object.version() == "TLSv1.3"

            local_addr = stream.get_extra_info("client_addr")
            assert local_addr[0] == "127.0.0.1"

            remote_addr = stream.get_extra_info("server_addr")
            assert f"https://{remote_addr[0]}:{remote_addr[1]}" == httpbin_secure.url

            sock = stream.get_extra_info("socket")
            assert hasattr(sock, "family")
            assert hasattr(sock, "type")

            invalid = stream.get_extra_info("invalid")
            assert invalid is None

            stream.get_extra_info("is_readable")
