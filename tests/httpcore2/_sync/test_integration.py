import ssl

import pytest
from pytest_httpbin.serve import Server

import httpcore2



def test_request(httpbin: Server) -> None:
    with httpcore2.ConnectionPool() as pool:
        response = pool.request("GET", httpbin.url)
        assert response.status == 200



def test_ssl_request(httpbin_secure: Server) -> None:
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    with httpcore2.ConnectionPool(ssl_context=ssl_context) as pool:
        response = pool.request("GET", httpbin_secure.url)
        assert response.status == 200



def test_ssl_verification_failure(httpbin_secure: Server) -> None:
    """
    A failed TLS handshake raises `SSLError`, which is a subclass of `ConnectError`
    so that existing `except ConnectError` handling keeps working.
    """
    with httpcore2.ConnectionPool() as pool:
        with pytest.raises(httpcore2.SSLError) as exc_info:
            pool.request("GET", httpbin_secure.url)

    assert isinstance(exc_info.value, httpcore2.ConnectError)



def test_ssl_verification_failure_includes_reason(httpbin_secure: Server) -> None:
    """
    The underlying `ssl.SSLError` message is preserved.

    Some backends wrap the handshake failure in an exception that carries no
    message of its own, so the reason has to be recovered from the `__cause__`.
    """
    with httpcore2.ConnectionPool() as pool:
        with pytest.raises(httpcore2.SSLError) as exc_info:
            pool.request("GET", httpbin_secure.url)

    # Match the lower-case reason text rather than the `CERTIFICATE_VERIFY_FAILED`
    # mnemonic, which is not emitted by every OpenSSL/LibreSSL build.
    assert "certificate verify failed" in str(exc_info.value)



def test_extra_info(httpbin_secure: Server) -> None:
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    with httpcore2.ConnectionPool(ssl_context=ssl_context) as pool:
        with pool.stream("GET", httpbin_secure.url) as response:
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
