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
            assert "https://%s:%d" % remote_addr == httpbin_secure.url

            sock = stream.get_extra_info("socket")
            assert hasattr(sock, "family")
            assert hasattr(sock, "type")

            invalid = stream.get_extra_info("invalid")
            assert invalid is None

            stream.get_extra_info("is_readable")
