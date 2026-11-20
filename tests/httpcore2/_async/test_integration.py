import ssl
from typing import Any

import pytest
from pytest_httpbin.serve import Server

import httpcore2


@pytest.mark.anyio
async def test_request(httpbin: Server) -> None:
    async with httpcore2.AsyncConnectionPool() as pool:
        response = await pool.request("GET", httpbin.url)
        assert response.status == 200


@pytest.mark.anyio
@pytest.mark.parametrize("phase", ["started", "complete"])
async def test_pool_releases_response_when_close_trace_is_retried(httpbin: Server, phase: str) -> None:
    error = OSError("close trace failed")
    started = "http11.response_closed.started"
    complete = "http11.response_closed.complete"
    close_events: list[str] = []

    async def trace(name: str, info: dict[str, Any]) -> None:
        if name in (started, complete):
            close_events.append(name)
            if name == f"http11.response_closed.{phase}" and close_events.count(name) == 1:
                await response.aclose()
                raise error

    async with httpcore2.AsyncConnectionPool(max_connections=1) as pool:
        async with pool.stream("GET", httpbin.url, extensions={"trace": trace}) as response:
            await response.aread()
            with pytest.raises(OSError) as exc:
                await response.aclose()
            assert exc.value is error
            await response.aclose()
            await response.aclose()

        assert "Requests: 0 active, 0 queued" in repr(pool)
        assert close_events == ([started, started, complete] if phase == "started" else [started, complete])
        follow_up = await pool.request("GET", httpbin.url, extensions={"timeout": {"pool": 1.0}})
        assert follow_up.status == 200


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
            assert f"https://{remote_addr[0]}:{remote_addr[1]}" == httpbin_secure.url

            sock = stream.get_extra_info("socket")
            assert hasattr(sock, "family")
            assert hasattr(sock, "type")

            invalid = stream.get_extra_info("invalid")
            assert invalid is None

            stream.get_extra_info("is_readable")
