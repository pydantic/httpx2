from __future__ import annotations

import select

import anyio
import pytest
import trustme

import httpx2
from tests.httpx2.http2 import http2_peer


@pytest.mark.parametrize("mode", ["tls", "tcp", "ping", "open"])
@pytest.mark.parametrize("keepalive_expiry", [None, 100])
def test_http2_keepalive(mode: str, localhost_cert: trustme.LeafCert, keepalive_expiry: float | None) -> None:
    with http2_peer(mode, localhost_cert) as (url, response_read, peer_ready, close):
        with httpx2.Client(http2=True, verify=False, limits=httpx2.Limits(keepalive_expiry=keepalive_expiry)) as client:
            first = client.post(url, content=iter([b"first"]))
            assert first.http_version == "HTTP/2"
            assert first.content == b"first"
            response_read.set()
            assert peer_ready.wait(5)
            if close:
                sock = first.extensions["network_stream"].get_extra_info("socket")
                assert select.select([sock], [], [], 5)[0]
            second = client.post(url, content=iter([b"second"]))
            assert second.content == b"second"
            assert (first.extensions["network_stream"] is not second.extensions["network_stream"]) == close


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["tls", "tcp", "ping", "open"])
@pytest.mark.parametrize("keepalive_expiry", [None, 100])
async def test_async_http2_keepalive(
    mode: str, localhost_cert: trustme.LeafCert, keepalive_expiry: float | None
) -> None:
    with http2_peer(mode, localhost_cert) as (url, response_read, peer_ready, close):
        async with httpx2.AsyncClient(
            http2=True, verify=False, limits=httpx2.Limits(keepalive_expiry=keepalive_expiry)
        ) as client:
            first = await client.post(url, content=b"first")
            assert first.http_version == "HTTP/2"
            assert first.content == b"first"
            response_read.set()
            assert await anyio.to_thread.run_sync(peer_ready.wait, 5)
            if close:
                sock = first.extensions["network_stream"].get_extra_info("socket")
                readable, _, _ = await anyio.to_thread.run_sync(select.select, [sock], [], [], 5)
                assert readable
            second = await client.post(url, content=b"second")
            assert second.content == b"second"
            assert (first.extensions["network_stream"] is not second.extensions["network_stream"]) == close


@pytest.mark.parametrize("mode", ["tls", "tcp", "ping", "open"])
def test_http2_peer_preserves_client_error(mode: str, localhost_cert: trustme.LeafCert) -> None:
    def fail_response(response: httpx2.Response) -> None:
        raise ValueError("Response hook failed")

    with pytest.raises(ValueError, match="Response hook failed"):
        with http2_peer(mode, localhost_cert) as (url, _, _, _):
            with httpx2.Client(http2=True, verify=False, event_hooks={"response": [fail_response]}) as client:
                client.post(url, content=b"first")


@pytest.mark.parametrize("fail_response", [False, True])
def test_http2_peer_reports_original_error(localhost_cert: trustme.LeafCert, fail_response: bool) -> None:
    def check_response(response: httpx2.Response) -> None:
        if fail_response and response.request.content == b"unexpected":
            raise ValueError("Response hook failed")

    error = ValueError if fail_response else AssertionError
    message = "Response hook failed" if fail_response else "Unexpected request bodies"
    with pytest.raises(error, match=message):
        with http2_peer("open", localhost_cert) as (url, response_read, peer_ready, _):
            with httpx2.Client(http2=True, verify=False, event_hooks={"response": [check_response]}) as client:
                client.post(url, content=b"first")
                response_read.set()
                assert peer_ready.wait(5)
                client.post(url, content=b"unexpected")
