import typing

import hpack
import hyperframe.frame
import pytest

import httpcore2



@pytest.mark.parametrize("max_connections", [1, 2])
@pytest.mark.parametrize(
    "first_hostname, second_hostname",
    [
        (None, None),
        ("first.example", "first.example"),
        ("first.example", "second.example"),
        ("first.example", None),
        (None, "first.example"),
        (None, ""),
        (None, "192.0.2.1"),
    ],
)
@pytest.mark.parametrize(
    "proxy_url, url, handshake",
    [
        (None, "https://192.0.2.1/", []),
        (
            "socks5://localhost:8080",
            "https://192.0.2.1/",
            [b"\x05\x00", b"\x05\x00\x00\x01\x7f\x00\x00\x01\x01\xbb"],
        ),
        ("http://localhost:8080", "https://192.0.2.1/", [b"HTTP/1.1 200 Connection established\r\n\r\n"]),
        ("https://localhost:8080", "https://192.0.2.1/", [b"HTTP/1.1 200 Connection established\r\n\r\n"]),
        ("https://localhost:8080", "http://192.0.2.1/", []),
    ],
)
def test_connection_pool_reuses_matching_sni_hostname(
    max_connections: int,
    first_hostname: str | None,
    second_hostname: str | None,
    proxy_url: str | None,
    url: str,
    handshake: list[bytes],
) -> None:
    network_backend = httpcore2.MockBackend(handshake + [b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK"] * 4)
    first_extensions = {} if first_hostname is None else {"sni_hostname": first_hostname}
    second_extensions = {"sni_hostname": second_hostname}

    with httpcore2.ConnectionPool(
        network_backend=network_backend,
        proxy=None if proxy_url is None else httpcore2.Proxy(proxy_url),
        max_connections=max_connections,
    ) as pool:
        first = pool.request("GET", url, extensions=first_extensions)
        second = pool.request("GET", url, extensions=second_extensions)
        assert first.content == second.content == b"OK"
        reused = first.extensions["network_stream"] is second.extensions["network_stream"]
        assert reused == (first_hostname == second_hostname)
        assert len(pool.connections) == (1 if reused else max_connections)

        third = pool.request("GET", url, extensions=second_extensions)
        assert third.content == b"OK"
        assert third.extensions["network_stream"] is second.extensions["network_stream"]

        fourth = pool.request("GET", url, extensions=first_extensions)
        assert fourth.content == b"OK"
        reused_first = fourth.extensions["network_stream"] is first.extensions["network_stream"]
        assert reused_first == (reused or max_connections == 2)

    assert pool.connections == []



@pytest.mark.parametrize("max_connections", [1, 2])
@pytest.mark.parametrize("second_hostname", ["first.example", "second.example", None])
def test_connection_pool_reuses_matching_sni_hostname_http2(
    max_connections: int, second_hostname: str | None
) -> None:
    buffer = [hyperframe.frame.SettingsFrame().serialize()]
    buffer.extend(
        hyperframe.frame.HeadersFrame(
            stream_id=stream_id,
            data=hpack.Encoder().encode([(b":status", b"200")]),
            flags=["END_HEADERS", "END_STREAM"],
        ).serialize()
        for stream_id in [1, 3]
    )
    hostnames: list[str] = []

    def trace(name: str, info: dict[str, typing.Any]) -> None:
        if name == "connection.start_tls.started":
            hostnames.append(info["server_hostname"])

    with httpcore2.ConnectionPool(
        network_backend=httpcore2.MockBackend(buffer, http2=True),
        max_connections=max_connections,
        http2=True,
    ) as pool:
        for hostname in ["first.example", second_hostname]:
            response = pool.request(
                "GET", "https://192.0.2.1/", extensions={"sni_hostname": hostname, "trace": trace}
            )
            assert response.status == 200

    expected = ["first.example"]
    if second_hostname != "first.example":
        expected.append(second_hostname or "192.0.2.1")
    assert hostnames == expected



@pytest.mark.parametrize("warm_connection", [False, True])
def test_connection_pool_reserves_http2_connection_for_sni_hostname(warm_connection: bool) -> None:
    buffer = [hyperframe.frame.SettingsFrame().serialize()]
    buffer.extend(
        hyperframe.frame.HeadersFrame(
            stream_id=stream_id,
            data=hpack.Encoder().encode([(b":status", b"200")]),
            flags=["END_HEADERS", "END_STREAM"],
        ).serialize()
        for stream_id in [1, 3]
    )
    network_backend = httpcore2.MockBackend(buffer, http2=True)

    class ReservedConnection(httpcore2.HTTPConnection):
        def handle_request(self, request: httpcore2.Request) -> httpcore2.Response:
            if request.extensions.get("check_reservation"):
                with pytest.raises(httpcore2.PoolTimeout):
                    pool.request(
                        "GET",
                        "https://192.0.2.1/",
                        extensions={"sni_hostname": "second.example", "timeout": {"pool": 0}},
                    )
            return super().handle_request(request)

    class ReservedConnectionPool(httpcore2.ConnectionPool):
        def create_connection(self, origin: httpcore2.Origin) -> httpcore2.ConnectionInterface:
            return ReservedConnection(origin, network_backend=network_backend, http2=True)

    with ReservedConnectionPool(max_connections=1) as pool:
        if warm_connection:
            response = pool.request("GET", "https://192.0.2.1/", extensions={"sni_hostname": "first.example"})
            assert response.status == 200
        response = pool.request(
            "GET",
            "https://192.0.2.1/",
            extensions={"sni_hostname": "first.example", "check_reservation": True},
        )
        assert response.status == 200
        assert len(pool.connections) == 1
