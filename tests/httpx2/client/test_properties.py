from __future__ import annotations

import httpx2


def test_client_base_url() -> None:
    client = httpx2.Client()
    client.base_url = "https://www.example.org/"
    assert isinstance(client.base_url, httpx2.URL)
    assert client.base_url == "https://www.example.org/"


def test_client_base_url_without_trailing_slash() -> None:
    client = httpx2.Client()
    client.base_url = "https://www.example.org/path"
    assert isinstance(client.base_url, httpx2.URL)
    assert client.base_url == "https://www.example.org/path/"


def test_client_base_url_with_trailing_slash() -> None:
    client = httpx2.Client()
    client.base_url = "https://www.example.org/path/"
    assert isinstance(client.base_url, httpx2.URL)
    assert client.base_url == "https://www.example.org/path/"


def test_client_headers() -> None:
    client = httpx2.Client()
    client.headers = {"a": "b"}
    assert isinstance(client.headers, httpx2.Headers)
    assert client.headers["A"] == "b"


def test_client_cookies() -> None:
    client = httpx2.Client()
    client.cookies = {"a": "b"}
    assert isinstance(client.cookies, httpx2.Cookies)
    mycookies = list(client.cookies.jar)
    assert len(mycookies) == 1
    assert mycookies[0].name == "a" and mycookies[0].value == "b"


def test_client_timeout() -> None:
    expected_timeout = 12.0
    client = httpx2.Client()

    client.timeout = expected_timeout

    assert isinstance(client.timeout, httpx2.Timeout)
    assert client.timeout.connect == expected_timeout
    assert client.timeout.read == expected_timeout
    assert client.timeout.write == expected_timeout
    assert client.timeout.pool == expected_timeout


def test_client_event_hooks() -> None:
    def on_request(request: httpx2.Request) -> None:
        pass  # pragma: no cover

    client = httpx2.Client()
    client.event_hooks = {"request": [on_request]}
    assert client.event_hooks == {"request": [on_request], "response": []}


def test_client_trust_env() -> None:
    client = httpx2.Client()
    assert client.trust_env

    client = httpx2.Client(trust_env=False)
    assert not client.trust_env
