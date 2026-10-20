import time
import urllib.request
from http.cookiejar import Cookie, CookieJar, DefaultCookiePolicy

import pytest

import httpx2


def get_and_set_cookies(request: httpx2.Request) -> httpx2.Response:
    if request.url.path == "/echo_cookies":
        data = {"cookies": request.headers.get("cookie")}
        return httpx2.Response(200, json=data)
    elif request.url.path == "/set_cookie":
        headers = {"set-cookie": "example-name=example-value"}
        return httpx2.Response(200, headers=headers)
    else:
        raise NotImplementedError()  # pragma: no cover


def test_set_cookie() -> None:
    """
    Send a request including a cookie.
    """
    url = "http://example.org/echo_cookies"
    cookies = {"example-name": "example-value"}

    client = httpx2.Client(cookies=cookies, transport=httpx2.MockTransport(get_and_set_cookies))
    response = client.get(url)

    assert response.status_code == 200
    assert response.json() == {"cookies": "example-name=example-value"}


def test_set_per_request_cookie_is_deprecated() -> None:
    """
    Sending a request including a per-request cookie is deprecated.
    """
    url = "http://example.org/echo_cookies"
    cookies = {"example-name": "example-value"}

    client = httpx2.Client(transport=httpx2.MockTransport(get_and_set_cookies))
    with pytest.warns(DeprecationWarning):
        response = client.get(url, cookies=cookies)

    assert response.status_code == 200
    assert response.json() == {"cookies": "example-name=example-value"}


def test_set_cookie_with_cookiejar() -> None:
    """
    Send a request including a cookie, using a `CookieJar` instance.
    """

    url = "http://example.org/echo_cookies"
    cookies = CookieJar()
    cookie = Cookie(
        version=0,
        name="example-name",
        value="example-value",
        port=None,
        port_specified=False,
        domain="",
        domain_specified=False,
        domain_initial_dot=False,
        path="/",
        path_specified=True,
        secure=False,
        expires=None,
        discard=True,
        comment=None,
        comment_url=None,
        rest={"HttpOnly": ""},
        rfc2109=False,
    )
    cookies.set_cookie(cookie)

    client = httpx2.Client(cookies=cookies, transport=httpx2.MockTransport(get_and_set_cookies))
    response = client.get(url)

    assert response.status_code == 200
    assert response.json() == {"cookies": "example-name=example-value"}


def test_setting_client_cookies_to_cookiejar() -> None:
    """
    Send a request including a cookie, using a `CookieJar` instance.
    """

    url = "http://example.org/echo_cookies"
    cookies = CookieJar()
    cookie = Cookie(
        version=0,
        name="example-name",
        value="example-value",
        port=None,
        port_specified=False,
        domain="",
        domain_specified=False,
        domain_initial_dot=False,
        path="/",
        path_specified=True,
        secure=False,
        expires=None,
        discard=True,
        comment=None,
        comment_url=None,
        rest={"HttpOnly": ""},
        rfc2109=False,
    )
    cookies.set_cookie(cookie)

    client = httpx2.Client(cookies=cookies, transport=httpx2.MockTransport(get_and_set_cookies))
    response = client.get(url)

    assert response.status_code == 200
    assert response.json() == {"cookies": "example-name=example-value"}


def test_set_cookie_with_cookies_model() -> None:
    """
    Send a request including a cookie, using a `Cookies` instance.
    """

    url = "http://example.org/echo_cookies"
    cookies = httpx2.Cookies()
    cookies["example-name"] = "example-value"

    client = httpx2.Client(transport=httpx2.MockTransport(get_and_set_cookies))
    client.cookies = cookies
    response = client.get(url)

    assert response.status_code == 200
    assert response.json() == {"cookies": "example-name=example-value"}


def test_get_cookie() -> None:
    url = "http://example.org/set_cookie"

    client = httpx2.Client(transport=httpx2.MockTransport(get_and_set_cookies))
    response = client.get(url)

    assert response.status_code == 200
    assert response.cookies["example-name"] == "example-value"
    assert client.cookies["example-name"] == "example-value"


def test_cookie_persistence() -> None:
    """
    Ensure that Client instances persist cookies between requests.
    """
    client = httpx2.Client(transport=httpx2.MockTransport(get_and_set_cookies))

    response = client.get("http://example.org/echo_cookies")
    assert response.status_code == 200
    assert response.json() == {"cookies": None}

    response = client.get("http://example.org/set_cookie")
    assert response.status_code == 200
    assert response.cookies["example-name"] == "example-value"
    assert client.cookies["example-name"] == "example-value"

    response = client.get("http://example.org/echo_cookies")
    assert response.status_code == 200
    assert response.json() == {"cookies": "example-name=example-value"}


def test_cookies_are_only_sent_to_matching_domains() -> None:
    client = httpx2.Client(transport=httpx2.MockTransport(get_and_set_cookies))
    client.cookies.set("any", "1")
    client.cookies.set("parent", "2", domain=".example.org")
    client.cookies.set("sub", "3", domain="sub.example.org")
    client.cookies.set("other", "4", domain="example.com")

    assert client.get("http://sub.example.org/echo_cookies").json() == {"cookies": "any=1; parent=2; sub=3"}
    assert client.get("http://example.org/echo_cookies").json() == {"cookies": "any=1; parent=2"}


def test_expired_cookies_are_not_sent_and_are_cleared() -> None:
    client = httpx2.Client(transport=httpx2.MockTransport(get_and_set_cookies))
    for name, expires in [("expired", int(time.time()) - 1), ("fresh", int(time.time()) + 3600)]:
        cookie = Cookie(
            version=0,
            name=name,
            value="value",
            port=None,
            port_specified=False,
            domain="",
            domain_specified=False,
            domain_initial_dot=False,
            path="/",
            path_specified=True,
            secure=False,
            expires=expires,
            discard=False,
            comment=None,
            comment_url=None,
            rest={},
            rfc2109=False,
        )
        client.cookies.jar.set_cookie(cookie)

    response = client.get("http://example.org/echo_cookies")

    assert response.json() == {"cookies": "fresh=value"}
    assert list(client.cookies) == ["fresh"]


def test_custom_cookie_policy_decides_which_domains_match() -> None:
    class SendEverywherePolicy(DefaultCookiePolicy):
        def domain_return_ok(self, domain: str, request: urllib.request.Request) -> bool:
            return True

        def return_ok(self, cookie: Cookie, request: urllib.request.Request) -> bool:
            return True

    client = httpx2.Client(transport=httpx2.MockTransport(get_and_set_cookies))
    client.cookies.set("other", "1", domain="example.com")
    client.cookies.jar.set_policy(SendEverywherePolicy())

    assert client.get("http://example.org/echo_cookies").json() == {"cookies": "other=1"}


def test_cookies_are_sent_after_clearing_the_jar() -> None:
    client = httpx2.Client(transport=httpx2.MockTransport(get_and_set_cookies))
    client.cookies.set("old", "1", domain="example.org")
    client.cookies.clear()
    client.cookies.set("first", "2", domain=".example.org")
    client.cookies.set("second", "3", domain="example.org")

    assert client.get("http://example.org/echo_cookies").json() == {"cookies": "first=2; second=3"}


def test_cookies_are_sent_to_hosts_longer_than_a_dns_name() -> None:
    client = httpx2.Client(transport=httpx2.MockTransport(get_and_set_cookies))
    client.cookies.set("parent", "1", domain=".example.org")
    client.cookies.set("other", "2", domain="example.com")

    response = client.get(f"http://{'a.' * 200}example.org/echo_cookies")

    assert response.json() == {"cookies": "parent=1"}
