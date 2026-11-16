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


def test_cookies_are_sent_to_hosts_longer_than_a_dns_name() -> None:
    client = httpx2.Client(transport=httpx2.MockTransport(get_and_set_cookies))
    client.cookies.set("parent", "1", domain=".example.org")
    client.cookies.set("other", "2", domain="example.com")

    response = client.get(f"http://{'a.' * 200}example.org/echo_cookies")

    assert response.json() == {"cookies": "parent=1"}


def test_cookies_ignore_url_userinfo() -> None:
    client = httpx2.Client(transport=httpx2.MockTransport(get_and_set_cookies))

    client.get("http://user:secret@example.org/set_cookie")

    assert [cookie.domain for cookie in client.cookies.jar] == ["example.org"]
    for url in ["http://example.org/echo_cookies", "http://user:secret@example.org/echo_cookies"]:
        assert client.get(url).json() == {"cookies": "example-name=example-value"}


def test_cookie_jar_sees_a_bounded_request_path() -> None:
    def echo_or_set_cookies(request: httpx2.Request) -> httpx2.Response:
        if request.url.path.endswith("/set_cookie"):
            return httpx2.Response(200, headers=[("set-cookie", "default=1"), ("set-cookie", "explicit=1; Path=/")])
        return httpx2.Response(200, json={"cookies": request.headers.get("cookie")})

    client = httpx2.Client(transport=httpx2.MockTransport(echo_or_set_cookies))
    directory = "/" + "a" * 1023
    deeper = directory + "/" + "b" * 2000

    client.get(f"http://example.org{directory}/set_cookie?q=1")
    client.get(f"http://example.com{deeper}/set_cookie")
    client.get(f"http://example.net/{'c' * 1024}/set_cookie")
    assert sorted((cookie.domain, cookie.name, cookie.path) for cookie in client.cookies.jar) == [
        ("example.com", "explicit", "/"),
        ("example.net", "explicit", "/"),
        ("example.org", "default", directory),
        ("example.org", "explicit", "/"),
    ]

    client.cookies.set("directory", "2", domain="example.org", path=directory)
    client.cookies.set("segment", "3", domain="example.org", path="/" + "a" * 1000)
    client.cookies.set("deeper", "4", domain="example.org", path=deeper)
    response = client.get(f"http://example.org{deeper}/echo_cookies")
    assert response.json() == {"cookies": "default=1; directory=2; explicit=1"}


def test_relative_url_cookies_still_raise() -> None:
    with pytest.raises(ValueError, match="unknown url type"):
        httpx2.Cookies().set_cookie_header(httpx2.Request("GET", "/echo_cookies"))


def test_cookie_policy_sees_url_without_query() -> None:
    urls: list[str] = []

    class RecordingPolicy(DefaultCookiePolicy):
        def set_ok(self, cookie: Cookie, request: urllib.request.Request) -> bool:
            urls.append(request.get_full_url())
            return super().set_ok(cookie, request)

    client = httpx2.Client(transport=httpx2.MockTransport(get_and_set_cookies))
    client.cookies.jar.set_policy(RecordingPolicy())

    client.get("http://user:secret@example.org/set_cookie?q=1#frag")

    assert urls == ["http://example.org/set_cookie"]
