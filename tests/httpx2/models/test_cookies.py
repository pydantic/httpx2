import http
from collections.abc import Iterator

import pytest

import httpx2


class NonIterableCookieJar(http.cookiejar.CookieJar):
    def __iter__(self) -> Iterator[http.cookiejar.Cookie]:
        raise AssertionError("CookieJar.__iter__ should not be used for truthiness")  # pragma: no cover


class IterableOnlyCookieJar(http.cookiejar.CookieJar):
    def __init__(self, cookies: list[http.cookiejar.Cookie]) -> None:
        super().__init__()
        self._iter_cookies = cookies
        delattr(self, "_cookies")

    def __iter__(self) -> Iterator[http.cookiejar.Cookie]:
        return iter(self._iter_cookies)


def make_cookie(name: str = "name", value: str = "value") -> http.cookiejar.Cookie:
    return http.cookiejar.Cookie(
        version=0,
        name=name,
        value=value,
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


def test_cookies() -> None:
    cookies = httpx2.Cookies({"name": "value"})
    assert cookies["name"] == "value"
    assert "name" in cookies
    assert len(cookies) == 1
    assert dict(cookies) == {"name": "value"}
    assert bool(cookies) is True

    del cookies["name"]
    assert "name" not in cookies
    assert len(cookies) == 0
    assert dict(cookies) == {}
    assert bool(cookies) is False


def test_cookies_bool_does_not_iterate_cookie_jar() -> None:
    jar = NonIterableCookieJar()
    cookies = httpx2.Cookies(jar)

    assert bool(cookies) is False

    cookies.set("name", "value")

    assert bool(cookies) is True


def test_cookies_bool_iterates_custom_cookie_jar_without_cookie_store() -> None:
    assert bool(httpx2.Cookies(IterableOnlyCookieJar([]))) is False
    assert bool(httpx2.Cookies(IterableOnlyCookieJar([make_cookie()]))) is True


def test_cookies_update() -> None:
    cookies = httpx2.Cookies()
    more_cookies = httpx2.Cookies()
    more_cookies.set("name", "value", domain="example.com")

    cookies.update(more_cookies)
    assert dict(cookies) == {"name": "value"}
    assert cookies.get("name", domain="example.com") == "value"


def test_cookies_with_domain() -> None:
    cookies = httpx2.Cookies()
    cookies.set("name", "value", domain="example.com")
    cookies.set("name", "value", domain="example.org")

    with pytest.raises(httpx2.CookieConflict):
        cookies["name"]

    cookies.clear(domain="example.com")
    assert len(cookies) == 1


def test_cookies_with_domain_and_path() -> None:
    cookies = httpx2.Cookies()
    cookies.set("name", "value", domain="example.com", path="/subpath/1")
    cookies.set("name", "value", domain="example.com", path="/subpath/2")
    cookies.clear(domain="example.com", path="/subpath/1")
    assert len(cookies) == 1
    cookies.delete("name", domain="example.com", path="/subpath/2")
    assert len(cookies) == 0


def test_multiple_set_cookie() -> None:
    jar = http.cookiejar.CookieJar()
    headers = [
        (
            b"Set-Cookie",
            b"1P_JAR=2020-08-09-18; expires=Tue, 08-Sep-2099 18:33:35 GMT; path=/; domain=.example.org; Secure",
        ),
        (
            b"Set-Cookie",
            b"NID=204=KWdXOuypc86YvRfBSiWoW1dEXfSl_5qI7sxZY4umlk4J35yNTeNEkw15"
            b"MRaujK6uYCwkrtjihTTXZPp285z_xDOUzrdHt4dj0Z5C0VOpbvdLwRdHatHAzQs7"
            b"7TsaiWY78a3qU9r7KP_RbSLvLl2hlhnWFR2Hp5nWKPsAcOhQgSg; expires=Mon, "
            b"08-Feb-2099 18:33:35 GMT; path=/; domain=.example.org; HttpOnly",
        ),
    ]
    request = httpx2.Request("GET", "https://www.example.org")
    response = httpx2.Response(200, request=request, headers=headers)

    cookies = httpx2.Cookies(jar)
    cookies.extract_cookies(response)

    assert len(cookies) == 2


def test_cookies_can_be_a_list_of_tuples() -> None:
    cookies_val = [("name1", "val1"), ("name2", "val2")]

    cookies = httpx2.Cookies(cookies_val)

    assert len(cookies.items()) == 2
    for k, v in cookies_val:
        assert cookies[k] == v


def test_cookies_repr() -> None:
    cookies = httpx2.Cookies()
    cookies.set(name="foo", value="bar", domain="http://blah.com")
    cookies.set(name="fizz", value="buzz", domain="http://hello.com")

    assert repr(cookies) == (
        "<Cookies[<Cookie foo=bar for http://blah.com />, <Cookie fizz=buzz for http://hello.com />]>"
    )
