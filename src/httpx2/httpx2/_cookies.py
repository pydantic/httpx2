from __future__ import annotations

import math
import threading
import time
import urllib.request
from http import cookiejar
from http.cookiejar import Cookie, CookieJar, CookiePolicy, DefaultCookiePolicy
from typing import TYPE_CHECKING

__all__ = ["IndexedCookieJar"]

# [RFC 1035](https://www.rfc-editor.org/rfc/rfc1035) caps DNS names at 255 octets; longer hosts take the stdlib path.
MAX_HOST_LENGTH = 255


class IndexedCookieJar(CookieJar):
    """
    A `CookieJar` whose per-request cost does not grow with the number of stored cookies.

    It looks up only the domains that can match the request host, and skips the
    expiry sweep until the earliest known expiry has passed.
    """

    _cookies: dict[str, dict[str, dict[str, Cookie]]]
    _cookies_lock: threading.RLock
    _policy: CookiePolicy
    _next_expiry: float = math.inf

    if TYPE_CHECKING:

        def _cookies_for_domain(self, domain: str, request: urllib.request.Request) -> list[Cookie]: ...

    def set_cookie(self, cookie: Cookie) -> None:
        with self._cookies_lock:
            super().set_cookie(cookie)
            if cookie.expires is not None:
                self._next_expiry = min(self._next_expiry, cookie.expires)

    def clear_expired_cookies(self) -> None:
        if time.time() < self._next_expiry:
            return
        with self._cookies_lock:
            super().clear_expired_cookies()
            self._next_expiry = min((c.expires for c in self if c.expires is not None), default=math.inf)

    def _cookies_for_request(self, request: urllib.request.Request) -> list[Cookie]:
        hosts: tuple[str, str] = cookiejar.eff_request_host(request)  # type: ignore[attr-defined]
        uses_default_matching = type(self._policy).domain_return_ok is DefaultCookiePolicy.domain_return_ok
        if not uses_default_matching or max(map(len, hosts)) > MAX_HOST_LENGTH:
            return super()._cookies_for_request(request)  # type: ignore[misc, no-any-return]

        cookies: list[Cookie] = []
        for domain in _matching_domains(hosts):
            if domain in self._cookies:
                cookies.extend(self._cookies_for_domain(domain, request))
        return cookies


def _matching_domains(hosts: tuple[str, str]) -> dict[str, None]:
    """
    Every cookie domain `DefaultCookiePolicy.domain_return_ok` can accept for these hosts, least specific first.

    For `a.example.org` that is `""`, `.org`, `org`, `.example.org`, `example.org`, `.a.example.org`, `a.example.org`.
    """
    domains = {"": None}
    for host in hosts:
        dotted = host if host.startswith(".") else f".{host}"
        for index in range(len(dotted) - 1, -1, -1):
            if dotted[index] == ".":
                domains[dotted[index:]] = None
                domains[dotted[index + 1 :]] = None
    return domains
