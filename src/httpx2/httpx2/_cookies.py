from __future__ import annotations

import itertools
import math
import time
import urllib.request
from http.cookiejar import (  # type: ignore[attr-defined]
    Cookie,
    CookieJar,
    CookiePolicy,
    DefaultCookiePolicy,
    eff_request_host,
)

__all__ = ["IndexedCookieJar"]


class IndexedCookieJar(CookieJar):
    """
    A `CookieJar` whose per-request cost does not grow with the number of stored cookies.

    It looks up only the domains that can match the request host, and skips the
    expiry sweep until the earliest known expiry has passed.
    """

    def __init__(self, policy: CookiePolicy | None = None) -> None:
        super().__init__(policy)
        self._next_expiry = math.inf
        self._domain_order: dict[str, int] = {}
        self._domain_counter = itertools.count()

    def set_cookie(self, cookie: Cookie) -> None:
        with self._cookies_lock:  # type: ignore[attr-defined]
            if cookie.domain not in self._cookies:  # type: ignore[attr-defined]
                self._domain_order[cookie.domain] = next(self._domain_counter)
            super().set_cookie(cookie)
            if cookie.expires is not None:
                self._next_expiry = min(self._next_expiry, cookie.expires)

    def clear(self, domain: str | None = None, path: str | None = None, name: str | None = None) -> None:
        with self._cookies_lock:  # type: ignore[attr-defined]
            super().clear(domain, path, name)
            if domain is None:
                self._domain_order.clear()
            elif path is None:
                del self._domain_order[domain]

    def clear_expired_cookies(self) -> None:
        if time.time() < self._next_expiry:
            return
        with self._cookies_lock:  # type: ignore[attr-defined]
            super().clear_expired_cookies()
            self._next_expiry = min((c.expires for c in self if c.expires is not None), default=math.inf)

    def _cookies_for_request(self, request: urllib.request.Request) -> list[Cookie]:
        hosts: tuple[str, str] = eff_request_host(request)
        # Hosts longer than any DNS name would make the suffix set quadratic in size.
        if (
            type(self._policy).domain_return_ok is not DefaultCookiePolicy.domain_return_ok  # type: ignore[attr-defined]
            or max(len(host) for host in hosts) > 255
        ):
            return super()._cookies_for_request(request)  # type: ignore[misc, no-any-return]

        # Every domain `DefaultCookiePolicy.domain_return_ok` can accept for this request host.
        candidates = {""}
        for host in hosts:
            dotted = host if host.startswith(".") else f".{host}"
            for index, char in enumerate(dotted):
                if char == ".":
                    candidates.add(dotted[index:])
                    candidates.add(dotted[index + 1 :])

        # Match the stdlib, which visits domains in insertion order.
        stored = self._cookies  # type: ignore[attr-defined]
        domains = sorted((domain for domain in candidates if domain in stored), key=self._domain_order.__getitem__)
        cookies: list[Cookie] = []
        for domain in domains:
            cookies.extend(self._cookies_for_domain(domain, request))  # type: ignore[attr-defined]
        return cookies
