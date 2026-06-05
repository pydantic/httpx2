"""
Tests for the URL constructor: `URL(url, params=...)` should merge the new
params with the URL's existing query string instead of replacing it.
Regression for issue #905.
"""

from __future__ import annotations

import httpx2


def test_url_constructor_params_merges_with_existing_query() -> None:
    """`URL("https://example.com/?a=1", params={"b": 2})` should keep `a=1`."""
    url = httpx2.URL("https://example.com/?a=1", params={"b": 2})
    assert url.params["a"] == "1"
    assert url.params["b"] == "2"


def test_url_constructor_params_keeps_order_existing_first() -> None:
    url = httpx2.URL("https://example.com/?a=1&b=2", params={"c": 3})
    assert str(url) == "https://example.com/?a=1&b=2&c=3"


def test_url_constructor_params_overrides_existing_on_key_collision() -> None:
    """When the same key is given, the explicit `params` value wins."""
    url = httpx2.URL("https://example.com/?a=1", params={"a": "2"})
    assert url.params["a"] == "2"


def test_url_constructor_no_params_keeps_existing_query() -> None:
    """No `params` keyword, existing query is preserved (sanity)."""
    url = httpx2.URL("https://example.com/?a=1&b=2")
    assert url.params["a"] == "1"
    assert url.params["b"] == "2"


def test_url_constructor_empty_params_keeps_existing_query() -> None:
    """Empty `params` should not wipe out the URL's query string."""
    url = httpx2.URL("https://example.com/?a=1", params={})
    assert url.params["a"] == "1"


def test_url_constructor_no_existing_query_just_uses_params() -> None:
    url = httpx2.URL("https://example.com/", params={"a": "1", "b": "2"})
    assert url.params["a"] == "1"
    assert url.params["b"] == "2"


def test_get_request_merges_query_with_params() -> None:
    """End-to-end: `httpx2.get(url, params={...})` should concatenate."""
    # We don't make a network call — we only check the request that would be sent.
    transport = httpx2.MockTransport(lambda req: httpx2.Response(200, json={"ok": True}))
    with httpx2.Client(transport=transport) as client:
        req = client.build_request(
            "GET",
            "https://httpbin.org/get?page=post&s=list",
            params={"pid": 0, "tags": "k-on!"},
        )
    assert "page=post" in str(req.url)
    assert "s=list" in str(req.url)
    assert "pid=0" in str(req.url)
    assert "tags=k-on" in str(req.url)  # `!` is percent-encoded
