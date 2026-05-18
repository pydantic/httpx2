from __future__ import annotations

import json
import logging
import os
import random
import typing

import pytest

import httpx2
from httpx2._utils import (
    IPNetPattern,
    WildcardURLPattern,
    build_url_pattern,
    get_environment_proxies,
)

if typing.TYPE_CHECKING:
    from conftest import TestServer


@pytest.mark.parametrize(
    "encoding",
    (
        "utf-32",
        "utf-8-sig",
        "utf-16",
        "utf-8",
        "utf-16-be",
        "utf-16-le",
        "utf-32-be",
        "utf-32-le",
    ),
)
def test_encoded(encoding: str) -> None:
    content = '{"abc": 123}'.encode(encoding)
    response = httpx2.Response(200, content=content)
    assert response.json() == {"abc": 123}


def test_bad_utf_like_encoding() -> None:
    content = b"\x00\x00\x00\x00"
    response = httpx2.Response(200, content=content)
    with pytest.raises(json.decoder.JSONDecodeError):
        response.json()


@pytest.mark.parametrize(
    ("encoding", "expected"),
    (
        ("utf-16-be", "utf-16"),
        ("utf-16-le", "utf-16"),
        ("utf-32-be", "utf-32"),
        ("utf-32-le", "utf-32"),
    ),
)
def test_guess_by_bom(encoding: str, expected: str) -> None:
    content = '\ufeff{"abc": 123}'.encode(encoding)
    response = httpx2.Response(200, content=content)
    assert response.json() == {"abc": 123}


def test_logging_request(server: TestServer, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    with httpx2.Client() as client:
        response = client.get(server.url)
        assert response.status_code == 200

    assert caplog.record_tuples == [
        (
            "httpx2",
            logging.INFO,
            f'HTTP Request: GET {server.url} "HTTP/1.1 200 OK"',
        )
    ]


def test_logging_redirect_chain(server: TestServer, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    redirect_url = server.url.copy_with(path="/redirect_301")
    with httpx2.Client(follow_redirects=True) as client:
        response = client.get(redirect_url)
        assert response.status_code == 200

    assert caplog.record_tuples == [
        (
            "httpx2",
            logging.INFO,
            f'HTTP Request: GET {redirect_url} "HTTP/1.1 301 Moved Permanently"',
        ),
        (
            "httpx2",
            logging.INFO,
            f'HTTP Request: GET {server.url} "HTTP/1.1 200 OK"',
        ),
    ]


@pytest.mark.parametrize(
    ["environment", "proxies"],
    [
        ({}, {}),
        ({"HTTP_PROXY": "http://127.0.0.1"}, {"http://": "http://127.0.0.1"}),
        (
            {"https_proxy": "http://127.0.0.1", "HTTP_PROXY": "https://127.0.0.1"},
            {"https://": "http://127.0.0.1", "http://": "https://127.0.0.1"},
        ),
        ({"all_proxy": "http://127.0.0.1"}, {"all://": "http://127.0.0.1"}),
        ({"TRAVIS_APT_PROXY": "http://127.0.0.1"}, {}),
        ({"no_proxy": "127.0.0.1"}, {"all://127.0.0.1": None}),
        ({"no_proxy": "192.168.0.0/16"}, {"all://192.168.0.0/16": None}),
        ({"no_proxy": "::1"}, {"all://[::1]": None}),
        ({"no_proxy": "fe11::/16"}, {"all://[fe11::]/16": None}),
        ({"no_proxy": "localhost"}, {"all://localhost": None}),
        ({"no_proxy": "github.com"}, {"all://*github.com": None}),
        ({"no_proxy": ".github.com"}, {"all://*.github.com": None}),
        ({"no_proxy": "http://github.com"}, {"http://github.com": None}),
    ],
)
def test_get_environment_proxies(environment: dict[str, str], proxies: dict[str, str | None]) -> None:
    os.environ.update(environment)

    assert get_environment_proxies() == proxies


@pytest.mark.parametrize(
    ["pattern", "url", "expected"],
    [
        ("http://example.com", "http://example.com", True),
        ("http://example.com", "https://example.com", False),
        ("http://example.com", "http://other.com", False),
        ("http://example.com:123", "http://example.com:123", True),
        ("http://example.com:123", "http://example.com:456", False),
        ("http://example.com:123", "http://example.com", False),
        ("all://example.com", "http://example.com", True),
        ("all://example.com", "https://example.com", True),
        ("http://", "http://example.com", True),
        ("http://", "https://example.com", False),
        ("all://", "https://example.com:123", True),
        ("", "https://example.com:123", True),
        ("all://192.168.0.0/24", "http://192.168.0.1", True),
        ("all://192.168.0.0/24", "https://192.168.1.1", False),
        ("all://[2001:db8:abcd:0012::]/64", "http://[2001:db8:abcd:12::1]", True),
        ("all://[2001:db8:abcd:0012::]/64", "http://[2001:db8:abcd:13::1]:8080", False),
    ],
)
def test_url_matches(pattern: str, url: str, expected: bool) -> None:
    url_pattern = build_url_pattern(pattern)
    assert url_pattern.matches(httpx2.URL(url)) == expected


@pytest.mark.parametrize(
    ["pattern", "url", "expected"],
    [
        ("all://192.168.0.0/24", "http://192.168.0.1", True),
        ("all://192.168.0.1", "http://192.168.0.1", True),
        ("all://192.168.0.0/24", "foobar", False),
    ],
)
def test_IPNetPattern(pattern: str, url: str, expected: bool) -> None:
    _, rest = pattern.split("://", 1)
    ip_pattern = IPNetPattern(rest)
    assert ip_pattern.matches(httpx2.URL(url)) == expected


def test_build_url_pattern() -> None:
    pattern1 = build_url_pattern("all://192.168.0.0/16")
    pattern2 = build_url_pattern("all://192.168.0.0/16")
    pattern3 = build_url_pattern("all://192.168.0.1")
    assert isinstance(pattern1, IPNetPattern)
    assert isinstance(pattern2, IPNetPattern)
    assert isinstance(pattern3, WildcardURLPattern)
    assert pattern1 == pattern2
    assert pattern2 != pattern3
    assert pattern1 < pattern3
    assert hash(pattern1) == hash(pattern2)
    assert hash(pattern2) != hash(pattern3)


def test_pattern_priority() -> None:
    matchers = [
        build_url_pattern("all://"),
        build_url_pattern("http://"),
        build_url_pattern("http://example.com"),
        build_url_pattern("http://example.com:123"),
        build_url_pattern("all://192.168.0.0/16"),
    ]
    random.shuffle(matchers)
    assert sorted(matchers) == [
        build_url_pattern("all://192.168.0.0/16"),
        build_url_pattern("http://example.com:123"),
        build_url_pattern("http://example.com"),
        build_url_pattern("http://"),
        build_url_pattern("all://"),
    ]
