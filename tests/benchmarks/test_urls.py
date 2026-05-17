from __future__ import annotations

import pytest

from httpx2 import URL, QueryParams
from httpx2._urlparse import urlparse
from tests.benchmarks.urls import (
    INTERNATIONAL_URL,
    IPV6_URL,
    LONG_QUERY_URL,
    RELATIVE_TARGET,
    SIMPLE_URL,
    TYPICAL_URL,
)

pytestmark = pytest.mark.benchmark


def test_bench_urlparse_simple() -> None:
    urlparse(SIMPLE_URL)


def test_bench_urlparse_typical() -> None:
    urlparse(TYPICAL_URL)


def test_bench_urlparse_long_query() -> None:
    urlparse(LONG_QUERY_URL)


def test_bench_urlparse_international() -> None:
    urlparse(INTERNATIONAL_URL)


def test_bench_urlparse_ipv6() -> None:
    urlparse(IPV6_URL)


def test_bench_url_construct_typical() -> None:
    URL(TYPICAL_URL)


def test_bench_url_join_relative() -> None:
    base = URL(TYPICAL_URL)
    base.join(RELATIVE_TARGET)


def test_bench_url_copy_with() -> None:
    url = URL(TYPICAL_URL)
    url.copy_with(path="/other", params={"a": "1", "b": "2"})


def test_bench_queryparams_construct() -> None:
    QueryParams([("a", "1"), ("b", "2"), ("c", "3"), ("d", "4"), ("a", "5")])


def test_bench_queryparams_str() -> None:
    params = QueryParams([("a", "1"), ("b", "2"), ("c", "3"), ("d", "4"), ("a", "5")])
    str(params)
