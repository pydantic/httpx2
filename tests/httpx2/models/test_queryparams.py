from __future__ import annotations

import typing

import pytest

import httpx2


@pytest.mark.parametrize(
    "source",
    [
        "a=123&a=456&b=789",
        {"a": ["123", "456"], "b": 789},
        {"a": ("123", "456"), "b": 789},
        [("a", "123"), ("a", "456"), ("b", "789")],
        (("a", "123"), ("a", "456"), ("b", "789")),
    ],
)
def test_queryparams(source: typing.Any) -> None:
    q = httpx2.QueryParams(source)
    assert "a" in q
    assert "A" not in q
    assert "c" not in q
    assert q["a"] == "123"
    assert q.get("a") == "123"
    assert q.get("nope", default=None) is None
    assert q.get_list("a") == ["123", "456"]

    assert list(q.keys()) == ["a", "b"]
    assert list(q.values()) == ["123", "789"]
    assert list(q.items()) == [("a", "123"), ("b", "789")]
    assert len(q) == 2
    assert list(q) == ["a", "b"]
    assert dict(q) == {"a": "123", "b": "789"}
    assert str(q) == "a=123&a=456&b=789"
    assert repr(q) == "QueryParams('a=123&a=456&b=789')"
    assert httpx2.QueryParams({"a": "123", "b": "456"}) == httpx2.QueryParams([("a", "123"), ("b", "456")])
    assert httpx2.QueryParams({"a": "123", "b": "456"}) == httpx2.QueryParams("a=123&b=456")
    assert httpx2.QueryParams({"a": "123", "b": "456"}) == httpx2.QueryParams({"b": "456", "a": "123"})
    assert httpx2.QueryParams() == httpx2.QueryParams({})
    assert httpx2.QueryParams([("a", "123"), ("a", "456")]) == httpx2.QueryParams("a=123&a=456")
    assert httpx2.QueryParams({"a": "123", "b": "456"}) != "invalid"

    q = httpx2.QueryParams([("a", "123"), ("a", "456")])
    assert httpx2.QueryParams(q) == q


def test_queryparam_types() -> None:
    q = httpx2.QueryParams(None)
    assert str(q) == ""

    q = httpx2.QueryParams({"a": True})
    assert str(q) == "a=true"

    q = httpx2.QueryParams({"a": False})
    assert str(q) == "a=false"

    q = httpx2.QueryParams({"a": ""})
    assert str(q) == "a="

    q = httpx2.QueryParams({"a": None})
    assert str(q) == "a="

    q = httpx2.QueryParams({"a": 1.23})
    assert str(q) == "a=1.23"

    q = httpx2.QueryParams({"a": 123})
    assert str(q) == "a=123"

    q = httpx2.QueryParams({"a": [1, 2]})
    assert str(q) == "a=1&a=2"


def test_empty_query_params() -> None:
    q = httpx2.QueryParams({"a": ""})
    assert str(q) == "a="

    q = httpx2.QueryParams("a=")
    assert str(q) == "a="

    q = httpx2.QueryParams("a")
    assert str(q) == "a="


def test_queryparam_update_is_hard_deprecated() -> None:
    q = httpx2.QueryParams("a=123")
    with pytest.raises(RuntimeError):
        q.update({"a": "456"})


def test_queryparam_setter_is_hard_deprecated() -> None:
    q = httpx2.QueryParams("a=123")
    with pytest.raises(RuntimeError):
        q["a"] = "456"


def test_queryparam_set() -> None:
    q = httpx2.QueryParams("a=123")
    q = q.set("a", "456")
    assert q == httpx2.QueryParams("a=456")


def test_queryparam_add() -> None:
    q = httpx2.QueryParams("a=123")
    q = q.add("a", "456")
    assert q == httpx2.QueryParams("a=123&a=456")


def test_queryparam_remove() -> None:
    q = httpx2.QueryParams("a=123")
    q = q.remove("a")
    assert q == httpx2.QueryParams("")


def test_queryparam_merge() -> None:
    q = httpx2.QueryParams("a=123")
    q = q.merge({"b": "456"})
    assert q == httpx2.QueryParams("a=123&b=456")
    q = q.merge({"a": "000", "c": "789"})
    assert q == httpx2.QueryParams("a=000&b=456&c=789")


def test_queryparams_are_hashable() -> None:
    params = (
        httpx2.QueryParams("a=123"),
        httpx2.QueryParams({"a": 123}),
        httpx2.QueryParams("b=456"),
        httpx2.QueryParams({"b": 456}),
    )

    assert len(set(params)) == 2
