import pytest

import httpx2


def test_status_code_as_int() -> None:
    # mypy doesn't (yet) recognize that IntEnum members are ints, so ignore it here
    assert httpx2.codes.NOT_FOUND == 404  # type: ignore[comparison-overlap]
    assert str(httpx2.codes.NOT_FOUND) == "404"


def test_status_code_value_lookup() -> None:
    assert httpx2.codes(404) == 404


def test_status_code_phrase_lookup() -> None:
    assert httpx2.codes["NOT_FOUND"] == 404


def test_lowercase_status_code() -> None:
    assert httpx2.codes.not_found == 404


def test_reason_phrase_for_status_code() -> None:
    assert httpx2.codes.get_reason_phrase(404) == "Not Found"


def test_reason_phrase_for_unknown_status_code() -> None:
    assert httpx2.codes.get_reason_phrase(499) == ""


def test_rfc9110_status_texts() -> None:
    assert httpx2.codes.get_reason_phrase(413) == "Content Too Large"
    assert httpx2.codes.get_reason_phrase(414) == "URI Too Long"
    assert httpx2.codes.get_reason_phrase(416) == "Range Not Satisfiable"
    assert httpx2.codes.get_reason_phrase(422) == "Unprocessable Content"


@pytest.mark.parametrize(
    ("old_name", "new_name"),
    [
        ("REQUEST_ENTITY_TOO_LARGE", "CONTENT_TOO_LARGE"),
        ("REQUEST_URI_TOO_LONG", "URI_TOO_LONG"),
        ("REQUESTED_RANGE_NOT_SATISFIABLE", "RANGE_NOT_SATISFIABLE"),
        ("UNPROCESSABLE_ENTITY", "UNPROCESSABLE_CONTENT"),
    ],
)
def test_pre_rfc9110_aliases_are_deprecated(old_name: str, new_name: str) -> None:
    # the pre-RFC 9110 constant names are kept as deprecated aliases
    msg = f"'{old_name}' is deprecated. Use '{new_name}' instead."
    with pytest.warns(DeprecationWarning, match=msg):
        assert getattr(httpx2.codes, old_name) == httpx2.codes[new_name]

    assert old_name in httpx2.codes.__members__
    assert httpx2.codes[old_name] == httpx2.codes[new_name]
    assert getattr(httpx2.codes, old_name.lower()) == httpx2.codes[new_name]


def test_unknown_status_code_attribute_raises() -> None:
    name = "NOT_A_REAL_CODE"
    with pytest.raises(AttributeError):
        getattr(httpx2.codes, name)
