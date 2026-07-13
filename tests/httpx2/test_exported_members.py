import httpx2


def test_all_imports_are_exported() -> None:
    included_private_members = ["__description__", "__title__", "__version__"]
    # `httpx2.websockets` is a subpackage, bound on `httpx2` by importing it, rather than a re-export.
    excluded_members = ["websockets"]
    assert httpx2.__all__ == sorted(
        (
            member
            for member in vars(httpx2).keys()
            if (not member.startswith("_") or member in included_private_members) and member not in excluded_members
        ),
        key=str.casefold,
    )
