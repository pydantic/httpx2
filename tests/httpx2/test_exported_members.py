import httpx2


def test_all_imports_are_exported() -> None:
    # `__all__` is grouped by source module rather than sorted alphabetically, so compare as sets.
    included_private_members = {"__description__", "__title__", "__version__"}
    public_members = {
        member for member in vars(httpx2).keys() if not member.startswith("_") or member in included_private_members
    }
    assert set(httpx2.__all__) == public_members
    assert len(httpx2.__all__) == len(set(httpx2.__all__)), "duplicate names in __all__"
