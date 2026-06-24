import httpx2


def test_all_imports_are_exported() -> None:
    included_private_members = ["__description__", "__title__", "__version__"]
    # WebSocket members are exported lazily through `__getattr__` so they only
    # appear in `vars(httpx2)` once accessed; force them in for the comparison.
    lazy_members = httpx2._WEBSOCKET_NAMES
    exported = {member for member in vars(httpx2) if not member.startswith("_") or member in included_private_members}
    assert set(httpx2.__all__) == exported | lazy_members
    assert httpx2.__all__ == sorted(httpx2.__all__, key=str.casefold)
