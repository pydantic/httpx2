import types

import httpx2


def test_all_imports_are_exported() -> None:
    included_private_members = ["__description__", "__title__", "__version__"]
    # Importing `httpx2.websockets` binds it as an attribute on `httpx2`; it is a submodule, not a re-export.
    exported = (
        member
        for member, value in vars(httpx2).items()
        if not isinstance(value, types.ModuleType)
        and (not member.startswith("_") or member in included_private_members)
    )
    assert httpx2.__all__ == sorted(exported, key=str.casefold)
