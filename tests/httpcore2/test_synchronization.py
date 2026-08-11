from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpcore2._synchronization as synchronization


def test_current_async_library_falls_back_to_asyncio_without_sniffio() -> None:
    """
    When sniffio isn't installed, current_async_library() should fall back
    to "asyncio" on every call, without importing anything on the hot path.
    """
    with patch.object(synchronization, "sniffio", None):
        with patch("builtins.__import__") as mock_import:
            for _ in range(3):
                assert synchronization.current_async_library() == "asyncio"
            mock_import.assert_not_called()


def test_current_async_library_uses_installed_sniffio() -> None:
    """
    When sniffio is installed, current_async_library() should defer to it
    on every call, since the same process may legitimately run under both
    asyncio and trio over its lifetime.
    """
    stub = MagicMock()
    stub.current_async_library.return_value = "trio"

    with patch.object(synchronization, "sniffio", stub):
        assert synchronization.current_async_library() == "trio"
        assert synchronization.current_async_library() == "trio"
        assert stub.current_async_library.call_count == 2
