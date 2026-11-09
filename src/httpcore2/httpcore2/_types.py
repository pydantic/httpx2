"""
Type definitions for type checking purposes.
"""

from collections.abc import Mapping, MutableMapping, Sequence
from typing import Any

ByteOrStr = bytes | str
HeadersAsSequence = Sequence[tuple[ByteOrStr, ByteOrStr]]
HeadersAsMapping = Mapping[ByteOrStr, ByteOrStr]
HeaderTypes = HeadersAsSequence | HeadersAsMapping | None

Extensions = MutableMapping[str, Any]

SOCKET_OPTION = tuple[int, int, int] | tuple[int, int, bytes | bytearray] | tuple[int, int, None, int]
