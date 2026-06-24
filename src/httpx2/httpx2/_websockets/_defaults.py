from __future__ import annotations

DEFAULT_MAX_MESSAGE_SIZE_BYTES = 65_536
DEFAULT_QUEUE_SIZE = 512
DEFAULT_KEEPALIVE_PING_INTERVAL_SECONDS = 20.0
DEFAULT_KEEPALIVE_PING_TIMEOUT_SECONDS = 20.0

WS_EXTRA_INSTALL_MESSAGE = "WebSocket support requires the `wsproto` package. Install it with `pip install httpx2[ws]`."
