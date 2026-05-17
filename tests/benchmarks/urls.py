from __future__ import annotations

SIMPLE_URL = "https://example.org/"
TYPICAL_URL = "https://www.example.org:8443/path/to/resource?key=value&other=1#frag"
LONG_QUERY_URL = "https://api.example.org/v1/search?" + "&".join(f"k{i}=v{i}" for i in range(64))
INTERNATIONAL_URL = "https://例え.テスト/パス/ファイル?キー=値"
IPV6_URL = "https://[2001:db8::1]:8443/path?x=1"
RELATIVE_TARGET = "/path/to/resource?key=value"
