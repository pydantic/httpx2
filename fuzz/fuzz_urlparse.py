from __future__ import annotations

import sys

import atheris

with atheris.instrument_imports():
    from httpx2 import InvalidURL
    from httpx2._urlparse import urlparse


def TestOneInput(data: bytes) -> None:
    try:
        url = data.decode("utf-8", errors="replace")
    except Exception:
        return
    try:
        urlparse(url)
    except (InvalidURL, UnicodeError, ValueError):
        pass


def main() -> None:
    atheris.Setup(sys.argv, TestOneInput)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
