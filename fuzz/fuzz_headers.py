from __future__ import annotations

import sys

import atheris

with atheris.instrument_imports():
    from httpx2 import Headers


def TestOneInput(data: bytes) -> None:
    fdp = atheris.FuzzedDataProvider(data)
    num_headers = fdp.ConsumeIntInRange(0, 32)
    raw: list[tuple[bytes, bytes]] = []
    for _ in range(num_headers):
        name = fdp.ConsumeBytes(fdp.ConsumeIntInRange(0, 64))
        value = fdp.ConsumeBytes(fdp.ConsumeIntInRange(0, 256))
        raw.append((name, value))
    try:
        headers = Headers(raw)
    except (UnicodeDecodeError, ValueError):
        return
    # Exercise iteration and lookup paths.
    for key in list(headers.keys()):
        headers.get(key)
    headers.multi_items()


def main() -> None:
    atheris.Setup(sys.argv, TestOneInput)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
