from __future__ import annotations

import sys

import atheris

with atheris.instrument_imports():
    from httpx2._decoders import (
        BrotliDecoder,
        DeflateDecoder,
        GZipDecoder,
        ZStandardDecoder,
    )
    from httpx2._exceptions import DecodingError

DECODERS = [DeflateDecoder, GZipDecoder, BrotliDecoder, ZStandardDecoder]


def TestOneInput(data: bytes) -> None:
    if len(data) < 2:
        return
    fdp = atheris.FuzzedDataProvider(data)
    decoder_cls = DECODERS[fdp.ConsumeIntInRange(0, len(DECODERS) - 1)]
    try:
        decoder = decoder_cls()
    except ImportError:
        return
    # Feed the decoder several chunks to exercise streaming state.
    num_chunks = fdp.ConsumeIntInRange(1, 8)
    for _ in range(num_chunks):
        chunk_size = fdp.ConsumeIntInRange(0, 4096)
        chunk = fdp.ConsumeBytes(chunk_size)
        try:
            decoder.decode(chunk)
        except DecodingError:
            return
    try:
        decoder.flush()
    except DecodingError:
        return


def main() -> None:
    atheris.Setup(sys.argv, TestOneInput)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
