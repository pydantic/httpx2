import sys
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import nullcontext

import pytest

from httpcore2._utils import safe_async_iterate


@pytest.mark.anyio
async def test_safe_async_iterate_out_of_order_finalization() -> None:
    async def source() -> AsyncIterator[bytes]:
        yield b"hello"

    async def stream() -> AsyncGenerator[bytes, None]:
        async with safe_async_iterate(source()) as iterator:
            async for chunk in iterator:
                yield chunk

    generators: list[AsyncGenerator[object, None]] = []
    hooks = sys.get_asyncgen_hooks()

    def firstiter(generator: AsyncGenerator[object, None]) -> None:
        generators.append(generator)
        if hooks.firstiter is not None:
            hooks.firstiter(generator)

    sys.set_asyncgen_hooks(firstiter=firstiter)
    try:
        assert await anext(stream()) == b"hello"
    finally:
        sys.set_asyncgen_hooks(*hooks)

        # Runtime shutdown can finalize inner generators before their consumers.
        for generator in reversed(generators):
            await generator.aclose()


@pytest.mark.anyio
@pytest.mark.parametrize("use_iterable", [False, True])
@pytest.mark.parametrize("raise_in_body", [False, True])
async def test_safe_async_iterate_closes_generator(use_iterable: bool, raise_in_body: bool) -> None:
    closed = False

    class Stream:
        async def __aiter__(self) -> AsyncIterator[bytes]:
            nonlocal closed
            try:
                yield b"hello"
            finally:
                closed = True

    stream = Stream()
    with pytest.raises(ValueError, match="consumer error") if raise_in_body else nullcontext():
        async with safe_async_iterate(stream if use_iterable else stream.__aiter__()) as iterator:
            assert await anext(iterator) == b"hello"
            assert not closed
            if raise_in_body:
                raise ValueError("consumer error")
    assert closed


@pytest.mark.anyio
async def test_safe_async_iterate_accepts_iterator_without_aclose() -> None:
    class Iterator(AsyncIterator[bytes]):
        async def __anext__(self) -> bytes:
            return b"hello"

    stream = Iterator()
    async with safe_async_iterate(stream) as iterator:
        assert iterator is stream
        assert await anext(iterator) == b"hello"


@pytest.mark.anyio
async def test_safe_async_iterate_propagates_close_error() -> None:
    async def source() -> AsyncIterator[bytes]:
        try:
            yield b"hello"
        finally:
            raise ValueError("close error")

    with pytest.raises(ValueError, match="close error"):
        async with safe_async_iterate(source()) as iterator:
            assert await anext(iterator) == b"hello"
