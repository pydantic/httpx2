"""
Server-sent events support, derived from httpx-sse (https://github.com/florimondmanca/httpx-sse).

Copyright (c) 2022 Florimond Manca, MIT License (https://github.com/florimondmanca/httpx-sse/blob/master/LICENSE).
"""

from __future__ import annotations

import json as jsonlib
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass

from ._exceptions import TransportError
from ._models import Response

__all__ = ["EventSource", "SSEError", "ServerSentEvent"]


class SSEError(TransportError):
    """
    An error that occurred while connecting to a server-sent events endpoint.
    """


@dataclass(frozen=True)
class ServerSentEvent:
    event: str = "message"
    data: str = ""
    id: str = ""
    retry: int | None = None

    def json(self) -> object:
        return jsonlib.loads(self.data)


class _SSEDecoder:
    def __init__(self) -> None:
        self._event = ""
        self._data: list[str] = []
        self._last_event_id = ""
        self._retry: int | None = None
        self._pending = False

    def decode(self, line: str) -> ServerSentEvent | None:
        if not line:
            if not self._pending:
                return None

            sse = ServerSentEvent(
                event=self._event or "message",
                data="\n".join(self._data),
                id=self._last_event_id,
                retry=self._retry,
            )
            self._event = ""
            self._data = []
            self._retry = None
            self._pending = False
            return sse

        if line.startswith(":"):
            return None

        fieldname, _, value = line.partition(":")
        value = value[1:] if value.startswith(" ") else value

        if fieldname == "event":
            self._event = value
            self._pending = True
        elif fieldname == "data":
            self._data.append(value)
            self._pending = True
        elif fieldname == "id":
            if "\0" not in value:
                self._last_event_id = value
                self._pending = True
        elif fieldname == "retry":
            try:
                self._retry = int(value)
                self._pending = True
            except ValueError:
                pass

        return None


class _SSELineDecoder:
    def __init__(self) -> None:
        self._buffer = ""
        self._trailing_cr = False

    def decode(self, text: str) -> list[str]:
        if self._trailing_cr:
            text = "\r" + text
            self._trailing_cr = False
        if text.endswith("\r"):
            self._trailing_cr = True
            text = text[:-1]

        text = self._buffer + text.replace("\r\n", "\n").replace("\r", "\n")
        lines = text.split("\n")
        self._buffer = lines.pop()
        return lines

    def flush(self) -> list[str]:
        if self._trailing_cr:
            self._buffer += "\n"
            self._trailing_cr = False
        if not self._buffer:
            return []
        lines = self._buffer.split("\n")
        self._buffer = ""
        return lines


class EventSource:
    def __init__(self, response: Response) -> None:
        self._response = response

    @property
    def response(self) -> Response:
        return self._response

    def _check_content_type(self) -> None:
        content_type, _, _ = self._response.headers.get("content-type", "").partition(";")
        if content_type.strip().lower() != "text/event-stream":
            raise SSEError(
                f"Expected response with content type 'text/event-stream', got {content_type.strip()!r}.",
                request=self._response.request,
            )

    def __iter__(self) -> Iterator[ServerSentEvent]:
        self._check_content_type()
        decoder = _SSEDecoder()
        lines = _SSELineDecoder()
        for chunk in self._response.iter_text():
            for line in lines.decode(chunk):
                sse = decoder.decode(line)
                if sse is not None:
                    yield sse
        for line in lines.flush():
            sse = decoder.decode(line)
            if sse is not None:
                yield sse

    async def __aiter__(self) -> AsyncIterator[ServerSentEvent]:
        self._check_content_type()
        decoder = _SSEDecoder()
        lines = _SSELineDecoder()
        async for chunk in self._response.aiter_text():
            for line in lines.decode(chunk):
                sse = decoder.decode(line)
                if sse is not None:
                    yield sse
        for line in lines.flush():
            sse = decoder.decode(line)
            if sse is not None:
                yield sse
