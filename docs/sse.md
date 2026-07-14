# Server-Sent Events

[Server-sent events][mdn] (SSE) let a server push a stream of events to the client over a single long-lived HTTP response with the `text/event-stream` content type. HTTPX has native support for consuming them through `client.sse()`.

## Consuming events

`client.sse()` is a context manager that yields an `EventSource`. Iterating the `EventSource` decodes the stream and yields a `ServerSentEvent` for each event:

```pycon
>>> with httpx2.Client() as client:
...     with client.sse("https://example.com/sse") as source:
...         for event in source:
...             print(event.event, event.data)
```

It works the same way with the async client, using `async with` and `async for`:

```pycon
>>> async with httpx2.AsyncClient() as client:
...     async with client.sse("https://example.com/sse") as source:
...         async for event in source:
...             print(event.event, event.data)
```

`sse()` issues a `GET` request by default. Some APIs stream events in response to a `POST` - pass `method="POST"` along with the usual request arguments:

```pycon
>>> with client.sse("https://example.com/sse", method="POST", json={"query": "..."}) as source:
...     for event in source:
...         print(event.data)
```

The `Accept: text/event-stream` and `Cache-Control: no-store` headers are set for you; any headers you pass take precedence.

## The `ServerSentEvent`

Each event exposes the fields defined by the SSE specification:

| Attribute | Description |
| --------- | ----------- |
| `event`   | The event type. Defaults to `"message"`. |
| `data`    | The event payload. Multiple `data:` lines are joined with `\n`. |
| `id`      | The last event ID, which persists across events until changed. |
| `retry`   | The reconnection time in milliseconds, or `None`. |

When the payload is JSON, `event.json()` decodes `event.data` for you:

```pycon
>>> with client.sse("https://example.com/sse") as source:
...     for event in source:
...         print(event.json())
```

## Accessing the response

The underlying [`Response`](api.md#response) is available as `source.response`, which is useful for inspecting the status code or headers before iterating:

```pycon
>>> with client.sse("https://example.com/sse") as source:
...     source.response.raise_for_status()
...     for event in source:
...         print(event.data)
```

## Error handling

If the response does not have a `text/event-stream` content type, iterating the `EventSource` raises `SSEError`:

```pycon
>>> with client.sse("https://example.com/not-an-event-stream") as source:
...     for event in source:  # raises httpx2.SSEError
...         ...
```

`SSEError` is a subclass of [`TransportError`](exceptions.md), so it is also caught by `except httpx2.TransportError`.

[mdn]: https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events
