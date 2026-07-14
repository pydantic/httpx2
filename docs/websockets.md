# WebSockets

[WebSockets][mdn] provide a full-duplex communication channel between the client and the server over a single long-lived connection, allowing both sides to send messages at any time. HTTPX has native support for them through `client.websocket()`.

WebSocket support requires the optional `wsproto` dependency:

```shell
pip install 'httpx2[ws]'
```

## Opening a session

`client.websocket()` is a context manager that performs the WebSocket handshake and yields a `WebSocketSession`:

```pycon
>>> with httpx2.Client() as client:
...     with client.websocket("wss://example.com/ws") as ws:
...         ws.send_text("Hello!")
...         message = ws.receive_text()
```

It works the same way with the async client, using `async with` and `await`:

```pycon
>>> async with httpx2.AsyncClient() as client:
...     async with client.websocket("wss://example.com/ws") as ws:
...         await ws.send_text("Hello!")
...         message = await ws.receive_text()
```

Both `ws://` and `wss://` URLs are supported, as well as their `http://` and `https://` equivalents. The handshake is a regular HTTP request, so the client's configuration - headers, cookies, authentication, proxies, and timeouts - applies as usual.

The session is closed automatically when exiting the context manager.

For quick one-off sessions, there is also a top-level `httpx2.websocket()` function, mirroring `httpx2.request()`:

```pycon
>>> with httpx2.websocket("wss://example.com/ws") as ws:
...     ws.send_text("Hello!")
```

## Sending and receiving messages

The session provides `send` and `receive` methods for text, bytes, and JSON messages:

```pycon
>>> ws.send_text("Hello!")
>>> ws.send_bytes(b"Hello!")
>>> ws.send_json({"message": "Hello!"})
```

```pycon
>>> ws.receive_text()
'Hello!'
>>> ws.receive_bytes()
b'Hello!'
>>> ws.receive_json()
{'message': 'Hello!'}
```

JSON messages are sent as text frames by default. Pass `mode="binary"` to `send_json()` or `receive_json()` to use binary frames instead.

The `receive` methods block until a message is available. Pass a `timeout` in seconds to bound the wait:

```pycon
>>> ws.receive_text(timeout=2.0)
```

If no message arrives in time, `TimeoutError` is raised.

If the received message doesn't match the expected type, `WebSocketInvalidTypeReceived` is raised.

## Subprotocols

To negotiate a subprotocol with the server, pass the `subprotocols` argument. The subprotocol accepted by the server is available as `ws.subprotocol`:

```pycon
>>> with client.websocket("wss://example.com/ws", subprotocols=["graphql-ws"]) as ws:
...     ws.subprotocol
'graphql-ws'
```

## Keepalive pings

The session automatically sends a Ping event at a regular interval to keep the connection alive. If the server doesn't answer in time, the connection is considered lost and `WebSocketNetworkError` is raised.

You can also send a Ping manually. `ping()` returns an event that is set when the corresponding Pong is received:

```pycon
>>> pong = ws.ping()
>>> pong.wait()  # Blocks until the server answers.
```

## Configuration

`client.websocket()` and `httpx2.websocket()` accept the following keyword arguments to tune the session behaviour:

| Argument | Default | Description |
| -------- | ------- | ----------- |
| `max_message_size_bytes` | 65536 | The number of bytes read from the network at a time. Larger messages are received in multiple chunks and reassembled. |
| `queue_size` | 512 | The size of the queue holding received messages until they are consumed. When full, the session stops reading from the server until room is available. |
| `keepalive_ping_interval_seconds` | 20.0 | The interval between automatic keepalive pings. Use `None` to disable them. |
| `keepalive_ping_timeout_seconds` | 20.0 | How long to wait for the server to answer a keepalive ping before considering the connection lost. |

## Accessing the handshake response

The [`Response`](api.md#response) that upgraded the connection is available as `ws.response`, which is useful for inspecting the handshake headers:

```pycon
>>> with client.websocket("wss://example.com/ws") as ws:
...     ws.response.headers
```

## Error handling

WebSocket exceptions live in the `httpx2.websockets` namespace:

```pycon
>>> from httpx2.websockets import WebSocketDisconnect
```

* `WebSocketUpgradeError` - the server didn't accept the upgrade, and responded with something other than a `101 Switching Protocols` status code. The response is available as `.response`.
* `WebSocketDisconnect` - the server closed the session. The close code and reason are available as `.code` and `.reason`.
* `WebSocketInvalidTypeReceived` - the received message didn't match the expected type.
* `WebSocketNetworkError` - a network error occurred, typically because the underlying stream closed or timed out.

All of these are subclasses of `httpx2.websockets.HTTPXWSException`:

```pycon
>>> from httpx2.websockets import WebSocketDisconnect
>>> with client.websocket("wss://example.com/ws") as ws:
...     try:
...         message = ws.receive_text()
...     except WebSocketDisconnect as exc:
...         print(f"Connection closed: {exc.code} {exc.reason}")
```

## Testing ASGI applications

Just as [`ASGITransport`](advanced/transports.md#asgi-transport) lets you make HTTP requests directly against an ASGI application, `ASGIWebSocketTransport` extends it with WebSocket support - useful for testing Starlette or FastAPI applications without running a server:

```python
from httpx2 import AsyncClient
from httpx2.websockets import ASGIWebSocketTransport

async with AsyncClient(transport=ASGIWebSocketTransport(app)) as client:
    async with client.websocket("ws://testserver/ws") as ws:
        await ws.send_text("Hello!")
        message = await ws.receive_text()
```

Regular HTTP requests through the same transport work too, so a single client can exercise the whole application.

## Acknowledgements

WebSocket support is derived from the excellent [httpx-ws](https://github.com/frankie567/httpx-ws) package by [François Voron](https://github.com/frankie567).

[mdn]: https://developer.mozilla.org/en-US/docs/Web/API/WebSockets_API
