# Logging

If you need to inspect the internal behaviour of `httpcore`, you can use Python's standard logging to output debug level information.

For example, the following configuration...

```python
import logging
import httpcore

logging.basicConfig(
    format="%(levelname)s [%(asctime)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG
)

httpcore.request('GET', 'https://www.example.com')
```

Will send debug level output to the console, or wherever `stdout` is directed too...

```
DEBUG [2023-01-09 14:44:00] httpcore.connection - connect_tcp.started host='www.example.com' port=443 local_address=None timeout=None
DEBUG [2023-01-09 14:44:00] httpcore.connection - connect_tcp.complete return_value=<httpcore.backends.sync.SyncStream object at 0x109ba6610>
DEBUG [2023-01-09 14:44:00] httpcore.connection - start_tls.started ssl_context=<ssl.SSLContext object at 0x109e427b0> server_hostname='www.example.com' timeout=None
DEBUG [2023-01-09 14:44:00] httpcore.connection - start_tls.complete return_value=<httpcore.backends.sync.SyncStream object at 0x109e8b050>
DEBUG [2023-01-09 14:44:00] httpcore.http11 - send_request_headers.started request=<Request [b'GET']>
DEBUG [2023-01-09 14:44:00] httpcore.http11 - send_request_headers.complete
DEBUG [2023-01-09 14:44:00] httpcore.http11 - send_request_body.started request=<Request [b'GET']>
DEBUG [2023-01-09 14:44:00] httpcore.http11 - send_request_body.complete
DEBUG [2023-01-09 14:44:00] httpcore.http11 - receive_response_headers.started request=<Request [b'GET']>
DEBUG [2023-01-09 14:44:00] httpcore.http11 - receive_response_headers.complete return_value=(b'HTTP/1.1', 200, b'OK', [(b'Age', b'572646'), (b'Cache-Control', b'max-age=604800'), (b'Content-Type', b'text/html; charset=UTF-8'), (b'Date', b'Mon, 09 Jan 2023 14:44:00 GMT'), (b'Etag', b'"3147526947+ident"'), (b'Expires', b'Mon, 16 Jan 2023 14:44:00 GMT'), (b'Last-Modified', b'Thu, 17 Oct 2019 07:18:26 GMT'), (b'Server', b'ECS (nyb/1D18)'), (b'Vary', b'Accept-Encoding'), (b'X-Cache', b'HIT'), (b'Content-Length', b'1256')])
DEBUG [2023-01-09 14:44:00] httpcore.http11 - receive_response_body.started request=<Request [b'GET']>
DEBUG [2023-01-09 14:44:00] httpcore.http11 - receive_response_body.complete
DEBUG [2023-01-09 14:44:00] httpcore.http11 - response_closed.started
DEBUG [2023-01-09 14:44:00] httpcore.http11 - response_closed.complete
DEBUG [2023-01-09 14:44:00] httpcore.connection - close.started
DEBUG [2023-01-09 14:44:00] httpcore.connection - close.complete
```

The exact formatting of the debug logging may be subject to change across different versions of `httpcore`. If you need to rely on a particular format it is recommended that you pin installation of the package to a fixed version.