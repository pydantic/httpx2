# Proxies

The `httpcore` package provides support for HTTP proxies, using either "HTTP Forwarding" or "HTTP Tunnelling". Forwarding is a proxy mechanism for sending requests to `http` URLs via an intermediate proxy. Tunnelling is a proxy mechanism for sending requests to `https` URLs via an intermediate proxy.

Sending requests via a proxy is very similar to sending requests using a standard connection pool:

```python
import httpcore

proxy = httpcore.Proxy("http://127.0.0.1:8080/")
pool = httpcore.ConnectionPool(proxy=proxy)
r = proxy.request("GET", "https://www.example.com/")

print(r)
# <Response [200]>
```

You can test the `httpcore` proxy support, using the Python [`proxy.py`](https://pypi.org/project/proxy.py/) tool:

```shell
$ pip install proxy.py
$ proxy --hostname 127.0.0.1 --port 8080
```

Requests will automatically use either forwarding or tunnelling, depending on if the scheme is `http` or `https`.

## Authentication

Proxy authentication can be included in the initial configuration:

```python
import httpcore

# A `Proxy-Authorization` header will be included on the initial proxy connection.
proxy = httpcore.Proxy(
    url="http://127.0.0.1:8080/",
    auth=("<username>", "<password>")
)
pool = httpcore.ConnectionPool(proxy=proxy)
```

Custom headers can also be included:

```python
import httpcore
import base64

# Construct and include a `Proxy-Authorization` header.
auth = base64.b64encode(b"<username>:<password>")
proxy = httpcore.Proxy(
    url="http://127.0.0.1:8080/",
    headers={"Proxy-Authorization": b"Basic " + auth}
)
pool = httpcore.ConnectionPool(proxy=proxy)
```

## Proxy SSL

The `httpcore` package also supports HTTPS proxies for http and https destinations.

HTTPS proxies can be used in the same way that HTTP proxies are.

```python
proxy = httpcore.Proxy(url="https://127.0.0.1:8080/")
```

Also, when using HTTPS proxies, you may need to configure the SSL context, which you can do with the `ssl_context` argument.

```python
import ssl
import httpcore

proxy_ssl_context = ssl.create_default_context()
proxy_ssl_context.check_hostname = False

proxy = httpcore.Proxy(
    url='https://127.0.0.1:8080/',
    ssl_context=proxy_ssl_context
)
pool = httpcore.ConnectionPool(proxy=proxy)
```

## HTTP Versions

If you use proxies, keep in mind that the `httpcore` package only supports proxies to HTTP/1.1 servers.

## SOCKS proxy support

The `httpcore` package also supports proxies using the SOCKS5 protocol.

Make sure to install the optional dependancy using `pip install 'httpcore[socks]'`.

The `SOCKSProxy` class should be using instead of a standard connection pool:

```python
import httpcore

# Note that the SOCKS port is 1080.
proxy = httpcore.Proxy(url="socks5://127.0.0.1:1080/")
pool = httpcore.ConnectionPool(proxy=proxy)
r = pool.request("GET", "https://www.example.com/")
```

Authentication via SOCKS is also supported:

```python
import httpcore

proxy = httpcore.Proxy(
    url="socks5://127.0.0.1:1080/",
    auth=("<username>", "<password>"),
)
pool = httpcore.ConnectionPool(proxy=proxy)
r = pool.request("GET", "https://www.example.com/")
```

---

# Reference

## `httpcore.Proxy`

::: httpcore.Proxy
    handler: python
    rendering:
        show_source: False
