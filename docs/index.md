<h1 align="center" style="font-size: 3rem; margin: -15px 0">
HTTPX2
</h1>

---

<div align="center">
<p>
<a href="https://github.com/pydantic/httpx2/actions">
    <img src="https://github.com/pydantic/httpx2/workflows/Test%20Suite/badge.svg" alt="Test Suite">
</a>
<a href="https://pypi.org/project/httpx2/">
    <img src="https://badge.fury.io/py/httpx2.svg" alt="Package version">
</a>
</p>

<em>A next-generation HTTP client for Python.</em>
</div>

HTTPX2 is a fully featured HTTP client for Python 3, which provides sync and async APIs, and support for both HTTP/1.1 and HTTP/2.

!!! note
    HTTPX2 is a continuation of the wonderful work started by [@lovelydinosaur](https://github.com/lovelydinosaur) and the broader HTTPX community. We're enormously grateful for everything that has gone into HTTPX over the years - it has been a foundational piece of the modern Python ecosystem, and this project would not exist without it.

    With HTTPX itself seeing limited activity recently, Pydantic Services is picking up stewardship under the HTTPX2 name so that users have a reliably maintained path forward - including timely security updates for a library that sits in the critical path of so many production systems. Our aim is to honour the original project's design, keep it stable for everyone relying on it, and continue evolving it carefully. Thank you to [@lovelydinosaur](https://github.com/lovelydinosaur) and every past contributor for laying such a strong foundation. 💙

---

Install HTTPX2 using pip:

```shell
pip install httpx2
```

Now, let's get started:

```pycon
>>> import httpx2
>>> r = httpx2.get('https://www.example.org/')
>>> r
<Response [200 OK]>
>>> r.status_code
200
>>> r.headers['content-type']
'text/html; charset=UTF-8'
>>> r.text
'<!doctype html>\n<html>\n<head>\n<title>Example Domain</title>...'
```

Or, using the command-line client.

```shell
# The command line client is an optional dependency.
pip install 'httpx2[cli]'
```

Which now allows us to use HTTPX2 directly from the command-line...

![httpx2 --help](img/httpx-help.png)

Sending a request...

![httpx2 http://httpbin.org/json](img/httpx-request.png)

## Features

HTTPX2 builds on the well-established usability of `requests`, and gives you:

* A broadly [requests-compatible API](compatibility.md).
* Standard synchronous interface, but with [async support if you need it](async.md).
* HTTP/1.1 [and HTTP/2 support](http2.md).
* Ability to make requests directly to [WSGI applications](advanced/transports.md#wsgi-transport) or [ASGI applications](advanced/transports.md#asgi-transport).
* Strict timeouts everywhere.
* Fully type annotated.
* 100% test coverage.

Plus all the standard features of `requests`...

* International Domains and URLs
* Keep-Alive & Connection Pooling
* Sessions with Cookie Persistence
* Browser-style SSL Verification
* Basic/Digest Authentication
* Elegant Key/Value Cookies
* Automatic Decompression
* Automatic Content Decoding
* Unicode Response Bodies
* Multipart File Uploads
* HTTP(S) Proxy Support
* Connection Timeouts
* Streaming Downloads
* .netrc Support
* Chunked Requests

## Documentation

For a run-through of all the basics, head over to the [QuickStart](quickstart.md).

For more advanced topics, see the **Advanced** section,
the [async support](async.md) section, or the [HTTP/2](http2.md) section.

The [Developer Interface](api.md) provides a comprehensive API reference.

To find out about tools that integrate with HTTPX2, see [Third Party Packages](third_party_packages.md).

## Dependencies

The HTTPX2 project relies on these excellent libraries:

* `httpcore2` - The underlying transport implementation for `httpx2`.
  * `h11` - HTTP/1.1 support.
* `anyio` - Structured concurrency primitives, used to support both `asyncio` and `trio`.
* `certifi` - SSL certificates.
* `idna` - Internationalized domain name support.

As well as these optional installs:

* `h2` - HTTP/2 support. *(Optional, with `httpx2[http2]`)*
* `socksio` - SOCKS proxy support. *(Optional, with `httpx2[socks]`)*
* `rich` - Rich terminal support. *(Optional, with `httpx2[cli]`)*
* `click` - Command line client support. *(Optional, with `httpx2[cli]`)*
* `brotli` or `brotlicffi` - Decoding for "brotli" compressed responses. *(Optional, with `httpx2[brotli]`)*
* `zstandard` - Decoding for "zstd" compressed responses. *(Optional, with `httpx2[zstd]`)*

A huge amount of credit is due to `requests` for the API layout that
much of this work follows, as well as to `urllib3` for plenty of design
inspiration around the lower-level networking details.

## Installation

Install with pip:

```shell
pip install httpx2
```

Or, to include the optional HTTP/2 support, use:

```shell
pip install 'httpx2[http2]'
```

To include the optional brotli and zstandard decoders support, use:

```shell
pip install 'httpx2[brotli,zstd]'
```

HTTPX2 requires Python 3.10+
