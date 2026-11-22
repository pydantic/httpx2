# Third Party Packages

These maintained packages integrate with HTTPX2.

<!-- NOTE: Entries are alphabetised. -->

## Plugins

### httpx-pki

[GitHub](https://github.com/ccbest/httpx-pki)

PKCS#12 / PEM client-certificate (mTLS) sessions, with OS cert-store loading, identity selection, and certificate rotation.

### httpx2-pytest

[GitHub](https://github.com/angryfoxx/httpx2-pytest) - [Documentation](https://github.com/angryfoxx/httpx2-pytest#about-httpx2)

Provides a [pytest](https://docs.pytest.org/en/latest/) fixture to mock HTTPX2 within test cases.
This package is a fork of [pytest-httpx](https://github.com/Colin-b/pytest_httpx) for HTTPX2.

### interlock-cb

[GitHub](https://github.com/bagowix/interlock) - [Documentation](https://bagowix.github.io/interlock/integrations/httpx2/)

A circuit breaker transport: keeps one breaker per host, so calls to a failing host fail fast instead of waiting out every timeout. Trips on failure rate over a sliding window and on slow responses.

### pytest-HTTPX2

[GitHub](https://github.com/lundberg/pytest-httpx2)

A pytest plugin for mocking out HTTPX2 using RESPX.

## Libraries with HTTPX2 support

### Authlib

[GitHub](https://github.com/authlib/authlib) - [Documentation](https://docs.authlib.org/en/latest/)

A Python library for building OAuth and OpenID Connect clients and servers. Includes an [OAuth HTTPX2 client](https://docs.authlib.org/en/latest/oauth2/client/http/httpx.html).

### httpdbg

[GitHub](https://github.com/cle-b/httpdbg) - [Documentation](https://httpdbg.readthedocs.io/)

A tool for python developers to easily debug the HTTP(S) client requests in a python program.

### httpx2-negotiate-sspi

[Github](https://github.com/achapkowski/httpx2_negotiate_sspi)
[Documentation](https://achapkowski.github.io/httpx2_negotiate_sspi)

HTTP Negotiate authentication for httpx2 using Windows SSPI.

### httpx2_kerberos

httpx2_kerberos is a Python library that adds Kerberos/GSSAPI authentication support to the HTTPX2 client for making secure, authenticated HTTP requests.

[github](https://github.com/achapkowski/httpx2_kerberos)
[Documentation](https://achapkowski.github.io/httpx2_kerberos)

### VCR.py

[GitHub](https://github.com/kevin1024/vcrpy) - [Documentation](https://vcrpy.readthedocs.io/)

Record and repeat requests.
