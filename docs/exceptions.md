# Exceptions

This page lists exceptions that may be raised when using HTTPX.

For an overview of how to work with HTTPX exceptions, see [Exceptions (Quickstart)](quickstart.md#exceptions).

## The exception hierarchy

* HTTPError
    * RequestError
        * TransportError
            * TimeoutException
                * ConnectTimeout
                * ReadTimeout
                * WriteTimeout
                * PoolTimeout
            * NetworkError
                * ConnectError
                * ReadError
                * WriteError
                * CloseError
            * ProtocolError
                * LocalProtocolError
                * RemoteProtocolError
            * ProxyError
            * UnsupportedProtocol
        * DecodingError
        * TooManyRedirects
    * HTTPStatusError
* InvalidURL
* CookieConflict
* StreamError
    * StreamConsumed
    * ResponseNotRead
    * RequestNotRead
    * StreamClosed

---

## Exception classes

::: httpx2.HTTPError

::: httpx2.RequestError

::: httpx2.TransportError

::: httpx2.TimeoutException

::: httpx2.ConnectTimeout

::: httpx2.ReadTimeout

::: httpx2.WriteTimeout

::: httpx2.PoolTimeout

::: httpx2.NetworkError

::: httpx2.ConnectError

::: httpx2.ReadError

::: httpx2.WriteError

::: httpx2.CloseError

::: httpx2.ProtocolError

::: httpx2.LocalProtocolError

::: httpx2.RemoteProtocolError

::: httpx2.ProxyError

::: httpx2.UnsupportedProtocol

::: httpx2.DecodingError

::: httpx2.TooManyRedirects

::: httpx2.HTTPStatusError

::: httpx2.InvalidURL

::: httpx2.CookieConflict

::: httpx2.StreamError

::: httpx2.StreamConsumed

::: httpx2.StreamClosed

::: httpx2.ResponseNotRead

::: httpx2.RequestNotRead
