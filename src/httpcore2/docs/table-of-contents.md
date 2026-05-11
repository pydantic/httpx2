# API Reference

* Quickstart
    * `httpcore.request()`
    * `httpcore.stream()`
* Requests, Responses, and URLs
    * `httpcore.Request`
    * `httpcore.Response`
    * `httpcore.URL`
* Connection Pools
    * `httpcore.ConnectionPool`
* Proxies
    * `httpcore.Proxy`
* Connections
    * `httpcore.HTTPConnection`
    * `httpcore.HTTP11Connection`
    * `httpcore.HTTP2Connection`
* Async Support
    * `httpcore.AsyncConnectionPool`
    * `httpcore.AsyncHTTPConnection`
    * `httpcore.AsyncHTTP11Connection`
    * `httpcore.AsyncHTTP2Connection`
* Network Backends
    * Sync
        * `httpcore.backends.sync.SyncBackend`
        * `httpcore.backends.mock.MockBackend`
    * Async
        * `httpcore.backends.auto.AutoBackend`
        * `httpcore.backends.asyncio.AsyncioBackend`
        * `httpcore.backends.trio.TrioBackend`
        * `httpcore.backends.mock.AsyncMockBackend`
    * Base interfaces
        * `httpcore.backends.base.NetworkBackend`
        * `httpcore.backends.base.AsyncNetworkBackend`
* Exceptions
    * `httpcore.TimeoutException`
        * `httpcore.PoolTimeout`
        * `httpcore.ConnectTimeout`
        * `httpcore.ReadTimeout`
        * `httpcore.WriteTimeout`
    * `httpcore.NetworkError`
        * `httpcore.ConnectError`
        * `httpcore.ReadError`
        * `httpcore.WriteError`
    * `httpcore.ProtocolError`
        * `httpcore.RemoteProtocolError`
        * `httpcore.LocalProtocolError`
    * `httpcore.ProxyError`
    * `httpcore.UnsupportedProtocol`
