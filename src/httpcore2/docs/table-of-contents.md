# API Reference

* Quickstart
    * `httpcore2.request()`
    * `httpcore2.stream()`
* Requests, Responses, and URLs
    * `httpcore2.Request`
    * `httpcore2.Response`
    * `httpcore2.URL`
* Connection Pools
    * `httpcore2.ConnectionPool`
* Proxies
    * `httpcore2.Proxy`
* Connections
    * `httpcore2.HTTPConnection`
    * `httpcore2.HTTP11Connection`
    * `httpcore2.HTTP2Connection`
* Async Support
    * `httpcore2.AsyncConnectionPool`
    * `httpcore2.AsyncHTTPConnection`
    * `httpcore2.AsyncHTTP11Connection`
    * `httpcore2.AsyncHTTP2Connection`
* Network Backends
    * Sync
        * `httpcore2._backends.sync.SyncBackend`
        * `httpcore2._backends.mock.MockBackend`
    * Async
        * `httpcore2._backends.auto.AutoBackend`
        * `httpcore2._backends.anyio.AnyIOBackend`
        * `httpcore2._backends.trio.TrioBackend`
        * `httpcore2._backends.mock.AsyncMockBackend`
    * Base interfaces
        * `httpcore2._backends.base.NetworkBackend`
        * `httpcore2._backends.base.AsyncNetworkBackend`
* Exceptions
    * `httpcore2.TimeoutException`
        * `httpcore2.PoolTimeout`
        * `httpcore2.ConnectTimeout`
        * `httpcore2.ReadTimeout`
        * `httpcore2.WriteTimeout`
    * `httpcore2.NetworkError`
        * `httpcore2.ConnectError`
        * `httpcore2.ReadError`
        * `httpcore2.WriteError`
    * `httpcore2.ProtocolError`
        * `httpcore2.RemoteProtocolError`
        * `httpcore2.LocalProtocolError`
    * `httpcore2.ProxyError`
    * `httpcore2.UnsupportedProtocol`
