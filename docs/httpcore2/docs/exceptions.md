# Exceptions

The following exceptions may be raised when sending a request:

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
