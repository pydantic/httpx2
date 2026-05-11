# Exceptions

The following exceptions may be raised when sending a request:

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
