---
template: pyodide.html
---

# Emscripten Support

httpx2 has support for running on WebAssembly / Emscripten using
[Pyodide](https://github.com/pyodide/pyodide/) or
[emscripten-forge](https://emscripten-forge.org/). It uses the `httpx2-jsfetch`
package ([for Pyodide](https://github.com/hoodmane/httpx2-jsfetch) or
[for emscripten-forge](https://github.com/davidbrochart/httpx2-jsfetch-emscripten-forge))
to define the `JavascriptFetchTransport` and `AsyncJavascriptFetchTransport`.

Asynchronous requests always use `fetch`. Synchronous requests use the following
methods:
1. If [JavaScript Promise Integration](https://github.com/WebAssembly/js-promise-integration/blob/main/proposals/js-promise-integration/Overview.md)
   (JSPI) is supported by the JavaScript runtime, the request will be made with
   `fetch` and stack switching.
2. Otherwise, if in a browser, the request will be made using a synchronous
   `XMLHttpRequest`.
3. Otherwise, if in Node, the request will fail. Synchronous requests in Node
   require JSPI.

In Emscripten, all network connections are handled by the enclosing JavaScript
runtime. As such, there is limited control over various features. In particular:

- Proxy servers are handled by the runtime, so httpx2 cannot control them.
- httpx2 has no control over connection pooling.
- Certificate handling is done by the browser, so httpx2 cannot modify it.
- Requests are constrained by cross-origin isolation settings in the same way as
  any request that is originated by JavaScript code.
- Timeouts will not work in the main browser thread unless the browser supports
  JSPI because main thread synchronous `XMLHttpRequest` does not support
  timeouts.

Setting any of the transport options that depend on these features (`verify`,
`cert`, `http2`, `limits`, `proxy`, `uds`, `local_address`, `retries`, or
`socket_options`) will emit a `UserWarning` and the option will be silently
ignored.

## Try it in your browser

Use the following live example to test httpx2 in your web browser. You can
change the code below and hit run again to test different features or web
addresses.

<div id="pyodide_editor">
import httpx2
from js import document
print("Sending response using httpx2 in the browser:")
print("--------------------------------------------")
r = httpx2.get(document.location.origin)
print("Status = ", r.status_code)
print("Response = ", r.text[:50], "...")
</div>

<div id="pyodide_output"></div>

<div id="pyodide_buttons"></div>
