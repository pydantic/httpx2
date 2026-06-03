---
template: pyodide.html
---

# Emscripten Support

httpx2 has support for running on WebAssembly / Emscripten using
[Pyodide](https://github.com/pyodide/pyodide/).

Asynchronous requests always use `fetch`. Synchronous requests use the following
methods:
1. If [Javascript Promise Integration](https://github.com/WebAssembly/js-promise-integration/blob/main/proposals/js-promise-integration/Overview.md)
   (JSPI) is supported by the JavaScript runtime, the request will be made with
   `fetch` and stack switching.
2. Otherwise, if in a browser, the request will be made using a synchronous
   `XMLHttpRequest`.
3. Otherwise, if in Node, the request will fail. Synchronous requests in Node
   require JSPI.

In Emscripten, all network connections are handled by the enclosing Javascript
runtime. As such, there is limited control over various features. In particular:

- Proxy servers are handled by the runtime, so httpx2 cannot control them.
- httpx2 has no control over connection pooling.
- Certificate handling is done by the browser, so httpx2 cannot modify it.
- Requests are constrained by cross-origin isolation settings in the same way as
  any request that is originated by Javascript code.
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

<div id="pyodide_editor">import httpx2
print("Sending response using httpx2 in the browser:")
print("--------------------------------------------")
r = httpx2.get("https://www.example.com")
print("Status = ", r.status_code)
print("Response = ", r.text[:50], "...")</div>

<div id="pyodide_output"></div>

<div id="pyodide_buttons"></div>

## Build it

Because `httpx2` is a pure python module, building is the same as ever
(`python -m build`), or use the built wheel from PyPI.

## Testing Custom Builds of httpx2 in Emscripten

Once you have a wheel you can test it in your browser. You can do this using the
[Pyodide console](https://pyodide.org/en/stable/console.html), or by hosting
your own web page. You will need version 0.26.2 or later of Pyodide.

1.  To test in Pyodide console, serve the wheel file via http (e.g. by calling
    python -m `http.server` in the dist directory.) Then in the [Pyodide
    console](https://pyodide.org/en/stable/console.html), type the following,
    replacing the URL of the locally served wheel.

    ```python
    import pyodide_js as pjs
    import ssl, certifi, idna
    await pjs.loadPackage("<URL_OF_THE_WHEEL>")
    import httpx2
    # Now httpx2 should work
    ```

2.  To test a custom-built wheel in your own web page, create a page which loads
    the Pyodide JavaScript (see the
    [instructions](https://pyodide.org/en/stable/usage/index.html) on the
    Pyodide website). After starting the Pyodide runtime, run the following code
    to load httpx2 and its dependencies:
    ```js
    await pyodide.loadPackage([httpx2_wheel_url, "ssl", "certifi", "idna"])
    ```

3.  To test in Node.js, run `npm i pyodide` or download a Pyodide distribution
    download to a known folder, then load Pyodide following the instructions on
    the Pyodide website (https://pyodide.org/en/stable/usage/index.html). After
    starting the Pyodide runtime, run the following code to load httpx2 and its
    dependencies:
    ```js
    await pyodide.loadPackage([httpx2_wheel_url, "ssl", "certifi", "idna"])
    ```
