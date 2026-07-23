# Migrating from HTTPX

So, you have an application (or a library) using `httpx`, and you want to move to `httpx2`.

Good news: this is probably the easiest migration you will do this year.

`httpx2` is a fork of `httpx 0.28.1`. It has the same public API. The same `Client`, the same `AsyncClient`, the same `Response`, the same everything. The main thing that changed is the name.

Let's see how to do it, step by step.

## In a Hurry?

If your application depends on `httpx` directly, the whole migration is:

* Replace `httpx` with `httpx2` in your dependencies.
* Replace `import httpx` with `import httpx2` in your code.

```python
import httpx2

r = httpx2.get("https://www.example.org/")
```

That's it. Everything else works the same.

If you want an even smaller diff, you can alias the import and keep the rest of your code untouched:

```python
import httpx2 as httpx

r = httpx.get("https://www.example.org/")
```

Both styles are fine. The alias style is great for a first migration pass in a big codebase, and you can rename properly later (or never, we won't judge).

## You Can Have Both Installed

This is the part that matters if you have a **huge codebase**, or dependencies you don't control.

`httpx` and `httpx2` are different packages, with different import names. They don't conflict. They can be installed in the same environment, at the same time, and each one works normally.

This means:

* Your transitive dependencies that still require `httpx` keep working, unchanged.
* You don't have to wait for every library in your dependency tree to migrate before you do.
* You can migrate your own code **incrementally**, one module at a time, one pull request at a time. There is no flag day.

!!! tip
    Don't try to migrate a large application in one giant pull request. Add `httpx2` next to `httpx`, migrate your own modules gradually, and remove the `httpx` pin once nothing of yours (and none of your dependencies) imports it anymore.

To find what still uses `httpx` in your environment:

```shell
grep -rn "import httpx\b" src/
```

And to see which installed packages still depend on it:

```shell
pip install pipdeptree
pipdeptree --reverse --packages httpx
```

### But objects don't cross the boundary

There is one rule to keep in mind: the two packages can live side by side, but their objects can't stand in for each other.

`httpx2.Client` and `httpx.Client` are **distinct classes**. Same for `Response`, `Request`, `Timeout`, all of it. If a library that is still on `httpx` accepts a client and checks `isinstance(client, httpx.Client)` internally - or is simply written against `httpx` types - passing it an `httpx2.Client` will fail. The same goes for exceptions: an `httpx.HTTPError` raised inside a still-on-`httpx` dependency will not be caught by `except httpx2.HTTPError`.

So, the rule of thumb: **the package that receives the object decides which module you create it from.**

```python
import httpx
import httpx2
import some_library_still_on_httpx

# Your own code: httpx2.
client = httpx2.AsyncClient()

# A dependency that expects httpx: give it httpx.
some_library_still_on_httpx.configure(http_client=httpx.AsyncClient())
```

This is exactly why incremental migration works: your code moves to `httpx2`, while each dependency keeps receiving the types it expects, until it migrates too.

And to be clear: this boundary is not a permanent feature of your life. It only exists while both packages are in your environment. Every dependency that moves to `httpx2` removes a piece of it - the [For Package Maintainers](#for-package-maintainers) section below gives maintainers two proven, low-effort paths to do it, so point them there. The day your last dependency migrates, you uninstall `httpx`, and this whole section stops applying to you.

If you don't want to wait for that day - your application is on `httpx2`, but some dependencies still `import httpx`, and you want everyone to share the same classes now - there is an escape hatch, `alias_httpx()`:

```python
import httpx2

httpx2.alias_httpx()
```

After this call, `import httpx` resolves to `httpx2`, process-wide. Submodule imports included. Dependencies importing `httpx` get the `httpx2` classes, so `isinstance()` checks pass and `except` clauses catch across the boundary. You can pass your `httpx2.Client` anywhere and drop the `httpx` install entirely.

There are exactly two rules:

* **Call it first.** It must run at the very top of your entrypoint, before anything imports `httpx`. Modules that already imported `httpx` hold references the alias can't rewrite, so `alias_httpx()` raises a `RuntimeError` instead of half-working.
* **Applications only, never libraries.** It changes the meaning of `import httpx` for the whole process. That is an application's decision to make, not something a library should impose on its users.

!!! warning
    Don't do `sys.modules["httpx"] = httpx2` by hand instead. It looks equivalent, but any `from httpx.something import ...` in a dependency loads the module a second time under the `httpx` name, quietly recreating the two-class-hierarchies problem. `alias_httpx()` handles submodules correctly.

And a few things no aliasing can fix, so you know where the edges are:

* `importlib.metadata.version("httpx")` reads installed package metadata, not imports - it will raise or report the old `httpx` version.
* A library configuring `logging.getLogger("httpx")` won't see `httpx2`'s logs (the loggers are `httpx2` and `httpcore2.*`).
* Subprocesses and spawn-based workers re-import from scratch: each new process needs to call `alias_httpx()` too, before importing `httpx`.

Think of it as a bridge, not a destination: it gives you the end state today - one set of classes, no `httpx` install - while your dependencies catch up. Once the last one migrates, you delete the call and the bridge was never there.

## What Changed

The public API did not change. But a rename touches a few visible things, and there are a couple of behavior differences worth knowing about.

### Renames

* **Package and import**: `httpx` is now `httpx2`.
* **The CLI**: the command line client is now `httpx2` (installed with `httpx2[cli]`).
* **The User-Agent header**: the default is now `python-httpx2/<version>`. If you have server-side logic or tests matching on `python-httpx/...`, update them.
* **The loggers**: `httpx` is now `httpx2`, and `httpcore.*` is now `httpcore2.*`. If you configure logging by logger name, update those names.
* **The transport package**: `httpcore` is now `httpcore2`. If you import `httpcore` directly (for example, to catch low-level exceptions in a transport), switch those imports to `httpcore2`.

### Behavior differences

* **SSL verification uses the operating system's trust store**, via [`truststore`](https://truststore.readthedocs.io/), instead of `certifi`'s bundled certificates. For most users this just means fewer corporate-proxy headaches. The `SSL_CERT_FILE` and `SSL_CERT_DIR` environment variables are still honored first, and you can still pass `verify=` explicitly. See [SSL](advanced/ssl.md) for details.
* **Python 3.10 or newer is required**. Support for Python 3.9 was dropped.
* **Deprecation warnings are visible by default**. `httpx2` uses `HTTPXDeprecationWarning`, a `UserWarning` subclass, so you will actually see deprecations without configuring warning filters.

### New things you get

Not required for the migration, but nice to have:

* **Server-Sent Events** support is built in, via `client.sse()`. If you use `httpx-sse`, you can drop it. See [Server-Sent Events](sse.md).
* **WebSockets** support is built in, with the `httpx2[ws]` extra. If you use `httpx-ws`, you can drop it too. See [WebSockets](websockets.md).

## For Package Maintainers

If you maintain a library that depends on `httpx`, you have two good options, depending on whether you can release a new major version or not. Both have been battle-tested by real projects.

!!! warning
    Whatever you choose, don't mix the two packages in one code path. `httpx.Response` and `httpx2.Response` are distinct classes: an `isinstance()` check against one will not match objects from the other, and a `httpx2.Client` cannot be passed where your code truly requires a `httpx.Client`. Pick one module per code path, as the patterns below do.

### If you can release a major version: replace it

The simple option. Swap the dependency, rename the imports, document it as a breaking change.

This is what the [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk) did in [#2972](https://github.com/modelcontextprotocol/python-sdk/pull/2972) for its v2 release:

* `httpx` (and `httpx-sse`) replaced with `httpx2` in `pyproject.toml`.
* Imports renamed across the codebase.
* The change documented in the migration guide, including the visible differences (trust store verification, logger names, User-Agent).

For your users, the impact is small: if your API lets them pass their own client (an `http_client=...` parameter, for example), they only need to change which module they build it from. Everything else is internal to your library.

### If you can't release a major version: support both, prefer `httpx2`

Maybe your library is stable, widely deployed, and a major version is not on the table. You can still migrate, without breaking anyone.

The pattern: **try `httpx2` first, fall back to `httpx` with a deprecation warning, and point your install extra at `httpx2`** so that new installs get the new package.

This is what [Starlette](https://github.com/encode/starlette) did for its `TestClient` in [#3291](https://github.com/encode/starlette/pull/3291):

```python
import warnings
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import httpx2 as httpx
else:
    try:
        import httpx2 as httpx
    except ModuleNotFoundError:
        try:
            import httpx
        except ModuleNotFoundError:
            raise RuntimeError(
                "This module requires the httpx2 package to be installed.\n"
                "You can install it with:\n"
                "    $ pip install httpx2\n"
            ) from None
        else:
            warnings.warn(
                "Using `httpx` with this package is deprecated; install `httpx2` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
```

Then the rest of the module just uses `httpx.Client`, `httpx.Response`, etc., and it works with whichever package is installed.

How this behaves:

* Users with `httpx2` installed get `httpx2`. Silently, no changes needed.
* Users with only `httpx` installed keep working, with a deprecation warning telling them where things are going.
* Users with neither get a clear error pointing at `httpx2`.

And in your `pyproject.toml`, update the extra (or dependency) that installs the HTTP client so that new installs bring in `httpx2`:

```toml
[project.optional-dependencies]
full = [
    "httpx2>=2.0.0",
]
```

!!! tip
    Keep the `TYPE_CHECKING` branch importing a single module (`httpx2`, aliased as `httpx`). That way your type checker always sees one consistent set of types, while the runtime fallback stays flexible.

!!! note
    Use a warning class that is visible by default (a `UserWarning` subclass) if you have one. Starlette added `StarletteDeprecationWarning` for exactly this reason: a deprecation nobody sees is a deprecation that doesn't work.

Later, when you do get to release a major version, you delete the fallback branch and the pattern collapses into a plain `import httpx2`.

## Checklist

A quick summary to review before you ship:

* Dependencies: `httpx` replaced by (or accompanied by) `httpx2`.
* Imports: `import httpx` replaced by `import httpx2` (or aliased).
* Direct `httpcore` imports switched to `httpcore2`.
* Logging configuration updated for the `httpx2` and `httpcore2.*` logger names.
* Anything matching on the `python-httpx/...` User-Agent updated.
* Custom CA setups verified against the trust store behavior (or `verify=` passed explicitly).
* `httpx-sse` and `httpx-ws` dependencies dropped if you migrate to the built-in [SSE](sse.md) and [WebSockets](websockets.md) support.
* Dependencies still importing `httpx`: either keep `httpx` installed for them, or call `alias_httpx()` at the top of your entrypoint.

If you get stuck on something not covered here, open a [discussion](https://github.com/pydantic/httpx2/discussions) and we will help you out - and probably update this page too.
