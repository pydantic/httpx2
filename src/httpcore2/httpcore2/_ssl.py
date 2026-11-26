import os
import platform
import re
import socket
import ssl
import threading

import truststore

# Mirrors `truststore`'s Linux fallbacks for when OpenSSL's compiled-in paths are unusable:
# https://github.com/sethmlarson/truststore/blob/0714f72a739d182cdb8502f4e1bb4cf7ebfe4eb5/src/truststore/_openssl.py#L8-L19
CA_FILE_CANDIDATES = [
    "/etc/ssl/cert.pem",
    "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
    "/etc/pki/tls/cert.pem",
    "/etc/ssl/certs/ca-certificates.crt",
    "/etc/ssl/ca-bundle.pem",
]


class _DeferredCAContext(ssl.SSLContext):
    """Loads the system CA certificates on the first `wrap_socket()` or `wrap_bio()`, so building it does no file I/O.

    `anyio` runs `wrap_bio()` in a worker thread for `ssl.SSLContext` subclasses, keeping the first load off the event
    loop. The load then turns the context into a plain `ssl.SSLContext`, which `anyio` wraps without a thread.
    """

    def __init__(self, protocol: int) -> None:
        self._ca_lock = threading.Lock()
        self._ca_loaded = False

    def _load_ca(self) -> None:
        with self._ca_lock:
            if not self._ca_loaded:
                # Compiled-in paths, since `set_default_verify_paths()` would read `SSL_CERT_*`.
                paths = ssl.get_default_verify_paths()
                cafile = paths.openssl_cafile if os.path.isfile(paths.openssl_cafile) else None
                capath = paths.openssl_capath if os.path.isdir(paths.openssl_capath) else None
                if capath and not cafile:  # pragma: no cover
                    if not any(re.fullmatch(r"[0-9a-fA-F]{8}\.\d", f) for f in os.listdir(capath)):
                        capath = None
                if not cafile and not capath:  # pragma: no cover
                    cafile = next(filter(os.path.isfile, CA_FILE_CANDIDATES), None)
                if cafile or capath:
                    self.load_verify_locations(cafile, capath)
                self._ca_loaded = True
                self.__class__ = ssl.SSLContext  # type: ignore[assignment]

    # The base calls name `ssl.SSLContext`: `super()` raises once another thread's load has changed `__class__`.
    def wrap_socket(
        self,
        sock: socket.socket,
        server_side: bool = False,
        do_handshake_on_connect: bool = True,
        suppress_ragged_eofs: bool = True,
        server_hostname: str | bytes | None = None,
        session: ssl.SSLSession | None = None,
    ) -> ssl.SSLSocket:
        self._load_ca()
        return ssl.SSLContext.wrap_socket(
            self, sock, server_side, do_handshake_on_connect, suppress_ragged_eofs, server_hostname, session
        )

    def wrap_bio(
        self,
        incoming: ssl.MemoryBIO,
        outgoing: ssl.MemoryBIO,
        server_side: bool = False,
        server_hostname: str | bytes | None = None,
        session: ssl.SSLSession | None = None,
    ) -> ssl.SSLObject:
        self._load_ca()
        return ssl.SSLContext.wrap_bio(self, incoming, outgoing, server_side, server_hostname, session)


# `_trust_env` is internal to `httpx2`; drop it with the Linux branch below once sethmlarson/truststore#213 ships.
def default_ssl_context(*, _trust_env: bool = True) -> ssl.SSLContext:
    if _trust_env and (cafile := os.environ.get("SSL_CERT_FILE")):  # pragma: no cover
        return ssl.create_default_context(cafile=cafile)
    if _trust_env and (capath := os.environ.get("SSL_CERT_DIR")):  # pragma: no cover
        return ssl.create_default_context(capath=capath)
    if platform.system() in ("Windows", "Darwin"):  # pragma: no cover
        return truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    # `truststore` re-adds verify paths per handshake, which OpenSSL < 3.4 accumulates (sethmlarson/truststore#212).
    return _DeferredCAContext(ssl.PROTOCOL_TLS_CLIENT)
