import os
import platform
import re
import ssl

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


def default_ssl_context(trust_env: bool = True) -> ssl.SSLContext:
    if trust_env and (cafile := os.environ.get("SSL_CERT_FILE")):  # pragma: no cover
        return ssl.create_default_context(cafile=cafile)
    if trust_env and (capath := os.environ.get("SSL_CERT_DIR")):  # pragma: no cover
        return ssl.create_default_context(capath=capath)
    if platform.system() in ("Windows", "Darwin"):  # pragma: no cover
        return truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    # `truststore` re-adds verify paths per handshake, which OpenSSL < 3.4 accumulates (sethmlarson/truststore#212).
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    # Compiled-in paths, since `set_default_verify_paths()` reads `SSL_CERT_*` even when `trust_env` is false.
    paths = ssl.get_default_verify_paths()
    cafile = paths.openssl_cafile if os.path.isfile(paths.openssl_cafile) else None
    capath = paths.openssl_capath if os.path.isdir(paths.openssl_capath) else None
    if capath and not cafile:  # pragma: no cover
        if not any(re.fullmatch(r"[0-9a-fA-F]{8}\.\d", f) for f in os.listdir(capath)):
            capath = None
    if not cafile and not capath:  # pragma: no cover
        cafile = next(filter(os.path.isfile, CA_FILE_CANDIDATES), None)
    if cafile or capath:
        ctx.load_verify_locations(cafile, capath)
    return ctx
