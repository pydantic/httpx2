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


def default_ssl_context() -> ssl.SSLContext:
    if cafile := os.environ.get("SSL_CERT_FILE"):  # pragma: no cover
        return ssl.create_default_context(cafile=cafile)
    if capath := os.environ.get("SSL_CERT_DIR"):  # pragma: no cover
        return ssl.create_default_context(capath=capath)
    return system_ssl_context()


def system_ssl_context() -> ssl.SSLContext:
    if platform.system() in ("Windows", "Darwin"):  # pragma: no cover
        return truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    # `truststore` re-adds verify paths per handshake, which OpenSSL < 3.4 accumulates (sethmlarson/truststore#212).
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    paths = ssl.get_default_verify_paths()
    if paths.cafile or (paths.capath and any(re.fullmatch(r"[0-9a-fA-F]{8}\.\d", f) for f in os.listdir(paths.capath))):
        ctx.set_default_verify_paths()
    elif cafile := next(filter(os.path.isfile, CA_FILE_CANDIDATES), None):  # pragma: no cover
        ctx.load_verify_locations(cafile=cafile)
    return ctx
