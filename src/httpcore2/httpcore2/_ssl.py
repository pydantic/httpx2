import os
import ssl

import truststore


def default_ssl_context() -> ssl.SSLContext:
    if cafile := os.environ.get("SSL_CERT_FILE"):
        return ssl.create_default_context(cafile=cafile)
    if capath := os.environ.get("SSL_CERT_DIR"):
        return ssl.create_default_context(capath=capath)
    return truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
