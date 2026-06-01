import ssl
from pathlib import Path

import certifi
import pytest
import truststore

from httpcore2._ssl import default_ssl_context


def test_default_ssl_context() -> None:
    context = default_ssl_context()
    assert isinstance(context, truststore.SSLContext)
    assert context.verify_mode == ssl.VerifyMode.CERT_REQUIRED


def test_default_ssl_context_with_cert_file(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SSL_CERT_FILE", certifi.where())
    context = default_ssl_context()
    assert isinstance(context, ssl.SSLContext)
    assert context.verify_mode == ssl.VerifyMode.CERT_REQUIRED


def test_default_ssl_context_with_cert_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    monkeypatch.setenv("SSL_CERT_DIR", str(tmp_path))
    context = default_ssl_context()
    assert isinstance(context, ssl.SSLContext)
    assert context.verify_mode == ssl.VerifyMode.CERT_REQUIRED
