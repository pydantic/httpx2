from __future__ import annotations

import ssl
import typing
from pathlib import Path

import certifi
import pytest

import httpx2

if typing.TYPE_CHECKING:
    from conftest import TestServer


def test_load_ssl_config() -> None:
    context = httpx2.create_ssl_context()
    assert context.verify_mode == ssl.VerifyMode.CERT_REQUIRED
    assert context.check_hostname is True


def test_load_ssl_config_verify_non_existing_file() -> None:
    with pytest.raises(IOError):
        context = httpx2.create_ssl_context()
        context.load_verify_locations(cafile="/path/to/nowhere")


def test_load_ssl_with_keylog(monkeypatch: typing.Any, tmp_path: Path) -> None:
    keylog_file = tmp_path / "sslkeylog"
    monkeypatch.setenv("SSLKEYLOGFILE", str(keylog_file))
    context = httpx2.create_ssl_context()
    assert context.keylog_filename == str(keylog_file)


def test_load_ssl_config_verify_existing_file() -> None:
    context = httpx2.create_ssl_context()
    context.load_verify_locations(capath=certifi.where())
    assert context.verify_mode == ssl.VerifyMode.CERT_REQUIRED
    assert context.check_hostname is True


def test_load_ssl_config_verify_directory() -> None:
    context = httpx2.create_ssl_context()
    context.load_verify_locations(capath=Path(certifi.where()).parent)
    assert context.verify_mode == ssl.VerifyMode.CERT_REQUIRED
    assert context.check_hostname is True


def test_load_ssl_config_cert_and_key(cert_pem_file: str, cert_private_key_file: str) -> None:
    context = httpx2.create_ssl_context()
    context.load_cert_chain(cert_pem_file, cert_private_key_file)
    assert context.verify_mode == ssl.VerifyMode.CERT_REQUIRED
    assert context.check_hostname is True


@pytest.mark.parametrize("password", [b"password", "password"])
def test_load_ssl_config_cert_and_encrypted_key(
    cert_pem_file: str, cert_encrypted_private_key_file: str, password: bytes | str
) -> None:
    context = httpx2.create_ssl_context()
    context.load_cert_chain(cert_pem_file, cert_encrypted_private_key_file, password)
    assert context.verify_mode == ssl.VerifyMode.CERT_REQUIRED
    assert context.check_hostname is True


def test_load_ssl_config_cert_and_key_invalid_password(
    cert_pem_file: str, cert_encrypted_private_key_file: str
) -> None:
    with pytest.raises(ssl.SSLError):
        context = httpx2.create_ssl_context()
        context.load_cert_chain(cert_pem_file, cert_encrypted_private_key_file, "password1")


def test_load_ssl_config_cert_without_key_raises(cert_pem_file: str) -> None:
    with pytest.raises(ssl.SSLError):
        context = httpx2.create_ssl_context()
        context.load_cert_chain(cert_pem_file)


def test_load_ssl_config_no_verify() -> None:
    context = httpx2.create_ssl_context(verify=False)
    assert context.verify_mode == ssl.VerifyMode.CERT_NONE
    assert context.check_hostname is False


def test_SSLContext_with_get_request(server: TestServer, cert_pem_file: str) -> None:
    context = httpx2.create_ssl_context()
    context.load_verify_locations(cert_pem_file)
    response = httpx2.get(server.url, verify=context)
    assert response.status_code == 200


def test_limits_repr() -> None:
    limits = httpx2.Limits(max_connections=100)
    expected = "Limits(max_connections=100, max_keepalive_connections=None, keepalive_expiry=5.0)"
    assert repr(limits) == expected


def test_limits_eq() -> None:
    limits = httpx2.Limits(max_connections=100)
    assert limits == httpx2.Limits(max_connections=100)


def test_timeout_eq() -> None:
    timeout = httpx2.Timeout(timeout=5.0)
    assert timeout == httpx2.Timeout(timeout=5.0)


def test_timeout_all_parameters_set() -> None:
    timeout = httpx2.Timeout(connect=5.0, read=5.0, write=5.0, pool=5.0)
    assert timeout == httpx2.Timeout(timeout=5.0)


def test_timeout_from_nothing() -> None:
    timeout = httpx2.Timeout(None)
    assert timeout.connect is None
    assert timeout.read is None
    assert timeout.write is None
    assert timeout.pool is None


def test_timeout_from_none() -> None:
    timeout = httpx2.Timeout(timeout=None)
    assert timeout == httpx2.Timeout(None)


def test_timeout_from_one_none_value() -> None:
    timeout = httpx2.Timeout(None, read=None)
    assert timeout == httpx2.Timeout(None)


def test_timeout_from_one_value() -> None:
    timeout = httpx2.Timeout(None, read=5.0)
    assert timeout == httpx2.Timeout(timeout=(None, 5.0, None, None))


def test_timeout_from_one_value_and_default() -> None:
    timeout = httpx2.Timeout(5.0, pool=60.0)
    assert timeout == httpx2.Timeout(timeout=(5.0, 5.0, 5.0, 60.0))


def test_timeout_missing_default() -> None:
    with pytest.raises(ValueError):
        httpx2.Timeout(pool=60.0)


def test_timeout_from_tuple() -> None:
    timeout = httpx2.Timeout(timeout=(5.0, 5.0, 5.0, 5.0))
    assert timeout == httpx2.Timeout(timeout=5.0)


def test_timeout_from_config_instance() -> None:
    timeout = httpx2.Timeout(timeout=5.0)
    assert httpx2.Timeout(timeout) == httpx2.Timeout(timeout=5.0)


def test_timeout_repr() -> None:
    timeout = httpx2.Timeout(timeout=5.0)
    assert repr(timeout) == "Timeout(timeout=5.0)"

    timeout = httpx2.Timeout(None, read=5.0)
    assert repr(timeout) == "Timeout(connect=None, read=5.0, write=None, pool=None)"


def test_proxy_from_url() -> None:
    proxy = httpx2.Proxy("https://example.com")

    assert str(proxy.url) == "https://example.com"
    assert proxy.auth is None
    assert proxy.headers == {}
    assert repr(proxy) == "Proxy('https://example.com')"


def test_proxy_with_auth_from_url() -> None:
    proxy = httpx2.Proxy("https://username:password@example.com")

    assert str(proxy.url) == "https://example.com"
    assert proxy.auth == ("username", "password")
    assert proxy.headers == {}
    assert repr(proxy) == "Proxy('https://example.com', auth=('username', '********'))"


def test_invalid_proxy_scheme() -> None:
    with pytest.raises(ValueError):
        httpx2.Proxy("invalid://example.com")
