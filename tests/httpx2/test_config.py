from __future__ import annotations

import asyncio
import gc
import platform
import socket
import ssl
import threading
import warnings
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
import trustme

import httpx2

if TYPE_CHECKING:
    from conftest import TestServer


def test_load_ssl_config() -> None:
    context = httpx2.create_ssl_context()
    assert context.verify_mode == ssl.VerifyMode.CERT_REQUIRED
    assert context.check_hostname is True


def test_load_ssl_config_verify_non_existing_file() -> None:
    with pytest.raises(IOError):
        context = httpx2.create_ssl_context()
        context.load_verify_locations(cafile="/path/to/nowhere")


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


@pytest.mark.parametrize("trust_env", [True, False])
def test_create_ssl_context_trust_env(
    trust_env: bool, cert_authority: trustme.CA, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ca_file = tmp_path / "ca.pem"
    cert_authority.cert_pem.write_to_path(str(ca_file))
    monkeypatch.setenv("SSL_CERT_FILE", str(ca_file))
    # `truststore`'s Windows and macOS contexts do not implement `get_ca_certs()`.
    monkeypatch.setattr(platform, "system", lambda: "Linux")

    context = httpx2.create_ssl_context(trust_env=trust_env)

    ca_der = ssl.PEM_cert_to_DER_cert(cert_authority.cert_pem.bytes().decode())
    assert (ca_der in context.get_ca_certs(binary_form=True)) is trust_env


@pytest.fixture
def ca_lookups(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """The thread id of each system CA lookup made by the default Linux context."""
    monkeypatch.setattr(platform, "system", lambda: "Linux")
    lookups: list[int] = []
    get_default_verify_paths = ssl.get_default_verify_paths

    def record() -> ssl.DefaultVerifyPaths:
        lookups.append(threading.get_ident())
        return get_default_verify_paths()

    monkeypatch.setattr(ssl, "get_default_verify_paths", record)
    return lookups


def test_client_construction_does_not_load_system_ca(ca_lookups: list[int]) -> None:
    httpx2.Client()
    httpx2.AsyncClient()
    assert ca_lookups == []


def test_default_ssl_context_loads_system_ca_once(ca_lookups: list[int]) -> None:
    context = httpx2.create_ssl_context()
    assert ca_lookups == []

    # Already the default; CodeQL's `py/insecure-protocol` flags `wrap_socket()` without it.
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    with socket.socket() as sock, context.wrap_socket(sock, server_hostname="example.com"):
        pass
    assert len(ca_lookups) == 1
    # A plain `ssl.SSLContext` is wrapped by `anyio` on the event loop, without a worker thread.
    assert type(context) is ssl.SSLContext

    context.wrap_bio(ssl.MemoryBIO(), ssl.MemoryBIO(), server_hostname="example.com")
    assert len(ca_lookups) == 1


# `trio.SSLStream` calls `wrap_bio()` on the event loop; `anyio` moves it to a worker thread.
@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio"])
async def test_default_ssl_context_loads_system_ca_off_the_event_loop(
    ca_lookups: list[int], cert_authority: trustme.CA
) -> None:
    async def respond(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.readuntil(b"\r\n\r\n")
        writer.write(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
        writer.close()
        await writer.wait_closed()

    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    cert_authority.issue_cert("127.0.0.1").configure_cert(server_context)
    context = httpx2.create_ssl_context()
    cert_authority.configure_trust(context)

    async with await asyncio.start_server(respond, "127.0.0.1", 0, ssl=server_context) as server:
        port = server.sockets[0].getsockname()[1]
        async with httpx2.AsyncClient(verify=context) as client:
            response = await client.get(f"https://127.0.0.1:{port}/")

    assert response.status_code == 204
    assert len(ca_lookups) == 1
    assert ca_lookups[0] != threading.get_ident()


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["trio"])
async def test_trio_unloadable_system_ca_raises_connect_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bundle = tmp_path / "ca.pem"
    bundle.write_text("not a certificate\n")
    missing = str(tmp_path / "missing")
    paths = ssl.DefaultVerifyPaths(str(bundle), missing, "SSL_CERT_FILE", str(bundle), "SSL_CERT_DIR", missing)
    monkeypatch.setattr(platform, "system", lambda: "Linux")
    monkeypatch.setattr(ssl, "get_default_verify_paths", lambda: paths)

    with warnings.catch_warnings(record=True) as caught, socket.create_server(("127.0.0.1", 0)) as server:
        warnings.simplefilter("always")
        port = server.getsockname()[1]
        async with httpx2.AsyncClient() as client:
            with pytest.raises(httpx2.ConnectError, match="NO_CERTIFICATE_OR_CRL_FOUND"):
                await client.get(f"https://127.0.0.1:{port}/")
        gc.collect()

    assert [w for w in caught if issubclass(w.category, ResourceWarning)] == []


def test_create_ssl_context_verify_str(cert_pem_file: str) -> None:
    with pytest.warns(DeprecationWarning, match="`verify=<str>` is deprecated"):
        context = httpx2.create_ssl_context(verify=cert_pem_file)
    assert context.verify_mode == ssl.VerifyMode.CERT_REQUIRED


def test_create_ssl_context_verify_str_with_cert_raises(cert_pem_file: str, cert_private_key_file: str) -> None:
    with pytest.raises(TypeError, match="cannot be combined with `cert=...`"):
        httpx2.create_ssl_context(verify=cert_pem_file, cert=(cert_pem_file, cert_private_key_file))


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


def test_timeout_from_unsupported_type() -> None:
    with pytest.raises(TypeError, match="got <class 'object'>"):
        httpx2.Timeout(object())  # type: ignore[arg-type]


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
