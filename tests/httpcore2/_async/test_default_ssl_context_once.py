"""The default TLS context is built once per pool, not once per connection:
loading CA certificates is expensive and an explicit ssl_context already
travels down to every connection (#1368)."""

import ssl
from pathlib import Path
from typing import Any

import hpack
import hyperframe.frame
import pytest

import httpcore2

CERT_PEM = (
    "-----BEGIN CERTIFICATE-----\n"
    "MIIDCTCCAfGgAwIBAgIUWKx/thjl7fPZQ9+E90c/pYTeTbEwDQYJKoZIhvcNAQEL\n"
    "BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MTEyNzE1NTU1M1oXDTI2MTEy\n"
    "ODE1NTU1M1owFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF\n"
    "AAOCAQ8AMIIBCgKCAQEAhmlTzxyOsHYW5hVKxYWyecneu12niona7kS9hY8DPiRO\n"
    "EWKtTWAVYTF38Mj0Se+/pdmfwPNnsRLAghIAKlP+7s4FthaK6ugRmuRKB0ijJSg9\n"
    "5bSxcOVObtwHKzxloWXCf3PE+Mujcrew7EdAIXJ5w4YugyjE/flENktQte2ptVxQ\n"
    "PyQ7heXxM/t79K5jrH4lQ5MQJZxG9elGbq2ClUY9aNnDG/aLn9JlmbU1IwUH5x4Q\n"
    "+RiK9ufxj5ODVpr/9zVUjgEFbL0K2vzNRaewjQ2n5s8/wUEGGDv12CLmjaNE+0wt\n"
    "3xSIDGFYpT8TKig0KfnIUdpCUtyOI4tENoOtzf36EQIDAQABo1MwUTAdBgNVHQ4E\n"
    "FgQU7V2kOV9fjhyhVeEOKfOuagMK6zcwHwYDVR0jBBgwFoAU7V2kOV9fjhyhVeEO\n"
    "KfOuagMK6zcwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAOuxP\n"
    "+qCk76fM5UhOjGKTUjapNmatI5FMISnzFYfJm45EGA5Oee4mLiY4MT+FCuuwfBLy\n"
    "8tu1SgNLU2V8jmITDErW0suh3NDu6y8Cvaab7wKcMpOvMOLGa6+Fu7qTTBg8SmJ2\n"
    "YGa6AuKOUTd+ZGA8jkNyWCrDPH95lCqy6vNfipd9Kl6lb92UzcF/A7IMR2NRSI1q\n"
    "QmYdSheC6HfFya3qq9vj4emDd2vyRGFq3JE51jgN95hbgWlsKHNPF7kC1StHK6/V\n"
    "wJtm5XFskOwecOJEQmmbNVYfkcvIhjaIsbkTkVdQVxl8vUh0phFlx1n/+0wrN4xP\n"
    "LK56Y1woomy4FVn6RA==\n"
    "-----END CERTIFICATE-----\n"
)


@pytest.mark.anyio
async def test_default_ssl_context_built_once_per_pool(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cert_file = tmp_path / "ca.pem"
    cert_file.write_text(CERT_PEM, encoding="utf-8")
    monkeypatch.setenv("SSL_CERT_FILE", str(cert_file))

    calls = 0
    original = ssl.SSLContext.load_verify_locations

    def counting(self: ssl.SSLContext, *args: Any, **kwargs: Any) -> None:
        nonlocal calls
        calls += 1
        original(self, *args, **kwargs)

    monkeypatch.setattr(ssl.SSLContext, "load_verify_locations", counting)

    frames = [
        hyperframe.frame.HeadersFrame(
            stream_id=stream_id,
            data=hpack.Encoder().encode([(b":status", b"200")]),
            flags=["END_HEADERS", "END_STREAM"],
        ).serialize()
        for stream_id in (1, 3, 5)
    ]
    backend = httpcore2.AsyncMockBackend(frames, http2=True)

    async with httpcore2.AsyncConnectionPool(network_backend=backend, http2=True) as pool:
        for _ in range(3):
            response = await pool.request("GET", "https://example.com/")
            assert response.status == 200

    assert calls == 1
