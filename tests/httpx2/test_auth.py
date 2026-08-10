"""
Unit tests for auth classes.

Integration tests also exist in tests/client/test_auth.py
"""

from urllib.request import parse_keqv_list

import pytest

import httpx2


def test_basic_auth() -> None:
    auth = httpx2.BasicAuth(username="user", password="pass")
    request = httpx2.Request("GET", "https://www.example.com")

    # The initial request should include a basic auth header.
    flow = auth.sync_auth_flow(request)
    request = next(flow)
    assert request.headers["Authorization"].startswith("Basic")

    # No other requests are made.
    response = httpx2.Response(content=b"Hello, world!", status_code=200)
    with pytest.raises(StopIteration):
        flow.send(response)


def test_digest_auth_with_200() -> None:
    auth = httpx2.DigestAuth(username="user", password="pass")
    request = httpx2.Request("GET", "https://www.example.com")

    # The initial request should not include an auth header.
    flow = auth.sync_auth_flow(request)
    request = next(flow)
    assert "Authorization" not in request.headers

    # If a 200 response is returned, then no other requests are made.
    response = httpx2.Response(content=b"Hello, world!", status_code=200)
    with pytest.raises(StopIteration):
        flow.send(response)


def test_digest_auth_with_401() -> None:
    auth = httpx2.DigestAuth(username="user", password="pass")
    request = httpx2.Request("GET", "https://www.example.com")

    # The initial request should not include an auth header.
    flow = auth.sync_auth_flow(request)
    request = next(flow)
    assert "Authorization" not in request.headers

    # If a 401 response is returned, then a digest auth request is made.
    headers = {"WWW-Authenticate": 'Digest realm="...", qop="auth", nonce="...", opaque="..."'}
    response = httpx2.Response(content=b"Auth required", status_code=401, headers=headers, request=request)
    request = flow.send(response)
    assert request.headers["Authorization"].startswith("Digest")

    # No other requests are made.
    response = httpx2.Response(content=b"Hello, world!", status_code=200)
    with pytest.raises(StopIteration):
        flow.send(response)


def test_digest_auth_with_401_nonce_counting() -> None:
    auth = httpx2.DigestAuth(username="user", password="pass")
    request = httpx2.Request("GET", "https://www.example.com")

    # The initial request should not include an auth header.
    flow = auth.sync_auth_flow(request)
    request = next(flow)
    assert "Authorization" not in request.headers

    # If a 401 response is returned, then a digest auth request is made.
    headers = {"WWW-Authenticate": 'Digest realm="...", qop="auth", nonce="...", opaque="..."'}
    response = httpx2.Response(content=b"Auth required", status_code=401, headers=headers, request=request)
    first_request = flow.send(response)
    assert first_request.headers["Authorization"].startswith("Digest")

    # Each subsequent request contains the digest header by default...
    request = httpx2.Request("GET", "https://www.example.com")
    flow = auth.sync_auth_flow(request)
    second_request = next(flow)
    assert second_request.headers["Authorization"].startswith("Digest")

    # ... and the client nonce count (nc) is increased
    first_nc = parse_keqv_list(first_request.headers["Authorization"].split(", "))["nc"]
    second_nc = parse_keqv_list(second_request.headers["Authorization"].split(", "))["nc"]
    assert int(first_nc, 16) + 1 == int(second_nc, 16)

    # No other requests are made.
    response = httpx2.Response(content=b"Hello, world!", status_code=200)
    with pytest.raises(StopIteration):
        flow.send(response)


def set_cookies(request: httpx2.Request) -> httpx2.Response:
    headers = {
        "Set-Cookie": "session=.session_value...",
        "WWW-Authenticate": 'Digest realm="...", qop="auth", nonce="...", opaque="..."',
    }
    if request.url.path == "/auth":
        return httpx2.Response(content=b"Auth required", status_code=401, headers=headers)
    else:
        raise NotImplementedError()  # pragma: no cover


def test_digest_auth_setting_cookie_in_request() -> None:
    url = "https://www.example.com/auth"
    client = httpx2.Client(transport=httpx2.MockTransport(set_cookies))
    request = client.build_request("GET", url)

    auth = httpx2.DigestAuth(username="user", password="pass")
    flow = auth.sync_auth_flow(request)
    request = next(flow)
    assert "Authorization" not in request.headers

    response = client.get(url)
    assert len(response.cookies) > 0
    assert response.cookies["session"] == ".session_value..."

    request = flow.send(response)
    assert request.headers["Authorization"].startswith("Digest")
    assert request.headers["Cookie"] == "session=.session_value..."

    # No other requests are made.
    response = httpx2.Response(content=b"Hello, world!", status_code=200, request=request)
    with pytest.raises(StopIteration):
        flow.send(response)


def test_digest_auth_rfc_2069() -> None:
    # Example from https://datatracker.ietf.org/doc/html/rfc2069#section-2.4
    # with corrected response from https://www.rfc-editor.org/errata/eid749

    auth = httpx2.DigestAuth(username="Mufasa", password="CircleOfLife")
    request = httpx2.Request("GET", "https://www.example.com/dir/index.html")

    # The initial request should not include an auth header.
    flow = auth.sync_auth_flow(request)
    request = next(flow)
    assert "Authorization" not in request.headers

    # If a 401 response is returned, then a digest auth request is made.
    headers = {
        "WWW-Authenticate": (
            'Digest realm="testrealm@host.com", '
            'nonce="dcd98b7102dd2f0e8b11d0f600bfb0c093", '
            'opaque="5ccc069c403ebaf9f0171e9517f40e41"'
        )
    }
    response = httpx2.Response(content=b"Auth required", status_code=401, headers=headers, request=request)
    request = flow.send(response)
    assert request.headers["Authorization"].startswith("Digest")
    assert 'username="Mufasa"' in request.headers["Authorization"]
    assert 'realm="testrealm@host.com"' in request.headers["Authorization"]
    assert 'nonce="dcd98b7102dd2f0e8b11d0f600bfb0c093"' in request.headers["Authorization"]
    assert 'uri="/dir/index.html"' in request.headers["Authorization"]
    assert 'opaque="5ccc069c403ebaf9f0171e9517f40e41"' in request.headers["Authorization"]
    assert 'response="1949323746fe6a43ef61f9606e7febea"' in request.headers["Authorization"]

    # No other requests are made.
    response = httpx2.Response(content=b"Hello, world!", status_code=200)
    with pytest.raises(StopIteration):
        flow.send(response)


def test_digest_auth_rfc_7616_md5(monkeypatch: pytest.MonkeyPatch) -> None:
    # Example from https://datatracker.ietf.org/doc/html/rfc7616#section-3.9.1

    def mock_get_client_nonce(nonce_count: int, nonce: bytes) -> bytes:
        return b"f2/wE4q74E6zIJEtWaHKaf5wv/H5QzzpXusqGemxURZJ"

    auth = httpx2.DigestAuth(username="Mufasa", password="Circle of Life")
    monkeypatch.setattr(auth, "_get_client_nonce", mock_get_client_nonce)

    request = httpx2.Request("GET", "https://www.example.com/dir/index.html")

    # The initial request should not include an auth header.
    flow = auth.sync_auth_flow(request)
    request = next(flow)
    assert "Authorization" not in request.headers

    # If a 401 response is returned, then a digest auth request is made.
    headers = {
        "WWW-Authenticate": (
            'Digest realm="http-auth@example.org", '
            'qop="auth, auth-int", '
            "algorithm=MD5, "
            'nonce="7ypf/xlj9XXwfDPEoM4URrv/xwf94BcCAzFZH4GiTo0v", '
            'opaque="FQhe/qaU925kfnzjCev0ciny7QMkPqMAFRtzCUYo5tdS"'
        )
    }
    response = httpx2.Response(content=b"Auth required", status_code=401, headers=headers, request=request)
    request = flow.send(response)
    assert request.headers["Authorization"].startswith("Digest")
    assert 'username="Mufasa"' in request.headers["Authorization"]
    assert 'realm="http-auth@example.org"' in request.headers["Authorization"]
    assert 'uri="/dir/index.html"' in request.headers["Authorization"]
    assert "algorithm=MD5" in request.headers["Authorization"]
    assert 'nonce="7ypf/xlj9XXwfDPEoM4URrv/xwf94BcCAzFZH4GiTo0v"' in request.headers["Authorization"]
    assert "nc=00000001" in request.headers["Authorization"]
    assert 'cnonce="f2/wE4q74E6zIJEtWaHKaf5wv/H5QzzpXusqGemxURZJ"' in request.headers["Authorization"]
    assert "qop=auth" in request.headers["Authorization"]
    assert 'opaque="FQhe/qaU925kfnzjCev0ciny7QMkPqMAFRtzCUYo5tdS"' in request.headers["Authorization"]
    assert 'response="8ca523f5e9506fed4657c9700eebdbec"' in request.headers["Authorization"]

    # No other requests are made.
    response = httpx2.Response(content=b"Hello, world!", status_code=200)
    with pytest.raises(StopIteration):
        flow.send(response)


def test_digest_auth_rfc_7616_sha_256(monkeypatch: pytest.MonkeyPatch) -> None:
    # Example from https://datatracker.ietf.org/doc/html/rfc7616#section-3.9.1

    def mock_get_client_nonce(nonce_count: int, nonce: bytes) -> bytes:
        return b"f2/wE4q74E6zIJEtWaHKaf5wv/H5QzzpXusqGemxURZJ"

    auth = httpx2.DigestAuth(username="Mufasa", password="Circle of Life")
    monkeypatch.setattr(auth, "_get_client_nonce", mock_get_client_nonce)

    request = httpx2.Request("GET", "https://www.example.com/dir/index.html")

    # The initial request should not include an auth header.
    flow = auth.sync_auth_flow(request)
    request = next(flow)
    assert "Authorization" not in request.headers

    # If a 401 response is returned, then a digest auth request is made.
    headers = {
        "WWW-Authenticate": (
            'Digest realm="http-auth@example.org", '
            'qop="auth, auth-int", '
            "algorithm=SHA-256, "
            'nonce="7ypf/xlj9XXwfDPEoM4URrv/xwf94BcCAzFZH4GiTo0v", '
            'opaque="FQhe/qaU925kfnzjCev0ciny7QMkPqMAFRtzCUYo5tdS"'
        )
    }
    response = httpx2.Response(content=b"Auth required", status_code=401, headers=headers, request=request)
    request = flow.send(response)
    assert request.headers["Authorization"].startswith("Digest")
    assert 'username="Mufasa"' in request.headers["Authorization"]
    assert 'realm="http-auth@example.org"' in request.headers["Authorization"]
    assert 'uri="/dir/index.html"' in request.headers["Authorization"]
    assert "algorithm=SHA-256" in request.headers["Authorization"]
    assert 'nonce="7ypf/xlj9XXwfDPEoM4URrv/xwf94BcCAzFZH4GiTo0v"' in request.headers["Authorization"]
    assert "nc=00000001" in request.headers["Authorization"]
    assert 'cnonce="f2/wE4q74E6zIJEtWaHKaf5wv/H5QzzpXusqGemxURZJ"' in request.headers["Authorization"]
    assert "qop=auth" in request.headers["Authorization"]
    assert 'opaque="FQhe/qaU925kfnzjCev0ciny7QMkPqMAFRtzCUYo5tdS"' in request.headers["Authorization"]
    assert (
        'response="753927fa0e85d155564e2e272a28d1802ca10daf4496794697cf8db5856cb6c1"'
        in request.headers["Authorization"]
    )

    # No other requests are made.
    response = httpx2.Response(content=b"Hello, world!", status_code=200)
    with pytest.raises(StopIteration):
        flow.send(response)


def test_digest_auth_empty_realm() -> None:
    auth = httpx2.DigestAuth(username="user", password="pass")
    request = httpx2.Request("GET", "https://www.example.com")

    flow = auth.sync_auth_flow(request)
    request = next(flow)

    # Digest realm has been left empty.
    headers = {"WWW-Authenticate": 'Digest realm=, qop="auth", nonce="...", opaque="..."'}
    response = httpx2.Response(content=b"Auth required", status_code=401, headers=headers, request=request)
    request = flow.send(response)

    assert request.headers["Authorization"].startswith('Digest username="user", realm="", nonce="..."')


def test_digest_auth_importable_without_hashlib_md5(monkeypatch: pytest.MonkeyPatch) -> None:
    # Verify the import-time hasattr guard works by reloading the module
    # with hashlib.md5 removed, simulating a FIPS build that strips it entirely.
    import importlib

    from httpx2 import _auth

    monkeypatch.delattr("hashlib.md5")
    try:
        importlib.reload(_auth)
        assert "MD5" not in _auth.DigestAuth._ALGORITHM_TO_HASH_FUNCTION
        assert "SHA-256" in _auth.DigestAuth._ALGORITHM_TO_HASH_FUNCTION
    finally:
        monkeypatch.undo()
        importlib.reload(_auth)


def test_digest_auth_importable_with_blocked_hashlib_md5(monkeypatch: pytest.MonkeyPatch) -> None:
    # Verify the import-time guard works when hashlib.md5 exists but raises
    # ValueError, simulating a FIPS build that blocks MD5 at call time.
    import importlib

    from httpx2 import _auth

    def fips_blocked_md5(*args: object, **kwargs: object) -> None:
        raise ValueError("[digital envelope routines] disabled for FIPS")

    monkeypatch.setattr("hashlib.md5", fips_blocked_md5)
    try:
        importlib.reload(_auth)
        assert "MD5" not in _auth.DigestAuth._ALGORITHM_TO_HASH_FUNCTION
        assert "SHA-256" in _auth.DigestAuth._ALGORITHM_TO_HASH_FUNCTION
    finally:
        monkeypatch.undo()
        importlib.reload(_auth)


def test_digest_auth_fips_missing_md5(monkeypatch: pytest.MonkeyPatch) -> None:
    # On FIPS-enforced Python, hashlib.md5 may not exist.
    # Simulate by removing MD5 entries from the algorithm map.
    fips_algorithms = {k: v for k, v in httpx2.DigestAuth._ALGORITHM_TO_HASH_FUNCTION.items() if "MD5" not in k}
    monkeypatch.setattr(httpx2.DigestAuth, "_ALGORITHM_TO_HASH_FUNCTION", fips_algorithms)

    # SHA-256 digest auth should still work.
    auth = httpx2.DigestAuth(username="user", password="pass")
    request = httpx2.Request("GET", "https://www.example.com")

    flow = auth.sync_auth_flow(request)
    request = next(flow)

    headers = {"WWW-Authenticate": 'Digest realm="test", qop="auth", algorithm=SHA-256, nonce="abc", opaque="xyz"'}
    response = httpx2.Response(content=b"Auth required", status_code=401, headers=headers, request=request)
    request = flow.send(response)
    assert request.headers["Authorization"].startswith("Digest")
    assert "algorithm=SHA-256" in request.headers["Authorization"]


def test_digest_auth_unavailable_algorithm_raises_protocol_error(monkeypatch: pytest.MonkeyPatch) -> None:
    # When a server requests an algorithm not in the map, a clear error is raised.
    monkeypatch.setattr(httpx2.DigestAuth, "_ALGORITHM_TO_HASH_FUNCTION", {})

    auth = httpx2.DigestAuth(username="user", password="pass")
    request = httpx2.Request("GET", "https://www.example.com")

    flow = auth.sync_auth_flow(request)
    request = next(flow)

    headers = {"WWW-Authenticate": 'Digest realm="test", qop="auth", algorithm=MD5, nonce="abc", opaque="xyz"'}
    response = httpx2.Response(content=b"Auth required", status_code=401, headers=headers, request=request)
    with pytest.raises(httpx2.ProtocolError, match="Unsupported or unavailable digest auth algorithm"):
        flow.send(response)
