import httpx2


def hello_world(request: httpx2.Request) -> httpx2.Response:
    return httpx2.Response(200, text="Hello, world")


def test_client_queryparams() -> None:
    client = httpx2.Client(params={"a": "b"})
    assert isinstance(client.params, httpx2.QueryParams)
    assert client.params["a"] == "b"


def test_client_queryparams_string() -> None:
    client = httpx2.Client(params="a=b")
    assert isinstance(client.params, httpx2.QueryParams)
    assert client.params["a"] == "b"

    client = httpx2.Client()
    client.params = "a=b"
    assert isinstance(client.params, httpx2.QueryParams)
    assert client.params["a"] == "b"


def test_client_queryparams_echo() -> None:
    url = "http://example.org/echo_queryparams"
    client_queryparams = "first=str"
    request_queryparams = {"second": "dict"}
    client = httpx2.Client(transport=httpx2.MockTransport(hello_world), params=client_queryparams)
    response = client.get(url, params=request_queryparams)

    assert response.status_code == 200
    assert response.url == "http://example.org/echo_queryparams?first=str&second=dict"


def test_base_url_with_request_params() -> None:
    # Query params in the request URL must not be dropped when request-level
    # params are also passed.
    client = httpx2.Client(base_url="https://api.example.com/v1/")
    request = client.build_request("GET", "users?active=true", params={"page": "2"})

    assert str(request.url) == "https://api.example.com/v1/users?active=true&page=2"


def test_base_url_with_client_params_and_url_query() -> None:
    # Client-level params must be appended to query params already present in
    # the request URL, not replace them.
    client = httpx2.Client(base_url="https://api.example.com/v1/", params={"api_key": "abc"})
    request = client.build_request("GET", "users?active=true")

    assert str(request.url) == "https://api.example.com/v1/users?active=true&api_key=abc"


def test_base_url_with_client_params_request_params_and_url_query() -> None:
    # All three sources of query params (URL, client-level, request-level)
    # must be combined without any being dropped.
    client = httpx2.Client(base_url="https://api.example.com/v1/", params={"api_key": "abc"})
    request = client.build_request("GET", "users?active=true", params={"page": "2"})

    assert str(request.url) == "https://api.example.com/v1/users?active=true&api_key=abc&page=2"
