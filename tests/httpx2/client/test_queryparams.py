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
