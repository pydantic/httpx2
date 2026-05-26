import json

from pytest_httpbin.serve import Server

import httpcore2


def test_request(httpbin: Server) -> None:
    response = httpcore2.request("GET", httpbin.url)
    assert response.status == 200


def test_stream(httpbin: Server) -> None:
    with httpcore2.stream("GET", httpbin.url) as response:
        assert response.status == 200


def test_request_with_content(httpbin: Server) -> None:
    url = f"{httpbin.url}/post"
    response = httpcore2.request("POST", url, content=b'{"hello":"world"}')
    assert response.status == 200
    assert json.loads(response.content)["json"] == {"hello": "world"}
