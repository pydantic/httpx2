from __future__ import annotations

import logging
import socket
import ssl
import threading
from concurrent.futures import ThreadPoolExecutor

import h2.config
import h2.connection
import h2.events
import pytest
import trustme

import httpx2


def test_http2_threaded_debug_requests(caplog: pytest.LogCaptureFixture) -> None:
    barrier = threading.Barrier(2, timeout=5)

    class ConcurrentHeaders(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            if record.getMessage().startswith("send_request_headers.started"):
                barrier.wait()
            return True

    ca = trustme.CA()
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ca.issue_cert("localhost").configure_cert(server_context)
    server_context.set_alpn_protocols(["h2"])
    client_context = ssl.create_default_context()
    ca.configure_trust(client_context)

    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        listener.settimeout(5)
        port = listener.getsockname()[1]

        def serve() -> None:
            raw, _ = listener.accept()
            with raw, server_context.wrap_socket(raw, server_side=True) as stream:
                stream.settimeout(5)
                peer = h2.connection.H2Connection(h2.config.H2Configuration(client_side=False))
                peer.initiate_connection()
                stream.sendall(peer.data_to_send())
                paths: dict[int, bytes] = {}
                bodies: dict[int, bytearray] = {}
                while data := stream.recv(65536):
                    for event in peer.receive_data(data):
                        if isinstance(event, h2.events.RequestReceived):
                            paths[event.stream_id] = dict(event.headers)[b":path"]
                            bodies[event.stream_id] = bytearray()
                        elif isinstance(event, h2.events.DataReceived):
                            bodies[event.stream_id].extend(event.data)
                            peer.acknowledge_received_data(event.flow_controlled_length, event.stream_id)
                        elif isinstance(event, h2.events.StreamEnded):
                            body = bodies.pop(event.stream_id)
                            path = paths.pop(event.stream_id)
                            assert body == path * (32768 // len(path))
                            peer.send_headers(event.stream_id, [(b":status", b"200"), (b"x-path", path)])
                            peer.send_data(event.stream_id, str(len(body)).encode(), end_stream=True)
                    stream.sendall(peer.data_to_send())

        with ThreadPoolExecutor(max_workers=3) as executor:
            server = executor.submit(serve)
            with httpx2.Client(http2=True, verify=client_context, trust_env=False, timeout=5) as client:
                url = f"https://localhost:{port}"
                assert client.post(url + "/warmup", content=b"/warmup" * (32768 // 7)).status_code == 200
                caplog.set_level(logging.DEBUG, logger="httpcore2.http2")
                debug_logger = logging.getLogger("httpcore2.http2")
                debug_filter = ConcurrentHeaders()
                debug_logger.addFilter(debug_filter)
                try:
                    futures = [
                        executor.submit(client.post, url + path, content=path.encode() * (32768 // len(path)))
                        for path in ("/one", "/two")
                    ]
                    for path, future in zip(("/one", "/two"), futures, strict=True):
                        response = future.result(timeout=10)
                        assert response.http_version == "HTTP/2"
                        assert response.headers["x-path"] == path
                        assert response.content == b"32768"
                finally:
                    debug_logger.removeFilter(debug_filter)
            server.result(timeout=10)
