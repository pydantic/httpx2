from __future__ import annotations

import pathlib
import socket
import ssl
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager

import h2.config
import h2.connection
import h2.events
import h2.settings
import hyperframe.frame
import pytest
import trustme

TESTS_DIR = pathlib.Path(__file__).parent
FIXTURES_DIR = TESTS_DIR / "fixtures"


@contextmanager
def http2_peer(
    mode: str, localhost_cert: trustme.LeafCert, *, proxy: bool = False
) -> Iterator[tuple[str, threading.Event, threading.Event, bool]]:
    response_read = threading.Event()
    peer_ready = threading.Event()
    stopping = threading.Event()
    close = mode in ("tls", "buffered_tls", "tcp", "goaway", "invalid")
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    localhost_cert.configure_cert(context)
    context.set_alpn_protocols(["h2", "http/1.1"])

    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        listener.settimeout(5)
        url = f"https://localhost:{listener.getsockname()[1]}"

        def serve() -> None:
            bodies = []
            with ExitStack() as stack:
                stream = None
                remaining = b""
                for index in range(2):
                    if stream is None:
                        sock, _ = listener.accept()
                        stream = stack.enter_context(sock)
                        stream.settimeout(5)
                        if proxy:
                            stream = stack.enter_context(context.wrap_socket(stream, server_side=True))
                            connect = b""
                            while not connect.endswith(b"\r\n\r\n"):
                                data = stream.recv(65536)
                                assert data
                                connect += data
                            assert connect.startswith(b"CONNECT ")
                            stream.sendall(b"HTTP/1.1 200 OK\r\n\r\n")
                        incoming, outgoing = ssl.MemoryBIO(), ssl.MemoryBIO()
                        tls = context.wrap_bio(incoming, outgoing, server_side=True)
                        while True:
                            try:
                                tls.do_handshake()
                                break
                            except ssl.SSLWantReadError:
                                stream.sendall(outgoing.read())
                                data = stream.recv(65536)
                                assert data
                                incoming.write(data)
                        connection = h2.connection.H2Connection(h2.config.H2Configuration(client_side=False))
                        connection.initiate_connection()
                        tls.write(connection.data_to_send())
                        stream.sendall(outgoing.read())
                        settings_acknowledged = False
                    body = b""
                    stream_id = None
                    ping_acknowledged = False
                    while stream_id is None or not settings_acknowledged:
                        try:
                            data = tls.read(65536)
                        except ssl.SSLWantReadError:
                            data = stream.recv(65536)
                            assert data
                            incoming.write(data)
                            continue
                        assert data
                        for event in connection.receive_data(data):
                            if isinstance(event, h2.events.SettingsAcknowledged):
                                settings_acknowledged = True
                            elif isinstance(event, h2.events.PingAckReceived):
                                ping_acknowledged = True
                            elif isinstance(event, h2.events.RequestReceived) and index == 1:
                                if mode == "ping":
                                    assert ping_acknowledged
                                elif mode == "settings":
                                    assert settings_acknowledged
                                elif mode in ("partial", "partial_tls"):
                                    stream.sendall(remaining)
                            elif isinstance(event, h2.events.DataReceived):
                                body += event.data
                            elif isinstance(event, h2.events.StreamEnded):
                                stream_id = event.stream_id
                        if data := connection.data_to_send():
                            tls.write(data)
                            stream.sendall(outgoing.read())
                    bodies.append(body)
                    connection.send_headers(stream_id, [(":status", "200")])
                    connection.send_data(stream_id, body, end_stream=mode != "incomplete")
                    tls.write(connection.data_to_send())
                    if index == 0 and mode == "buffered_tls":
                        with pytest.raises(ssl.SSLWantReadError):
                            tls.unwrap()
                    stream.sendall(outgoing.read())
                    if mode == "incomplete":
                        stream.close()
                    if index == 0:
                        assert response_read.wait(5)
                        if stopping.is_set():
                            return
                        if mode == "tls":
                            with pytest.raises(ssl.SSLWantReadError):
                                tls.unwrap()
                            stream.sendall(outgoing.read())
                        elif mode == "tcp":
                            stream.close()
                        elif mode == "goaway":
                            connection.close_connection()
                            tls.write(connection.data_to_send())
                            stream.sendall(outgoing.read())
                        elif mode in ("ping", "partial", "partial_tls"):
                            connection.ping(b"12345678")
                            data = connection.data_to_send()
                            if mode == "partial":
                                tls.write(data[:5])
                                stream.sendall(outgoing.read())
                                tls.write(data[5:])
                                remaining = outgoing.read()
                            else:
                                tls.write(data)
                                data = outgoing.read()
                                if mode == "partial_tls":
                                    stream.sendall(data[:5])
                                    remaining = data[5:]
                                else:
                                    stream.sendall(data)
                        elif mode == "invalid":
                            tls.write(
                                hyperframe.frame.SettingsFrame(
                                    settings={h2.settings.SettingCodes.MAX_FRAME_SIZE: 0}
                                ).serialize()
                            )
                            stream.sendall(outgoing.read())
                        elif mode == "settings":
                            settings_acknowledged = False
                            connection.update_settings({h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS: 1})
                            tls.write(connection.data_to_send())
                            stream.sendall(outgoing.read())
                        if close:
                            stream = None
                        peer_ready.set()
                assert bodies == [b"first", b"second"], "Unexpected request bodies"

        with ThreadPoolExecutor(max_workers=1) as executor:
            server = executor.submit(serve)
            try:
                yield url, response_read, peer_ready, close
            finally:
                stopping.set()
                response_read.set()
            server.result(timeout=10)
