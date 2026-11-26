#!/usr/bin/env python3
"""TLS wire observer for a private PostgreSQL fixture.

Both legs use the same certificate identity, including SCRAM channel binding.
Only protocol boundaries and socket outcomes are logged.
"""

import select
import socket
import ssl
import struct
import threading
import time
from pathlib import Path

SSL_REQUEST = struct.pack("!II", 8, 80877103)
TERMINATE = b"X\0\0\0\x04"
STATE = Path("/wire-state")


def receive_exact(sock, size):
    data = bytearray()
    while len(data) < size:
        addition = sock.recv(size - len(data))
        if not addition:
            raise EOFError
        data.extend(addition)
    return bytes(data)


def event(connection, outcome):
    print(f"wire connection={connection} event={outcome}", flush=True)


def pause(connection, stage, sock):
    marker = STATE / stage
    if marker.exists():
        event(connection, stage)
    while marker.exists():
        if select.select([sock], [], [], 0.025)[0]:
            if not sock.recv(1, socket.MSG_PEEK):
                raise EOFError


def forward(front, back, connection):
    # The first PostgreSQL packet after TLS is the untagged startup message.
    head = receive_exact(front, 4)
    back.sendall(head + receive_exact(front, struct.unpack("!I", head)[0] - 4))
    pending = bytearray()
    backend_open = True
    terminating_at = None
    commits = 0
    while True:
        sockets = [front, back] if backend_open else [front]
        ready = [sock for sock in sockets if sock.pending()]
        if not ready:
            ready = select.select(sockets, [], [], 0.2)[0]
        if not ready and terminating_at and time.monotonic() - terminating_at > 2:
            raise TimeoutError
        for sock in ready:
            data = sock.recv(65536)
            if sock is back:
                if data:
                    front.sendall(data)
                else:
                    backend_open = False
                continue
            if not data:
                event(connection, "close_notify")
                return
            pending.extend(data)
            while len(pending) >= 5:
                size = struct.unpack_from("!I", pending, 1)[0] + 1
                if len(pending) < size:
                    break
                packet = bytes(pending[:size])
                del pending[:size]
                if packet == TERMINATE:
                    event(connection, "terminate")
                    terminating_at = time.monotonic()
                if packet[0:1] == b"Q" and packet[5:] == b"COMMIT\0":
                    marker = STATE / "hold_commit"
                    if marker.exists():
                        commits += 1
                    else:
                        commits = 0
                    if marker.exists() and commits > int(marker.read_text()):
                        event(connection, "hold_commit")
                        while marker.exists():
                            if front.pending() or select.select([front], [], [], 0.025)[0]:
                                if not front.recv(1):
                                    event(connection, "close_notify")
                                    return
                if backend_open:
                    back.sendall(packet)


def handle(raw, connection, server, client):
    try:
        with raw:
            if receive_exact(raw, 8) != SSL_REQUEST:
                raise ValueError("expected PostgreSQL SSLRequest")
            pause(connection, "hold_tls", raw)
            raw.sendall(b"S")
            with server.wrap_socket(raw, server_side=True, suppress_ragged_eofs=False) as front:
                event(connection, "tls")
                with socket.create_connection(("127.0.0.1", 5432), timeout=3) as upstream:
                    upstream.sendall(SSL_REQUEST)
                    if receive_exact(upstream, 1) != b"S":
                        raise ValueError("upstream refused PostgreSQL TLS")
                    with client.wrap_socket(upstream, server_hostname="postgres",
                                            suppress_ragged_eofs=False) as back:
                        forward(front, back, connection)
    except (EOFError, OSError) as error:
        event(connection, "abrupt" if isinstance(error, (EOFError, ssl.SSLEOFError,
                                                         ConnectionResetError, BrokenPipeError))
              else "transport_error")


def main():
    server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server.load_cert_chain("/wire-tls/postgres.crt", "/wire-tls/postgres.key")
    client = ssl.create_default_context(cafile="/wire-tls/ca.crt")
    with socket.create_server(("0.0.0.0", 15432)) as listener:
        print("ready", flush=True)
        connection = time.monotonic_ns()
        while True:
            raw, _ = listener.accept()
            connection += 1
            raw.settimeout(3)
            threading.Thread(target=handle, args=(raw, connection, server, client),
                             daemon=True).start()


if __name__ == "__main__":
    main()
