#!/usr/bin/env python3
"""Raw HTTP checks from the private ingress network."""

import http.client
import json
import socket
import sys
import time


def discovery():
    return json.dumps({
        "jsonrpc": "2.0", "id": 1, "method": "server/discover",
        "params": {"_meta": {
            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
            "io.modelcontextprotocol/clientCapabilities": {},
        }},
    }).encode()


def headers(method, source):
    fields = {
        "Host": "rfc.test", "Accept": "application/json, text/event-stream",
        "Content-Type": "application/json", "MCP-Protocol-Version": "2026-07-28",
        "Mcp-Method": method, "X-Real-IP": source, "Connection": "close",
    }
    if method == "tools/call":
        fields["Mcp-Name"] = "get"
    return fields


def request(source, path="/mcp", *, header="X-Real-IP", host="serve"):
    fields = headers("server/discover", source)
    if header != "X-Real-IP":
        fields["X-Real-IP"] = "192.0.2.254"
        if source is not None:
            fields[header] = source
    with_socket = http.client.HTTPConnection(host, 8780, timeout=3)
    try:
        with_socket.request("POST", path, discovery(), fields)
        response = with_socket.getresponse()
        response.read()
        return response.status
    finally:
        with_socket.close()


def start(fields, body, length):
    sock = socket.create_connection(("serve", 8780), timeout=3)
    lines = ["POST /mcp HTTP/1.1", *(f"{name}: {value}" for name, value in fields.items()),
             f"Content-Length: {length}", "", ""]
    sock.sendall("\r\n".join(lines).encode() + body)
    return sock


def untrusted(header, host):
    assert request("192.0.2.1", "/metrics", header=header, host=host) == 404
    statuses = [request(f"192.0.2.{number}", header=header, host=host) for number in range(1, 16)]
    assert statuses[:10] == [200] * 10 and 429 in statuses[10:], statuses
    print(f"Socket-peer rate limit verified for an untrusted sender of {header}")


def chain(host):
    header = "X-Forwarded-For"
    request_args = {"header": header, "host": host}
    assert request(None, **request_args) == 400
    assert request("192.0.2.1, , 203.0.113.3", **request_args) == 400
    assert request("203.0.113.3", **request_args) == 400
    # A changing prefix must resolve to the same address behind the trusted hop.
    statuses = [request(f"192.0.2.{number}, 2001:db8::9, 203.0.113.3", **request_args)
                for number in range(1, 16)]
    assert statuses[:10] == [200] * 10 and 429 in statuses[10:], statuses
    # A different resolved address gets its own bucket even with the same legacy header.
    statuses = [request(f"192.0.2.1, 198.51.100.{number}, 203.0.113.3", **request_args)
                for number in range(1, 16)]
    assert statuses == [200] * 15, statuses
    print("Alternate header chain selection, rejection, and rate limits verified")


if sys.argv[1] == "untrusted":
    untrusted("X-Real-IP", "serve")
elif sys.argv[1] == "untrusted-chain":
    untrusted("X-Forwarded-For", sys.argv[2] if len(sys.argv) > 2 else "serve")
elif sys.argv[1] == "chain":
    chain(sys.argv[2] if len(sys.argv) > 2 else "serve")
elif sys.argv[1] == "trusted":
    assert [request(f"192.0.2.{number}") for number in range(1, 16)] == [200] * 15
    connection = http.client.HTTPConnection("serve", 8780, timeout=3)
    try:
        body = discovery()
        connection.putrequest("POST", "/mcp", skip_host=True)
        for name, value in headers("server/discover", "192.0.2.16").items():
            connection.putheader(name, value)
        connection.putheader("Mcp-Method", "tools/list")
        connection.putheader("Content-Length", str(len(body)))
        connection.endheaders(body)
        response = connection.getresponse()
        assert response.status == 400 and json.loads(response.read())["error"]["code"] == -32020
    finally:
        connection.close()
    sockets = []
    try:
        for number in range(1, 17):
            sockets.append(start(headers("server/discover", f"198.51.100.{number}"), b"{", 100))
        time.sleep(0.2)
        assert request("198.51.100.17") == 429
    finally:
        for sock in sockets:
            sock.close()
    print("Trusted peer identity and 16-request admission cap verified")
elif sys.argv[1] == "cancel":
    body = json.loads(discovery())
    body["method"] = "tools/call"
    body["params"].update(name="get", arguments={"id": 2119})
    encoded = json.dumps(body).encode()
    with start(headers("tools/call", sys.argv[2] if len(sys.argv) > 2 else "203.0.113.1"), encoded, len(encoded)) as sock:
        print("sent", flush=True)
        assert sys.stdin.readline().strip() == "disconnect"
        sock.shutdown(socket.SHUT_RDWR)
else:
    raise AssertionError("unknown direct probe")
