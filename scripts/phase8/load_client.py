#!/usr/bin/env python3
"""Private HTTPS load generator used inside separate test network namespaces."""

import http.client
import json
from pathlib import Path
import socket
import ssl
import sys
import time

VERSION = "2026-07-28"
PREFIX = "io.modelcontextprotocol/"
MIX = (
    ("list", {"limit": 20}),
    ("search", {"query": 'in::fulltext:"missing capacity token"', "limit": 20}),
    ("get", {"id": 2119}),
    ("table", {"id": 102}),
    ("rg", {"pattern": "missing capacity token", "mode": "literal", "ids": [102, 108, 109]}),
)


class Connection(http.client.HTTPSConnection):
    def connect(self):
        raw = socket.create_connection((self.host, self.port), self.timeout)
        try:
            self.sock = self._context.wrap_socket(raw, server_hostname="localhost")
        except BaseException:
            raw.close()
            raise


def exchange(address, port, context, name, arguments):
    body = {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {
        "name": name, "arguments": arguments, "_meta": {
            PREFIX + "protocolVersion": VERSION,
            PREFIX + "clientInfo": {"name": "phase8-capacity", "version": "1"},
            PREFIX + "clientCapabilities": {},
        },
    }}
    headers = {"Host": "rfc.test", "Accept": "application/json, text/event-stream",
               "Content-Type": "application/json", "Mcp-Method": "tools/call",
               "Mcp-Name": name, "MCP-Protocol-Version": VERSION}
    started = time.monotonic()
    connection = Connection(address, port, context=context, timeout=18)
    try:
        connection.request("POST", "/mcp", json.dumps(body), headers)
        response = connection.getresponse()
        payload = response.read(256 * 1024 + 1)
        status = response.status
        assert len(payload) <= 256 * 1024
        if status == 429:
            assert response.getheader("Retry-After") == "1"
    finally:
        connection.close()
    elapsed = time.monotonic() - started
    if status == 429:
        return [name, "HTTP 429", elapsed, 0]
    assert status == 200, (name, status, payload[:240])
    result = json.loads(payload)
    assert result["id"] == 1 and result["result"]["resultType"] == "complete", result
    result = result["result"]
    output = result["structuredContent"]
    outcome = output["code"] if result["isError"] else "ok"
    assert outcome in {"ok", "resource_limit_exceeded", "deadline_exceeded"}, outcome
    return [name, outcome, elapsed, output.get("source_bytes_charged", 0)]


def main():
    index, duration = map(int, sys.argv[1:])
    context = ssl.create_default_context(cafile="/capacity-ca.crt")
    gate = Path("/capacity-gate/go")
    print("ready", flush=True)
    deadline = time.monotonic() + 60
    while not gate.exists():
        assert time.monotonic() < deadline, "load client start deadline"
        time.sleep(0.025)
    mode = gate.read_text().strip()
    assert mode in {"mixed", "admission"}
    if mode == "admission":
        records = [exchange("test-proxy", 8443, context, *MIX[1])]
    else:
        records = []
        started = time.monotonic()
        while time.monotonic() < started + duration + 3:
            tick = time.monotonic()
            record = exchange("test-proxy", 8443, context, *MIX[index % len(MIX)])
            if tick >= started + 3:
                records.append(record)
            index += 1
            time.sleep(max(0, tick + 0.27 - time.monotonic()))
    print(json.dumps(records))


if __name__ == "__main__":
    main()
