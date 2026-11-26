"""Hold one valid MCP call open until told to disconnect."""

import json
import socket
import sys

name = sys.argv[2] if len(sys.argv) > 2 else "rg"
arguments = {"id": 2119} if name == "get" else {"pattern": "stall", "mode": "literal", "ids": [108]}
body = json.dumps({
    "jsonrpc": "2.0", "id": 1, "method": "tools/call",
    "params": {"name": name, "arguments": arguments,
               "_meta": {"io.modelcontextprotocol/protocolVersion": "2026-07-28",
                         "io.modelcontextprotocol/clientCapabilities": {}}},
}).encode()
headers = (
    "POST /mcp HTTP/1.1\r\nHost: rfc.test\r\n"
    "Accept: application/json, text/event-stream\r\nContent-Type: application/json\r\n"
    f"MCP-Protocol-Version: 2026-07-28\r\nMcp-Method: tools/call\r\nMcp-Name: {name}\r\n"
    f"X-Real-IP: {sys.argv[1]}\r\nContent-Length: {len(body)}\r\n\r\n"
)
with socket.create_connection(("rfc-mcp-docker-serve", 8780), timeout=3) as stream:
    stream.sendall(headers.encode() + body)
    print("sent", flush=True)
    assert sys.stdin.readline().strip() == "disconnect"
    stream.shutdown(socket.SHUT_RDWR)
