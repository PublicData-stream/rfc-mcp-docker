#!/usr/bin/env python3
"""Run the same HTTP wire assertions on a local binary or loopback port."""

import argparse
import base64
import http.client
import json
import select
import subprocess

VERSION = "2026-07-28"
PREFIX = "io.modelcontextprotocol/"
TOOL = "phase0_read_fixture"


def rpc(method, **params):
    params["_meta"] = {
        PREFIX + "protocolVersion": VERSION,
        PREFIX + "clientInfo": {"name": "phase-zero", "version": "1"},
        PREFIX + "clientCapabilities": {},
    }
    return {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}


def headers(body):
    result = {
        "Accept": "application/json, text/event-stream",
        "Content-Type": "application/json",
        "MCP-Protocol-Version": VERSION,
        "Mcp-Method": body["method"],
    }
    if body["method"] == "tools/call":
        result["Mcp-Name"] = body["params"]["name"]
    return result


def probe(exchange, baseline):
    def check(label, body=None, *, http_method="POST", changes=None):
        outgoing = headers(body) if body is not None else {}
        for key, value in (changes or {}).items():
            if value is None:
                outgoing.pop(key, None)
            else:
                outgoing[key] = value
        response = exchange(http_method, outgoing, json.dumps(body).encode() if body else b"")
        response.begin()
        raw = response.read()
        status = response.status
        json_type = response.getheader("Content-Type", "").startswith("application/json")
        result = json.loads(raw) if json_type and raw else {}
        assert response.getheader("Mcp-Session-Id") is None, label
        print(f"{label}: HTTP {status}, error={result.get('error', {}).get('code')}", flush=True)
        return status, result

    def complete(label, body, **kwargs):
        status, result = check(label, body, **kwargs)
        assert status == 200 and result["id"] == 1, (label, status, result)
        assert result["result"]["resultType"] == "complete", result
        return result["result"]

    call = rpc("tools/call", name=TOOL, arguments={})
    result = complete("direct call before discovery", call)
    assert result["content"] == [{"type": "text", "text": "phase-zero fixture"}]
    assert result["isError"] is False
    discover = complete("server/discover", rpc("server/discover"))
    assert discover["supportedVersions"] == [VERSION]
    assert set(discover["capabilities"]) == {"tools"}
    listed = complete("tools/list", rpc("tools/list"))
    assert [tool["name"] for tool in listed["tools"]] == [TOOL]
    assert listed["tools"][0]["annotations"]["readOnlyHint"] is True
    complete("valid request on unrelated connection", call)
    complete("legacy session headers are ignored", call, changes={
        "Mcp-Session-Id": "irrelevant", "Last-Event-ID": "irrelevant",
    })

    def reject(
        label, body=None, *, status=400, code=None, http_method="POST", changes=None,
    ):
        actual, result = check(label, body, http_method=http_method, changes=changes)
        if baseline:
            return
        assert actual == status, (label, actual, result)
        if code is not None:
            assert result["id"] == 1 and result["error"]["code"] == code, (label, result)
        if code == -32022:
            assert result["error"]["data"]["supported"] == [VERSION], result

    reject("missing version header", call, code=-32020, changes={"MCP-Protocol-Version": None})
    reject("missing method header", call, code=-32020, changes={"Mcp-Method": None})
    reject("missing name header", call, code=-32020, changes={"Mcp-Name": None})
    reject("method mismatch", call, code=-32020, changes={"Mcp-Method": "tools/list"})
    reject("name mismatch", call, code=-32020, changes={"Mcp-Name": "other"})
    reject("version mismatch", call, code=-32020, changes={"MCP-Protocol-Version": "2025-11-25"})

    encoded = rpc("tools/call", name="문서", arguments={})
    name = base64.b64encode(encoded["params"]["name"].encode()).decode()
    status, result = check("encoded Unicode name reaches application", encoded, changes={
        "Mcp-Name": f"=?base64?{name}?=",
    })
    assert result["error"]["code"] == -32602, (status, result)
    reject("malformed encoded name", encoded, code=-32020, changes={"Mcp-Name": "=?base64?{}?="})

    for version in ("2025-11-25", "2030-01-01"):
        unsupported = rpc("tools/list")
        unsupported["params"]["_meta"][PREFIX + "protocolVersion"] = version
        reject(f"unsupported {version}", unsupported, code=-32022,
               changes={"MCP-Protocol-Version": version})

    missing = rpc("tools/list")
    del missing["params"]["_meta"]
    reject("missing metadata", missing, code=-32602)
    for field in ("protocolVersion", "clientCapabilities"):
        missing = rpc("tools/list")
        del missing["params"]["_meta"][PREFIX + field]
        reject(f"missing metadata {field}", missing, code=-32602)

    optional = rpc("tools/list")
    del optional["params"]["_meta"][PREFIX + "clientInfo"]
    complete("optional clientInfo omitted", optional)

    init = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
        "protocolVersion": "2025-11-25", "clientInfo": {"name": "legacy", "version": "1"},
        "capabilities": {},
    }}
    reject("legacy initialize", init, code=-32022, changes={"MCP-Protocol-Version": "2025-11-25"})
    reject("legacy initialize without headers", init, code=-32020, changes={
        "MCP-Protocol-Version": None, "Mcp-Method": None,
    })
    reject("initialize with modern header", init, status=404, code=-32601)
    reject("legacy initialized notification", {"jsonrpc": "2.0", "method": "notifications/initialized"})
    reject("GET event stream", status=405, http_method="GET", changes={"Accept": "text/event-stream"})
    reject("DELETE session", status=405, http_method="DELETE")
    reject("untrusted Origin", call, status=403, changes={"Origin": "https://untrusted.invalid"})


def host_exchange(port):
    def exchange(method, headers, body):
        # A fresh connection per request also proves that no negotiation state is retained.
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=6)
        connection.request(method, "/mcp", body, {**headers, "Connection": "close"})
        response = http.client.HTTPResponse(connection.sock)
        connection.close()
        return response

    return exchange


def main():
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--binary")
    mode.add_argument("--port", type=int)
    parser.add_argument("--direct-baseline", action="store_true")
    args = parser.parse_args()
    if args.port is not None:
        probe(host_exchange(args.port), args.direct_baseline)
        return

    with subprocess.Popen([args.binary, "--phase0-mcp", "0"],
                          stdout=subprocess.PIPE, text=True) as child:
        try:
            if not select.select([child.stdout], [], [], 5)[0]:
                raise RuntimeError("MCP probe did not start")
            address = child.stdout.readline().strip()
            probe(host_exchange(int(address.rsplit(":", 1)[1])), args.direct_baseline)
        finally:
            child.terminate()
            child.wait(timeout=5)


if __name__ == "__main__":
    main()
