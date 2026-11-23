#!/usr/bin/env python3
"""Deterministic wire mutation smoke against the offline MCP probe."""

import importlib.util
import json
from pathlib import Path
import random
import select
import subprocess


def main():
    path = Path(__file__).resolve().parents[1] / "phase0" / "mcp-wire.py"
    spec = importlib.util.spec_from_file_location("mcp_wire", path)
    wire = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(wire)
    rng = random.Random(20260728)
    body = wire.rpc("tools/call", name=wire.TOOL, arguments={})
    headers = wire.headers(body)
    original = json.dumps(body).encode()
    cases = []
    for index in range(256):
        mutated = bytearray(original)
        at = rng.randrange(len(mutated))
        operation = index % 4
        if operation == 0:
            mutated[at] = rng.randrange(256)
        elif operation == 1:
            del mutated[at:at + rng.randrange(1, 17)]
        elif operation == 2:
            mutated[at:at] = rng.randbytes(rng.randrange(1, 17))
        else:
            del mutated[at:]
        cases.append(bytes(mutated))
    cases.extend((b"[" * 200 + b"]" * 200, b"{" + b" " * 65536))

    binary = "/tmp/rust/target/debug/rfc-mcp-docker-serve"
    with subprocess.Popen([binary, "--phase0-mcp", "0"],
                          stdout=subprocess.PIPE, text=True) as child:
        try:
            assert select.select([child.stdout], [], [], 5)[0], "probe startup"
            address = child.stdout.readline().strip()
            exchange = wire.host_exchange(int(address.rsplit(":", 1)[1]))
            counts = {}
            for index, payload in enumerate([original, *cases, original]):
                with exchange("POST", headers, payload) as response:
                    response.begin()
                    raw = response.read(256 * 1024 + 1)
                    assert response.status in {200, 400, 404, 413}, (index, response.status, raw)
                    assert len(raw) <= 256 * 1024
                    assert response.getheader("Mcp-Session-Id") is None
                    counts[response.status] = counts.get(response.status, 0) + 1
                    if index in {0, len(cases) + 1}:
                        assert response.status == 200
                        assert json.loads(raw)["result"]["content"][0]["text"] == "phase-zero fixture"
            print(f"HTTP mutation seed 20260728, {len(cases)} cases, statuses {counts}")
        finally:
            child.terminate()
            child.wait(timeout=5)


if __name__ == "__main__":
    if not __debug__:
        raise SystemExit("HTTP mutation smoke requires enabled assertions")
    main()
