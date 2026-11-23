"""Disposable test peer for private control traffic."""

import base64
import hashlib
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import socket
import sys
from pathlib import Path

FEED = "rfc-mcp-docker-feed"
SYNC = "rfc-mcp-docker-sync"
SERVE = "rfc-mcp-docker-serve"


def token(value):
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def mac(key, fields, body):
    return token(hmac.digest(key, ("\n".join(fields) + "\n\n").encode("ascii") + body, hashlib.sha384))


def response_mac(key, key_id, receiver, caller, nonce, body):
    return mac(key, ["RFMCP-HMAC-SHA384-RESPONSE-V1", key_id, receiver, caller, nonce,
                     "200", "application/json", str(len(body))], body)


class FakeServe(BaseHTTPRequestHandler):
    def do_POST(self):
        key = Path("/run/secrets/fixture").read_bytes()
        nonce = self.headers["X-RFC-Nonce"]
        request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.path == "/internal/v1/identify":
            body = json.dumps({"epoch": token(bytes(32))}).encode()
            if Path("/fixture-spoof-mode").read_text().strip() == "identity":
                signature = token(bytes(48))
                self.log_message("forged identity")
            else:
                signature = response_mac(key, "fixture-notify", SERVE, SYNC, nonce, body)
        else:
            body = json.dumps({"event_id": request["event_id"], "outcome": "observed"}).encode()
            signature = token(bytes(48))
            self.log_message("forged acknowledgment")
        self.send_response_only(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-RFC-Request-Nonce", nonce)
        self.send_header("X-RFC-Receiver", SERVE)
        self.send_header("X-RFC-Response-Signature", signature)
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)

    protocol_version = "HTTP/1.1"


def main():
    if sys.argv[1] == "spoof":
        with ThreadingHTTPServer(("0.0.0.0", 8782), FakeServe) as server:
            server.serve_forever()
    else:
        with socket.create_connection((sys.argv[1], int(sys.argv[2])), 6) as peer:
            peer.settimeout(10)
            peer.sendall(sys.stdin.buffer.read())
            while part := peer.recv(65536):
                sys.stdout.buffer.write(part)


if __name__ == "__main__":
    main()
