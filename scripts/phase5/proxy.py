#!/usr/bin/env python3
"""Disposable TLS proxy for the Phase 5 private Compose suite."""

import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import ssl


class Proxy(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def forward(self):
        size = int(self.headers.get("Content-Length", "0"))
        if size > 128 * 1024:
            self.send_error(413)
            return
        allow = {"host", "accept", "origin", "content-type", "content-encoding",
                 "mcp-protocol-version", "mcp-method", "mcp-name", "mcp-session-id", "last-event-id"}
        headers = {key: value for key, value in self.headers.items() if key.lower() in allow}
        headers["X-Real-IP"] = self.client_address[0]
        headers["Connection"] = "close"
        upstream = http.client.HTTPConnection("serve", 8780, timeout=17)
        try:
            upstream.request(self.command, self.path, self.rfile.read(size) if size else None, headers)
            response = upstream.getresponse()
            body = response.read()
            self.send_response(response.status)
            for key, value in response.getheaders():
                if key.lower() not in {"connection", "content-length", "server", "date", "transfer-encoding"}:
                    self.send_header(key, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except OSError:
            self.close_connection = True
        finally:
            upstream.close()

    do_POST = forward
    do_GET = forward
    do_DELETE = forward

    def log_message(self, *_):
        pass


if __name__ == "__main__":
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain("/tls/localhost.crt", "/tls/localhost.key")
    server = ThreadingHTTPServer(("0.0.0.0", 8443), Proxy)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    server.serve_forever()
