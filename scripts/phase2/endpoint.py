"""Private TLS and rsync fixture endpoints, including bounded fault cases."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import selectors
import socket
import socketserver
import ssl
import subprocess
import threading
import time

ROOT = Path("/fixture-source")


def mode():
    return (ROOT / "mode").read_text().strip()


class Https(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    drift_captures = 0

    def response(self, body, media_type):
        self.send_response(200)
        self.send_header("Content-Type", media_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("ETag", '"fixture"')
        self.send_header("Last-Modified", "Sun, 22 Nov 2026 00:00:00 GMT")
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, ssl.SSLError):
            pass

    def do_GET(self):
        scenario = mode()
        if scenario == "overlap" and self.path == "/rfc/rfc100.txt":
            time.sleep(2.35)
        if scenario == "slow-txt" and self.path == "/rfc/rfc100.txt":
            time.sleep(2.0)
        if scenario == "benchmark":
            # Alternate long and short responses so ordering and overlap matter.
            longer = self.path.endswith((".xml", "0.txt", "2.txt", "4.txt", "6.txt", "8.txt"))
            time.sleep(1.20 if longer else 0.10)

        if self.path == "/rfc-index.xml":
            if scenario != "catalog-drift":
                type(self).drift_captures = 0
            if scenario == "catalog-compressed":
                self.send_response(200)
                self.send_header("Content-Encoding", "gzip")
                self.send_header("Content-Type", "application/xml")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            if scenario == "catalog-oversize":
                self.send_response(200)
                self.send_header("Content-Type", "application/xml")
                self.send_header("Transfer-Encoding", "chunked")
                self.end_headers()
                chunk = b"10000\r\n" + b"x" * 65536 + b"\r\n"
                try:
                    for _ in range(513):
                        self.wfile.write(chunk)
                    self.wfile.write(b"0\r\n\r\n")
                except (BrokenPipeError, ConnectionResetError, ssl.SSLError):
                    pass
                return
            if scenario == "catalog-redirect":
                self.send_response(302)
                self.send_header("Location", "https://invalid.example/rfc-index.xml")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            body = (ROOT / "catalog.xml").read_bytes()
            if scenario == "catalog-malformed":
                body = body[:-20]
            if scenario == "catalog-drift":
                type(self).drift_captures += 1
                if type(self).drift_captures > 1:
                    body += b"\n"
            self.response(body, "application/xml")
            return
        if not self.path.startswith("/rfc/") or "/" in self.path[5:]:
            self.send_error(404)
            return
        if self.path == "/rfc/rfc100.txt":
            if scenario == "txt-html":
                self.response(b"<html>Not an RFC</html>", "text/html")
                return
            if scenario in {"txt-compressed", "txt-redirect"}:
                redirect = scenario == "txt-redirect"
                self.send_response(302 if redirect else 200)
                if redirect:
                    self.send_header("Location", "https://invalid.example/rfc100.txt")
                else:
                    self.send_header("Content-Encoding", "gzip")
                    self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
        file = ROOT / "https" / self.path[5:]
        if not file.is_file():
            self.send_error(404)
            return
        payload = file.read_bytes()
        if scenario == "bad-rfcxml" and file.name == "rfc8174.xml":
            payload = b"<rfc number='8174'><back>"
        self.response(payload, "text/plain" if file.suffix == ".txt" else "application/xml")


class Rsync(socketserver.BaseRequestHandler):
    def handle(self):
        scenario = mode()
        if scenario == "hold-rsync":
            time.sleep(15)
        if scenario == "benchmark":
            time.sleep(0.20)
        with socket.create_connection(("127.0.0.1", 9873), 3) as upstream:
            upstream.settimeout(10)
            self.request.settimeout(10)
            with selectors.DefaultSelector() as relay:
                relay.register(self.request, selectors.EVENT_READ, upstream)
                relay.register(upstream, selectors.EVENT_READ, self.request)
                relayed = 0
                while ready := relay.select(10):
                    for key, _ in ready:
                        chunk = key.fileobj.recv(32768)
                        if not chunk:
                            return
                        if key.fileobj is upstream and mode() == "rsync-interrupted":
                            remaining = 1024 - relayed
                            if len(chunk) >= remaining:
                                self.request.sendall(chunk[:remaining])
                                return
                            relayed += len(chunk)
                        key.data.sendall(chunk)


class Relay(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True


def main():
    with subprocess.Popen(["/usr/bin/rsync", "--daemon", "--no-detach", "--port=9873",
                           "--config=/fixture-source/rsyncd.conf"]) as rsync:
        try:
            with Relay(("0.0.0.0", 8873), Rsync) as relay:
                threading.Thread(target=relay.serve_forever, daemon=True).start()
                with ThreadingHTTPServer(("0.0.0.0", 8443), Https) as server:
                    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                    context.load_cert_chain(ROOT / "fixtures.crt", ROOT / "fixtures.key")
                    server.socket = context.wrap_socket(server.socket, server_side=True)
                    server.serve_forever()
        finally:
            rsync.terminate()


if __name__ == "__main__":
    main()
