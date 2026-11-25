"""Private TLS and rsync fixture endpoints, including bounded fault cases."""

from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from functools import cache
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
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


def wait_for_release(name):
    until = time.monotonic() + 25
    while not (ROOT / name).exists() and time.monotonic() < until:
        time.sleep(0.02)


@cache
def benchmark_xml_cases():
    return json.loads((ROOT / "phase8-xml.json").read_text())


class Https(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    drift_captures = 0
    xml_attempts = {}
    benchmark_lock = threading.Lock()

    def response(self, body, media_type):
        self.send_response(200)
        self.send_header("Content-Type", media_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("ETag", '"fixture"')
        self.send_header("Last-Modified", "Sun, 22 Nov 2026 00:00:00 GMT")
        self.end_headers()
        try:
            return self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, ssl.SSLError):
            return 0

    def do_GET(self):
        scenario = mode()
        if scenario.startswith("acquisition-"):
            print(f"acquisition start {self.path} {time.monotonic_ns()}", flush=True)
            if scenario == "acquisition-db-delay":
                print(f"checkpoint fixture request {self.path} {time.time_ns()}", flush=True)
            if scenario in ("acquisition-hold-txt", "acquisition-hold-xml"):
                suffix = scenario.rsplit("-", 1)[-1]
                if self.path == f"/rfc/rfc100.{suffix}":
                    deadline = time.monotonic() + 24
                    while not (ROOT / "release-acquisition").exists() and time.monotonic() < deadline:
                        time.sleep(0.05)
            if scenario in ("acquisition-delay", "acquisition-http-date") and self.path == "/rfc/rfc100.txt":
                from email.utils import formatdate
                self.send_response(429 if scenario.endswith("delay") else 503)
                self.send_header("Retry-After", "7" if scenario.endswith("delay") else
                                 formatdate(time.time() + 8, usegmt=True))
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            if scenario == "acquisition-xml-status":
                status = {"/rfc/rfc101.xml": 410, "/rfc/rfc9110.xml": 502}.get(self.path)
                if status is not None:
                    self.send_response(status)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
            if self.path == "/rfc/rfc100.txt":
                if scenario == "acquisition-truncated":
                    self.send_response(200)
                    self.send_header("Content-Type", "text/plain")
                    self.send_header("Content-Length", "300")
                    self.send_header("Connection", "close")
                    self.end_headers()
                    self.wfile.write(b"short")
                    self.close_connection = True
                    return
                if scenario == "acquisition-oversized":
                    self.send_response(200)
                    self.send_header("Content-Type", "text/plain")
                    self.send_header("Content-Length", str(4 * 1024 * 1024 + 1))
                    self.send_header("Connection", "close")
                    self.end_headers()
                    self.close_connection = True
                    return
        if scenario.startswith("phase8-") and self.path.startswith("/rfc/") and self.path.endswith(".xml"):
            self.benchmark_xml(scenario)
            return
        if scenario.startswith("xml-"):
            print(f"fixture request {scenario} {self.path} {time.monotonic():.6f}", flush=True)
            if self.path.endswith(".xml") and self.path.startswith("/rfc/") and self.xml_fixture(scenario):
                return
        if scenario == "overlap" and self.path == "/rfc/rfc100.txt":
            time.sleep(2.35)
        if scenario == "slow-txt" and self.path == "/rfc/rfc100.txt":
            time.sleep(2.0)
        if scenario in ("benchmark", "cold-bootstrap", "phase8-paced"):
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
        if scenario == "scale-bootstrap":
            rfc = Path(self.path).stem.removeprefix("rfc")
            time.sleep(0.025 if rfc.isascii() and rfc.isdecimal() and int(rfc) % 3 == 0 else 0.005)
        if scenario in ("cold-bootstrap", "scale-bootstrap") and self.path == "/rfc/rfc101.xml":
            self.send_error(410)
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

    def benchmark_xml(self, scenario):
        start = time.monotonic_ns()
        rfc = int(self.path.removeprefix("/rfc/rfc").removesuffix(".xml"))
        case = benchmark_xml_cases()[str(rfc)]
        if scenario == "phase8-paced":
            time.sleep(1.20)
        else:
            time.sleep(0.10 if rfc % 97 == 0 else 0.025 if rfc % 3 == 0 else 0.005)
        if case == "retried":
            with self.benchmark_lock:
                key = (scenario, rfc)
                attempt = self.xml_attempts.get(key, 0) + 1
                self.xml_attempts[key] = attempt
        else:
            attempt = 1
        status = {"not_found": 404, "gone": 410, "retries_exhausted": 503}.get(case, 200)
        if case == "retried" and attempt == 1:
            status = 503
        if status == 200:
            sent = self.response((ROOT / "https" / f"rfc{rfc}.xml").read_bytes(), "application/xml")
        else:
            self.status(status)
            sent = 0
        print("phase8_xml " + json.dumps({
            "scenario": scenario, "rfc": rfc, "start_ns": start, "status": status, "body_bytes_sent": sent,
        }), flush=True)

    def xml_fixture(self, scenario):
        rfc = int(self.path.removeprefix("/rfc/rfc").removesuffix(".xml"))
        if scenario == "xml-interrupted" and rfc == 9110:
            key = (scenario, rfc)
            if key not in self.xml_attempts:
                self.xml_attempts[key] = 1
                self.status(200, {"Content-Type": "application/xml"}, length=64)
                self.wfile.write(b"<rfc")
                self.close_connection = True
                return True
        if scenario == "xml-overlap":
            if rfc == 100:
                wait_for_release("release-xml")
            return False
        if scenario == "xml-stop-response":
            if rfc == 100:
                wait_for_release("release-xml")
            return False
        if scenario in ("xml-stop-retry", "xml-stop-cooldown") and rfc == 100:
            self.status(429, {"Retry-After": "20" if scenario == "xml-stop-retry" else "60"})
            return True
        if scenario in ("xml-statuses", "xml-policy"):
            key = (scenario, rfc)
            attempt = self.xml_attempts.get(key, 0) + 1
            self.xml_attempts[key] = attempt
            if rfc == 100:
                if attempt == 1:
                    if scenario == "xml-statuses":
                        self.status(429, {"Retry-After": "2"})
                    else:
                        date = datetime.now(timezone.utc) + timedelta(seconds=3)
                        self.status(503, {"Retry-After": format_datetime(date, usegmt=True)})
                else:
                    self.response(b'<rfc number="100"><middle/></rfc>', "application/xml")
                return True
        if scenario == "xml-statuses":
            if rfc in (101, 102, 2119):
                self.status({101: 410, 102: 404, 2119: 502}[rfc])
            elif rfc == 8174:
                self.response(b'<rfc number="102"><middle/></rfc>', "application/xml")
            else:
                self.response(b"<html/>", "text/html")
            return True
        if scenario == "xml-policy":
            if rfc == 101:
                self.status(302, {"Location": "https://invalid.example/rfc/rfc101.xml"})
            elif rfc == 102:
                self.status(200, {"Content-Type": "application/xml", "Content-Encoding": "gzip"})
            elif rfc == 2119:
                self.status(200, {"Content-Type": "application/xml"}, length=8 * 1024**2 + 1)
            elif rfc == 8174:
                self.response(b"<rfc number='8174'><back>", "application/xml")
            else:
                time.sleep(1.5)
                self.response(b'<rfc number="9110"><middle/></rfc>', "application/xml")
            return True
        return False

    def status(self, status, headers=None, *, length=0):
        self.send_response(status)
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.send_header("Content-Length", str(length))
        self.end_headers()
        if status in (429, 503):
            print(f"fixture cooldown {mode()} {self.path} {time.monotonic():.6f}", flush=True)


class Rsync(socketserver.BaseRequestHandler):
    def handle(self):
        scenario = mode()
        if scenario == "hold-rsync":
            time.sleep(15)
        if scenario == "hold-rsync-timeout":
            time.sleep(45)
        if scenario == "xml-overlap":
            print("fixture rsync held for XML", flush=True)
            wait_for_release("release-rsync")
        if scenario in ("benchmark", "cold-bootstrap", "phase8-paced"):
            time.sleep(0.20)
        interrupted = {"rsync-interrupted": 1024, "rsync-partial": 65536}.get(scenario)
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
                        if key.fileobj is upstream and interrupted is not None:
                            remaining = interrupted - relayed
                            if len(chunk) >= remaining:
                                self.request.sendall(chunk[:remaining])
                                print(f"fixture rsync truncated after {interrupted} bytes", flush=True)
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
