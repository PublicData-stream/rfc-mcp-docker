"""Add conditional and faultable feed endpoints to the offline upstream fixture."""

import hashlib
import importlib.util
from pathlib import Path
import time

spec = importlib.util.spec_from_file_location("phase2", "/fixture-endpoint.py")
phase2 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(phase2)


class FeedUpstream(phase2.Https):
    def do_GET(self):
        root = phase2.ROOT
        if self.path not in ("/rfcatom.xml", "/rfcrss.xml"):
            if self.path == "/rfc-index.xml":
                deadline = time.monotonic() + 25
                while (root / "hold-catalog").exists() and time.monotonic() < deadline:
                    time.sleep(0.1)
            return super().do_GET()
        scenario = (root / "feed-mode").read_text().strip()
        self.log_message("feed scenario=%s at=%.3f", scenario, time.monotonic())
        if scenario == "rate-limited":
            self.send_response(429)
            self.send_header("Retry-After", "4")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if scenario == "oversized":
            length = 768 * 1024 + 1
            self.send_response(200)
            self.send_header("Content-Type", "application/xml")
            self.send_header("Content-Length", str(length))
            self.send_header("Connection", "close")
            self.end_headers()
            self.close_connection = True
            return
        if scenario == "malformed":
            self.response(b"<broken>", "application/xml")
            return
        if scenario == "absent":
            self.send_error(503)
            return
        body = (root / Path(self.path).name).read_bytes()
        tag = '"' + hashlib.sha256(body).hexdigest() + '"'
        if self.headers.get("If-None-Match") == tag:
            self.send_response(304)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/xml")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("ETag", tag)
        self.end_headers()
        self.wfile.write(body)


phase2.Https = FeedUpstream
phase2.main()
