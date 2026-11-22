#!/usr/bin/env python3
"""Fetch small official RFC Editor samples without following source-supplied URLs."""

import argparse
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import tempfile
import time
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener
from xml.etree import ElementTree

HOST = "www.rfc-editor.org"
KIB = 1024
SAMPLES = {
    "rfcatom.xml": ("/rfcatom.xml", 768 * KIB, False),
    "rfcrss.xml": ("/rfcrss.xml", 768 * KIB, False),
    "rfc-index.xml": ("/rfc-index.xml", 192 * KIB, True),
    "rfc-index.xsd": ("/rfc-index.xsd", 256 * KIB, False),
    "rfc2119.txt": ("/rfc/rfc2119.txt", 32 * KIB, False),
    "rfc8174.txt": ("/rfc/rfc8174.txt", 32 * KIB, False),
    "rfc9110.xml": ("/rfc/rfc9110.xml", 3 * 1024 * KIB, False),
}


class OfficialRedirects(HTTPRedirectHandler):
    max_redirections = 2

    def __init__(self):
        self.history = []

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        target = urlsplit(newurl)
        if (target.scheme != "https" or target.netloc != HOST
                or target.username or target.password or target.port):
            raise HTTPError(req.full_url, code, "redirect outside fixed source host", headers, fp)
        self.history.append({"status": code, "from": req.full_url, "to": newurl})
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def observations(data, xml, partial):
    if not xml:
        data.decode("utf-8", errors="strict")
        return {"encoding": "UTF-8", "crlf_count": data.count(b"\r\n"),
                "form_feed_count": data.count(b"\f")}
    # This parser has no external resolver. Base the catalog observations on
    # complete element events within the prefix rather than closing it by hand.
    parser = ElementTree.XMLPullParser(("start", "end"))
    parser.feed(data)
    tags = {}
    root = None
    for event, element in parser.read_events():
        if root is None and event == "start":
            root = element.tag
        if event == "end":
            tags[element.tag.rsplit("}", 1)[-1]] = tags.get(element.tag.rsplit("}", 1)[-1], 0) + 1
    if not partial:
        parser.close()
    if root is None:
        raise ValueError("sample did not contain an XML root")
    relevant = {"entry", "item", "channel", "rfc-entry", "doc-id", "stream", "updates",
                "obsoletes", "references", "reference", "referencegroup", "schema", "element"}
    return {"root": root, "complete_elements": {tag: tags[tag] for tag in sorted(tags.keys() & relevant)},
            "has_doctype": b"<!DOCTYPE" in data}


def fetch(path, limit, prefix):
    url = f"https://{HOST}{path}"
    redirects = OfficialRedirects()
    # Request the prefix when supported. An HTTP 200 is still capped locally.
    headers = {"User-Agent": "rfc-mcp-docker-phase0/0.0.0", "Accept-Encoding": "identity"}
    if prefix:
        headers["Range"] = f"bytes=0-{limit - 1}"
    request = Request(url, headers=headers)
    started = time.monotonic()
    with build_opener(redirects).open(request, timeout=12) as response:
        if response.headers.get("Content-Encoding", "identity") != "identity":
            raise ValueError("unexpected content encoding")
        data = bytearray()
        while len(data) <= limit:
            if time.monotonic() - started > 25:
                raise TimeoutError(url)
            chunk = response.read1(min(32 * KIB, limit + 1 - len(data)))
            if not chunk:
                break
            data.extend(chunk)
        partial = len(data) > limit or response.status == 206
        if partial and not prefix:
            raise ValueError(f"sample exceeded {limit} bytes")
        data = bytes(data[:limit])
        if response.status == 206 and not response.headers.get("Content-Range", "").startswith("bytes 0-"):
            raise ValueError("catalog range did not start at byte zero")
        return data, {
            "source": url, "effective_url": response.url, "status": response.status,
            "captured_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "content_type": response.headers.get("Content-Type"),
            "content_range": response.headers.get("Content-Range"),
            "etag": response.headers.get("ETag"), "redirects": redirects.history,
            "bytes": len(data), "partial": partial, "sha256": sha256(data).hexdigest(),
        }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    output = (args.output or Path(tempfile.mkdtemp(prefix="rfc-mcp-upstream-", dir="/tmp"))).resolve()
    if not output.is_relative_to("/tmp") or output == Path("/tmp"):
        parser.error("--output must be a dedicated directory under /tmp")
    output.mkdir(mode=0o700, parents=True, exist_ok=True)
    report = {}
    failed = False
    for index, (name, (path, limit, prefix)) in enumerate(SAMPLES.items()):
        if index:
            time.sleep(1)
        try:
            data, receipt = fetch(path, limit, prefix)
            receipt["observations"] = observations(data, not name.endswith(".txt"), receipt["partial"])
            (output / name).write_bytes(data)
            report[name] = receipt
        except (OSError, ValueError, ElementTree.ParseError) as error:
            failed = True
            report[name] = {"source": f"https://{HOST}{path}", "error": str(error)}
        print(name, json.dumps(report[name], ensure_ascii=False), flush=True)
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(f"Reports and captures: {output}")
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
