#!/usr/bin/env python3
"""Verify the production server binary over MCP in disposable private Compose."""

import argparse
import base64
from contextlib import contextmanager
import hashlib
import hmac
import http.client
import importlib.util
import json
from pathlib import Path
import secrets
import select
import ssl
import subprocess
import sys
import tempfile
import time

from jsonschema import Draft202012Validator, ValidationError

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("phase3", HERE.parent / "phase3" / "integration.py")
phase3 = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(phase3)
phase2, phase1, run = phase3.phase2, phase3.phase1, phase3.run
VERSION = "2026-07-28"
PREFIX = "io.modelcontextprotocol/"
LIMIT = 256 * 1024
TOOL_SCHEMA = phase2.REPO / "apps" / "schemas" / "tools.json"
NOTICE = b"Original synthetic copyright and authorship notice.\r\n"
EXCERPT = (
    b"Request for Comments: 102\r\nFixture Author\r\n\r\n"
    b"Copyright (c) 2026 IETF Trust.\r\n\r\n"
    b"Legal Provisions Relating to IETF Documents.\r\n\r\n"
    b"1. Duplicate\r\n\r\n"
    + "Élan TLS, exact bytes\r\n\r\n".encode()
    + b"1.1. Child\r\n\r\nDescendant text.\r\n\r\n2. Duplicate\r\n\r\nAnother section.\r\n"
)
TEXT = NOTICE + ("🌐\r\n".encode() + b"\x01" * 1024 + b"\x0c\r\n") * 90
BINARY_NOTICE = b"Synthetic notice \xff.\r\n"
BINARY = BINARY_NOTICE + b"Original RFC bytes \xff\r\n\x0c"
LARGE_NOTICE = b"\x01" * 25_000
RG_ORIGINAL = (b"\xef\xbb\xbf\r\n\r\n" + EXCERPT.replace(b"Comments: 102", b"Comments: 108")
               .replace(b"TLS, exact bytes", b"TLS TLS, exact bytes") + "ÿ tls --version\r\n".encode())
RG_DENSE = EXCERPT.replace(b"Comments: 102", b"Comments: 109") + b"z" * 65536 + b"\r\n"


class Project(phase3.Project):
    def command(self, *args):
        command = super().command(*args)
        insertion = len(self.docker) + 5
        return [*command[:insertion], "-f", str(HERE / "compose.yml"), *command[insertion:]]

    def digest(self, payload):
        command = self.operation("sync", "digest")
        return subprocess.run(command, env=self.env, input=payload, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, timeout=20, check=True).stdout.decode().strip()

    def prepare_reviews(self):
        sources = phase2.prepare(self)
        additions = {
            102: (EXCERPT, "TLS excerpt fixture"),
            105: (TEXT, "Reviewed UTF-8 pages"),
            106: (LARGE_NOTICE + b"Body", "Oversized notice budget"),
            107: (b"1. Root\n\n3.2. Orphaned heading\n\nBody\n", "Partial section hierarchy"),
            108: (RG_ORIGINAL, "Raw ripgrep fixture"),
            109: (RG_DENSE, "Dense ripgrep output fixture"),
        }
        catalog = self.source / "catalog.xml"
        entries = "".join(phase2.catalog_entry(rfc, title) for rfc, (_, title) in additions.items())
        catalog.write_text(catalog.read_text().replace("</rfc-index>", entries + "</rfc-index>"))
        for rfc, payload in [(101, BINARY), *((rfc, pair[0]) for rfc, pair in additions.items())]:
            sources[rfc] = payload
            for transport in ("https", "rsync"):
                (self.source / transport / f"rfc{rfc}.txt").write_bytes(payload)
        registry = json.loads((phase2.REPO / "crates" / "sync" / "src" / "policy.json").read_text())
        registry["version"] = "phase5-synthetic"
        self.digests = {rfc: self.digest(sources[rfc]) for rfc in (101, 102, 105, 106, 108, 109)}
        for rfc, notice in [(101, BINARY_NOTICE), (102, None), (105, NOTICE),
                            (106, LARGE_NOTICE), (108, None), (109, None)]:
            override = dict(rfc=rfc, digest=self.digests[rfc], review="Private synthetic fixture only",
                            policy="whole_paginated" if rfc in {101, 105, 106} else "excerpt_allowed")
            if notice:
                override["notices"] = [dict(category="copyright", start_byte=0, end_byte=len(notice))]
            registry["overrides"].append(override)
        (Path(self.env["PHASE5_DIR"]) / "fixture-policy.json").write_text(json.dumps(registry))
        return sources

    def reconcile_command(self, live, scope):
        assert not live and scope == "full"
        return self.operation("sync", "full", "--review-fixture")

    def start_mcp(self):
        self.run("up", "-d", "--no-deps", "proxy")
        container = self.capture("ps", "-q", "proxy")
        template = '{{range $network, $value := .NetworkSettings.Networks}}{{$network}} {{$value.IPAddress}}{{println}}{{end}}'
        addresses = phase1.capture([*self.docker, "inspect", "--format", template, container])
        self.env["PHASE5_PROXY_IP"] = dict(line.split() for line in addresses.splitlines())[
            f"{self.name}_phase5_ingress"]
        self.run("up", "-d", "--no-deps", "serve")
        port = int(self.capture("port", "proxy", "8443").rsplit(":", 1)[1])
        assert self.capture("port", "proxy", "8443") == f"127.0.0.1:{port}"
        return Client(port, self.directory / "certs" / "ca.crt")

    def admin(self, path):
        return subprocess.run(self.command("exec", "-T", "serve", "wget", "-S", "-O", "-",
                                           f"http://127.0.0.1:8781{path}"),
                              env=self.env, text=True, capture_output=True, timeout=8)


def envelope(method, params=None, request_id=1):
    meta = {PREFIX + "protocolVersion": VERSION,
            PREFIX + "clientInfo": {"name": "phase5-conforming-client", "version": "1"},
            PREFIX + "clientCapabilities": {}}
    body = {"jsonrpc": "2.0", "id": request_id, "method": method,
            "params": {**(params or {}), "_meta": meta}}
    headers = {"Host": "rfc.test", "Content-Type": "application/json",
               "Accept": "application/json, text/event-stream",
               "MCP-Protocol-Version": VERSION, "Mcp-Method": method}
    if method == "tools/call":
        headers["Mcp-Name"] = body["params"]["name"]
    return body, headers


class Client:
    def __init__(self, port, ca_file):
        self.context = ssl.create_default_context(cafile=str(ca_file))
        self.port = port
        self.last = 0
        self.schemas = {}

    def exchange(self, body, headers, method="POST", path="/mcp"):
        # Ordinary application calls stay under the configured per-source rate.
        time.sleep(max(0, self.last + 0.205 - time.monotonic()))
        self.last = time.monotonic()
        conn = http.client.HTTPSConnection("localhost", self.port, context=self.context, timeout=18)
        try:
            conn.request(method, path, json.dumps(body) if isinstance(body, dict) else body, headers)
            response = conn.getresponse()
            raw = response.read()
            assert len(raw) <= LIMIT and response.getheader("Mcp-Session-Id") is None
            data = json.loads(raw) if response.getheader("Content-Type", "").startswith("application/json") else raw
            return response.status, data, len(raw)
        finally:
            conn.close()

    def rpc(self, method, **params):
        body, headers = envelope(method, params)
        status, result, size = self.exchange(body, headers)
        assert status == 200 and result["id"] == body["id"], (status, result)
        assert result["result"]["resultType"] == "complete", result
        return result["result"], size

    def call(self, name, _error=None, **arguments):
        result, size = self.rpc("tools/call", name=name, arguments=arguments)
        structured = result["structuredContent"]
        self.schemas[name].validate(structured)
        assert len(result["content"]) == 1 and result["content"][0]["type"] == "text"
        assert json.loads(result["content"][0]["text"]) == structured
        assert result["isError"] is bool(_error), (name, structured)
        if _error:
            assert structured["code"] == _error, (name, structured)
        return structured, size


def wire_tests(client):
    body, headers = envelope("tools/call", {"name": "list", "arguments": {}})
    result = client.exchange(body, headers)
    assert result[0] == 200 and result[1]["result"]["structuredContent"]["code"] == "corpus_not_ready", result
    discovery, _ = client.rpc("server/discover")
    assert discovery["supportedVersions"] == [VERSION]
    assert set(discovery["capabilities"]) == {"tools"}
    tools = client.rpc("tools/list")[0]["tools"]
    snapshots = json.loads(TOOL_SCHEMA.read_text())
    assert [tool["name"] for tool in tools] == ["rg", "list", "search", "get", "table"]
    examples = [{"pattern": "TLS"}, {}, {"query": 'in::title:"TLS"'}, {"id": 2119}, {"id": 102}]
    for tool, snapshot, example in zip(tools, snapshots, examples, strict=True):
        assert {key: tool[key] for key in snapshot} == snapshot
        assert tool["annotations"] == {"readOnlyHint": True, "destructiveHint": False,
                                       "idempotentHint": True, "openWorldHint": False}
        Draft202012Validator.check_schema(tool["inputSchema"])
        Draft202012Validator.check_schema(tool["outputSchema"])
        inputs = Draft202012Validator(tool["inputSchema"])
        inputs.validate(example)
        try:
            inputs.validate({**example, "unknown": 1})
        except ValidationError:
            pass
        else:
            raise AssertionError("schema accepted unexpected input")
        client.schemas[tool["name"]] = Draft202012Validator(tool["outputSchema"])
        client.call(tool["name"], _error="invalid_arguments", **example, unknown=1)

    client.call("rg", _error="corpus_not_ready", pattern="TLS")
    base, standard = envelope("tools/call", {"name": "list", "arguments": {}})
    issues = [
        ({key: value for key, value in standard.items() if key != "MCP-Protocol-Version"}, 400, -32020),
        ({**standard, "Mcp-Name": "table"}, 400, -32020),
        ({**standard, "Mcp-Method": "tools/list"}, 400, -32020),
        ({**standard, "MCP-Protocol-Version": "2025-11-25"}, 400, -32020),
        ({**standard, "Mcp-Name": "=?base64?{}?="}, 400, -32020),
        ({**standard, "Host": "wrong.test"}, 403, None),
        ({**standard, "Host": "rfc.test:invalid"}, 400, None),
        ({**standard, "Origin": "https://rfc.test:8443"}, 403, None),
        ({**standard, "Origin": "null"}, 403, None),
        ({**standard, "Content-Encoding": "gzip"}, 415, None),
    ]
    for altered, expected, code in issues:
        status, result, _ = client.exchange(base, altered)
        assert status == expected, (altered, status, result)
        if code:
            assert result["error"]["code"] == code and result["id"] == 1
    allowed = {**standard, "Origin": "https://rfc.test", "Mcp-Session-Id": "irrelevant",
               "Last-Event-ID": "irrelevant"}
    assert client.exchange(base, allowed)[0] == 200
    omitted_meta = {**base, "params": {"name": "list", "arguments": {}}}
    assert client.exchange(omitted_meta, standard)[1]["error"]["code"] == -32602
    unsupported = json.loads(json.dumps(base))
    unsupported["params"]["_meta"][PREFIX + "protocolVersion"] = "2030-01-01"
    status, result, _ = client.exchange(unsupported, {**standard, "MCP-Protocol-Version": "2030-01-01"})
    assert status == 400 and result["error"]["code"] == -32022
    assert result["error"]["data"]["supported"] == [VERSION]
    unknown, unknown_headers = envelope("tools/call", {"name": "없는툴"})
    unknown_headers["Mcp-Name"] = "=?base64?" + base64.b64encode("없는툴".encode()).decode() + "?="
    assert client.exchange(unknown, unknown_headers)[1]["error"]["code"] == -32602
    missing_method, missing_headers = envelope("fixtures/unknown")
    status, result, _ = client.exchange(missing_method, missing_headers)
    assert status == 404 and result["error"]["code"] == -32601
    wrong_params = {**base, "params": {**base["params"], "arguments": []}}
    status, result, _ = client.exchange(wrong_params, standard)
    assert status == 200 and result["error"]["code"] == -32602, (status, result)
    assert client.exchange(b"{" + b" " * 65536, standard)[0] == 413
    for invalid, code in ((b"not json", -32700), ({**base, "jsonrpc": "1.0"}, -32600), ([base], -32600)):
        if isinstance(invalid, list):
            invalid = json.dumps(invalid).encode()
        status, result, _ = client.exchange(invalid, standard)
        assert status == 400 and result["id"] is None and result["error"]["code"] == code
    init, init_headers = envelope("initialize")
    assert client.exchange(init, init_headers)[1]["error"]["code"] == -32601
    for method in ("GET", "DELETE"):
        assert client.exchange(None, {"Host": "rfc.test", "Accept": "text/event-stream"}, method)[0] == 405
    for path in ("/ready", "/metrics", "/sync", "/feed"):
        assert client.exchange(None, {"Host": "rfc.test"}, "GET", path)[0] == 404
    print("Conforming discovery/client calls, schema snapshots, and raw wire rejection contracts passed", flush=True)


def bytes_from(value):
    return value["content"].encode() if "content" in value else base64.b64decode(
        value["content_base64"], validate=True)


def reassemble(client, project, rfc, **arguments):
    first, largest = client.call("get", id=rfc, **arguments)
    cursor, result, content = first["next_cursor"], first, bytearray()
    while True:
        original = result["original"]
        part = bytes_from(original)
        assert original["digest"] == project.digests[rfc]
        assert original["start_byte"] == len(content)
        content.extend(part)
        assert original["end_byte"] == len(content)
        notices = result["notice_groups"][0]["notices"]
        expected = BINARY_NOTICE if rfc == 101 else NOTICE
        assert len(notices) == 1 and bytes_from(notices[0]) == expected
        assert original["notice_group_id"] == result["notice_groups"][0]["id"]
        token = result["next_cursor"]
        if not token:
            break
        result, size = client.call("get", id=rfc, cursor=token)
        largest = max(largest, size)
        assert result["generation_id"] == first["generation_id"]
    assert cursor is not None and len(cursor) <= 512
    print(f"RFC {rfc} reassembled {len(content)} bytes; largest complete wire body {largest} bytes", flush=True)
    return bytes(content), cursor


def read_tests(client, project, sources):
    present = sorted(sources)
    assert [item["id"] for item in client.call("list")[0]["items"]] == present
    for rfc in (2119, 8174):
        document = client.call("get", id=rfc)[0]
        assert bytes_from(document["original"]) == sources[rfc] and document["next_cursor"] is None
        original_digest = json.loads((phase2.REPO / "fixtures/rfc/provenance.json").read_text())
        assert document["original"]["digest"] == original_digest[f"rfc{rfc}.txt"]["blake3"]
        client.call("get", id=rfc, range={"start_byte": 0, "end_byte": 10}, _error="source_policy_restricted")
        client.call("get", id=rfc, max_bytes=100, _error="resource_limit_exceeded")
    binary, _ = reassemble(client, project, 101, max_bytes=8)
    text, cursor = reassemble(client, project, 105)
    assert binary == BINARY and text == TEXT
    assert project.digest(binary) == project.digests[101]
    assert project.digest(text) == project.digests[105]
    client.call("get", id=101, format="text", _error="unsupported_source_encoding")
    client.call("get", id=105, range={"start_byte": 0, "end_byte": 1}, _error="source_policy_restricted")
    client.call("get", id=106, _error="resource_limit_exceeded")
    client.call("get", id=100, _error="source_policy_restricted")
    client.call("get", id=9999, _error="rfc_not_found")
    client.call("get", id=105, cursor=cursor, format="base64", _error="cursor_invalid")
    client.call("get", id=102, cursor=cursor, _error="cursor_invalid")
    client.call("get", id=105, cursor=("A" if cursor[0] != "A" else "B") + cursor[1:], _error="cursor_invalid")
    payload = json.loads(base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4))[:-48])
    payload["expiry"] = 1
    encoded = json.dumps(payload, separators=(",", ":")).encode()
    expired = base64.urlsafe_b64encode(encoded + hmac.digest(project.key, encoded, hashlib.sha384)).decode().rstrip("=")
    client.call("get", id=105, cursor=expired, _error="cursor_expired")

    partial = client.call("table", id=107)[0]
    assert partial["coverage"] == {"section_state": "partial", "headings_included": False}, partial
    assert all("heading_text" not in item for item in partial["sections"])
    table = client.call("table", id=102, limit=1)[0]
    assert table["coverage"]["headings_included"] and table["next_cursor"]
    sections = table["sections"][:]
    while table["next_cursor"]:
        table = client.call("table", id=102, limit=1, cursor=table["next_cursor"])[0]
        sections += table["sections"]
    for section in sections:
        bounds = section["heading_range"]
        assert bytes_from(section["original_heading"]) == EXCERPT[bounds["start_byte"]:bounds["end_byte"]]
    assert all(section["depth"] == 0 for section in client.call("table", id=102, max_depth=0)[0]["sections"])
    duplicate = client.call("get", id=102, filter_section_text="Duplicate", _error="ambiguous_section")[0]
    duplicates = [section for section in sections if section["heading_text"] == "Duplicate"]
    assert duplicate["candidate_section_ids"] == [section["section_id"] for section in duplicates]
    selected = client.call("get", id=102, section_id=duplicates[0]["section_id"])[0]
    assert b"Descendant text." in bytes_from(selected["original"])
    client.call("get", id=102, filter_section_text="duplicate", _error="section_not_found")
    client.call("get", id=102, filter_section_text="Duplicate", section_id="s-1", _error="invalid_arguments")
    utf8 = EXCERPT.index("É".encode())
    client.call("get", id=102, range={"start_byte": utf8 + 1, "end_byte": utf8 + 2},
                _error="invalid_utf8_boundary")
    client.call("get", id=102, max_bytes=1, range={"start_byte": utf8, "end_byte": utf8 + 2},
                _error="resource_limit_exceeded")
    raw = client.call("get", id=102, format="base64", range={"start_byte": utf8 + 1, "end_byte": utf8 + 2})[0]
    assert bytes_from(raw["original"]) == EXCERPT[utf8 + 1:utf8 + 2]
    client.call("get", id=102, range={"start_byte": 1, "end_byte": len(EXCERPT) + 1}, _error="invalid_byte_range")
    search = client.call("search", query='in::fulltext:"TLS"', include_snippets=True)[0]
    snippet = next(hit for hit in search["hits"] if hit["id"] == 102)["excerpts"][0]
    assert bytes_from(snippet) == EXCERPT[snippet["start_byte"]:snippet["end_byte"]]
    syntax, _ = client.call("search", query="in::title:'unclosed", _error="invalid_query_syntax")
    assert len(syntax["span"]) == 2
    print("Original-byte identity, sections, policy gating, UTF-8 boundaries, and search snippets passed", flush=True)
    return cursor


def rg_tests(client):
    arguments = dict(pattern="tls", mode="literal", case_sensitive=False,
                     ids=[108, 108], context_lines=1, limit=1)
    first = client.call("rg", **arguments)[0]
    assert first["truncation_reason"] == "match_limit" and not first["scan_complete"], first
    cursor = first["next_cursor"]
    assert cursor
    hit = first["matches"][0]
    assert hit["match_range"] == [RG_ORIGINAL.index(b"TLS"), RG_ORIGINAL.index(b"TLS") + 3]
    notices = first["notice_groups"][0]["notices"]
    assert notices and any(b"Copyright" in bytes_from(notice) for notice in notices)
    for notice in notices:
        assert bytes_from(notice) == RG_ORIGINAL[notice["start_byte"]:notice["end_byte"]]
    arguments["ids"] = [108]
    client.call("rg", **{**arguments, "pattern": "tlsx"}, cursor=cursor, _error="cursor_invalid")
    page = first
    hits = hit["match_range"],
    while page["next_cursor"]:
        page = client.call("rg", **arguments, cursor=page["next_cursor"])[0]
        for hit in page["matches"]:
            start, end = hit["start_byte"], hit["end_byte"]
            assert bytes_from(hit) == RG_ORIGINAL[start:end]
            hits += (hit["match_range"],)
    assert page["scan_complete"] and len(hits) == 3
    assert [RG_ORIGINAL[start:end] for start, end in hits] == [b"TLS", b"TLS", b"tls"]
    assert hits == tuple(sorted(hits))
    option = client.call("rg", pattern="--version", mode="literal", ids=[108])[0]
    hit = option["matches"][0]
    assert bytes_from(hit) == "ÿ tls --version\r\n".encode() and hit["encoding"] == "utf8"
    start = RG_ORIGINAL.index(b"--version")
    assert hit["match_range"] == [start, start + 9]
    bom = client.call("rg", pattern="^\ufeff", ids=[108])[0]["matches"][0]
    assert bom["match_range"] == [0, 3]
    zero = client.call("rg", pattern="^", ids=[108], limit=100)[0]
    line_starts = [0] + [i + 1 for i, byte in enumerate(RG_ORIGINAL[:-1]) if byte == 10]
    assert [hit["match_range"] for hit in zero["matches"]] == [[start, start] for start in line_starts]
    client.call("rg", pattern="[", ids=[108], _error="invalid_regex")
    client.call("rg", pattern="(?=TLS)", ids=[108], _error="invalid_regex")
    client.call("rg", pattern="one\ntwo", mode="literal", _error="invalid_arguments")
    restricted = client.call("rg", pattern="TLS", ids=[101])[0]
    assert not restricted["scan_complete"] and not restricted["matches"] and not restricted["next_cursor"]
    assert restricted["omitted"][0]["code"] == "source_policy_restricted"
    empty = client.call("rg", pattern="absent", ids=[108])[0]
    assert empty["scan_complete"] and empty["matches"] == [] and empty["next_cursor"] is None
    client.call("rg", pattern="z", mode="literal", ids=[109], _error="resource_limit_exceeded")
    partial = client.call("rg", pattern="z", mode="literal", ids=[108, 109])[0]
    assert not partial["scan_complete"] and not partial["matches"]
    assert partial["truncation_reason"] == "worker_budget" and partial["next_cursor"]
    client.call("rg", pattern="z", mode="literal", ids=[108, 109], cursor=partial["next_cursor"],
                _error="resource_limit_exceeded")
    print("Ripgrep exact bytes, signed pagination, option-like patterns, and coverage passed", flush=True)
    return arguments, cursor


@contextmanager
def block_corpus(project):
    with subprocess.Popen(project.psql(), env=project.env, text=True, stdin=subprocess.PIPE,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE) as lock:
        lock.stdin.write("BEGIN; LOCK rfc.generation_documents IN ACCESS EXCLUSIVE MODE;\n\\echo locked\n")
        lock.stdin.flush()
        try:
            assert select.select([lock.stdout], [], [], 5)[0] and lock.stdout.readline().strip() == "locked"
            yield
        finally:
            lock.communicate("ROLLBACK;\n", timeout=8)


def cancellation(project, client):
    sessions = "SELECT count(*) FROM pg_stat_activity WHERE usename='rfc_serve'"
    with block_corpus(project):
        with subprocess.Popen(project.command("exec", "-T", "proxy", "python3", "-B", "/phase5/direct.py", "cancel"),
                              env=project.env, text=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE) as request:
            try:
                assert select.select([request.stdout], [], [], 6)[0] and request.stdout.readline().strip() == "sent"
                waiting = "SELECT count(*) FROM pg_stat_activity WHERE usename='rfc_serve' AND wait_event_type='Lock'"
                phase3.wait_for_sql(project, waiting, "1")
            finally:
                started = time.monotonic()
                _, errors = request.communicate("disconnect\n", timeout=5)
            assert request.returncode == 0, errors
            phase3.wait_for_sql(project, sessions, "0")
            print(f"Disconnected database session was gone after {(time.monotonic() - started) * 1000:.0f} ms",
                  flush=True)
        client.call("get", id=2119, _error="deadline_exceeded")
        phase3.wait_for_sql(project, sessions, "0")
    print("Client disconnect and a lock deadline both closed their database sessions", flush=True)


def publication(project, client, first, get_cursor, rg_page):
    original = client.call("list", limit=1, sort="id_desc")[0]
    cursor = original["next_cursor"]
    assert original["items"][0]["id"] == 9110
    client.call("list", limit=1, sort="id_asc", cursor=cursor, _error="cursor_invalid")
    phase3.changed_catalog(project, "Changed generation document")
    second = int(project.reconcile()["generation_id"])
    project.activate(second, 1)
    fresh = client.call("list")[0]
    assert fresh["generation_id"] == str(second) and fresh["publication_sequence"] == "2"
    assert any(item["title"] == "Changed generation document" for item in fresh["items"])
    pinned = client.call("list", limit=1, sort="id_desc", cursor=cursor)[0]
    assert pinned["generation_id"] == str(first) and pinned["publication_sequence"] == "1"
    assert pinned["items"][0]["id"] == 8174
    rg_args, rg_cursor = rg_page
    rg_pinned = client.call("rg", **rg_args, cursor=rg_cursor)[0]
    assert (rg_pinned["generation_id"], rg_pinned["publication_sequence"]) == (str(first), "1")
    project.activate(first, 2, "rollback")
    assert client.call("list")[0]["publication_sequence"] == "3"
    assert client.call("get", id=105, cursor=get_cursor)[0]["publication_sequence"] == "1"
    project.activate(second, 3, "rollback")
    project.sql("BEGIN; SET LOCAL ROLE rfc_owner; "
                f"UPDATE rfc.generations SET state='deleting' WHERE id={first}; "
                f"UPDATE rfc.generations SET state='deleted' WHERE id={first}; COMMIT")
    client.call("get", id=105, cursor=get_cursor, _error="generation_unavailable")
    client.call("rg", **rg_args, cursor=rg_cursor, _error="generation_unavailable")
    print("Fresh reads and authenticated continuations passed publication, rollback and retirement", flush=True)


def upgrade(project):
    paths = ("0001-storage.sql", "0002-ingestion.sql", "0003-publication.sql", "0004-search.sql")
    script = (phase2.REPO / "migrations" / "migrate.sql").read_text().split(
        "SELECT NOT EXISTS (SELECT FROM rfc.schema_migrations WHERE version = 5)")[0]
    for name in paths:
        script = script.replace(f"\\ir {name}", f"\\i /migrations/{name}")
    run(project.psql(), env=project.env, input=script + "COMMIT;\n", stdout=subprocess.DEVNULL)
    assert project.sql("SELECT version FROM rfc.schema_migrations ORDER BY version") == "1\n2\n3\n4"
    project.sql("""
        INSERT INTO rfc.rfc_identities VALUES (98765);
        INSERT INTO rfc.blobs (digest, byte_length) VALUES (decode(repeat('7a', 32), 'hex'), 4);
        INSERT INTO rfc.document_versions
            (identity, rfc_id, txt_digest, byte_length, parser_version, index_version, encoding,
             indexing_state, notice_state, notice_policy, index_identity, sealed)
        VALUES (decode(repeat('7b', 32), 'hex'), 98765, decode(repeat('7a', 32), 'hex'), 4,
                'fixture-v4', 'fixture-v4', 'utf8', 'complete', 'complete', 'whole_only',
                decode(repeat('7c', 32), 'hex'), true)
    """)
    record = "SELECT to_jsonb(d) FROM rfc.document_versions d WHERE rfc_id=98765"
    before = project.sql(record)
    project.check("serve", expected="unsupported storage schema version")
    project.migrate()
    project.migrate()
    assert project.sql("SELECT version FROM rfc.schema_migrations ORDER BY version") == "\n".join(map(str, range(1, 12)))
    assert project.sql(record) == before


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--context", default="rootless")
    parser.add_argument("--image-prefix", default="rfc-phase5-final")
    args = parser.parse_args()
    if not __debug__:
        raise RuntimeError("Phase 5 requires assertions")
    docker = ["docker", "--context", args.context]
    with tempfile.TemporaryDirectory(prefix="rfc-phase5-", dir="/tmp") as directory:
        work = Path(directory)
        phase3.fixture_driver(docker, args.image_prefix, work / "publication-driver")
        project = Project(work / work.name, work / "publication-driver", args.image_prefix, docker)
        project.env.update(PHASE5_DIR=str(work), PHASE5_SCRIPTS=str(HERE), PHASE5_PROXY_IP="")
        project.key = secrets.token_bytes(48)
        key_file = work / "cursor-key"
        key_file.write_bytes(project.key)
        key_file.chmod(0o444)
        (work / "fixture-policy.json").touch()
        (work / "tls").mkdir()
        phase1.phase0.make_leaf(project.directory / "certs", "localhost", work / "tls")
        try:
            project.start()
            upgrade(project)
            sources = project.prepare_reviews()
            project.run("up", "-d", "--wait", "--wait-timeout", "30", "fixtures")
            client = project.start_mcp()
            for _ in range(25):
                if project.admin("/live").returncode == 0:
                    break
                time.sleep(0.2)
            assert project.admin("/live").returncode == 0
            assert "503" in project.admin("/ready").stderr
            wire_tests(client)
            first = int(project.reconcile()["generation_id"])
            project.activate(first, 0)
            assert project.admin("/ready").returncode == 0
            assert "rfc_mcp_rg_available 1" in project.admin("/metrics").stdout
            cursor = read_tests(client, project, sources)
            rg_page = rg_tests(client)
            project.denied_sql("DELETE FROM rfc.document_versions", phase1.SERVE)
            phase1.exercise_isolation(project)
            for service in ("sync", "feed"):
                project.run("run", "--rm", "--no-deps", "--entrypoint", "sh", service,
                            "-ec", "test ! -e /run/secrets/rfc_cursor_key")
            project.run("run", "--rm", "--no-deps", "-T", "--entrypoint", "python3", "proxy",
                        "-B", "/phase5/direct.py", "untrusted")
            project.run("exec", "-T", "proxy", "python3", "-B", "/phase5/direct.py", "trusted")
            cancellation(project, client)
            publication(project, client, first, cursor, rg_page)
            project.env.update(
                PHASE5_CLIENT_IP_HEADER="X-Forwarded-For",
                PHASE5_CLIENT_IP_HEADER_MODE="chain",
                PHASE5_PROXY_IP=project.env["PHASE5_PROXY_IP"] + ",203.0.113.3",
            )
            project.run("up", "-d", "--no-deps", "--force-recreate", "serve")
            for _ in range(25):
                if project.admin("/live").returncode == 0:
                    break
                time.sleep(0.2)
            assert project.admin("/live").returncode == 0
            project.run("exec", "-T", "proxy", "python3", "-B", "/phase5/direct.py", "chain")
            project.run("run", "--rm", "--no-deps", "-T", "--entrypoint", "python3", "proxy",
                        "-B", "/phase5/direct.py", "untrusted-chain")
        except Exception:
            subprocess.run(project.command("logs", "--tail", "30", "serve", "proxy"),
                           env=project.env, timeout=15, check=False)
            raise
        finally:
            project.run("stop", "proxy", "serve", timeout=30)
            project.close()


if __name__ == "__main__":
    main()
