#!/usr/bin/env python3
"""Exercise source verification and candidate indexing in disposable Compose projects."""

import argparse
from contextlib import ExitStack, contextmanager
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
from statistics import median
import subprocess
import sys
import tempfile
import time

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
SPEC = importlib.util.spec_from_file_location("phase1", HERE.parent / "phase1" / "integration.py")
phase1 = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(phase1)
run = phase1.run
NUL_RFC = 102


class Project(phase1.Project):
    def __init__(self, directory, driver, image_prefix, docker, live):
        self.overlay = HERE / ("live.yml" if live else "compose.yml")
        super().__init__(directory, driver, image_prefix, docker, directory.name)
        self.source = directory / "source"
        self.env.update(PHASE2_DRIVER=str(driver), PHASE2_SOURCE=str(self.source),
                        PHASE2_ENDPOINT=str(HERE / "endpoint.py"))

    def command(self, *args):
        command = super().command(*args)
        insertion = len(self.docker) + 3
        return [*command[:insertion], "-f", str(self.overlay), *command[insertion:]]

    def reconcile_command(self, live, scope, settings=(), *, options=(), prelude=None):
        if live:
            return self.app("sync", args=("--phase2-reconcile", f"--{scope}"))
        overrides = [part for setting in settings for part in ("-e", setting)]
        if prelude is not None:
            return self.app("sync", *options, *overrides, "--entrypoint", "/bin/sh",
                            args=("-c", prelude))
        return self.app("sync", *options, *overrides, "--entrypoint", "/phase2/fixture-driver", args=(scope,))

    def reconcile(self, scope="full", expected=None, live=False, settings=(), timeout=180, prelude=None, *, options=()):
        if settings or prelude is not None or options:
            command = self.reconcile_command(live, scope, settings, options=options, prelude=prelude)
        else:
            command = self.reconcile_command(live, scope)
        result = subprocess.run(command, env=self.env, text=True, capture_output=True, timeout=timeout)
        if not result.stdout.strip():
            raise AssertionError(f"no ingestion report: {result.stderr}")
        report = json.loads(result.stdout.strip().splitlines()[-1])
        print(json.dumps(report, sort_keys=True), flush=True)
        if live:
            return result.returncode, report
        if expected is None:
            assert result.returncode == 0 and report["state"] == "ready", (result.stderr, report)
        else:
            assert result.returncode != 0 and expected in report["error"], (result.stderr, report)
            assert self.sql(f"SELECT state FROM rfc.generations WHERE id = {int(report['generation_id'])}") == "failed"
        return report

    def verify_manifest(self, report):
        generation = int(report["generation_id"])
        command = self.app("serve", "--entrypoint", "/phase2/fixture-driver", args=("verify", str(generation)))
        result = phase1.capture(command, env=self.env)
        assert result == f"verified {generation} {report['manifest_digest']}"


def catalog_entry(rfc, title, tags="", file_format="TXT", day=""):
    return (f"<rfc-entry><doc-id>RFC{rfc}</doc-id><title>{title}</title>"
            f"<date><month>May</month>{day}<year>2026</year></date>"
            f"<format><file-format>{file_format}</file-format></format>"
            f"{tags}<current-status>INFORMATIONAL</current-status><stream>IRTF</stream></rfc-entry>")


def prepare(project, *, include_nul=False):
    source = project.source
    source.mkdir()
    for name in ("https", "rsync"):
        (source / name).mkdir()
    phase1.phase0.make_leaf(project.directory / "certs", "fixtures", source)
    (source / "rsyncd.conf").write_text(
        "pid file = /tmp/rsyncd.pid\nlock file = /tmp/rsyncd.lock\nuse chroot = false\n"
        "read only = true\n[rfc]\npath = /fixture-source/rsync\n"
    )
    fixture = (REPO / "fixtures/synthetic/headings.txt").read_bytes()
    # The 16 KiB byte cap splits "boundarytoken" before the next LF.
    chunk_start = len(fixture)
    between = b"firstterm " + b"x" * (16384 - chunk_start - len(b"firstterm bound")) + b" "
    fixture += between + b"boundarytoken lastterm\n\n5. References\n\n   [RFC90000] Example\n"
    sources = {
        100: fixture,
        101: b"Historical RFC bytes\r\nunencoded \xff bytes\n",
        2119: (REPO / "fixtures/rfc/rfc2119.txt").read_bytes(),
        8174: (REPO / "fixtures/rfc/rfc8174.txt").read_bytes(),
        9110: b"1. An RFCXML classification fixture\n\n   Text.\n",
    }
    if include_nul:
        sources[NUL_RFC] = (b"1. Introduction\r\n\r\n   caf\xc3\xa9\x00text.\n\n"
                            b"2. Copyright Notice\n\n   Copyright example.\n\n"
                            b"3. References\n\n   [RFC100] Example\n")
    for rfc, payload in sources.items():
        for directory in ("https", "rsync"):
            (source / directory / f"rfc{rfc}.txt").write_bytes(payload)
    (source / "https/rfc8174.xml").write_bytes(
        b'<rfc number="8174"><back><references><name>Normative References</name>'
        b'<reference><seriesInfo name="RFC" value="2119"/></reference></references></back></rfc>'
    )
    (source / "https/rfc9110.xml").write_bytes(
        b'<rfc number="9110"><back><references><name>References</name><references>'
        b'<name>Informative References</name><reference><seriesInfo name="RFC" value="100"/>'
        b'</reference></references><reference><seriesInfo name="RFC" value="101"/></reference>'
        b'<reference anchor="RFC-unresolved"><seriesInfo name="RFC" value="invalid"/></reference>'
        b'</references></back></rfc>'
    )
    entries = [
        catalog_entry(100, "Mixed original bytes", "<obsoleted-by><doc-id>RFC103</doc-id></obsoleted-by>"),
        catalog_entry(101, "Legacy bytes", day="<day>23</day>"),
        catalog_entry(103, "PDF only", "<obsoletes><doc-id>RFC100</doc-id></obsoletes>", "PDF"),
        catalog_entry(2119, "RFC 2119", "<updated-by><doc-id>RFC8174</doc-id></updated-by>"),
        catalog_entry(8174, "RFC 8174", "<updates><doc-id>RFC2119</doc-id></updates>"),
        catalog_entry(9110, "Synthetic XML classification"),
        "<rfc-not-issued-entry><doc-id>RFC104</doc-id></rfc-not-issued-entry>",
    ]
    if include_nul:
        entries.append(catalog_entry(NUL_RFC, "NUL bytes with RFCXML"))
        (source / f"https/rfc{NUL_RFC}.xml").write_bytes(
            b'<rfc number="102"><back><references><name>Normative References</name>'
            b'<reference><seriesInfo name="RFC" value="2119"/></reference></references></back></rfc>'
        )
    (source / "catalog.xml").write_text(
        '<rfc-index xmlns="https://www.rfc-editor.org/rfc-index">' + "".join(entries) + "</rfc-index>"
    )
    (source / "mode").write_text("normal")
    return sources


def migrate_v1_then_v2(project):
    setup_v1 = """BEGIN;
SET LOCAL ROLE rfc_owner;
SET LOCAL search_path = pg_catalog, rfc;
ALTER DEFAULT PRIVILEGES REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
ALTER DEFAULT PRIVILEGES REVOKE USAGE ON TYPES FROM PUBLIC;
CREATE SCHEMA rfc AUTHORIZATION rfc_owner;
CREATE TABLE rfc.schema_migrations (version integer PRIMARY KEY CHECK (version > 0),
    applied_at timestamptz NOT NULL DEFAULT now());
\\i /migrations/0001-storage.sql
INSERT INTO rfc.schema_migrations (version) VALUES (1);
COMMIT;
"""
    run(project.psql(), env=project.env, input=setup_v1, stdout=subprocess.DEVNULL)
    assert project.sql("SELECT version FROM rfc.schema_migrations") == "1"
    project.migrate()
    project.migrate()
    assert project.sql("SELECT version FROM rfc.schema_migrations ORDER BY version") == "1\n2\n3\n4\n5\n6\n7\n8\n9"


def verify_ranges(project, report, sources, txt_verification="verified"):
    generation = int(report["generation_id"])
    rows = project.sql(
        "SELECT json_build_array(g.rfc_id, encode(d.txt_digest, 'hex'), d.line_starts, d.encoding, d.notice_policy)"
        " FROM rfc.generation_documents g JOIN rfc.document_versions d ON g.document_version = d.identity"
        f" WHERE g.generation_id={generation} ORDER BY g.rfc_id", phase1.SERVE,
    )
    docs = [json.loads(row) for row in rows.splitlines()]
    assert [doc[0] for doc in docs] == sorted(sources)
    for rfc, digest, line_starts, encoding, policy in docs:
        raw = sources[rfc]
        bytes_mode = rfc in {101, NUL_RFC}
        assert (project.directory / "mirror/blobs/blake3" / digest[:2] / digest).read_bytes() == raw
        assert line_starts == [0, *(i + 1 for i, byte in enumerate(raw[:-1]) if byte == 10)]
        assert encoding == ("bytes" if bytes_mode else "utf8")
        assert policy == ("whole_only" if rfc in {2119, 8174} else "metadata_only")
        version = (f"(SELECT document_version FROM rfc.generation_documents WHERE generation_id={generation}"
                   f" AND rfc_id={rfc})")
        sections = project.sql(
            "SELECT json_build_array(heading_text, start_byte, end_byte, heading_start_byte, heading_end_byte)"
            f" FROM rfc.sections WHERE document_version={version} ORDER BY ordinal", phase1.SERVE,
        )
        for row in sections.splitlines():
            heading, start, end, head_start, head_end = json.loads(row)
            assert start == head_start < head_end <= end <= len(raw)
            raw_heading = " ".join(raw[head_start:head_end].decode().split())
            assert " ".join(heading.split()) in raw_heading
            assert raw[start:end].decode().startswith(raw[head_start:head_end].decode())
        ranges = project.sql(
            "SELECT start_byte,end_byte FROM rfc.notice_ranges"
            f" WHERE document_version={version} UNION ALL"
            " SELECT start_byte,end_byte FROM rfc.relation_evidence"
            f" WHERE document_version={version} AND method='txt'", phase1.SERVE,
        )
        for row in ranges.splitlines():
            start, end = (int(value) for value in row.split("|"))
            assert 0 <= start < end <= len(raw) and raw[start:end].decode()
        chunks = project.sql(
            "SELECT start_byte, end_byte, encode(convert_to(search_text, 'UTF8'), 'hex')"
            f" FROM rfc.text_chunks WHERE document_version={version} ORDER BY start_byte", phase1.SERVE,
        )
        recovered = bytearray()
        for row in chunks.splitlines():
            start, end, hex_text = row.split("|")
            assert int(start) == len(recovered)
            assert bytes.fromhex(hex_text) == raw[int(start):int(end)]
            if rfc == 100:
                assert b"boundarytoken" not in bytes.fromhex(hex_text)
            recovered.extend(bytes.fromhex(hex_text))
        assert bytes(recovered) == (b"" if bytes_mode else raw)
        if bytes_mode:
            assert not sections and not ranges and not chunks
            assert project.sql(
                f"SELECT count(*) FROM rfc.document_lexemes WHERE document_version={version}", phase1.SERVE,
            ) == "0"
        if rfc == NUL_RFC:
            verify_nul_document(project, generation, version, digest, raw, txt_verification)
        if rfc == 100:
            assert b"boundarytoken" in recovered
            assert project.sql(
                f"SELECT count(*) FROM rfc.document_lexemes WHERE document_version={version}"
                " AND lexeme IN ('firstterm', 'boundarytoken', 'lastterm')", phase1.SERVE,
            ) == "3"

    assert project.sql(
        f"SELECT scope, catalog_only->0->>0 FROM rfc.generation_ingestion WHERE generation_id={generation}"
    ) == "full|103"
    assert project.sql(
        "SELECT b.raw_digest=a.raw_digest, b.identity<>a.identity, b.etag "
        "FROM rfc.generation_ingestion i JOIN rfc.source_observations b ON b.identity=i.catalog_before "
        f"JOIN rfc.source_observations a ON a.identity=i.catalog_after WHERE i.generation_id={generation}"
    ) == 't|t|"fixture"'
    assert project.sql(
        f"SELECT rfc_id,publication_date,publication_month FROM rfc.generation_documents WHERE generation_id={generation}"
        " AND rfc_id IN (100,101) ORDER BY rfc_id"
    ) == "100||2026-05-01\n101|2026-05-23|"
    assert project.sql(
        f"SELECT count(*) FROM rfc.edge_evidence WHERE generation_id={generation}"
        " AND kind='updates' AND source_rfc=8174 AND target_rfc=2119"
    ) == "2"
    assert project.sql(
        f"SELECT count(*) FROM rfc.rfc_relations WHERE generation_id={generation}"
        " AND ((source_rfc,kind,target_rfc) IN ((100,'unclassified_reference',90000),"
        " (8174,'normative_reference',2119),(9110,'informative_reference',100),"
        " (9110,'unclassified_reference',101),(103,'obsoletes',100)))", phase1.SERVE,
    ) == "5"
    assert project.sql(
        f"SELECT state FROM rfc.relation_coverage WHERE generation_id={generation} AND rfc_id=9110"
        " AND family='all_references'"
    ) == "partial"


def verify_nul_document(project, generation, version, digest, raw, txt_verification):
    assert raw.decode("utf-8").count("\x00") == 1
    assert project.sql(
        "SELECT byte_length,sealed,indexing_state,section_state,notice_state"
        f" FROM rfc.document_versions WHERE identity={version}", phase1.SERVE,
    ) == f"{len(raw)}|t|unavailable|unavailable|unavailable"
    command = project.app("serve", "-T", "--entrypoint", "/phase2/fixture-driver",
                          args=("read-blob", digest, str(len(raw))))
    result = subprocess.run(command, env=project.env, capture_output=True, check=True, timeout=30)
    assert result.stdout == raw
    assert project.sql(
        "SELECT g.title,t.https_verification,t.raw_digest=d.txt_digest,x.kind,x.https_verification"
        " FROM rfc.generation_documents g JOIN rfc.document_versions d ON d.identity=g.document_version"
        " JOIN rfc.source_observations t ON t.identity=g.txt_observation"
        " JOIN rfc.source_observations x ON x.identity=g.rfcxml_observation"
        f" WHERE g.generation_id={generation} AND g.rfc_id={NUL_RFC}", phase1.SERVE,
    ) == f"NUL bytes with RFCXML|{txt_verification}|t|rfcxml|verified"
    assert project.sql(
        "SELECT r.method FROM rfc.edge_evidence e JOIN rfc.relation_evidence r ON r.identity=e.evidence"
        f" WHERE e.generation_id={generation} AND e.source_rfc={NUL_RFC}"
        " AND e.kind='normative_reference' AND e.target_rfc=2119", phase1.SERVE,
    ) == "rfcxml"


def exercise(project):
    sources = prepare(project, include_nul=True)
    project.start()
    migrate_v1_then_v2(project)
    project.check("sync")
    project.check("serve")
    project.run("up", "-d", "--wait", "fixtures", timeout=35)
    mode = project.source / "mode"
    mode.write_text("overlap")
    first = project.reconcile(settings=("RFC_PHASE2_PACED=1",))
    assert_pipeline_bounds(first, http_limit=2, paced=True)
    assert first["metrics"]["https_peak"] == 2 and first["metrics"]["out_of_order"] > 0
    mode.write_text("normal")
    assert first["verified_txt"] == len(sources) and first["reused_txt"] == 0
    assert first["txt_audit"]["matched"] == len(sources)
    assert project.sql(
        "SELECT count(*) FROM rfc.source_observations h JOIN rfc.source_observations r"
        " ON r.identity=h.comparison_rsync AND r.raw_digest=h.raw_digest"
        " WHERE h.verification_method='dual_source' AND r.verification_method='rsync_integrity'"
        " AND r.source_url='rsync://rsync.rfc-editor.org/rfcs-text-only/' || split_part(h.source_url,'/',5)",
        phase1.SERVE,
    ) == str(len(sources))
    for digest, peer in (
        ("p.raw_digest", "p.raw_digest <> h.raw_digest AND r.identity=h.comparison_rsync"),
        ("p.raw_digest", "p.raw_digest <> h.raw_digest AND r.identity=p.identity"),
    ):
        project.denied_sql(
            "INSERT INTO rfc.source_observations "
            "(identity,kind,source_url,observed_at,fetched_at,raw_digest,https_verification,comparison_rsync) "
            f"SELECT sha256(h.identity),'txt',h.source_url,now(),now(),{digest},'verified',r.identity "
            "FROM rfc.source_observations h CROSS JOIN rfc.source_observations p CROSS JOIN rfc.source_observations r "
            "WHERE h.verification_method='dual_source' AND p.verification_method='rsync_integrity' AND "
            + peer + " LIMIT 1", phase1.SYNC, state="23514",
        )
    assert first["invalid_utf8"] == first["contains_nul"] == 1
    verify_ranges(project, first, sources)
    project.verify_manifest(first)
    catalog = project.source / "catalog.xml"
    catalog.write_text(catalog.read_text().replace("Synthetic XML classification", "Updated catalog metadata"))
    second = project.reconcile()
    assert second["reused_txt"] == len(sources)
    assert second["txt_audit"]["reused_dual_source"] == len(sources)
    assert second["metrics"]["https_requests"] == len(sources) + 2
    assert second["invalid_utf8"] == second["contains_nul"] == 1
    assert project.sql("SELECT count(*) FROM rfc.document_versions") == str(len(sources))
    assert project.sql(
        "SELECT count(*),count(DISTINCT document_version),count(DISTINCT txt_observation)"
        f" FROM rfc.generation_documents WHERE rfc_id={NUL_RFC}"
        f" AND generation_id IN ({int(first['generation_id'])},{int(second['generation_id'])})",
    ) == "2|1|1"
    assert project.sql(
        "SELECT count(*) FROM rfc.source_observations WHERE kind='txt'"
        f" AND source_url='https://www.rfc-editor.org/rfc/rfc{NUL_RFC}.txt'",
    ) == "1"
    assert project.sql(
        f"SELECT title FROM rfc.generation_documents WHERE generation_id={int(second['generation_id'])} AND rfc_id=9110"
    ) == "Updated catalog metadata"
    assert second["manifest_digest"] != first["manifest_digest"]
    project.verify_manifest(second)
    clock = "RFC_PHASE2_FIXED_CLOCK=1"
    sample = project.reconcile(scope="sample", settings=(clock,))
    assert sample["verified_txt"] == 3 and sample["scope"] == "sample"
    project.verify_manifest(sample)
    exercise_config(project, sample)

    unchanged_catalog = catalog.read_text()
    catalog.write_text(unchanged_catalog.replace(catalog_entry(101, "Legacy bytes", day="<day>23</day>"), ""))
    project.reconcile(expected="catalog lost previously accepted RFC 101")
    catalog.write_text(unchanged_catalog)

    for scenario, expected in [
        ("catalog-compressed", "content encoding"),
        ("catalog-oversize", "byte limit"),
        ("catalog-redirect", "302"),
        ("catalog-malformed", "unexpected end"),
        ("rsync-interrupted", "rsync batch failed"),
        ("catalog-drift", "catalog changed"),
    ]:
        mode.write_text(scenario)
        project.reconcile(expected=expected)
    mode.write_text("normal")
    catalog.write_text(unchanged_catalog.replace("<doc-id>RFC100</doc-id>", "<doc-id>RFC../100</doc-id>", 1))
    rejected_catalog = project.reconcile(expected="invalid RFC catalog document number")
    assert rejected_catalog["metrics"]["rsync"]["calls"] == 0
    catalog.write_text(unchanged_catalog)
    mode.write_text("bad-rfcxml")
    fallback = project.reconcile()
    assert any("RFC 8174 RFCXML parse" in error for error in fallback["diagnostics"])
    assert project.sql(
        f"SELECT state FROM rfc.relation_coverage WHERE generation_id={int(fallback['generation_id'])}"
        " AND rfc_id=8174 AND family='all_references'"
    ) == "partial"
    exercise_xml_statuses(project)
    mode.write_text("normal")
    original = project.source / "rsync/rfc100.txt"
    original.unlink()
    project.reconcile(expected="missing expected TXT rfc100.txt")
    original.symlink_to("../https/rfc100.txt")
    project.reconcile(expected="missing expected TXT")
    original.unlink()
    original.write_bytes(b"Different same-ID candidate.\x00\n")
    mismatch = project.reconcile(expected="rsync and HTTPS TXT disagree")
    assert mismatch["txt_audit"]["mismatched"] == 1
    assert mismatch["metrics"]["received_bytes"] > 0
    for filename, expected in (("body", original.read_bytes()), ("https-body", sources[100])):
        quarantine = project.app("sync", "-T", "--entrypoint", "/bin/sh", args=(
            "-ec", 'find /mirror/quarantine -type f -name "$1" -exec cat {} +', "sh", filename,
        ))
        captured = subprocess.run(quarantine, env=project.env, check=True,
                                  stdout=subprocess.PIPE, timeout=30).stdout
        assert captured == expected
    original.write_bytes(sources[100])

    locks = project.psql(phase1.SYNC)
    with subprocess.Popen(locks, env=project.env, text=True,
                          stdin=subprocess.PIPE, stdout=subprocess.PIPE) as holder:
        try:
            holder.stdin.write("SELECT pg_try_advisory_lock(-1::bigint);\n")
            holder.stdin.flush()
            assert holder.stdout.readline().strip() == "t"
            contender = subprocess.run(project.reconcile_command(False, "full"), env=project.env,
                                       capture_output=True, text=True, timeout=25)
            assert contender.returncode != 0 and "holds the corpus lock" in contender.stdout
        finally:
            holder.stdin.close()
            holder.wait(timeout=6)

    assert project.sql("SELECT generation_id,publication_sequence FROM rfc.active_generation") == "|0"
    assert project.sql("SELECT count(*) FROM rfc.notification_outbox") == "0"
    project.denied_sql("DELETE FROM rfc.document_lexemes", phase1.SERVE)
    project.denied_sql("UPDATE rfc.active_generation SET generation_id=NULL", phase1.SYNC)
    phase1.exercise_isolation(project)
    exercise_cancellation(project)
    exercise_transaction_failure(project)

    changed = sources[100] + b"\nCorrected original.\n"
    original.write_bytes(changed)
    (project.source / "https/rfc100.txt").write_bytes(changed)
    refreshed = project.reconcile()
    assert refreshed["reused_txt"] == 5 and refreshed["metrics"]["txt"]["calls"] == 1
    project.verify_manifest(refreshed)
    assert project.sql("""SELECT count(DISTINCT txt_observation) FROM rfc.generation_documents
                          WHERE rfc_id=100""") == "2"
    control = project.reconcile(settings=(clock, "RFC_SYNC_HTTP_CONCURRENCY=2"))
    blocked = exercise_writer_backpressure(project)
    assert blocked["manifest_digest"] == control["manifest_digest"]
    verify_ranges(project, blocked, {**sources, 100: changed})
    print("Phase 2 fixture candidate, byte indexes, provenance, failure fences and role boundaries passed", flush=True)


def assert_pipeline_bounds(report, *, http_limit, paced=False):
    metrics = report["metrics"]
    assert 1 <= metrics["https_peak"] <= http_limit, metrics
    assert 1 <= metrics["parse_peak"] <= 2, metrics
    assert 1 <= metrics["admitted_peak"] <= 3, metrics
    assert metrics["admitted_peak"] <= metrics["xml_window_peak"] <= 3, metrics
    assert metrics["raw_reserved_peak"] == metrics["xml_window_peak"] * 16 * 1024**2, metrics
    assert metrics["staged_peak"] == 1 and metrics["rsync_peak_bytes"] <= 128 * 1024**2, metrics
    if paced:
        assert metrics["minimum_start_spacing_us"] >= 1_000_000, metrics


def exercise_config(project, baseline):
    config = project.directory / "ingestion.toml"
    config.write_bytes((REPO / "deploy/ingestion.toml.example").read_bytes())
    options = ("-v", f"{config}:/phase2/ingestion.toml:ro")
    settings = ("RFC_SYNC_CONFIG_FILE=/phase2/ingestion.toml", "RFC_PHASE2_FIXED_CLOCK=1")
    default_file = project.reconcile(scope="sample", settings=settings, options=options)
    assert default_file["manifest_digest"] == baseline["manifest_digest"]

    config.write_text("""schema_version = 1
[ingestion.https]
max_in_flight_requests = 1
min_request_start_interval_ms = 1050
request_timeout_seconds = 5
rfcxml_max_attempts = 1
[ingestion.rsync]
batch_size = 1
transfer_timeout_seconds = 30
[ingestion.pipeline]
max_pending_documents = 1
""")
    tuned = project.reconcile(scope="sample", settings=(*settings, "RFC_PHASE2_PACED=1"), options=options)
    assert tuned["manifest_digest"] == baseline["manifest_digest"]
    assert_pipeline_bounds(tuned, http_limit=1, paced=True)
    metrics = tuned["metrics"]
    assert metrics["parse_peak"] == metrics["admitted_peak"] == metrics["xml_window_peak"] == 1, metrics
    assert metrics["rsync"]["calls"] == 3 and metrics["minimum_start_spacing_us"] >= 1_050_000, metrics
    assert project.sql("SELECT generation_id,publication_sequence FROM rfc.active_generation") == "|0"

    config.write_text("schema_version = 1\n[ingestion]\nmax_accepted_bytes = 1\n")
    limited = project.reconcile(scope="sample", expected="run input exceeds byte limit",
                                settings=settings, options=options)
    assert limited["accepted_bytes"] == 0 and limited["verified_txt"] == 0
    print("Sync TOML defaults and tuned reconciliation bounds passed", flush=True)


def fixture_logs(project):
    return project.capture("logs", "--no-color", "--no-log-prefix", "fixtures")


def verify_xml_observation(project, report, rfc, body):
    generation = int(report["generation_id"])
    digest, transport, verification, url = project.sql(
        "SELECT encode(o.raw_digest,'hex'),o.source_transport,o.https_verification,o.source_url "
        "FROM rfc.generation_documents d JOIN rfc.source_observations o ON o.identity=d.rfcxml_observation "
        f"WHERE d.generation_id={generation} AND d.rfc_id={rfc}"
    ).split("|")
    assert (transport, verification, url) == (
        "https", "verified", f"https://www.rfc-editor.org/rfc/rfc{rfc}.xml",
    )
    assert (project.directory / "mirror/blobs/blake3" / digest[:2] / digest).read_bytes() == body


def exercise_xml_statuses(project):
    for scenario, expected, malformed, terminal_ids in (
        ("xml-statuses", {"not_found": 1, "gone": 1, "terminal_failure": 1},
         b'<rfc number="102"><middle/></rfc>', [9110]),
        ("xml-policy", {"not_found": 0, "gone": 0, "terminal_failure": 3},
         b"<rfc number='8174'><back>", [101, 102, 2119]),
    ):
        (project.source / "mode").write_text(scenario)
        report = project.reconcile(settings=(
            "RFC_SYNC_HTTP_CONCURRENCY=1", "RFC_PHASE2_HTTP_TIMEOUT_MS=1000",
        ))
        assert report["rfcxml"] == {
            "captured": 1, "reused": 0, "malformed": 1, "retries_exhausted": 1, **expected,
        }, report
        assert report["xml_unavailable"] == 4 and report["metrics"]["retries"] == 3, report
        verify_xml_observation(project, report, 100, b'<rfc number="100"><middle/></rfc>')
        verify_xml_observation(project, report, 8174, malformed)
        generation = int(report["generation_id"])
        assert project.sql(
            f"SELECT count(*) FROM rfc.generation_documents WHERE generation_id={generation}"
            " AND rfcxml_observation IS NOT NULL"
        ) == "2"
        assert project.sql(
            f"SELECT state FROM rfc.relation_coverage WHERE generation_id={generation}"
            " AND rfc_id=8174 AND family='all_references'"
        ) == "partial"
        coverage = project.sql(f"""
            SELECT d.rfc_id, d.rfcxml_observation IS NOT NULL,
                a.state, c.state, EXISTS (SELECT FROM rfc.edge_evidence e
                    JOIN rfc.relation_evidence r ON r.identity=e.evidence
                    WHERE (e.generation_id,e.source_rfc)=(d.generation_id,d.rfc_id) AND r.method='rfcxml')
            FROM rfc.generation_documents d
            JOIN rfc.relation_coverage a ON (a.generation_id,a.rfc_id,a.family) =
                (d.generation_id,d.rfc_id,'all_references')
            JOIN rfc.relation_coverage c ON (c.generation_id,c.rfc_id,c.family) =
                (d.generation_id,d.rfc_id,'classified_references')
            WHERE d.generation_id={generation} ORDER BY d.rfc_id
        """, phase1.SERVE).splitlines()
        assert coverage[0] == "100|t|complete|complete|f", coverage
        assert coverage[1:3] == ["101|f|unavailable|unavailable|f", "102|f|unavailable|unavailable|f"], coverage
        for row in coverage[3:]:
            rfc, captured, all_refs, classified, xml_edge = row.split("|")
            assert captured == ("t" if rfc == "8174" else "f") and xml_edge == "f", coverage
            assert all_refs != "complete" and classified != "complete", coverage
        logs = fixture_logs(project)
        cooldown, = re.findall(rf"fixture cooldown {scenario} /rfc/rfc100.xml (\d+\.\d+)", logs)
        starts = [(path, float(at)) for path, at in
                  re.findall(rf"fixture request {scenario} (\S+) (\d+\.\d+)", logs)]
        after_cooldown = [(path, at) for path, at in starts if at > float(cooldown)]
        assert after_cooldown[0][1] - float(cooldown) >= 1.9, starts
        assert sum(path == "/rfc/rfc100.xml" for path, _ in starts) == 2
        exhausted = 2119 if scenario == "xml-statuses" else 9110
        assert sum(path == f"/rfc/rfc{exhausted}.xml" for path, _ in starts) == 3
        for rfc in terminal_ids:
            assert sum(path == f"/rfc/rfc{rfc}.xml" for path, _ in starts) == 1
        project.verify_manifest(report)
    (project.source / "mode").write_text("xml-interrupted")
    interrupted = project.reconcile()
    assert interrupted["metrics"]["retries"] == 1 and interrupted["rfcxml"]["captured"] == 3, interrupted
    verify_xml_observation(project, interrupted, 9110, (project.source / "https/rfc9110.xml").read_bytes())
    project.verify_manifest(interrupted)
    print("RFCXML HTTP outcomes, evidence and shared Retry-After passed", flush=True)


@contextmanager
def xml_collector(project, scenario, *, options, settings):
    mode = project.source / "mode"
    previous = mode.read_text()
    mode.write_text(scenario)
    name = f"{project.name}-{scenario}"
    command = project.reconcile_command(False, "full", settings=settings,
                                        options=("--name", name, *options))
    stdout = project.directory / f"{scenario}.json"
    stderr = project.directory / f"{scenario}.log"
    with stdout.open("w") as out, stderr.open("w") as err:
        with subprocess.Popen(command, env=project.env, stdout=out, stderr=err) as collector:
            try:
                yield collector, name, stdout, stderr
            finally:
                for gate in ("release-rsync", "release-xml"):
                    (project.source / gate).touch()
                subprocess.run([*project.docker, "rm", "-f", name], stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL, timeout=15)
                mode.write_text(previous)
                for gate in ("release-rsync", "release-xml"):
                    (project.source / gate).unlink()


def wait_for_fixture(collector, check, description):
    until = time.monotonic() + 15
    while time.monotonic() < until:
        if check():
            return
        assert collector.poll() is None, description
        time.sleep(0.1)
    raise AssertionError(description)


def exercise_xml_overlap(project, *, options, settings):
    with xml_collector(project, "xml-overlap", options=options, settings=settings) as run:
        collector, _, stdout, stderr = run
        def xml_starts(logs):
            return set(re.findall(r"fixture request xml-overlap /rfc/rfc(\d+)\.xml", logs))

        def rsync_and_window():
            logs = fixture_logs(project)
            return "fixture rsync held for XML" in logs and xml_starts(logs) == {"100", "101", "102"}

        def made_progress():
            state = project.sql("""
                SELECT g.state,
                    EXISTS (SELECT FROM rfc.document_versions WHERE rfc_id=100 AND sealed),
                    EXISTS (SELECT FROM rfc.generation_documents WHERE generation_id=g.id AND rfc_id=100),
                    EXISTS (SELECT FROM rfc.generation_documents WHERE generation_id=g.id AND rfc_id=101)
                FROM rfc.generations g ORDER BY g.id DESC LIMIT 1
            """)
            return state == "building|t|f|t" and "2119" in xml_starts(fixture_logs(project))

        wait_for_fixture(collector, rsync_and_window, "XML window did not fill while bulk rsync was held")
        (project.source / "release-rsync").touch()
        wait_for_fixture(collector, made_progress, "TXT sealing and slot refill stalled behind RFC 100 XML")
        (project.source / "release-xml").touch()
        assert collector.wait(timeout=30) == 0, stderr.read_text()
        report = json.loads(stdout.read_text().strip().splitlines()[-1])
    assert report["state"] == "ready" and report["metrics"]["xml_window_peak"] == 3, report
    assert_pipeline_bounds(report, http_limit=2)
    assert report["metrics"]["out_of_order"] > 0, report
    print("TXT sealed, later membership committed, and the slot refilled while RFC 100 XML was held", flush=True)
    return report


def exercise_writer_backpressure(project):
    key = 741007
    project.sql(f"""
        CREATE FUNCTION rfc.fixture_hold_membership() RETURNS trigger LANGUAGE plpgsql
        SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
        BEGIN PERFORM pg_catalog.pg_advisory_xact_lock({key}::bigint); RETURN NEW; END $$;
        REVOKE EXECUTE ON FUNCTION rfc.fixture_hold_membership() FROM PUBLIC;
        CREATE TRIGGER fixture_hold_membership BEFORE INSERT ON rfc.generation_documents
        FOR EACH ROW EXECUTE FUNCTION rfc.fixture_hold_membership()
    """)
    try:
        lock = project.psql()
        with subprocess.Popen(lock, env=project.env, text=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE) as holder:
            try:
                holder.stdin.write(f"SELECT pg_try_advisory_lock({key}::bigint);\n")
                holder.stdin.flush()
                assert holder.stdout.readline().strip() == "t"
                with xml_collector(project, "xml-writer-block", options=(), settings=(
                    "RFC_PHASE2_FIXED_CLOCK=1", "RFC_SYNC_HTTP_CONCURRENCY=2",
                )) as (collector, _, stdout, stderr):
                    pattern = r"fixture request xml-writer-block /rfc/rfc(\d+)\.xml (\d+\.\d+)"

                    def stalled():
                        waiting = project.sql(f"SELECT count(*) FROM pg_locks WHERE locktype='advisory'"
                                              f" AND objid={key} AND NOT granted")
                        starts = re.findall(pattern, fixture_logs(project))
                        assert len(starts) <= 3, starts
                        return waiting == "1" and len(starts) == 3

                    try:
                        wait_for_fixture(collector, stalled, "writer did not stall with a full XML window")
                    except AssertionError as error:
                        raise AssertionError((str(error), project.sql(
                            f"SELECT locktype,objid,granted FROM pg_locks WHERE objid={key}"
                        ), re.findall(pattern, fixture_logs(project)), stdout.read_text(), stderr.read_text())) from error
                    before_release = time.monotonic()
                    holder.stdin.close()
                    holder.wait(timeout=6)
                    assert collector.wait(timeout=40) == 0, stderr.read_text()
                    report = json.loads(stdout.read_text().strip().splitlines()[-1])
                    starts = re.findall(pattern, fixture_logs(project))
                    assert sorted(int(rfc) for rfc, _ in starts) == [100, 101, 102, 2119, 8174, 9110]
                    assert {int(rfc) for rfc, when in starts if float(when) < before_release} == {100, 101, 102}, starts
            finally:
                if not holder.stdin.closed:
                    holder.stdin.close()
                holder.wait(timeout=6)
    finally:
        project.sql("DROP TRIGGER fixture_hold_membership ON rfc.generation_documents;"
                    " DROP FUNCTION rfc.fixture_hold_membership()")
    assert_pipeline_bounds(report, http_limit=2)
    assert report["metrics"]["xml_window_peak"] == 3
    project.verify_manifest(report)
    print("PostgreSQL writer backpressure stopped admission at the XML item and byte window", flush=True)
    return report


def unused_sealed_versions(project, rfc):
    return project.sql(
        "SELECT count(*) FROM rfc.document_versions JOIN rfc.unused_document_versions USING (identity)"
        f" WHERE sealed AND rfc_id={rfc}"
    )


def exercise_xml_cancellation(project, *, options, settings, active):
    rsync = project.source / "rsync/rfc100.txt"
    https = project.source / "https/rfc100.txt"
    original = rsync.read_bytes()
    changed = original + b"\nEarly seal cancellation fixture.\n"
    try:
        rsync.write_bytes(changed)
        https.write_bytes(changed)
        with xml_collector(project, "xml-stop-response", options=options, settings=settings) as run:
            collector, name, stdout, stderr = run
            wait_for_fixture(collector, lambda: unused_sealed_versions(project, 100) == "1",
                             "changed TXT did not seal while RFC 100 XML was held")
            phase1.run([*project.docker, "kill", "--signal", "TERM", name], stdout=subprocess.DEVNULL)
            assert collector.wait(timeout=10) == 1, stderr.read_text()
            report = json.loads(stdout.read_text().strip().splitlines()[-1])
            assert report["error"] == "reconciliation cancelled" and report["metrics"]["retries"] == 0, report
            assert project.sql(f"SELECT state FROM rfc.generations WHERE id={int(report['generation_id'])}") == "failed"
    finally:
        rsync.write_bytes(original)
        https.write_bytes(original)
    phase1.capture(project.app("sync", args=("--phase3-recover",)), env=project.env)
    assert unused_sealed_versions(project, 100) == "0"
    assert project.sql("SELECT generation_id,publication_sequence FROM rfc.active_generation") == active

    for scenario, retries_exhausted in (
        ("xml-stop-retry", 0), ("xml-stop-cooldown", 1),
    ):
        with xml_collector(project, scenario, options=options, settings=settings) as run:
            collector, name, stdout, stderr = run
            wait_for_fixture(collector, lambda: f"fixture cooldown {scenario} /rfc/rfc100.xml" in fixture_logs(project),
                             f"RFC 100 did not reach {scenario}")
            if retries_exhausted:
                wait_for_fixture(collector, lambda: project.sql(
                    "SELECT count(*) FROM rfc.generation_documents d JOIN rfc.generations g ON g.id=d.generation_id "
                    "WHERE g.state='building' AND d.rfc_id=100"
                ) == "1", "long cooldown did not settle RFC 100 without retrying it")
            phase1.run([*project.docker, "kill", "--signal", "TERM", name], stdout=subprocess.DEVNULL)
            assert collector.wait(timeout=10) == 1, stderr.read_text()
            report = json.loads(stdout.read_text().strip().splitlines()[-1])
            assert report["error"] == "reconciliation cancelled" and report["metrics"]["retries"] == 0, report
            assert report["rfcxml"]["retries_exhausted"] == retries_exhausted, report
        assert project.sql("SELECT state FROM rfc.generations ORDER BY id DESC LIMIT 1") == "failed"
        assert project.sql("SELECT generation_id,publication_sequence FROM rfc.active_generation") == active
    print("RFCXML response and retry cancellation preserved the active generation", flush=True)


def exercise_cancellation(project, *, options=(), settings=(), active="|0"):
    mode = project.source / "mode"
    mode.write_text("hold-rsync")
    name = f"{project.name}-cancel"
    script = """
/phase2/fixture-driver full &
child=$!
trap 'kill -TERM "$child"' TERM
wait "$child"
status=$?
if [ "$status" -gt 128 ]; then wait "$child"; status=$?; fi
test "$status" -eq 1 || exit 2
test -z "$(find /tmp -name 'rfc-rsync-*')" || exit 3
temporary=/rfc-rsync-txt/full/temporary
if [ -d "$temporary" ]; then test -z "$(ls -A "$temporary")" || exit 5; fi
if pidof rsync >/dev/null; then exit 4; fi
printf 'cancelled and reaped\n'
"""
    command = project.reconcile_command(False, "full", settings=settings,
                                        options=("--name", name, *options), prelude=script)
    with subprocess.Popen(command, env=project.env, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, text=True) as collector:
        try:
            until = time.monotonic() + 12
            while time.monotonic() < until:
                try:
                    result = subprocess.run([*project.docker, "exec", name, "pidof", "rsync"],
                                            capture_output=True, timeout=3)
                except subprocess.TimeoutExpired:
                    result = None
                if result is not None and result.returncode == 0:
                    break
                assert collector.poll() is None
                time.sleep(0.1)
            else:
                raise AssertionError("rsync did not reach the cancellation boundary")
            run([*project.docker, "kill", "--signal", "TERM", name], stdout=subprocess.DEVNULL)
            stdout, stderr = collector.communicate(timeout=15)
            assert collector.returncode == 0 and "cancelled and reaped" in stdout, (stdout, stderr)
            assert "reconciliation cancelled" in stdout
        finally:
            subprocess.run([*project.docker, "rm", "-f", name], stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL, timeout=15)
            mode.write_text("normal")
    assert project.sql("SELECT state FROM rfc.generations ORDER BY id DESC LIMIT 1") == "failed"
    assert project.sql("SELECT generation_id,publication_sequence FROM rfc.active_generation") == active


def exercise_transaction_failure(project):
    rsync = project.source / "rsync/rfc101.txt"
    https = project.source / "https/rfc101.txt"
    original = rsync.read_bytes()
    project.sql("""CREATE FUNCTION rfc.fixture_fail_member() RETURNS trigger LANGUAGE plpgsql
        AS $$ BEGIN RAISE EXCEPTION 'fixture member transaction failure'; END $$;
        REVOKE EXECUTE ON FUNCTION rfc.fixture_fail_member() FROM PUBLIC;
        CREATE TRIGGER fixture_fail_member BEFORE INSERT ON rfc.generation_documents
        FOR EACH ROW WHEN (NEW.rfc_id = 101) EXECUTE FUNCTION rfc.fixture_fail_member()""")
    try:
        changed = original + b"\nMembership failure fixture.\n"
        rsync.write_bytes(changed)
        https.write_bytes(changed)
        failure = project.reconcile(expected="db error")
        generation = int(failure["generation_id"])
        assert project.sql(f"SELECT count(*) FROM rfc.generation_documents"
                           f" WHERE generation_id={generation} AND rfc_id=101") == "0"
        assert unused_sealed_versions(project, 101) == "1"
    finally:
        rsync.write_bytes(original)
        https.write_bytes(original)
        project.sql("""DROP TRIGGER fixture_fail_member ON rfc.generation_documents;
                       DROP FUNCTION rfc.fixture_fail_member()""")
    retry = project.reconcile()
    assert retry["verified_txt"] == retry["reused_txt"] == 6
    project.verify_manifest(retry)
    assert unused_sealed_versions(project, 101) == "0"
    assert project.sql("SELECT count(*) FROM rfc.generations WHERE state='building'") == "0"
    assert project.sql("SELECT generation_id,publication_sequence FROM rfc.active_generation") == "|0"
    assert project.sql("SELECT count(*) FROM rfc.notification_outbox") == "0"


def exercise_resources(project):
    project.env["PHASE2_TMPFS"] = "144m"
    prepare(project, include_nul=True)
    source = project.source
    entries = []
    for rfc in range(100, 133):
        entries.append(catalog_entry(rfc, f"Capacity {rfc}"))
        for directory in ("rsync", "https"):
            with (source / directory / f"rfc{rfc}.txt").open("wb") as file:
                file.write(f"RFC {rfc}\n".encode() + b"\0")
                if rfc < 132:
                    file.truncate(4 * 1024**2)
    (source / "catalog.xml").write_text(
        '<rfc-index xmlns="https://www.rfc-editor.org/rfc-index">' + "".join(entries) + "</rfc-index>"
    )
    xml_prefix = b'<rfc number="100"><middle/>'
    xml_suffix = b"</rfc>"
    (source / "https/rfc100.xml").write_bytes(
        xml_prefix + b" " * (8 * 1024**2 - len(xml_prefix) - len(xml_suffix)) + xml_suffix
    )
    project.start()
    project.migrate()
    project.run("up", "-d", "--wait", "fixtures", timeout=35)
    result = project.reconcile(settings=("RFC_SYNC_HTTP_CONCURRENCY=2", "RFC_SYNC_PARSE_CONCURRENCY=2"))
    assert result["verified_txt"] == result["contains_nul"] == 33
    assert result["rfcxml"]["captured"] == 2, result
    assert result["metrics"]["rsync"]["calls"] == 2
    assert_pipeline_bounds(result, http_limit=2)
    metrics = result["metrics"]
    assert metrics["rsync_peak_bytes"] == 128 * 1024**2, metrics
    assert 128 * 1024**2 <= metrics["scratch_peak_bytes"] < 144 * 1024**2, metrics
    assert 0 < metrics["memory_peak"] < 1536 * 1024**2, metrics
    project.verify_manifest(result)

    before = project.sql("SELECT count(*) FROM rfc.generations")
    command = project.reconcile_command(False, "full", prelude=
        "dd if=/dev/zero of=/tmp/occupied bs=1M count=140 2>/dev/null || exit; "
        "exec /phase2/fixture-driver full")
    phase1.failed(command, project.env, "ingestion scratch needs")
    assert project.sql("SELECT count(*) FROM rfc.generations") == before
    project.reconcile(expected="parse budget exceeded", settings=("RFC_PHASE2_PARSE_BUDGET=512",))
    assert project.sql("SELECT generation_id,publication_sequence FROM rfc.active_generation") == "|0"


def exercise_timeout(project):
    prepare(project, include_nul=True)
    project.start()
    project.migrate()
    project.run("up", "-d", "--wait", "fixtures", timeout=35)
    mode = project.source / "mode"
    for scenario, expected in (
        ("txt-html", "txt unexpected HTTPS media type"),
        ("txt-compressed", "unsupported HTTPS content encoding"),
        ("txt-redirect", "txt unexpected HTTPS status 302"),
    ):
        mode.write_text(scenario)
        project.reconcile(expected=expected)
    mode.write_text("slow-txt")
    project.reconcile(expected="txt HTTPS transport", settings=("RFC_PHASE2_HTTP_TIMEOUT_MS=500",))
    saved = int(project.sql("SELECT count(*) FROM rfc.ingestion_checkpoints WHERE kind='txt'"))
    mode.write_text("normal")
    result = project.reconcile()
    assert 0 <= saved < 6
    assert result["verified_txt"] == 6 and result["reused_txt"] == result["checkpoint_txt"] == saved, result
    assert result["metrics"]["txt"]["calls"] == 6 - saved, result
    project.verify_manifest(result)
    assert project.sql("SELECT generation_id,publication_sequence FROM rfc.active_generation") == "|0"



def fixture_driver(docker, prefix, target, *, prebuilt=False, publication=False):
    phase, purpose = (3, "publication") if publication else (2, "ingestion")
    image = f"{prefix}-{purpose}-fixture"
    executable = f"/rfc-phase{phase}-fixture"
    phase1.phase0.prepare_fixture_image(docker, REPO, f"phase{phase}-fixture", image, skip_build=prebuilt)
    container = phase1.capture([*docker, "create", *phase1.phase0.local_image_args(), image,
                                executable])
    try:
        run([*docker, "cp", f"{container}:{executable}", str(target)])
    finally:
        run([*docker, "rm", container], stdout=subprocess.DEVNULL)
    target.chmod(0o755)


def prepare_benchmark(project, size):
    sources = prepare(project, include_nul=True)
    catalog = project.source / "catalog.xml"
    entries = []
    for rfc in range(10000, 10000 + size - len(sources)):
        prefix = f"1. Synthetic RFC {rfc}\n\n".encode()
        payload = prefix + b"   Repeated text for indexing.\n" * (8192 if rfc % 9 == 0 else 128)
        for directory in ("https", "rsync"):
            (project.source / directory / f"rfc{rfc}.txt").write_bytes(payload)
        if rfc % 3 == 0:
            (project.source / "https" / f"rfc{rfc}.xml").write_bytes(
                f'<rfc number="{rfc}"><middle/></rfc>'.encode()
            )
        entries.append(catalog_entry(rfc, f"Benchmark {rfc}"))
    catalog.write_text(catalog.read_text().replace("</rfc-index>", "".join(entries) + "</rfc-index>"))
    (project.source / "mode").write_text("benchmark")
    return sources


@contextmanager
def capped_stage(project, *, mebibytes=64, inodes=512):
    image = project.directory / "rsync.ext4"
    mountpoint = project.directory / "rsync-mount"
    mountpoint.mkdir()
    with image.open("wb") as disk:
        disk.truncate(mebibytes * 1024**2)
    run(["mkfs.ext4", "-q", "-N", str(inodes), "-m", "0", str(image)])
    run(["sudo", "-n", "mount", "-o", "loop,nosuid,nodev,noexec", str(image), str(mountpoint)])
    try:
        maps = phase1.capture([
            *project.docker, "run", "--rm", *phase1.phase0.local_image_args(), "--network", "none",
            "--entrypoint", "/bin/sh", project.env["PHASE1_IMAGE_PREFIX"] + "-sync",
            "-c", "cat /proc/self/uid_map; echo :; cat /proc/self/gid_map",
        ])
        def host_id(mapping):
            for line in mapping.strip().splitlines():
                internal, external, count = map(int, line.split())
                if internal <= 10002 < internal + count:
                    return external + 10002 - internal
            raise AssertionError("Sync UID or GID is outside the Docker namespace mapping")
        uid, gid = map(host_id, maps.split(":"))
        run(["sudo", "-n", "chown", f"{uid}:{gid}", str(mountpoint)])
        run(["sudo", "-n", "chmod", "0700", str(mountpoint)])
        yield mountpoint
    finally:
        run(["sudo", "-n", "umount", str(mountpoint)])


def bulk_options(project, mountpoint, *, bootstrap=False, seconds=900):
    config = project.directory / "bulk.toml"
    config.write_text("schema_version = 1\n"
                      + ("[ingestion]\ntxt_provenance = 'rsync_bootstrap'\n" if bootstrap else "")
                      + "[ingestion.rsync]\nmode = 'persistent_bulk'\n"
                        f"bulk_transfer_timeout_seconds = {seconds}\n")
    return ("-v", f"{mountpoint}:/rfc-rsync-txt",
            "-v", f"{config}:/phase2/bulk.toml:ro")


def exercise_bulk(project):
    size = 36
    prepare_benchmark(project, size)
    with capped_stage(project) as mountpoint:
        project.start()
        project.migrate()
        project.run("up", "-d", "--wait", "fixtures", timeout=35)
        options = bulk_options(project, mountpoint)
        settings = ("RFC_SYNC_CONFIG_FILE=/phase2/bulk.toml", "RFC_PHASE2_FIXED_CLOCK=1")
        first = project.reconcile(options=options, settings=settings)
        metrics = first["metrics"]
        assert metrics["rsync"]["calls"] == 1 and metrics["rsync_transferred_files"] == size, first
        assert metrics["rsync_checksum_hits"] == 0 and metrics["https_requests"] == 2 * size + 2, first
        assert first["contains_nul"] == 1 and first["invalid_utf8"] == 1
        project.verify_manifest(first)
        warm = project.reconcile(options=options, settings=settings)
        metrics = warm["metrics"]
        assert metrics["rsync"]["calls"] == 1 and metrics["rsync_transferred_files"] == 0, warm
        assert metrics["rsync_checksum_hits"] == size and warm["reused_txt"] == size, warm
        assert first["manifest_digest"] == warm["manifest_digest"]

        source = project.source / "rsync/rfc10000.txt"
        payload = source.read_bytes()
        times = source.stat()
        edited = payload.replace(b"Synthetic", b"Different", 1)
        source.write_bytes(edited)
        os.utime(source, ns=(times.st_atime_ns, times.st_mtime_ns))
        https = project.source / "https/rfc10000.txt"
        https.write_bytes(edited)
        changed = project.reconcile(options=options, settings=settings)
        assert changed["metrics"]["rsync_transferred_files"] == 1 and changed["reused_txt"] == size - 1
        source.unlink()
        source.symlink_to("rfc10001.txt")
        project.reconcile(expected="bulk rsync", options=options, settings=settings)
        source.unlink()
        source.write_bytes(edited)
        recovered = project.reconcile(options=options, settings=settings)
        assert recovered["metrics"]["rsync_checksum_hits"] == size
        assert project.sql("SELECT generation_id,publication_sequence FROM rfc.active_generation") == "|0"
        assert project.sql("SELECT count(*) FROM rfc.notification_outbox") == "0"


def activate(project, generation, expected, *, rollback=False, error=None):
    operation = "--phase3-rollback" if rollback else "--phase3-publish"
    command = project.app("sync", "-T", args=(operation, str(generation), "--expect-sequence", str(expected)))
    result = subprocess.run(command, env=project.env, text=True, capture_output=True, timeout=90)
    if error:
        assert result.returncode != 0 and error in result.stderr, result
    else:
        assert result.returncode == 0, result
        assert json.loads(result.stdout)["publication_sequence"] == str(expected + 1)


def exercise_bootstrap(project):
    sources = prepare(project, include_nul=True)
    count = len(sources)
    with capped_stage(project) as mountpoint:
        settings = ("RFC_SYNC_CONFIG_FILE=/phase2/bulk.toml", "RFC_PHASE2_FIXED_CLOCK=1")
        options = bulk_options(project, mountpoint, bootstrap=True)
        check = project.app("sync", *options, "-e", settings[0], args=("--check-config",))
        config = json.loads(phase1.capture(check, env=project.env))
        assert config["resources"]["bulk_filesystem"]["inodes"] <= 512
        project.start()
        project.migrate()
        project.run("up", "-d", "--wait", "fixtures", timeout=35)
        (project.source / "rsync/rfc20000.txt").write_bytes(b"Not in the selected catalog.\n")

        def shell(command):
            return phase1.capture(project.app("sync", options[0], options[1], "--entrypoint", "/bin/sh",
                                              args=("-ec", command)), env=project.env)

        first = exercise_xml_overlap(project, options=options, settings=settings)
        assert first["verified_txt"] == 0 and first["rsync_only_txt"] == count
        assert first["metrics"]["txt"]["calls"] == 0 and first["metrics"]["https_requests"] == count + 2
        first_id = int(first["generation_id"])
        verify_ranges(project, first, sources, "unverified")
        project.verify_manifest(first)
        assert project.sql("SELECT count(*) FROM rfc.generation_documents WHERE rfc_id=20000") == "0"
        shell("test ! -e /rfc-rsync-txt/full/files/rfc20000.txt; "
              "find /rfc-rsync-txt/full/files -type f -exec rm -- {} +")
        replay = project.reconcile(options=options, settings=settings)
        assert replay["metrics"]["rsync_transferred_files"] == count
        assert replay["manifest_digest"] == first["manifest_digest"]
        project.verify_manifest(replay)

        def provenance(generation):
            return project.sql(
                "SELECT o.source_transport, o.https_verification, o.fetched_at IS NULL, "
                "o.source_url = 'rsync://rsync.rfc-editor.org/rfcs-text-only/rfc' || d.rfc_id || '.txt' "
                "FROM rfc.generation_documents d JOIN rfc.source_observations o ON o.identity = d.txt_observation "
                f"WHERE d.generation_id = {generation} ORDER BY d.rfc_id"
            ).splitlines()
        assert provenance(first_id) == ["rsync|unverified|f|t"] * count
        stale = int(project.reconcile(options=options, settings=settings)["generation_id"])
        assert provenance(stale) == ["rsync|unverified|t|t"] * count
        assert project.sql("SELECT generation_id,publication_sequence FROM rfc.active_generation") == "|0"
        activate(project, first_id, 0)
        activate(project, stale, 1, error="rsync-only TXT observation outside active membership")
        active = f"{first_id}|1"
        assert project.sql("SELECT generation_id,publication_sequence FROM rfc.active_generation") == active
        assert project.sql(f"SELECT state FROM rfc.generations WHERE id={stale}") == "ready"
        exercise_xml_cancellation(project, options=options, settings=settings, active=active)
        project.reconcile(expected="fixture RFCXML parser panic", options=options,
                          settings=(*settings, "RFC_PHASE2_PANIC_XML=9110"))
        assert project.sql("SELECT generation_id,publication_sequence FROM rfc.active_generation") == active
        assert project.sql("SELECT count(*) FROM rfc.notification_outbox") == "1"

        rsync = project.source / "rsync/rfc100.txt"
        https = project.source / "https/rfc100.txt"
        original = rsync.read_bytes()
        rsync.unlink()
        project.reconcile(expected="bulk rsync failed", options=options, settings=settings)
        rsync.write_bytes(b"x" * (4 * 1024**2 + 1))
        project.reconcile(expected="bulk rsync", options=options, settings=settings)
        rsync.write_bytes(original)
        mode = project.source / "mode"
        mode.write_text("catalog-drift")
        project.reconcile(expected="catalog changed", options=options, settings=settings)
        exercise_cancellation(project, options=options, settings=settings, active=active)
        mode.write_text("hold-rsync-timeout")
        options = bulk_options(project, mountpoint, bootstrap=True, seconds=30)
        project.reconcile(expected="bulk rsync", options=options, settings=settings, timeout=50)
        mode.write_text("normal")

        changed = original + b"\nChanged after publication.\n"
        rsync.write_bytes(changed)
        project.reconcile(expected="rsync and HTTPS TXT disagree", options=options, settings=settings)
        https.write_bytes(changed)
        for fill in (
            "for i in $(seq 1 600); do touch /rfc-rsync-txt/quota/$i 2>/dev/null || exit 0; done; exit 1",
            "dd if=/dev/zero of=/rfc-rsync-txt/quota/full bs=1M 2>/dev/null && exit 1; "
            "dd if=/dev/zero of=/rfc-rsync-txt/quota/tail bs=1K 2>/dev/null && exit 1; exit 0",
        ):
            shell("mkdir /rfc-rsync-txt/quota; " + fill)
            # The prior mismatch populated the cache with changed bytes. Change
            # the source again so rsync must allocate a replacement in the cap.
            changed += b"\n"
            rsync.write_bytes(changed)
            https.write_bytes(changed)
            try:
                project.reconcile(expected="bulk rsync", options=options, settings=settings)
            finally:
                shell("rm -rf /rfc-rsync-txt/quota")
        assert project.sql("SELECT generation_id,publication_sequence FROM rfc.active_generation") == active
        assert project.sql("SELECT count(*) FROM rfc.notification_outbox") == "1"
        repaired = project.reconcile(options=options, settings=settings)
        assert repaired["metrics"]["txt"]["calls"] == 1 and repaired["reused_txt"] == count - 1
        assert repaired["verified_txt"] == 1 and repaired["rsync_only_txt"] == count - 1
        activate(project, repaired["generation_id"], 1)
        activate(project, first_id, 2, rollback=True)
        strict = project.reconcile(options=bulk_options(project, mountpoint), settings=settings)
        assert strict["verified_txt"] == count and strict["rsync_only_txt"] == 0
        sample = project.reconcile(scope="sample", options=bulk_options(project, mountpoint, bootstrap=True),
                                   settings=settings)
        assert sample["rsync_only_txt"] == 0 and sample["verified_txt"] == 3
        assert project.sql("SELECT generation_id,publication_sequence FROM rfc.active_generation") == f"{first_id}|3"
        active = f"{first_id}|3"
        for body, error in ((b"", "bulk rsync"), (b"%PDF-1.7\n", "input is not RFC TXT")):
            rsync.write_bytes(body)
            https.write_bytes(body)
            rejected = project.reconcile(expected=error, options=options, settings=settings)
            assert project.sql(f"SELECT count(*) FROM rfc.generation_documents WHERE generation_id="
                               f"{int(rejected['generation_id'])} AND rfc_id=100") == "0"
        rsync.write_bytes(bytes(range(256)) * 4096)
        mode.write_text("rsync-partial")
        try:
            project.reconcile(expected="bulk rsync", options=options, settings=settings)
            assert "fixture rsync truncated after 65536 bytes" in fixture_logs(project)
            shell("test -z \"$(ls -A /rfc-rsync-txt/full/temporary)\"")
        finally:
            mode.write_text("normal")
            rsync.write_bytes(changed)
            https.write_bytes(changed)
        shell("touch /rfc-rsync-txt/full/files/unexpected.txt")
        try:
            project.reconcile(expected="unexpected name in bulk rsync staging", options=options, settings=settings)
        finally:
            shell("rm /rfc-rsync-txt/full/files/unexpected.txt")
        assert project.sql("SELECT generation_id,publication_sequence FROM rfc.active_generation") == active
        assert project.sql("SELECT count(*) FROM rfc.notification_outbox") == "3"
        verify_ranges(project, first, sources, "unverified")
        print("Bulk bootstrap provenance, capped failures, promotion fencing and rollback passed", flush=True)


def migrate_to_historical_schema(project, old_schema):
    migrations = (REPO / "migrations/migrate.sql").read_text()
    boundary = f"SELECT NOT EXISTS (SELECT FROM rfc.schema_migrations WHERE version = {old_schema+1})"
    old_sql, _ = migrations.split(boundary, 1)
    run([*project.psql(), "-f", "/dev/stdin"], env=project.env,
        input=old_sql.replace("\\ir ", "\\i /migrations/") + "COMMIT;\n")
    assert project.sql("SELECT max(version) FROM rfc.schema_migrations") == str(old_schema)


def exercise_provenance_upgrade(project, old_prefix, old_driver, old_schema, reader):
    sources = prepare(project, include_nul=True)
    project.start()
    migrate_to_historical_schema(project, old_schema)
    project.run("up", "-d", "--wait", "fixtures", timeout=35)
    current = {key: project.env[key] for key in ("PHASE1_IMAGE_PREFIX", "PHASE2_DRIVER")}
    project.env.update(PHASE1_IMAGE_PREFIX=old_prefix, PHASE2_DRIVER=str(old_driver))
    try:
        first = project.reconcile()
        activate(project, first["generation_id"], 0)
        second = project.reconcile()
        activate(project, second["generation_id"], 1)
    finally:
        project.env.update(current)
    query = ("SELECT encode(identity, 'hex'), source_url, observed_at, fetched_at, https_verification "
             "FROM rfc.source_observations ORDER BY identity")
    before = project.sql(query)
    manifests = ("SELECT id,encode(manifest_bytes,'hex') FROM rfc.generations"
                 " WHERE state IN ('published','retained') ORDER BY id")
    before_manifests = project.sql(manifests)
    project.migrate()
    project.migrate()
    assert project.sql("SELECT max(version) FROM rfc.schema_migrations") == "9"
    assert project.sql(query) == before
    assert project.sql(manifests) == before_manifests
    assert project.sql("SELECT count(*) FROM rfc.source_observations WHERE source_transport <> 'https'") == "0"
    for state, selector, report in (("retained", first["generation_id"], first),
                                    ("published", "active", second)):
        assert project.sql(
            f"SELECT encode(manifest_digest,'hex') FROM rfc.generations WHERE state = '{state}'"
        ) == report["manifest_digest"]
        command = project.app("serve", "-T", "-v", f"{reader}:/phase2/upgrade-reader:ro",
                              "--entrypoint", "/phase2/upgrade-reader", args=("read", selector))
        guarded = json.loads(phase1.capture(command, env=project.env))
        assert guarded["generation"] == int(report["generation_id"])
        assert guarded["manifest"] == report["manifest_digest"] and bytes(guarded["body"]) == sources[101]
    assert project.sql(
        "SELECT count(*) FROM rfc.source_observations WHERE verification_method='legacy_https_verified'"
    ) == str(first["verified_txt"])
    verify_ranges(project, first, sources)
    next_candidate = project.reconcile()
    assert next_candidate["reused_txt"] == first["verified_txt"]
    assert next_candidate["txt_audit"]["reused_legacy"] == first["verified_txt"]
    assert next_candidate["metrics"]["txt"]["calls"] == 0
    project.verify_manifest(next_candidate)
    changed = sources[100] + b"\nPost-migration change.\n"
    (project.source / "rsync/rfc100.txt").write_bytes(changed)
    project.reconcile(expected="rsync and HTTPS TXT disagree")
    (project.source / "https/rfc100.txt").write_bytes(changed)
    refreshed = project.reconcile()
    assert refreshed["metrics"]["txt"]["calls"] == 1 and refreshed["txt_audit"]["matched"] == 1
    assert refreshed["txt_audit"]["reused_legacy"] == len(sources) - 1
    assert project.sql(f"""
        SELECT h.verification_method, r.verification_method, h.raw_digest=r.raw_digest
        FROM rfc.generation_documents d JOIN rfc.source_observations h ON h.identity=d.txt_observation
        JOIN rfc.source_observations r ON r.identity=h.comparison_rsync
        WHERE d.generation_id={int(refreshed['generation_id'])} AND d.rfc_id=100
    """) == "dual_source|rsync_integrity|t"
    project.verify_manifest(refreshed)
    assert project.sql("SELECT generation_id,publication_sequence FROM rfc.active_generation") == f"{second['generation_id']}|2"
    print(f"Schema {old_schema} retained and published data, source identities and strict reuse survived schema 9 upgrade", flush=True)


def benchmark_content(project, report):
    generation = int(report["generation_id"])
    members = f"SELECT * FROM rfc.generation_documents WHERE generation_id={generation}"
    originals = project.sql(f"""
        SELECT json_build_array(g.rfc_id, encode(d.txt_digest,'hex'), d.line_starts, d.encoding,
            g.title, g.current_status, g.publication_date, g.publication_month, g.stream, encode(x.raw_digest,'hex'))
        FROM ({members}) g JOIN rfc.document_versions d ON d.identity=g.document_version
        LEFT JOIN rfc.source_observations x ON x.identity=g.rfcxml_observation ORDER BY g.rfc_id
    """, phase1.SERVE)
    rsync = project.source / "rsync"
    assert len(originals.splitlines()) == report["expected_txt"] == len(list(rsync.glob("rfc*.txt")))
    for row in originals.splitlines():
        rfc, digest, *_ = json.loads(row)
        assert (project.directory / "mirror/blobs/blake3" / digest[:2] / digest).read_bytes() == (
            rsync / f"rfc{rfc}.txt"
        ).read_bytes(), rfc
    ranges = project.sql(f"""
        SELECT g.rfc_id, ranges.kind, ranges.start_byte, ranges.end_byte
        FROM ({members}) g JOIN (
            SELECT document_version, 'section' AS kind, start_byte, end_byte FROM rfc.sections
            UNION ALL SELECT document_version, 'heading', heading_start_byte, heading_end_byte FROM rfc.sections
            UNION ALL SELECT document_version, 'chunk', start_byte, end_byte FROM rfc.text_chunks
            UNION ALL SELECT document_version, 'notice', start_byte, end_byte FROM rfc.notice_ranges
            UNION ALL SELECT document_version, 'reference', start_byte, end_byte FROM rfc.relation_evidence
                WHERE start_byte IS NOT NULL
        ) ranges ON ranges.document_version=g.document_version ORDER BY 1,2,3,4
    """, phase1.SERVE)
    relations = project.sql(f"""
        SELECT e.source_rfc, e.kind, e.target_rfc, r.method, r.xml_locator, r.start_byte, r.end_byte
        FROM rfc.edge_evidence e JOIN rfc.relation_evidence r ON r.identity=e.evidence
        WHERE e.generation_id={generation} ORDER BY 1,2,3,4,5,6,7
    """, phase1.SERVE)
    coverage = project.sql(f"""
        SELECT scope,rfc_id,family,state FROM rfc.relation_coverage
        WHERE generation_id={generation} ORDER BY 1,2,3,4
    """, phase1.SERVE)
    return {name: hashlib.sha256(value.encode()).hexdigest() for name, value in (
        ("originals_and_membership", originals), ("ranges", ranges), ("relations", relations), ("coverage", coverage),
    )}


def benchmark_comparison(args, docker, work, driver):
    serial = work / "serial-driver"
    fixture_driver(docker, args.serial_image_prefix, serial, prebuilt=True)
    profiles = (("serial", None, None), ("single", 1, 1), ("pipeline", 3, 2))
    settings = ("RFC_PHASE2_FIXED_CLOCK=1", "RFC_PHASE2_PACED=1")
    revision = phase1.capture(["git", "rev-parse", "HEAD"], cwd=REPO)
    images = {}
    for name, prefix in (("serial", args.serial_image_prefix), ("current", args.image_prefix)):
        tags = [f"{prefix}-{kind}" for kind in ("sync", "ingestion-fixture")]
        images[name] = dict(zip(tags, phase1.capture([
            *docker, "image", "inspect", "--format", "{{.Id}}", *tags,
        ]).splitlines(), strict=True))
    runs = []
    for repeat in range(1, args.repeats + 1):
        for name, slots, workers in profiles:
            with ExitStack() as cleanup:
                project = Project(work / f"{work.name}-{name}-{repeat}", driver, args.image_prefix, docker, False)
                cleanup.callback(project.close)
                prepare_benchmark(project, 96)
                project.start()
                if name == "serial":
                    migrate_to_historical_schema(project, 6)
                else:
                    project.migrate()
                project.run("up", "-d", "--wait", "fixtures", timeout=35)
                if name == "serial":
                    previous = project.env.copy()
                    project.env.update(PHASE1_IMAGE_PREFIX=args.serial_image_prefix, PHASE2_DRIVER=str(serial))
                    try:
                        report = project.reconcile(settings=(*settings, "RFC_SYNC_HTTP_CONCURRENCY=1"), timeout=480)
                        # Run the historical verifier with the read-only role against schema 6.
                        project.env["PHASE1_IMAGE_PREFIX"] = args.image_prefix
                        project.verify_manifest(report)
                    finally:
                        project.env = previous
                else:
                    config = project.directory / "benchmark.toml"
                    config.write_text(f"schema_version = 1\n[ingestion.https]\nmax_in_flight_requests = {workers}\n"
                                      f"[ingestion.pipeline]\nmax_pending_documents = {slots}\nparser_workers = {workers}\n")
                    report = project.reconcile(settings=(*settings, "RFC_SYNC_CONFIG_FILE=/phase2/benchmark.toml"),
                                               options=("-v", f"{config}:/phase2/benchmark.toml:ro"), timeout=480)
                    project.verify_manifest(report)
                    assert_pipeline_bounds(report, http_limit=workers, paced=True)
                    assert report["metrics"]["parse_peak"] <= workers
                    assert report["metrics"]["xml_window_peak"] <= slots
                assert report["verified_txt"] == 96 and report["reused_txt"] == 0
                assert report["metrics"]["https_requests"] == 194
                assert report["metrics"]["minimum_start_spacing_us"] >= 1_000_000
                generation = int(report["generation_id"])
                manifest = project.sql(f"SELECT encode(manifest_bytes,'hex') FROM rfc.generations WHERE id={generation}")
                content = benchmark_content(project, report)
                pgdata = project.capture("exec", "-T", "postgres", "du", "-sk", "/var/lib/postgresql")
                mirror = phase1.capture(["sudo", "-n", "du", "-sk", "--", str(project.directory / "mirror")])
                runs.append({"profile": name, "repeat": repeat, "content": content,
                             "manifest_sha256": hashlib.sha256(bytes.fromhex(manifest)).hexdigest(),
                             "storage_bytes": {"pgdata": int(pgdata.split()[0]) * 1024,
                                               "mirror": int(mirror.split()[0]) * 1024}, **report})
                receipt = {"revision": revision, "images": images, "paced": True, "runs": runs}
                args.receipt.write_text(json.dumps(receipt, indent=2) + "\n")
    assert all(run["content"] == runs[0]["content"] for run in runs), [
        (run["profile"], run["repeat"], run["content"]) for run in runs
    ]
    assert len({run["manifest_sha256"] for run in runs if run["profile"] != "serial"}) == 1
    receipt["median_wall_us"] = {name: median(run["metrics"]["wall_us"] for run in runs if run["profile"] == name)
                                 for name, _, _ in profiles}
    receipt["comparison_passed"] = True
    args.receipt.write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt["median_wall_us"], sort_keys=True), flush=True)


def benchmark(args, docker, work, driver):
    settings = ["RFC_PHASE2_FIXED_CLOCK=1", f"RFC_SYNC_HTTP_CONCURRENCY={args.http_concurrency}"]
    if not args.unpaced:
        settings.append("RFC_PHASE2_PACED=1")
    runs = []
    for size in args.benchmark:
        for repeat in range(args.repeats):
            with ExitStack() as cleanup:
                project = Project(work / f"{work.name}-{size}-{repeat}", driver, args.image_prefix, docker, False)
                cleanup.callback(project.close)
                sources = prepare_benchmark(project, size)
                project.start()
                project.migrate()
                project.run("up", "-d", "--wait", "fixtures", timeout=35)
                report = project.reconcile(settings=settings, timeout=max(180, 5 * size))
                assert report["verified_txt"] == size and report["reused_txt"] == 0
                assert report["metrics"]["https_requests"] == 2 * size + 2
                project.verify_manifest(report)
                if size == len(sources):
                    verify_ranges(project, report, sources)
                    report["manifest_hex"] = project.sql(
                        "SELECT encode(manifest_bytes, 'hex') FROM rfc.generations"
                        f" WHERE id={int(report['generation_id'])}"
                    )
                runs.append({"size": size, "repeat": repeat, **report})
    receipt = {"image_prefix": args.image_prefix, "paced": not args.unpaced,
               "configured_https_concurrency": args.http_concurrency, "runs": runs,
               "median_wall_us": {size: median(run["metrics"]["wall_us"] for run in runs if run["size"] == size)
                                  for size in args.benchmark}}
    if args.reference_receipt:
        reference = json.loads(args.reference_receipt.read_text())
        expected = {(item["size"], item["repeat"]): item for item in reference["runs"]}
        for item in runs:
            original = expected[item["size"], item["repeat"]]
            for key in ("manifest_digest", "catalog_digest", "verified_txt", "accepted_bytes"):
                assert item[key] == original[key], (key, item, original)
            if "manifest_hex" in original:
                assert item["manifest_hex"] == original["manifest_hex"]
            assert item["metrics"]["https_requests"] == original["metrics"]["https_requests"]
    if args.receipt:
        args.receipt.write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, sort_keys=True), flush=True)



def main():
    if not __debug__:
        raise RuntimeError("Phase 2 requires enabled assertions")
    parser = argparse.ArgumentParser()
    parser.add_argument("--context", default="rootless")
    parser.add_argument("--image-prefix", default="rfc-phase2-regression")
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--bulk", action="store_true", help="exercise persistent rsync with a disposable ext4 mount")
    parser.add_argument("--bulk-bootstrap", action="store_true", help="exercise bulk trust, promotion and capped faults")
    parser.add_argument("--upgrade-from", metavar="SCHEMA6_IMAGE_PREFIX",
                        help="upgrade a published fixture built by preloaded schema 6 images")
    parser.add_argument("--upgrade-from-schema7", metavar="SCHEMA7_IMAGE_PREFIX",
                        help="upgrade a published fixture built by preloaded schema 7 images")
    parser.add_argument("--benchmark", type=int, choices=(6, 96), nargs="+")
    parser.add_argument("--repeats", type=int, choices=range(1, 6), default=3)
    parser.add_argument("--http-concurrency", type=int, choices=(1, 2), default=1)
    parser.add_argument("--unpaced", action="store_true")
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--reference-receipt", type=Path)
    parser.add_argument("--serial-image-prefix", help="compare the historical schema 6 serial collector at 96 RFCs")
    args = parser.parse_args()
    if sum(map(bool, (args.live, args.bulk, args.bulk_bootstrap, args.benchmark,
                      args.upgrade_from, args.upgrade_from_schema7))) > 1:
        parser.error("select only one live, bulk, bulk-bootstrap, benchmark or upgrade profile")
    if args.serial_image_prefix and (args.benchmark != [96] or args.unpaced or args.reference_receipt
                                     or args.receipt is None or Path("/tmp") not in args.receipt.resolve().parents):
        parser.error("--serial-image-prefix requires paced --benchmark 96 and a --receipt under /tmp")
    docker = ["docker", "--context", args.context]
    with tempfile.TemporaryDirectory(prefix="rfc-phase2-", dir="/tmp") as root:
        work = Path(root)
        driver = work / "fixture-driver"
        if args.live:
            driver.touch()
        else:
            fixture_driver(docker, args.image_prefix, driver)
        if args.benchmark:
            if args.serial_image_prefix:
                benchmark_comparison(args, docker, work, driver)
            else:
                benchmark(args, docker, work, driver)
            return 0
        project = Project(work / f"{work.name}-candidate", driver, args.image_prefix, docker, args.live)
        try:
            if args.live:
                project.start()
                project.migrate()
                code, report = project.reconcile(scope="sample", live=True)
                assert project.sql("SELECT generation_id,publication_sequence FROM rfc.active_generation") == "|0"
                if code != 0:
                    return 1
                assert report["verified_txt"] == 3
                assert re.fullmatch("[0-9a-f]{64}", report["manifest_digest"])
                observations = json.loads(project.sql("""
                    SELECT json_agg(json_build_object(
                        'rfc', d.rfc_id, 'url', o.source_url, 'fetched_at', o.fetched_at,
                        'digest', encode(o.raw_digest,'hex'), 'length', v.byte_length,
                        'notice_policy', v.notice_policy, 'section_state', v.section_state,
                        'https_verification', o.https_verification
                    ) ORDER BY d.rfc_id)
                    FROM rfc.generation_documents d
                    JOIN rfc.source_observations o ON o.identity=d.txt_observation
                    JOIN rfc.document_versions v ON v.identity=d.document_version
                """))
                assert [entry["rfc"] for entry in observations] == [2119, 8174, 9110]
                assert all(entry["https_verification"] == "verified" for entry in observations)
                print(json.dumps({"txt_observations": observations}, sort_keys=True), flush=True)
            elif args.bulk:
                exercise_bulk(project)
            elif args.bulk_bootstrap:
                exercise_bootstrap(project)
            elif args.upgrade_from or args.upgrade_from_schema7:
                old_prefix = args.upgrade_from_schema7 or args.upgrade_from
                old_schema = 7 if args.upgrade_from_schema7 else 6
                old_driver = work / "previous-fixture-driver"
                fixture_driver(docker, old_prefix, old_driver, prebuilt=True)
                reader = work / "upgrade-reader"
                fixture_driver(docker, args.image_prefix, reader, publication=True)
                exercise_provenance_upgrade(project, old_prefix, old_driver, old_schema, reader)
            else:
                exercise(project)
        finally:
            project.close()
        if args.live or args.bulk or args.bulk_bootstrap or args.upgrade_from or args.upgrade_from_schema7:
            return 0
        for name, exercise_case in (("resources", exercise_resources), ("timeout", exercise_timeout)):
            with ExitStack() as cleanup:
                case = Project(work / f"{work.name}-{name}", driver, args.image_prefix, docker, False)
                cleanup.callback(case.close)
                exercise_case(case)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
