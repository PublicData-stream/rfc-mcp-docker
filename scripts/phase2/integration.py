#!/usr/bin/env python3
"""Exercise source verification and candidate indexing in disposable Compose projects."""

import argparse
import importlib.util
import json
from pathlib import Path
import re
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

    def reconcile_command(self, live, scope):
        if live:
            return self.app("sync", args=("--phase2-reconcile", f"--{scope}"))
        return self.app("sync", "--entrypoint", "/phase2/fixture-driver", args=(scope,))

    def reconcile(self, scope="full", expected=None, live=False):
        result = subprocess.run(self.reconcile_command(live, scope), env=self.env,
                                text=True, capture_output=True, timeout=180)
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


def prepare(project):
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
    assert project.sql("SELECT version FROM rfc.schema_migrations ORDER BY version") == "1\n2\n3\n4\n5\n6"


def verify_ranges(project, report, sources):
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
        assert (project.directory / "mirror/blobs/blake3" / digest[:2] / digest).read_bytes() == raw
        assert line_starts == [0, *(i + 1 for i, byte in enumerate(raw[:-1]) if byte == 10)]
        assert encoding == ("bytes" if rfc == 101 else "utf8")
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
        assert bytes(recovered) == (b"" if rfc == 101 else raw)
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


def exercise(project):
    sources = prepare(project)
    project.start()
    migrate_v1_then_v2(project)
    project.check("sync")
    project.check("serve")
    project.run("up", "-d", "--wait", "fixtures", timeout=35)
    first = project.reconcile()
    assert first["verified_txt"] == len(sources) and first["reused_txt"] == 0
    verify_ranges(project, first, sources)
    project.verify_manifest(first)
    catalog = project.source / "catalog.xml"
    catalog.write_text(catalog.read_text().replace("Synthetic XML classification", "Updated catalog metadata"))
    second = project.reconcile()
    assert second["reused_txt"] == len(sources)
    assert project.sql("SELECT count(*) FROM rfc.document_versions") == str(len(sources))
    assert project.sql(
        f"SELECT title FROM rfc.generation_documents WHERE generation_id={int(second['generation_id'])} AND rfc_id=9110"
    ) == "Updated catalog metadata"
    assert second["manifest_digest"] != first["manifest_digest"]
    project.verify_manifest(second)
    sample = project.reconcile(scope="sample")
    assert sample["verified_txt"] == 3 and sample["scope"] == "sample"
    project.verify_manifest(sample)

    unchanged_catalog = catalog.read_text()
    catalog.write_text(unchanged_catalog.replace(catalog_entry(101, "Legacy bytes", day="<day>23</day>"), ""))
    project.reconcile(expected="catalog lost previously accepted RFC 101")
    catalog.write_text(unchanged_catalog)

    mode = project.source / "mode"
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
    mode.write_text("bad-rfcxml")
    fallback = project.reconcile()
    assert any("RFC 8174 RFCXML parse" in error for error in fallback["diagnostics"])
    assert project.sql(
        f"SELECT state FROM rfc.relation_coverage WHERE generation_id={int(fallback['generation_id'])}"
        " AND rfc_id=8174 AND family='all_references'"
    ) == "partial"
    mode.write_text("normal")
    original = project.source / "rsync/rfc100.txt"
    original.unlink()
    project.reconcile(expected="missing expected TXT rfc100.txt")
    original.symlink_to("../https/rfc100.txt")
    project.reconcile(expected="missing expected TXT")
    original.unlink()
    original.write_bytes(b"Different same-ID candidate.\n")
    project.reconcile(expected="rsync and HTTPS TXT disagree")
    quarantine = project.app("sync", "--entrypoint", "/bin/sh", args=(
        "-ec", "test \"$(find /mirror/quarantine -name https-body | wc -l)\" -eq 1"
    ))
    run(quarantine, env=project.env)
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
    print("Phase 2 fixture candidate, byte indexes, provenance, failure fences and role boundaries passed", flush=True)


def fixture_driver(docker, prefix, target):
    image = f"{prefix}-ingestion-fixture"
    run([*docker, "build", "--target", "phase2-fixture", "-t", image, str(REPO)], timeout=600)
    container = phase1.capture([*docker, "create", image, "/rfc-phase2-fixture"])
    try:
        run([*docker, "cp", f"{container}:/rfc-phase2-fixture", str(target)])
    finally:
        run([*docker, "rm", container], stdout=subprocess.DEVNULL)
    target.chmod(0o755)


def main():
    if not __debug__:
        raise RuntimeError("Phase 2 requires enabled assertions")
    parser = argparse.ArgumentParser()
    parser.add_argument("--context", default="rootless")
    parser.add_argument("--image-prefix", default="rfc-phase2-regression")
    parser.add_argument("--live", action="store_true")
    args = parser.parse_args()
    docker = ["docker", "--context", args.context]
    with tempfile.TemporaryDirectory(prefix="rfc-phase2-", dir="/tmp") as root:
        work = Path(root)
        driver = work / "fixture-driver"
        if args.live:
            driver.touch()
        else:
            fixture_driver(docker, args.image_prefix, driver)
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
            else:
                exercise(project)
        finally:
            project.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
