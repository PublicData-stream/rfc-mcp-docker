#!/usr/bin/env python3
"""Exercise source verification and candidate indexing in disposable Compose projects."""

import argparse
from contextlib import ExitStack
import importlib.util
import json
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

    def reconcile_command(self, live, scope, settings=()):
        if live:
            return self.app("sync", args=("--phase2-reconcile", f"--{scope}"))
        options = [part for setting in settings for part in ("-e", setting)]
        return self.app("sync", *options, "--entrypoint", "/phase2/fixture-driver", args=(scope,))

    def reconcile(self, scope="full", expected=None, live=False, settings=(), timeout=180):
        result = subprocess.run(self.reconcile_command(live, scope, settings), env=self.env,
                                text=True, capture_output=True, timeout=timeout)
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
            verify_nul_document(project, generation, version, digest, raw)
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


def verify_nul_document(project, generation, version, digest, raw):
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
    ) == "NUL bytes with RFCXML|verified|t|rfcxml|verified"
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
    first = project.reconcile()
    assert first["verified_txt"] == len(sources) and first["reused_txt"] == 0
    assert first["invalid_utf8"] == first["contains_nul"] == 1
    verify_ranges(project, first, sources)
    project.verify_manifest(first)
    catalog = project.source / "catalog.xml"
    catalog.write_text(catalog.read_text().replace("Synthetic XML classification", "Updated catalog metadata"))
    second = project.reconcile()
    assert second["reused_txt"] == len(sources)
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
    original.write_bytes(b"Different same-ID candidate.\x00\n")
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
    phase1.phase0.prepare_fixture_image(docker, REPO, "phase2-fixture", image)
    container = phase1.capture([*docker, "create", *phase1.phase0.local_image_args(), image,
                                "/rfc-phase2-fixture"])
    try:
        run([*docker, "cp", f"{container}:/rfc-phase2-fixture", str(target)])
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
                prepare_benchmark(project, size)
                project.start()
                project.migrate()
                project.run("up", "-d", "--wait", "fixtures", timeout=35)
                report = project.reconcile(settings=settings, timeout=max(180, 5 * size))
                assert report["verified_txt"] == size and report["reused_txt"] == 0
                assert report["metrics"]["https_requests"] == 2 * size + 2
                project.verify_manifest(report)
                runs.append({"size": size, "repeat": repeat, **report})
    receipt = {"image_prefix": args.image_prefix, "paced": not args.unpaced,
               "configured_https_concurrency": args.http_concurrency, "runs": runs,
               "median_wall_us": {size: median(run["metrics"]["wall_us"] for run in runs if run["size"] == size)
                                  for size in args.benchmark}}
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
    parser.add_argument("--benchmark", type=int, choices=(6, 96), nargs="+")
    parser.add_argument("--repeats", type=int, choices=range(1, 6), default=3)
    parser.add_argument("--http-concurrency", type=int, choices=(1, 2), default=1)
    parser.add_argument("--unpaced", action="store_true")
    parser.add_argument("--receipt", type=Path)
    args = parser.parse_args()
    if args.live and args.benchmark:
        parser.error("the benchmark requires private fixtures")
    docker = ["docker", "--context", args.context]
    with tempfile.TemporaryDirectory(prefix="rfc-phase2-", dir="/tmp") as root:
        work = Path(root)
        driver = work / "fixture-driver"
        if args.live:
            driver.touch()
        else:
            fixture_driver(docker, args.image_prefix, driver)
        if args.benchmark:
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
            else:
                exercise(project)
        finally:
            project.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
