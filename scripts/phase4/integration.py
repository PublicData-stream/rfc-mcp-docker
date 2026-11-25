#!/usr/bin/env python3
"""Verify generation-pinned search on the Phase 0 PostgreSQL image."""

import argparse
import base64
from contextlib import contextmanager
import hashlib
import hmac
import importlib.util
import json
from pathlib import Path
import random
import secrets
import select
import statistics
import subprocess
import sys
import tempfile
import time

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("phase3", HERE.parent / "phase3" / "integration.py")
phase3 = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(phase3)
phase2, phase1, run = phase3.phase2, phase3.phase1, phase3.run
NOTICE = b"Original synthetic copyright and authorship notice.\r\n"
CONTENT = NOTICE + "Élan TLS DTLS TLS-alpha\r\nuncut %_\\ path\x0c\n".encode()


def search_driver(docker, prefix, target, skip_build):
    image = f"{prefix}-search-fixture"
    phase1.phase0.prepare_fixture_image(docker, phase2.REPO, "phase4-fixture", image, skip_build=skip_build)
    container = phase1.capture([*docker, "create", *phase1.phase0.local_image_args(), image,
                                "/rfc-phase4-fixture"])
    try:
        run([*docker, "cp", f"{container}:/rfc-phase4-fixture", str(target)])
    finally:
        run([*docker, "rm", container], stdout=subprocess.DEVNULL)
    target.chmod(0o755)
    print("Phase 4 fixture image " + phase1.capture(
        [*docker, "image", "inspect", "--format", "{{.Id}}", image]), flush=True)


class Project(phase3.Project):
    def search_command(self, service, *args):
        options = ("-T", "-v", f"{self.search_driver}:/phase4/fixture-driver:ro",
                   "--entrypoint", "/phase4/fixture-driver")
        if service == "serve":
            options += ("-v", f"{self.key_file}:/phase4/cursor-key:ro")
        return self.app(service, *options, args=args)


@contextmanager
def search_session(project):
    with subprocess.Popen(project.search_command("serve"), env=project.env, text=True,
                          stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as proc:
        def request(argument):
            proc.stdin.write(json.dumps(argument) + "\n")
            proc.stdin.flush()
            assert select.select([proc.stdout], [], [], 24)[0], argument
            line = proc.stdout.readline()
            assert line, "search fixture exited before writing a response"
            return json.loads(line)

        try:
            yield request
        finally:
            proc.stdin.close()
            proc.stdin = None
            _, errors = proc.communicate(timeout=27)
            if proc.returncode:
                print(errors, file=sys.stderr)
            assert proc.returncode == 0


def page(request, query, **options):
    reply = request(dict(query=query, **options))
    assert "result" in reply, (query, reply)
    return reply["result"]


def hit_ids(result):
    return [hit["id"] for hit in result["hits"]]


def expect_error(request, query, code, **options):
    reply = request(dict(query=query, **options))
    assert reply.get("error") == code, (query, reply)
    return reply


def add_source(project):
    for path in ("https", "rsync"):
        (project.source / path / "rfc102.txt").write_bytes(CONTENT)
        (project.source / path / "rfc105.txt").write_bytes(b"large fixture\n" + b"z" * 196608 + b"\n")
    catalog = project.source / "catalog.xml"
    entry = phase2.catalog_entry(102, "Élan DTLS Catalog",
                                 "<updates><doc-id>RFC100</doc-id></updates>"
                                 "<obsoletes><doc-id>RFC101</doc-id></obsoletes>")
    entry += phase2.catalog_entry(105, "Large fixture")
    catalog.write_text(catalog.read_text().replace("</rfc-index>", entry + "</rfc-index>"))
    (project.source / "https/rfc102.xml").write_bytes(
        b'<rfc number="102"><back><references><name>Normative References</name>'
        b'<reference><seriesInfo name="RFC" value="8174"/></reference>'
        b'</references></back></rfc>')


def upgrade(project):
    migration = (phase2.REPO / "migrations" / "migrate.sql").read_text().split(
        "SELECT NOT EXISTS (SELECT FROM rfc.schema_migrations WHERE version = 4)")[0]
    for file in ("0001-storage.sql", "0002-ingestion.sql", "0003-publication.sql"):
        migration = migration.replace(r"\ir " + file, r"\i /migrations/" + file)
    run(project.psql(), env=project.env, input=migration + "COMMIT;\n", stdout=subprocess.DEVNULL)
    assert project.sql("SELECT version FROM rfc.schema_migrations ORDER BY version") == "1\n2\n3"
    legacy = project.sql("INSERT INTO rfc.generations (parser_version,index_version) "
                         "VALUES ('legacy','legacy') RETURNING id")
    project.sql("UPDATE rfc.generations SET state='ready',manifest_format=1,manifest_bytes=decode('0123','hex'),"
                f"manifest_digest=decode(repeat('ab',32),'hex') WHERE id={legacy}")
    row = f"SELECT to_jsonb(g) FROM rfc.generations g WHERE id={legacy}"
    before = project.sql(row)
    project.check("serve", expected="unsupported storage schema version")
    project.migrate()
    project.migrate()
    assert project.sql("SELECT version FROM rfc.schema_migrations ORDER BY version") == "\n".join(map(str, range(1, 12)))
    assert project.sql(row) == before
    project.denied_sql("DELETE FROM rfc.generation_documents", phase1.SERVE)


def approve_synthetic_excerpt(project, generation):
    # Only the isolated, synthetic RFC 102 gets an excerpt policy in this fixture.
    version = f"(SELECT document_version FROM rfc.generation_documents WHERE generation_id={generation} AND rfc_id=102)"
    project.sql("BEGIN; ALTER TABLE rfc.document_versions DISABLE TRIGGER seal_document; "
                "ALTER TABLE rfc.notice_ranges DISABLE TRIGGER insert_notice; "
                "UPDATE rfc.document_versions SET notice_policy='excerpt_allowed',notice_state='complete' "
                f"WHERE identity={version}; "
                f"DELETE FROM rfc.notice_ranges WHERE document_version={version}; "
                "INSERT INTO rfc.notice_ranges SELECT identity,byte_length,0,'copyright',0,"
                f"{len(NOTICE)},'complete','reviewed' FROM rfc.document_versions WHERE identity={version}; "
                "ALTER TABLE rfc.notice_ranges ENABLE TRIGGER insert_notice; "
                "ALTER TABLE rfc.document_versions ENABLE TRIGGER seal_document; COMMIT")


def check_text_and_snippets(request):
    for query, expected in [
        ('in::title:"TLS"', [102]),
        ('in::title:"tls"', []),
        ("in::title:'tls'", []),
        ("in::title:'ÉLAN dtls'", [102]),
        ('in::fulltext:"%_"', [102]),
        ('in::fulltext:"\\\\"', [102]),
        ('in::fulltext:"DTLS TLS-alpha\\r\\nuncut"', [102]),
        ('in::fulltext:"boundarytoken lastterm"', [100]),
        ("in::fulltext:'lastterm firstterm'", [100]),
        ('in::fulltext:"firstterm lastterm"', []),
        ('in::fulltext:"Historical RFC bytes"', [101]),
        ('in::title:"%_\'; -- (SELECT)"', []),
    ]:
        assert hit_ids(page(request, query)) == expected, query
    report = page(request, 'in::title:"Legacy" AND !in::fulltext:\'absent\'')
    assert hit_ids(report) == [] and report["coverage"]["unknown"] == 1
    assert report["coverage"]["catalog_only"] == 1
    assert set(report["coverage"]["warnings"]) == {"incomplete_index", "incomplete_catalog"}
    expect_error(request, "in::title:'%_'", "invalid_query_syntax")
    expect_error(request, "in::title:'" + " ".join(f"word{n}" for n in range(65)) + "'", "query_too_complex")
    reply = expect_error(request, "in::title:'é\\q'", "invalid_query_syntax")
    assert reply["span"] == [13, 15]

    for query, fragments in [
        ('in::title:"Catalog" AND in::fulltext:"%_"', [b"%_"]),
        ('in::title:"Catalog" AND in::fulltext:\'élan tls\'', ["Élan".encode(), b"TLS", b"TLS"]),
    ]:
        result = page(request, query, include_snippets=True)
        assert hit_ids(result) == [102]
        excerpts = result["hits"][0]["excerpts"]
        for item in excerpts:
            start, end = item["start_byte"], item["end_byte"]
            assert item["content"].encode() == CONTENT[start:end]
            assert item["match_range"] == [start, end]
        assert [item["content"].encode() for item in excerpts] == fragments, excerpts
        groups = result["notice_groups"]
        assert len(groups) == 1 and groups[0]["id"] == excerpts[0]["notice_group_id"]
        assert groups[0]["notices"] == [{"start_byte": 0, "end_byte": len(NOTICE), "content": NOTICE.decode()}]
        assert groups[0]["source_url"].endswith("/rfc102.txt") and groups[0]["source_name"] == "RFC Editor"
    result = page(request, 'in::fulltext:"boundarytoken"', include_snippets=True)
    assert hit_ids(result) == [100] and result["notice_groups"] == []
    assert result["coverage"]["snippets_omitted"] == 1
    result = page(request, 'in::title:"Catalog" AND !in::fulltext:"absent"', include_snippets=True)
    assert hit_ids(result) == [102] and result["notice_groups"] == []


def check_text_search_limits(project):
    result = project.sql("""
        SELECT count(*) FILTER (WHERE lexeme='tailword' AND positions=ARRAY[16383]::smallint[])
            || ':' || max(cardinality(positions)) FILTER (WHERE lexeme='word')
        FROM unnest(to_tsvector('pg_catalog.simple', repeat('word ',17000) || 'tailword'))
    """, phase1.SERVE)
    assert result == "1:255", result
    assert project.sql("SELECT length(to_tsvector('pg_catalog.simple', repeat('z',2048)))",
                       phase1.SERVE) == "0"


def check_corrupt_original(project, request, generation):
    digest = project.sql(f"SELECT encode(d.txt_digest,'hex') FROM rfc.generation_documents m "
                         "JOIN rfc.document_versions d ON d.identity=m.document_version "
                         f"WHERE m.generation_id={generation} AND m.rfc_id=102")
    path = f"/mirror/blobs/blake3/{digest[:2]}/{digest}"
    flip = ('chmod 0644 "$1"; printf "%s" "$2" | dd of="$1" bs=1 count=1 conv=notrunc 2>/dev/null; '
            'chmod 0444 "$1"')
    query = 'in::title:"Catalog" AND !in::fulltext:"never found"'
    project.run("run", "--rm", "--no-deps", "volume-admin", flip, "--", path, "X")
    try:
        result = page(request, query)
        assert result["hits"] == [] and result["coverage"]["unknown"] == 1
    finally:
        project.run("run", "--rm", "--no-deps", "volume-admin", flip, "--", path, "O")
    assert hit_ids(page(request, query)) == [102]


def check_pagination(project, request, first):
    query = 'in::title:"" OR is::obsoleted:true'
    expect_error(request, query, "invalid_query_syntax")
    query = 'is::obsoleted:true OR !is::obsoleted:true'
    result = page(request, query, limit=1)
    assert hit_ids(result) == [100] and result["generation_id"] == str(first)
    cursor = result["next_cursor"]
    expect_error(request, query, "cursor_invalid", limit=2, cursor=cursor)
    expect_error(request, '!is::obsoleted:true', "cursor_invalid", limit=1, cursor=cursor)
    expect_error(request, query, "cursor_invalid", limit=1, cursor=("A" if cursor[0] != "A" else "B") + cursor[1:])
    decoded = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4))
    payload = json.loads(decoded[:-48])
    payload["expiry"] = 1
    raw = json.dumps(payload).encode()
    tag = hmac.digest(project.key_file.read_bytes(), raw, hashlib.sha384)
    expired = base64.urlsafe_b64encode(raw + tag).decode().rstrip("=")
    expect_error(request, query, "cursor_expired", limit=1, cursor=expired)

    phase3.changed_catalog(project, "new generation title")
    second = int(project.reconcile()["generation_id"])
    project.activate(second, 1)
    continuation = page(request, query, limit=1, cursor=cursor)
    assert (continuation["generation_id"], continuation["publication_sequence"], hit_ids(continuation)) == (str(first), "1", [101])
    all_ids = [100, 101]
    while continuation["next_cursor"]:
        continuation = page(request, query, limit=1, cursor=continuation["next_cursor"])
        assert continuation["publication_sequence"] == "1"
        all_ids.extend(hit_ids(continuation))
    assert all_ids == [100, 101, 102, 105, 2119, 8174, 9110]
    assert hit_ids(page(request, 'in::title:"new generation title"')) == [101]
    project.activate(first, 2, "rollback")
    assert page(request, query, limit=1)["publication_sequence"] == "3"
    assert page(request, query, limit=1, cursor=cursor)["publication_sequence"] == "1"
    project.activate(second, 3, "rollback")
    # Fast-forward only this private fixture through the lifecycle to its tombstone.
    project.sql("BEGIN; SET LOCAL ROLE rfc_owner; "
                f"UPDATE rfc.generations SET state='deleting' WHERE id={first}; "
                f"UPDATE rfc.generations SET state='deleted' WHERE id={first}; COMMIT")
    expect_error(request, query, "generation_unavailable", limit=1, cursor=cursor)
    return second


def check_timeout(project, request):
    with subprocess.Popen(project.psql(), env=project.env, text=True,
                          stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as lock:
        lock.stdin.write("BEGIN; LOCK rfc.rfc_relations IN ACCESS EXCLUSIVE MODE;\n\\echo locked\n")
        lock.stdin.flush()
        try:
            assert select.select([lock.stdout], [], [], 5)[0] and lock.stdout.readline().strip() == "locked"
            result = expect_error(request, "relation::updates:100", "deadline_exceeded")
            assert result["elapsed_us"] < 15_000_000, result
        finally:
            lock.communicate("ROLLBACK;\n", timeout=10)
    phase3.wait_for_sql(project, "SELECT count(*) FROM pg_stat_activity WHERE usename='rfc_serve'", "0")
    assert 102 in hit_ids(page(request, "relation::updates:100"))


KINDS = ("normative_reference", "informative_reference", "unclassified_reference", "updates", "obsoletes")
FAMILIES = ("all_references", "classified_references", "updates", "obsoletes")
GRAPH_IDS = tuple(range(10000, 10012))
OPERATORS = {
    "references": ("all_references", KINDS[:3]),
    "normative_reference": ("classified_references", KINDS[:1]),
    "informative_reference": ("classified_references", KINDS[1:2]),
    "updates": ("updates", ("updates",)),
    "obsoletes": ("obsoletes", ("obsoletes",)),
}
INVERSES = ("referenced_by", "normative_referenced_by", "informative_referenced_by", "updated_by", "obsoleted_by")


def seed(project, base, sequence, start, end, edges, coverage, bulk=False):
    generation = int(project.sql("INSERT INTO rfc.generations (parser_version,index_version) "
                                 f"SELECT parser_version,index_version FROM rfc.generations WHERE id={base} RETURNING id"))
    copy_fields = ("rfc_id,document_version,metadata_identity,catalog_observation,txt_observation,"
                   "title,stream,publication_date,current_status,publication_month,rfcxml_observation")
    template = 105 if bulk else 102
    seed_edges = (
        f"SELECT {generation}, n, 'obsoletes', 10000+mod(n,12) FROM generate_series({start},{end}) n"
        if bulk else "VALUES " + ",".join(f"({generation},{a},'{kind}',{b})" for a, kind, b in edges)
    )
    assignments = ("" if bulk else "VALUES " + ",".join(
        f"({rfc},'{kind}','{state}')" for (rfc, kind), state in coverage.items()))
    covered = ("CASE WHEN family='obsoletes' THEN 'complete' ELSE 'unavailable' END" if bulk
               else "coalesce(s.state, 'unavailable')")
    coverage_join = ("" if bulk else f"LEFT JOIN ({assignments}) s(rfc_id, kind, state) ON (s.rfc_id,s.kind)=(n,family)")
    project.sql(f"""
        BEGIN;
        INSERT INTO rfc.rfc_identities SELECT n FROM generate_series({start},{end}) n;
        INSERT INTO rfc.source_observations (identity,kind,source_url,observed_at,fetched_at,raw_digest,https_verification,capture_only)
          SELECT sha256(convert_to('phase4-txt-'||n,'UTF8')), 'txt',
                 'https://www.rfc-editor.org/rfc/rfc'||n||'.txt', now(), now(), d.txt_digest, 'verified', true
          FROM generate_series({start},{end}) n CROSS JOIN rfc.generation_documents m
          JOIN rfc.document_versions d ON d.identity=m.document_version
          WHERE m.generation_id={base} AND m.rfc_id={template};
        INSERT INTO rfc.document_versions (identity,rfc_id,txt_digest,byte_length,parser_version,index_version,encoding,
                 indexing_state,notice_state,notice_policy,index_identity,section_state,line_starts)
          SELECT sha256(convert_to('phase4-doc-'||n,'UTF8')), n, d.txt_digest, d.byte_length, d.parser_version,
                 d.index_version, d.encoding, d.indexing_state, 'unavailable', 'metadata_only', d.index_identity,
                 d.section_state, d.line_starts
          FROM generate_series({start},{end}) n CROSS JOIN rfc.generation_documents m
          JOIN rfc.document_versions d ON d.identity=m.document_version
          WHERE m.generation_id={base} AND m.rfc_id={template};
        INSERT INTO rfc.document_lexemes
          SELECT sha256(convert_to('phase4-doc-'||n,'UTF8')), l.lexeme
          FROM generate_series({start},{end}) n CROSS JOIN rfc.generation_documents m
          JOIN rfc.document_lexemes l ON l.document_version=m.document_version
          WHERE m.generation_id={base} AND m.rfc_id={template};
        UPDATE rfc.document_versions SET sealed=true WHERE rfc_id BETWEEN {start} AND {end};
        INSERT INTO rfc.generation_ingestion
          SELECT {generation},scope,catalog_before,catalog_after,policy_registry,catalog_only
          FROM rfc.generation_ingestion WHERE generation_id={base};
        INSERT INTO rfc.generation_audit
          SELECT {generation},sha256(convert_to('phase4-selection-{generation}','UTF8')),inventory,policy,
                 array(SELECT rfc FROM (SELECT rfc_id::integer rfc FROM rfc.generation_documents
                     WHERE generation_id={base} UNION ALL SELECT generate_series({start},{end})) ids ORDER BY rfc),
                 {sequence}, (extract(epoch FROM now())*1000000)::bigint,
                 (extract(epoch FROM now())*1000000)::bigint+3600000000
          FROM rfc.generation_audit WHERE generation_id={base};
        -- This search-only fixture clones canonical bytes and synthesizes graph/proof rows.
        INSERT INTO rfc.generation_source_evidence (generation_id,selection,rfc_id,txt_observation,
            document_version,txt_digest,byte_length,method,receipt,rsync_url,started_us,observed_us,
            checksum_choice,rfcxml_outcome,rfcxml_observation)
          SELECT a.generation_id,a.selection,e.rfc_id,e.txt_observation,e.document_version,e.txt_digest,
                 e.byte_length,'rsync_checksum_match',a.selection,e.rsync_url,a.started_us,a.started_us,
                 'auto',e.rfcxml_outcome,e.rfcxml_observation
          FROM rfc.generation_source_evidence e CROSS JOIN rfc.generation_audit a
          WHERE e.generation_id={base} AND a.generation_id={generation}
          UNION ALL
          SELECT a.generation_id,a.selection,d.rfc_id,sha256(convert_to('phase4-txt-'||d.rfc_id,'UTF8')),
                 d.identity,d.txt_digest,d.byte_length,'rsync_checksum_match',a.selection,
                 'rsync://rsync.rfc-editor.org/rfcs-text-only/rfc'||d.rfc_id||'.txt',a.started_us,a.started_us,
                 'auto','not_found',NULL
          FROM rfc.document_versions d CROSS JOIN rfc.generation_audit a
          WHERE d.rfc_id BETWEEN {start} AND {end} AND a.generation_id={generation};
        INSERT INTO rfc.generation_documents (generation_id,{copy_fields})
          SELECT {generation},{copy_fields} FROM rfc.generation_documents WHERE generation_id={base};
        INSERT INTO rfc.generation_documents (generation_id,{copy_fields})
          SELECT {generation},n,sha256(convert_to('phase4-doc-'||n,'UTF8')),
                 sha256(convert_to('phase4-metadata-'||n,'UTF8')),catalog_observation,
                 sha256(convert_to('phase4-txt-'||n,'UTF8')),'Graph node '||n,stream,
                 publication_date,current_status,publication_month,NULL
          FROM generate_series({start},{end}) n CROSS JOIN rfc.generation_documents
          WHERE generation_id={base} AND rfc_id={template};
        INSERT INTO rfc.rfc_relations
          SELECT {generation},source_rfc,kind,target_rfc FROM rfc.rfc_relations WHERE generation_id={base};
        INSERT INTO rfc.rfc_relations {seed_edges};
        INSERT INTO rfc.edge_evidence
          SELECT {generation},source_rfc,kind,target_rfc,evidence FROM rfc.edge_evidence WHERE generation_id={base};
        INSERT INTO rfc.edge_evidence
          SELECT e.generation_id,e.source_rfc,e.kind,e.target_rfc,p.evidence
          FROM rfc.rfc_relations e CROSS JOIN (
              SELECT DISTINCT evidence FROM rfc.edge_evidence WHERE generation_id={base} ORDER BY evidence LIMIT 2
          ) p WHERE e.generation_id={generation} AND e.source_rfc BETWEEN {start} AND {end};
        INSERT INTO rfc.relation_coverage
          SELECT {generation}, scope, rfc_id, family,
                 CASE WHEN scope='catalog' AND family='obsoletes' THEN 'partial' ELSE state END
          FROM rfc.relation_coverage WHERE generation_id={base};
        INSERT INTO rfc.relation_coverage
          SELECT {generation}, 'document', n, family, ({covered})::rfc.coverage_state
          FROM generate_series({start},{end}) n CROSS JOIN unnest(ARRAY{list(FAMILIES)}) family
          {coverage_join};
        COMMIT;
    """)
    run(project.search_command("sync", str(generation)), env=project.env, timeout=35)
    project.activate(generation, sequence)
    return generation


def oracle(graph, predicate, rfc):
    edges, coverage = graph
    name, *arguments = predicate
    if name == "NOT":
        value = oracle(graph, arguments[0], rfc)
        return None if value is None else not value
    if name in ("AND", "OR"):
        values = [oracle(graph, child, rfc) for child in arguments]
        decisive = name == "OR"
        return decisive if decisive in values else None if None in values else not decisive
    if name == "is":
        known = any(kind == "obsoletes" and target == rfc for _, kind, target in edges)
        return arguments[0] if known else None
    remote = arguments[0]
    if name in INVERSES:
        name = tuple(OPERATORS)[INVERSES.index(name)]
        source, target = remote, rfc
    else:
        source, target = rfc, remote
    family, kinds = OPERATORS[name]
    if any((source, kind, target) in edges for kind in kinds):
        return True
    if family == "classified_references" and (source, "unclassified_reference", target) in edges:
        return None
    return False if coverage.get((source, family)) == "complete" else None


def render(predicate):
    name, *arguments = predicate
    if name == "NOT":
        return f"NOT({render(arguments[0])})"
    if name in ("AND", "OR"):
        return "(" + f" {name} ".join(map(render, arguments)) + ")"
    if name == "is":
        return f"is::obsoleted:{str(arguments[0]).lower()}"
    return f"relation::{name}:{arguments[0]}"


def seeded_graph_and_benchmark(project, request, base):
    rng = random.Random(918)
    edges = {(rfc, kind, rng.choice(GRAPH_IDS)) for rfc in GRAPH_IDS for kind in KINDS if rng.randrange(3) == 0}
    coverage = {(number, family): rng.choice(("complete", "partial", "unavailable"))
                for number in GRAPH_IDS for family in FAMILIES if rng.randrange(6) != 0}
    assert {kind for _, kind, _ in edges} == set(KINDS)
    graph = seed(project, base, 4, GRAPH_IDS[0], GRAPH_IDS[-1], sorted(edges), coverage)
    assert int(project.sql(f"SELECT count(*) FROM rfc.edge_evidence WHERE generation_id={graph} AND source_rfc>=10000")) == 2 * len(edges)
    candidates = [(operator, remote) for operator in (*OPERATORS, *INVERSES) for remote in GRAPH_IDS]
    observed = set()
    for operator in (*OPERATORS, *INVERSES):
        targeted = [entry for entry in candidates if entry[0] == operator
                    and any(oracle((edges, coverage), entry, rfc) is True for rfc in GRAPH_IDS)]
        assert targeted, operator
        observed.add(targeted[0])
    observed.update([("is", True), ("is", False), ("normative_referenced_by", 99999)])

    def ast(depth):
        if not depth or rng.randrange(3) == 0:
            return rng.choice(candidates)
        if rng.randrange(3) == 0:
            return ("NOT", ast(depth - 1))
        return (rng.choice(("AND", "OR")), ast(depth - 1), ast(depth - 1))

    expressions = sorted(observed) + [("NOT", leaf) for leaf in sorted(observed)] + [ast(4) for _ in range(80)]
    combinations = {}
    for leaf in candidates:
        combinations[leaf] = tuple(oracle((edges, coverage), leaf, rfc) for rfc in GRAPH_IDS)
    for op in ("AND", "OR"):
        for first in (True, False, None):
            for second in (True, False, None):
                left = next(leaf for leaf, truth in combinations.items() if truth[0] is first)
                right = next(leaf for leaf, truth in combinations.items() if truth[0] is second)
                expressions.append((op, left, right))
    for expression in expressions:
        expected = [(rfc, oracle((edges, coverage), expression, rfc)) for rfc in GRAPH_IDS]
        query = 'in::title:"Graph node " AND (' + render(expression) + ")"
        result = page(request, query)
        assert hit_ids(result) == [rfc for rfc, truth in expected if truth is True], (query, result)
        assert result["coverage"]["unknown"] == sum(truth is None for _, truth in expected), (query, result)
    assert hit_ids(page(request, 'in::title:"Graph node " AND depend::id:10000')) == hit_ids(
        page(request, 'in::title:"Graph node " AND relation::references:10000'))
    print(f"Compared {len(expressions)} generated/known ASTs with the independent graph evaluator", flush=True)

    # 7 original fixture members and 12 graph members are carried forward.
    large = seed(project, graph, 5, 10012, 19992, (), {}, bulk=True)
    assert int(project.sql(f"SELECT count(*) FROM rfc.generation_documents WHERE generation_id={large}")) == 10_000
    project.sql("ANALYZE rfc.generation_documents; ANALYZE rfc.rfc_relations; ANALYZE rfc.document_lexemes")
    row_bound = "relation::references:1"
    byte_bound = 'in::fulltext:"never found in the fixture"'
    first = page(request, row_bound)
    assert first["scan_truncated"] and first["hits"] == [] and first["coverage"]["evaluated"] == 512
    second = page(request, row_bound, cursor=first["next_cursor"])
    assert second["scan_truncated"] and second["hits"] == [] and second["coverage"]["evaluated"] == 512
    assert first["next_cursor"] != second["next_cursor"]
    first = page(request, byte_bound)
    assert first["scan_truncated"] and first["hits"] == [] and 0 < first["coverage"]["evaluated"] < 512
    expensive = " OR ".join(f'in::fulltext:"missing {number}"' for number in range(64))
    comparison = page(request, expensive)
    assert comparison["scan_truncated"] and comparison["coverage"]["evaluated"] < first["coverage"]["evaluated"]

    for label, query in [
        ("title_token", "in::title:'graph node'"),
        ("inbound_obsolescence", "is::obsoleted:true"),
        ("row_budget", row_bound),
        ("original_byte_budget", byte_bound),
        ("comparison_budget", expensive),
    ]:
        timings = []
        for _ in range(7):
            result = request({"query": query})
            assert "result" in result, (label, result)
            timings.append(result["elapsed_us"] / 1000)
        timings = sorted(timings[1:])
        print(f"{label}: median={statistics.median(timings):.2f} ms max={timings[-1]:.2f} ms "
              f"evaluated={result['result']['coverage']['evaluated']}", flush=True)
        if label in ("title_token", "inbound_obsolescence", "row_budget"):
            plan = request({"explain": query})
            assert "result" in plan, plan
            sample = "\n".join(plan["result"])
            assert "generation_documents_pkey" in sample and "source_observations_pkey" in sample, sample
            assert "Seq Scan on relation_coverage" not in sample, sample
            assert "generation_source_evidence_pkey" in sample, sample
            assert "Seq Scan on generation_source_evidence" not in sample, sample
            assert "Bitmap Heap Scan on generation_source_evidence" not in sample, sample
            if label == "inbound_obsolescence":
                assert "relations_inbound_obsolescence" in sample, sample
            print(label + " EXPLAIN (ANALYZE, BUFFERS)\n" + sample, flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--context", default="rootless")
    parser.add_argument("--image-prefix", default="rfc-phase4-final")
    parser.add_argument("--skip-fixture-build", action="store_true")
    args = parser.parse_args()
    if not __debug__:
        raise RuntimeError("Phase 4 requires assertions")
    docker = ["docker", "--context", args.context]
    with tempfile.TemporaryDirectory(prefix="rfc-phase4-", dir="/tmp") as directory:
        work = Path(directory)
        publication_driver, driver = work / "publication-driver", work / "search-driver"
        phase3.fixture_driver(docker, args.image_prefix, publication_driver)
        search_driver(docker, args.image_prefix, driver, args.skip_fixture_build)
        project = Project(work / work.name, publication_driver, args.image_prefix, docker)
        project.search_driver = driver
        project.key_file = work / "cursor-key"
        project.key_file.write_bytes(secrets.token_bytes(48))
        project.key_file.chmod(0o444)
        try:
            phase2.prepare(project)
            add_source(project)
            project.start()
            upgrade(project)
            check_text_search_limits(project)
            project.run("up", "-d", "--wait", "--wait-timeout", "30", "fixtures")
            with search_session(project) as request:
                expect_error(request, "is::obsoleted:true", "corpus_not_ready")
                report = project.reconcile()
                first = int(report["generation_id"])
                project.activate(first, 0)
                assert project.read()["manifest"] == report["manifest_digest"]
                approve_synthetic_excerpt(project, first)
                check_text_and_snippets(request)
                check_corrupt_original(project, request, first)
                second = check_pagination(project, request, first)
                check_timeout(project, request)
                seeded_graph_and_benchmark(project, request, second)
        finally:
            project.close()


if __name__ == "__main__":
    main()
