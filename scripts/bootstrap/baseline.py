#!/usr/bin/env python3
"""Measure fresh private Compose stacks through first publication and MCP readiness."""

import argparse
import base64
from collections import Counter
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import re
import shutil
from statistics import median
import subprocess
import sys
import tempfile
from threading import Event, Thread
import time

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("phase6", HERE.parent / "phase6" / "integration.py")
phase6 = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(phase6)
phase1, phase2, phase3, phase5 = phase6.phase1, phase6.phase2, phase6.phase3, phase6.phase5
SERVICES = ("postgres", "serve", "sync", "feed")
PUBLICATION_SQL = """SELECT json_build_object(
    'active', a.generation_id, 'sequence', a.publication_sequence,
    'job_count', (SELECT count(*) FROM rfc.sync_jobs),
    'outbox_count', (SELECT count(*) FROM rfc.notification_outbox),
    'latest_generation', (SELECT json_build_object('id', id, 'state', state,
        'created_at', created_at, 'published_at', published_at, 'manifest', encode(manifest_digest, 'hex'))
        FROM rfc.generations ORDER BY id DESC LIMIT 1),
    'job', (SELECT json_build_object(
        'id', j.id, 'state', j.state, 'outcome', j.outcome, 'retries', j.retries,
        'generation', j.generation_id, 'queued_at', j.created_at,
        'started_at', j.started_at, 'finished_at', j.finished_at, 'next_attempt_at', j.next_attempt_at,
        'expected_sequence', j.expected_publication_sequence,
        'generation_state', g.state, 'generation_created_at', g.created_at,
        'published_at', g.published_at, 'scope', i.scope, 'manifest', encode(g.manifest_digest, 'hex'))
        FROM rfc.sync_jobs j LEFT JOIN rfc.generations g ON g.id = j.generation_id
        LEFT JOIN rfc.generation_ingestion i ON i.generation_id = g.id
        WHERE j.id > {after_job} ORDER BY j.id LIMIT 1),
    'outbox', (SELECT json_build_object('state', delivery_state, 'attempts', attempts,
        'generation', generation_id, 'delivered_at', delivered_at, 'event_id', event_id)
        FROM rfc.notification_outbox WHERE publication_sequence = {sequence}))
    FROM rfc.active_generation a WHERE a.singleton"""
XML_RESPONSE = re.compile(r'"GET /rfc/rfc[0-9]+\.xml HTTP/1\.1" ([0-9]{3}) ')
SCALE_SIZE = 3072
BULK_MODES = {"bulk-daemon", "bulk-manual", "bulk-scale"}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


@contextmanager
def duration(timings, name):
    start = time.monotonic()
    try:
        yield
    finally:
        timings[name] = time.monotonic() - start


class Project(phase6.Project):
    def __init__(self, directory, driver, prefix, docker, setup):
        self.setup = setup
        super().__init__(directory, driver, prefix, docker)
        self.env.update(PHASE2_TMPFS="512m", PHASE6_CRASH="", PHASE6_FORMAT="atom",
                        PHASE6_HOURLY="3600", PHASE6_DAILY="86400")

    def prepare(self):
        with duration(self.setup, "database_tls"):
            self.prepare_tls()
        with duration(self.setup, "database_secret_provisioning"):
            self.prepare_database_secrets()

    def command(self, *args):
        command = super().command(*args)
        index = command.index("-p")
        return [*command[:index], "-f", str(HERE / "compose.yml"), *command[index:]]

    def preprovision(self, size, work, *, scale=False, paced=True):
        with duration(self.setup, "fixture_subnet_selection"):
            self.prepare_network(work)
        with duration(self.setup, "proxy_tls"):
            self.prepare_proxy_tls(work)
        with duration(self.setup, "control_secret_provisioning"):
            self.prepare_control_secrets(work)
        self.env["PHASE6_NOTIFY_TARGET"] = f"{self.net}.34:8782"
        with duration(self.setup, "fixture_payload_and_upstream_tls"):
            base_sources = phase2.prepare_benchmark(self, size)
            (self.source / "mode").write_text("scale-bootstrap" if scale else "cold-bootstrap")
            self.env["PHASE_BOOTSTRAP_PACED"] = "1" if paced and not scale else "0"
            if scale:
                for rfc in range(10000, 10000 + size - len(base_sources)):
                    if rfc % 3 == 0:
                        (self.source / "https" / f"rfc{rfc}.xml").write_text(
                            f'<rfc number="{rfc}"><back><references><name>Normative References</name>'
                            '<reference><seriesInfo name="RFC" value="2119"/></reference>'
                            '</references></back></rfc>'
                        )
            phase6.feed_data(self, [])
            (self.source / "feed-mode").write_text("normal")
        assert not any((self.directory / "pgdata").iterdir())
        assert not any((self.directory / "mirror").iterdir())

    def publication_snapshot(self, sequence=1, after_job=0):
        return json.loads(self.sql(PUBLICATION_SQL.format(sequence=int(sequence), after_job=int(after_job))))

    def resources(self, mode):
        data = {}
        services = SERVICES if mode == "baseline" else ("postgres", "serve", "sync")
        for service in services[:2] if mode == "bulk-manual" else services:
            output = self.capture("exec", "-T", service, "sh", "-ec",
                                  "cd /sys/fs/cgroup; cat cpu.max memory.max memory.peak; "
                                  "awk '$1 == \"usage_usec\" { print $2 }' cpu.stat")
            cpu, limit, peak, usage = output.splitlines()
            data[service] = {"cpu_max": cpu, "memory_limit_bytes": int(limit),
                             "memory_peak_bytes": int(peak), "cpu_us": int(usage)}
        return data

    def close(self):
        try:
            self.run("stop", "feed", "sync", "serve", timeout=40)
        finally:
            super().close()


class DiskSamples:
    """Sample allocated blocks independently of publication and readiness polling."""

    def __init__(self, project, measurements, mode):
        self.project = project
        self.measurements = measurements
        self.mode = mode
        self.done = Event()
        self.thread = Thread(target=self.sample_periodically)

    def sample(self):
        commands = {
            "pgdata": self.project.command("exec", "-T", "postgres", "du", "-sk", "/var/lib/postgresql"),
            "mirror": self.project.command("exec", "-T", "sync", "du", "-sk", "/mirror"),
        }
        if self.mode != "baseline":
            # The manual run has no Sync daemon, and Serve cannot traverse private mirror directories.
            commands["mirror"] = ["sudo", "-n", "du", "-sk", "--", str(self.project.directory / "mirror")]
        for name, command in commands.items():
            try:
                raw = phase1.capture(command, env=self.project.env, timeout=5, stderr=subprocess.PIPE)
                self.record(name, int(raw.split()[0]) * 1024)
            except (IndexError, OSError, ValueError, subprocess.SubprocessError) as error:
                diagnostic = getattr(error, "stderr", None)
                self.measurements["sampling_errors"][name] = diagnostic.strip() if isinstance(diagnostic, str) else str(error)
        if hasattr(self.project, "stage_mount"):
            try:
                self.record("bulk_stage", shutil.disk_usage(self.project.stage_mount).used)
            except OSError as error:
                self.measurements["sampling_errors"]["bulk_stage"] = str(error)

    def record(self, name, allocated):
        peak = self.measurements["peak_allocated_bytes"]
        peak[name] = max(peak.get(name, 0), allocated)
        self.measurements["samples"][name] += 1

    def sample_periodically(self):
        while not self.done.wait(5):
            self.sample()

    def __enter__(self):
        self.thread.start()

    def __exit__(self, *_):
        self.done.set()
        self.thread.join()
        self.sample()


def fixture_inventory(project):
    inventory = hashlib.sha256()
    texts = {}
    for path in sorted(project.source.rglob("*")):
        name = path.relative_to(project.source)
        if not path.is_file() or path.suffix not in (".xml", ".txt"):
            continue
        payload = path.read_bytes()
        inventory.update(str(name).encode() + b"\0" + hashlib.sha256(payload).digest())
        if name.parts[0] == "rsync":
            texts[int(path.stem[3:])] = payload
    return inventory.hexdigest(), texts


def verify_publication(project, active, texts, report, *, manual, bulk):
    generation = int(active["active"])
    if manual:
        assert active["job_count"] == 0 and active["outbox"]["state"] == "pending", active
    else:
        assert active["job_count"] == 1 and active["job"]["expected_sequence"] == 0, active
        assert active["job"]["generation"] == generation and active["job"]["state"] == "succeeded", active
        assert active["job"]["outcome"] == "published" and active["outbox"]["state"] == "delivered", active
    assert active["outbox_count"] == 1 and active["outbox"]["generation"] == generation, active
    assert active["latest_generation"]["manifest"] == report["manifest_digest"], active
    rows = json.loads(project.sql(f"""
        SELECT json_agg(json_build_array(g.rfc_id, encode(d.txt_digest, 'hex'), d.sealed,
            o.https_verification, o.raw_digest = d.txt_digest, o.source_transport) ORDER BY g.rfc_id)
        FROM rfc.generation_documents g JOIN rfc.document_versions d ON d.identity = g.document_version
        JOIN rfc.source_observations o ON o.identity = g.txt_observation WHERE g.generation_id = {generation}
    """))
    assert [row[0] for row in rows] == sorted(texts), rows
    for rfc, digest, sealed, verification, same_digest, transport in rows:
        expected = ("unverified", "rsync") if bulk else ("verified", "https")
        assert sealed and same_digest and (verification, transport) == expected, (rfc, digest)
        assert project.canonical(digest).read_bytes() == texts[rfc], rfc
    verified = json.loads(project.capture("exec", "-T", "serve", "/phase2/fixture-driver", "verify-restore",
                                          timeout=300))
    assert verified["manifests"] == {str(generation): report["manifest_digest"]}, verified
    return verified


def verify_scale(project, report, disk):
    metrics = report["metrics"]
    assert report["expected_txt"] == SCALE_SIZE and metrics["https_requests"] == SCALE_SIZE + 2
    assert metrics["txt"]["calls"] == 0 and metrics["rsync"]["calls"] == 1
    assert metrics["rsync_transferred_files"] == SCALE_SIZE and metrics["rsync_checksum_hits"] == 0
    assert 1 <= metrics["https_peak"] <= 2 and metrics["parse_peak"] == 1
    assert 1 <= metrics["admitted_peak"] <= metrics["xml_window_peak"] <= 3
    assert metrics["raw_reserved_peak"] == metrics["xml_window_peak"] * 16 * 1024**2
    assert 0 < metrics["memory_peak"] <= 2 * 1024**3
    assert 0 < disk["peak_allocated_bytes"]["bulk_stage"] <= 512 * 1024**2
    assert not disk["sampling_errors"], disk
    generation = int(report["generation_id"])
    rows = project.sql(f"""
        SELECT json_build_array(d.rfc_id, encode(o.raw_digest, 'hex'), o.source_transport,
            o.https_verification, o.source_url, a.state, c.state)
        FROM rfc.generation_documents d
        LEFT JOIN rfc.source_observations o ON o.identity=d.rfcxml_observation
        JOIN rfc.relation_coverage a ON (a.generation_id, a.rfc_id, a.family) =
            (d.generation_id, d.rfc_id, 'all_references')
        JOIN rfc.relation_coverage c ON (c.generation_id, c.rfc_id, c.family) =
            (d.generation_id, d.rfc_id, 'classified_references')
        WHERE d.generation_id={generation} ORDER BY d.rfc_id
    """, phase1.SERVE)
    documents = [json.loads(row) for row in rows.splitlines()]
    assert len(documents) == SCALE_SIZE
    expected_edges = []
    for rfc, digest, transport, verification, url, all_refs, classified in documents:
        xml = project.source / "https" / f"rfc{rfc}.xml"
        if xml.exists():
            assert (transport, verification, url) == (
                "https", "verified", f"https://www.rfc-editor.org/rfc/{xml.name}",
            )
            assert project.canonical(digest).read_bytes() == xml.read_bytes(), rfc
            if rfc >= 10000:
                assert (all_refs, classified) == ("complete", "complete"), rfc
                expected_edges.append(rfc)
        else:
            assert (digest, transport, verification, url) == (None, None, None, None), rfc
            if rfc >= 10000:
                assert (all_refs, classified) == ("unavailable", "unavailable"), rfc
    edges = project.sql(f"""
        SELECT e.source_rfc, e.target_rfc, e.kind, o.observation=d.rfcxml_observation
        FROM rfc.edge_evidence e JOIN rfc.relation_evidence o ON o.identity=e.evidence
        JOIN rfc.generation_documents d ON (d.generation_id,d.rfc_id) = (e.generation_id,e.source_rfc)
        WHERE e.generation_id={generation} AND e.source_rfc>=10000 AND o.method='rfcxml'
        ORDER BY e.source_rfc
    """, phase1.SERVE)
    assert edges.splitlines() == [f"{rfc}|2119|normative_reference|t" for rfc in expected_edges]


def collect_logs(project, evidence):
    logs = {}
    for service in (*SERVICES, "fixtures", "proxy"):
        raw = project.capture("logs", "--timestamps", "--no-color", "--no-log-prefix", service)
        (evidence / f"{service}.log").write_text(raw + "\n")
        logs[service] = raw
    reports, events = [], []
    for line in logs["sync"].splitlines():
        timestamp, _, message = line.partition(" ")
        if message.startswith('{"scope":'):
            reports.append({"log_timestamp": timestamp, **json.loads(message)})
        elif message.startswith("{"):
            events.append({"log_timestamp": timestamp, **json.loads(message)})
    manual_report = evidence / "manual-sync.json"
    if manual_report.exists():
        reports.append(json.loads(manual_report.read_text()))
        events.extend(json.loads(line) for line in (evidence / "manual-sync.stderr").read_text().splitlines()
                      if line.startswith("{"))
    counts = Counter(XML_RESPONSE.findall(logs["fixtures"]))
    http_errors = sum(count for status, count in counts.items() if status not in {"200", "404", "410"})
    xml_calls = sum(report.get("metrics", {}).get("rfcxml", {}).get("calls", 0) for report in reports)
    return reports, events, {
        "200": counts["200"], "404": counts["404"], "410": counts["410"],
        "http_errors": http_errors,
        "without_logged_response": max(0, xml_calls - sum(counts.values())),
        "fetch_errors": sum(" RFCXML fetch: " in item for report in reports for item in report["diagnostics"]),
        "parse_errors": sum(" RFCXML parse: " in item for report in reports for item in report["diagnostics"]),
    }


def prepare_manual_candidate(project, evidence, mark):
    command = project.app("sync", "-T", args=("--phase2-reconcile", "--full"))
    with subprocess.Popen(command, env=project.env, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE) as process:
        try:
            phase6.wait(lambda: process.poll() is not None or
                        (project.publication_snapshot()["latest_generation"] or {}).get("state") == "building")
            assert process.poll() is None, "manual reconciliation ended before observing building"
            mark("first_building")
            stdout, stderr = process.communicate(timeout=90)
        finally:
            if process.poll() is None:
                process.kill()
    (evidence / "manual-sync.stderr").write_text(stderr)
    (evidence / "manual-sync.json").write_text(stdout)
    report = json.loads(stdout)
    assert process.returncode == 0 and report["state"] == "ready", (report, stderr)
    generation = int(report["generation_id"])
    state = project.publication_snapshot()
    assert state["latest_generation"]["state"] == "ready" and state["latest_generation"]["id"] == generation, state
    assert state["sequence"] == 0 and state["active"] is None and state["outbox_count"] == 0, state
    assert state["job_count"] == 0 and "503" in project.admin("/ready").stderr, state
    mark("candidate_ready_observed")
    return generation


def initialize_stack(project, stages, mark, *, preflight):
    for name, action in (
        ("volume_initialization", project.initialize_volumes),
        ("postgres_initdb_and_start", project.start_postgres),
        ("roles_passwords_and_bootstrap", project.bootstrap_database),
        ("migration", project.migrate),
        ("storage_checks", lambda: [project.check(service) for service in ("sync", "serve")]),
    ):
        with duration(stages, name):
            action()
    config = None
    if preflight:
        with duration(stages, "sync_config_preflight"):
            config = json.loads(phase1.capture(
                project.app("sync", "-T", "--entrypoint", "/phase6/sync", args=("--check-config",)),
                env=project.env))
    empty = project.publication_snapshot()
    assert empty["sequence"] == 0 and empty["latest_generation"] is None and empty["job_count"] == 0, empty
    assert empty["outbox_count"] == 0
    mark("empty_observed")
    with duration(stages, "upstream_serve_and_proxy_start"):
        project.run("up", "-d", "--wait", "fixtures", timeout=35)
        client = project.start_mcp()
        phase6.wait(lambda: "503" in project.admin("/ready").stderr)
        schema = next(tool["outputSchema"] for tool in json.loads(phase5.TOOL_SCHEMA.read_bytes())
                      if tool["name"] == "get")
        client.schemas["get"] = phase5.Draft202012Validator(schema)
    return client, config


def run_workflow(project, receipt, texts, failure, evidence):
    mode = receipt["mode"]
    manual = mode == "bulk-manual"
    bulk = mode in BULK_MODES
    scale = mode == "bulk-scale"
    stages, milestones = receipt["workflow_seconds"], receipt["milestones"]
    receipt["t0_utc"] = utc_now()
    t0 = time.monotonic()

    def mark(name):
        milestones[name] = {"utc": utc_now(), "seconds": time.monotonic() - t0}

    client, config = initialize_stack(project, stages, mark, preflight=mode != "baseline")
    if config is not None:
        receipt["effective_config"] = config
    disk = receipt["disk"] = {"peak_allocated_bytes": {}, "samples": Counter(), "sampling_errors": {}}
    with DiskSamples(project, disk, mode):
        if manual:
            with duration(stages, "manual_reconciliation"):
                generation = prepare_manual_candidate(project, evidence, mark)
            with duration(stages, "manual_publication"):
                project.activate(generation, 0)
        else:
            services = ("sync", "feed") if mode == "baseline" else ("sync",)
            with duration(stages, "sync_and_feed_start" if mode == "baseline" else "sync_start"):
                project.run("up", "-d", "--no-deps", *services, timeout=45)

        def check():
            state = project.publication_snapshot()
            receipt["database"] = state
            job = state["job"] or {}
            if failure:
                return job.get("generation_state") == "failed" and job["retries"] == 1
            if job.get("generation_state") == "failed" or job.get("retries", 0):
                raise AssertionError(f"unexpected first-publication retry: {job}")
            if job.get("generation_state") == "building" and "first_building" not in milestones:
                mark("first_building")
            if manual:
                return state["active"] == generation and state["latest_generation"]["state"] == "published"
            return state["active"] is not None and job.get("generation_state") == "published"

        phase6.wait(check, seconds=1800 if scale else max(120, len(texts) * 5))
        active = receipt["database"]
        if failure:
            mark("first_failed_attempt")
            assert active["sequence"] == 0 and active["active"] is None and active["outbox_count"] == 0, active
            assert "503" in project.admin("/ready").stderr
        else:
            mark("published_observed")
            assert active["sequence"] == 1 and (manual or active["job"]["scope"] == "full"), active
            phase6.wait(lambda: project.admin("/ready").returncode == 0)
            mark("ready_observed")
            original, _ = client.call("get", id=2119, format="base64")
            assert original["generation_id"] == str(active["active"]), original
            assert original["publication_sequence"] == "1", original
            content = original["original"]
            assert content["start_byte"] == 0 and content["end_byte"] == len(texts[2119]), content
            assert base64.b64decode(content["content_base64"], validate=True) == texts[2119]
            mark("t1_mcp_read")

            def delivered():
                receipt["database"] = project.publication_snapshot()
                return (receipt["database"]["outbox"] or {}).get("state") == "delivered"

            if not manual:
                phase6.wait(delivered, seconds=35)
                mark("notification_observed")

    receipt["cgroups"] = project.resources(mode)
    reports, events, statuses = collect_logs(project, evidence)
    receipt.update(collector_reports=reports, sync_events=events, rfcxml=statuses)
    assert len(reports) == 1, reports
    report = reports[0]
    metrics = report["metrics"]
    if metrics["https_start_window_us"]:
        receipt["https_starts_per_second"] = (metrics["https_requests"] - 1) * 1e6 / metrics["https_start_window_us"]
    assert metrics["rsync_files"] >= 1 and metrics["peak_rss_bytes"] > 0, metrics
    if failure:
        assert report["state"] == "failed" and "rsync and HTTPS TXT disagree" in report["error"], report
        if mode != "baseline":
            assert active["job"]["state"] == "running" and active["job"]["next_attempt_at"], active
            assert report["job_id"] == str(active["job"]["id"]), report
            assert report["generation_id"] == str(active["job"]["generation"]), report
            assert not any(event.get("event") == "publication" for event in events), events
        receipt["status"] = "expected_failure"
    else:
        assert report["state"] == "ready" and report["scope"] == "full", report
        assert int(report["generation_id"]) == active["active"], report
        assert report["expected_txt"] == metrics["rsync_files"] == len(texts)
        assert report["verified_txt"] == (0 if bulk else len(texts))
        assert report["rsync_only_txt"] == (len(texts) if bulk else 0)
        assert report["reused_txt"] == 0 and metrics["rsync_payload_bytes"] == sum(map(len, texts.values()))
        assert metrics["https_requests"] == len(texts) * (1 if bulk else 2) + 2
        if not scale:
            assert metrics["minimum_start_spacing_us"] >= 1_000_000
        assert statuses["200"] > 0 and statuses["404"] > 0 and statuses["410"] == 1, statuses
        assert sum(statuses[status] for status in ("200", "404", "410")) == len(texts), statuses
        assert not any(statuses[key] for key in ("http_errors", "without_logged_response", "fetch_errors", "parse_errors"))
        receipt["storage_verification"] = verify_publication(
            project, receipt["database"], texts, report, manual=manual, bulk=bulk)
        if bulk:
            verify_bulk_progress(receipt, report, texts, manual)
            generation = int(active["active"])
            manifest_hex = project.sql(f"SELECT encode(manifest_bytes, 'hex') FROM rfc.generations WHERE id = {generation}")
            (evidence / "manifest.json").write_bytes(bytes.fromhex(manifest_hex))
        if scale:
            verify_scale(project, report, disk)
        receipt["status"] = "published_and_ready"


def verify_bulk_progress(receipt, report, texts, manual):
    count = len(texts)
    generation = report["generation_id"]
    job = None if manual else str(receipt["database"]["job"]["id"])
    assert report.get("job_id") == job
    assert "first_building" in receipt["milestones"], receipt["milestones"]
    metrics = report["metrics"]
    assert metrics["validation"]["calls"] == 1, metrics
    assert metrics["rsync_transferred_payload_bytes"] == sum(map(len, texts.values())), metrics
    assert metrics["progress"] == {
        "selected": count, "txt_completed": count, "xml_completed": count,
        "txt_remaining": 0, "xml_remaining": 0, "admitted": 0, "xml_reserved": 0,
    }, report
    events = receipt["sync_events"]
    progress = [event for event in events if event.get("event") == "ingestion_progress"]
    assert any(event["progress"]["xml_remaining"] > 0 for event in progress), events
    for event in progress:
        assert event["state"] == "building" and event["job_id"] == job and event["generation_id"] == generation
        snapshot = event["progress"]
        assert snapshot["selected"] in {0, count}
        for source in ("txt", "xml"):
            assert snapshot[f"{source}_completed"] + snapshot[f"{source}_remaining"] == snapshot["selected"]
        assert snapshot["admitted"] <= metrics["admitted_peak"]
        assert snapshot["xml_reserved"] <= metrics["xml_window_peak"]
    publications = [event for event in events if event.get("event") == "publication"]
    if manual:
        assert not publications, publications
        return
    published, = publications
    assert published["state"] == "published" and published["generation_id"] == generation and published["job_id"] == job
    assert published["expected_publication_sequence"] == "0" and published["publication_sequence"] == "1"
    assert report["log_timestamp"] <= published["log_timestamp"], (report, published)
    receipt["milestones"]["candidate_ready_logged"] = {"utc": report["log_timestamp"]}
    receipt["milestones"]["publication_committed_logged"] = {"utc": published["log_timestamp"]}


def trial(args, docker, driver, work, size, repeat, failure=False, mode="baseline"):
    label = mode if mode != "baseline" else ("txt-mismatch" if failure else f"{size}-rfc-{repeat}")
    evidence = args.evidence / label
    evidence.mkdir()
    receipt = {"status": "error", "mode": mode, "size": size, "repeat": repeat, "failure_probe": failure,
               "private_fixture_paced": mode != "bulk-scale", "setup_seconds": {},
               "workflow_seconds": {}, "milestones": {}}
    directory = work / label
    directory.mkdir()
    project = None
    staging = ExitStack()
    try:
        for service in ("sync", "feed"):
            os.link(work / service, directory / service)
        project = Project(directory / f"{work.name}-{label}", driver, args.image_prefix,
                          docker, receipt["setup_seconds"])
        scale = mode == "bulk-scale"
        project.preprovision(size, directory, scale=scale)
        if mode in BULK_MODES:
            with duration(receipt["setup_seconds"], "bulk_stage_provisioning"):
                capacity = {"mebibytes": 512, "inodes": 8192} if scale else {}
                project.stage_mount = staging.enter_context(phase2.capped_stage(project, **capacity))
                project.use_bulk_bootstrap(project.stage_mount)
        if failure:
            path = project.source / "https/rfc100.txt"
            path.write_bytes(path.read_bytes() + b"\nfixture disagreement\n")
        receipt["fixture_sha256"], texts = fixture_inventory(project)
        assert len(texts) == size
        run_workflow(project, receipt, texts, failure, evidence)
    except (AssertionError, KeyError, OSError, RuntimeError, TypeError, ValueError, subprocess.SubprocessError) as error:
        receipt["error"] = f"{type(error).__name__}: {error}"
        if project:
            try:
                receipt["collector_reports"], receipt["sync_events"], receipt["rfcxml"] = collect_logs(project, evidence)
            except (OSError, ValueError, subprocess.SubprocessError) as log_error:
                receipt["log_error"] = str(log_error)
    finally:
        try:
            with staging:
                if project:
                    project.close()
        except subprocess.SubprocessError as cleanup_error:
            receipt.update(status="error", cleanup_error=str(cleanup_error))
        (evidence / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    t1 = receipt["milestones"].get("t1_mcp_read", {}).get("seconds")
    print(f"{label}: {receipt['status']} T1={t1} {receipt.get('error', '')}", flush=True)
    return receipt


def main():
    if not __debug__:
        raise RuntimeError("the baseline requires enabled assertions")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", default="rootless")
    parser.add_argument("--image-prefix", required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    profiles = parser.add_mutually_exclusive_group()
    profiles.add_argument("--phase6", action="store_true", help="compare cold bulk daemon and manual publication")
    profiles.add_argument("--phase7-scale", action="store_true", help="qualify 3072 RFCs on a local private fixture")
    profiles.add_argument("--phase8", choices=("quick", "paced", "full", "rollback"),
                          help="measure the private dual-queue bootstrap qualification")
    parser.add_argument("--sizes", type=int, choices=(6, 96), nargs="+")
    parser.add_argument("--repeats", type=int, choices=range(1, 6))
    args = parser.parse_args()
    args.evidence = args.evidence.resolve()
    if Path("/tmp") not in args.evidence.parents or args.evidence.exists():
        parser.error("--evidence must be a new directory beneath /tmp")
    if (args.phase6 or args.phase7_scale or args.phase8) and (args.sizes is not None or args.repeats is not None):
        parser.error("the selected profile uses a fixed trial set")
    sizes = args.sizes or (6, 96)
    repeats = args.repeats or 3
    if len(set(sizes)) != len(sizes):
        parser.error("--sizes must be distinct")
    args.evidence.mkdir(mode=0o700, parents=True)
    os.environ["RFC_E2E_PRELOADED"] = "1"
    docker = ["docker", "--context", args.context]
    info = json.loads(phase1.capture([*docker, "info", "--format", "{{json .}}"]))
    summary = {
        "source_head": phase1.capture(["git", "rev-parse", "HEAD"], cwd=phase2.REPO),
        "kernel": platform.release(), "architecture": platform.machine(),
        "docker_context": args.context,
        "docker": {key: info[key] for key in ("ServerVersion", "NCPU", "MemTotal", "Driver", "CgroupVersion")},
        "compose": phase1.capture([*docker, "compose", "version", "--short"]),
        "oci_cache": "preloaded", "registry_pull_seconds": None, "runs": [],
    }
    with tempfile.TemporaryDirectory(prefix="rfc-bootstrap-", dir="/tmp") as temp:
        work = Path(temp)
        driver = work / "publication-driver"
        postgres = re.search(r"(?m)^    image: (docker\.io/library/postgres[^\s]+)$",
                             (phase1.HERE / "compose.yml").read_text()).group(1)
        images = [f"{args.image_prefix}-{suffix}" for suffix in
                  ("serve", "sync", "feed", "fixtures", "publication-fixture", "control-fixture")] + [postgres]
        with duration(summary, "preloaded_image_check_and_fixture_extraction_seconds"):
            ids = phase1.capture([*docker, "image", "inspect", "--format", "{{.Id}}", *images]).splitlines()
            summary["images"] = dict(zip(images, ids, strict=True))
            phase3.fixture_driver(docker, args.image_prefix, driver)
            phase6.fixture_binaries(docker, args.image_prefix, work)
        if args.phase8:
            import qualification

            summary["phase8"] = args.phase8
            qualification.run(args, docker, driver, work, summary, sys.modules[__name__])
        elif args.phase7_scale:
            summary["runs"].append(trial(args, docker, driver, work, SCALE_SIZE, 1, mode="bulk-scale"))
        elif args.phase6:
            for mode in ("bulk-daemon", "bulk-manual", "strict-failure"):
                summary["runs"].append(trial(args, docker, driver, work, 6, 1,
                                             failure=mode == "strict-failure", mode=mode))
        else:
            for size in sizes:
                for repeat in range(1, repeats + 1):
                    summary["runs"].append(trial(args, docker, driver, work, size, repeat))
            summary["runs"].append(trial(args, docker, driver, work, 6, 1, failure=True))
    if args.phase6:
        daemon, manual, _ = summary["runs"]
        matched = all(r["status"] == "published_and_ready" for r in (daemon, manual)) and (
            daemon["fixture_sha256"] == manual["fixture_sha256"]
            and (args.evidence / "bulk-daemon/manifest.json").read_bytes()
            == (args.evidence / "bulk-manual/manifest.json").read_bytes()
        )
        summary["manual_daemon_equivalent"] = matched
    elif not args.phase7_scale and not args.phase8:
        summary["median_t1_seconds"] = {}
        for size in sizes:
            runs = [r for r in summary["runs"] if r["size"] == size and r["status"] == "published_and_ready"]
            if runs:
                summary["median_t1_seconds"][size] = median(r["milestones"]["t1_mcp_read"]["seconds"] for r in runs)
                assert len({r["collector_reports"][0]["manifest_digest"] for r in runs}) == 1, runs
    (args.evidence / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return int(any(run["status"] == "error" for run in summary["runs"])
               or (args.phase8 and not (summary["fixtures_reproducible"] and summary["cold_manifests_reproducible"]))
               or (args.phase6 and not summary["manual_daemon_equivalent"]))


if __name__ == "__main__":
    sys.exit(main())
