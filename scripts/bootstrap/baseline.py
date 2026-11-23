#!/usr/bin/env python3
"""Measure fresh private Compose stacks through first publication and MCP readiness."""

import argparse
import base64
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import re
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
    'job', (SELECT json_build_object(
        'id', j.id, 'state', j.state, 'outcome', j.outcome, 'retries', j.retries,
        'generation', j.generation_id, 'queued_at', j.created_at,
        'started_at', j.started_at, 'finished_at', j.finished_at, 'next_attempt_at', j.next_attempt_at,
        'expected_sequence', j.expected_publication_sequence,
        'generation_state', g.state, 'generation_created_at', g.created_at,
        'published_at', g.published_at, 'scope', i.scope, 'manifest', encode(g.manifest_digest, 'hex'))
        FROM rfc.sync_jobs j LEFT JOIN rfc.generations g ON g.id = j.generation_id
        LEFT JOIN rfc.generation_ingestion i ON i.generation_id = g.id ORDER BY j.id LIMIT 1),
    'outbox', (SELECT json_build_object('state', delivery_state, 'attempts', attempts,
        'generation', generation_id, 'delivered_at', delivered_at, 'event_id', event_id)
        FROM rfc.notification_outbox WHERE publication_sequence = 1))
    FROM rfc.active_generation a WHERE a.singleton"""
XML_RESPONSE = re.compile(r'"GET /rfc/rfc[0-9]+\.xml HTTP/1\.1" ([0-9]{3}) ')


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

    def preprovision(self, size, work):
        with duration(self.setup, "fixture_subnet_selection"):
            self.prepare_network(work)
        with duration(self.setup, "proxy_tls"):
            self.prepare_proxy_tls(work)
        with duration(self.setup, "control_secret_provisioning"):
            self.prepare_control_secrets(work)
        self.env["PHASE6_NOTIFY_TARGET"] = f"{self.net}.34:8782"
        with duration(self.setup, "fixture_payload_and_upstream_tls"):
            phase2.prepare_benchmark(self, size)
            (self.source / "mode").write_text("cold-bootstrap")
            phase6.feed_data(self, [])
            (self.source / "feed-mode").write_text("normal")
        assert not any((self.directory / "pgdata").iterdir())
        assert not any((self.directory / "mirror").iterdir())

    def publication_snapshot(self):
        return json.loads(self.sql(PUBLICATION_SQL))

    def resources(self):
        data = {}
        for service in SERVICES:
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

    def __init__(self, project, measurements):
        self.project = project
        self.measurements = measurements
        self.done = Event()
        self.thread = Thread(target=self.sample_periodically)

    def sample(self):
        for service, name, path in (("postgres", "pgdata", "/var/lib/postgresql"),
                                    ("sync", "mirror", "/mirror")):
            try:
                raw = self.project.capture("exec", "-T", service, "du", "-sk", path,
                                           timeout=5, stderr=subprocess.PIPE)
            except subprocess.SubprocessError as error:
                self.measurements["sampling_errors"][name] = str(error)
                continue
            peak = self.measurements["peak_allocated_bytes"]
            peak[name] = max(peak.get(name, 0), int(raw.split()[0]) * 1024)
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


def verify_publication(project, active, texts, report):
    generation = int(active["active"])
    assert active["job_count"] == 1 and active["job"]["expected_sequence"] == 0, active
    assert active["job"]["generation"] == generation and active["job"]["state"] == "succeeded", active
    assert active["job"]["outcome"] == "published" and active["outbox"]["generation"] == generation, active
    assert active["job"]["manifest"] == report["manifest_digest"], active
    rows = json.loads(project.sql(f"""
        SELECT json_agg(json_build_array(g.rfc_id, encode(d.txt_digest, 'hex'), d.sealed,
            o.https_verification, o.raw_digest = d.txt_digest) ORDER BY g.rfc_id)
        FROM rfc.generation_documents g JOIN rfc.document_versions d ON d.identity = g.document_version
        JOIN rfc.source_observations o ON o.identity = g.txt_observation WHERE g.generation_id = {generation}
    """))
    assert [row[0] for row in rows] == sorted(texts), rows
    for rfc, digest, sealed, verification, same_digest in rows:
        assert sealed and verification == "verified" and same_digest, (rfc, digest)
        assert project.canonical(digest).read_bytes() == texts[rfc], rfc
    verified = json.loads(project.capture("exec", "-T", "serve", "/phase2/fixture-driver", "verify-restore"))
    assert verified["manifests"] == {str(generation): report["manifest_digest"]}, verified
    return verified


def collect_logs(project, evidence):
    logs = {}
    for service in (*SERVICES, "fixtures", "proxy"):
        raw = project.capture("logs", "--timestamps", "--no-color", "--no-log-prefix", service)
        (evidence / f"{service}.log").write_text(raw + "\n")
        logs[service] = raw
    reports = []
    for line in logs["sync"].splitlines():
        timestamp, _, message = line.partition(" ")
        if message.startswith('{"scope":'):
            reports.append({"log_timestamp": timestamp, **json.loads(message)})
    counts = Counter(XML_RESPONSE.findall(logs["fixtures"]))
    http_errors = sum(count for status, count in counts.items() if status not in {"200", "404", "410"})
    xml_calls = sum(report.get("metrics", {}).get("rfcxml", {}).get("calls", 0) for report in reports)
    return reports, {
        "200": counts["200"], "404": counts["404"], "410": counts["410"],
        "http_errors": http_errors,
        "without_logged_response": max(0, xml_calls - sum(counts.values())),
        "fetch_errors": sum(" RFCXML fetch: " in item for report in reports for item in report["diagnostics"]),
        "parse_errors": sum(" RFCXML parse: " in item for report in reports for item in report["diagnostics"]),
    }


def run_workflow(project, receipt, texts, failure, evidence):
    stages, milestones = receipt["workflow_seconds"], receipt["milestones"]
    receipt["t0_utc"] = utc_now()
    t0 = time.monotonic()

    def mark(name):
        milestones[name] = {"utc": utc_now(), "seconds": time.monotonic() - t0}

    for name, action in (
        ("volume_initialization", project.initialize_volumes),
        ("postgres_initdb_and_start", project.start_postgres),
        ("roles_passwords_and_bootstrap", project.bootstrap_database),
        ("migration", project.migrate),
        ("storage_checks", lambda: [project.check(service) for service in ("sync", "serve")]),
    ):
        with duration(stages, name):
            action()
    assert project.publication() == 0
    with duration(stages, "upstream_serve_and_proxy_start"):
        project.run("up", "-d", "--wait", "fixtures", timeout=35)
        client = project.start_mcp()
        phase6.wait(lambda: "503" in project.admin("/ready").stderr)
        schema = next(tool["outputSchema"] for tool in json.loads(phase5.TOOL_SCHEMA.read_bytes())
                      if tool["name"] == "get")
        client.schemas["get"] = phase5.Draft202012Validator(schema)
    with duration(stages, "sync_and_feed_start"):
        project.run("up", "-d", "--no-deps", "sync", "feed", timeout=45)

    disk = receipt["disk"] = {"peak_allocated_bytes": {}, "samples": Counter(), "sampling_errors": {}}
    with DiskSamples(project, disk):
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
            return state["active"] is not None and job.get("generation_state") == "published"

        phase6.wait(check, seconds=max(120, len(texts) * 5))
        active = receipt["database"]
        if failure:
            mark("first_failed_attempt")
            assert active["sequence"] == 0 and active["active"] is None and active["outbox"] is None, active
            assert "503" in project.admin("/ready").stderr
        else:
            mark("published_observed")
            assert active["sequence"] == 1 and active["job"]["scope"] == "full", active
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

            phase6.wait(delivered, seconds=35)
            mark("notification_observed")

    receipt["cgroups"] = project.resources()
    reports, statuses = collect_logs(project, evidence)
    receipt.update(collector_reports=reports, rfcxml=statuses)
    assert len(reports) == 1, reports
    report = reports[0]
    metrics = report["metrics"]
    if metrics["https_start_window_us"]:
        receipt["https_starts_per_second"] = (metrics["https_requests"] - 1) * 1e6 / metrics["https_start_window_us"]
    assert metrics["rsync_files"] >= 1 and metrics["peak_rss_bytes"] > 0, metrics
    if failure:
        assert report["state"] == "failed" and "rsync and HTTPS TXT disagree" in report["error"], report
        receipt["status"] = "expected_failure"
    else:
        assert report["state"] == "ready" and int(report["generation_id"]) == active["active"], report
        assert report["expected_txt"] == report["verified_txt"] == metrics["rsync_files"] == len(texts)
        assert report["reused_txt"] == 0 and metrics["rsync_payload_bytes"] == sum(map(len, texts.values()))
        assert metrics["https_requests"] == 2 * len(texts) + 2 and metrics["minimum_start_spacing_us"] >= 1_000_000
        assert statuses["200"] > 0 and statuses["404"] > 0 and statuses["410"] == 1, statuses
        assert sum(statuses[status] for status in ("200", "404", "410")) == len(texts), statuses
        assert not any(statuses[key] for key in ("http_errors", "without_logged_response", "fetch_errors", "parse_errors"))
        receipt["storage_verification"] = verify_publication(project, receipt["database"], texts, report)
        receipt["status"] = "published_and_ready"


def trial(args, docker, driver, work, size, repeat, failure=False):
    label = "txt-mismatch" if failure else f"{size}-rfc-{repeat}"
    evidence = args.evidence / label
    evidence.mkdir()
    receipt = {"status": "error", "size": size, "repeat": repeat, "failure_probe": failure,
               "setup_seconds": {}, "workflow_seconds": {}, "milestones": {}}
    directory = work / label
    directory.mkdir()
    project = None
    try:
        for service in ("sync", "feed"):
            os.link(work / service, directory / service)
        project = Project(directory / f"{work.name}-{label}", driver, args.image_prefix,
                          docker, receipt["setup_seconds"])
        project.preprovision(size, directory)
        if failure:
            path = project.source / "https/rfc100.txt"
            path.write_bytes(path.read_bytes() + b"\nfixture disagreement\n")
        receipt["fixture_sha256"], texts = fixture_inventory(project)
        assert len(texts) == size
        run_workflow(project, receipt, texts, failure, evidence)
    except (AssertionError, KeyError, OSError, ValueError, subprocess.SubprocessError) as error:
        receipt["error"] = f"{type(error).__name__}: {error}"
        if project:
            try:
                receipt["collector_reports"], receipt["rfcxml"] = collect_logs(project, evidence)
            except (OSError, ValueError, subprocess.SubprocessError) as log_error:
                receipt["log_error"] = str(log_error)
    finally:
        if project:
            try:
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
    parser.add_argument("--sizes", type=int, choices=(6, 96), nargs="+", default=(6, 96))
    parser.add_argument("--repeats", type=int, choices=range(1, 6), default=3)
    args = parser.parse_args()
    args.evidence = args.evidence.resolve()
    if Path("/tmp") not in args.evidence.parents or args.evidence.exists():
        parser.error("--evidence must be a new directory beneath /tmp")
    if len(set(args.sizes)) != len(args.sizes):
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
        for size in args.sizes:
            for repeat in range(1, args.repeats + 1):
                summary["runs"].append(trial(args, docker, driver, work, size, repeat))
        summary["runs"].append(trial(args, docker, driver, work, 6, 1, failure=True))
    summary["median_t1_seconds"] = {}
    for size in args.sizes:
        runs = [r for r in summary["runs"] if r["size"] == size and r["status"] == "published_and_ready"]
        if runs:
            summary["median_t1_seconds"][size] = median(r["milestones"]["t1_mcp_read"]["seconds"] for r in runs)
            assert len({r["collector_reports"][0]["manifest_digest"] for r in runs}) == 1, runs
    (args.evidence / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return int(any(run["status"] == "error" for run in summary["runs"]))


if __name__ == "__main__":
    sys.exit(main())
