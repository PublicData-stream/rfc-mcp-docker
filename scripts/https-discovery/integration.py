#!/usr/bin/env python3
"""Qualify HTTPS-first discovery against private fixtures and schema 9."""

import argparse
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("phase6", HERE.parent / "phase6" / "integration.py")
phase6 = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(phase6)
phase1, phase2, phase3 = phase6.phase1, phase6.phase2, phase6.phase3

CONFIG_PATH = "/etc/rfc-mcp/ingestion.toml"


def publication_snapshot(project):
    return project.sql("""
        SELECT coalesce(generation_id, -1), publication_sequence,
            (SELECT count(*) FROM rfc.generations),
            (SELECT count(*) FROM rfc.notification_outbox),
            (SELECT count(*) FROM rfc.source_observations)
        FROM rfc.active_generation
    """)


def manual(project, scope="full", state="discovery_complete", *, error=None, options=()):
    command = project.app("sync", "-T", *options, args=("--phase2-reconcile", f"--{scope}"))
    result = subprocess.run(command, env=project.env, text=True, capture_output=True, timeout=45)
    assert bool(result.returncode) == (state != "discovery_complete"), result
    report = json.loads(result.stdout)
    assert report["state"] == state and report["publication_gate"] == "https_txt_audit_pending", report
    assert report["metrics"]["txt"]["calls"] == report["metrics"]["rfcxml"]["calls"] == 0, report
    if error:
        assert error in report["error"], report
    if state == "discovery_complete":
        assert report["selection_id"]
        assert report["catalog_before"]["digest"] == report["catalog_after"]["digest"], report
        assert report["metrics"]["https_requests"] == 2
        assert report["metrics"]["minimum_start_spacing_us"] >= 1_000_000
    else:
        assert report["selection_id"] is None
    return report


def daemon_reports(project):
    logs = project.capture("logs", "--no-color", "--no-log-prefix", "sync")
    return [event for line in logs.splitlines() if line.startswith("{")
            and (event := json.loads(line)).get("event") == "discovery"]


def wait_report(project, **expected):
    return phase6.wait(lambda: next((row for row in reversed(daemon_reports(project))
                                     if all(row.get(key) == value for key, value in expected.items())), None))


def wait_job(project, job, outcome):
    phase6.wait(lambda: project.sql(
        f"SELECT outcome FROM rfc.sync_jobs WHERE id = {int(job)}"
    ) == outcome)


def advance_retry(project, job, retries):
    phase6.wait(lambda: project.sql(
        f"SELECT retries FROM rfc.sync_jobs WHERE id = {int(job)} AND next_attempt_at > clock_timestamp()"
    ) == str(retries))
    project.sql(f"UPDATE rfc.sync_jobs SET next_attempt_at = now() WHERE id = {int(job)}")


def feed_convergence(project, catalog, initial, config):
    # Changing the file in place makes the daemon's immutable startup snapshot observable.
    config.write_text("schema_version = 1\n")
    phase1.failed(project.command("exec", "-T", "sync", "/phase6/sync", "--phase2-reconcile", "--full"),
                  project.env, "migrate to schema v2")
    phase6.feed_data(project, [200, 201, 200])
    project.run("up", "-d", "--no-deps", "feed")
    before = wait_report(project, state="discovery_deferred", hints_awaiting_catalog=2)
    job = before["job_id"]
    assert project.sql(f"SELECT count(*) FROM rfc.control_events WHERE job_id = {int(job)}") == "2"
    queued = project.sql(f"SELECT extract(epoch FROM started_at - created_at) >= 1 "
                         f"FROM rfc.sync_jobs WHERE id = {int(job)}")
    assert queued == "t", queued
    entries = "".join(phase2.catalog_entry(rfc, "Feed convergence") for rfc in (200, 201))
    catalog.write_text(initial.replace("</rfc-index>", entries + "</rfc-index>"))
    advance_retry(project, job, 1)
    wait_report(project, state="discovery_deferred", job_id=job, hints_awaiting_mirror=2)
    for rfc in (200, 201):
        (project.source / "rsync" / f"rfc{rfc}.txt").write_text("New fixture original\n")
    advance_retry(project, job, 2)
    after = wait_report(project, state="discovery_complete", job_id=job, hints_present=2)
    wait_job(project, job, "discovery_complete")
    assert after["new_txt"] == before["expected_txt"] + 2
    epoch = phase6.identity(project)
    payloads = json.loads(project.sql("""
        SELECT json_agg(json_build_object(
            'event_id', id, 'observation_digest', encode(observation_digest, 'hex'),
            'observed_at', extract(epoch FROM observed_at)::bigint, 'reason', 'feed_publication'
        )) FROM rfc.control_events
    """))
    for payload in payloads:
        phase6.recover_event(project, epoch, payload, force_queue_busy=False)
    assert project.sql("SELECT count(*) FROM rfc.control_events") == "2"
    assert project.sql("SELECT count(*) FROM rfc.sync_jobs WHERE state IN ('pending','running')") == "0"
    project.run("stop", "feed", "sync")
    config.write_bytes((phase2.REPO / "deploy/ingestion.toml.example").read_bytes())
    for rfc in (200, 201):
        (project.source / "rsync" / f"rfc{rfc}.txt").unlink()
    catalog.write_text(initial)
    print("Feed hint coalescing, catalog and inventory convergence, and immutable v2 snapshot passed", flush=True)


def cancel_inventory(project):
    mode = project.source / "mode"
    mode.write_text("xml-overlap")
    release = project.source / "release-rsync"
    name = f"{project.name}-cancel"
    command = project.app("sync", "-T", "--name", name, args=("--phase2-reconcile", "--full"))
    with subprocess.Popen(command, env=project.env, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE) as proc:
        try:
            def have_rsync():
                if proc.poll() is not None:
                    stdout, stderr = proc.communicate()
                    raise AssertionError(("discovery exited before cancellation", stdout, stderr))
                return "fixture rsync held for XML" in project.capture("logs", "--no-color", "fixtures")
            phase6.wait(have_rsync, seconds=30)
            phase6.run([*project.docker, "kill", "--signal=TERM", name])
            stdout, stderr = proc.communicate(timeout=10)
            report = json.loads(stdout)
            assert proc.returncode != 0 and report["state"] == "discovery_failed", (report, stderr)
            assert "cancelled" in report["error"]
        finally:
            release.touch()
            if proc.poll() is None:
                subprocess.run([*project.docker, "rm", "--force", name], capture_output=True, timeout=10)
            mode.write_text("normal")
    assert manual(project)["state"] == "discovery_complete"
    print("SIGTERM cancelled the live rsync child and released the corpus fence", flush=True)


def canonical_edit(project, digest, data):
    path = f"/mirror/blobs/blake3/{digest[:2]}/{digest}"
    command = project.app("sync", "-T", "--entrypoint", "/bin/sh", args=(
        "-ec", 'chmod u+w "$1"; cat > "$1"; chmod a-w "$1"', "sh", path,
    ))
    subprocess.run(command, env=project.env, input=data, check=True, timeout=20)


def verify_populated(project, published, sequence, sources, work):
    before = publication_snapshot(project)
    count = len(sources)
    result = manual(project)
    assert result["local_https_txt_audit_pending"] == count and result["new_txt"] == 0, result
    candidate = project.sql("SELECT id FROM rfc.generations WHERE state='ready'")
    assert candidate
    for daemon_fixture in (False, True):
        options = ("--entrypoint", "/usr/local/bin/rfc-mcp-docker-sync") if not daemon_fixture else ()
        phase1.failed(project.app("sync", "-T", *options, args=(
            "--phase3-publish", candidate, "--expect-sequence", str(sequence),
        )), project.env, "publication is gated")
    missing = project.source / "rsync" / "rfc101.txt"
    missing.unlink()
    manual(project, state="discovery_failed", error="inventory lost previously accepted RFC 101")
    missing.write_bytes(sources[101])
    catalog = project.source / "catalog.xml"
    original_catalog = catalog.read_text()
    original_entry = phase2.catalog_entry(101, "Legacy bytes", day="<day>23</day>")
    assert original_entry in original_catalog
    catalog.write_text(original_catalog.replace(original_entry, ""))
    manual(project, state="discovery_failed", error="catalog lost previously accepted RFC 101")
    catalog.write_text(original_catalog)

    digest = project.txt_digest(published)
    assert digest
    changed = bytes([sources[101][0] ^ 1]) + sources[101][1:]
    canonical_edit(project, digest, changed)
    try:
        manual(project, state="discovery_failed", error="canonical TXT")
    finally:
        canonical_edit(project, digest, sources[101])

    audit = work / "audit"
    audit.mkdir()
    (audit / missing.name).write_bytes(sources[101])
    stamp = missing.stat()
    os.utime(audit / missing.name, ns=(stamp.st_atime_ns, stamp.st_mtime_ns))
    inventory_before = manual(project)["inventory_digest"]
    missing.write_bytes(changed)
    os.utime(missing, ns=(stamp.st_atime_ns, stamp.st_mtime_ns))
    unchanged_metadata = manual(project)
    assert unchanged_metadata["inventory_digest"] == inventory_before
    assert unchanged_metadata["inventory_size_changes"] == 0
    assert unchanged_metadata["local_https_txt_audit_pending"] == count
    dry_run = project.app("sync", "-T", "-v", f"{audit}:/audit:ro", "--entrypoint", "/usr/bin/rsync",
                          args=("--dry-run", "--checksum", "--checksum-choice=auto", "--itemize-changes",
                                "rsync://fixtures:8873/rfc/rfc101.txt", "/audit/"))
    output = phase1.capture(dry_run, env=project.env)
    assert output.startswith(">fc") and "rfc101.txt" in output, output
    assert (audit / missing.name).read_bytes() == sources[101]
    missing.write_bytes(sources[101])
    assert publication_snapshot(project) == before
    print("Incumbent loss, corrupt CAS, promotion gate, and same-size checksum control passed", flush=True)


def exercise(project, work):
    sources = phase2.prepare(project, include_nul=True)
    original = project.source / "catalog.xml"
    initial = original.read_text()
    phase6.feed_data(project, [])
    (project.source / "feed-mode").write_text("normal")
    project.start()
    project.migrate()
    project.run("up", "-d", "--wait", "fixtures", "control-probe", timeout=35)
    client = project.start_mcp()
    phase6.wait(lambda: "503" in project.admin("/ready").stderr)

    config = work / "ingestion.toml"
    config.write_bytes((phase2.REPO / "deploy/ingestion.toml.example").read_bytes())
    overlay = work / "discovery-compose.json"
    overlay.write_text(json.dumps({"services": {"sync": {
        "environment": {"RFC_SYNC_CONFIG_FILE": CONFIG_PATH, "RFC_DISCOVERY_FIXTURES": "1"},
        "volumes": [f"{config}:{CONFIG_PATH}:ro"],
    }}}))
    project.ingestion_overlay = overlay
    empty = publication_snapshot(project)
    assert empty == "-1|0|0|0|0"
    fresh = manual(project)
    assert fresh["expected_txt"] == fresh["new_txt"] == len(sources)
    assert fresh["catalog_without_txt"] == 1
    assert manual(project, scope="sample")["expected_txt"] == 3

    mirror = project.source / "rsync"
    (mirror / "bcp0001.txt").write_text("ignored alias\n")
    (mirror / "rfc99999.txt").write_text("mirror-only\n")
    assert manual(project)["mirror_only_txt"] == 1
    (mirror / "rfc99999.txt").unlink()
    (mirror / "rfc0100.txt").write_text("unexpected alias\n")
    manual(project, state="discovery_failed", error="noncanonical rsync inventory")
    (mirror / "rfc0100.txt").unlink()
    (mirror / "rfc1.txt").symlink_to("rfc100.txt")
    manual(project, state="discovery_failed", error="rsync inventory")
    (mirror / "rfc1.txt").unlink()
    (project.source / "mode").write_text("catalog-drift")
    manual(project, state="discovery_failed", error="catalog changed")
    (project.source / "mode").write_text("normal")
    cancel_inventory(project)
    (project.source / "mode").write_text("hold-rsync-timeout")
    try:
        config.write_text("schema_version = 2\n[ingestion.discovery]\ninventory_timeout_seconds = 5\n")
        manual(project, state="discovery_failed", error="deadline exceeded")
    finally:
        config.write_bytes((phase2.REPO / "deploy/ingestion.toml.example").read_bytes())
        (project.source / "mode").write_text("normal")

    project.restart_sync(PHASE6_NOTIFY_TARGET=f"{project.net}.34:8782")
    daemon = wait_report(project, state="discovery_complete")
    wait_job(project, daemon["job_id"], "discovery_complete")
    assert daemon["selection_id"] == fresh["selection_id"], (daemon, fresh)
    assert daemon["metrics"]["minimum_start_spacing_us"] >= 1_000_000
    assert publication_snapshot(project) == empty
    feed_convergence(project, original, initial, config)
    assert publication_snapshot(project) == empty

    # Seed historical evidence via the private legacy fixture binary.
    del project.ingestion_overlay
    def legacy_candidate():
        return int(json.loads(project.invoke("sync", "full").splitlines()[-1])["generation_id"])

    previous = legacy_candidate()
    project.activate(previous, 0)
    published = legacy_candidate()
    project.activate(published, 1)
    legacy_candidate()
    project.sql("UPDATE rfc.notification_outbox SET delivery_state = 'delivered', delivered_at = now()")
    phase6.wait(lambda: project.admin("/ready").returncode == 0)
    client.schemas = {"get": phase6.phase5.Draft202012Validator(next(
        tool["outputSchema"] for tool in client.rpc("tools/list")[0]["tools"] if tool["name"] == "get"
    ))}
    document = client.call("get", id=2119)[0]
    assert document["publication_sequence"] == "2"
    assert phase6.phase5.bytes_from(document["original"]) == sources[2119]
    project.ingestion_overlay = overlay
    populated = publication_snapshot(project)
    verify_populated(project, published, 2, sources, work)
    project.restart_sync(PHASE6_NOTIFY_TARGET=f"{project.net}.34:8782")
    warm = wait_report(project, state="discovery_complete", publication_sequence=2)
    wait_job(project, warm["job_id"], "discovery_complete")
    assert warm["local_https_txt_audit_pending"] == len(sources)
    assert project.admin("/ready").returncode == 0
    final = client.call("get", id=2119)[0]
    assert final["publication_sequence"] == "2"
    assert phase6.phase5.bytes_from(final["original"]) == sources[2119]
    assert publication_snapshot(project) == populated
    project.run("stop", "sync")
    rollback = phase1.capture(project.app(
        "sync", "-T", "--entrypoint", "/usr/local/bin/rfc-mcp-docker-sync",
        args=("--phase3-rollback", str(previous), "--expect-sequence", "2"),
    ), env=project.env)
    assert json.loads(rollback)["publication_sequence"] == "3"
    final = client.call("get", id=2119)[0]
    assert final["publication_sequence"] == "3" and final["generation_id"] == str(previous)
    assert phase6.phase5.bytes_from(final["original"]) == sources[2119]
    print(f"Phase 1 discovery and retained rollback passed. Cold start spacing {fresh['metrics']['minimum_start_spacing_us']} us; "
          f"warm start spacing {warm['metrics']['minimum_start_spacing_us']} us", flush=True)


def main():
    if not __debug__:
        raise RuntimeError("Phase 1 discovery requires assertions")
    parser = argparse.ArgumentParser()
    parser.add_argument("--context", default="rootless")
    parser.add_argument("--image-prefix", required=True)
    args = parser.parse_args()
    docker = ["docker", "--context", args.context]
    with tempfile.TemporaryDirectory(prefix="rfc-https-discovery-", dir="/tmp") as directory:
        work = Path(directory)
        driver = work / "publication-driver"
        phase3.fixture_driver(docker, args.image_prefix, driver)
        phase6.fixture_binaries(docker, args.image_prefix, work)
        project = phase6.Project(work / work.name.lower(), driver, args.image_prefix, docker)
        try:
            project.prepare_phase6(work)
            exercise(project, work)
        except Exception:
            project.run("logs", "--no-color", "--tail", "15", "sync", "fixtures", "feed")
            raise
        finally:
            project.close()


if __name__ == "__main__":
    main()
