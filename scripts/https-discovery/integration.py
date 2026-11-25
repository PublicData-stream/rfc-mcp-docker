#!/usr/bin/env python3
"""Qualify HTTPS-first acquisition against private fixtures and schema 9."""

import argparse
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

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


def manual(project, scope="full", state="acquisition_complete", *, error=None, options=()):
    command = project.app("sync", "-T", *options, args=("--phase2-reconcile", f"--{scope}"))
    result = subprocess.run(command, env=project.env, text=True, capture_output=True, timeout=45)
    assert bool(result.returncode) == (state != "acquisition_complete"), result
    report = json.loads(result.stdout)
    assert report["state"] == state and report["publication_gate"] == "https_txt_audit_pending", report
    if error:
        assert error in report["error"], report
    if state == "acquisition_complete":
        assert report["selection_id"]
        assert report["catalog_before"]["digest"] == report["catalog_after"]["digest"], report
        counts = report["acquisition"]
        assert counts["txt_sealed"] == counts["xml_classified"] == report["expected_txt"], report
        assert counts["txt_audited"] == counts["membership_committed"] == 0, report
        assert counts["stage_peak_jobs"] <= 3, report
        assert report["metrics"]["https_peak"] <= 2, report
        assert report["metrics"]["minimum_start_spacing_us"] >= 1_000_000
    return report


def daemon_reports(project):
    logs = project.capture("logs", "--no-color", "--no-log-prefix", "sync")
    return [event for line in logs.splitlines() if line.startswith("{")
            and (event := json.loads(line)).get("event") == "acquisition"]


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
    phase1.failed(project.command("exec", "-T", "sync", "/phase6/sync", "--check-config"),
                  project.env, "migrate to schema v2")
    offset = len(project.capture("logs", "--no-log-prefix", "fixtures").splitlines())
    mode = project.source / "mode"
    mode.write_text("acquisition-feed")
    phase6.feed_data(project, [200, 201, 200])
    project.run("up", "-d", "--no-deps", "feed")
    before = wait_report(project, state="acquisition_deferred", hints_awaiting_catalog=2)
    job = before["job_id"]
    assert project.sql(f"SELECT count(*) FROM rfc.control_events WHERE job_id = {int(job)}") == "2"
    queued = project.sql(f"SELECT extract(epoch FROM started_at - created_at) >= 1 "
                         f"FROM rfc.sync_jobs WHERE id = {int(job)}")
    assert queued == "t", queued
    entries = "".join(phase2.catalog_entry(rfc, "Feed convergence") for rfc in (200, 201))
    catalog.write_text(initial.replace("</rfc-index>", entries + "</rfc-index>"))
    advance_retry(project, job, 1)
    wait_report(project, state="acquisition_deferred", job_id=job, hints_awaiting_mirror=2)
    for rfc in (200, 201):
        for transport in ("https", "rsync"):
            (project.source / transport / f"rfc{rfc}.txt").write_text("New fixture original\n")
    advance_retry(project, job, 2)
    after = wait_report(project, state="acquisition_complete", job_id=job, hints_present=2)
    wait_job(project, job, "acquisition_complete")
    traffic = sorted(starts(project, offset), key=lambda event: event[1])
    assert any(path == "/rfcatom.xml" for path, _ in traffic), traffic
    assert all(right[1] - left[1] >= 990_000_000 for left, right in zip(traffic, traffic[1:])), traffic
    mode.write_text("normal")
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
        for transport in ("https", "rsync"):
            (project.source / transport / f"rfc{rfc}.txt").unlink()
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
            assert proc.returncode != 0 and report["state"] == "acquisition_failed", (report, stderr)
            assert "cancelled" in report["error"]
        finally:
            release.touch()
            if proc.poll() is None:
                subprocess.run([*project.docker, "rm", "--force", name], capture_output=True, timeout=10)
            mode.write_text("normal")
    assert manual(project)["state"] == "acquisition_complete"
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
    manual(project, state="acquisition_failed", error="inventory lost previously accepted RFC 101")
    missing.write_bytes(sources[101])
    catalog = project.source / "catalog.xml"
    original_catalog = catalog.read_text()
    original_entry = phase2.catalog_entry(101, "Legacy bytes", day="<day>23</day>")
    assert original_entry in original_catalog
    catalog.write_text(original_catalog.replace(original_entry, ""))
    manual(project, state="acquisition_failed", error="catalog lost previously accepted RFC 101")
    catalog.write_text(original_catalog)

    digest = project.txt_digest(published)
    assert digest
    changed = bytes([sources[101][0] ^ 1]) + sources[101][1:]
    canonical_edit(project, digest, changed)
    try:
        manual(project, state="acquisition_failed", error="canonical TXT")
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


def starts(project, offset):
    rows = project.capture("logs", "--no-color", "--no-log-prefix", "fixtures").splitlines()
    events = []
    for line in rows[offset:]:
        if line.startswith("acquisition start "):
            _, _, path, stamp = line.split()
            events.append((path, int(stamp)))
    return events


def acquisition_faults(project, sources, spool):
    mode = project.source / "mode"
    for scenario, state, error in (
        ("bad-rfcxml", "acquisition_complete", None),
        ("acquisition-xml-status", "acquisition_complete", None),
        ("txt-html", "acquisition_failed", "unexpected HTTPS media type"),
        ("acquisition-oversized", "acquisition_failed", "HTTPS transfer exceeds byte limit"),
        ("acquisition-truncated", "acquisition_failed", "HTTPS transport"),
    ):
        mode.write_text(scenario)
        report = manual(project, state=state, error=error)
        if scenario == "bad-rfcxml":
            assert report["acquisition"]["xml_malformed"] == 1, report
        if scenario == "acquisition-xml-status":
            assert report["acquisition"]["xml_gone"] == 1, report
            assert report["acquisition"]["xml_retries_exhausted"] == 1, report
            assert report["acquisition"]["xml_not_found"] > 0, report
    required = project.source / "https" / "rfc100.txt"
    required.unlink()
    try:
        mode.write_text("normal")
        manual(project, state="acquisition_failed", error="404")
    finally:
        required.write_bytes(sources[100])

    # Hold the first response in each lane and require progress in the other
    # lane while the response remains held.
    for suffix, other in (("txt", "xml"), ("xml", "txt")):
        offset = len(project.capture("logs", "--no-log-prefix", "fixtures").splitlines())
        mode.write_text(f"acquisition-hold-{suffix}")
        release = project.source / "release-acquisition"
        operation = project.app("sync", "-T", args=("--phase2-reconcile", "--full"))
        with subprocess.Popen(operation, env=project.env, text=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE) as proc:
            try:
                def overlap():
                    current = starts(project, offset)
                    return (any(path == f"/rfc/rfc100.{suffix}" for path, _ in current)
                            and sum(path.startswith("/rfc/") and path.endswith(f".{other}")
                                    for path, _ in current) >= 2)
                phase6.wait(overlap, seconds=15)
                staged_name = "100-txt" if other == "txt" else "102-rfcxml"
                expected_bytes = (sources[100] if other == "txt" else
                                  (project.source / "https/rfc102.xml").read_bytes())
                path = spool / "acquisition" / staged_name
                def staged():
                    check = subprocess.run(["sudo", "-n", "cat", str(path)],
                                           capture_output=True, check=False, timeout=5)
                    if check.returncode == 0:
                        assert check.stdout == expected_bytes, staged_name
                        return True
                    return False
                phase6.wait(staged, seconds=15)
            finally:
                release.touch()
            stdout, stderr = proc.communicate(timeout=45)
            assert proc.returncode == 0, (stdout, stderr)
            release.unlink()
        assert json.loads(stdout)["acquisition"]["txt_sealed"] == len(sources)
    mode.write_text("normal")
    normal = manual(project)["acquisition"]["content_digest"]
    mode.write_text("overlap")
    assert manual(project)["acquisition"]["content_digest"] == normal
    mode.write_text("normal")
    manual(project, state="acquisition_failed", error="fixture RFCXML parser panic",
           options=("-e", "RFC_PHASE2_PANIC_XML=8174"))
    assert manual(project)["acquisition"]["content_digest"] == normal
    print("Acquisition faults and independent TXT/RFCXML progress passed", flush=True)


def restart_cooldown(project, spool):
    mode = project.source / "mode"
    for scenario in ("acquisition-delay", "acquisition-http-date"):
        offset = len(project.capture("logs", "--no-log-prefix", "fixtures").splitlines())
        mode.write_text(scenario)
        name = f"{project.name}-cooldown"
        command = project.app("sync", "-T", "--name", name, args=("--phase2-reconcile", "--full"))
        def deadline():
            result = subprocess.run(["sudo", "-n", "cat", str(spool / "cooldown")],
                                    capture_output=True, text=True, check=False, timeout=5)
            return int(result.stdout) if result.returncode == 0 else 0
        previous = deadline()
        with subprocess.Popen(command, env=project.env, text=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE) as proc:
            try:
                phase6.wait(lambda: deadline() > previous, seconds=18)
                duplicate = subprocess.run(
                    [*project.docker, "exec", name, "/phase6/sync", "--phase2-reconcile", "--full"],
                    env=project.env, capture_output=True, text=True, timeout=8,
                )
                assert duplicate.returncode and "owns the HTTPS deployment lease" in duplicate.stderr, duplicate
                phase6.run([*project.docker, "kill", "--signal=KILL", name])
                proc.communicate(timeout=10)
            finally:
                mode.write_text("acquisition-restart")
                if proc.poll() is None:
                    subprocess.run([*project.docker, "rm", "--force", name], capture_output=True, timeout=10)
        assert manual(project)["state"] == "acquisition_complete"
        current = starts(project, offset)
        retry_start = next(stamp for path, stamp in current if path == "/rfc/rfc100.txt")
        resumed = next(stamp for path, stamp in current if path == "/rfc-index.xml" and stamp > retry_start)
        assert resumed - retry_start >= 7_000_000_000, current
    mode.write_text("normal")
    print("Both Retry-After forms persisted across SIGKILL and blocked competing processes", flush=True)


def operator_runs(project):
    hold = project.source / "hold-catalog"
    hold.touch()
    def launch(scope):
        return subprocess.Popen(project.command("exec", "-T", "sync", "/phase6/sync",
                                "--phase2-reconcile", f"--{scope}"),
                                env=project.env, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
    try:
        project.restart_sync(PHASE6_NOTIFY_TARGET=f"{project.net}.34:8782")
        phase6.wait(lambda: project.sql("SELECT count(*) FROM rfc.sync_jobs WHERE state='running'") == "1")
        full = launch("full")
        sample = launch("sample")
        time.sleep(1)
        with launch("sample") as busy:
            stdout, stderr = busy.communicate(timeout=10)
            assert busy.returncode and json.loads(stdout)["state"] == "busy", (stdout, stderr)
    finally:
        hold.unlink(missing_ok=True)
    with full, sample:
        full_out, full_err = full.communicate(timeout=45)
        sample_out, sample_err = sample.communicate(timeout=45)
        assert full.returncode == sample.returncode == 0, (full_out, full_err, sample_out, sample_err)
    full_report, sample_report = map(json.loads, (full_out, sample_out))
    assert full_report["job_id"] and sample_report["job_id"] is None
    assert full_report["scope"] == "full" and sample_report["expected_txt"] == 3
    print("Daemon one-shot attachment, different-scope queue and busy response passed", flush=True)
    return full_report


def exercise(project, work, spool, incumbent_only):
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
    overlay.write_text(json.dumps({"services": {
        "sync": {
            "environment": {"RFC_SYNC_CONFIG_FILE": CONFIG_PATH, "RFC_DISCOVERY_FIXTURES": "1"},
            "volumes": [f"{config}:{CONFIG_PATH}:ro", f"{spool}:/rfc-https-spool"],
        },
        "feed": {"environment": {"RFC_DISCOVERY_FIXTURES": "1"}},
    }}))
    project.ingestion_overlay = overlay
    if incumbent_only:
        exercise_incumbent(project, work, overlay, sources, client)
        return
    empty = publication_snapshot(project)
    assert empty == "-1|0|0|0|0"
    fresh = manual(project)
    assert fresh["expected_txt"] == fresh["new_txt"] == len(sources)
    assert fresh["catalog_without_txt"] == 1
    assert fresh["metrics"]["https_requests"] == 2 + 2 * len(sources)
    assert fresh["acquisition"]["txt_downloaded"] == len(sources)
    assert fresh["acquisition"]["txt_contains_nul"] == fresh["acquisition"]["txt_invalid_utf8"] == 1
    print(f"Cold fixture reported {fresh['metrics']['https_requests']} HTTPS starts, "
          f"{fresh['acquisition']['staged_body_bytes']} staged artifact bytes, "
          f"and peak {fresh['acquisition']['stage_peak_jobs']} active artifact jobs", flush=True)
    assert manual(project, scope="sample")["expected_txt"] == 3

    mirror = project.source / "rsync"
    (mirror / "bcp0001.txt").write_text("ignored alias\n")
    (mirror / "rfc99999.txt").write_text("mirror-only\n")
    assert manual(project)["mirror_only_txt"] == 1
    (mirror / "rfc99999.txt").unlink()
    (mirror / "rfc0100.txt").write_text("unexpected alias\n")
    manual(project, state="acquisition_failed", error="noncanonical rsync inventory")
    (mirror / "rfc0100.txt").unlink()
    (mirror / "rfc1.txt").symlink_to("rfc100.txt")
    manual(project, state="acquisition_failed", error="rsync inventory")
    (mirror / "rfc1.txt").unlink()
    (project.source / "mode").write_text("catalog-drift")
    manual(project, state="acquisition_failed", error="catalog changed")
    (project.source / "mode").write_text("normal")
    cancel_inventory(project)
    acquisition_faults(project, sources, spool)
    valid = config.read_bytes()
    config.write_text("""schema_version = 2
[ingestion.https]
max_in_flight_requests = 1
[ingestion.pipeline]
txt_queue_items = 1
xml_queue_items = 1
result_queue_items = 1
max_active_documents = 1
parser_workers = 1
raw_payload_budget_bytes = 16777216
parse_budget_bytes = 134217728
""")
    try:
        tiny = manual(project)["acquisition"]
        assert tiny["content_digest"] == fresh["acquisition"]["content_digest"]
        assert tiny["stage_peak_jobs"] == tiny["txt_queue_peak"] == tiny["xml_queue_peak"] == 1, tiny
        config.write_bytes(valid.replace(b"max_accepted_bytes = 4294967296", b"max_accepted_bytes = 8192"))
        manual(project, state="acquisition_failed", error="accepted byte budget")
    finally:
        config.write_bytes(valid)

    fill = spool / "space-probe"
    try:
        phase6.run(["sudo", "-n", "fallocate", "-l", "1G", str(fill)])
        full_spool = manual(project, state="acquisition_failed", error="spool lacks free bytes")
        assert full_spool["metrics"]["https_requests"] == 0
    finally:
        phase6.run(["sudo", "-n", "rm", "-f", str(fill)])
    print("One-slot backpressure and byte and filesystem budgets passed", flush=True)
    restart_cooldown(project, spool)
    (project.source / "mode").write_text("hold-rsync-timeout")
    try:
        config.write_text("schema_version = 2\n[ingestion.discovery]\ninventory_timeout_seconds = 5\n")
        manual(project, state="acquisition_failed", error="deadline exceeded")
    finally:
        config.write_bytes((phase2.REPO / "deploy/ingestion.toml.example").read_bytes())
        (project.source / "mode").write_text("normal")

    daemon = operator_runs(project)
    wait_job(project, daemon["job_id"], "acquisition_complete")
    assert daemon["selection_id"] == fresh["selection_id"], (daemon, fresh)
    assert daemon["metrics"]["minimum_start_spacing_us"] >= 1_000_000
    assert publication_snapshot(project) == empty
    feed_convergence(project, original, initial, config)
    assert publication_snapshot(project) == empty

    exercise_incumbent(project, work, overlay, sources, client)
    print(f"Cold minimum start spacing {fresh['metrics']['minimum_start_spacing_us']} us", flush=True)


def exercise_incumbent(project, work, overlay, sources, client):
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
    mode = project.source / "mode"
    mode.write_text("acquisition-hourly")
    offset = len(project.capture("logs", "--no-log-prefix", "fixtures").splitlines())
    project.restart_sync(PHASE6_NOTIFY_TARGET=f"{project.net}.34:8782", PHASE6_HOURLY="12")
    warm = wait_report(project, state="acquisition_complete", publication_sequence=2)
    wait_job(project, warm["job_id"], "acquisition_complete")
    assert warm["local_https_txt_audit_pending"] == len(sources)
    assert warm["acquisition"]["txt_downloaded"] == 0
    assert warm["acquisition"]["txt_reused"] == len(sources)
    assert warm["acquisition"]["xml_reused"] > 0, warm
    assert project.admin("/ready").returncode == 0
    final = client.call("get", id=2119)[0]
    assert final["publication_sequence"] == "2"
    assert phase6.phase5.bytes_from(final["original"]) == sources[2119]
    assert publication_snapshot(project) == populated
    phase6.wait(lambda: sum(path == "/rfc-index.xml" for path, _ in starts(project, offset)) >= 3)
    assert project.sql("SELECT count(*) FROM rfc.sync_jobs WHERE state IN ('pending','running')") == "0"
    mode.write_text("normal")
    project.run("stop", "sync")
    rollback = phase1.capture(project.app(
        "sync", "-T", "--entrypoint", "/usr/local/bin/rfc-mcp-docker-sync",
        args=("--phase3-rollback", str(previous), "--expect-sequence", "2"),
    ), env=project.env)
    assert json.loads(rollback)["publication_sequence"] == "3"
    final = client.call("get", id=2119)[0]
    assert final["publication_sequence"] == "3" and final["generation_id"] == str(previous)
    assert phase6.phase5.bytes_from(final["original"]) == sources[2119]
    print(f"Warm acquisition, idle catalog check, and retained rollback passed. "
          f"Warm minimum start spacing {warm['metrics']['minimum_start_spacing_us']} us", flush=True)


def main():
    if not __debug__:
        raise RuntimeError("Phase 2 acquisition requires assertions")
    parser = argparse.ArgumentParser()
    parser.add_argument("--context", default="rootless")
    parser.add_argument("--image-prefix", required=True)
    parser.add_argument("--incumbent-only", action="store_true",
                        help="repeat warm reuse, the idle catalog check and retained rollback")
    args = parser.parse_args()
    docker = ["docker", "--context", args.context]
    with tempfile.TemporaryDirectory(prefix="rfc-https-discovery-", dir="/tmp") as directory:
        work = Path(directory)
        driver = work / "publication-driver"
        phase3.fixture_driver(docker, args.image_prefix, driver)
        phase6.fixture_binaries(docker, args.image_prefix, work)
        project = phase6.Project(work / work.name.lower(), driver, args.image_prefix, docker)
        project.prepare_phase6(work)
        with phase2.capped_stage(project, mebibytes=5120, inodes=65536) as spool:
            try:
                exercise(project, work, spool, args.incumbent_only)
            except Exception:
                project.run("logs", "--no-color", "--tail", "15", "sync", "fixtures", "feed")
                raise
            finally:
                project.close()


if __name__ == "__main__":
    main()
