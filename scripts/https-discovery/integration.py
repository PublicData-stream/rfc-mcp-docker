#!/usr/bin/env python3
"""Qualify durable HTTPS processing and rsync audit against private fixtures."""

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


class Project(phase6.Project):
    def prepare_phase6(self, work):
        super().prepare_phase6(work)
        self.manual_network = work / "manual-network.yml"
        self.manual_network.write_text("""services:
  sync:
    networks: !override
      db_write: {}
      fixture_source: {}
  serve:
    networks: !override
      db_read: {}
""")

    def app(self, service, *options, **kwargs):
        command = super().app(service, *options, **kwargs)
        if service in ("sync", "serve"):
            index = command.index("-p")
            return [*command[:index], "-f", str(self.manual_network), *command[index:]]
        return command


def publication_snapshot(project):
    return project.sql("""
        SELECT coalesce(generation_id, -1), publication_sequence,
            (SELECT count(*) FROM rfc.generations WHERE state IN ('published','retained')),
            (SELECT count(*) FROM rfc.notification_outbox)
        FROM rfc.active_generation
    """)


def manual(project, scope="full", state="ready", *, error=None, options=()):
    command = project.app("sync", "-T", *options, args=("--phase2-reconcile", f"--{scope}"))
    result = subprocess.run(command, env=project.env, text=True, capture_output=True, timeout=115)
    assert bool(result.returncode) == (state != "ready"), result
    report = json.loads(result.stdout)
    assert report["state"] == state and report["publication_gate"] == "https_txt_audit_required", report
    if error:
        assert error in report["error"], report
    if state == "ready":
        assert report["generation_id"] and report["manifest_digest"] and report["promotion_deadline_us"]
        assert report["selection_id"]
        assert report["catalog_before"]["digest"] == report["catalog_after"]["digest"], report
        counts = report["acquisition"]
        assert counts["txt_sealed"] == counts["xml_classified"] == report["expected_txt"], report
        assert counts["membership_committed"] == report["expected_txt"] and counts["txt_audit_pending"] == 0, report
        assert counts["documents_joined"] == counts["txt_audited"] == report["expected_txt"], report
        assert report["audit"]["matched"] + report["audit"]["repaired"] == report["expected_txt"], report
        assert counts["processing_digest"], report
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
    after = wait_report(project, state="published", job_id=job, hints_present=2)
    wait_job(project, job, "published")
    traffic = sorted(starts(project, offset), key=lambda event: event[1])
    assert any(path == "/rfcatom.xml" for path, _ in traffic), traffic
    assert all(right[1] - left[1] >= 990_000_000 for left, right in zip(traffic, traffic[1:])), traffic
    mode.write_text("normal")
    assert after["new_txt"] == 2 and after["expected_txt"] == before["expected_txt"] + 2
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
    assert manual(project)["state"] == "ready"
    print("SIGTERM cancelled the live rsync child and released the corpus fence", flush=True)


def canonical_edit(project, digest, data):
    path = f"/mirror/blobs/blake3/{digest[:2]}/{digest}"
    command = project.app("sync", "-T", "--entrypoint", "/bin/sh", args=(
        "-ec", 'chmod u+w "$1"; cat > "$1"; chmod a-w "$1"', "sh", path,
    ))
    subprocess.run(command, env=project.env, input=data, check=True, timeout=20)


def assert_capture_only_gate(project, generation):
    query = f"""
        BEGIN;
        SET LOCAL ROLE rfc_sync;
        INSERT INTO rfc.source_observations
            (identity,kind,source_url,observed_at,fetched_at,raw_digest,https_verification,capture_only)
        SELECT decode(repeat('11',32),'hex'), 'txt', o.source_url, o.observed_at, o.fetched_at,
               o.raw_digest, 'verified', true
        FROM rfc.generation_documents d JOIN rfc.source_observations o ON o.identity=d.txt_observation
        WHERE d.generation_id={int(generation)} AND d.rfc_id=2119;
        WITH building AS (
            INSERT INTO rfc.generations (parser_version,index_version)
            SELECT parser_version,index_version FROM rfc.generations WHERE id={int(generation)}
            RETURNING id
        )
        INSERT INTO rfc.generation_documents
            (generation_id,rfc_id,document_version,metadata_identity,catalog_observation,txt_observation,title)
        SELECT building.id,rfc_id,document_version,metadata_identity,catalog_observation,
               decode(repeat('11',32),'hex'),title
        FROM building,rfc.generation_documents WHERE generation_id={int(generation)} AND rfc_id=2119;
        ROLLBACK;
    """
    phase1.failed([*project.psql(), "-c", query], project.env,
                  "HTTPS TXT capture alone does not satisfy the audit gate")


def starts(project, offset):
    rows = project.capture("logs", "--no-color", "--no-log-prefix", "fixtures").splitlines()
    events = []
    for line in rows[offset:]:
        if line.startswith("acquisition start "):
            _, _, path, stamp = line.split()
            events.append((path, int(stamp)))
    return events


def clear_checkpoints(project):
    project.sql("DELETE FROM rfc.ingestion_checkpoints WHERE evidence_state <> 'legacy'")


def checkpoint_rows(project):
    return project.sql("""
        SELECT c.rfc_id, c.kind, encode(c.observation,'hex'), encode(o.raw_digest,'hex'),
               o.observed_at, o.fetched_at, c.expires_at
        FROM rfc.ingestion_checkpoints c JOIN rfc.source_observations o ON o.identity=c.observation
        WHERE c.scope='full' AND c.evidence_state='https' ORDER BY c.rfc_id, c.kind
    """)


def verify_cas(project, sources):
    entries = project.sql("""
        SELECT c.rfc_id, encode(o.raw_digest,'hex'), o.verification_method, d.encoding, d.sealed
        FROM rfc.generation_documents c JOIN rfc.source_observations o ON o.identity=c.txt_observation
        JOIN rfc.document_versions d ON d.identity=c.document_version
        WHERE c.generation_id=(SELECT max(g.id) FROM rfc.generations g
            JOIN rfc.generation_ingestion i ON i.generation_id=g.id
            WHERE i.scope='full' AND g.state IN ('ready','published','retained')) ORDER BY c.rfc_id
    """).splitlines()
    assert len(entries) == len(sources), entries
    for row in entries:
        number, digest, method, encoding, sealed = row.split("|")
        rfc = int(number)
        assert method == "https_capture" and sealed == "t", row
        assert encoding == ("bytes" if rfc in (101, phase2.NUL_RFC) else "utf8"), row
        assert project.canonical(digest).read_bytes() == sources[rfc], row


def checkpoint_restarts(project, sources, fresh):
    crash_at(project, "before_readiness")
    historical = checkpoint_rows(project)
    warm = manual(project)
    counts = warm["acquisition"]
    assert counts["checkpoint_txt"] == len(sources) and counts["checkpoint_xml"] == 3, warm
    assert warm["metrics"]["https_requests"] == 5, warm
    assert counts["processing_digest"] == fresh["acquisition"]["processing_digest"]
    assert historical and not checkpoint_rows(project)
    verify_cas(project, sources)
    discard_ready(project)

    # Run crash injections with one active artifact so the first commit is
    # a TXT checkpoint even when a bodyless XML result could otherwise win.
    crash_config = project.directory / "checkpoint-crash.toml"
    crash_config.write_text("""schema_version = 2
[ingestion.https]
max_in_flight_requests = 1
[ingestion.pipeline]
max_active_documents = 1
parser_workers = 1
""")
    crash_config.chmod(0o444)
    for boundary in ("before_https_cas_install", "after_blob_install",
                     "before_https_checkpoint_commit", "after_https_checkpoint_commit",
                     "before_https_catalog_validation", "after_https_catalog_validation"):
        clear_checkpoints(project)
        recover(project)
        command = project.app("sync", "-T", "-e", f"RFC_PHASE3_CRASH={boundary}",
                              "-e", "RFC_SYNC_CONFIG_FILE=/etc/rfc-mcp/crash.toml",
                              "-v", f"{crash_config}:/etc/rfc-mcp/crash.toml:ro",
                              args=("--phase2-reconcile", "--full"))
        result = subprocess.run(command, env=project.env, capture_output=True, text=True, timeout=75)
        assert result.returncode == 86 and f"crash at {boundary}" in result.stderr, result
        committed = checkpoint_rows(project)
        assert bool(committed) == (boundary in ("after_https_checkpoint_commit",
                                                "before_https_catalog_validation",
                                                "after_https_catalog_validation")), (boundary, committed)
        resumed = manual(project)
        assert resumed["acquisition"]["processing_digest"] == counts["processing_digest"], resumed
        assert not checkpoint_rows(project)
        proofs = project.sql(f"""
            SELECT rfc_id,'txt',encode(txt_observation,'hex') FROM rfc.generation_documents
            WHERE generation_id={int(resumed['generation_id'])}
            UNION ALL SELECT rfc_id,'rfcxml',encode(rfcxml_observation,'hex') FROM rfc.generation_documents
            WHERE generation_id={int(resumed['generation_id'])} AND rfcxml_observation IS NOT NULL
        """).splitlines()
        assert all("|".join(row.split("|")[:3]) in proofs for row in committed.splitlines())
        if boundary != "after_https_catalog_validation":
            discard_ready(project)

    crash_at(project, "before_readiness")
    digest = project.sql("""
        SELECT encode(o.raw_digest,'hex') FROM rfc.ingestion_checkpoints c
        JOIN rfc.source_observations o ON o.identity=c.observation
        WHERE c.scope='full' AND c.rfc_id=100 AND c.kind='txt'
    """)
    recover(project)
    verify_cas(project, sources)
    canonical_edit(project, digest, b"wrong bytes")
    try:
        manual(project, state="acquisition_failed", error="RFC 100 checkpoint failed verification")
        assert project.sql("""
            SELECT count(*) FROM rfc.ingestion_checkpoints
            WHERE scope='full' AND rfc_id=100 AND kind='txt'
        """) == "0"
    finally:
        canonical_edit(project, digest, sources[100])

    project.sql("UPDATE rfc.ingestion_checkpoints SET expires_at=now()-interval '1 second'")
    discard_ready(project)
    recover(project)
    assert not checkpoint_rows(project)
    assert project.sql("SELECT count(*) FROM rfc.document_versions") == "0"
    assert project.sql("SELECT count(*) FROM rfc.blobs") == "0"
    again = manual(project)
    assert again["acquisition"]["txt_downloaded"] == len(sources)
    print(f"Crash boundaries, exact CAS, expiry and GC passed. Warm HTTPS starts "
          f"{warm['metrics']['https_requests']} versus cold {fresh['metrics']['https_requests']}", flush=True)


def recover(project):
    return phase1.capture(project.app("sync", "-T", args=("--phase3-recover",)), env=project.env,
                          timeout=45)


def discard_ready(project):
    for generation in project.sql("SELECT id FROM rfc.generations WHERE state='ready'").splitlines():
        phase1.capture(project.app("sync", "-T", args=("--phase3-discard", generation)), env=project.env)


def acquisition_faults(project, sources, spool):
    mode = project.source / "mode"
    for scenario, state, error in (
        ("bad-rfcxml", "ready", None),
        ("acquisition-xml-status", "ready", None),
        ("txt-html", "acquisition_failed", "unexpected HTTPS media type"),
        ("acquisition-oversized", "acquisition_failed", "HTTPS transfer exceeds byte limit"),
        ("acquisition-truncated", "acquisition_failed", "HTTPS transport"),
    ):
        clear_checkpoints(project)
        mode.write_text(scenario)
        report = manual(project, state=state, error=error)
        if scenario == "bad-rfcxml":
            assert report["acquisition"]["xml_malformed"] == 1, report
            assert project.sql("""
                SELECT count(*) FROM rfc.generation_source_evidence e
                JOIN rfc.source_observations o ON o.identity=e.rfcxml_observation
                WHERE e.rfcxml_outcome='malformed' AND o.verification_method='https_capture'
            """) == "1"
        if scenario == "acquisition-xml-status":
            assert report["acquisition"]["xml_gone"] == 1, report
            assert report["acquisition"]["xml_retries_exhausted"] == 1, report
            assert report["acquisition"]["xml_not_found"] > 0, report
    clear_checkpoints(project)
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
        clear_checkpoints(project)
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
                rfc = int(staged_name.split("-")[0])
                artifact = "txt" if other == "txt" else "rfcxml"
                phase6.wait(lambda: project.sql(f"""
                    SELECT count(*) FROM rfc.ingestion_checkpoints
                    WHERE scope='full' AND rfc_id={rfc} AND kind='{artifact}' AND evidence_state='https'
                """) == "1", seconds=10)
            finally:
                release.touch()
            stdout, stderr = proc.communicate(timeout=45)
            assert proc.returncode == 0, (stdout, stderr)
            release.unlink()
        assert json.loads(stdout)["acquisition"]["txt_sealed"] == len(sources)
    mode.write_text("normal")
    clear_checkpoints(project)
    normal = manual(project)["acquisition"]["processing_digest"]
    clear_checkpoints(project)
    mode.write_text("overlap")
    assert manual(project)["acquisition"]["processing_digest"] == normal
    mode.write_text("normal")
    clear_checkpoints(project)
    manual(project, state="acquisition_failed", error="fixture RFCXML parser panic",
           options=("-e", "RFC_PHASE2_PANIC_XML=8174"))
    assert manual(project)["acquisition"]["processing_digest"] == normal
    print("Acquisition faults and independent TXT/RFCXML progress passed", flush=True)


def slow_checkpoint_commit(project):
    clear_checkpoints(project)
    mode = project.source / "mode"
    offset = len(project.capture("logs", "--no-color", "--no-log-prefix", "fixtures").splitlines())
    project.sql("""
        BEGIN;
        SET LOCAL ROLE rfc_owner;
        CREATE TABLE rfc.fixture_checkpoint_wait (started_at timestamptz, finished_at timestamptz);
        CREATE FUNCTION rfc.fixture_delay_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE started timestamptz;
        BEGIN
            IF NEW.rfc_id=100 AND NEW.kind='txt' THEN
                started := clock_timestamp();
                PERFORM pg_sleep(4);
                INSERT INTO rfc.fixture_checkpoint_wait VALUES (started, clock_timestamp());
            END IF;
            RETURN NEW;
        END $$;
        CREATE TRIGGER fixture_delay_checkpoint BEFORE INSERT ON rfc.ingestion_checkpoints
        FOR EACH ROW EXECUTE FUNCTION rfc.fixture_delay_checkpoint();
        COMMIT;
    """)
    try:
        mode.write_text("acquisition-db-delay")
        report = manual(project)
        assert report["acquisition"]["stage_peak_jobs"] <= 3, report
        started, finished = map(int, project.sql("""
            SELECT (extract(epoch FROM started_at)*1000000000)::bigint,
                   (extract(epoch FROM finished_at)*1000000000)::bigint
            FROM rfc.fixture_checkpoint_wait
        """).split("|"))
        assert finished - started >= 4_000_000_000
        logs = project.capture("logs", "--no-color", "--no-log-prefix", "fixtures").splitlines()
        requests = [
            (path, int(stamp))
            for line in logs[offset:] if line.startswith("checkpoint fixture request ")
            for _, _, _, path, stamp in [line.split()]
        ]
        assert any(path.startswith("/rfc/") and started < stamp < finished
                   for path, stamp in requests), (started, finished, requests)
    finally:
        mode.write_text("normal")
        project.sql("""
            DROP TRIGGER fixture_delay_checkpoint ON rfc.ingestion_checkpoints;
            DROP FUNCTION rfc.fixture_delay_checkpoint();
            DROP TABLE rfc.fixture_checkpoint_wait
        """)
    print("A delayed first checkpoint allowed later HTTPS progress within the active window", flush=True)


def restart_cooldown(project, spool):
    mode = project.source / "mode"
    for scenario in ("acquisition-delay", "acquisition-http-date"):
        clear_checkpoints(project)
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
        assert manual(project)["state"] == "ready"
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


def history(project):
    return (
        project.sql("""
            SELECT identity, raw_digest, verification_method, source_url, observed_at, fetched_at
            FROM rfc.source_observations ORDER BY identity
        """),
        project.sql("SELECT id, manifest_bytes, manifest_digest FROM rfc.generations ORDER BY id"),
        publication_snapshot(project),
    )


def fast_audit_config(work, *, batch_size=512, session_timeout=20, retry_seconds=1):
    config = work / f"audit-{batch_size}-{session_timeout}-{retry_seconds}.toml"
    config.write_text(f"""schema_version = 2
[ingestion.revalidation]
batch_size = {batch_size}
max_attempts = 2
retry_backoff_seconds = [{retry_seconds}]
session_timeout_seconds = {session_timeout}
""")
    config.chmod(0o444)
    return ("-e", "RFC_SYNC_CONFIG_FILE=/etc/rfc-mcp/audit.toml",
            "-v", f"{config}:/etc/rfc-mcp/audit.toml:ro")


def crash_at(project, boundary, options=()):
    command = project.app("sync", "-T", *options, "-e", f"RFC_PHASE3_CRASH={boundary}",
                          args=("--phase2-reconcile", "--full"))
    result = subprocess.run(command, env=project.env, capture_output=True, text=True, timeout=100)
    assert result.returncode == 86 and f"crash at {boundary}" in result.stderr, result


def exercise_audit(project, work, sources, fresh, audit):
    assert fresh["audit"]["matched"] == len(sources)
    assert fresh["audit"]["sessions"] == 1
    assert fresh["audit"]["repair_attempts"] == 0
    assert fresh["metrics"]["rsync_checksum_hits"] == len(sources)
    assert fresh["metrics"]["rsync_transferred_files"] == 0
    assert fresh["metrics"]["rsync_transferred_payload_bytes"] == 0
    verify_cas(project, sources)
    publication_command(project, fresh)
    fast = fast_audit_config(work)
    one = fast_audit_config(work, batch_size=1)
    mode = project.source / "mode"
    https = project.source / "https/rfc100.txt"
    rsync = project.source / "rsync/rfc100.txt"
    snapshot = publication_snapshot(project)

    # A restart after the complete dry run recovers each match without treating
    # the next independently completed reconciliation as part of the old run.
    crash_at(project, "after_rsync_audit_receipt")
    resumed = manual(project)
    assert resumed["audit"]["reused"] == len(sources) and resumed["audit"]["sessions"] == 0, resumed
    assert manual(project)["audit"]["reused"] == 0

    for scenario in ("overlap", "missing", "mutation", "timeout", "size"):
        if scenario != "size":
            clear_checkpoints(project)
        tag = f"acquisition-audit-{scenario}"
        release = project.source / "release-audit"
        release.unlink(missing_ok=True)
        mode.write_text(tag)
        offset = len(project.capture("logs", "--no-log-prefix", "fixtures").splitlines())
        options = one if scenario == "overlap" else fast
        if scenario == "timeout":
            options = fast_audit_config(work, session_timeout=5)
        command = project.app("sync", "-T", *options, args=("--phase2-reconcile", "--full"))
        with subprocess.Popen(command, env=project.env, text=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE) as collector:
            try:
                phase6.wait(lambda: f"fixture audit held {tag}" in project.capture(
                    "logs", "--no-log-prefix", "fixtures"), seconds=40)
                if scenario == "overlap":
                    before = sum(path.endswith(".xml") for path, _ in starts(project, offset))
                    phase6.wait(lambda: sum(path.endswith(".xml") for path, _ in starts(project, offset))
                                > before, seconds=12)
                elif scenario == "missing":
                    rsync.unlink()
                elif scenario == "mutation":
                    staged = audit / "full/snapshot/txt/rfc100.txt"
                    subprocess.run(["sudo", "-n", "dd", "of=" + str(staged), "conv=notrunc"],
                                   input=b"x", check=True, capture_output=True, timeout=5)
                elif scenario == "timeout":
                    time.sleep(6)
                elif scenario == "size":
                    changed_size = sources[100] + b"\nExtra original bytes.\n"
                    rsync.write_bytes(changed_size)
                    https.write_bytes(changed_size)
            finally:
                release.touch()
            output, error = collector.communicate(timeout=90)
        report = json.loads(output)
        if scenario in ("overlap", "timeout"):
            assert collector.returncode == 0 and report["audit"]["matched"] == len(sources), (report, error)
            if scenario == "timeout":
                assert report["audit"]["failed_sessions"] == 1, report
        elif scenario == "size":
            assert collector.returncode == 0 and report["audit"]["repaired"] == 1, (report, error)
            assert report["metrics"]["rsync_transferred_files"] == 1, report
            assert report["metrics"]["rsync_transferred_payload_bytes"] == len(changed_size), report
            verify_cas(project, sources | {100: changed_size})
            rsync.write_bytes(sources[100])
            https.write_bytes(sources[100])
        else:
            assert collector.returncode != 0 and report["audit"]["matched"] == 0, (report, error)
            if scenario == "mutation":
                assert "audit snapshot changed" in report["error"], report
        if scenario == "missing":
            rsync.write_bytes(sources[100])
        mode.write_text("normal")
    mode.write_text("acquisition-audit-truncate")
    truncated = manual(project, state="acquisition_failed", error="rsync audit", options=fast)
    assert truncated["audit"]["failed_sessions"] == 2 and truncated["audit"]["matched"] == 0, truncated

    # Give the remote file newer metadata only, then change both sources while
    # preserving the size and timestamp to force checksum-based repair.
    mode.write_text("normal")
    remote_stamp = time.time_ns()
    os.utime(rsync, ns=(remote_stamp, remote_stamp))
    metadata = manual(project, options=fast)
    assert metadata["audit"]["matched"] == len(sources), metadata
    changed = bytes([sources[100][0] ^ 1]) + sources[100][1:]
    https.write_bytes(changed)
    rsync.write_bytes(changed)
    os.utime(rsync, ns=(remote_stamp, remote_stamp))
    crash_at(project, "before_rsync_repair_capture", fast)
    crash_at(project, "after_rsync_repair_pair", fast)
    repaired = manual(project, options=fast)
    assert repaired["audit"]["repaired"] == 1 and repaired["audit"]["repair_attempts"] == 0, repaired
    assert repaired["inventory_digest"] == metadata["inventory_digest"], repaired
    verify_cas(project, sources | {100: changed})
    assert repaired["acquisition"]["processing_digest"] != fresh["acquisition"]["processing_digest"]

    # A repair-specific HTTP 429 applies to the retry through the same gate.
    second = bytes([changed[0] ^ 2]) + changed[1:]
    rsync.write_bytes(second)
    https.write_bytes(second)
    os.utime(rsync, ns=(remote_stamp, remote_stamp))
    mode.write_text("acquisition-audit-cooldown")
    offset = len(project.capture("logs", "--no-log-prefix", "fixtures").splitlines())
    cooldown = manual(project, options=fast)
    attempts = [stamp for path, stamp in starts(project, offset) if path == "/rfc/rfc100.txt"]
    assert cooldown["audit"]["repaired"] == 1 and cooldown["audit"]["repair_attempts"] == 2, cooldown
    assert len(attempts) == 2 and attempts[1] - attempts[0] >= 2_990_000_000, attempts
    verify_cas(project, sources | {100: second})

    # Start a divergent pair, then let the HTTPS source converge after the
    # original pair is durably quarantined.
    remote_stamp -= 5_000_000_000
    rsync.write_bytes(changed)
    os.utime(rsync, ns=(remote_stamp, remote_stamp))
    mode.write_text("normal")
    convergence_config = fast_audit_config(work, retry_seconds=4)
    operation = project.app("sync", "-T", *convergence_config, args=("--phase2-reconcile", "--full"))
    with subprocess.Popen(operation, env=project.env, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE) as collector:
        def quarantined():
            if collector.poll() is not None:
                raise AssertionError(("repair finished before the fixture diverged", collector.communicate()))
            return subprocess.run(["sudo", "-n", "find", str(audit / "quarantine"), "-name", "receipt",
                                   "-print", "-quit"], text=True, capture_output=True,
                                  check=True, timeout=5).stdout
        phase6.wait(quarantined, seconds=25)
        https.write_bytes(changed)
        output, error = collector.communicate(timeout=40)
    convergence = json.loads(output)
    assert collector.returncode == 0 and convergence["audit"]["repair_attempts"] == 2, (convergence, error)
    assert convergence["audit"]["repaired"] == 1, convergence
    verify_cas(project, sources | {100: changed})

    remote_stamp -= 5_000_000_000
    rsync.write_bytes(second)
    os.utime(rsync, ns=(remote_stamp, remote_stamp))
    mode.write_text("normal")
    divergent = manual(project, state="acquisition_failed", error="TXT repair exhausted", options=fast)
    assert divergent["audit"]["repair_attempts"] == 2 and divergent["audit"]["quarantine_bytes"] > 0, divergent
    exhausted = manual(project, state="acquisition_failed", error="TXT repair exhausted", options=fast)
    assert exhausted["audit"]["repair_attempts"] == 0, exhausted
    verify_cas(project, sources | {100: changed})

    # Fill the item quota with copies of an actual quarantined capture.
    now = time.time_ns() // 1000
    subprocess.run(["sudo", "-n", "sh", "-ec", """
        stamp=$2
        set -- "$1"/*
        source=$1
        parent=${source%/*}
        index=$#
        while [ "$index" -lt 128 ]; do
          cp -a "$source" "$parent/$stamp-0-100-$index"
          index=$((index+1))
        done
    """, "sh", str(audit / "quarantine"), str(now)], check=True, timeout=15)
    os.utime(rsync, ns=(remote_stamp - 5_000_000_000, remote_stamp - 5_000_000_000))
    full = manual(project, state="acquisition_failed", error="quarantine is full", options=fast)
    assert full["audit"]["repair_attempts"] == 0, full
    assert publication_snapshot(project) == snapshot
    print("Audit overlap, explicit receipts, snapshot mutation, missing and truncated sessions, "
          "timeout recovery, pair replay, cooldown, convergence, exhaustion, exact repaired CAS, "
          "and quarantine bounds passed", flush=True)


def publication_command(project, report, *, sequence=None, rollback=False, error=None, crash=None):
    expected = report["expected_publication_sequence"] if sequence is None else sequence
    operation = "rollback" if rollback else "publish"
    options = ("-e", f"RFC_PHASE3_CRASH={crash}") if crash else ()
    args = (f"--phase3-{operation}", report["generation_id"], "--expect-sequence", str(expected))
    result = subprocess.run(project.app("sync", "-T", *options, args=args), env=project.env,
                            text=True, capture_output=True, timeout=60)
    if crash:
        assert result.returncode == 86 and f"crash at {crash}" in result.stderr, result
    elif error:
        assert result.returncode and error in result.stderr, result
    else:
        assert result.returncode == 0, result
        value = json.loads(result.stdout)
        assert value["publication_sequence"] == str(expected + 1), value
        return value


def database_publication_guards(project, generation):
    cases = (
        (f"DELETE FROM rfc.generation_source_evidence WHERE generation_id={generation} AND rfc_id=100",
         "candidate membership and audit must equal the sealed selection"),
        (f"UPDATE rfc.generation_source_evidence SET txt_observation=(SELECT txt_observation "
         f"FROM rfc.generation_documents WHERE generation_id={generation} AND rfc_id=101) "
         f"WHERE generation_id={generation} AND rfc_id=100", "candidate contains ineligible source evidence"),
        (f"UPDATE rfc.generation_audit SET started_us=1, expires_us=2 WHERE generation_id={generation}",
         "candidate audit cycle expired"),
        (f"UPDATE rfc.generation_audit SET expected_sequence=1 WHERE generation_id={generation}",
         "candidate publication sequence changed"),
    )
    for mutation, message in cases:
        query = (f"BEGIN; SELECT pg_advisory_xact_lock(-1); {mutation}; SET LOCAL ROLE rfc_sync; "
                 f"SELECT rfc.promote_generation({generation}, 0, false); ROLLBACK")
        phase1.failed([*project.psql(), "-c", query], project.env, message)
        assert project.publication() == 0
    phase1.failed([*project.psql(), "-c", f"UPDATE rfc.generation_source_evidence "
                   f"SET selection=decode(repeat('a1',32),'hex') WHERE generation_id={generation}"],
                  project.env, "23503")


def load_mcp_schemas(client):
    tools = client.rpc("tools/list")[0]["tools"]
    client.schemas = {tool["name"]: phase6.phase5.Draft202012Validator(tool["outputSchema"]) for tool in tools}


def publication_contract(project, client, sources, first):
    initial = publication_snapshot(project)
    assert initial == "-1|0|0|0", initial
    generation = int(first["generation_id"])
    assert project.sql(f"SELECT state,manifest_format FROM rfc.generations WHERE id={generation}") == "ready|3"
    assert project.sql(f"""
        SELECT count(*), count(DISTINCT receipt), count(DISTINCT algorithm),
               bool_and(method='rsync_checksum_match' AND rfcxml_audit='audit_not_applicable')
        FROM rfc.generation_source_evidence WHERE generation_id={generation}
    """) == f"{len(sources)}|1|1|t"
    assert not checkpoint_rows(project)
    assert_capture_only_gate(project, generation)
    database_publication_guards(project, generation)
    project.denied_sql(f"UPDATE rfc.generation_source_evidence SET receipt=txt_digest WHERE generation_id={generation}",
                       phase1.SYNC)
    project.denied_sql(f"DELETE FROM rfc.generation_audit WHERE generation_id={generation}", phase1.SYNC)
    sample = manual(project, scope="sample")
    publication_command(project, sample, error="not eligible")
    project.maintain("discard", sample["generation_id"])
    assert project.sql("SELECT count(*) FROM rfc.generation_source_evidence "
                       f"WHERE generation_id={int(sample['generation_id'])}") == "0"
    verify_cas(project, sources)
    publication_command(project, first, crash="before_publication_commit")
    assert publication_snapshot(project) == initial
    with phase3.paused(project, "sync", "before_promotion", "publish", first["generation_id"], 0,
                       timeout=35) as publisher:
        publication_command(project, first, error="corpus lock")
        output, error = phase3.release(publisher)
        assert publisher.returncode == 0 and output.strip() == "1", (output, error)
    phase6.wait(lambda: project.admin("/ready").returncode == 0)
    load_mcp_schemas(client)
    document = client.call("get", id=2119)[0]
    assert phase6.phase5.bytes_from(document["original"]) == sources[2119]
    assert document["original"]["source_verification"]["method"] == "rsync_checksum_match", document
    assert client.call("table", id=2119)[0]["source_verification"] == document["original"]["source_verification"]
    assert client.call("list", limit=1)[0]["items"][0]["source_verification"]["method"] == "rsync_checksum_match"
    hits = client.call("search", query='in::title:"RFC 2119"')[0]["hits"]
    assert hits[0]["source_verification"]["method"] == "rsync_checksum_match", hits
    publication_command(project, first, sequence=1, error="not eligible")
    assert project.sql("SELECT count(*) FROM rfc.notification_outbox") == "1"

    changed = bytes([sources[100][0] ^ 1]) + sources[100][1:]
    for transport in ("https", "rsync"):
        (project.source / transport / "rfc100.txt").write_bytes(changed)
    crash_at(project, "after_rsync_repair_pair")
    repair = manual(project)
    assert repair["audit"]["repaired"] == 1 and repair["audit"]["repair_attempts"] == 0, repair
    repaired_id = int(repair["generation_id"])
    assert project.sql(f"""
        SELECT e.method, o.raw_digest=e.txt_digest, o.https_verification
        FROM rfc.generation_source_evidence e JOIN rfc.source_observations o ON o.identity=e.rsync_observation
        WHERE e.generation_id={repaired_id} AND e.rfc_id=100
    """) == "dual_source_byte_match|t|unverified"
    assert project.sql(f"SELECT count(*) FROM rfc.generation_source_evidence WHERE generation_id={repaired_id}") == str(len(sources))
    assert project.publication() == 1
    publication_command(project, repair, crash="after_publication_commit")
    assert project.publication() == 2
    assert project.sql("SELECT count(*) FROM rfc.notification_outbox") == "2"
    publication_command(project, first, sequence=2, rollback=True)
    assert project.publication() == 3
    assert project.admin("/ready").returncode == 0
    recover(project)
    assert project.sql(f"""
        SELECT count(*) FROM rfc.generation_source_evidence e JOIN rfc.source_observations o ON o.identity=e.rsync_observation
        WHERE e.generation_id={repaired_id} AND e.rfc_id=100
    """) == "1"

    project.restart_sync(crash="after_readiness", PHASE6_NOTIFY_TARGET=f"{project.net}.34:8782")
    project.wait_crash("after_readiness")
    assert project.publication() == 3
    ready = project.sql("SELECT generation_id FROM rfc.sync_jobs WHERE state='running'")
    assert ready and project.state(ready) == "ready"
    project.restart_sync(PHASE6_NOTIFY_TARGET=f"{project.net}.34:8782")
    report = wait_report(project, state="published", generation_id=ready)
    wait_job(project, report["job_id"], "published")
    assert project.publication() == 4
    phase6.wait(lambda: project.sql("SELECT delivery_state FROM rfc.notification_outbox WHERE publication_sequence=4") == "delivered")
    assert project.sql("SELECT count(*) FROM rfc.notification_outbox WHERE publication_sequence=4") == "1"
    print("Manual publication, repaired proof, MCP verification, rollback and daemon restart passed", flush=True)


def upgrade_publication(project, old_driver, sources):
    current = project.env["PHASE2_DRIVER"]
    overlay = project.ingestion_overlay
    del project.ingestion_overlay
    project.env["PHASE2_DRIVER"] = str(old_driver)
    with phase2.capped_stage(project, label="schema10-bootstrap") as stage:
        old_options = phase2.bulk_options(project, stage, bootstrap=True)
        command = project.app("sync", "-T", *old_options, "-e", "RFC_SYNC_CONFIG_FILE=/phase2/bulk.toml",
                              "--entrypoint", "/phase2/fixture-driver", args=("full",))
        def legacy_full():
            return int(json.loads(phase1.capture(command, env=project.env, timeout=100))["generation_id"])
        first = legacy_full()
        project.activate(first, 0)
        mode = project.source / "mode"
        mode.write_text("bad-rfcxml")
        incumbent = legacy_full()
        project.activate(incumbent, 1)
        legacy_ready = legacy_full()
        mode.write_text("normal")
    assert project.sql(f"""
        SELECT count(*) FROM rfc.generation_documents d JOIN rfc.source_observations o ON o.identity=d.txt_observation
        WHERE d.generation_id={incumbent} AND o.verification_method='rsync_integrity'
    """) == str(len(sources))
    before = history(project)
    project.env["PHASE2_DRIVER"] = current
    project.ingestion_overlay = overlay
    project.migrate()
    project.migrate()
    assert project.sql("SELECT max(version) FROM rfc.schema_migrations") == "11"
    assert history(project) == before
    client = project.start_mcp()
    phase6.wait(lambda: project.admin("/ready").returncode == 0)
    load_mcp_schemas(client)
    document = client.call("get", id=2119)[0]
    assert phase6.phase5.bytes_from(document["original"]) == sources[2119]
    assert document["original"]["source_verification"] == {"method": "rsync_integrity"}
    assert project.read(first)["generation"] == first
    publication_command(project, {"generation_id": str(legacy_ready), "expected_publication_sequence": 2},
                        error="lacks HTTPS-first audit evidence")
    replacement = manual(project)
    counts = replacement["acquisition"]
    assert counts["txt_downloaded"] == counts["txt_audited"] == len(sources), replacement
    assert project.sql(f"SELECT rfcxml_outcome FROM rfc.generation_source_evidence "
                       f"WHERE generation_id={int(replacement['generation_id'])} AND rfc_id=8174") == "captured"
    publication_command(project, replacement)
    document = client.call("get", id=2119)[0]
    assert phase6.phase5.bytes_from(document["original"]) == sources[2119]
    assert document["original"]["source_verification"]["method"] == "rsync_checksum_match"
    publication_command(project, {"generation_id": str(first), "expected_publication_sequence": 3}, rollback=True)
    assert client.call("get", id=2119)[0]["original"]["source_verification"] == {"method": "rsync_integrity"}
    print("Populated schema 10 upgraded twice and retained byte-exact rollback with original proof", flush=True)


def exercise(project, work, spool, audit, incumbent_only, old_driver=None, scheduler_only=False, audit_only=False,
             publication_only=False, upgrade=False):
    sources = phase2.prepare(project, include_nul=True)
    original = project.source / "catalog.xml"
    initial = original.read_text()
    phase6.feed_data(project, [])
    (project.source / "feed-mode").write_text("normal")
    project.start()
    if old_driver:
        phase2.migrate_to_historical_schema(project, 10 if upgrade else 9)
    else:
        project.migrate()
    project.run("up", "-d", "--wait", "fixtures", "control-probe", timeout=35)
    client = None
    if not old_driver:
        client = project.start_mcp()
        phase6.wait(lambda: "503" in project.admin("/ready").stderr)

    config = work / "ingestion.toml"
    config.write_bytes((phase2.REPO / "deploy/ingestion.toml.example").read_bytes())
    overlay = work / "discovery-compose.json"
    overlay.write_text(json.dumps({"services": {
        "sync": {
            "environment": {"RFC_SYNC_CONFIG_FILE": CONFIG_PATH, "RFC_DISCOVERY_FIXTURES": "1"},
            "volumes": [f"{config}:{CONFIG_PATH}:ro", f"{spool}:/rfc-https-spool", f"{audit}:/rfc-rsync-audit"],
        },
        "feed": {"environment": {"RFC_DISCOVERY_FIXTURES": "1"}},
    }}))
    project.ingestion_overlay = overlay
    if old_driver:
        upgrade_publication(project, old_driver, sources)
        return
    if incumbent_only:
        exercise_incumbent(project, sources, client)
        return
    empty = publication_snapshot(project)
    assert empty == "-1|0|0|0"
    fresh = manual(project)
    assert fresh["expected_txt"] == fresh["new_txt"] == len(sources)
    assert fresh["catalog_without_txt"] == 1
    assert fresh["metrics"]["https_requests"] == 2 + 2 * len(sources)
    assert fresh["acquisition"]["txt_downloaded"] == len(sources)
    assert fresh["acquisition"]["txt_contains_nul"] == fresh["acquisition"]["txt_invalid_utf8"] == 1
    print(f"Cold fixture reported {fresh['metrics']['https_requests']} HTTPS starts, "
          f"{fresh['acquisition']['staged_body_bytes']} staged artifact bytes, "
          f"and peak {fresh['acquisition']['stage_peak_jobs']} active artifact jobs. "
          f"Rsync matched {fresh['audit']['matched']} files in {fresh['audit']['session_ms']} ms "
          f"with a {fresh['audit']['snapshot_peak_bytes']}-byte snapshot.", flush=True)
    if audit_only:
        exercise_audit(project, work, sources, fresh, audit)
        return
    if publication_only:
        publication_contract(project, client, sources, fresh)
        return
    if not scheduler_only:
        checkpoint_restarts(project, sources, fresh)
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
        assert not checkpoint_rows(project)
        (project.source / "mode").write_text("normal")
        cancel_inventory(project)
        acquisition_faults(project, sources, spool)
    slow_checkpoint_commit(project)
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
        assert tiny["processing_digest"] == fresh["acquisition"]["processing_digest"]
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
    wait_job(project, daemon["job_id"], "published")
    assert daemon["selection_id"] == fresh["selection_id"], (daemon, fresh)
    assert daemon["metrics"]["minimum_start_spacing_us"] >= 1_000_000
    assert daemon["state"] == "published" and project.publication() == 1
    feed_convergence(project, original, initial, config)
    assert project.publication() == 2

    print(f"Cold minimum start spacing {fresh['metrics']['minimum_start_spacing_us']} us", flush=True)


def exercise_incumbent(project, sources, client):
    first = manual(project)
    publication_command(project, first)
    second = manual(project)
    publication_command(project, second)
    phase6.wait(lambda: project.admin("/ready").returncode == 0)
    load_mcp_schemas(client)
    prior = publication_snapshot(project)

    missing = project.source / "rsync/rfc101.txt"
    missing.unlink()
    manual(project, state="acquisition_failed", error="inventory lost previously accepted RFC 101")
    missing.write_bytes(sources[101])
    catalog = project.source / "catalog.xml"
    original = catalog.read_text()
    catalog.write_text(original.replace(phase2.catalog_entry(101, "Legacy bytes", day="<day>23</day>"), ""))
    manual(project, state="acquisition_failed", error="catalog lost previously accepted RFC 101")
    catalog.write_text(original)
    digest = project.txt_digest(second["generation_id"])
    original_txt = sources[101]
    canonical_edit(project, digest, bytes([original_txt[0] ^ 1]) + original_txt[1:])
    try:
        manual(project, state="acquisition_failed", error="canonical TXT")
    finally:
        canonical_edit(project, digest, original_txt)
    assert publication_snapshot(project) == prior
    assert phase6.phase5.bytes_from(client.call("get", id=2119)[0]["original"]) == sources[2119]

    mode = project.source / "mode"
    mode.write_text("acquisition-hourly")
    offset = len(project.capture("logs", "--no-log-prefix", "fixtures").splitlines())
    project.restart_sync(PHASE6_NOTIFY_TARGET=f"{project.net}.34:8782", PHASE6_HOURLY="12")
    warm = wait_report(project, state="published", publication_sequence=2)
    wait_job(project, warm["job_id"], "published")
    assert warm["acquisition"]["txt_downloaded"] == 0
    assert warm["acquisition"]["txt_reused"] == len(sources)
    assert warm["acquisition"]["xml_reused"] > 0, warm
    assert project.admin("/ready").returncode == 0
    phase6.wait(lambda: sum(path == "/rfc-index.xml" for path, _ in starts(project, offset)) >= 3)
    assert project.sql("SELECT count(*) FROM rfc.sync_jobs WHERE state IN ('pending','running')") == "0"
    project.run("stop", "sync")
    publication_command(project, first, sequence=3, rollback=True)
    final = client.call("get", id=2119)[0]
    assert final["publication_sequence"] == "4" and final["generation_id"] == first["generation_id"]
    assert phase6.phase5.bytes_from(final["original"]) == sources[2119]
    print("Warm acquisition, idle catalog check, incumbent failure handling and retained rollback passed", flush=True)


def main():
    if not __debug__:
        raise RuntimeError("Phase 2 acquisition requires assertions")
    parser = argparse.ArgumentParser()
    parser.add_argument("--context", default="rootless")
    parser.add_argument("--image-prefix", required=True)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--audit-only", action="store_true", help="exercise audit and exact-byte repair")
    modes.add_argument("--publication-only", action="store_true", help="exercise audited publication and recovery")
    modes.add_argument("--incumbent-only", action="store_true",
                       help="repeat warm reuse, the idle catalog check and retained rollback")
    modes.add_argument("--scheduler-only", action="store_true",
                       help="repeat checkpoint backpressure, pacing and daemon routing")
    modes.add_argument("--upgrade-from-schema9", metavar="SCHEMA9_IMAGE_PREFIX",
                       help="seed a populated schema 9 store before running incumbent checks")
    modes.add_argument("--upgrade-from-schema10", metavar="SCHEMA10_IMAGE_PREFIX",
                       help="qualify a populated schema 10 to 11 upgrade")
    args = parser.parse_args()
    docker = ["docker", "--context", args.context]
    with tempfile.TemporaryDirectory(prefix="rfc-https-discovery-", dir="/tmp") as directory:
        work = Path(directory)
        driver = work / "publication-driver"
        phase3.fixture_driver(docker, args.image_prefix, driver)
        old_driver = None
        old_prefix = args.upgrade_from_schema10 or args.upgrade_from_schema9
        if old_prefix:
            old_driver = work / "publication-driver-historical"
            phase3.fixture_driver(docker, old_prefix, old_driver)
        phase6.fixture_binaries(docker, args.image_prefix, work)
        project = Project(work / work.name.lower(), driver, args.image_prefix, docker)
        project.external_ingestion = True
        project.prepare_phase6(work)
        with (phase2.capped_stage(project, mebibytes=5120, inodes=65536) as spool,
              phase2.capped_stage(project, mebibytes=1024, inodes=4096, label="audit") as audit):
            try:
                exercise(project, work, spool, audit, args.incumbent_only or bool(old_driver), old_driver,
                         scheduler_only=args.scheduler_only, audit_only=args.audit_only,
                         publication_only=args.publication_only, upgrade=bool(args.upgrade_from_schema10))
            except Exception:
                project.run("logs", "--no-color", "--tail", "15", "sync", "fixtures", "feed")
                raise
            finally:
                project.close()


if __name__ == "__main__":
    main()
