#!/usr/bin/env python3
"""Exercise publication and recovery on pinned PostgreSQL in private Compose projects."""

import argparse
from contextlib import contextmanager
import importlib.util
import json
from pathlib import Path
import select
import shutil
import subprocess
import sys
import tempfile
import time

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("phase2", HERE.parent / "phase2" / "integration.py")
phase2 = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(phase2)
phase1 = phase2.phase1
run = phase2.run
EMPTY_DIGEST = "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"


def fixture_driver(docker, prefix, target):
    image = f"{prefix}-publication-fixture"
    phase1.phase0.prepare_fixture_image(docker, phase2.REPO, "phase3-fixture", image)
    container = phase1.capture([*docker, "create", *phase1.phase0.local_image_args(), image,
                                "/rfc-phase3-fixture"])
    try:
        run([*docker, "cp", f"{container}:/rfc-phase3-fixture", str(target)])
    finally:
        run([*docker, "rm", container], stdout=subprocess.DEVNULL)
    target.chmod(0o755)
    print("Phase 3 fixture image " + phase1.capture([*docker, "image", "inspect", "--format", "{{.Id}}", image]), flush=True)


class Project(phase2.Project):
    def __init__(self, directory, driver, prefix, docker):
        super().__init__(directory, driver, prefix, docker, False)

    def operation(self, service, *args, crash=None, pause=None):
        options = ["-T", "--entrypoint", "/phase2/fixture-driver"]
        for name, value in (("CRASH", crash), ("PAUSE", pause)):
            if value:
                options.extend(("-e", f"RFC_PHASE3_{name}={value}"))
        return self.app(service, *options, args=tuple(map(str, args)))

    def invoke(self, service, *args, crash=None, error=None):
        command = self.operation(service, *args, crash=crash)
        result = subprocess.run(command, env=self.env, text=True, capture_output=True, timeout=90)
        if crash:
            assert result.returncode == 86 and f"crash at {crash}" in result.stderr, result
        elif error:
            assert result.returncode != 0 and error in result.stderr, result
        else:
            assert result.returncode == 0, result
        return result.stdout.strip()

    def read(self, generation="active", error=None):
        output = self.invoke("serve", "read", generation, error=error)
        return None if error else json.loads(output)

    def activate(self, generation, expected, verb="publish", **options):
        actual = self.invoke("sync", verb, generation, expected, **options)
        if not options:
            assert int(actual) == expected + 1
        return actual

    def maintain(self, verb, *args):
        command = self.app("sync", "-T", args=(f"--phase3-{verb}", *map(str, args)))
        return json.loads(phase1.capture(command, env=self.env))

    def canonical(self, digest):
        return self.directory / "mirror" / "blobs" / "blake3" / digest[:2] / digest

    def txt_digest(self, generation):
        return self.sql("SELECT encode(d.txt_digest,'hex') FROM rfc.generation_documents g "
                        f"JOIN rfc.document_versions d ON d.identity=g.document_version WHERE generation_id={generation} AND g.rfc_id=101")

    def state(self, generation):
        return self.sql(f"SELECT state FROM rfc.generations WHERE id={generation}")


@contextmanager
def paused(project, service, label, *args):
    proc = subprocess.Popen(project.operation(service, *args, pause=label), env=project.env,
                            text=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        assert select.select([proc.stdout], [], [], 10)[0], f"no checkpoint: {label}"
        assert proc.stdout.readline().strip() == f"paused {label}", f"wrong checkpoint: {label}"
        yield proc
    finally:
        if proc.poll() is None:
            proc.communicate("release\n", timeout=25)


def release(proc):
    return proc.communicate("release\n", timeout=25)


def wait_for_sql(project, query, expected):
    expiry = time.monotonic() + 2.3
    while time.monotonic() < expiry:
        if project.sql(query) == expected:
            return
        time.sleep(0.035)
    raise AssertionError(f"waiting for {expected}: {query}")


def upgrade(project):
    migration = (phase2.REPO / "migrations" / "migrate.sql").read_text().split("SELECT NOT EXISTS (SELECT FROM rfc.schema_migrations WHERE version = 3)")[0]
    migration = migration.replace(r"\ir 0001-storage.sql", r"\i /migrations/0001-storage.sql").replace(
        r"\ir 0002-ingestion.sql", r"\i /migrations/0002-ingestion.sql")
    run(project.psql(), env=project.env, input=migration + "COMMIT;\n", stdout=subprocess.DEVNULL)
    assert project.sql("SELECT version FROM rfc.schema_migrations ORDER BY version") == "1\n2"
    old = project.sql("INSERT INTO rfc.generations (parser_version,index_version) VALUES ('legacy','legacy') RETURNING id")
    project.sql("UPDATE rfc.generations SET state='ready',manifest_format=1,manifest_bytes=decode('0123','hex'),"
                f"manifest_digest=decode(repeat('ab',32),'hex') WHERE id={old}")
    project.check("sync", expected="unsupported storage schema version")
    project.migrate()
    project.migrate()
    assert project.sql("SELECT version FROM rfc.schema_migrations ORDER BY version") == "1\n2\n3\n4\n5\n6\n7\n8\n9\n10"
    assert project.sql(f"SELECT state,manifest_format,encode(manifest_bytes,'hex') FROM rfc.generations WHERE id={old}") == "ready|1|0123"
    project.activate(old, 0, error="not eligible")


def changed_catalog(project, marker):
    catalog = project.source / "catalog.xml"
    current = catalog.read_text()
    prefix = current.split("<doc-id>RFC101</doc-id><title>", 1)[0] + "<doc-id>RFC101</doc-id><title>"
    suffix = current[len(prefix):].split("</title>", 1)[1]
    catalog.write_text(prefix + marker + "</title>" + suffix)


def check_ingestion_crashes(project, first):
    for boundary in ("before_candidate_insert", "after_candidate_insert", "before_file_sync", "after_file_sync",
                     "after_blob_install", "after_directory_sync", "before_readiness", "after_readiness"):
        changed_catalog(project, boundary)
        project.invoke("sync", "full", crash=boundary)
        assert project.read()["generation"] == first
        project.invoke("sync", "recover")
        assert project.sql("SELECT count(*) FROM rfc.generations WHERE state='building'") == "0"
        assert project.sql("SELECT count(*) FROM rfc.blob_deletion_queue") == "0"
        if boundary == "after_readiness":
            assert not saved_sources(project)
            ready = project.sql("SELECT id FROM rfc.generations ORDER BY id DESC LIMIT 1")
            assert project.state(ready) == "ready"
            project.invoke("sync", "discard", ready)
    print("Candidate and file durability interruptions preserve the active generation", flush=True)


def saved_sources(project):
    return json.loads(project.sql("""
        SELECT coalesce(json_agg(json_build_object('rfc',c.rfc_id,'kind',c.kind,
               'proof',encode(c.observation,'hex'),'digest',encode(o.raw_digest,'hex'),
               'fetched',o.fetched_at,'expires',c.expires_at)), '[]')
        FROM rfc.ingestion_checkpoints c JOIN rfc.source_observations o ON o.identity=c.observation
    """))


def change_cached_file(project, digest, script):
    path = f"/mirror/blobs/blake3/{digest[:2]}/{digest}"
    command = project.app("sync", "-T", "--entrypoint", "/bin/sh", args=("-ec", script, "sh", path))
    run(command, env=project.env, stdout=subprocess.DEVNULL)


def check_checkpoint_quota(project):
    for entries, length, expected in ((40_000, 1, "40000|50001"), (1024, 4 * 1024**2, "1024|50001")):
        observed = project.sql(f"""
            BEGIN;
            SELECT pg_advisory_xact_lock(-1);
            DELETE FROM rfc.ingestion_checkpoints;
            INSERT INTO rfc.ingestion_checkpoints
                (scope,context,rfc_id,kind,observation,byte_length,recorded_at,expires_at)
            SELECT 'sample',sha256('quota'::bytea),n,'txt',o.identity,{length},
                   now()-interval '1 hour'+n*interval '1 microsecond',now()+interval '1 hour'
            FROM generate_series(50000,{49999 + entries}) n
            CROSS JOIN rfc.source_observations o
            WHERE o.source_url='https://www.rfc-editor.org/rfc/rfc100.txt'
              AND o.verification_method='dual_source';
            SET LOCAL synchronous_commit = off;
            SELECT rfc.store_ingestion_checkpoint('full',sha256('quota'::bytea),100,'txt',o.identity)
            FROM rfc.source_observations o
            WHERE o.source_url='https://www.rfc-editor.org/rfc/rfc100.txt'
              AND o.verification_method='dual_source';
            SHOW synchronous_commit;
            SELECT u.entries,min(c.rfc_id) FILTER (WHERE c.scope='sample')
            FROM rfc.ingestion_checkpoints c CROSS JOIN rfc.ingestion_checkpoint_usage u
            GROUP BY u.entries HAVING u.entries=count(*) AND u.entries<=40000 AND sum(c.byte_length)<=4294967296;
            ROLLBACK;
        """)
        assert observed.splitlines()[-2:] == ["on", expected], observed


def exercise_checkpoints(project):
    sources = phase2.prepare(project, include_nul=True)
    project.start()
    project.migrate()
    project.run("up", "-d", "--wait", "fixtures", timeout=35)
    project.denied_sql("SELECT * FROM rfc.ingestion_checkpoints", phase1.SERVE)
    project.denied_sql("DELETE FROM rfc.ingestion_checkpoints", phase1.SYNC)
    project.denied_sql("SELECT rfc.prune_ingestion_checkpoints(NULL, NULL)", phase1.SYNC, "55000")
    project.invoke("sync", "full", crash="before_txt_checkpoint_commit")
    assert not saved_sources(project)
    project.invoke("sync", "full", crash="after_txt_checkpoint_commit")
    first, = saved_sources(project)
    assert first["kind"] == "txt"
    project.maintain("recover")
    assert saved_sources(project) == [first]
    assert project.canonical(first["digest"]).is_file()
    project.invoke("sync", "full", crash="before_xml_checkpoint_commit")
    assert all(row["kind"] == "txt" for row in saved_sources(project))
    project.invoke("sync", "full", crash="after_xml_checkpoint_commit")
    assert sum(row["kind"] == "rfcxml" for row in saved_sources(project)) == 1
    project.invoke("sync", "full", crash="before_readiness")
    saved = {(row["rfc"], row["kind"]): row for row in saved_sources(project)}
    assert len(saved) == len(sources) + 3, saved
    assert saved[(first["rfc"], "txt")] == first
    check_checkpoint_quota(project)
    change_cached_file(project, saved[(8174, "rfcxml")]["digest"], 'rm -- "$1"')
    project.sql("UPDATE rfc.ingestion_checkpoints SET expires_at=now()-interval '1 second'"
                " WHERE rfc_id=102 AND kind='rfcxml'")
    (project.source / "https/rfc102.xml").unlink()
    for transport in ("rsync", "https"):
        (project.source / transport / "rfc101.txt").write_bytes(sources[101] + b"\nUpdated TXT.\n")
    ready = project.reconcile()
    assert ready["checkpoint_txt"] == len(sources) - 1 and ready["metrics"]["txt"]["calls"] == 1, ready
    assert ready["rfcxml"]["reused"] == ready["rfcxml"]["captured"] == 1, ready
    assert ready["rfcxml"]["not_found"] == len(sources)-2, ready
    assert not project.canonical(saved[(102, "rfcxml")]["digest"]).exists()
    generation = int(ready["generation_id"])
    retained = saved[(9110, "rfcxml")]
    assert project.sql(
        f"SELECT encode(txt_observation,'hex') FROM rfc.generation_documents"
        f" WHERE generation_id={generation} AND rfc_id=101"
    ) != saved[(101, "txt")]["proof"]
    assert project.sql(
        f"SELECT encode(o.identity,'hex'),o.fetched_at::text FROM rfc.generation_documents g"
        f" JOIN rfc.source_observations o ON o.identity=g.rfcxml_observation"
        f" WHERE g.generation_id={generation} AND g.rfc_id=9110"
    ).split("|")[0] == retained["proof"]
    assert not saved_sources(project) and project.state(generation) == "ready"
    assert project.sql("SELECT publication_sequence FROM rfc.active_generation") == "0"
    project.activate(generation, 0)
    assert project.read()["manifest"] == ready["manifest_digest"]

    changed_catalog(project, "Checkpoint drift probe")
    project.invoke("sync", "full", crash="before_readiness")
    assert saved_sources(project)
    mode = project.source / "mode"
    mode.write_text("catalog-drift")
    project.reconcile(expected="catalog changed")
    assert not saved_sources(project)
    mode.write_text("normal")

    source = project.source / "https/rfc9110.xml"
    source.write_bytes(source.read_bytes() + b"\n")
    project.invoke("sync", "full", crash="before_readiness")
    suspect = next(row["digest"] for row in saved_sources(project) if row["rfc"] == 9110)
    change_cached_file(project, suspect,
                       'chmod u+w "$1"; printf x | dd of="$1" bs=1 conv=notrunc 2>/dev/null; chmod 0444 "$1"')
    failure = project.reconcile(expected=f"rfcxml checkpoint {suspect} failed verification")
    assert failure["rfcxml"]["reused"] <= 1
    assert all(row["digest"] != suspect for row in saved_sources(project))
    gc = project.maintain("gc")
    assert gc["blocked_deletions"] == 1 and project.canonical(suspect).is_file(), gc
    current = project.read()
    assert current["generation"] == generation
    assert bytes(current["body"]) == sources[101] + b"\nUpdated TXT.\n"
    assert project.sql("SELECT publication_sequence FROM rfc.active_generation") == "1"
    print("Checkpoint commits, retries, expiry, eviction, corruption and GC passed", flush=True)


def check_discard_and_unlink(project):
    uncommitted = project.reconcile()["generation_id"]
    with paused(project, "sync", "retirement_locked", "discard", uncommitted) as collector:
        assert project.sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename='rfc_sync'") == "t"
        project.invoke("sync", "full", error="another synchronization holds the corpus lock")
        release(collector)
        assert collector.returncode != 0
    assert project.state(uncommitted) == "ready"
    assert project.maintain("discard", uncommitted)["retired_generations"] == 1
    for boundary, state in (("before_retirement_commit", "ready"), ("after_retirement_commit", "deleting")):
        doomed = project.reconcile()["generation_id"]
        project.invoke("sync", "discard", doomed, crash=boundary)
        assert project.state(doomed) == state
        if state == "ready":
            project.invoke("sync", "discard", doomed)
        else:
            project.invoke("sync", "recover")
        assert project.state(doomed) == "deleted"
    (project.directory / "fixture" / EMPTY_DIGEST).touch()
    for boundary in ("before_blob_unlink", "after_blob_unlink", "after_unlink_sync"):
        project.invoke("sync", "restore-blob", EMPTY_DIGEST)
        path = project.canonical(EMPTY_DIGEST)
        assert path.is_file()
        project.invoke("sync", "recover", crash=boundary)
        assert path.exists() == (boundary == "before_blob_unlink")
        assert project.sql(f"SELECT count(*) FROM rfc.blob_deletion_queue WHERE digest=decode('{EMPTY_DIGEST}','hex')") == "1"
        project.denied_sql(f"INSERT INTO rfc.blobs (digest,byte_length) VALUES (decode('{EMPTY_DIGEST}','hex'),0)",
                           phase1.SYNC, "55000")
        project.invoke("sync", "recover")
        assert not path.exists()
        assert project.sql("SELECT count(*) FROM rfc.blob_deletion_queue") == "0"
    print("Retirement and unlink interruptions resume without reusing queued bytes", flush=True)


def check_reader_gc(project, first, old_digest, shared_digest):
    gc = project.operation("sync", "gc")
    first_path = project.canonical(old_digest)
    shared = project.canonical(shared_digest)
    shared_inode = shared.stat().st_ino
    with paused(project, "serve", "reader_accepted", "read", first) as reader:
        # Identify the paused reader by the lock it holds. Starting a GC
        # container first can outlast the reader's idle transaction timeout.
        assert project.sql(f"""
            SELECT pg_terminate_backend(a.pid, 5000)
            FROM pg_stat_activity a JOIN pg_locks l ON l.pid=a.pid
            WHERE a.usename='rfc_serve' AND l.locktype='advisory' AND l.classid=0
              AND l.objid={int(first)} AND l.granted AND l.mode='ShareLock'
        """) == "t"
        assert json.loads(phase1.capture(gc, env=project.env))["retired_generations"] == 0
        assert first_path.is_file()
        _, stderr = release(reader)
        assert reader.returncode != 0 and f"worker read {old_digest}" in stderr, stderr
    assert project.state(first) == "retained"

    with paused(project, "sync", "retirement_locked", "gc") as collector:
        project.invoke("sync", "full", error="another synchronization holds the corpus lock")
        # Warm one psql connection before starting a disposable reader. Poll
        # inside PostgreSQL so container startup does not hide the lock wait.
        with subprocess.Popen(project.psql(), env=project.env, text=True, stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE) as observer:
            observer.stdin.write("\\echo ready\n")
            observer.stdin.flush()
            assert select.select([observer.stdout], [], [], 15)[0], "lock observer did not start"
            assert observer.stdout.readline().strip() == "ready"
            reader = subprocess.Popen(project.operation("serve", "read", first), env=project.env,
                                      text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                watch = f"""
DO $observer$
DECLARE deadline timestamptz := clock_timestamp() + interval '20 seconds';
BEGIN
  LOOP
    IF EXISTS (SELECT FROM pg_locks WHERE locktype = 'advisory' AND objid = {first} AND NOT granted) THEN
      RETURN;
    END IF;
    IF clock_timestamp() >= deadline THEN
      RAISE EXCEPTION 'waiting reader never acquired a place in the advisory lock queue';
    END IF;
    PERFORM pg_sleep(0.025);
  END LOOP;
END
$observer$;
"""
                _, observer_error = observer.communicate(watch, timeout=24)
                if observer.returncode != 0:
                    reader_error = reader.communicate()[1] if reader.poll() is not None else "reader still running"
                    raise AssertionError(f"{observer_error}\n{reader_error}")
                stdout, stderr = release(collector)
                assert collector.returncode == 0 and json.loads(stdout)["retired_generations"] == 1, stderr
                stdout, stderr = reader.communicate(timeout=20)
                assert reader.returncode != 0 and "GenerationUnavailable" in stderr and "worker read" not in stderr, stderr
            finally:
                if collector.poll() is None:
                    release(collector)
                if reader.poll() is None:
                    reader.communicate(timeout=20)
    assert project.state(first) == "deleted"
    assert not first_path.exists()
    assert shared.stat().st_ino == shared_inode
    project.read(first, error="GenerationUnavailable")
    project.invoke("serve", "cancel-worker")
    print("Accepted readers, database loss, post-lock tombstone checks, worker reaping and blob reuse passed", flush=True)


def backup_restore(project, fresh, work):
    dump = work / "phase3.dump"
    with dump.open("wb") as output:
        subprocess.run(project.command("exec", "-T", "postgres", "pg_dump", "-Fc", "-d", "rfc"),
                       env=project.env, stdout=output, check=True, timeout=40)
    digests = project.sql("SELECT encode(digest,'hex') FROM rfc.blobs ORDER BY digest").splitlines()
    readable = project.sql("SELECT id FROM rfc.generations WHERE state IN ('published','retained') ORDER BY id").splitlines()
    fresh.start()
    for digest in digests:
        shutil.copyfile(project.canonical(digest), fresh.directory / "fixture" / digest)
        fresh.invoke("sync", "restore-blob", digest)
    for generation in readable:
        fresh.invoke("sync", "restore-fence", generation)
    with dump.open("rb") as data:
        subprocess.run(fresh.command("exec", "-T", "postgres", "pg_restore", "--exit-on-error", "-d", "rfc"),
                       env=fresh.env, stdin=data, check=True, timeout=40)
    assert fresh.read() == project.read()
    for generation in readable:
        assert fresh.read(generation) == project.read(generation)
    assert fresh.sql("SELECT count(*) FROM rfc.notification_outbox") == project.sql("SELECT count(*) FROM rfc.notification_outbox")
    print("Quiescent backup restored the active and retained snapshots into a fresh Compose project", flush=True)


def exercise(project):
    old_sources = phase2.prepare(project)
    project.start()
    upgrade(project)
    project.run("up", "-d", "--wait", "--wait-timeout", "30", "fixtures")
    project.read(error="GenerationUnavailable")
    first_report = project.reconcile()
    first = int(first_report["generation_id"])
    sample = project.reconcile("sample")["generation_id"]
    project.activate(sample, 0, error="not eligible")
    project.activate(first, 1, error="publication sequence conflict")
    for boundary in ("before_promotion", "before_publication_commit"):
        project.activate(first, 0, crash=boundary)
        assert project.sql("SELECT generation_id,publication_sequence FROM rfc.active_generation") == "|0"
        assert project.sql("SELECT count(*) FROM rfc.notification_outbox") == "0"
    project.sql("CREATE FUNCTION rfc.reject_outbox_fixture() RETURNS trigger LANGUAGE plpgsql AS $$ "
                "BEGIN RAISE EXCEPTION 'fixture rejects outbox insert'; END $$; "
                "REVOKE ALL ON FUNCTION rfc.reject_outbox_fixture() FROM PUBLIC; "
                "CREATE TRIGGER reject_outbox_fixture BEFORE INSERT ON rfc.notification_outbox "
                "FOR EACH ROW EXECUTE FUNCTION rfc.reject_outbox_fixture()")
    project.activate(first, 0, error="fixture rejects outbox insert")
    assert project.sql("SELECT generation_id,publication_sequence FROM rfc.active_generation") == "|0"
    assert project.state(first) == "ready"
    project.sql("DROP TRIGGER reject_outbox_fixture ON rfc.notification_outbox; DROP FUNCTION rfc.reject_outbox_fixture()")
    project.activate(first, 0, crash="after_publication_commit")
    assert project.sql("SELECT count(*) FROM rfc.notification_outbox WHERE delivery_state='pending'") == "1"
    project.check("serve")
    old = project.read()
    assert old["generation"] == first and old["sequence"] == 1 and old["manifest"] == first_report["manifest_digest"]
    assert bytes(old["body"]) == old_sources[101]
    old_digest = project.txt_digest(first)
    project.denied_sql("SELECT rfc.promote_generation(1::bigint,0::bigint,false)", phase1.SERVE)
    project.denied_sql("SELECT rfc.promote_generation(1::bigint,0::bigint,false)", phase1.SYNC, "55000")
    project.denied_sql("UPDATE rfc.active_generation SET publication_sequence=99", phase1.SYNC)
    check_ingestion_crashes(project, first)

    replacement = b"Replacement bytes\r\n\x0cwith an invalid sequence \xff\n"
    for source in ("https", "rsync"):
        (project.source / source / "rfc101.txt").write_bytes(replacement)
    changed_catalog(project, "Replacement title")
    second = int(project.reconcile()["generation_id"])
    shared_digest = project.txt_digest(second)
    container_path = f"/mirror/blobs/blake3/{shared_digest[:2]}/{shared_digest}"
    flip_byte = ('chmod 0644 "$1"; printf "%s" "$2" | dd of="$1" bs=1 count=1 conv=notrunc 2>/dev/null; '
                 'chmod 0444 "$1"')
    project.run("run", "--rm", "--no-deps", "volume-admin", flip_byte, "--", container_path, "X")
    project.activate(second, 1, error="failed verification")
    assert project.read() == old
    project.run("run", "--rm", "--no-deps", "volume-admin", flip_byte, "--", container_path, "R")
    with paused(project, "serve", "reader_accepted", "read", "active") as reader:
        project.activate(second, 1)
        stdout, stderr = release(reader)
        assert reader.returncode == 0 and json.loads(stdout) == old, stderr
    new = project.read()
    assert (new["generation"], new["sequence"], new["title"], bytes(new["body"])) == (second, 2, "Replacement title", replacement)
    assert project.read(first) == old
    project.activate(first, 2, verb="rollback")
    assert project.read() == dict(old, sequence=3)
    project.activate(second, 3, verb="rollback")
    assert project.read() == dict(new, sequence=4)
    for sequence in range(4, 7):
        changed_catalog(project, f"revision {sequence}")
        target = project.reconcile()["generation_id"]
        project.activate(target, sequence)
    assert project.read()["sequence"] == 7
    assert project.state(sample) == "ready"
    assert project.maintain("gc")["retired_generations"] == 0
    project.sql("BEGIN; ALTER TABLE rfc.generations DISABLE TRIGGER freeze_generation; "
                "UPDATE rfc.generations SET retired_at=now()-interval '25 hours' WHERE state='retained'; "
                "ALTER TABLE rfc.generations ENABLE TRIGGER freeze_generation; COMMIT")
    check_reader_gc(project, first, old_digest, shared_digest)
    check_discard_and_unlink(project)
    assert project.sql("SELECT count(*) FROM rfc.notification_outbox WHERE delivery_state='pending'") == "7"
    assert project.sql("SELECT count(*) FROM rfc.generations WHERE state='retained'") == "3"
    assert project.state(sample) == "ready"
    assert bytes(project.read()["body"]) == replacement


def main():
    if not __debug__:
        raise RuntimeError("Phase 3 requires enabled assertions")
    parser = argparse.ArgumentParser()
    parser.add_argument("--context", default="rootless")
    parser.add_argument("--image-prefix", default="rfc-phase3-regression")
    parser.add_argument("--checkpoints", action="store_true", help="run only the durable checkpoint scenarios")
    args = parser.parse_args()
    docker = ["docker", "--context", args.context]
    with tempfile.TemporaryDirectory(prefix="rfc-phase3-", dir="/tmp") as directory:
        work = Path(directory)
        driver = work / "fixture-driver"
        fixture_driver(docker, args.image_prefix, driver)
        project = Project(work / f"{work.name}-source", driver, args.image_prefix, docker)
        try:
            if args.checkpoints:
                exercise_checkpoints(project)
                return
            exercise(project)
            fresh = Project(work / f"{work.name}-restore", driver, args.image_prefix, docker)
            try:
                backup_restore(project, fresh, work)
            finally:
                fresh.close()
        finally:
            project.close()
        checkpoints = Project(work / f"{work.name}-checkpoints", driver, args.image_prefix, docker)
        try:
            exercise_checkpoints(checkpoints)
        finally:
            checkpoints.close()


if __name__ == "__main__":
    main()
