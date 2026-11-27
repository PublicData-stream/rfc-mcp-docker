"""Exercise the production Compose upgrade against a populated historical fixture."""

from contextlib import contextmanager
import importlib.util
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("phase7", ROOT / "scripts/phase7/integration.py")
phase7 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(phase7)
SERVE, SYNC, FEED = phase7.SERVE, phase7.SYNC, phase7.FEED
PSQL = phase7.phase1.SQL_OPTIONS + ["-U", "postgres", "-d", "rfc"]
INVENTORY = {
    "pointer": "SELECT generation_id,publication_sequence FROM rfc.active_generation",
    "readable": "SELECT id FROM rfc.generations WHERE state IN ('published','retained') ORDER BY id",
    "blobs": "SELECT encode(digest,'hex'),byte_length FROM rfc.blobs ORDER BY digest",
}
HISTORY = "SELECT version FROM rfc.schema_migrations ORDER BY version"


def dump(command, env, target):
    with target.open("wb") as stream:
        subprocess.run(command("exec", "-T", "postgres", "pg_dump", "-Fc", "--schema=rfc",
                               "-U", "postgres", "-d", "rfc"), env=env, stdout=stream,
                       check=True, timeout=45)


def rejected(command, env, message):
    result = subprocess.run(command, env=env, text=True, capture_output=True, timeout=110)
    assert result.returncode != 0 and message in result.stdout + result.stderr, result


def application_gate(stack, message, *, force=True):
    flags = ["--force-recreate"] if force else []
    rejected(stack.command("up", "-d", *flags, "--wait", "--wait-timeout", "30",
                           SERVE, SYNC, FEED), stack.env, "preflight")
    assert not stack.capture("ps", "-q", SERVE, SYNC, FEED)
    logs = stack.capture("logs", "--no-log-prefix", "serve-db-preflight", "sync-db-preflight")
    assert message in logs, logs


def verify_session_cleanup(stack, upgrade_command):
    stack.sql("-c", "ALTER SYSTEM SET log_line_prefix = '%m [%p] user=%u db=%d '")
    assert stack.sql("-c", "SELECT pg_reload_conf()") == "t"
    rejected(stack.command("run", "--rm", "--no-deps", "-T", SYNC, "--check-storage-schema"),
             stack.env, "unsupported storage schema version")
    stack.run("up", "-d", "--no-deps", SERVE)
    try:
        rejected(upgrade_command, stack.env, "stop these services")
        # Hold the old schema until the pool has rejected and retired a session.
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if re.search(r"rfc_mcp_db_pool_graceful_closes_total [1-9]", stack.admin("/metrics")):
                break
            time.sleep(0.25)
        else:
            raise AssertionError("Serve did not gracefully retire a rejected database session")
    finally:
        stack.run("stop", SERVE)
    assert stack.sql("-c", "SELECT count(*) FROM pg_stat_activity WHERE usename IN "
                     "('rfc_serve','rfc_sync')") == "0"
    logs = stack.capture("logs", "--no-log-prefix", "postgres")
    assert "unexpected EOF on client connection with an open transaction" not in logs, logs
    for line in logs.splitlines():
        if "user=rfc_" in line:
            assert not any(text in line for text in ("unexpected EOF", "unexpected eof",
                                                     "Connection reset by peer")), line


@contextmanager
def hold_coordinator(stack):
    command = stack.command("exec", "-T", "postgres", *PSQL)
    with subprocess.Popen(command, env=stack.env, stdin=subprocess.PIPE,
                          stdout=subprocess.PIPE, text=True) as process:
        try:
            process.stdin.write("SELECT pg_try_advisory_lock(-1);\n")
            process.stdin.flush()
            assert process.stdout.readline() == "t\n"
            yield
        finally:
            process.stdin.close()
            process.wait(timeout=10)


def backup_faults(stack, backup, command, files, blobs):
    with hold_coordinator(stack):
        rejected(command, stack.env, "corpus coordinator lock is already held")

    archive = backup / "corpus.dump"
    valid_dump = archive.read_bytes()
    archive.write_bytes(b"not a PostgreSQL archive")
    rejected(command, stack.env, "could not decode the PostgreSQL dump")
    archive.write_bytes(valid_dump)

    blob = next(blob for blob in blobs if blob.stat().st_size)
    valid = blob.read_bytes()
    blob.write_bytes(bytes([valid[0] ^ 1]) + valid[1:])
    rejected(command, stack.env, "wrong BLAKE3 digest")
    blob.write_bytes(valid)

    applied_at = "UPDATE rfc.schema_migrations SET applied_at = applied_at %s interval '1 second' WHERE version = 1"
    stack.sql("-c", applied_at % "+")
    rejected(command, stack.env, "backup corpus.dump differs")
    stack.sql("-c", applied_at % "-")

    (backup / "pointer").write_text("0|0\n")
    rejected(command, stack.env, "backup pointer differs")
    (backup / "pointer").write_text(files["pointer"])

    stack.sql("-c", "UPDATE rfc.schema_migrations SET version = 99 WHERE version = 11")
    rejected(command, stack.env, "unsupported source migration history")
    stack.sql("-c", "UPDATE rfc.schema_migrations SET version = 11 WHERE version = 99")

    stack.sql("-c", "CREATE TABLE rfc.policy_state (collision boolean)")
    dump(stack.command, stack.env, archive)
    rejected(command, stack.env, 'relation "policy_state" already exists')
    assert stack.sql("-c", "SELECT to_regclass('rfc.policy_receipts') IS NULL") == "t"
    assert stack.sql("-c", HISTORY).splitlines() == list(map(str, range(1, 12)))
    for name, sql in INVENTORY.items():
        assert stack.sql("-c", sql) == files[name].strip()
    stack.sql("-c", "DROP TABLE rfc.policy_state")
    dump(stack.command, stack.env, archive)


def qualify(project, source_schema, originals, driver, mixed_sync=None):
    root = project.directory.parent
    work = root / f"{root.name}-upgrade"
    work.mkdir()
    (work / "tls").mkdir()
    phase7.phase0.make_leaf(project.directory / "certs", "localhost", work / "tls")
    history_dump = work / "historical.dump"
    dump(project.command, project.env, history_dump)
    backup = work / "backup"
    backup.mkdir(mode=0o700)
    env_file = work / "compose.env"
    env_file.touch(mode=0o600)
    prefix = project.env["PHASE1_IMAGE_PREFIX"]
    with phase7.deployment(work, project, prefix, label_prefix="upgrade-") as stack:
        base = [sys.executable, "-B", str(ROOT / "scripts/deploy/upgrade.py"),
                "--env-file", str(env_file), "--project-name", stack.name,
                "--context", stack.docker[-1], "-f", str(ROOT / "scripts/phase7/compose.yml")]
        command = [*base, "--backup-dir", str(backup)]
        stack.provision(migrate=False)
        if source_schema == 11:
            rejected(command, stack.env, "database is uninitialized")
        stack.restore(history_dump)
        files = {name: subprocess.check_output(stack.command("exec", "-T", "postgres", *PSQL, "-c", sql),
                                               env=stack.env, text=True, timeout=45)
                 for name, sql in INVENTORY.items()}
        for name, content in files.items():
            (backup / name).write_text(content)
        blobs = []
        for line in files["blobs"].splitlines():
            digest, _ = line.split("|")
            original = project.canonical(digest)
            path = backup / "mirror" / "blobs" / "blake3" / digest[:2] / digest
            path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(original, path)
            blobs.append(path)
        dump(stack.command, stack.env, backup / "corpus.dump")
        application_gate(stack, "unsupported storage schema version")
        if source_schema == 11:
            verify_session_cleanup(stack, command)
            backup_faults(stack, backup, command, files, blobs)

        result = subprocess.check_output(command, env=stack.env, text=True, timeout=110)
        assert f"migration committed from schema {source_schema} to 13" in result, result
        assert not stack.capture("ps", "-q", SERVE, SYNC, FEED)
        assert stack.sql("-c", HISTORY).splitlines() == list(map(str, range(1, 14)))
        for name, sql in INVENTORY.items():
            assert stack.sql("-c", sql) == files[name].strip()
        result = subprocess.check_output(base, env=stack.env, text=True, timeout=90)
        assert "already at schema 13; no migration applied" in result, result

        if mixed_sync:
            current = stack.env["RFC_SYNC_IMAGE"]
            stack.env["RFC_SYNC_IMAGE"] = mixed_sync
            try:
                application_gate(stack, "Use --check-config")
                rejected(base, stack.env, "--storage-schema-version")
            finally:
                stack.env["RFC_SYNC_IMAGE"] = current

        stack.open(SERVE, "test-proxy")
        assert stack.admin("/ready") == ""
        tools = stack.client.rpc("tools/list")[0]["tools"]
        stack.client.schemas = {tool["name"]: phase7.phase5.Draft202012Validator(tool["outputSchema"])
                                for tool in tools}
        active = stack.client.call("get", id=2119, format="base64")[0]
        assert f"{active['generation_id']}|{active['publication_sequence']}\n" == files["pointer"]
        assert phase7.phase5.bytes_from(active["original"]) == originals[2119]
        stack.run("stop", SERVE)
        if source_schema == 11:
            # A successful preflight from an earlier up must be checked again.
            stack.sql("-c", "UPDATE rfc.schema_migrations SET version = 99 WHERE version = 13")
            try:
                application_gate(stack, "99]", force=False)
            finally:
                stack.sql("-c", "UPDATE rfc.schema_migrations SET version = 13 WHERE version = 99")
        for generation in files["readable"].splitlines():
            document = json.loads(stack.capture("run", "--rm", "--no-deps", "-T", "-v",
                                                f"{driver}:/fixture-driver:ro", "--entrypoint",
                                                "/fixture-driver", SERVE, "read", generation))
            assert str(document["generation"]) == generation
            assert bytes(document["body"]) == originals[101]
        print(f"Production Compose preserved the incumbent and verified schema {source_schema} → 13", flush=True)
