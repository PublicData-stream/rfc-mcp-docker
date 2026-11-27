#!/usr/bin/env python3
"""Qualify automatic backup, migration and startup in production Compose."""

import argparse
import json
import secrets
from pathlib import Path
import subprocess
import sys
import tempfile

from compose_upgrade import (HISTORY, INVENTORY, SERVE, SYNC, FEED, PSQL, ROOT, dump,
                             hold_coordinator, phase7, rejected)


def host_command(stack):
    path = stack.work / "compose.env"
    path.touch(mode=0o600)
    return [sys.executable, "-B", str(ROOT / "scripts/deploy/upgrade.py"), "--env-file", str(path),
            "--project-name", stack.name, "--context", stack.docker[-1],
            "-f", str(ROOT / "scripts/phase7/compose.yml")]


def tables(stack):
    return {name: stack.sql("-c", query) for name, query in INVENTORY.items()}


def source_sql(target):
    sql = (ROOT / "migrations/migrate.sql").read_text()
    if target < 13:
        sql = sql.split(f"SELECT NOT EXISTS (SELECT FROM rfc.schema_migrations WHERE version = {target + 1})", 1)[0]
        sql += "COMMIT;\n"
    return sql.replace("\\ir ", "\\i /migrations/")


def maintenance_logs(stack):
    return stack.capture("logs", "--no-log-prefix", "db-maintenance")


def completed_backup(stack):
    return stack.capture("run", "--rm", "--no-deps", "--entrypoint", "/bin/sh", "db-maintenance",
                         "-ec", "ls -1 /backups/*/receipt.json").splitlines()


def fresh(stack):
    mirror = stack.work / "fresh-mirror"
    mirror.mkdir()
    stack.env["RFC_MIRROR_DIR"] = str(mirror)
    admin = ("--profile", "tools", "run", "--rm", "--no-deps", "volume-admin")
    try:
        stack.run(*admin)
        stack.run("up", "-d", "--wait", SERVE, timeout=180)
        assert stack.sql("-c", HISTORY).splitlines() == list(map(str, range(1, 14)))
        assert stack.sql("-c", INVENTORY["pointer"]) == "|0"
        stack.run("run", "--rm", "--no-deps", "--entrypoint", "/bin/sh", "db-maintenance", "-ec",
                  'for receipt in /backups/pg*/receipt.json; do if [ -f "$receipt" ]; then exit 1; fi; done')
    finally:
        stack.run("stop", SERVE)
        stack.run(*admin, "find /mirror -mindepth 1 -maxdepth 1 -exec rm -rf {} +; chown 0:0 /mirror")
    print("Fresh Compose startup provisioned the database and initialized the mirror", flush=True)


def automatic_schema(stack, version, digest, original):
    stack.provision(migrate=False)
    stack.capture("exec", "-T", "postgres", *PSQL, "-f", "/dev/stdin", input=source_sql(version))
    # An independently backed up catalog row must survive even without a published generation.
    stack.sql("-c", "INSERT INTO rfc.generations(parser_version,index_version) VALUES ('fixture','fixture')")
    stack.sql("-c", f"INSERT INTO rfc.blobs(digest,byte_length) VALUES (decode('{digest}','hex'),{len(original)})")
    expected = tables(stack)
    if version == 11:
        rejected(stack.command("up", "-d", "--wait", SERVE), stack.env, "db-maintenance")
        assert "running PostgreSQL requires maintenance" in maintenance_logs(stack)
        preflight = stack.env["RFC_SERVE_IMAGE"]
        stack.env["RFC_SERVE_IMAGE"] = f"{stack.prefix}-feed"
        try:
            rejected(stack.command("up", "-d", "--wait", SERVE), stack.env, "serve-schema-preflight")
        finally:
            stack.env["RFC_SERVE_IMAGE"] = preflight
        stack.sql("-c", "UPDATE rfc.schema_migrations SET version=99 WHERE version=11")
        stack.run("stop", "postgres")
        rejected(stack.command("up", "-d", "--wait", SERVE), stack.env, "db-maintenance")
        assert "unsupported source migration history" in maintenance_logs(stack)
        stack.run("up", "-d", "--no-deps", "--wait", "postgres")
        stack.sql("-c", "UPDATE rfc.schema_migrations SET version=11 WHERE version=99")
        stack.sql("-c", "CREATE TABLE rfc.policy_state (collision boolean)")
        stack.run("stop", "postgres")
        rejected(stack.command("up", "-d", "--wait", SERVE), stack.env, "db-maintenance")
        assert 'relation "policy_state" already exists' in maintenance_logs(stack)
        stack.run("up", "-d", "--no-deps", "--wait", "postgres")
        assert stack.sql("-c", HISTORY).splitlines() == list(map(str, range(1, version + 1)))
        assert tables(stack) == expected
        stack.sql("-c", "DROP TABLE rfc.policy_state")
        stack.run("stop", "postgres")
        blob = f"/mirror/blobs/blake3/{digest[:2]}/{digest}"
        admin = ("--profile", "tools", "run", "--rm", "--no-deps", "volume-admin")
        stack.run(*admin, f"mv {blob} {blob}.saved; head -c {len(original)} /dev/zero >{blob}; chmod 0444 {blob}")
        try:
            rejected(stack.command("up", "-d", "--wait", SERVE), stack.env, "db-maintenance")
            assert "incorrect length or BLAKE3" in maintenance_logs(stack)
        finally:
            stack.run(*admin, f"mv -f {blob}.saved {blob}")
        allocate = """
import errno
import os
from pathlib import Path

root = Path("/backups/fill")
root.mkdir()
for slot in range(os.statvfs(root).f_favail + 1):
    try:
        (root / str(slot)).touch(exist_ok=False)
    except OSError as error:
        if error.errno != errno.ENOSPC:
            raise
        break
else:
    raise AssertionError("backup filesystem still has free inodes")
"""
        stack.run("run", "--rm", "--no-deps", "--entrypoint", "python3",
                  "db-maintenance", "-B", "-c", allocate)
        try:
            rejected(stack.command("up", "-d", "--wait", SERVE), stack.env, "db-maintenance")
            assert "No space left on device" in maintenance_logs(stack)
        finally:
            stack.run("run", "--rm", "--no-deps", "--entrypoint", "/bin/sh",
                      "db-maintenance", "-ec", "rm -rf /backups/fill")
    stack.run("stop", "postgres")
    if version == 12:
        subprocess.run([*host_command(stack), "preflight"], env=stack.env, check=True, timeout=150)
        assert not stack.capture("ps", "-q", "postgres", SERVE, SYNC, FEED)
        subprocess.run([*host_command(stack), "preflight", "--apply"], env=stack.env, check=True, timeout=240)
        assert not stack.capture("ps", "-q", SERVE, SYNC, FEED)
    else:
        stack.run("up", "-d", "--wait", SERVE, timeout=240)
    assert f"application schema {version} to 13" in maintenance_logs(stack)
    assert stack.sql("-c", HISTORY).splitlines() == list(map(str, range(1, 14)))
    assert stack.sql("-c", "SELECT parser_version FROM rfc.generations") == "fixture"
    assert tables(stack) == expected
    backups = completed_backup(stack)
    assert backups
    postgres = stack.capture("ps", "-q", "postgres")
    stack.run("up", "-d", "--wait", SERVE, timeout=150)
    assert completed_backup(stack) == backups
    assert stack.capture("ps", "-q", "postgres") == postgres
    if version == 12:
        stack.run("stop", SERVE)
        stack.run("exec", "-T", "postgres", "test", "-s", "/var/lib/postgresql/.rfc-managed-system-id")
        stack.run("kill", "-s", "SIGKILL", "postgres")
        stack.run("up", "-d", "--wait", SERVE, timeout=180)
        assert completed_backup(stack) == backups and tables(stack) == expected
    print(f"Automatic same-major schema {version} upgrade and rerun passed", flush=True)


def automatic_major(stack, archive, relative, expected_rfc, retained_rfc, candidate, *, deploy):
    stack.run("--profile", "tools", "run", "--rm", "--no-deps", "volume-admin")
    stack.env["RFC_POSTGRES18_DATA_DIR"] = relative
    command = stack.command("run", "--rm", "--no-deps", "-T", "-e", f"OLD_DATA={relative}",
                            "-v", f"{archive}:/tmp/source.dump:ro", "--entrypoint", "/bin/sh",
                            "db-maintenance", "-se")
    populate = """
old=/var/lib/postgresql/$OLD_DATA
pg18=/opt/postgresql18/bin
mkdir -p "$old"
"$pg18/initdb" -D "$old" -U postgres --encoding=UTF8 --locale=C --locale-provider=icu \
  --icu-locale=und --auth-local=peer --auth-host=scram-sha-256
mkdir -m 0700 /tmp/old
"$pg18/pg_ctl" -D "$old" -l /tmp/old/server.log -o '-c listen_addresses= -c unix_socket_directories=/tmp/old' -w start
trap '"$pg18/pg_ctl" -D "$old" -m fast -w stop' EXIT
export PGHOST=/tmp/old PGUSER=postgres
createdb rfc
psql -X -v ON_ERROR_STOP=1 -d rfc -f /migrations/roles.sql -f /migrations/bootstrap.sql
for name in rfc_serve rfc_sync; do
    password=$(cat "/run/secrets/$name")
    printf '%s\n%s\n' "$password" "$password" | psql -X -v ON_ERROR_STOP=1 -d rfc -c "\\password $name"
done
pg_restore --exit-on-error --single-transaction -d rfc /tmp/source.dump
"""
    subprocess.run(command, input=populate, env=stack.env, text=True, check=True, timeout=150)
    control = lambda: stack.capture("run", "--rm", "--no-deps", "--entrypoint", "sha256sum", "db-maintenance",
                                    f"/var/lib/postgresql/{relative}/global/pg_control")
    source = control()
    stack.run("run", "--rm", "--no-deps", "--entrypoint", "/bin/sh", "db-maintenance", "-ec",
              "mkdir -p /var/lib/postgresql/.rfc-work/attempt-interrupted/target; "
              "printf '19\n' > /var/lib/postgresql/.rfc-work/attempt-interrupted/target/PG_VERSION")
    if deploy:
        old = stack.work / "previous-compose.json"
        old.write_text(json.dumps({"services": {"postgres": {
            "image": stack.env["RFC_MAINTENANCE_IMAGE"],
            "entrypoint": ["/bin/sh", "-ec"],
            "command": ["cp /run/secrets/postgres_key /run/postgresql/server.key; "
                        "chmod 0600 /run/postgresql/server.key; "
                        f"exec /opt/postgresql18/bin/postgres -D /var/lib/postgresql/{relative} "
                        "-c ssl=on -c ssl_cert_file=/run/secrets/postgres_cert "
                        "-c ssl_key_file=/run/postgresql/server.key -c hba_file=/etc/rfc-pg-hba.conf"],
        }}}))
        subprocess.run(stack.command("up", "-d", "--wait", "--no-deps", "postgres", extra=old),
                       env=stack.env, check=True, timeout=90)
        result = subprocess.check_output([*host_command(stack), "deploy"], env=stack.env, text=True, timeout=360)
        assert "Serve MCP verified generation" in result and "Serve MCP verified byte-exact RFC" in result, result
        assert stack.capture("ps", "-q", SERVE, SYNC, FEED).count("\n") == 2
        stack.run("stop", FEED, SYNC)
    else:
        stack.run("up", "-d", "--wait", "--wait-timeout", "180", SERVE, "test-proxy", timeout=220)
        port = int(stack.capture("port", "test-proxy", "8443").rsplit(":", 1)[1])
        stack.client = phase7.phase5.Client(port, stack.ca)
    assert "PostgreSQL 18 to 19" in maintenance_logs(stack)
    # A running legacy server may write a shutdown checkpoint while deploy drains it.
    if not deploy:
        assert control() == source
    source = control()
    assert stack.sql("-c", "SHOW server_version").startswith("19.")
    assert stack.sql("-c", HISTORY).splitlines() == list(map(str, range(1, 14)))
    assert stack.sql("-c", INVENTORY["readable"]).splitlines() == ["1", "2"]
    assert stack.sql("-c", INVENTORY["pointer"]) == "2|2"
    assert stack.sql("-c", "SELECT rights_epoch FROM rfc.policy_state") == "1"
    if not deploy:
        result, _ = stack.client.rpc("tools/call", name="get", arguments={"id": 2119, "format": "base64"})
        assert not result["isError"], result
        result = result["structuredContent"]
        assert result["publication_sequence"] == "2"
        assert phase7.phase5.bytes_from(result["original"]) == expected_rfc
    stack.run("stop", SERVE)
    driver = stack.work.parent / "publication-driver"
    retained = json.loads(stack.capture("run", "--rm", "--no-deps", "-T", "-v",
        f"{driver}:/fixture-driver:ro", "--entrypoint", "/fixture-driver", SERVE, "read", "1"))
    assert str(retained["generation"]) == "1" and bytes(retained["body"]) == retained_rfc
    backups = completed_backup(stack)
    assert len(backups) == 1
    if not deploy:
        stack.sql("-c", "DELETE FROM rfc.source_rights WHERE generation_id=2 AND rfc_id=2119")
        with hold_coordinator(stack):
            rejected(stack.command("up", "-d", "--wait", SERVE), stack.env, "sync-rights-preflight")
        assert not stack.capture("ps", "-q", SERVE)
        assert stack.sql("-c", INVENTORY["pointer"]) == "2|2"
        stack.run("up", "-d", "--wait", SERVE, timeout=150)
        assert stack.sql("-c", "SELECT count(*) FROM rfc.source_rights WHERE generation_id=2 AND rfc_id=2119") == "1"
    stack.run("stop", SERVE, "postgres")
    stack.run("up", "-d", "--wait", SERVE, timeout=180)
    assert completed_backup(stack) == backups and control() == source
    stack.run("run", "--rm", "--no-deps", "-T", SYNC, "--phase3-publish", str(candidate), "--expect-sequence", "2")
    assert stack.sql("-c", INVENTORY["pointer"]) == f"{candidate}|3"
    print(f"Automatic PostgreSQL 18 upgrade passed for {relative}", flush=True)


def prepare_seed(work, prefix, docker):
    driver = work / "publication-driver"
    phase7.phase5.phase3.fixture_driver(docker, prefix, driver)
    seed = phase7.phase5.Project(work / work.name, driver, prefix, docker)
    seed.env.update(PHASE5_DIR=str(work), PHASE5_SCRIPTS=str(phase7.PHASE5), PHASE5_PROXY_IP="")
    seed.key = secrets.token_bytes(48)
    (work / "cursor-key").write_bytes(seed.key)
    (work / "cursor-key").chmod(0o444)
    (work / "fixture-policy.json").touch()
    for name, contents in (("fixture.txt", "phase-zero test match\n"), ("unselected.txt", "unselected\n")):
        (seed.directory / "mirror" / name).write_text(contents)
    return seed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", default="rootless")
    parser.add_argument("--image-prefix", required=True)
    parser.add_argument("--case", action="append",
                        choices=("fresh", "9", "10", "11", "12", "pg18-docker", "pg18-custom"))
    args = parser.parse_args()
    selected = set(args.case or ("fresh", "9", "10", "11", "12", "pg18-docker", "pg18-custom"))
    docker = ["docker", "--context", args.context]
    with tempfile.TemporaryDirectory(prefix="rfc-auto-upgrade-", dir="/tmp") as directory:
        work = Path(directory)
        seed = prepare_seed(work, args.image_prefix, docker)
        try:
            seed.start()
            seed.migrate()
            seed.prepare_reviews()
            seed.run("up", "-d", "--wait", "fixtures", timeout=60)
            seed.activate(int(seed.reconcile()["generation_id"]), 0)
            seed.activate(int(seed.reconcile()["generation_id"]), 1)
            phase7.phase1.capture(seed.app("sync", "-T", args=("policy", "restrict", "--rfc", "101",
                "--deny", "internal_search", "--reason", "rights_revoked", "--review-ref",
                "automatic upgrade fixture", "--expect-epoch", "0")), env=seed.env)
            candidate = int(seed.reconcile()["generation_id"])
            digest = seed.sql(
                "SELECT encode(v.txt_digest,'hex') FROM rfc.active_generation a "
                "JOIN rfc.generation_documents d ON d.generation_id=a.generation_id "
                "JOIN rfc.document_versions v ON v.identity=d.document_version WHERE d.rfc_id=2119"
            )
            expected_rfc = seed.canonical(digest).read_bytes()
            retained_rfc = seed.canonical(seed.txt_digest(1)).read_bytes()
            archive = work / "source.dump"
            dump(seed.command, seed.env, archive)
            archive.chmod(0o444)
            seed.run("stop", "fixtures", "postgres")
            if "fresh" in selected:
                case = work / f"{work.name}-fresh"
                case.mkdir()
                with phase7.deployment(case, seed, args.image_prefix, label_prefix="fresh-") as stack:
                    fresh(stack)
            for version in (9, 10, 11, 12):
                if str(version) not in selected:
                    continue
                case = work / f"{work.name}-schema{version}"
                case.mkdir()
                with phase7.deployment(case, seed, args.image_prefix, label_prefix=f"s{version}-") as stack:
                    automatic_schema(stack, version, digest, expected_rfc)
            for label, relative, deploy in (("pg18-docker", "18/docker", False), ("pg18-custom", "legacy/main", True)):
                if label not in selected:
                    continue
                case = work / f"{work.name}-pg18-{int(deploy)}"
                case.mkdir()
                (case / "tls").mkdir()
                phase7.phase0.make_leaf(seed.directory / "certs", "localhost", case / "tls")
                with phase7.deployment(case, seed, args.image_prefix, label_prefix=f"p{int(deploy)}-") as stack:
                    automatic_major(stack, archive, relative, expected_rfc, retained_rfc, candidate, deploy=deploy)
        finally:
            seed.close()


if __name__ == "__main__":
    main()
