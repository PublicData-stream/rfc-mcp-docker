#!/usr/bin/env python3
"""Private PostgreSQL 19, named-volume and restore checks for Phase 1."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import json
import os
from pathlib import Path
import re
import secrets
import select
import shutil
import subprocess
import sys
import tempfile

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("phase0", HERE.parent / "phase0" / "integration.py")
phase0 = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(phase0)
run = phase0.run



def capture(command, **kwargs):
    return run(command, stdout=subprocess.PIPE, **kwargs).stdout.strip()


SERVE = "rfc_serve"
SYNC = "rfc_sync"
SQL_OPTIONS = ["psql", "-XqAt", "-v", "ON_ERROR_STOP=1", "-v", "VERBOSITY=verbose"]
VOLUME_INIT = "chmod 0755 /mirror; chmod 0750 /pgdata; chown 10002:10002 /mirror; chown 70:70 /pgdata"
VOLUME_CLEANUP = (
    "find /mirror /pgdata -mindepth 1 -maxdepth 1 -exec rm -rf {} +; "
    "chown 0:0 /mirror /pgdata; chmod 0755 /mirror /pgdata"
)


class Project:
    def __init__(self, directory, driver, image_prefix, docker, name):
        directory.mkdir()
        for location in ("mirror", "pgdata", "fixture", "certs", "postgres"):
            (directory / location).mkdir(mode=0o755)
        self.directory = directory
        self.docker = docker
        self.name = name
        self.env = dict(os.environ, PHASE1_DIR=str(directory), PHASE1_DRIVER=str(driver),
                        PHASE1_IMAGE_PREFIX=image_prefix)
        self.passwords = {role: secrets.token_hex(24) for role in ("postgres", SERVE, SYNC)}
        self.prepare()

    def prepare(self):
        self.prepare_tls()
        self.prepare_database_secrets()

    def prepare_tls(self):
        certs = self.directory / "certs"
        db = self.directory / "postgres"
        phase0.make_ca(certs, "ca")
        phase0.make_ca(certs, "wrong-ca")
        phase0.make_leaf(certs, "postgres", db)
        shutil.copyfile(certs / "ca.crt", db / "ca.crt")

    def prepare_database_secrets(self):
        db = self.directory / "postgres"
        (self.directory / "reader-password").write_text(self.passwords[SERVE])
        (self.directory / "writer-password").write_text(self.passwords[SYNC])
        (db / "postgres-password").write_text(self.passwords["postgres"])
        (db / "pgpass").write_text("".join(
            f"postgres:5432:rfc:{role}:{self.passwords[role]}\n" for role in (SERVE, SYNC)
        ))
        (db / "pg_hba.conf").write_text(
            "local all postgres peer\nlocal all all reject\n"
            "hostssl rfc rfc_serve,rfc_sync all scram-sha-256\nhost all all all reject\n"
        )

    def command(self, *args):
        return [*self.docker, "compose", "-f", str(HERE / "compose.yml"), "-p", self.name,
                *phase0.compose_args(*args)]

    def run(self, *args, **kwargs):
        return run(self.command(*args), env=self.env, **kwargs)

    def capture(self, *args, **kwargs):
        return capture(self.command(*args), env=self.env, **kwargs)

    def psql(self, role="postgres"):
        if role == "postgres":
            return self.command("exec", "-T", "postgres", *SQL_OPTIONS, "-U", role, "-d", "rfc")
        return self.command(
            "exec", "-T", "-e", "PGPASSFILE=/run/postgresql/pgpass", "postgres", *SQL_OPTIONS,
            f"host=postgres dbname=rfc user={role} sslmode=verify-full sslrootcert=/phase1/ca.crt connect_timeout=4",
        )

    def sql(self, query, role="postgres"):
        return capture([*self.psql(role), "-c", query], env=self.env)

    def denied_sql(self, query, role, state="42501"):
        failed([*self.psql(role), "-c", f"BEGIN READ WRITE; {query}; ROLLBACK"], self.env, state)

    def start(self):
        self.initialize_volumes()
        self.start_postgres()
        self.bootstrap_database()

    def initialize_volumes(self):
        self.run("run", "--rm", "--no-deps", "volume-admin", VOLUME_INIT)

    def start_postgres(self):
        self.run("up", "-d", "--wait", "--wait-timeout", "65", "postgres", timeout=100)

    def bootstrap_database(self):
        run([*self.psql(), "-f", "/migrations/roles.sql"], env=self.env)
        passwords = "".join(f"ALTER ROLE {role} PASSWORD '{self.passwords[role]}';\n" for role in (SERVE, SYNC))
        # Pipe synthetic secrets over stdin. Keep them out of command lines and logs.
        run(self.psql(), env=self.env, input=passwords, stdout=subprocess.DEVNULL)
        run([*self.psql(), "-f", "/migrations/bootstrap.sql"], env=self.env)

    def migrate(self):
        run([*self.psql(), "-f", "/migrations/migrate.sql"], env=self.env)

    def app(self, service, *options, args=("--phase1-check-storage",)):
        return self.command("run", "--rm", "--no-deps", *options, service, *args)

    def check(self, service, *options, expected=None):
        command = self.app(service, *options)
        if expected:
            failed(command, self.env, expected)
        else:
            print(capture(command, env=self.env))

    def fixture(self, service, *args):
        return capture(self.app(service, "--entrypoint", "/phase1/fixture-driver", args=args), env=self.env)

    def close(self):
        try:
            self.run("stop", "postgres", timeout=40)
            self.run("run", "--rm", "--no-deps", "volume-admin", VOLUME_CLEANUP, timeout=40)
        finally:
            self.run("down", "--volumes", "--remove-orphans", timeout=60)


def failed(command, environment, expected):
    result = subprocess.run(command, text=True, capture_output=True, env=environment, timeout=30)
    assert result.returncode != 0, f"unexpected success for {command}"
    assert expected in result.stderr, result.stderr


def exercise_roles(project, generation):
    assert project.sql("SHOW default_transaction_read_only", SERVE) == "on"
    assert project.sql("SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()", SERVE) == "t"
    assert project.sql("SELECT count(*) FROM pg_authid WHERE rolname IN ('rfc_serve','rfc_sync') "
                       "AND rolpassword LIKE 'SCRAM-SHA-256$%'") == "2"
    assert project.sql("SELECT type || ':' || auth_method FROM pg_hba_file_rules "
                       "WHERE database = ARRAY['rfc']") == "hostssl:scram-sha-256"
    assert project.sql("BEGIN; SET LOCAL ROLE rfc_owner; "
                       "CREATE FUNCTION rfc.acl_probe() RETURNS integer LANGUAGE SQL AS 'SELECT 1'; "
                       "SELECT has_function_privilege('rfc_serve','rfc.acl_probe()','EXECUTE'); ROLLBACK") == "f"
    assert project.sql("SELECT current_setting('server_encoding') || ':' || datlocale || ':' || datlocprovider::text "
                       "FROM pg_database WHERE datname = current_database()") == "UTF8:und:i"
    assert project.sql("SELECT count(*) FROM rfc.text_chunks WHERE "
                       "search_vector @@ plainto_tsquery('pg_catalog.simple', 'MUST')", SERVE) == "2"
    for query in (
        "INSERT INTO rfc.rfc_identities VALUES (1)",
        "UPDATE rfc.generation_documents SET title = 'denied'",
        "DELETE FROM rfc.generation_documents",
        "TRUNCATE rfc.text_chunks",
        "CREATE TABLE rfc.denied (value int)",
        "ALTER TABLE rfc.text_chunks ADD COLUMN denied integer",
        "CREATE SCHEMA denied",
        "CREATE TEMP TABLE denied (value int)",
        "CREATE ROLE denied",
        "SET ROLE rfc_sync",
        "SET ROLE rfc_owner",
        "SELECT nextval('rfc.generations_id_seq')",
        "SELECT pg_advisory_lock(17::bigint)",
        "SELECT lo_create(0)",
        "SELECT pg_notify('denied', '')",
        "SELECT pg_read_file('PG_VERSION')",
        "COPY rfc.rfc_identities TO PROGRAM 'true'",
    ):
        project.denied_sql(query, SERVE)
    for query in ("UPDATE rfc.active_generation SET publication_sequence = 1",
                  "UPDATE rfc.generation_documents SET title = 'changed'",
                  "DELETE FROM rfc.rfc_relations", "TRUNCATE rfc.generations",
                  "CREATE TABLE rfc.denied (value int)", "SET ROLE rfc_owner"):
        project.denied_sql(query, SYNC)
    immutable = (
        f"UPDATE rfc.generations SET manifest_bytes = decode('00','hex') WHERE id = {generation}",
        f"INSERT INTO rfc.relation_coverage VALUES ({generation}, 'catalog', NULL, 'obsoletes', 'partial')",
        "UPDATE rfc.document_versions SET sealed = false",
        "INSERT INTO rfc.notice_ranges (document_version, blob_length, ordinal, category, start_byte, end_byte, extraction_state, review_state) "
        "SELECT identity, byte_length, 40, 'other', 0, 1, 'partial', 'unreviewed' FROM rfc.document_versions LIMIT 1",
        "INSERT INTO rfc.rfc_identities VALUES (0)",
        "INSERT INTO rfc.blobs (digest, byte_length) VALUES (decode('00','hex'), 2)",
        "INSERT INTO rfc.blobs (digest, byte_length) VALUES (decode(repeat('41',32),'hex'), -1)",
        "INSERT INTO rfc.relation_evidence (identity, document_version, blob_length, start_byte, end_byte, method) "
        "SELECT decode(repeat('42',32),'hex'), identity, byte_length, 0, byte_length + 1, 'txt' FROM rfc.document_versions LIMIT 1",
    )
    for query in immutable:
        project.denied_sql(query, SYNC, "23514")
    for outcome in ("COMMIT", "ROLLBACK"):
        lock = f"SELECT pg_try_advisory_xact_lock({generation}::bigint)"
        with subprocess.Popen(project.psql(SERVE), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                              text=True, env=project.env) as reader:
            try:
                reader.stdin.write("BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY;\n")
                reader.stdin.write("SET LOCAL idle_in_transaction_session_timeout = '15s';\n"
                                   f"SELECT pg_advisory_xact_lock_shared({generation}::bigint);\n")
                reader.stdin.flush()
                assert select.select([reader.stdout], [], [], 6)[0], "reader lock timed out"
                assert reader.stdout.readline().rstrip("\n") == ""
                assert project.sql(lock, SYNC) == "f"
                reader.stdin.write(f"{outcome};\n")
                reader.stdin.close()
                assert reader.wait(timeout=6) == 0
                assert project.sql(lock, SYNC) == "t"
            finally:
                if reader.poll() is None:
                    reader.kill()
                    reader.wait(timeout=5)
    print("PostgreSQL 19 TLS, ICU, FTS, runtime roles, immutability, and advisory lock release passed")


def exercise_config(project):
    project.check("serve", "-e", "RFC_DB_CA_FILE=/phase1/wrong-ca.crt", expected="UnknownIssuer")
    project.check("serve", "-e", "RFC_DB_HOST=wrong-postgres", expected="NotValidForName")
    project.check("serve", "-e", "RFC_DB_USER=rfc_sync", "-e", "RFC_DB_PASSWORD_FILE=/phase1/wrong-password",
                  "-v", f"{project.directory}/writer-password:/phase1/wrong-password:ro", expected="database identity")
    override = project.directory / "wrong-mount.yml"
    override.write_text("services:\n  serve:\n    volumes: ['mirror:/mirror:rw']\n"
                        "  sync:\n    volumes: ['mirror:/mirror:ro']\n")
    for service, expected in (("serve", "read-only mirror mount"), ("sync", "writable mirror mount")):
        command = project.command("-f", str(override), "run", "--rm", "--no-deps", service, "--phase1-check-storage")
        failed(command, project.env, expected)
    try:
        project.sql("GRANT UPDATE (title) ON rfc.generation_documents TO rfc_serve")
        project.check("serve", expected="generation_documents.title:UPDATE")
    finally:
        project.sql("REVOKE UPDATE (title) ON rfc.generation_documents FROM rfc_serve")
    try:
        project.sql("GRANT UPDATE ON rfc.active_generation TO rfc_sync")
        project.check("sync", expected="active_generation:UPDATE")
    finally:
        project.sql("REVOKE UPDATE ON rfc.active_generation FROM rfc_sync")
    for service in ("serve", "sync"):
        project.check(service)
    print("Startup rejection of wrong roles, TLS identities, excess grants and mount modes passed")


def exercise_isolation(project):
    project.run("run", "--rm", "--no-deps", "--entrypoint", "sh", "serve", "-ec",
                "test -r /mirror/blobs/blake3; ! touch /mirror/denied; "
                "test ! -r /mirror/staging; test ! -r /mirror/quarantine")
    container = project.capture("ps", "-q", "postgres")
    inspect = json.loads(capture([*project.docker, "inspect", container]))[0]
    assert not any(inspect["NetworkSettings"]["Ports"].values())
    ips = [item["IPAddress"] for item in inspect["NetworkSettings"]["Networks"].values()]
    ssl_request = r"""printf '\000\000\000\010\004\322\026\057' | nc -w 2 "$1" 5432 | dd bs=1 count=1 2>/dev/null"""
    for service in ("serve", "sync"):
        assert project.capture("run", "--rm", "--no-deps", "--entrypoint", "sh", service, "-c",
                               ssl_request, "--", "postgres") == "S"
    project.run("run", "--rm", "--no-deps", "--entrypoint", "sh", "feed", "-ec",
                'test ! -e /mirror; test ! -e /phase1/db-password')
    for ip in ips:
        assert project.capture("run", "--rm", "--no-deps", "--entrypoint", "sh", "feed", "-c",
                               ssl_request, "--", ip) == ""
    print("Read-only serve mount, private staging, and feed database and volume isolation passed")


def exercise_files(project):
    fixture = project.directory / "fixture"
    source = fixture / "concurrent.txt"
    source.write_bytes(b"exact\r\n\f\xc3\xa9\xff\n")
    with ThreadPoolExecutor(max_workers=4) as workers:
        results = list(workers.map(lambda _: project.fixture("sync", "install", source.name).split(), range(4)))
    assert sorted(status for status, _ in results) == ["new", "reused", "reused", "reused"]
    digest = results[0][1]
    assert all(value == digest for _, value in results)
    canonical = project.directory / "mirror" / "blobs" / "blake3" / digest[:2] / digest
    assert canonical.read_bytes() == source.read_bytes()
    (fixture / "symlink.txt").symlink_to(source.name)
    os.mkfifo(fixture / "fifo.txt")
    for path in ("../fixture/concurrent.txt", "symlink.txt", "fifo.txt"):
        failed(project.app("sync", "--entrypoint", "/phase1/fixture-driver", args=("install", path)),
               project.env, "Error:")

    # Only the disposable harness simulates an operator introducing corruption.
    corrupt = b"X" + source.read_bytes()[1:]
    project.run("run", "--rm", "--no-deps", "volume-admin",
                'chmod 0644 "$1"; printf "%s" "$2" | dd of="$1" bs=1 count=1 conv=notrunc 2>/dev/null; '
                'chmod 0444 "$1"', "--", f"/mirror/blobs/blake3/{digest[:2]}/{digest}", "X")
    failed(project.app("sync", "--entrypoint", "/phase1/fixture-driver", args=("install", source.name)),
           project.env, "candidate quarantined")
    assert canonical.read_bytes() == corrupt
    project.run("run", "--rm", "--no-deps", "--entrypoint", "sh", "sync", "-ec",
                'digest="$1"; set -- /mirror/quarantine/*; test "$#" = 1; '
                'cmp "$1/body" /fixture/concurrent.txt; grep -q "^digest=$digest " "$1/incident.txt"', "--", digest)
    print("Concurrent same-digest installs, unsafe paths and quarantine passed on the named mirror")


def restore(source, fresh, manifests, work):
    dump = work / "fixture.dump"
    with dump.open("wb") as output:
        subprocess.run(source.command("exec", "-T", "postgres", "pg_dump", "-Fc", "-d", "rfc"),
                       env=source.env, stdout=output, check=True, timeout=40)
    # The fixture is quiescent. Every registered blob is reachable from a source observation.
    digests = source.sql("SELECT encode(raw_digest, 'hex') FROM rfc.source_observations GROUP BY raw_digest ORDER BY 1").splitlines()
    assert len(digests) == 3 and all(re.fullmatch("[0-9a-f]{64}", digest) for digest in digests)
    for digest in digests:
        shutil.copyfile(source.directory / "mirror" / "blobs" / "blake3" / digest[:2] / digest,
                        fresh.directory / "fixture" / digest)
        assert fresh.fixture("sync", "install", digest) == f"new {digest}"
    with dump.open("rb") as data:
        subprocess.run(fresh.command("exec", "-T", "postgres", "pg_restore", "--exit-on-error", "-d", "rfc"),
                       env=fresh.env, stdin=data, check=True, timeout=40)
    for service in ("serve", "sync"):
        fresh.check(service)
    for generation, digest in manifests:
        assert fresh.fixture("serve", "verify", generation) == f"verified {generation} {digest}"
    exercise_roles(fresh, manifests[0][0])
    exercise_isolation(fresh)
    print("Fresh PostgreSQL and mirror restore verified all fixture digests, ranges and manifests")


def main():
    if not __debug__:
        raise RuntimeError("Phase 1 requires enabled assertions")
    parser = argparse.ArgumentParser()
    parser.add_argument("--context", default="rootless")
    parser.add_argument("--image-prefix", default="rfc-phase1-regression")
    parser.add_argument("--skip-fixture-build", action="store_true")
    args = parser.parse_args()
    docker = ["docker", "--context", args.context]
    print(capture([*docker, "info", "--format", "{{.ServerVersion}} {{.Architecture}} {{json .SecurityOptions}}"]), flush=True)
    with tempfile.TemporaryDirectory(prefix="rfc-mcp-phase1-", dir="/tmp") as path:
        work = Path(path)
        driver = work / "fixture-driver"
        image = f"{args.image_prefix}-storage-fixture"
        phase0.prepare_fixture_image(docker, HERE.parent.parent, "phase1-fixture", image,
                                     skip_build=args.skip_fixture_build)
        container = capture([*docker, "create", *phase0.local_image_args(), image, "/rfc-phase1-fixture"])
        try:
            run([*docker, "cp", f"{container}:/rfc-phase1-fixture", str(driver)])
        finally:
            run([*docker, "rm", container], stdout=subprocess.DEVNULL)
        driver.chmod(0o755)
        for service in ("serve", "sync", "feed", "storage-fixture"):
            print(f"{service}: " + capture([*docker, "image", "inspect", "--format", "{{.Id}}",
                                            f"{args.image_prefix}-{service}"]), flush=True)

        source = Project(work / "source", driver, args.image_prefix, docker, f"{work.name}-source")
        try:
            source.start()
            source.check("sync", expected="does not exist")
            source.migrate()
            source.migrate()
            source.check("sync")
            source.check("serve")
            manifests = [line.split() for line in source.fixture("sync", "populate").splitlines()]
            assert len(manifests) == 2 and manifests[0][1] == manifests[1][1]
            print(f"Fixture manifest {manifests[0][1]} for two ready generations", flush=True)
            for generation, digest in manifests:
                assert source.fixture("serve", "verify", generation) == f"verified {generation} {digest}"
            exercise_roles(source, manifests[0][0])
            exercise_config(source)
            exercise_isolation(source)
            exercise_files(source)
            source.run("up", "-d", "--force-recreate", "--wait", "--wait-timeout", "65", "postgres", timeout=100)
            for generation, digest in manifests:
                assert source.fixture("serve", "verify", generation) == f"verified {generation} {digest}"
            fresh = Project(work / "restored", driver, args.image_prefix, docker, f"{work.name}-restored")
            try:
                fresh.start()
                restore(source, fresh, manifests, work)
            finally:
                fresh.close()
        finally:
            source.close()


if __name__ == "__main__":
    main()
