#!/usr/bin/env python3
"""Run disposable Phase 0 probes against the production Alpine runtime targets."""

import argparse
import json
import os
from pathlib import Path
import platform
import re
import secrets
import select
import subprocess
import sys
import tempfile
import time

HERE = Path(__file__).resolve().parent
CERT_COMMAND = ("openssl", "req", "-new", "-newkey", "rsa:2048", "-noenc", "-sha256")


def run(command, *, timeout=30, **kwargs):
    return subprocess.run(command, encoding="utf-8", check=True, timeout=timeout, **kwargs)


def capture(command, *, timeout=30):
    return run(command, timeout=timeout, stdout=subprocess.PIPE).stdout.strip()


def make_ca(directory, name):
    run([*CERT_COMMAND, "-x509", "-days", "2", "-subj", f"/CN={name}",
         "-addext", "basicConstraints=critical,CA:TRUE",
         "-keyout", str(directory / f"{name}.key"), "-out", str(directory / f"{name}.crt")],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def make_leaf(directory, name, target):
    request = directory / f"{name}.csr"
    extensions = directory / f"{name}.ext"
    key = target / f"{name}.key"
    cert = target / f"{name}.crt"
    extensions.write_text(f"subjectAltName=DNS:{name}\nextendedKeyUsage=serverAuth\n")
    run([*CERT_COMMAND, "-subj", f"/CN={name}", "-keyout", str(key), "-out", str(request)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    run(["openssl", "x509", "-req", "-in", str(request), "-days", "2", "-sha256",
         "-CA", str(directory / "ca.crt"), "-CAkey", str(directory / "ca.key"),
         "-extfile", str(extensions), "-out", str(cert)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    # These are short-lived synthetic keys in a host-private directory. Inside
    # PostgreSQL the entrypoint copies the server key into a mode 0400 tmpfs file.
    key.chmod(0o444)


def prepare(directory):
    for name in ("certs", "mirror", "pgdata", "postgres", "upstream"):
        (directory / name).mkdir(mode=0o755)
    for name in ("pgdata", "mirror"):
        # Rootless Docker maps the host directory owner to namespace UID 0.
        # Non-root service UIDs create their own children inside these mounts.
        (directory / name).chmod(0o1777)
    certs = directory / "certs"
    postgres = directory / "postgres"
    upstream = directory / "upstream"
    make_ca(certs, "ca")
    make_ca(certs, "wrong-ca")
    make_leaf(certs, "postgres", postgres)
    make_leaf(certs, "fixtures", upstream)
    (postgres / "ca.crt").write_bytes((certs / "ca.crt").read_bytes())
    (directory / "secret.txt").write_text("synthetic secret\n")
    (directory / "mirror" / "fixture.txt").write_text("phase-zero match\n")
    (directory / "mirror" / "unselected.txt").write_text("unselected fixture\n")
    (upstream / "sample.txt").write_bytes(b"phase-zero rsync\r\n\f\xc3\xa9\n")
    (upstream / "rsyncd.conf").write_text(
        "pid file = /tmp/rsyncd.pid\nlock file = /tmp/rsyncd.lock\n"
        "use chroot = false\nread only = true\n[fixture]\npath = /phase0\n"
        # Only the actual TXT object is downloadable, never fixture key files.
        "include = /sample.txt\nexclude = *\n"
    )

    passwords = {role: secrets.token_hex(24) for role in ("postgres", "phase0_reader", "phase0_writer")}
    (postgres / "postgres-password").write_text(passwords["postgres"])
    (directory / "reader-password").write_text(passwords["phase0_reader"])
    (directory / "writer-password").write_text(passwords["phase0_writer"])
    (postgres / "pgpass").write_text("".join(
        f"postgres:5432:rfc_phase0:{role}:{password}\n" for role, password in passwords.items()
    ))
    roles = (
        "CREATE ROLE phase0_owner NOLOGIN;\n"
        "REVOKE CONNECT ON DATABASE rfc_phase0 FROM PUBLIC;\n"
        f"CREATE ROLE phase0_reader LOGIN PASSWORD '{passwords['phase0_reader']}';\n"
        f"CREATE ROLE phase0_writer LOGIN PASSWORD '{passwords['phase0_writer']}';\n"
        "ALTER ROLE phase0_reader SET default_transaction_read_only = on;\n"
    )
    (postgres / "init.sql").write_text(roles + (HERE / "postgres.sql").read_text())


def postgres_probe(compose):
    def psql(role):
        return compose(
            "exec", "-T", "-e", "PGPASSFILE=/run/postgresql/pgpass", "postgres",
            "psql", "-XqAt", "-v", "ON_ERROR_STOP=1", "-v", "VERBOSITY=verbose",
            f"host=postgres dbname=rfc_phase0 user={role} sslmode=verify-full "
            "sslrootcert=/phase0/ca.crt connect_timeout=3",
        )

    def sql(role, query):
        return capture([*psql(role), "-c", query])

    locale = """
        SELECT current_setting('server_encoding'), datlocprovider, datlocale,
               datcollversion = pg_database_collation_actual_version(oid)
        FROM pg_database WHERE datname = current_database()
    """
    expected = "UTF8|i|und|t"
    assert sql("postgres", locale) == expected
    print(sql("postgres", "SELECT version(), current_setting('data_directory'), datcollversion "
                          "FROM pg_database WHERE datname = current_database()"))
    assert sql("phase0_reader", "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()") == "t"
    assert sql("phase0_reader", "SHOW default_transaction_read_only") == "on"
    assert sql("postgres", "SELECT string_agg(n, ',' ORDER BY n) "
                          "FROM (VALUES ('z'), ('ä'), ('a')) AS fixture(n)") == "a,ä,z"
    assert sql("postgres", r"SELECT U&'\00E9' <> U&'\0065\0301'") == "t"
    assert sql("phase0_reader", "SELECT to_tsvector('pg_catalog.simple', 'RFC CAFÉ') "
                               "@@ plainto_tsquery('pg_catalog.simple', 'café rfc')") == "t"
    assert sql("phase0_reader", "SELECT title FROM phase0.corpus "
                               "WHERE phase0.similarity(title, 'RFC café') = 1 "
                               "AND title OPERATOR(phase0.%) 'RFC café'") == "RFC café"
    assert sql("postgres", "SELECT extversion FROM pg_extension WHERE extname = 'pg_trgm'")
    assert "Bitmap Index Scan" in sql(
        "phase0_reader",
        "SET enable_seqscan = off; EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF) "
        "SELECT title FROM phase0.corpus WHERE title OPERATOR(phase0.%) 'RFC café'",
    )

    for outcome in ("COMMIT", "ROLLBACK"):
        with subprocess.Popen(psql("phase0_reader"), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                              text=True) as reader:
            try:
                reader.stdin.write(
                    "BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY;\n"
                    "SET LOCAL statement_timeout = '5s';\n"
                    "SET LOCAL idle_in_transaction_session_timeout = '15s';\n"
                    "SELECT pg_advisory_xact_lock_shared(31087, 1);\n"
                    "SELECT title FROM phase0.corpus WHERE id=1;\n"
                )
                reader.stdin.flush()
                assert select.select([reader.stdout], [], [], 6)[0], "reader lock timed out"
                assert reader.stdout.readline().rstrip("\n") == ""
                assert reader.stdout.readline().rstrip("\n") == "RFC café"
                lock = "SELECT pg_try_advisory_xact_lock(31087, 1)"
                assert sql("postgres", lock) == "f"
                reader.stdin.write(f"{outcome};\n")
                reader.stdin.close()
                assert reader.wait(timeout=6) == 0
                assert sql("postgres", lock) == "t"
            finally:
                if reader.poll() is None:
                    reader.kill()
                    reader.wait(timeout=5)

    for statement in (
        "INSERT INTO phase0.corpus VALUES (3, 'denied')",
        "UPDATE phase0.corpus SET title = 'denied' WHERE id = 1",
        "DELETE FROM phase0.corpus WHERE id = 1",
        "CREATE SCHEMA denied",
        "CREATE TEMP TABLE denied (id int)",
    ):
        denied = subprocess.run(
            [*psql("phase0_reader"), "-c", f"BEGIN READ WRITE; {statement}; ROLLBACK"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=8,
        )
        assert denied.returncode != 0 and "42501" in denied.stderr, denied.stderr
    assert sql("phase0_writer", "BEGIN; INSERT INTO phase0.corpus VALUES (3, 'writer') "
                               "RETURNING title; ROLLBACK") == "writer"
    run(compose("restart", "postgres"))
    run(compose("up", "-d", "--wait", "--wait-timeout", "65", "postgres"), timeout=75)
    assert sql("postgres", locale) == expected
    print("PostgreSQL UTF-8/ICU, simple text search, pg_trgm, verified TLS, permissions, lock and restart passed")


def inspect_profile(docker, container, image, uid, network_keys, project):
    details = json.loads(capture([*docker, "inspect", container]))[0]
    assert details["Image"] == image
    config = details["HostConfig"]
    assert config["ReadonlyRootfs"] and config["CapDrop"] == ["ALL"]
    assert config["SecurityOpt"] == ["no-new-privileges:true"]
    assert not config["Privileged"] and not config["PortBindings"]
    assert details["Config"]["User"] == f"{uid}:{uid}"
    assert set(details["NetworkSettings"]["Networks"]) == {
        f"{project}_{network}" for network in network_keys
    }


def integration(directory, project, image_prefix, context, skip_build):
    prepare(directory)
    os.environ.update(PHASE0_DIR=str(directory), PHASE0_IMAGE_PREFIX=image_prefix)
    docker = ["docker", "--context", context]

    def compose(*args):
        return [*docker, "compose", "-f", str(HERE / "compose.yml"), "-p", project, *args]

    engine = capture([*docker, "info", "--format", "{{.ServerVersion}} {{.Architecture}} {{json .SecurityOptions}}"])
    print(platform.release(), engine, capture(compose("version")), flush=True)
    if context == "rootless":
        assert "name=rootless" in engine
    if not skip_build:
        run(compose("build"), timeout=600)
    containers = {}
    try:
        run(compose("up", "-d", "--wait", "--wait-timeout", "65", "serve", "postgres", "fixtures"), timeout=110)
        containers = {
            service: capture(compose("ps", "-q", service)) for service in ("serve", "postgres", "fixtures")
        }
        # Keep the app images in their expected runtime profiles while calling
        # each real entrypoint for every probe.
        for service in ("sync", "feed"):
            containers[service] = capture(compose(
                "run", "-d", "--no-deps", "--entrypoint", "/bin/sleep", service, "infinity",
            )).splitlines()[-1]
        images = {}
        for service, container in containers.items():
            image = capture([*docker, "inspect", "--format", "{{.Image}}", container])
            images[service] = image
            print(f"{service} image {image}", flush=True)
            if service in ("serve", "sync", "feed"):
                assert image == capture([*docker, "image", "inspect", "--format", "{{.Id}}",
                                         f"{image_prefix}-{service}"])
                assert capture([*docker, "run", "--rm", "--read-only", "--network", "none",
                                "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true",
                                image, "--version"]).startswith(f"rfc-mcp-docker-{service} ")
        for service, uid, networks in (
            ("serve", 10001, ("ingress", "db_read", "sync_notify")),
            ("sync", 10002, ("db_write", "sync_notify", "feed_control", "sync_egress")),
            ("feed", 10003, ("feed_control", "feed_egress")),
            ("postgres", 70, ("db_read", "db_write")),
        ):
            inspect_profile(docker, containers[service], images[service], uid, networks, project)

        def execute(service, *args):
            return [*docker, "exec", containers[service], *args]

        for service in ("serve", "sync", "feed", "postgres"):
            run(execute(service, "sh", "-ec",
                        'test "$(id -u)" != 0; grep -Eq "^CapEff:[[:space:]]+0+$" /proc/self/status; '
                        'grep -Eq "^Seccomp:[[:space:]]+2$" /proc/self/status; '
                        'grep -Eq "^NoNewPrivs:[[:space:]]+1$" /proc/self/status; '
                        '! touch /phase0-root-denied'))
        run(execute("serve", "sh", "-ec", "test -r /phase0/db-password; ! touch /mirror/write-denied"))
        run(execute("feed", "sh", "-ec", "test ! -e /mirror; test ! -e /phase0/db-password"))

        # The fixed SSLRequest sent by a client yields one byte if the DB is
        # reachable. The same request from feed must yield no response.
        network_probe = r"""printf '\000\000\000\010\004\322\026\057' | nc -w 2 "$1" 5432 | dd bs=1 count=1 2>/dev/null"""
        for service in ("serve", "sync"):
            assert capture(execute(service, "sh", "-c", network_probe, "--", "postgres")) == "S"
        networks = json.loads(capture(
            [*docker, "inspect", "--format", "{{json .NetworkSettings.Networks}}", containers["postgres"]]
        ))
        for endpoint in networks.values():
            assert capture(execute("feed", "sh", "-c", network_probe, "--", endpoint["IPAddress"])) == ""
        print("Container user, root/mirror permissions, network reachability, capabilities and seccomp passed", flush=True)

        print(capture(execute("sync", "rsync", "--version")).splitlines()[0])
        run(execute("sync", "rsync", "--timeout=5", "rsync://fixtures:8873/fixture/sample.txt",
                    "/mirror/rsync-copy.txt"))
        assert (directory / "mirror" / "rsync-copy.txt").read_bytes() == (
            directory / "upstream" / "sample.txt"
        ).read_bytes()
        for service in ("sync", "feed"):
            run(execute(service, "test", "-s", "/etc/ssl/certs/ca-certificates.crt"))
            for host, ca, expected in (
                ("fixtures", "ca.crt", "ok"),
                ("fixtures", "wrong-ca.crt", "certificate-error"),
                ("wrong.fixture", "ca.crt", "certificate-error"),
            ):
                run(execute(service, f"/usr/local/bin/rfc-mcp-docker-{service}", "--phase0-https",
                            f"https://{host}:8443/", f"/phase0/{ca}", expected), timeout=10)
        print("Exact rsync transfer and Rustls trusted, wrong CA and wrong hostname cases passed", flush=True)
        postgres_probe(compose)
        # A disposable client joins only the serve container's network namespace
        # so its actual loopback listener stays private and unmodified.
        run([*docker, "run", "--rm", "--network", f"container:{containers['serve']}",
             "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true",
             "-v", f"{HERE / 'mcp-wire.py'}:/mcp-wire.py:ro", "--entrypoint", "python3",
             images["fixtures"], "/mcp-wire.py", "--port", "8780"], timeout=90)
        print(capture(execute("serve", "/usr/local/bin/rg", "--version")).splitlines()[0])
        output = capture(execute("serve", "timeout", "8", "/usr/local/bin/rfc-mcp-docker-serve",
                                 "--phase0-sandbox"), timeout=10)
        matches = [event["data"] for line in output.splitlines()
                   if (event := json.loads(line))["type"] == "match"]
        assert len(matches) == 1 and matches[0]["lines"]["text"] == "phase-zero match\n"

    except Exception:
        diagnostic = subprocess.run(compose("logs", "--no-color", "--tail", "25"),
                                    text=True, stdout=subprocess.PIPE, timeout=15)
        print(re.sub(r"(?<![0-9a-f])[0-9a-f]{48}(?![0-9a-f])", "[redacted]", diagnostic.stdout),
              file=sys.stderr)
        raise
    finally:
        for service in ("sync", "feed"):
            if container := containers.get(service):
                subprocess.run([*docker, "rm", "-f", container], stdout=subprocess.DEVNULL, timeout=20)
        # PostgreSQL owns its data as the mapped UID in a private temporary
        # bind mount. Clean it using that same UID before exiting the harness.
        if (directory / "pgdata" / "19").exists():
            run(compose("stop", "postgres"), timeout=30)
            run(compose("run", "--rm", "--no-deps", "--entrypoint", "/bin/sh", "postgres",
                        "-ec", "rm -rf /var/lib/postgresql/19"), timeout=30)
        run(compose("down", "--volumes"), timeout=60)


def main():
    if not __debug__:
        raise RuntimeError("Phase 0 requires enabled assertions")
    parser = argparse.ArgumentParser()
    parser.add_argument("--context", default="rootless")
    parser.add_argument("--project", help="reuse only the specified Phase 0 test image tags")
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()
    if args.skip_build and not args.project:
        parser.error("--skip-build requires --project with prebuilt image tags")
    with tempfile.TemporaryDirectory(prefix="rfc-mcp-phase0-", dir="/tmp") as path:
        directory = Path(path)
        project = directory.name.lower()
        integration(directory, project, args.project or project, args.context, args.skip_build)


if __name__ == "__main__":
    main()
