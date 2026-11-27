#!/usr/bin/env python3
"""Verify a quiescent deployment and migrate its PostgreSQL catalog."""

import argparse
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import re
import secrets
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS = ROOT / "migrations"
SERVE, SYNC, FEED = ("rfc-mcp-docker-" + role for role in ("serve", "sync", "feed"))
PSQL = ("psql", "-XqAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "rfc")
DUMP_AND_DECODE = """
set -o pipefail
pg_dump -Fc --schema=rfc --lock-wait-timeout=5000 -U postgres -d rfc | "$@"
"""
HISTORY = "SELECT version FROM rfc.schema_migrations ORDER BY version"
INVENTORY = {
    "pointer": "SELECT generation_id,publication_sequence FROM rfc.active_generation",
    "readable": "SELECT id FROM rfc.generations WHERE state IN ('published','retained') ORDER BY id",
    "blobs": "SELECT encode(digest,'hex'),byte_length FROM rfc.blobs ORDER BY digest",
}


def capture(command, timeout=60):
    return subprocess.check_output(command, text=True, timeout=timeout)


class Deployment:
    def __init__(self, args):
        self.docker = ["docker", *(["--context", args.context] if args.context else [])]
        self.project_name = args.project_name
        self.compose = [*self.docker, "compose", "--env-file", str(args.env_file),
                        "--project-name", args.project_name, "-f", str(ROOT / "compose.yml")]
        for overlay in args.file:
            self.compose.extend(("-f", str(overlay)))
        config = json.loads(capture([*self.compose, "config", "--format", "json"]))
        services = config["services"]
        self.live_paths = []
        for volume in ("mirror", "pgdata"):
            driver_options = config["volumes"][volume].get("driver_opts") or {}
            device = driver_options.get("device")
            if device:
                self.live_paths.append(Path(device).resolve())
        self.images = {service: services[service]["image"] for service in (SERVE, SYNC, FEED)}
        self.ids = {service: self.image_id(ref) for service, ref in self.images.items()}
        self.postgres = capture([*self.compose, "ps", "-q", "postgres"]).strip()
        if not self.postgres or "\n" in self.postgres:
            raise ValueError("start the existing PostgreSQL service before upgrading")
        mounts = json.loads(capture([*self.docker, "inspect", "--format", "{{json .Mounts}}",
                                    self.postgres]))
        if not any(mount["Destination"] == "/migrations" and mount["Type"] == "bind"
                   and not mount["RW"] and Path(mount["Source"]).resolve() == MIGRATIONS
                   for mount in mounts):
            raise ValueError("PostgreSQL must mount this checkout's migrations read-only")

    def image_id(self, ref):
        return capture([*self.docker, "image", "inspect", "--format", "{{.Id}}", ref]).strip()

    def verify_images(self):
        for service, ref in self.images.items():
            if self.image_id(ref) != self.ids[service]:
                raise ValueError(f"the local image for {service} changed during the upgrade")

    def require_stopped(self):
        command = [*self.docker, "ps", "--filter",
                   f"label=com.docker.compose.project={self.project_name}",
                   "--format", '{{.Label "com.docker.compose.service"}}']
        running = set(capture(command).splitlines()) & {SERVE, SYNC, FEED}
        if running:
            raise ValueError(f"stop these services and their one-shot commands: {', '.join(sorted(running))}")
        if capture([*self.compose, "ps", "-q", "postgres"]).strip() != self.postgres:
            raise ValueError("PostgreSQL was replaced during the upgrade")

    def schema_required(self, service):
        return capture([*self.docker, "run", "--rm", "--pull=never", "--network=none",
                        "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges",
                        self.ids[service], "--storage-schema-version"]).strip()

    def exec(self, *args):
        return [*self.compose, "exec", "-T", "postgres", *args]

    def probe(self, service, *options, timeout=90):
        self.verify_images()
        subprocess.run([*self.compose, "run", "--rm", "--no-deps", "-T", "--pull=never",
                        service, *options], check=True, timeout=timeout)
        self.verify_images()


@contextmanager
def peer_session(stack):
    # EOF closes the connection and releases session locks, including after a SQL error.
    with subprocess.Popen(stack.exec(*PSQL), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                          text=True) as process:
        def query(sql):
            marker = "upgrade_" + secrets.token_hex(16)
            process.stdin.write(f"{sql}\n\\echo {marker}\n")
            process.stdin.flush()
            output = []
            for line in process.stdout:
                if line.rstrip("\n") == marker:
                    return "".join(output)
                output.append(line)
            raise ValueError("the PostgreSQL maintenance session closed before completing the command")

        try:
            query("SET lock_timeout = '5s';")
            for lock, purpose in ((-2, "migration"), (-3, "Sync leader"), (-1, "corpus coordinator")):
                if query(f"SELECT pg_catalog.pg_try_advisory_lock({lock}::bigint);") != "t\n":
                    raise ValueError(f"the {purpose} lock is already held")
            yield query
        finally:
            process.stdin.close()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def decoded_digest(command, *, archive=None):
    with subprocess.Popen(command, stdin=archive, stdout=subprocess.PIPE) as process:
        digest = hashlib.file_digest(process.stdout, "sha256").digest()
        if process.wait():
            raise ValueError("could not decode the PostgreSQL dump")
    return digest


def backup_file(root, path):
    candidate = root / path
    if candidate.resolve() != candidate or not candidate.is_file():
        raise ValueError(f"backup file {path} must be a regular file in the backup directory")
    return candidate


def verify_backup(stack, backup, inventory):
    if any(backup == live or backup in live.parents or live in backup.parents for live in stack.live_paths):
        raise ValueError("backup directory overlaps the live PostgreSQL or mirror directory")
    for name, expected in inventory.items():
        if backup_file(backup, name).read_bytes() != expected.encode():
            raise ValueError(f"backup {name} differs from the quiescent database")

    key = secrets.token_hex(16)
    restore = ("pg_restore", "--exit-on-error", f"--restrict-key={key}", "--file=-")
    with backup_file(backup, "corpus.dump").open("rb") as archive:
        stored = decoded_digest(stack.exec(*restore), archive=archive)
    live = decoded_digest(stack.exec("/bin/sh", "-ec", DUMP_AND_DECODE, "upgrade", *restore))
    if stored != live:
        raise ValueError("backup corpus.dump differs from the quiescent database")

    for row in inventory["blobs"].splitlines():
        digest, length = row.split("|")
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError("invalid blob digest in database inventory")
        blob = backup_file(backup, Path("mirror", "blobs", "blake3", digest[:2], digest))
        if blob.stat().st_size != int(length):
            raise ValueError(f"backup blob {digest} has the wrong length")
        if capture(["b3sum", "--", str(blob)]).split()[0] != digest:
            raise ValueError(f"backup blob {digest} has the wrong BLAKE3 digest")


def upgrade(args):
    versions = sorted(int(path.name[:4]) for path in MIGRATIONS.glob("[0-9][0-9][0-9][0-9]-*.sql"))
    if not versions or versions != list(range(1, versions[-1] + 1)):
        raise ValueError("the checkout's migration files are incomplete")
    target = versions[-1]
    stack = Deployment(args)
    stack.require_stopped()
    for service in (SERVE, SYNC):
        expected = stack.schema_required(service)
        if expected != str(target):
            raise ValueError(f"{service} requires schema {expected}, checkout targets schema {target}")
    stack.probe(SYNC, "--check-config")

    with peer_session(stack) as query:
        if query("SELECT current_setting('server_version_num')::integer / 10000;") != "19\n":
            raise ValueError("the upgrade requires PostgreSQL 19")
        if query("SELECT to_regclass('rfc.schema_migrations') IS NOT NULL;") != "t\n":
            raise ValueError("database is uninitialized, see docs/deployment.md")
        history = [int(version) for version in query(HISTORY + ";").splitlines()]
        if history == versions:
            state = f"database was already at schema {target}; no migration applied"
        else:
            source = len(history)
            if source not in (9, 10, 11, 12) or history != versions[:source]:
                raise ValueError(f"unsupported source migration history {history}")
            if args.backup_dir is None:
                raise ValueError("--backup-dir is required before applying migrations")
            inventory = {name: query(sql + ";") for name, sql in INVENTORY.items()}
            verify_backup(stack, args.backup_dir.resolve(strict=True), inventory)
            pointer = inventory["pointer"].strip()
            readable = ",".join(inventory["readable"].splitlines())
            if not re.fullmatch(r"[0-9]*\|[0-9]+", pointer) or not re.fullmatch(r"(?:[0-9]+(?:,[0-9]+)*)?", readable):
                raise ValueError("invalid pointer or readable generations in the backup inventory")
            stack.verify_images()
            stack.require_stopped()
            query(f"\\set upgrade_from {source}\n\\set upgrade_to {target}\n"
                  f"\\set upgrade_pointer '{pointer}'\n\\set upgrade_readable '{readable}'\n"
                  "\\i /migrations/migrate.sql")
            state = f"migration committed from schema {source} to {target}"

    print(state, flush=True)
    try:
        stack.require_stopped()
        if target >= 13:
            stack.probe(SYNC, "policy", "refresh", timeout=900)
        for service in (SERVE, SYNC):
            stack.probe(service, "--phase1-check-storage")
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        raise ValueError(f"postflight failed after {state}: {error}") from error
    print("Both runtime storage checks passed. Start Serve and verify pinned reads before resuming Sync and Feed.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--project-name", required=True)
    parser.add_argument("--backup-dir", type=Path, help="verified independent backup required for migration")
    parser.add_argument("--context")
    parser.add_argument("-f", "--file", action="append", type=Path, default=[],
                        help="add an overlay after the repository Compose file")
    args = parser.parse_args()
    try:
        upgrade(args)
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        print(f"Upgrade failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
