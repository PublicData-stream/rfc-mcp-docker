#!/usr/bin/env python3
"""Deploy RFC MCP with automatic PostgreSQL maintenance when required."""

import argparse
import base64
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS = ROOT / "migrations"
SERVE, SYNC, FEED = ("rfc-mcp-docker-" + role for role in ("serve", "sync", "feed"))
PSQL = ("psql", "-XqAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "rfc")
DUMP_AND_DECODE = """
set -o pipefail
pg_dump -Fc --schema=rfc --lock-wait-timeout=5000 -U postgres -d rfc | "$@"
"""
HISTORY = "SELECT version FROM rfc.schema_migrations ORDER BY version"
SERVE_TEMP_LIMIT = "serve-resource-limits.sql"
SERVE_TEMP_OVERRIDE = """
SELECT coalesce(to_json((
    SELECT substring(item FROM position('=' IN item) + 1)
    FROM pg_catalog.pg_db_role_setting
    CROSS JOIN LATERAL pg_catalog.unnest(setconfig) AS config(item)
    WHERE setdatabase=(SELECT oid FROM pg_catalog.pg_database WHERE datname='rfc')
      AND setrole=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname='rfc_serve')
      AND item LIKE 'temp_file_limit=%'
))::text,'null')
"""
INVENTORY = {
    "pointer": "SELECT generation_id,publication_sequence FROM rfc.active_generation",
    "readable": "SELECT id FROM rfc.generations WHERE state IN ('published','retained') ORDER BY id",
    "blobs": "SELECT encode(digest,'hex'),byte_length FROM rfc.blobs ORDER BY digest",
}


def capture(command, timeout=60):
    return subprocess.check_output(command, text=True, timeout=timeout)


class Deployment:
    def __init__(self, args, *, manual=True):
        self.docker = ["docker", *(["--context", args.context] if args.context else [])]
        self.project_name = args.project_name
        self.compose = [*self.docker, "compose", "--env-file", str(args.env_file),
                        "--project-name", args.project_name, "-f", str(ROOT / "compose.yml")]
        for overlay in args.file:
            self.compose.extend(("-f", str(overlay)))
        config = json.loads(capture([*self.compose, "config", "--format", "json"]))
        services = config["services"]
        self.services = services
        self.secrets = config.get("secrets", {})
        self.volumes = config["volumes"]
        self.live_paths = []
        for volume in ("mirror", "pgdata"):
            driver_options = config["volumes"][volume].get("driver_opts") or {}
            device = driver_options.get("device")
            if device:
                self.live_paths.append(Path(device).resolve())
        roles = (SERVE, SYNC, FEED) if manual else (SERVE, SYNC, FEED, "db-maintenance", "postgres")
        self.images = {service: services[service]["image"] for service in roles}
        self.ids = {service: self.image_id(ref) for service, ref in self.images.items()}
        self.postgres = capture([*self.compose, "ps", "-q", "postgres"]).strip()
        if not manual:
            return
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

    def running_applications(self):
        command = [*self.docker, "ps", "--filter",
                   f"label=com.docker.compose.project={self.project_name}",
                   "--format", '{{.Label "com.docker.compose.service"}}']
        return set(capture(command).splitlines()) & {SERVE, SYNC, FEED}

    def require_stopped(self):
        running = self.running_applications()
        if running:
            raise ValueError(f"stop these services and their one-shot commands: {', '.join(sorted(running))}")
        if capture([*self.compose, "ps", "-q", "postgres"]).strip() != self.postgres:
            raise ValueError("PostgreSQL was replaced during the upgrade")

    def schema_required(self, service, image=None):
        return capture(self.image_command(image or self.ids[service], "--storage-schema-version")).strip()

    def image_command(self, image, option):
        return [*self.docker, "run", "--rm", "--pull=never", "--network=none",
                "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges", image, option]

    def native_search(self, image):
        probe = subprocess.run(self.image_command(image, "--online-search-rollout"),
                               capture_output=True, text=True, timeout=60)
        return probe.returncode == 0 and probe.stdout.strip() == "1"

    def exec(self, *args):
        return [*self.compose, "exec", "-T", "postgres", *args]

    def service_container(self, service):
        ids = capture([*self.compose, "ps", "--all", "--quiet", service]).splitlines()
        if not ids:
            return None
        if len(ids) != 1:
            raise ValueError(f"expected a single {service} container")
        container = json.loads(capture([*self.docker, "inspect", ids[0]]))[0]
        return {
            "id": container["Id"],
            "image": container["Image"],
            "reference": container["Config"]["Image"],
            "hash": container["Config"]["Labels"].get("com.docker.compose.config-hash"),
            "running": container["State"]["Running"],
            "started": container["State"]["StartedAt"],
        }

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
        if stack.native_search(stack.ids[SERVE]):
            query("\\i /migrations/serve-resource-limits.sql")

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


def run_compose(stack, *args, timeout=1800, **kwargs):
    return subprocess.run([*stack.compose, *args], check=True, timeout=timeout, **kwargs)


def validate_target(stack, *, running=True):
    maintenance = stack.services["db-maintenance"]["environment"]
    target = maintenance["RFC_STORAGE_SCHEMA_TARGET"]
    for role in (SERVE, SYNC):
        expected = stack.schema_required(role)
        if expected != target:
            raise ValueError(f"{role} requires schema {expected}, Compose targets schema {target}")
    stack.verify_images()
    run_compose(stack, "run", "--rm", "--no-deps", "-T", "--pull=never", "sync-config-preflight")
    command = [*stack.compose, "run", "--rm", "--no-deps", "-T", "--pull=never", "db-maintenance", "--check"]
    if stack.postgres and running:
        # Older Compose images expose only their in-container peer socket.
        current = live_database(stack)
        if current == (19, list(range(1, int(target) + 1))):
            command.append("--online-current")
    report = json.loads(capture(command, timeout=90))
    print(json.dumps({**report, "image_ids": stack.ids}, sort_keys=True), flush=True)
    stack.verify_images()
    return report


def live_database(stack):
    try:
        value = capture(stack.exec(*PSQL, "-c", "SELECT current_setting('server_version_num')::integer / 10000",
                                   "-c", HISTORY), timeout=15).splitlines()
        return int(value[0]), [int(version) for version in value[1:]]
    except (IndexError, ValueError, subprocess.SubprocessError):
        return None


@contextmanager
def deployment_lock(stack):
    root = Path("/tmp", f"rfc-deploy-{os.getuid()}")
    root.mkdir(mode=0o700, exist_ok=True)
    if root.is_symlink() or root.stat().st_uid != os.getuid() or root.stat().st_mode & 0o077:
        raise ValueError("deployment lock directory must be private")
    key = hashlib.sha256(json.dumps([stack.docker, stack.project_name]).encode()).hexdigest()
    fd = os.open(root / key, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield root / f"{key}.online-serve.json"
    finally:
        os.close(fd)


def apply_preflight(stack):
    running = stack.running_applications()
    if running:
        raise ValueError(f"stop these services and their one-shot commands: {', '.join(sorted(running))}")
    stack.verify_images()
    checks = ("serve-db-preflight", "sync-db-preflight", "sync-rights-preflight")
    try:
        run_compose(stack, "up", "-d", "--pull", "never", *checks)
        for service in checks:
            container = capture([*stack.compose, "ps", "-a", "-q", service]).strip()
            if not container:
                raise ValueError(f"Compose did not start {service}")
            code = capture([*stack.docker, "wait", container], timeout=1800).strip()
            if code != "0":
                raise ValueError(f"{service} exited with status {code}")
    except (ValueError, subprocess.SubprocessError):
        subprocess.run([*stack.compose, "logs", "--tail", "20", "db-maintenance", *checks],
                       check=False, timeout=30)
        raise
    stack.verify_images()


def serve_rpc(stack, method, name=None, arguments=None):
    version = "2026-07-28"
    prefix = "io.modelcontextprotocol/"
    params = {"_meta": {prefix + "protocolVersion": version, prefix + "clientCapabilities": {},
                        prefix + "clientInfo": {"name": "rfc-deploy", "version": "1"}}}
    headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream",
               "MCP-Protocol-Version": version, "Mcp-Method": method}
    if name:
        headers["Mcp-Name"] = name
        params.update(name=name, arguments=arguments)
    service = stack.services[SERVE]["environment"]
    headers["Host"] = service["RFC_MCP_HOSTS"].split(",")[0].strip()
    data = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    command = [*stack.compose, "exec", "-T", SERVE, "wget", "-qO-", "-T", "17",
               *(f"--header={key}: {value}" for key, value in headers.items()),
               "--post-data", json.dumps(data), f"http://{service['RFC_MCP_BIND']}/mcp"]
    reply = json.loads(capture(command, timeout=20))
    if reply.get("id") != 1 or "result" not in reply:
        raise ValueError(f"Serve rejected the deployment MCP {method} check")
    result = reply["result"]
    if result.get("isError"):
        raise ValueError(f"Serve MCP {name} reported {result.get('structuredContent', {}).get('code')}")
    return result


def verify_serve(stack, pointer):
    generation, sequence = pointer.split("|")
    deadline = time.monotonic() + 45
    while True:
        probe = subprocess.run([*stack.compose, "exec", "-T", SERVE, "wget", "-S", "-O", "/dev/null", "-T", "3",
                                "http://127.0.0.1:8781/ready"], capture_output=True, timeout=7)
        if (generation and probe.returncode == 0) or (not generation and b" 503 " in probe.stderr):
            break
        if time.monotonic() >= deadline:
            raise ValueError("Serve readiness does not match the active generation")
        time.sleep(1)
    serve_rpc(stack, "server/discover")
    if not generation:
        print("Serve discovery passed with the empty corpus", flush=True)
        return
    listing = serve_rpc(stack, "tools/call", "list", {"limit": 1})["structuredContent"]
    if (listing["generation_id"], listing["publication_sequence"]) != (generation, sequence):
        raise ValueError("Serve MCP list returned the wrong active generation or sequence")
    document = capture(stack.exec(*PSQL, "-c", """
SELECT d.rfc_id, encode(v.txt_digest, 'hex'), b.byte_length
FROM rfc.active_generation a
JOIN rfc.generation_documents d ON d.generation_id=a.generation_id
JOIN rfc.document_versions v ON v.identity=d.document_version
JOIN rfc.blobs b ON b.digest=v.txt_digest
JOIN rfc.source_rights r ON (r.generation_id,r.rfc_id)=(d.generation_id,d.rfc_id)
LEFT JOIN rfc.source_restrictions s ON s.rfc_id=d.rfc_id
WHERE jsonb_typeof(r.evidence->'whole')='object' AND (coalesce(s.denied,0) & 1)=0
  AND (b.byte_length <= 16384 OR (r.evidence->>'pages'='true' AND (coalesce(s.denied,0) & 2)=0))
ORDER BY b.byte_length, d.rfc_id LIMIT 1
""")).strip()
    if document:
        rfc, digest, length = document.split("|")
        original = bytearray()
        arguments = {"id": int(rfc), "format": "base64"}
        while True:
            time.sleep(0.21)
            result = serve_rpc(stack, "tools/call", "get", arguments)["structuredContent"]
            part = result["original"]
            if (result["generation_id"], result["publication_sequence"], part["digest"], part["start_byte"]) != (
                    generation, sequence, digest, len(original)):
                raise ValueError("Serve MCP get returned an inconsistent original or sequence")
            original.extend(base64.b64decode(part["content_base64"], validate=True))
            if len(original) != part["end_byte"] or len(original) > int(length):
                raise ValueError("Serve MCP get returned an invalid byte range")
            if not result["next_cursor"]:
                break
            if not part["content_base64"]:
                raise ValueError("Serve MCP get did not advance its cursor")
            arguments["cursor"] = result["next_cursor"]
        checksum = subprocess.check_output([*stack.docker, "run", "--rm", "-i", "--pull=never",
            "--network=none", "--read-only", "--cap-drop=ALL", "--entrypoint", "b3sum",
            stack.ids["db-maintenance"]], input=original, timeout=30).split()[0].decode()
        if len(original) != int(length) or checksum != digest:
            raise ValueError("Serve MCP original differs from the stored byte length or digest")
        print(f"Serve MCP verified byte-exact RFC {rfc}", flush=True)
    print(f"Serve MCP verified generation {generation} at sequence {sequence}", flush=True)


@contextmanager
def serve_override(stack, image):
    with tempfile.TemporaryDirectory(prefix="rfc-serve-image-", dir="/tmp") as directory:
        override = Path(directory, "compose.json")
        override.write_text(json.dumps({"services": {SERVE: {"image": image}}}))
        yield [*stack.compose, "-f", str(override)]


def config_hashes(compose):
    return dict(line.split() for line in capture([*compose, "config", "--hash", "*"]).splitlines())


def set_serve_temp_limit(stack):
    subprocess.run(stack.exec(*PSQL, "-f", "-"), input=(MIGRATIONS / SERVE_TEMP_LIMIT).read_text(),
                   check=True, text=True, timeout=20)


def restore_serve_temp_limit(stack, previous):
    current = json.loads(capture(stack.exec(*PSQL, "-c", SERVE_TEMP_OVERRIDE)))
    if current == previous:
        return
    if current != "32MB":
        raise ValueError("Serve resource settings changed independently during deployment")
    if previous is None:
        capture(stack.exec(*PSQL, "-c", "ALTER ROLE rfc_serve IN DATABASE rfc RESET temp_file_limit"))
    else:
        statement = r"""SELECT format(
            'ALTER ROLE rfc_serve IN DATABASE rfc SET temp_file_limit = %L', :'previous')
\gexec
"""
        subprocess.run(stack.exec(*PSQL, "-v", f"previous={previous}", "-f", "-"),
                       input=statement, text=True, check=True, timeout=20)


def online_state(stack):
    companions = ("postgres", SYNC, FEED, "db-maintenance")
    deployed = {service: stack.service_container(service) for service in (*companions, SERVE)}
    prior = deployed[SERVE]
    if prior is None or not prior["running"]:
        return None
    if not all(deployed[service] and deployed[service]["running"] for service in companions[:3]):
        return None
    hashes = config_hashes(stack.compose)
    if any(not deployed[service] or deployed[service]["hash"] != hashes.get(service)
           or deployed[service]["image"] != stack.ids[service] for service in companions):
        return None
    for name in ("pgdata", "mirror", "db_backups"):
        volume = stack.volumes[name]
        actual, = json.loads(capture([*stack.docker, "volume", "inspect", volume["name"]]))
        if actual["Driver"] != volume.get("driver", "local") or actual["Options"] != volume.get("driver_opts"):
            return None
    if prior["image"] == stack.ids[SERVE] and prior["hash"] != hashes.get(SERVE):
        return None
    with serve_override(stack, prior["reference"]) as compose:
        if prior["hash"] != config_hashes(compose).get(SERVE):
            return None
    version = stack.services["db-maintenance"]["environment"]["RFC_STORAGE_SCHEMA_TARGET"]
    if version != "13":
        return None
    actual = live_database(stack)
    if actual is None:
        raise ValueError("could not validate the live database for online Serve deployment")
    if actual != (19, list(range(1, 14))):
        return None
    return {"prior": prior, "companions": {service: deployed[service] for service in companions}}


def verify_online_serve(stack, *, native=False):
    deadline = time.monotonic() + 55
    query = "is::obsoleted:true OR is::obsoleted:false"
    context_query = """SELECT a.generation_id,a.publication_sequence,p.rights_epoch
        FROM rfc.active_generation a CROSS JOIN rfc.policy_state p WHERE a.singleton AND p.singleton"""

    def context():
        return capture(stack.exec(*PSQL, "-c", context_query)).strip()

    while True:
        before = context()
        generation, sequence, _ = before.split("|")
        probe = subprocess.run([*stack.compose, "exec", "-T", SERVE, "wget", "-S", "-O", "/dev/null",
                                "-T", "3", "http://127.0.0.1:8781/ready"], capture_output=True, timeout=7)
        ready = (generation and probe.returncode == 0) or (not generation and b" 503 " in probe.stderr)
        if ready:
            try:
                serve_rpc(stack, "server/discover")
                if generation:
                    listing = serve_rpc(stack, "tools/call", "list", {"limit": 1})["structuredContent"]
                    search = serve_rpc(stack, "tools/call", "search", {"query": query, "limit": 1})["structuredContent"]
                    for result in (listing, search):
                        if (result["generation_id"], result["publication_sequence"]) != (generation, sequence):
                            raise ValueError("Serve did not read the active generation during online deployment")
                    if native and search["scan_truncated"]:
                        raise ValueError("online Serve did not complete the native search smoke check")
            except (KeyError, ValueError, subprocess.SubprocessError):
                if context() == before:
                    raise
            else:
                if context() == before:
                    return
        if time.monotonic() >= deadline:
            raise ValueError("Serve readiness or active generation did not stabilize during online deployment")
        time.sleep(0.5)


def verify_companions(stack, companions):
    for service, container in companions.items():
        if stack.service_container(service) != container:
            raise ValueError(f"{service} changed during online Serve deployment")


def recover_online_serve(stack, journal, state):
    verify_companions(stack, state["companions"])
    prior = state["prior"]
    if stack.image_id(prior["image"]) != prior["image"]:
        raise ValueError("previous Serve image is unavailable for rollback")
    with serve_override(stack, prior["reference"]) as compose:
        if config_hashes(compose).get(SERVE) != prior["hash"]:
            raise ValueError("Serve configuration changed since the interrupted deployment")
    restore_serve_temp_limit(stack, state["temp_override"])
    current = stack.service_container(SERVE)
    if current is None or not current["running"] or current["image"] != prior["image"]:
        image = prior["image"]
        try:
            if stack.image_id(prior["reference"]) == image:
                image = prior["reference"]
        except subprocess.CalledProcessError:
            pass
        with serve_override(stack, image) as compose:
            subprocess.run([*compose, "up", "-d", "--no-deps", "--force-recreate",
                            "--pull", "never", SERVE], check=True, timeout=70)
    verify_online_serve(stack)
    verify_companions(stack, state["companions"])
    journal.unlink()
    print("Restored the previous Serve image", flush=True)


def online_deploy(stack, journal, state, native):
    prior = state["prior"]
    if not native and not stack.native_search(prior["image"]):
        return False
    if any(stack.schema_required(service) != "13" for service in (SERVE, SYNC)):
        raise ValueError("online Serve deployment requires schema 13 images")
    if stack.schema_required(SERVE, prior["image"]) != "13":
        raise ValueError("previous Serve image requires a different storage schema")
    if prior["image"] == stack.ids[SERVE]:
        if native:
            set_serve_temp_limit(stack)
            stack.probe("serve-db-preflight", "--phase1-check-storage")
        verify_online_serve(stack, native=native)
        print("Serve is already running the requested image", flush=True)
        return True

    secret = next(item for item in stack.services[SERVE]["secrets"] if item.get("target") == "cursor_key")
    path = Path(stack.secrets[secret["source"]]["file"])
    installed = capture([*stack.docker, "exec", prior["id"], "sha256sum", "/run/secrets/cursor_key"])
    candidate = capture([*stack.docker, "run", "--rm", "--pull=never", "--network=none",
                         "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges",
                         "--user", stack.services[SERVE]["user"], "--entrypoint", "/bin/sh",
                         "--mount", f"type=bind,src={path},dst=/run/cursor-key,readonly",
                         stack.ids[SERVE], "-ec", 'test "$(wc -c </run/cursor-key)" -eq 48; '
                         'sha256sum /run/cursor-key'])
    if candidate.split()[0] != installed.split()[0]:
        raise ValueError("online Serve deployment requires the installed cursor key")
    state["temp_override"] = json.loads(capture(stack.exec(*PSQL, "-c", SERVE_TEMP_OVERRIDE)))
    with tempfile.NamedTemporaryFile(mode="w", dir=journal.parent, delete=False) as pending:
        try:
            json.dump(state, pending)
            pending.flush()
            os.fsync(pending.fileno())
            os.replace(pending.name, journal)
        finally:
            Path(pending.name).unlink(missing_ok=True)

    try:
        if native:
            set_serve_temp_limit(stack)
        stack.probe("serve-db-preflight", "--phase1-check-storage")
        verify_companions(stack, state["companions"])
        run_compose(stack, "up", "-d", "--no-deps", "--force-recreate",
                    "--pull", "never", "--wait", "--wait-timeout", "30", SERVE, timeout=70)
        if stack.service_container(SERVE)["image"] != stack.ids[SERVE]:
            raise ValueError("online Serve deployment started an unexpected image")
        verify_online_serve(stack, native=native)
        verify_companions(stack, state["companions"])
    except (KeyError, ValueError, OSError, subprocess.SubprocessError):
        recover_online_serve(stack, journal, state)
        raise
    journal.unlink()
    print("Replaced Serve with PostgreSQL, Sync, and Feed online", flush=True)
    return True


def automatic(args):
    stack = Deployment(args, manual=False)
    if args.command == "preflight" and not args.apply:
        validate_target(stack)
        return
    with deployment_lock(stack) as journal:
        if journal.exists():
            recover_online_serve(stack, journal, json.loads(journal.read_text()))
        native = stack.native_search(stack.ids[SERVE])
        if args.command == "deploy":
            state = online_state(stack)
            if state and online_deploy(stack, journal, state, native):
                return
        report = validate_target(stack)
        if args.command == "preflight":
            if stack.postgres and report["history"] != list(range(1, report["target_schema"] + 1)):
                raise ValueError("stop the running PostgreSQL service before applying maintenance")
            if native and stack.postgres:
                stack.require_stopped()
                set_serve_temp_limit(stack)
            apply_preflight(stack)
            return
        for service in (FEED, SYNC, SERVE):
            if service in stack.running_applications():
                run_compose(stack, "stop", "-t", "60", service, timeout=85)
        if stack.running_applications():
            raise ValueError("application containers are still running, including one-shot commands")
        if stack.postgres and report["history"] != list(range(1, report["target_schema"] + 1)):
            run_compose(stack, "stop", "-t", "60", "postgres", timeout=85)
        elif native and stack.postgres:
            set_serve_temp_limit(stack)
        apply_preflight(stack)
        pointer = capture(stack.exec(*PSQL, "-c", INVENTORY["pointer"])).strip()
        run_compose(stack, "up", "-d", "--wait", "--wait-timeout", "60", "--pull", "never", SERVE)
        try:
            verify_serve(stack, pointer)
        except (KeyError, OSError, ValueError, subprocess.SubprocessError):
            run_compose(stack, "stop", "-t", "30", SERVE, timeout=55)
            raise
        run_compose(stack, "up", "-d", "--pull", "never", SYNC, FEED)



def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("preflight", "deploy"), nargs="?")
    parser.add_argument("--apply", action="store_true", help="apply preflight without starting application services")
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--project-name", required=True)
    parser.add_argument("--backup-dir", type=Path, help="verified independent backup required for migration")
    parser.add_argument("--context")
    parser.add_argument("-f", "--file", action="append", type=Path, default=[],
                        help="add an overlay after the repository Compose file")
    args = parser.parse_args()
    if args.apply and args.command != "preflight":
        parser.error("--apply requires preflight")
    if args.backup_dir and args.command:
        parser.error("--backup-dir applies to the manual recovery command")
    try:
        if args.command:
            automatic(args)
        else:
            upgrade(args)
    except (KeyError, ValueError, OSError, subprocess.SubprocessError) as error:
        print(f"Upgrade failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
