#!/usr/bin/env python3
"""Prepare the database on the locked volume before Compose admits clients."""

import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import subprocess
import sys
import tempfile
import time


ROOT = Path("/var/lib/postgresql")
MIRROR = Path("/mirror")
BACKUPS = Path("/backups")
MIGRATIONS = Path("/migrations")
TARGET = ROOT / "19/docker"
RECEIPT = ".rfc-pg18-import.json"
MANAGED = ROOT / ".rfc-managed-system-id"
SECRET_DIR = Path("/run/secrets")
ROLES = ("postgres", "rfc_serve", "rfc_sync")
INVENTORY = {
    "pointer": "SELECT generation_id,publication_sequence FROM rfc.active_generation",
    "readable": "SELECT id FROM rfc.generations WHERE state IN ('published','retained') ORDER BY id",
    "blobs": "SELECT encode(digest,'hex'),byte_length FROM rfc.blobs ORDER BY digest",
}
HISTORY = "SELECT version FROM rfc.schema_migrations ORDER BY version"
PG_ENV = {"PATH": "/usr/local/bin:/usr/bin:/bin", "LC_ALL": "C", "PGCONNECT_TIMEOUT": "5",
          "PGOPTIONS": "-c timezone=UTC -c statement_timeout=0 -c lock_timeout=5000"}
INIT = ("--encoding=UTF8", "--locale=C", "--locale-provider=icu", "--icu-locale=und",
        "--auth-host=scram-sha-256", "--auth-local=peer", "--username=postgres")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def tool(name, major=19):
    return str(Path("/usr/local/bin" if major == 19 else "/opt/postgresql18/bin") / name)


def run(command, **options):
    return subprocess.run(command, env=PG_ENV, check=True, **options)


def output(*command):
    return run(command, stdout=subprocess.PIPE, text=True).stdout


def fsync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def publish_json(path, data):
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("x") as stream:
        json.dump(data, stream, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.rename(path)
    fsync_directory(path.parent)


def migration_target():
    files = sorted(MIGRATIONS.glob("[0-9][0-9][0-9][0-9]-*.sql"))
    versions = [int(path.name[:4]) for path in files]
    expected = int(os.environ["RFC_STORAGE_SCHEMA_TARGET"])
    require(versions == list(range(1, expected + 1)) and expected >= 13,
            "maintenance SQL and configured application schema target differ")
    return expected


def relative_source():
    value = os.environ.get("RFC_POSTGRES18_DATA_DIR") or "18/docker"
    path = ROOT / value
    require(not Path(value).is_absolute() and ".." not in Path(value).parts
            and path not in (ROOT, TARGET) and ROOT in path.parents
            and TARGET not in path.parents and path not in TARGET.parents
            and path.resolve() == path and not path.relative_to(ROOT).parts[0].startswith("."),
            "RFC_POSTGRES18_DATA_DIR must name a separate, real directory beneath PGDATA")
    return path


def version(path):
    require(path.resolve() == path, "symlinks in PostgreSQL data paths are unsupported")
    marker = path / "PG_VERSION"
    if marker.exists() or marker.is_symlink():
        require(path.resolve() == path and marker.is_file() and not marker.is_symlink(),
                "invalid PostgreSQL version marker")
        return marker.read_text().strip()
    require(not path.exists() or (path.is_dir() and not any(path.iterdir())),
            f"incomplete PostgreSQL directory {path.relative_to(ROOT)}")
    return None


def control(path, major):
    data = output(tool("pg_controldata", major), str(path))
    values = dict(line.split(":", 1) for line in data.splitlines() if ":" in line)
    return values["Database system identifier"].strip(), values["Database cluster state"].strip()


def locate():
    old = relative_source()
    known = {TARGET, old}
    for item in (ROOT, ROOT / "data", *ROOT.glob("*/docker")):
        if item not in known:
            require(not (item / "PG_VERSION").exists(), "ambiguous or unsupported PostgreSQL data directory")
    current, previous = version(TARGET), version(old)
    require(current in (None, "19") and previous in (None, "18"), "only PostgreSQL 18 and 19 are supported")
    if current and previous:
        receipt = TARGET / RECEIPT
        require(receipt.is_file() and not receipt.is_symlink(), "PostgreSQL 18 and 19 both exist without a cutover receipt")
        saved = json.loads(receipt.read_text())
        require(saved["source"] == control(old, 18)[0] and saved["target"] == control(TARGET, 19)[0],
                "PostgreSQL cutover receipt belongs to different database volumes")
    if not (current or previous):
        allowed = {".rfc-maintenance.lock", ".rfc-peer", ".rfc-work", "19", "lost+found"}
        require(all(path.name in allowed for path in ROOT.iterdir()),
                "database volume is not empty and has no recognized cluster")
    if current:
        return TARGET, 19
    return (old, 18) if previous else (TARGET, None)


def sql(socket, statement=None, *, database="rfc", args=(), secret=None):
    command = [tool("psql"), "-XqAt", "-v", "ON_ERROR_STOP=1", "-h", str(socket),
               "-U", "postgres", "-d", database, *args]
    if statement is not None:
        command.extend(("-c", statement))
    return run(command, input=secret, text=True, stdout=subprocess.PIPE).stdout


@contextmanager
def cluster(data, major, socket, *, managed_identity=None):
    require(not (data / "postmaster.pid").exists(),
            "PostgreSQL is running or left a PID file. Stop it cleanly before maintenance.")
    socket.mkdir(mode=0o700)
    hba = socket / "pg_hba.conf"
    hba.write_text("local all postgres peer\nlocal all all reject\n")
    options = (f"-c listen_addresses= -c unix_socket_directories={socket} "
               f"-c unix_socket_permissions=0700 -c ssl=off -c hba_file={hba} "
               "-c shared_preload_libraries= -c archive_mode=off -c log_statement=none "
               "-c log_min_error_statement=panic -c logging_collector=off")
    # A container stop terminates the entire private PID namespace.
    pg_ctl = [tool("pg_ctl", major), "-D", str(data), "-w", "-t", "60"]
    run([*pg_ctl, "-l", str(socket / "postgres.log"), "-o", options, "start"])
    try:
        if managed_identity is not None:
            pidfile = (data / "postmaster.pid").read_text().splitlines()[:3]
            token = "\n".join([managed_identity, *pidfile]) + "\n"
            with MANAGED.open("w") as marker:
                marker.write(token)
                marker.flush()
                os.fsync(marker.fileno())
            fsync_directory(ROOT)
        yield socket
    finally:
        run([*pg_ctl, "-m", "fast", "stop"])


def init(data):
    run([tool("initdb"), "-D", str(data), *INIT], stdout=subprocess.DEVNULL)


def password(name):
    data = (SECRET_DIR / name).read_bytes()
    if data.endswith(b"\n"):
        data = data[:-1].removesuffix(b"\r")
    value = data.decode("utf-8")
    require(0 < len(data) <= 4096 and not any(char in value for char in "\x00\r\n"),
            f"invalid password file for {name}")
    return value


def role_password(socket, role, name):
    value = password(name)
    sql(socket, rf"\password {role}", database="postgres", secret=f"{value}\n{value}\n")


def bootstrap(socket, *, create_database=True, verifiers=None):
    if create_database:
        sql(socket, "CREATE DATABASE rfc", database="postgres")
    sql(socket, args=("-f", str(MIGRATIONS / "roles.sql")))
    for role in ROLES:
        if verifiers is None:
            role_password(socket, role, role)
        elif verifiers[role]:
            verifier = verifiers[role]
            require(re.fullmatch(r"SCRAM-SHA-256\$[0-9]+:[A-Za-z0-9+/=]+\$[A-Za-z0-9+/=]+:[A-Za-z0-9+/=]+",
                                 verifier), f"{role} requires a SCRAM password verifier")
            sql(socket, args=("-f", "-"), secret=f"ALTER ROLE {role} PASSWORD '{verifier}';\n")
    sql(socket, args=("-f", str(MIGRATIONS / "bootstrap.sql")))


def get_history(socket, *, readonly=False):
    prefix = "SET default_transaction_read_only=on; " if readonly else ""
    if sql(socket, prefix + "SELECT to_regclass('rfc.schema_migrations') IS NOT NULL").strip() != "t":
        return []
    return [int(row) for row in sql(socket, prefix + HISTORY).splitlines()]


def expect_history(history, target):
    require(history == list(range(1, len(history) + 1)) and 9 <= len(history) <= target,
            f"unsupported source migration history {history}; expected a complete history from schema 9 to {target}")


def inventory(socket):
    return {name: sql(socket, query) for name, query in INVENTORY.items()}


def dedicated(socket):
    require(sql(socket, "SELECT datname FROM pg_database ORDER BY datname", database="postgres").splitlines()
            == ["postgres", "rfc", "template0", "template1"], "expected a dedicated RFC PostgreSQL cluster")
    require(sql(socket, "SELECT rolname FROM pg_roles WHERE left(rolname,3) <> 'pg_' ORDER BY rolname").splitlines()
            == ["postgres", "rfc_owner", "rfc_serve", "rfc_sync"], "unexpected PostgreSQL roles")
    require(sql(socket, "SELECT count(*) FROM pg_auth_members WHERE member IN "
                "(SELECT oid FROM pg_roles WHERE rolname IN ('rfc_owner','rfc_serve','rfc_sync'))").strip() == "0",
            "unexpected RFC role memberships")
    require(sql(socket, "SELECT count(*) FROM pg_roles WHERE rolname IN ('rfc_owner','rfc_serve','rfc_sync') "
                "AND (rolsuper OR rolinherit OR rolcreaterole OR rolcreatedb OR rolreplication OR rolbypassrls "
                "OR rolcanlogin <> (rolname <> 'rfc_owner'))").strip() == "0", "unexpected RFC role attributes")
    require(sql(socket, "SELECT current_setting('server_encoding') || ':' || datlocale || ':' || datlocprovider::text "
                "FROM pg_database WHERE datname='rfc'").strip() == "UTF8:und:i",
            "the RFC database must use the supported UTF-8 ICU und locale")
    require(sql(socket, "SELECT nspname FROM pg_namespace WHERE left(nspname,3) <> 'pg_' ORDER BY nspname").splitlines()
            == ["information_schema", "public", "rfc"], "unexpected schema in the dedicated RFC database")
    require(sql(socket, "SELECT extname FROM pg_extension ORDER BY extname").splitlines() == ["plpgsql"],
            "unexpected extension in the dedicated RFC database")


def role_state(socket):
    return sql(socket, """
SELECT r.rolname, r.rolcanlogin, r.rolinherit, r.rolsuper, r.rolcreaterole, r.rolcreatedb,
       r.rolreplication, r.rolbypassrls, r.rolconnlimit, coalesce(r.rolvaliduntil::text,''),
       coalesce(s.setconfig::text,''),
       has_database_privilege(r.oid, 'rfc', 'CONNECT'),
       has_database_privilege(r.oid, 'rfc', 'CREATE'),
       has_database_privilege(r.oid, 'rfc', 'TEMP'),
       has_schema_privilege(r.oid, 'public', 'USAGE'),
       has_schema_privilege(r.oid, 'public', 'CREATE'),
       has_function_privilege(r.oid, 'pg_catalog.pg_try_advisory_lock(bigint)', 'EXECUTE'),
       has_function_privilege(r.oid, 'pg_catalog.pg_advisory_xact_lock_shared(bigint)', 'EXECUTE')
FROM pg_roles r LEFT JOIN pg_db_role_setting s ON s.setrole=r.oid
  AND s.setdatabase=(SELECT oid FROM pg_database WHERE datname='rfc')
WHERE r.rolname IN ('rfc_serve','rfc_sync','rfc_owner') ORDER BY r.rolname
""")


def fingerprint(socket, *, rebuild_search=False):
    catalog = sql(socket, """
SELECT c.relkind, format('%I',c.relname), pg_get_userbyid(c.relowner), coalesce(c.relacl::text,'')
FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE n.nspname='rfc' AND c.relkind IN ('r','p','S','v','m') ORDER BY c.relname COLLATE "C"
""")
    shape = sql(socket, """
SELECT c.relname, a.attname, pg_catalog.format_type(a.atttypid,a.atttypmod),
       a.attnotnull, coalesce(a.attacl::text,'')
FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid
JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE n.nspname='rfc' AND c.relkind IN ('r','p','v','m') AND a.attnum>0 AND NOT a.attisdropped
ORDER BY c.relname COLLATE "C", a.attnum
""")
    routines = sql(socket, """
SELECT p.oid::regprocedure, pg_get_userbyid(p.proowner), coalesce(p.proacl::text,'')
FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='rfc'
ORDER BY p.oid::regprocedure::text COLLATE "C"
""")
    state = {name: hashlib.sha256(value.encode()).hexdigest()
             for name, value in (("relations", catalog), ("columns", shape),
                                 ("routine grants", routines), ("runtime roles", role_state(socket)))}
    for row in catalog.splitlines():
        kind, relation, *_ = row.split("|")
        require(re.fullmatch(r"[a-z_][a-z_0-9]*", relation), "unexpected RFC relation name")
        if kind == "S":
            state[relation] = hashlib.sha256(sql(socket, f"SELECT last_value,is_called FROM rfc.{relation}").encode()).hexdigest()
        elif kind in ("r", "p", "m"):
            # PostgreSQL rebuilds this generated column from the immutable search text.
            row = "to_jsonb(t) - 'search_vector'" if rebuild_search and relation == "text_chunks" else "to_jsonb(t)"
            command = [tool("psql"), "-XqAt", "-v", "ON_ERROR_STOP=1", "-h", str(socket),
                       "-U", "postgres", "-d", "rfc", "-c",
                       f'COPY (SELECT ({row})::text FROM ONLY rfc.{relation} t '
                       f'ORDER BY ({row})::text COLLATE "C") TO STDOUT']
            with subprocess.Popen(command, env=PG_ENV, stdout=subprocess.PIPE) as process:
                state[relation] = hashlib.file_digest(process.stdout, "sha256").hexdigest()
                require(process.wait() == 0, f"could not hash RFC relation {relation}")
    return state


def check_backup_location():
    require(BACKUPS.is_dir() and BACKUPS.stat().st_uid == os.getuid()
            and BACKUPS.stat().st_mode & 0o077 == 0 and os.access(BACKUPS, os.W_OK | os.X_OK),
            "backup directory must be private and owned by PostgreSQL")
    require(BACKUPS.stat().st_dev not in (ROOT.stat().st_dev, MIRROR.stat().st_dev),
            "backup storage must be on a different filesystem from PostgreSQL and the mirror")


def copy_blobs(directory, rows):
    for row in rows.splitlines():
        digest, length = row.split("|")
        require(re.fullmatch(r"[0-9a-f]{64}", digest) and int(length) >= 0, "invalid blob inventory row")
        relative = Path("blobs", "blake3", digest[:2], digest)
        source, copy = MIRROR / relative, directory / "mirror" / relative
        require(source.resolve() == source and source.is_file(), f"unsafe or missing mirror blob {digest}")
        copy.parent.mkdir(parents=True, exist_ok=True)
        with source.open("rb") as original, copy.open("xb") as saved:
            shutil.copyfileobj(original, saved)
            saved.flush()
            os.fsync(saved.fileno())
        require(copy.stat().st_size == int(length)
                and output("b3sum", "--", str(copy)).split()[0] == digest,
                f"incorrect length or BLAKE3 for backup blob {digest}")


def backup(source, restore, source_major, source_id, history, files):
    check_backup_location()
    token = f"pg{source_major}-schema{len(history)}-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-{secrets.token_hex(6)}"
    partial = Path(tempfile.mkdtemp(prefix=".incomplete-", dir=BACKUPS))
    completed = BACKUPS / token
    archive = partial / "corpus.dump"
    try:
        for name, value in files.items():
            (partial / name).write_text(value)
        run([tool("pg_dump"), "-h", str(source), "-U", "postgres", "-d", "rfc", "-Fc",
             "--schema=rfc", "--lock-wait-timeout=5000", "-f", str(archive)])
        copy_blobs(partial, files["blobs"])
        verifiers = dict(row.split("|", 1) for row in sql(source,
            "SELECT rolname,rolpassword FROM pg_authid WHERE rolname IN ('postgres','rfc_serve','rfc_sync')").splitlines())
        bootstrap(restore, verifiers=verifiers)
        run([tool("pg_restore"), "-h", str(restore), "-U", "postgres", "-d", "rfc",
             "--exit-on-error", "--single-transaction", str(archive)])
        expected = fingerprint(source, rebuild_search=source_major == 18)
        restored = fingerprint(restore, rebuild_search=source_major == 18)
        if source_major == 18:
            require(sql(restore, "SELECT NOT EXISTS (SELECT FROM rfc.text_chunks WHERE search_vector "
                        "IS DISTINCT FROM to_tsvector('pg_catalog.simple'::regconfig, search_text))").strip() == "t",
                    "PostgreSQL 19 restore has inconsistent generated search vectors")
        changes = sorted(name for name in expected.keys() | restored.keys() if expected.get(name) != restored.get(name))
        require(not changes, f"backup restore differs from the source in: {', '.join(changes)}")
        require(inventory(restore) == files, "backup restore changed the publication or blob inventory")
        with archive.open("rb") as dump:
            archive_sha = hashlib.file_digest(dump, "sha256").hexdigest()
        publish_json(partial / "receipt.json", {
            "postgres_major": source_major, "system_identifier": source_id, "history": history,
            "catalog_sha256": hashlib.sha256(json.dumps(expected, sort_keys=True).encode()).hexdigest(),
            "archive_sha256": archive_sha,
        })
        for root, _, names in os.walk(partial, topdown=False):
            for name in names:
                with (Path(root) / name).open("rb") as stream:
                    os.fsync(stream.fileno())
            fsync_directory(Path(root))
        partial.rename(completed)
        fsync_directory(BACKUPS)
    finally:
        if partial.exists():
            shutil.rmtree(partial)
    print(f"Verified backup: {token}", flush=True)
    return token


def migrate(socket, history, target, files=None):
    args = []
    if files is not None:
        pointer = files["pointer"].strip()
        readable = ",".join(files["readable"].splitlines())
        require(re.fullmatch(r"[0-9]*\|[0-9]+", pointer), "invalid active pointer")
        args = ["-v", f"upgrade_from={len(history)}", "-v", f"upgrade_to={target}",
                "-v", f"upgrade_pointer={pointer}", "-v", f"upgrade_readable={readable}"]
    sql(socket, args=(*args, "-f", str(MIGRATIONS / "migrate.sql")))
    require(get_history(socket) == list(range(1, target + 1)), "unexpected migration history after SQL")
    if files is not None:
        require(inventory(socket) == files, "migration changed the publication or blob inventory")


def install(data):
    TARGET.parent.mkdir(mode=0o700, exist_ok=True)
    if TARGET.exists():
        TARGET.rmdir()
    data.rename(TARGET)
    fsync_directory(TARGET.parent)
    fsync_directory(data.parent)


def prepare(source, major, target):
    work_root = ROOT / ".rfc-work"
    work_root.mkdir(mode=0o700, exist_ok=True)
    require(work_root.resolve() == work_root, "unsafe database maintenance work directory")
    # All temporary servers run inside this job's private PID namespace.
    for abandoned in work_root.iterdir():
        require(abandoned.name.startswith("attempt-") and abandoned.is_dir() and not abandoned.is_symlink(),
                "unexpected database maintenance work directory entry")
        shutil.rmtree(abandoned)
    with tempfile.TemporaryDirectory(prefix="attempt-", dir=work_root) as temp, \
         tempfile.TemporaryDirectory(prefix="rfc-db-", dir="/tmp") as sockets:
        work, sockets = Path(temp), Path(sockets)
        staged = work / "target"
        if major is None:
            init(staged)
            with cluster(staged, 19, sockets / "fresh") as fresh:
                bootstrap(fresh)
                migrate(fresh, [], target)
            install(staged)
            print(f"Initialized PostgreSQL 19 at application schema {target}")
            return
        source_id, state = control(source, major)
        if major == 18:
            require(state == "shut down" and not (source / "postmaster.pid").exists(),
                    "stop PostgreSQL 18 cleanly before importing it")
            require(not any(item.is_symlink() for item in source.rglob("*")),
                    "external paths and tablespaces in the PostgreSQL 18 cluster are unsupported")
            copied = Path(shutil.copytree(source, work / "source"))
            require(not (source / "postmaster.pid").exists()
                    and (source / "global/pg_control").read_bytes() == (copied / "global/pg_control").read_bytes(),
                    "PostgreSQL 18 changed while its stopped cluster was being copied")
            source = copied
        else:
            pidfile = source / "postmaster.pid"
            if pidfile.exists():
                token = "\n".join([source_id, *pidfile.read_text().splitlines()[:3]]) + "\n"
                require(MANAGED.is_file() and MANAGED.read_text() == token,
                        "stop the unmanaged PostgreSQL 19 cluster cleanly before migration")
                # This PID file was left by a previous holder of the volume lock.
                pidfile.unlink()
        with cluster(source, major, sockets / "source", managed_identity=source_id if major == 19 else None) as live:
            history = get_history(live)
            expect_history(history, target)
            if major == 19 and len(history) == target:
                print(f"PostgreSQL 19 already uses application schema {target}")
                return
            dedicated(live)
            files = inventory(live)
            init(staged)
            with cluster(staged, 19, sockets / "restore") as restored:
                token = backup(live, restored, major, source_id, history, files)
                migrate(restored if major == 18 else live, history, target, files)
        if major == 18:
            publish_json(staged / RECEIPT, {"source": source_id, "target": control(staged, 19)[0], "backup": token})
            install(staged)
        print(f"Database prepared: PostgreSQL {major} to 19, application schema {len(history)} to {target}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", action="version",
                        version="rfc-mcp-docker-maintenance " + os.getenv("RFC_MAINTENANCE_VERSION", "0.0.0"))
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--online-current", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    os.umask(0o077)
    target = migration_target()
    if args.check:
        source, major = locate()
        report = {"postgres_major": major, "target_schema": target, "history": None,
                  "data_directory": str(source.relative_to(ROOT))}
        if major == 19 and (ROOT / ".rfc-peer/.s.PGSQL.5432").is_socket():
            try:
                report["history"] = get_history(ROOT / ".rfc-peer", readonly=True)
            except subprocess.CalledProcessError:
                pass  # A stopped container can leave a stale socket entry.
            else:
                expect_history(report["history"], target)
        if args.online_current and major == 19 and report["history"] is None:
            report["history"] = list(range(1, target + 1))
        if major is not None and (major != 19 or report["history"] != list(range(1, target + 1))):
            check_backup_location()
            space = os.statvfs(BACKUPS)
            report["backup_available_bytes"] = space.f_bavail * space.f_frsize
        print(json.dumps(report, sort_keys=True))
        return
    with (ROOT / ".rfc-maintenance.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            socket = ROOT / ".rfc-peer"
            require((socket / ".s.PGSQL.5432").is_socket(), "database maintenance lock is already held")
            require(get_history(socket, readonly=True) == list(range(1, target + 1)),
                    "running PostgreSQL requires maintenance. Stop the stack or use the deploy command.")
            print(f"Running PostgreSQL verified at application schema {target}")
            return
        prepare(*locate(), target)


def terminate(_signum, _frame):
    raise InterruptedError("database maintenance was stopped")


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, terminate)
    try:
        main()
    except (KeyError, OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"Database maintenance failed: {error}", file=sys.stderr)
        sys.exit(1)
