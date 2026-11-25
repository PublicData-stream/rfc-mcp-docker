#!/usr/bin/env python3
"""Exercise rootless production Compose with synthetic originals and local TLS."""

import argparse
from contextlib import contextmanager, ExitStack
import importlib.util
from ipaddress import ip_address, ip_network
import json
import os
from pathlib import Path
import secrets
import select
import subprocess
import sys
import tempfile
import time

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
PHASE5 = HERE.parent / "phase5"
spec = importlib.util.spec_from_file_location("phase5", PHASE5 / "integration.py")
phase5 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(phase5)
phase1 = phase5.phase1
phase0 = phase1.phase0
SERVE, SYNC, FEED = ("rfc-mcp-docker-" + role for role in ("serve", "sync", "feed"))
INGESTION_PATH = "/etc/rfc-mcp/ingestion.toml"


class Deployment:
    def __init__(self, work, seed, prefix, spool, audit):
        self.work, self.docker = work, seed.docker
        self.ca = seed.directory / "certs/ca.crt"
        self.name = work.name + "-prod"
        self.network = self.name + "-ingress"
        self.prefix = prefix
        self.spool = spool
        self.audit = audit
        self.passwords = seed.passwords
        self.pgdata = work / "production-pgdata"
        self.pgdata.mkdir()
        ids = phase1.capture([*self.docker, "network", "ls", "-q"]).splitlines()
        networks = json.loads(phase1.capture([*self.docker, "network", "inspect", *ids]))
        used = [ip_network(config["Subnet"]) for network in networks
                for config in network["IPAM"]["Config"] or [] if config.get("Subnet")]
        candidates = (f"10.254.{index}" for index in range(256))
        self.net = next(net for net in candidates if not any(
            other.version == 4 and other.overlaps(ip_network(f"{net}.0/24")) for other in used))
        self.env = dict(os.environ, PHASE7_DIR=str(work), PHASE7_PHASE5=str(PHASE5),
                        PHASE7_SCRIPTS=str(HERE),
                        PHASE7_IMAGE_PREFIX=prefix)
        for line in (ROOT / "deploy/compose.env.example").read_text().splitlines():
            if line and not line.startswith("#"):
                key, value = line.split("=", 1)
                self.env[key] = value.replace("172.30.70.", self.net + ".")
        for role in ("serve", "sync", "feed"):
            self.env[f"RFC_{role.upper()}_IMAGE"] = f"{prefix}-{role}"
        self.ingestion = work / "ingestion.toml"
        self.ingestion.write_bytes((ROOT / "deploy/ingestion.toml.example").read_bytes())
        self.ingestion.chmod(0o444)
        self.env.update(RFC_INGRESS_NETWORK=self.network, RFC_MCP_HOSTS="rfc.test",
                        RFC_MCP_ORIGINS="https://rfc.test",
                        RFC_SYNC_CONFIG_HOST_FILE=str(self.ingestion),
                        RFC_SYNC_SPOOL_HOST_DIR=str(spool),
                        RFC_SYNC_AUDIT_HOST_DIR=str(audit),
                        RFC_MIRROR_DIR=str(seed.directory / "mirror"),
                        RFC_PGDATA_DIR=str(self.pgdata),
                        RFC_DB_CA=str(seed.directory / "certs/ca.crt"),
                        RFC_POSTGRES_CERT=str(seed.directory / "postgres/postgres.crt"))
        hba = work / "pg_hba.conf"
        hba.write_text((ROOT / "deploy/pg_hba.conf.example").read_text()
                       .replace("172.30.70.", self.net + "."))
        self.env["RFC_PG_HBA"] = str(hba)
        self.keys = work / "keys"
        self.keys.mkdir()
        notify, feed = secrets.token_bytes(48), secrets.token_bytes(48)
        files = {
            "POSTGRES_KEY": (70, (seed.directory / "postgres/postgres.key").read_bytes()),
            "POSTGRES_PASSWORD": (70, seed.passwords["postgres"].encode()),
            "SERVE_DB_PASSWORD": (10001, seed.passwords[phase1.SERVE].encode()),
            "SYNC_DB_PASSWORD": (10002, seed.passwords[phase1.SYNC].encode()),
            "SERVE_CURSOR_KEY": (10001, seed.key),
            "SERVE_NOTIFY_KEY": (10001, notify),
            "SYNC_NOTIFY_KEY": (10002, notify),
            "SYNC_FEED_KEY": (10002, feed),
            "FEED_FEED_KEY": (10003, feed),
        }
        for label, (uid, payload) in files.items():
            path = self.keys / f"{uid}-{label}"
            path.touch(mode=0o600)
            path.write_bytes(payload)
            self.env["RFC_" + label] = str(path)
        self.key_ownership(
            "for key in /keys/*; do basename=${key##*/}; uid=${basename%%-*}; "
            'chmod 0400 "$key"; chown "$uid:$uid" "$key"; done')
        (work / "sandbox-secret").write_text("synthetic worker test secret\n")
        phase5.run([*self.docker, "network", "create", "--internal", "--subnet",
                    f"{self.net}.0/28", self.network])

    def key_ownership(self, command):
        phase5.run([*self.docker, "run", *phase0.local_image_args(), "--rm", "--network", "none", "--read-only",
                    "--cap-drop", "ALL", "--cap-add", "CHOWN", "--cap-add", "FOWNER",
                    "-u", "0:0", "-v", f"{self.keys}:/keys", "--entrypoint", "/bin/sh",
                    f"{self.prefix}-sync", "-ec", command])

    def command(self, *args, extra=None):
        compose = [*self.docker, "compose", "-p", self.name, "-f", str(ROOT / "compose.yml"),
                   "-f", str(HERE / "compose.yml")]
        if extra:
            compose.extend(("-f", str(extra)))
        return [*compose, *phase0.compose_args(*args)]

    def run(self, *args, **kwargs):
        return phase5.run(self.command(*args), env=self.env, **kwargs)

    def capture(self, *args, **kwargs):
        return phase1.capture(self.command(*args), env=self.env, **kwargs)

    def inspect(self, service):
        return json.loads(phase1.capture([*self.docker, "inspect",
                                          self.capture("ps", "-q", service)]))[0]

    def sql(self, *args):
        return self.capture("exec", "-T", "postgres", *phase1.SQL_OPTIONS,
                            "-U", "postgres", "-d", "rfc", *args)

    def admin(self, path):
        return self.capture("exec", "-T", SERVE, "wget", "-qO-", "http://127.0.0.1:8781" + path)

    def check_config_mount(self):
        project = json.loads(self.capture("--profile", "tools", "config", "--format", "json"))
        for service, spec in project["services"].items():
            mounts = [volume for volume in spec.get("volumes", []) if volume["target"] == INGESTION_PATH]
            path = (spec.get("environment") or {}).get("RFC_SYNC_CONFIG_FILE")
            spool = [volume for volume in spec.get("volumes", []) if volume["target"] == "/rfc-https-spool"]
            audit = [volume for volume in spec.get("volumes", []) if volume["target"] == "/rfc-rsync-audit"]
            if service == SYNC:
                mount, = mounts
                assert mount["type"] == "bind" and mount["source"] == str(self.ingestion)
                assert mount["read_only"] and not mount["bind"]["create_host_path"]
                assert path == INGESTION_PATH
                staging, = spool
                assert staging["type"] == "bind" and staging["source"] == str(self.spool)
                assert not staging.get("read_only") and not staging["bind"]["create_host_path"]
                audit_volume, = audit
                assert audit_volume["type"] == "bind" and audit_volume["source"] == str(self.audit)
                assert not audit_volume.get("read_only") and not audit_volume["bind"]["create_host_path"]
            else:
                assert not mounts and not spool and not audit and path is None
        assert set(project["services"][FEED]["networks"]) == {"feed_control"}
        missing_spool = dict(self.env)
        del missing_spool["RFC_SYNC_SPOOL_HOST_DIR"]
        phase1.failed(self.command("config", "--quiet"), missing_spool, "Provision a private disk-backed")
        missing_audit = dict(self.env)
        del missing_audit["RFC_SYNC_AUDIT_HOST_DIR"]
        phase1.failed(self.command("config", "--quiet"), missing_audit, "Provision a private disk-backed rsync audit")
        missing = dict(self.env)
        del missing["RFC_SYNC_CONFIG_HOST_FILE"]
        phase1.failed(self.command("config", "--quiet"), missing, "Set RFC_SYNC_CONFIG_HOST_FILE")
        report = json.loads(self.capture(
            "run", "--rm", "--no-deps", "-T", "--entrypoint", "/bin/sh", SYNC, "-ec",
            """test -r "$RFC_SYNC_CONFIG_FILE"
awk -v path="$RFC_SYNC_CONFIG_FILE" '$5 == path && $6 ~ /(^|,)ro(,|$)/ { found = 1 }
END { exit !found }' /proc/self/mountinfo
exec /usr/local/bin/rfc-mcp-docker-sync --check-config""",
            timeout=60,
        ))
        assert report["source"] == "file" and report["schema_version"] == 2
        assert report["publication_gate"] == "https_txt_audit_required"
        assert report["resources"]["cgroup_memory_limit_bytes"] == 2 * 1024**3
        assert report["resources"]["scratch_available_bytes"] == 512 * 1024**2
        assert report["resources"]["spool_available_inodes"] >= 40016
        assert report["resources"]["audit"]["available_bytes"] >= 256 * 1024**2
        print("Base Compose isolates the spool, audit volume, v2 config, and Feed egress", flush=True)

    def provision(self):
        self.check_config_mount()
        self.run("--profile", "tools", "run", "--rm", "--no-deps", "volume-admin")
        self.run("up", "-d", "--wait", "postgres", timeout=90)
        assert self.sql("-c", "SHOW data_directory") == "/var/lib/postgresql/19/docker"
        self.sql("-f", "/migrations/roles.sql")
        password_sql = "".join(
            f"ALTER ROLE {role} PASSWORD '{self.passwords[role]}';\n"
            for role in (phase1.SERVE, phase1.SYNC))
        self.capture("exec", "-T", "postgres", *phase1.SQL_OPTIONS, "-U", "postgres",
                     "-d", "rfc", input=password_sql)
        self.sql("-f", "/migrations/bootstrap.sql")
        self.sql("-f", "/migrations/migrate.sql")
        assert self.sql("-c", "SELECT max(version) FROM rfc.schema_migrations") == "11"
        assert self.sql("-c", "SELECT current_setting('server_encoding') || ':' || datlocale || ':' "
                        "|| datlocprovider::text FROM pg_database WHERE datname = current_database()") == "UTF8:und:i"
        assert self.sql("-c", "SELECT has_table_privilege('rfc_serve','rfc.generation_documents','INSERT')") == "f"

    def restore(self, dump):
        with dump.open("rb") as fixture:
            subprocess.run(self.command("exec", "-T", "postgres", "pg_restore", "--clean", "--if-exists",
                                        "--exit-on-error", "-U", "postgres", "-d", "rfc"),
                           env=self.env, stdin=fixture, check=True, timeout=55)

    def open(self, *services):
        self.run("up", "-d", "--wait", "--wait-timeout", "55", *services, timeout=85)
        port = int(self.capture("port", "test-proxy", "8443").rsplit(":", 1)[1])
        self.client = phase5.Client(port, self.ca)

    def start(self):
        self.provision()
        self.restore(self.work / "fixture.dump")
        # Dynamic clients can start before the corresponding fixed listener.
        self.run("up", "-d", "--no-deps", SYNC, timeout=55)
        self.open()

    def check_config(self):
        def probe(*options, memory="2g", scratch="512m", audit=True, expected=None, args=("--check-config",)):
            command = [*self.docker, "run", *phase0.local_image_args(), "--rm", "--network", "none",
                       "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
                       "--memory", memory, "--tmpfs", f"/tmp:size={scratch},mode=1777,noexec,nosuid,nodev",
                       "-v", f"{self.spool}:/rfc-https-spool",
                       *(("-v", f"{self.audit}:/rfc-rsync-audit") if audit else ()),
                       *options, f"{self.prefix}-sync", *args]
            if expected is not None:
                phase1.failed(command, self.env, expected)
                return None
            return json.loads(phase1.capture(command, env=self.env, timeout=40))

        file_options = ("-e", f"RFC_SYNC_CONFIG_FILE={INGESTION_PATH}",
                        "-v", f"{self.ingestion}:{INGESTION_PATH}:ro")
        defaults = probe()
        assert defaults["source"] == "defaults"
        assert defaults["ingestion"] == probe(*file_options)["ingestion"]
        assert defaults["schema_version"] == 2 and defaults["phase"] == "audit"
        assert defaults["publication_gate"] == "https_txt_audit_required"
        assert defaults["pending_consumers"] == []
        probe(audit=False, expected="private rsync audit directory")
        probe("--tmpfs", "/rfc-rsync-audit:size=1g,uid=10002,gid=10002,mode=0700",
              audit=False, expected="separate, disk-backed")
        for name in ("RFC_SYNC_HTTP_CONCURRENCY", "RFC_SYNC_PARSE_CONCURRENCY"):
            probe("-e", f"{name}=1", expected=f"{name} was removed")
            probe(*file_options, "-e", f"{name}=1", expected=f"{name} was removed")
        probe(*file_options, args=("--check-config", "--full"), expected="--check-config")
        probe("-e", f"RFC_SYNC_CONFIG_FILE={INGESTION_PATH}", expected="cannot open ingestion config")
        probe("-e", "RFC_SYNC_CONFIG_FILE=", expected="must be nonempty")
        probe(memory="768m", expected="ingestion memory needs")
        probe(scratch="4m", expected="ingestion scratch needs")

        small = self.work / "small-ingestion.toml"
        small.write_text("""schema_version = 2
[ingestion.https]
max_in_flight_requests = 1
[ingestion.pipeline]
max_active_documents = 1
parser_workers = 1
raw_payload_budget_bytes = 16777216
parse_budget_bytes = 134217728
""")
        small_options = ("-e", f"RFC_SYNC_CONFIG_FILE={INGESTION_PATH}",
                         "-v", f"{small}:{INGESTION_PATH}:ro")
        small.chmod(0o400)
        probe(*small_options, expected="cannot open ingestion config")
        small.chmod(0o444)
        resources = probe(*small_options, memory="800m", scratch="16m")["resources"]
        assert resources["cgroup_memory_limit_bytes"] == resources["memory_available_bytes"] == 800 * 1024**2
        assert resources["scratch_available_bytes"] == 16 * 1024**2
        for body, expected in (
            (b"schema_version =", "invalid ingestion TOML"),
            (b"schema_version = 1", "migrate to schema v2"),
            (b"schema_version = 3", "schema_version must be 2"),
            (b"schema_version = 2\n[ingestion]\nunknown = true", "invalid ingestion TOML"),
            (b"schema_version = 2\n[ingestion.https]\nurl = 'https://invalid.example'", "invalid ingestion TOML"),
            (b"schema_version = 2\n[ingestion.rsync]\npath = '/mirror/blobs'", "migrate to schema v2"),
            (b"schema_version = 2\n[ingestion]\ntxt_provenance = 'rsync_bootstrap'", "migrate to schema v2"),
            (b"schema_version = 2\n[ingestion.revalidation]\nchecksum_choice = 'none'", "invalid ingestion TOML"),
            (b"schema_version = 2\n[ingestion.https]\nmax_in_flight_requests = 0", "1 through 2"),
            (b"schema_version = 2\n[ingestion.https]\nmin_request_start_interval_ms = 999", "1000 through 60000"),
            (b"schema_version = 2\n[ingestion]\nmax_accepted_bytes = 4294967297", "1 through 4294967296"),
            (b"schema_version = 2\n[ingestion.pipeline]\nmax_active_documents = 1", "must fit the active document window"),
            (b"#" + b"x" * (16 * 1024), "exceeds 16 KiB"),
            (b"\xff", "must be UTF-8"),
        ):
            small.chmod(0o644)
            small.write_bytes(body)
            small.chmod(0o444)
            probe(*small_options, expected=expected)
        print("Sync configuration preflight passed with isolated and constrained containers", flush=True)

    def verify(self):
        for service, uid, secrets_expected in (
            (SERVE, 10001, {"cursor_key", "db_ca", "db_password", "notify_key"}),
            (SYNC, 10002, {"db_ca", "db_password", "feed_key", "notify_key"}),
            (FEED, 10003, {"feed_key"}),
            ("postgres", 70, {"postgres_cert", "postgres_key", "postgres_password"}),
        ):
            info = self.inspect(service)
            host = info["HostConfig"]
            assert info["Config"]["User"] == f"{uid}:{uid}"
            assert host["ReadonlyRootfs"] and host["CapDrop"] == ["ALL"] and not host["Privileged"]
            assert host["SecurityOpt"] == ["no-new-privileges:true"] and host["PidsLimit"] > 0
            assert host["NanoCpus"] > 0 and host["Memory"] > 0 and host["PortBindings"] == {}
            assert host["LogConfig"]["Type"] == "local" and host["LogConfig"]["Config"]["max-file"] == "3"
            assert set(self.capture("exec", "-T", service, "ls", "/run/secrets").split()) == secrets_expected
            config_mounts = [mount for mount in info["Mounts"] if mount["Destination"] == INGESTION_PATH]
            config_env = [value for value in info["Config"]["Env"] if value.startswith("RFC_SYNC_CONFIG_FILE=")]
            if service == SYNC:
                assert len(config_mounts) == 1 and not config_mounts[0]["RW"]
                assert config_env == [f"RFC_SYNC_CONFIG_FILE={INGESTION_PATH}"]
                self.run("exec", "-T", SYNC, "sh", "-ec", 'test -r "$RFC_SYNC_CONFIG_FILE"')
                assert host["Memory"] == 2 * 1024**3 and host["NanoCpus"] == 2_000_000_000
                block_size, blocks = map(int, self.capture(
                    "exec", "-T", SYNC, "stat", "-f", "-c", "%S %b", "/tmp"
                ).split())
                assert block_size * blocks == 512 * 1024**2
            else:
                assert not config_mounts and not config_env
            if service == FEED:
                assert not any(mount["Destination"] == "/mirror" for mount in info["Mounts"])
            if service in (SYNC, FEED):
                networks = info["NetworkSettings"]["Networks"]
                assert self.network not in networks
                network, pool = (("sync_notify", "RFC_NOTIFY_DYNAMIC_RANGE") if service == SYNC
                                 else ("feed_control", "RFC_CONTROL_DYNAMIC_RANGE"))
                address = networks[f"{self.name}_{network}"]["IPAddress"]
                assert ip_address(address) in ip_network(self.env[pool])
            if service != "postgres":
                self.run("exec", "-T", service, "sh", "-ec",
                         "for key in /run/secrets/*; do case $key in */db_ca) continue;; esac; "
                         f'test "$(stat -c %a:%u "$key")" = 400:{uid}; done')
                self.run("exec", "-T", service, "sh", "-ec",
                         "test \"$(cat /sys/fs/cgroup/memory.max)\" != max; "
                         "test \"$(cat /sys/fs/cgroup/pids.max)\" != max; "
                         "case $(cat /sys/fs/cgroup/cpu.max) in max*) exit 1;; esac")
        assert "rfc_mcp_rg_available 1" in self.admin("/metrics")
        self.admin("/ready")
        self.run("exec", "-T", SERVE, "sh", "-ec", "test -r /mirror; ! touch /mirror/denied")
        self.run("exec", "-T", SYNC, "sh", "-ec", "touch /mirror/phase7-write && rm /mirror/phase7-write")
        db_ips = [net["IPAddress"] for net in self.inspect("postgres")["NetworkSettings"]["Networks"].values()]
        self.run("exec", "-T", FEED, "sh", "-ec",
                 f"nc -zw 2 {self.net}.66 8783; test ! -e /mirror; "
                 f"! nc -zw 2 {db_ips[0]} 5432; ! nc -zw 2 {db_ips[1]} 5432")
        self.run("exec", "-T", "test-proxy", "python3", "-B", "-c", """
import socket, sys
socket.create_connection((sys.argv[1], 8780), timeout=2).close()
for addr, port in ((sys.argv[1], 8781), (sys.argv[1], 8782),
                   (sys.argv[2], 8782), (sys.argv[3], 8783)):
    try:
        socket.create_connection((addr, port), timeout=2).close()
    except OSError:
        continue
    raise AssertionError((addr, port))
""", f"{self.net}.2", f"{self.net}.50", f"{self.net}.66")
        self.client.schemas = {tool["name"]: phase5.Draft202012Validator(tool["outputSchema"])
                               for tool in self.client.rpc("tools/list")[0]["tools"]}
        for tool, arguments in (("rg", {"pattern": "--version", "mode": "literal", "ids": [108]}),
                                ("list", {}), ("search", {"query": 'in::title:"TLS"'}),
                                ("get", {"id": 2119}), ("table", {"id": 102})):
            self.client.call(tool, **arguments)
        for path in ("/ready", "/metrics", "/internal/v1/identify", "/internal/v1/generation-committed"):
            assert self.client.exchange(None, {"Host": "rfc.test"}, "GET", path)[0] == 404
        binary = "/usr/local/bin/rfc-mcp-docker-serve"
        probe_command = 'timeout 8 sh -c \'"$1" --phase7-sandbox "$2"; exit $?\' probe "$1" "$2"'
        probe = self.capture("exec", "-T", SERVE, "sh", "-ec", probe_command, "probe", binary, "denials")
        assert any(json.loads(line)["type"] == "match" for line in probe.splitlines())
        started = time.monotonic()
        killed = subprocess.run(self.command("exec", "-T", SERVE, "sh", "-ec", probe_command,
                                             "probe", binary, "cpu"), env=self.env,
                                timeout=12, capture_output=True)
        assert killed.returncode == 137 and time.monotonic() - started < 8, killed.stderr
        raw = b"\xef\xbb\xbfmatch\r\n\xffmatch\x00\n"
        child = subprocess.run(self.command("exec", "-T", SERVE, binary, "--rg-worker", "literal",
                                            "sensitive", "match"), env=self.env, input=raw,
                               capture_output=True, timeout=10, check=True)
        lines = [event["data"] for line in child.stdout.splitlines()
                 if (event := json.loads(line))["type"] == "match"]
        assert lines[0]["submatches"][0]["start"] == 3
        assert lines[1]["absolute_offset"] == 10 and lines[1]["lines"]["bytes"] == "/21hdGNoAAo="
        print("Production confinement, MCP, TLS, capabilities, cgroup and network checks passed", flush=True)

    def fail_closed(self):
        # Simulate a kernel that refuses Landlock in this disposable container.
        profile = self.work / "deny-landlock.json"
        profile.write_text(json.dumps({
            "defaultAction": "SCMP_ACT_ALLOW",
            "syscalls": [{"names": ["landlock_create_ruleset"], "action": "SCMP_ACT_ERRNO"}],
        }))
        override = self.work / "denied.yml"
        override.write_text(f"services:\n  {SERVE}:\n    security_opt:\n"
                            f"      - seccomp={profile}\n")
        phase5.run(self.command("up", "-d", "--no-deps", "--wait", "--force-recreate", SERVE,
                                extra=override), env=self.env, timeout=90)
        assert "rfc_mcp_rg_available 0" in self.admin("/metrics")
        result = subprocess.run(self.command("exec", "-T", SERVE, "wget", "-SO-",
                                             "http://127.0.0.1:8781/ready"),
                                text=True, capture_output=True, env=self.env, timeout=10)
        assert result.returncode != 0 and "503" in result.stderr
        self.client.call("rg", pattern="TLS", _error="sandbox_unavailable")
        self.client.call("list")
        print("Simulated missing Landlock failed closed and kept safe tools usable", flush=True)

    def worker_lifecycle(self):
        rust = next(line.split()[1] for line in (ROOT / "Dockerfile").read_text().splitlines()
                    if line.startswith("FROM docker.io/library/rust:"))
        phase5.run([*self.docker, "run", *phase0.local_image_args(), "--rm", "--network", "none",
                    "-v", f"{HERE / 'stall-worker.c'}:/stall.c:ro", "-v", f"{self.work}:/fixture",
                    "--entrypoint", "cc", rust, "-static", "-O2", "-o", "/fixture/stall", "/stall.c"])
        override = self.work / "stall.yml"
        override.write_text(f"services:\n  {SERVE}:\n    volumes:\n"
                            f"      - {self.work}/stall:/usr/local/bin/rg:ro\n")
        phase5.run(self.command("up", "-d", "--no-deps", "--wait", "--force-recreate", SERVE,
                                extra=override), env=self.env, timeout=80)
        assert "rfc_mcp_rg_available 1" in self.admin("/metrics")

        def workers():
            return self.capture("exec", "-T", SERVE, "sh", "-ec",
                                "ps -o comm | grep -c '^rg$' || true")

        def wait_workers(wanted):
            for _ in range(20):
                if workers() == str(wanted):
                    return
                time.sleep(0.1)
            raise AssertionError(f"expected {wanted} worker processes, saw {workers()}")

        arguments = dict(pattern="stall", mode="literal", ids=[108])
        with ExitStack() as stack:
            clients = []
            for address in ("192.0.2.1", "192.0.2.2"):
                request = stack.enter_context(subprocess.Popen(
                    self.command("exec", "-T", "test-proxy", "python3", "-B", "/phase7/disconnect.py",
                                 address), env=self.env, text=True, stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE))
                clients.append(request)
                assert select.select([request.stdout], [], [], 3)[0]
                assert request.stdout.readline().strip() == "sent"
            try:
                wait_workers(2)
                self.client.call("rg", **arguments, _error="resource_limit_exceeded")
            finally:
                for request in clients:
                    _, errors = request.communicate("disconnect\n", timeout=8)
                    assert request.returncode == 0, errors
            wait_workers(0)
        assert self.sql("-c", "SELECT count(*) FROM pg_stat_activity WHERE usename='rfc_serve'") == "0"
        self.run("exec", "-T", SYNC, "flock", "-n", "/mirror/generation-locks/1", "true")
        started = time.monotonic()
        self.client.call("rg", **arguments, _error="deadline_exceeded")
        assert 4.0 <= time.monotonic() - started < 8.0
        wait_workers(0)
        self.run("exec", "-T", SYNC, "flock", "-n", "/mirror/generation-locks/1", "true")
        print("Two-worker saturation, disconnect, wall deadline, reaping, and fence release passed", flush=True)

    def close(self):
        try:
            self.run("stop", "postgres", timeout=35)
            self.run("--profile", "tools", "run", "--rm", "--no-deps", "volume-admin",
                     "find /pgdata -mindepth 1 -maxdepth 1 -exec rm -rf {} +; "
                     "chown 0:0 /pgdata; chmod 0755 /pgdata", timeout=40)
        finally:
            try:
                self.run("down", "--volumes", "--remove-orphans", timeout=90)
            finally:
                phase5.run([*self.docker, "network", "rm", self.network])
                self.key_ownership("chown 0:0 /keys/*")


@contextmanager
def deployment(work, seed, prefix):
    with (phase5.phase2.capped_stage(seed, mebibytes=5120, inodes=65536) as spool,
          phase5.phase2.capped_stage(seed, mebibytes=1024, inodes=4096, label="audit") as audit):
        stack = Deployment(work, seed, prefix, spool, audit)
        try:
            yield stack
        finally:
            stack.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--context", default="rootless")
    parser.add_argument("--image-prefix", default="rfc-phase7-final")
    parser.add_argument("--config-only", action="store_true",
                        help="exercise preflight and the production config mount without seeding a corpus")
    args = parser.parse_args()
    if not __debug__:
        raise RuntimeError("Phase 7 requires assertions")
    docker = ["docker", "--context", args.context]
    with tempfile.TemporaryDirectory(prefix="rfc-phase7-", dir="/tmp") as directory:
        work = Path(directory)
        if args.config_only:
            seed = phase1.Project(work / "config-fixture", work / "unused", args.image_prefix, docker, work.name)
            seed.key = secrets.token_bytes(48)
            with deployment(work, seed, args.image_prefix) as stack:
                stack.check_config()
                stack.check_config_mount()
            return
        phase5.phase3.fixture_driver(docker, args.image_prefix, work / "publication-driver")
        seed = phase5.Project(work / work.name, work / "publication-driver", args.image_prefix, docker)
        seed.env.update(PHASE5_DIR=str(work), PHASE5_SCRIPTS=str(PHASE5), PHASE5_PROXY_IP="")
        seed.key = secrets.token_bytes(48)
        (work / "cursor-key").touch()
        (work / "fixture-policy.json").touch()
        (work / "tls").mkdir()
        phase1.phase0.make_leaf(seed.directory / "certs", "localhost", work / "tls")
        for name, data in (("fixture.txt", "phase-zero test match\n"), ("unselected.txt", "unselected\n")):
            (seed.directory / "mirror" / name).write_text(data)
        try:
            seed.start()
            seed.migrate()
            seed.prepare_reviews()
            seed.run("up", "-d", "--wait", "fixtures", timeout=60)
            seed.activate(int(seed.reconcile()["generation_id"]), 0)
            with (work / "fixture.dump").open("wb") as wire:
                subprocess.run(seed.command("exec", "-T", "postgres", "pg_dump", "-Fc", "--schema=rfc",
                                            "-U", "postgres", "-d", "rfc"), env=seed.env,
                               stdout=wire, check=True, timeout=45)
            seed.run("stop", "fixtures", "postgres", timeout=50)
            with deployment(work, seed, args.image_prefix) as stack:
                stack.check_config()
                stack.start()
                stack.verify()
                stack.run("up", "-d", "--force-recreate", "--wait", "--wait-timeout", "55",
                          timeout=90)
                port = int(stack.capture("port", "test-proxy", "8443").rsplit(":", 1)[1])
                stack.client.port = port
                stack.verify()
                stack.worker_lifecycle()
                stack.fail_closed()
        finally:
            seed.close()


if __name__ == "__main__":
    main()
