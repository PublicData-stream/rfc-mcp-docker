#!/usr/bin/env python3
"""Qualify manual and automatic HTTPS-first publication on disposable Minikube."""

import argparse
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import tempfile
import time
from types import SimpleNamespace
from uuid import uuid4

from integration import phase1, phase2, phase6

REPO = phase2.REPO
POSTGRES = re.search(
    r"(?m)^    image: (docker\.io/library/postgres:\S+)$", (REPO / "compose.yml").read_text()
).group(1)


class Probe:
    def __init__(self, profile, context, prefix, work, spool_root, audit_root):
        self.minikube = ["minikube", "-p", profile]
        self.docker = ["docker", "--context", context]
        self.node = profile
        self.prefix = prefix
        self.work = work
        self.namespace = f"rfc-processing-{uuid4().hex[:8]}"
        self.node_work = f"/tmp/{self.namespace}"
        self.node_spool = f"{spool_root.rstrip('/')}/{self.namespace}"
        self.node_audit = f"{audit_root.rstrip('/')}/{self.namespace}"
        self.pg_image = f"{profile}-postgres-fixture"
        self.kube = [*self.minikube, "kubectl", "--", "-n", self.namespace]

    def execute(self, command, *, input=None, timeout=90, stderr=None):
        return subprocess.run(command, input=input, check=True, text=True, stdout=subprocess.PIPE,
                              stderr=stderr, timeout=timeout).stdout

    def kubectl(self, *args, **kwargs):
        return self.execute([*self.kube, *args], **kwargs)

    def load(self, images):
        tar = self.work / "images.tar"
        self.execute([*self.docker, "save", "-o", str(tar), *images], timeout=180)
        with tar.open("rb") as stream:
            archive = f"{self.node_work}/images.tar"
            subprocess.run([*self.docker, "exec", "--user", "0", "-i", self.node,
                            "sh", "-ec", 'cat > "$1"; ctr -n k8s.io images import "$1"; rm "$1"',
                            "sh", archive],
                           stdin=stream, stdout=subprocess.DEVNULL, check=True, timeout=240)
        tar.unlink()

    def apply(self, kind, name, spec):
        data = {"apiVersion": "v1", "kind": kind, "metadata": {"name": name}}
        if kind == "Pod":
            data["metadata"]["labels"] = {"app": name}
        data["spec"] = spec
        self.apply_resource(data)

    def apply_resource(self, data):
        for attempt in range(3):
            try:
                self.kubectl("apply", "-f", "-", input=json.dumps(data), stderr=subprocess.PIPE)
                return
            except subprocess.CalledProcessError as error:
                if attempt == 2 or "etcdserver: request timed out" not in error.stderr:
                    raise RuntimeError(error.stderr.strip()) from error
                print("Retrying a timed-out Minikube API write", flush=True)
                time.sleep(attempt + 1)

    def pod(self, name, image, uid, command, *, env=None, mounts=(), scratch="32Mi",
            readiness=None, bind_ip=False):
        volumes = [{"name": key, **spec} for key, _, spec, _ in mounts]
        volumes.append({"name": "tmp", "emptyDir": {"medium": "Memory", "sizeLimit": scratch}})
        variables = []
        if bind_ip:
            variables.append({"name": "POD_IP", "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}}})
        variables.extend({"name": key, "value": value} for key, value in (env or {}).items())
        spec = {
            "name": "main", "image": image, "imagePullPolicy": "Never", "command": command,
            "securityContext": {
                "runAsUser": uid, "runAsGroup": uid, "runAsNonRoot": True,
                "readOnlyRootFilesystem": True, "allowPrivilegeEscalation": False,
                "capabilities": {"drop": ["ALL"]}, "seccompProfile": {"type": "RuntimeDefault"},
            },
            "resources": {"limits": {"cpu": "2", "memory": "2Gi"}} if uid == 10002 else {},
            "env": variables,
            "volumeMounts": [
                {"name": key, "mountPath": path, "readOnly": read_only}
                for key, path, _, read_only in mounts
            ] + [{"name": "tmp", "mountPath": "/tmp"}],
        }
        if readiness:
            spec["readinessProbe"] = {"tcpSocket": {"port": readiness}, "periodSeconds": 1}
        self.apply("Pod", name, {"restartPolicy": "Never", "containers": [spec], "volumes": volumes})

    def host(self, name, mount, filename, read_only=True):
        return name, mount, {"hostPath": {"path": f"{self.node_work}/{filename}"}}, read_only

    def service(self, name, ports):
        self.apply("Service", name, {"selector": {"app": name}, "ports": [
            {"name": f"port-{port}", "port": port, "targetPort": port} for port in ports
        ]})

    def address(self, name, port):
        ip = self.kubectl("get", f"service/{name}", "-o", "jsonpath={.spec.clusterIP}")
        return f"{ip}:{port}"

    def verify_served(self, sequence, expected):
        active = self.database("-c", "SELECT generation_id,publication_sequence FROM rfc.active_generation")
        generation, current = active.split("|")
        assert current == str(sequence), active
        ready = [*self.kube, "exec", "serve", "--", "wget", "-qO-", "http://127.0.0.1:8781/ready"]
        phase6.wait(lambda: subprocess.run(ready, capture_output=True, timeout=10).returncode == 0)
        result = self.mcp("serve", "get", id=2119, format="base64")
        assert not result["isError"], result
        document = result["structuredContent"]
        assert document["publication_sequence"] == current and document["generation_id"] == generation
        assert document["original"]["source_verification"]["method"] == "rsync_checksum_match"
        assert phase6.phase5.bytes_from(document["original"]) == expected

    def mcp(self, service, tool, **arguments):
        body, headers = phase6.phase5.envelope("tools/call", {"name": tool, "arguments": arguments})
        response = self.kubectl("exec", "-i", "fixtures", "--", "python3", "-B", "-c", """
import http.client
import json
import sys
service, body, headers = json.load(sys.stdin)
conn = http.client.HTTPConnection(service, 8780, timeout=10)
conn.request("POST", "/mcp", json.dumps(body), headers)
response = conn.getresponse()
assert response.status == 200, response.status
print(response.read().decode())
""", input=json.dumps((service, body, headers)))
        return json.loads(response)["result"]

    def verify_policy_replicas(self, serve, expected):
        secondary = "serve-secondary"
        self.service(secondary, [8780])
        serve(secondary)
        self.kubectl("wait", f"pod/{secondary}", "--for=condition=Ready", "--timeout=55s")
        for service in ("serve", secondary):
            result = self.mcp(service, "get", id=2119, format="base64")
            assert not result["isError"], result
            assert phase6.phase5.bytes_from(result["structuredContent"]["original"]) == expected
        cursor = self.mcp("serve", "list", limit=1)["structuredContent"]["next_cursor"]
        assert cursor
        restricted = self.kubectl("exec", "sync", "--", "/phase6/sync", "policy", "restrict",
                                  "--rfc", "2119", "--ceiling", "metadata_only", "--reason", "rights_revoked",
                                  "--review-ref", "private Minikube fixture", "--expect-epoch", "0")
        assert "rights epoch 1" in restricted, restricted
        def denied(service):
            result = self.mcp(service, "get", id=2119)
            assert result["isError"] and result["structuredContent"]["code"] == "source_policy_restricted", result
        denied("serve")
        denied(secondary)
        resumed = self.mcp(secondary, "list", limit=1, cursor=cursor)
        assert resumed["isError"] and resumed["structuredContent"]["code"] == "cursor_expired", resumed
        self.kubectl("delete", "pod", secondary, "--timeout=55s")
        serve(secondary)
        self.kubectl("wait", f"pod/{secondary}", "--for=condition=Ready", "--timeout=55s")
        denied(secondary)
        print("Minikube applied rights restrictions across two Serve replicas and pod replacement", flush=True)

    def delivered(self, sequence):
        query = ("SELECT count(*),count(*) FILTER (WHERE delivery_state='delivered') "
                 f"FROM rfc.notification_outbox WHERE publication_sequence={sequence}")
        phase6.wait(lambda: self.database("-c", query) == "1|1", seconds=120)

    def done(self, name):
        try:
            self.kubectl("wait", f"pod/{name}", "--for=jsonpath={.status.phase}=Succeeded",
                         "--timeout=120s", timeout=135)
        except subprocess.CalledProcessError:
            print(self.kubectl("describe", "pod", name))
            print(self.kubectl("logs", name))
            raise
        # Kubernetes combines the command result and structured stderr logs.
        # The final line can come from either stream.
        for line in reversed(self.kubectl("logs", name).splitlines()):
            if line.startswith("{"):
                payload = json.loads(line)
                if {"event", "level", "timestamp_ms"} <= payload.keys():
                    continue
            return line
        raise AssertionError(f"{name} produced no command result")

    def database(self, *sql, input=None):
        return self.kubectl("exec", "-i", "postgres", "--", "psql", "-XqAt",
                            "-v", "ON_ERROR_STOP=1", "-d", "rfc", *sql, input=input).strip()

    def prepare(self):
        work = self.work / "node"
        work.mkdir(mode=0o755)
        (work / "certs").mkdir()
        phase1.phase0.make_ca(work / "certs", "ca")
        sources = phase2.prepare(SimpleNamespace(source=work / "source", directory=work), include_nul=True)
        (work / "db").mkdir()
        phase1.phase0.make_leaf(work / "certs", "postgres", work / "db")
        shutil.copyfile(work / "certs/ca.crt", work / "ca.crt")
        shutil.rmtree(work / "certs")
        shutil.copyfile(REPO / "scripts/phase2/endpoint.py", work / "endpoint.py")
        shutil.copytree(REPO / "migrations", work / "migrations")
        phase6.fixture_binaries(self.docker, self.prefix, work)
        shutil.copyfile(REPO / "scripts/phase6/upstream.py", work / "upstream.py")
        phase6.feed_data(SimpleNamespace(source=work / "source"), [])
        (work / "source/feed-mode").write_text("normal")
        for name in ("pgdata", "pgsocket", "mirror"):
            (work / name).mkdir(mode=0o700)
        admin, writer, reader = (secrets.token_hex(32) for _ in range(3))
        for path, password in (("db/admin", admin), ("writer", writer), ("reader", reader)):
            (work / path).write_text(password)
            (work / path).chmod(0o400)
        notify, feed = (secrets.token_bytes(48) for _ in range(2))
        for name, content in (("feed-key-sync", feed), ("feed-key-feed", feed),
                              ("notify-key-sync", notify), ("notify-key-serve", notify),
                              ("cursor-key", secrets.token_bytes(48))):
            (work / name).write_bytes(content)
            (work / name).chmod(0o400)
        archive = self.work / "node.tar"
        self.execute(["tar", "-cf", str(archive), "-C", str(work), "."])
        with archive.open("rb") as stream:
            subprocess.run([*self.docker, "exec", "--user", "0", "-i", self.node,
                            "tar", "-xf", "-", "-C", self.node_work],
                           stdin=stream, check=True, timeout=90)
        archive.unlink()
        self.execute([*self.docker, "exec", "--user", "0", self.node, "sh", "-ec", """
            chown -R 70:70 "$1/pgdata" "$1/pgsocket" "$1/db"
            chmod 0400 "$1/db/postgres.key"
            chmod 0755 "$1/mirror"
            chown -R 10002:10002 "$1/mirror" "$1/writer" "$1/feed-key-sync" "$1/notify-key-sync"
            chown 10001:10001 "$1/reader" "$1/notify-key-serve" "$1/cursor-key"
            chown 10003:10003 "$1/feed-key-feed"
        """, "sh", self.node_work])
        return sources, writer, reader

    def run(self, *, automatic, serve_pool=False):
        self.apply_resource({"apiVersion": "v1", "kind": "Namespace",
                             "metadata": {"name": self.namespace}})
        self.execute([*self.docker, "exec", "--user", "0", self.node,
                      "mkdir", "-m", "0755", self.node_work])
        self.execute([*self.docker, "exec", "--user", "10002:10002", self.node,
                      "mkdir", "-m", "0700", self.node_spool, self.node_audit])
        # Docker archives need a named tag for containerd to resolve the local PostgreSQL image.
        self.execute([*self.docker, "tag", POSTGRES, self.pg_image])
        try:
            self.load([f"{self.prefix}-{name}" for name in ("sync", "serve", "feed", "fixtures")]
                      + [self.pg_image])
        finally:
            self.execute([*self.docker, "image", "rm", self.pg_image])
        sources, writer, reader = self.prepare()
        expected = len(sources)
        self.apply_resource({
            "apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "ingestion"},
            "data": {"ingestion.toml": (REPO / "deploy/ingestion.toml.example").read_text()},
        })
        config = ("config", "/etc/rfc-mcp", {"configMap": {"name": "ingestion"}}, True)
        spool = ("spool", "/rfc-https-spool",
                 {"hostPath": {"path": self.node_spool, "type": "Directory"}}, False)
        audit = ("audit", "/rfc-rsync-audit",
                 {"hostPath": {"path": self.node_audit, "type": "Directory"}}, False)
        cfg = {"RFC_SYNC_CONFIG_FILE": "/etc/rfc-mcp/ingestion.toml"}
        self.pod("stock-sync", f"{self.prefix}-sync", 10002, [
            "/bin/sh", "-ec", """
                test "$(id -u)" = 10002
                test "$(stat -c '%a' /rfc-https-spool)" = 700
                test "$(stat -c '%a' /rfc-rsync-audit)" = 700
                if (echo x >/etc/rfc-mcp/ingestion.toml) 2>/dev/null; then exit 1; fi
                read -r quota period </sys/fs/cgroup/cpu.max
                test "$quota" = "$((2 * period))"
                for path in /rfc-https-spool /rfc-rsync-audit; do
                    test "$(stat -f -c '%t' "$path")" = ef53
                done
                test "$(stat -c '%d' /rfc-https-spool)" != "$(stat -c '%d' /rfc-rsync-audit)"
                test "$(stat -f -c '%c' /rfc-https-spool)" -le 66000
                test "$(stat -f -c '%c' /rfc-rsync-audit)" -le 4096
                set -- $(stat -f -c '%b %S' /rfc-https-spool)
                test "$(($1 * $2))" -le 5368709120
                set -- $(stat -f -c '%b %S' /rfc-rsync-audit)
                test "$(($1 * $2))" -le 1073741824
                exec /usr/local/bin/rfc-mcp-docker-sync --check-config
            """,
        ], env=cfg, mounts=(config, spool, audit), scratch="512Mi")
        self.pod("stock-feed", f"{self.prefix}-feed", 10003, [
            "/bin/sh", "-ec", """
                test "$(id -u)" = 10003
                test ! -e /etc/rfc-mcp/ingestion.toml
                test ! -e /rfc-https-spool
                test ! -e /rfc-rsync-audit
                echo isolated
            """,
        ])
        preflight = json.loads(self.done("stock-sync"))
        assert preflight["source"] == "file" and preflight["schema_version"] == 2, preflight
        assert preflight["resources"]["cgroup_memory_limit_bytes"] == 2 * 1024**3, preflight
        assert preflight["resources"]["scratch_available_bytes"] >= 511 * 1024**2, preflight
        assert preflight["resources"]["audit"]["available_bytes"] >= 256 * 1024**2, preflight
        assert self.done("stock-feed") == "isolated"
        print("Stock Minikube preflight passed", flush=True)

        self.pod("postgres", self.pg_image, 70, ["/usr/local/bin/docker-entrypoint.sh", "postgres",
                 "-c", "ssl=on", "-c", "ssl_cert_file=/phase1/postgres.crt",
                 "-c", "ssl_key_file=/phase1/postgres.key"],
                 env={
                     "POSTGRES_USER": "postgres", "POSTGRES_DB": "rfc",
                     "POSTGRES_PASSWORD_FILE": "/phase1/admin", "PGDATA": "/data/pg",
                     "POSTGRES_INITDB_ARGS": "--encoding=UTF8 --locale=C --locale-provider=icu "
                     "--icu-locale=und --auth-host=scram-sha-256 --auth-local=peer",
                 },
                 mounts=(self.host("data", "/data", "pgdata", False),
                         self.host("socket", "/run/postgresql", "pgsocket", False),
                         self.host("db", "/phase1", "db"),
                         self.host("migrations", "/migrations", "migrations")),
                 readiness=5432)
        self.service("postgres", [5432])
        self.pod("fixtures", f"{self.prefix}-fixtures", 10004, ["python3", "-B", "/upstream.py"],
                 mounts=(self.host("input", "/fixture-source", "source"),
                         self.host("endpoint", "/fixture-endpoint.py", "endpoint.py"),
                         self.host("upstream", "/upstream.py", "upstream.py")),
                 readiness=8443)
        self.service("fixtures", [8443, 8873])
        try:
            self.kubectl("wait", "--for=condition=Ready", "pod/postgres", "pod/fixtures",
                         "--timeout=90s", timeout=105)
        except subprocess.CalledProcessError:
            print(self.kubectl("describe", "pod", "postgres", "fixtures"))
            for name in ("postgres", "fixtures"):
                subprocess.run([*self.kube, "logs", name], check=False, timeout=10)
            raise
        self.database("-f", "/migrations/roles.sql")
        self.database(input=f"ALTER ROLE rfc_sync PASSWORD '{writer}';\n"
                            f"ALTER ROLE rfc_serve PASSWORD '{reader}';\n")
        self.database("-f", "/migrations/bootstrap.sql", "-f", "/migrations/migrate.sql")

        self.service("serve", [8780, 8782])
        self.service("sync", [8783])
        ca = self.host("ca", "/phase2/ca.crt", "ca.crt")
        database = {"RFC_DB_HOST": "postgres", "RFC_DB_CA_FILE": "/phase2/ca.crt",
                    "RFC_DB_PASSWORD_FILE": "/phase1/db-password"}
        notify = {"RFC_SYNC_SERVE_KEY_ID": "fixture-notify",
                  "RFC_SYNC_SERVE_KEY_FILE": "/run/secrets/notify"}
        feed = {"RFC_FEED_SYNC_KEY_ID": "fixture-feed",
                "RFC_FEED_SYNC_KEY_FILE": "/run/secrets/feed"}
        def serve(name):
            self.pod(name, f"{self.prefix}-serve", 10001, ["/usr/local/bin/rfc-mcp-docker-serve"], env={
                **database, **notify, "RFC_MCP_BIND": "$(POD_IP):8780",
                "RFC_SERVE_NOTIFY_BIND": "$(POD_IP):8782", "RFC_MCP_HOSTS": "rfc.test",
                "RFC_MCP_CURSOR_KEY_FILE": "/run/secrets/cursor",
            }, mounts=(ca, self.host("mirror", "/mirror", "mirror"),
                       self.host("reader", "/phase1/db-password", "reader"),
                       self.host("cursor", "/run/secrets/cursor", "cursor-key"),
                       self.host("notify", "/run/secrets/notify", "notify-key-serve")), readiness=8780, bind_ip=True)

        serve("serve")
        self.kubectl("wait", "--for=condition=Ready", "pod/serve", "--timeout=55s")
        self.kubectl("exec", "serve", "--", "sh", "-ec",
                     "test ! -e /rfc-https-spool && test ! -e /rfc-rsync-audit")

        def sync(name, crash=None, args=("--phase2-reconcile", "--full")):
            self.pod(name, f"{self.prefix}-sync", 10002, ["/phase6/sync", *args], env={
                **cfg, **database, **notify, **feed, "RFC_DISCOVERY_FIXTURES": "1",
                "RFC_SYNC_CONTROL_BIND": "$(POD_IP):8783",
                "RFC_SYNC_SERVE_ADDR": self.address("serve", 8782),
                **({"RFC_PHASE3_CRASH": crash} if crash else {}),
            }, mounts=(config, spool, audit, ca, self.host("mirror", "/mirror", "mirror", False),
                       self.host("sync", "/phase6/sync", "sync"),
                       self.host("writer", "/phase1/db-password", "writer"),
                       self.host("notify", "/run/secrets/notify", "notify-key-sync"),
                       self.host("feed", "/run/secrets/feed", "feed-key-sync")),
                scratch="512Mi", bind_ip=True)

        assert self.database("-c", "SELECT publication_sequence FROM rfc.active_generation") == "0"
        if automatic:
            sync("sync", args=())
            def live_status():
                try:
                    report = json.loads(self.kubectl("exec", "sync", "--", "/phase6/sync", "status", "--json",
                                                    timeout=20, stderr=subprocess.PIPE))
                except subprocess.CalledProcessError:
                    return None
                return report if report["live"]["state"] == "available" else None

            status = phase6.wait(live_status, seconds=45)
            assert status["schema_version"] == 1 and status["database"]["state"] == "available", status
            watched = self.kubectl("exec", "sync", "--", "sh", "-ec",
                                   '/phase6/sync watch --json & watcher=$!; sleep 4; kill -TERM "$watcher"; wait "$watcher"')
            snapshots = [json.loads(line) for line in watched.splitlines()]
            assert snapshots[0]["live"]["state"] == "available", snapshots
            assert all(row["database"]["state"] == "not_requested" for row in snapshots), snapshots
            self.delivered(1)
            diagnosed = json.loads(self.kubectl("exec", "sync", "--", "/phase6/sync", "diagnose", "--json"))
            assert diagnosed["database"]["data"]["publication_sequence"] == 1, diagnosed
            assert all(finding["severity"] != "error" for finding in diagnosed["findings"]), diagnosed
            assert "https_attempt" not in self.kubectl("logs", "sync")
            self.verify_served(1, sources[2119])
            self.add_feed_publication()
            self.pod("feed", f"{self.prefix}-feed", 10003, ["/phase6/feed"], env={
                **feed, "RFC_DISCOVERY_FIXTURES": "1", "RFC_FEED_SYNC_ADDR": self.address("sync", 8783),
            }, mounts=(self.host("feed", "/phase6/feed", "feed"),
                       self.host("feed-key", "/run/secrets/feed", "feed-key-feed")))
            self.delivered(2)
            self.verify_served(2, sources[2119])
            assert self.database("-c", "SELECT count(*) FROM rfc.control_events WHERE status='completed'") == "1"
            self.verify_policy_replicas(serve, sources[2119])
            print("Minikube status, watch, and diagnose passed through kubectl exec as Sync", flush=True)
            print("Minikube fresh daemon and brokered Feed publications passed with authenticated outbox and MCP reads",
                  flush=True)
            return

        sync("sync-cold")
        cold = json.loads(self.done("sync-cold"))
        assert self.database("-c", "SELECT publication_sequence FROM rfc.active_generation") == "0"
        sync("publish-cold", args=("--phase3-publish", cold["generation_id"], "--expect-sequence", "0"))
        assert json.loads(self.done("publish-cold"))["publication_sequence"] == "1"
        self.verify_served(1, sources[2119])
        sync("sync-warm")
        warm = json.loads(self.done("sync-warm"))
        assert warm["state"] == cold["state"] == "ready", (cold, warm)
        assert warm["acquisition"]["txt_reused"] == cold["expected_txt"] == expected, warm
        assert warm["acquisition"]["xml_reused"] > 0, warm
        assert cold["acquisition"]["processing_digest"] == warm["acquisition"]["processing_digest"]
        assert warm["metrics"]["https_requests"] < cold["metrics"]["https_requests"]
        # Update both sources while holding rsync metadata constant. A warm
        # run detects the change and loses the pod after persisting the pair.
        self.execute([*self.docker, "exec", "--user", "0", self.node, "sh", "-ec", """
            rsync="$1/source/rsync/rfc100.txt"
            https="$1/source/https/rfc100.txt"
            touch -r "$rsync" "$1/stamp"
            printf Z | dd of="$rsync" conv=notrunc 2>/dev/null
            cp "$rsync" "$https"
            touch -r "$1/stamp" "$rsync"
        """, "sh", self.node_work])
        sync("sync-crashing", "after_rsync_repair_pair")
        self.kubectl("wait", "pod/sync-crashing", "--for=jsonpath={.status.phase}=Failed",
                     "--timeout=120s", timeout=135)
        assert "crash at after_rsync_repair_pair" in self.kubectl("logs", "sync-crashing")
        self.kubectl("delete", "pod", "sync-crashing", "--wait=true")
        sync("sync-replacement")
        repair = json.loads(self.done("sync-replacement"))
        assert repair["audit"]["repaired"] == 1 and repair["audit"]["repair_attempts"] == 0, repair
        assert repair["audit"]["reused"] == expected - 1, repair
        assert repair["acquisition"]["processing_digest"] != cold["acquisition"]["processing_digest"], repair
        assert self.database("-c", "SELECT publication_sequence FROM rfc.active_generation") == "1"
        sync("publish-repaired", args=("--phase3-publish", repair["generation_id"], "--expect-sequence", "1"))
        assert json.loads(self.done("publish-repaired"))["publication_sequence"] == "2"
        assert self.database("-c", """
            SELECT e.method, e.rsync_observation IS NOT NULL
            FROM rfc.active_generation a JOIN rfc.generation_source_evidence e USING (generation_id)
            WHERE a.singleton AND e.rfc_id=100
        """) == "dual_source_byte_match|t"
        self.verify_served(2, sources[2119])
        assert self.database("-c", "SELECT count(*) FROM rfc.notification_outbox WHERE delivery_state='pending'") == "2"
        sync("sync", args=())
        self.delivered(2)
        self.delivered(1)
        if serve_pool:
            self.verify_serve_pool(2, sources[2119])
        print("Minikube manual publication and repair replay passed with exact MCP reads and daemon outbox delivery",
              flush=True)
        print(f"Minikube pod replacement reused {expected} TXT and "
              f"{warm['acquisition']['xml_reused']} RFCXML originals. HTTPS starts "
              f"{cold['metrics']['https_requests']} cold and {warm['metrics']['https_requests']} warm.",
              flush=True)

    def verify_serve_pool(self, sequence, original):
        readers = ("SELECT pid FROM pg_stat_activity WHERE usename='rfc_serve' "
                   "AND backend_type='client backend' ORDER BY pid")
        self.verify_served(sequence, original)
        before = set(self.database("-c", readers).splitlines())
        self.verify_served(sequence, original)
        assert before.intersection(self.database("-c", readers).splitlines())

        database = json.loads(self.kubectl("get", "pod", "postgres", "-o", "json"))
        self.kubectl("delete", "pod/postgres", "--wait=true", "--timeout=55s", timeout=60)

        def admin(path):
            return subprocess.run([*self.kube, "exec", "serve", "--", "wget",
                                   "-qO-", f"http://127.0.0.1:8781{path}"],
                                  capture_output=True, timeout=8).returncode

        assert admin("/live") == 0
        assert admin("/ready") != 0
        self.apply("Pod", "postgres", database["spec"])
        self.kubectl("wait", "pod/postgres", "--for=condition=Ready", "--timeout=90s", timeout=95)
        self.verify_served(sequence, original)
        # The fixture has RestartPolicy Never so the clean exit is inspectable.
        subprocess.run([*self.kube, "exec", "serve", "--", "sh", "-ec", "kill -TERM 1"],
                       capture_output=True, timeout=8)
        self.kubectl("wait", "pod/serve", "--for=jsonpath={.status.phase}=Succeeded",
                     "--timeout=25s", timeout=30)
        assert self.database("-c", readers) == ""
        print("Minikube Serve pool reused a backend, recovered after PostgreSQL restart, "
              "and closed sessions on SIGTERM", flush=True)

    def add_feed_publication(self):
        entry = phase2.catalog_entry(200, "Feed-triggered publication")
        atom = ('<feed xmlns="http://www.w3.org/2005/Atom"><entry>'
                '<id>https://www.rfc-editor.org/info/rfc200/</id></entry></feed>')
        self.execute([*self.docker, "exec", "--user", "0", self.node, "sh", "-ec", """
            cd "$1/source"
            sed "s|</rfc-index>|$2</rfc-index>|" catalog.xml > catalog.new
            mv catalog.new catalog.xml
            printf '%s\n' "Feed publication original" > https/rfc200.txt
            cp https/rfc200.txt rsync/rfc200.txt
            printf '%s' "$3" > rfcatom.xml
        """, "sh", self.node_work, entry, atom])

    def close(self):
        self.kubectl("delete", "namespace", self.namespace, "--ignore-not-found", "--timeout=120s", timeout=135)
        self.execute([*self.docker, "exec", "--user", "10002:10002", self.node,
                      "sh", "-ec", 'for dir do test ! -e "$dir" || rm -rf -- "$dir"; done',
                      "sh", self.node_spool, self.node_audit])
        self.execute([*self.docker, "exec", "--user", "0", self.node,
                      "rm", "-rf", "--", self.node_work])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True, help="dedicated single-node Docker Minikube profile")
    parser.add_argument("--context", default="rootless")
    parser.add_argument("--image-prefix", required=True)
    parser.add_argument("--serve-pool", action="store_true",
                        help="qualify Serve pool lifecycle after the manual publication probe")
    for stage in ("spool", "audit"):
        parser.add_argument(f"--{stage}-node-dir", required=True,
                            help="private disk-backed mount beneath the Minikube node's /tmp")
    args = parser.parse_args()
    assert os.environ.get("MINIKUBE_HOME", "").startswith("/tmp/"), "set MINIKUBE_HOME under /tmp"
    assert os.environ.get("KUBECONFIG", "").startswith("/tmp/"), "set KUBECONFIG under /tmp"
    for path in (args.spool_node_dir, args.audit_node_dir):
        if Path("/tmp") not in Path(path).parents or ".." in Path(path).parts:
            parser.error("staging mount paths must be beneath the node's /tmp")
    os.environ["RFC_E2E_PRELOADED"] = "1"
    for automatic in (False, True):
        with tempfile.TemporaryDirectory(prefix="rfc-minikube-sync-", dir="/tmp") as work:
            probe = Probe(args.profile, args.context, args.image_prefix, Path(work),
                          args.spool_node_dir, args.audit_node_dir)
            try:
                probe.run(automatic=automatic, serve_pool=args.serve_pool)
            except Exception:
                subprocess.run([*probe.kube, "get", "pods"], check=False, timeout=15)
                for name in ("stock-sync", "serve", "sync", "feed"):
                    subprocess.run([*probe.kube, "logs", "--tail=15", name], check=False, timeout=15)
                raise
            finally:
                probe.close()


if __name__ == "__main__":
    main()
