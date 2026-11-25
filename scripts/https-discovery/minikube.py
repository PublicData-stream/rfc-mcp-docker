#!/usr/bin/env python3
"""Check audit volume isolation and repair replay on a disposable Minikube node."""

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
    def __init__(self, profile, context, prefix, work, audit_root):
        self.minikube = ["minikube", "-p", profile]
        self.docker = ["docker", "--context", context]
        self.node = profile
        self.prefix = prefix
        self.work = work
        self.namespace = f"rfc-processing-{uuid4().hex[:8]}"
        self.node_work = f"/tmp/{self.namespace}"
        self.node_audit = f"{audit_root.rstrip('/')}/{self.namespace}"
        self.pg_image = f"{self.namespace}-postgres"
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
            readiness=None):
        volumes = [{"name": key, **spec} for key, _, spec, _ in mounts]
        volumes.append({"name": "tmp", "emptyDir": {"medium": "Memory", "sizeLimit": scratch}})
        spec = {
            "name": "main", "image": image, "imagePullPolicy": "Never", "command": command,
            "securityContext": {
                "runAsUser": uid, "runAsGroup": uid, "runAsNonRoot": True,
                "readOnlyRootFilesystem": True, "allowPrivilegeEscalation": False,
                "capabilities": {"drop": ["ALL"]}, "seccompProfile": {"type": "RuntimeDefault"},
            },
            "resources": {"limits": {"cpu": "2", "memory": "2Gi"}} if uid == 10002 else {},
            "env": [{"name": key, "value": value} for key, value in (env or {}).items()],
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

    def done(self, name):
        try:
            self.kubectl("wait", f"pod/{name}", "--for=jsonpath={.status.phase}=Succeeded", "--timeout=55s")
        except subprocess.CalledProcessError:
            print(self.kubectl("describe", "pod", name))
            print(self.kubectl("logs", name))
            raise
        return self.kubectl("logs", name).splitlines()[-1]

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
        (work / "feed").unlink()
        for name in ("pgdata", "pgsocket", "mirror", "spool"):
            (work / name).mkdir(mode=0o700)
        admin, writer = (secrets.token_hex(32) for _ in range(2))
        for path, password in (("db/admin", admin), ("writer", writer)):
            (work / path).write_text(password)
            (work / path).chmod(0o400)
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
            chown -R 10002:10002 "$1/mirror" "$1/spool" "$1/writer"
        """, "sh", self.node_work])
        return len(sources), writer

    def run(self):
        self.apply_resource({"apiVersion": "v1", "kind": "Namespace",
                             "metadata": {"name": self.namespace}})
        self.execute([*self.docker, "exec", "--user", "0", self.node,
                      "mkdir", "-m", "0755", self.node_work])
        self.execute([*self.docker, "exec", "--user", "10002:10002", self.node,
                      "mkdir", "-m", "0700", self.node_audit])
        # Docker archives need a named tag for containerd to resolve the local PostgreSQL image.
        self.execute([*self.docker, "tag", POSTGRES, self.pg_image])
        try:
            self.load([f"{self.prefix}-{name}" for name in ("sync", "feed", "fixtures")]
                      + [self.pg_image])
        finally:
            self.execute([*self.docker, "image", "rm", self.pg_image])
        expected, writer = self.prepare()
        self.apply_resource({
            "apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "ingestion"},
            "data": {"ingestion.toml": (REPO / "deploy/ingestion.toml.example").read_text()},
        })
        config = ("config", "/etc/rfc-mcp", {"configMap": {"name": "ingestion"}}, True)
        spool = self.host("spool", "/rfc-https-spool", "spool", False)
        audit = ("audit", "/rfc-rsync-audit",
                 {"hostPath": {"path": self.node_audit, "type": "Directory"}}, False)
        cfg = {"RFC_SYNC_CONFIG_FILE": "/etc/rfc-mcp/ingestion.toml"}
        self.pod("stock-sync", f"{self.prefix}-sync", 10002, [
            "/bin/sh", "-ec", """
                test "$(id -u)" = 10002
                test "$(stat -c '%a' /rfc-https-spool)" = 700
                test "$(stat -c '%a' /rfc-rsync-audit)" = 700
                if (echo x >/etc/rfc-mcp/ingestion.toml) 2>/dev/null; then exit 1; fi
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
        self.pod("fixtures", f"{self.prefix}-fixtures", 10004, ["python3", "-B", "/fixture-endpoint.py"],
                 mounts=(self.host("input", "/fixture-source", "source"),
                         self.host("endpoint", "/fixture-endpoint.py", "endpoint.py")),
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
        self.database(input=f"ALTER ROLE rfc_sync PASSWORD '{writer}';\n")
        self.database("-f", "/migrations/bootstrap.sql", "-f", "/migrations/migrate.sql")

        def sync(name, crash=None, args=("--phase2-reconcile", "--full")):
            self.pod(name, f"{self.prefix}-sync", 10002, [
                "/phase6/sync", *args,
            ], env={
                **cfg, "RFC_PHASE6_FIXTURES": "1", "RFC_DISCOVERY_FIXTURES": "1",
                "RFC_DB_HOST": "postgres", "RFC_DB_CA_FILE": "/phase2/ca.crt",
                "RFC_DB_PASSWORD_FILE": "/phase1/db-password",
                **({"RFC_PHASE3_CRASH": crash} if crash else {}),
            }, mounts=(config, spool, audit, self.host("mirror", "/mirror", "mirror", False),
                       self.host("sync", "/phase6/sync", "sync"),
                       self.host("ca", "/phase2/ca.crt", "ca.crt"),
                       self.host("writer", "/phase1/db-password", "writer")),
                scratch="512Mi")

        sync("sync-cold")
        cold = json.loads(self.done("sync-cold"))
        sync("publish-cold", args=("--phase3-publish", cold["generation_id"], "--expect-sequence", "0"))
        assert json.loads(self.done("publish-cold"))["publication_sequence"] == "1"
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
        self.kubectl("wait", "pod/sync-crashing", "--for=jsonpath={.status.phase}=Failed", "--timeout=55s")
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
        print("Minikube replacement sealed the durable repair pair without another source attempt", flush=True)
        print(f"Minikube pod replacement reused {expected} TXT and "
              f"{warm['acquisition']['xml_reused']} RFCXML originals. HTTPS starts "
              f"{cold['metrics']['https_requests']} cold and {warm['metrics']['https_requests']} warm.",
              flush=True)

    def close(self):
        self.kubectl("delete", "namespace", self.namespace, "--ignore-not-found", "--timeout=60s")
        self.execute([*self.docker, "exec", "--user", "10002:10002", self.node,
                      "rm", "-rf", "--", self.node_audit])
        self.execute([*self.docker, "exec", "--user", "0", self.node,
                      "rm", "-rf", "--", self.node_work])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True, help="dedicated single-node Docker Minikube profile")
    parser.add_argument("--context", default="rootless")
    parser.add_argument("--image-prefix", required=True)
    parser.add_argument("--audit-node-dir", required=True,
                        help="disk-backed path under the Minikube node's /tmp, writable by UID/GID 10002")
    args = parser.parse_args()
    assert os.environ.get("MINIKUBE_HOME", "").startswith("/tmp/"), "set MINIKUBE_HOME under /tmp"
    assert os.environ.get("KUBECONFIG", "").startswith("/tmp/"), "set KUBECONFIG under /tmp"
    if not Path(args.audit_node_dir).is_relative_to("/tmp") or ".." in Path(args.audit_node_dir).parts:
        parser.error("--audit-node-dir must be beneath the node's /tmp")
    os.environ["RFC_E2E_PRELOADED"] = "1"
    with tempfile.TemporaryDirectory(prefix="rfc-minikube-processing-", dir="/tmp") as work:
        probe = Probe(args.profile, args.context, args.image_prefix, Path(work), args.audit_node_dir)
        try:
            probe.run()
        finally:
            probe.close()


if __name__ == "__main__":
    main()
