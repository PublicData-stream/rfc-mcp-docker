#!/usr/bin/env python3
"""Rehearse an offline schema upgrade and repeated startup on Minikube."""

import argparse
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile

from automatic_upgrade import source_sql


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--context", default="rootless")
    args = parser.parse_args()
    for name in ("MINIKUBE_HOME", "KUBECONFIG"):
        if Path("/tmp") not in Path(os.environ.get(name, "")).resolve().parents:
            parser.error(f"set {name} under /tmp")
    minikube = ["minikube", "-p", args.profile]
    kube = [*minikube, "kubectl", "--"]
    namespace = "rfc-migration-" + secrets.token_hex(5)
    with tempfile.TemporaryDirectory(prefix="rfc-minikube-upgrade-", dir="/tmp") as work:
        archive = str(Path(work) / "maintenance.tar")
        subprocess.run(["docker", "--context", args.context, "save", "-o", archive, args.image],
                       check=True, timeout=180)
        subprocess.run([*minikube, "image", "load", archive], check=True, timeout=240)

    def apply(kind, name, **settings):
        resource = {"apiVersion": "v1", "kind": kind, "metadata": {"name": name}, **settings}
        if kind != "Namespace":
            resource["metadata"]["namespace"] = namespace
        subprocess.run([*kube, "apply", "-f", "-"], input=json.dumps(resource), text=True,
                       check=True, timeout=35)

    def container(name, command, *, uid=70):
        mounts = [("data", "/var/lib/postgresql"), ("backups", "/backups"),
                  ("mirror", "/mirror"), ("scratch", "/tmp"), ("passwords", "/run/secrets")]
        return {"name": name, "image": args.image, "imagePullPolicy": "Never", "command": command,
                "env": [{"name": "RFC_STORAGE_SCHEMA_TARGET", "value": "13"}],
                "securityContext": {"runAsUser": uid, "runAsGroup": uid,
                    "readOnlyRootFilesystem": True, "allowPrivilegeEscalation": False,
                    "capabilities": {"drop": ["ALL"], "add": ["CHOWN"] if uid == 0 else []}},
                "volumeMounts": [{"name": key, "mountPath": path} for key, path in mounts]}

    load = """
import importlib.util
import os
from pathlib import Path
import tempfile

spec = importlib.util.spec_from_file_location("maintenance", "/usr/local/libexec/rfc-maintain.py")
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
os.umask(0o077)
"""
    seed = load + f"""
m.TARGET.parent.mkdir()
m.init(m.TARGET)
with tempfile.TemporaryDirectory(dir="/tmp") as scratch, m.cluster(m.TARGET, 19, Path(scratch) / "peer") as db:
    m.bootstrap(db)
    m.sql(db, args=("-f", "-"), secret={source_sql(12)!r})
    m.sql(db, "INSERT INTO rfc.generations(parser_version,index_version) VALUES ('minikube','fixture')")
    assert m.get_history(db) == list(range(1, 13))
"""
    check = load + """
with tempfile.TemporaryDirectory(dir="/tmp") as scratch, m.cluster(m.TARGET, 19, Path(scratch) / "peer") as db:
    assert m.get_history(db) == list(range(1, 14))
    assert m.sql(db, "SELECT parser_version FROM rfc.generations").strip() == "minikube"
    assert m.inventory(db)["pointer"] == "|0\\n"
assert len(list(m.BACKUPS.glob("*/receipt.json"))) == 1
print("Minikube schema 12 to 13, restore verification and repeated startup passed")
"""
    runner = ["python3", "-B", "/usr/local/libexec/rfc-maintain.py"]
    apply("Namespace", namespace)
    try:
        apply("Secret", "passwords", stringData={role: secrets.token_hex(32)
                                                for role in ("postgres", "rfc_serve", "rfc_sync")})
        apply("Pod", "migration", spec={
            "restartPolicy": "Never",
            "automountServiceAccountToken": False,
            "volumes": [{"name": name, "emptyDir": {"medium": "Memory"} if name in ("backups", "scratch") else {}}
                        for name in ("data", "backups", "mirror", "scratch")]
                        + [{"name": "passwords", "secret": {"secretName": "passwords"}}],
            "initContainers": [
                container("volumes", ["sh", "-ec",
                          "chmod 1777 /tmp; chmod 0700 /var/lib/postgresql /backups; chown 70:70 /var/lib/postgresql /backups"], uid=0),
                container("seed", ["python3", "-B", "-c", seed]),
                container("upgrade", runner),
                container("repeat", runner),
            ],
            "containers": [container("verify", ["python3", "-B", "-c", check])],
        })
        try:
            subprocess.run([*kube, "-n", namespace, "wait", "pod/migration",
                            "--for=jsonpath={.status.phase}=Succeeded", "--timeout=150s"],
                           check=True, timeout=160)
        finally:
            subprocess.run([*kube, "-n", namespace, "logs", "migration", "--all-containers", "--prefix"],
                           check=False, timeout=25)
    finally:
        subprocess.run([*kube, "delete", "namespace", namespace, "--wait=true", "--timeout=90s"],
                       check=True, timeout=95)


if __name__ == "__main__":
    main()
