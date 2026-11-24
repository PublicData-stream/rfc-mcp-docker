#!/usr/bin/env python3
"""Measure an offline import of the benchmark images into a fresh Docker store."""

import argparse
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tempfile
import time


ROOT = Path(__file__).resolve().parents[2]
SERVICES = ("serve", "sync", "feed", "fixtures", "publication-fixture", "control-fixture")


def capture(command, *, timeout=30):
    return subprocess.check_output(command, text=True, timeout=timeout).strip()


def image_ids(docker, refs):
    ids = capture([*docker, "image", "inspect", "--format", "{{.Id}}", *refs])
    return dict(zip(refs, ids.splitlines(), strict=True))


def sha256(path):
    with path.open("rb") as archive:
        return hashlib.file_digest(archive, "sha256").hexdigest()


@contextmanager
def background(command, log):
    with log.open("w") as output:
        process = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT)
        try:
            yield process
        finally:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=60)


def import_images(source, prefix, evidence, receipt):
    postgres = re.search(r"(?m)^    image: (docker\.io/library/postgres\S+)$",
                         (ROOT / "scripts" / "phase1" / "compose.yml").read_text()).group(1)
    with tempfile.TemporaryDirectory(prefix="rfc-image-cache-", dir="/tmp") as temp:
        work = Path(temp)
        archive = work / "images.tar"
        alias = f"rfc-image-cache:{work.name}"
        refs = [f"{prefix}-{service}" for service in SERVICES] + [alias]
        receipt["postgres_source"] = postgres
        tag = image_ids(source, [postgres])[postgres]
        subprocess.run([*source, "image", "tag", tag, alias], check=True, timeout=30)
        try:
            receipt["images"] = image_ids(source, refs)
            start = time.monotonic()
            subprocess.run([*source, "image", "save", "--output", archive, *refs], check=True, timeout=300)
            receipt["archive_export_seconds"] = time.monotonic() - start
        finally:
            subprocess.run([*source, "image", "rm", alias], check=True, timeout=30)
        receipt["archive_bytes"] = archive.stat().st_size
        receipt["archive_sha256"] = sha256(archive)
        (work / "daemon.json").write_text("{}\n")
        (work / "containerd.toml").write_text("version = 2\n")
        socket = f"unix://{work / 'docker.sock'}"
        cold = ["sudo", "-n", "docker", "--host", socket]
        # Docker otherwise detects the system containerd socket even with a fresh data root.
        containerd_socket = work / "containerd.sock"
        containerd = [
            "sudo", "-n", "containerd", "--config", work / "containerd.toml",
            "--address", containerd_socket, "--root", work / "content", "--state", work / "state",
        ]
        daemon = [
            "sudo", "-n", "dockerd", "--host", socket, "--config-file", work / "daemon.json",
            "--data-root", work / "data", "--exec-root", work / "exec", "--pidfile", work / "pid",
            "--containerd", containerd_socket,
            "--bridge=none", "--iptables=false", "--ip6tables=false", "--ip-forward=false", "--ip-masq=false",
        ]
        start = time.monotonic()
        try:
            with (background(containerd, evidence / "containerd.log") as backend,
                  background(daemon, evidence / "daemon.log") as process):
                while time.monotonic() < start + 60:
                    if backend.poll() is not None:
                        raise RuntimeError(f"isolated containerd exited with status {backend.returncode}")
                    if process.poll() is not None:
                        raise RuntimeError(f"isolated dockerd exited with status {process.returncode}")
                    try:
                        info = json.loads(capture([*cold, "info", "--format", "{{json .}}"], timeout=2))
                    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                        time.sleep(1)
                    else:
                        break
                else:
                    raise RuntimeError("isolated dockerd failed to start within 60 seconds")
                receipt["daemon_setup_seconds"] = time.monotonic() - start
                receipt["cold_docker"] = {key: info[key] for key in ("ServerVersion", "Driver", "Architecture")}
                assert Path(info["DockerRootDir"]) == work / "data"
                assert not capture([*cold, "image", "ls", "-aq"]), "cold image store is not empty"
                assert sha256(archive) == receipt["archive_sha256"], "archive checksum changed"
                start = time.monotonic()
                subprocess.run([*cold, "image", "load", "--input", archive], check=True, timeout=300)
                receipt["cold_image_load_seconds"] = time.monotonic() - start
                start = time.monotonic()
                assert image_ids(cold, refs) == receipt["images"], "loaded image IDs differ"
                receipt["warm_image_inspection_seconds"] = time.monotonic() - start
        finally:
            # The daemons own these paths inside this newly created directory.
            subprocess.run(["sudo", "-n", "rm", "-rf", "--", work / "data", work / "exec",
                            work / "content", work / "state"], check=True, timeout=60)


def main():
    if not __debug__:
        raise RuntimeError("the image cache probe requires enabled assertions")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", default="default")
    parser.add_argument("--image-prefix", required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    evidence = args.evidence.resolve()
    if Path("/tmp") not in evidence.parents or evidence.exists():
        parser.error("--evidence must be a new directory beneath /tmp")
    evidence.mkdir(mode=0o700)
    receipt = {"source_context": args.context, "registry_pull_seconds": None, "status": "error"}
    try:
        import_images(["docker", "--context", args.context], args.image_prefix, evidence, receipt)
        receipt["status"] = "verified"
    except (AssertionError, OSError, RuntimeError, ValueError, subprocess.SubprocessError) as error:
        receipt["error"] = f"{type(error).__name__}: {error}"
    (evidence / "image-cache.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))
    return int(receipt["status"] != "verified")


if __name__ == "__main__":
    raise SystemExit(main())
