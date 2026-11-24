#!/usr/bin/env python3
"""Build, transfer, and exercise the images used by the Compose CI matrix."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[2]
APP_TARGETS = {"serve": "serve", "sync": "sync", "feed": "feed", "fixtures": "phase0-fixtures"}
FIXTURE_TARGETS = {
    "storage-fixture": "phase1-fixture",
    "ingestion-fixture": "phase2-fixture",
    "publication-fixture": "phase3-fixture",
    "search-fixture": "phase4-fixture",
    "control-fixture": "phase6-fixture",
}
CASE_FIXTURES = {
    "phase0": (),
    "phase1": ("storage-fixture",),
    "phase2": ("ingestion-fixture",),
    "phase3": ("publication-fixture",),
    "phase4": ("publication-fixture", "search-fixture"),
    "phase5": ("publication-fixture",),
    "phase6": ("publication-fixture", "control-fixture"),
    "phase7": ("publication-fixture",),
    "phase8": ("publication-fixture",),
    "bootstrap-quick": ("publication-fixture", "control-fixture"),
}
ARCH_NAMES = {"x86_64": "amd64", "aarch64": "arm64", "amd64": "amd64", "arm64": "arm64"}


def temporary_path(value):
    path = Path(value).resolve()
    if Path("/tmp") not in path.parents:
        raise argparse.ArgumentTypeError("expected a path beneath /tmp")
    return path


def case_list(value):
    cases = json.loads(value)
    if (not isinstance(cases, list) or not cases
            or any(not isinstance(case, str) or case not in CASE_FIXTURES for case in cases)
            or len(cases) != len(set(cases))):
        raise argparse.ArgumentTypeError("expected a nonempty JSON list of distinct phase names")
    return cases


def image_targets(prefix, cases):
    targets = dict(APP_TARGETS)
    for case in cases:
        for fixture in CASE_FIXTURES[case]:
            targets[fixture] = FIXTURE_TARGETS[fixture]
    return {f"{prefix}-{suffix}": target for suffix, target in targets.items()}


def capture(command):
    return subprocess.check_output(command, cwd=ROOT, text=True).strip()


def run(command, **kwargs):
    return subprocess.run(command, cwd=ROOT, check=True, **kwargs)


def require_native(docker, arch):
    engine_os, engine_arch = capture([*docker, "info", "--format", "{{.OSType}} {{.Architecture}}"]).split()
    if engine_os != "linux" or ARCH_NAMES.get(engine_arch) != arch or ARCH_NAMES.get(platform.machine()) != arch:
        raise ValueError(f"expected native Linux {arch}, got host {platform.machine()} and Docker {engine_os}/{engine_arch}")


def image_ids(docker, refs, arch):
    contents = json.loads(capture([*docker, "image", "inspect", *refs]))
    ids = {}
    for ref, image in zip(refs, contents, strict=True):
        if image["Os"] != "linux" or image["Architecture"] != arch:
            raise ValueError(f"{ref} is {image['Os']}/{image['Architecture']}, expected linux/{arch}")
        ids[ref] = image["Id"]
    return ids


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def image_bundle(args, docker):
    require_native(docker, args.arch)
    targets = image_targets(args.image_prefix, args.cases)
    archive = args.bundle / "images.tar"
    manifest = args.bundle / "manifest.json"
    identity = {"revision": capture(["git", "rev-parse", "HEAD"]), "platform": f"linux/{args.arch}"}
    if args.command == "build":
        args.bundle.mkdir(parents=True)
        for ref, target in targets.items():
            run([*docker, "buildx", "build", "--load", "--platform", identity["platform"],
                 "--provenance=false", "--target", target, "--tag", ref, "."])
        ids = image_ids(docker, targets, args.arch)
        run([*docker, "image", "save", "--output", str(archive), *targets])
        receipt = {**identity, "images": ids, "archive_sha256": sha256(archive)}
        manifest.write_text(json.dumps(receipt, indent=2) + "\n")
    else:
        receipt = json.loads(manifest.read_text())
        if any(receipt[key] != value for key, value in identity.items()) or set(receipt["images"]) != set(targets):
            raise ValueError("image artifact does not match this checkout, architecture, and case matrix")
        if sha256(archive) != receipt["archive_sha256"]:
            raise ValueError("image artifact archive checksum mismatch")
        run([*docker, "image", "load", "--input", str(archive)])
        if image_ids(docker, targets, args.arch) != receipt["images"]:
            raise ValueError("loaded image IDs do not match the image artifact manifest")
    print(manifest.read_text(), end="", flush=True)


def pinned_images(case):
    postgres = re.search(r"(?m)^    image: (docker\.io/library/postgres:\S+@sha256:[0-9a-f]{64})$",
                         (ROOT / "compose.yml").read_text())
    yield postgres.group(1)
    if case == "phase7":
        compiler = re.search(r"(?m)^FROM (docker\.io/library/rust:\S+@sha256:[0-9a-f]{64}) AS build$",
                             (ROOT / "Dockerfile").read_text())
        yield compiler.group(1)


def run_case(args, docker):
    args.evidence.mkdir(parents=True)
    scripts = ROOT / "scripts"
    script = (scripts / "bootstrap" / "baseline.py" if args.case == "bootstrap-quick"
              else scripts / args.case / "integration.py")
    prefix_option = "--project" if args.case == "phase0" else "--image-prefix"
    command = [sys.executable, "-B", str(script), "--context", args.context, prefix_option, args.image_prefix]
    if args.case == "phase8":
        command.extend(("--evidence", str(args.evidence)))
    elif args.case == "bootstrap-quick":
        command.extend(("--phase8", "quick", "--evidence", str(args.evidence / "bootstrap-quick")))
    env = dict(os.environ, RFC_E2E_PRELOADED="1", TMPDIR="/tmp", PYTHONUNBUFFERED="1")
    with tempfile.TemporaryFile(mode="w+", encoding="utf-8", errors="replace", dir=args.evidence) as stream:
        try:
            output = {"stdout": stream, "stderr": subprocess.STDOUT}
            run([*docker, "image", "inspect", *image_targets(args.image_prefix, [args.case])],
                stdout=subprocess.DEVNULL, stderr=stream)
            for image in pinned_images(args.case):
                run([*docker, "pull", image], **output)
            run(["timeout", "--signal=INT", "--kill-after=120s", "20m", *command], env=env, **output)
        finally:
            stream.seek(0)
            log = stream.read()
            log = re.sub(r"(?is)-----BEGIN [^\n]*PRIVATE KEY-----.*?(?:-----END [^\n]*PRIVATE KEY-----|\Z)",
                         "[redacted private key]", log)
            log = re.sub(r"(?i)(?<![0-9a-f])[0-9a-f]{48}(?![0-9a-f])", "[redacted]", log)
            (args.evidence / "case.log").write_text(log)
            print(log, end="", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", default="default")
    parser.add_argument("--image-prefix", required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("build", "load"):
        bundle = commands.add_parser(command)
        bundle.add_argument("bundle", type=temporary_path)
        bundle.add_argument("--arch", choices=("amd64", "arm64"), required=True)
        bundle.add_argument("--cases", type=case_list, required=True)
    case = commands.add_parser("run")
    case.add_argument("case", choices=CASE_FIXTURES)
    case.add_argument("evidence", type=temporary_path)
    args = parser.parse_args()
    docker = ["docker", "--context", args.context]
    try:
        if args.command == "run":
            run_case(args, docker)
        else:
            image_bundle(args, docker)
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"{args.command} failed: {error}\n")


if __name__ == "__main__":
    main()
