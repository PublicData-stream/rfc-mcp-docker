#!/usr/bin/env python3
"""Build, qualify, and publish the same OCI image bytes."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import tarfile
import tempfile
import tomllib


ROOT = Path(__file__).resolve().parents[2]
SOURCE = "https://github.com/PublicData-stream/rfc-mcp-docker"
ROLES = ("serve", "sync", "feed")
PLATFORMS = {
    "amd64v3": {"os": "linux", "architecture": "amd64", "variant": "v3"},
    "arm64": {"os": "linux", "architecture": "arm64"},
}
INDEX = "application/vnd.oci.image.index.v1+json"
MANIFEST = "application/vnd.oci.image.manifest.v1+json"
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def run(*command, **kwargs):
    return subprocess.run(command, check=True, **kwargs)


def output(*command):
    return run(*command, stdout=subprocess.PIPE, text=True).stdout.strip()


def sha256(data):
    return "sha256:" + hashlib.sha256(data).hexdigest()


def file_digest(path):
    with path.open("rb") as stream:
        return "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()


def write_json(path, value):
    path.write_bytes(json.dumps(value, indent=2, sort_keys=True).encode() + b"\n")


def identity(args):
    return {"version": args.version, "revision": args.revision}


def labels(args):
    return {
        "org.opencontainers.image.source": SOURCE,
        "org.opencontainers.image.version": args.version,
        "org.opencontainers.image.revision": args.revision,
    }


def stamp(source, version):
    manifest = source / "Cargo.toml"
    original = manifest.read_text()
    require(tomllib.loads(original)["workspace"]["package"]["version"] == "0.0.0",
            "expected the workspace version placeholder")
    lock = source / "Cargo.lock"
    original_lock = lock.read_text()
    packages = tomllib.loads(original_lock)["package"]
    names = {item["name"] for item in packages if "source" not in item}
    require(len(names) == 6 and all(item["version"] == "0.0.0" for item in packages
                                   if item["name"] in names), "unexpected first-party Cargo lock entries")
    new_manifest, count = re.subn(r'^version = "0\.0\.0"$', f'version = "{version}"', original, flags=re.M)
    require(count == 1, "expected one workspace version placeholder")
    new_lock, count = re.subn(
        r'(?m)(^name = "(?:' + "|".join(sorted(map(re.escape, names))) + r')"\n)version = "0\.0\.0"$',
        lambda match: f'{match[1]}version = "{version}"', original_lock,
    )
    require(count == len(names), "unable to stamp all first-party lock entries")
    manifest.write_text(new_manifest)
    lock.write_text(new_lock)


def blob_path(digest):
    require(DIGEST.fullmatch(digest), f"invalid OCI digest: {digest}")
    return "blobs/sha256/" + digest.removeprefix("sha256:")


def read_blob(archive, descriptor):
    data = archive.extractfile(blob_path(descriptor["digest"])).read()
    require(len(data) == descriptor["size"] and sha256(data) == descriptor["digest"],
            f"OCI blob does not match descriptor: {descriptor['digest']}")
    return data


def image_archive(path, target, expected_labels, directory=None):
    with tarfile.open(path, "r") as archive:
        index = json.load(archive.extractfile("index.json"))
        require(index["schemaVersion"] == 2 and len(index["manifests"]) == 1,
                f"expected one manifest in {path}")
        descriptor = index["manifests"][0]
        require(descriptor["mediaType"] == MANIFEST and descriptor["platform"] == target,
                f"unexpected platform or media type in {path}")
        manifest_bytes = read_blob(archive, descriptor)
        manifest = json.loads(manifest_bytes)
        config_descriptor = manifest["config"]
        config_bytes = read_blob(archive, config_descriptor)
        config = json.loads(config_bytes)
        require(manifest["schemaVersion"] == 2 and manifest["mediaType"] == MANIFEST
                and all(config.get(key) == value for key, value in target.items() if key != "variant")
                and all(config["config"]["Labels"].get(key) == value for key, value in expected_labels.items()),
                f"unexpected image identity in {path}")
        if directory is not None:
            (directory / blob_path(descriptor["digest"])).write_bytes(manifest_bytes)
            (directory / blob_path(config_descriptor["digest"])).write_bytes(config_bytes)
            for layer in manifest["layers"]:
                name = blob_path(layer["digest"])
                require(archive.getmember(name).size == layer["size"], f"wrong OCI layer size in {path}")
                destination = directory / name
                if not destination.exists():
                    with archive.extractfile(name) as contents, destination.open("wb") as blob:
                        digest = hashlib.sha256()
                        while chunk := contents.read(1024 * 1024):
                            digest.update(chunk)
                            blob.write(chunk)
                    require("sha256:" + digest.hexdigest() == layer["digest"], f"corrupt OCI layer in {path}")
        return ({key: descriptor[key] for key in ("mediaType", "digest", "size", "platform")},
                config_descriptor["digest"])


def build(args):
    docker = ("docker", "--context", args.context)
    target = PLATFORMS[args.arch]
    arch = target["architecture"]
    require(platform.machine() == {"amd64": "x86_64", "arm64": "aarch64"}[arch]
            and output(*docker, "info", "--format", "{{.OSType}} {{.Architecture}}")
            in {f"linux {platform.machine()}", f"linux {arch}"}, f"expected native Linux {arch}")
    if args.arch == "amd64v3":
        require("x86-64-v3 (supported, searched)" in output("/lib64/ld-linux-x86-64.so.2", "--help"),
                "build host does not support x86-64-v3")
    require(output("git", "rev-parse", "HEAD") == args.revision, "checkout does not match release SHA")
    args.artifacts.mkdir()
    args.evidence.mkdir()
    prefix = f"rfc-release-{args.revision[:12]}"
    build_platform = "/".join(target.values())
    with tempfile.TemporaryDirectory(prefix="rfc-release-source-", dir="/tmp") as temporary:
        source = Path(temporary)
        with subprocess.Popen(["git", "archive", args.revision], stdout=subprocess.PIPE) as archive:
            run("tar", "-xf", "-", "-C", str(source), stdin=archive.stdout)
            require(archive.wait() == 0, "git archive failed")
        stamp(source, args.version)
        run("cargo", "metadata", "--format-version", "1", "--frozen", cwd=source, stdout=subprocess.DEVNULL)
        epoch = output("git", "show", "-s", "--format=%ct", args.revision)
        buildx = (*docker, "buildx", "build", "--platform", build_platform, "--provenance=false", "--sbom=false",
                  "--build-arg", f"SOURCE_DATE_EPOCH={epoch}")
        receipt = {**identity(args), "platform": build_platform, "images": {}}
        for role in ROLES:
            image = f"{prefix}-{role}"
            archive = args.artifacts / f"{role}.tar"
            metadata = source / "build-metadata.json"
            label_args = [arg for key, value in labels(args).items() for arg in ("--label", f"{key}={value}")]
            run(*buildx, "--target", role, "--tag", image, *label_args, "--metadata-file", str(metadata),
                "--output", f"type=docker,oci-mediatypes=true,compression=gzip,force-compression=true,"
                            f"rewrite-timestamp=true,dest={archive}", str(source))
            descriptor, config = image_archive(archive, target, labels(args))
            build_metadata = json.loads(metadata.read_bytes())
            require(descriptor["digest"] == build_metadata["containerimage.digest"]
                    and config == build_metadata["containerimage.config.digest"], f"Buildx metadata differs for {role}")
            run(*docker, "image", "load", "--input", str(archive))
            loaded = json.loads(output(*docker, "image", "inspect", image))[0]
            require(loaded["Id"] in {config, descriptor["digest"]}, f"wrong loaded image for {role}")
            version = output(*docker, "run", "--rm", "--read-only", "--network", "none", "--cap-drop", "ALL",
                             "--security-opt", "no-new-privileges:true", loaded["Id"], "--version")
            require(version == f"rfc-mcp-docker-{role} {args.version}", f"wrong binary version for {role}")
            receipt["images"][role] = {"archive": file_digest(archive), "manifest": descriptor["digest"],
                                       "config": config, "image_id": loaded["Id"]}
        write_json(args.evidence / "image-identities.json", receipt)
        run(*buildx, "--target", "phase0-fixtures", "--tag", f"{prefix}-fixtures", "--load", str(source))
        run(sys.executable, "-B", str(ROOT / "scripts/ci/docker-e2e.py"), "--context", args.context,
            "--image-prefix", prefix, "run", "phase0", str(args.evidence / "phase0"))
        inputs = args.evidence / "rust"
        run(*buildx, "--target", "audit-inputs", "--output", f"type=local,dest={inputs}", str(source))
        run("bash", str(ROOT / "scripts/ci/audit-release-images.sh"), args.context, args.kind,
            str(args.evidence))
    write_json(args.artifacts / "receipt.json", receipt)


def assemble(args, directory):
    work = {}
    receipts = {}
    for arch, target in PLATFORMS.items():
        path = args.artifacts / arch
        receipt = json.loads((path / "receipt.json").read_bytes())
        require(all(receipt.get(key) == value for key, value in identity(args).items())
                and receipt["platform"] == "/".join(target.values()) and set(receipt["images"]) == set(ROLES),
                f"receipt does not match release for {arch}")
        receipts[arch] = receipt["images"]
    for role in ROLES:
        layout = directory / role
        (layout / "blobs/sha256").mkdir(parents=True)
        manifests = []
        for arch, target in PLATFORMS.items():
            archive = args.artifacts / arch / f"{role}.tar"
            record = receipts[arch][role]
            require(file_digest(archive) == record["archive"], f"archive checksum mismatch for {role}/{arch}")
            descriptor, config = image_archive(archive, target, labels(args), layout)
            require(descriptor["digest"] == record["manifest"] and config == record["config"]
                    and record["image_id"] in {config, descriptor["digest"]}, f"receipt mismatch for {role}/{arch}")
            manifests.append(descriptor)
        index = json.dumps({"schemaVersion": 2, "mediaType": INDEX, "manifests": manifests,
                            "annotations": labels(args)}, sort_keys=True, separators=(",", ":")).encode()
        digest = sha256(index)
        (layout / blob_path(digest)).write_bytes(index)
        write_json(layout / "oci-layout", {"imageLayoutVersion": "1.0.0"})
        write_json(layout / "index.json", {"schemaVersion": 2, "mediaType": INDEX, "manifests": [
            {"mediaType": INDEX, "size": len(index), "digest": digest,
             "annotations": {"org.opencontainers.image.ref.name": args.version}},
        ]})
        work[role] = {
            "image": f"{args.registry}/rfc-mcp-docker-{role}",
            "index": digest,
            "platforms": {arch: item["digest"] for arch, item in zip(PLATFORMS, manifests, strict=True)},
        }
    return work


def registry_options(args, prefix=""):
    return [f"--{prefix}tls-verify=false"] if args.local_registry else []


def registry_digest(args, ref, *, allow_missing=False, allow_bootstrap=False):
    result = subprocess.run(["skopeo", "inspect", "--raw", *registry_options(args), f"docker://{ref}"],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode == 0:
        return sha256(result.stdout)
    error = result.stderr.decode(errors="replace")
    if allow_missing and re.search(r"\b(?:manifest unknown|name unknown)\b", error, re.I):
        return None
    if (allow_bootstrap and ref.startswith("ghcr.io/publicdata-stream/")
            and re.search(r"Requesting bearer token: .*\b403 Forbidden\b", error, re.I)):
        return "bootstrap"
    raise ValueError(f"cannot inspect {ref}: {error.strip()}")


def copy_index(args, source, ref):
    run("skopeo", "copy", "--all", "--preserve-digests", *registry_options(args, "dest-"),
        f"oci:{source}:{args.version}", f"docker://{ref}")


def publish(args):
    with tempfile.TemporaryDirectory(prefix="rfc-release-index-", dir="/tmp") as temporary:
        directory = Path(temporary)
        images = assemble(args, directory)
        states = {}
        for role, entry in images.items():
            ref = f"{entry['image']}:{args.version}"
            states[role] = registry_digest(args, ref, allow_missing=True, allow_bootstrap=not args.local_registry)
            require(states[role] in {None, "bootstrap", entry["index"]}, f"refusing to replace {ref}")
        for role, entry in images.items():
            if states[role] == "bootstrap":
                copy_index(args, directory / role, f"{entry['image']}@{entry['index']}")
                ref = f"{entry['image']}:{args.version}"
                states[role] = registry_digest(args, ref, allow_missing=True)
                require(states[role] in {None, entry["index"]}, f"refusing to replace {ref}")
        for role, entry in images.items():
            ref = f"{entry['image']}:{args.version}"
            if states[role] is None:
                current = registry_digest(args, ref, allow_missing=True)
                require(current in {None, entry["index"]}, f"refusing to replace {ref}")
                if current is None:
                    copy_index(args, directory / role, ref)
            require(registry_digest(args, ref) == entry["index"], f"registry index mismatch for {ref}")
        write_json(args.receipt, {**identity(args), "images": images})


def verify_anonymous(args):
    receipt = json.loads(args.receipt.read_bytes())
    require(all(receipt.get(key) == value for key, value in identity(args).items())
            and set(receipt["images"]) == set(ROLES), "invalid publication receipt")
    with tempfile.TemporaryDirectory(prefix="rfc-release-anonymous-", dir="/tmp") as temporary:
        directory = Path(temporary)
        auth = directory / "auth.json"
        auth.write_text("{}\n")
        for role, entry in receipt["images"].items():
            require(entry["image"] == f"{args.registry}/rfc-mcp-docker-{role}"
                    and DIGEST.fullmatch(entry["index"]) and set(entry["platforms"]) == set(PLATFORMS),
                    f"invalid publication receipt entry for {role}")
            ref = f"{entry['image']}:{args.version}"
            local = directory / role
            try:
                run("skopeo", "copy", "--all", "--preserve-digests", "--src-no-creds", "--src-authfile",
                    str(auth), *registry_options(args, "src-"), f"docker://{ref}", f"oci:{local}:verified")
            except subprocess.CalledProcessError as error:
                raise ValueError(f"anonymous pull failed for {ref}. Check the GHCR package visibility.") from error
            root = json.loads((local / "index.json").read_bytes())
            index_path = local / blob_path(entry["index"])
            require(len(root["manifests"]) == 1 and root["manifests"][0]["digest"] == entry["index"]
                    and file_digest(index_path) == entry["index"], f"anonymous index mismatch for {ref}")
            index = json.loads(index_path.read_bytes())
            require(index["mediaType"] == INDEX and len(index["manifests"]) == len(PLATFORMS),
                    f"unexpected OCI index for {ref}")
            for arch, target in PLATFORMS.items():
                digest = entry["platforms"][arch]
                require(any(item["digest"] == digest and item["platform"] == target for item in index["manifests"])
                        and file_digest(local / blob_path(digest)) == digest, f"anonymous {arch} mismatch for {ref}")
            print(f"Verified anonymous pull of {ref}@{entry['index']}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--revision", required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    builder = commands.add_parser("build")
    builder.add_argument("--context", default="default")
    builder.add_argument("--arch", choices=PLATFORMS, required=True)
    builder.add_argument("--kind", choices=("build", "beta", "stable"), required=True)
    builder.add_argument("--artifacts", type=Path, required=True)
    builder.add_argument("--evidence", type=Path, required=True)
    for command in ("publish", "verify-anonymous"):
        registry = commands.add_parser(command)
        registry.add_argument("--local-registry", help="use a disposable loopback registry instead of GHCR")
        registry.add_argument("--receipt", type=Path, required=True)
        if command == "publish":
            registry.add_argument("--artifacts", type=Path, required=True)
    args = parser.parse_args()
    require(re.fullmatch(r"[0-9a-f]{40}", args.revision)
            and re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:-beta\.[0-9]+|-build\.[0-9a-f]{8})?", args.version)
            and len(args.version) <= 128, "invalid release identity")
    if args.command == "build":
        build(args)
        return
    if args.local_registry:
        require(re.fullmatch(r"localhost:[0-9]{1,5}", args.local_registry), "expected a loopback registry")
    args.registry = args.local_registry or "ghcr.io/publicdata-stream"
    if args.command == "publish":
        publish(args)
    else:
        verify_anonymous(args)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, KeyError) as error:
        sys.exit(str(error))
