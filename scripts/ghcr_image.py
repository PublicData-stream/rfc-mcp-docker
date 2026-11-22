"""Resolve the inputs and verify the outputs of the GHCR workflow."""

import argparse
from collections import deque
from datetime import date, datetime, timezone
import json
import re
import subprocess
import time
from urllib.error import URLError
from urllib.request import Request, urlopen


SCHEDULE_ANCHOR = date(2026, 11, 22)
UPSTREAM = "https://api.github.com/repos/shbernal/rfc-ai-tooling"
VERSION = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+")
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
ARCHITECTURES = {"amd64", "arm64"}
WRAPPER_REVISION = "org.opencontainers.image.revision"
UPSTREAM_REVISION = "stream.publicdata.rfc-mcp.upstream-revision"


def schedule(event, created_at):
    triggered = datetime.fromisoformat(created_at.replace("Z", "+00:00")).astimezone(
        timezone.utc
    )
    day = triggered.date()
    week = (day - SCHEDULE_ANCHOR).days // 7
    return event == "workflow_dispatch" or week % 2 == 0, day.strftime("%Y%m%d")


def github_release_json(path):
    request = Request(
        UPSTREAM + path,
        headers={"Accept": "application/vnd.github+json"},
    )
    with urlopen(request, timeout=30) as response:
        return json.load(response)


def stable_release(release):
    return (
        bool(release["published_at"])
        and not release["draft"]
        and not release["prerelease"]
        and not release["tag_name"].startswith("index-")
        and VERSION.fullmatch(release["name"] or "") is not None
    )


def release_commit(tag):
    return subprocess.run(
        ["git", "-C", "source", "rev-parse", "--verify", "--quiet", f"refs/tags/{tag}^{{commit}}"],
        capture_output=True,
        text=True,
    ).stdout.strip()


def pinned_commit():
    pin = subprocess.check_output(["git", "rev-parse", "HEAD:source"], text=True).strip()
    checked_out = subprocess.check_output(
        ["git", "-C", "source", "rev-parse", "HEAD"], text=True
    ).strip()
    if pin != checked_out:
        raise ValueError(f"Source checkout {checked_out} differs from the recorded pin {pin}")
    return pin


def nearest_release(pin, parents, releases):
    """Find the closest release by parent edges, then prefer the higher version."""
    queue = deque([(pin, 0)])
    seen = {pin}
    candidates = []
    while queue:
        commit, distance = queue.popleft()
        if candidates and distance > candidates[0][0]:
            break
        for name in releases.get(commit, []):
            candidates.append((distance, tuple(map(int, name.split("."))), name))
        if not candidates:
            for parent in parents[commit]:
                if parent not in seen:
                    seen.add(parent)
                    queue.append((parent, distance + 1))
    if not candidates:
        raise ValueError(f"No published stable release is an ancestor of upstream {pin}")
    return max(candidates)[2]


def resolve_pinned_release(pin):
    parents = {}
    history = subprocess.check_output(
        ["git", "-C", "source", "rev-list", "--parents", pin], text=True
    )
    for line in history.splitlines():
        commit, *ancestors = line.split()
        parents[commit] = ancestors

    releases = {}
    page = 1
    while True:
        batch = github_release_json(f"/releases?per_page=100&page={page}")
        for release in batch:
            if stable_release(release):
                commit = release_commit(release["tag_name"])
                if commit in parents:
                    releases.setdefault(commit, []).append(release["name"])
        if len(batch) < 100:
            break
        page += 1
    return nearest_release(pin, parents, releases)


def imagetools_inspect(reference, field):
    return subprocess.run(
        ["docker", "buildx", "imagetools", "inspect", "--format", "{{json ." + field + "}}", reference],
        capture_output=True,
        text=True,
    )


def image_digest(image, tag, wrapper_sha, source_sha, allow_uninitialized=False, expected=None):
    reference = f"{image}:{tag}"
    result = imagetools_inspect(reference, "Manifest")
    if result.returncode:
        # GHCR can refuse a pull token until the first upload creates the package.
        # The publish job checks again, without this exception, after uploading by digest.
        error = result.stderr.strip()
        if error == f"ERROR: {reference}: not found":
            return ""
        if (
            allow_uninitialized
            and "https://ghcr.io/token?" in error
            and error.endswith("403 Forbidden")
        ):
            return ""
        raise ValueError(f"Cannot inspect {reference}: {error}")

    manifest = json.loads(result.stdout)
    digest = manifest["digest"]
    if not DIGEST.fullmatch(digest):
        raise ValueError(f"Invalid manifest digest for {reference}")
    platforms = manifest.get("manifests", [])
    images = {
        item["platform"]["architecture"]: item["digest"]
        for item in platforms
        if item.get("platform", {}).get("os") == "linux"
    }
    if images.keys() != ARCHITECTURES or len(platforms) != len(ARCHITECTURES):
        raise ValueError(f"{reference} must contain exactly the Linux AMD64 and ARM64 images")
    if expected is not None and images != expected:
        raise ValueError(f"{reference} does not point to the tested platform digests")

    for architecture, platform_digest in images.items():
        if not DIGEST.fullmatch(platform_digest):
            raise ValueError(f"Invalid {architecture} digest for {reference}")
        config = imagetools_inspect(f"{image}@{platform_digest}", "Image")
        if config.returncode:
            raise ValueError(f"Cannot inspect {architecture} config: {config.stderr.strip()}")
        labels = json.loads(config.stdout)["config"].get("Labels") or {}
        if labels.get(WRAPPER_REVISION) != wrapper_sha or labels.get(UPSTREAM_REVISION) != source_sha:
            raise ValueError(f"{reference} already contains a different {architecture} source revision")
    return digest


def smoke_http(url):
    request = Request(
        url,
        data=json.dumps(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {"name": "ghcr-image-smoke", "version": "1"},
                },
            }
        ).encode(),
        headers={
            "Accept": "application/json, text/event-stream",
            "Content-Type": "application/json",
        },
    )
    deadline = time.monotonic() + 30
    while True:
        try:
            response = urlopen(request, timeout=5)
        except (URLError, ConnectionError) as exc:
            reason = exc.reason if isinstance(exc, URLError) else exc
            if isinstance(reason, ConnectionError) and time.monotonic() < deadline:
                time.sleep(1)
                continue
            raise
        with response:
            if response.headers.get_content_type() == "text/event-stream":
                message = next(
                    (
                        json.loads(line.removeprefix(b"data:"))
                        for line in response
                        if line.startswith(b"data:")
                    ),
                    {},
                )
            else:
                message = json.load(response)
        if message.get("id") != 1 or message.get("result", {}).get("serverInfo", {}).get("name") != "rfc":
            raise ValueError(f"MCP initialize failed: {message}")
        return


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("schedule")
    run.add_argument("event", choices=["schedule", "workflow_dispatch"])
    run.add_argument("created_at")
    commands.add_parser("pinned-release")
    commands.add_parser("latest-release")
    image = commands.add_parser("image-digest")
    image.add_argument("image")
    image.add_argument("tag")
    image.add_argument("wrapper_sha")
    image.add_argument("source_sha")
    image.add_argument("--allow-uninitialized", action="store_true")
    image.add_argument("--expected", nargs=2, metavar=("AMD64_DIGEST", "ARM64_DIGEST"))
    smoke = commands.add_parser("smoke-http")
    smoke.add_argument("url")
    args = parser.parse_args()

    if args.command == "schedule":
        eligible, day = schedule(args.event, args.created_at)
        print(str(eligible).lower(), day)
    elif args.command == "pinned-release":
        pin = pinned_commit()
        print(resolve_pinned_release(pin), pin)
    elif args.command == "latest-release":
        release = github_release_json("/releases/latest")
        if not stable_release(release):
            raise ValueError("Latest upstream release does not have a stable X.Y.Z title")
        commit = release_commit(release["tag_name"])
        if not commit:
            raise ValueError(f"Tag {release['tag_name']} for the latest release is missing")
        print(release["name"], commit)
    elif args.command == "image-digest":
        expected = dict(zip(("amd64", "arm64"), args.expected)) if args.expected else None
        print(
            image_digest(
                args.image, args.tag, args.wrapper_sha, args.source_sha,
                args.allow_uninitialized, expected,
            )
        )
    else:
        smoke_http(args.url)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, subprocess.CalledProcessError) as error:
        raise SystemExit(str(error)) from error
