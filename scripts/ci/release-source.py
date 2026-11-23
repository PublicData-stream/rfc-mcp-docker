#!/usr/bin/env python3
"""Resolve a release event and require its canonical main-branch CI."""

import json
import os
from pathlib import Path
import re
import subprocess
from urllib.parse import quote


REPOSITORY = "PublicData-stream/rfc-mcp-docker"
NUMBER = r"(?:0|[1-9][0-9]*)"
TAG = re.compile(rf"{NUMBER}\.{NUMBER}\.{NUMBER}(?:-beta\.[1-9][0-9]*|-build\.[0-9a-f]{{8}})?")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def git(*args):
    return subprocess.check_output(["git", *args], text=True).strip()


def api(endpoint, *, paginate=False):
    command = ["gh", "api"]
    if paginate:
        command.extend(("--paginate", "--slurp"))
    return json.loads(subprocess.check_output([*command, f"repos/{REPOSITORY}/{endpoint}"]))


def release_kind(version, revision):
    require(len(version) <= 128 and TAG.fullmatch(version), "invalid release version")
    if "-build." in version:
        require(version.endswith(f"-build.{revision[:8]}"), "build tag does not match the source SHA")
        return "build"
    return "beta" if "-beta." in version else "stable"


def verify_event(event_name, event, version, revision, tag_object):
    kind = release_kind(version, revision)
    if kind == "build":
        require(event_name == "push" and event.get("created") is True
                and event.get("after") in {tag_object, revision},
                "a build tag requires its original tag creation event")
    else:
        release = event.get("release", {})
        require(event_name == "release" and event.get("action") == "published"
                and release.get("tag_name") == version and release.get("draft") is False
                and release.get("prerelease") is (kind == "beta"),
                "a stable or beta tag requires the matching published GitHub Release")
    return kind


def verify_tag(ref, version, revision, tag_object):
    current = api(f"git/ref/{quote(ref.removeprefix('refs/'), safe='/')}")
    require(current["ref"] == ref and current["object"]["type"] == "tag"
            and current["object"]["sha"] == tag_object, "remote annotated tag changed")
    tag = api(f"git/tags/{tag_object}")
    require(tag["sha"] == tag_object and tag["tag"] == version
            and tag["object"]["type"] == "commit" and tag["object"]["sha"] == revision
            and tag["verification"]["verified"] is True, "GitHub did not verify the signed release tag")


def canonical_run(run, revision):
    return (run["path"] == ".github/workflows/ci.yml" and run["event"] == "push"
            and run["head_branch"] == "main" and run["head_sha"] == revision
            and run["repository"]["full_name"] == REPOSITORY
            and run["head_repository"]["full_name"] == REPOSITORY)


def completed(run):
    return run["status"] == "completed" and run["conclusion"] == "success"


def verify_ci(revision):
    query = f"branch=main&event=push&head_sha={revision}&per_page=100"
    pages = api(f"actions/workflows/ci.yml/runs?{query}", paginate=True)
    runs = [run for page in pages for run in page["workflow_runs"] if canonical_run(run, revision)]
    require(runs, "no canonical main CI run for the release SHA")
    run = max(runs, key=lambda item: item["id"])
    attempt = run["run_attempt"]
    require(completed(run) and type(attempt) is int and attempt > 0,
            f"newest canonical CI did not pass: {run['html_url']}")
    pages = api(f"actions/runs/{run['id']}/attempts/{attempt}/jobs?per_page=100", paginate=True)
    summaries = [job for page in pages for job in page["jobs"] if job["name"] == "CI summary"]
    require(len(summaries) == 1, "expected one canonical CI summary")
    job = summaries[0]
    check_id = re.search(r"/check-runs/([0-9]+)$", job["check_run_url"])
    require(completed(job) and job["head_sha"] == revision and check_id,
            "CI summary did not pass for the release SHA")
    check = api(f"check-runs/{check_id[1]}")
    require(check["name"] == "CI summary" and check["head_sha"] == revision and completed(check)
            and check["app"]["id"] == 15368 and check["app"]["slug"] == "github-actions"
            and check["details_url"] == job["html_url"], "unexpected CI summary check identity")
    final = api(f"actions/runs/{run['id']}")
    require(canonical_run(final, revision) and completed(final) and final["run_attempt"] == attempt,
            "canonical CI changed attempt during release verification")
    print(f"Accepted CI summary: {run['html_url']} (attempt {attempt})")


def main():
    require(os.environ["GITHUB_REPOSITORY"] == REPOSITORY, "unexpected release repository")
    ref = os.environ["GITHUB_REF"]
    require(ref.startswith("refs/tags/"), "release source must be a tag")
    version = ref.removeprefix("refs/tags/")
    revision = git("rev-parse", "HEAD")
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_bytes())
    tag_object = git("rev-parse", "--verify", ref)
    kind = verify_event(os.environ["GITHUB_EVENT_NAME"], event, version, revision, tag_object)
    require(os.environ["GITHUB_SHA"] in {tag_object, revision}
            and git("rev-parse", f"{ref}^{{commit}}") == revision, "tag, event, and checkout differ")
    git("merge-base", "--is-ancestor", revision, "refs/remotes/origin/main")
    verify_tag(ref, version, revision, tag_object)
    verify_ci(revision)
    with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
        print(f"version={version}\nrevision={revision}\nkind={kind}\ntag_object={tag_object}", file=output)


if __name__ == "__main__":
    main()
