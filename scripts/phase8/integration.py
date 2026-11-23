#!/usr/bin/env python3
"""Rehearse destructive restore and client compatibility on private Compose."""

import argparse
from contextlib import contextmanager
import importlib.util
import json
from pathlib import Path
import secrets
import shutil
import subprocess
import tempfile
import time

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("phase7", HERE.parent / "phase7/integration.py")
phase7 = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(phase7)
phase5 = phase7.phase5
phase3 = phase5.phase3
SERVE, SYNC, FEED = phase7.SERVE, phase7.SYNC, phase7.FEED
BLOB_QUERY = "SELECT encode(digest,'hex'),byte_length FROM rfc.blobs ORDER BY digest"
READABLE_QUERY = "SELECT id FROM rfc.generations WHERE state IN ('published','retained') ORDER BY id"
POINTER_QUERY = "SELECT generation_id,publication_sequence FROM rfc.active_generation"
TOOLS = (
    ("list", {"limit": 2}),
    ("get", {"id": 2119}),
    ("search", {"query": 'in::title:"TLS"', "include_snippets": True}),
    ("table", {"id": 102}),
    ("rg", {"pattern": "TLS", "ids": [102, 108], "limit": 1}),
)


def arguments(parser):
    parser.add_argument("--context", default="rootless")
    parser.add_argument("--image-prefix", default="rfc-phase8-qualified")
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    args.evidence = args.evidence.resolve()
    if Path("/tmp") not in args.evidence.parents or not args.evidence.is_dir():
        parser.error("--evidence must be an existing directory beneath /tmp")
    if not __debug__:
        parser.error("qualification requires enabled assertions")
    return args


@contextmanager
def project(work, args, driver, fixture_type=phase5.Project):
    work.mkdir()
    docker = ["docker", "--context", args.context]
    seed = fixture_type(work / work.name, driver, args.image_prefix, docker)
    seed.key = secrets.token_bytes(48)
    seed.env.update(PHASE5_DIR=str(work), PHASE5_SCRIPTS=str(phase7.PHASE5), PHASE5_PROXY_IP="")
    (work / "fixture-policy.json").touch()
    (work / "cursor-key").write_bytes(seed.key)
    (work / "tls").mkdir()
    phase7.phase1.phase0.make_leaf(seed.directory / "certs", "localhost", work / "tls")
    try:
        yield seed
    finally:
        seed.close()


@contextmanager
def deployment(work, seed, args):
    stack = phase7.Deployment(work, seed, args.image_prefix)
    try:
        yield stack
    finally:
        stack.close()


def dump(command, env, path):
    with path.open("wb") as output:
        subprocess.run([*command, "exec", "-T", "postgres", "pg_dump", "-Fc", "--schema=rfc",
                        "-U", "postgres", "-d", "rfc"], env=env, stdout=output,
                       check=True, timeout=90)


def driver_call(stack, driver, role, *args, archive=None):
    options = ["run", "--rm", "--no-deps", "-T", "-v", f"{driver}:/phase8/driver:ro",
               "--entrypoint", "/phase8/driver"]
    if archive:
        options.extend(("-v", f"{archive}:/fixture:ro"))
    proc = subprocess.run(stack.command(*options, role, *args), env=stack.env, text=True,
                          capture_output=True, timeout=90)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)
    return proc.stdout.strip()


def verify_archive(seed, archive, blobs):
    for digest, length in blobs:
        path = archive / digest
        if not path.is_file() or path.stat().st_size != length:
            raise ValueError(f"missing or wrong-length backup blob {digest}")
        if seed.digest(path.read_bytes()) != digest:
            raise ValueError(f"wrong digest for backup blob {digest}")


def without_cursors(value):
    if isinstance(value, dict):
        return {key: without_cursors(entry) for key, entry in value.items()
                if key != "next_cursor"}
    if isinstance(value, list):
        return [without_cursors(entry) for entry in value]
    return value


def reads(client):
    tools = client.rpc("tools/list")[0]["tools"]
    assert [tool["name"] for tool in tools] == ["rg", "list", "search", "get", "table"]
    client.schemas = {tool["name"]: phase5.Draft202012Validator(tool["outputSchema"]) for tool in tools}
    return {name: without_cursors(client.call(name, **options)[0]) for name, options in TOOLS}


def compatibility(stack):
    client = stack.client
    expected = reads(client)
    assert client.rpc("server/discover")[0]["supportedVersions"] == [phase5.VERSION]

    def curl(method, headers, payload):
        command = ["curl", "--silent", "--show-error", "--noproxy", "*", "--http1.1",
                   "--cacert", str(stack.ca), "--max-time", "18", "--request", method,
                   "--write-out", "\n%{http_code}"]
        for key, value in headers.items():
            command.extend(("--header", f"{key}: {value}"))
        if payload is not None:
            command.extend(("--data-binary", "@-"))
        time.sleep(0.21)
        response = subprocess.run(
            [*command, f"https://localhost:{client.port}/mcp"],
            input=json.dumps(payload) if payload is not None else None,
            text=True, capture_output=True, check=True, timeout=20,
        ).stdout
        data, status = response.rsplit("\n", 1)
        return int(status), data

    # The first independent request is a direct call without negotiation.
    for name, options in TOOLS:
        body, headers = phase5.envelope("tools/call", {"name": name, "arguments": options})
        status, data = curl("POST", headers, body)
        assert status == 200 and without_cursors(json.loads(data)["result"]["structuredContent"]) == expected[name]
    for method, field, expected_value in (
        ("server/discover", "supportedVersions", [phase5.VERSION]),
        ("tools/list", "tools", client.rpc("tools/list")[0]["tools"]),
    ):
        body, headers = phase5.envelope(method)
        status, data = curl("POST", headers, body)
        assert status == 200 and json.loads(data)["result"][field] == expected_value
    body, headers = phase5.envelope("tools/list")
    body["params"]["_meta"][phase5.PREFIX + "protocolVersion"] = "2025-11-25"
    headers["MCP-Protocol-Version"] = "2025-11-25"
    status, data = curl("POST", headers, body)
    assert status == 400 and json.loads(data)["error"]["code"] == -32022
    assert client.exchange(body, headers)[0] == 400
    for method in ("GET", "DELETE"):
        headers = {"Host": "rfc.test", "Accept": "text/event-stream"}
        assert curl(method, headers, None)[0] == 405
        assert client.exchange(None, headers, method)[0] == 405
    return {
        "python": subprocess.check_output(["python3", "--version"], text=True).strip(),
        "curl": subprocess.check_output(["curl", "--version"], text=True).splitlines()[0],
        "mcp": phase5.VERSION,
        "http": {"direct_calls_discovery_list": 200, "2025-11-25": 400, "GET_and_DELETE": 405},
    }


def prepare_backup(root, args, driver, backup):
    work = root / f"{root.name}-source"
    with project(work, args, driver) as seed:
        seed.start()
        seed.migrate()
        seed.prepare_reviews()
        seed.run("up", "-d", "--wait", "fixtures", timeout=60)
        first = seed.reconcile()["generation_id"]
        seed.activate(int(first), 0)
        phase3.changed_catalog(seed, "Phase 8 metadata-only publication")
        report = seed.reconcile()
        second = report["generation_id"]
        assert report["verified_txt"] > 0 and report["reused_txt"] == report["verified_txt"], report
        seed.activate(int(second), 1)
        phase3.changed_catalog(seed, "Phase 8 ready candidate")
        ready = seed.reconcile()["generation_id"]
        dump(seed.command(), seed.env, work / "fixture.dump")
        seed.run("stop", "fixtures", "postgres", timeout=50)
        with deployment(work, seed, args) as stack:
            stack.provision()
            stack.restore(work / "fixture.dump")
            baseline_verify = json.loads(driver_call(stack, driver, SERVE, "verify-restore"))
            assert set(baseline_verify["manifests"]) == {str(first), str(second), str(ready)}
            stack.open(SERVE, "test-proxy")
            assert stack.admin("/ready") == ""
            assert stack.sql("-c", "SELECT pg_try_advisory_lock(-1)") == "t"
            baseline = reads(stack.client)
            pointer = stack.sql("-c", POINTER_QUERY)
            assert pointer == f"{second}|2"
            readable = stack.sql("-c", READABLE_QUERY).splitlines()
            blobs = [(digest, int(length)) for digest, length in
                     (line.split("|") for line in stack.sql("-c", BLOB_QUERY).splitlines())]
            dump(stack.command(), stack.env, backup / "corpus.dump")
            for digest, _ in blobs:
                shutil.copyfile(seed.canonical(digest), backup / digest)
            verify_archive(seed, backup, blobs)
            (backup / "inventory.json").write_text(json.dumps({
                "blobs": blobs, "pointer": pointer, "readable": readable,
            }))
            return baseline, baseline_verify


def restore(root, args, driver, backup, baseline, baseline_verify):
    inventory = json.loads((backup / "inventory.json").read_text())
    work = root / f"{root.name}-destination"
    with project(work, args, driver) as seed:
        with deployment(work, seed, args) as stack:
            stack.provision()
            stack.restore(backup / "corpus.dump")
            # Check preflight failures before any public or sync container starts.
            digest, length = next((digest, length) for digest, length in inventory["blobs"] if length)
            blob = backup / digest
            moved = backup / "omitted"
            blob.rename(moved)
            try:
                try:
                    verify_archive(seed, backup, [(digest, length)])
                except ValueError as error:
                    assert "missing" in str(error)
                else:
                    raise AssertionError("accepted missing archive blob")
                original = moved.read_bytes()
                blob.write_bytes(bytes([original[0] ^ 1]) + original[1:])
                try:
                    verify_archive(seed, backup, [(digest, length)])
                except ValueError as error:
                    assert "wrong digest" in str(error)
                else:
                    raise AssertionError("accepted same-length archive corruption")
            finally:
                moved.replace(blob)
            verify_archive(seed, backup, inventory["blobs"])
            for digest, _ in inventory["blobs"]:
                driver_call(stack, driver, SYNC, "restore-blob", digest, archive=backup)
            for generation in inventory["readable"]:
                driver_call(stack, driver, SYNC, "restore-fence", generation)
            assert stack.sql("-c", POINTER_QUERY) == inventory["pointer"]
            assert stack.sql("-c", READABLE_QUERY).splitlines() == inventory["readable"]
            actual = json.loads(driver_call(stack, driver, SERVE, "verify-restore"))
            assert actual == baseline_verify, (actual, baseline_verify)
            stack.open(SERVE, "test-proxy")
            assert stack.admin("/ready") == ""
            assert reads(stack.client) == baseline
            return {"verified": actual, "pointer": inventory["pointer"],
                    "readable": inventory["readable"], "compatibility": compatibility(stack)}


def main():
    args = arguments(argparse.ArgumentParser(description=__doc__))
    with tempfile.TemporaryDirectory(prefix="rfc-phase8-restore-", dir="/tmp") as directory:
        root = Path(directory)
        backup = root / "backup"
        backup.mkdir(mode=0o755)
        driver = root / "publication-driver"
        phase3.fixture_driver(["docker", "--context", args.context], args.image_prefix, driver)
        baseline, verified = prepare_backup(root, args, driver, backup)
        source = root / f"{root.name}-source"
        shutil.rmtree(source)
        assert not source.exists()
        report = restore(root, args, driver, backup, baseline, verified)
    (args.evidence / "restore.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
