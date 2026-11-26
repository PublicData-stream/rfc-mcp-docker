#!/usr/bin/env python3
"""Qualify the physical Serve pool and its PostgreSQL TLS boundaries."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, ExitStack
import json
from pathlib import Path
import secrets
import select
import subprocess
import sys
import tempfile
import time

sys.dont_write_bytecode = True
import integration as phase5

phase3, phase1 = phase5.phase3, phase5.phase1


def wait(probe, seconds=6):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = probe()
        if result:
            return result
        time.sleep(0.04)
    raise AssertionError(f"timeout: {probe.__name__}")


@contextmanager
def paused_request(project, address="203.0.113.1"):
    command = project.command("exec", "-T", "proxy", "python3", "-B", "/phase5/direct.py", "cancel", address)
    with subprocess.Popen(command, env=project.env, text=True, stdin=subprocess.PIPE,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE) as request:
        try:
            assert select.select([request.stdout], [], [], 5)[0] and request.stdout.readline().strip() == "sent"
            yield
        finally:
            _, errors = request.communicate("disconnect\n", timeout=5)
            assert request.returncode == 0, errors


@contextmanager
def hold(project, relation):
    with subprocess.Popen(project.psql(), env=project.env, text=True, stdin=subprocess.PIPE,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE) as lock:
        lock.stdin.write(f"BEGIN; LOCK {relation} IN ACCESS EXCLUSIVE MODE;\n\\echo locked\n")
        lock.stdin.flush()
        try:
            assert select.select([lock.stdout], [], [], 5)[0] and lock.stdout.readline().strip() == "locked"
            yield
        finally:
            lock.communicate("ROLLBACK;\n", timeout=8)
            assert lock.returncode == 0


class Observer:
    def __init__(self, project, work, image_prefix):
        self.project = project
        self.name = project.name + "-pg-wire"
        self.state = work / "wire-state"
        self.state.mkdir()
        postgres = project.capture("ps", "-q", "postgres")
        phase5.run([*project.docker, "run", "-d", "--name", self.name, "--network",
                    f"container:{postgres}", "--read-only", "--cap-drop", "ALL", "--pids-limit", "24",
                    "--security-opt", "no-new-privileges:true",
                    "-v", f"{project.directory / 'postgres'}:/wire-tls:ro",
                    "-v", f"{self.state}:/wire-state:ro", "-v", f"{phase5.HERE / 'pg-tls.py'}:/pg-tls.py:ro",
                    "--entrypoint", "python3", f"{image_prefix}-fixtures", "-B", "/pg-tls.py"])
        wait(lambda: "ready" in self.logs())

    def logs(self):
        return phase1.capture([*self.project.docker, "logs", self.name])

    def events(self):
        return [(int(connection.split("=")[1]), outcome.split("=")[1])
                for prefix, connection, outcome in (line.split() for line in self.logs().splitlines()
                                                     if line.startswith("wire "))]

    def observe(self, outcome, previous=()):
        return wait(lambda: next((connection for connection, event in self.events()
                                  if event == outcome and (connection, event) not in previous), None))

    @contextmanager
    def pause(self, stage, value=""):
        marker = self.state / stage
        marker.write_text(value)
        try:
            yield
        finally:
            marker.unlink()

    def close(self):
        phase5.run([*self.project.docker, "rm", "-f", self.name], stdout=subprocess.DEVNULL)


def metrics(project):
    result = project.admin("/metrics")
    assert result.returncode == 0, result.stderr
    return dict(line.split() for line in result.stdout.splitlines())


def pids(project, predicate="true"):
    return phase3.reader_pids(project.sql, predicate)


@contextmanager
def public_saturation(project):
    blocked = "wait_event_type='Lock'"
    with phase5.block_corpus(project), ExitStack() as calls:
        for index in range(2):
            calls.enter_context(paused_request(project, f"203.0.113.{index + 1}"))
        wait(lambda: len(pids(project, blocked)) == 2)
        working = pids(project, blocked)
        yield working
    phase3.assert_readers_idle(project.sql, gone=working, max_connections=3)


def check_acquire(project, client):
    with public_saturation(project):
        assert project.admin("/ready").returncode == 0
        assert len(pids(project)) == 3
        start = time.monotonic()
        client.call("get", id=2119, _error="resource_limit_exceeded")
        elapsed = time.monotonic() - start
        assert 0.8 <= elapsed < 2.5, elapsed

    with public_saturation(project) as working:
        with paused_request(project, "203.0.113.3"):
            wait(lambda: int(metrics(project)["rfc_mcp_db_pool_waiters"]) == 1)
            assert pids(project, "wait_event_type='Lock'") == working
        wait(lambda: int(metrics(project)["rfc_mcp_db_pool_waiters"]) == 0)
        assert pids(project, "wait_event_type='Lock'") == working
    print("Physical capacity, bounded wait, independent readiness and cancelled waiter passed", flush=True)


def disconnect_latency(project):
    with phase5.block_corpus(project):
        with paused_request(project):
            blocked = phase3.wait_for_readers(project.sql, "wait_event_type='Lock'")
            assert len(blocked) == 1
            start = time.monotonic()
        phase3.assert_readers_idle(project.sql, gone=blocked, max_connections=3)
        return 1000 * (time.monotonic() - start)


def rotate_and_recover(project, client):
    for change, restore in (
        ("REVOKE SELECT ON rfc.document_lexemes FROM rfc_serve",
         "GRANT SELECT ON rfc.document_lexemes TO rfc_serve"),
        ("ALTER ROLE rfc_serve INHERIT", "ALTER ROLE rfc_serve NOINHERIT"),
        ("INSERT INTO rfc.schema_migrations(version) VALUES (99)",
         "DELETE FROM rfc.schema_migrations WHERE version=99"),
    ):
        # Every change must be rejected on a previously healthy session.
        client.call("get", id=2119)
        live = pids(project)
        try:
            project.sql(change)
            client.call("get", id=2119, _error="search_unavailable")
        finally:
            project.sql(restore)
        wait(lambda: live - pids(project))
        client.call("get", id=2119)

    password = project.directory / "reader-password"
    original = password.read_bytes()
    try:
        password.write_bytes(secrets.token_hex(24).encode())
        client.call("get", id=2119, _error="search_unavailable")
        wait(lambda: not pids(project))
    finally:
        password.write_bytes(original)
    time.sleep(0.4)
    client.call("get", id=2119)
    ca = project.directory / "certs" / "ca.crt"
    trusted = ca.read_bytes()
    try:
        ca.write_bytes(ca.with_name("wrong-ca.crt").read_bytes())
        client.call("get", id=2119, _error="search_unavailable")
        wait(lambda: not pids(project))
    finally:
        ca.write_bytes(trusted)
    time.sleep(0.4)
    client.call("get", id=2119)
    old = pids(project)
    project.sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename='rfc_serve'")
    phase3.assert_readers_idle(project.sql, gone=old)
    client.call("get", id=2119)
    assert pids(project) - old
    print("Checkout role, schema, grants, TLS trust, and password rotation passed", flush=True)


def qualification(project, client, wire):
    assert project.admin("/live").returncode == 0
    assert "503" in project.admin("/ready").stderr
    generation = int(project.reconcile()["generation_id"])
    project.activate(generation, 0)

    for stage in ("hold_tls", "hold_commit"):
        before = wire.events()
        with wire.pause(stage, "1"):
            with paused_request(project):
                connection = wire.observe(stage, before)
            wire.observe("abrupt", before)
            assert (connection, "abrupt") in wire.events()
        phase3.assert_readers_idle(project.sql, max_connections=3)

    with hold(project, "rfc.schema_migrations"):
        with paused_request(project):
            verifying = phase3.wait_for_readers(project.sql, "wait_event_type='Lock'")
            assert len(verifying) == 1
        phase3.assert_readers_idle(project.sql, gone=verifying, max_connections=3)

    reserved = pids(project)
    client.call("get", id=2119)
    stable = pids(project)
    assert len(stable - reserved) == 1
    for _ in range(5):
        client.call("get", id=2119)
        assert pids(project) == stable
    assert int(metrics(project)["rfc_mcp_db_pool_reused_total"]) >= 5
    client.call("get", id=2_147_483_647, _error="rfc_not_found")
    assert phase3.assert_readers_idle(project.sql, max_connections=3) == stable

    check_acquire(project, client)
    rotate_and_recover(project, client)

    captured = wire.events()
    container = project.capture("ps", "-q", "serve")
    with ThreadPoolExecutor(max_workers=1) as worker:
        with phase5.block_corpus(project):
            request = worker.submit(client.call, "get", id=2119)
            draining = phase3.wait_for_readers(project.sql, "wait_event_type='Lock'")
            phase5.run([*project.docker, "kill", "--signal=TERM", container], stdout=subprocess.DEVNULL)
            assert not request.done()
        request.result(timeout=10)
    assert phase1.capture([*project.docker, "wait", container], timeout=25) == "0"
    phase3.assert_readers_idle(project.sql, gone=draining, max_connections=0)
    wire.observe("close_notify", captured)
    candidate_events = [item for item in wire.events() if item not in captured]
    assert candidate_events and all(
        (connection, "terminate") in wire.events()
        for connection, outcome in candidate_events if outcome == "close_notify"
    ), candidate_events
    print("Cancellation during TLS, verification and commit, PID reuse, and SIGTERM TLS closure passed",
          flush=True)


def network_interruption(project, client, wire):
    client.call("get", id=2119)
    old = pids(project)
    assert old
    phase5.run([*project.docker, "stop", "--time", "0", wire.name], stdout=subprocess.DEVNULL)
    try:
        phase3.assert_readers_idle(project.sql, gone=old)
        client.call("get", id=2119, _error="search_unavailable")
        assert project.admin("/live").returncode == 0
    finally:
        phase5.run([*project.docker, "start", wire.name], stdout=subprocess.DEVNULL)
    time.sleep(0.4)
    client.call("get", id=2119)
    assert pids(project) - old
    print("Interrupted PostgreSQL transport discarded stale sessions and recovered", flush=True)


def retirement(project, client, wire):
    project.env.update(PHASE5_POOL_IDLE_TIMEOUT_SECONDS="1", PHASE5_POOL_MAX_LIFETIME_SECONDS="60")
    project.run("up", "-d", "--no-deps", "--force-recreate", "serve")
    wait(lambda: project.admin("/live").returncode == 0)
    before = wire.events()
    client.call("get", id=2119)
    first = wire.observe("tls", before)
    wait(lambda: (first, "close_notify") in wire.events())
    assert (first, "terminate") in wire.events()

    project.env["PHASE5_POOL_MAX_LIFETIME_SECONDS"] = "1"
    project.run("up", "-d", "--no-deps", "--force-recreate", "serve")
    wait(lambda: project.admin("/live").returncode == 0)
    before = wire.events()
    with ThreadPoolExecutor(max_workers=1) as worker:
        with phase5.block_corpus(project):
            request = worker.submit(client.call, "get", id=2119)
            blocked = phase3.wait_for_readers(project.sql, "wait_event_type='Lock'")
            time.sleep(1.15)
            assert pids(project, "wait_event_type='Lock'") == blocked
        request.result(timeout=5)
    phase3.assert_readers_idle(project.sql, gone=blocked, max_connections=0)
    second = wire.observe("close_notify", before)
    assert (second, "terminate") in wire.events()
    print("Idle eviction and leased lifetime expiration both retired with TLS shutdown", flush=True)
    project.env.pop("PHASE5_POOL_IDLE_TIMEOUT_SECONDS")
    project.env.pop("PHASE5_POOL_MAX_LIFETIME_SECONDS")
    project.run("up", "-d", "--no-deps", "--force-recreate", "serve")
    wait(lambda: project.admin("/live").returncode == 0)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", default="rootless")
    parser.add_argument("--image-prefix", required=True)
    parser.add_argument("--baseline-image")
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    assert args.evidence.is_dir() and args.evidence.resolve().is_relative_to("/tmp")
    with tempfile.TemporaryDirectory(prefix="rfc-phase5-pool-", dir="/tmp") as directory:
        work = Path(directory)
        docker = ["docker", "--context", args.context]
        driver = work / "publication-driver"
        phase3.fixture_driver(docker, args.image_prefix, driver)
        project = phase5.Project(work / work.name, driver, args.image_prefix, docker)
        project.env.update(PHASE5_DIR=str(work), PHASE5_SCRIPTS=str(phase5.HERE), PHASE5_PROXY_IP="",
                           PHASE5_DB_PORT="15432", PHASE5_POOL_MIN_IDLE="0", PHASE5_POOL_MAX_CONNECTIONS="3")
        project.key = secrets.token_bytes(48)
        (work / "cursor-key").write_bytes(project.key)
        (work / "cursor-key").chmod(0o444)
        (work / "fixture-policy.json").touch()
        (work / "tls").mkdir()
        phase1.phase0.make_leaf(project.directory / "certs", "localhost", work / "tls")
        wire = None
        try:
            project.start()
            project.migrate()
            project.sql("ALTER SYSTEM SET log_line_prefix TO '%m [%p] %u@%d '")
            project.sql("ALTER SYSTEM SET log_connections TO 'authorization'")
            project.sql("SELECT pg_reload_conf()")
            project.prepare_reviews()
            project.run("up", "-d", "--wait", "fixtures")
            wire = Observer(project, work, args.image_prefix)
            client = project.start_mcp()
            wait(lambda: project.admin("/live").returncode == 0)
            snapshots = json.loads(phase5.TOOL_SCHEMA.read_text())
            client.schemas = {tool["name"]: phase5.Draft202012Validator(tool["outputSchema"])
                              for tool in snapshots}
            qualification(project, client, wire)
            retirement(project, client, wire)
            network_interruption(project, client, wire)
            timings = {"pooled_disconnect_upper_bound_ms": round(disconnect_latency(project), 2)}
            if args.baseline_image:
                project.env["PHASE5_SERVE_IMAGE"] = args.baseline_image
                client = project.start_mcp()
                wait(lambda: project.admin("/live").returncode == 0)
                before = wire.events()
                status, result, _ = client.exchange(*phase5.envelope("tools/call", {
                    "name": "get", "arguments": {"id": 2119},
                }))
                assert status == 200 and not result["result"]["isError"]
                baseline = wire.observe("abrupt", before)
                assert (baseline, "terminate") not in wire.events()
                print("Dedicated baseline closed the TLS socket abruptly after a successful Get", flush=True)
                timings["dedicated_disconnect_upper_bound_ms"] = round(disconnect_latency(project), 2)
            (args.evidence / f"pool-cancellation-{args.context}.json").write_text(json.dumps(timings) + "\n")
            print(timings, flush=True)
        finally:
            if wire is not None:
                (args.evidence / f"pool-wire-{args.context}.log").write_text(wire.logs())
                (args.evidence / f"pool-postgres-{args.context}.log").write_text(
                    project.capture("logs", "--no-color", "postgres") + "\n")
                wire.close()
            project.close()


if __name__ == "__main__":
    main()
