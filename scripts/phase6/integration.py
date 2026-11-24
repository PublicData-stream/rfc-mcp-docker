#!/usr/bin/env python3
"""Exercise Phase 6 wire behavior and crash recovery using disposable Compose."""

import argparse
import concurrent.futures
import hashlib
import importlib.util
from ipaddress import ip_network
import json
from pathlib import Path
from random import SystemRandom
import secrets
import select
import subprocess
import sys
import tempfile
import time
import uuid

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("phase5", HERE.parent / "phase5" / "integration.py")
phase5 = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(phase5)
phase1, phase2, phase3, run = phase5.phase1, phase5.phase2, phase5.phase3, phase5.run
sys.path.insert(0, str(HERE))
from probe import FEED, SYNC, SERVE, mac, response_mac, token


class Project(phase5.Project):
    def command(self, *args):
        command = super().command(*args)
        index = command.index("-p")
        files = ["-f", str(HERE / "compose.yml")]
        if hasattr(self, "ingestion_overlay"):
            files.extend(("-f", str(self.ingestion_overlay)))
        if hasattr(self, "rotation"):
            files.extend(("-f", str(self.rotation)))
        return [*command[:index], *files, *command[index:]]

    def prepare_phase6(self, work):
        self.prepare_network(work)
        self.prepare_proxy_tls(work)
        self.prepare_control_secrets(work)

    def prepare_network(self, work):
        ids = phase1.capture([*self.docker, "network", "ls", "-q"]).splitlines()
        networks = json.loads(phase1.capture([*self.docker, "network", "inspect", *ids]))
        used = [ip_network(config["Subnet"]) for network in networks
                for config in network["IPAM"]["Config"] or [] if config.get("Subnet")]
        for index in SystemRandom().sample(range(256), 256):
            net = f"10.253.{index}"
            if not any(subnet.overlaps(ip_network(f"{net}.0/24")) for subnet in used):
                break
        else:
            raise RuntimeError("no private Phase 6 fixture subnet available")
        self.env.update(
            PHASE5_DIR=str(work), PHASE5_SCRIPTS=str(HERE.parent / "phase5"), PHASE5_PROXY_IP="",
            PHASE6_DIR=str(work), PHASE6_SCRIPTS=str(HERE), PHASE6_NET=net,
            PHASE6_NOTIFY_TARGET=f"{net}.35:8782",
        )
        self.net = net
        (work / "fixture-policy.json").touch()
        (work / "spoof-mode").write_text("identity")

    def prepare_proxy_tls(self, work):
        (work / "tls").mkdir()
        phase1.phase0.make_leaf(self.directory / "certs", "localhost", work / "tls")

    def prepare_control_secrets(self, work):
        self.key = secrets.token_bytes(48)
        self.feed_key = secrets.token_bytes(48)
        self.notify_key = secrets.token_bytes(48)
        keys = {
            "cursor-key": (self.key, 10001),
            "notify-serve": (self.notify_key, 10001),
            "notify-sync": (self.notify_key, 10002),
            "notify-probe": (self.notify_key, 10004),
            "feed-sync": (self.feed_key, 10002),
            "feed-feed": (self.feed_key, 10003),
        }
        for filename, (value, _) in keys.items():
            (work / filename).write_bytes(value)
            (work / filename).chmod(0o400)
        commands = " && ".join(
            f"chown 0:{gid} /keys/{filename} && chmod 0440 /keys/{filename}"
            for filename, (_, gid) in keys.items())
        self.run("run", "--rm", "--no-deps", "-v", f"{work}:/keys", "volume-admin", commands, timeout=90)

    def upgrade_control(self):
        baseline = (phase2.REPO / "migrations" / "migrate.sql").read_text().split(
            "SELECT NOT EXISTS (SELECT FROM rfc.schema_migrations WHERE version = 6)")[0]
        run([*self.psql(), "-f", "/dev/stdin"], env=self.env,
            input=baseline.replace("\\ir ", "\\i /migrations/") + "COMMIT;\n")
        assert self.sql("SELECT max(version) FROM rfc.schema_migrations") == "5"
        self.sql("INSERT INTO rfc.sync_jobs (state, outcome) VALUES ('failed', 'schema-five-fixture')")
        self.migrate()
        self.migrate()
        assert self.sql("SELECT max(version) FROM rfc.schema_migrations") == "9"
        assert self.sql("SELECT count(*) FROM rfc.sync_jobs WHERE outcome = 'schema-five-fixture'") == "1"

    def publication(self):
        return int(self.sql("SELECT publication_sequence FROM rfc.active_generation WHERE singleton"))

    def use_bulk_bootstrap(self, mountpoint):
        _, stage_mount, _, config_mount = phase2.bulk_options(self, mountpoint, bootstrap=True)
        self.ingestion_overlay = self.directory / "ingestion-compose.json"
        self.ingestion_overlay.write_text(json.dumps({"services": {"sync": {
            "environment": {"RFC_SYNC_CONFIG_FILE": "/phase2/bulk.toml"},
            "volumes": [stage_mount, config_mount],
        }}}))

    def restart_sync(self, crash="", **settings):
        self.env.update(PHASE6_CRASH=crash, **settings)
        self.run("up", "-d", "--no-deps", "--force-recreate", "sync")

    def crashed(self, boundary):
        return f"crash at {boundary}" in self.capture("logs", "--no-color", "sync")

    def wait_crash(self, boundary):
        wait(lambda: self.crashed(boundary))

    def send(self, message, to_serve=False):
        address, port = (f"{self.net}.34", "8782") if to_serve else (f"{self.net}.2", "8783")
        command = self.command("exec", "-T", "control-probe", "python3", "-B", "/phase6-tests/probe.py", address, port)
        result = subprocess.run(command, env=self.env, input=message, capture_output=True, timeout=14)
        if result.returncode != 0 or not result.stdout:
            return 0, {}, b""
        head, body = result.stdout.split(b"\r\n\r\n", 1)
        status, *headers = head.split(b"\r\n")
        return int(status.split()[1]), {
            name.lower(): value for name, value in (line.decode().split(": ", 1) for line in headers)
        }, body


def wait(predicate, seconds=40):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.25)
    raise AssertionError("timed out waiting for " + getattr(predicate, "__name__", "fixture condition"))


def request(project, path, epoch="bootstrap", body=b"{}", to_serve=False, **overrides):
    caller, target, key_id, key = ((SYNC, SERVE, "fixture-notify", project.notify_key) if to_serve
                                    else (FEED, SYNC, "fixture-feed", project.feed_key))
    key_id, key = overrides.get("credentials", (key_id, key))
    nonce = token(secrets.token_bytes(32))
    timestamp = str(int(time.time()))
    signature = mac(key, ["RFMCP-HMAC-SHA384-V1", key_id, caller, target, epoch,
                          timestamp, nonce, "POST", path, "application/json", str(len(body))], body)
    headers = {"Host": "control", "Content-Type": "application/json", "Content-Length": str(len(body)),
               "X-RFC-Key-Id": key_id, "X-RFC-Caller": caller, "X-RFC-Audience": target,
               "X-RFC-Epoch": epoch, "X-RFC-Timestamp": timestamp, "X-RFC-Nonce": nonce,
               "X-RFC-Signature": signature}
    headers.update(overrides.get("headers", {}))
    target_path = overrides.get("request_path", path)
    method = overrides.get("method", "POST")
    fields = "".join(f"{name}: {value}\r\n" for name, value in headers.items())
    extra = overrides.get("extra", "")
    start = f"{method} {target_path} HTTP/1.1\r\n{fields}{extra}\r\n"
    return start.encode() + body, (key, key_id, target, caller, nonce)


def identity(project, to_serve=False):
    def attempt():
        wire, expected = request(project, "/internal/v1/identify", to_serve=to_serve)
        status, headers, body = project.send(wire, to_serve)
        if status != 200:
            return None
        assert headers["x-rfc-response-signature"] == response_mac(*expected, body)
        return json.loads(body)["epoch"]
    return wait(attempt)


def event(label):
    digest = hashlib.sha256(label.encode()).digest()
    uid = bytearray(digest[:16])
    uid[6] = (uid[6] & 0x0F) | 0x80
    uid[8] = (uid[8] & 0x3F) | 0x80
    return {"event_id": str(uuid.UUID(bytes=bytes(uid))), "observation_digest": digest.hex(),
            "observed_at": int(time.time()), "reason": "feed_publication"}


def trigger(project, epoch, payload):
    body = json.dumps(payload).encode()
    return request(project, "/internal/v1/sync-requested", epoch, body)


def assert_ack(project, signed, event_id, *, retry_busy=False):
    wire, expected = signed
    status, headers, body = project.send(wire)
    if status == 503 and retry_busy:
        return False
    assert status == 200, (event_id, status)
    assert headers["x-rfc-response-signature"] == response_mac(*expected, body)
    ack = json.loads(body)
    assert ack["event_id"] == event_id
    assert ack["outcome"] == "accepted"
    return True


def assert_queue_busy(project, accept_event):
    with subprocess.Popen(project.psql(), env=project.env, text=True,
                          stdin=subprocess.PIPE, stdout=subprocess.PIPE) as holder:
        try:
            holder.stdin.write("SET statement_timeout = '5s';\n"
                               "SET idle_session_timeout = '25s';\n"
                               "SELECT true FROM pg_advisory_lock(-4::bigint);\n")
            holder.stdin.flush()
            assert select.select([holder.stdout], [], [], 6)[0], "queue lock timed out"
            assert holder.stdout.readline().strip() == "t", "queue lock was not acquired"
            assert not accept_event(), "expected HTTP 503 while the queue is locked"
        finally:
            holder.stdin.close()
            try:
                holder.wait(timeout=6)
            except subprocess.TimeoutExpired:
                holder.kill()
                holder.wait(timeout=6)
                raise
        assert holder.returncode == 0, holder.returncode


def recover_event(project, epoch, payload, *, force_queue_busy):
    def accept_event():
        return assert_ack(project, trigger(project, epoch, payload), payload["event_id"], retry_busy=True)

    if force_queue_busy:
        assert_queue_busy(project, accept_event)
    wait(accept_event, seconds=15)


def feed_data(project, ids):
    root = project.source
    entries = "".join(f"<entry><id>https://www.rfc-editor.org/info/rfc{id}/</id></entry>" for id in ids)
    (root / "rfcatom.xml").write_text(f'<feed xmlns="http://www.w3.org/2005/Atom">{entries}</feed>')
    entries = "".join(f"<item><guid>https://www.rfc-editor.org/info/rfc{id}/</guid></item>" for id in ids)
    (root / "rfcrss.xml").write_text(f"<rss><channel>{entries}</channel></rss>")


def wire_checks(project, epoch):
    path = "/internal/v1/identify"
    for overrides in (
        {"request_path": path + "?q=1"}, {"request_path": "/internal/v1/%69dentify"}, {"method": "GET"},
        {"extra": "Content-Length: 2\r\n"}, {"extra": "X-RFC-Key-Id: fixture-feed\r\n"},
        {"extra": "Transfer-Encoding: chunked\r\n"}, {"extra": "Content-Encoding: gzip\r\n"},
        {"headers": {"Content-Length": "02"}}, {"headers": {"X-RFC-Caller": SERVE}},
        {"headers": {"X-RFC-Audience": SERVE}}, {"headers": {"X-RFC-Signature": "invalid"}},
    ):
        signed, _ = request(project, path, **overrides)
        assert project.send(signed)[0] in (400, 401), overrides.keys()
    assert project.send(request(project, path, epoch=epoch)[0])[0] == 401
    replay, _ = trigger(project, epoch, event("concurrent"))
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as workers:
        statuses = sorted(workers.map(lambda _: project.send(replay)[0], range(4)))
    assert statuses == [200, 409, 409, 409], statuses


def rotate_links(project):
    work = project.directory.parent
    project.rotation = work / "rotation.json"
    services = {role: {"environment": {}, "volumes": []} for role in ("serve", "sync", "feed")}
    groups = {"serve": 10001, "sync": 10002, "feed": 10003}
    path = "/internal/v1/identify"
    overlap = str(int(time.time()) + 600)
    expired = str(int(time.time()) - 1)
    events_sql = "SELECT count(*) FROM rfc.control_events"
    pending_sql = "SELECT count(*) FROM rfc.notification_outbox WHERE delivery_state='pending'"

    def delivered_after(sequence):
        wait(lambda: project.publication() > sequence)
        wait(lambda: project.sql(pending_sql) == "0")

    def save():
        project.rotation.write_text(json.dumps({"services": services}))

    def restart(role):
        save()
        project.run("up", "-d", "--no-deps", "--force-recreate", role)

    def check(active, target, status):
        wire, expected = request(project, path, to_serve=target, credentials=active)
        actual, headers, body = project.send(wire, target)
        if actual != status:
            return False
        if status == 200:
            assert headers["x-rfc-response-signature"] == response_mac(*expected, body)
        return True

    def configure(role, filename, prefix, key_id, key, previous=False):
        new = f"rotated-{filename}-{role}"
        (work / new).write_bytes(key)
        (work / new).chmod(0o400)
        project.run("run", "--rm", "--no-deps", "-v", f"{work}:/keys", "volume-admin",
                    f"chown 0:{groups[role]} /keys/{new} && chmod 0440 /keys/{new}")
        settings = services[role]
        active_path = f"/run/secrets/rfc_{filename}"
        settings["environment"][prefix + "_KEY_ID"] = key_id
        settings["volumes"].append(f"{work / new}:{active_path}:ro")
        if previous:
            old_path = active_path + "_previous"
            settings["environment"].update({
                prefix + "_PREVIOUS_KEY_ID": f"fixture-{filename}",
                prefix + "_PREVIOUS_KEY_FILE": old_path,
                prefix + "_PREVIOUS_KEY_EXPIRES": overlap,
            })
            settings["volumes"].append(f"{work / (filename + '-' + role)}:{old_path}:ro")

    save()
    for filename, sender, receiver, prefix, key, target in (
        ("feed", "feed", "sync", "RFC_FEED_SYNC", project.feed_key, False),
        ("notify", "sync", "serve", "RFC_SYNC_SERVE", project.notify_key, True),
    ):
        old = (f"fixture-{filename}", key)
        new = (f"rotated-{filename}", secrets.token_bytes(48))
        configure(receiver, filename, prefix, *new, previous=True)
        restart(receiver)
        wait(lambda: check(old, target, 200))
        wait(lambda: check(new, target, 200))
        before = project.publication()
        if target:
            restart(sender)
        else:
            events = int(project.sql(events_sql))
            feed_data(project, [2119, 8174, 9110])
            wait(lambda: int(project.sql(events_sql)) == events + 1)
        delivered_after(before)
        configure(sender, filename, prefix, *new)
        before = project.publication()
        restart(sender)
        if not target:
            events = int(project.sql(events_sql))
            feed_data(project, [2119, 8174, 9110, 9000])
            wait(lambda: int(project.sql(events_sql)) == events + 1)
        delivered_after(before)
        services[receiver]["environment"][prefix + "_PREVIOUS_KEY_EXPIRES"] = expired
        restart(receiver)
        wait(lambda: check(new, target, 200))
        assert check(old, target, 401)
        for suffix in ("_PREVIOUS_KEY_ID", "_PREVIOUS_KEY_FILE", "_PREVIOUS_KEY_EXPIRES"):
            del services[receiver]["environment"][prefix + suffix]
        services[receiver]["volumes"] = [
            mount for mount in services[receiver]["volumes"]
            if not mount.endswith(f":/run/secrets/rfc_{filename}_previous:ro")
        ]
        restart(receiver)
        wait(lambda: check(new, target, 200))
        print(f"{filename} receiver-first rotation, signed overlap, sender switch, "
              "outbox delivery and expired-key denial passed", flush=True)


def exercise(project):
    project.start()
    project.upgrade_control()
    phase2.prepare(project)
    feed_data(project, [2119, 8174])
    (project.source / "feed-mode").write_text("absent")
    hold = project.source / "hold-catalog"
    hold.touch()
    project.run("up", "-d", "--wait", "--wait-timeout", "30", "fixtures", "control-probe")
    config = project.directory / "ingestion.toml"
    valid = (phase2.REPO / "deploy/ingestion.toml.example").read_bytes()
    invalid = b"schema_version = 2\n"
    config.write_bytes(invalid)
    container_path = "/etc/rfc-mcp/ingestion.toml"
    project.ingestion_overlay = project.directory / "ingestion-compose.json"
    project.ingestion_overlay.write_text(json.dumps({"services": {"sync": {
        "environment": {"RFC_SYNC_CONFIG_FILE": container_path},
        "volumes": [f"{config}:{container_path}:ro"],
    }}}))
    counts = "SELECT (SELECT count(*) FROM rfc.sync_jobs), (SELECT count(*) FROM rfc.generations)"
    previous = project.sql(counts)
    requests = project.capture("logs", "--no-color", "fixtures")
    for options, args in (((), ()),
                          (("--entrypoint", "/usr/local/bin/rfc-mcp-docker-sync"),
                           ("--phase2-reconcile", "--sample"))):
        phase1.failed(project.app("sync", *options, args=args), project.env,
                      "ingestion schema_version must be 1")
    assert project.sql(counts) == previous
    assert project.capture("logs", "--no-color", "fixtures") == requests
    config.write_bytes(valid)
    project.restart_sync()
    try:
        epoch = identity(project)
        # Later jobs must keep the startup snapshot even if the mounted file changes.
        config.write_bytes(invalid)
        assert project.publication() == 0
        wire_checks(project, epoch)
        payload = event("repeat")
        assert_ack(project, trigger(project, epoch, payload), payload["event_id"])
        payload["observed_at"] -= 1
        assert_ack(project, trigger(project, epoch, payload), payload["event_id"])
        conflicting = {**payload, "observation_digest": payload["observation_digest"][:-2] + "00"}
        if conflicting["observation_digest"] == payload["observation_digest"]:
            conflicting["observation_digest"] = payload["observation_digest"][:-2] + "01"
        assert project.send(trigger(project, epoch, conflicting)[0])[0] == 409
        assert project.sql("SELECT state || ':' || count(*) FROM rfc.sync_jobs WHERE state IN ('pending','running') GROUP BY state ORDER BY state") == "pending:1\nrunning:1"
        print("Sync queue saturation retained one running job and one coalesced pending job", flush=True)
    finally:
        hold.unlink()
    try:
        wait(lambda: project.publication() == 2)
    finally:
        config.write_bytes(valid)
    print("Sync rejected invalid startup before side effects and reused its startup configuration", flush=True)
    wait(lambda: "forged identity" in project.capture("logs", "control-probe"))
    assert "forged acknowledgment" not in project.capture("logs", "control-probe")
    (project.directory.parent / "spoof-mode").write_text("acknowledgment")
    wait(lambda: "forged acknowledgment" in project.capture("logs", "control-probe"))
    assert project.sql("SELECT count(*) FROM rfc.notification_outbox WHERE delivery_state = 'delivered'") == "0"
    assert project.sql("SELECT count(*) FROM rfc.control_events WHERE status = 'completed'") == "2"
    print("coalescing, startup without feed, and spoofed outbox acknowledgment passed", flush=True)

    client = project.start_mcp()
    serve_epoch = identity(project, to_serve=True)
    assert client.rpc("server/discover")[0]["supportedVersions"] == [phase5.VERSION]
    tools = client.rpc("tools/list")[0]["tools"]
    client.schemas = {tool["name"]: phase5.Draft202012Validator(tool["outputSchema"]) for tool in tools}
    current = project.publication()
    assert int(client.call("list")[0]["publication_sequence"]) == current
    hints = [
        (current + 10, 1, 409),
        (current, 0, 409),
        (current - 1, 0, 200),
    ]
    for sequence, generation, expected_status in hints:
        hint = json.dumps({"event_id": str(uuid.uuid4()), "generation_id": generation,
                           "publication_sequence": sequence}).encode()
        signed, _ = request(project, "/internal/v1/generation-committed", serve_epoch, hint, to_serve=True)
        assert project.send(signed, True)[0] == expected_status
    assert project.publication() == current
    project.run("up", "-d", "--no-deps", "--force-recreate", "serve")
    assert identity(project, to_serve=True) != serve_epoch
    active = client.call("list")[0]
    assert int(active["publication_sequence"]) == current, active
    assert project.admin("/ready").returncode == 0
    body, headers = phase5.envelope("tools/list")
    assert client.exchange(body, headers, path="/internal/v1/identify")[0] == 404
    assert client.rpc("tools/list")[0]["tools"]
    ingress = f"{project.net}.34"
    deny = "import socket, sys; s=socket.socket(); s.settimeout(1); sys.exit(1 if s.connect_ex((sys.argv[1],8782)) == 0 else 0)"
    project.run("exec", "-T", "proxy", "python3", "-c", deny, ingress)
    project.denied_sql("UPDATE rfc.notification_outbox SET attempts=1", phase1.SERVE)
    project.denied_sql("UPDATE rfc.notification_outbox SET generation_id=1", phase1.SYNC)
    project.run("exec", "-T", "serve", "sh", "-ec",
                "test -r /mirror/blobs/blake3; ! touch /mirror/denied; test ! -r /mirror/staging; "
                "test -r /run/secrets/rfc_notify; test ! -e /run/secrets/rfc_feed")
    project.run("run", "--rm", "--no-deps", "--entrypoint", "sh", "feed", "-ec",
                "test -r /run/secrets/rfc_feed; test ! -e /run/secrets/rfc_notify; "
                "test ! -e /mirror; test ! -e /phase1/db-password")
    for service in ("serve", "sync", "postgres"):
        container = project.capture("ps", "-q", service)
        settings = json.loads(phase1.capture([*project.docker, "inspect", container]))[0]["NetworkSettings"]
        assert not any(settings["Ports"].values())
        if service == "postgres":
            for network in settings["Networks"].values():
                target = network["IPAddress"]
                assert project.capture("run", "--rm", "--no-deps", "--entrypoint", "sh", "feed",
                                       "-c", 'if nc -z -w 2 "$1" 5432 2>/dev/null; then echo reachable; fi', "--", target,
                                       ) != "reachable"

    actual = f"{project.net}.34:8782"
    project.restart_sync(PHASE6_NOTIFY_TARGET=actual, PHASE6_DAILY="20", PHASE6_HOURLY="5")
    wait(lambda: project.publication() >= current + 2, seconds=42)
    wait(lambda: project.sql("SELECT count(*) FROM rfc.notification_outbox WHERE delivery_state = 'pending'") == "0")
    current = project.publication()
    with (project.source / "catalog.xml").open("a") as source:
        source.write("\n")
    wait(lambda: project.publication() > current, seconds=15)
    print("authoritative MCP reads, daily reconciliation, and hourly change detection passed", flush=True)

    for boundary, persisted in (("before_event_commit", False), ("after_event_commit", True)):
        project.restart_sync(boundary, PHASE6_DAILY="3600", PHASE6_HOURLY="3600")
        epoch = identity(project)
        payload = event(boundary)
        def trigger_crash():
            signed = trigger(project, epoch, payload)
            status = project.send(signed[0])[0]
            assert status in (0, 503), (boundary, status)
            return signed if project.crashed(boundary) else None
        signed = wait(trigger_crash)
        stmt = f"SELECT count(*), min(job_id) FROM rfc.control_events WHERE id = '{payload['event_id']}'"
        count, original_job = project.sql(stmt).split("|")
        assert (count, original_job.isdecimal()) == (str(int(persisted)), persisted)
        project.restart_sync()
        new_epoch = identity(project)
        assert new_epoch != epoch
        assert project.send(signed[0])[0] == 409
        recover_event(project, new_epoch, payload, force_queue_busy=persisted)
        count, job = project.sql(stmt).split("|")
        assert count == "1" and job.isdecimal(), (boundary, count, job)
        if persisted:
            assert job == original_job, (job, original_job)
            print("forced queue contention returned HTTP 503 and retry preserved the committed event's job", flush=True)
    wait(lambda: project.sql("SELECT count(*) FROM rfc.sync_jobs WHERE state IN ('pending','running')") == "0")

    for boundary in ("before_publication_commit", "after_publication_commit"):
        previous = project.publication()
        project.restart_sync(boundary)
        project.wait_crash(boundary)
        published = boundary.startswith("after")
        assert project.publication() == previous + int(published)
        expected = "succeeded:published" if published else "running:ready"
        assert project.sql("SELECT j.state || ':' || g.state FROM rfc.sync_jobs j JOIN rfc.generations g ON g.id=j.generation_id ORDER BY j.id DESC LIMIT 1") == expected
        project.restart_sync()
        wait(lambda: project.publication() == previous + 1 + int(published))
    print("event acceptance and publication commit crash recovery passed", flush=True)

    initial = int(project.sql("SELECT count(*) FROM rfc.control_events"))
    project.run("up", "-d", "--no-deps", "feed")
    for scenario in ("malformed", "oversized", "rate-limited"):
        (project.source / "feed-mode").write_text(scenario)
        wait(lambda: f"feed scenario={scenario} at=" in project.capture("logs", "fixtures"))
        assert project.sql("SELECT count(*) FROM rfc.control_events") == str(initial)
    limit = "feed scenario=rate-limited at="
    def rate_timestamps():
        lines = project.capture("logs", "fixtures").splitlines()
        return [float(line.rsplit(limit, 1)[1]) for line in lines if limit in line]
    wait(lambda: len(rate_timestamps()) >= 2)
    timestamps = rate_timestamps()
    assert timestamps[1] - timestamps[0] >= 4
    (project.source / "feed-mode").write_text("ok")
    wait(lambda: int(project.sql("SELECT count(*) FROM rfc.control_events")) == initial + 2)
    wait(lambda: '"GET /rfcatom.xml HTTP/1.1" 304' in project.capture("logs", "fixtures"))
    feed_data(project, [8174, 2119, 8174])
    time.sleep(4)
    assert int(project.sql("SELECT count(*) FROM rfc.control_events")) == initial + 2
    project.env["PHASE6_FORMAT"] = "rss"
    project.run("up", "-d", "--no-deps", "--force-recreate", "feed")
    wait(lambda: '"GET /rfcrss.xml HTTP/1.1" 304' in project.capture("logs", "fixtures"))
    assert int(project.sql("SELECT count(*) FROM rfc.control_events")) == initial + 2
    print("conditional Atom and RSS polling, malformed and oversized feeds, and duplicate suppression passed", flush=True)
    rotate_links(project)


def fixture_binaries(docker, prefix, work):
    image = f"{prefix}-control-fixture"
    phase1.phase0.prepare_fixture_image(docker, phase2.REPO, "phase6-fixture", image)
    container = phase1.capture([*docker, "create", *phase1.phase0.local_image_args(), image,
                                "/rfc-phase6-sync"])
    try:
        for service in ("sync", "feed"):
            run([*docker, "cp", f"{container}:/rfc-phase6-{service}", str(work / service)])
            (work / service).chmod(0o755)
    finally:
        run([*docker, "rm", container])


def exercise_bulk_bootstrap(project):
    sources = phase2.prepare(project, include_nul=True)
    project.start()
    project.migrate()
    project.run("up", "-d", "--wait", "fixtures")
    with phase2.capped_stage(project) as mountpoint:
        project.use_bulk_bootstrap(mountpoint)
        try:
            client = project.start_mcp()
            project.restart_sync(PHASE6_NOTIFY_TARGET=f"{project.net}.34:8782")
            wait(lambda: project.publication() == 1)
            wait(lambda: project.admin("/ready").returncode == 0)
            wait(lambda: project.sql(
                "SELECT count(*) FROM rfc.notification_outbox WHERE delivery_state = 'delivered'"
            ) == "1")
            tools = client.rpc("tools/list")[0]["tools"]
            client.schemas = {tool["name"]: phase5.Draft202012Validator(tool["outputSchema"]) for tool in tools}
            document = client.call("get", id=2119)[0]
            assert document["publication_sequence"] == "1"
            assert phase5.bytes_from(document["original"]) == sources[2119]
            assert project.sql(
                "SELECT count(*) FROM rfc.generation_documents d "
                "JOIN rfc.active_generation a ON a.generation_id = d.generation_id "
                "JOIN rfc.source_observations o ON o.identity = d.txt_observation "
                "WHERE o.source_transport = 'rsync' AND o.https_verification = 'unverified'"
            ) == str(len(sources))
            job_id = project.sql("SELECT id FROM rfc.sync_jobs WHERE outcome = 'published'")
            generation = document["generation_id"]
            entries = [json.loads(line) for line in
                       project.capture("logs", "--no-color", "--no-log-prefix", "sync").splitlines()
                       if line.startswith("{")]
            ready, = [entry for entry in entries if entry.get("state") == "ready"]
            published, = [entry for entry in entries if entry.get("event") == "publication"]
            assert ready["job_id"] == published["job_id"] == job_id
            assert ready["generation_id"] == published["generation_id"] == generation
            assert published["state"] == "published" and published["publication_sequence"] == "1"
            assert published["expected_publication_sequence"] == "0"
            assert ready["metrics"]["progress"] == {
                "selected": len(sources), "txt_completed": len(sources), "xml_completed": len(sources),
                "txt_remaining": 0, "xml_remaining": 0, "admitted": 0, "xml_reserved": 0,
            }
            assert all("/rfc/rfc" not in line or ".txt" not in line
                       for line in project.capture("logs", "--no-color", "fixtures").splitlines())
            print("Bulk daemon bootstrap published and delivered; /ready and MCP original bytes passed", flush=True)
        except Exception:
            project.run("logs", "--no-color", "--tail", "25", "sync")
            raise
        finally:
            project.run("rm", "--stop", "--force", "sync")


def main():
    if not __debug__:
        raise RuntimeError("Phase 6 requires assertions")
    parser = argparse.ArgumentParser()
    parser.add_argument("--context", default="rootless")
    parser.add_argument("--image-prefix", default="rfc-phase6-final")
    parser.add_argument("--bulk-bootstrap", action="store_true")
    args = parser.parse_args()
    docker = ["docker", "--context", args.context]
    with tempfile.TemporaryDirectory(prefix="rfc-phase6-", dir="/tmp") as directory:
        work = Path(directory)
        driver = work / "publication-driver"
        phase3.fixture_driver(docker, args.image_prefix, driver)
        fixture_binaries(docker, args.image_prefix, work)
        project = Project(work / work.name, driver, args.image_prefix, docker)
        try:
            project.prepare_phase6(work)
            if args.bulk_bootstrap:
                exercise_bulk_bootstrap(project)
            else:
                exercise(project)
        except Exception:
            project.run("logs", "--no-color", "--tail", "25")
            raise
        finally:
            project.close()


if __name__ == "__main__":
    main()
