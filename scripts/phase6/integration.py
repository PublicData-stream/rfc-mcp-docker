#!/usr/bin/env python3
"""Exercise Phase 6 wire behavior and crash recovery using disposable Compose."""

import argparse
import concurrent.futures
import importlib.util
from ipaddress import ip_network
import json
from pathlib import Path
from random import SystemRandom
import secrets
import select
import subprocess
import sys
import time

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

    def publication(self):
        return int(self.sql("SELECT publication_sequence FROM rfc.active_generation WHERE singleton"))

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


def wire_checks(project, epoch, completed_event):
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
    replay, _ = trigger(project, epoch, completed_event)
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as workers:
        statuses = sorted(workers.map(lambda _: project.send(replay)[0], range(4)))
    assert statuses == [200, 409, 409, 409], statuses


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


def main():
    if not __debug__:
        raise RuntimeError("Phase 6 requires assertions")
    parser = argparse.ArgumentParser()
    parser.add_argument("--context", default="rootless")
    parser.add_argument("--image-prefix", default="rfc-phase6-final")
    parser.add_argument("--bulk-bootstrap", action="store_true", help="run the pinned schema 10 bootstrap fixture")
    args = parser.parse_args()
    options = ["--context", args.context, "--image-prefix", args.image_prefix]
    if args.bulk_bootstrap:
        spec = importlib.util.spec_from_file_location("e2e", HERE.parent / "ci/docker-e2e.py")
        e2e = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(e2e)
        with e2e.historical_checkout() as checkout:
            historical = checkout / "scripts/phase6/integration.py"
            subprocess.run([sys.executable, "-B", str(historical), *options, "--bulk-bootstrap"], check=True)
    else:
        scheduler = HERE.parent / "https-discovery/integration.py"
        for profile in ("--cold-daemon-only", "--scheduler-only"):
            subprocess.run([sys.executable, "-B", str(scheduler), *options, profile], check=True)


if __name__ == "__main__":
    main()
