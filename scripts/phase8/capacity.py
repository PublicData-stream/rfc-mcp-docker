#!/usr/bin/env python3
"""Measure synthetic corpus serving through the hardened Compose profile."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import json
import math
from pathlib import Path
import select
import ssl
import subprocess
import tempfile
import time

from integration import arguments, deployment, dump, phase3, phase5, phase7, project, SERVE
from load_client import exchange, MIX

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("phase4", HERE.parent / "phase4/integration.py")
phase4 = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(phase4)
CONCURRENCY = (1, 4, 16, 32)
SECONDS = 15


class Corpus(phase5.Project):
    search_command = phase4.Project.search_command


def statistics(records, duration):
    by_tool = {}
    for name, _ in MIX:
        requests = [record for record in records if record[0] == name]
        timings = sorted(row[2] * 1000 for row in requests if row[1] == "ok")
        counts = dict(Counter(row[1] for row in requests))
        percentiles = {f"p{p}": round(timings[math.ceil(len(timings) * p / 100) - 1], 2)
                       for p in (50, 95, 99)} if timings else {}
        by_tool[name] = {"outcomes": counts, "ok_latency_ms": percentiles,
                         "ok_per_second": round(len(timings) / duration, 2)}
        if name == "rg":
            scan_bytes = sum(row[3] for row in requests if row[1] == "ok")
            by_tool[name]["charged_bytes_per_second"] = round(scan_bytes / duration)
    return by_tool


def sample(stack):
    serve_memory, descriptors, workers = map(int, stack.capture(
        "exec", "-T", SERVE, "sh", "-ec",
        'cat /sys/fs/cgroup/memory.current; test -r /proc/1/fd; '
        'set -- /proc/1/fd/[0-9]*; printf "%s\\n" "$#"; '
        "ps -o comm | grep -c '^rg$' || true",
    ).split())
    memory = {}
    for service in (phase7.SYNC, phase7.FEED, "postgres"):
        memory[service] = int(stack.capture("exec", "-T", service, "cat",
                                            "/sys/fs/cgroup/memory.current"))
    in_flight = next(int(line.rsplit(" ", 1)[1]) for line in stack.admin("/metrics").splitlines()
                     if line.startswith("rfc_mcp_in_flight "))
    readers, pending, running = map(int, stack.sql("-c", """
        SELECT (SELECT count(*) FROM pg_stat_activity WHERE usename='rfc_serve'),
               count(*) FILTER (WHERE state='pending'), count(*) FILTER (WHERE state='running')
        FROM rfc.sync_jobs
    """).split("|"))
    return {"serve_cgroup_bytes": serve_memory,
            **{name + "_cgroup_bytes": value for name, value in memory.items()},
            "serve_fds": descriptors, "rg_workers": workers, "in_flight": in_flight,
            "db_readers": readers, "sync_pending": pending, "sync_running": running}


def clients(stack, work, count, mode):
    with tempfile.TemporaryDirectory(prefix="capacity-gate-", dir=work) as gate_dir:
        gate = Path(gate_dir)
        gate.chmod(0o755)
        clients = {}
        measurements = []
        try:
            for index in range(count):
                name = f"{stack.name}-load-{index}"
                command = [*stack.docker, "run", "--rm", "--name", name,
                           "--network", f"{stack.name}_test_edge", "--read-only",
                           "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true",
                           "--pids-limit", "16", "--entrypoint", "python3",
                           "-v", f"{HERE / 'load_client.py'}:/client.py:ro",
                           "-v", f"{stack.ca}:/capacity-ca.crt:ro", "-v", f"{gate}:/capacity-gate:ro",
                           f"{stack.prefix}-fixtures", "-B", "/client.py", str(index), str(SECONDS)]
                clients[name] = subprocess.Popen(command, stdout=subprocess.PIPE,
                                                 stderr=subprocess.PIPE, text=True)
            for client in clients.values():
                assert select.select([client.stdout], [], [], 30)[0], "load worker startup timeout"
                assert client.stdout.readline().strip() == "ready", "load worker startup failed"
            temporary = gate / "start"
            temporary.write_text(mode)
            temporary.replace(gate / "go")
            deadline = time.monotonic() + SECONDS + 23
            while any(client.poll() is None for client in clients.values()):
                assert time.monotonic() < deadline, "load worker completion timeout"
                measurements.append(sample(stack))
                time.sleep(0.35)
            records = []
            for client in clients.values():
                stdout, stderr = client.communicate(timeout=4)
                assert client.returncode == 0, stderr
                records.extend(json.loads(stdout))
            return records, measurements
        finally:
            active = [name for name, client in clients.items() if client.poll() is None]
            if active:
                subprocess.run([*stack.docker, "rm", "-f", *active], capture_output=True, timeout=30)
                for name in active:
                    clients[name].communicate(timeout=8)


def shared_source(stack):
    time.sleep(2.1)
    context = ssl.create_default_context(cafile=str(stack.ca))
    with ThreadPoolExecutor(max_workers=32) as pool:
        records = list(pool.map(lambda _: exchange("localhost", stack.client.port, context, "list", {}),
                                range(32)))
    result = dict(Counter(row[1] for row in records))
    assert result.get("ok") and result.get("HTTP 429"), result
    return result


def admission_burst(stack, work):
    command = stack.command("exec", "-T", "postgres", *phase7.phase1.SQL_OPTIONS,
                            "-U", "postgres", "-d", "rfc")
    with subprocess.Popen(command, env=stack.env, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, text=True) as lock:
        lock.stdin.write("BEGIN; LOCK rfc.generation_documents IN ACCESS EXCLUSIVE MODE; "
                         "SELECT 'locked';\n")
        lock.stdin.flush()
        try:
            assert select.select([lock.stdout], [], [], 10)[0]
            assert lock.stdout.readline().strip() == "locked"
            records, measurements = clients(stack, work, 32, "admission")
        finally:
            _, errors = lock.communicate("ROLLBACK;\n", timeout=10)
            assert lock.returncode == 0, errors
    result = dict(Counter(row[1] for row in records))
    assert result.get("HTTP 429") and result.get("deadline_exceeded"), result
    return {"outcomes": result, "max_in_flight": max(row["in_flight"] for row in measurements),
            "after": recovery(stack)}


def recovery(stack):
    time.sleep(2.1)
    assert stack.client.rpc("tools/list")[0]["tools"]
    current = sample(stack)
    assert current["db_readers"] == current["rg_workers"] == current["in_flight"] == 0, current
    return current


def run_profile(root, args, publication_driver, search_driver, size):
    work = root / f"{root.name}-{size}"
    with project(work, args, publication_driver, fixture_type=Corpus) as seed:
        seed.search_driver = search_driver
        seed.key_file = work / "cursor-key"
        seed.start()
        seed.migrate()
        seed.prepare_reviews()
        seed.run("up", "-d", "--wait", "fixtures", timeout=60)
        base = int(seed.reconcile()["generation_id"])
        seed.activate(base, 0)
        base_size = int(seed.sql(f"SELECT count(*) FROM rfc.generation_documents WHERE generation_id={base}"))
        seed.sql("INSERT INTO rfc.rfc_identities SELECT generate_series(10000,10011) "
                 "ON CONFLICT DO NOTHING")
        start = 20001
        active = phase4.seed(seed, base, 1, start, start + size - base_size - 1, (), {}, bulk=True)
        assert seed.sql(f"SELECT count(*) FROM rfc.generation_documents WHERE generation_id={active}") == str(size)
        seed.sql("ANALYZE rfc.generation_documents; ANALYZE rfc.rfc_relations; ANALYZE rfc.document_lexemes")
        plans = {}
        with phase4.search_session(seed) as explain:
            for label, query in (("title_token", "in::title:'graph node'"),
                                 ("exact_body", MIX[1][1]["query"]),
                                 ("relationship", "is::obsoleted:true")):
                result = explain({"explain": query})
                assert "result" in result, result
                plans[label] = result["result"]
        dump(seed.command(), seed.env, work / "fixture.dump")
        seed.run("stop", "fixtures", "postgres", timeout=50)
        with deployment(work, seed, args) as stack:
            stack.start()
            stack.sql("-c", "ANALYZE")
            baseline = sample(stack)
            profiles = {}
            for count in CONCURRENCY:
                records, samples = clients(stack, work, count, "mixed")
                assert len(records) and any(row[1] == "ok" for row in records)
                if count == 1:
                    assert {row[0] for row in records if row[1] == "ok"} == {name for name, _ in MIX}
                recovered = recovery(stack)
                profiles[count] = {
                    "tools": statistics(records, SECONDS),
                    "peak_sample": {key: max(row[key] for row in samples) for key in baseline},
                    "after": recovered,
                }
                print(json.dumps({"corpus": size, "concurrency": count, **profiles[count]}), flush=True)
            shared = shared_source(stack)
            recovered = recovery(stack)
            admission = admission_burst(stack, work)
    return {"rfc_count": size, "initial": baseline, "profiles": profiles,
            "shared_source_burst": shared, "after_burst": recovered,
            "distinct_source_admission": admission, "query_plans": plans}


def main():
    args = arguments(argparse.ArgumentParser(description=__doc__))
    with tempfile.TemporaryDirectory(prefix="rfc-phase8-capacity-", dir="/tmp") as directory:
        root = Path(directory)
        publication, search = root / "publication-driver", root / "search-driver"
        docker = ["docker", "--context", args.context]
        phase3.fixture_driver(docker, args.image_prefix, publication)
        phase4.search_driver(docker, args.image_prefix, search, False)
        reports = [run_profile(root, args, publication, search, size) for size in (1000, 10000)]
    (args.evidence / "capacity.json").write_text(json.dumps({
        "warmup_seconds": 3, "sample_seconds": SECONDS, "reports": reports,
    }, indent=2) + "\n")


if __name__ == "__main__":
    main()
