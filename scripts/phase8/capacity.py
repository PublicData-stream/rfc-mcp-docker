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

from integration import arguments, dump, phase3, phase5, phase7, project, SERVE
from load_client import exchange, MIX, SEARCH

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("phase4", HERE.parent / "phase4/integration.py")
phase4 = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(phase4)
CONCURRENCY = (1, 4, 16, 32)
SECONDS = 15


class Corpus(phase5.Project):
    search_command = phase4.Project.search_command


def statistics(records, duration, workload=MIX):
    by_tool = {}
    for name, _ in workload:
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


def sample(stack, *, include_companions=True):
    serve_memory, descriptors, workers = map(int, stack.capture(
        "exec", "-T", SERVE, "sh", "-ec",
        'cat /sys/fs/cgroup/memory.current; test -r /proc/1/fd; '
        'set -- /proc/1/fd/[0-9]*; printf "%s\\n" "$#"; '
        "ps -o comm | grep -c '^rg$' || true",
    ).split())
    memory = {}
    services = (phase7.SYNC, phase7.FEED, "postgres") if include_companions else ("postgres",)
    for service in services:
        memory[service] = int(stack.capture("exec", "-T", service, "cat",
                                            "/sys/fs/cgroup/memory.current"))
    in_flight = next(int(line.rsplit(" ", 1)[1]) for line in stack.admin("/metrics").splitlines()
                     if line.startswith("rfc_mcp_in_flight "))
    readers, pending, running = map(int, stack.sql("-c", """
        SELECT (SELECT count(*) FROM pg_stat_activity
                WHERE usename='rfc_serve' AND backend_type='client backend'),
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
                measurements.append(sample(stack, include_companions=mode not in {"comparison", "search"}))
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
    assert all(result.get(code) for code in ("HTTP 429", "deadline_exceeded", "resource_limit_exceeded")), result
    return {"outcomes": result, "max_in_flight": max(row["in_flight"] for row in measurements),
            "after": recovery(stack)}


def recovery(stack, *, include_companions=True):
    time.sleep(2.1)
    assert stack.client.rpc("tools/list")[0]["tools"]
    current = sample(stack, include_companions=include_companions)
    assert current["rg_workers"] == current["in_flight"] == 0, current
    phase3.assert_readers_idle(lambda query: stack.sql("-c", query))
    return current


def cpu_sample(stack):
    return {service: int(stack.capture("exec", "-T", service, "sh", "-ec",
                                      "sed -n 's/^usage_usec //p' /sys/fs/cgroup/cpu.stat"))
            for service in (SERVE, "postgres")}


def pool_metrics(stack):
    return {name: float(value) for name, value in
            (line.split() for line in stack.admin("/metrics").splitlines())
            if name.startswith("rfc_mcp_db_pool_")}


def postgres_logs(stack):
    return stack.capture("logs", "--no-color", "postgres").splitlines()


def connection_counts(lines):
    authorized = [line for line in lines if "connection authorized:" in line and "user=rfc_serve " in line]
    diagnostics = [line for line in lines if any(error in line.lower()
                   for error in ("unexpected eof", "reset by peer", "could not receive data from client"))]
    # PostgreSQL includes user and PID in the prefix we configure for this fixture.
    return {
        "connections": len(authorized),
        "tls_handshakes": sum("ssl enabled" in line.lower() for line in authorized),
        "serve_disconnect_diagnostics": sum("rfc_serve@" in line for line in diagnostics),
        "other_disconnect_diagnostics": sum("rfc_serve@" not in line for line in diagnostics),
    }


def compare(stack, work, args):
    candidate = stack.env["RFC_SERVE_IMAGE"]
    for statement in (
        "ALTER SYSTEM SET log_connections TO 'authorization'",
        "ALTER SYSTEM SET log_line_prefix TO '%m [%p] %u@%d '",
        "SELECT pg_reload_conf()",
    ):
        stack.sql("-c", statement)
    assert stack.sql("-c", "SHOW log_connections") == "authorization"
    stack.run("stop", phase7.SYNC, timeout=50)
    trials = []
    try:
        for trial in range(args.pool_trials):
            variants = [("dedicated", args.baseline_serve_image), ("pooled", candidate)]
            if trial % 2:
                variants.reverse()
            for variant, image in variants:
                stack.env["RFC_SERVE_IMAGE"] = image
                start_lines = len(postgres_logs(stack))
                stack.run("up", "-d", "--no-deps", "--force-recreate", "--wait", SERVE, timeout=80)
                assert stack.admin("/ready") == ""
                row = {"trial": trial + 1, "variant": variant,
                       "image": stack.inspect(SERVE)["Image"], "profiles": {}}
                for count in CONCURRENCY:
                    old_cpu = cpu_sample(stack)
                    old_metrics = pool_metrics(stack)
                    old_log = len(postgres_logs(stack))
                    records, samples = clients(stack, work, count, "comparison")
                    new_log = postgres_logs(stack)[old_log:]
                    new_cpu = cpu_sample(stack)
                    new_metrics = pool_metrics(stack)
                    successful = sum(record[1] == "ok" for record in records)
                    assert successful, (variant, count, records[:10])
                    if variant == "pooled":
                        assert all(sample["db_readers"] <= 8 for sample in samples), samples
                    connections = connection_counts(new_log)
                    if connections["connections"]:
                        assert connections["tls_handshakes"] == connections["connections"], connections
                    waits = {key: new_metrics[key] - old_metrics.get(key, 0) for key in new_metrics
                             if "wait_seconds_" in key}
                    row["profiles"][count] = {
                        "tools": statistics(records, SECONDS), **connections,
                        "connections_per_request": connections["connections"] / len(records),
                        "cpu_us": {name: new_cpu[name] - old_cpu[name] for name in old_cpu},
                        "peak_memory_bytes": {key: max(sample[key] for sample in samples) for key in
                                              ("serve_cgroup_bytes", "postgres_cgroup_bytes")},
                        "pool_wait": waits if variant == "pooled" else None,
                    }
                stack.run("stop", SERVE, timeout=35)
                row["combined_postgres_logs"] = connection_counts(postgres_logs(stack)[start_lines:])
                trials.append(row)
                print(json.dumps(row), flush=True)
    finally:
        stack.env["RFC_SERVE_IMAGE"] = candidate
    for trial in range(1, args.pool_trials + 1):
        dedicated, pooled = [next(row for row in trials if row["trial"] == trial and row["variant"] == variant)
                             for variant in ("dedicated", "pooled")]
        for count in CONCURRENCY:
            assert (pooled["profiles"][count]["connections_per_request"] <
                    dedicated["profiles"][count]["connections_per_request"]), (trial, count)
        assert pooled["combined_postgres_logs"]["serve_disconnect_diagnostics"] == 0, pooled
    return {"sample_seconds": SECONDS, "profiles": trials}


def search_capacity(stack, work, args):
    candidate = stack.env["RFC_SERVE_IMAGE"]
    stack.run("stop", phase7.SYNC, timeout=50)

    def start_serve():
        stack.run("up", "-d", "--no-deps", "--force-recreate", "--wait", SERVE, timeout=80)
        assert stack.admin("/ready") == ""

    def spill():
        values = stack.sql("-c", "SELECT temp_files,temp_bytes FROM pg_stat_database WHERE datname='rfc'")
        return dict(zip(("temp_files", "temp_bytes"), map(int, values.split("|")), strict=True))

    def search(arguments):
        started = time.monotonic()
        result, _ = stack.client.rpc("tools/call", name="search", arguments=arguments)
        assert not result["isError"], result
        return {"latency_ms": (time.monotonic() - started) * 1000, "hits": len(result["structuredContent"]["hits"]),
                "more": bool(result["structuredContent"]["next_cursor"])}

    profiles = {}
    for label, image in (("previous", args.search_baseline_image), ("candidate", candidate)):
        if not image:
            continue
        stack.env["RFC_SERVE_IMAGE"] = image
        stack.run("stop", SERVE, timeout=35)
        stack.run("restart", "postgres", timeout=70)
        stack.run("up", "-d", "--no-deps", "--wait", "postgres", timeout=70)
        start_serve()
        row = {"image": stack.inspect(SERVE)["Image"]}
        row["cold_first_request"] = search(SEARCH[1][1])
        row["warm"] = {}
        for count in (1, 16):
            old_cpu, old_pool, old_spill = cpu_sample(stack), pool_metrics(stack), spill()
            records, samples = clients(stack, work, count, "search")
            new_cpu, new_pool, new_spill = cpu_sample(stack), pool_metrics(stack), spill()
            assert any(record[1] == "ok" for record in records)
            row["warm"][count] = {"queries": statistics(records, SECONDS, SEARCH),
                                  "cpu_us": {name: new_cpu[name] - old_cpu[name] for name in old_cpu},
                                  "spill": {name: new_spill[name] - old_spill[name] for name in old_spill},
                                  "pool_wait": {name: value - old_pool.get(name, 0)
                                                for name, value in new_pool.items() if "wait_seconds_" in name},
                                  "peak_memory_bytes": {key: max(sample[key] for sample in samples) for key in
                                                        ("serve_cgroup_bytes", "postgres_cgroup_bytes")},
                                  "after": recovery(stack, include_companions=False)}
        stack.sql("-c", "ALTER ROLE rfc_serve IN DATABASE rfc SET log_statement='all'")
        stack.sql("-c", "ALTER ROLE rfc_serve IN DATABASE rfc SET log_parameter_max_length=0")
        try:
            start_serve()
            row["warm_statement_counts"] = {}
            search(SEARCH[1][1])
            for name, arguments in SEARCH:
                start = len(postgres_logs(stack))
                search(arguments)
                row["warm_statement_counts"][name] = sum(
                    "statement:" in line or "execute " in line
                    for line in postgres_logs(stack)[start:]
                )
        finally:
            stack.sql("-c", "ALTER ROLE rfc_serve IN DATABASE rfc RESET log_statement")
            stack.sql("-c", "ALTER ROLE rfc_serve IN DATABASE rfc RESET log_parameter_max_length")
        profiles[label] = row
        print(json.dumps({label: row}), flush=True)
    stack.env["RFC_SERVE_IMAGE"] = candidate
    return profiles


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
        with phase7.deployment(work, seed, args.image_prefix) as stack:
            stack.start()
            stack.sql("-c", "ANALYZE")
            if args.native_search_only:
                return {"rfc_count": size, "search": search_capacity(stack, work, args), "query_plans": plans}
            if args.baseline_serve_image:
                return {"rfc_count": size, **compare(stack, work, args)}
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
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-serve-image", help="compare an existing dedicated-session Serve image")
    parser.add_argument("--pool-trials", type=int, default=3)
    parser.add_argument("--native-search-only", action="store_true", help="measure native and hybrid search")
    parser.add_argument("--search-baseline-image", help="pinned pre-change Serve image for the search comparison")
    args = arguments(parser)
    if args.search_baseline_image and not args.native_search_only:
        parser.error("--search-baseline-image requires --native-search-only")
    if args.native_search_only and args.baseline_serve_image:
        parser.error("--native-search-only and --baseline-serve-image select different benchmarks")
    if not 1 <= args.pool_trials <= 10:
        parser.error("--pool-trials must be within 1–10")
    with tempfile.TemporaryDirectory(prefix="rfc-phase8-capacity-", dir="/tmp") as directory:
        root = Path(directory)
        publication, search = root / "publication-driver", root / "search-driver"
        docker = ["docker", "--context", args.context]
        phase3.fixture_driver(docker, args.image_prefix, publication)
        phase4.search_driver(docker, args.image_prefix, search, False)
        sizes = (1000,) if args.baseline_serve_image else (1000, 10000)
        reports = [run_profile(root, args, publication, search, size) for size in sizes]
    filename = ("search-capacity.json" if args.native_search_only
                else "pool-comparison.json" if args.baseline_serve_image else "capacity.json")
    (args.evidence / filename).write_text(json.dumps({
        "warmup_seconds": 0 if args.baseline_serve_image else 3, "sample_seconds": SECONDS, "reports": reports,
    }, indent=2) + "\n")


if __name__ == "__main__":
    main()
