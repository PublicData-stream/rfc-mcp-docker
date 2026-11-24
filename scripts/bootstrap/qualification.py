"""Private Phase 8 benchmark profiles, invoked by baseline.py."""

import base64
from collections import Counter, defaultdict
from contextlib import ExitStack
import json
import math
from pathlib import Path
from statistics import median
import subprocess
import time


FULL_SIZE = 9861
TARGET_SECONDS = 900
ACQUISITION_BUDGET_SECONDS = 720
LEG_LIMIT_SECONDS = 3600
ATTEMPT_STATUS = {
    "captured": [200],
    "retried": [503, 200],
    "malformed": [200],
    "not_found": [404],
    "gone": [410],
    "retries_exhausted": [503, 503, 503],
}


def prepare_sources(project, baseline, size, full):
    source = project.source
    if full:
        with baseline.duration(project.setup, "full_fixture_payload"):
            # Keep the base fixture catalog, including the NUL and non-UTF-8 cases.
            sizes_kib = [8] * 12 + [32] * 6 + [128, 256]
            entries = []
            for offset in range(size - 6):
                rfc = 10000 + offset
                entries.append(baseline.phase2.catalog_entry(rfc, f"Benchmark {rfc}"))
                header = f"1. Introduction\n\n   Synthetic RFC {rfc}.\n\n2. References\n\n   [RFC2119] Example\n\n".encode()
                line = f"   Example body for synthetic RFC {rfc}. This text exercises indexing.\n".encode()
                length = sizes_kib[offset % len(sizes_kib)] * 1024 - len(header)
                body = header + (line * (length // len(line) + 1))[:length]
                for directory in ("rsync", "https"):
                    (source / directory / f"rfc{rfc}.txt").write_bytes(body)
                xml = source / "https" / f"rfc{rfc}.xml"
                slot = offset % 1000
                if slot == 335:
                    xml.write_bytes(f'<rfc number="{rfc}"><back>'.encode())
                elif slot < 330 or slot == 336:
                    padding = "x" * (1024 << (2 * (rfc % 4)))
                    xml.write_text(
                        f'<rfc number="{rfc}"><!--{padding}--><back><references>'
                        '<name>Normative References</name><reference><seriesInfo name="RFC" value="2119"/>'
                        '</reference></references></back></rfc>'
                    )
            catalog = source / "catalog.xml"
            catalog.write_text(catalog.read_text().replace("</rfc-index>", "".join(entries) + "</rfc-index>"))
    cases = {}
    for path in (source / "rsync").glob("rfc*.txt"):
        rfc = int(path.stem[3:])
        slot = (rfc - 10000) % 1000 if full and rfc >= 10000 else -1
        if rfc == 101 or 330 <= slot <= 334:
            case = "gone"
        elif slot in (335, 336, 337):
            case = {335: "malformed", 336: "retried", 337: "retries_exhausted"}[slot]
        else:
            case = "captured" if (source / "https" / f"{path.stem}.xml").exists() else "not_found"
        cases[rfc] = case
    assert len(cases) == size
    (source / "phase8-xml.json").write_text(json.dumps(cases, sort_keys=True) + "\n")
    (source / "mode").write_text("phase8-cold" if full else "phase8-paced")
    return cases


def read_exact(client, active, rfc, expected):
    response, _ = client.call("get", id=rfc, format="base64")
    assert response["generation_id"] == str(active["active"]), response
    assert response["publication_sequence"] == str(active["sequence"]), response
    original = response["original"]
    assert (original["start_byte"], original["end_byte"]) == (0, len(expected)), original
    assert base64.b64decode(original["content_base64"], validate=True) == expected, rfc


def verify_documents(project, baseline, active, texts, cases, dual_ids, prior=None):
    generation = int(active["active"])
    old_generation = int(prior["active"]) if prior else 0
    rows = project.sql(f"""
        SELECT json_build_array(d.rfc_id, encode(t.raw_digest,'hex'), t.verification_method,
            v.sealed AND v.txt_digest = t.raw_digest,
            old.txt_observation IS NOT DISTINCT FROM d.txt_observation,
            encode(x.raw_digest,'hex'), x.source_url, x.verification_method, a.state, c.state)
        FROM rfc.generation_documents d
        JOIN rfc.document_versions v ON v.identity = d.document_version
        JOIN rfc.source_observations t ON t.identity = d.txt_observation
        LEFT JOIN rfc.source_observations x ON x.identity = d.rfcxml_observation
        LEFT JOIN rfc.generation_documents old ON old.generation_id={old_generation} AND old.rfc_id=d.rfc_id
        JOIN rfc.relation_coverage a ON (a.generation_id,a.rfc_id,a.family) =
            (d.generation_id,d.rfc_id,'all_references')
        JOIN rfc.relation_coverage c ON (c.generation_id,c.rfc_id,c.family) =
            (d.generation_id,d.rfc_id,'classified_references')
        WHERE d.generation_id={generation} ORDER BY d.rfc_id
    """, baseline.phase1.SERVE)
    documents = [json.loads(line) for line in rows.splitlines()]
    assert [row[0] for row in documents] == sorted(texts)
    for rfc, txt, method, sealed, reused, xml, source, xml_method, all_refs, classified in documents:
        assert sealed and method == ("dual_source" if rfc in dual_ids else "rsync_integrity"), (rfc, method)
        assert reused == (prior is not None and rfc not in dual_ids), (rfc, reused)
        assert project.canonical(txt).read_bytes() == texts[rfc], rfc
        case = cases[rfc]
        if case in ("captured", "retried", "malformed"):
            assert source == f"https://www.rfc-editor.org/rfc/rfc{rfc}.xml" and xml_method == "https_capture", rfc
            assert project.canonical(xml).read_bytes() == (project.source / "https" / f"rfc{rfc}.xml").read_bytes(), rfc
        else:
            assert (xml, source, xml_method) == (None, None, None), rfc
        if case in ("malformed", "retries_exhausted"):
            assert "complete" not in (all_refs, classified), (rfc, all_refs, classified)
        if rfc >= 10000 and case == "retried":
            assert (all_refs, classified) == ("complete", "complete"), (rfc, all_refs, classified)
    verified = json.loads(project.capture("exec", "-T", "serve", "/phase2/fixture-driver", "verify-restore",
                                          timeout=600))
    expected = {str(generation): active["latest_generation"]["manifest"]}
    if prior:
        expected[str(old_generation)] = prior["latest_generation"]["manifest"]
    assert verified["manifests"] == expected, verified
    return verified


def xml_telemetry(path, scenario, cases, report):
    requests = defaultdict(list)
    starts = []
    body_bytes = 0
    for line in path.read_text().splitlines():
        _, separator, payload = line.partition(" phase8_xml ")
        if not separator:
            continue
        event = json.loads(payload)
        if event["scenario"] != scenario:
            continue
        requests[event["rfc"]].append(event)
        starts.append(event["start_ns"])
        body_bytes += event["body_bytes_sent"]
    assert requests.keys() == cases.keys()
    counts = Counter(cases.values())
    expected = {key: counts[key] for key in ("not_found", "gone", "malformed", "retries_exhausted")}
    expected.update(captured=counts["captured"] + counts["retried"], reused=0, terminal_failure=0)
    assert report["rfcxml"] == expected, (report["rfcxml"], expected)
    statuses = Counter()
    for rfc, events in requests.items():
        observed = [event["status"] for event in sorted(events, key=lambda event: event["start_ns"])]
        assert observed == ATTEMPT_STATUS[cases[rfc]], (rfc, observed)
        statuses.update(observed)
        assert all((event["body_bytes_sent"] > 0) == (event["status"] == 200) for event in events), rfc
    assert len(starts) == report["metrics"]["rfcxml"]["calls"]
    return {"http_statuses": statuses, "outcomes": counts, "attempts": len(starts),
            "attempts_per_second": (len(starts) - 1) * 1e9 / (max(starts) - min(starts)),
            "response_body_bytes": body_bytes}


def monitor(project, leg, mark, deadline, sequence, after_job, incumbent):
    while time.monotonic() < deadline:
        active = project.publication_snapshot(sequence, after_job)
        leg["database"] = active
        job = active["job"] or {}
        assert not job.get("retries") and job.get("generation_state") != "failed", job
        if job.get("generation_state") == "building" and "building" not in leg["milestones"]:
            mark("building")
            if incumbent:
                assert active["sequence"] == sequence - 1, active
                incumbent(active)
                mark("incumbent_read")
        if active["sequence"] == sequence and job.get("generation_state") == "published":
            assert active["active"] == job["generation"] and job["scope"] == "full", active
            assert job["expected_sequence"] == sequence - 1, job
            if incumbent:
                assert "incumbent_read" in leg["milestones"]
            mark("published_observed")
            return active
        time.sleep(1)
    raise AssertionError(f"timed out waiting for publication sequence {sequence}")


def measure(project, baseline, receipt, evidence, texts, cases, *, bulk, full, prior=None, changed=()):
    incremental = prior is not None
    sequence = 2 if incremental else 1
    after_job = prior["job"]["id"] if incremental else 0
    evidence.mkdir()
    scenario = (project.source / "mode").read_text()
    leg = receipt["incremental" if incremental else "cold"] = {
        "milestones": {}, "workflow_seconds": {}, "t0_utc": baseline.utc_now(),
    }
    stages = leg["workflow_seconds"]
    disk = leg["disk"] = {"peak_allocated_bytes": {}, "sampling_errors": {}, "samples": Counter()}
    if incremental:
        epoch = baseline.phase6.identity(project)
        event = baseline.phase6.event("phase8-incremental")
        leg["event_id"] = event["event_id"]
        signed = baseline.phase6.trigger(project, epoch, event)
        before = project.resources("bulk-scale")
        leg["t0_utc"] = baseline.utc_now()
    t0 = time.monotonic()
    deadline = t0 + (LEG_LIMIT_SECONDS if full else 600)

    def mark(name):
        leg["milestones"][name] = {"utc": baseline.utc_now(), "seconds": time.monotonic() - t0}

    if not incremental:
        project.phase8_client, leg["effective_config"] = baseline.initialize_stack(
            project, stages, mark, preflight=True,
        )
    client = project.phase8_client

    def read_incumbent(snapshot):
        assert snapshot["active"] == prior["active"] and project.admin("/ready").returncode == 0
        read_exact(client, snapshot, 2119, texts[2119])

    with baseline.DiskSamples(project, disk, "bulk-scale" if bulk else "baseline"):
        if incremental:
            with baseline.duration(stages, "signed_sync_request"):
                baseline.phase6.assert_ack(project, signed, event["event_id"])
        else:
            with baseline.duration(stages, "sync_start"):
                project.run("up", "-d", "--no-deps", "sync", timeout=45)
        active = monitor(project, leg, mark, deadline, sequence, after_job,
                         read_incumbent if incremental else None)
        baseline.phase6.wait(lambda: project.admin("/ready").returncode == 0)
        mark("ready_observed")
        rfc = min(changed) if incremental else 2119
        read_exact(client, active, rfc, texts[rfc])
        mark("t1_mcp_read")
        assert time.monotonic() < deadline, "publication and MCP read exceeded the trial limit"

        def delivered():
            state = project.publication_snapshot(sequence, after_job)
            leg["database"] = state
            return (state["outbox"] or {}).get("state") == "delivered" and state["job"]["state"] == "succeeded"

        baseline.phase6.wait(delivered, seconds=35)
        mark("notification_observed")

    active = leg["database"]
    assert active["job_count"] == active["outbox_count"] == sequence, active
    assert active["outbox"]["generation"] == active["active"] and active["job"]["outcome"] == "published", active
    if incremental:
        job_id = project.sql(f"SELECT job_id FROM rfc.control_events WHERE id='{event['event_id']}'")
        assert int(job_id) == active["job"]["id"], active
    leg["cgroups"] = project.resources("bulk-scale")
    if incremental:
        for service, counters in leg["cgroups"].items():
            counters["leg_cpu_us"] = counters["cpu_us"] - before[service]["cpu_us"]
    assert not disk["sampling_errors"], disk

    reports, events, _ = baseline.collect_logs(project, evidence)
    report, = [report for report in reports if int(report.get("job_id") or 0) == active["job"]["id"]]
    publication, = [event for event in events if event.get("event") == "publication"
                    and event.get("job_id") == report["job_id"]]
    assert publication["state"] == "published" and int(publication["publication_sequence"]) == sequence, publication
    leg["collector"] = report
    leg["publication"] = publication
    leg["rfcxml"] = xml_telemetry(evidence / "fixtures.log", scenario, cases, report)
    size = len(texts)
    dual_ids = set(changed) if bulk else set(texts)
    metrics = report["metrics"]
    assert report["state"] == "ready" and int(report["generation_id"]) == active["active"], report
    assert report["manifest_digest"] == active["latest_generation"]["manifest"], active
    assert report["expected_txt"] == metrics["progress"]["txt_completed"] == metrics["progress"]["xml_completed"] == size
    assert report["verified_txt"] == len(dual_ids) and report["rsync_only_txt"] == size - len(dual_ids)
    assert report["reused_txt"] == (size - len(changed) if incremental else 0), report
    assert metrics["txt"]["calls"] == len(dual_ids)
    assert metrics["https_requests"] == len(dual_ids) + leg["rfcxml"]["attempts"] + 2
    assert 1 <= metrics["https_peak"] <= 2 and 1 <= metrics["admitted_peak"] <= 3
    assert 1 <= metrics["xml_window_peak"] <= 3 and metrics["parse_peak"] == 1
    assert metrics["raw_reserved_peak"] == metrics["xml_window_peak"] * 16 * 1024**2
    assert 0 < metrics["memory_peak"] <= 2 * 1024**3
    assert metrics["peak_rss_bytes"] > 0
    if not full:
        assert metrics["minimum_start_spacing_us"] >= 1_000_000
    if bulk:
        transferred = changed if incremental else texts.keys()
        assert metrics["rsync"]["calls"] == 1 and metrics["rsync_transferred_files"] == len(transferred)
        assert metrics["rsync_checksum_hits"] == size - len(transferred)
        assert metrics["rsync_transferred_payload_bytes"] == sum(len(texts[rfc]) for rfc in transferred)
        assert disk["peak_allocated_bytes"]["bulk_stage"] <= receipt["stage_limit_bytes"]
    with baseline.duration(stages, "read_only_storage_verification"):
        leg["storage_verification"] = verify_documents(project, baseline, active, texts, cases, dual_ids, prior)
    elapsed = leg["milestones"]["t1_mcp_read"]["seconds"]
    leg["rfc_per_second_at_t1"] = size / elapsed
    leg["fixture_900_second_target_met"] = elapsed <= TARGET_SECONDS
    return active


def rehearse_rollback(project, baseline, receipt, evidence, active, texts, cases):
    evidence.mkdir()
    record = receipt["rollback"] = {}
    project.run("stop", "sync", timeout=40)
    client = project.phase8_client
    read_exact(client, active, 2119, texts[2119])
    generation = int(active["active"])
    historical_proofs = f"""
        SELECT encode(txt_observation,'hex') FROM rfc.generation_documents
        WHERE generation_id={generation} ORDER BY rfc_id
    """
    previous = project.sql(historical_proofs, baseline.phase1.SERVE)
    config = project.directory / "bulk.toml"
    config.write_text("""schema_version = 1
[ingestion]
txt_provenance = "dual_source"
[ingestion.https]
max_in_flight_requests = 1
[ingestion.rsync]
mode = "batched"
[ingestion.pipeline]
max_pending_documents = 1
parser_workers = 1
""")
    # Keep the existing fixture mount for configuration and drop the bulk staging bind.
    overlay = json.loads(project.ingestion_overlay.read_text())
    sync = overlay["services"]["sync"]
    sync["volumes"] = [mount for mount in sync["volumes"] if not mount.endswith(":/rfc-rsync-txt")]
    project.ingestion_overlay.write_text(json.dumps(overlay))
    effective = baseline.phase1.capture(project.app("sync", "-T", args=("--check-config",)), env=project.env)
    record["effective_config"] = json.loads(effective)
    https = project.source / "https" / "rfc100.txt"
    https.write_bytes(texts[100] + b"\nFixture disagreement during strict rollback.\n")
    project.restart_sync()

    def snapshot():
        return project.publication_snapshot(2, active["job"]["id"])

    def failed_attempt():
        state = snapshot()
        job = state["job"] or {}
        return state if job.get("generation_state") == "failed" and job["retries"] == 1 else None

    failed = record["failed_attempt"] = baseline.phase6.wait(failed_attempt, seconds=90)
    assert failed["sequence"] == 1 and failed["active"] == active["active"], failed
    assert failed["outbox_count"] == 1 and failed["outbox"] is None, failed
    assert project.admin("/ready").returncode == 0
    read_exact(client, active, 2119, texts[2119])
    read_exact(client, active, 100, texts[100])
    https.write_bytes(texts[100])

    def delivered():
        state = snapshot()
        return state if (state["outbox"] or {}).get("state") == "delivered" else None

    verified = record["verified_publication"] = baseline.phase6.wait(delivered, seconds=120)
    assert verified["sequence"] == 2 and verified["outbox_count"] == 2, verified
    assert verified["job"]["state"] == "succeeded" and verified["job"]["expected_sequence"] == 1, verified
    assert project.admin("/ready").returncode == 0
    read_exact(client, verified, 100, texts[100])
    assert project.sql(historical_proofs, baseline.phase1.SERVE) == previous
    reports, events, _ = baseline.collect_logs(project, evidence)
    failed_report, successful_report = reports
    assert "rsync and HTTPS TXT disagree" in failed_report["error"], failed_report
    assert successful_report["verified_txt"] == len(texts) and successful_report["rsync_only_txt"] == 0
    assert successful_report["metrics"]["txt"]["calls"] == len(texts)
    assert successful_report["metrics"]["https_peak"] == successful_report["metrics"]["parse_peak"] == 1
    assert successful_report["metrics"]["admitted_peak"] == 1
    record["collector"] = successful_report
    record["publication"] = [event for event in events if event.get("event") == "publication"]
    record["verification"] = verify_documents(project, baseline, verified, texts, cases, set(texts), active)
    print("Strict configuration rollback reused the incumbent until verified sequence 2", flush=True)


def trial(args, docker, driver, work, baseline, repeat, size, *, bulk=False, full=False, rollback=False):
    label = f"{size}-{'bulk' if bulk else 'strict'}-{repeat}"
    evidence = args.evidence / label
    evidence.mkdir()
    directory = work / label
    directory.mkdir()
    receipt = {"status": "error", "size": size, "repeat": repeat, "bulk": bulk,
               "private_fixture_paced": not full, "setup_seconds": {}}
    project = None
    staging = ExitStack()
    try:
        for service in ("sync", "feed"):
            (directory / service).hardlink_to(work / service)
        project = baseline.Project(directory / f"{work.name}-{label}", driver, args.image_prefix,
                                   docker, receipt["setup_seconds"])
        project.preprovision(6 if full else size, directory, paced=not full)
        project.env.update(PHASE6_HOURLY="14400", PHASE6_DAILY="14400")
        cases = prepare_sources(project, baseline, size, full)
        stage_bytes = None
        if bulk:
            capacity = {"mebibytes": 640, "inodes": 16384} if full else {"mebibytes": 64, "inodes": 512}
            with baseline.duration(receipt["setup_seconds"], "bulk_stage_provisioning"):
                project.stage_mount = staging.enter_context(baseline.phase2.capped_stage(project, **capacity))
                stage_bytes = capacity["mebibytes"] * 1024**2
                project.use_bulk_bootstrap(project.stage_mount)
        receipt["fixture_sha256"], texts = baseline.fixture_inventory(project)
        receipt["stage_limit_bytes"] = stage_bytes
        active = measure(project, baseline, receipt, evidence / "cold", texts, cases, bulk=bulk, full=full)
        print(f"{label} cold T1={receipt['cold']['milestones']['t1_mcp_read']['seconds']:.3f}s", flush=True)
        if full:
            changed = tuple(range(10000, 10010))
            for rfc in changed:
                texts[rfc] += b"\n   Updated synthetic original for incremental qualification.\n"
                for endpoint in ("rsync", "https"):
                    (project.source / endpoint / f"rfc{rfc}.txt").write_bytes(texts[rfc])
            (project.source / "mode").write_text("phase8-incremental")
            receipt["incremental_fixture_sha256"], _ = baseline.fixture_inventory(project)
            project.run("up", "-d", "--no-deps", "control-probe", timeout=30)
            measure(project, baseline, receipt, evidence / "incremental", texts, cases,
                    bulk=True, full=True, prior=active, changed=changed)
            print(f"{label} incremental T1={receipt['incremental']['milestones']['t1_mcp_read']['seconds']:.3f}s",
                  flush=True)
        if rollback:
            rehearse_rollback(project, baseline, receipt, evidence / "rollback", active, texts, cases)
        receipt["status"] = "published_and_ready"
    except (AssertionError, KeyError, OSError, RuntimeError, TypeError, ValueError, subprocess.SubprocessError) as error:
        receipt["error"] = f"{type(error).__name__}: {error}"
        if project:
            try:
                baseline.collect_logs(project, evidence)
            except (OSError, ValueError, subprocess.SubprocessError) as log_error:
                receipt["log_error"] = str(log_error)
        print(f"{label}: {receipt['error'][:500]}", flush=True)
    finally:
        try:
            with staging:
                if project:
                    project.close()
        except subprocess.SubprocessError as cleanup_error:
            receipt.update(status="error", cleanup_error=str(cleanup_error))
        (evidence / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return receipt


def gate(size):
    profiles = {}
    for profile, per_rfc in (("bulk_txt_xml_https", 1), ("strict_txt_and_xml_https", 2)):
        starts = size * per_rfc + 2
        profiles[profile] = {"starts": starts, "minimum_first_to_last_start_seconds": starts - 1,
                             "required_starts_per_second": starts / ACQUISITION_BUDGET_SECONDS}
    return {"planning_rfc_count": size, "target_seconds": TARGET_SECONDS,
            "acquisition_budget_seconds": ACQUISITION_BUDGET_SECONDS,
            "production_start_interval_seconds": 1, "production_status": "infeasible_at_current_pacing",
            "profiles": profiles}


def run(args, docker, driver, work, summary, baseline):
    summary["production_gate"] = gate(FULL_SIZE)
    profiles = {
        "quick": [(6, False, False, 1)],
        "paced": [(96, bulk, False, repeat) for repeat in range(1, 4) for bulk in (False, True)],
        "full": [(FULL_SIZE, True, True, repeat) for repeat in range(1, 4)],
        "rollback": [(6, True, False, 1)],
    }
    for size, bulk, full, repeat in profiles[args.phase8]:
        record = trial(args, docker, driver, work, baseline, repeat, size, bulk=bulk, full=full,
                       rollback=args.phase8 == "rollback")
        summary["runs"].append(record)
        if record["status"] == "error":
            break
    groups = defaultdict(list)
    manifests = defaultdict(set)
    inventories = defaultdict(set)
    for record in summary["runs"]:
        if record["status"] != "published_and_ready":
            continue
        for name in ("cold", "incremental"):
            if name in record:
                leg = record[name]
                key = f"{'bulk' if record['bulk'] else 'strict'}_{name}"
                groups[key].append(leg["milestones"]["t1_mcp_read"]["seconds"])
                manifests[key].add(leg["collector"]["manifest_digest"])
                inventories[name].add(record["fixture_sha256" if name == "cold" else "incremental_fixture_sha256"])
    summary["aggregate_t1_seconds"] = {
        key: {"trials": values, "p50": median(values), "p95": sorted(values)[math.ceil(0.95 * len(values)) - 1]}
        for key, values in groups.items()
    }
    summary["fixtures_reproducible"] = all(len(digests) == 1 for digests in inventories.values())
    # Incremental captures and warm reuse have legitimate time-dependent provenance.
    summary["cold_manifests_reproducible"] = all(len(digests) == 1 for key, digests in manifests.items()
                                                 if key.endswith("_cold"))
