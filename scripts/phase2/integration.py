#!/usr/bin/env python3
"""Private corpus fixtures shared by ingestion, publication, and upgrade probes."""

from contextlib import contextmanager
import importlib.util
import json
from pathlib import Path
import subprocess
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
SPEC = importlib.util.spec_from_file_location("phase1", HERE.parent / "phase1" / "integration.py")
phase1 = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(phase1)
run = phase1.run
NUL_RFC = 542


class Project(phase1.Project):
    def __init__(self, directory, driver, image_prefix, docker):
        super().__init__(directory, driver, image_prefix, docker, directory.name)
        self.source = directory / "source"
        self.env.update(PHASE2_DRIVER=str(driver), PHASE2_SOURCE=str(self.source),
                        PHASE2_ENDPOINT=str(HERE / "endpoint.py"))

    def command(self, *args):
        command = super().command(*args)
        insertion = len(self.docker) + 3
        return [*command[:insertion], "-f", str(HERE / "compose.yml"), *command[insertion:]]

    def reconcile(self, scope="full", expected=None, live=False, settings=(), timeout=180, prelude=None, *, options=()):
        if settings or prelude is not None or options:
            command = self.reconcile_command(live, scope, settings, options=options, prelude=prelude)
        else:
            command = self.reconcile_command(live, scope)
        result = subprocess.run(command, env=self.env, text=True, capture_output=True, timeout=timeout)
        if not result.stdout.strip():
            raise AssertionError(f"no ingestion report: {result.stderr}")
        report = json.loads(result.stdout.strip().splitlines()[-1])
        print(json.dumps(report, sort_keys=True), flush=True)
        if expected is None:
            assert result.returncode == 0 and report["state"] == "ready", (result.stderr, report)
        else:
            assert result.returncode != 0 and expected in report["error"], (result.stderr, report)
            if report.get("generation_id"):
                assert self.sql(f"SELECT state FROM rfc.generations WHERE id = {int(report['generation_id'])}") == "failed"
        return report


def catalog_entry(rfc, title, tags="", file_format="TXT", day=""):
    month, year, stream, authors = {
        2119: ("March", 1997, "IETF", "<author><name>S. Bradner</name></author>"),
        8174: ("May", 2017, "IETF", "<author><name>B. Leiba</name></author>"),
    }.get(rfc, ("May", 2026, "IRTF", ""))
    return (f"<rfc-entry><doc-id>RFC{rfc}</doc-id><title>{title}</title>{authors}"
            f"<date><month>{month}</month>{day}<year>{year}</year></date>"
            f"<format><file-format>{file_format}</file-format></format>"
            f"{tags}<current-status>INFORMATIONAL</current-status><stream>{stream}</stream></rfc-entry>")



def prepare(project, *, include_nul=False):
    source = project.source
    source.mkdir()
    for name in ("https", "rsync"):
        (source / name).mkdir()
    phase1.phase0.make_leaf(project.directory / "certs", "fixtures", source)
    (source / "rsyncd.conf").write_text(
        "pid file = /tmp/rsyncd.pid\nlock file = /tmp/rsyncd.lock\nuse chroot = false\n"
        "read only = true\n[rfc]\npath = /fixture-source/rsync\n"
    )
    fixture = (REPO / "fixtures/synthetic/headings.txt").read_bytes()
    # The 16 KiB byte cap splits "boundarytoken" before the next LF.
    chunk_start = len(fixture)
    between = b"firstterm " + b"x" * (16384 - chunk_start - len(b"firstterm bound")) + b" "
    fixture += between + b"boundarytoken lastterm\n\n5. References\n\n   [RFC90000] Example\n"
    sources = {
        100: fixture,
        101: b"Historical RFC bytes\r\nunencoded \xff bytes\n",
        2119: (REPO / "fixtures/rfc/rfc2119.txt").read_bytes(),
        8174: (REPO / "fixtures/rfc/rfc8174.txt").read_bytes(),
        9110: b"1. An RFCXML classification fixture\n\n   Text.\n",
    }
    if include_nul:
        sources[NUL_RFC] = (b"1. Introduction\r\n\r\n   caf\xc3\xa9\x00text.\n\n"
                            b"2. Copyright Notice\n\n   Copyright example.\n\n"
                            b"3. References\n\n   [RFC100] Example\n")
    for rfc, payload in sources.items():
        for directory in ("https", "rsync"):
            (source / directory / f"rfc{rfc}.txt").write_bytes(payload)
    (source / "https/rfc8174.xml").write_bytes(
        b'<rfc number="8174"><back><references><name>Normative References</name>'
        b'<reference><seriesInfo name="RFC" value="2119"/></reference></references></back></rfc>'
    )
    (source / "https/rfc9110.xml").write_bytes(
        b'<rfc number="9110"><back><references><name>References</name><references>'
        b'<name>Informative References</name><reference><seriesInfo name="RFC" value="100"/>'
        b'</reference></references><reference><seriesInfo name="RFC" value="101"/></reference>'
        b'<reference anchor="RFC-unresolved"><seriesInfo name="RFC" value="invalid"/></reference>'
        b'</references></back></rfc>'
    )
    entries = [
        catalog_entry(100, "Mixed original bytes", "<obsoleted-by><doc-id>RFC103</doc-id></obsoleted-by>"),
        catalog_entry(101, "Legacy bytes", day="<day>23</day>"),
        catalog_entry(103, "PDF only", "<obsoletes><doc-id>RFC100</doc-id></obsoletes>", "PDF"),
        catalog_entry(2119, "RFC 2119", "<updated-by><doc-id>RFC8174</doc-id></updated-by>"),
        catalog_entry(8174, "RFC 8174", "<updates><doc-id>RFC2119</doc-id></updates>"),
        catalog_entry(9110, "Synthetic XML classification"),
        "<rfc-not-issued-entry><doc-id>RFC104</doc-id></rfc-not-issued-entry>",
    ]
    if include_nul:
        entries.append(catalog_entry(NUL_RFC, "NUL bytes with RFCXML"))
        (source / f"https/rfc{NUL_RFC}.xml").write_bytes(
            f'<rfc number="{NUL_RFC}">'.encode()
            + b'<back><references><name>Normative References</name>'
            b'<reference><seriesInfo name="RFC" value="2119"/></reference></references></back></rfc>'
        )
    (source / "catalog.xml").write_text(
        '<rfc-index xmlns="https://www.rfc-editor.org/rfc-index">' + "".join(entries) + "</rfc-index>"
    )
    (source / "mode").write_text("normal")
    return sources



@contextmanager
def capped_stage(project, *, mebibytes=64, inodes=512, label="rsync"):
    image = project.directory / f"{label}.ext4"
    mountpoint = project.directory / f"{label}-mount"
    mountpoint.mkdir()
    with image.open("wb") as disk:
        disk.truncate(mebibytes * 1024**2)
    run(["mkfs.ext4", "-q", "-N", str(inodes), "-m", "0", str(image)])
    run(["sudo", "-n", "mount", "-o", "loop,nosuid,nodev,noexec", str(image), str(mountpoint)])
    try:
        maps = phase1.capture([
            *project.docker, "run", "--rm", *phase1.phase0.local_image_args(), "--network", "none",
            "--entrypoint", "/bin/sh", project.env["PHASE1_IMAGE_PREFIX"] + "-sync",
            "-c", "cat /proc/self/uid_map; echo :; cat /proc/self/gid_map",
        ])
        def host_id(mapping):
            for line in mapping.strip().splitlines():
                internal, external, count = map(int, line.split())
                if internal <= 10002 < internal + count:
                    return external + 10002 - internal
            raise AssertionError("Sync UID or GID is outside the Docker namespace mapping")
        uid, gid = map(host_id, maps.split(":"))
        run(["sudo", "-n", "chown", f"{uid}:{gid}", str(mountpoint)])
        run(["sudo", "-n", "chmod", "0700", str(mountpoint)])
        yield mountpoint
    finally:
        run(["sudo", "-n", "umount", str(mountpoint)])



def bulk_options(project, mountpoint, *, bootstrap=False, seconds=900):
    config = project.directory / "bulk.toml"
    config.write_text("schema_version = 1\n"
                      + ("[ingestion]\ntxt_provenance = 'rsync_bootstrap'\n" if bootstrap else "")
                      + "[ingestion.rsync]\nmode = 'persistent_bulk'\n"
                        f"bulk_transfer_timeout_seconds = {seconds}\n")
    return ("-v", f"{mountpoint}:/rfc-rsync-txt",
            "-v", f"{config}:/phase2/bulk.toml:ro")



def migrate_to_historical_schema(project, old_schema):
    migrations = (REPO / "migrations/migrate.sql").read_text()
    boundary = f"SELECT NOT EXISTS (SELECT FROM rfc.schema_migrations WHERE version = {old_schema+1})"
    old_sql, _ = migrations.split(boundary, 1)
    run([*project.psql(), "-f", "/dev/stdin"], env=project.env,
        input=old_sql.replace("\\ir ", "\\i /migrations/") + "COMMIT;\n")
    assert project.sql("SELECT max(version) FROM rfc.schema_migrations") == str(old_schema)



if __name__ == "__main__":
    spec = importlib.util.spec_from_file_location("e2e", HERE.parent / "ci" / "docker-e2e.py")
    e2e = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(e2e)
    with e2e.historical_checkout() as checkout:
        subprocess.run([sys.executable, "-B", str(checkout / "scripts/phase2/integration.py"), *sys.argv[1:]],
                       check=True)
