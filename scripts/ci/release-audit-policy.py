#!/usr/bin/env python3
"""Enforce publication severity thresholds on Trivy reports."""

import argparse
import json
from pathlib import Path
import sys


def blocked_findings(report, kind):
    if report["SchemaVersion"] != 2 or not report["ArtifactName"]:
        raise ValueError("unexpected Trivy report schema")
    results = report["Results"]
    expected = {"filesystem": ("lang-pkgs", "cargo"), "container_image": ("os-pkgs", "alpine")}
    if expected.get(report["ArtifactType"]) not in {(item["Class"], item["Type"]) for item in results}:
        raise ValueError("Trivy did not report the expected Cargo or Alpine scan")
    for result in results:
        for finding in result.get("Vulnerabilities") or []:
            severity = finding["Severity"]
            fixed = (finding.get("FixedVersion") or "").strip()
            if severity == "CRITICAL" or (kind != "build" and severity == "HIGH" and fixed):
                yield finding


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=("build", "beta", "stable"))
    parser.add_argument("reports", type=Path, nargs="+")
    args = parser.parse_args()
    blocked = False
    for path in args.reports:
        for item in blocked_findings(json.loads(path.read_bytes()), args.kind):
            blocked = True
            print(f"{path.name}: {item['Severity']} {item['VulnerabilityID']} "
                  f"{item['PkgName']} {item['InstalledVersion']}", file=sys.stderr)
    if blocked:
        sys.exit("release image audit failed")


if __name__ == "__main__":
    main()
