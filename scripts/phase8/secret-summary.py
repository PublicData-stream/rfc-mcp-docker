#!/usr/bin/env python3
"""Keep scanner rule and location only, including when a secret is detected."""

import json
from pathlib import Path
import sys

report = json.loads(Path(sys.argv[1]).read_text())
fields = ("RuleID", "Category", "Severity", "StartLine", "EndLine")
summary = [
    {"target": result["Target"], **{field: secret[field] for field in fields if field in secret}}
    for result in report.get("Results") or []
    for secret in result.get("Secrets") or []
]
print(json.dumps(summary, indent=2))
