#!/usr/bin/env python3
"""Check repository-relative targets in tracked Markdown."""

from functools import cache
from html import unescape
from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import unquote, urlsplit


ROOT = Path(__file__).resolve().parents[2]
FENCE = re.compile(r"^\s*(`{3,}|~{3,})(.*)$")
CODE = re.compile(r"(?<!`)(`+)(?!`).*?(?<!`)\1(?!`)")
CODE_OR_COMMENT = re.compile(CODE.pattern + r"|(?P<comment><!--.*?(?:-->|$))")
HEADING = re.compile(r"^ {0,3}#{1,6}\s+(.+?)(?:\s+#+)?\s*$")
UNDERLINE = re.compile(r"^ {0,3}(?:=+|-+)\s*$")
ANCHOR = re.compile(r"""<[\w-]+\b[^<>]*?\s(?:id|name)\s*=\s*["']([^"']+)["']""", re.IGNORECASE)
DESTINATION = r"(?P<angle><[^>\n]+>)|(?P<bare>(?:\\.|[^\s()\\]|\([^()\n]*\))+)"
INLINE = re.compile(r"(?<!\\)\]\(\s*(?:" + DESTINATION + r""")(?:\s+["'][^"']*["'])?\s*\)""")
REFERENCE = re.compile(r"^ {0,3}\[[^]]+\]:\s*(?:" + DESTINATION + r")")


@cache
def markdown_lines(path):
    fence = ""
    in_comment = False
    lines = []

    def strip_comment(token):
        nonlocal in_comment
        if token["comment"] is None:
            return token[0]
        in_comment = not token[0].endswith("-->")
        return ""

    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if in_comment:
            _, end, line = line.partition("-->")
            if not end:
                continue
            in_comment = False
        marker = FENCE.match(line)
        if fence:
            if marker and marker[1].startswith(fence) and not marker[2].strip():
                fence = ""
        elif marker:
            fence = marker[1]
        elif not line.startswith(("    ", "\t")):
            lines.append((number, CODE_OR_COMMENT.sub(strip_comment, line)))
    return lines


@cache
def anchors(path):
    result = set()
    headings = set()
    previous = (0, "")
    for number, line in markdown_lines(path):
        result.update(unescape(value) for value in ANCHOR.findall(CODE.sub("", line)))
        heading = HEADING.match(line)
        if heading:
            label = heading[1]
        elif UNDERLINE.fullmatch(line) and previous[0] == number - 1 and previous[1].strip():
            label = previous[1]
        else:
            previous = (number, line)
            continue
        previous = (0, "")
        label = INLINE.sub("", label)
        label = re.sub(r"<[^<>]+>", "", label)
        label = unescape(label).lower().replace(" ", "-")
        base = re.sub(r"[^\w-]", "", label)
        slug = base
        suffix = 1
        while slug in headings:
            slug = f"{base}-{suffix}"
            suffix += 1
        headings.add(slug)
    return headings | result


def broken_links(root, source):
    for number, line in markdown_lines(source):
        line = CODE.sub("", line)
        links = list(INLINE.finditer(line))
        reference = REFERENCE.match(line)
        if reference:
            links.append(reference)
        for link in links:
            url = link["angle"][1:-1] if link["angle"] else link["bare"]
            url = unescape(re.sub(r"\\([^\w\s])", r"\1", url))
            if url.startswith("/") or re.match(r"[a-zA-Z][a-zA-Z0-9+.-]*:", url):
                continue
            parts = urlsplit(url)
            target = (source.parent / unquote(parts.path)).resolve() if parts.path else source
            if not target.is_relative_to(root) or not target.exists():
                yield number, url, "missing repository target"
            elif parts.fragment and target.suffix.lower() == ".md" and target.is_file():
                if unquote(parts.fragment) not in anchors(target):
                    yield number, url, "missing Markdown anchor"


def main():
    entries = subprocess.check_output(["git", "-C", str(ROOT), "ls-files", "-z", ":(icase)*.md"])
    failures = 0
    for entry in filter(None, entries.split(b"\0")):
        relative = entry.decode("utf-8")
        source = ROOT / relative
        if not source.resolve().is_relative_to(ROOT) or not source.is_file():
            print(f"{relative}: unavailable repository Markdown source", file=sys.stderr)
            failures += 1
            continue
        for line, url, reason in broken_links(ROOT, source):
            print(f"{relative}:{line}: {reason}: {url}", file=sys.stderr)
            failures += 1
    if not failures:
        print("Repository Markdown links passed")
    return int(bool(failures))


if __name__ == "__main__":
    sys.exit(main())
