#!/usr/bin/env python3
"""Download the official Engineer 004 fixture at a pinned commit and check its SHA-256.

The fixture belongs to Single Grain's public beat-claude repository, which has no license
file, so this repository does not redistribute it. Run this once before analyze_fixture.py.
Standard library only; nothing is written unless the checksum matches.

    python3 tools/fetch_fixture.py
"""
import hashlib
import sys
import urllib.request
from pathlib import Path

COMMIT = "4b719e4501c8ff98fd3e641c46d3bffe34b9aaa5"
URL = "https://raw.githubusercontent.com/ericosiu/beat-claude/%s/challenges/engineer-004/fixtures/event_sample.jsonl" % COMMIT
SHA256 = "1aeb24b415009e89fcf8acb5a178410faf216dc17b16920d9849ecc8bbb24235"
OUT = Path(__file__).resolve().parent.parent / "fixtures" / "event_sample.jsonl"


def main():
    with urllib.request.urlopen(URL, timeout=60) as response:
        data = response.read()
    digest = hashlib.sha256(data).hexdigest()
    if digest != SHA256:
        print("checksum mismatch: got %s, expected %s; nothing written" % (digest, SHA256), file=sys.stderr)
        return 1
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_bytes(data)
    print("official fixture written to %s (commit %s, sha256 %s)" % (OUT.relative_to(OUT.parent.parent), COMMIT[:8], digest))
    return 0


if __name__ == "__main__":
    sys.exit(main())
