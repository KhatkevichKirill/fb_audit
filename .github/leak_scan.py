#!/usr/bin/env python3
"""
leak_scan — fail CI if anything sensitive is committed to this public repo.

Self-contained (no deps) so GitHub Actions can run it directly. Scans every
git-tracked text file for access tokens, literal passwords/secrets, internal
paths, private host IPs, real account ids, or credential-bearing Postgres URLs.
Exits non-zero on the first finding.

This mirrors the leak gate in the private sync tool — it's a second line of
defense in case something slips past the export step.
"""

import re
import subprocess
import sys

SECRET_PATTERNS = [
    ("facebook_access_token", re.compile(r"EAA[A-Za-z0-9]{20,}")),
    ("literal_password",      re.compile(r"(?i)password\s*[=:]\s*['\"][^'\"]+['\"]")),
    ("literal_secret_assign", re.compile(r"(?i)(secret|api[_-]?key|token)\s*=\s*['\"][^'\"]{12,}['\"]")),
    ("internal_opt_path",     re.compile(r"/opt/fb_audit")),
    ("private_db_host_ip",    re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")),
    ("real_account_id",       re.compile(r"\bact_\d{6,}\b")),
    ("postgres_url",          re.compile(r"postgres(?:ql)?://[^/\s'\"]+:[^/\s'\"]+@")),
]

ALLOW_SUBSTRINGS = [
    "DB_PASSWORD", "FB_ACCESS_TOKEN",
    "os.environ", 'environ["', "environ.get",
    "127.0.0.1", "0.0.0.0", "localhost",
    "act_<id>", "'act_' + account_id", '"act_" + account_id',
    "EAA...", "EAAH...",
]

# files exempt from scanning (this scanner contains the patterns themselves)
SKIP_FILES = {".github/leak_scan.py"}


def tracked_files():
    out = subprocess.check_output(["git", "ls-files"], text=True)
    return [f for f in out.splitlines() if f and f not in SKIP_FILES]


def version_like(text):
    return bool(re.fullmatch(r"v?\d{1,3}\.\d{1,3}", text))


def scan_file(path):
    hits = []
    try:
        with open(path, encoding="utf-8") as fh:
            lines = fh.read().splitlines()
    except (UnicodeDecodeError, FileNotFoundError):
        return hits
    for i, line in enumerate(lines, 1):
        if any(tok in line for tok in ALLOW_SUBSTRINGS):
            continue
        for name, pat in SECRET_PATTERNS:
            for m in pat.finditer(line):
                if name == "private_db_host_ip" and version_like(m.group(0)):
                    continue
                hits.append((i, name, m.group(0)[:60]))
    return hits


def main():
    findings = {}
    for path in tracked_files():
        hits = scan_file(path)
        if hits:
            findings[path] = hits
    if findings:
        print("Secret scan FAILED — sensitive content found:")
        for path, hits in findings.items():
            print(f"  {path}:")
            for ln, kind, snip in hits:
                print(f"    line {ln}: [{kind}] {snip}")
        sys.exit(1)
    print("Secret scan passed: no sensitive content found.")


if __name__ == "__main__":
    main()
