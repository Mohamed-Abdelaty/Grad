"""Scans evaluation documentation, scripts, and results for secrets and oversized files.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
TARGET_DIRS = [
    REPO_ROOT / "docs" / "evaluation",
    REPO_ROOT / "scripts" / "evaluation",
    REPO_ROOT / "tests" / "evaluation",
]

SECRET_PATTERNS = [
    re.compile(r'(?i)(password|secret|token|api_key|private_key)\s*[:=]\s*["\'][^"\']+["\']'),
    re.compile(r"-----BEGIN [A-Z ]+ PRIVATE KEY-----"),
]

ALLOWLISTED_SNIPPETS = {
    "change_me",
    "letmein",
    "secret_key",
    "placeholder",
    "default",
    "your_password",
}


def scan():
    print("=" * 80)
    print("HYGIENE AND SECRETS AUDIT")
    print("=" * 80)

    secrets_flagged = []
    large_files = []

    for d in TARGET_DIRS:
        for p in d.rglob("*"):
            if not p.is_file():
                continue
            
            # Check size
            size_mb = p.stat().st_size / (1024 * 1024)
            if size_mb > 5.0:
                large_files.append((str(p.relative_to(REPO_ROOT)), f"{size_mb:.2f} MB"))

            # Check secrets in text files
            if p.suffix.lower() in (".md", ".py", ".json", ".yaml", ".yml", ".txt"):
                try:
                    text = p.read_text(encoding="utf-8", errors="ignore")
                except Exception:
                    continue
                for pat in SECRET_PATTERNS:
                    for m in pat.finditer(text):
                        val = m.group(0)
                        if any(allow in val.lower() for allow in ALLOWLISTED_SNIPPETS):
                            continue
                        secrets_flagged.append((str(p.relative_to(REPO_ROOT)), val[:60]))

    if large_files:
        print(f"Warning: {len(large_files)} files larger than 5 MB found:")
        for path, size in large_files:
            print(f"  {path}: {size}")
    else:
        print("Large files (>5MB) check: PASSED (0 files exceed 5MB).")

    if secrets_flagged:
        print(f"FAILED: {len(secrets_flagged)} potential secrets found:")
        for path, snippet in secrets_flagged:
            print(f"  {path}: {snippet}")
        return 1
    else:
        print("Secrets scan check: PASSED (0 secrets detected).")
        return 0


if __name__ == "__main__":
    import sys
    sys.exit(scan())
