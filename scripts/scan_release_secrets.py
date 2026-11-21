"""Check public working files and Git history for actual local configured secrets.

Only counts and relative paths are emitted. No credentials or source snippets.
This is an exact-value packaging check, not a general vulnerability certificate.
"""

import argparse
import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    values = set()
    for path in [ROOT / "deploy/contest/secrets.env"]:
        if path.exists():
            for line in path.read_text().splitlines():
                if "=" in line and not line.startswith("#"):
                    key, value = line.split("=", 1)
                    if re.search(r"KEY|PASSWORD|SECRET|TOKEN", key, re.I):
                        value = value.strip().strip("\"'")
                        if len(value) >= 16:
                            values.add(value.encode())
    for root in [ROOT / ".quantos-contest/access"]:
        if not root.exists():
            continue
        for path in root.rglob("*llm*.json"):
            try:
                config = json.loads(path.read_text())
            except (ValueError, OSError):
                continue
            for key in ["api_key", "secret", "token"]:
                value = config.get(key)
                if isinstance(value, str) and len(value) >= 16:
                    values.add(value.encode())
    credentials = ROOT / ".quantos-contest/access/credentials.txt"
    if credentials.exists():
        for line in credentials.read_text().splitlines():
            fields = line.split()
            if len(fields) == 2 and fields[0] in {"admin", "researcher", "viewer"}:
                values.add(fields[1].encode())
    files = set(
        subprocess.check_output(
            ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
            cwd=ROOT,
        )
        .decode()
        .split("\0")
    ) - {""}
    hits = []
    for name in sorted(files):
        path = ROOT / name
        if path.is_file() and any(value in path.read_bytes() for value in values):
            hits.append(name)
    history = subprocess.check_output(
        ["git", "log", "--all", "-p", "--format="], cwd=ROOT
    )
    history_matches = sum(value in history for value in values)
    forbidden = [
        name
        for name in files
        if (name.startswith((".quantos", "data/", "tmp/")) and name != "data/README.md")
        or name in {".env", "deploy/contest/secrets.env"}
    ]
    result = {
        "method": "exact locally configured secret values, public working files plus all Git patch history; no secret values emitted",
        "scanned_files": len(files),
        "known_secret_values": len(values),
        "working_file_matches": hits,
        "history_secret_matches": history_matches,
        "forbidden_packaging_paths": forbidden,
        "passed": not hits and not history_matches and not forbidden,
    }
    if args.output:
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
