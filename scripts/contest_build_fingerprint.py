"""Standard-library-only build-input fingerprint; excludes runtime data/secrets."""

import hashlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def fingerprint():
    paths = {
        ROOT / "requirements-contest.lock",
        ROOT / "Dockerfile",
        ROOT / "docker-compose.yml",
        ROOT / ".dockerignore",
    }
    for folder in ["backend", "frontend", "services", "deploy/contest"]:
        for path in (ROOT / folder).rglob("*"):
            parts = path.relative_to(ROOT).parts
            if not path.is_file() or any(
                part
                in {
                    "node_modules",
                    "dist",
                    "coverage",
                    "target",
                    "__pycache__",
                    ".pytest_cache",
                }
                for part in parts
            ):
                continue
            if path.name == "secrets.env" or path.name.startswith(".env"):
                continue
            if path.suffix in {
                ".py",
                ".tsx",
                ".ts",
                ".css",
                ".html",
                ".java",
                ".xml",
                ".json",
            } or path.name.startswith("Dockerfile"):
                paths.add(path)
    digest = hashlib.sha256()
    for path in sorted(paths):
        digest.update(path.relative_to(ROOT).as_posix().encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


if __name__ == "__main__":
    print(fingerprint())
