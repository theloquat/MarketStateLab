"""Standard-library-only setup; works before installing application dependencies."""

import hashlib
import json
import os
import secrets
from pathlib import Path


def password_hash(password, salt=None):
    salt = salt or secrets.token_hex(16)
    return (
        salt
        + ":"
        + hashlib.pbkdf2_hmac(
            "sha256", password.encode(), bytes.fromhex(salt), 300000
        ).hex()
    )


def initialize_access(root):
    directory = Path(root) / "access"
    directory.mkdir(parents=True, exist_ok=True)
    os.chmod(directory, 0o700)
    users = directory / "users.json"
    credentials = directory / "credentials.txt"
    if users.exists():
        return
    records = {}
    lines = ["Market State Lab 本地登录信息；请勿提交或录入演示视频。\n"]
    for username, role in [
        ("admin", "admin"),
        ("researcher", "researcher"),
        ("viewer", "viewer"),
    ]:
        password = secrets.token_urlsafe(18)
        records[username] = {"role": role, "password_hash": password_hash(password)}
        lines.append(username + "  " + password)
    for path, value in [
        (users, json.dumps(records, ensure_ascii=False, sort_keys=True)),
        (credentials, "\n".join(lines) + "\n"),
    ]:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(value)
