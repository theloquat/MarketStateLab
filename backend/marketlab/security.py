from __future__ import annotations
import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from fastapi import HTTPException
from .access_setup import password_hash
from .contracts import canonical

ROLES = {"viewer": 0, "researcher": 1, "admin": 2}


def b64(value):
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


def unb64(value):
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def password_ok(password, stored):
    try:
        return hmac.compare_digest(
            password_hash(password, stored.split(":")[0]), stored
        )
    except (ValueError, IndexError):
        return False


def issue(user, role, secret, ttl=1800):
    now = int(time.time())
    claims = {
        "sub": user,
        "role": role,
        "iat": now,
        "exp": now + ttl,
        "jti": secrets.token_hex(16),
        "iss": "quantos",
        "aud": "market-lab",
    }
    unsigned = (
        b64(b'{"alg":"HS256","typ":"JWT"}') + "." + b64(canonical(claims).encode())
    )
    return (
        unsigned
        + "."
        + b64(hmac.new(secret.encode(), unsigned.encode(), hashlib.sha256).digest())
    )


def verify(token, secret, now=None):
    try:
        header, payload, signature = token.split(".")
        if json.loads(unb64(header)).get("alg") != "HS256":
            raise ValueError()
        expected = hmac.new(
            secret.encode(), (header + "." + payload).encode(), hashlib.sha256
        ).digest()
        if not hmac.compare_digest(expected, unb64(signature)):
            raise ValueError()
        claims = json.loads(unb64(payload))
        clock = time.time() if now is None else now
        if (
            claims["exp"] <= clock
            or claims["iat"] > clock + 30
            or claims["iss"] != "quantos"
            or claims["aud"] != "market-lab"
            or claims["role"] not in ROLES
            or not isinstance(claims.get("sub"), str)
            or not claims["sub"]
            or not claims.get("jti")
        ):
            raise ValueError()
        return claims
    except (ValueError, KeyError, TypeError, UnicodeDecodeError):
        raise HTTPException(401, "会话无效或已过期")


def token_from(request):
    authorization = request.headers.get("authorization", "")
    return (
        authorization[7:]
        if authorization.startswith("Bearer ")
        else request.cookies.get("quantos_session", "")
    )


def identity_from(request):
    claims = verify(token_from(request), os.environ["CONTEST_JWT_SECRET"])
    if os.getenv("CONTEST_MYSQL_HOST"):
        from .distributed import cache

        if cache().exists("quantos:revoked:" + claims["jti"]):
            raise HTTPException(401, "会话已注销")
    return claims


def require_role(request, minimum="viewer"):
    if not os.getenv("CONTEST_AUTH_REQUIRED") == "1":
        return {"sub": "local-test", "role": "admin"}
    claims = identity_from(request)
    if ROLES[claims["role"]] < ROLES[minimum]:
        raise HTTPException(403, "当前角色没有此操作权限")
    return claims


def users_file(lab):
    return lab.root / "access" / "users.json"
