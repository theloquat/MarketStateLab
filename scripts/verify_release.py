#!/usr/bin/env python3
"""Real local deployment acceptance. Never prints passwords, cookies or tokens."""

import argparse
import asyncio
import json
import time
from pathlib import Path
import httpx
import websockets

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://127.0.0.1:18080")
    args = parser.parse_args()
    credentials = {}
    for line in (
        (ROOT / ".quantos-contest/access/credentials.txt").read_text().splitlines()
    ):
        fields = line.split()
        if len(fields) == 2 and fields[0] in ("admin", "researcher", "viewer"):
            credentials[fields[0]] = fields[1]
    report = {"started_at": time.time(), "checks": {}}

    def check(name, ok):
        report["checks"][name] = bool(ok)
        if not ok:
            raise AssertionError(name)

    with httpx.Client(base_url=args.base, timeout=60, follow_redirects=False) as c:
        check("anonymous_blocked", c.get("/api/lab/summary").status_code == 401)
        login = c.post(
            "/auth/login",
            json={"username": "viewer", "password": credentials["viewer"]},
        )
        check("viewer_login", login.status_code == 200)
        check(
            "viewer_write_rejected",
            c.post("/api/lab/train", json={"train_end": "2022-12-31"}).status_code
            == 403,
        )
        check("viewer_admin_rejected", c.get("/api/lab/admin/audit").status_code == 403)
        check(
            "graphql_dashboard",
            bool(
                c.post("/graphql", json={"query": "{ dashboard }"}).json()["data"][
                    "dashboard"
                ]["datasets"]
            ),
        )
        check(
            "origin_rejected",
            c.post(
                "/graphql",
                headers={"Origin": "http://untrusted.invalid"},
                json={"query": "{ dashboard }"},
            ).status_code
            == 403,
        )
        old = login.json()["access_token"]
        c.post("/auth/logout")
        check(
            "logout_revokes_bearer",
            c.get("/auth/me", headers={"Authorization": "Bearer " + old}).status_code
            == 401,
        )
        login = c.post(
            "/auth/login", json={"username": "admin", "password": credentials["admin"]}
        )
        check("admin_login", login.status_code == 200)
        summary = c.get("/api/lab/summary").json()
        completed = [
            r
            for r in c.get("/api/lab/stream").json()["runs"]
            if r["status"] == "features_completed"
        ]
        if not completed:
            raise RuntimeError("先完成一个真实分布式回放，再运行服务验收")
        selected = max(completed, key=lambda r: r["payload"].get("created_at", ""))
        rid = selected["id"]
        mid = selected["payload"]["model_id"]
        check(
            "feature_service", c.get("/api/features/latest/" + rid).status_code == 200
        )
        check("model_service", c.get("/api/models/" + mid).status_code == 200)
        check("alert_service", c.get("/api/alerts/" + rid).status_code == 200)
        c.get("/api/models/" + mid)
        check(
            "caffeine_used",
            c.get("/api/models/" + mid).headers.get("x-cache") == "CAFFEINE",
        )
        check("openapi", bool(c.get("/openapi.json").json().get("paths")))
        check(
            "swagger_local_assets",
            c.get("/vendor/swagger-ui-bundle.js").status_code == 200,
        )
        token = login.json()["access_token"]

        async def socket_test():
            async with websockets.connect(
                args.base.replace("http", "ws", 1) + "/api/lab/events",
                additional_headers={"Authorization": "Bearer " + token},
                origin=args.base,
                open_timeout=10,
            ) as ws:
                return (
                    json.loads(await asyncio.wait_for(ws.recv(), 5)).get("type")
                    == "connected"
                )

        check("websocket_bridge", asyncio.run(socket_test()))
        check("monitor", c.get("/api/lab/monitor").status_code == 200)
        check(
            "agent_config_no_call",
            c.get("/api/lab/admin/llm").status_code == 200,
        )
        pdf = ROOT / ".quantos-contest/raw/external/treasury-2020Q2.pdf"
        if pdf.exists():
            with pdf.open("rb") as f:
                upload = c.post(
                    "/api/lab/pdf",
                    files={"file": (pdf.name, f, "application/pdf")},
                    data={
                        "source_url": "https://home.treasury.gov/system/files/221/TBACCharge1Q22020.pdf",
                        "published_at": "",
                    },
                )
            check("real_pdf_archive", upload.status_code == 201)
            receipt = upload.json()
            report["pdf"] = receipt
            check(
                "pdf_pages",
                len(c.get("/api/lab/pdf/" + receipt["id"] + "/pages").json()["items"])
                == receipt["pages"],
            )
            check(
                "pdf_original",
                c.get("/api/lab/pdf/" + receipt["id"] + "/original").content[:5]
                == b"%PDF-",
            )
        report["models"] = [m["id"] for m in summary["models"]]
    report["finished_at"] = time.time()
    out = ROOT / (
        "docs/contest/release-integration-" + time.strftime("%Y%m%d-%H%M%S") + ".json"
    )
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
