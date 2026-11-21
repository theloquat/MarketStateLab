from __future__ import annotations
import hashlib
import hmac
import json
import os
import subprocess
import sys
import time
from datetime import date
from pathlib import Path
from fastapi import HTTPException, Request, Response, File, Form, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field, ConfigDict
from .contracts import canonical, identity, utcnow
from .security import (
    require_role,
    identity_from,
    issue,
    password_ok,
    users_file,
)


class Login(BaseModel):
    model_config = ConfigDict(extra="forbid")
    username: str = Field(min_length=1, max_length=128)
    password: str = Field(min_length=1, max_length=4096)


class Candidate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    method: str = Field(default="hmm", pattern="^(gmm|hmm)$")
    components: int = Field(default=3, ge=2, le=4)
    reduction: str = Field(default="pca95", pattern="^(none|pca95)$")
    train_end: date = date(2022, 12, 31)


class Compare(BaseModel):
    model_ids: list[str] = Field(min_length=1, max_length=4)


class Alias(BaseModel):
    model_id: str = Field(pattern=r"^[a-f0-9]{24}$")


class ResearchInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    tool: str = Field(max_length=40)
    hypothesis: str = Field(min_length=3, max_length=3000)
    model_ids: list[str] = Field(default_factory=list, max_length=4)
    day: date = date(2020, 3, 16)


class Decision(BaseModel):
    status: str = Field(pattern="^(approved|rejected|accepted|cancelled)$")
    note: str = Field(default="", max_length=2000)


def install(app, lab, summary, task):
    @app.middleware("http")
    async def authentication(request, call_next):
        path = request.url.path
        if (
            os.getenv("CONTEST_AUTH_REQUIRED") == "1"
            and path != "/health"
            and (
                path.startswith(("/api/", "/auth/"))
                or path in ("/graphql", "/docs", "/openapi.json")
            )
        ):
            if not hmac.compare_digest(
                request.headers.get("x-internal-token", ""),
                os.environ.get("CONTEST_INTERNAL_TOKEN", "disabled"),
            ):
                return JSONResponse({"detail": "请通过统一网关访问"}, 403)
            if path != "/auth/login":
                try:
                    minimum = (
                        "admin"
                        if path.startswith("/api/lab/admin/")
                        else (
                            "researcher"
                            if request.method not in ("GET", "HEAD", "OPTIONS")
                            and path not in ("/graphql", "/auth/logout")
                            else "viewer"
                        )
                    )
                    claims = require_role(request, minimum)
                    request.state.actor = claims["sub"]
                except HTTPException as exc:
                    return JSONResponse({"detail": exc.detail}, exc.status_code)
        response = await call_next(request)
        if path.startswith("/api/") and request.method not in (
            "GET",
            "HEAD",
            "OPTIONS",
        ):
            lab.execute(
                "INSERT INTO audit(at,actor,action,subject,detail) VALUES(?,?,?,?,?)",
                (
                    utcnow(),
                    getattr(request.state, "actor", "local"),
                    "http:" + request.method,
                    path,
                    canonical(
                        {
                            "status": response.status_code,
                            "request_id": request.headers.get("x-request-id", ""),
                        }
                    ),
                ),
            )
        return response

    @app.post("/auth/login")
    def login(body: Login, response: Response):
        users = (
            json.loads(users_file(lab).read_text()) if users_file(lab).exists() else {}
        )
        user = users.get(body.username)
        dummy = "00" * 16 + ":" + "00" * 32
        good = password_ok(body.password, user["password_hash"] if user else dummy)
        if not user or not good:
            raise HTTPException(401, "用户名或密码不正确")
        token = issue(body.username, user["role"], os.environ["CONTEST_JWT_SECRET"])
        response.set_cookie(
            "quantos_session",
            token,
            httponly=True,
            samesite="strict",
            max_age=1800,
            path="/",
        )
        lab.execute(
            "INSERT INTO audit(at,actor,action,subject,detail) VALUES(?,?,?,?,?)",
            (utcnow(), body.username, "login", "session", "{}"),
        )
        return {
            "username": body.username,
            "role": user["role"],
            "access_token": token,
            "token_type": "bearer",
            "expires_in": 1800,
        }

    @app.get("/auth/me")
    def me(request: Request):
        c = require_role(request)
        return {"username": c["sub"], "role": c["role"]}

    @app.post("/auth/logout")
    def logout(request: Request, response: Response):
        if os.getenv("CONTEST_AUTH_REQUIRED") == "1":
            from .distributed import cache

            c = identity_from(request)
            cache().set(
                "quantos:revoked:" + c["jti"],
                "1",
                ex=max(1, int(c["exp"] - time.time())),
            )
        response.delete_cookie("quantos_session", path="/")
        return {"status": "logged_out"}

    @app.post("/api/lab/models", status_code=202)
    def candidate(body: Candidate):
        from .modeling import fit_candidate

        ds = lab.dataset()["id"]
        return task(
            "train_" + body.method,
            lambda: fit_candidate(
                lab,
                ds,
                body.train_end.isoformat(),
                body.method,
                body.components,
                body.reduction,
            ),
        )

    @app.post("/api/lab/evaluations", status_code=202)
    def compare(body: Compare):
        from .evaluation import evaluate

        return task("temporal_evaluation", lambda: evaluate(lab, body.model_ids))

    @app.get("/api/lab/analyses")
    def analyses():
        return {
            "items": [
                json.loads(r["payload"])
                for r in lab.rows("SELECT * FROM analyses ORDER BY rowid DESC LIMIT 50")
            ]
        }

    @app.post("/api/lab/models/active")
    def activate(body: Alias, request: Request):
        from .models import load

        card, _ = load(lab, body.model_id)
        actor = require_role(request, "researcher")["sub"]
        with lab.db() as c:
            previous = c.execute(
                "SELECT model_id FROM model_aliases WHERE alias='active'"
            ).fetchone()
            c.execute(
                "INSERT OR REPLACE INTO model_aliases VALUES('active',?,?)",
                (body.model_id, utcnow()),
            )
            c.execute(
                "INSERT INTO audit(at,actor,action,subject,detail) VALUES(?,?,?,?,?)",
                (
                    utcnow(),
                    actor,
                    "activate_model",
                    body.model_id,
                    canonical({"previous": previous["model_id"] if previous else None}),
                ),
            )
        return {
            "active": body.model_id,
            "train_end": card["train_end"],
            "note": "只影响新运行，既有运行继续使用冻结版本",
        }

    @app.get("/api/lab/registry")
    def registry():
        return {
            "aliases": lab.rows("SELECT * FROM model_aliases"),
            "history": lab.rows(
                "SELECT * FROM audit WHERE action='activate_model' ORDER BY id DESC LIMIT 50"
            ),
        }

    @app.get("/api/lab/admin/audit")
    def audit(request: Request):
        require_role(request, "admin")
        return {"items": lab.rows("SELECT * FROM audit ORDER BY id DESC LIMIT 200")}

    @app.post("/api/lab/research")
    def research(body: ResearchInput, request: Request):
        from .research import create

        return {
            "id": create(
                lab,
                require_role(request, "researcher")["sub"],
                body.model_dump(mode="json"),
            )
        }

    @app.get("/api/lab/research")
    def research_list():
        return {
            "items": [
                {**r, "spec": json.loads(r["spec"]), "result": json.loads(r["result"])}
                for r in lab.rows(
                    "SELECT * FROM research ORDER BY created_at DESC LIMIT 100"
                )
            ]
        }

    @app.post("/api/lab/research/{rid}/decision")
    def decision(rid: str, body: Decision, request: Request):
        actor = require_role(request, "researcher")["sub"]
        with lab.db() as c:
            c.execute("BEGIN IMMEDIATE")
            row = c.execute("SELECT * FROM research WHERE id=?", (rid,)).fetchone()
            if not row:
                raise HTTPException(404, "研究不存在")
            permitted = {
                "proposed": {"approved", "rejected", "cancelled"},
                "approved": {"cancelled"},
                "review": {"accepted", "rejected"},
            }
            if body.status not in permitted.get(row["status"], set()):
                raise ValueError("当前阶段不能执行该操作")
            c.execute(
                "UPDATE research SET status=?,updated_at=? WHERE id=?",
                (body.status, utcnow(), rid),
            )
            c.execute(
                "INSERT INTO audit(at,actor,action,subject,detail) VALUES(?,?,?,?,?)",
                (
                    utcnow(),
                    actor,
                    "research:" + body.status,
                    rid,
                    canonical({"note": body.note}),
                ),
            )
        return {"id": rid, "status": body.status}

    @app.post("/api/lab/research/{rid}/execute", status_code=202)
    def execute(rid: str, request: Request):
        from .research import execute

        actor = require_role(request, "researcher")["sub"]
        row = lab.rows("SELECT status FROM research WHERE id=?", (rid,))
        if not row or row[0]["status"] != "approved":
            raise ValueError("请先批准研究")
        return task("research_tool", lambda: execute(lab, rid, actor))

    @app.get("/api/lab/features/{model_id}")
    def feature_catalog(model_id: str):
        from .models import load

        card, artifact = load(lab, model_id)
        return {
            "version": artifact["feature_version"],
            "items": card["features"],
            "projection": artifact.get("projection"),
            "model_id": model_id,
        }

    @app.get("/api/lab/quality")
    def quality():
        series = lab.series()
        return {
            "dataset": lab.dataset(),
            "series": series,
            "sources": lab.rows("SELECT * FROM sources"),
            "warnings": [
                "未知时区和发布时间尚未认证；时间政策有版本",
                "高收益债缺失；不伪造利差",
                "匿名指标不命名为CPI/PMI",
                "推文互动量属于事后快照，未用于主模型",
            ],
            "layers": {
                "ODS": "MinIO原始归档/原始PDF",
                "DWD": "带来源和单位的规范Parquet/文档",
                "DWS": "Flink版本化特征/MySQL元数据/CH时序",
                "ADS": "状态、预警、研究结果及API",
            },
        }

    @app.post("/api/lab/pdf", status_code=201)
    async def pdf(
        file: UploadFile = File(...),
        source_url: str = Form(""),
        published_at: str = Form(""),
    ):
        data = await file.read(10 * 1024 * 1024 + 1)
        if len(data) > 10 * 1024 * 1024 or not data.startswith(b"%PDF-"):
            raise ValueError("仅接受10MB以内的真实PDF")
        sha = hashlib.sha256(data).hexdigest()
        path = lab.root / "raw" / ("pdf-" + sha + ".pdf")
        path.write_bytes(data)
        result = subprocess.run(
            [
                sys.executable,
                str(Path(__file__).with_name("pdf_extract.py")),
                str(path),
            ],
            capture_output=True,
            text=True,
            timeout=20,
        )
        if result.returncode:
            raise ValueError("PDF解析失败或超出资源限制")
        pages = json.loads(result.stdout)
        ds = lab.dataset()["id"]
        did = identity([ds, sha])
        with lab.db() as c:
            c.execute(
                "INSERT OR IGNORE INTO documents VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    did,
                    ds,
                    "pdf",
                    published_at[:10] if published_at else None,
                    file.filename or "PDF",
                    "\n".join(p["text"] for p in pages),
                    str(path.relative_to(lab.root)),
                    1,
                    canonical(
                        {
                            "sha256": sha,
                            "source_url": source_url[:2000],
                            "published_at_declared": published_at[:100],
                            "publication_verified": False,
                        }
                    ),
                    "C_declared_publication",
                ),
            )
            c.executemany(
                "INSERT OR IGNORE INTO pdf_pages VALUES(?,?,?)",
                [(did, p["page"], p["text"]) for p in pages],
            )
        if os.getenv("CONTEST_MYSQL_HOST"):
            from .distributed import s3

            s3().fput_object("quantos", "documents/" + sha + ".pdf", str(path))
        return {
            "id": did,
            "sha256": sha,
            "pages": len(pages),
            "publication_verified": False,
        }

    @app.get("/api/lab/pdf/{did}/pages")
    def pages(did: str):
        return {
            "items": lab.rows(
                "SELECT page,text FROM pdf_pages WHERE document_id=? ORDER BY page",
                (did,),
            )
        }

    @app.get("/api/lab/pdf/{did}/original")
    def original(did: str):
        rows = lab.rows(
            "SELECT source FROM documents WHERE id=? AND kind='pdf'", (did,)
        )
        if not rows:
            raise HTTPException(404, "PDF不存在")
        path = (lab.root / rows[0]["source"]).resolve()
        if lab.root not in path.parents:
            raise HTTPException(400, "无效对象地址")
        return FileResponse(path, media_type="application/pdf", filename=did + ".pdf")

    @app.get("/api/lab/monitor")
    def monitor():
        from .distributed import infrastructure_status, mysql

        status = infrastructure_status()
        counts = {}
        with mysql() as c, c.cursor() as cur:
            for table in ("outbox", "ch_outbox"):
                cur.execute(f"SELECT sent,COUNT(*) n FROM {table} GROUP BY sent")
                counts[table] = cur.fetchall()
            cur.execute("SELECT role,updated_at FROM worker_health")
            workers = cur.fetchall()
        return {
            **status,
            "outboxes": counts,
            "workers": workers,
            "clock": "historical replay / separate processing wall clock",
            "performance_reports": [
                p.name for p in (lab.root / "reports").glob("performance*.json")
            ],
            "evidence": [
                {"name": p.name, "bytes": p.stat().st_size}
                for p in sorted(evidence_root().glob("*.json"), reverse=True)
                if not p.name.endswith("-local.json")
            ][:100],
        }

    def evidence_root():
        return Path(os.getenv("CONTEST_EVIDENCE_DIR", str(lab.root / "reports")))

    @app.get("/api/lab/reports/{name}")
    def evidence_file(name: str):
        import re

        if not re.fullmatch(r"[A-Za-z0-9_-]+\.json", name) or name.endswith(
            "-local.json"
        ):
            raise HTTPException(404, "报告不存在")
        path = evidence_root() / name
        if not path.is_file() or path.is_symlink():
            raise HTTPException(404, "报告不存在")
        return FileResponse(path, media_type="application/json", filename=name)

    @app.get("/api/lab/admin/llm")
    def llm_config(request: Request):
        require_role(request, "admin")
        from .agent import public_config

        return public_config(lab)

    @app.post("/api/lab/admin/llm")
    async def llm_save(request: Request):
        require_role(request, "admin")
        body = await request.json()
        fields = {
            "base_url",
            "model",
            "api_key",
            "max_calls_per_day",
            "token_parameter",
            "token_budget_per_day",
        }
        if not isinstance(body, dict) or set(body) - fields:
            raise ValueError("模型配置包含未知字段")
        from .agent import save_config

        return save_config(lab, body)

    @app.post("/api/lab/agent/plan", status_code=202)
    async def agent_plan(request: Request):
        actor = require_role(request, "researcher")["sub"]
        body = await request.json()
        if (
            not isinstance(body, dict)
            or set(body) != {"question"}
            or not isinstance(body["question"], str)
            or not 3 <= len(body["question"]) <= 1500
        ):
            raise ValueError("请输入3至1500字的研究问题")
        from .agent import plan

        return task("llm_plan", lambda: plan(lab, body["question"], actor))

    from .graphql_api import router

    app.include_router(router(lab, summary), prefix="/graphql")
    from .events import websocket_events

    app.websocket("/api/lab/events")(websocket_events)
