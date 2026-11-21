from __future__ import annotations

import json
import os
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field
from starlette.middleware.trustedhost import TrustedHostMiddleware

from .contracts import ReplayRequest, utcnow
from .ingest import import_archive
from .models import train
from .replay import advance, create_run
from .storage import Lab, ROOT


class TrainRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    train_end: str = Field(default="2022-12-31", pattern=r"^\d{4}-\d{2}-\d{2}$")


class Steps(BaseModel):
    steps: int = Field(default=1, ge=1, le=5000)


class Disposition(BaseModel):
    status: str = Field(pattern=r"^(open|acknowledged|dismissed)$")
    note: str = Field(default="", max_length=2000)


def create_app(lab=None):
    lab = lab or Lab()
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="market-lab")
    slots = threading.BoundedSemaphore(8)
    from .agent import initialize as initialize_agent

    initialize_agent(lab)

    @asynccontextmanager
    async def lifespan(app):
        # Work is never silently re-issued after a restart.
        lab.execute(
            "UPDATE tasks SET status='interrupted' WHERE status IN ('queued','running')"
        )
        if os.getenv("CONTEST_MYSQL_HOST"):
            from .distributed import mysql

            with mysql() as c, c.cursor() as cur:
                cur.execute("SELECT GET_LOCK('quantos_stream_publisher',0) acquired")
                if cur.fetchone()["acquired"]:
                    cur.execute(
                        "UPDATE stream_runs SET status='publish_failed',error='Publisher interrupted; resume explicitly.' WHERE status IN ('publishing','pause_requested')"
                    )
                    c.commit()
                    cur.execute("SELECT RELEASE_LOCK('quantos_stream_publisher')")
        yield
        executor.shutdown(wait=True, cancel_futures=True)

    app = FastAPI(
        title="Market State Lab",
        version="1.0.0-rc1",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
    )
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=["127.0.0.1", "localhost", "testserver", "data-api"],
    )

    @app.middleware("http")
    async def origin_guard(request, call_next):
        origin = request.headers.get("origin")
        if request.method not in ("GET", "HEAD", "OPTIONS") and origin:
            from urllib.parse import urlsplit

            if urlsplit(origin).netloc != request.headers.get(
                "x-forwarded-host", request.headers.get("host")
            ):
                return JSONResponse({"detail": "跨来源写操作被拒绝"}, status_code=403)
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "same-origin"
        return response

    @app.exception_handler(ValueError)
    async def invalid(request, exc):
        return JSONResponse({"detail": str(exc)}, status_code=400)

    def task(kind, function):
        if not slots.acquire(blocking=False):
            raise HTTPException(429, "后台队列已满，请等待任务完成")
        tid = uuid.uuid4().hex[:24]
        lab.task(tid, kind, "queued", {})

        def run():
            lab.task(tid, kind, "running", {})
            try:
                lab.task(tid, kind, "completed", function())
            except Exception as exc:
                lab.task(tid, kind, "failed", {"error": str(exc)})
            finally:
                slots.release()

        executor.submit(run)
        return {"task_id": tid}

    @app.get("/health")
    def health():
        return {
            "status": "ok",
            "mode": "contest-lab",
            "version": "0.1.0",
            "llm_calls": lab.rows("SELECT COUNT(*) n FROM agent_calls")[0]["n"],
        }

    @app.get("/api/lab/summary")
    def summary():
        datasets = [
            {**json.loads(r["manifest"]), "status": r["status"]}
            for r in lab.rows("SELECT * FROM datasets ORDER BY rowid DESC")
        ]
        ds = datasets[0]["id"] if datasets else None
        return {
            "datasets": datasets,
            "series": lab.series(ds) if ds else [],
            "sources": (
                lab.rows("SELECT * FROM sources WHERE dataset_id=?", (ds,))
                if ds
                else []
            ),
            "models": [
                {
                    k: v
                    for k, v in json.loads(r["card"]).items()
                    if k
                    in {
                        "id",
                        "dataset_id",
                        "train_end",
                        "train_start",
                        "train_rows",
                        "method",
                        "components",
                        "reduction",
                        "latent_dimensions",
                        "converged",
                        "created_at",
                        "artifact_hash",
                        "limitations",
                    }
                }
                for r in lab.rows("SELECT * FROM models ORDER BY rowid DESC LIMIT 100")
            ],
            "active_model": (
                lab.rows("SELECT model_id FROM model_aliases WHERE alias='active'")
                or [{}]
            )[0].get("model_id"),
            "runs": [
                {**r, "spec": json.loads(r["spec"])}
                for r in lab.rows(
                    "SELECT * FROM runs ORDER BY created_at DESC LIMIT 100"
                )
            ],
            "tasks": [
                {
                    **r,
                    "detail": {
                        k: v
                        for k, v in json.loads(r["detail"]).items()
                        if k
                        in {
                            "id",
                            "error",
                            "model_id",
                            "research_id",
                            "call_id",
                            "usage",
                            "status",
                            "method",
                        }
                    },
                }
                for r in lab.rows(
                    "SELECT * FROM tasks ORDER BY updated_at DESC LIMIT 20"
                )
            ],
            "clock": "历史回放，日期+2自然日UTC为重建假设",
            "llm_calls": lab.rows("SELECT COUNT(*) n FROM agent_calls")[0]["n"],
            "server_time": utcnow(),
            "deployment": (
                "distributed"
                if os.getenv("CONTEST_MYSQL_HOST")
                else "offline_reference"
            ),
        }

    @app.get("/api/lab/tasks/{tid}")
    def task_detail(tid: str):
        rows = lab.rows("SELECT * FROM tasks WHERE id=?", (tid,))
        if not rows:
            raise HTTPException(404, "任务不存在")
        return {**rows[0], "detail": json.loads(rows[0]["detail"])}

    @app.post("/api/lab/import", status_code=202)
    def ingest():
        input_dir = Path("/input") if Path("/input").exists() else ROOT / "data"
        archives = list(input_dir.glob("*.zip"))
        if len(archives) != 1:
            raise ValueError("data目录应存在唯一官方ZIP；不支持任意服务器文件路径")
        return task("official_import", lambda: import_archive(lab, archives[0]))

    @app.post("/api/lab/train", status_code=202)
    def training(body: TrainRequest):
        from datetime import date

        date.fromisoformat(body.train_end)
        ds = lab.dataset()["id"]
        return task("train_gmm", lambda: train(lab, ds, body.train_end))

    @app.get("/api/lab/series/{sid}/observations")
    def observations(sid: str, limit: int = 100, offset: int = 0):
        if not 1 <= limit <= 500 or offset < 0:
            raise ValueError("invalid pagination")
        s = next((s for s in lab.series() if s["id"] == sid), None)
        if not s or s["status"] != "ready":
            raise HTTPException(404, "资产数据不可用")
        frame = pd.read_parquet(lab.root / s["path"])
        return {
            "series": s,
            "total": len(frame),
            "items": frame.iloc[offset : offset + limit].to_dict("records"),
        }

    @app.get("/api/lab/evidence/{event_id}")
    def evidence(event_id: str):
        if len(event_id) != 64:
            raise HTTPException(404, "证据不存在")
        for s in lab.series():
            if s["status"] != "ready":
                continue
            df = pd.read_parquet(
                lab.root / s["path"], filters=[("event_id", "==", event_id)]
            )
            if not df.empty:
                record = df.iloc[0].to_dict()
                record["quality_flags"] = record["quality_flags"].tolist()
                return {
                    "observation": record,
                    "series": s,
                    "window_definition": "计算使用本条及此前20条观测；未使用之后的数据",
                }
        raise HTTPException(404, "证据不存在")

    @app.get("/api/lab/documents")
    def documents(
        q: str = "", kind: str = "", day: str = "", limit: int = 30, offset: int = 0
    ):
        if not 1 <= limit <= 100 or offset < 0 or len(q) > 100:
            raise ValueError("invalid query")
        sql = "FROM documents WHERE dataset_id=?"
        params = [lab.dataset()["id"]]
        if q:
            sql += " AND title LIKE ?"
            params.append("%" + q + "%")
        if kind:
            sql += " AND kind=?"
            params.append(kind)
        if day:
            sql += " AND day=?"
            params.append(day)
        return {
            "items": lab.rows(
                "SELECT id,kind,day,title,source,source_row,trust "
                + sql
                + " ORDER BY day DESC LIMIT ? OFFSET ?",
                (*params, limit, offset),
            )
        }

    @app.get("/api/lab/documents/{did}")
    def document(did: str):
        rows = lab.rows("SELECT * FROM documents WHERE id=?", (did,))
        if not rows:
            raise HTTPException(404, "文档不存在")
        return rows[0]  # Frontend renders text only; never executes supplied HTML.

    @app.post("/api/lab/runs")
    def new_run(body: ReplayRequest):
        return {"id": create_run(lab, body)}

    @app.post("/api/lab/runs/{rid}/advance", status_code=202)
    def step(rid: str, body: Steps):
        if not lab.rows("SELECT id FROM runs WHERE id=?", (rid,)):
            raise HTTPException(404, "回放不存在")
        return task("replay", lambda: advance(lab, rid, body.steps))

    @app.get("/api/lab/runs/{rid}/results")
    def results(rid: str, limit: int = 200, offset: int = 0):
        if not 1 <= limit <= 5000 or offset < 0:
            raise ValueError("invalid pagination")
        return {
            "items": [
                json.loads(r["payload"])
                for r in lab.rows(
                    "SELECT payload FROM results WHERE run_id=? ORDER BY day LIMIT ? OFFSET ?",
                    (rid, limit, offset),
                )
            ],
            "alerts": [
                {**r, "payload": json.loads(r["payload"])}
                for r in lab.rows(
                    "SELECT * FROM alerts WHERE run_id=? ORDER BY day", (rid,)
                )
            ],
        }

    @app.post("/api/lab/alerts/{aid}")
    def disposition(aid: str, body: Disposition):
        count = lab.execute(
            "UPDATE alerts SET disposition=?,note=? WHERE id=?",
            (body.status, body.note, aid),
        )
        if not count:
            raise HTTPException(404, "预警不存在")
        return {"id": aid, **body.model_dump()}

    @app.get("/api/lab/stream")
    def stream_status():
        from .distributed import stream_rows, infrastructure_status

        try:
            return {**infrastructure_status(), "runs": stream_rows("stream_runs")}
        except Exception:
            return {
                "available": False,
                "reason": "分布式服务未连接；当前为离线参照路径",
            }

    @app.post("/api/lab/stream", status_code=202)
    def stream_start(body: ReplayRequest):
        from .distributed import start_stream, publish_isolated, infrastructure_status

        if not infrastructure_status()["ready"]:
            raise ValueError("计算链路尚未就绪，请检查组件状态后重试")

        # Reserve the immutable run before returning so the UI can subscribe
        # immediately. If the bounded queue rejects, leave an explicit resumable
        # publisher failure rather than an invisible orphan.
        spec = start_stream(
            lab,
            body.model_id,
            body.start.isoformat(),
            body.end.isoformat(),
            body.warning_threshold,
            body.cooldown_days,
            rate_per_second=body.rate_per_second,
        )

        def start():
            publish_isolated(lab, spec["id"])
            return spec

        try:
            queued = task("distributed_replay", start)
        except Exception:
            from .distributed import mysql

            with mysql() as c, c.cursor() as cur:
                cur.execute(
                    "UPDATE stream_runs SET status='publish_failed',error='Task queue rejected; resume explicitly.' WHERE id=%s",
                    (spec["id"],),
                )
                c.commit()
            raise
        return {**queued, "run_id": spec["id"]}

    @app.get("/api/lab/stream/{rid}")
    def stream_results(rid: str):
        from .distributed import stream_rows, hot_state

        return {
            "items": stream_rows("stream_results", rid, 5000),
            "alerts": stream_rows("stream_alerts", rid, 5000),
            "latest": hot_state(rid),
        }

    @app.get("/api/lab/stream/{rid}/progress")
    def stream_progress(rid: str):
        from .distributed import stream_rows

        rows = stream_rows("stream_runs", rid, 1)
        if not rows:
            raise HTTPException(404, "分布式运行不存在")
        return rows[0]

    @app.post("/api/lab/stream/{rid}/pause")
    def pause_stream(rid: str):
        from .distributed import mysql

        with mysql() as c, c.cursor() as cur:
            cur.execute(
                "UPDATE stream_runs SET status='pause_requested' WHERE id=%s AND status='publishing'",
                (rid,),
            )
            c.commit()
        return {
            "id": rid,
            "status": "pause_requested",
            "note": "在当前批次安全落盘后暂停",
        }

    @app.post("/api/lab/stream/{rid}/resume", status_code=202)
    def resume_stream(rid: str):
        from .distributed import mysql, publish_isolated

        with mysql() as c, c.cursor() as cur:
            cur.execute("SELECT status FROM stream_runs WHERE id=%s FOR UPDATE", (rid,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(404, "分布式回放不存在")
            if row["status"] not in ("paused", "publish_failed"):
                raise ValueError("该运行未暂停或失败，不能重复启动发布")
            cur.execute(
                "UPDATE stream_runs SET status='publishing' WHERE id=%s", (rid,)
            )
            c.commit()
        try:
            return task(
                "distributed_resume", lambda: publish_isolated(lab, rid) or {"id": rid}
            )
        except Exception:
            with mysql() as c, c.cursor() as cur:
                cur.execute(
                    "UPDATE stream_runs SET status='publish_failed',error='Task queue rejected; resume explicitly.' WHERE id=%s",
                    (rid,),
                )
                c.commit()
            raise

    @app.post("/api/lab/stream-alerts/{aid}")
    def stream_disposition(aid: str, body: Disposition):
        from .distributed import mysql

        with mysql() as c, c.cursor() as cur:
            cur.execute(
                "UPDATE stream_alerts SET disposition=%s,note=%s WHERE id=%s",
                (body.status, body.note, aid),
            )
            if not cur.rowcount:
                cur.execute("SELECT id FROM stream_alerts WHERE id=%s", (aid,))
                if not cur.fetchone():
                    raise HTTPException(404, "分布式预警不存在")
            c.commit()
        return {"id": aid, **body.model_dump()}

    dist = ROOT / "frontend" / "dist"
    if (dist / "assets").exists():
        app.mount("/assets", StaticFiles(directory=dist / "assets"), name="assets")

    @app.get("/")
    def root():
        return RedirectResponse("/lab")

    @app.get("/lab")
    def index():
        if not (dist / "index.html").exists():
            raise HTTPException(503, "请先在frontend运行npm run build")
        return FileResponse(dist / "index.html", media_type="text/html")

    from .release_api import install

    install(app, lab, summary, task)
    from fastapi.openapi.docs import get_swagger_ui_html

    if (dist / "vendor").exists():
        app.mount(
            "/vendor", StaticFiles(directory=dist / "vendor"), name="api-doc-assets"
        )

    @app.get("/docs", include_in_schema=False)
    def docs():
        return get_swagger_ui_html(
            openapi_url="/openapi.json",
            title="Market State Lab API",
            swagger_js_url="/vendor/swagger-ui-bundle.js",
            swagger_css_url="/vendor/swagger-ui.css",
        )

    return app


app = create_app()
