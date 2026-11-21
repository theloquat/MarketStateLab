"""Independent feature/model and alert HTTP services with supervised consumers."""

from __future__ import annotations
import json
import logging
import os
import threading
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from .security import require_role


def create_service(role):
    stop = threading.Event()

    @asynccontextmanager
    async def lifespan(app):
        from .distributed import consume

        def run():
            delay = 1
            while not stop.is_set():
                try:
                    profile = None
                    if os.getenv("CONTEST_PROFILE") == "1":
                        import cProfile

                        profile = cProfile.Profile()
                        profile.enable()
                    try:
                        consume("features" if role == "features" else "alerts", stop)
                    finally:
                        if profile:
                            profile.disable()
                            profile.dump_stats(
                                str(
                                    __import__("pathlib").Path(
                                        os.environ["QUANTOS_CONTEST_DIR"]
                                    )
                                    / "reports"
                                    / ("profile-" + role + ".prof")
                                )
                            )
                    delay = 1
                except Exception:
                    logging.exception(
                        "%s consumer interrupted; retry in %ss", role, delay
                    )
                    stop.wait(delay)
                    delay = min(delay * 2, 30)

        worker = threading.Thread(target=run, name=role + "-consumer", daemon=True)
        worker.start()
        yield
        stop.set()
        worker.join(timeout=10)

    app = FastAPI(title="Market State Lab " + role, version="1.0.0", lifespan=lifespan)

    @app.middleware("http")
    async def auth(request, call_next):
        if request.url.path != "/health":
            import hmac

            if not hmac.compare_digest(
                request.headers.get("x-internal-token", ""),
                os.environ.get("CONTEST_INTERNAL_TOKEN", "not-configured"),
            ):
                return JSONResponse({"detail": "internal gateway required"}, 403)
            try:
                require_role(request, "viewer")
            except HTTPException as e:
                return JSONResponse({"detail": e.detail}, e.status_code)
        return await call_next(request)

    @app.get("/health")
    def health():
        return {"status": "ok", "service": role}

    @app.get("/api/features/latest/{run_id}")
    def latest(run_id: str):
        from .distributed import hot_state

        state = hot_state(run_id)
        if state is None:
            raise HTTPException(404, "状态不存在")
        return state

    @app.get("/api/features/catalog/{model_id}")
    def catalog(model_id: str):
        from .distributed import mysql

        with mysql() as c, c.cursor() as cur:
            cur.execute(
                "SELECT payload FROM lab_meta WHERE id=%s", ("model:" + model_id,)
            )
            row = cur.fetchone()
        if not row:
            raise HTTPException(404, "模型尚未发布至分布式注册表")
        return {"model_id": model_id, "items": json.loads(row["payload"])["features"]}

    @app.get("/api/models/{model_id}")
    def model(model_id: str):
        from .distributed import mysql

        with mysql() as c, c.cursor() as cur:
            cur.execute(
                "SELECT payload FROM lab_meta WHERE id=%s", ("model:" + model_id,)
            )
            row = cur.fetchone()
        if not row:
            raise HTTPException(404, "模型未发布")
        return json.loads(row["payload"])

    @app.get("/api/alerts/{run_id}")
    def alerts(run_id: str, limit: int = 100):
        from .distributed import stream_rows

        if not 1 <= limit <= 500:
            raise HTTPException(400, "invalid limit")
        return {"items": stream_rows("stream_alerts", run_id, limit)}

    return app


app = create_service(os.getenv("CONTEST_SERVICE_ROLE", "features"))
