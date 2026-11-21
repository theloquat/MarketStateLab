"""Actual official-data pipeline latency under accelerated replay, no paid calls."""

import asyncio
import os
import json
import time
from pathlib import Path
import httpx
import numpy as np
import websockets

ROOT = Path(__file__).resolve().parents[1]
BASE = "http://127.0.0.1:18080"


def quantiles(values):
    return (
        {
            k: float(np.percentile(values, p))
            for k, p in [("p50_ms", 50), ("p95_ms", 95), ("p99_ms", 99)]
        }
        if values
        else {}
    )


async def main():
    password = next(
        r
        for r in (ROOT / ".quantos-contest/access/credentials.txt")
        .read_text()
        .splitlines()
        if r.startswith("admin ")
    ).split()[1]
    async with httpx.AsyncClient(base_url=BASE, timeout=60) as c:
        login = await c.post(
            "/auth/login", json={"username": "admin", "password": password}
        )
        login.raise_for_status()
        token = login.json()["access_token"]
        summary_response = await c.get("/api/lab/summary")
        summary_response.raise_for_status()
        summary = summary_response.json()
        models = summary["models"]
        if not models:
            raise RuntimeError("先导入官方数据并训练模型")
        model_id = os.getenv("BENCH_MODEL_ID") or next(
            (m["id"] for m in models if m["method"] == "hmm"), models[0]["id"]
        )
        spec = {
            "model_id": model_id,
            "start": "2023-01-01",
            "end": "2026-05-02",
            "warning_threshold": 0.75,
            "cooldown_days": 5,
        }
        reference = next(
            (
                r
                for r in summary["runs"]
                if all(r["spec"].get(k) == v for k, v in spec.items())
            ),
            None,
        )
        if reference is None:
            response = await c.post("/api/lab/runs", json=spec)
            response.raise_for_status()
            reference = {"id": response.json()["id"], "status": "created"}
        if reference["status"] != "completed":
            response = await c.post(
                "/api/lab/runs/" + reference["id"] + "/advance", json={"steps": 5000}
            )
            response.raise_for_status()
            reference_task = response.json()["task_id"]
            for _ in range(600):
                row = (await c.get("/api/lab/tasks/" + reference_task)).json()
                if row["status"] == "completed":
                    break
                if row["status"] not in ("queued", "running"):
                    raise RuntimeError("Offline reference failed")
                await asyncio.sleep(1)
            else:
                raise TimeoutError("Offline reference deadline")
        response = await c.get(
            "/api/lab/runs/" + reference["id"] + "/results?limit=5000"
        )
        response.raise_for_status()
        expected = {row["day"]: row for row in response.json()["items"]}
        if not expected:
            raise RuntimeError("Empty offline reference")
        async with websockets.connect(
            BASE.replace("http", "ws", 1) + "/api/lab/events",
            additional_headers={"Authorization": "Bearer " + token},
            origin=BASE,
            open_timeout=10,
            max_queue=2048,
        ) as socket:
            await socket.recv()
            response = await c.post(
                "/api/lab/stream",
                json={
                    **spec,
                    "rate_per_second": int(os.getenv("BENCH_RATE", "0")),
                },
            )
            response.raise_for_status()
            tid = response.json()["task_id"]
            run_id = response.json()["run_id"]
            results = {}
            started = time.monotonic()
            last = time.monotonic()
            rid = run_id
            while time.monotonic() - started < 240:
                try:
                    d = json.loads(await asyncio.wait_for(socket.recv(), 3))
                    if d.get("type") != "state":
                        continue
                    state = d["payload"]
                    candidate = state.get("run_id")
                    if candidate != run_id:
                        continue
                    rid = candidate
                    timing = state.get("timing", {})
                    results[d["event_id"]] = {
                        "day": state["day"],
                        "state_kafka_queue_ms": (
                            (
                                timing["state_received_at_ns"]
                                - timing["inference_finished_at_ns"]
                            )
                            / 1e6
                            if timing.get("state_received_at_ns")
                            else None
                        ),
                        "state_preparation_ms": (
                            (
                                timing["state_prepared_at_ns"]
                                - timing["state_received_at_ns"]
                            )
                            / 1e6
                            if timing.get("state_received_at_ns")
                            else None
                        ),
                        "state_persist_and_relay_ms": (
                            (d["published_at_ns"] - timing["state_prepared_at_ns"])
                            / 1e6
                            if timing.get("state_prepared_at_ns")
                            else None
                        ),
                        "redis_ws_client_ms": (
                            (time.time_ns() - d["published_at_ns"]) / 1e6
                            if d.get("published_at_ns")
                            else None
                        ),
                        "e2e_ms": (
                            (time.time_ns() - timing["window_triggered_at_ns"]) / 1e6
                            if timing.get("window_triggered_at_ns")
                            else None
                        ),
                        "feature_ms": (
                            (
                                timing["features_completed_at_ns"]
                                - timing["window_triggered_at_ns"]
                            )
                            / 1e6
                            if timing.get("features_completed_at_ns")
                            else None
                        ),
                        "inference_ms": timing.get("inference_ms"),
                        "kafka_feature_queue_ms": (
                            (
                                timing["feature_received_at_ns"]
                                - timing["features_completed_at_ns"]
                            )
                            / 1e6
                            if timing.get("feature_received_at_ns")
                            else None
                        ),
                        "model_and_db_queue_ms": (
                            (
                                timing["inference_finished_at_ns"]
                                - timing["feature_received_at_ns"]
                            )
                            / 1e6
                            if timing.get("feature_received_at_ns")
                            else None
                        ),
                        "after_inference_to_client_ms": (
                            (time.time_ns() - timing["inference_finished_at_ns"]) / 1e6
                            if timing.get("inference_finished_at_ns")
                            else None
                        ),
                    }
                    last = time.monotonic()
                    if len(results) == len(expected):
                        break
                except asyncio.TimeoutError:
                    task = next(
                        t
                        for t in (await c.get("/api/lab/summary")).json()["tasks"]
                        if t["id"] == tid
                    )
                    if task["status"] == "failed":
                        raise RuntimeError(task["detail"].get("error", "task failed"))
                    if rid and time.monotonic() - last > 10:
                        run = next(
                            r
                            for r in (await c.get("/api/lab/stream")).json()["runs"]
                            if r["id"] == rid
                        )
                        if run["status"] == "features_completed":
                            break
            parity = None
            if rid and len(results) == len(expected):
                actual_response = await c.get("/api/lab/stream/" + rid)
                actual_response.raise_for_status()
                actual = {
                    row["day"]: row["payload"]
                    for row in actual_response.json()["items"]
                }
                if actual.keys() != expected.keys():
                    raise ValueError("batch/stream days differ")
                difference = max(
                    abs(
                        actual[day]["probabilities"][key]
                        - expected[day]["probabilities"][key]
                    )
                    for day in expected
                    for key in expected[day]["probabilities"]
                )
                if difference > 1e-8:
                    raise ValueError("batch/stream probability mismatch")
                parity = {
                    "reference": reference["id"],
                    "max_probability_difference": difference,
                }
            progress = (await c.get("/api/lab/stream/" + run_id + "/progress")).json()
            input_records = progress["payload"]["input_records"]
            report = {
                "run_id": rid,
                "model_id": model_id,
                "input_records": input_records,
                "scenario": os.getenv(
                    "BENCH_SCENARIO", "independent maximum-speed replay"
                ),
                "configured_input_rate": int(os.getenv("BENCH_RATE", "0")),
                "batch_stream_parity": parity,
                "effective_observations_per_second": input_records
                / (time.monotonic() - started),
                "throughput_scope": "one complete official-observation replay including setup/warmup/processing; not a sustained capacity upper-bound test",
                "task_id": tid,
                "received_unique_states": len(results),
                "expected_states": len(expected),
                "seconds": time.monotonic() - started,
                "scope": "actual official historical replay at configured input rate (0=unpaced); end-to-end window-trigger in Flink -> calculation -> Kafka -> model/MySQL -> outbox/Kafka/Redis -> WS/Java -> client; excludes event-time day waiting and source ingress before watermark",
                "end_to_end": quantiles(
                    [r["e2e_ms"] for r in results.values() if r["e2e_ms"] is not None]
                ),
                "feature": quantiles(
                    [
                        r["feature_ms"]
                        for r in results.values()
                        if r["feature_ms"] is not None
                    ]
                ),
                "inference": quantiles(
                    [
                        r["inference_ms"]
                        for r in results.values()
                        if r["inference_ms"] is not None
                    ]
                ),
                "phases": {
                    key: quantiles(
                        [r[key] for r in results.values() if r.get(key) is not None]
                    )
                    for key in [
                        "state_kafka_queue_ms",
                        "state_preparation_ms",
                        "state_persist_and_relay_ms",
                        "redis_ws_client_ms",
                    ]
                },
                "snapshots": list(results.values()),
                "paid_calls": 0,
            }
            path = ROOT / (
                "docs/contest/performance-pipeline-"
                + time.strftime("%Y%m%d-%H%M%S")
                + ".json"
            )
            path.write_text(json.dumps(report, indent=2) + "\n")
            print(
                json.dumps(
                    {k: v for k, v in report.items() if k != "snapshots"}, indent=2
                )
            )
            if len(results) != len(expected) or parity is None:
                raise RuntimeError("Incomplete measured delivery; raw report retained")


if __name__ == "__main__":
    asyncio.run(main())
