"""120 concurrent authenticated WS connections and genuine Redis hot-state reads."""

import asyncio
import json
import sys
import time
import uuid
from pathlib import Path
import numpy as np
import redis.asyncio as redis
import websockets

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))
from marketlab.security import issue  # noqa: E402


def quantiles(values):
    return (
        {f"p{p}_ms": float(np.percentile(values, p)) for p in (50, 95, 99)}
        if values
        else {}
    )


async def main():
    env = dict(
        line.split("=", 1)
        for line in (ROOT / "deploy/contest/secrets.env").read_text().splitlines()
        if "=" in line
    )
    client = redis.Redis(host="127.0.0.1", port=16379)
    key = None
    async for candidate in client.scan_iter(match="quantos:state:*", count=100):
        key = candidate
        break
    if key is None:
        raise RuntimeError("先完成一个真实分布式回放")
    payload = await client.get(key)
    day = json.loads(payload)["day"]
    reads = []
    for _ in range(1000):
        started = time.perf_counter_ns()
        assert json.loads(await client.get(key))["day"] == day
        reads.append((time.perf_counter_ns() - started) / 1e6)
    scope = uuid.uuid4().hex
    ready = 0
    barrier = asyncio.Event()
    stopped = asyncio.Event()
    latencies, errors = [], []
    counts = [0] * 120

    async def listen(i):
        nonlocal ready
        try:
            token = issue(
                "ws-acceptance-" + str(i), "viewer", env["CONTEST_JWT_SECRET"]
            )
            async with websockets.connect(
                "ws://127.0.0.1:18080/api/lab/events",
                origin="http://127.0.0.1:18080",
                additional_headers={"Authorization": "Bearer " + token},
                open_timeout=20,
                max_queue=64,
            ) as ws:
                assert (
                    json.loads(await asyncio.wait_for(ws.recv(), 10))["type"]
                    == "connected"
                )
                ready += 1
                if ready == 120:
                    barrier.set()
                while not stopped.is_set():
                    try:
                        event = json.loads(await asyncio.wait_for(ws.recv(), 1))
                    except asyncio.TimeoutError:
                        continue
                    if event.get("benchmark_scope") == scope:
                        latencies.append(
                            (time.time_ns() - event["benchmark_emit_ns"]) / 1e6
                        )
                        counts[i] += 1
        except Exception as error:
            errors.append(type(error).__name__)

    tasks = [asyncio.create_task(listen(i)) for i in range(120)]
    try:
        await asyncio.wait_for(barrier.wait(), 30)
        for n in range(20):
            event = {
                "type": "benchmark",
                "synthetic_capacity_event": True,
                "benchmark_scope": scope,
                "sequence": n,
                "benchmark_emit_ns": time.time_ns(),
                "sample_real_state": json.loads(payload),
            }
            await client.publish("quantos:events", json.dumps(event))
            await asyncio.sleep(0.25)
        await asyncio.sleep(1)
    finally:
        stopped.set()
        await asyncio.gather(*tasks)
        await client.aclose()
    result = {
        "redis": {
            "reads": len(reads),
            "payload_bytes": len(payload),
            "scope": "actual hot state GET plus JSON decode from host over Docker port",
            **quantiles(reads),
        },
        "websocket": {
            "connections": ready,
            "messages": len(latencies),
            "expected_messages": 2400,
            "minimum_messages_per_user": min(counts),
            "errors": errors,
            **quantiles(latencies),
            "scope": "synthetic transport envelope containing genuine state-sized payload; Redis publish -> Python/Java WS -> host clients; excludes model/SQL/Kafka",
        },
        "paid_calls": 0,
    }
    destination = (
        ROOT
        / "docs/contest"
        / ("performance-edges-" + time.strftime("%Y%m%d-%H%M%S") + ".json")
    )
    destination.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    if ready != 120 or min(counts) != 20 or errors:
        raise AssertionError("Incomplete WebSocket delivery")


if __name__ == "__main__":
    asyncio.run(main())
