"""Authenticated event-driven push. Redis PubSub transports committed domain events."""

from __future__ import annotations
import asyncio
import json
import os
import time
from fastapi import WebSocket, WebSocketDisconnect
from .security import verify


async def websocket_events(ws: WebSocket):
    from redis.asyncio import Redis
    from .security import token_from

    origin = ws.headers.get("origin", "")
    host = ws.headers.get("x-forwarded-host", ws.headers.get("host"))
    from urllib.parse import urlsplit

    if origin and urlsplit(origin).netloc != host:
        await ws.close(code=1008)
        return
    if os.getenv("CONTEST_AUTH_REQUIRED") == "1":
        import hmac

        if not hmac.compare_digest(
            ws.headers.get("x-internal-token", ""), os.environ["CONTEST_INTERNAL_TOKEN"]
        ):
            await ws.close(code=1008)
            return
        try:
            claims = verify(token_from(ws), os.environ["CONTEST_JWT_SECRET"])
        except Exception:
            await ws.close(code=1008)
            return
    else:
        claims = {"exp": time.time() + 1800, "jti": "test"}
    client = Redis(
        host=os.getenv("CONTEST_REDIS_HOST", "127.0.0.1"),
        port=int(os.getenv("CONTEST_REDIS_PORT", "6379")),
    )
    pubsub = client.pubsub()
    await pubsub.subscribe("quantos:events")
    await ws.accept()

    async def watch_disconnect():
        while True:
            message = await ws.receive()
            if message["type"] == "websocket.disconnect":
                return

    disconnected = asyncio.create_task(watch_disconnect())
    try:
        await ws.send_json(
            {
                "type": "connected",
                "transport": "redis-pubsub",
                "historical_replay": True,
            }
        )
        while not disconnected.done():
            if claims["exp"] <= time.time() or await client.exists(
                "quantos:revoked:" + claims["jti"]
            ):
                await ws.close(code=1008)
                break
            message = await pubsub.get_message(
                ignore_subscribe_messages=True, timeout=1
            )
            if message:
                data = json.loads(message["data"])
                data["browser_push_at_ns"] = time.time_ns()
                await asyncio.wait_for(ws.send_json(data), timeout=5)
            else:
                await asyncio.sleep(0.05)
    except (WebSocketDisconnect, RuntimeError, asyncio.TimeoutError):
        pass
    finally:
        disconnected.cancel()
        await asyncio.gather(disconnected, return_exceptions=True)
        await pubsub.aclose()
        await client.aclose()
