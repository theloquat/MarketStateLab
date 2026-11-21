from __future__ import annotations
import json
import strawberry
from strawberry.fastapi import GraphQLRouter
from strawberry.scalars import JSON
from strawberry.extensions import QueryDepthLimiter, MaxTokensLimiter, MaxAliasesLimiter
from starlette.concurrency import run_in_threadpool


@strawberry.type
class Model:
    id: str
    method: str
    train_end: str
    components: int
    reduction: str


@strawberry.type
class Feature:
    key: str
    name: str
    measure: str
    unit: str
    definition: str


def router(lab, summary):
    @strawberry.type
    class Query:
        @strawberry.field
        async def dashboard(self) -> JSON:
            return await run_in_threadpool(summary)

        @strawberry.field
        async def models(self) -> list[Model]:
            cards = await run_in_threadpool(
                lab.rows, "SELECT card FROM models ORDER BY rowid DESC LIMIT 100"
            )
            return [
                Model(
                    id=(c := json.loads(r["card"]))["id"],
                    method=c["method"],
                    train_end=c["train_end"],
                    components=c.get("components", 3),
                    reduction=c.get("reduction", "none"),
                )
                for r in cards
            ]

        @strawberry.field
        async def feature_catalog(self, model_id: str) -> list[Feature]:
            from .models import load

            card, _ = await run_in_threadpool(load, lab, model_id)
            return [
                Feature(
                    **{
                        k: f[k]
                        for k in ("key", "name", "measure", "unit", "definition")
                    }
                )
                for f in card["features"]
            ]

        @strawberry.field
        async def states(self, run_id: str, limit: int = 100) -> JSON:
            if not 1 <= limit <= 500:
                raise ValueError("limit must be 1..500")
            rows = await run_in_threadpool(
                lab.rows,
                "SELECT payload FROM results WHERE run_id=? ORDER BY day DESC LIMIT ?",
                (run_id, limit),
            )
            return [json.loads(r["payload"]) for r in rows]

    schema = strawberry.Schema(
        query=Query,
        extensions=[
            lambda: QueryDepthLimiter(max_depth=8),
            lambda: MaxTokensLimiter(max_token_count=600),
            lambda: MaxAliasesLimiter(max_alias_count=10),
        ],
    )
    return GraphQLRouter(schema, graphql_ide=None, allow_queries_via_get=False)
