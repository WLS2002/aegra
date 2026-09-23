"""Health check endpoints"""

import asyncio
from collections.abc import Awaitable
from typing import Any, cast

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import text

from aegra_api import __version__
from aegra_api.core.database import db_manager
from aegra_api.core.redis_manager import redis_manager
from aegra_api.models.errors import UNAVAILABLE
from aegra_api.observability.metrics import RUNTIME_BACKLOG
from aegra_api.settings import settings

router = APIRouter(tags=["Health"])


class HealthResponse(BaseModel):
    """Health check response model"""

    status: str = Field(..., description="Overall health status: 'healthy' or 'unhealthy'.")
    database: str = Field(..., description="PostgreSQL connection status.")
    langgraph_checkpointer: str = Field(..., description="Checkpoint backend connection status.")
    langgraph_store: str = Field(..., description="Store backend connection status.")
    redis: str = "disabled"
    warnings: list[str] = Field(default_factory=list)
    runtime: dict[str, float] = Field(default_factory=dict)


class InfoResponse(BaseModel):
    """Info endpoint response model"""

    name: str = Field(..., description="Service name.")
    version: str = Field(..., description="Current server version.")
    description: str = Field(..., description="Service description.")
    status: str = Field(..., description="Current service status.")
    flags: dict = Field(..., description="Feature flags indicating available capabilities.")


@router.get("/info", response_model=InfoResponse)
async def info(_request: Request) -> InfoResponse:
    """Get service information.

    Returns the server name, version, and feature flags. This endpoint does
    not require authentication.
    """
    return InfoResponse(
        name="Aegra",
        version=__version__,
        description="Production-ready Agent Protocol server built on LangGraph",
        status="running",
        flags={"assistants": True, "crons": settings.cron.CRON_ENABLED},
    )


async def _database_probe() -> None:
    if db_manager.engine is None:
        raise RuntimeError("database_not_initialized")
    async with db_manager.engine.connect() as conn:
        await conn.execute(text("SELECT 1"))


async def _checkpoint_probe() -> None:
    await db_manager.get_checkpointer().aget_tuple({"configurable": {"thread_id": "__aegra_health_probe__"}})


async def _store_probe() -> None:
    await db_manager.get_store().aget(("__aegra_health__",), "probe")


async def _probe(operation: Awaitable[Any]) -> str:
    try:
        await asyncio.wait_for(operation, timeout=3)
    except Exception:
        return "unavailable"
    return "connected"


async def _runtime_probe() -> dict[str, float]:
    if db_manager.engine is None:
        raise RuntimeError("database_not_initialized")
    async with db_manager.engine.connect() as conn:
        row = (
            (
                await conn.execute(
                    text("""
            SELECT
              (SELECT count(*) FROM runs WHERE status='pending') AS pending_runs,
              (SELECT COALESCE(EXTRACT(EPOCH FROM now()-min(created_at)),0) FROM runs WHERE status='pending') AS oldest_pending_seconds,
              (SELECT count(*) FROM crons WHERE enabled AND blocked) AS blocked_crons,
              (SELECT count(*) FROM crons WHERE enabled AND NOT blocked AND next_run_date < now()-interval '120 seconds') AS overdue_crons,
              (SELECT count(*) FROM run_wakeups WHERE status='pending' AND due_at < now()-interval '60 seconds') AS overdue_wakeups,
              (SELECT count(*) FROM run_wakeups WHERE status='blocked') AS blocked_wakeups,
              (SELECT count(*) FROM thread WHERE cleanup_run_id IS NOT NULL) AS pending_cleanups
        """)
                )
            )
            .mappings()
            .one()
        )
    return {key: float(value) for key, value in row.items()}


async def _redis_probe() -> str:
    client = redis_manager.get_client()
    await cast(Awaitable[bool], client.ping())
    memory = await client.info("memory")
    limit = int(memory.get("maxmemory", 0))
    used = int(memory.get("used_memory", 0)) - int(memory.get("mem_not_counted_for_evict", 0))
    if limit and used >= limit:
        return "write_limited"
    return "connected"


async def _health_snapshot() -> HealthResponse:
    database, checkpoint, store = await asyncio.gather(
        _probe(_database_probe()), _probe(_checkpoint_probe()), _probe(_store_probe())
    )
    result = HealthResponse(
        status="healthy", database=database, langgraph_checkpointer=checkpoint, langgraph_store=store
    )
    if "unavailable" in (database, checkpoint, store):
        result.status = "unhealthy"
    if settings.redis.REDIS_BROKER_ENABLED:
        try:
            result.redis = await asyncio.wait_for(_redis_probe(), timeout=3)
        except Exception:
            result.redis = "unavailable"
        if result.redis != "connected":
            result.warnings.append(f"redis_{result.redis}_postgres_dispatch_active")
    if database == "connected":
        try:
            result.runtime = await asyncio.wait_for(_runtime_probe(), timeout=3)
        except Exception:
            result.status = "unhealthy"
            result.warnings.append("runtime_schema_unavailable")
        for name, value in result.runtime.items():
            RUNTIME_BACKLOG.labels(kind=name).set(value)
            if (name == "oldest_pending_seconds" and value > 120) or (
                name in {"blocked_crons", "overdue_crons", "overdue_wakeups", "blocked_wakeups"} and value > 0
            ):
                result.warnings.append(name)
    if result.status == "healthy" and result.warnings:
        result.status = "degraded"
    return result


@router.get("/health", response_model=HealthResponse, responses={**UNAVAILABLE})
async def health_check(_request: Request) -> HealthResponse | JSONResponse:
    result = await _health_snapshot()
    if result.status == "unhealthy":
        return JSONResponse(status_code=503, content=result.model_dump())
    return result


@router.get("/ready", response_model=HealthResponse, responses={**UNAVAILABLE})
async def readiness_check(_request: Request) -> HealthResponse | JSONResponse:
    result = await _health_snapshot()
    if result.status == "unhealthy":
        return JSONResponse(status_code=503, content=result.model_dump())
    if result.status == "healthy":
        result.status = "ready"
    return result


@router.get("/live")
async def liveness_check(_request: Request) -> dict[str, str]:
    """Kubernetes liveness probe.

    Always returns 200 to indicate the process is alive. Does not check
    backend connectivity.
    """
    return {"status": "alive"}
