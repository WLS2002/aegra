"""Transactional request deduplication, independent of graph business semantics."""

import hashlib
import json

from fastapi import HTTPException
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from aegra_api.core.orm import Run as RunORM
from aegra_api.core.orm import RunRequest
from aegra_api.models import RunCreate


def request_digest(request: RunCreate) -> str:
    payload = request.model_dump(mode="json")
    try:
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    except ValueError as exc:
        raise HTTPException(422, "Idempotent run parameters must be finite JSON values") from exc
    return hashlib.sha256(encoded.encode()).hexdigest()


async def find_request(session: AsyncSession, *, user_id: str, thread_id: str, key: str, digest: str) -> RunORM | None:
    if not key.strip() or len(key) > 200:
        raise HTTPException(422, "Idempotency-Key must contain 1–200 nonblank characters")
    scope = json.dumps([user_id, thread_id, key], separators=(",", ":"))
    await session.execute(select(func.pg_advisory_xact_lock(func.hashtextextended(scope, 0))))
    saved = await session.scalar(
        select(RunRequest).where(
            RunRequest.user_id == user_id, RunRequest.thread_id == thread_id, RunRequest.key == key
        )
    )
    if saved is None:
        return None
    if saved.request_hash != digest:
        raise HTTPException(409, "Idempotency-Key was already used with different run parameters")
    run = await session.scalar(select(RunORM).where(RunORM.run_id == saved.run_id, RunORM.user_id == user_id))
    if run is None:
        raise HTTPException(409, "The original run is no longer available")
    return run
