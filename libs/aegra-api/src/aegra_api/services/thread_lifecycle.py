"""Transaction gate shared by thread writers and checkpoint cleanup."""

from fastapi import HTTPException
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from aegra_api.core.orm import Thread


async def lock_thread(session: AsyncSession, thread_id: str) -> None:
    await session.execute(select(func.pg_advisory_xact_lock(func.hashtextextended(thread_id, 0))))


async def require_writable_thread(session: AsyncSession, thread_id: str, *, user_id: str) -> None:
    await lock_thread(session, thread_id)
    marker = await session.scalar(
        select(Thread.cleanup_run_id).where(Thread.thread_id == thread_id, Thread.user_id == user_id)
    )
    if marker is not None:
        raise HTTPException(409, "thread_cleanup_pending")
