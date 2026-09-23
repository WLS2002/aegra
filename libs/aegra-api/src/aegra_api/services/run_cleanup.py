"""Durable, conservative cleanup for ephemeral threads."""

import asyncio
import contextlib

import structlog
from psycopg import Error as PsycopgError
from redis import RedisError
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from aegra_api.core.database import db_manager
from aegra_api.core.orm import Run as RunORM
from aegra_api.core.orm import RunWakeup, _get_session_maker
from aegra_api.core.orm import Thread as ThreadORM
from aegra_api.services.executor import executor
from aegra_api.services.thread_lifecycle import lock_thread
from aegra_api.settings import settings

logger = structlog.getLogger(__name__)
_background_cleanup_tasks: set[asyncio.Task[None]] = set()
_CLEANUP_ERRORS: tuple[type[BaseException], ...] = (RedisError, SQLAlchemyError, OSError, PsycopgError)
_EMPTY_THREAD = "__empty__"


async def _eligible(session: AsyncSession, thread: ThreadORM, run_id: str | None) -> bool:
    if thread.cleanup_protected:
        return False
    latest = await session.scalar(
        select(RunORM)
        .where(RunORM.thread_id == thread.thread_id, RunORM.user_id == thread.user_id)
        .order_by(RunORM.created_at.desc(), RunORM.run_id.desc())
        .limit(1)
    )
    if run_id is None:
        return latest is None
    if latest is None or latest.run_id != run_id or latest.status != "success" or thread.status != "idle":
        return False
    if (latest.execution_params or {}).get("parent"):
        return False
    active = await session.scalar(
        select(RunORM.run_id)
        .where(RunORM.thread_id == thread.thread_id, RunORM.status.in_(["pending", "running"]))
        .limit(1)
    )
    if active is not None:
        return False
    timer = await session.scalar(
        select(RunWakeup.run_id)
        .join(RunORM, RunWakeup.run_id == RunORM.run_id)
        .where(RunORM.thread_id == thread.thread_id, RunWakeup.status.in_(["pending", "blocked"]))
        .limit(1)
    )
    if timer is not None:
        return False
    child = await session.scalar(
        select(RunORM.run_id).where(RunORM.execution_params["parent"]["thread_id"].astext == thread.thread_id).limit(1)
    )
    return child is None


async def thread_cleanup_allowed(run_id: str, thread_id: str, user_id: str) -> bool:
    """Diagnostic only; deletion repeats the predicate under the admission lock."""
    try:
        async with _get_session_maker()() as session:
            await lock_thread(session, thread_id)
            thread = await session.scalar(
                select(ThreadORM).where(ThreadORM.thread_id == thread_id, ThreadORM.user_id == user_id)
            )
            return thread is not None and thread.cleanup_run_id is None and await _eligible(session, thread, run_id)
    except (*_CLEANUP_ERRORS, RuntimeError):
        logger.warning("Unable to verify ephemeral cleanup safety", run_id=run_id, thread_id=thread_id)
        return False


async def _finish_cleanup(thread_id: str, user_id: str) -> None:
    async with _get_session_maker()() as session:
        await lock_thread(session, thread_id)
        thread = await session.scalar(
            select(ThreadORM).where(ThreadORM.thread_id == thread_id, ThreadORM.user_id == user_id)
        )
        if thread is None or thread.cleanup_run_id is None:
            return
        # The committed marker blocks all writers; a crash here is retried by the sweeper.
        await db_manager.get_checkpointer().adelete_thread(thread_id)
        await session.delete(thread)
        await session.commit()
    logger.info("Ephemeral thread cleanup completed", thread_id=thread_id)


async def _request_cleanup(thread_id: str, user_id: str, run_id: str | None) -> None:
    async with _get_session_maker()() as session:
        await lock_thread(session, thread_id)
        thread = await session.scalar(
            select(ThreadORM).where(ThreadORM.thread_id == thread_id, ThreadORM.user_id == user_id)
        )
        if thread is None:
            return
        if thread.cleanup_run_id is None:
            if not await _eligible(session, thread, run_id):
                logger.info("Keeping ephemeral thread with recovery state", thread_id=thread_id, run_id=run_id)
                return
            thread.cleanup_run_id = run_id or _EMPTY_THREAD
            await session.commit()
    await _finish_cleanup(thread_id, user_id)


async def delete_thread_by_id(thread_id: str, user_id: str) -> None:
    """Clean up failed creation only if no run was ever admitted."""
    await _request_cleanup(thread_id, user_id, None)


async def cleanup_thread_if_safe(run_id: str, thread_id: str, user_id: str) -> None:
    try:
        await _request_cleanup(thread_id, user_id, run_id)
    except _CLEANUP_ERRORS:
        logger.exception("Ephemeral thread cleanup deferred", thread_id=thread_id, run_id=run_id)


async def cleanup_after_background_run(run_id: str, thread_id: str, user_id: str) -> None:
    try:
        await executor.wait_for_completion(run_id, timeout=3600.0)
    except (asyncio.CancelledError, TimeoutError):
        logger.warning("Keeping ephemeral thread after incomplete run", run_id=run_id)
        return
    except _CLEANUP_ERRORS:
        logger.exception("Error waiting for background run", run_id=run_id)
        return
    await cleanup_thread_if_safe(run_id, thread_id, user_id)


def schedule_background_cleanup(run_id: str, thread_id: str, user_id: str) -> asyncio.Task[None]:
    task = asyncio.create_task(cleanup_after_background_run(run_id, thread_id, user_id))
    _background_cleanup_tasks.add(task)
    task.add_done_callback(_background_cleanup_tasks.discard)
    return task


class CleanupSweeper:
    def __init__(self) -> None:
        self._task: asyncio.Task[None] | None = None

    async def tick(self) -> None:
        async with _get_session_maker()() as session:
            pending = (
                await session.execute(
                    select(ThreadORM.thread_id, ThreadORM.user_id)
                    .where(ThreadORM.cleanup_run_id.is_not(None))
                    .limit(100)
                )
            ).all()
        for thread_id, user_id in pending:
            try:
                await _finish_cleanup(thread_id, user_id)
            except _CLEANUP_ERRORS:
                logger.exception("Persistent thread cleanup deferred", thread_id=thread_id)

    async def _loop(self) -> None:
        while True:
            try:
                await self.tick()
            except _CLEANUP_ERRORS:
                logger.exception("Cleanup sweep unavailable")
            await asyncio.sleep(settings.app.CLEANUP_SWEEP_INTERVAL_SECONDS)

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        tasks = list(_background_cleanup_tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


cleanup_sweeper = CleanupSweeper()
