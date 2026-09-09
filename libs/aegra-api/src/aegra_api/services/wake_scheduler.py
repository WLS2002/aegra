"""Resume persisted timer interrupts; no application business logic lives here."""

import asyncio
import contextlib
import hashlib
import json
from datetime import UTC, datetime
from typing import cast

import structlog
from langchain_core.runnables import RunnableConfig
from sqlalchemy import select

from aegra_api.core.orm import Run as RunORM
from aegra_api.core.orm import RunWakeup, _get_session_maker
from aegra_api.core.orm import Thread as ThreadORM
from aegra_api.models import RunCreate
from aegra_api.models.run_job import RunJob
from aegra_api.services.langgraph_service import create_run_config, get_langgraph_service
from aegra_api.services.run_auth import apply_run_authorization
from aegra_api.services.run_preparation import _admit_run, _prepare_run
from aegra_api.services.run_requests import find_request, request_digest
from aegra_api.settings import settings

logger = structlog.getLogger(__name__)


class WakeScheduler:
    def __init__(self) -> None:
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _loop(self) -> None:
        while True:
            try:
                await self.tick()
            except Exception:
                logger.exception("Timer wakeup tick failed")
            await asyncio.sleep(settings.wakeups.WAKEUP_POLL_INTERVAL_SECONDS)

    async def tick(self) -> None:
        async with _get_session_maker()() as session:
            run_ids = list(
                await session.scalars(
                    select(RunWakeup.run_id)
                    .where(
                        RunWakeup.status == "pending",
                        RunWakeup.due_at <= datetime.now(UTC),
                    )
                    .distinct()
                    .limit(100)
                )
            )
        for run_id in run_ids:
            try:
                await self._wake(run_id)
            except Exception:
                logger.exception("Timer resume deferred", run_id=run_id)

    async def _wake(self, run_id: str) -> None:
        async with _get_session_maker()() as session:
            original = await session.get(RunORM, run_id)
            if original is None:
                return
            timers = list(
                await session.scalars(
                    select(RunWakeup)
                    .where(
                        RunWakeup.run_id == run_id,
                        RunWakeup.status == "pending",
                        RunWakeup.due_at <= datetime.now(UTC),
                    )
                    .order_by(RunWakeup.interrupt_id)
                )
            )
            if not timers:
                return
            job = RunJob.from_run_orm(original)
            key = "wake:" + hashlib.sha256(json.dumps([run_id, [t.interrupt_id for t in timers]]).encode()).hexdigest()
            request = RunCreate(
                assistant_id=original.assistant_id or job.identity.graph_id,
                input=None,
                command={"resume": {timer.interrupt_id: None for timer in timers}},
                config=job.execution.config,
                context=job.execution.context,
                on_completion="keep",
                on_disconnect="continue",
                multitask_strategy="reject",
            )
            await apply_run_authorization(job.user, original.thread_id, request)
            # Match preparation's lock order: request first, then thread.
            saved = await find_request(
                session,
                user_id=original.user_id,
                thread_id=original.thread_id,
                key=key,
                digest=request_digest(request),
            )
            if saved is not None:
                for timer in timers:
                    timer.status = "dispatched"
                await session.commit()
                return
            await _admit_run(session, original.thread_id, user_id=original.user_id, strategy=None)
            await session.refresh(original)
            thread = await session.get(ThreadORM, original.thread_id)
            latest = await session.scalar(
                select(RunORM.run_id)
                .where(
                    RunORM.thread_id == original.thread_id,
                )
                .order_by(RunORM.created_at.desc(), RunORM.run_id.desc())
                .limit(1)
            )
            if original.status != "interrupted" or thread is None or thread.status != "interrupted" or latest != run_id:
                for timer in timers:
                    timer.status = "cancelled"
                await session.commit()
                return
            config = create_run_config(run_id, original.thread_id, job.user, additional_config=job.execution.config)
            config["configurable"].pop("checkpoint_id", None)
            service = get_langgraph_service()
            async with service.get_graph(
                job.identity.graph_id,
                config=config,
                access_context="threads.create_run",
                user=job.user,
                context=job.execution.context,
            ) as graph:
                snapshot = await graph.aget_state(cast(RunnableConfig, config))
            checkpoint_id = (snapshot.config or {}).get("configurable", {}).get("checkpoint_id")
            interrupt_ids = {item.id for item in snapshot.interrupts}
            if any(t.checkpoint_id != checkpoint_id or t.interrupt_id not in interrupt_ids for t in timers):
                for timer in timers:
                    timer.status = "cancelled"
                await session.commit()
                return
            for timer in timers:
                timer.status = "dispatched"
            # Timer consumption and the pending continuation commit together.
            await _prepare_run(
                session,
                original.thread_id,
                request,
                job.user,
                initial_status="pending",
                idempotency_key=key,
            )


wake_scheduler = WakeScheduler()
