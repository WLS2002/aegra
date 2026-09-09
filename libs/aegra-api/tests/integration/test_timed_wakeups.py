"""Exercise LangGraph interrupts and PostgreSQL continuation transactions together."""

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, TypedDict
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from aegra_api.api.runs import _request_run_interruption
from aegra_api.core.execution_context import current_job
from aegra_api.core.orm import Assistant, Run, RunWakeup
from aegra_api.models import RunCreate, User
from aegra_api.models.run_job import RunJob
from aegra_api.models.wakeups import extract_wakeups
from aegra_api.runtime import wait_until
from aegra_api.services import run_preparation, run_status, wake_scheduler
from tests.integration.test_run_idempotency import database as database


class WaitState(TypedDict, total=False):
    done: bool


@pytest.mark.parametrize("mode", ["normal", "changed_checkpoint", "cancelled", "human", "future"])
async def test_timer_resumes_once_and_rejects_obsolete_checkpoints(
    database: async_sessionmaker[AsyncSession], monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    async def wait_node(_state: WaitState) -> WaitState:
        if mode == "human":
            interrupt({"question": "Synthetic human decision"})
        else:
            wait_until(datetime.now(UTC) + timedelta(seconds=3600 if mode == "future" else -1))
        return {"done": True}

    builder = StateGraph(WaitState)
    builder.add_node("wait", wait_node)
    builder.add_edge(START, "wait")
    builder.add_edge("wait", END)
    graph = builder.compile(checkpointer=InMemorySaver())

    @asynccontextmanager
    async def get_graph(*_args: Any, **_kwargs: Any):
        yield graph

    service = MagicMock()
    service.list_graphs.return_value = ["wait"]
    service.get_graph = get_graph
    monkeypatch.setattr(run_preparation, "get_langgraph_service", lambda: service)
    monkeypatch.setattr(wake_scheduler, "get_langgraph_service", lambda: service)
    monkeypatch.setattr(wake_scheduler, "_get_session_maker", lambda: database)
    monkeypatch.setattr(run_status, "_get_session_maker", lambda: database)
    submit = AsyncMock()
    monkeypatch.setattr(run_preparation.executor, "submit", submit)
    user = User(identity="synthetic-owner", is_authenticated=True)
    assistant_id, thread_id = str(uuid4()), str(uuid4())
    now = datetime.now(UTC)
    async with database() as session:
        session.add(
            Assistant(
                assistant_id=assistant_id,
                name="Synthetic wait",
                graph_id="wait",
                user_id=user.identity,
                config={},
                context={},
                created_at=now,
                updated_at=now,
            )
        )
        await session.commit()
        run_id, _, job = await run_preparation._prepare_run(
            session,
            thread_id,
            RunCreate(assistant_id=assistant_id, input={}),
            user,
            initial_status="pending",
        )
    config: RunnableConfig = {"configurable": {"thread_id": thread_id}}
    token = current_job.set(job)
    try:
        await graph.ainvoke({}, config)
    finally:
        current_job.reset(token)
    snapshot = await graph.aget_state(config)
    timers = extract_wakeups(snapshot)
    assert len(timers) == (0 if mode == "human" else 1)
    assert await run_status.finalize_run(
        run_id,
        thread_id,
        user_id=user.identity,
        status="interrupted",
        thread_status="interrupted",
        wakeups=timers,
    )
    if mode == "changed_checkpoint":
        await graph.aupdate_state(config, {"done": False})
    if mode == "cancelled":
        async with database() as session:
            original = await session.get(Run, run_id)
            assert original is not None
            await _request_run_interruption(session, original, "cancel")
    await asyncio.gather(*(wake_scheduler.WakeScheduler().tick() for _ in range(8)))
    async with database() as session:
        runs = list(await session.scalars(select(Run).order_by(Run.created_at)))
        timer = await session.scalar(select(RunWakeup))
        if mode == "human":
            assert timer is None
        else:
            assert timer is not None
            expected = "pending" if mode == "future" else "dispatched" if mode == "normal" else "cancelled"
            assert timer.status == expected
    assert len(runs) == (2 if mode == "normal" else 1)
    assert submit.await_count == len(runs)
    if mode == "normal":
        continuation = runs[-1]
        assert continuation.execution_params is not None
        assert continuation.execution_params["execution"]["command"]["resume"] == {timers[0].interrupt_id: None}

        token = current_job.set(RunJob.from_run_orm(continuation))
        try:
            result = await graph.ainvoke(Command(resume={timers[0].interrupt_id: None}), config)
        finally:
            current_job.reset(token)
        assert result["done"] is True
