"""Persist parent replay and timed child continuations across checkpointer connections."""

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, TypedDict
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command
from psycopg import AsyncConnection
from psycopg.rows import dict_row
from sqlalchemy import select

from aegra_api import runtime
from aegra_api.api.runs import list_child_runs
from aegra_api.core.execution_context import current_job
from aegra_api.core.orm import Assistant, Run, RunWakeup
from aegra_api.models import RunCreate, User
from aegra_api.models.run_job import RunJob
from aegra_api.models.wakeups import extract_wakeups
from aegra_api.services import run_preparation, run_status, wake_scheduler
from tests.integration.test_run_idempotency import database as database


class State(TypedDict, total=False):
    child_id: str
    value: int
    done: bool


async def test_parent_replay_and_timed_child_keep_persistent_identity(database, monkeypatch):
    user = User(identity="persistent-test-owner", is_authenticated=True)
    parent_assistant, child_assistant, parent_thread = (str(uuid4()) for _ in range(3))
    async with database() as session:
        engine = session.bind
        schema = engine.get_execution_options()["schema_translate_map"][None]
        now = datetime.now(UTC)
        for identifier, graph_id in [(parent_assistant, "parent"), (child_assistant, "child")]:
            session.add(
                Assistant(
                    assistant_id=identifier,
                    name=graph_id,
                    graph_id=graph_id,
                    user_id=user.identity,
                    config={},
                    context={},
                    created_at=now,
                    updated_at=now,
                )
            )
        await session.commit()
    service = MagicMock()
    service.list_graphs.return_value = ["parent", "child"]
    monkeypatch.setattr(run_preparation, "get_langgraph_service", lambda: service)
    monkeypatch.setattr(wake_scheduler, "get_langgraph_service", lambda: service)
    for module in [runtime, run_status, wake_scheduler]:
        monkeypatch.setattr(module, "_get_session_maker", lambda: database)
    submitted = AsyncMock()
    monkeypatch.setattr(run_preparation.executor, "submit", submitted)
    lost = True
    effects = []

    async def parent_node(state: State):
        nonlocal lost
        child = await runtime.create_child(child_assistant, key="one-child", input={"value": 42})
        if lost:
            lost = False
            raise RuntimeError("Synthetic lost acknowledgement after child commit")
        return {"child_id": child.run_id}

    async def child_node(state: State):
        runtime.wait_until(datetime.now(UTC) - timedelta(seconds=1))
        effects.append(state["value"])
        return {"done": True}

    builders = {}
    for name, node in [("parent", parent_node), ("child", child_node)]:
        builder = StateGraph(State)
        builder.add_node("work", node)
        builder.add_edge(START, "work")
        builder.add_edge("work", END)
        builders[name] = builder
    graphs = {}

    @asynccontextmanager
    async def graph_service(graph_id: str, *_args: Any, **_kwargs: Any):
        yield graphs[graph_id]

    service.get_graph = graph_service

    @asynccontextmanager
    async def checkpoint_process():
        connection = await AsyncConnection.connect(
            os.environ["AEGRA_TEST_DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://"),
            autocommit=True,
            row_factory=dict_row,
            options=f"-c search_path={schema}",
        )
        try:
            saver = AsyncPostgresSaver(connection)
            await saver.setup()
            graphs.update({name: builder.compile(checkpointer=saver) for name, builder in builders.items()})
            yield
        finally:
            await connection.close()

    async def prepare(assistant, thread, input, *, command=None, checkpoint=None):
        async with database() as session:
            _, _, job = await run_preparation._prepare_run(
                session,
                thread,
                RunCreate(assistant_id=assistant, input=input, command=command, checkpoint=checkpoint),
                user,
                initial_status="running",
            )
            return job

    async def invoke(graph_id, job, value):
        token = current_job.set(job)
        try:
            config: RunnableConfig = {"configurable": {"thread_id": job.identity.thread_id}}
            return await graphs[graph_id].ainvoke(value, config)
        finally:
            current_job.reset(token)

    parent_job = await prepare(parent_assistant, parent_thread, {})
    async with checkpoint_process():
        with pytest.raises(RuntimeError, match="lost acknowledgement"):
            await invoke("parent", parent_job, {})
        snapshot = await graphs["parent"].aget_state({"configurable": {"thread_id": parent_thread}})
        checkpoint = {"checkpoint_id": snapshot.config["configurable"]["checkpoint_id"]}
    await run_status.finalize_run(
        parent_job.identity.run_id, parent_thread, user_id=user.identity, status="error", thread_status="error"
    )
    retry = await prepare(parent_assistant, parent_thread, None, checkpoint=checkpoint)
    async with checkpoint_process():
        result = await invoke("parent", retry, None)
        async with database() as session:
            child_row = await session.get(Run, result["child_id"])
            assert child_row is not None
            assert child_row.execution_params["parent"]["run_id"] == parent_job.identity.run_id
            child_row.status = "running"
            await session.commit()
            child_job = RunJob.from_run_orm(child_row)
        await invoke("child", child_job, {"value": 42})
        config: RunnableConfig = {"configurable": {"thread_id": child_job.identity.thread_id}}
        timers = extract_wakeups(await graphs["child"].aget_state(config))
        assert len(timers) == 1 and effects == []
        await run_status.finalize_run(
            child_job.identity.run_id,
            child_job.identity.thread_id,
            user_id=user.identity,
            status="interrupted",
            thread_status="interrupted",
            wakeups=timers,
        )
    async with checkpoint_process():
        await asyncio.gather(*(wake_scheduler.WakeScheduler().tick() for _ in range(8)))
        async with database() as session:
            timer = await session.scalar(select(RunWakeup))
            assert timer.status == "dispatched"
            children = await list_child_runs(parent_thread, limit=20, offset=0, user=user, session=session)
            assert len(children) == 2 and all(row.parent.run_id == parent_job.identity.run_id for row in children)
            continuation = await session.scalar(
                select(Run).where(
                    Run.thread_id == child_job.identity.thread_id, Run.run_id != child_job.identity.run_id
                )
            )
            assert continuation is not None
            continued_job = RunJob.from_run_orm(continuation)
        result = await invoke("child", continued_job, Command(resume={timers[0].interrupt_id: None}))
        assert result["done"] and effects == [42]
    assert submitted.await_count == 4  # original parent, one child, parent retry, one timed continuation
