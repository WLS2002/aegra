"""Trusted capabilities for graphs running inside AEGRA."""

import json
from datetime import UTC, datetime
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from langgraph.types import interrupt
from sqlalchemy import select

from aegra_api.core.execution_context import current_job
from aegra_api.core.orm import Run as RunORM
from aegra_api.core.orm import _get_session_maker
from aegra_api.models import Run, RunCreate
from aegra_api.models.run_job import RunIdentity
from aegra_api.models.wakeups import WAIT_MARKER
from aegra_api.services.run_auth import apply_run_authorization
from aegra_api.services.run_preparation import _prepare_run


def execution_identity() -> RunIdentity:
    return current_job.get().identity.model_copy()


def wait_until(due_at: datetime) -> None:
    """Checkpoint this node and let AEGRA resume its interrupt at the deadline.

    Persist the deadline in graph state before entering this node. LangGraph
    replays the node on resume, so operations before this call must be idempotent.
    """
    current_job.get()
    if due_at.tzinfo is None or due_at.utcoffset() is None:
        raise ValueError("wait_until requires a timezone-aware deadline")
    interrupt({"type": WAIT_MARKER, "due_at": due_at.astimezone(UTC).isoformat()})


def child_thread_id(user_id: str, parent_thread_id: str, key: str) -> str:
    if not key.strip() or len(key) > 200:
        raise ValueError("Child key must contain 1–200 nonblank characters")
    return str(uuid5(NAMESPACE_URL, json.dumps(["aegra-child", user_id, parent_thread_id, key])))


async def create_child(
    assistant_id: str, *, key: str, input: dict[str, Any], config: dict[str, Any] | None = None
) -> Run:
    parent = current_job.get()
    thread_id = child_thread_id(parent.user.identity, parent.identity.thread_id, key)
    request = RunCreate(
        assistant_id=assistant_id,
        input=input,
        config=config or {},
        on_disconnect="continue",
        on_completion="keep",
        multitask_strategy="reject",
    )
    await apply_run_authorization(parent.user, thread_id, request)
    async with _get_session_maker()() as session:
        active = await session.scalar(
            select(RunORM.run_id).where(
                RunORM.run_id == parent.identity.run_id,
                RunORM.user_id == parent.user.identity,
                RunORM.thread_id == parent.identity.thread_id,
                RunORM.status == "running",
            )
        )
        if active is None:
            raise RuntimeError("Only an active execution may create child runs")
        _, run, _ = await _prepare_run(
            session,
            thread_id,
            request,
            parent.user,
            initial_status="pending",
            idempotency_key=key,
            parent_identity=parent.identity,
        )
        return run
