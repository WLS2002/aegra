"""Trusted graph capabilities must not trust configurable execution IDs."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from aegra_api.core.execution_context import current_job
from aegra_api.models import User
from aegra_api.models.run_job import RunIdentity, RunJob
from aegra_api.runtime import child_thread_id, create_child, execution_identity
from aegra_api.services import run_executor


@pytest.fixture
def job() -> RunJob:
    return RunJob(
        identity=RunIdentity(run_id="parent-run", thread_id="parent-thread", graph_id="parent-graph"),
        user=User(identity="owner", is_authenticated=True),
    )


def test_execution_identity_requires_executor_context() -> None:
    with pytest.raises(LookupError):
        execution_identity()


def test_child_keys_are_scoped_and_unambiguous() -> None:
    assert child_thread_id("u", "t", "k") == child_thread_id("u", "t", "k")
    assert child_thread_id("u", "t", "k") != child_thread_id("other", "t", "k")
    assert child_thread_id("u", "t", "k") != child_thread_id("u", "other", "k")
    assert child_thread_id("u:t", "x", "k") != child_thread_id("u", "t:x", "k")
    with pytest.raises(ValueError):
        child_thread_id("u", "t", " ")


@pytest.mark.parametrize("fails", [False, True])
async def test_executor_restores_context_even_on_failure(job: RunJob, fails: bool) -> None:
    async def execute(received: RunJob) -> None:
        assert received is job
        assert execution_identity() == job.identity
        if fails:
            raise RuntimeError("synthetic failure")

    with patch.object(run_executor, "_execute_run", side_effect=execute):
        if fails:
            with pytest.raises(RuntimeError, match="synthetic failure"):
                await run_executor.execute_run(job)
        else:
            await run_executor.execute_run(job)
    with pytest.raises(LookupError):
        execution_identity()


@pytest.mark.parametrize("active", [True, False])
async def test_child_requires_active_parent_and_records_trusted_relation(job: RunJob, active: bool) -> None:
    session = AsyncMock()
    session.scalar.return_value = job.identity.run_id if active else None
    session.__aenter__.return_value = session
    maker = MagicMock(return_value=session)
    child = object()
    token = current_job.set(job)
    try:
        with (
            patch("aegra_api.runtime._get_session_maker", return_value=maker),
            patch("aegra_api.runtime.apply_run_authorization", new_callable=AsyncMock) as auth,
            patch("aegra_api.runtime._prepare_run", new_callable=AsyncMock) as prepare,
        ):
            prepare.return_value = ("child-run", child, None)
            if active:
                assert await create_child("child-graph", key="item:1", input={"item": 1}) is child
                assert prepare.call_args.kwargs["parent_identity"] == job.identity
                assert prepare.call_args.kwargs["idempotency_key"] == "item:1"
                assert prepare.call_args.args[3] == job.user
                assert prepare.call_args.args[2].on_completion == "keep"
            else:
                with pytest.raises(RuntimeError, match="active execution"):
                    await create_child("child-graph", key="item:1", input={"item": 1})
                prepare.assert_not_awaited()
            auth.assert_awaited_once()
    finally:
        current_job.reset(token)


async def test_child_obeys_authorization_before_database_access(job: RunJob) -> None:
    token = current_job.set(job)
    try:
        with (
            patch("aegra_api.runtime._get_session_maker") as maker,
            patch("aegra_api.runtime.apply_run_authorization", side_effect=HTTPException(403, "denied")),
        ):
            with pytest.raises(HTTPException) as exc:
                await create_child("private", key="item", input={})
            assert exc.value.status_code == 403
            maker.assert_not_called()
    finally:
        current_job.reset(token)
