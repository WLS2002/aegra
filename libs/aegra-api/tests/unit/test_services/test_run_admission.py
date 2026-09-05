from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from sqlalchemy.dialects import postgresql

from aegra_api.services.run_preparation import _admit_run


@pytest.mark.parametrize("active_run", ["pending-run", "running-run"])
async def test_reject_active_run_without_committing(active_run: str) -> None:
    session = AsyncMock()
    session.scalar.return_value = active_run

    with pytest.raises(HTTPException) as exc:
        await _admit_run(session, "thread-1", user_id="user-1", strategy="reject")

    assert exc.value.status_code == 409
    session.execute.assert_awaited_once()
    session.commit.assert_not_awaited()
    statement = session.scalar.call_args.args[0].compile(dialect=postgresql.dialect())
    assert set(statement.params["status_1"]) == {"pending", "running"}
    assert statement.params["thread_id_1"] == "thread-1"
    assert statement.params["user_id_1"] == "user-1"


async def test_reject_allows_thread_without_active_runs() -> None:
    session = AsyncMock()
    session.scalar.return_value = None

    await _admit_run(session, "thread-1", user_id="user-1", strategy="reject")

    session.commit.assert_not_awaited()


@pytest.mark.parametrize("strategy", [None, "enqueue", "interrupt", "rollback"])
async def test_other_strategies_serialize_admission_without_changing_behavior(strategy: str | None) -> None:
    session = AsyncMock()

    await _admit_run(session, "thread-1", user_id="user-1", strategy=strategy)

    session.scalar.assert_not_awaited()
    statement = session.execute.call_args.args[0].compile(dialect=postgresql.dialect())
    assert "pg_advisory_xact_lock" in str(statement)
    assert "thread-1" in statement.params.values()
