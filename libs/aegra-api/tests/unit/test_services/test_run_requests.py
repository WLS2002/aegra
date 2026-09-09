from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from sqlalchemy.dialects import postgresql

from aegra_api.models import RunCreate
from aegra_api.services.run_requests import find_request, request_digest


def test_request_digest_is_canonical_and_includes_config() -> None:
    first = RunCreate(assistant_id="echo", input={"a": 1, "b": 2})
    reordered = RunCreate(assistant_id="echo", input={"b": 2, "a": 1})
    changed = first.model_copy(update={"config": {"temperature": 1}})
    assert request_digest(first) == request_digest(reordered)
    assert request_digest(first) != request_digest(changed)


async def test_duplicate_request_returns_original_run_without_submission() -> None:
    session = AsyncMock()
    run = SimpleNamespace(run_id="original")
    session.scalar.side_effect = [SimpleNamespace(request_hash="digest", run_id="original"), run]
    assert await find_request(session, user_id="u", thread_id="t", key="k", digest="digest") is run
    session.commit.assert_not_awaited()
    statement = session.scalar.call_args_list[0].args[0].compile(dialect=postgresql.dialect())
    assert set(statement.params.values()) == {"u", "t", "k"}


async def test_conflicting_replay_never_reads_or_creates_a_run() -> None:
    session = AsyncMock()
    session.scalar.return_value = SimpleNamespace(request_hash="old")
    with pytest.raises(HTTPException) as exc:
        await find_request(session, user_id="u", thread_id="t", key="k", digest="changed")
    assert exc.value.status_code == 409
    session.scalar.assert_awaited_once()


async def test_deleted_run_keeps_request_tombstone() -> None:
    session = AsyncMock()
    session.scalar.side_effect = [SimpleNamespace(request_hash="digest", run_id="gone"), None]
    with pytest.raises(HTTPException) as exc:
        await find_request(session, user_id="u", thread_id="t", key="k", digest="digest")
    assert exc.value.status_code == 409


@pytest.mark.parametrize("key", ["", "  ", "x" * 201])
async def test_invalid_key_is_rejected_before_database_access(key: str) -> None:
    session = AsyncMock()
    with pytest.raises(HTTPException) as exc:
        await find_request(session, user_id="u", thread_id="t", key=key, digest="digest")
    assert exc.value.status_code == 422
    session.execute.assert_not_awaited()
