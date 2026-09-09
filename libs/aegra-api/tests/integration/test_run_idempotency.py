"""Run against an explicitly supplied isolated PostgreSQL database."""

import asyncio
import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi import HTTPException
from redis.exceptions import ConnectionError as RedisConnectionError
from sqlalchemy import delete, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from aegra_api.core.orm import Assistant, Base, Run, RunRequest
from aegra_api.models import RunCreate, User
from aegra_api.services import run_preparation
from aegra_api.settings import settings


@pytest.fixture
async def database() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    url = os.environ.get("AEGRA_TEST_DATABASE_URL")
    if not url:
        pytest.skip("Set AEGRA_TEST_DATABASE_URL to an isolated PostgreSQL database")
    schema = f"test_idempotency_{uuid4().hex}"
    engine = create_async_engine(url, execution_options={"schema_translate_map": {None: schema}})
    async with engine.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        await connection.run_sync(Base.metadata.create_all)
    try:
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await engine.dispose()


@pytest.mark.parametrize("broker_unavailable", [False, True])
async def test_concurrent_replays_commit_one_run_and_preserve_tombstone(
    database: async_sessionmaker[AsyncSession], monkeypatch: pytest.MonkeyPatch, broker_unavailable: bool
) -> None:
    assistant_id, thread_id = str(uuid4()), str(uuid4())
    now = datetime.now(UTC)
    user = User(identity="synthetic-owner", is_authenticated=True)
    async with database() as session:
        session.add(
            Assistant(
                assistant_id=assistant_id,
                name="Synthetic",
                graph_id="synthetic",
                user_id=user.identity,
                config={},
                context={},
                created_at=now,
                updated_at=now,
            )
        )
        await session.commit()
    service = MagicMock()
    service.list_graphs.return_value = ["synthetic"]
    monkeypatch.setattr(run_preparation, "get_langgraph_service", lambda: service)
    submit = AsyncMock(side_effect=RedisConnectionError("synthetic outage") if broker_unavailable else None)
    monkeypatch.setattr(run_preparation.executor, "submit", submit)
    monkeypatch.setattr(settings.redis, "REDIS_BROKER_ENABLED", True)

    async def create(value: int = 1) -> str:
        async with database() as session:
            result = await run_preparation._prepare_run(
                session,
                thread_id,
                RunCreate(assistant_id=assistant_id, input={"value": value}, multitask_strategy="reject"),
                user,
                initial_status="pending",
                idempotency_key="same-request",
            )
            return result[0]

    ids = await asyncio.gather(*(create() for _ in range(12)))
    assert len(set(ids)) == 1
    submit.assert_awaited_once()
    async with database() as session:
        assert await session.scalar(select(func.count()).select_from(Run)) == 1
        assert await session.scalar(select(func.count()).select_from(RunRequest)) == 1
    with pytest.raises(HTTPException) as conflict:
        await create(2)
    assert conflict.value.status_code == 409
    async with database() as session:
        await session.execute(delete(Run).where(Run.run_id == ids[0]))
        await session.commit()
    with pytest.raises(HTTPException) as deleted:
        await create()
    assert deleted.value.status_code == 409
    submit.assert_awaited_once()
