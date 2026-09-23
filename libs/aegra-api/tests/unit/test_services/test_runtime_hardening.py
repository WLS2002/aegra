"""Regression boundaries for runtime hardening, without external services."""

import hashlib
import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from aegra_api.core import health
from aegra_api.models import RunCreate, User
from aegra_api.services import run_auth, run_cleanup, scheduled_auth
from aegra_api.services.broker import BrokerManager, RunBroker
from aegra_api.services.error_details import describe_error, error_summary
from aegra_api.services.run_preparation import _admit_run
from aegra_api.services.run_requests import request_digest
from aegra_api.services.worker_executor import WorkerExecutor
from aegra_api.utils.assistants import resolve_assistant_id


def test_group_errors_are_bounded_and_contain_no_raw_text() -> None:
    error = ExceptionGroup("private-root", [ValueError("private-input quota exceeded") for _ in range(30)])
    details = describe_error(error)
    assert len(details["leaves"]) == 16 and details["truncated"]
    assert all(item["code"] == "quota_exhausted" for item in details["leaves"])
    assert len(json.dumps(details).encode()) < 16_384
    assert "private" not in json.dumps(details) + error_summary(details)


def test_broken_exception_string_cannot_break_finalization() -> None:
    class BrokenError(Exception):
        def __str__(self) -> str:
            raise ValueError("do not expose")

    assert describe_error(BrokenError())["leaves"][0]["code"] == "execution_error"


def test_legacy_receipt_digest_ignores_absent_checkpoint_alias() -> None:
    request = RunCreate(assistant_id="echo", input={"value": 1})

    payload = request.model_dump(mode="json")
    payload.pop("checkpoint_id")
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    assert request_digest(request) == hashlib.sha256(encoded.encode()).hexdigest()


@pytest.mark.parametrize(
    "principal",
    [None, {}, {"permissions": ["backend"]}, {"identity": "other"}, {"identity": "owner", "is_authenticated": False}],
)
async def test_scheduled_identity_never_falls_back_to_owner(principal: dict[str, Any] | None) -> None:
    with pytest.raises(HTTPException) as exc:
        await scheduled_auth.restore_scheduled_user(principal, owner="owner", graph_id="maintenance", source="cron")
    assert exc.value.status_code == 403


async def test_schedule_snapshot_excludes_secrets_and_keeps_permissions(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(scheduled_auth.settings.app, "SCHEDULED_PRINCIPAL_RESOLVER", "")
    user = User(identity="owner", permissions=["backend"], token="private", email="private@example.invalid")
    snapshot = scheduled_auth.principal_snapshot(user)
    assert "private" not in json.dumps(snapshot)
    restored = await scheduled_auth.restore_scheduled_user(
        snapshot, owner="owner", graph_id="maintenance", source="cron"
    )
    assert restored.permissions == ["backend"]


async def test_schedule_current_policy_can_revoke(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(scheduled_auth.settings.app, "SCHEDULED_PRINCIPAL_RESOLVER", "fixture:resolve")
    monkeypatch.setattr(scheduled_auth.importlib, "import_module", lambda _: SimpleNamespace(resolve=lambda **_: None))
    with pytest.raises(HTTPException, match="scheduled_principal_revoked"):
        await scheduled_auth.restore_scheduled_user(
            {"identity": "owner"}, owner="owner", graph_id="maintenance", source="wakeup"
        )


@pytest.mark.parametrize("use_uuid", [False, True])
async def test_authorization_uses_trusted_graph_for_alias_and_uuid(
    monkeypatch: pytest.MonkeyPatch, use_uuid: bool
) -> None:
    graphs = {"maintenance": {}}
    monkeypatch.setattr(run_auth, "get_langgraph_service", lambda: SimpleNamespace(list_graphs=lambda: graphs))
    identifier = resolve_assistant_id("maintenance", graphs) if use_uuid else "maintenance"
    request = RunCreate(assistant_id=identifier, input={}, graph_id="untrusted")
    handler = AsyncMock(return_value=None)
    await run_auth.apply_run_authorization(User(identity="owner"), "thread", request, event_handler=handler)
    assert handler.await_args_list[0].args[1]["graph_id"] == "maintenance"
    assert str(request.assistant_id) == identifier


@pytest.mark.parametrize("strategy", [None, "reject", "enqueue", "interrupt", "rollback"])
async def test_cleanup_marker_blocks_every_admission_strategy(strategy: str | None) -> None:
    session = AsyncMock()
    session.scalar.return_value = "old-run"
    with pytest.raises(HTTPException, match="thread_cleanup_pending"):
        await _admit_run(session, "thread", user_id="owner", strategy=strategy)
    session.commit.assert_not_awaited()


@pytest.mark.parametrize(
    "latest_id,status,parent,active,timer,child,protected",
    [
        ("new", "interrupted", None, None, None, None, False),
        ("target", "error", None, None, None, None, False),
        ("target", "success", {"thread_id": "parent"}, None, None, None, False),
        ("target", "success", None, "running", None, None, False),
        ("target", "success", None, None, "blocked-timer", None, False),
        ("target", "success", None, None, None, "child", False),
        ("target", "success", None, None, None, None, True),
    ],
)
async def test_automatic_cleanup_preserves_recovery_state(
    latest_id: str,
    status: str,
    parent: dict[str, str] | None,
    active: str | None,
    timer: str | None,
    child: str | None,
    protected: bool,
) -> None:
    thread = SimpleNamespace(thread_id="thread", user_id="owner", status="idle", cleanup_protected=protected)
    latest = SimpleNamespace(run_id=latest_id, status=status, execution_params={"parent": parent})
    session = AsyncMock()
    session.scalar.side_effect = [latest, active, timer, child]
    assert not await run_cleanup._eligible(session, thread, "target")


async def test_live_redis_cannot_starve_postgres_discovery(monkeypatch: pytest.MonkeyPatch) -> None:
    executor = WorkerExecutor()
    poll = AsyncMock(return_value="committed-only-in-postgres")
    monkeypatch.setattr(executor, "_poll_postgres", poll)
    assert await executor._dequeue() == "committed-only-in-postgres"
    poll.assert_awaited_once()


async def test_replay_has_byte_and_global_budgets(monkeypatch: pytest.MonkeyPatch) -> None:
    from aegra_api.services import broker as module

    monkeypatch.setattr(module.settings.event_streaming, "SSE_REPLAY_RUN_BYTES", 2048)
    monkeypatch.setattr(module.settings.event_streaming, "SSE_REPLAY_TOTAL_BYTES", 3072)
    manager = BrokerManager()
    for index in range(8):
        broker = manager.get_or_create_broker(str(index))
        for sequence in range(4):
            await broker.put(f"{index}-event-{sequence}", ("values", {"value": "界" * 100}))
        assert broker._replay_bytes <= 2048
    assert sum(item._replay_bytes for item in manager._brokers.values()) <= 3072
    with pytest.raises(HTTPException) as exc:
        await manager.get_or_create_broker("0").replay("0-event-0")
    assert exc.value.status_code == 409


async def test_oversize_event_invalidates_old_replay_cursor(monkeypatch: pytest.MonkeyPatch) -> None:
    from aegra_api.services import broker as module

    monkeypatch.setattr(module.settings.event_streaming, "SSE_REPLAY_RUN_BYTES", 1024)
    broker = RunBroker("run")
    await broker.put("one", ("values", {}))
    await broker.put("two", ("values", "a" * 2048))
    with pytest.raises(HTTPException):
        await broker.replay("one")


@pytest.mark.parametrize("component", ["_database_probe", "_checkpoint_probe", "_store_probe"])
async def test_real_probe_failure_is_not_suppressed(monkeypatch: pytest.MonkeyPatch, component: str) -> None:
    for name in ("_database_probe", "_checkpoint_probe", "_store_probe"):
        monkeypatch.setattr(health, name, AsyncMock())
    monkeypatch.setattr(health, component, AsyncMock(side_effect=OSError("private-backend-error")))
    monkeypatch.setattr(health, "_runtime_probe", AsyncMock(return_value={}))
    monkeypatch.setattr(health.settings.redis, "REDIS_BROKER_ENABLED", False)
    result = await health._health_snapshot()
    assert result.status == "unhealthy"
    assert "private" not in result.model_dump_json()


async def test_health_identifies_redis_write_limit_without_writing(monkeypatch: pytest.MonkeyPatch) -> None:
    client = AsyncMock()
    client.info.return_value = {"used_memory": 1040, "maxmemory": 1000, "mem_not_counted_for_evict": 20}
    monkeypatch.setattr(health.redis_manager, "get_client", lambda: client)
    assert await health._redis_probe() == "write_limited"
    client.info.assert_awaited_once_with("memory")
    client.set.assert_not_called()
