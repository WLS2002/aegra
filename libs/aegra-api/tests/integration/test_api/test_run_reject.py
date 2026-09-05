from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from aegra_api.api import runs
from aegra_api.services import run_preparation
from tests.fixtures.clients import create_test_app, make_client
from tests.fixtures.session_fixtures import override_session_dependency


@pytest.mark.parametrize("suffix", ["", "/stream", "/wait"])
def test_conflicting_reject_returns_409_before_persistence_or_dispatch(
    monkeypatch: pytest.MonkeyPatch, suffix: str
) -> None:
    session = AsyncMock()
    session.add = MagicMock()
    session.scalar.side_effect = [
        SimpleNamespace(user_id="test-user"),
        SimpleNamespace(assistant_id="echo", graph_id="echo", config={}, context={}),
        "active-run",
    ]
    session.__aenter__.return_value = session
    graph_service = MagicMock()
    graph_service.list_graphs.return_value = {"echo": {}}
    monkeypatch.setattr(run_preparation, "get_langgraph_service", lambda: graph_service)
    monkeypatch.setattr(runs, "_apply_create_run_auth", AsyncMock())
    monkeypatch.setattr(runs, "_get_session_maker", lambda: lambda: session)
    submit = AsyncMock()
    monkeypatch.setattr(run_preparation.executor, "submit", submit)
    app = create_test_app(include_threads=False)
    override_session_dependency(app, lambda: session)

    with make_client(app) as client:
        response = client.post(
            f"/threads/thread-1/runs{suffix}",
            json={"assistant_id": "echo", "input": {}, "multitask_strategy": "reject"},
        )

    assert response.status_code == 409
    session.add.assert_not_called()
    session.commit.assert_not_awaited()
    submit.assert_not_awaited()
