"""HTTP integration replaces database boundaries; real DB tests keep their fixtures."""

from unittest.mock import AsyncMock

import pytest

from aegra_api.api import threads
from aegra_api.models import User
from aegra_api.services import run_auth, run_preparation


@pytest.fixture(autouse=True)
def runtime_database_boundaries(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    path = str(request.path)
    if "/test_api/" not in path and not path.endswith("test_auth_enforcement_routes.py"):
        return

    async def resolve(user: User, requested_id: str) -> tuple[str, str]:
        return requested_id, requested_id

    monkeypatch.setattr(run_auth, "resolve_authorization_target", resolve)
    monkeypatch.setattr(run_preparation, "require_writable_thread", AsyncMock())
    monkeypatch.setattr(threads, "require_writable_thread", AsyncMock())
