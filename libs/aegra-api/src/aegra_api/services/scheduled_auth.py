"""Restore server-owned scheduled identities and apply current application policy."""

import importlib
import inspect
from typing import Any

from fastapi import HTTPException
from pydantic import ValidationError

from aegra_api.models import User
from aegra_api.settings import settings


def principal_snapshot(user: User) -> dict[str, Any]:
    """Persist only authorization identifiers, never tokens or request headers."""
    return user.model_dump(include={"identity", "is_authenticated", "permissions", "scopes", "org_id"}, mode="json")


async def restore_scheduled_user(principal: dict[str, Any] | None, *, owner: str, graph_id: str, source: str) -> User:
    """Reject missing/mismatched principals; a trusted hook may revoke access."""
    if not principal:
        raise HTTPException(403, "scheduled_principal_missing")
    try:
        user = User.model_validate(principal)
    except ValidationError as exc:
        raise HTTPException(403, "scheduled_principal_invalid") from exc
    if user.identity != owner or not user.is_authenticated:
        raise HTTPException(403, "scheduled_principal_invalid")
    path = settings.app.SCHEDULED_PRINCIPAL_RESOLVER
    if path:
        module, separator, name = path.partition(":")
        if not separator:
            raise RuntimeError("SCHEDULED_PRINCIPAL_RESOLVER must be module:function")
        resolver = getattr(importlib.import_module(module), name)
        result = resolver(user=user.to_dict(), graph_id=graph_id, source=source)
        result = await result if inspect.isawaitable(result) else result
        if result is None:
            raise HTTPException(403, "scheduled_principal_revoked")
        try:
            user = User.model_validate(result)
        except ValidationError as exc:
            raise HTTPException(403, "scheduled_principal_invalid") from exc
        if user.identity != owner or not user.is_authenticated:
            raise HTTPException(403, "scheduled_principal_invalid")
    return user
