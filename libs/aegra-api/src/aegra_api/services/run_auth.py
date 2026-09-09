"""Shared authorization for HTTP and trusted graph run creation."""

from collections.abc import Awaitable, Callable
from typing import Any

from aegra_api.core.auth_handlers import AuthContextWrapper, build_auth_context, handle_event
from aegra_api.models import RunCreate, User


async def apply_run_authorization(
    user: User,
    thread_id: str,
    request: RunCreate,
    *,
    event_handler: Callable[
        [AuthContextWrapper | None, dict[str, Any]], Awaitable[dict[str, Any] | None]
    ] = handle_event,
) -> None:
    """Authorize threads.create_run and merge config/context overrides into request.

    Handler-returned filter dict wins; otherwise fall back to in-place value mutations.
    """
    ctx = build_auth_context(user, "threads", "create_run")
    value = {**request.model_dump(), "thread_id": thread_id}
    filters = await event_handler(ctx, value)

    source = filters if filters is not None else value
    config_overrides = source.get("config")
    if isinstance(config_overrides, dict):
        request.config = {**(request.config or {}), **config_overrides}
    context_overrides = source.get("context")
    if isinstance(context_overrides, dict):
        request.context = {**(request.context or {}), **context_overrides}

    # Creating a run also reads its assistant, so per-assistant handler rules
    # apply here exactly as they do in the cron-create chain.
    await event_handler(
        build_auth_context(user, "assistants", "read"),
        {"assistant_id": request.assistant_id},
    )
