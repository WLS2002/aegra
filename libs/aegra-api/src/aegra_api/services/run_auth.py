"""Shared authorization for HTTP and trusted graph run creation."""

from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import HTTPException
from sqlalchemy import or_, select

from aegra_api.core.auth_handlers import AuthContextWrapper, build_auth_context, handle_event
from aegra_api.core.orm import Assistant, _get_session_maker
from aegra_api.models import RunCreate, User
from aegra_api.services.langgraph_service import get_langgraph_service
from aegra_api.utils.assistants import resolve_assistant_id


async def resolve_authorization_target(user: User, requested_id: str) -> tuple[str, str]:
    """Resolve trusted graph identity without accepting graph_id from a request."""
    graphs = get_langgraph_service().list_graphs()
    resolved = resolve_assistant_id(requested_id, graphs)
    for graph_id in graphs:
        if resolve_assistant_id(graph_id, graphs) == resolved:
            return resolved, graph_id
    async with _get_session_maker()() as session:
        assistant = await session.scalar(
            select(Assistant).where(
                Assistant.assistant_id == resolved,
                or_(Assistant.user_id == user.identity, Assistant.user_id == "system"),
            )
        )
        if assistant is None or assistant.graph_id not in graphs:
            raise HTTPException(404, "Assistant not found")
        return str(assistant.assistant_id), assistant.graph_id


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
    resolved_id, graph_id = await resolve_authorization_target(user, str(request.assistant_id))
    value.update(graph_id=graph_id, resolved_assistant_id=resolved_id)
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
        {"assistant_id": request.assistant_id, "resolved_assistant_id": resolved_id, "graph_id": graph_id},
    )
