"""Only explicit runtime timer interrupts are eligible for automatic resume."""

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel

WAIT_MARKER = "aegra.wait_until.v1"


class TimedWakeup(BaseModel):
    interrupt_id: str
    checkpoint_id: str
    due_at: datetime


def extract_wakeups(snapshot: Any) -> list[TimedWakeup]:
    """Read persisted interrupt IDs; never turn ordinary human interrupts into timers."""
    checkpoint_id = (snapshot.config or {}).get("configurable", {}).get("checkpoint_id")
    result: list[TimedWakeup] = []
    for item in snapshot.interrupts:
        value = item.value
        if not isinstance(value, dict) or value.get("type") != WAIT_MARKER:
            continue
        if not checkpoint_id:
            raise ValueError("Timed waits require a persistent checkpoint")
        due_at = datetime.fromisoformat(value["due_at"])
        if due_at.tzinfo is None or due_at.utcoffset() is None:
            raise ValueError("Timed waits require an absolute timezone-aware deadline")
        result.append(
            TimedWakeup(
                interrupt_id=item.id,
                checkpoint_id=checkpoint_id,
                due_at=due_at.astimezone(UTC),
            )
        )
    return result
