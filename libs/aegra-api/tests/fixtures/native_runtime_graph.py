"""Synthetic HTTP-worker acceptance; never registered in an application manifest."""

from datetime import UTC, datetime, timedelta
from typing import TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

from aegra_api.runtime import create_child, wait_until


class State(TypedDict, total=False):
    delay_seconds: int
    due_at: str
    child_thread: str
    value: int
    done: bool


async def spawn(state):
    child = await create_child(
        "native_child", key="one-child", input={"value": 42, "delay_seconds": state.get("delay_seconds", 2)}
    )
    return {"child_thread": child.thread_id}


def deadline(state):
    return {"due_at": (datetime.now(UTC) + timedelta(seconds=state.get("delay_seconds", 2))).isoformat()}


def wake(state):
    wait_until(datetime.fromisoformat(state["due_at"]))
    return {"done": True}


def human(state):
    return {"value": interrupt({"question": "Synthetic resume value"}), "done": True}


parent_builder = StateGraph(State)
parent_builder.add_node("spawn", spawn)
parent_builder.add_edge(START, "spawn")
parent_builder.add_edge("spawn", END)
parent = parent_builder.compile()
child_builder = StateGraph(State)
child_builder.add_node("deadline", deadline)
child_builder.add_node("wake", wake)
child_builder.add_edge(START, "deadline")
child_builder.add_edge("deadline", "wake")
child_builder.add_edge("wake", END)
child = child_builder.compile()
human_builder = StateGraph(State)
human_builder.add_node("human", human)
human_builder.add_edge(START, "human")
human_builder.add_edge("human", END)
review = human_builder.compile()
