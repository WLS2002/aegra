"""Run against a disposable server loading fixtures/native_runtime_graph.py."""

import asyncio
import os
from uuid import uuid4

import httpx
import pytest

URL = os.environ.get("AEGRA_NATIVE_TEST_URL")
pytestmark = pytest.mark.skipif(not URL, reason="Requires the isolated native runtime fixture server")


async def test_http_parent_child_timer_and_request_replay():
    assert URL in {"http://127.0.0.1:19541", "http://127.0.0.1:19542"}
    async with httpx.AsyncClient(base_url=URL, timeout=15) as client:

        async def request(method, path, **kwargs):
            response = await client.request(method, path, **kwargs)
            response.raise_for_status()
            return response.json()

        thread = await request("POST", "/threads", json={})
        tid = thread["thread_id"]
        body = {"assistant_id": "native_parent", "input": {"value": 1}, "on_disconnect": "continue"}
        headers = {"Idempotency-Key": str(uuid4())}
        first = await request("POST", f"/threads/{tid}/runs", json=body, headers=headers)
        again = await request("POST", f"/threads/{tid}/runs", json=body, headers=headers)
        assert first["run_id"] == again["run_id"]
        conflict = await client.post(f"/threads/{tid}/runs", json={**body, "input": {"value": 2}}, headers=headers)
        assert conflict.status_code == 409
        children = []
        for _ in range(150):
            children = await request("GET", f"/threads/{tid}/children")
            if len(children) == 2 and any(run["status"] == "success" for run in children):
                break
            await asyncio.sleep(0.2)
        assert len(children) == 2
        assert len({run["thread_id"] for run in children}) == 1
        assert all(run["parent"]["run_id"] == first["run_id"] for run in children)
        assert {run["status"] for run in children} == {"interrupted", "success"}
        state = await request("GET", f"/threads/{children[0]['thread_id']}/state")
        assert state["values"]["done"] is True and state["values"]["value"] == 42
        parent = await request("GET", f"/threads/{tid}/runs/{first['run_id']}")
        assert parent["status"] == "success"


async def test_http_human_interrupt_needs_explicit_resume_and_cancel_removes_timer():
    assert URL in {"http://127.0.0.1:19541", "http://127.0.0.1:19542"}
    async with httpx.AsyncClient(base_url=URL, timeout=15) as client:

        async def request(method, path, **kwargs):
            response = await client.request(method, path, **kwargs)
            response.raise_for_status()
            return response.json()

        async def start(graph, value):
            thread = await request("POST", "/threads", json={})
            tid = thread["thread_id"]
            run = await request(
                "POST",
                f"/threads/{tid}/runs",
                json={"assistant_id": graph, "input": value, "on_disconnect": "continue"},
            )
            for _ in range(100):
                current = await request("GET", f"/threads/{tid}/runs/{run['run_id']}")
                if current["status"] == "interrupted":
                    return tid, run
                await asyncio.sleep(0.1)
            pytest.fail("Synthetic fixture did not reach its interrupt")

        human_thread, human = await start("native_review", {})
        timer_thread, timer = await start("native_child", {"delay_seconds": 3})
        cancelled = await request("POST", f"/threads/{timer_thread}/runs/{timer['run_id']}/cancel?wait=1")
        assert cancelled["status"] in {"interrupted", "error"}
        await asyncio.sleep(4)
        assert len(await request("GET", f"/threads/{timer_thread}/runs")) == 1
        assert len(await request("GET", f"/threads/{human_thread}/runs")) == 1
        state = await request("GET", f"/threads/{human_thread}/state")
        assert not state["values"].get("done")
        resumed = await request(
            "POST",
            f"/threads/{human_thread}/runs",
            json={"assistant_id": "native_review", "command": {"resume": 73}, "on_disconnect": "continue"},
        )
        for _ in range(100):
            current = await request("GET", f"/threads/{human_thread}/runs/{resumed['run_id']}")
            if current["status"] == "success":
                break
            await asyncio.sleep(0.1)
        assert current["status"] == "success"
        state = await request("GET", f"/threads/{human_thread}/state")
        assert state["values"] == {"value": 73, "done": True}
