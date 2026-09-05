import asyncio
import os
from collections.abc import AsyncIterator
from urllib.parse import urlparse
from uuid import uuid4

import httpx
import pytest

from tests.e2e._utils import elog

pytestmark = pytest.mark.e2e


@pytest.fixture
async def admission_clients() -> AsyncIterator[list[httpx.AsyncClient]]:
    urls = os.getenv("FORK_TEST_URLS", "").split(",")
    if not urls[0]:
        pytest.skip("FORK_TEST_URLS must select the isolated echo graph stack")
    allowed = {"aegra", "aegra-peer", "127.0.0.1", "localhost"}
    assert all(urlparse(url).hostname in allowed for url in urls)
    headers = {"Authorization": "Bearer " + os.environ["FORK_TEST_TOKEN"]}
    clients = [httpx.AsyncClient(base_url=url, headers=headers, timeout=30) for url in urls]
    try:
        for client in clients:
            response = await client.post("/assistants/search", json={"limit": 100})
            response.raise_for_status()
            assert any(a["graph_id"] == "echo" for a in response.json())
        yield clients
    finally:
        for client in clients:
            await client.aclose()


async def settle(client: httpx.AsyncClient, thread_id: str, run_id: str) -> str:
    for _ in range(100):
        response = await client.get(f"/threads/{thread_id}/runs/{run_id}")
        response.raise_for_status()
        status = response.json()["status"]
        if status not in {"pending", "running"}:
            return status
        await asyncio.sleep(0.1)
    raise AssertionError("Run did not settle")


async def cancel(client: httpx.AsyncClient, thread_id: str, run_id: str) -> None:
    response = await client.post(f"/threads/{thread_id}/runs/{run_id}/cancel", params={"wait": 0, "action": "cancel"})
    response.raise_for_status()
    assert await settle(client, thread_id, run_id) == "interrupted"


@pytest.mark.parametrize("precreate", [True, False])
async def test_simultaneous_reject_accepts_exactly_one_across_instances(
    admission_clients: list[httpx.AsyncClient], precreate: bool
) -> None:
    clients = admission_clients
    thread_id = str(uuid4())
    client = clients[0]
    if precreate:
        response = await client.post("/threads", json={"thread_id": thread_id})
        response.raise_for_status()
    body = {"assistant_id": "echo", "input": {"message": "admission test", "delay": 30}, "multitask_strategy": "reject"}
    try:
        results = await asyncio.gather(
            *(clients[i % len(clients)].post(f"/threads/{thread_id}/runs", json=body) for i in range(20))
        )
        statuses = [r.status_code for r in results]
        elog("Concurrent admission status counts", {str(s): statuses.count(s) for s in set(statuses)})
        assert statuses.count(200) == 1
        assert statuses.count(409) == 19
        run_id = next(r.json()["run_id"] for r in results if r.status_code == 200)
        for suffix in ["/stream", "/wait"]:
            assert (await clients[-1].post(f"/threads/{thread_id}/runs{suffix}", json=body)).status_code == 409
        rows = (await client.get(f"/threads/{thread_id}/runs")).json()
        assert len(rows) == 1
        await cancel(client, thread_id, run_id)

        for data, expected in [
            ({"message": "completed", "delay": 0, "fail": False}, "success"),
            ({"delay": 0, "fail": True}, "error"),
        ]:
            response = await client.post(f"/threads/{thread_id}/runs", json={**body, "input": data})
            response.raise_for_status()
            assert await settle(client, thread_id, response.json()["run_id"]) == expected
    finally:
        await client.delete(f"/threads/{thread_id}")


async def test_different_threads_can_run_concurrently(admission_clients: list[httpx.AsyncClient]) -> None:
    threads = [str(uuid4()), str(uuid4())]
    clients = admission_clients
    body = {"assistant_id": "echo", "input": {"delay": 30}, "multitask_strategy": "reject"}
    try:
        responses = await asyncio.gather(
            *(clients[i % len(clients)].post(f"/threads/{tid}/runs", json=body) for i, tid in enumerate(threads))
        )
        assert all(r.status_code == 200 for r in responses)
        for tid, response in zip(threads, responses, strict=True):
            await cancel(clients[0], tid, response.json()["run_id"])
    finally:
        for tid in threads:
            await clients[0].delete(f"/threads/{tid}")


async def test_bound_cron_does_not_overlap_active_run(admission_clients: list[httpx.AsyncClient]) -> None:
    client = admission_clients[0]
    thread_id = str(uuid4())
    cron_id: str | None = None
    body = {"assistant_id": "echo", "input": {"delay": 30}, "multitask_strategy": "reject"}
    try:
        response = await client.post(f"/threads/{thread_id}/runs", json=body)
        response.raise_for_status()
        run_id = response.json()["run_id"]
        response = await client.post(
            f"/threads/{thread_id}/runs/crons",
            json={**body, "schedule": "* * * * * *", "enabled": False},
        )
        response.raise_for_status()
        cron_id = response.json()["cron_id"]
        (await client.patch(f"/runs/crons/{cron_id}", json={"enabled": True})).raise_for_status()
        await asyncio.sleep(5)
        assert len((await client.get(f"/threads/{thread_id}/runs")).json()) == 1
        await cancel(client, thread_id, run_id)
        for _ in range(100):
            rows = (await client.get(f"/threads/{thread_id}/runs")).json()
            if len(rows) == 2:
                break
            await asyncio.sleep(0.1)
        assert len(rows) == 2
        (await client.patch(f"/runs/crons/{cron_id}", json={"enabled": False})).raise_for_status()
        await cancel(client, thread_id, next(r["run_id"] for r in rows if r["run_id"] != run_id))
    finally:
        if cron_id:
            await client.delete(f"/runs/crons/{cron_id}")
        await client.delete(f"/threads/{thread_id}")
