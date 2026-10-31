"""The upstream's HTTP pipeline over an in-process ASGI transport, with the reaper off unless a test runs the
lifespan."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

import httpx
import pytest
from fastapi import FastAPI

from helpers import FakeClock
from shared.clock import PeriodicTimer
from upstream.config import UpstreamConfig
from upstream.main import MAX_WAIT_S, create_app, wait_seconds
from upstream.task_queue import TaskQueue

pytestmark = pytest.mark.anyio


@pytest.fixture
def app(clock: FakeClock) -> FastAPI:
    return create_app(UpstreamConfig(), clock, run_reaper=False)


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://upstream") as http:
        yield http


async def test_events_can_be_appended_and_read(client: httpx.AsyncClient) -> None:
    created = await client.post("/events", json={"account": "acct-1", "amount": 5})
    page = await client.get("/events", params={"after": 0})
    health = await client.get("/health")

    assert (created.status_code, created.json()) == (201, {"seq": 1, "account": "acct-1", "amount": 5})
    assert created.headers["x-served-by"] == "upstream"
    assert page.json() == {"events": [{"seq": 1, "account": "acct-1", "amount": 5}], "last": 1}
    assert health.json()["lastEvent"] == 1
    assert health.json()["tasks"]["total"] == 0


async def test_query_parameters_bind_like_aspnet(client: httpx.AsyncClient) -> None:
    for i in range(3):
        await client.post("/events", json={"account": "acct", "amount": i})

    empty_after = await client.get("/events?after=&LIMIT=1&waitms=0")
    signed = await client.get("/events?after=+1")

    assert [event["seq"] for event in empty_after.json()["events"]] == [1]
    assert [event["seq"] for event in signed.json()["events"]] == [2, 3]


@pytest.mark.parametrize(
    "query",
    ["after=abc", "after=1.5", "after=-1", "limit=0", "limit=2147483648", "waitMs=x", "after=9223372036854775808"],
)
async def test_bad_query_parameters_are_400(client: httpx.AsyncClient, query: str) -> None:
    response = await client.get(f"/events?{query}")

    assert response.status_code == 400
    assert "error" in response.json()


async def test_reading_long_polls_until_an_event_arrives(client: httpx.AsyncClient) -> None:
    reading = asyncio.ensure_future(client.get("/events?after=0&waitMs=2000"))
    await asyncio.sleep(0.05)
    assert not reading.done()

    await client.post("/events", json={"account": "acct", "amount": 1})

    page = (await asyncio.wait_for(reading, 1.0)).json()
    assert page["last"] == 1


async def test_tasks_go_through_lease_heartbeat_and_complete(client: httpx.AsyncClient) -> None:
    created = await client.post("/tasks", json={"input": 3})

    leased = await client.post("/tasks/lease", json={"worker": "a"})
    held = (await client.get("/tasks/stats")).json()
    empty = await client.post("/tasks/lease", json={"worker": "b"})
    heartbeat = await client.post("/tasks/1/heartbeat", json={"worker": "a", "attempt": 1})
    complete = await client.post("/tasks/1/complete", json={"worker": "a", "attempt": 1})
    again = await client.post("/tasks/1/complete", json={"worker": "a", "attempt": 1})
    other = await client.post("/tasks/1/complete", json={"worker": "b", "attempt": 1})
    lost = await client.post("/tasks/1/heartbeat", json={"worker": "a", "attempt": 1})
    stats = (await client.get("/tasks/stats")).json()

    assert (created.status_code, created.json()) == (201, {"id": 1})
    assert leased.json() == {"id": 1, "input": 3, "attempt": 1, "ttlMs": 2000}
    assert (held["leased"], held["leases"]) == (1, [{"id": 1, "worker": "a", "attempt": 1}])
    assert (empty.status_code, empty.content) == (204, b"")
    assert (heartbeat.status_code, heartbeat.content) == (200, b"")
    assert complete.status_code == 200
    assert again.status_code == 200  # the same report twice is fine
    assert (other.status_code, other.json()) == (409, {"error": "lease lost; another attempt owns this task"})
    assert (lost.status_code, lost.json()) == (409, {"error": "lease lost"})
    assert (stats["done"], stats["staleCompletions"]) == (1, 1)


async def test_unknown_tasks_are_404(client: httpx.AsyncClient) -> None:
    unknown = await client.post("/tasks/7/complete", json={"worker": "a", "attempt": 1})
    not_a_number = await client.post("/tasks/seven/heartbeat", json={"worker": "a", "attempt": 1})

    assert (unknown.status_code, unknown.json()) == (404, {"error": "task 7 not found"})
    assert not_a_number.status_code == 404


@pytest.mark.parametrize(
    ("path", "body", "error"),
    [
        ("/events", {"account": "", "amount": 1}, "account must be 1-64"),
        ("/events", {"account": "acct", "amount": "2"}, "invalid JSON body at $.amount"),
        ("/events", {"account": "acct", "amount": 2.0}, "invalid JSON body at $.amount"),
        ("/events", {"account": "acct", "amount": True}, "invalid JSON body at $.amount"),
        ("/events", {"account": "acct", "amount": 2_000_000_000}, "amount must be between"),
        ("/tasks", {"input": "3"}, "invalid JSON body at $.input"),
        ("/tasks", {"input": 3_000_000_001}, "input must be between"),
        ("/tasks/lease", {"worker": "A B"}, "worker must be"),
        ("/tasks/lease", {"worker": "a", "waitMs": 2**31}, "invalid JSON body at $.waitMs"),
        ("/tasks/1/heartbeat", {"worker": "a", "attempt": "1"}, "invalid JSON body at $.attempt"),
        ("/tasks", None, "request body is required"),
    ],
)
async def test_bad_requests_are_400_with_an_error(
    client: httpx.AsyncClient, path: str, body: object, error: str
) -> None:
    await client.post("/tasks", json={"input": 1})

    response = await client.post(path, content=json.dumps(body), headers={"content-type": "application/json"})

    assert response.status_code == 400
    assert response.json()["error"].startswith(error)


async def test_a_non_json_body_is_400(client: httpx.AsyncClient) -> None:
    response = await client.post("/tasks", content=b"input=3", headers={"content-type": "text/plain"})

    assert (response.status_code, response.json()) == (400, {"error": "request body must be JSON"})


def test_waits_are_kept_between_zero_and_ten_seconds() -> None:
    assert (wait_seconds(-5), wait_seconds(500), wait_seconds(60_000)) == (0.0, 0.5, MAX_WAIT_S)


async def test_a_lease_request_whose_caller_hung_up_takes_no_task(app: FastAPI) -> None:
    """Calls the ASGI app directly, so the test decides when the client goes away."""
    queue: TaskQueue = app.state.tasks
    hang_up = asyncio.Event()
    sent: list[dict[str, object]] = []
    body = json.dumps({"worker": "a", "waitMs": 5000}).encode()
    # The body, then an empty message the server may send while it waits (the hang-up watcher must skip it).
    messages = [{"type": "http.request", "body": b"", "more_body": False}, {"type": "http.request", "body": body}]

    async def receive() -> dict[str, object]:
        if messages:
            return messages.pop()
        await hang_up.wait()
        return {"type": "http.disconnect"}

    async def send(message: dict[str, object]) -> None:
        sent.append(message)

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/tasks/lease",
        "raw_path": b"/tasks/lease",
        "query_string": b"",
        "headers": [(b"content-type", b"application/json"), (b"host", b"upstream")],
        "server": ("upstream", 80),
        "client": ("127.0.0.1", 1234),
        "root_path": "",
    }
    request = asyncio.ensure_future(app(scope, receive, send))
    await asyncio.sleep(0.05)
    hang_up.set()
    await asyncio.wait_for(request, 1.0)

    queue.add(1)

    assert queue.stats().queued == 1  # nobody is waiting any more
    assert sent[0]["status"] == 204


async def test_the_reaper_requeues_expired_leases_in_the_background(clock: FakeClock) -> None:
    failures: list[BaseException] = []
    app = create_app(UpstreamConfig(task_lease_ttl_ms=100), clock, on_fatal=failures.append)
    app_state = app.state.tasks
    app_state.add(1)
    await app_state.lease("a", 0)

    async with app.router.lifespan_context(app):
        clock.advance(0.1)
        await asyncio.sleep(0.25)
        stats = app_state.stats()

    assert (stats.queued, stats.reassignments) == (1, 1)
    assert failures == []


async def test_periodic_timer_skips_ticks_it_missed() -> None:
    timer = PeriodicTimer(0.02)
    loop = asyncio.get_running_loop()
    started = loop.time()

    await timer.tick()
    first = loop.time() - started
    await asyncio.sleep(0.07)  # three ticks missed
    before = loop.time()
    await timer.tick()  # fires at once
    caught_up = loop.time() - before
    await timer.tick()  # back on the grid: less than a full interval later
    on_grid = loop.time() - before

    assert 0.015 <= first < 0.06
    assert caught_up < 0.01
    assert on_grid < 0.03


def test_create_app_reads_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASK_LEASE_TTL_MS", "500")

    assert create_app(run_reaper=False).state.tasks.stats().total == 0
    monkeypatch.setenv("TASK_LEASE_TTL_MS", "50")
    with pytest.raises(SystemExit, match="upstream: TASK_LEASE_TTL_MS"):
        create_app()
