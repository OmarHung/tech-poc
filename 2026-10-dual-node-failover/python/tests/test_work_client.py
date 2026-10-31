"""How the worker's client turns every kind of answer, or none, into a result."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable

import httpx
import pytest

from shared.work_contracts import EventPage, LoggedEvent, ResultRecord, TaskLease
from worker.client import WorkClient
from worker.config import WorkerConfig

pytestmark = pytest.mark.anyio

LEASE = TaskLease(4, 2, 3, 2000)
RECORD = ResultRecord("task-4", "a", 3, "2^2=4")
Handler = Callable[[httpx.Request], httpx.Response]


@pytest.fixture
async def make() -> AsyncIterator[Callable[[Handler], WorkClient]]:
    clients: list[WorkClient] = []

    def create(handler: Handler) -> WorkClient:
        config = WorkerConfig(upstream_url="http://upstream/", sink_url="http://sink/", rpc_timeout_ms=100)
        client = WorkClient(config, httpx.MockTransport(handler))
        clients.append(client)
        return client

    yield create
    for client in clients:
        await client.aclose()


def down(request: httpx.Request) -> httpx.Response:
    raise httpx.ConnectError("refused", request=request)


async def test_requests_carry_the_worker_and_the_poll_wait(make) -> None:
    seen: list[httpx.Request] = []

    def answer(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == "/events":
            return httpx.Response(200, json={"events": [{"seq": 1, "account": "x", "amount": 2}], "last": 9})
        if request.url.path == "/tasks/lease":
            return httpx.Response(200, json=LEASE.to_wire())
        return httpx.Response(200)

    client = make(answer)

    page = await client.read_events(0, 100)
    last = await client.last_event()
    reached, lease = await client.lease_task()
    beat = await client.heartbeat(LEASE, 0.1)
    done = await client.complete(LEASE)

    assert page == EventPage((LoggedEvent(1, "x", 2),), 9)
    assert last == 9
    assert (reached, lease, beat, done) == (True, LEASE, True, True)
    assert str(seen[0].url) == "http://upstream/events?after=0&limit=100&waitMs=500"
    assert str(seen[1].url) == "http://upstream/events?after=0&limit=1&waitMs=0"
    assert json.loads(seen[2].content) == {"worker": "a", "waitMs": 500}
    assert (seen[3].url.path, json.loads(seen[3].content)) == ("/tasks/4/heartbeat", {"worker": "a", "attempt": 3})
    assert seen[4].url.path == "/tasks/4/complete"
    assert seen[4].headers["content-type"] == "application/json"


async def test_unreachable_means_none(make) -> None:
    client = make(down)

    assert await client.read_events(0, 100) is None
    assert await client.last_event() is None
    assert await client.lease_task() == (False, None)
    assert await client.heartbeat(LEASE, 0.1) is None
    assert await client.complete(LEASE) is None
    assert await client.deliver(RECORD) is None


async def test_a_call_that_hangs_is_cut_off_at_its_timeout(make) -> None:
    async def hang(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(10)
        return httpx.Response(200)  # pragma: no cover

    client = WorkClient(WorkerConfig(sink_url="http://sink/"), httpx.MockTransport(hang))  # type: ignore[arg-type]
    loop = asyncio.get_running_loop()
    started = loop.time()

    assert await client.deliver(RECORD, timeout=0.05) is None
    assert loop.time() - started < 0.5
    await client.aclose()


@pytest.mark.parametrize(
    ("status", "body", "expected"),
    [
        (204, None, (True, None)),
        (200, {"id": "x"}, (True, None)),  # garbage lease: reached, so ask again right away
        (400, {"error": "bad"}, (False, None)),
        (503, None, (False, None)),
    ],
)
async def test_lease_answers(make, status: int, body: object, expected: tuple[bool, None]) -> None:
    client = make(lambda request: httpx.Response(status, json=body))

    assert await client.lease_task() == expected


@pytest.mark.parametrize(("status", "expected"), [(200, True), (409, False), (404, False), (400, None), (500, None)])
async def test_task_call_answers(make, status: int, expected: bool | None) -> None:
    client = make(lambda request: httpx.Response(status, json={"error": "x"}))

    assert await client.heartbeat(LEASE, 0.1) is expected
    assert await client.complete(LEASE) is expected


@pytest.mark.parametrize(
    ("status", "body", "expected"),
    [
        (201, {"status": "applied"}, "applied"),
        (409, {"status": "stale"}, "stale"),
        (400, {"error": "bad"}, None),
        (500, {"status": "applied"}, None),
        (200, "not an object", None),
    ],
)
async def test_deliver_answers(make, status: int, body: object, expected: str | None) -> None:
    seen: list[httpx.Request] = []

    def answer(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(status, json=body)

    client = make(answer)

    assert await client.deliver(RECORD) == expected
    assert (seen[0].url, json.loads(seen[0].content)) == ("http://sink/results", RECORD.to_wire())


@pytest.mark.parametrize(
    ("status", "content"), [(500, b'{"events":[],"last":0}'), (200, b"<html>"), (200, b'{"events":[{"seq":1}]}')]
)
async def test_a_bad_event_page_is_none(make, status: int, content: bytes) -> None:
    client = make(lambda request: httpx.Response(status, content=content))

    assert await client.read_events(0, 10) is None
    assert await client.last_event() is None


@pytest.mark.parametrize(("status", "expected"), [(200, True), (204, True), (503, False), (404, False)])
async def test_the_sink_probe_asks_for_its_health(make, status: int, expected: bool) -> None:
    seen: list[httpx.Request] = []

    def answer(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(status)

    assert await make(answer).sink_reachable() is expected
    assert (seen[0].method, str(seen[0].url)) == ("GET", "http://sink/health")


async def test_an_unreachable_sink_fails_the_probe(make) -> None:
    assert await make(down).sink_reachable() is False
