"""Server-sent events: the push alternative to polling a ticket."""

import asyncio
import json
from collections.abc import AsyncIterator
from dataclasses import replace
from typing import Any

import pytest
from redis.asyncio import Redis

from checkout_queue import events
from checkout_queue.ids import new_id
from support import AppUnderTest, SaleUnderTest, running_app

pytestmark = pytest.mark.anyio

EVENT_TIMEOUT_S = 5.0


async def join(app: AppUnderTest, user_id: str) -> str:
    return (await app.http.post("/queue/join", json={"userId": user_id})).json()["ticketId"]


async def next_line(lines: AsyncIterator[str]) -> str | None:
    try:
        return await asyncio.wait_for(anext(lines), EVENT_TIMEOUT_S)
    except StopAsyncIteration:
        return None


async def next_event(lines: AsyncIterator[str]) -> dict[str, Any]:
    """Reads lines up to the blank line that ends an event and returns its data. Skips comments."""
    data = None
    while True:
        line = await next_line(lines)
        if line is None:
            raise EOFError("stream ended")
        if line.startswith("data: "):
            data = line[len("data: ") :]
        elif line == "" and data is not None:
            return json.loads(data)


async def wait_until(condition: Any, timeout_s: float = EVENT_TIMEOUT_S) -> None:
    async with asyncio.timeout(timeout_s):
        while not condition():
            await asyncio.sleep(0.01)


async def test_watch_returns_the_views_and_counts_as_presence(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis, lambda c: replace(c, max_active=2))
    watched = (await sale.queue.join("streaming")).ticket
    silent = (await sale.queue.join("silent")).ticket

    sale.clock.advance(16_000)
    views = await sale.queue.watch([watched.ticket_id, new_id()])
    await sale.queue.admit()

    assert [v.ticket_id for v in views] == [watched.ticket_id]
    assert (await sale.queue.get_ticket(watched.ticket_id)).status == "admitted"
    assert (await sale.queue.get_ticket(silent.ticket_id)).status == "abandoned"


async def test_events_start_with_the_ticket_and_push_the_admission_right_away(redis: Redis) -> None:
    async with running_app(redis) as app:
        ticket_id = await join(app, "listener")
        async with app.http.stream("GET", f"/queue/tickets/{ticket_id}/events") as response:
            lines = response.aiter_lines()
            first = await next_event(lines)
            await app.maintenance.run_once()  # admit.lua publishes; no clock tick needed
            second = await next_event(lines)

    assert response.status_code == 200
    assert response.headers["content-type"] == "text/event-stream"
    assert response.headers["cache-control"] == "no-cache"
    assert response.headers["x-accel-buffering"] == "no"
    assert response.headers["x-served-by"] == "test-instance"
    assert (first["status"], first["position"], first["pollAfterMs"]) == ("waiting", 1, 1000)
    assert second["status"] == "admitted"


async def test_events_end_once_the_ticket_can_no_longer_change(redis: Redis) -> None:
    async with running_app(redis) as app:
        ticket_id = await join(app, "buyer")
        await app.maintenance.run_once()
        async with app.http.stream("GET", f"/queue/tickets/{ticket_id}/events") as response:
            lines = response.aiter_lines()
            assert (await next_event(lines))["status"] == "admitted"

            await app.http.post("/checkout", json={"ticketId": ticket_id, "quantity": 1})
            app.clock.advance(1_000)
            last = await next_event(lines)

            assert last["status"] == "ordered"
            assert await next_line(lines) is None
        await wait_until(lambda: not app.services.streams.ticket_ids)


async def test_position_updates_arrive_with_the_periodic_refresh(redis: Redis) -> None:
    async with running_app(redis, lambda c: replace(c, max_active=1)) as app:
        await join(app, "first")
        second = await join(app, "second")
        async with app.http.stream("GET", f"/queue/tickets/{second}/events") as response:
            lines = response.aiter_lines()
            assert (await next_event(lines))["position"] == 2

            await app.maintenance.run_once()  # admits "first" only; "second" is not told over pub/sub
            app.clock.advance(1_000)
            moved_up = await next_event(lines)

    assert (moved_up["status"], moved_up["position"]) == ("waiting", 1)


async def test_a_quiet_stream_sends_keep_alive_comments(redis: Redis, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(events, "KEEP_ALIVE_S", 0.05)
    async with running_app(redis) as app:
        ticket_id = await join(app, "patient")
        async with app.http.stream("GET", f"/queue/tickets/{ticket_id}/events") as response:
            lines = response.aiter_lines()
            await next_event(lines)
            comment = await next_line(lines)

    assert comment == ": keep-alive"


async def test_a_client_that_disconnects_is_unregistered_and_stops_counting_as_present(redis: Redis) -> None:
    async with running_app(redis) as app:
        ticket_id = await join(app, "leaver")
        async with app.http.stream("GET", f"/queue/tickets/{ticket_id}/events") as response:
            await next_event(response.aiter_lines())
            assert app.services.streams.ticket_ids == [ticket_id]

        await wait_until(lambda: not app.services.streams.ticket_ids)


async def test_two_streams_for_one_ticket_both_get_the_admission(redis: Redis) -> None:
    async with running_app(redis) as app:
        ticket_id = await join(app, "two-tabs")
        path = f"/queue/tickets/{ticket_id}/events"
        async with app.http.stream("GET", path) as tab1, app.http.stream("GET", path) as tab2:
            lines1, lines2 = tab1.aiter_lines(), tab2.aiter_lines()
            await next_event(lines1)
            await next_event(lines2)

            await app.maintenance.run_once()

            assert (await next_event(lines1))["status"] == "admitted"
            assert (await next_event(lines2))["status"] == "admitted"
