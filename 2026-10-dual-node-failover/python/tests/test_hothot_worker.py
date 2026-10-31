"""The hot-hot loop against a scripted upstream and sink."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from shared.work_contracts import EventPage, LoggedEvent, ResultRecord
from worker.client import WorkClient
from worker.config import StartFrom, WorkerConfig
from worker.hothot import CHECKPOINT_FILE, HotHotWorker
from worker.status import WorkerStatus

pytestmark = pytest.mark.anyio


class FakeFeed:
    """The upstream's event log and the sink, as the worker's client sees them."""

    def __init__(self, *amounts: int) -> None:
        self.events = [LoggedEvent(i + 1, f"acct-{i % 2}", amount) for i, amount in enumerate(amounts)]
        self.delivered: list[ResultRecord] = []
        self.upstream_up = True
        self.last_override: int | None = None
        self.sink_failures = 0
        self.reply = "applied"
        self.reads = 0

    def add(self, amount: int) -> None:
        self.events.append(LoggedEvent(len(self.events) + 1, "acct-0", amount))

    async def read_events(self, after: int, limit: int) -> EventPage | None:
        self.reads += 1
        await asyncio.sleep(0.001)
        if not self.upstream_up:
            return None
        last = len(self.events) if self.last_override is None else self.last_override
        return EventPage(tuple(self.events[after : after + limit]), last)

    async def last_event(self) -> int | None:
        await asyncio.sleep(0)
        return len(self.events) if self.upstream_up else None

    async def deliver(self, record: ResultRecord, timeout: float | None = None) -> str | None:
        await asyncio.sleep(0)
        if self.sink_failures > 0:
            self.sink_failures -= 1
            return None
        self.delivered.append(record)
        return self.reply


@pytest.fixture
def config(tmp_path: Path) -> WorkerConfig:
    return WorkerConfig(data_dir=str(tmp_path), retry_delay_ms=5, poll_wait_ms=20)


async def run_until(worker: HotHotWorker, condition: Callable[[], bool], what: str) -> None:
    running = asyncio.ensure_future(worker.run())
    try:
        deadline = time.monotonic() + 2
        while not condition():
            assert not running.done(), running.exception() if running.done() else None
            assert time.monotonic() < deadline, f"timed out waiting for {what}"
            await asyncio.sleep(0.005)
    finally:
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)


def make(feed: FakeFeed, config: WorkerConfig) -> tuple[HotHotWorker, WorkerStatus]:
    status = WorkerStatus()
    return HotHotWorker(feed, status, config), status  # type: ignore[arg-type]


def checkpoint(config: WorkerConfig) -> object:
    return json.loads((Path(config.data_dir) / CHECKPOINT_FILE).read_text())


async def test_every_event_is_delivered_then_checkpointed(config: WorkerConfig) -> None:
    feed = FakeFeed(10, 5, -3)
    worker, status = make(feed, config)

    await run_until(worker, lambda: status.ledger.offset == 3, "the batch")

    assert [(r.key, r.node, r.attempt, r.value) for r in feed.delivered] == [
        ("event-1", "a", 0, "acct-0:10"),
        ("event-2", "a", 0, "acct-1:5"),
        ("event-3", "a", 0, "acct-0:7"),
    ]
    assert checkpoint(config) == {"offset": 3, "balances": {"acct-0": 7, "acct-1": 5}}
    assert status.health(config)["sinkReplies"] == {"applied": 3}


async def test_a_restart_resumes_from_the_checkpoint(config: WorkerConfig) -> None:
    feed = FakeFeed(10, 5, -3, 1)
    first, status = make(feed, config)
    await run_until(first, lambda: status.ledger.offset == 4, "the first run")
    feed.delivered.clear()
    feed.add(100)

    again, restarted = make(feed, config)
    await run_until(again, lambda: restarted.ledger.offset == 5, "the second run")

    assert [(r.key, r.value) for r in feed.delivered] == [("event-5", "acct-0:107")]


async def test_a_delivery_is_retried_until_the_sink_answers(config: WorkerConfig) -> None:
    feed = FakeFeed(1)
    feed.sink_failures = 3
    feed.reply = "conflict"
    worker, status = make(feed, config)

    await run_until(worker, lambda: status.ledger.offset == 1, "the delivery")

    assert len(feed.delivered) == 1
    assert status.health(config)["sinkReplies"] == {"conflict": 1}


async def test_an_unreachable_upstream_is_asked_again_after_a_pause(config: WorkerConfig) -> None:
    feed = FakeFeed(1)
    feed.upstream_up = False
    worker, status = make(feed, config)
    running = asyncio.ensure_future(worker.run())
    await asyncio.sleep(0.05)
    tries = feed.reads
    feed.upstream_up = True

    deadline = time.monotonic() + 2
    while status.ledger.offset < 1 and time.monotonic() < deadline:
        await asyncio.sleep(0.005)
    running.cancel()
    await asyncio.gather(running, return_exceptions=True)

    assert 2 <= tries <= 15  # paused 5 ms between tries, not spinning
    assert status.ledger.offset == 1


async def test_an_upstream_that_lost_events_is_not_processed(
    config: WorkerConfig, caplog: pytest.LogCaptureFixture
) -> None:
    feed = FakeFeed(1, 2, 3)
    first, status = make(feed, config)
    await run_until(first, lambda: status.ledger.offset == 3, "the first run")
    feed.delivered.clear()
    restarted_upstream = FakeFeed(9)  # an in-memory upstream that restarted: its event 1 is not our event 1

    again, after = make(restarted_upstream, config)
    await run_until(again, lambda: restarted_upstream.reads >= 2, "a couple of reads")

    assert restarted_upstream.delivered == []
    assert after.ledger.offset == 3
    assert "Upstream ends at event 1 but this worker is at 3" in caplog.text


async def test_start_from_latest_saves_the_skip_and_keeps_old_balances(config: WorkerConfig) -> None:
    feed = FakeFeed(10)
    first, status = make(feed, config)
    await run_until(first, lambda: status.ledger.offset == 1, "the first run")
    feed.delivered.clear()
    feed.add(20)
    feed.add(30)  # events 2 and 3 happen while the worker is down
    skipping = WorkerConfig(data_dir=config.data_dir, retry_delay_ms=5, poll_wait_ms=20, start_from=StartFrom.LATEST)

    async def add_later() -> None:
        await asyncio.sleep(0.05)
        feed.add(5)

    adding = asyncio.ensure_future(add_later())
    again, restarted = make(feed, skipping)
    await run_until(again, lambda: restarted.ledger.offset == 4, "the event after the skip")
    await adding

    assert [(r.key, r.value) for r in feed.delivered] == [("event-4", "acct-0:15")]  # 10 + 5: 20 and 30 are missing
    assert checkpoint(config) == {"offset": 4, "balances": {"acct-0": 15}}


async def test_start_from_latest_with_nothing_new_keeps_the_position(config: WorkerConfig) -> None:
    feed = FakeFeed()
    feed.upstream_up = False
    skipping = WorkerConfig(data_dir=config.data_dir, retry_delay_ms=5, poll_wait_ms=20, start_from=StartFrom.LATEST)
    worker, status = make(feed, skipping)

    async def come_back() -> None:
        await asyncio.sleep(0.03)
        feed.upstream_up = True  # with nothing new: there is nothing to skip
        await asyncio.sleep(0.03)
        feed.add(4)

    coming = asyncio.ensure_future(come_back())
    await run_until(worker, lambda: status.ledger.offset == 1, "the event")
    await coming

    assert [(r.key, r.value) for r in feed.delivered] == [("event-1", "acct-0:4")]


async def test_a_nondeterministic_worker_stamps_its_clock(config: WorkerConfig) -> None:
    feed = FakeFeed(3)
    stamping = WorkerConfig(data_dir=config.data_dir, retry_delay_ms=5, poll_wait_ms=20, deterministic=False)
    worker, status = make(feed, stamping)
    before = int(time.time() * 1000)

    await run_until(worker, lambda: status.ledger.offset == 1, "the event")

    balance, stamp = feed.delivered[0].value.split("@")  # type: ignore[union-attr]
    assert balance == "acct-0:3"
    assert before <= int(stamp) <= int(time.time() * 1000)


def test_the_worker_uses_the_real_client_type() -> None:
    """The fake above must offer what HotHotWorker calls on the real client."""
    for name in ("read_events", "last_event", "deliver"):
        assert hasattr(WorkClient, name) and hasattr(FakeFeed, name)
