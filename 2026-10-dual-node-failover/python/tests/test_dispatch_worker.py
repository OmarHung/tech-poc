"""The dispatch loop against a scripted upstream queue and sink, on short leases."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from helpers import FakeClock
from shared.clock import MonotonicClock
from shared.work_contracts import ResultRecord, TaskLease
from worker.config import WorkerConfig, WorkerMode
from worker.dispatch import DispatchWorker
from worker.lease_tracker import LeaseTracker, lease_end
from worker.status import WorkerStatus

pytestmark = pytest.mark.anyio


class FakeQueue:
    """The upstream's task queue and the sink, as the worker's client sees them. Records every call."""

    def __init__(self, status: WorkerStatus) -> None:
        self.status = status
        self.leases: list[tuple[bool, TaskLease | None]] = []
        self.heartbeat_reply: bool | None = True
        self.heartbeat_error: Exception | None = None
        self.deliver_reply: str | None = "applied"
        self.deliver_takes = 0.0
        self.complete_replies: list[bool | None] = [True]
        self.probe_replies: list[bool] = []
        self.calls: list[tuple[str, object]] = []
        self.lease_calls = 0

    async def lease_task(self) -> tuple[bool, TaskLease | None]:
        self.lease_calls += 1
        self.calls.append(("lease", None))
        if self.leases:
            return self.leases.pop(0)
        await asyncio.sleep(0.01)  # an empty long-poll
        return True, None

    async def heartbeat(self, lease: TaskLease, timeout: float) -> bool | None:
        self.calls.append(("heartbeat", timeout))
        if self.heartbeat_error is not None:
            raise self.heartbeat_error
        return self.heartbeat_reply

    async def deliver(self, record: ResultRecord, timeout: float | None = None) -> str | None:
        self.calls.append(("deliver", (record, timeout, self.status.phase)))
        await asyncio.sleep(min(self.deliver_takes, timeout or 0))
        return self.deliver_reply

    async def sink_reachable(self) -> bool:
        reachable = self.probe_replies.pop(0) if self.probe_replies else True
        self.calls.append(("probe", reachable))
        await asyncio.sleep(0)
        return reachable

    async def complete(self, lease: TaskLease) -> bool | None:
        self.calls.append(("complete", self.status.phase))
        await asyncio.sleep(0)
        return self.complete_replies.pop(0) if len(self.complete_replies) > 1 else self.complete_replies[0]

    def called(self, name: str) -> list[object]:
        return [args for call, args in self.calls if call == name]


Make = Callable[..., tuple[DispatchWorker, FakeQueue, WorkerStatus]]


@pytest.fixture
def make(tmp_path: Path) -> Make:
    def create(**overrides: object) -> tuple[DispatchWorker, FakeQueue, WorkerStatus]:
        settings: dict[str, object] = {"mode": WorkerMode.DISPATCH, "data_dir": str(tmp_path), "retry_delay_ms": 20}
        config = WorkerConfig(**{**settings, **overrides})  # type: ignore[arg-type]
        status = WorkerStatus()
        queue = FakeQueue(status)
        return DispatchWorker(queue, status, config, MonotonicClock()), queue, status  # type: ignore[arg-type]

    return create


async def run_task(worker: DispatchWorker, ttl_ms: int, task_input: int = 2) -> None:
    clock = worker._clock  # the worker's own clock, as its loop would use
    lease = TaskLease(4, task_input, 3, ttl_ms)
    await asyncio.wait_for(worker.run_task(lease, LeaseTracker(clock, lease_end(clock.now(), ttl_ms))), 5)


async def test_a_task_is_worked_delivered_then_completed(make: Make) -> None:
    worker, queue, status = make(work_ms=10)

    await run_task(worker, 2000)

    record, timeout, phase = queue.called("deliver")[0]  # type: ignore[misc]
    assert record == ResultRecord("task-4", "a", 3, "2^2=4")
    assert phase == "delivering"
    assert 0 < timeout <= 1.0
    assert queue.called("complete") == ["completing"]
    health = status.health(worker._config)
    assert (health["completed"], health["sinkReplies"], health["phase"]) == (1, {"applied": 1}, None)


async def test_a_task_is_abandoned_when_the_lease_ran_out_during_the_work(make: Make) -> None:
    worker, queue, status = make(work_ms=150)
    queue.heartbeat_reply = None  # the upstream cannot be reached, so the lease is never extended

    await run_task(worker, 100)

    assert queue.called("deliver") == []
    assert status.health(worker._config)["abandoned"] == 1


async def test_without_the_lease_check_a_late_result_is_written_anyway(make: Make) -> None:
    worker, queue, status = make(work_ms=150, lease_check=False)
    queue.heartbeat_reply = None
    queue.complete_replies = [False]  # another attempt owns the task by now

    await run_task(worker, 100)

    _, timeout, _ = queue.called("deliver")[0]  # type: ignore[misc]
    assert timeout == 1.0  # the full RPC timeout, not what is left of a lease that is gone
    assert status.health(worker._config)["staleCompletions"] == 1


async def test_a_lost_lease_reported_by_a_heartbeat_ends_the_task(make: Make) -> None:
    worker, queue, status = make(work_ms=250)
    queue.heartbeat_reply = False

    await run_task(worker, 300)

    assert len(queue.called("heartbeat")) == 1  # revoked: no more heartbeats
    assert queue.called("deliver") == []
    assert status.health(worker._config)["abandoned"] == 1


async def test_heartbeats_keep_a_task_longer_than_the_lease_alive(make: Make) -> None:
    worker, queue, status = make(work_ms=700)

    await run_task(worker, 300)

    beats = queue.called("heartbeat")
    assert len(beats) >= 5
    assert all(timeout == pytest.approx(0.1) for timeout in beats)  # each call bounded by the interval
    assert status.health(worker._config)["completed"] == 1


async def test_heartbeats_keep_their_schedule_when_one_call_hangs(make: Make) -> None:
    worker, queue, status = make(work_ms=1_000)
    sent: list[float] = []
    started = time.monotonic()

    async def hung_then_fine(lease: TaskLease, timeout: float) -> bool | None:
        sent.append(time.monotonic() - started)
        if len(sent) == 1:
            await asyncio.sleep(timeout)  # hangs until its bound
            return None
        return True

    queue.heartbeat = hung_then_fine  # type: ignore[method-assign]
    await run_task(worker, 600)  # a heartbeat every 200 ms

    assert status.health(worker._config)["completed"] == 1
    assert len(sent) >= 4
    # The one after the hung call goes out on the grid (right as the hung one is cut off), not 200 ms after it.
    assert sent[1] - sent[0] == pytest.approx(0.2, abs=0.08)
    assert sent[2] - sent[0] == pytest.approx(0.4, abs=0.08)


async def test_a_worker_that_cannot_reach_the_sink_gives_the_task_up_after_one_lease(make: Make) -> None:
    worker, queue, status = make(work_ms=0)
    queue.deliver_reply = None
    started = time.monotonic()

    await run_task(worker, 300)

    elapsed = time.monotonic() - started
    assert 0.3 <= elapsed < 0.6
    assert len(queue.called("deliver")) >= 3
    assert queue.called("complete") == []
    assert status.health(worker._config)["abandoned"] == 1


async def test_each_sink_write_is_bounded_by_what_is_left_of_the_lease(make: Make) -> None:
    worker, queue, status = make(work_ms=0)
    queue.heartbeat_reply = None
    queue.deliver_reply = None
    queue.deliver_takes = 10.0  # every write hangs until its timeout

    await run_task(worker, 500)

    timeouts = [timeout for _, timeout, _ in queue.called("deliver")]  # type: ignore[misc]
    assert len(timeouts) == 1
    assert timeouts[0] == pytest.approx(0.45, abs=0.02)  # 90% of the TTL, well under the 1 s RPC timeout
    assert status.health(worker._config)["abandoned"] == 1


async def test_a_write_cannot_start_once_the_lease_is_gone(make: Make, clock: FakeClock) -> None:
    worker, queue, status = make(work_ms=0)
    lease = TaskLease(4, 2, 3, 1000)
    gone = LeaseTracker(clock, clock.now() - 1)

    assert await worker._deliver(lease, gone, ResultRecord("task-4", "a", 3, "2^2=4")) is False
    assert queue.called("deliver") == []
    assert status.health(worker._config)["abandoned"] == 1


async def test_completion_is_retried_until_the_upstream_answers(make: Make) -> None:
    worker, queue, status = make(work_ms=0)
    queue.complete_replies = [None, None, True]

    await run_task(worker, 2000)

    assert len(queue.called("complete")) == 3
    assert status.health(worker._config)["completed"] == 1


async def test_a_crashed_heartbeat_loop_is_not_swallowed(make: Make) -> None:
    worker, queue, _ = make(work_ms=300)
    queue.heartbeat_error = RuntimeError("bug in the heartbeat loop")

    with pytest.raises(RuntimeError, match="bug in the heartbeat loop"):
        await run_task(worker, 300)


async def test_the_loop_runs_leased_tasks_and_pauses_only_when_the_upstream_is_unreachable(make: Make) -> None:
    worker, queue, status = make(work_ms=0)
    queue.leases = [(True, TaskLease(1, 5, 1, 2000)), (False, None), (False, None)]
    running = asyncio.ensure_future(worker.run())

    await asyncio.sleep(0.03)
    after_failures = queue.lease_calls
    await asyncio.sleep(0.06)
    running.cancel()
    await asyncio.gather(running, return_exceptions=True)

    assert status.health(worker._config)["completed"] == 1
    assert after_failures <= 3  # two unreachable answers: a 20 ms pause after each, no spinning
    assert queue.called("deliver")[0][0].value == "5^2=25"  # type: ignore[index]


async def run_loop_until(worker: DispatchWorker, condition: Callable[[], bool], what: str) -> None:
    running = asyncio.ensure_future(worker.run())
    try:
        deadline = time.monotonic() + 3
        while not condition():
            assert not running.done(), "the loop stopped"
            assert time.monotonic() < deadline, f"timed out waiting for {what}"
            await asyncio.sleep(0.005)
    finally:
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)


async def test_after_giving_a_task_up_for_the_sink_no_task_is_taken_until_the_sink_answers(
    make: Make, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger="worker.dispatch")
    worker, queue, status = make(work_ms=0)
    queue.deliver_reply = None
    queue.leases = [(True, TaskLease(1, 5, 1, 300)), (True, TaskLease(2, 6, 1, 2000))]
    queue.probe_replies = [False, False, True]

    await run_loop_until(worker, lambda: len(queue.called("lease")) >= 2, "the second lease")

    after_give_up = [call for call, _ in queue.calls if call in ("lease", "probe")]
    assert after_give_up[:5] == ["lease", "probe", "probe", "probe", "lease"]
    assert queue.called("probe") == [False, False, True]
    assert status.health(worker._config)["abandoned"] == 1
    assert "Sink reachable again; taking tasks" in caplog.text


async def test_losing_the_lease_does_not_stop_the_worker_taking_tasks(make: Make) -> None:
    worker, queue, status = make(work_ms=250)
    queue.heartbeat_reply = False  # the lease is lost: not the sink's fault
    queue.leases = [(True, TaskLease(1, 5, 1, 300))]

    await run_loop_until(worker, lambda: len(queue.called("lease")) >= 2, "the next lease")

    assert status.health(worker._config)["abandoned"] == 1
    assert queue.called("probe") == []
