from __future__ import annotations

import asyncio

import pytest

from helpers import FakeClock
from shared.errors import InvalidInput, NotFound
from shared.work_contracts import TaskAttempt, TaskLease
from upstream.config import UpstreamConfig
from upstream.task_queue import MAX_INPUT, LeaseView, TaskQueue

pytestmark = pytest.mark.anyio

TTL = 2.0


@pytest.fixture
def queue(clock: FakeClock) -> TaskQueue:
    return TaskQueue(UpstreamConfig(task_lease_ttl_ms=int(TTL * 1000)), clock)


async def lease(queue: TaskQueue, worker: str) -> TaskLease | None:
    return await queue.lease(worker, 0)


async def test_lease_hands_out_tasks_in_order(queue: TaskQueue) -> None:
    queue.add(10)
    queue.add(20)

    first = await lease(queue, "a")
    second = await lease(queue, "b")

    assert first == TaskLease(1, 10, 1, 2000)
    assert second == TaskLease(2, 20, 1, 2000)
    assert await lease(queue, "a") is None
    assert queue.stats().leases == (LeaseView(1, "a", 1), LeaseView(2, "b", 1))


async def test_lease_waits_for_a_new_task(queue: TaskQueue) -> None:
    waiting = asyncio.ensure_future(queue.lease("a", 1.0))
    await asyncio.sleep(0)
    assert not waiting.done()

    queue.add(7)

    leased = await asyncio.wait_for(waiting, 0.5)
    assert leased is not None and leased.input == 7


async def test_two_waiting_workers_never_get_the_same_task(queue: TaskQueue) -> None:
    waiting = [asyncio.ensure_future(queue.lease(worker, 1.0)) for worker in ("a", "b")]
    await asyncio.sleep(0)

    queue.add(1)
    queue.add(2)

    leases = await asyncio.wait_for(asyncio.gather(*waiting), 0.5)
    assert sorted(lease.id for lease in leases if lease is not None) == [1, 2]


async def test_a_waiter_woken_for_a_task_someone_else_took_keeps_waiting(queue: TaskQueue) -> None:
    waiting = asyncio.ensure_future(queue.lease("a", 0.2))
    await asyncio.sleep(0)

    queue.add(1)
    taken = await lease(queue, "b")  # runs before the woken waiter gets to look

    assert taken is not None and taken.id == 1
    assert await asyncio.wait_for(waiting, 1.0) is None


async def test_lease_gives_up_after_the_wait(queue: TaskQueue) -> None:
    assert await asyncio.wait_for(queue.lease("a", 0.05), 1.0) is None


async def test_heartbeats_keep_the_lease(queue: TaskQueue, clock: FakeClock) -> None:
    queue.add(1)
    leased = await lease(queue, "a")
    assert leased is not None

    clock.advance(TTL * 0.75)
    assert queue.heartbeat(leased.id, TaskAttempt("a", 1)) is True
    clock.advance(TTL * 0.75)

    assert queue.complete(leased.id, TaskAttempt("a", 1)) is True
    stats = queue.stats()
    assert (stats.done, stats.leased, stats.reassignments) == (1, 0, 0)


async def test_an_expired_lease_goes_back_to_the_front_with_the_next_attempt(
    queue: TaskQueue, clock: FakeClock
) -> None:
    queue.add(1)
    queue.add(2)
    await lease(queue, "a")

    clock.advance(TTL)
    retried = await lease(queue, "b")

    assert retried == TaskLease(1, 1, 2, 2000)
    assert queue.stats().reassignments == 1


async def test_several_expired_leases_are_requeued_oldest_first(queue: TaskQueue, clock: FakeClock) -> None:
    for i in range(3):
        queue.add(i)
    await lease(queue, "a")
    await lease(queue, "b")

    clock.advance(TTL)
    queue.expire_overdue()

    order = [(await lease(queue, "c")) for _ in range(3)]
    assert [(t.id, t.attempt) for t in order if t is not None] == [(1, 2), (2, 2), (3, 1)]


async def test_a_lost_lease_cannot_heartbeat_or_complete(queue: TaskQueue, clock: FakeClock) -> None:
    queue.add(1)
    await lease(queue, "a")
    clock.advance(TTL)
    await lease(queue, "b")

    assert queue.heartbeat(1, TaskAttempt("a", 1)) is False
    assert queue.complete(1, TaskAttempt("a", 1)) is False
    assert queue.complete(1, TaskAttempt("b", 2)) is True
    assert queue.complete(1, TaskAttempt("a", 1)) is False
    assert queue.heartbeat(1, TaskAttempt("b", 2)) is False  # done: nothing left to extend

    stats = queue.stats()
    assert (stats.stale_completions, stats.done) == (2, 1)


async def test_the_same_worker_with_an_old_attempt_does_not_hold_the_task(queue: TaskQueue, clock: FakeClock) -> None:
    queue.add(1)
    await lease(queue, "a")
    clock.advance(TTL)
    await lease(queue, "a")  # attempt 2, same worker

    assert queue.heartbeat(1, TaskAttempt("a", 1)) is False
    assert queue.complete(1, TaskAttempt("a", 2)) is True


async def test_expire_overdue_requeues_without_anyone_asking_and_wakes_a_waiter(
    queue: TaskQueue, clock: FakeClock
) -> None:
    queue.add(1)
    await lease(queue, "a")
    waiting = asyncio.ensure_future(queue.lease("b", 1.0))
    await asyncio.sleep(0)

    clock.advance(TTL)
    stats_before = queue.stats()
    queue.expire_overdue()

    assert stats_before.leases == (LeaseView(1, "a", 1),)  # stats alone do not expire anything
    retried = await asyncio.wait_for(waiting, 0.5)
    assert retried == TaskLease(1, 1, 2, 2000)


async def test_a_repeated_completion_by_the_same_attempt_is_not_stale(queue: TaskQueue) -> None:
    queue.add(1)
    await lease(queue, "a")

    assert queue.complete(1, TaskAttempt("a", 1)) is True
    assert queue.complete(1, TaskAttempt("a", 1)) is True  # a retry whose first reply was lost

    assert queue.stats().stale_completions == 0


def test_add_rejects_inputs_whose_square_would_overflow(queue: TaskQueue) -> None:
    assert queue.add(MAX_INPUT) == 1
    assert queue.add(-MAX_INPUT) == 2
    with pytest.raises(InvalidInput, match="input must be between"):
        queue.add(MAX_INPUT + 1)


async def test_unknown_tasks_and_bad_workers_are_rejected(queue: TaskQueue) -> None:
    with pytest.raises(NotFound, match="task 99 not found"):
        queue.complete(99, TaskAttempt("a", 1))
    with pytest.raises(NotFound):
        queue.heartbeat(99, TaskAttempt("a", 1))
    for worker in ("Bad Worker", None, "x" * 33):
        with pytest.raises(InvalidInput, match="worker must be"):
            queue.heartbeat(1, TaskAttempt(worker, 1))
        with pytest.raises(InvalidInput):
            await queue.lease(worker, 0)


def test_stats_count_everything(queue: TaskQueue) -> None:
    queue.add(1)
    queue.add(2)

    assert queue.stats().to_wire() == {
        "total": 2,
        "queued": 2,
        "leased": 0,
        "done": 0,
        "reassignments": 0,
        "staleCompletions": 0,
        "leases": [],
    }
