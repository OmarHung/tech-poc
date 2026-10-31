"""Dispatched worker: takes one task at a time from the upstream queue, keeps the lease alive with heartbeats while
working, writes the result to the sink, then reports the task done. Writing before reporting makes delivery
at-least-once: a crash in between means another worker does the task again, and the sink sees a second copy."""

from __future__ import annotations

import asyncio
import logging

from shared import tasks
from shared.clock import Clock, PeriodicTimer
from shared.work_contracts import ResultRecord, TaskLease
from worker.client import WorkClient
from worker.config import WorkerConfig
from worker.lease_tracker import LeaseTracker, lease_end
from worker.status import WorkerStatus

logger = logging.getLogger(__name__)


def compute(task_input: int) -> str:
    """The work itself: deterministic, so a task done twice gives the same result twice."""
    return f"{task_input}^2={task_input * task_input}"


class DispatchWorker:
    def __init__(self, client: WorkClient, status: WorkerStatus, config: WorkerConfig, clock: Clock) -> None:
        self._client = client
        self._status = status
        self._config = config
        self._clock = clock
        self._sink_down = False

    async def run(self) -> None:
        config = self._config
        logger.info(
            "Dispatched worker %s starting; lease check %s", config.worker_id, "on" if config.lease_check else "off"
        )
        while True:
            if self._sink_down and not await self._sink_back():
                continue
            sent_at = self._clock.now()
            reached, lease = await self._client.lease_task()
            if lease is not None:
                await self.run_task(lease, LeaseTracker(self._clock, lease_end(sent_at, lease.ttl_ms)))
            elif not reached:
                await asyncio.sleep(config.retry_delay)  # refused or reset: do not spin

    async def run_task(self, lease: TaskLease, tracker: LeaseTracker) -> None:
        status = self._status
        status.task_phase(lease.id, "working")
        heartbeating = tasks.spawn(self._heartbeat(lease, tracker), f"heartbeat-{lease.id}")
        try:
            await asyncio.sleep(self._config.work_time)  # the work itself
            if not self._still_holds(lease, tracker, "before writing the result"):
                return
            status.task_phase(lease.id, "delivering")
            result = ResultRecord(f"task-{lease.id}", self._config.worker_id, lease.attempt, compute(lease.input))
            if not await self._deliver(lease, tracker, result):
                return
            status.task_phase(lease.id, "completing")
            completed = await self._complete(lease, tracker)
            status.task_finished("completed" if completed else "stale")
        finally:
            heartbeating.cancel()
            await asyncio.wait((heartbeating,))
            # A heartbeat loop that crashed (rather than being stopped here) is a bug; let it stop the worker.
            if not heartbeating.cancelled() and (error := heartbeating.exception()) is not None:
                raise error

    def _still_holds(self, lease: TaskLease, tracker: LeaseTracker, when: str) -> bool:
        if not self._config.lease_check or tracker.is_valid:
            return True
        return self._give_up(lease, when)

    def _give_up(self, lease: TaskLease, when: str) -> bool:
        """Records the task as abandoned. Always False, so callers can return it."""
        logger.warning("Gave up task %d attempt %d %s: lease lost", lease.id, lease.attempt, when)
        self._status.task_finished("abandoned")
        return False

    async def _deliver(self, lease: TaskLease, tracker: LeaseTracker, result: ResultRecord) -> bool:
        """Retries until the sink answers. With the lease check on, each try is cut off when the lease runs out, so a
        write cannot start late; only one already on the wire can still land after it. A worker that cannot reach the
        sink for a whole lease gives the task up (heartbeats stop with it), so the other worker can get it."""
        config, clock = self._config, self._clock
        give_up_at = clock.now() + lease.ttl_ms / 1000
        while True:
            timeout = min(config.rpc_timeout, tracker.remaining) if config.lease_check else config.rpc_timeout
            if timeout <= 0:
                # Out of lease and nothing written. Do not look again: a heartbeat landing in between could make the
                # lease valid, and returning True would report the task done without a result.
                return self._give_up(lease, "while writing the result")
            reply = await self._client.deliver(result, timeout)
            if reply is not None:
                self._status.sink_replied(reply)
                return True
            if clock.now() >= give_up_at:
                logger.warning(
                    "Gave up task %d attempt %d: sink unreachable for a whole lease", lease.id, lease.attempt
                )
                self._status.task_finished("abandoned")
                # Take no more work until it is back, or the next task (maybe this one again) stalls too.
                self._sink_down = True
                return False
            await asyncio.sleep(config.retry_delay)
            if not self._still_holds(lease, tracker, "while the sink was unreachable"):
                return False

    async def _sink_back(self) -> bool:
        """Circuit breaker after giving a task up for an unreachable sink: lease nothing until the sink answers again.
        Otherwise this worker keeps taking tasks it cannot finish, and may even get back the one it just let go."""
        if await self._client.sink_reachable():
            logger.info("Sink reachable again; taking tasks")
            self._sink_down = False
            return True
        await asyncio.sleep(self._config.retry_delay)
        return False

    async def _complete(self, lease: TaskLease, tracker: LeaseTracker) -> bool:
        """Keeps reporting until the upstream answers. False means another attempt owns the task (it will run
        again)."""
        while (completed := await self._client.complete(lease)) is None:
            await asyncio.sleep(self._config.retry_delay)
        if not completed:
            logger.warning("Task %d attempt %d was reassigned before it was reported done", lease.id, lease.attempt)
            tracker.revoke()
        return completed

    async def _heartbeat(self, lease: TaskLease, tracker: LeaseTracker) -> None:
        """Heartbeats on a fixed schedule, each bounded by the interval, so one hung call cannot push the next one past
        the lease. The lease is extended from each heartbeat's send time."""
        interval = lease.ttl_ms / 3 / 1000
        timer = PeriodicTimer(interval)
        while True:
            await timer.tick()
            sent_at = self._clock.now()
            extended = await self._client.heartbeat(lease, interval)
            if extended is True:
                tracker.extend_to(lease_end(sent_at, lease.ttl_ms))
            elif extended is False:
                tracker.revoke()
                return
