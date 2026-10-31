"""The dispatcher for worker-style redundancy. Every task is leased to one worker at a time; a worker that stops
heartbeating loses the lease and the task goes back to the front of the queue with a higher attempt number. Workers
never talk to each other: the queue is the only thing that knows who does what.

Delivery is at-least-once: a worker that did the work but died before reporting it, or that was frozen past its lease,
means the task runs twice."""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass, replace

from shared.clock import Clock
from shared.errors import InvalidInput, NotFound
from shared.wire import is_node_id
from shared.work_contracts import TaskAttempt, TaskLease
from upstream.config import UpstreamConfig

MAX_INPUT = 3_000_000_000
"""Inputs are squared by the workers; this keeps the square inside a 64-bit integer, as in the .NET version."""

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _Holder:
    worker: str
    until: float
    """When the lease runs out, on the queue's monotonic clock."""


@dataclass(frozen=True)
class _Task:
    id: int
    input: int
    attempt: int = 0
    holder: _Holder | None = None
    """Who holds the lease; None while the task waits in the queue and once it is done."""
    completed_by: str | None = None

    @property
    def done(self) -> bool:
        return self.completed_by is not None


@dataclass(frozen=True)
class _HeldTask:
    task: _Task
    holder: _Holder


@dataclass(frozen=True)
class LeaseView:
    id: int
    worker: str
    attempt: int

    def to_wire(self) -> dict[str, object]:
        return {"id": self.id, "worker": self.worker, "attempt": self.attempt}


@dataclass(frozen=True)
class TaskStats:
    total: int
    queued: int
    leased: int
    done: int
    reassignments: int
    stale_completions: int
    leases: tuple[LeaseView, ...]

    def to_wire(self) -> dict[str, object]:
        return {
            "total": self.total,
            "queued": self.queued,
            "leased": self.leased,
            "done": self.done,
            "reassignments": self.reassignments,
            "staleCompletions": self.stale_completions,
            "leases": [lease.to_wire() for lease in self.leases],
        }


class TaskQueue:
    """Every method but lease is synchronous, and lease never awaits between checking the queue and taking a task (or
    picking up the signal it waits on), so two workers can never get the same task and no wake-up is missed."""

    def __init__(self, config: UpstreamConfig, clock: Clock) -> None:
        self._clock = clock
        self._ttl = config.task_lease_ttl
        self._ttl_ms = config.task_lease_ttl_ms
        self._tasks: dict[int, _Task] = {}
        self._pending: deque[int] = deque()
        self._available = asyncio.Event()
        self._reassignments = 0
        self._stale_completions = 0

    def add(self, task_input: int) -> int:
        if not -MAX_INPUT <= task_input <= MAX_INPUT:
            raise InvalidInput(f"input must be between -{MAX_INPUT} and {MAX_INPUT}")
        task_id = len(self._tasks) + 1
        self._tasks[task_id] = _Task(task_id, task_input)
        self._pending.append(task_id)
        self._pulse()
        return task_id

    async def lease(self, worker: str | None, wait: float) -> TaskLease | None:
        """Hands the oldest waiting task to `worker`, waiting up to `wait` seconds for one."""
        worker = _require_worker(worker)
        deadline = self._clock.now() + wait
        while True:
            self._expire()
            if self._pending:
                task = self._tasks[self._pending.popleft()]
                leased = replace(task, attempt=task.attempt + 1, holder=self._new_holder(worker))
                self._tasks[task.id] = leased
                return TaskLease(leased.id, leased.input, leased.attempt, self._ttl_ms)
            available = self._available
            remaining = deadline - self._clock.now()
            if remaining <= 0:
                return None
            try:
                async with asyncio.timeout(remaining):
                    await available.wait()
            except TimeoutError:
                return None

    def heartbeat(self, task_id: int, request: TaskAttempt) -> bool:
        """Extends the lease if `request` still holds it."""
        _require_worker(request.worker)
        self._expire()
        held = self._holding(task_id, request)
        if held is None:
            return False
        self._tasks[task_id] = replace(held.task, holder=self._new_holder(held.holder.worker))
        return True

    def complete(self, task_id: int, request: TaskAttempt) -> bool:
        """Marks the task done if `request` still holds it; anything else is a stale completion."""
        _require_worker(request.worker)
        self._expire()
        done = self._tasks.get(task_id)
        if done is not None and done.completed_by == request.worker and done.attempt == request.attempt:
            return True  # the same report again, e.g. a retry after its reply was lost
        held = self._holding(task_id, request)
        if held is None:
            self._stale_completions += 1
            logger.warning("Stale completion of task %d attempt %d by %s", task_id, request.attempt, request.worker)
            return False
        self._tasks[task_id] = replace(held.task, holder=None, completed_by=held.holder.worker)
        return True

    def expire_overdue(self) -> None:
        """Puts every task whose lease ran out back at the front of the queue. Called on every access and by a timer."""
        self._expire()

    def stats(self) -> TaskStats:
        leases = tuple(
            LeaseView(task.id, task.holder.worker, task.attempt)
            for task in self._tasks.values()
            if task.holder is not None
        )
        return TaskStats(
            total=len(self._tasks),
            queued=len(self._pending),
            leased=len(leases),
            done=sum(1 for task in self._tasks.values() if task.done),
            reassignments=self._reassignments,
            stale_completions=self._stale_completions,
            leases=leases,
        )

    def _new_holder(self, worker: str) -> _Holder:
        return _Holder(worker, self._clock.now() + self._ttl)

    def _holding(self, task_id: int, request: TaskAttempt) -> _HeldTask | None:
        """The task if `request` holds its lease right now (expired ones were requeued just before); None if not; 404
        if there is no such task."""
        task = self._tasks.get(task_id)
        if task is None:
            raise NotFound(f"task {task_id} not found")
        holder = task.holder
        if holder is None or holder.worker != request.worker or task.attempt != request.attempt:
            return None
        return _HeldTask(task, holder)

    def _expire(self) -> None:
        now = self._clock.now()
        expired = sorted(
            (task for task in self._tasks.values() if task.holder is not None and task.holder.until <= now),
            key=lambda task: task.id,
            reverse=True,
        )
        for task in expired:
            worker = None if task.holder is None else task.holder.worker
            logger.warning("Lease on task %d attempt %d by %s expired; requeued", task.id, task.attempt, worker)
            self._tasks[task.id] = replace(task, holder=None)
            self._pending.appendleft(task.id)  # retried work goes first: it has waited longest
            self._reassignments += 1
        if expired:
            self._pulse()

    def _pulse(self) -> None:
        """Wakes everyone waiting for a task; the next wait picks up a fresh signal."""
        fired, self._available = self._available, asyncio.Event()
        fired.set()


def _require_worker(worker: str | None) -> str:
    if worker is None or not is_node_id(worker):
        raise InvalidInput("worker must be 1-32 lowercase letters, digits or '-'")
    return worker
