"""Both architectures end to end in one process: upstream, sink and two workers on ASGI transports, with the real
loops on short timings. verify/independent.py covers the same ground with real processes."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Callable
from contextlib import AsyncExitStack
from pathlib import Path

import httpx
import pytest

from helpers import FakeNetwork
from shared.work_contracts import NewEvent
from sink.config import SinkConfig
from sink.main import create_app as create_sink
from sink.result_store import ResultStore
from upstream.config import UpstreamConfig
from upstream.event_log import EventLog
from upstream.main import create_app as create_upstream
from upstream.task_queue import TaskQueue
from worker.config import WorkerConfig, WorkerMode
from worker.main import WorkerRuntime, build_app
from worker.main import create_app as create_worker

pytestmark = pytest.mark.anyio

TASK_LEASE_TTL_MS = 400
WORKERS = ("a", "b")


class InProcessWork:
    def __init__(self, root: Path, mode: WorkerMode, work_ms: int) -> None:
        self.network = FakeNetwork()
        self.fatal: list[BaseException] = []
        self.upstream = create_upstream(
            UpstreamConfig(task_lease_ttl_ms=TASK_LEASE_TTL_MS, reap_interval_ms=20), on_fatal=self.fatal.append
        )
        self.network.apps["upstream"] = self.upstream
        self.network.apps["sink"] = create_sink(SinkConfig())
        self.runtimes: dict[str, WorkerRuntime] = {}
        for worker in WORKERS:
            config = WorkerConfig(
                worker_id=worker,
                mode=mode,
                upstream_url="http://upstream/",
                sink_url="http://sink/",
                data_dir=str(root / worker),
                work_ms=work_ms,
                poll_wait_ms=100,
                rpc_timeout_ms=500,
            )
            runtime = WorkerRuntime(config, self.network.transport_from(worker), on_fatal=self.fatal.append)
            self.runtimes[worker] = runtime
            self.network.apps[worker] = build_app(runtime, run_loop=False)
        self._stack = AsyncExitStack()

    @property
    def events(self) -> EventLog:
        return self.upstream.state.events

    @property
    def tasks(self) -> TaskQueue:
        return self.upstream.state.tasks

    @property
    def results(self) -> ResultStore:
        return self.network.apps["sink"].state.results  # type: ignore[attr-defined]

    async def start(self) -> None:
        await self._stack.enter_async_context(self.upstream.router.lifespan_context(self.upstream))
        for runtime in self.runtimes.values():
            await runtime.start()

    async def stop(self) -> None:
        for worker, runtime in self.runtimes.items():
            if worker not in self.network.down:
                await runtime.stop()
        await self._stack.aclose()

    async def kill(self, worker: str) -> None:
        """Stops the worker and makes it unreachable, like kill -9."""
        self.network.down.add(worker)
        await self.runtimes[worker].stop()

    def holder(self) -> str:
        return self.tasks.stats().leases[0].worker


async def wait_until(condition: Callable[[], bool], what: str) -> None:
    deadline = time.monotonic() + 10
    while not condition():
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        await asyncio.sleep(0.02)


@pytest.fixture
async def start(tmp_path: Path) -> AsyncIterator[Callable[..., object]]:
    started: list[InProcessWork] = []

    async def create(mode: WorkerMode, work_ms: int = 150) -> InProcessWork:
        work = InProcessWork(tmp_path, mode, work_ms)
        started.append(work)
        await work.start()
        return work

    yield create
    for work in started:
        await work.stop()
        assert work.fatal == []


def append(work: InProcessWork, expected: dict[str, int], first: int, last: int) -> None:
    for i in range(first, last + 1):
        account = f"acct-{i % 3}"
        work.events.append(NewEvent(account, i))
        expected[account] = expected.get(account, 0) + i


async def test_hothot_both_workers_process_every_event_and_losing_one_loses_nothing(start) -> None:
    work: InProcessWork = await start(WorkerMode.HOTHOT)
    expected: dict[str, int] = {}
    append(work, expected, 1, 30)
    await wait_until(lambda: work.results.summary.received == 60, "both workers to deliver")

    await work.kill("a")
    append(work, expected, 31, 60)
    await wait_until(lambda: work.results.summary.applied == 60, "b to deliver the rest")
    await wait_until(lambda: work.runtimes["b"].status.ledger.offset == 60, "b to checkpoint")

    assert work.results.summary.conflicts == 0
    assert dict(work.runtimes["b"].status.ledger.balances) == expected
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=work.network.apps["b"])) as client:
        state = await client.get("http://b/state")
    assert state.json() == {"offset": 60, "balances": expected}


async def test_dispatch_a_task_held_by_a_dead_worker_is_done_by_the_other(start) -> None:
    work: InProcessWork = await start(WorkerMode.DISPATCH)
    work.tasks.add(7)
    await wait_until(lambda: work.tasks.stats().leased == 1, "a worker to take the task")
    holder = work.holder()

    await work.kill(holder)
    await wait_until(lambda: work.tasks.stats().done == 1, "the task to be redone")

    result = work.results.result("task-1")
    assert work.tasks.stats().reassignments == 1
    assert result is not None and result["node"] != holder
    assert (result["value"], result["attempt"]) == ("7^2=49", 2)


async def test_dispatch_a_task_longer_than_the_lease_is_kept_alive_by_heartbeats(start) -> None:
    work: InProcessWork = await start(WorkerMode.DISPATCH, work_ms=1_000)  # the lease is 400 ms
    work.tasks.add(3)

    await wait_until(lambda: work.tasks.stats().done == 1, "the long task")

    assert work.tasks.stats().reassignments == 0
    assert work.results.summary.received == 1


async def test_dispatch_a_worker_that_cannot_reach_the_sink_lets_the_task_go(start) -> None:
    work: InProcessWork = await start(WorkerMode.DISPATCH)
    work.tasks.add(5)
    await wait_until(lambda: work.tasks.stats().leased == 1, "a worker to take the task")
    holder = work.holder()

    work.network.cut.add((holder, "sink"))
    await wait_until(lambda: work.tasks.stats().done == 1, "the other worker to finish it")

    # A single task and no backlog: without the circuit breaker the cut-off worker, waiting longest, could get the
    # same task straight back and stall it again.
    result = work.results.result("task-1")
    assert result is not None and result["node"] != holder
    assert work.tasks.stats().reassignments == 1
    assert work.runtimes[holder].status.health(work.runtimes[holder].config)["abandoned"] == 1


async def test_dispatch_spreads_tasks_over_both_workers(start) -> None:
    work: InProcessWork = await start(WorkerMode.DISPATCH)
    for i in range(1, 7):
        work.tasks.add(i)

    await wait_until(lambda: work.tasks.stats().done == 6, "all tasks")

    snapshot = work.results.snapshot()
    assert sorted({result["node"] for result in snapshot["results"].values()}) == ["a", "b"]  # type: ignore[attr-defined]
    assert work.results.summary.received == 6


async def test_the_worker_lifespan_runs_the_loop_and_serves_health(tmp_path: Path) -> None:
    config = WorkerConfig(worker_id="c", data_dir=str(tmp_path), upstream_url="http://nowhere/")
    app = create_worker(config, transport=httpx.MockTransport(lambda r: httpx.Response(503)), on_fatal=pytest.fail)
    runtime: WorkerRuntime = app.state.runtime

    async with app.router.lifespan_context(app):
        await asyncio.sleep(0.05)
        running = runtime.running
        assert running is not None and not running.done()
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c") as client:
            health = await client.get("/health")

    assert running.cancelled()
    assert health.headers["x-served-by"] == "worker-c"
    assert (health.json()["worker"], health.json()["mode"], health.json()["offset"]) == ("c", "hothot", 0)


def test_worker_create_app_reads_the_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("WORKER_ID", "b")
    monkeypatch.setenv("MODE", "dispatch")
    monkeypatch.setenv("DATA_DIR", str(tmp_path))

    assert create_worker().state.runtime.config.mode is WorkerMode.DISPATCH
    monkeypatch.setenv("MODE", "both")
    with pytest.raises(SystemExit, match="worker: MODE"):
        create_worker()
