"""The election loop's decisions, the RPC and witness clients' failure handling, and the task supervisor."""

from __future__ import annotations

import asyncio
import json
import signal
from pathlib import Path

import httpx
import pytest

from helpers import FakeClock
from node.cluster.election import ElectionWorker
from node.cluster.node_state import NodeState
from node.cluster.rpc import RpcClient
from node.cluster.witness_client import NoArbiter, WitnessClient
from node.config import ArbiterMode, NodeConfig
from node.main import create_app
from node.storage.replicated_log import ReplicatedLog
from shared import tasks
from shared.witness_contracts import LeaseReply

TTL = 3.0

pytestmark = pytest.mark.anyio


class ScriptedWitness:
    """Answers acquire() from a list of replies (None = unreachable)."""

    def __init__(self, *replies: LeaseReply | None) -> None:
        self.replies = list(replies)
        self.calls = 0

    async def acquire(self) -> LeaseReply | None:
        self.calls += 1
        return self.replies.pop(0)


def lease(granted: bool, epoch: int, in_sync: bool = False, version: int = 1, reason: str | None = None) -> LeaseReply:
    return LeaseReply(granted, "a" if granted else "b", epoch, 3000, in_sync, version, reason)


@pytest.fixture
def make(tmp_path: Path, clock: FakeClock):
    logs: list[ReplicatedLog] = []

    def create(witness: object, **overrides: object) -> tuple[ElectionWorker, NodeState]:
        config = NodeConfig(data_dir=str(tmp_path), **overrides)  # type: ignore[arg-type]
        log = ReplicatedLog.open(tmp_path / "log.jsonl")
        logs.append(log)
        state = NodeState(config, log, NoArbiter(), clock)
        return ElectionWorker(state, witness, config), state  # type: ignore[arg-type]

    yield create
    for log in logs:
        log.close()


async def test_a_standby_asks_only_after_a_full_lease_of_silence(make, clock: FakeClock) -> None:
    witness = ScriptedWitness(lease(True, 1, in_sync=True, version=4))
    worker, state = make(witness)

    await worker.tick_with_witness()
    assert witness.calls == 0
    clock.advance(TTL)
    await worker.tick_with_witness()

    term = state.current_term
    assert term is not None and term.epoch == 1
    assert term.peer_state.value == "in-sync"
    # The lease is counted from when the request was sent and ends a tenth early.
    assert state.health().lease_remaining_ms == 2700


async def test_a_refusal_adopts_the_witness_epoch_and_ends_candidacy(make, clock: FakeClock) -> None:
    worker, state = make(ScriptedWitness(lease(False, 7, reason="lease held by b"), None))
    clock.advance(TTL)

    await worker.tick_with_witness()
    await worker.tick_with_witness()  # unreachable witness: nothing happens

    assert (state.epoch, state.current_term) == (7, None)
    assert state.try_begin_candidacy() is True  # the failed attempts did not leave it a candidate


async def test_renewal_extends_the_lease_or_steps_down_when_refused(make, clock: FakeClock) -> None:
    witness = ScriptedWitness(
        lease(True, 1),
        lease(True, 1, in_sync=False, version=9),
        None,
        lease(False, 2, reason="lease held by b"),
    )
    worker, state = make(witness)
    clock.advance(TTL)
    await worker.tick_with_witness()
    term = state.current_term

    clock.advance(1)
    await worker.tick_with_witness()  # renewed: 2.7 s from now
    clock.advance(1)
    await worker.tick_with_witness()  # witness unreachable: the lease simply runs on
    assert state.serving_term() is term
    await worker.tick_with_witness()

    assert state.current_term is None and term.is_ended
    assert state.epoch == 2


async def test_a_renewal_granted_in_a_newer_epoch_starts_a_new_term(make, clock: FakeClock) -> None:
    worker, state = make(ScriptedWitness(lease(True, 1), lease(True, 3)))
    clock.advance(TTL)
    await worker.tick_with_witness()
    first = state.current_term

    await worker.tick_with_witness()

    assert first is not None and first.is_ended
    assert state.current_term is not None and state.current_term.epoch == 3


async def test_an_expired_lease_steps_the_primary_down_before_renewing(make, clock: FakeClock) -> None:
    worker, state = make(ScriptedWitness(lease(True, 1), None))
    clock.advance(TTL)
    await worker.tick_with_witness()

    clock.advance(TTL)
    await worker.tick_with_witness()

    assert state.current_term is None


async def test_without_a_witness_the_standby_promotes_itself_after_silence(make, clock: FakeClock) -> None:
    worker, state = make(ScriptedWitness(), arbiter=ArbiterMode.NONE, election_delay_ms=1500)

    clock.advance(TTL)
    worker.tick_without_witness()
    assert state.current_term is None
    clock.advance(1.5)
    worker.tick_without_witness()
    worker.tick_without_witness()

    term = state.current_term
    assert term is not None and term.epoch == 1
    assert state.serving_term() is term
    assert state.health().lease_remaining_ms is None


async def test_the_loop_ticks_on_a_fixed_grid(tmp_path: Path) -> None:
    config = NodeConfig(data_dir=str(tmp_path), lease_ttl_ms=300, renew_interval_ms=20, replication_timeout_ms=100)
    log = ReplicatedLog.open(tmp_path / "log.jsonl")
    worker = ElectionWorker(NodeState(config, log, NoArbiter(), FakeClock()), ScriptedWitness(), config)  # type: ignore[arg-type]
    ticks = 0

    async def tick() -> None:
        nonlocal ticks
        ticks += 1

    worker.tick_with_witness = tick  # type: ignore[method-assign]
    running = asyncio.ensure_future(worker.run())
    await asyncio.sleep(0.1)
    running.cancel()
    await asyncio.gather(running, return_exceptions=True)
    log.close()

    assert ticks >= 2


async def test_rpc_returns_none_when_unreachable_or_too_slow() -> None:
    async def slow(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(1)
        return httpx.Response(200)

    def refused(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    too_slow = RpcClient(0.02, httpx.MockTransport(slow))
    unreachable = RpcClient(0.02, httpx.MockTransport(refused))

    assert await too_slow.post("http://peer/x", {}) is None
    assert await unreachable.post("http://peer/x", {}) is None
    await too_slow.aclose()
    await unreachable.aclose()


@pytest.mark.parametrize(
    ("status", "body", "expected_sync"),
    [
        (200, {"epoch": 1, "standbyInSync": True, "syncVersion": 5}, True),
        (409, {"error": "standby state changed"}, None),
        (500, {"error": "boom"}, None),
        (200, {"epoch": "1"}, None),
    ],
)
async def test_witness_client_turns_every_failure_into_none(status: int, body: object, expected_sync) -> None:
    seen: list[dict[str, object]] = []

    def answer(request: httpx.Request) -> httpx.Response:
        seen.append(json.loads(request.content))
        if request.url.path == "/lease":
            return httpx.Response(status, content=b"not json")
        return httpx.Response(status, json=body)

    rpc = RpcClient(1.0, httpx.MockTransport(answer))
    client = WitnessClient(rpc, NodeConfig(witness_url="http://witness/"))

    reply = await client.set_standby_in_sync(1, True, 4)
    assert await client.acquire() is None

    assert (None if reply is None else reply.standby_in_sync) == expected_sync
    assert seen[0] == {"node": "a", "epoch": 1, "inSync": True, "expectedVersion": 4}
    await rpc.aclose()


async def test_supervise_reports_a_crashed_or_finished_loop() -> None:
    failures: list[BaseException] = []

    async def crash() -> None:
        raise ValueError("bug")

    async def finish() -> None:
        return None

    await tasks.supervise("crashing loop", crash, failures.append)
    await tasks.supervise("finishing loop", finish, failures.append)

    assert [type(error) for error in failures] == [ValueError, RuntimeError]


async def test_supervise_treats_a_stray_cancellation_as_fatal_but_lets_its_own_through() -> None:
    failures: list[BaseException] = []

    async def stray() -> None:
        raise asyncio.CancelledError  # e.g. from awaiting a task someone else cancelled

    await tasks.supervise("stray loop", stray, failures.append)
    own = asyncio.ensure_future(tasks.supervise("cancelled loop", lambda: asyncio.sleep(10), failures.append))
    await asyncio.sleep(0)
    own.cancel()
    await asyncio.gather(own, return_exceptions=True)

    assert [type(error) for error in failures] == [asyncio.CancelledError]
    assert own.cancelled()


async def test_a_failed_background_task_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    async def fail() -> None:
        raise ValueError("unseen")

    task = tasks.spawn(fail(), "doomed")
    await asyncio.gather(task, return_exceptions=True)
    await asyncio.sleep(0)

    assert "Task doomed failed" in caplog.text


def test_stop_process_sends_sigterm_to_itself(monkeypatch: pytest.MonkeyPatch) -> None:
    sent: list[tuple[int, int]] = []
    monkeypatch.setattr("os.kill", lambda pid, sig: sent.append((pid, sig)))

    tasks.stop_process(RuntimeError("x"))

    assert sent and sent[0][1] == signal.SIGTERM


def test_node_create_app_reads_the_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("NODE_ID", "b")

    assert create_app().state.runtime.config.node_id == "b"
    monkeypatch.setenv("LEASE_TTL_MS", "100")
    with pytest.raises(SystemExit, match="node: LEASE_TTL_MS"):
        create_app()


async def test_lifespan_starts_and_stops_the_loops(tmp_path: Path) -> None:
    config = NodeConfig(data_dir=str(tmp_path), lease_ttl_ms=300, renew_interval_ms=50, replication_timeout_ms=100)
    app = create_app(config, transport=httpx.MockTransport(lambda r: httpx.Response(503)), on_fatal=pytest.fail)
    runtime = app.state.runtime

    async with app.router.lifespan_context(app):
        await asyncio.sleep(0.05)
        workers = runtime.workers
        assert len(workers) == 2
        assert not any(worker.done() for worker in workers)

    assert all(worker.cancelled() for worker in workers)
