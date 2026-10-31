from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from node.cluster.primary_term import PeerState, PrimaryTerm
from node.storage.replicated_log import ReplicatedLog
from shared.clock import MonotonicClock
from shared.errors import Unavailable
from shared.witness_contracts import SyncReply

TIMEOUT = 0.05  # real seconds: these tests wait on asyncio timers

pytestmark = pytest.mark.anyio


class FakeWitness:
    """Behaves like LeaseTable.set_standby_in_sync for the holder in its own epoch."""

    def __init__(self) -> None:
        self.version = 10
        self.standby_in_sync = False
        self.reachable = True
        self.requests: list[tuple[bool, int]] = []
        self.gate: asyncio.Event | None = None  # when set, calls wait for it

    async def set_standby_in_sync(self, epoch: int, in_sync: bool, expected_version: int) -> SyncReply | None:
        self.requests.append((in_sync, expected_version))
        if self.gate is not None:
            await self.gate.wait()
        if not self.reachable or (in_sync and expected_version != self.version):
            return None
        self.version += 1
        self.standby_in_sync = in_sync
        return SyncReply(epoch, in_sync, self.version)


@pytest.fixture
def log(tmp_path: Path):
    opened = ReplicatedLog.open(tmp_path / "log.jsonl")
    yield opened
    opened.close()


@pytest.fixture
def witness() -> FakeWitness:
    return FakeWitness()


@pytest.fixture
def new_term(log: ReplicatedLog, witness: FakeWitness):
    def create(standby_in_sync: bool, catch_up_slack: int = 16) -> PrimaryTerm:
        return PrimaryTerm(1, standby_in_sync, witness.version, log, witness, catch_up_slack)

    return create


async def solo_term(new_term, catch_up_slack: int = 16) -> PrimaryTerm:
    term = new_term(standby_in_sync=False, catch_up_slack=catch_up_slack)
    assert await term.try_go_solo() is True
    return term


def wait(term: PrimaryTerm, index: int, timeout: float = TIMEOUT) -> asyncio.Task[None]:
    return asyncio.ensure_future(term.wait_durable(index, timeout, MonotonicClock()))


async def test_in_sync_write_is_durable_once_the_standby_has_it(new_term, log, witness) -> None:
    term = new_term(standby_in_sync=True)
    entry = log.append(1, "k", "v")

    waiting = wait(term, entry.index, timeout=5)
    await asyncio.sleep(0.01)
    assert not waiting.done()
    term.record_replicated(entry.index)

    await waiting
    assert witness.requests == []


async def test_in_sync_slow_standby_goes_solo_when_the_witness_agrees(new_term, log, witness) -> None:
    term = new_term(standby_in_sync=True)
    entry = log.append(1, "k", "v")

    await wait(term, entry.index)

    assert term.peer_state is PeerState.SOLO
    assert witness.requests == [(False, 10)]


async def test_in_sync_slow_standby_fails_the_write_when_the_witness_refuses(new_term, log, witness) -> None:
    witness.reachable = False
    term = new_term(standby_in_sync=True)
    entry = log.append(1, "k", "v")

    with pytest.raises(Unavailable, match="witness"):
        await wait(term, entry.index)
    assert term.peer_state is PeerState.IN_SYNC


async def test_stale_start_writes_wait_until_the_witness_agrees_to_solo(new_term, log) -> None:
    term = new_term(standby_in_sync=False)
    assert term.peer_state is PeerState.CATCHING_UP
    entry = log.append(1, "k", "v")

    waiting = wait(term, entry.index, timeout=5)
    await asyncio.sleep(0.01)
    assert not waiting.done()
    assert await term.try_go_solo() is True

    await waiting
    assert term.peer_state is PeerState.SOLO


async def test_waiting_writes_share_one_solo_request(new_term, log, witness) -> None:
    witness.gate = asyncio.Event()
    term = new_term(standby_in_sync=True)
    entry = log.append(1, "k", "v")

    waiters = [wait(term, entry.index) for _ in range(5)]
    await asyncio.sleep(TIMEOUT * 2)
    witness.gate.set()
    await asyncio.gather(*waiters)

    assert witness.requests == [(False, 10)]


async def test_solo_write_is_durable_right_away(new_term, log) -> None:
    term = await solo_term(new_term)
    entry = log.append(1, "k", "v")

    await term.wait_durable(entry.index, TIMEOUT, MonotonicClock())


async def test_solo_catches_up_then_reports_in_sync_against_the_latest_version(new_term, log, witness) -> None:
    term = await solo_term(new_term)  # witness version 10 -> 11
    log.append(1, "a", "1")
    log.append(1, "b", "2")

    assert term.record_replicated(2) is True
    assert term.peer_state is PeerState.CATCHING_UP
    await term.try_mark_in_sync()

    assert term.peer_state is PeerState.IN_SYNC
    assert witness.requests[-1] == (True, 11)
    assert witness.standby_in_sync is True


async def test_in_sync_report_is_refused_when_the_witness_changed_in_between(new_term, log, witness) -> None:
    term = await solo_term(new_term)
    log.append(1, "a", "1")
    term.record_replicated(1)
    witness.version += 5  # e.g. a late "not in sync" from an older process got there first

    await term.try_mark_in_sync()

    assert term.peer_state is PeerState.CATCHING_UP
    assert witness.standby_in_sync is False


async def test_catching_up_new_writes_wait_for_the_standby_again(new_term, log) -> None:
    term = await solo_term(new_term)
    log.append(1, "a", "1")
    term.record_replicated(0)  # within the slack: the catch-up target is index 1
    assert term.peer_state is PeerState.CATCHING_UP
    entry = log.append(1, "b", "2")

    waiting = wait(term, entry.index, timeout=5)
    assert term.record_replicated(1) is True
    await term.try_mark_in_sync()
    assert term.peer_state is PeerState.IN_SYNC
    await asyncio.sleep(0.01)
    assert not waiting.done()
    term.record_replicated(2)

    await waiting


async def test_catching_up_does_not_report_before_the_target(new_term, log, witness) -> None:
    term = await solo_term(new_term)
    log.append(1, "a", "1")
    log.append(1, "b", "2")

    assert term.record_replicated(1) is False
    await term.try_mark_in_sync()

    assert term.peer_state is PeerState.CATCHING_UP
    assert not any(in_sync for in_sync, _ in witness.requests)


async def test_solo_stays_solo_while_the_standby_is_far_behind(new_term, log) -> None:
    term = await solo_term(new_term, catch_up_slack=1)
    for key in "abc":
        log.append(1, key, key)

    assert term.record_replicated(1) is False
    assert term.peer_state is PeerState.SOLO


async def test_witness_saying_behind_sends_an_in_sync_standby_through_catching_up(new_term, log) -> None:
    term = new_term(standby_in_sync=True)
    log.append(1, "a", "1")

    term.observe_witness(11, standby_in_sync=False)  # a "go on alone" whose reply was lost

    assert term.peer_state is PeerState.CATCHING_UP
    assert term.record_replicated(1) is True


async def test_witness_replies_older_than_one_already_seen_are_ignored(new_term) -> None:
    term = new_term(standby_in_sync=True)

    term.observe_witness(9, standby_in_sync=False)

    assert term.peer_state is PeerState.IN_SYNC


async def test_ended_term_fails_writes_still_waiting(new_term, log) -> None:
    term = new_term(standby_in_sync=True)
    entry = log.append(1, "k", "v")

    waiting = wait(term, entry.index, timeout=5)
    await asyncio.sleep(0.01)
    term.end()

    with pytest.raises(Unavailable, match="lost the primary role"):
        await waiting
    assert term.is_ended
    assert await term.try_go_solo() is False


async def test_ending_the_term_cancels_a_solo_request_in_flight(new_term, log, witness) -> None:
    witness.gate = asyncio.Event()
    term = new_term(standby_in_sync=True)
    entry = log.append(1, "k", "v")

    waiting = wait(term, entry.index)
    await asyncio.sleep(TIMEOUT * 2)  # now stuck in the witness call
    assert witness.requests == [(False, 10)]
    term.end()

    with pytest.raises(Unavailable, match="witness did not allow"):
        await waiting
    assert term.peer_state is PeerState.IN_SYNC


async def test_term_tasks_are_cancelled_on_end_and_refused_after(new_term) -> None:
    term = new_term(standby_in_sync=True)
    running = term.spawn(asyncio.sleep(10), "sleeper")
    await asyncio.sleep(0)

    term.end()
    late = term.spawn(asyncio.sleep(10), "late")

    await asyncio.gather(running, late, return_exceptions=True)
    assert running.cancelled() and late.cancelled()


async def test_append_signal_fires_on_the_next_append(new_term) -> None:
    term = new_term(standby_in_sync=True)
    signal = term.append_signal

    assert not signal.is_set()
    term.notify_appended()

    assert signal.is_set()
    assert not term.append_signal.is_set()
