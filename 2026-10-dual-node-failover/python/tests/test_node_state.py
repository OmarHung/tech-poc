from __future__ import annotations

import asyncio
import math
from pathlib import Path

import pytest

from helpers import FakeClock
from node.cluster.contracts import ReplicateReply, ReplicateRequest, ReplicateStatus
from node.cluster.node_state import NodeHealth, NodeState
from node.cluster.witness_client import NoArbiter
from node.config import NodeConfig
from node.storage.log_entry import LogEntry
from node.storage.replicated_log import ReplicatedLog
from shared.errors import Unavailable

TTL = 3.0


@pytest.fixture
def new_state(tmp_path: Path, clock: FakeClock):
    logs: list[ReplicatedLog] = []

    def create(**overrides: object) -> NodeState:
        config = NodeConfig(data_dir=str(tmp_path), lease_ttl_ms=3000, **overrides)  # type: ignore[arg-type]
        log = ReplicatedLog.open(tmp_path / f"log-{len(logs)}.jsonl")
        logs.append(log)
        return NodeState(config, log, NoArbiter(), clock)

    yield create
    for log in logs:
        log.close()


def request(epoch: int, prev_index: int, entries: list[LogEntry]) -> ReplicateRequest:
    prev_epoch = 0 if prev_index == 0 else epoch
    return ReplicateRequest("b", epoch, prev_index, prev_epoch, tuple(entries), prev_index + len(entries))


def test_standby_refuses_writes(new_state) -> None:
    state = new_state()

    assert state.serving_term() is None
    with pytest.raises(Unavailable, match="not primary"):
        state.append_as_primary("k", "v")
    assert state.health().role == "standby"


def test_primary_serves_until_its_lease_runs_out(new_state, clock: FakeClock) -> None:
    state = new_state()
    state.try_promote(1, clock.now() + TTL, standby_in_sync=False)

    entry, term = state.append_as_primary("k", "v")
    assert entry == LogEntry(1, 1, "k", "v")
    assert state.health().role == "primary"

    clock.advance(TTL)
    assert state.serving_term() is None
    with pytest.raises(Unavailable):
        state.append_as_primary("k", "v2")
    state.step_down_if_lease_expired()
    assert state.current_term is None
    assert term.is_ended


def test_peer_lease_keeps_the_primary_serving_without_the_witness(new_state, clock: FakeClock) -> None:
    state = new_state()
    state.try_promote(1, clock.now() + TTL, standby_in_sync=True)
    term = state.current_term

    clock.advance(TTL - 1)
    state.extend_peer_lease(term, clock.now() + TTL)
    clock.advance(2)

    assert state.serving_term() is term


def test_lease_extensions_for_an_old_term_are_ignored(new_state, clock: FakeClock) -> None:
    state = new_state()
    state.try_promote(1, clock.now() + TTL, standby_in_sync=False)
    old = state.current_term
    state.try_promote(2, clock.now() + TTL, standby_in_sync=False)

    state.extend_witness_lease(old, clock.now() + TTL * 10)
    state.extend_peer_lease(old, clock.now() + TTL * 10)
    state.step_down(old, "ignored")
    clock.advance(TTL)

    assert old.is_ended
    assert state.serving_term() is None
    assert state.current_term is not None


def test_try_promote_refuses_an_older_epoch(new_state, clock: FakeClock) -> None:
    state = new_state()
    state.observe_epoch(5, "test")

    assert state.try_promote(4, clock.now() + TTL, standby_in_sync=False) is False
    assert state.try_promote(5, clock.now() + TTL, standby_in_sync=False) is True


@pytest.mark.anyio
async def test_wait_for_term_completes_on_promotion(new_state, clock: FakeClock) -> None:
    state = new_state()
    waiting = asyncio.ensure_future(state.wait_for_term())
    await asyncio.sleep(0)
    assert not waiting.done()

    state.try_promote(1, clock.now() + TTL, standby_in_sync=False)

    assert await waiting is state.current_term
    assert await state.wait_for_term() is state.current_term


def test_replicate_applies_entries_from_the_current_primary(new_state) -> None:
    state = new_state()

    reply = state.handle_replicate(request(1, 0, [LogEntry(1, 1, "k", "v")]))

    assert reply == ReplicateReply(ReplicateStatus.OK, 1, 1, 3000, None)
    assert state.log.get("k") == LogEntry(1, 1, "k", "v")


def test_replicate_reports_a_mismatch(new_state) -> None:
    reply = new_state().handle_replicate(request(1, 3, []))

    assert (reply.status, reply.match_index, reply.promise_ms) == (ReplicateStatus.MISMATCH, 0, 3000)


def test_replicate_from_a_deposed_primary_is_rejected_with_the_newer_epoch(new_state) -> None:
    state = new_state()
    state.observe_epoch(3, "test")

    reply = state.handle_replicate(request(2, 0, [LogEntry(1, 2, "k", "v")]))

    assert (reply.status, reply.epoch, reply.promise_ms) == (ReplicateStatus.REJECTED, 3, 0)
    assert state.log.get("k") is None


def test_replicate_with_a_newer_epoch_deposes_this_primary(new_state, clock: FakeClock) -> None:
    state = new_state()
    state.try_promote(1, clock.now() + TTL, standby_in_sync=False)
    term = state.current_term

    reply = state.handle_replicate(request(2, 0, []))

    assert reply.status is ReplicateStatus.OK
    assert state.current_term is None
    assert term.is_ended
    assert state.epoch == 2


def test_replicate_in_the_same_epoch_while_primary_is_rejected(new_state) -> None:
    state = new_state()
    state.try_promote(1, math.inf, standby_in_sync=False)

    reply = state.handle_replicate(request(1, 0, []))

    assert reply.status is ReplicateStatus.REJECTED
    assert state.serving_term() is not None


def test_candidacy_starts_only_after_a_full_lease_of_silence(new_state, clock: FakeClock) -> None:
    state = new_state()

    assert state.try_begin_candidacy() is False  # just started: the old primary may still count on us
    clock.advance(TTL)
    state.handle_replicate(request(1, 0, []))
    clock.advance(TTL - 0.001)
    assert state.try_begin_candidacy() is False
    clock.advance(0.001)

    assert state.try_begin_candidacy() is True
    assert state.try_begin_candidacy() is False


def test_candidate_refuses_replication_so_no_primary_can_count_on_it(new_state, clock: FakeClock) -> None:
    state = new_state()
    clock.advance(TTL)
    assert state.try_begin_candidacy() is True

    reply = state.handle_replicate(request(1, 0, [LogEntry(1, 1, "k", "v")]))

    assert reply.status is ReplicateStatus.REJECTED
    assert state.epoch == 1  # still adopts the newer epoch
    assert state.log.get("k") is None
    state.end_candidacy()
    assert state.handle_replicate(request(1, 0, [])).status is ReplicateStatus.OK


def test_election_delay_postpones_candidacy(new_state, clock: FakeClock) -> None:
    state = new_state(election_delay_ms=1000)

    clock.advance(TTL)
    assert state.try_begin_candidacy() is False
    clock.advance(1)

    assert state.try_begin_candidacy() is True


def test_epoch_survives_a_restart(new_state) -> None:
    new_state().observe_epoch(7, "test")

    assert new_state().epoch == 7


def test_health_describes_a_primary(new_state, clock: FakeClock) -> None:
    state = new_state()
    state.try_promote(4, clock.now() + TTL, standby_in_sync=False)
    state.append_as_primary("k", "v")

    health = state.health()

    assert health == NodeHealth("ok", "a", "primary", 4, 1, 0, "catching-up", 3000, 0, "witness", "sync")
    assert health.to_wire()["leaseRemainingMs"] == 3000


def test_shut_down_ends_the_term(new_state, clock: FakeClock) -> None:
    state = new_state()
    state.try_promote(1, clock.now() + TTL, standby_in_sync=False)
    term = state.current_term

    state.shut_down()

    assert term.is_ended
    assert state.current_term is None
