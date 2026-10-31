from __future__ import annotations

from pathlib import Path

import pytest

from helpers import FakeClock
from shared.errors import Conflict, InvalidInput
from shared.witness_contracts import SyncReply, SyncRequest
from witness.config import WitnessConfig
from witness.lease_table import LeaseTable, WitnessState

TTL = 3.0


@pytest.fixture
def new_table(tmp_path: Path, clock: FakeClock):
    return lambda: LeaseTable(WitnessConfig(data_dir=str(tmp_path / "witness"), lease_ttl_ms=3000), clock)


def mark_in_sync(table: LeaseTable, node: str, epoch: int) -> None:
    table.set_standby_in_sync(SyncRequest(node, epoch, True, table.state.sync_version))


def test_first_request_gets_epoch_one_and_the_other_node_is_not_yet_eligible(new_table) -> None:
    reply = new_table().acquire("a")

    assert (reply.granted, reply.holder, reply.epoch, reply.ttl_ms, reply.standby_in_sync) == (
        True,
        "a",
        1,
        3000,
        False,
    )


def test_holder_renews_in_the_same_epoch_even_after_expiry(new_table, clock: FakeClock) -> None:
    table = new_table()
    table.acquire("a")

    clock.advance(TTL * 2)
    reply = table.acquire("a")

    assert (reply.granted, reply.epoch) == (True, 1)


def test_other_node_is_refused_while_the_lease_is_valid(new_table, clock: FakeClock) -> None:
    table = new_table()
    table.acquire("a")
    mark_in_sync(table, "a", 1)

    clock.advance(TTL - 0.001)
    reply = table.acquire("b")

    assert (reply.granted, reply.holder) == (False, "a")
    assert "held by a" in (reply.reason or "")


def test_in_sync_standby_takes_over_after_expiry_with_a_new_epoch(new_table, clock: FakeClock) -> None:
    table = new_table()
    table.acquire("a")
    mark_in_sync(table, "a", 1)

    clock.advance(TTL)
    reply = table.acquire("b")

    assert (reply.granted, reply.holder, reply.epoch) == (True, "b", 2)
    assert reply.standby_in_sync is False  # a is behind until b says otherwise


def test_stale_standby_is_never_promoted(new_table, clock: FakeClock) -> None:
    table = new_table()
    table.acquire("a")  # the standby starts out not in sync

    clock.advance(TTL * 10)
    reply = table.acquire("b")

    assert (reply.granted, reply.holder) == (False, "a")
    assert "missing acknowledged writes" in (reply.reason or "")


def test_deposed_holder_is_refused_and_cannot_change_the_sync_flag(new_table, clock: FakeClock) -> None:
    table = new_table()
    table.acquire("a")
    mark_in_sync(table, "a", 1)
    clock.advance(TTL)
    table.acquire("b")

    assert table.acquire("a").granted is False
    with pytest.raises(Conflict, match="not the lease holder"):
        table.set_standby_in_sync(SyncRequest("a", 1, False))
    with pytest.raises(Conflict):
        table.set_standby_in_sync(SyncRequest("b", 1, True))


def test_state_survives_a_restart_and_the_holder_keeps_the_lease_for_one_more_ttl(new_table, clock: FakeClock) -> None:
    before = new_table()
    before.acquire("a")
    mark_in_sync(before, "a", 1)
    clock.advance(TTL * 5)

    after = new_table()

    assert after.state == WitnessState(1, "a", True, 2)
    assert after.acquire("b").granted is False
    clock.advance(TTL)
    assert after.acquire("b").granted is True


def test_in_sync_report_is_refused_if_the_state_changed_since_the_holder_looked(new_table) -> None:
    table = new_table()
    seen = table.acquire("a").sync_version
    table.set_standby_in_sync(SyncRequest("a", 1, False))  # e.g. sent after the report below, arrived first

    with pytest.raises(Conflict, match="changed since version"):
        table.set_standby_in_sync(SyncRequest("a", 1, True, seen))
    assert table.state.standby_in_sync is False


def test_not_in_sync_reports_are_always_accepted_and_every_report_bumps_the_version(new_table) -> None:
    table = new_table()
    granted = table.acquire("a").sync_version

    first = table.set_standby_in_sync(SyncRequest("a", 1, False, expected_version=999))
    second = table.set_standby_in_sync(SyncRequest("a", 1, True, first.sync_version))

    assert first.sync_version == granted + 1
    assert second == SyncReply(1, True, granted + 2)


def test_handover_bumps_the_version_so_old_reports_cannot_apply(new_table, clock: FakeClock) -> None:
    table = new_table()
    table.acquire("a")
    mark_in_sync(table, "a", 1)
    before = table.state.sync_version
    clock.advance(TTL)

    assert table.acquire("b").sync_version == before + 1


@pytest.mark.parametrize("node", ["c", "", None])
def test_unknown_nodes_are_rejected(new_table, node: str | None) -> None:
    table = new_table()

    with pytest.raises(InvalidInput, match="node must be one of a, b"):
        table.acquire(node)
    with pytest.raises(InvalidInput):
        table.set_standby_in_sync(SyncRequest(node, 0, True))


def test_health_reports_the_holder_and_remaining_lease(new_table, clock: FakeClock) -> None:
    table = new_table()
    assert table.health()["leaseRemainingMs"] == 0
    table.acquire("a")
    clock.advance(1.0)

    assert table.health() == {
        "status": "ok",
        "service": "witness",
        "holder": "a",
        "epoch": 1,
        "standbyInSync": False,
        "syncVersion": 1,
        "leaseRemainingMs": 2000,
    }
