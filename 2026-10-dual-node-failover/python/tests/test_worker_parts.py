"""The worker's building blocks: ledger, lease tracker, status and config."""

from __future__ import annotations

import pytest

from helpers import FakeClock
from shared.errors import ConfigError
from shared.wire import WireError
from shared.work_contracts import LoggedEvent
from sink.config import SinkConfig
from upstream.config import UpstreamConfig
from worker.config import StartFrom, WorkerConfig, WorkerMode
from worker.dispatch import compute
from worker.lease_tracker import LeaseTracker, lease_end
from worker.ledger import EMPTY_LEDGER, Ledger
from worker.status import WorkerStatus

# --------------------------------------------------------------------------- ledger


def test_apply_keeps_running_balances_in_order() -> None:
    first, b1 = EMPTY_LEDGER.apply(LoggedEvent(1, "x", 10))
    second, b2 = first.apply(LoggedEvent(2, "y", 3))
    third, b3 = second.apply(LoggedEvent(3, "x", -4))

    assert (b1, b2, b3) == (10, 3, 6)
    assert third.to_file() == {"offset": 3, "balances": {"x": 6, "y": 3}}
    assert EMPTY_LEDGER.to_file() == {"offset": 0, "balances": {}}  # applying never changes the old ledger


def test_apply_refuses_a_gap_or_a_repeat() -> None:
    with pytest.raises(ValueError, match="event 2 does not follow 0"):
        EMPTY_LEDGER.apply(LoggedEvent(2, "x", 1))
    ledger, _ = EMPTY_LEDGER.apply(LoggedEvent(1, "x", 1))
    with pytest.raises(ValueError):
        ledger.apply(LoggedEvent(1, "x", 1))


def test_skip_to_moves_the_position_but_keeps_the_old_balances() -> None:
    ledger, _ = EMPTY_LEDGER.apply(LoggedEvent(1, "x", 10))

    skipped = ledger.skip_to(50)

    assert (skipped.offset, skipped.balances["x"]) == (50, 10)
    assert skipped.apply(LoggedEvent(51, "x", 50))[1] == 60


def test_the_checkpoint_round_trips_and_keeps_account_case() -> None:
    ledger, _ = EMPTY_LEDGER.apply(LoggedEvent(1, "Acct", 10))

    restored = Ledger.from_file(ledger.to_file())

    assert restored == ledger
    assert dict(restored.balances) == {"Acct": 10}


@pytest.mark.parametrize(
    "saved", [{"offset": 1}, {"offset": 1, "balances": []}, {"offset": 1, "balances": {"x": "1"}}, {"offset": "1"}]
)
def test_a_damaged_checkpoint_is_refused(saved: object) -> None:
    with pytest.raises(WireError):
        Ledger.from_file(saved)


# --------------------------------------------------------------------------- lease tracker


def test_the_lease_ends_at_ninety_percent_of_the_ttl_after_the_send() -> None:
    assert lease_end(1.0, 1_000) == pytest.approx(1.9)


def test_the_lease_runs_out_unless_extended(clock: FakeClock) -> None:
    tracker = LeaseTracker(clock, lease_end(clock.now(), 1_000))

    clock.advance(0.8)
    assert tracker.is_valid
    tracker.extend_to(lease_end(clock.now(), 1_000))
    tracker.extend_to(0.0)  # an older renewal never shortens it
    clock.advance(0.8)
    assert tracker.is_valid
    clock.advance(0.1)

    assert not tracker.is_valid


def test_remaining_counts_down_to_the_end(clock: FakeClock) -> None:
    tracker = LeaseTracker(clock, lease_end(clock.now(), 1_000))

    clock.advance(0.6)

    assert tracker.remaining == pytest.approx(0.3)


def test_revoke_ends_the_lease_at_once(clock: FakeClock) -> None:
    tracker = LeaseTracker(clock, lease_end(clock.now(), 1_000))

    tracker.revoke()
    tracker.extend_to(clock.now() + 10)

    assert not tracker.is_valid
    assert tracker.remaining == 0.0


def test_the_task_result_is_deterministic() -> None:
    assert compute(12) == "12^2=144"
    assert compute(-3_000_000_000) == "-3000000000^2=9000000000000000000"


# --------------------------------------------------------------------------- status


def test_status_reports_phases_outcomes_and_sink_replies() -> None:
    status = WorkerStatus()
    config = WorkerConfig(worker_id="b", mode=WorkerMode.DISPATCH)

    status.task_phase(7, "delivering")
    during = status.health(config)
    status.sink_replied("duplicate")
    status.sink_replied("applied")
    status.sink_replied("applied")
    status.task_finished("completed")
    status.task_finished("abandoned")
    status.task_finished("stale")
    after = status.health(config)

    assert (during["currentTask"], during["phase"], status.phase) == (7, "delivering", None)
    assert after == {
        "status": "ok",
        "worker": "b",
        "mode": "dispatch",
        "offset": 0,
        "sinkReplies": {"applied": 2, "duplicate": 1},
        "completed": 1,
        "abandoned": 1,
        "staleCompletions": 1,
        "currentTask": None,
        "phase": None,
    }
    assert list(after["sinkReplies"]) == ["applied", "duplicate"]  # type: ignore[call-overload]


# --------------------------------------------------------------------------- config


def test_worker_defaults_apply_when_nothing_is_set() -> None:
    config = WorkerConfig.from_lookup(lambda _: None)

    assert config == WorkerConfig()
    assert (config.port, config.mode, config.data_dir, config.start_from) == (
        8121,
        WorkerMode.HOTHOT,
        "data/worker-a",
        StartFrom.CHECKPOINT,
    )
    assert (config.deterministic, config.lease_check) == (True, True)
    assert (config.work_time, config.rpc_timeout, config.poll_wait, config.retry_delay) == (0.3, 1.0, 0.5, 0.1)
    assert (config.upstream_url, config.sink_url) == ("http://127.0.0.1:8120", "http://127.0.0.1:8123")


def test_worker_settings_are_read_from_the_environment() -> None:
    env = {
        "PORT": "9122",
        "WORKER_ID": "b",
        "MODE": "dispatch",
        "START_FROM": "latest",
        "DETERMINISTIC": "false",
        "LEASE_CHECK": "off",
        "WORK_MS": "50",
        "RPC_TIMEOUT_MS": "200",
        "UPSTREAM_URL": "http://127.0.0.1:9120",
        "SINK_URL": "http://127.0.0.1:9123",
    }

    config = WorkerConfig.from_lookup(env.get)

    assert config == WorkerConfig(
        port=9122,
        worker_id="b",
        mode=WorkerMode.DISPATCH,
        upstream_url="http://127.0.0.1:9120",
        sink_url="http://127.0.0.1:9123",
        data_dir="data/worker-b",
        start_from=StartFrom.LATEST,
        deterministic=False,
        lease_check=False,
        work_ms=50,
        rpc_timeout_ms=200,
    )


def test_worker_from_env_reads_os_environ(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WORKER_ID", "c")
    monkeypatch.setenv("DATA_DIR", "/tmp/somewhere")

    assert (WorkerConfig.from_env().worker_id, WorkerConfig.from_env().data_dir) == ("c", "/tmp/somewhere")


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("WORKER_ID", "Worker A"),
        ("MODE", "both"),
        ("START_FROM", "beginning"),
        ("DETERMINISTIC", "yes"),
        ("LEASE_CHECK", "yes"),
        ("WORK_MS", "60001"),
        ("RPC_TIMEOUT_MS", "5"),
        ("UPSTREAM_URL", "not a url"),
        ("SINK_URL", "ftp://host"),
        ("PORT", "70000"),
    ],
)
def test_bad_worker_settings_name_the_variable(name: str, value: str) -> None:
    with pytest.raises(ConfigError, match=name):
        WorkerConfig.from_lookup(lambda n: value if n == name else None)


def test_upstream_and_sink_settings() -> None:
    assert UpstreamConfig.from_lookup(lambda _: None) == UpstreamConfig(8120, 2000, 100)
    upstream = UpstreamConfig.from_lookup({"PORT": "9120", "TASK_LEASE_TTL_MS": "400"}.get)
    assert (upstream.port, upstream.task_lease_ttl, upstream.reap_interval) == (9120, 0.4, 0.1)
    assert SinkConfig.from_lookup(lambda _: None).port == 8123
    assert SinkConfig.from_lookup({"PORT": "9123"}.get).port == 9123
    with pytest.raises(ConfigError, match="TASK_LEASE_TTL_MS"):
        UpstreamConfig.from_lookup({"TASK_LEASE_TTL_MS": "60001"}.get)
