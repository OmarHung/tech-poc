from __future__ import annotations

import errno
import os
from pathlib import Path

import pytest

from node.storage.log_entry import LogEntry
from node.storage.log_file import CorruptLogError
from node.storage.replicated_log import AppendOutcome, AppendStatus, ReplicatedLog
from shared.errors import InvalidInput

LEADER_EPOCH = 5


def entry(index: int, epoch: int, key: str) -> LogEntry:
    return LogEntry(index, epoch, key, key)


@pytest.fixture
def path(tmp_path: Path) -> Path:
    return tmp_path / "log.jsonl"


@pytest.fixture
def log(path: Path):
    opened = ReplicatedLog.open(path)
    yield opened
    opened.close()


def ok(match_index: int) -> AppendOutcome:
    return AppendOutcome(AppendStatus.OK, match_index)


def mismatch(hint: int) -> AppendOutcome:
    return AppendOutcome(AppendStatus.MISMATCH, hint)


def test_append_numbers_entries_and_they_survive_a_reopen(path: Path) -> None:
    log = ReplicatedLog.open(path)
    assert log.append(1, "k1", "v1") == LogEntry(1, 1, "k1", "v1")
    assert log.append(2, "k1", "v2") == LogEntry(2, 2, "k1", "v2")
    log.close()

    reopened = ReplicatedLog.open(path)

    assert (reopened.last_index, reopened.last_epoch) == (2, 2)
    assert reopened.get("k1") == LogEntry(2, 2, "k1", "v2")
    assert path.read_text().splitlines()[0] == '{"index":1,"epoch":1,"key":"k1","value":"v1"}'
    reopened.close()


def test_read_returns_the_batch_with_its_surroundings(log: ReplicatedLog) -> None:
    log.append(1, "a", "1")
    log.append(2, "b", "2")
    log.append(2, "c", "3")

    batch = log.read(2, 1)

    assert batch is not None
    assert (batch.prev_epoch, batch.entries, batch.last_index) == (1, (LogEntry(2, 2, "b", "2"),), 3)
    assert log.read(4, 10).entries == ()  # type: ignore[union-attr]
    assert log.read(5, 10) is None
    assert log.read(0, 10) is None


def test_append_from_adds_entries_that_continue_the_log(log: ReplicatedLog) -> None:
    assert log.append_from(LEADER_EPOCH, 0, 0, [entry(1, 1, "a"), entry(2, 1, "b")], 2) == ok(2)
    assert log.last_index == 2


def test_append_from_reports_a_mismatch_when_the_previous_entry_is_missing_or_different(log: ReplicatedLog) -> None:
    log.append(1, "a", "1")
    log.append(1, "b", "2")

    assert log.append_from(LEADER_EPOCH, 5, 3, [], 5) == mismatch(2)
    assert log.append_from(LEADER_EPOCH, 2, 2, [], 2) == mismatch(0)
    assert log.append_from(LEADER_EPOCH, 0, 4, [], 0) == mismatch(0)
    assert log.last_index == 2


def test_mismatch_skips_the_whole_disagreeing_epoch(log: ReplicatedLog) -> None:
    for epoch, key in [(1, "a"), (1, "b"), (2, "x"), (2, "y"), (2, "z")]:  # epoch 2 written alone by a deposed primary
        log.append(epoch, key, key)

    assert log.append_from(LEADER_EPOCH, 5, 3, [], 6) == mismatch(2)


def test_append_from_ignores_entries_it_already_has(log: ReplicatedLog) -> None:
    log.append_from(LEADER_EPOCH, 0, 0, [entry(1, 1, "a"), entry(2, 1, "b")], 2)

    outcome = log.append_from(LEADER_EPOCH, 0, 0, [entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 1, "c")], 3)

    assert outcome == ok(3)
    assert (log.last_index, log.truncated_entries) == (3, 0)


def test_append_from_replaces_entries_a_newer_primary_never_had(path: Path) -> None:
    log = ReplicatedLog.open(path)
    log.append(1, "k", "old")
    log.append(1, "k", "unacknowledged")  # the old primary wrote this, then crashed

    assert log.append_from(LEADER_EPOCH, 1, 1, [entry(2, 2, "other")], 2) == ok(2)

    assert log.truncated_entries == 1
    assert log.get("k") == LogEntry(1, 1, "k", "old")
    assert log.snapshot() == {"k": "old", "other": "other"}
    log.close()
    reopened = ReplicatedLog.open(path)
    assert reopened.snapshot() == {"k": "old", "other": "other"}
    reopened.close()


def test_append_from_drops_older_epochs_past_the_leaders_last_entry(log: ReplicatedLog) -> None:
    log.append(1, "a", "1")
    log.append(1, "stale", "2")
    log.append(1, "stale2", "3")

    assert log.append_from(LEADER_EPOCH, 1, 1, [], 1) == ok(1)

    assert (log.last_index, log.truncated_entries) == (1, 2)
    assert log.get("stale") is None


def test_late_heartbeat_from_the_same_epoch_drops_nothing(log: ReplicatedLog) -> None:
    log.append_from(3, 0, 0, [entry(1, 3, "a"), entry(2, 3, "b"), entry(3, 3, "c")], 3)

    # A heartbeat the primary sent when its log ended at 1, arriving after the batch above.
    assert log.append_from(3, 1, 3, [], 1) == ok(1)

    assert (log.last_index, log.truncated_entries) == (3, 0)


def test_append_from_keeps_entries_past_a_partial_batch(log: ReplicatedLog) -> None:
    log.append_from(LEADER_EPOCH, 0, 0, [entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 1, "c")], 3)

    assert log.append_from(LEADER_EPOCH, 0, 0, [entry(1, 1, "a")], 3) == ok(1)
    assert log.last_index == 3


@pytest.mark.parametrize(
    ("leader_epoch", "prev_index", "entries", "leader_last_index"),
    [
        (LEADER_EPOCH, 0, [entry(2, 1, "a")], 2),
        (LEADER_EPOCH, 0, [entry(1, 0, "a")], 1),
        (LEADER_EPOCH, 0, [entry(1, 1, "a")], 0),
        (LEADER_EPOCH, -1, [], 0),
        (1, 0, [entry(1, 2, "a")], 1),
        (LEADER_EPOCH, 0, [None], 1),
    ],
)
def test_append_from_rejects_batches_that_do_not_fit_together(
    log: ReplicatedLog, leader_epoch: int, prev_index: int, entries: list[LogEntry | None], leader_last_index: int
) -> None:
    with pytest.raises(InvalidInput):
        log.append_from(leader_epoch, prev_index, 0, entries, leader_last_index)


def test_open_drops_a_partial_last_line(path: Path) -> None:
    path.write_text('{"index":1,"epoch":1,"key":"a","value":"1"}\n{"index":2,"ep')

    log = ReplicatedLog.open(path)
    assert log.last_index == 1
    log.append(1, "b", "2")
    log.close()

    reopened = ReplicatedLog.open(path)
    assert reopened.last_index == 2
    reopened.close()


@pytest.mark.parametrize(
    "content",
    [
        'garbage\n{"index":1,"epoch":1,"key":"a","value":"1"}\n',
        '{"index":2,"epoch":1,"key":"a","value":"1"}\n{"index":3,"epoch":1,"key":"a","value":"1"}\n',
        '{"index":1,"epoch":1,"key":null,"value":"1"}\n{"index":2,"epoch":1,"key":"a","value":"1"}\n',
    ],
)
def test_open_refuses_corruption_before_the_end(path: Path, content: str) -> None:
    path.write_text(content)

    with pytest.raises(CorruptLogError, match="line 1 is corrupt"):
        ReplicatedLog.open(path)


def test_a_failed_append_leaves_no_partial_line_behind(path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    log = ReplicatedLog.open(path)
    log.append(1, "a", "1")
    real_write = os.write

    def disk_full(fd: int, data: bytes) -> int:
        real_write(fd, bytes(data[:5]))  # the OS took a few bytes, then gave up
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr("node.storage.log_file.os.write", disk_full)
    with pytest.raises(OSError):
        log.append(1, "b", "2")
    monkeypatch.undo()
    log.append(1, "c", "3")
    log.close()

    reopened = ReplicatedLog.open(path)
    assert reopened.snapshot() == {"a": "1", "c": "3"}
    assert [line[-1] for line in path.read_text().splitlines()] == ["}", "}"]
    reopened.close()


def test_a_last_line_that_lost_only_its_newline_is_kept_and_terminated(path: Path) -> None:
    path.write_text('{"index":1,"epoch":1,"key":"a","value":"1"}')

    log = ReplicatedLog.open(path)
    log.append(1, "b", "2")
    log.close()

    reopened = ReplicatedLog.open(path)
    assert reopened.snapshot() == {"a": "1", "b": "2"}
    reopened.close()
