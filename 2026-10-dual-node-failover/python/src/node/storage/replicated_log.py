from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from node.storage.log_entry import LogEntry, LogSlice
from node.storage.log_file import LogFile
from shared.errors import InvalidInput


class AppendStatus(Enum):
    OK = "ok"
    MISMATCH = "mismatch"


@dataclass(frozen=True, slots=True)
class AppendOutcome:
    """match_index: on OK, the last index of the batch, now known to match the leader.
    On MISMATCH, where the leader should continue from."""

    status: AppendStatus
    match_index: int


class ReplicatedLog:
    """The node's data: an ordered log of writes plus the key-value view built from it.

    Replication uses Raft's log-matching rule: an entry is identified by (index, epoch), a standby only accepts a
    batch that continues an entry it already has, and drops whatever it has past that point that disagrees.

    No locks: every method is synchronous and the node runs on one event loop, so each call is atomic.
    """

    def __init__(self, file: LogFile, entries: list[LogEntry]) -> None:
        self._file = file
        self._entries = entries
        self._latest: dict[str, LogEntry] = {}
        self._truncated = 0
        self._rebuild_latest()

    @classmethod
    def open(cls, path: Path) -> ReplicatedLog:
        file, entries = LogFile.open(path)
        return cls(file, entries)

    @property
    def last_index(self) -> int:
        return len(self._entries)

    @property
    def last_epoch(self) -> int:
        return self._entries[-1].epoch if self._entries else 0

    @property
    def truncated_entries(self) -> int:
        """Entries this node dropped because a newer primary never had them (writes nobody was told succeeded)."""
        return self._truncated

    def append(self, epoch: int, key: str, value: str) -> LogEntry:
        entry = LogEntry(len(self._entries) + 1, epoch, key, value)
        self._file.append((entry,))
        self._add(entry)
        return entry

    def append_from(
        self,
        leader_epoch: int,
        prev_index: int,
        prev_epoch: int,
        entries: Sequence[LogEntry | None],
        leader_last_index: int,
    ) -> AppendOutcome:
        """Applies a batch from the primary (the standby side of replication).
        leader_last_index is the primary's last index when it built the batch."""
        batch = _validate_batch(leader_epoch, prev_index, entries, leader_last_index)
        if self._epoch_at(prev_index) != prev_epoch:
            return AppendOutcome(AppendStatus.MISMATCH, self._mismatch_hint(prev_index))
        fresh: list[LogEntry] = []
        for entry in batch:
            if not fresh and entry.index <= len(self._entries):
                if self._entries[entry.index - 1].epoch == entry.epoch:
                    continue  # already have it, e.g. a resend after a timeout
                self._truncate(entry.index)
            fresh.append(entry)
        if fresh:
            self._file.append(fresh)
            for entry in fresh:
                self._add(entry)
        matched = prev_index + len(batch)
        # The batch reached the primary's last entry when it was sent, so entries past it from an older epoch came from
        # an older primary. Entries from this epoch stay: a request that arrived late must not undo a newer one.
        if (
            matched == leader_last_index
            and len(self._entries) > matched
            and self._entries[matched].epoch < leader_epoch
        ):
            self._truncate(matched + 1)
        return AppendOutcome(AppendStatus.OK, matched)

    def read(self, from_index: int, max_count: int) -> LogSlice | None:
        """Up to max_count entries starting at from_index; None if that is past the end."""
        prev_epoch = self._epoch_at(from_index - 1)
        if prev_epoch is None:
            return None
        start = from_index - 1
        return LogSlice(prev_epoch, tuple(self._entries[start : start + max_count]), len(self._entries))

    def get(self, key: str) -> LogEntry | None:
        return self._latest.get(key)

    def snapshot(self) -> dict[str, str]:
        return {key: entry.value for key, entry in self._latest.items()}

    def close(self) -> None:
        self._file.close()

    def _epoch_at(self, index: int) -> int | None:
        """Epoch at index; 0 for index 0 (the empty prefix), None past either end."""
        if index == 0:
            return 0
        if index < 0 or index > len(self._entries):
            return None
        return self._entries[index - 1].epoch

    def _mismatch_hint(self, prev_index: int) -> int:
        """Where the leader should retry from: the end of this log if it is too short, otherwise just before the
        first entry of the disagreeing epoch, so a whole divergent stretch is skipped in one round trip (Raft's
        fast backup)."""
        if prev_index > len(self._entries):
            return len(self._entries)
        if prev_index < 1:
            return 0  # prevIndex 0 with a non-zero prevEpoch: nothing to walk back to
        conflict_epoch = self._entries[prev_index - 1].epoch
        index = prev_index
        while index > 1 and self._entries[index - 2].epoch == conflict_epoch:
            index -= 1
        return index - 1

    def _truncate(self, from_index: int) -> None:
        kept = self._entries[: from_index - 1]
        self._file.rewrite(kept)  # disk first: if that fails, memory still matches the file
        self._truncated += len(self._entries) - len(kept)
        self._entries = kept
        self._rebuild_latest()

    def _add(self, entry: LogEntry) -> None:
        self._entries.append(entry)
        self._latest[entry.key] = entry

    def _rebuild_latest(self) -> None:
        self._latest = {}
        for entry in self._entries:
            self._latest[entry.key] = entry


def _validate_batch(
    leader_epoch: int, prev_index: int, entries: Sequence[LogEntry | None], leader_last_index: int
) -> list[LogEntry]:
    if prev_index < 0 or leader_last_index < prev_index + len(entries):
        raise InvalidInput("prevIndex and leaderLastIndex do not fit the batch")
    batch: list[LogEntry] = []
    for offset, entry in enumerate(entries):
        expected = prev_index + 1 + offset
        if entry is None or entry.index != expected or entry.epoch < 1 or entry.epoch > leader_epoch:
            raise InvalidInput(f"entry {offset} does not continue the batch at index {expected}")
        batch.append(entry)
    return batch
