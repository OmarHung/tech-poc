"""The downstream end. Applying a result is the side effect (send the mail, book the payment), so it must happen once
per key no matter how many copies arrive: the first copy is applied, later ones are only counted."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from enum import StrEnum

from shared.clock import unix_ms
from shared.errors import InvalidInput
from shared.wire import is_node_id, utf16_length
from shared.work_contracts import ResultRecord, is_work_id

MAX_VALUE_LENGTH = 1024


class ResultStatus(StrEnum):
    APPLIED = "applied"
    """First result for this key: the side effect happens now."""
    DUPLICATE = "duplicate"
    """The same value again (the other hot-hot worker, or a retried task). Ignored."""
    CONFLICT = "conflict"
    """A different value for a key that already has one: the two copies disagree. The first one stays."""
    STALE = "stale"
    """From an older attempt than one already seen: a worker that lost its task lease and kept going."""


@dataclass(frozen=True)
class _Entry:
    value: str
    node: str
    attempt: int
    first_at_ms: int
    """Wall-clock time (Unix ms) the first copy arrived."""
    max_attempt: int
    copies: int = 1
    conflicts: int = 0
    stale: int = 0

    def to_wire(self) -> dict[str, object]:
        return {
            "value": self.value,
            "node": self.node,
            "attempt": self.attempt,
            "firstAtMs": self.first_at_ms,
            "copies": self.copies,
            "conflicts": self.conflicts,
            "stale": self.stale,
        }


@dataclass(frozen=True)
class SinkSummary:
    """received: every result that arrived, i.e. the side effects a sink without deduplication would cause.
    applied: distinct keys, i.e. the side effects that actually happened."""

    received: int = 0
    applied: int = 0
    duplicates: int = 0
    conflicts: int = 0
    stale: int = 0

    def to_wire(self) -> dict[str, object]:
        return {
            "received": self.received,
            "applied": self.applied,
            "duplicates": self.duplicates,
            "conflicts": self.conflicts,
            "stale": self.stale,
        }


class ResultStore:
    """record is synchronous, so each result is judged against everything that arrived before it."""

    def __init__(self, wall_clock_ms: Callable[[], int] = unix_ms) -> None:
        self._wall_clock_ms = wall_clock_ms
        self._entries: dict[str, _Entry] = {}
        self._summary = SinkSummary()

    def record(self, record: ResultRecord) -> ResultStatus:
        key, node, value = record.key, record.node, record.value
        if (
            not is_work_id(key)
            or node is None
            or not is_node_id(node)
            or record.attempt < 0
            or value is None
            or utf16_length(value) > MAX_VALUE_LENGTH
        ):
            raise InvalidInput(
                "key must be a valid id, node a node id, attempt >= 0 and value a string of at most "
                f"{MAX_VALUE_LENGTH} characters"
            )
        summary = replace(self._summary, received=self._summary.received + 1)
        entry = self._entries.get(key)
        if entry is None:
            self._entries[key] = _Entry(value, node, record.attempt, self._wall_clock_ms(), record.attempt)
            self._summary = replace(summary, applied=summary.applied + 1)
            return ResultStatus.APPLIED
        copied = replace(entry, copies=entry.copies + 1)
        if record.attempt < entry.max_attempt:
            self._entries[key] = replace(copied, stale=copied.stale + 1)
            self._summary = replace(summary, stale=summary.stale + 1)
            return ResultStatus.STALE
        newest = replace(copied, max_attempt=record.attempt)
        if value == entry.value:
            self._entries[key] = newest
            self._summary = replace(summary, duplicates=summary.duplicates + 1)
            return ResultStatus.DUPLICATE
        self._entries[key] = replace(newest, conflicts=newest.conflicts + 1)
        self._summary = replace(summary, conflicts=summary.conflicts + 1)
        return ResultStatus.CONFLICT

    @property
    def summary(self) -> SinkSummary:
        return self._summary

    def snapshot(self) -> dict[str, object]:
        return {
            "summary": self._summary.to_wire(),
            "results": {key: entry.to_wire() for key, entry in self._entries.items()},
        }

    def result(self, key: str) -> dict[str, object] | None:
        entry = self._entries.get(key)
        return None if entry is None else entry.to_wire()
