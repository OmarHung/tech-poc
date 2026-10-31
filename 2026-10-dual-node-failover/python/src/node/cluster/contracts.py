"""Primary to standby: "append these after (prevIndex, prevEpoch)". An empty batch is the heartbeat."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from node.storage.log_entry import LogEntry
from shared.wire import JsonObject, WireError

MAX_BATCH = 256
"""Entries per replication request, and how close the standby must be before writes wait for it again."""


class ReplicateStatus(StrEnum):
    OK = "ok"
    """Applied. The standby now matches the primary up to matchIndex."""
    MISMATCH = "mismatch"
    """The standby lacks or disagrees with the entry at prevIndex; retry from matchIndex + 1."""
    REJECTED = "rejected"
    """Not accepted from this sender at all (older epoch, or the standby is asking the witness for the lease)."""


@dataclass(frozen=True)
class ReplicateRequest:
    leader: str | None
    epoch: int
    prev_index: int
    prev_epoch: int
    entries: tuple[LogEntry | None, ...]
    """None stands for a null or incomplete entry on the wire; the log refuses the batch with a 400."""
    leader_last_index: int

    def to_wire(self) -> dict[str, object]:
        return {
            "leader": self.leader,
            "epoch": self.epoch,
            "prevIndex": self.prev_index,
            "prevEpoch": self.prev_epoch,
            "entries": [None if entry is None else entry.to_wire() for entry in self.entries],
            "leaderLastIndex": self.leader_last_index,
        }

    @staticmethod
    def from_wire(value: object) -> ReplicateRequest:
        fields = JsonObject(value)
        items = fields.array("entries") or []
        return ReplicateRequest(
            leader=fields.string("leader"),
            epoch=fields.integer("epoch"),
            prev_index=fields.integer("prevIndex"),
            prev_epoch=fields.integer("prevEpoch"),
            entries=tuple(LogEntry.from_wire(item, f"{fields.path('entries')}[{i}]") for i, item in enumerate(items)),
            leader_last_index=fields.integer("leaderLastIndex"),
        )


@dataclass(frozen=True)
class ReplicateReply:
    """epoch: the standby's epoch after handling the request. Higher than the sender's means the sender was deposed.
    promise_ms: how long from receiving the request the standby will not seek the lease (0 when rejected)."""

    status: ReplicateStatus
    epoch: int
    match_index: int
    promise_ms: int
    reason: str | None = None

    def to_wire(self) -> dict[str, object]:
        return {
            "status": self.status.value,
            "epoch": self.epoch,
            "matchIndex": self.match_index,
            "promiseMs": self.promise_ms,
            "reason": self.reason,
        }

    @staticmethod
    def from_wire(value: object) -> ReplicateReply:
        fields = JsonObject(value)
        raw_status = fields.string("status") if fields.has("status") else ReplicateStatus.OK.value
        try:
            status = ReplicateStatus((raw_status or "").lower())
        except ValueError as ex:
            raise WireError(fields.path("status")) from ex
        return ReplicateReply(
            status=status,
            epoch=fields.integer("epoch"),
            match_index=fields.integer("matchIndex"),
            promise_ms=fields.integer("promiseMs"),
            reason=fields.string("reason"),
        )
