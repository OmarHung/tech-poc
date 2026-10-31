"""Wire format of the two architectures where the workers never talk to each other: an upstream that feeds them (an
event log for hot-hot redundancy, a task queue for dispatched workers) and a sink that receives their results.
Field names on the wire are camelCase."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TypeGuard

from shared.wire import JsonObject, WireError

WORK_ID_MAX_LENGTH = 64
_WORK_ID = re.compile(rf"[A-Za-z0-9._-]{{1,{WORK_ID_MAX_LENGTH}}}")


def is_work_id(value: object) -> TypeGuard[str]:
    """Accounts and result keys: 1-64 ASCII letters, digits, '-', '_' or '.'."""
    return isinstance(value, str) and _WORK_ID.fullmatch(value) is not None


def _required_string(fields: JsonObject, name: str) -> str:
    value = fields.string(name)
    if value is None:
        raise WireError(fields.path(name))
    return value


@dataclass(frozen=True)
class NewEvent:
    """An event appended to the upstream log. Both hot-hot workers read every event."""

    account: str | None
    amount: int

    @staticmethod
    def from_wire(value: object) -> NewEvent:
        fields = JsonObject(value)
        return NewEvent(fields.string("account"), fields.integer("amount"))


@dataclass(frozen=True)
class LoggedEvent:
    seq: int
    account: str
    amount: int

    def to_wire(self) -> dict[str, object]:
        return {"seq": self.seq, "account": self.account, "amount": self.amount}

    @staticmethod
    def from_wire(value: object, path: str = "$") -> LoggedEvent:
        fields = JsonObject(value, path)
        return LoggedEvent(fields.integer("seq"), _required_string(fields, "account"), fields.integer("amount"))


@dataclass(frozen=True)
class EventPage:
    """last: the newest sequence number in the log, so a reader knows how far behind it is."""

    events: tuple[LoggedEvent, ...]
    last: int

    def to_wire(self) -> dict[str, object]:
        return {"events": [event.to_wire() for event in self.events], "last": self.last}

    @staticmethod
    def from_wire(value: object) -> EventPage:
        fields = JsonObject(value)
        events = fields.array("events") or []
        return EventPage(
            tuple(LoggedEvent.from_wire(event, f"$.events[{i}]") for i, event in enumerate(events)),
            fields.integer("last"),
        )


@dataclass(frozen=True)
class NewTask:
    """A task for the upstream queue. Exactly one worker holds it at a time."""

    input: int

    @staticmethod
    def from_wire(value: object) -> NewTask:
        return NewTask(JsonObject(value).integer("input"))


@dataclass(frozen=True)
class TaskLeaseRequest:
    worker: str | None
    wait_ms: int = 0

    def to_wire(self) -> dict[str, object]:
        return {"worker": self.worker, "waitMs": self.wait_ms}

    @staticmethod
    def from_wire(value: object) -> TaskLeaseRequest:
        fields = JsonObject(value)
        return TaskLeaseRequest(fields.string("worker"), fields.int32("waitMs"))


@dataclass(frozen=True)
class TaskLease:
    """attempt: how many times this task has been handed out, this time included. Doubles as a fencing token.
    ttl_ms: the lease runs out this long after the request was sent, unless a heartbeat extends it."""

    id: int
    input: int
    attempt: int
    ttl_ms: int

    def to_wire(self) -> dict[str, object]:
        return {"id": self.id, "input": self.input, "attempt": self.attempt, "ttlMs": self.ttl_ms}

    @staticmethod
    def from_wire(value: object) -> TaskLease:
        fields = JsonObject(value)
        return TaskLease(fields.integer("id"), fields.integer("input"), fields.int32("attempt"), fields.int32("ttlMs"))


@dataclass(frozen=True)
class TaskAttempt:
    """Who reports on a task (heartbeat or completion), and for which attempt."""

    worker: str | None
    attempt: int

    def to_wire(self) -> dict[str, object]:
        return {"worker": self.worker, "attempt": self.attempt}

    @staticmethod
    def from_wire(value: object) -> TaskAttempt:
        fields = JsonObject(value)
        return TaskAttempt(fields.string("worker"), fields.int32("attempt"))


@dataclass(frozen=True)
class ResultRecord:
    """One result delivered to the sink. key identifies the event or task it is for."""

    key: str | None
    node: str | None
    attempt: int
    value: str | None

    def to_wire(self) -> dict[str, object]:
        return {"key": self.key, "node": self.node, "attempt": self.attempt, "value": self.value}

    @staticmethod
    def from_wire(value: object) -> ResultRecord:
        fields = JsonObject(value)
        return ResultRecord(
            fields.string("key"), fields.string("node"), fields.int32("attempt"), fields.string("value")
        )


@dataclass(frozen=True)
class ResultReply:
    """status: "applied" (first copy), "duplicate" (same value again), "conflict" (a different value), "stale" (an
    older attempt than one already seen)."""

    status: str | None

    def to_wire(self) -> dict[str, object]:
        return {"status": self.status}

    @staticmethod
    def from_wire(value: object) -> ResultReply:
        return ResultReply(JsonObject(value).string("status"))
