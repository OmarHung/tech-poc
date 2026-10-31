from __future__ import annotations

from dataclasses import dataclass

from shared.wire import JsonObject


@dataclass(frozen=True, slots=True)
class LogEntry:
    """One write. index orders writes; epoch says which primary term created it."""

    index: int
    epoch: int
    key: str
    value: str

    def to_wire(self) -> dict[str, object]:
        return {"index": self.index, "epoch": self.epoch, "key": self.key, "value": self.value}

    @staticmethod
    def from_wire(value: object, path: str = "$") -> LogEntry | None:
        """None for a JSON null or an entry without key or value; a wrong type raises WireError."""
        if value is None:
            return None
        fields = JsonObject(value, path)
        index, epoch = fields.integer("index"), fields.integer("epoch")
        key, text = fields.string("key"), fields.string("value")
        if key is None or text is None:
            return None
        return LogEntry(index, epoch, key, text)


@dataclass(frozen=True, slots=True)
class LogSlice:
    """A consistent read of the log: the entries asked for, plus what surrounds them at that moment.
    prev_epoch is the epoch of the entry just before them (0 when they start at index 1)."""

    prev_epoch: int
    entries: tuple[LogEntry, ...]
    last_index: int
