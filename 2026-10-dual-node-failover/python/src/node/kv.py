"""The client-facing store. Only the primary serves, and in sync mode only what is safe to acknowledge."""

from __future__ import annotations

import re
from dataclasses import dataclass

from node.cluster.node_state import NodeState, not_primary
from node.cluster.primary_term import PrimaryTerm
from node.config import NodeConfig, ReplicationMode
from node.storage.log_entry import LogEntry
from shared.errors import InvalidInput, NotFound
from shared.wire import JsonObject, utf16_length

MAX_KEY_LENGTH = 128
MAX_VALUE_LENGTH = 4096
_KEY = re.compile(rf"[A-Za-z0-9._:-]{{1,{MAX_KEY_LENGTH}}}")


@dataclass(frozen=True)
class KvItem:
    key: str
    value: str
    index: int
    epoch: int

    @staticmethod
    def from_entry(entry: LogEntry) -> KvItem:
        return KvItem(entry.key, entry.value, entry.index, entry.epoch)

    def to_wire(self) -> dict[str, object]:
        return {"key": self.key, "value": self.value, "index": self.index, "epoch": self.epoch}


def validate_key(key: str) -> None:
    if _KEY.fullmatch(key) is None:
        raise InvalidInput(f"key must be 1-{MAX_KEY_LENGTH} ASCII letters, digits, '-', '_', '.' or ':'")


def parse_value(body: object) -> str:
    """The value of a PUT body. Counted in UTF-16 code units like .NET's string.Length, so both versions draw the
    line at the same place."""
    value = JsonObject(body).string("value")
    if value is None or utf16_length(value) > MAX_VALUE_LENGTH:
        raise InvalidInput(f"value must be a string of at most {MAX_VALUE_LENGTH} characters")
    return value


class KvService:
    def __init__(self, state: NodeState, config: NodeConfig) -> None:
        self._state = state
        self._config = config

    async def put(self, key: str, value: str) -> KvItem:
        entry, term = self._state.append_as_primary(key, value)
        await self._wait_durable(term, entry)
        return KvItem.from_entry(entry)

    async def get(self, key: str) -> KvItem:
        term = self._state.serving_term()
        if term is None:
            raise not_primary()
        entry = self._state.log.get(key)
        if entry is None:
            raise NotFound(f"key '{key}' not found")
        # Never show a value that could still vanish in a failover.
        await self._wait_durable(term, entry)
        return KvItem.from_entry(entry)

    async def _wait_durable(self, term: PrimaryTerm, entry: LogEntry) -> None:
        if self._config.replication is ReplicationMode.SYNC:
            await term.wait_durable(entry.index, self._config.replication_timeout, self._state.clock)
