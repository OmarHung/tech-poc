"""The witness API. Nodes call it; nothing else should. Field names on the wire are camelCase."""

from __future__ import annotations

from dataclasses import dataclass

from shared.wire import JsonObject


@dataclass(frozen=True)
class LeaseRequest:
    node: str | None

    def to_wire(self) -> dict[str, object]:
        return {"node": self.node}

    @staticmethod
    def from_wire(value: object) -> LeaseRequest:
        return LeaseRequest(JsonObject(value).string("node"))


@dataclass(frozen=True)
class LeaseReply:
    """granted: whether the caller now holds the lease for ttl_ms from when it sent the request.
    epoch: goes up by one on every handover and doubles as the fencing token.
    standby_in_sync: whether the node that does not hold the lease may take over. Reset to false on every handover.
    sync_version: goes up on every change of standby_in_sync and every handover."""

    granted: bool
    holder: str | None
    epoch: int
    ttl_ms: int
    standby_in_sync: bool
    sync_version: int
    reason: str | None

    def to_wire(self) -> dict[str, object]:
        return {
            "granted": self.granted,
            "holder": self.holder,
            "epoch": self.epoch,
            "ttlMs": self.ttl_ms,
            "standbyInSync": self.standby_in_sync,
            "syncVersion": self.sync_version,
            "reason": self.reason,
        }

    @staticmethod
    def from_wire(value: object) -> LeaseReply:
        fields = JsonObject(value)
        return LeaseReply(
            granted=fields.boolean("granted"),
            holder=fields.string("holder"),
            epoch=fields.integer("epoch"),
            ttl_ms=fields.integer("ttlMs"),
            standby_in_sync=fields.boolean("standbyInSync"),
            sync_version=fields.integer("syncVersion"),
            reason=fields.string("reason"),
        )


@dataclass(frozen=True)
class SyncRequest:
    """Sent by the lease holder to say whether the other node has every write it acknowledged.

    expected_version matters for in_sync = true only: the latest sync version the holder has seen. If anything
    changed since, the claim may be out of date (for example a request that arrived late) and is refused."""

    node: str | None
    epoch: int
    in_sync: bool
    expected_version: int = 0

    def to_wire(self) -> dict[str, object]:
        return {
            "node": self.node,
            "epoch": self.epoch,
            "inSync": self.in_sync,
            "expectedVersion": self.expected_version,
        }

    @staticmethod
    def from_wire(value: object) -> SyncRequest:
        fields = JsonObject(value)
        return SyncRequest(
            node=fields.string("node"),
            epoch=fields.integer("epoch"),
            in_sync=fields.boolean("inSync"),
            expected_version=fields.integer("expectedVersion"),
        )


@dataclass(frozen=True)
class SyncReply:
    epoch: int
    standby_in_sync: bool
    sync_version: int

    def to_wire(self) -> dict[str, object]:
        return {"epoch": self.epoch, "standbyInSync": self.standby_in_sync, "syncVersion": self.sync_version}

    @staticmethod
    def from_wire(value: object) -> SyncReply:
        fields = JsonObject(value)
        return SyncReply(fields.integer("epoch"), fields.boolean("standbyInSync"), fields.integer("syncVersion"))
