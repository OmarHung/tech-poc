"""The tie-breaker between the two nodes.

It hands out one lease at a time and bumps the epoch on every handover, so a deposed primary can be recognised by its
old epoch (the fencing token). It also remembers whether the standby has every acknowledged write, and refuses to
promote it if not: a stale standby taking over would silently drop writes the clients were told had succeeded.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, replace
from pathlib import Path

from shared import state_file
from shared.clock import Clock
from shared.errors import Conflict, InvalidInput
from shared.wire import JsonObject
from shared.witness_contracts import LeaseReply, SyncReply, SyncRequest
from witness.config import WitnessConfig

STATE_FILE_NAME = "witness.json"

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WitnessState:
    """What the witness keeps on disk. sync_version is bumped on every accepted sync report and every handover, so a
    stale "in sync" can be told apart."""

    epoch: int
    holder: str | None
    standby_in_sync: bool
    sync_version: int = 0

    def to_wire(self) -> dict[str, object]:
        return {
            "epoch": self.epoch,
            "holder": self.holder,
            "standbyInSync": self.standby_in_sync,
            "syncVersion": self.sync_version,
        }

    @staticmethod
    def from_wire(value: object) -> WitnessState:
        fields = JsonObject(value)
        return WitnessState(
            fields.integer("epoch"),
            fields.string("holder"),
            fields.boolean("standbyInSync"),
            fields.integer("syncVersion"),
        )


class LeaseTable:
    """Every method is synchronous, so each runs start to finish without another request in between."""

    def __init__(self, config: WitnessConfig, clock: Clock) -> None:
        self._config = config
        self._clock = clock
        self._path = Path(config.data_dir) / STATE_FILE_NAME
        saved = state_file.load(self._path)
        self._state = WitnessState(0, None, False) if saved is None else WitnessState.from_wire(saved)
        # After a restart the old holder may still be inside its lease, so nobody else gets it for one more TTL.
        self._expires_at = -math.inf if self._state.holder is None else clock.now() + config.lease_ttl

    @property
    def state(self) -> WitnessState:
        return self._state

    def acquire(self, node: str | None) -> LeaseReply:
        """Grants, renews or refuses the lease. Renewing and acquiring are the same call."""
        self._require_known(node)
        now = self._clock.now()
        holder = self._state.holder
        if holder == node:
            # Also after expiry: nobody else took over in between, so the holder still has the newest data.
            self._expires_at = now + self._config.lease_ttl
            return self._reply(granted=True)
        if holder is not None and now < self._expires_at:
            return self._reply(granted=False, reason=f"lease held by {holder}")
        if holder is not None and not self._state.standby_in_sync:
            return self._reply(
                granted=False, reason=f"{node} may be missing acknowledged writes; waiting for {holder} to return"
            )
        # The node that just lost the lease is behind until the new holder says otherwise.
        self._save(WitnessState(self._state.epoch + 1, node, False, self._state.sync_version + 1))
        self._expires_at = now + self._config.lease_ttl
        logger.warning("Lease handed from %s to %s, epoch %d", holder or "nobody", node, self._state.epoch)
        return self._reply(granted=True)

    def set_standby_in_sync(self, request: SyncRequest) -> SyncReply:
        """Records whether the standby may be promoted. Only the holder, in its own epoch, may say so. "Not in sync" is
        always accepted; "in sync" only if nothing changed since the holder last looked, because an HTTP request the
        holder gave up on can still arrive later and must not undo a newer "not in sync"."""
        self._require_known(request.node)
        state = self._state
        if state.holder != request.node or state.epoch != request.epoch:
            raise Conflict(
                f"{request.node} is not the lease holder in epoch {request.epoch} "
                f"(holder {state.holder or 'none'}, epoch {state.epoch})"
            )
        if request.in_sync and request.expected_version != state.sync_version:
            raise Conflict(
                f"standby state changed since version {request.expected_version} (now {state.sync_version}); "
                "check again"
            )
        changed = state.standby_in_sync != request.in_sync
        self._save(replace(state, standby_in_sync=request.in_sync, sync_version=state.sync_version + 1))
        if changed:
            logger.info("Epoch %d: standby in sync = %s", self._state.epoch, request.in_sync)
        return SyncReply(self._state.epoch, self._state.standby_in_sync, self._state.sync_version)

    def health(self) -> dict[str, object]:
        state = self._state
        remaining = 0.0 if state.holder is None else self._expires_at - self._clock.now()
        return {
            "status": "ok",
            "service": "witness",
            "holder": state.holder,
            "epoch": state.epoch,
            "standbyInSync": state.standby_in_sync,
            "syncVersion": state.sync_version,
            "leaseRemainingMs": int(max(0.0, remaining) * 1000),
        }

    def _reply(self, granted: bool, reason: str | None = None) -> LeaseReply:
        state = self._state
        return LeaseReply(
            granted,
            state.holder,
            state.epoch,
            self._config.lease_ttl_ms,
            state.standby_in_sync,
            state.sync_version,
            reason,
        )

    def _save(self, state: WitnessState) -> None:
        """Persists first, then adopts: if the write fails, the witness has promised nothing new."""
        state_file.save(self._path, state.to_wire())
        self._state = state

    def _require_known(self, node: str | None) -> None:
        if node is None or node not in self._config.nodes:
            raise InvalidInput(f"node must be one of {', '.join(self._config.nodes)}, got '{node or ''}'")
