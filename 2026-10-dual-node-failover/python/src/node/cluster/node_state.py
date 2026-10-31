"""Role, epoch and leases of this node, and the only way to write to its log.

A primary may serve while either lease is valid: the witness lease, or the "peer lease" the standby grants by accepting
replication (it promises not to seek the lease for one TTL after hearing from the primary). With two of three votes
either way, losing the witness alone or the standby alone does not stop the primary.

The .NET version guards all of this with one lock. Here every method is synchronous and runs on the event loop, which
gives the same guarantee: nothing can interleave between "am I still primary?" and "append".
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from pathlib import Path

from node.cluster.contracts import MAX_BATCH, ReplicateReply, ReplicateRequest, ReplicateStatus
from node.cluster.primary_term import Arbiter, PrimaryTerm, Signal
from node.config import ArbiterMode, NodeConfig
from node.storage.log_entry import LogEntry
from node.storage.replicated_log import AppendStatus, ReplicatedLog
from shared import state_file
from shared.clock import Clock
from shared.errors import Unavailable
from shared.wire import JsonObject

EPOCH_FILE_NAME = "epoch.json"

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class NodeHealth:
    """What GET /health returns. The gateway routes to the node that reports role = primary."""

    status: str
    node: str
    role: str
    epoch: int
    last_index: int
    match_index: int | None
    peer: str | None
    lease_remaining_ms: int | None
    truncated_entries: int
    arbiter: str
    replication: str

    def to_wire(self) -> dict[str, object]:
        return {
            "status": self.status,
            "node": self.node,
            "role": self.role,
            "epoch": self.epoch,
            "lastIndex": self.last_index,
            "matchIndex": self.match_index,
            "peer": self.peer,
            "leaseRemainingMs": self.lease_remaining_ms,
            "truncatedEntries": self.truncated_entries,
            "arbiter": self.arbiter,
            "replication": self.replication,
        }


def not_primary() -> Unavailable:
    return Unavailable("not primary")


class NodeState:
    def __init__(self, config: NodeConfig, log: ReplicatedLog, arbiter: Arbiter, clock: Clock) -> None:
        self._config = config
        self._arbiter = arbiter
        self.log = log
        self.clock = clock
        self._epoch_path = Path(config.data_dir) / EPOCH_FILE_NAME
        self._epoch = max(_load_epoch(self._epoch_path), log.last_epoch)
        self._term: PrimaryTerm | None = None
        self._witness_lease_until = -math.inf
        self._peer_lease_until = -math.inf
        # Count startup as having just heard from a primary: a node that restarts must not seek the lease while the
        # primary may still be relying on the promise this node made before it went down.
        self._last_heard_from_primary = clock.now()
        self._candidate = False
        self._next_term = Signal()

    @property
    def epoch(self) -> int:
        return self._epoch

    @property
    def current_term(self) -> PrimaryTerm | None:
        """The current term while this node is primary, even if its lease just ran out."""
        return self._term

    def serving_term(self) -> PrimaryTerm | None:
        """The current term if this node may serve right now (primary with a valid lease)."""
        if self._term is not None and self.clock.now() < self._lease_until():
            return self._term
        return None

    async def wait_for_term(self) -> PrimaryTerm:
        """Returns the current term, or the next one once this node is promoted."""
        while self._term is None:
            await self._next_term.current.wait()
        return self._term

    def append_as_primary(self, key: str, value: str) -> tuple[LogEntry, PrimaryTerm]:
        """Checks the lease and appends in one atomic step, so a node can never append after it stopped serving."""
        term = self.serving_term()
        if term is None:
            raise not_primary()
        entry = self.log.append(term.epoch, key, value)
        term.notify_appended()
        return entry, term

    def handle_replicate(self, request: ReplicateRequest) -> ReplicateReply:
        """The standby side of replication."""
        source = f"replication from {request.leader or ''}"
        if request.epoch < self._epoch:
            return self._rejected(f"epoch {request.epoch} is older than {self._epoch}")
        if self._candidate:
            # Accepting would promise the sender a lease while this node may be granted one by the witness.
            self._observe_epoch(request.epoch, source)
            return self._rejected("asking the witness for the lease")
        if request.epoch > self._epoch:
            self._observe_epoch(request.epoch, source)
        elif self._term is not None:
            return self._rejected(f"also primary in epoch {self._epoch}")  # only possible without a witness
        self._last_heard_from_primary = self.clock.now()
        outcome = self.log.append_from(
            request.epoch, request.prev_index, request.prev_epoch, request.entries, request.leader_last_index
        )
        # The promise: this node will not seek the lease for one TTL from now (see try_begin_candidacy).
        promise_ms = self._config.lease_ttl_ms
        if outcome.status is AppendStatus.OK:
            return ReplicateReply(ReplicateStatus.OK, self._epoch, outcome.match_index, promise_ms)
        return ReplicateReply(
            ReplicateStatus.MISMATCH,
            self._epoch,
            outcome.match_index,
            promise_ms,
            "log does not contain prevIndex/prevEpoch",
        )

    def try_promote(self, epoch: int, lease_until: float, standby_in_sync: bool, witness_version: int = 0) -> bool:
        """Starts a primary term. Returns false if a newer epoch is already known.
        standby_in_sync and witness_version are what the arbiter said when it granted the lease."""
        if epoch < self._epoch:
            return False
        if self._term is not None:
            self._step_down("starting a new term")
        self._save_epoch(epoch)
        term = PrimaryTerm(epoch, standby_in_sync, witness_version, self.log, self._arbiter, MAX_BATCH)
        self._term = term
        self._witness_lease_until = lease_until
        self._peer_lease_until = -math.inf
        self._next_term.pulse()
        logger.warning(
            "Promoted to primary in epoch %d at index %d; standby in sync: %s",
            epoch,
            self.log.last_index,
            standby_in_sync,
        )
        return True

    def extend_witness_lease(self, term: PrimaryTerm, until: float) -> None:
        if self._term is term and until > self._witness_lease_until:
            self._witness_lease_until = until

    def extend_peer_lease(self, term: PrimaryTerm, until: float) -> None:
        if self._term is term and until > self._peer_lease_until:
            self._peer_lease_until = until

    def step_down(self, term: PrimaryTerm, reason: str) -> None:
        if self._term is term:
            self._step_down(reason)

    def step_down_if_lease_expired(self) -> None:
        if self._term is not None and self.serving_term() is None:
            self._step_down("lease expired")

    def observe_epoch(self, epoch: int, source: str) -> None:
        """Adopts a newer epoch seen elsewhere; a primary that sees one has been deposed."""
        self._observe_epoch(epoch, source)

    def shut_down(self) -> None:
        self._step_down("shutting down")

    def try_begin_candidacy(self) -> bool:
        """Marks this standby as a candidate if it has not heard from a primary for a full lease (plus the election
        delay). While a candidate it refuses replication, so no primary can count on it while it asks for the lease."""
        silence = self.clock.now() - self._last_heard_from_primary
        if self._term is not None or self._candidate or silence < self._config.lease_ttl + self._config.election_delay:
            return False
        self._candidate = True
        return True

    def end_candidacy(self) -> None:
        self._candidate = False

    def health(self) -> NodeHealth:
        serving = self.serving_term()
        lease_remaining_ms: int | None = None
        if serving is not None and self._config.arbiter is not ArbiterMode.NONE:
            remaining = self._lease_until() - self.clock.now()
            lease_remaining_ms = int(remaining * 1000) if math.isfinite(remaining) else None
        return NodeHealth(
            status="ok",
            node=self._config.node_id,
            role="standby" if serving is None else "primary",
            epoch=self._epoch,
            last_index=self.log.last_index,
            match_index=None if serving is None else serving.match_index,
            peer=None if serving is None else serving.peer_state.value,
            lease_remaining_ms=lease_remaining_ms,
            truncated_entries=self.log.truncated_entries,
            arbiter=self._config.arbiter.value,
            replication=self._config.replication.value,
        )

    def _lease_until(self) -> float:
        return max(self._witness_lease_until, self._peer_lease_until)

    def _observe_epoch(self, epoch: int, source: str) -> None:
        if epoch <= self._epoch:
            return
        self._save_epoch(epoch)
        if self._term is not None:
            self._step_down(f"epoch {epoch} seen in {source}")
            # Someone newer is primary; give it a full lease to reach this node before competing again.
            self._last_heard_from_primary = self.clock.now()

    def _step_down(self, reason: str) -> None:
        term = self._term
        if term is None:
            return
        self._term = None
        self._witness_lease_until = -math.inf
        self._peer_lease_until = -math.inf
        term.end()
        logger.warning("Stepped down from epoch %d at index %d: %s", term.epoch, self.log.last_index, reason)

    def _save_epoch(self, epoch: int) -> None:
        if epoch == self._epoch:
            return
        state_file.save(self._epoch_path, {"epoch": epoch})
        self._epoch = epoch

    def _rejected(self, reason: str) -> ReplicateReply:
        return ReplicateReply(ReplicateStatus.REJECTED, self._epoch, self.log.last_index, 0, reason)


def _load_epoch(path: Path) -> int:
    saved = state_file.load(path)
    return 0 if saved is None else JsonObject(saved).integer("epoch")
