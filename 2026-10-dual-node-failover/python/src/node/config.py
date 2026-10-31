from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum

from shared.env import EnvReader, Lookup
from shared.errors import ConfigError
from shared.wire import is_node_id


class ArbiterMode(StrEnum):
    WITNESS = "witness"
    """A third process hands out the primary lease (the design under test)."""
    NONE = "none"
    """The standby promotes itself when the primary goes quiet. The classic setup that can split-brain;
    kept as the control group."""


class ReplicationMode(StrEnum):
    SYNC = "sync"
    """A write is acknowledged once it is on the standby too (or the witness agreed the standby is out)."""
    ASYNC = "async"
    """A write is acknowledged once it is on this node; the standby catches up in the background."""


@dataclass(frozen=True)
class NodeConfig:
    port: int = 8101
    node_id: str = "a"
    peer_url: str = "http://127.0.0.1:8102"
    witness_url: str = "http://127.0.0.1:8103"
    data_dir: str = "data/a"
    arbiter: ArbiterMode = ArbiterMode.WITNESS
    replication: ReplicationMode = ReplicationMode.SYNC
    lease_ttl_ms: int = 3000
    """How long a lease lasts, and how long a standby must hear nothing from the primary before it seeks the lease."""
    renew_interval_ms: int = 500
    """How often the primary renews its lease and heartbeats the standby. Also the timeout of every call to them."""
    replication_timeout_ms: int = 1000
    """How long a sync write waits for the standby before asking the witness to go on without it."""
    election_delay_ms: int = 0
    """Extra wait before this node tries to become primary. Without a witness it plays keepalived's priority."""

    def __post_init__(self) -> None:
        """Checks the rules that span several settings. Raises with the variable name to fix."""
        if not is_node_id(self.node_id):
            raise ConfigError(f"NODE_ID must be 1-32 lowercase letters, digits or '-', got '{self.node_id}'")
        # One slow round trip must not cost the primary its lease: it gets several renewals per lease.
        if self.lease_ttl_ms < self.renew_interval_ms * 3:
            raise ConfigError(
                f"LEASE_TTL_MS ({self.lease_ttl_ms}) must be at least 3 x RENEW_INTERVAL_MS ({self.renew_interval_ms})"
            )
        # A write stuck on the standby must still have lease left when it asks the witness to go on alone.
        if self.replication_timeout_ms * 2 > self.lease_ttl_ms:
            raise ConfigError(
                f"REPLICATION_TIMEOUT_MS ({self.replication_timeout_ms}) must be at most half of "
                f"LEASE_TTL_MS ({self.lease_ttl_ms})"
            )

    @property
    def lease_ttl(self) -> float:
        return self.lease_ttl_ms / 1000

    @property
    def renew_interval(self) -> float:
        return self.renew_interval_ms / 1000

    @property
    def replication_timeout(self) -> float:
        return self.replication_timeout_ms / 1000

    @property
    def election_delay(self) -> float:
        return self.election_delay_ms / 1000

    @property
    def lease_safety_margin(self) -> float:
        """The primary treats its leases as ending this much earlier than the other side does, to absorb clock drift."""
        return self.lease_ttl / 10

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> NodeConfig:
        source = os.environ if environ is None else environ
        return cls.from_lookup(source.get)

    @classmethod
    def from_lookup(cls, lookup: Lookup) -> NodeConfig:
        env = EnvReader(lookup)
        d = cls()
        node_id = env.string("NODE_ID", d.node_id)
        return cls(
            port=env.port("PORT", d.port),
            node_id=node_id,
            peer_url=env.url("PEER_URL", d.peer_url),
            witness_url=env.url("WITNESS_URL", d.witness_url),
            data_dir=env.string("DATA_DIR", f"data/{node_id}"),
            arbiter=env.choice("ARBITER", d.arbiter, {mode.value: mode for mode in ArbiterMode}),
            replication=env.choice("REPLICATION", d.replication, {mode.value: mode for mode in ReplicationMode}),
            lease_ttl_ms=env.integer("LEASE_TTL_MS", d.lease_ttl_ms, 100, 60_000),
            renew_interval_ms=env.integer("RENEW_INTERVAL_MS", d.renew_interval_ms, 10, 20_000),
            replication_timeout_ms=env.integer("REPLICATION_TIMEOUT_MS", d.replication_timeout_ms, 10, 30_000),
            election_delay_ms=env.integer("ELECTION_DELAY_MS", d.election_delay_ms, 0, 60_000),
        )
