from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

from shared.env import EnvReader, Lookup
from shared.errors import ConfigError
from shared.wire import is_node_id


@dataclass(frozen=True)
class WitnessConfig:
    port: int = 8103
    data_dir: str = "data/witness"
    lease_ttl_ms: int = 3000
    nodes: tuple[str, ...] = ("a", "b")
    """The two node ids allowed to hold the lease."""

    def __post_init__(self) -> None:
        if len(self.nodes) != 2 or self.nodes[0] == self.nodes[1] or not all(map(is_node_id, self.nodes)):
            raise ConfigError(
                f"NODES must be two different ids of lowercase letters, digits or '-', got '{','.join(self.nodes)}'"
            )

    @property
    def lease_ttl(self) -> float:
        return self.lease_ttl_ms / 1000

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> WitnessConfig:
        source = os.environ if environ is None else environ
        return cls.from_lookup(source.get)

    @classmethod
    def from_lookup(cls, lookup: Lookup) -> WitnessConfig:
        env = EnvReader(lookup)
        d = cls()
        return cls(
            port=env.port("PORT", d.port),
            data_dir=env.string("DATA_DIR", d.data_dir),
            lease_ttl_ms=env.integer("LEASE_TTL_MS", d.lease_ttl_ms, 100, 60_000),
            nodes=tuple(part.strip() for part in env.string("NODES", ",".join(d.nodes)).split(",")),
        )
