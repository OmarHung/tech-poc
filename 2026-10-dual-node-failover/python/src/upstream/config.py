from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

from shared.env import EnvReader, Lookup


@dataclass(frozen=True)
class UpstreamConfig:
    port: int = 8120
    task_lease_ttl_ms: int = 2000
    """How long a worker holds a task without a heartbeat before it is handed to someone else."""
    reap_interval_ms: int = 100
    """How often expired task leases are looked for."""

    @property
    def task_lease_ttl(self) -> float:
        return self.task_lease_ttl_ms / 1000

    @property
    def reap_interval(self) -> float:
        return self.reap_interval_ms / 1000

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> UpstreamConfig:
        source = os.environ if environ is None else environ
        return cls.from_lookup(source.get)

    @classmethod
    def from_lookup(cls, lookup: Lookup) -> UpstreamConfig:
        env = EnvReader(lookup)
        d = cls()
        return cls(
            port=env.port("PORT", d.port),
            task_lease_ttl_ms=env.integer("TASK_LEASE_TTL_MS", d.task_lease_ttl_ms, 100, 60_000),
        )
