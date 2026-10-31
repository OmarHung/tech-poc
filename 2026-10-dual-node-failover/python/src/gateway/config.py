from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

from shared.env import EnvReader, Lookup, parse_url
from shared.errors import ConfigError
from shared.wire import is_node_id

UPSTREAM_TIMEOUT_S = 5.0


@dataclass(frozen=True)
class NodeTarget:
    id: str
    url: str


@dataclass(frozen=True)
class GatewayConfig:
    port: int = 8100
    nodes: tuple[NodeTarget, ...] = (
        NodeTarget("a", "http://127.0.0.1:8101"),
        NodeTarget("b", "http://127.0.0.1:8102"),
    )
    poll_interval_ms: int = 100
    """How often every node's /health is polled to find the primary. Part of the failover time."""

    @property
    def poll_interval(self) -> float:
        return self.poll_interval_ms / 1000

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> GatewayConfig:
        source = os.environ if environ is None else environ
        return cls.from_lookup(source.get)

    @classmethod
    def from_lookup(cls, lookup: Lookup) -> GatewayConfig:
        env = EnvReader(lookup)
        d = cls()
        raw_nodes = env.raw("NODE_URLS")
        return cls(
            port=env.port("PORT", d.port),
            nodes=d.nodes if raw_nodes is None else parse_nodes(raw_nodes),
            poll_interval_ms=env.integer("POLL_INTERVAL_MS", d.poll_interval_ms, 10, 10_000),
        )


def parse_nodes(raw: str) -> tuple[NodeTarget, ...]:
    """Parses `a=http://host:port,b=http://host:port`."""
    nodes: list[NodeTarget] = []
    for part in (p.strip() for p in raw.split(",")):
        if not part:
            continue
        pair = part.split("=", 1)
        if len(pair) != 2 or not is_node_id(pair[0]):
            raise ConfigError(f"NODE_URLS entries must look like a=http://127.0.0.1:8101, got '{part}'")
        nodes.append(NodeTarget(pair[0], parse_url("NODE_URLS", pair[1])))
    if not nodes or len({node.id for node in nodes}) != len(nodes):
        raise ConfigError(f"NODE_URLS must list distinct node ids, got '{raw}'")
    return tuple(nodes)
